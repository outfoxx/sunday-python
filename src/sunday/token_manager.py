# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Shared token lifecycle for the supported asyncio runtime."""

import asyncio
import hashlib
import json
import math
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from time import time
from typing import cast
from uuid import uuid4

from .request_cancellation_error import RequestCancellationError
from .security import SecurityBinding
from .token_provider import (
    AuthorizationRequiredError,
    RefreshingTokenProvider,
    TokenConfiguration,
    TokenLease,
    TokenProvider,
    TokenProviderError,
    TokenRequest,
    TokenSet,
    TokenStore,
    _resolve_token_request,
)


@dataclass(slots=True)
class _Renewal:
    task: asyncio.Task[TokenLease]
    waiters: int = 0
    committing: bool = False


@dataclass(slots=True)
class _Lock:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    users: int = 0


class TokenManager:
    """Cache and renew tokens per provider/client/profile/grant on a single asyncio event loop.

    A canceled caller leaves other waiters running. Canceling the last waiter cancels acquisition;
    once persistence begins, its rotated token is saved even if every caller has canceled.
    Share one manager for each application token store to coalesce renewal across clients.
    """

    def __init__(
        self,
        providers: Mapping[str, TokenProvider] | TokenProvider,
        *,
        store: TokenStore | None = None,
        expiry_skew: float = 30,
        now: Callable[[], float] = time,
    ) -> None:
        if not math.isfinite(expiry_skew) or expiry_skew < 0:
            raise ValueError("Invalid token expiry skew")
        self._provider = None if isinstance(providers, Mapping) else providers
        self._providers = dict(providers) if isinstance(providers, Mapping) else {}
        self.identity = self._provider.identity if self._provider is not None else str(uuid4())
        self._store = store if store is not None else _MemoryTokenStore()
        self._skew = expiry_skew
        self._now = now
        self._renewals: dict[str, _Renewal] = {}
        self._locks: dict[str, _Lock] = {}
        self._authorization_attempts: set[bytes] = set()

    def configure(self, binding: SecurityBinding) -> TokenConfiguration:
        """Forward configuration to the explicitly managed provider."""
        provider = self._provider if self._provider is not None else self._providers[binding.provider]
        return provider.configure(binding)

    async def acquire(self, request: TokenRequest) -> TokenSet:
        """Acquire managed credentials; transports use receipts to preserve rejection identity."""
        return (await self.credentials(request)).tokens

    async def credentials(self, binding: SecurityBinding) -> TokenLease:
        """Obtain current credentials without reusing a failed or consumed authorization grant."""
        # Honor a pending caller cancellation before creating work in an independent task.
        await asyncio.sleep(0)
        provider, request, key = self._resolve(binding)
        renewal = self._renewals.get(key)
        if renewal is None or renewal.task.cancelling():
            task = asyncio.create_task(self._renew(key, provider, request))
            renewal = _Renewal(task)
            self._renewals[key] = renewal
            task.add_done_callback(lambda completed: self._finished(key, completed))
        renewal.waiters += 1
        try:
            await asyncio.wait((renewal.task,))
            lease = renewal.task.result()

            async def reject() -> bool:
                await self.invalidate(lease)
                return True

            return replace(lease, rejection=reject)
        finally:
            renewal.waiters -= 1
            if renewal.waiters == 0 and not renewal.committing and not renewal.task.done():
                renewal.task.cancel()

    async def invalidate(self, lease: TokenLease) -> None:
        """Expire a rejected credential while preserving refresh tokens and concurrent renewals."""
        try:
            async with self._exclusive(lease.key):
                current = await self._store.load(lease.key)
                if current is not None and current.access_token == lease.tokens.access_token:
                    await self._store.save(lease.key, replace(current, expires_at=0))
        except RequestCancellationError:
            raise
        except Exception:
            raise TokenProviderError() from None

    def _resolve(self, binding: SecurityBinding) -> tuple[TokenProvider, TokenRequest, str]:
        try:
            provider = self._provider if self._provider is not None else self._providers[binding.provider]
            request = _resolve_token_request(provider, binding)
            if request.flow == "authorizationCode" and not (request.grant_identity or "").strip():
                raise TokenProviderError()
            key = json.dumps(
                [
                    request.scheme,
                    binding.provider,
                    provider.identity,
                    request.client_identity,
                    request.grant_identity,
                    request.profile,
                    request.flow,
                    request.discovery_url,
                    request.authorization_url,
                    request.token_url,
                    request.refresh_url,
                    request.scopes,
                    request.audience,
                    request.resource,
                ],
                separators=(",", ":"),
            )
            return provider, request, key
        except RequestCancellationError:
            raise
        except Exception:
            raise TokenProviderError() from None

    @asynccontextmanager
    async def _exclusive(self, key: str) -> AsyncIterator[None]:
        entry = self._locks.setdefault(key, _Lock())
        entry.users += 1
        try:
            async with entry.lock:
                yield
        finally:
            entry.users -= 1
            if entry.users == 0:
                self._locks.pop(key, None)

    async def _renew(self, key: str, provider: TokenProvider, request: TokenRequest) -> TokenLease:
        # Register the renewal and its waiters before work starts, even with an eager task factory.
        await asyncio.sleep(0)
        authorization_key = hashlib.sha256(key.encode()).digest()
        try:
            async with self._exclusive(key):
                stored = await self._store.load(key)
                if stored is not None and (stored.expires_at is None or stored.expires_at > self._now() + self._skew):
                    return TokenLease(key, stored)
                if stored is not None and stored.refresh_token and callable(getattr(provider, "refresh", None)):
                    try:
                        tokens = await cast(RefreshingTokenProvider, provider).refresh(request, stored.refresh_token)
                        if tokens.refresh_token is None:
                            tokens = replace(tokens, refresh_token=stored.refresh_token)
                    except TokenProviderError as error:
                        if isinstance(error, RequestCancellationError):
                            raise
                        if error.reason != "invalid_grant" or request.flow != "clientCredentials":
                            raise
                        await asyncio.sleep(0)
                        await self._store.remove(key)
                        tokens = await provider.acquire(request)
                elif request.flow == "authorizationCode" and (
                    stored is not None or authorization_key in self._authorization_attempts
                ):
                    raise AuthorizationRequiredError()
                else:
                    if request.flow == "authorizationCode":
                        # Never evict a consumed grant to make space: that would permit reuse.
                        if len(self._authorization_attempts) >= 1024:
                            raise AuthorizationRequiredError()
                        self._authorization_attempts.add(authorization_key)
                    tokens = await provider.acquire(request)
                if not tokens.access_token or (
                    tokens.expires_at is not None
                    and (not math.isfinite(tokens.expires_at) or tokens.expires_at <= self._now())
                ):
                    raise TokenProviderError()
                task = asyncio.current_task()
                renewal = self._renewals.get(key)
                if (
                    task is None
                    or task.cancelling()
                    or renewal is None
                    or renewal.task is not task
                    or not renewal.waiters
                ):
                    raise asyncio.CancelledError
                renewal.committing = True
                await self._store.save(key, tokens)
                return TokenLease(key, tokens)
        except (AuthorizationRequiredError, TokenProviderError):
            raise
        except RequestCancellationError:
            raise
        except Exception:
            raise TokenProviderError() from None

    def _finished(self, key: str, task: asyncio.Task[TokenLease]) -> None:
        if (renewal := self._renewals.get(key)) is not None and renewal.task is task:
            del self._renewals[key]
        if not task.cancelled():
            task.exception()  # Retrieve failures even when the last waiter canceled during storage.


class _MemoryTokenStore:
    def __init__(self) -> None:
        self._tokens: dict[str, TokenSet] = {}

    async def load(self, key: str) -> TokenSet | None:
        return self._tokens.get(key)

    async def save(self, key: str, tokens: TokenSet) -> None:
        self._tokens[key] = tokens

    async def remove(self, key: str) -> None:
        self._tokens.pop(key, None)
