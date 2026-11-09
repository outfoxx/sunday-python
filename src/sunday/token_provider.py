# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Application credential acquisition and storage contracts."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, fields
from time import time
from typing import Literal, Protocol, cast

from .request_cancellation_error import RequestCancellationError
from .security import SecurityBinding, SecurityEndpoints


@dataclass(frozen=True, slots=True)
class TokenSet:
    """Credentials with an optional expiry in Unix seconds; secrets are omitted from repr."""

    access_token: str = field(repr=False)
    expires_at: float | None = None
    refresh_token: str | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class TokenConfiguration:
    """Non-secret identities isolate clients and sessions; grant_identity changes with grant inputs."""

    client_identity: str
    grant_identity: str | None = None
    endpoints: SecurityEndpoints | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class TokenRequest(SecurityBinding):
    """Resolved acquisition parameters without client secrets or authorization codes."""

    client_identity: str
    grant_identity: str | None = None


class TokenProvider(Protocol):
    """Application provider; cancellation propagates to its asynchronous acquisition calls."""

    @property
    def identity(self) -> str:
        """Non-secret provider identity used to isolate cached credentials."""
        ...

    def configure(self, binding: SecurityBinding) -> TokenConfiguration:
        """Resolve the application client/session and optional deployment endpoint overrides."""
        ...

    async def acquire(self, request: TokenRequest) -> TokenSet:
        """Acquire credentials for a fresh grant or an external/static binding."""
        ...


class RefreshingTokenProvider(TokenProvider, Protocol):
    """Optional provider capability for rotating an existing refresh token."""

    async def refresh(self, request: TokenRequest, refresh_token: str) -> TokenSet:
        """Exchange a refresh token; invalid interactive sessions raise AuthorizationRequiredError."""
        ...


class TokenStore(Protocol):
    """Application-owned credential storage; implementations must keep token values private."""

    async def load(self, key: str) -> TokenSet | None:
        """Return stored credentials for this non-secret cache key."""
        ...

    async def save(self, key: str, tokens: TokenSet) -> None:
        """Atomically replace credentials, including a newly rotated refresh token."""
        ...

    async def remove(self, key: str) -> None:
        """Remove credentials for a terminated application session."""
        ...


class AuthorizationRequiredError(RequestCancellationError):
    """The application must obtain fresh authorization and supply a new grant identity."""

    def __init__(self) -> None:
        super().__init__("Fresh application authorization is required")


class TokenProviderError(Exception):
    """Safe provider failure that does not expose credential-bearing provider messages."""

    def __init__(self, reason: Literal["unavailable", "temporary", "invalid_grant"] = "unavailable") -> None:
        self.reason = reason
        super().__init__("The credential provider could not supply usable credentials")


class TokenRejectedError(RequestCancellationError):
    """The credential owner declined automatic recovery of a rejected token."""

    def __init__(self) -> None:
        super().__init__("The credential owner requires application recovery")


@dataclass(frozen=True, slots=True)
class TokenLease:
    """Receipt retaining only the original credential's rejection context."""

    key: str = field(repr=False)
    tokens: TokenSet = field(repr=False)
    rejection: Callable[[], Awaitable[bool]] | None = field(default=None, repr=False, compare=False)

    async def invalidate(self) -> bool:
        """Reject this credential through its owner, preserving concurrent replacements."""
        try:
            return await self.rejection() if self.rejection is not None else False
        except (RequestCancellationError, TokenProviderError):
            raise
        except Exception:
            raise TokenProviderError() from None


class RejectingTokenProvider(TokenProvider, Protocol):
    """Optional capability for external frameworks that own rejection recovery."""

    async def reject(self, request: TokenRequest, rejected_token: str) -> bool:
        """Reject only this token in its original session; return false if recovery is unsupported."""
        ...


async def provider_credentials(provider: TokenProvider, binding: SecurityBinding) -> TokenLease:
    """Acquire once without adding a token cache or managing refresh credentials."""
    await asyncio.sleep(0)
    credentials = getattr(provider, "credentials", None)
    if callable(credentials):
        return cast(TokenLease, await credentials(binding))
    try:
        request = _resolve_token_request(provider, binding)
        tokens = await provider.acquire(request)
        await asyncio.sleep(0)
        if not tokens.access_token or (
            tokens.expires_at is not None and (not math.isfinite(tokens.expires_at) or tokens.expires_at <= time())
        ):
            raise TokenProviderError()

        async def reject() -> bool:
            hook = getattr(provider, "reject", None)
            return bool(await hook(request, tokens.access_token)) if callable(hook) else False

        return TokenLease("", tokens, reject)
    except (RequestCancellationError, TokenProviderError):
        raise
    except Exception:
        raise TokenProviderError() from None


def _resolve_token_request(provider: TokenProvider, binding: SecurityBinding) -> TokenRequest:
    """Resolve provider configuration without applying cache or grant-reuse policy."""
    config = provider.configure(binding)
    if not provider.identity.strip() or not config.client_identity.strip():
        raise TokenProviderError()
    values = {item.name: getattr(binding, item.name) for item in fields(SecurityBinding)}
    if config.endpoints is not None:
        values.update(
            {
                item.name: value
                for item in fields(config.endpoints)
                if (value := getattr(config.endpoints, item.name)) is not None
            }
        )
    values["scopes"] = tuple(sorted(set(binding.scopes)))
    return TokenRequest(**values, client_identity=config.client_identity, grant_identity=config.grant_identity)
