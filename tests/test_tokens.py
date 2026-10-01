# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

import asyncio
from dataclasses import replace

import pytest

from sunday.security import SecurityBinding, SecurityEndpoints, SecurityTransport
from sunday.token_manager import TokenManager
from sunday.token_provider import (
    AuthorizationRequiredError,
    TokenConfiguration,
    TokenProviderError,
    TokenRequest,
    TokenSet,
)

BINDING = SecurityBinding(
    scheme="oauth",
    provider="identity",
    profile="external",
    flow="clientCredentials",
    token_url="https://identity.example/token",
    scopes=("read", "write"),
    transport=SecurityTransport(location="header", name="Authorization", prefix="Bearer"),
)


class Provider:
    identity = "application-provider"

    def __init__(self) -> None:
        self.configuration = TokenConfiguration("client", "session")
        self.acquisitions: list[TokenRequest] = []
        self.refreshes: list[str] = []
        self.tokens = TokenSet("first", 100, "refresh-first")

    def configure(self, _binding: SecurityBinding) -> TokenConfiguration:
        return self.configuration

    async def acquire(self, request: TokenRequest) -> TokenSet:
        self.acquisitions.append(request)
        return self.tokens

    async def refresh(self, _request: TokenRequest, refresh_token: str) -> TokenSet:
        self.refreshes.append(refresh_token)
        return self.tokens


@pytest.mark.anyio
async def test_expiry_skew_rotation_and_conditional_invalidation() -> None:
    provider = Provider()
    now = 0.0
    manager = TokenManager({"identity": provider}, now=lambda: now)
    first = await manager.credentials(BINDING)
    assert await manager.credentials(BINDING) == first
    assert len(provider.acquisitions) == 1
    now = 71
    provider.tokens = TokenSet("second", 200, "refresh-second")
    second = await manager.credentials(BINDING)
    assert second.tokens.access_token == "second"
    assert provider.refreshes == ["refresh-first"]
    await manager.invalidate(first)
    assert await manager.credentials(BINDING) == second
    await manager.invalidate(second)
    provider.tokens = TokenSet("third", 300)
    third = await manager.credentials(BINDING)
    assert third.tokens.refresh_token == "refresh-second"
    assert provider.refreshes == ["refresh-first", "refresh-second"]
    assert "third" not in repr(third) and "refresh-second" not in repr(third.tokens)


@pytest.mark.anyio
async def test_interactive_session_cannot_reuse_authorization() -> None:
    provider = Provider()
    provider.tokens = TokenSet("initial", 100)
    manager = TokenManager({"identity": provider}, now=lambda: 0)
    binding = replace(BINDING, flow="authorizationCode")
    initial = await manager.credentials(binding)
    await manager.invalidate(initial)
    with pytest.raises(AuthorizationRequiredError):
        await manager.credentials(binding)
    assert len(provider.acquisitions) == 1
    provider.configuration = replace(provider.configuration, grant_identity="new-session")
    assert (await manager.credentials(binding)).tokens.access_token == "initial"
    assert len(provider.acquisitions) == 2


@pytest.mark.anyio
async def test_failed_authorization_is_not_retried_and_errors_are_safe() -> None:
    class FailingProvider(Provider):
        async def acquire(self, request: TokenRequest) -> TokenSet:
            self.acquisitions.append(request)
            raise ValueError("SECRET credentials must not appear")

    provider = FailingProvider()
    manager = TokenManager({"identity": provider}, now=lambda: 0)
    with pytest.raises(TokenProviderError, match="could not supply") as error:
        await manager.credentials(replace(BINDING, flow="authorizationCode"))
    assert "SECRET" not in str(error.value)
    assert error.value.__suppress_context__
    with pytest.raises(AuthorizationRequiredError):
        await manager.credentials(replace(BINDING, flow="authorizationCode"))
    assert len(provider.acquisitions) == 1
    # Noninteractive acquisition failures are not cached.
    for _ in range(2):
        with pytest.raises(TokenProviderError):
            await manager.credentials(BINDING)
    assert len(provider.acquisitions) == 3


@pytest.mark.anyio
async def test_cache_key_includes_every_acquisition_dimension() -> None:
    provider = Provider()
    manager = TokenManager({"identity": provider, "alternate": provider}, now=lambda: 0)
    first = await manager.credentials(BINDING)
    assert await manager.credentials(replace(BINDING, scopes=("write", "read", "read"))) == first
    for binding in (
        replace(BINDING, profile="internal"),
        replace(BINDING, scopes=("read",)),
        replace(BINDING, audience="other"),
        replace(BINDING, resource="api"),
        replace(BINDING, flow="external"),
        replace(BINDING, provider="alternate"),
        replace(BINDING, discovery_url="https://id.example/discovery"),
        replace(BINDING, authorization_url="https://id.example/authorize"),
        replace(BINDING, refresh_url="https://id.example/refresh"),
        replace(BINDING, token_url="https://id.example/token"),
    ):
        assert (await manager.credentials(binding)).key != first.key
    for configuration in (
        TokenConfiguration("other-client", "session"),
        TokenConfiguration("client", "other-session"),
        TokenConfiguration("client", "session", SecurityEndpoints(token_url="https://override.example/token")),
    ):
        provider.configuration = configuration
        assert (await manager.credentials(BINDING)).key != first.key
    assert provider.acquisitions[-1].profile == "external"
    provider.identity = "other-provider"
    assert (await manager.credentials(BINDING)).key != first.key
    assert len(provider.acquisitions) == 15


@pytest.mark.anyio
async def test_renewal_coalesces_and_one_cancellation_does_not_cancel_other_waiters() -> None:
    started, finish, canceled = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class WaitingProvider(Provider):
        async def acquire(self, request: TokenRequest) -> TokenSet:
            self.acquisitions.append(request)
            started.set()
            try:
                await finish.wait()
                return self.tokens
            except asyncio.CancelledError:
                canceled.set()
                raise

    provider = WaitingProvider()
    manager = TokenManager({"identity": provider}, now=lambda: 0)
    first = asyncio.create_task(manager.credentials(BINDING))
    await started.wait()
    second = asyncio.create_task(manager.credentials(BINDING))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert not canceled.is_set()
    finish.set()
    assert (await second).tokens.access_token == "first"
    assert len(provider.acquisitions) == 1


@pytest.mark.anyio
async def test_final_waiter_cancellation_cancels_acquisition_and_new_caller_can_retry() -> None:
    started, canceled = asyncio.Event(), asyncio.Event()

    class WaitingProvider(Provider):
        async def acquire(self, request: TokenRequest) -> TokenSet:
            self.acquisitions.append(request)
            if len(self.acquisitions) > 1:
                return self.tokens
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                canceled.set()
            raise AssertionError("unreachable")

    provider = WaitingProvider()
    manager = TokenManager({"identity": provider}, now=lambda: 0)
    waiter = asyncio.create_task(manager.credentials(BINDING))
    await started.wait()
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    await canceled.wait()
    assert (await manager.credentials(BINDING)).tokens.access_token == "first"


@pytest.mark.anyio
async def test_cancellation_after_rotation_preserves_application_storage() -> None:
    saving, finish = asyncio.Event(), asyncio.Event()

    class Store:
        stored: TokenSet | None = None

        async def load(self, _key: str) -> TokenSet | None:
            return self.stored

        async def save(self, _key: str, tokens: TokenSet) -> None:
            saving.set()
            await finish.wait()
            self.stored = tokens

        async def remove(self, _key: str) -> None:
            self.stored = None

    store = Store()
    provider = Provider()
    manager = TokenManager({"identity": provider}, store=store, now=lambda: 0)
    waiter = asyncio.create_task(manager.credentials(BINDING))
    await saving.wait()
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    finish.set()
    assert (await manager.credentials(BINDING)).tokens == provider.tokens
    assert store.stored == provider.tokens
    assert len(provider.acquisitions) == 1


@pytest.mark.anyio
async def test_pre_canceled_call_does_not_start_acquisition() -> None:
    provider = Provider()
    manager = TokenManager({"identity": provider}, now=lambda: 0)
    task = asyncio.create_task(manager.credentials(BINDING))
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert provider.acquisitions == []


@pytest.mark.anyio
async def test_missing_provider_invalid_tokens_and_session_fail_before_use() -> None:
    with pytest.raises(TokenProviderError):
        await TokenManager({}).credentials(BINDING)
    provider = Provider()
    for token in (TokenSet(""), TokenSet("token", float("nan")), TokenSet("token", 0)):
        provider.tokens = token
        with pytest.raises(TokenProviderError):
            await TokenManager({"identity": provider}, now=lambda: 0).credentials(BINDING)
    provider.configuration = TokenConfiguration("client")
    with pytest.raises(TokenProviderError):
        await TokenManager({"identity": provider}).credentials(replace(BINDING, flow="authorizationCode"))
