"""Application-owned manager storage survives settings instances without eager I/O."""

import asyncio
from dataclasses import replace

import pytest

from sunday import (
    ClientSettings,
    ProviderCredentials,
    SecurityBinding,
    SecurityTransport,
    TokenConfiguration,
    TokenManager,
    TokenRequest,
    TokenSet,
    provider_credentials,
)

BINDING = SecurityBinding(
    scheme="identity",
    provider="resolved-provider",
    flow="authorizationCode",
    profile="development",
    transport=SecurityTransport(location="header", name="Authorization", prefix="Bearer"),
)


def test_persisted_sessions_rotation_isolation_and_logout() -> None:
    async def run() -> None:
        values: dict[str, TokenSet] = {}
        reads = saves = acquisitions = factories = 0
        refreshes: list[str] = []

        class Store:
            async def load(self, key: str) -> TokenSet | None:
                nonlocal reads
                reads += 1
                return values.get(key)

            async def save(self, key: str, tokens: TokenSet) -> None:
                nonlocal saves
                saves += 1
                values[key] = tokens

            async def remove(self, key: str) -> None:
                values.pop(key, None)

        store = Store()

        def settings(
            time: float, session: str = "session", profile: str = "development", *, direct: bool = False
        ) -> ClientSettings:
            class Provider:
                identity = "application"

                def configure(self, binding: SecurityBinding) -> TokenConfiguration:
                    return TokenConfiguration("client", session)

                async def acquire(self, request: TokenRequest) -> TokenSet:
                    nonlocal acquisitions
                    acquisitions += 1
                    return TokenSet("initial", expires_at=100, refresh_token="refresh-1")

                async def refresh(self, request: TokenRequest, refresh_token: str) -> TokenSet:
                    refreshes.append(refresh_token)
                    return TokenSet(
                        f"rotated-{len(refreshes)}",
                        expires_at=time + 100,
                        refresh_token=f"refresh-{len(refreshes) + 1}",
                    )

            provider = Provider()

            nonlocal factories
            factories += 1
            manager = TokenManager(provider, store=store, expiry_skew=5, now=lambda: time)

            selected = replace(BINDING, profile=profile)
            credentials = {"identity": ProviderCredentials(manager)}
            if direct:
                return ClientSettings("https://api.example", {"read": [selected]}, credentials)
            return ClientSettings.resolve(
                "https://api.example",
                {"read": [[selected]], "other": [[selected]], "public": [[]]},
                credentials,
            )

        async def token(settings: ClientSettings) -> TokenSet:
            assert settings.token_providers
            return (
                await provider_credentials(settings.token_providers["resolved-provider"], settings.bindings["read"][0])
            ).tokens

        first = settings(0)
        assert (reads, saves, acquisitions, factories) == (0, 0, 0, 1)
        assert first.token_providers
        lease = await provider_credentials(first.token_providers["resolved-provider"], first.bindings["read"][0])
        assert lease.tokens.access_token == "initial"
        assert (await token(settings(0, direct=True))).access_token == "initial"
        assert acquisitions == 1
        third = settings(96)
        tokens = await asyncio.gather(*(token(third) for _ in range(20)))
        assert all(value.access_token == "rotated-1" for value in tokens)
        assert refreshes == ["refresh-1"]
        assert (await token(settings(96))).refresh_token == "refresh-2"
        await token(settings(192))
        assert refreshes == ["refresh-1", "refresh-2"]
        assert saves == 3
        await token(settings(0, "other-session"))
        await token(settings(0, profile="production"))
        assert acquisitions == 3
        await store.remove(lease.key)
        await token(settings(0))
        assert acquisitions == 4

    asyncio.run(run())


def test_public_settings_and_missing_credentials() -> None:
    assert not ClientSettings("https://api.example").token_providers
    assert not ClientSettings.resolve("https://api.example", {"public": [[]]}, {}).token_providers
    with pytest.raises(ValueError, match="Missing credentials"):
        ClientSettings("https://api.example", {"read": [BINDING]})
