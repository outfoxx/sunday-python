"""Transport configuration preserves typed credentials and lazy authentication."""

import asyncio
import subprocess
import sys
from dataclasses import replace

import httpx
import pytest

from sunday import (
    ApiKeyCredentials,
    BearerCredentials,
    ClientSettings,
    OAuthCredentials,
    SecurityBinding,
    SecurityEndpoints,
    SecurityTransport,
)
from sunday.httpx import AuthorizationGrant, HttpxTransport

BINDING = SecurityBinding(
    scheme="identity",
    provider="identity",
    flow="static",
    transport=SecurityTransport(location="header", name="Authorization", prefix="Bearer"),
)


def test_static_settings_and_httpx_adapter() -> None:
    settings = ClientSettings(
        "https://api.example", {"list": [BINDING]}, {"identity": BearerCredentials("private-token")}
    )
    assert "private-token" not in repr(settings)
    assert "private-token" not in repr(BearerCredentials("private-token"))

    async def check() -> None:
        async with httpx.AsyncClient(base_url=settings.base_url) as client:
            transport = HttpxTransport.from_settings(settings, client)
            assert transport.client is client
            assert transport.token_manager is not None
            lease = await transport.token_manager.credentials(BINDING)
            assert lease.tokens.access_token == "private-token"

    asyncio.run(check())


def test_missing_or_incompatible_credentials() -> None:
    with pytest.raises(ValueError, match="Missing credentials"):
        ClientSettings("https://api.example", {"list": [BINDING]})
    with pytest.raises(ValueError, match="do not match"):
        ClientSettings("https://api.example", {"list": [BINDING]}, {"identity": ApiKeyCredentials("secret")})


def test_settings_snapshot_and_adapter_endpoint_validation() -> None:
    bindings = {"list": [BINDING]}
    credentials = {"identity": BearerCredentials("secret")}
    settings = ClientSettings("https://api.example", bindings, credentials)
    bindings.clear()
    credentials.clear()
    assert settings.bindings["list"] == (BINDING,)

    async def check() -> None:
        async with httpx.AsyncClient(base_url="https://other.example") as client:
            with pytest.raises(ValueError, match="base URL"):
                HttpxTransport.from_settings(settings, client)

    asyncio.run(check())


def test_oauth_secret_representations_and_grant_compatibility() -> None:
    credentials = OAuthCredentials(
        identity="application",
        client_id="client",
        flow="clientCredentials",
        provider_factory=lambda _: (_ for _ in ()).throw(AssertionError("Unexpected provider construction")),
        client_secret="private-secret",
        authentication="client_secret_basic",
    )
    assert "private-secret" not in repr(credentials)
    assert "private-code" not in repr(AuthorizationGrant("private-code", "https://app.example", "private-verifier"))


def test_core_credentials_do_not_load_httpx() -> None:
    subprocess.run([sys.executable, "-c", "import sunday; import sys; assert 'httpx' not in sys.modules"], check=True)


def test_complete_conjunction_public_override_and_selection() -> None:
    key = replace(BINDING, scheme="key", provider="key", transport=SecurityTransport(location="query", name="key"))
    credentials = {"identity": BearerCredentials("one"), "key": ApiKeyCredentials("two")}
    alternatives = {"list": [[BINDING, key], [BINDING]], "public": [[]]}
    with pytest.raises(ValueError, match="one complete"):
        ClientSettings.resolve("https://api.example", alternatives, credentials)
    settings = ClientSettings.resolve("https://api.example", alternatives, credentials, {"list": ["identity", "key"]})
    assert [binding.scheme for binding in settings.bindings["list"]] == ["identity", "key"]
    assert settings.bindings["public"] == ()
    with pytest.raises(ValueError):
        ClientSettings.resolve("https://api.example", {"list": [[BINDING, key]]}, {"identity": credentials["identity"]})


def test_relative_endpoints_and_security_templates() -> None:
    endpoint = ClientSettings.server_url("../{version}", {"version": "v2"}, "https://api.example/spec/openapi.yaml")
    assert endpoint == "https://api.example/v2"
    binding = replace(BINDING, token_url="oauth/token")
    settings = ClientSettings(endpoint, {"list": [binding]}, {"identity": BearerCredentials("secret")})
    assert settings.bindings["list"][0].token_url == "https://api.example/oauth/token"
    with pytest.raises(ValueError):
        ClientSettings.server_url("/v2", {})
    with pytest.raises(ValueError):
        ClientSettings(
            endpoint,
            {"list": [replace(binding, token_url="/{version}/token")]},
            {"identity": BearerCredentials("secret")},
        )


def test_token_manager_isolation() -> None:
    async def run() -> None:
        for token in ["one", "two"]:
            settings = ClientSettings(
                "https://api.example", {"list": [BINDING]}, {"identity": BearerCredentials(token)}
            )

            manager = settings.token_manager
            assert manager is not None
            assert (await manager.credentials(BINDING)).tokens.access_token == token

    asyncio.run(run())


def test_oauth_manager_is_prepared_once_before_transport_construction() -> None:
    calls = []

    class Provider:
        identity = "application"

        def configure(self, binding):
            raise AssertionError("Unexpected request configuration")

        async def acquire(self, request):
            raise AssertionError("Unexpected token acquisition")

    provider = Provider()

    def factory(credentials):
        calls.append(credentials)
        assert credentials.endpoints.token_url == "https://api.example/oauth/token"
        return provider

    oauth = replace(BINDING, flow="clientCredentials")
    credentials = OAuthCredentials(
        identity="application",
        client_id="client",
        flow="clientCredentials",
        client_secret="secret",
        authentication="client_secret_basic",
        provider_factory=factory,
        endpoints=SecurityEndpoints(token_url="oauth/token"),
    )
    settings = ClientSettings("https://api.example/v1", {"list": [oauth], "read": [oauth]}, {"identity": credentials})
    assert settings.token_manager is not None
    assert len(calls) == 1
    assert credentials.endpoints.token_url == "oauth/token"
