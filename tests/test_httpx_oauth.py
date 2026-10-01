# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

import base64
from dataclasses import replace
from typing import Literal
from urllib.parse import parse_qs

import httpx
import pytest

from sunday import (
    AuthorizationRequiredError,
    SecurityBinding,
    SecurityEndpoints,
    SecurityTransport,
    TokenManager,
    TokenProviderError,
    TokenRequest,
)
from sunday.httpx import AuthorizationGrant, HttpxOAuthTokenProvider

BINDING = SecurityBinding(
    scheme="identity",
    provider="identity",
    profile="external",
    flow="clientCredentials",
    token_url="https://identity.example/token",
    scopes=("read", "write"),
    audience="api",
    resource="urn:api",
    transport=SecurityTransport(location="header", name="Authorization", prefix="Bearer"),
)


@pytest.mark.anyio
@pytest.mark.parametrize("authentication", ["client_secret_basic", "client_secret_post"])
async def test_client_credentials_and_rotating_refresh(
    authentication: Literal["client_secret_basic", "client_secret_post"],
) -> None:
    exchanges = []

    def handle(request: httpx.Request) -> httpx.Response:
        form = parse_qs(request.content.decode())
        exchanges.append(form)
        assert request.url == "https://identity.example/token"
        assert form["scope"] == ["read write"]
        assert form["audience"] == ["api"] and form["resource"] == ["urn:api"]
        assert "cookie" not in request.headers and "x-default" not in request.headers
        if authentication == "client_secret_basic":
            expected = base64.b64encode(b"client%3Aname:s+e%3Ac").decode()
            assert request.headers["authorization"] == "Basic " + expected
            assert "client_secret" not in form and "client_id" not in form
        else:
            assert "authorization" not in request.headers
            assert form["client_secret"] == ["s e:c"] and form["client_id"] == ["client:name"]
        if len(exchanges) == 1:
            assert form["grant_type"] == ["client_credentials"]
        else:
            assert form["grant_type"] == ["refresh_token"]
            assert form["refresh_token"] == [f"refresh-{len(exchanges) - 1}"]
        return httpx.Response(
            200,
            json={
                "access_token": f"token-{len(exchanges)}",
                "refresh_token": f"refresh-{len(exchanges)}",
                "token_type": "Bearer",
                "expires_in": 60,
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handle),
        headers={"X-Default": "private"},
        cookies={"session": "private"},
        auth=("other", "secret"),
    ) as client:
        provider = HttpxOAuthTokenProvider(
            client,
            identity="app",
            client_id="client:name",
            client_secret="s e:c",
            authentication=authentication,
            now=lambda: 0,
        )
        manager = TokenManager({"identity": provider}, now=lambda: 0)
        for _ in range(3):
            lease = await manager.credentials(BINDING)
            assert lease.tokens.expires_at == 60
            await manager.invalidate(lease)
    assert len(exchanges) == 3


@pytest.mark.anyio
async def test_pkce_consumes_authorization_once_and_invalid_refresh_requires_new_session() -> None:
    grants, exchanges = [], []

    async def authorize(request: TokenRequest) -> AuthorizationGrant:
        grants.append(request)
        return AuthorizationGrant("fresh-code", "https://app.example/callback", "v" * 43)

    def handle(request: httpx.Request) -> httpx.Response:
        form = parse_qs(request.content.decode())
        exchanges.append(form)
        assert form["client_id"] == ["public-client"]
        assert "authorization" not in request.headers
        if form["grant_type"] == ["authorization_code"]:
            assert form["code"] == ["fresh-code"] and form["code_verifier"] == ["v" * 43]
            assert form["redirect_uri"] == ["https://app.example/callback"]
            return httpx.Response(
                200,
                json={
                    "token_type": "Bearer",
                    "access_token": "first",
                    "refresh_token": "rotating",
                    "expires_in": 60,
                },
            )
        assert "code" not in form and "code_verifier" not in form
        return httpx.Response(400, json={"error": "invalid_grant", "error_description": "SECRET"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        provider = HttpxOAuthTokenProvider(
            client,
            identity="application",
            client_id="public-client",
            grant_identity="fresh-session",
            authorization=authorize,
            now=lambda: 0,
        )
        manager = TokenManager({"identity": provider}, now=lambda: 0)
        binding = replace(BINDING, flow="authorizationCode", authorization_url="https://identity.example/authorize")
        await manager.invalidate(await manager.credentials(binding))
        with pytest.raises(AuthorizationRequiredError):
            await manager.credentials(binding)
        # A different cache dimension cannot cause the same code to be exchanged again.
        with pytest.raises(AuthorizationRequiredError):
            await manager.credentials(replace(binding, scopes=("read",)))
    assert len(exchanges) == 2
    assert len(grants) == 2
    assert "fresh-code" not in repr(AuthorizationGrant("fresh-code", "https://app.example/callback", "v" * 43))


@pytest.mark.anyio
async def test_discovery_validates_issuer_and_endpoint_overrides_do_not_change_it() -> None:
    seen = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "issuer": "https://trusted.example",
                    "token_endpoint": "https://trusted.example/token",
                    "token_endpoint_auth_methods_supported": ["client_secret_basic"],
                },
            )
        return httpx.Response(200, json={"access_token": "token", "token_type": "bearer"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        provider = HttpxOAuthTokenProvider(
            client,
            identity="application",
            client_id="client",
            client_secret="secret",
            authentication="client_secret_basic",
            issuer="https://trusted.example",
            endpoints=SecurityEndpoints(token_url="https://deployment.example/token"),
        )
        manager = TokenManager({"identity": provider})
        binding = replace(BINDING, discovery_url="https://metadata.example/document", token_url=None)
        await manager.credentials(binding)
        await manager.credentials(replace(binding, scopes=("read",)))
        assert seen == [
            "https://metadata.example/document",
            "https://deployment.example/token",
            "https://metadata.example/document",
            "https://deployment.example/token",
        ]
        wrong = HttpxOAuthTokenProvider(
            client,
            identity="wrong",
            client_id="client",
            client_secret="secret",
            authentication="client_secret_basic",
            issuer="https://other.example",
        )
        with pytest.raises(TokenProviderError):
            await TokenManager({"identity": wrong}).credentials(binding)
        assert seen[-1] == "https://metadata.example/document"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(302, headers={"Location": "https://other.example"}),
        httpx.Response(200, json={"access_token": "secret", "token_type": "unsupported"}),
        httpx.Response(200, json={"access_token": "secret", "token_type": "bearer", "expires_in": -1}),
        httpx.Response(200, json={"access_token": "secret", "token_type": "bearer", "scope": "read"}),
        httpx.Response(400, json={"error": "invalid_client", "error_description": "SECRET"}),
    ],
)
async def test_exchange_errors_are_safe_and_never_follow_redirects(response: httpx.Response) -> None:
    calls = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return response

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle), follow_redirects=True) as client:
        provider = HttpxOAuthTokenProvider(
            client,
            identity="app",
            client_id="client",
            client_secret="secret",
            authentication="client_secret_post",
        )
        with pytest.raises(TokenProviderError) as error:
            await TokenManager({"identity": provider}).credentials(BINDING)
    assert len(calls) == 1
    assert "SECRET" not in str(error.value)


@pytest.mark.anyio
async def test_public_client_credentials_and_unsafe_endpoints_fail_before_exchange() -> None:
    calls = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        public = HttpxOAuthTokenProvider(client, identity="public", client_id="public-client")
        with pytest.raises(TokenProviderError):
            await TokenManager({"identity": public}).credentials(BINDING)
        provider = HttpxOAuthTokenProvider(
            client,
            identity="app",
            client_id="client",
            client_secret="secret",
            authentication="client_secret_post",
        )
        for endpoint in ("http://remote.example/token", "https://user:password@example.test/token", "file:///token"):
            with pytest.raises(TokenProviderError):
                await TokenManager({"identity": provider}).credentials(replace(BINDING, token_url=endpoint))
    assert calls == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    "status,body,reason",
    [
        (503, "SECRET outage", "temporary"),
        (429, "SECRET rate limit", "temporary"),
        (400, '{"error":"temporarily_unavailable"}', "temporary"),
        (400, '{"error":"invalid_grant"}', "invalid_grant"),
        (400, '{"error":"invalid_client"}', "unavailable"),
    ],
)
async def test_token_error_classification(status: int, body: str, reason: str) -> None:
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(status, text=body))) as client:
        provider = HttpxOAuthTokenProvider(
            client, identity="app", client_id="client", client_secret="secret", authentication="client_secret_post"
        )
        with pytest.raises(TokenProviderError) as error:
            await TokenManager({"identity": provider}).credentials(BINDING)
        assert error.value.reason == reason
        assert "SECRET" not in str(error.value)


@pytest.mark.anyio
async def test_consumed_authorization_history_is_bounded_without_reusing_codes() -> None:
    grants = 0

    async def authorize(_: TokenRequest) -> AuthorizationGrant:
        nonlocal grants
        grants += 1
        return AuthorizationGrant(f"code-{grants}", "https://app.example/callback", "v" * 43)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json={"access_token": "token", "token_type": "bearer"})
        )
    ) as client:
        provider = HttpxOAuthTokenProvider(
            client, identity="app", client_id="public", grant_identity="session", authorization=authorize
        )
        manager = TokenManager({"identity": provider})
        for index in range(1024):
            await manager.credentials(replace(BINDING, flow="authorizationCode", scopes=(str(index),)))
        with pytest.raises(AuthorizationRequiredError):
            await manager.credentials(replace(BINDING, flow="authorizationCode", scopes=("next",)))
    assert grants == 1024
