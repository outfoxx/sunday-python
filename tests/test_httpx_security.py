# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from dataclasses import replace
from typing import Any

import httpx
import pytest

from sunday import (
    Operation,
    OperationSpec,
    RequestSpec,
    ResponseSpec,
    SecurityBinding,
    SecurityTransport,
    TokenConfiguration,
    TokenManager,
    TokenProviderError,
    TokenRequest,
    TokenSet,
    TransportEvent,
    UnexpectedResponse,
)
from sunday.httpx import HttpxTransport

BINDING = SecurityBinding(
    scheme="token",
    provider="identity",
    profile="external",
    flow="clientCredentials",
    token_url="https://identity.example/token",
    scopes=("read",),
    transport=SecurityTransport(location="header", name="Authorization", prefix="Bearer"),
)


class Provider:
    identity = "test-provider"

    def __init__(self) -> None:
        self.acquisitions = 0
        self.refreshes: list[str] = []

    def configure(self, _binding: SecurityBinding) -> TokenConfiguration:
        return TokenConfiguration("client")

    async def acquire(self, _request: TokenRequest) -> TokenSet:
        self.acquisitions += 1
        return TokenSet(f"access-{self.acquisitions}", refresh_token="refresh-first")

    async def refresh(self, _request: TokenRequest, refresh_token: str) -> TokenSet:
        self.refreshes.append(refresh_token)
        return TokenSet(f"renewed-{len(self.refreshes)}", refresh_token="refresh-next")


class Observer:
    def __init__(self) -> None:
        self.events: list[TransportEvent] = []

    def observe(self, event: TransportEvent) -> None:
        self.events.append(event)


async def send(transport: HttpxTransport, spec: RequestSpec[Any]) -> httpx.Response:
    return await transport.transport_response(await transport.transport_request(spec))


@pytest.mark.anyio
async def test_complete_credentials_and_one_safe_recovery() -> None:
    provider = Provider()
    authorization: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        authorization.append(request.headers["authorization"])
        if len(authorization) == 1:
            return httpx.Response(401, headers={"WWW-Authenticate": 'Bearer realm="api", error="invalid_token"'})
        return httpx.Response(204)

    async with httpx.AsyncClient(base_url="https://api.example", transport=httpx.MockTransport(handle)) as client:
        transport = HttpxTransport(client, token_manager=TokenManager({"identity": provider}))
        response = await send(transport, RequestSpec("GET", "/value", security=(BINDING,)))
    assert response.status_code == 204
    assert response.request.headers["authorization"] == "[redacted]"
    assert authorization == ["Bearer access-1", "Bearer renewed-1"]
    assert provider.acquisitions == 1
    assert provider.refreshes == ["refresh-first"]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "method,status,challenge,expected",
    [
        ("POST", 401, 'Bearer error="invalid_token"', 1),
        ("GET", 403, 'Bearer error="invalid_token"', 1),
        ("GET", 401, 'Bearer realm="api", Basic, error="invalid_token"', 1),
        ("GET", 401, 'Bearer error="INVALID_TOKEN"', 1),
        ("GET", 401, 'Basic realm="x", error="invalid_token"', 1),
        ("GET", 401, 'Bearer error="insufficient_scope"', 1),
        ("GET", 401, 'Bearer realm="x,y", error="invalid_token"', 2),
        ("GET", 401, 'Basic realm="x", Bearer error="invalid_token"', 2),
        ("GET", 401, 'Bearer realm="unterminated, error=invalid_token', 1),
    ],
)
async def test_recovery_is_bounded_safe_and_never_uses_403(
    method: str,
    status: int,
    challenge: str,
    expected: int,
) -> None:
    provider, requests = Provider(), []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status, headers={"WWW-Authenticate": challenge})

    async with httpx.AsyncClient(base_url="https://api.example", transport=httpx.MockTransport(handle)) as client:
        transport = HttpxTransport(client, token_manager=TokenManager({"identity": provider}))
        operation: Operation[None, httpx.Request, httpx.Response] = Operation(
            transport,
            OperationSpec(RequestSpec(method, "/value", security=(BINDING,)), (ResponseSpec(204),)),
        )
        with pytest.raises(UnexpectedResponse):
            await operation.execute()
    assert len(requests) == expected
    assert len(provider.refreshes) == expected - 1


@pytest.mark.anyio
async def test_native_request_rechecks_expiry_and_public_operations_do_not_acquire() -> None:
    provider, seen = Provider(), []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("authorization"))
        return httpx.Response(204)

    manager = TokenManager({"identity": provider})
    async with httpx.AsyncClient(base_url="https://api.example", transport=httpx.MockTransport(handle)) as client:
        transport = HttpxTransport(client, token_manager=manager)
        request = await transport.transport_request(RequestSpec("GET", "/value", security=(BINDING,)))
        await transport.transport_response(request)
        await manager.invalidate(await manager.credentials(BINDING))
        await transport.transport_response(request)
        await send(transport, RequestSpec("GET", "/public", security=()))
    assert seen == ["Bearer access-1", "Bearer renewed-1", None]


@pytest.mark.anyio
async def test_all_and_credentials_are_attached_without_losing_body_and_diagnostics_redact() -> None:
    provider, observer = Provider(), Observer()
    bindings = (
        BINDING,
        replace(
            BINDING,
            scheme="key",
            flow="static",
            transport=SecurityTransport(
                location="query",
                name="api_key",
            ),
        ),
        replace(
            BINDING,
            scheme="cookie",
            flow="external",
            transport=SecurityTransport(
                location="cookie",
                name="session",
            ),
        ),
    )

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer access-1"
        assert request.url.params["api_key"].startswith("access-")
        assert "session=access-" in request.headers["cookie"]
        assert request.content == b'{"message":"unchanged"}'
        return httpx.Response(204)

    async with httpx.AsyncClient(base_url="https://api.example", transport=httpx.MockTransport(handle)) as client:
        transport = HttpxTransport(client, token_manager=TokenManager({"identity": provider}), observers=(observer,))
        response = await send(
            transport,
            RequestSpec(
                "POST",
                "/value",
                body={"message": "unchanged"},
                security=bindings,
            ),
        )
    assert "access-" not in str(response.request.url)
    assert "access-" not in repr(observer.events)
    assert "access-" not in str(response.request.headers)


@pytest.mark.anyio
async def test_conflicts_and_missing_provider_fail_before_network() -> None:
    sends = 0

    def handle(_request: httpx.Request) -> httpx.Response:
        nonlocal sends
        sends += 1
        return httpx.Response(204)

    async with httpx.AsyncClient(base_url="https://api.example", transport=httpx.MockTransport(handle)) as client:
        missing = HttpxTransport(client)
        with pytest.raises(TokenProviderError):
            await send(missing, RequestSpec("GET", "/value", security=(BINDING,)))
        transport = HttpxTransport(client, token_manager=TokenManager({"identity": Provider()}))
        with pytest.raises(TokenProviderError):
            await send(
                transport,
                RequestSpec(
                    "GET",
                    "/value",
                    security=(BINDING,),
                    headers=(("Authorization", "existing"),),
                ),
            )
        with pytest.raises(TokenProviderError):
            await send(transport, RequestSpec("GET", "/value", security=(BINDING, BINDING)))
    assert sends == 0


@pytest.mark.anyio
async def test_transport_errors_and_observers_do_not_leak_query_credentials() -> None:
    observer = Observer()
    binding = replace(BINDING, transport=SecurityTransport(location="query", name="key"))

    def handle(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(f"failed: {request.url}", request=request)

    async with httpx.AsyncClient(base_url="https://api.example", transport=httpx.MockTransport(handle)) as client:
        transport = HttpxTransport(client, token_manager=TokenManager({"identity": Provider()}), observers=(observer,))
        with pytest.raises(httpx.ConnectError) as error:
            await send(transport, RequestSpec("GET", "/value", security=(binding,)))
    assert "access-" not in str(error.value)
    assert "access-" not in str(error.value.request.url)
    assert "access-" not in repr(observer.events)


@pytest.mark.anyio
async def test_managed_credentials_cannot_follow_redirects_or_be_overridden_by_native_auth() -> None:
    sends = []
    binding = replace(BINDING, transport=SecurityTransport(location="header", name="X-API-Key"))

    def handle(request: httpx.Request) -> httpx.Response:
        sends.append(str(request.url))
        assert request.headers["x-api-key"] == "access-1"
        assert "authorization" not in request.headers
        return httpx.Response(302, headers={"Location": "https://unrelated.example/"})

    async with httpx.AsyncClient(
        base_url="https://api.example",
        transport=httpx.MockTransport(handle),
        follow_redirects=True,
        auth=("unrelated", "credentials"),
    ) as client:
        transport = HttpxTransport(client, token_manager=TokenManager({"identity": Provider()}))
        response = await send(transport, RequestSpec("GET", "/value", security=(binding,)))
    assert response.status_code == 302
    assert sends == ["https://api.example/value"]


@pytest.mark.anyio
async def test_event_reconnects_share_one_authentication_recovery() -> None:
    from sunday import EventStreamOptions

    provider = Provider()
    authorization: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        authorization.append(request.headers["authorization"])
        if len(authorization) == 2:
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content="data: first\n\n")
        return httpx.Response(401, headers={"WWW-Authenticate": "Bearer error=invalid_token"})

    async with httpx.AsyncClient(base_url="https://api.example", transport=httpx.MockTransport(handle)) as client:
        transport = HttpxTransport(client, token_manager=TokenManager({"identity": provider}))
        stream = transport.event_stream(
            RequestSpec("GET", "/events", security=(BINDING,)),
            lambda event: event.data,
            options=EventStreamOptions(retry=0),
        )
        values = []
        with pytest.raises(UnexpectedResponse):
            async for value in stream:
                values.append(value)
        await stream.aclose()
    assert values == ["first"]
    assert authorization == ["Bearer access-1", "Bearer renewed-1", "Bearer renewed-1"]
    assert provider.refreshes == ["refresh-first"]
