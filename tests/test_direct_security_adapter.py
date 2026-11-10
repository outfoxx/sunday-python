from typing import Any

import httpx
import pytest

from sunday import RequestCancellationError, RequestSpec, SecurityBinding, SecurityTransport
from sunday.httpx import HttpxTransport, RequestAdapters, copy_request, request_security_bindings

pytestmark = pytest.mark.anyio
BINDING = SecurityBinding(
    scheme="bearerAuth",
    provider="application",
    flow="external",
    audience="https://api.example",
    transport=SecurityTransport(location="header", name="Authorization", prefix="Bearer"),
)


async def test_direct_adapters_receive_bindings_without_token_providers() -> None:
    calls = []

    async def adapt(request: httpx.Request) -> httpx.Request:
        assert request_security_bindings(request) == (BINDING,)
        return httpx.Request(request.method, request.url, headers={"Authorization": "Bearer custom"})

    async def copy(request: httpx.Request) -> httpx.Request:
        assert request_security_bindings(request) == (BINDING,)
        return copy_request(request)

    async def response_adapter(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        assert request_security_bindings(request) == (BINDING,)
        assert request.headers["Authorization"] == "Bearer custom"
        return response

    async def send(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(401, headers={"WWW-Authenticate": 'Bearer error="invalid_token"'}, content=b"original")

    async with httpx.AsyncClient(base_url="https://api.example", transport=httpx.MockTransport(send)) as client:
        transport = HttpxTransport(
            client=client, request_adapter=RequestAdapters(adapt, copy), response_adapter=response_adapter
        )
        spec: RequestSpec[Any] = RequestSpec("GET", "/value", security=(BINDING,))
        exported = await transport.transport_request(spec)
        assert exported.headers["Authorization"] == "Bearer custom"
        response = await transport.execute_request(spec)
        assert response.status_code == 401
        assert response.content == b"original"
        assert len(calls) == 1


async def test_direct_adapter_cancellation_prevents_sending() -> None:
    stop = RequestCancellationError("application authorization unavailable")

    async def adapt(request: httpx.Request) -> httpx.Request:
        raise stop

    async def send(request: httpx.Request) -> httpx.Response:
        pytest.fail("Cancelled request was sent")

    async with httpx.AsyncClient(base_url="https://api.example", transport=httpx.MockTransport(send)) as client:
        transport = HttpxTransport(client=client, request_adapter=adapt)
        with pytest.raises(RequestCancellationError) as caught:
            await transport.execute_request(RequestSpec("GET", "/value", security=(BINDING,)))
        assert caught.value is stop
