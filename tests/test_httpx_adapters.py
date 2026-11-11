# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from dataclasses import replace
from typing import Any

import anyio
import httpx
import pytest

from sunday import (
    EventStreamOptions,
    MediaType,
    MediaTypeEncoders,
    NullableOperation,
    NullifySpec,
    OperationSpec,
    Problem,
    ProblemPayload,
    RequestCancellationError,
    RequestSpec,
    SecurityBinding,
    SecurityTransport,
    StreamingBody,
    TokenConfiguration,
    TokenRequest,
    TokenSet,
    TransportEvent,
    TransportEventKind,
)
from sunday.httpx import (
    HttpxEventStream,
    HttpxTransport,
    RequestAdapter,
    RequestAdapters,
    ResponseAdapter,
    ResponseAdapters,
    authentication_adapters,
    copy_request,
)

pytestmark = pytest.mark.anyio


class Stop(httpx.ReadError, RequestCancellationError):
    pass


class Body(httpx.AsyncByteStream):
    def __init__(
        self,
        content: bytes = b"payload",
        *,
        error: BaseException | None = None,
        close_error: BaseException | None = None,
    ) -> None:
        self.content = content
        self.error = error
        self.close_error = close_error
        self.reads = 0
        self.closes = 0

    async def __aiter__(self) -> AsyncIterator[bytes]:
        self.reads += 1
        if self.error is not None:
            raise self.error
        yield self.content

    async def aclose(self) -> None:
        await anyio.sleep(0)
        self.closes += 1
        if self.close_error is not None:
            raise self.close_error


async def perform(transport: HttpxTransport, streaming: bool, spec: RequestSpec[Any] | None = None) -> httpx.Response:
    spec = spec or RequestSpec("GET", "/value")
    if not streaming:
        return await transport.execute_request(spec)
    request = await transport._prepare_request(spec)
    response = await transport._send_event_stream(
        request,
    )
    try:
        await response.aread()
        return response
    finally:
        await response.aclose()


@pytest.mark.parametrize("streaming", [False, True])
async def test_independent_flat_chains_and_early_response_adaptation(streaming: bool) -> None:
    calls: list[str] = []
    body = Body(b"x" * (4 * 1024 * 1024))

    class First(RequestAdapter):
        async def adapt(self, request: httpx.Request) -> httpx.Request:
            calls.append("request-first")
            request.headers["X-Adapted"] = "yes"
            return request

    async def second(request: httpx.Request) -> httpx.Request:
        calls.append("request-second")
        return copy_request(request)

    class FirstResponse(ResponseAdapter):
        async def adapt(self, response: httpx.Response, request: httpx.Request) -> httpx.Response:
            calls.append("response-first")
            assert request.headers["X-Adapted"] == "yes"
            assert body.reads == 0
            response.status_code = 200
            return response

    async def second_response(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        calls.append("response-second")
        assert response.status_code == 200
        return response

    async with httpx.AsyncClient(
        base_url="https://api.example", transport=httpx.MockTransport(lambda _: httpx.Response(400, stream=body))
    ) as client:
        transport = HttpxTransport(
            client,
            request_adapter=First().chain_to(RequestAdapters(second)),
            response_adapter=FirstResponse().chain_to(ResponseAdapters(second_response)),
        )
        result = await perform(transport, streaming)
        assert result.content == body.content
    assert calls == ["request-first", "request-second", "response-first", "response-second"]
    assert body.reads == body.closes == 1


async def test_export_adapts_once_and_has_no_hidden_execution_state() -> None:
    calls: list[str] = []

    async def request_adapter(request: httpx.Request) -> httpx.Request:
        calls.append("request")
        return request

    async def response_adapter(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        calls.append("response")
        return response

    async with httpx.AsyncClient(
        base_url="https://api.example", transport=httpx.MockTransport(lambda _: httpx.Response(204))
    ) as client:
        transport = HttpxTransport(client, request_adapter=request_adapter, response_adapter=response_adapter)
        exported = await transport.transport_request(RequestSpec("GET", "/value"))
        await client.send(exported)
        assert calls == ["request"]
        assert set(exported.extensions) == {"timeout"}
        await transport.execute_request(RequestSpec("GET", "/value"))
        assert calls == ["request", "request", "response"]


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("stage", ["request", "response", "read"])
async def test_stop_identity_and_cleanup(streaming: bool, stage: str) -> None:
    stop = Stop("stop")
    cleanup = RuntimeError("cleanup failed")
    body = Body(error=stop if stage == "read" else None, close_error=cleanup)
    sends = 0

    async def request_adapter(request: httpx.Request) -> httpx.Request:
        if stage == "request":
            raise stop
        return request

    async def response_adapter(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        if stage == "response":
            raise stop
        return response

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal sends
        sends += 1
        return httpx.Response(200, stream=body)

    async with httpx.AsyncClient(base_url="https://api.example", transport=httpx.MockTransport(handle)) as client:
        transport = HttpxTransport(client, request_adapter=request_adapter, response_adapter=response_adapter)
        if streaming and stage == "read":
            # The event-stream consumer owns read cleanup and must retain the stop over close failures.
            async def sse_headers(response: httpx.Response, request: httpx.Request) -> httpx.Response:
                response.headers["Content-Type"] = "text/event-stream"
                return response

            transport = HttpxTransport(client, response_adapter=sse_headers)
            events = transport.event_stream(RequestSpec("GET", "/value"), lambda event: event)
            with pytest.raises(Stop) as failure:
                await anext(aiter(events))
        else:
            with pytest.raises(Stop) as failure:
                await perform(transport, streaming)
        assert failure.value is stop
    assert sends == (0 if stage == "request" else 1)
    assert body.closes == sends


@pytest.mark.parametrize("streaming", [False, True])
async def test_response_adapter_consumes_and_replaces_body(streaming: bool) -> None:
    body = Body()

    async def replace(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        assert await response.aread() == b"payload"
        return httpx.Response(200, content=b"replacement", request=request, extensions=response.extensions)

    async with httpx.AsyncClient(
        base_url="https://api.example", transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=body))
    ) as client:
        result = await perform(HttpxTransport(client, response_adapter=replace), streaming)
        assert result.content == b"replacement"
    assert body.closes == 1


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("read_in_adapter", [False, True])
async def test_native_cancellation_interrupts_reads_and_shields_cleanup(streaming: bool, read_in_adapter: bool) -> None:
    reading = anyio.Event()

    class WaitingBody(Body):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            reading.set()
            await anyio.sleep_forever()
            yield b"unreachable"

    body = WaitingBody()

    async def adapter(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        if read_in_adapter:
            await response.aread()
        return response

    async with httpx.AsyncClient(
        base_url="https://api.example",
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=body)
        ),
    ) as client:
        transport = HttpxTransport(client, response_adapter=adapter)

        async def execute() -> None:
            if streaming:
                events = transport.event_stream(RequestSpec("GET", "/value"), lambda event: event)
                await anext(aiter(events))
            else:
                await perform(transport, False)

        task = asyncio.create_task(execute())
        with anyio.fail_after(1):
            await reading.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    assert body.closes == 1


@pytest.mark.parametrize("streaming", [False, True])
async def test_adapted_redirects_reprepare_without_accumulating_headers(streaming: bool) -> None:
    seen: list[httpx.Request] = []

    async def request_adapter(request: httpx.Request) -> httpx.Request:
        request.headers["X-Attempt"] = request.headers.get("X-Attempt", "") + "one"
        return request

    async def response_adapter(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/value":
            response.status_code = 303
            response.headers["Location"] = "/redirected"
        return response

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(
        base_url="https://api.example", follow_redirects=True, transport=httpx.MockTransport(handle)
    ) as client:
        transport = HttpxTransport(client, request_adapter=request_adapter, response_adapter=response_adapter)
        await perform(transport, streaming, RequestSpec("POST", "/value", body=b"original"))
    assert [r.method for r in seen] == ["POST", "GET"]
    assert seen[1].content == b""
    assert "content-type" not in seen[1].headers
    assert [r.headers["X-Attempt"] for r in seen] == ["one", "one"]
    assert seen[1].extensions["timeout"] == seen[0].extensions["timeout"]


@pytest.mark.parametrize("streaming", [False, True])
async def test_replay_body_factory_failure_retains_identity(streaming: bool) -> None:
    stop = Stop("factory failed")
    opens = 0

    def content() -> bytes:
        nonlocal opens
        opens += 1
        if opens == 2:
            raise stop
        return b"payload"

    async with httpx.AsyncClient(
        base_url="https://api.example",
        follow_redirects=True,
        transport=httpx.MockTransport(lambda _: httpx.Response(307, headers={"Location": "/next"})),
    ) as client:
        with pytest.raises(Stop) as failure:
            await perform(HttpxTransport(client), streaming, RequestSpec("POST", "/value", body=StreamingBody(content)))
        assert failure.value is stop
    assert opens == 2


BINDING = SecurityBinding(
    scheme="token",
    provider="identity",
    profile="external",
    flow="external",
    transport=SecurityTransport(location="header", name="Authorization", prefix="Bearer"),
)


class Provider:
    identity = "external"

    def __init__(self) -> None:
        self.acquisitions = 0
        self.rejections: list[str] = []

    def configure(self, binding: SecurityBinding) -> TokenConfiguration:
        return TokenConfiguration("client")

    async def acquire(self, request: TokenRequest) -> TokenSet:
        self.acquisitions += 1
        return TokenSet(f"token-{self.acquisitions}")

    async def reject(self, request: TokenRequest, rejected_token: str) -> bool:
        self.rejections.append(rejected_token)
        return True


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("method,expected", [("POST", 1), ("GET", 1)])
@pytest.mark.parametrize("rewrite", [False, True])
async def test_feedback_does_not_retry_and_matches_wire_token(
    streaming: bool,
    method: str,
    expected: int,
    rewrite: bool,
) -> None:
    provider = Provider()
    observations: list[tuple[str, tuple[str, ...]]] = []

    async def observe(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        observations.append((request.headers["authorization"], tuple(provider.rejections)))
        return response

    async def hook(request: httpx.Request) -> None:
        if rewrite:
            request.headers["Authorization"] = "Bearer unrelated"

    async with httpx.AsyncClient(
        base_url="https://api.example",
        event_hooks={"request": [hook]},
        transport=httpx.MockTransport(
            lambda _: httpx.Response(401, headers={"WWW-Authenticate": "Bearer error=invalid_token"})
        ),
    ) as client:
        transport = HttpxTransport(client, **authentication_adapters({"identity": provider}, None, observe))
        response = await perform(transport, streaming, RequestSpec(method, "/value", security=(BINDING,)))
        assert response.status_code == 401
    assert provider.acquisitions == expected
    assert provider.rejections == ([] if rewrite else [f"token-{i + 1}" for i in range(expected)])

    assert len(observations) == expected
    assert observations[-1][1] == tuple(provider.rejections)
    assert observations[-1][0] == ("Bearer unrelated" if rewrite else f"Bearer token-{expected}")


@pytest.mark.parametrize("stage", ["request", "response", "decoder"])
async def test_sse_stop_never_reconnects(stage: str) -> None:
    stop = Stop("stop SSE")
    sends = 0

    async def request_adapter(request: httpx.Request) -> httpx.Request:
        if stage == "request":
            raise stop
        return request

    async def response_adapter(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        if stage == "response":
            raise stop
        return response

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal sends
        sends += 1
        return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content="data: value\n\n")

    def decode(event: Any) -> None:
        raise stop

    async with httpx.AsyncClient(base_url="https://api.example", transport=httpx.MockTransport(handle)) as client:
        transport = HttpxTransport(client, request_adapter=request_adapter, response_adapter=response_adapter)
        events: HttpxEventStream[None] = transport.event_stream(
            RequestSpec("GET", "/value"), decode, options=EventStreamOptions(retry=0)
        )
        with anyio.fail_after(1), pytest.raises(Stop) as failure:
            await anext(aiter(events))
        assert failure.value is stop
    assert sends == (0 if stage == "request" else 1)


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("stage", ["acquire", "reject", "upload"])
async def test_secured_execution_never_sanitizes_stop_errors(streaming: bool, stage: str) -> None:
    stop = Stop("application-owned stop")

    class FailingObserver:
        def observe(self, event: TransportEvent) -> None:
            if event.kind is TransportEventKind.FAILURE:
                raise ValueError("observer failure")

    class StoppingProvider(Provider):
        async def acquire(self, request: TokenRequest) -> TokenSet:
            if stage == "acquire":
                raise stop
            return await super().acquire(request)

        async def reject(self, request: TokenRequest, rejected_token: str) -> bool:
            raise stop

    def content() -> bytes:
        raise stop

    async with httpx.AsyncClient(
        base_url="https://api.example",
        transport=httpx.MockTransport(
            lambda _: httpx.Response(401, headers={"WWW-Authenticate": "Bearer error=invalid_token"})
        ),
    ) as client:
        transport = HttpxTransport(
            client,
            observers=(FailingObserver(),),
            **authentication_adapters({"identity": StoppingProvider()}, None, None),
        )
        spec = RequestSpec(
            "POST", "/value", security=(BINDING,), body=StreamingBody(content) if stage == "upload" else None
        )
        with pytest.raises(Stop) as failure:
            await perform(transport, streaming, spec)
        assert failure.value is stop


@pytest.mark.parametrize("streaming", [False, True])
async def test_failed_response_chain_closes_the_replacement(streaming: bool) -> None:
    original, replacement = Body(), Body()
    stop = Stop("later adapter")

    async def replace(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        await response.aclose()
        return httpx.Response(200, stream=replacement, request=request)

    async def fail(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        raise stop

    async with httpx.AsyncClient(
        base_url="https://api.example", transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=original))
    ) as client:
        transport = HttpxTransport(client, response_adapter=ResponseAdapters(replace, fail))
        with pytest.raises(Stop) as failure:
            await perform(transport, streaming)
        assert failure.value is stop
    assert original.closes == replacement.closes == 1
    assert original.reads == replacement.reads == 0


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("read_in_adapter", [False, True])
async def test_cancellation_releases_a_stalled_native_tcp_response(streaming: bool, read_in_adapter: bool) -> None:
    from anyio.abc import SocketAttribute, SocketStream

    received = anyio.Event()
    disconnected = anyio.Event()

    async def serve(socket: SocketStream) -> None:
        async with socket:
            await socket.receive()
            await socket.send(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 100\r\n\r\n")
            try:
                await socket.receive()
            except anyio.EndOfStream:
                disconnected.set()

    async def adapt(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        received.set()
        if read_in_adapter:
            await response.aread()
        return response

    listener = await anyio.create_tcp_listener(local_host="127.0.0.1")
    port = listener.extra(SocketAttribute.local_port)
    async with listener, anyio.create_task_group() as group:
        group.start_soon(listener.serve, serve)
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=None, trust_env=False) as client:
            transport = HttpxTransport(client, response_adapter=adapt)

            async def execute() -> None:
                if streaming:
                    events = transport.event_stream(RequestSpec("GET", "/value"), lambda event: event)
                    await anext(aiter(events))
                else:
                    await transport.execute_request(RequestSpec("GET", "/value"))

            task = asyncio.create_task(execute())
            try:
                with anyio.fail_after(2):
                    await received.wait()
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                    # Prove cancellation released the socket before closing the borrowed client.
                    await disconnected.wait()
            finally:
                task.cancel()
                group.cancel_scope.cancel()


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("status", [307, 308])
async def test_streamed_upload_redirect_preserves_native_host(streaming: bool, status: int) -> None:
    from anyio.abc import SocketAttribute, SocketStream

    received: list[bytes] = []
    opens = 0

    def content() -> bytes:
        nonlocal opens
        opens += 1
        return b"payload"

    async def serve(socket: SocketStream) -> None:
        async with socket:
            data = bytearray()
            while b"\r\n\r\n" not in data:
                data.extend(await socket.receive())
            headers, body = bytes(data).split(b"\r\n\r\n", 1)
            while len(body) < len(b"payload"):
                body += await socket.receive()
            assert body == b"payload"
            received.append(headers)
            response = (
                f"HTTP/1.1 {status} Redirect\r\nLocation: /next\r\n" if len(received) == 1 else "HTTP/1.1 200 OK\r\n"
            )
            await socket.send((response + "Content-Length: 0\r\nConnection: close\r\n\r\n").encode())

    listener = await anyio.create_tcp_listener(local_host="127.0.0.1")
    port = listener.extra(SocketAttribute.local_port)
    async with listener, anyio.create_task_group() as group:
        group.start_soon(listener.serve, serve)
        try:
            async with httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{port}", follow_redirects=True, trust_env=False
            ) as client:
                spec = RequestSpec("POST", "/value", body=StreamingBody(content), headers=(("Content-Length", "7"),))
                with anyio.fail_after(2):
                    response = await perform(HttpxTransport(client), streaming, spec)
                assert response.status_code == 200
        finally:
            group.cancel_scope.cancel()
    assert opens == 2
    assert len(received) == 2
    assert received[1].startswith(b"POST /next HTTP/1.1\r\n")
    assert f"host: 127.0.0.1:{port}\r\n".encode() in received[1].lower() + b"\r\n"


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize(
    ("location", "cookie", "expected"),
    [
        ("/next", "session=accepted; Path=/", "session=accepted"),
        ("/elsewhere", "session=accepted; Path=/next", None),
        ("https://other.example/next", "session=accepted; Path=/", None),
        ("http://api.example/next", "session=accepted; Path=/; Secure", None),
    ],
)
async def test_redirect_cookies_match_native_httpx(
    streaming: bool, location: str, cookie: str, expected: str | None
) -> None:
    async def execute(managed: bool) -> list[str | None]:
        seen: list[str | None] = []
        adapted: list[str | None] = []

        def handle(request: httpx.Request) -> httpx.Response:
            seen.append(request.headers.get("cookie"))
            if request.url.path == "/value":
                return httpx.Response(302, headers={"Location": location, "Set-Cookie": cookie})
            return httpx.Response(204)

        async def adapt(request: httpx.Request) -> httpx.Request:
            adapted.append(request.headers.get("cookie"))
            return request

        async with httpx.AsyncClient(
            base_url="https://api.example", follow_redirects=True, transport=httpx.MockTransport(handle)
        ) as client:
            response = (
                await perform(HttpxTransport(client, request_adapter=adapt), streaming)
                if managed
                else await client.get("/value")
            )
            assert response.status_code == 204
        if managed:
            assert adapted == seen
        return seen

    assert await execute(True) == await execute(False) == [None, expected]


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("body", [b"payload", bytearray(b"payload"), memoryview(b"payload")])
async def test_binary_encoder_selection_preserves_stop_identity(streaming: bool, body: object) -> None:
    stop = Stop("stop selecting encoder")
    sends = 0

    class Encoder:
        @property
        def media_types(self) -> tuple[MediaType, ...]:
            raise stop

        def encode(self, value: object) -> bytes:
            raise AssertionError("Encoding must not follow cancellation")

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal sends
        sends += 1
        return httpx.Response(204)

    async with httpx.AsyncClient(base_url="https://api.example", transport=httpx.MockTransport(handle)) as client:
        transport = HttpxTransport(client, encoders=MediaTypeEncoders([Encoder()]))
        with pytest.raises(Stop) as failure:
            await perform(transport, streaming, RequestSpec("POST", "/value", body=body))
        assert failure.value is stop
    assert sends == 0


@pytest.mark.parametrize("execution", ["buffered", "streaming", "nullable-result", "nullable-response"])
@pytest.mark.parametrize("stage", ["request", "response"])
async def test_problem_stop_is_never_nullified_or_reconnected(execution: str, stage: str) -> None:
    class StopProblem(Problem, RequestCancellationError):
        pass

    stop = StopProblem(ProblemPayload(status=404, title="Stop the request"))
    body = Body(b"data: value\n\n")
    sends = 0

    async def adapt_request(request: httpx.Request) -> httpx.Request:
        if stage == "request":
            raise stop
        return request

    async def adapt_response(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        if stage == "response":
            raise stop
        return response

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal sends
        sends += 1
        return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=body)

    async with httpx.AsyncClient(base_url="https://api.example", transport=httpx.MockTransport(handle)) as client:
        transport = HttpxTransport(client, request_adapter=adapt_request, response_adapter=adapt_response)
        spec: RequestSpec[None] = RequestSpec("GET", "/value")
        operation = NullableOperation[None, httpx.Request, httpx.Response](
            transport, OperationSpec(spec, ()), NullifySpec(statuses=(404,), problem_types=(StopProblem,))
        )
        with anyio.fail_after(1), pytest.raises(StopProblem) as failure:
            if execution == "buffered":
                await transport.execute_request(spec)
            elif execution == "streaming":
                async with transport.event_stream(spec, lambda event: event) as events:
                    await anext(aiter(events))
            elif execution == "nullable-result":
                await operation.execute_or_none()
            else:
                await operation.response_or_none()
        assert failure.value is stop
    assert sends == (0 if stage == "request" else 1)
    assert body.closes == sends


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("streamed_body", [False, True])
@pytest.mark.parametrize(
    ("url", "headers", "host"),
    [
        ("https://edge.example:8443/target", None, "edge.example:8443"),
        ("https://edge.example:8443/target", {"X-Client": "value"}, "edge.example:8443"),
        ("https://edge.example:8443/target", {"Host": "override.example"}, "override.example"),
        ("https://api.example/target", None, "virtual.example"),
    ],
)
async def test_request_copy_updates_host_unless_explicitly_overridden(
    streaming: bool, streamed_body: bool, url: str, headers: dict[str, str] | None, host: str
) -> None:
    seen: list[httpx.Request] = []

    async def adapt(request: httpx.Request) -> httpx.Request:
        copied = copy_request(request, url=url, headers=headers)
        assert copied.extensions == request.extensions
        return copied

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(204)

    async with httpx.AsyncClient(
        base_url="https://api.example", headers={"Host": "virtual.example"}, transport=httpx.MockTransport(handle)
    ) as client:
        body = StreamingBody.bytes(b"payload") if streamed_body else b"payload"
        response = await perform(
            HttpxTransport(client, request_adapter=adapt), streaming, RequestSpec("POST", "/value", body=body)
        )
        assert response.status_code == 204
    assert len(seen) == 1
    assert str(seen[0].url) == url
    assert seen[0].headers["host"] == host
    assert seen[0].content == b"payload"


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("native_auth", [False, True])
async def test_redirect_history_preserves_native_authentication_and_buffered_bodies(
    streaming: bool, native_auth: bool
) -> None:
    class ChallengeAuth(httpx.Auth):
        async def async_auth_flow(self, request: httpx.Request) -> AsyncGenerator[httpx.Request, httpx.Response]:
            response = yield request
            if response.status_code == 401:
                request.headers["X-Native-Auth"] = "accepted"
                yield request

    bodies: list[Body] = []
    observed: list[list[int]] = []

    def handle(request: httpx.Request) -> httpx.Response:
        if native_auth and "X-Native-Auth" not in request.headers:
            status, headers = 401, {}
        elif request.url.path == "/value":
            status, headers = 302, {"Location": "/middle"}
        elif request.url.path == "/middle":
            status, headers = 307, {"Location": "/final"}
        else:
            status, headers = 200, {}
        body = Body(f"body-{status}".encode())
        bodies.append(body)
        return httpx.Response(status, headers=headers, stream=body)

    async def adapt(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        observed.append([previous.status_code for previous in response.history])
        assert bodies[-1].reads == 0
        assert all(previous.is_closed for previous in response.history)
        return response

    async with httpx.AsyncClient(
        base_url="https://api.example",
        follow_redirects=True,
        transport=httpx.MockTransport(handle),
        auth=ChallengeAuth() if native_auth else None,
    ) as client:
        response = await perform(HttpxTransport(client, response_adapter=adapt), streaming)
    expected = [401, 302, 401, 307, 401] if native_auth else [302, 307]
    assert [previous.status_code for previous in response.history] == expected
    assert [previous.content for previous in response.history] == [f"body-{status}".encode() for status in expected]
    assert observed == ([[401], [401, 302, 401], expected] if native_auth else [[], [302], expected])
    assert response.content == b"body-200"
    assert all(body.reads == body.closes == 1 for body in bodies)


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("stage", ["read", "close"])
async def test_redirect_history_collection_preserves_stop_identity(streaming: bool, stage: str) -> None:
    stop = Stop("stop redirect collection")
    body = Body(
        error=stop if stage == "read" else None, close_error=stop if stage == "close" else RuntimeError("close")
    )
    sends = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal sends
        sends += 1
        return httpx.Response(302, headers={"Location": "/next"}, stream=body)

    async with httpx.AsyncClient(
        base_url="https://api.example", follow_redirects=True, transport=httpx.MockTransport(handle)
    ) as client:
        with pytest.raises(Stop) as failure:
            await perform(HttpxTransport(client), streaming)
        assert failure.value is stop
    assert sends == body.reads == body.closes == 1


@pytest.mark.parametrize(
    ("stage", "streaming"),
    [
        ("request", False),
        ("request", True),
        ("send", False),
        ("send", True),
        ("adapter", False),
        ("adapter", True),
        ("collection", False),
    ],
)
@pytest.mark.parametrize("bound_error", [False, True])
@pytest.mark.parametrize("stop", [False, True])
@pytest.mark.parametrize("managed", [True, False])
async def test_managed_errors_sanitize_diagnostics_without_rewriting_stop_errors(
    stage: str, streaming: bool, bound_error: bool, stop: bool, managed: bool
) -> None:
    provider = Provider()
    bindings = (
        BINDING,
        replace(BINDING, scheme="query", transport=SecurityTransport(location="query", name="api_key")),
        replace(BINDING, scheme="cookie", transport=SecurityTransport(location="cookie", name="session")),
    )
    sent: list[httpx.Request] = []
    failed_requests: list[httpx.Request] = []
    errors: list[httpx.ReadError] = []
    bodies: list[Body] = []

    def make_error(request: httpx.Request) -> httpx.ReadError:
        failed_requests.append(request)
        error = (Stop if stop else httpx.ReadError)(
            f"Read failed for {request.url}: {request.headers}", request=request if bound_error else None
        )
        errors.append(error)
        return error

    async def adapt_request(request: httpx.Request) -> httpx.Request:
        if stage == "request":
            raise make_error(request)
        return request

    def handle(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        error = make_error(request)
        if stage == "send":
            raise error
        body = Body(error=error)
        bodies.append(body)
        return httpx.Response(200, stream=body)

    async def adapt(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        # Adapters need the effective credentials; only escaping diagnostics change.
        if managed:
            assert request.headers["authorization"] == "Bearer token-1"
            assert request.url.params["api_key"] == "token-2"
            assert request.headers["cookie"] == "session=token-3"
        if stage == "adapter":
            await response.aread()
        return response

    async with httpx.AsyncClient(base_url="https://api.example", transport=httpx.MockTransport(handle)) as client:
        transport = HttpxTransport(client, **authentication_adapters({"identity": provider}, adapt_request, adapt))
        with pytest.raises(httpx.ReadError) as failure:
            await perform(transport, streaming, RequestSpec("GET", "/value", security=bindings if managed else None))
    error = failure.value
    assert len(sent) == (0 if stage == "request" else 1)
    assert [body.closes for body in bodies] == ([] if stage in {"request", "send"} else [1])
    if managed and not stop:
        assert type(error) is httpx.ReadError
        assert error is not errors[0]
        assert str(error) == "HTTP request failed"
        assert error.request.headers["authorization"] == "[redacted]"
        assert error.request.url.params["api_key"] == "[redacted]"
        assert error.request.headers["cookie"] == "[redacted]"
        assert "token-" not in str(error.request.url)
        assert "token-" not in str(error.request.headers)
        assert error.__cause__ is None and error.__suppress_context__
        assert failed_requests[0].headers["authorization"] == "Bearer token-1"
        assert failed_requests[0].url.params["api_key"] == "token-2"
        assert failed_requests[0].headers["cookie"] == "session=token-3"
    else:
        assert error is errors[0]


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("stage", ["request", "hook", "response"])
async def test_managed_adapter_and_hook_task_cancellation_keeps_identity(streaming: bool, stage: str) -> None:
    cancellation = asyncio.CancelledError("original adapter cancellation")
    body = Body()
    sends = 0

    async def adapt_request(request: httpx.Request) -> httpx.Request:
        if stage == "request":
            raise cancellation
        return request

    async def hook(response: httpx.Response) -> None:
        if stage == "hook":
            raise cancellation

    async def adapt_response(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        raise cancellation

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal sends
        sends += 1
        return httpx.Response(200, stream=body)

    async with httpx.AsyncClient(
        base_url="https://api.example", transport=httpx.MockTransport(handle), event_hooks={"response": [hook]}
    ) as client:
        transport = HttpxTransport(
            client, **authentication_adapters({"identity": Provider()}, adapt_request, adapt_response)
        )
        with pytest.raises(asyncio.CancelledError) as failure:
            await perform(transport, streaming, RequestSpec("GET", "/value", security=(BINDING,)))
        assert failure.value is cancellation
    assert sends == body.closes == (0 if stage == "request" else 1)


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("stage", ["hook", "adapter"])
@pytest.mark.parametrize("buffered", [False, True])
@pytest.mark.parametrize("stop", [False, True])
@pytest.mark.parametrize("managed", [False, True])
async def test_managed_status_errors_preserve_response_and_cancellation_identity(
    streaming: bool, stage: str, buffered: bool, stop: bool, managed: bool
) -> None:
    class StatusStop(httpx.HTTPStatusError, RequestCancellationError):
        pass

    bindings = (
        BINDING,
        replace(BINDING, scheme="query", transport=SecurityTransport(location="query", name="api_key")),
        replace(BINDING, scheme="cookie", transport=SecurityTransport(location="cookie", name="session")),
    )
    body = Body(b"original body")
    sent: list[httpx.Request] = []
    responses: list[httpx.Response] = []
    originals: list[httpx.HTTPStatusError] = []
    observed: list[BaseException] = []

    class Observer:
        def observe(self, event: TransportEvent) -> None:
            if event.kind is TransportEventKind.FAILURE:
                assert event.error is not None
                observed.append(event.error)

    async def fail(response: httpx.Response) -> None:
        if buffered:
            await response.aread()
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as error:
            if stop:
                error = StatusStop(str(error), request=error.request, response=error.response)
            originals.append(error)
            raise error

    async def hook(response: httpx.Response) -> None:
        if stage == "hook":
            await fail(response)

    async def adapt(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        await fail(response)
        return response

    def handle(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        response = httpx.Response(
            403, headers={"X-Detail": "preserved"}, stream=body, extensions={"http_version": b"HTTP/1.1"}
        )
        responses.append(response)
        return response

    async with httpx.AsyncClient(
        base_url="https://api.example", transport=httpx.MockTransport(handle), event_hooks={"response": [hook]}
    ) as client:
        transport = HttpxTransport(
            client, observers=(Observer(),), **authentication_adapters({"identity": Provider()}, None, adapt)
        )
        with pytest.raises(httpx.HTTPStatusError) as failure:
            await perform(transport, streaming, RequestSpec("GET", "/value", security=bindings if managed else None))
    error = failure.value
    assert len(sent) == len(originals) == body.closes == 1
    assert error.response is responses[0]
    assert error.response.status_code == 403
    assert error.response.headers["x-detail"] == "preserved"
    assert error.response.http_version == "HTTP/1.1"
    assert error.response.is_closed
    if buffered:
        assert error.response.content == b"original body"
    else:
        assert body.reads == 0
    assert len(observed) == (1 if stage == "hook" else 0)
    if managed and not stop:
        for diagnostic in [error, *observed]:
            assert type(diagnostic) is httpx.HTTPStatusError
            assert str(diagnostic) == "HTTP request failed"
            assert diagnostic.request.headers["authorization"] == "[redacted]"
            assert diagnostic.request.url.params["api_key"] == "[redacted]"
            assert diagnostic.request.headers["cookie"] == "[redacted]"
            assert diagnostic.response.request is diagnostic.request
        assert error is not originals[0]
        assert error.__cause__ is None and error.__suppress_context__
    else:
        assert error is originals[0]
        assert all(item is error for item in observed)
    if managed:
        assert sent[0].headers["authorization"] == "Bearer token-1"
        assert sent[0].url.params["api_key"] == "token-2"
        assert sent[0].headers["cookie"] == "session=token-3"
        if stop:
            assert error.request is sent[0]
            assert error.response.request is sent[0]


@pytest.mark.parametrize("stage", ["request", "send", "response"])
@pytest.mark.parametrize("cancel", [False, True])
async def test_unchanged_failures_preserve_their_explicit_cause(stage: str, cancel: bool) -> None:
    cause = ValueError("application cause")
    failure = RequestCancellationError("stopped") if cancel else RuntimeError("failed")

    async def request_adapter(request: httpx.Request) -> httpx.Request:
        if stage == "request":
            raise failure from cause
        return request

    async def respond(request: httpx.Request) -> httpx.Response:
        if stage == "send":
            raise failure from cause
        return httpx.Response(200)

    async def response_adapter(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        raise failure from cause

    async with (
        httpx.AsyncClient(base_url="https://example.test", transport=httpx.MockTransport(respond)) as client,
        HttpxTransport(client, request_adapter=request_adapter, response_adapter=response_adapter) as transport,
    ):
        with pytest.raises(type(failure)) as caught:
            await transport.execute_request(RequestSpec("GET", "/value"))
    assert caught.value is failure
    assert caught.value.__cause__ is cause


@pytest.mark.parametrize("method", ["GET", "POST"])
@pytest.mark.parametrize("status", [200, 401, 403])
@pytest.mark.parametrize(
    "challenge", ["Bearer error=invalid_token", "Basic error=invalid_token", "Bearer error=insufficient_scope"]
)
async def test_explicit_bearer_rejection_policy_preserves_bodies(method: str, status: int, challenge: str) -> None:
    from sunday.httpx import BearerTokenRejectionAdapter

    request = httpx.Request(
        method, "https://example.test", headers={"Authorization": "Bearer fixture"}, content=b"request"
    )
    body = Body()
    response = httpx.Response(status, headers={"WWW-Authenticate": challenge}, stream=body)
    adapter = BearerTokenRejectionAdapter()
    if status == 401 and challenge == "Bearer error=invalid_token":
        for _ in range(2):
            with pytest.raises(RequestCancellationError):
                await adapter.adapt(response, request)
    else:
        assert await adapter.adapt(response, request) is response
    del request.headers["Authorization"]
    assert await adapter.adapt(response, request) is response
    assert request.content == b"request"
    assert body.reads == 0
    await response.aclose()
