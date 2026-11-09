# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import anyio
import httpx
import pytest

from sunday import (
    EventSourceState,
    EventStreamOptions,
    Problem,
    RequestCancellationError,
    RequestEncodingError,
    RequestSpec,
    TransportError,
    UnexpectedResponse,
)
from sunday.httpx import HttpxEventStream, HttpxTransport


class Chunks(httpx.AsyncByteStream):
    def __init__(self, *chunks: bytes) -> None:
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


class Blocking(httpx.AsyncByteStream):
    def __init__(self, started: anyio.Event) -> None:
        self.started = started
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        self.started.set()
        await anyio.sleep_forever()
        yield b""  # pragma: no cover

    async def aclose(self) -> None:
        self.closed = True


class ControlledBlocking(httpx.AsyncByteStream):
    def __init__(self, control: bytes, started: anyio.Event) -> None:
        self.control = control
        self.started = started
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self.control
        self.started.set()
        await anyio.sleep_forever()
        yield b""  # pragma: no cover

    async def aclose(self) -> None:
        self.closed = True


class DelayedChunks(httpx.AsyncByteStream):
    def __init__(self, *chunks: tuple[float, bytes], block: bool = False) -> None:
        self.chunks = chunks
        self.block = block
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for delay, chunk in self.chunks:
            await anyio.sleep(delay)
            yield chunk
        if self.block:
            await anyio.sleep_forever()
            yield b""  # pragma: no cover

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.anyio
async def test_event_stream_reconnects_with_last_event_id_and_controls() -> None:
    requests: list[httpx.Request] = []
    adaptations = 0
    stream = Chunks(b'retry: 1\nretry-max: 5\nkeepalive: 100\nid: event-1\ndata: {"value":1}\n\n')

    async def adapter(_transport: HttpxTransport, request: httpx.Request) -> None:
        nonlocal adaptations
        adaptations += 1
        request.headers["X-Adapted"] = str(adaptations)

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(200, headers={"Content-Type": "text/event-stream; charset=utf-8"}, stream=stream)
        return httpx.Response(204)

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        events = HttpxTransport(client, adapters=(adapter,)).event_stream(
            RequestSpec("GET", "/events"),
            lambda event: event.data,
        )
        values = [value async for value in events]

    assert values == ['{"value":1}']
    assert requests[0].headers["accept"] == "text/event-stream"
    assert [request.headers["x-adapted"] for request in requests] == ["1", "2"]
    assert requests[1].headers["last-event-id"] == "event-1"
    assert adaptations == 2
    assert stream.closed


@pytest.mark.anyio
async def test_event_stream_treats_http_and_media_errors_as_fatal() -> None:
    responses = [
        httpx.Response(
            400,
            headers={"Content-Type": "application/problem+json"},
            json={"type": "https://example.test/problems/bad", "title": "Bad"},
        ),
        httpx.Response(200, headers={"Content-Type": "application/json"}, json={"value": 1}),
    ]

    def handler(_request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        transport = HttpxTransport(client)
        with pytest.raises(Problem):
            _ = [event async for event in transport.event_stream(RequestSpec("GET", "/events"), lambda event: event)]
        with pytest.raises(UnexpectedResponse):
            _ = [event async for event in transport.event_stream(RequestSpec("GET", "/events"), lambda event: event)]


@pytest.mark.anyio
async def test_server_keepalive_enables_timeout_and_reconnects() -> None:
    started = anyio.Event()
    blocking = ControlledBlocking(b"keepalive: 1\n\n", started)
    requests = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        if requests == 1:
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=blocking)
        return httpx.Response(204)

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        event_stream = HttpxTransport(client).event_stream(
            RequestSpec("GET", "/events"),
            lambda event: event,
            options=EventStreamOptions(retry=0.001, retry_max=0.002),
        )
        with anyio.fail_after(2):
            assert [event async for event in event_stream] == []

    assert requests == 2
    assert blocking.closed


@pytest.mark.anyio
async def test_event_stream_explicit_close_cancels_active_consumer() -> None:
    started = anyio.Event()
    blocking = Blocking(started)

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=blocking)

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        event_stream = HttpxTransport(client).event_stream(RequestSpec("GET", "/events"), lambda event: event)

        async def consume() -> None:
            _ = [event async for event in event_stream]

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(consume)
            await started.wait()
            await event_stream.aclose()

    assert blocking.closed


@pytest.mark.anyio
async def test_event_stream_caps_network_retries_and_resets_after_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    delays: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts in {1, 2, 3}:
            raise httpx.ConnectError("interrupted", request=request)
        if attempts == 4:
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=Chunks())
        return httpx.Response(204)

    async def record_retry(_stream: HttpxEventStream[object], delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(HttpxEventStream, "_wait_for_retry", record_retry)
    monkeypatch.setattr("sunday.httpx._reconnect.random.uniform", lambda _minimum, _maximum: 1.0)

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        event_stream = HttpxTransport(client).event_stream(
            RequestSpec("GET", "/events"),
            lambda event: event,
            options=EventStreamOptions(retry=0.001, retry_max=0.002),
        )
        assert [event async for event in event_stream] == []

    assert attempts == 5
    assert delays == [0.001, 0.002, 0.002, 0.001]


@pytest.mark.anyio
async def test_non_positive_keepalive_does_not_clear_active_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sunday.httpx._sse._KEEPALIVE_TIMEOUT_FLOOR", 0.05)
    blocking = DelayedChunks((0, b"keepalive: 1\n\n"), (0.01, b"keepalive: 0\n\n"), block=True)
    attempts = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=blocking)
        return httpx.Response(204)

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        event_stream = HttpxTransport(client).event_stream(
            RequestSpec("GET", "/events"),
            lambda event: event,
            options=EventStreamOptions(retry=0.001, retry_max=0.001),
        )
        with anyio.fail_after(0.2):
            assert [event async for event in event_stream] == []

    assert attempts == 2
    assert blocking.closed


@pytest.mark.anyio
async def test_comment_chunks_keep_an_active_stream_alive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sunday.httpx._sse._KEEPALIVE_TIMEOUT_FLOOR", 0.05)
    comments = DelayedChunks(
        (0, b"keepalive: 1\n\n"),
        (0.03, b": first\n\n"),
        (0.03, b": second\n\n"),
        (0.03, b"data: value\n\n"),
    )
    attempts = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=comments)
        return httpx.Response(204)

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        stream = HttpxTransport(client).event_stream(
            RequestSpec("GET", "/events"),
            lambda event: event.data,
            options=EventStreamOptions(retry=0.001, retry_max=0.001),
        )
        with anyio.fail_after(0.3):
            assert [event async for event in stream] == ["value"]

    assert attempts == 2


@pytest.mark.anyio
async def test_no_keepalive_disables_silence_detection_and_httpx_read_timeout() -> None:
    started = anyio.Event()
    blocking = Blocking(started)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=blocking)

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        timeout=0.01,
        transport=httpx.MockTransport(handler),
    ) as client:
        stream = HttpxTransport(client).event_stream(RequestSpec("GET", "/events"), lambda event: event)

        async def consume() -> None:
            _ = [event async for event in stream]

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(consume)
            await started.wait()
            await anyio.sleep(0.05)
            assert len(requests) == 1
            assert requests[0].extensions["timeout"]["read"] is None
            assert requests[0].extensions["timeout"]["connect"] == 0.01
            await stream.aclose()


@pytest.mark.anyio
async def test_keepalive_timeout_does_not_leak_to_next_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sunday.httpx._sse._KEEPALIVE_TIMEOUT_FLOOR", 0.02)
    second_started = anyio.Event()
    second = Blocking(second_started)
    attempts = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream"},
                stream=Chunks(b"keepalive: 1\n\n"),
            )
        return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=second)

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        stream = HttpxTransport(client).event_stream(
            RequestSpec("GET", "/events"),
            lambda event: event,
            options=EventStreamOptions(retry=0.001, retry_max=0.001),
        )

        async def consume() -> None:
            _ = [event async for event in stream]

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(consume)
            await second_started.wait()
            await anyio.sleep(0.05)
            assert attempts == 2
            await stream.aclose()


@pytest.mark.anyio
async def test_event_stream_readapts_each_managed_redirect() -> None:
    requests: list[httpx.Request] = []
    adaptations = 0

    async def adapter(_transport: HttpxTransport, request: httpx.Request) -> None:
        nonlocal adaptations
        adaptations += 1

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/events":
            return httpx.Response(307, headers={"Location": "/redirected"})
        return httpx.Response(204)

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        follow_redirects=False,
        transport=httpx.MockTransport(handler),
    ) as client:
        stream = HttpxTransport(client, adapters=(adapter,)).event_stream(
            RequestSpec("GET", "/events"),
            lambda event: event,
        )
        assert [event async for event in stream] == []

    assert [request.url.path for request in requests] == ["/events", "/redirected"]
    assert adaptations == 2


@pytest.mark.anyio
async def test_explicit_close_cancels_retry_wait() -> None:
    attempted = anyio.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        attempted.set()
        raise httpx.ConnectError("interrupted", request=request)

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        event_stream = HttpxTransport(client).event_stream(
            RequestSpec("GET", "/events"),
            lambda event: event,
            options=EventStreamOptions(retry=60, retry_max=60),
        )

        async def consume() -> None:
            _ = [event async for event in event_stream]

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(consume)
            await attempted.wait()
            await anyio.sleep(0)
            with anyio.fail_after(0.2):
                await event_stream.aclose()


@pytest.mark.anyio
async def test_event_source_dispatches_callbacks_and_typed_listeners() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream"},
                stream=Chunks(b"id: one\nevent: project\ndata: first\n\ndata: second\n\n"),
            )
        return httpx.Response(204)

    opened: list[EventSourceState] = []
    messages: list[str | None] = []
    projects: list[str | None] = []

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        transport = HttpxTransport(client)
        source = transport.event_source(RequestSpec("GET", "/events"))
        source.on_open = lambda: opened.append(source.ready_state)
        source.on_message = lambda event: messages.append(event.data)
        listener = source.add_event_listener("project", lambda event: projects.append(event.data))

        source.connect()
        with anyio.fail_after(1):
            while source.ready_state is not EventSourceState.CLOSED or len(requests) < 2:
                await anyio.sleep(0)

        source.remove_event_listener("project", listener)

    assert opened == [EventSourceState.OPEN]
    assert messages == ["first", "second"]
    assert projects == ["first"]
    assert requests[1].headers["last-event-id"] == "one"


@pytest.mark.anyio
async def test_event_source_returns_to_connecting_after_clean_completion() -> None:
    attempts = 0
    opened = anyio.Event()
    errors: list[BaseException | None] = []

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=Chunks())
        return httpx.Response(204)

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        source = HttpxTransport(client).event_source(
            RequestSpec("GET", "/events"),
            options=EventStreamOptions(retry=0.05, retry_max=0.05),
        )
        source.on_open = opened.set
        source.on_error = errors.append
        source.connect()
        await opened.wait()
        with anyio.fail_after(0.1):
            while source.ready_state is not EventSourceState.CONNECTING:
                await anyio.sleep(0)
        assert attempts == 1
        with anyio.fail_after(0.2):
            await source._wait_closed()

    assert attempts == 2
    assert errors == []


@pytest.mark.anyio
async def test_event_source_reports_retries_terminal_errors_and_callback_failures() -> None:
    attempts = 0
    errors: list[BaseException | None] = []
    callback_errors: list[BaseException] = []
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(
        lambda _loop, context: callback_errors.append(context["exception"]),
    )

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ConnectError("interrupted", request=request)
        if attempts == 2:
            return httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream"},
                stream=Chunks(b"data: value\n\n"),
            )
        return httpx.Response(
            400,
            headers={"Content-Type": "application/problem+json"},
            json={"type": "https://example.test/problems/bad", "title": "Bad"},
        )

    try:
        async with httpx.AsyncClient(
            base_url="https://api.example.test",
            transport=httpx.MockTransport(handler),
        ) as client:
            source = HttpxTransport(client).event_source(
                RequestSpec("GET", "/events"),
                options=EventStreamOptions(retry=0.001, retry_max=0.001),
            )
            source.on_error = errors.append

            def fail(_event: object) -> None:
                raise ValueError("callback failed")

            source.on_message = fail
            source.connect()
            with anyio.fail_after(1):
                while source.ready_state is not EventSourceState.CLOSED or attempts < 3:
                    await anyio.sleep(0)
    finally:
        loop.set_exception_handler(previous_handler)

    assert isinstance(errors[0], httpx.ConnectError)
    assert isinstance(errors[-1], Problem)
    assert isinstance(callback_errors[0], ValueError)


@pytest.mark.anyio
async def test_event_source_async_factory_adapts_once_and_close_interrupts_read() -> None:
    started = anyio.Event()
    blocking = Blocking(started)
    adaptations = 0

    async def adapter(_transport: HttpxTransport, request: httpx.Request) -> None:
        nonlocal adaptations
        adaptations += 1
        request.headers["X-Adapted"] = str(adaptations)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["x-adapted"] == "1"
        assert request.extensions["timeout"]["read"] is None
        return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=blocking)

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        transport = HttpxTransport(client, adapters=(adapter,))

        async def request_factory(headers: tuple[tuple[str, str], ...]) -> httpx.Request:
            await anyio.sleep(0)
            return client.build_request("GET", "/events", headers=headers)

        source = transport.event_source_from(request_factory)
        source.connect()
        await started.wait()
        source.close()
        with anyio.fail_after(1):
            while not blocking.closed:
                await anyio.sleep(0)

    assert adaptations == 1
    assert source.ready_state is EventSourceState.CLOSED


@pytest.mark.anyio
async def test_closed_event_resources_raise_transport_errors() -> None:
    async with httpx.AsyncClient(base_url="https://api.example.test") as client:
        transport = HttpxTransport(client)
        stream = transport.event_stream(RequestSpec("GET", "/events"), lambda event: event)
        await stream.aclose()
        with pytest.raises(TransportError, match="HttpxEventStream is closed"):
            async with stream:
                pass  # pragma: no cover

        source = transport.event_source(RequestSpec("GET", "/events"))
        await transport.aclose()
        with pytest.raises(TransportError, match="HttpxTransport is closed"):
            source.connect()


@pytest.mark.anyio
async def test_invalid_parameters_are_terminal_encoding_errors_for_sse() -> None:
    validations = 0
    sends = 0
    failure = ValueError("Unknown parameter")

    def validate() -> None:
        nonlocal validations
        validations += 1
        raise failure

    def send(_request: httpx.Request) -> httpx.Response:
        nonlocal sends
        sends += 1
        return httpx.Response(204)

    spec: RequestSpec[None] = RequestSpec("GET", "/events", parameter_validation=validate)
    async with httpx.AsyncClient(transport=httpx.MockTransport(send), base_url="https://example.com") as client:
        transport = HttpxTransport(client)
        stream = transport.event_stream(spec, lambda event: event.data)
        with anyio.fail_after(1), pytest.raises(RequestEncodingError) as raised:
            await anext(aiter(stream))
        assert raised.value.__cause__ is failure
        assert validations == 1
        source = transport.event_source(spec)
        errors: list[BaseException | None] = []
        source.on_error = errors.append
        source.connect()
        with anyio.fail_after(1):
            await source._wait_closed()
        assert len(errors) == 1 and isinstance(errors[0], RequestEncodingError)
        assert errors[0].__cause__ is failure
        assert validations == 2
        assert sends == 0


class TrackedChunks(Chunks):
    def __init__(self, *chunks: bytes, close_error: BaseException | None = None) -> None:
        super().__init__(*chunks)
        self.closes = 0
        self.close_error = close_error

    async def aclose(self) -> None:
        self.closes += 1
        await anyio.sleep(0)
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


@pytest.mark.anyio
@pytest.mark.parametrize("close_mode", ["context", "explicit", "external"])
async def test_early_stream_exit_leaves_consumer_task_usable(close_mode: str) -> None:
    body = TrackedChunks(b"data: first\n\ndata: second\n\n")

    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/after":
            return httpx.Response(204)
        return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=body)

    async with httpx.AsyncClient(base_url="https://api.example", transport=httpx.MockTransport(handle)) as client:
        transport = HttpxTransport(client)
        stream = transport.event_stream(RequestSpec("GET", "/events"), lambda event: event.data)
        iterator = stream.events()
        try:
            if close_mode == "context":
                async with stream:
                    async for value in iterator:
                        assert value == "first"
                        break
            else:
                assert await anext(iterator) == "first"
                if close_mode == "external":
                    await asyncio.create_task(stream.aclose())
                else:
                    await stream.aclose()
            # Closing an SSE stream must not leave cancellation active in its consumer.
            await anyio.sleep(0)
            assert (await transport.execute_request(RequestSpec("GET", "/after"))).status_code == 204
            with pytest.raises(StopAsyncIteration):
                await anext(iterator)
            await stream.aclose()
        finally:
            await iterator.aclose()
    assert body.closed and body.closes == 1


@pytest.mark.anyio
@pytest.mark.parametrize("error_kind", ["stop", "task-cancellation", "ordinary"])
async def test_stream_context_cleanup_preserves_consumer_error_identity(error_kind: str) -> None:
    error = {
        "stop": RequestCancellationError("consumer stop"),
        "task-cancellation": asyncio.CancelledError("consumer cancellation"),
        "ordinary": ValueError("consumer failure"),
    }[error_kind]
    body = TrackedChunks(b"data: value\n\n", close_error=RuntimeError("cleanup failure"))
    async with httpx.AsyncClient(
        base_url="https://api.example",
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=body)
        ),
    ) as client:
        stream = HttpxTransport(client).event_stream(RequestSpec("GET", "/events"), lambda event: event.data)
        iterator = stream.events()
        try:
            with pytest.raises(type(error)) as failure:
                async with stream:
                    assert await anext(iterator) == "value"
                    raise error
            assert failure.value is error
            assert "Closing the HTTP response also failed" in error.__notes__
            await anyio.sleep(0)
            await stream.aclose()
        finally:
            await iterator.aclose()
    assert body.closes == 1


@pytest.mark.anyio
@pytest.mark.parametrize("error_type", [RequestCancellationError, asyncio.CancelledError, RuntimeError])
async def test_stream_context_propagates_cleanup_failure_without_primary_error(
    error_type: type[BaseException],
) -> None:
    error = error_type("cleanup stop")
    body = TrackedChunks(b"data: value\n\n", close_error=error)
    async with httpx.AsyncClient(
        base_url="https://api.example",
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=body)
        ),
    ) as client:
        stream = HttpxTransport(client).event_stream(RequestSpec("GET", "/events"), lambda event: event.data)
        iterator = stream.events()
        try:
            with pytest.raises(error_type) as failure:
                async with stream:
                    await anext(iterator)
            assert failure.value is error
            await stream.aclose()
        finally:
            await iterator.aclose()
    assert body.closes == 1


@pytest.mark.anyio
@pytest.mark.parametrize("stage", ["request", "response"])
async def test_stream_close_interrupts_adapters_without_cancelling_consumer(stage: str) -> None:
    entered = anyio.Event()
    body = TrackedChunks(b"data: value\n\n")

    async def adapt_request(request: httpx.Request) -> httpx.Request:
        if stage == "request":
            entered.set()
            await anyio.sleep_forever()
        return request

    async def adapt_response(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        if stage == "response":
            entered.set()
            await anyio.sleep_forever()
        return response

    async with httpx.AsyncClient(
        base_url="https://api.example",
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=body)
        ),
    ) as client:
        transport = HttpxTransport(client, request_adapter=adapt_request, response_adapter=adapt_response)
        stream = transport.event_stream(RequestSpec("GET", "/events"), lambda event: event.data)
        completed = anyio.Event()

        async def consume() -> None:
            assert [event async for event in stream] == []
            await anyio.sleep(0)
            completed.set()

        with anyio.fail_after(2):
            async with anyio.create_task_group() as group:
                group.start_soon(consume)
                await entered.wait()
                await stream.aclose()
                assert body.closes == (1 if stage == "response" else 0)
                await completed.wait()
    assert body.closes == (1 if stage == "response" else 0)


@pytest.mark.anyio
async def test_early_stream_exit_releases_native_connection_before_client_close() -> None:
    from anyio.abc import SocketAttribute, SocketStream

    disconnected = anyio.Event()

    async def serve(socket: SocketStream) -> None:
        async with socket:
            headers = bytearray()
            while b"\r\n\r\n" not in headers:
                headers.extend(await socket.receive())
            if headers.startswith(b"GET /after "):
                await socket.send(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
                return
            await socket.send(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 1024\r\n\r\ndata: value\n\n"
            )
            with pytest.raises(anyio.EndOfStream):
                await socket.receive()
            disconnected.set()

    listener = await anyio.create_tcp_listener(local_host="127.0.0.1")
    port = listener.extra(SocketAttribute.local_port)
    async with listener, anyio.create_task_group() as group:
        group.start_soon(listener.serve, serve)
        try:
            async with httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{port}", trust_env=False, limits=httpx.Limits(max_connections=1)
            ) as client:
                transport = HttpxTransport(client)
                stream = transport.event_stream(RequestSpec("GET", "/events"), lambda event: event.data)
                iterator = stream.events()
                try:
                    with anyio.fail_after(2):
                        async with stream:
                            assert await anext(iterator) == "value"
                        await disconnected.wait()
                        assert (await transport.execute_request(RequestSpec("GET", "/after"))).status_code == 204
                finally:
                    await iterator.aclose()
        finally:
            group.cancel_scope.cancel()
