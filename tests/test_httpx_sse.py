# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import AsyncIterator

import anyio
import httpx
import pytest

from sunday import EventStreamOptions, Problem, RequestSpec, UnexpectedResponse
from sunday.httpx import HttpxTransport
from sunday.httpx_sse import HttpxEventStream


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


@pytest.mark.anyio
async def test_event_stream_reconnects_with_last_event_id_and_controls() -> None:
    requests: list[httpx.Request] = []
    stream = Chunks(b'retry: 1\nretry-max: 5\nkeepalive: 100\nid: event-1\ndata: {"value":1}\n\n')

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(200, headers={"Content-Type": "text/event-stream; charset=utf-8"}, stream=stream)
        return httpx.Response(204)

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        events = HttpxTransport(client).event_stream(
            RequestSpec("GET", "/events"),
            lambda event: event.data,
        )
        values = [value async for value in events]

    assert values == ['{"value":1}']
    assert requests[0].headers["accept"] == "text/event-stream"
    assert requests[1].headers["last-event-id"] == "event-1"
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
async def test_event_stream_timeout_reconnects_and_close_interrupts_reads() -> None:
    started = anyio.Event()
    blocking = Blocking(started)
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
            options=EventStreamOptions(retry=0.001, retry_max=0.002, event_timeout=0.01),
        )
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
async def test_explicit_event_timeout_overrides_keepalive_control() -> None:
    started = anyio.Event()
    blocking = ControlledBlocking(b"keepalive: 10000\n\n", started)
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
            options=EventStreamOptions(retry=0, retry_max=0, event_timeout=0.01),
        )
        with anyio.fail_after(0.2):
            assert [event async for event in event_stream] == []

    assert attempts == 2
    assert blocking.closed


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
