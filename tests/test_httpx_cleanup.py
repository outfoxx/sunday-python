# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

import anyio
import h11
import httpx
import pytest
from anyio.abc import SocketAttribute, SocketStream

from sunday import (
    EventSourceState,
    MediaType,
    OperationSpec,
    RequestCancellationError,
    RequestSpec,
    ResponseSpec,
    SecurityTransport,
    StreamingBody,
    TokenRequest,
)
from sunday.httpx import HttpxOAuthTokenProvider, HttpxTransport, copy_request
from sunday.httpx._security import AuthenticationRecoveryBudget

pytestmark = pytest.mark.anyio


@dataclass
class Server:
    url: str
    disconnected: anyio.Event = field(default_factory=anyio.Event)
    uploads: list[tuple[httpx.Headers, bytes]] = field(default_factory=list)

    async def serve(self, socket: SocketStream) -> None:
        async with socket:
            connection = h11.Connection(h11.SERVER)
            request: h11.Request | None = None
            body = bytearray()
            while True:
                event = connection.next_event()
                if event is h11.NEED_DATA:
                    try:
                        connection.receive_data(await socket.receive())
                    except anyio.EndOfStream:
                        self.disconnected.set()
                        return
                elif isinstance(event, h11.Request):
                    request = event
                elif isinstance(event, h11.Data):
                    body.extend(event.data)
                elif isinstance(event, h11.EndOfMessage):
                    break
            assert request is not None
            if request.target == b"/upload":
                self.uploads.append((httpx.Headers(request.headers), bytes(body)))
            if request.target in {b"/upload", b"/after"}:
                await socket.send(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
                return
            if request.target == b"/headers":
                with pytest.raises(anyio.EndOfStream):
                    await socket.receive()
                self.disconnected.set()
                return
            if request.target == b"/bulk":
                payload = b"x" * (1024 * 1024)
                await socket.send(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: "
                    + str(len(payload)).encode()
                    + b"\r\nConnection: close\r\n\r\n"
                    + payload
                )
            elif request.target == b"/eof":
                await socket.send(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                    b"Content-Length: 13\r\nConnection: close\r\n\r\ndata: first\n\n"
                )
            else:
                await socket.send(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 1024\r\n\r\ndata: first\n\n"
                )
            with pytest.raises(anyio.EndOfStream):
                await socket.receive()
            self.disconnected.set()


@pytest.fixture
async def server() -> AsyncIterator[Server]:
    listener = await anyio.create_tcp_listener(local_host="127.0.0.1")
    state = Server(f"http://127.0.0.1:{listener.extra(SocketAttribute.local_port)}")
    async with listener, anyio.create_task_group() as group:
        group.start_soon(listener.serve, state.serve)
        try:
            yield state
        finally:
            group.cancel_scope.cancel()


async def execute(transport: HttpxTransport, streaming: bool, spec: RequestSpec[Any]) -> httpx.Response:
    if not streaming:
        return await transport.execute_request(spec)
    response = await transport._send_event_stream(
        await transport._prepare_request(spec), recovery_budget=AuthenticationRecoveryBudget()
    )
    try:
        await response.aread()
        return response
    finally:
        await response.aclose()


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("original", ["length", "chunked", "missing"])
@pytest.mark.parametrize("override", [None, "length", "chunked"])
async def test_copy_preserves_valid_streaming_framing_over_tcp(
    server: Server, streaming: bool, original: str, override: str | None
) -> None:
    async def adapt(request: httpx.Request) -> httpx.Request:
        if original == "missing":
            request.headers.pop("transfer-encoding", None)
        headers = {"X-Custom": "value"}
        if override == "length":
            headers["Content-Length"] = "7"
        elif override == "chunked":
            headers["Transfer-Encoding"] = "chunked"
        copied = copy_request(request, headers=headers)
        assert copied.stream is request.stream
        return copied

    async with httpx.AsyncClient(base_url=server.url, trust_env=False) as client:
        spec = RequestSpec(
            "POST",
            "/upload",
            body=StreamingBody.bytes(b"payload"),
            headers=(("Content-Length", "7"),) if original == "length" else (),
        )
        with anyio.fail_after(2):
            response = await execute(HttpxTransport(client, request_adapter=adapt), streaming, spec)
        assert response.status_code == 204
    assert len(server.uploads) == 1
    headers, body = server.uploads[0]
    expected = override or ("length" if original == "length" else "chunked")
    assert body == b"payload"
    assert headers["x-custom"] == "value"
    if expected == "length":
        assert headers["content-length"] == "7"
        assert "transfer-encoding" not in headers
    else:
        assert headers["transfer-encoding"] == "chunked"
        assert "content-length" not in headers


class Stop(httpx.ReadError, RequestCancellationError):
    pass


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("primary", ["stop", "cancellation"])
@pytest.mark.parametrize("close_fails", [False, True])
async def test_repeated_cancellation_waits_for_native_cleanup_and_preserves_identity(
    server: Server, streaming: bool, primary: str, close_fails: bool
) -> None:
    closing, release = anyio.Event(), anyio.Event()
    stop = Stop("original stop")
    observed: list[BaseException] = []
    closes = 0

    class GatedClose(httpx.AsyncByteStream):
        def __init__(self, wrapped: httpx.AsyncByteStream) -> None:
            self.wrapped = wrapped

        async def __aiter__(self) -> AsyncIterator[bytes]:
            async for chunk in self.wrapped:
                yield chunk

        async def aclose(self) -> None:
            nonlocal closes
            closes += 1
            closing.set()
            await release.wait()
            await self.wrapped.aclose()
            if close_fails:
                raise RuntimeError("cleanup failed after releasing the native connection")

    async def adapt(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        assert isinstance(response.stream, httpx.AsyncByteStream)
        response.stream = GatedClose(response.stream)
        owner = asyncio.current_task()
        assert owner is not None
        owner.cancel("first cancellation")
        if primary == "stop":
            raise stop
        try:
            await anyio.sleep(0)
        except asyncio.CancelledError as error:
            observed.append(error)
            raise
        raise AssertionError("Pending cancellation was not delivered")

    async with httpx.AsyncClient(
        base_url=server.url, trust_env=False, limits=httpx.Limits(max_connections=1), timeout=1
    ) as client:
        transport = HttpxTransport(client, response_adapter=adapt)
        task = asyncio.create_task(execute(transport, streaming, RequestSpec("GET", "/events")))
        try:
            with anyio.fail_after(3):
                await closing.wait()
                task.cancel("second cancellation")
                await anyio.sleep(0)
                task.cancel("third cancellation")
                await anyio.sleep(0)
                assert not task.done()
                release.set()
                with pytest.raises(Stop if primary == "stop" else asyncio.CancelledError) as failure:
                    await task
                expected = stop if primary == "stop" else observed[0]
                assert failure.value is expected
                if close_fails:
                    assert "Closing the HTTP response also failed" in expected.__notes__
                await server.disconnected.wait()
                # Prove pool release before closing the borrowed client.
                assert (await client.get("/after")).status_code == 204
        finally:
            release.set()
            if not task.done():
                task.cancel()
                with suppress(Stop, asyncio.CancelledError):
                    await task
    assert closes == 1


async def test_pending_cancellation_during_stream_context_exit_releases_native_pool(server: Server) -> None:
    observed: list[BaseException] = []
    async with httpx.AsyncClient(
        base_url=server.url, trust_env=False, limits=httpx.Limits(max_connections=1), timeout=1
    ) as client:
        transport = HttpxTransport(client)

        async def consume() -> None:
            stream = transport.event_stream(RequestSpec("GET", "/events"), lambda event: event.data)
            iterator = stream.events()
            try:
                async with stream:
                    assert await anext(iterator) == "first"
                    owner = asyncio.current_task()
                    assert owner is not None
                    owner.cancel("pending at context exit")
            except asyncio.CancelledError as error:
                observed.append(error)
                raise
            finally:
                await iterator.aclose()

        with anyio.fail_after(3):
            with pytest.raises(asyncio.CancelledError) as failure:
                await asyncio.create_task(consume())
            assert failure.value is observed[0]
            assert failure.value.args == ("pending at context exit",)
            await server.disconnected.wait()
            assert (await transport.execute_request(RequestSpec("GET", "/after"))).status_code == 204


async def test_event_source_callback_close_releases_native_pool_without_task_cancellation(server: Server) -> None:
    received = anyio.Event()
    tasks: list[asyncio.Task[Any]] = []
    async with httpx.AsyncClient(
        base_url=server.url, trust_env=False, limits=httpx.Limits(max_connections=1), timeout=1
    ) as client:
        transport = HttpxTransport(client)
        source = transport.event_source(RequestSpec("GET", "/events"))

        def on_message(event: object) -> None:
            task = asyncio.current_task()
            assert task is not None
            tasks.append(task)
            source.close()
            received.set()

        source.on_message = on_message
        source.connect()
        with anyio.fail_after(3):
            await received.wait()
            await source._wait_closed()
            assert len(tasks) == 1 and not tasks[0].cancelled()
            await server.disconnected.wait()
            assert (await transport.execute_request(RequestSpec("GET", "/after"))).status_code == 204


async def consume_eof(transport: HttpxTransport, streaming: bool) -> None:
    if not streaming:
        await transport.execute_request(RequestSpec("GET", "/eof"))
        return
    async with transport.event_stream(RequestSpec("GET", "/eof"), lambda event: event.data) as stream:
        async for event in stream:
            assert event == "first"


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("cancellation", ["task", "scope"])
async def test_eof_cancellation_finishes_native_cleanup_before_propagating(
    server: Server, streaming: bool, cancellation: str
) -> None:
    closing, release = anyio.Event(), anyio.Event()
    completed = 0
    observed: list[asyncio.CancelledError] = []
    scope = anyio.CancelScope()

    async def trace(name: str, info: dict[str, Any]) -> None:
        nonlocal completed
        if name == "http11.response_closed.started":
            closing.set()
            await release.wait()
        elif name == "http11.response_closed.complete":
            completed += 1

    async def adapt(request: httpx.Request) -> httpx.Request:
        request.extensions["trace"] = trace
        return request

    async with httpx.AsyncClient(
        base_url=server.url, trust_env=False, limits=httpx.Limits(max_connections=1), timeout=1
    ) as client:
        transport = HttpxTransport(client, request_adapter=adapt)

        async def run() -> None:
            with scope:
                try:
                    await consume_eof(transport, streaming)
                    await anyio.lowlevel.checkpoint()
                except asyncio.CancelledError as error:
                    observed.append(error)
                    raise

        task = asyncio.create_task(run())
        try:
            with anyio.fail_after(3):
                await closing.wait()
                if cancellation == "task":
                    task.cancel("first EOF cancellation")
                    await anyio.sleep(0)
                    task.cancel("second EOF cancellation")
                else:
                    scope.cancel()
                await anyio.sleep(0)
                assert not task.done()
                assert completed == 0
                release.set()
                if cancellation == "task":
                    with pytest.raises(asyncio.CancelledError) as failure:
                        await task
                    assert failure.value is observed[0]
                    assert failure.value.args == ("first EOF cancellation",)
                else:
                    await task
                    assert scope.cancelled_caught and len(observed) == 1
                assert completed == 1
                await server.disconnected.wait()
                assert (await client.get("/after")).status_code == 204
        finally:
            release.set()
            if not task.done():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("error_type", [Stop, asyncio.CancelledError, RuntimeError])
async def test_eof_cleanup_preserves_original_error_identity(
    server: Server, streaming: bool, error_type: type[BaseException]
) -> None:
    error = error_type("original EOF cleanup failure")
    closes = 0

    class FailingClose(httpx.AsyncByteStream):
        def __init__(self, stream: httpx.AsyncByteStream) -> None:
            self.stream = stream

        def __aiter__(self) -> AsyncIterator[bytes]:
            return self.stream.__aiter__()

        async def aclose(self) -> None:
            nonlocal closes
            closes += 1
            await self.stream.aclose()
            raise error

    async def adapt(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        assert isinstance(response.stream, httpx.AsyncByteStream)
        response.stream = FailingClose(response.stream)
        return response

    async with httpx.AsyncClient(
        base_url=server.url, trust_env=False, limits=httpx.Limits(max_connections=1), timeout=1
    ) as client:
        with anyio.fail_after(3):
            with pytest.raises(error_type) as failure:
                await consume_eof(HttpxTransport(client, response_adapter=adapt), streaming)
            assert failure.value is error
            assert closes == 1
            await server.disconnected.wait()
            assert (await client.get("/after")).status_code == 204


@pytest.mark.parametrize("before_start", [False, True])
async def test_event_source_immediate_reconnect_keeps_new_run_open(server: Server, before_start: bool) -> None:
    received = anyio.Event()
    tasks: list[asyncio.Task[Any]] = []
    errors: list[BaseException | None] = []
    async with httpx.AsyncClient(base_url=server.url, trust_env=False) as client:
        transport = HttpxTransport(client)
        source = transport.event_source(RequestSpec("GET", "/events"))

        def on_message(event: object) -> None:
            task = asyncio.current_task()
            assert task is not None
            tasks.append(task)
            received.set()

        source.on_message = on_message
        source.on_error = errors.append
        source.connect()
        try:
            with anyio.fail_after(3):
                if not before_start:
                    await received.wait()
                    received = anyio.Event()
                source.close()
                source.connect()
                await received.wait()
                if not before_start:
                    await tasks[0]
                # Let the retired run's completion callback execute.
                await anyio.sleep(0)
                assert source.ready_state is EventSourceState.OPEN
                assert not tasks[-1].done()
                assert source in transport._event_sources
                assert errors == []
                await transport.aclose()
                assert all(task.done() and not task.cancelled() for task in tasks)
                assert str(source.ready_state) == "closed"
                assert source not in transport._event_sources
        finally:
            await transport.aclose()


async def test_transport_shutdown_waits_for_retiring_event_source_cleanup(server: Server) -> None:
    closing, release, received = anyio.Event(), anyio.Event(), anyio.Event()
    connections = 0

    async def trace(name: str, info: dict[str, Any]) -> None:
        if name == "http11.response_closed.started":
            closing.set()
            await release.wait()

    async def adapt(request: httpx.Request) -> httpx.Request:
        nonlocal connections
        connections += 1
        if connections == 1:
            request.extensions["trace"] = trace
        return request

    async with httpx.AsyncClient(base_url=server.url, trust_env=False) as client:
        transport = HttpxTransport(client, request_adapter=adapt)
        source = transport.event_source(RequestSpec("GET", "/events"))
        source.on_message = lambda event: received.set()
        source.connect()
        shutdown: asyncio.Task[None] | None = None
        try:
            with anyio.fail_after(3):
                await received.wait()
                received = anyio.Event()
                source.close()
                source.connect()
                await closing.wait()
                await received.wait()
                source.close()
                # The new run can finish while the old run still owns its connection.
                while len(source._tasks) > 1:
                    await anyio.sleep(0)
                assert source in transport._event_sources
                shutdown = asyncio.create_task(transport.aclose())
                await anyio.sleep(0)
                assert not shutdown.done()
                release.set()
                await shutdown
                assert not source._tasks
                assert source not in transport._event_sources
                assert (await client.get("/after")).status_code == 204
        finally:
            release.set()
            if shutdown is not None:
                await shutdown
            else:
                await transport.aclose()


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("stage", ["read", "headers", "adapter"])
@pytest.mark.parametrize("cancellation", ["task", "scope"])
async def test_native_io_cancellation_waits_for_internal_cleanup(
    server: Server, streaming: bool, stage: str, cancellation: str
) -> None:
    entered, closing, release = anyio.Event(), anyio.Event(), anyio.Event()
    scope = anyio.CancelScope()
    observed: list[asyncio.CancelledError] = []

    async def trace(name: str, info: dict[str, Any]) -> None:
        entry = (
            "http11.receive_response_body.started" if stage != "headers" else "http11.receive_response_headers.started"
        )
        closure = "http11.response_closed.started"
        if name == entry:
            entered.set()
        elif name == closure:
            closing.set()
            await release.wait()

    async def adapt(request: httpx.Request) -> httpx.Request:
        request.extensions["trace"] = trace
        return request

    async with httpx.AsyncClient(
        base_url=server.url, trust_env=False, limits=httpx.Limits(max_connections=1), timeout=1
    ) as client:

        async def read_response(response: httpx.Response, request: httpx.Request) -> httpx.Response:
            await response.aread()
            return response

        transport = HttpxTransport(
            client, request_adapter=adapt, response_adapter=read_response if stage == "adapter" else None
        )
        spec: RequestSpec[None] = RequestSpec("GET", "/headers" if stage == "headers" else "/events")

        async def run() -> None:
            with scope:
                try:
                    if streaming:
                        async with transport.event_stream(spec, lambda event: event.data) as stream:
                            async for event in stream:
                                assert event == "first"
                    else:
                        await transport.execute_request(spec)
                except asyncio.CancelledError as error:
                    observed.append(error)
                    raise

        task = asyncio.create_task(run())
        try:
            with anyio.fail_after(3):
                await entered.wait()
                if cancellation == "task":
                    task.cancel("original I/O cancellation")
                else:
                    scope.cancel()
                await closing.wait()
                task.cancel("second I/O cancellation")
                await anyio.sleep(0)
                task.cancel("third I/O cancellation")
                await anyio.sleep(0)
                assert not task.done()
                release.set()
                if cancellation == "task":
                    with pytest.raises(asyncio.CancelledError) as failure:
                        await task
                    assert failure.value is observed[0]
                    assert failure.value.args == ("original I/O cancellation",)
                else:
                    await task
                    assert scope.cancelled_caught and len(observed) == 1
                await server.disconnected.wait()
                assert (await client.get("/after")).status_code == 204
        finally:
            release.set()
            if not task.done():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task


@pytest.mark.parametrize("owned", [False, True])
@pytest.mark.parametrize("cancellation", ["task", "scope"])
async def test_shutdown_shares_cleanup_and_preserves_caller_cancellation(
    server: Server, owned: bool, cancellation: str
) -> None:
    opened, closing, release = anyio.Event(), anyio.Event(), anyio.Event()
    observed: list[asyncio.CancelledError] = []
    scope = anyio.CancelScope()

    async def trace(name: str, info: dict[str, Any]) -> None:
        if name == "http11.response_closed.started":
            closing.set()
            await release.wait()

    async def adapt(request: httpx.Request) -> httpx.Request:
        request.extensions["trace"] = trace
        return request

    async with httpx.AsyncClient(
        base_url=server.url, trust_env=False, limits=httpx.Limits(max_connections=1), timeout=1
    ) as client:
        transport = (
            HttpxTransport(base_url=server.url, request_adapter=adapt)
            if owned
            else HttpxTransport(client, request_adapter=adapt)
        )
        source = transport.event_source(RequestSpec("GET", "/events"))
        source.on_message = lambda event: opened.set()
        source.connect()
        await opened.wait()

        async def shutdown() -> None:
            with scope:
                try:
                    await transport.aclose()
                except asyncio.CancelledError as error:
                    observed.append(error)
                    raise

        task = asyncio.create_task(shutdown())
        other: asyncio.Task[None] | None = None
        try:
            with anyio.fail_after(3):
                await closing.wait()
                other = asyncio.create_task(transport.aclose())
                if cancellation == "task":
                    task.cancel("original shutdown cancellation")
                else:
                    scope.cancel()
                await anyio.sleep(0)
                if cancellation == "task":
                    task.cancel("second shutdown cancellation")
                else:
                    scope.cancel()
                await anyio.sleep(0)
                assert not task.done() and not other.done()
                release.set()
                if cancellation == "task":
                    with pytest.raises(asyncio.CancelledError) as failure:
                        await task
                    assert failure.value is observed[0]
                    assert failure.value.args == ("original shutdown cancellation",)
                else:
                    await task
                    assert scope.cancelled_caught and len(observed) == 1
                await other
                await server.disconnected.wait()
                assert not source._tasks
                assert transport.client.is_closed is owned
                if not owned:
                    assert (await client.get("/after")).status_code == 204
        finally:
            release.set()
            with suppress(asyncio.CancelledError):
                await task
            if other is not None:
                await other
            await transport.aclose()


@pytest.mark.parametrize("error_type", [Stop, ValueError])
async def test_concurrent_stream_closers_join_consumer_cleanup(server: Server, error_type: type[Exception]) -> None:
    closing, release = anyio.Event(), anyio.Event()
    original = error_type("original decoder failure")
    observed: list[asyncio.CancelledError] = []

    async def trace(name: str, info: dict[str, Any]) -> None:
        if name == "http11.response_closed.started":
            closing.set()
            await release.wait()

    async def adapt(request: httpx.Request) -> httpx.Request:
        request.extensions["trace"] = trace
        return request

    def decode(event: object) -> str:
        raise original

    async with httpx.AsyncClient(
        base_url=server.url, trust_env=False, limits=httpx.Limits(max_connections=1), timeout=1
    ) as client:
        transport = HttpxTransport(client, request_adapter=adapt)
        stream = transport.event_stream(RequestSpec("GET", "/events"), decode)

        async def consume() -> None:
            async with stream:
                async for _event in stream:
                    raise AssertionError("Decoder must fail")

        async def close() -> None:
            try:
                await stream.aclose()
            except asyncio.CancelledError as error:
                observed.append(error)
                raise

        consumer = asyncio.create_task(consume())
        closers: list[asyncio.Task[None]] = []
        try:
            with anyio.fail_after(3):
                await closing.wait()
                closers = [asyncio.create_task(close()), asyncio.create_task(stream.aclose())]
                await anyio.sleep(0)
                closers[0].cancel("original close cancellation")
                await anyio.sleep(0)
                closers[0].cancel("second close cancellation")
                await anyio.sleep(0)
                assert not consumer.done() and all(not task.done() for task in closers)
                release.set()
                with pytest.raises(error_type) as failure:
                    await consumer
                assert failure.value is original
                with pytest.raises(asyncio.CancelledError) as cancelled:
                    await closers[0]
                assert cancelled.value is observed[0]
                assert cancelled.value.args == ("original close cancellation",)
                await closers[1]
                await server.disconnected.wait()
                assert (await client.get("/after")).status_code == 204
        finally:
            release.set()
            for task in [consumer, *closers]:
                with suppress(error_type, asyncio.CancelledError):
                    await task


@pytest.mark.parametrize("streaming", [False, True])
async def test_native_stop_during_read_keeps_identity_despite_later_cancellation(
    server: Server, streaming: bool
) -> None:
    closing, release = anyio.Event(), anyio.Event()
    original = Stop("original native stop")

    async def trace(name: str, info: dict[str, Any]) -> None:
        if name == "http11.receive_response_body.started":
            raise original
        if name == "http11.response_closed.started":
            closing.set()
            await release.wait()

    async def adapt(request: httpx.Request) -> httpx.Request:
        request.extensions["trace"] = trace
        return request

    async with httpx.AsyncClient(
        base_url=server.url, trust_env=False, limits=httpx.Limits(max_connections=1), timeout=1
    ) as client:
        task = asyncio.create_task(
            execute(HttpxTransport(client, request_adapter=adapt), streaming, RequestSpec("GET", "/events"))
        )
        try:
            with anyio.fail_after(3):
                await closing.wait()
                task.cancel("late cancellation")
                await anyio.sleep(0)
                assert not task.done()
                release.set()
                with pytest.raises(Stop) as failure:
                    await task
                assert failure.value is original
                await server.disconnected.wait()
                assert (await client.get("/after")).status_code == 204
        finally:
            release.set()
            with suppress(Stop, asyncio.CancelledError):
                await task


@pytest.mark.parametrize("streaming", [False, True])
async def test_cancellation_racing_response_delivery_releases_undelivered_response(
    server: Server, streaming: bool
) -> None:
    closing, release = anyio.Event(), anyio.Event()
    owner: asyncio.Task[httpx.Response] | None = None

    async def delivered(response: httpx.Response) -> None:
        if response.request.url.path == "/events":
            assert owner is not None
            owner.cancel("cancel response handoff")

    async def trace(name: str, info: dict[str, Any]) -> None:
        if name == "http11.response_closed.started":
            closing.set()
            await release.wait()

    async def adapt(request: httpx.Request) -> httpx.Request:
        request.extensions["trace"] = trace
        return request

    async with httpx.AsyncClient(
        base_url=server.url,
        trust_env=False,
        limits=httpx.Limits(max_connections=1),
        timeout=1,
        event_hooks={"response": [delivered]},
    ) as client:
        owner = asyncio.create_task(
            execute(HttpxTransport(client, request_adapter=adapt), streaming, RequestSpec("GET", "/events"))
        )
        try:
            with anyio.fail_after(3):
                await closing.wait()
                owner.cancel("second handoff cancellation")
                await anyio.sleep(0)
                assert not owner.done()
                release.set()
                with pytest.raises(asyncio.CancelledError) as failure:
                    await owner
                assert failure.value.args == ("cancel response handoff",)
                await server.disconnected.wait()
                assert (await client.get("/after")).status_code == 204
        finally:
            release.set()
            with suppress(asyncio.CancelledError):
                await owner


@pytest.mark.parametrize("operation", ["acquire", "refresh", "discovery"])
async def test_oauth_native_cleanup_preserves_cancellation_and_releases_pool(server: Server, operation: str) -> None:
    reading, closing, release = anyio.Event(), anyio.Event(), anyio.Event()

    async def trace(name: str, info: dict[str, Any]) -> None:
        if name == "http11.receive_response_body.started":
            reading.set()
        if name == "http11.response_closed.started":
            closing.set()
            await release.wait()

    async def prepare(request: httpx.Request) -> None:
        if request.url.path != "/after":
            request.extensions["trace"] = trace

    async with httpx.AsyncClient(
        base_url=server.url,
        trust_env=False,
        limits=httpx.Limits(max_connections=1),
        timeout=1,
        event_hooks={"request": [prepare]},
    ) as client:
        provider = HttpxOAuthTokenProvider(
            client,
            identity="test",
            client_id="test",
            client_secret="test",
            authentication="client_secret_basic",
            issuer=server.url,
        )
        request = TokenRequest(
            scheme="test",
            provider="test",
            flow="clientCredentials",
            client_identity="test",
            token_url=f"{server.url}/events",
            discovery_url=f"{server.url}/events" if operation == "discovery" else None,
            transport=SecurityTransport(location="header", name="Authorization", prefix="Bearer"),
        )
        task = asyncio.create_task(
            provider.refresh(request, "refresh") if operation == "refresh" else provider.acquire(request)
        )
        try:
            with anyio.fail_after(3):
                await reading.wait()
                task.cancel("original OAuth cancellation")
                await closing.wait()
                task.cancel("second OAuth cancellation")
                await anyio.sleep(0)
                assert not task.done()
                release.set()
                with pytest.raises(asyncio.CancelledError) as failure:
                    await task
                assert failure.value.args == ("original OAuth cancellation",)
                await server.disconnected.wait()
                assert (await client.get("/after")).status_code == 204
        finally:
            release.set()
            with suppress(asyncio.CancelledError):
                await task


async def test_buffered_body_collection_uses_one_owner_task(server: Server) -> None:
    owners: set[asyncio.Task[Any] | None] = set()
    chunks = 0

    class TrackedBody(httpx.AsyncByteStream):
        def __init__(self, stream: httpx.AsyncByteStream) -> None:
            self.stream = stream

        async def __aiter__(self) -> AsyncIterator[bytes]:
            nonlocal chunks
            async for chunk in self.stream:
                owners.add(asyncio.current_task())
                chunks += 1
                yield chunk

        async def aclose(self) -> None:
            await self.stream.aclose()

    async def adapt(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        assert isinstance(response.stream, httpx.AsyncByteStream)
        response.stream = TrackedBody(response.stream)
        return response

    async with httpx.AsyncClient(base_url=server.url, trust_env=False) as client:
        response = await HttpxTransport(client, response_adapter=adapt).execute_request(RequestSpec("GET", "/bulk"))
        assert response.content == b"x" * (1024 * 1024)
        assert chunks > 1 and len(owners) == 1


@pytest.mark.parametrize("error_type", [Stop, asyncio.CancelledError, ValueError])
async def test_transport_context_preserves_original_failure(error_type: type[BaseException]) -> None:
    original = error_type("original context failure")

    class FailingCloseTransport(HttpxTransport):
        async def _close(self) -> None:
            await super()._close()
            raise RuntimeError("native client cleanup failed")

    transport = FailingCloseTransport()
    with pytest.raises(error_type) as failure:
        async with transport:
            raise original
    assert failure.value is original
    assert transport.client.is_closed
    assert "Closing the HTTP transport also failed" in original.__notes__


@pytest.mark.parametrize("streaming", [False, True])
async def test_upload_cancellation_finishes_native_cleanup(server: Server, streaming: bool) -> None:
    uploading, closing, release = anyio.Event(), anyio.Event(), anyio.Event()

    async def content() -> AsyncIterator[bytes]:
        yield b"first"
        uploading.set()
        await anyio.sleep_forever()

    async def trace(name: str, info: dict[str, Any]) -> None:
        if name == "http11.response_closed.started":
            closing.set()
            await release.wait()

    async def adapt(request: httpx.Request) -> httpx.Request:
        request.extensions["trace"] = trace
        return request

    async with httpx.AsyncClient(
        base_url=server.url, trust_env=False, limits=httpx.Limits(max_connections=1), timeout=1
    ) as client:
        task = asyncio.create_task(
            execute(
                HttpxTransport(client, request_adapter=adapt),
                streaming,
                RequestSpec("POST", "/upload", body=StreamingBody(content)),
            )
        )
        try:
            with anyio.fail_after(3):
                await uploading.wait()
                task.cancel("original upload cancellation")
                await closing.wait()
                task.cancel("second upload cancellation")
                await anyio.sleep(0)
                assert not task.done()
                release.set()
                with pytest.raises(asyncio.CancelledError) as failure:
                    await task
                assert failure.value.args == ("original upload cancellation",)
                await server.disconnected.wait()
                assert (await client.get("/after")).status_code == 204
        finally:
            release.set()
            with suppress(asyncio.CancelledError):
                await task


async def test_shutdown_joins_an_independently_cancelled_source(server: Server) -> None:
    closing, release = anyio.Event(), anyio.Event()
    tasks: list[asyncio.Task[Any]] = []

    async def trace(name: str, info: dict[str, Any]) -> None:
        if name == "http11.response_closed.started":
            closing.set()
            await release.wait()

    async def adapt(request: httpx.Request) -> httpx.Request:
        request.extensions["trace"] = trace
        return request

    transport = HttpxTransport(base_url=server.url, request_adapter=adapt)
    source = transport.event_source(RequestSpec("GET", "/events"))

    def cancel_source(event: object) -> None:
        task = asyncio.current_task()
        assert task is not None
        tasks.append(task)
        task.cancel("source cancelled independently")

    source.on_message = cancel_source
    source.connect()
    shutdown: asyncio.Task[None] | None = None
    try:
        with anyio.fail_after(3):
            await closing.wait()
            shutdown = asyncio.create_task(transport.aclose())
            await anyio.sleep(0)
            assert not shutdown.done()
            release.set()
            await shutdown
            assert tasks[0].cancelled()
            assert transport.client.is_closed
            await server.disconnected.wait()
    finally:
        release.set()
        if shutdown is not None:
            await shutdown
        await transport.aclose()


@pytest.mark.parametrize("stage", ["headers", "response-adapter"])
@pytest.mark.parametrize("failure_kind", ["none", "task-cancel", "context-stop", "context-error", "context-cancel"])
async def test_stream_close_joins_attempt_before_response_handoff(
    server: Server, stage: str, failure_kind: str
) -> None:
    entered, closing, release, consumer_release = (anyio.Event() for _ in range(4))
    original = {
        "context-stop": Stop("original close-context stop"),
        "context-error": ValueError("original close-context error"),
        "context-cancel": asyncio.CancelledError("original close-context cancellation"),
    }.get(failure_kind)
    observed: list[asyncio.CancelledError] = []

    async def trace(name: str, info: dict[str, Any]) -> None:
        if stage == "headers" and name == "http11.receive_response_headers.started":
            entered.set()
        if name == "http11.response_closed.started":
            closing.set()
            await release.wait()

    async def adapt_request(request: httpx.Request) -> httpx.Request:
        request.extensions["trace"] = trace
        return request

    async def adapt_response(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        if stage == "response-adapter":
            entered.set()
            await anyio.sleep_forever()
        return response

    async with httpx.AsyncClient(
        base_url=server.url, trust_env=False, limits=httpx.Limits(max_connections=1), timeout=1
    ) as client:
        transport = HttpxTransport(client, request_adapter=adapt_request, response_adapter=adapt_response)
        stream = transport.event_stream(
            RequestSpec("GET", "/headers" if stage == "headers" else "/events"), lambda event: event.data
        )

        async def consume() -> None:
            assert [event async for event in stream] == []
            # Closure must finish without joining application work after iteration.
            await consumer_release.wait()

        async def close() -> None:
            if original is not None:
                async with stream:
                    raise original
            else:
                try:
                    await stream.aclose()
                except asyncio.CancelledError as error:
                    observed.append(error)
                    raise

        consumer = asyncio.create_task(consume())
        closers: list[asyncio.Task[None]] = []
        try:
            with anyio.fail_after(3):
                await entered.wait()
                closers = [asyncio.create_task(close()), asyncio.create_task(stream.aclose())]
                await closing.wait()
                if failure_kind != "none":
                    closers[0].cancel("original close cancellation")
                    await anyio.sleep(0)
                    closers[0].cancel("later close cancellation")
                await anyio.sleep(0)
                assert all(not task.done() for task in closers)
                release.set()
                if original is not None:
                    with pytest.raises(type(original)) as failure:
                        await closers[0]
                    assert failure.value is original
                elif failure_kind == "task-cancel":
                    with pytest.raises(asyncio.CancelledError) as failure:
                        await closers[0]
                    assert failure.value is observed[0]
                    assert failure.value.args == ("original close cancellation",)
                else:
                    await closers[0]
                await closers[1]
                # Prove cleanup via the borrowed pool before awaiting the consumer.
                assert not consumer.done()
                assert (await client.get("/after")).status_code == 204
                await server.disconnected.wait()
                consumer_release.set()
                await consumer
        finally:
            release.set()
            consumer_release.set()
            await asyncio.gather(consumer, *closers, return_exceptions=True)


async def test_close_during_response_adaptation_preserves_consumer_stop(server: Server) -> None:
    entered, closing, release = anyio.Event(), anyio.Event(), anyio.Event()
    original = Stop("original adapter stop")

    async def trace(name: str, info: dict[str, Any]) -> None:
        if name == "http11.response_closed.started":
            closing.set()
            await release.wait()

    async def adapt_request(request: httpx.Request) -> httpx.Request:
        request.extensions["trace"] = trace
        return request

    async def adapt_response(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        entered.set()
        try:
            await anyio.sleep_forever()
        except asyncio.CancelledError:
            raise original from None
        raise AssertionError("Adapter must be interrupted")

    async with httpx.AsyncClient(
        base_url=server.url, trust_env=False, limits=httpx.Limits(max_connections=1), timeout=1
    ) as client:
        stream = HttpxTransport(client, request_adapter=adapt_request, response_adapter=adapt_response).event_stream(
            RequestSpec("GET", "/events"), lambda event: event.data
        )

        async def consume() -> None:
            async with stream:
                async for _event in stream:
                    raise AssertionError("Adapter must fail before delivery")

        consumer = asyncio.create_task(consume())
        closer: asyncio.Task[None] | None = None
        try:
            with anyio.fail_after(3):
                await entered.wait()
                closer = asyncio.create_task(stream.aclose())
                await closing.wait()
                assert not closer.done()
                release.set()
                await closer
                assert (await client.get("/after")).status_code == 204
                with pytest.raises(Stop) as failure:
                    await consumer
                assert failure.value is original
        finally:
            release.set()
            await asyncio.gather(consumer, *([closer] if closer is not None else []), return_exceptions=True)


@pytest.mark.parametrize("stage", ["request-adapter", "response-adapter", "native-hook"])
async def test_close_from_own_adapter_or_hook_does_not_join_itself(server: Server, stage: str) -> None:
    async def adapt_request(request: httpx.Request) -> httpx.Request:
        if stage == "request-adapter":
            await stream.aclose()
        return request

    async def adapt_response(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        if stage == "response-adapter":
            await stream.aclose()
        return response

    async def native_hook(response: httpx.Response) -> None:
        if stage == "native-hook" and response.request.url.path == "/events":
            await stream.aclose()

    async with httpx.AsyncClient(
        base_url=server.url,
        trust_env=False,
        limits=httpx.Limits(max_connections=1),
        timeout=1,
        event_hooks={"response": [native_hook]},
    ) as client:
        stream = HttpxTransport(client, request_adapter=adapt_request, response_adapter=adapt_response).event_stream(
            RequestSpec("GET", "/events"), lambda event: event.data
        )
        with anyio.fail_after(3):
            async with stream:
                assert [event async for event in stream] == []
            assert (await client.get("/after")).status_code == 204


@pytest.mark.parametrize("failure_kind", ["none", "stop", "cancellation"])
async def test_decode_uses_collected_response_and_preserves_error_identity(server: Server, failure_kind: str) -> None:
    native: list[httpx.Response] = []
    original = {
        "stop": Stop("original decoder stop"),
        "cancellation": asyncio.CancelledError("original decoder cancellation"),
    }.get(failure_kind)

    async def capture(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        native.append(response)
        return response

    def decode(value: object) -> int:
        assert value == b"x" * (1024 * 1024)
        assert native[0].is_closed and native[0].is_stream_consumed
        if original is not None:
            raise original
        return len(value) if isinstance(value, bytes) else 0

    async with httpx.AsyncClient(
        base_url=server.url, trust_env=False, limits=httpx.Limits(max_connections=1), timeout=1
    ) as client:
        transport = HttpxTransport(client, response_adapter=capture)
        operation: OperationSpec[None, int] = OperationSpec(
            RequestSpec("GET", "/bulk"),
            (ResponseSpec(200, (MediaType("application/octet-stream"),), decode, binary_body=True),),
        )
        if original is None:
            result = await transport.response(operation)
            assert result.result == 1024 * 1024
        else:
            with pytest.raises(type(original)) as failure:
                await transport.response(operation)
            assert failure.value is original
        assert (await client.get("/after")).status_code == 204


@pytest.mark.parametrize("stage", ["request-adapter", "response-adapter", "request-hook", "response-hook"])
@pytest.mark.parametrize("external_first", [False, True])
@pytest.mark.parametrize("failure_kind", ["none", "stop"])
@pytest.mark.parametrize("owned", [False, True])
async def test_reentrant_transport_shutdown_joins_from_external_callers(
    server: Server, stage: str, external_first: bool, failure_kind: str, owned: bool
) -> None:
    opened, entered, proceed, returned, closing, release = (anyio.Event() for _ in range(6))
    original = Stop("original shutdown adapter stop") if failure_kind == "stop" else None
    errors: list[BaseException | None] = []
    cancellations: list[asyncio.CancelledError] = []

    async def shutdown_from_callback() -> None:
        # Cleanup callbacks may finish under a shield after external shutdown has
        # interrupted the attempt. They must not join the run they belong to.
        with anyio.CancelScope(shield=True):
            entered.set()
            await proceed.wait()
            await transport.aclose()
            returned.set()
            if original is not None:
                raise original

    async def trace(name: str, info: dict[str, Any]) -> None:
        if name == "http11.response_closed.started":
            closing.set()
            await release.wait()

    async def adapt_request(request: httpx.Request) -> httpx.Request:
        if request.url.path == "/events":
            request.extensions["trace"] = trace
        elif stage == "request-adapter":
            await shutdown_from_callback()
        return request

    async def adapt_response(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/shutdown" and stage == "response-adapter":
            await shutdown_from_callback()
        return response

    async def request_hook(request: httpx.Request) -> None:
        if request.url.path == "/shutdown" and stage == "request-hook":
            await shutdown_from_callback()

    async def response_hook(response: httpx.Response) -> None:
        if response.request.url.path == "/shutdown" and stage == "response-hook":
            await shutdown_from_callback()

    async def shutdown_externally() -> None:
        try:
            await transport.aclose()
        except asyncio.CancelledError as error:
            cancellations.append(error)
            raise

    async with httpx.AsyncClient(base_url=server.url, trust_env=False, timeout=1) as client:
        transport = HttpxTransport(
            None if owned else client,
            base_url=server.url if owned else None,
            request_adapter=adapt_request,
            response_adapter=adapt_response,
        )
        transport.client.event_hooks = {"request": [request_hook], "response": [response_hook]}
        peer = transport.event_source(RequestSpec("GET", "/events"))
        peer.on_message = lambda event: opened.set()
        peer.connect()
        source = transport.event_source(RequestSpec("GET", "/shutdown"))
        source.on_error = errors.append
        closers: list[asyncio.Task[None]] = []
        run: asyncio.Task[None] | None = None
        try:
            with anyio.fail_after(3):
                # Even request-adapter shutdown must release a real active socket.
                await opened.wait()
                source.connect()
                (run,) = source._tasks
                await entered.wait()
                if external_first:
                    closers = [asyncio.create_task(shutdown_externally()), asyncio.create_task(transport.aclose())]
                    await closing.wait()
                proceed.set()
                await returned.wait()
                if not external_first:
                    await closing.wait()
                    closers = [asyncio.create_task(shutdown_externally()), asyncio.create_task(transport.aclose())]
                await anyio.sleep(0)
                closers[0].cancel("first external shutdown cancellation")
                await anyio.sleep(0)
                closers[0].cancel("later external shutdown cancellation")
                await anyio.sleep(0)
                assert all(not closer.done() for closer in closers)
                assert not transport.client.is_closed
                release.set()
                with pytest.raises(asyncio.CancelledError) as failure:
                    await closers[0]
                assert failure.value is cancellations[0]
                assert failure.value.args == ("first external shutdown cancellation",)
                await closers[1]
                assert run.done()
                run.result()
                assert errors == ([] if original is None else [original])
                if original is not None:
                    assert errors[0] is original
                await server.disconnected.wait()
                assert not peer._tasks and not source._tasks
                assert transport.client.is_closed is owned
                if not owned:
                    assert (await client.get("/after")).status_code == 204
        finally:
            proceed.set()
            release.set()
            await asyncio.gather(*closers, return_exceptions=True)
            await transport.aclose()


@pytest.mark.parametrize("stage", ["request-adapter", "response-adapter", "request-hook", "response-hook"])
async def test_retiring_event_source_can_request_transport_shutdown(server: Server, stage: str) -> None:
    entered, proceed, returned, replacement_opened = (anyio.Event() for _ in range(4))
    attempts = 0

    async def shutdown() -> None:
        with anyio.CancelScope(shield=True):
            entered.set()
            await proceed.wait()
            await transport.aclose()
            returned.set()

    async def adapt_request(request: httpx.Request) -> httpx.Request:
        if request.url.path == "/retiring" and stage == "request-adapter":
            await shutdown()
        return request

    async def adapt_response(response: httpx.Response, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/retiring" and stage == "response-adapter":
            await shutdown()
        return response

    async def request_hook(request: httpx.Request) -> None:
        if request.url.path == "/retiring" and stage == "request-hook":
            await shutdown()

    async def response_hook(response: httpx.Response) -> None:
        if response.request.url.path == "/retiring" and stage == "response-hook":
            await shutdown()

    async with httpx.AsyncClient(
        base_url=server.url,
        trust_env=False,
        timeout=1,
        event_hooks={"request": [request_hook], "response": [response_hook]},
    ) as client:
        transport = HttpxTransport(client, request_adapter=adapt_request, response_adapter=adapt_response)

        def factory(headers: tuple[tuple[str, str], ...]) -> httpx.Request:
            nonlocal attempts
            attempts += 1
            return client.build_request("GET", "/retiring" if attempts == 1 else "/events", headers=headers)

        source = transport.event_source_from(factory)
        source.on_message = lambda event: replacement_opened.set()
        source.connect()
        try:
            with anyio.fail_after(3):
                await entered.wait()
                source.close()
                source.connect()
                await replacement_opened.wait()
                assert len(source._tasks) == 2
                proceed.set()
                await returned.wait()
                await transport.aclose()
                assert not source._tasks
                assert source.ready_state is EventSourceState.CLOSED
                assert attempts == 2
                assert (await client.get("/after")).status_code == 204
        finally:
            proceed.set()
            await transport.aclose()
