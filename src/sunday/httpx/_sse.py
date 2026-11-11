# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import asyncio
import inspect
import sys
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from types import TracebackType
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import anyio
import httpx

from ..errors import TransportError, UnexpectedResponse
from ..event_source import EventSourceErrorHandler, EventSourceMessageHandler, EventSourceOpenHandler, EventSourceState
from ..media import MediaType
from ..request_cancellation_error import RequestCancellationError
from ..specs import RequestSpec
from ..sse import EventParser, EventStreamOptions, ServerSentEvent
from ._async import complete, run_io
from ._reconnect import _ReconnectPolicy
from ._response_cleanup import close_response, in_response_cleanup
from .request_security_bindings import _set_request_security_bindings

if TYPE_CHECKING:
    from ._transport import HttpxEventSourceRequestFactory, HttpxTransport

_KEEPALIVE_TIMEOUT_FLOOR = 1.0
_io_owners: ContextVar[tuple[HttpxEventStream._IO, ...]] = ContextVar("sunday_event_stream_io_owners", default=())


class HttpxEventStream[EventT]:
    """Reconnectable HTTPX server-sent event stream."""

    @dataclass(frozen=True, slots=True)
    class _IO:
        """Completion of an I/O block, independent of the consumer's lifetime."""

        scope: anyio.CancelScope
        done: anyio.Event = field(default_factory=anyio.Event)

    def __init__(
        self,
        transport: HttpxTransport,
        spec: RequestSpec[None],
        decoder: Callable[[ServerSentEvent], EventT | None],
        *,
        options: EventStreamOptions | None = None,
        request_factory: HttpxEventSourceRequestFactory | None = None,
        on_open: Callable[[], None] | None = None,
        on_error: Callable[[BaseException], None] | None = None,
        on_connecting: Callable[[], None] | None = None,
    ) -> None:
        self._transport = transport
        self._spec = spec
        self._decoder = decoder
        self._options = options or EventStreamOptions()
        self._request_factory = request_factory
        self._on_open = on_open
        self._on_error = on_error
        self._on_connecting = on_connecting
        self._closed = False
        self._running = False
        self._response: httpx.Response | None = None
        self._active_io: HttpxEventStream._IO | None = None
        self._close_event: anyio.Event | None = None
        self._retry_time = self._options.retry

    @property
    def retry_time(self) -> float:
        """Return the current initial reconnect delay in seconds."""
        return self._retry_time

    def __aiter__(self) -> AsyncIterator[EventT]:
        return self.events()

    async def __aenter__(self) -> HttpxEventStream[EventT]:
        """Enter an asynchronous event-stream lifecycle scope."""
        if self._closed:
            raise TransportError("HttpxEventStream is closed")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the event stream when leaving its lifecycle scope."""
        del exc_type, traceback
        await self._close(exc_value)

    async def aclose(self) -> None:
        """Interrupt active work and wait for its I/O and response cleanup.

        A call from within this stream's active adapter or native hook requests
        closure without waiting on itself; external callers join completion.
        """
        await self._close()

    async def _close(self, error: BaseException | None = None) -> None:
        self._close_nowait()
        active = self._active_io
        # Callbacks cannot join the I/O or cleanup waiting for them to return.
        # Keep the response reference so external callers still join its cleanup.
        if self._owns_current_work():
            return
        try:
            if active is None:
                await self._release_response()
            else:
                await complete(asyncio.create_task(self._finish_close(active)))
        except BaseException:
            if error is None:
                raise
            error.add_note("Closing the HTTP response also failed")

    def _owns_current_work(self) -> bool:
        return (self._active_io is not None and any(owner is self._active_io for owner in _io_owners.get())) or (
            self._response is not None and in_response_cleanup(self._response)
        )

    async def _finish_close(self, active: _IO) -> None:
        await active.done.wait()
        await self._release_response()

    async def _release_response(self, error: BaseException | None = None) -> None:
        response = self._response
        if response is not None:
            try:
                await close_response(response, error)
            finally:
                if self._response is response:
                    self._response = None

    @contextmanager
    def _interruptible(self) -> Iterator[None]:
        """Interrupt active I/O without leaving a cancel scope around a yielded event."""
        with anyio.CancelScope() as scope:
            active = self._IO(scope)
            self._active_io = active
            token = _io_owners.set((*_io_owners.get(), active))
            try:
                if self._closed:
                    scope.cancel()
                yield
            finally:
                _io_owners.reset(token)
                self._active_io = None
                active.done.set()

    def _close_nowait(self) -> None:
        self._closed = True
        if self._close_event is not None:
            self._close_event.set()
        if self._active_io is not None:
            self._active_io.scope.cancel()

    async def events(self) -> AsyncGenerator[EventT, None]:
        """Connect, reconnect, and yield decoded server-sent events."""
        if self._running:
            raise TransportError("An event stream supports only one active consumer")
        self._running = True
        self._close_event = anyio.Event()
        reconnect = _ReconnectPolicy(self._options.retry, self._options.retry_max)
        last_event_id: str | None = None

        try:
            while not self._closed:
                response: httpx.Response | None = None
                connected = False
                try:
                    with self._interruptible():
                        headers = [("Accept", "text/event-stream"), ("Cache-Control", "no-store")]
                        if last_event_id is not None:
                            headers.append(("Last-Event-ID", last_event_id))
                        if self._request_factory is None:
                            request = self._transport._build_request(self._spec.with_headers(*headers))
                        else:
                            request_value = self._request_factory(tuple(headers))
                            request = await request_value if inspect.isawaitable(request_value) else request_value
                            if self._spec.security is not None:
                                _set_request_security_bindings(request, self._spec.security)
                        self._disable_httpx_read_timeout(request)
                        response = await self._transport._send_event_stream(
                            request,
                        )
                        self._response = response

                        if response.status_code == 204:
                            return
                        if not 200 <= response.status_code < 300:
                            body = await run_io(response.aread)
                            self._transport.raise_problem(response, body)
                        self._validate_content_type(response)
                    if self._closed:
                        return
                    connected = True
                    reconnect.opened()
                    if self._on_open is not None:
                        self._on_open()

                    parser = EventParser()
                    timeout_state: list[float | None] = [None]

                    def current_timeout(state: list[float | None] = timeout_state) -> float | None:
                        return state[0]

                    async for chunk in self._chunks(response, current_timeout):
                        for event in parser.feed(chunk):
                            timeout_state[0], last_event_id = self._apply_controls(
                                event, reconnect, timeout_state[0], last_event_id
                            )
                            if event.data is not None:
                                decoded = self._decoder(event)
                                if decoded is not None:
                                    yield decoded
                                    if self._closed:
                                        return
                    if self._closed:
                        return
                    for event in parser.finalize():
                        timeout_state[0], last_event_id = self._apply_controls(
                            event, reconnect, timeout_state[0], last_event_id
                        )
                        if event.data is not None:
                            decoded = self._decoder(event)
                            if decoded is not None:
                                yield decoded
                                if self._closed:
                                    return
                except RequestCancellationError:
                    raise
                except (httpx.TransportError, TimeoutError) as error:
                    if self._closed:
                        return
                    if self._on_error is not None:
                        self._on_error(error)
                finally:
                    await self._release_response(sys.exception())

                if self._closed:
                    return
                if self._on_connecting is not None:
                    self._on_connecting()
                delay = reconnect.next_delay(failed=not connected)
                await self._wait_for_retry(delay)
        finally:
            self._active_io = None
            self._close_event = None
            self._running = False

    async def _chunks(
        self,
        response: httpx.Response,
        timeout: Callable[[], float | None],
    ) -> AsyncIterator[bytes]:
        iterator = response.aiter_bytes().__aiter__()
        while not self._closed:
            try:
                with self._interruptible():
                    current_timeout = timeout()
                    if current_timeout is None:
                        chunk = await run_io(lambda: anext(iterator))
                    else:
                        with anyio.fail_after(current_timeout):
                            chunk = await run_io(lambda: anext(iterator))
            except StopAsyncIteration:
                return
            if self._closed:
                return
            yield chunk

    async def _wait_for_retry(self, delay: float) -> None:
        if self._close_event is None:
            return
        with anyio.move_on_after(delay):
            await self._close_event.wait()

    def _validate_content_type(self, response: httpx.Response) -> None:
        value = response.headers.get("content-type")
        try:
            content_type = MediaType(value) if value is not None else None
        except ValueError:
            content_type = None
        if content_type is None or not MediaType("text/event-stream").matches(content_type):
            raise UnexpectedResponse(
                "Event stream response is not text/event-stream",
                status=response.status_code,
                content_type=value,
                transport_response=response,
            )

    def _apply_controls(
        self,
        event: ServerSentEvent,
        reconnect: _ReconnectPolicy,
        event_timeout: float | None,
        last_event_id: str | None,
    ) -> tuple[float | None, str | None]:
        if event.retry is not None:
            reconnect.update_retry(event.retry / 1000)
            self._retry_time = reconnect.retry
        if event.retry_max is not None:
            reconnect.update_retry_max(event.retry_max / 1000)
        if event.keepalive is not None and event.keepalive > 0:
            event_timeout = max(event.keepalive * 3 / 1000, _KEEPALIVE_TIMEOUT_FLOOR)
        if event.id is not None:
            last_event_id = event.id or None
        return event_timeout, last_event_id

    def _disable_httpx_read_timeout(self, request: httpx.Request) -> None:
        timeout = request.extensions.get("timeout")
        values = dict(timeout) if isinstance(timeout, dict) else self._transport.client.timeout.as_dict()
        values["read"] = None
        request.extensions = {**request.extensions, "timeout": values}


class HttpxEventSource:
    """Callback-oriented HTTPX server-sent event source."""

    def __init__(
        self,
        transport: HttpxTransport,
        request_factory: HttpxEventSourceRequestFactory,
        *,
        options: EventStreamOptions | None = None,
        spec: RequestSpec[None] | None = None,
    ) -> None:
        self._transport = transport
        self._request_factory = request_factory
        self._options = options or EventStreamOptions()
        self._spec = spec or RequestSpec("GET", "/")
        self._ready_state = EventSourceState.CLOSED
        self._stream: HttpxEventStream[ServerSentEvent] | None = None
        self._tasks: dict[asyncio.Task[None], HttpxEventStream[ServerSentEvent]] = {}
        self._listeners: dict[str, dict[UUID, EventSourceMessageHandler]] = {}
        self.on_open: EventSourceOpenHandler | None = None
        self.on_error: EventSourceErrorHandler | None = None
        self.on_message: EventSourceMessageHandler | None = None

    @property
    def ready_state(self) -> EventSourceState:
        """Return the current connection state."""
        return self._ready_state

    @property
    def retry_time(self) -> float:
        """Return the current initial reconnect delay in seconds."""
        return self._stream.retry_time if self._stream is not None else self._options.retry

    def add_event_listener(self, event: str, handler: EventSourceMessageHandler) -> UUID:
        """Register a handler for one event type and return its listener token."""
        listener = uuid4()
        self._listeners.setdefault(event, {})[listener] = handler
        return listener

    def remove_event_listener(self, event: str, listener: UUID) -> None:
        """Remove a previously registered event listener."""
        handlers = self._listeners.get(event)
        if handlers is None:
            return
        handlers.pop(listener, None)
        if not handlers:
            self._listeners.pop(event, None)

    def connect(self) -> None:
        """Begin connecting without blocking the calling event loop."""
        if self._ready_state is not EventSourceState.CLOSED:
            return
        loop = asyncio.get_running_loop()
        if self._transport._closed:
            raise TransportError("HttpxTransport is closed")
        self._ready_state = EventSourceState.CONNECTING
        self._transport._event_source_started(self)
        stream = HttpxEventStream(
            self._transport,
            self._spec,
            lambda event: event,
            options=self._options,
            request_factory=self._request_factory,
            on_open=lambda: self._opened(stream),
            on_error=lambda error: self._retrying(stream, error),
            on_connecting=lambda: self._connecting(stream),
        )
        self._stream = stream
        task = loop.create_task(self._run(stream), name="sunday-httpx-event-source")
        self._tasks[task] = stream
        task.add_done_callback(self._finished)

    def close(self) -> None:
        """Close the source and interrupt active reads or reconnect waits."""
        self._ready_state = EventSourceState.CLOSED
        if self._stream is not None:
            self._stream._close_nowait()
        if not self._tasks:
            self._transport._event_source_closed(self)

    def _owns_current_work(self) -> bool:
        # Cleanup can outlive the I/O block. Retain each run's stream so callbacks
        # from retiring runs are recognized as well as the current run's callbacks.
        return any(stream._owns_current_work() for stream in self._tasks.values())

    async def _wait_closed(self) -> None:
        if self._tasks:
            await asyncio.wait(tuple(self._tasks))

    async def _run(self, stream: HttpxEventStream[ServerSentEvent]) -> None:
        try:
            # Register the run before callbacks can request shutdown, even with an eager task factory.
            await asyncio.sleep(0)
            if not stream._closed:
                async with stream:
                    async for event in stream:
                        if self._stream is stream:
                            self._dispatch(event)
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            if self._stream is stream:
                self._invoke(self.on_error, error)
        finally:
            if self._stream is stream:
                self._ready_state = EventSourceState.CLOSED

    def _finished(self, task: asyncio.Task[None]) -> None:
        # Retiring runs still own resources even after a replacement has connected.
        self._tasks.pop(task, None)
        if not self._tasks:
            self._transport._event_source_closed(self)

    def _opened(self, stream: HttpxEventStream[ServerSentEvent]) -> None:
        if self._stream is stream:
            self._ready_state = EventSourceState.OPEN
            self._invoke(self.on_open)

    def _retrying(self, stream: HttpxEventStream[ServerSentEvent], error: BaseException) -> None:
        if self._stream is stream:
            self._ready_state = EventSourceState.CONNECTING
            self._invoke(self.on_error, error)

    def _connecting(self, stream: HttpxEventStream[ServerSentEvent]) -> None:
        if self._stream is stream:
            self._ready_state = EventSourceState.CONNECTING

    def _dispatch(self, event: ServerSentEvent) -> None:
        self._invoke(self.on_message, event)
        if event.event is not None:
            for handler in tuple(self._listeners.get(event.event, {}).values()):
                self._invoke(handler, event)

    @staticmethod
    def _invoke(handler: Callable[..., None] | None, *args: object) -> None:
        if handler is None:
            return
        try:
            handler(*args)
        except BaseException as error:
            asyncio.get_running_loop().call_exception_handler(
                {
                    "message": "Sunday EventSource callback failed",
                    "exception": error,
                    "callback": handler,
                }
            )
