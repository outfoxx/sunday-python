# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Callable
from contextlib import suppress
from types import TracebackType
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import anyio
import httpx

from ..errors import TransportError, UnexpectedResponse
from ..event_source import EventSourceErrorHandler, EventSourceMessageHandler, EventSourceOpenHandler, EventSourceState
from ..media import MediaType
from ..specs import RequestSpec
from ..sse import EventParser, EventStreamOptions, ServerSentEvent
from ._reconnect import _ReconnectPolicy

if TYPE_CHECKING:
    from ._transport import HttpxEventSourceRequestFactory, HttpxTransport

_KEEPALIVE_TIMEOUT_FLOOR = 1.0


class HttpxEventStream[EventT]:
    """Reconnectable HTTPX server-sent event stream."""

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
        self._cancel_scope: anyio.CancelScope | None = None
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
        del exc_type, exc_value, traceback
        await self.aclose()

    async def aclose(self) -> None:
        """Close the active response and interrupt reads or retry waits."""
        self._close_nowait()
        if self._response is not None:
            with anyio.CancelScope(shield=True):
                await self._response.aclose()

    def _close_nowait(self) -> None:
        self._closed = True
        if self._close_event is not None:
            self._close_event.set()
        if self._cancel_scope is not None:
            self._cancel_scope.cancel()

    async def events(self) -> AsyncIterator[EventT]:
        """Connect, reconnect, and yield decoded server-sent events."""
        if self._running:
            raise TransportError("An event stream supports only one active consumer")
        self._running = True
        self._close_event = anyio.Event()
        reconnect = _ReconnectPolicy(self._options.retry, self._options.retry_max)
        last_event_id: str | None = None

        try:
            with anyio.CancelScope() as cancel_scope:
                self._cancel_scope = cancel_scope
                while not self._closed:
                    response: httpx.Response | None = None
                    connected = False
                    try:
                        headers = [("Accept", "text/event-stream"), ("Cache-Control", "no-store")]
                        if last_event_id is not None:
                            headers.append(("Last-Event-ID", last_event_id))
                        if self._request_factory is None:
                            request = self._transport._build_request(self._spec.with_headers(*headers))
                        else:
                            request_value = self._request_factory(tuple(headers))
                            request = await request_value if inspect.isawaitable(request_value) else request_value
                        request = await self._transport._adapt_request(request)
                        self._disable_httpx_read_timeout(request)
                        response = await self._transport._send_event_stream(request)
                        self._response = response

                        if response.status_code == 204:
                            return
                        if not 200 <= response.status_code < 300:
                            body = await response.aread()
                            self._transport.raise_problem(response, body)
                        self._validate_content_type(response)
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
                                    event,
                                    reconnect,
                                    timeout_state[0],
                                    last_event_id,
                                )
                                if event.data is not None:
                                    decoded = self._decoder(event)
                                    if decoded is not None:
                                        yield decoded
                        for event in parser.finalize():
                            timeout_state[0], last_event_id = self._apply_controls(
                                event,
                                reconnect,
                                timeout_state[0],
                                last_event_id,
                            )
                            if event.data is not None:
                                decoded = self._decoder(event)
                                if decoded is not None:
                                    yield decoded
                    except (httpx.TransportError, TimeoutError) as error:
                        if self._closed:
                            return
                        if self._on_error is not None:
                            self._on_error(error)
                    finally:
                        if response is not None:
                            with anyio.CancelScope(shield=True):
                                await response.aclose()
                        self._response = None

                    if self._closed:
                        return
                    if self._on_connecting is not None:
                        self._on_connecting()
                    delay = reconnect.next_delay(failed=not connected)
                    await self._wait_for_retry(delay)
        finally:
            self._cancel_scope = None
            self._close_event = None
            self._running = False

    async def _chunks(
        self,
        response: httpx.Response,
        timeout: Callable[[], float | None],
    ) -> AsyncIterator[bytes]:
        iterator = response.aiter_bytes().__aiter__()
        while True:
            try:
                current_timeout = timeout()
                if current_timeout is None:
                    chunk = await anext(iterator)
                else:
                    with anyio.fail_after(current_timeout):
                        chunk = await anext(iterator)
            except StopAsyncIteration:
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
    ) -> None:
        self._transport = transport
        self._request_factory = request_factory
        self._options = options or EventStreamOptions()
        self._ready_state = EventSourceState.CLOSED
        self._stream: HttpxEventStream[ServerSentEvent] | None = None
        self._task: asyncio.Task[None] | None = None
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
        self._stream = HttpxEventStream(
            self._transport,
            RequestSpec("GET", "/"),
            lambda event: event,
            options=self._options,
            request_factory=self._request_factory,
            on_open=self._opened,
            on_error=self._retrying,
            on_connecting=self._connecting,
        )
        self._task = loop.create_task(self._run(), name="sunday-httpx-event-source")

    def close(self) -> None:
        """Close the source and interrupt active reads or reconnect waits."""
        self._ready_state = EventSourceState.CLOSED
        if self._stream is not None:
            self._stream._close_nowait()
        if self._task is not None:
            self._task.cancel()
        else:
            self._transport._event_source_closed(self)

    async def _wait_closed(self) -> None:
        task = self._task
        if task is not None:
            with suppress(asyncio.CancelledError):
                await task

    async def _run(self) -> None:
        try:
            assert self._stream is not None
            async for event in self._stream:
                self._dispatch(event)
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            self._invoke(self.on_error, error)
        finally:
            if self._stream is not None:
                await self._stream.aclose()
            self._ready_state = EventSourceState.CLOSED
            self._transport._event_source_closed(self)

    def _opened(self) -> None:
        self._ready_state = EventSourceState.OPEN
        self._invoke(self.on_open)

    def _retrying(self, error: BaseException) -> None:
        self._ready_state = EventSourceState.CONNECTING
        self._invoke(self.on_error, error)

    def _connecting(self) -> None:
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
