# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from typing import TYPE_CHECKING

import anyio
import httpx

from .errors import UnexpectedResponse
from .media import MediaType
from .specs import RequestSpec
from .sse import EventParser, EventStreamOptions, ServerSentEvent

if TYPE_CHECKING:
    from .httpx import HttpxTransport


class HttpxEventStream[EventT]:
    """Reconnectable HTTPX server-sent event stream."""

    def __init__(
        self,
        transport: HttpxTransport,
        spec: RequestSpec[None],
        decoder: Callable[[ServerSentEvent], EventT],
        *,
        options: EventStreamOptions | None = None,
    ) -> None:
        self._transport = transport
        self._spec = spec
        self._decoder = decoder
        self._options = options or EventStreamOptions()
        self._closed = False
        self._running = False
        self._response: httpx.Response | None = None
        self._cancel_scope: anyio.CancelScope | None = None
        self._close_event: anyio.Event | None = None

    def __aiter__(self) -> AsyncIterator[EventT]:
        return self.events()

    async def aclose(self) -> None:
        """Close the active response and interrupt reads or retry waits."""
        self._closed = True
        if self._close_event is not None:
            self._close_event.set()
        if self._cancel_scope is not None:
            self._cancel_scope.cancel()
        if self._response is not None:
            with anyio.CancelScope(shield=True):
                await self._response.aclose()

    async def events(self) -> AsyncIterator[EventT]:
        """Connect, reconnect, and yield decoded server-sent events."""
        if self._running:
            raise RuntimeError("An event stream supports only one active consumer")
        self._running = True
        self._close_event = anyio.Event()
        retry = self._options.retry
        retry_max = max(retry, self._options.retry_max)
        retry_attempt = 0
        last_event_id: str | None = None
        event_timeout = self._options.event_timeout

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
                        request = await self._transport.build_request(self._spec.with_headers(*headers))
                        response = await self._transport.send(request, stream=True)
                        self._response = response

                        if response.status_code == 204:
                            return
                        if not 200 <= response.status_code < 300:
                            body = await response.aread()
                            self._transport.raise_problem(response, body)
                        self._validate_content_type(response)
                        connected = True
                        retry_attempt = 0

                        parser = EventParser()
                        timeout_state = [event_timeout]

                        def current_timeout(state: list[float | None] = timeout_state) -> float | None:
                            return state[0]

                        async for chunk in self._chunks(response, current_timeout):
                            for event in parser.feed(chunk):
                                retry, retry_max, event_timeout, last_event_id = self._apply_controls(
                                    event,
                                    retry,
                                    retry_max,
                                    event_timeout,
                                    last_event_id,
                                )
                                timeout_state[0] = event_timeout
                                if event.data is not None:
                                    yield self._decoder(event)
                        for event in parser.finalize():
                            retry, retry_max, event_timeout, last_event_id = self._apply_controls(
                                event,
                                retry,
                                retry_max,
                                event_timeout,
                                last_event_id,
                            )
                            if event.data is not None:
                                yield self._decoder(event)
                    except (httpx.TransportError, TimeoutError):
                        if self._closed:
                            return
                    finally:
                        if response is not None:
                            with anyio.CancelScope(shield=True):
                                await response.aclose()
                        self._response = None

                    if self._closed:
                        return
                    delay = min(retry * (2**retry_attempt), retry_max)
                    retry_attempt = 0 if connected else retry_attempt + 1
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
        retry: float,
        retry_max: float,
        event_timeout: float | None,
        last_event_id: str | None,
    ) -> tuple[float, float, float | None, str | None]:
        if event.retry is not None:
            retry = event.retry / 1000
        if event.retry_max is not None:
            retry_max = max(retry, event.retry_max / 1000)
        if self._options.event_timeout is None and event.keepalive is not None:
            event_timeout = None if event.keepalive == 0 else max(event.keepalive * 3 / 1000, 1.0)
        if event.id is not None:
            last_event_id = event.id or None
        return retry, retry_max, event_timeout, last_event_id
