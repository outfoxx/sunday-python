# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable, Sequence
from types import TracebackType
from typing import Any, Protocol, cast

from .event_source import EventSource
from .operations import OperationResponse
from .problems import Problem
from .specs import OperationSpec, RequestSpec, ResponseSpec
from .sse import EventStreamOptions, ServerSentEvent


class ProblemRegistrar(Protocol):
    """Registers typed problem exceptions for response decoding."""

    def register_problem(self, type_uri: str, problem_type: type[Problem]) -> None:
        """Register ``problem_type`` for the given problem type URI."""
        ...


class EventStream[EventT](Protocol):
    """Transport-neutral asynchronous server-sent event stream."""

    def __aiter__(self) -> AsyncIterator[EventT]:
        """Iterate decoded events."""
        ...

    async def aclose(self) -> None:
        """Close the stream and release its active response."""
        ...

    async def __aenter__(self) -> EventStream[EventT]:
        """Enter an asynchronous stream lifecycle scope."""
        ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release stream resources when leaving a lifecycle scope."""
        ...


class Transport[TransportRequestT, TransportResponseT](ProblemRegistrar, Protocol):
    """Transport contract used by generated operations."""

    async def transport_request(self, spec: RequestSpec[Any]) -> TransportRequestT:
        """Build and adapt a native request from a declarative request specification."""
        ...

    async def transport_response(self, request: TransportRequestT) -> TransportResponseT:
        """Send a native request and return the native response."""
        ...

    async def response[ResponseT](
        self,
        spec: OperationSpec[Any, ResponseT],
    ) -> OperationResponse[ResponseT, TransportResponseT]:
        """Execute an operation and return its decoded response with metadata."""
        ...

    async def result[ResponseT](self, spec: OperationSpec[Any, ResponseT]) -> ResponseT:
        """Execute an operation and return only its decoded result."""
        ...

    def event_stream[EventT](
        self,
        spec: RequestSpec[None],
        decoder: Callable[[ServerSentEvent], EventT | None],
        *,
        options: EventStreamOptions | None = None,
    ) -> EventStream[EventT]:
        """Create a typed server-sent event stream."""
        ...

    def event_source(
        self,
        spec: RequestSpec[None],
        *,
        options: EventStreamOptions | None = None,
    ) -> EventSource:
        """Create a callback-oriented raw server-sent event source."""
        ...

    async def aclose(self) -> None:
        """Release transport-owned resources idempotently."""
        ...

    async def __aenter__(self) -> Transport[TransportRequestT, TransportResponseT]:
        """Enter an asynchronous transport lifecycle scope."""
        ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release transport-owned resources when leaving a lifecycle scope."""
        ...


class BaseTransport[TransportRequestT, TransportResponseT](ABC):
    """Base transport implementing Sunday's public operation orchestration."""

    async def transport_request(self, spec: RequestSpec[Any]) -> TransportRequestT:
        """Build and adapt a native request from a declarative request specification."""
        return await self._prepare_request(spec)

    async def transport_response(self, request: TransportRequestT) -> TransportResponseT:
        """Send a native request and return the native response."""
        return await self._send(request)

    async def response[ResponseT](
        self,
        spec: OperationSpec[Any, ResponseT],
    ) -> OperationResponse[ResponseT, TransportResponseT]:
        """Execute an operation and return its decoded response with metadata."""
        request = await self.transport_request(spec.request)
        response = await self.transport_response(request)
        decoded = await self._decode_response(response, spec.responses)
        return cast(OperationResponse[ResponseT, TransportResponseT], decoded)

    async def result[ResponseT](self, spec: OperationSpec[Any, ResponseT]) -> ResponseT:
        """Execute an operation and return only its decoded result."""
        return (await self.response(spec)).result

    @abstractmethod
    async def _prepare_request(self, spec: RequestSpec[Any]) -> TransportRequestT:
        """Build and adapt one native request."""

    @abstractmethod
    async def _send(self, request: TransportRequestT, *, stream: bool = False) -> TransportResponseT:
        """Send one native request, optionally retaining its streaming response."""

    @abstractmethod
    async def _decode_response(
        self,
        response: TransportResponseT,
        responses: Sequence[ResponseSpec[Any]],
    ) -> OperationResponse[Any, TransportResponseT]:
        """Decode one native response according to generated response specifications."""

    @abstractmethod
    async def aclose(self) -> None:
        """Release transport-owned resources idempotently."""

    async def __aenter__(self) -> BaseTransport[TransportRequestT, TransportResponseT]:
        """Enter an asynchronous transport lifecycle scope."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release transport-owned resources when leaving a lifecycle scope."""
        del exc_type, exc_value, traceback
        await self.aclose()
