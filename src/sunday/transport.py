# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Sequence
from types import TracebackType
from typing import Any, Protocol

from .operations import OperationResponse
from .problems import Problem
from .specs import RequestSpec, ResponseSpec
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

    def build_request(self, spec: RequestSpec[Any]) -> TransportRequestT:
        """Build a native request from a declarative request specification."""
        ...

    async def send(self, request: TransportRequestT, *, stream: bool = False) -> TransportResponseT:
        """Send a native request and return the native response."""
        ...

    async def decode_response(
        self,
        response: TransportResponseT,
        responses: Sequence[ResponseSpec[Any]],
    ) -> OperationResponse[Any, TransportResponseT]:
        """Decode a native response according to generated response specifications."""
        ...

    def event_stream[EventT](
        self,
        spec: RequestSpec[None],
        decoder: Callable[[ServerSentEvent], EventT],
        *,
        options: EventStreamOptions | None = None,
    ) -> EventStream[EventT]:
        """Create a typed server-sent event stream."""
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
