# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

from .headers import ResponseHeaders
from .media import MediaType
from .problems import Problem
from .specs import NullifySpec, OperationSpec

if TYPE_CHECKING:
    from .transport import Transport


@dataclass(frozen=True, slots=True)
class OperationResponse[ResponseT, TransportResponseT]:
    """A decoded operation result with transport response metadata."""

    result: ResponseT
    transport_response: TransportResponseT
    status: int
    headers: ResponseHeaders
    decoded_headers: Mapping[str, object] = field(default_factory=dict)

    def get_headers(self, name: str) -> tuple[str, ...]:
        """Return every response header matching ``name``."""
        return self.headers.get_all(name)

    def get_header(self, name: str) -> str | None:
        """Return the first response header matching ``name``."""
        return self.headers.get(name)

    def decoded_header(self, name: str) -> object | None:
        """Return a decoded declared header value case-insensitively."""
        lower_name = name.lower()
        return next((value for key, value in self.decoded_headers.items() if key.lower() == lower_name), None)

    @property
    def content_type(self) -> MediaType | None:
        """Return the parsed response Content-Type."""
        return self.headers.content_type


@dataclass(frozen=True, slots=True)
class Operation[ResponseT, TransportRequestT, TransportResponseT]:
    """A reusable generated HTTP operation."""

    transport: Transport[TransportRequestT, TransportResponseT]
    spec: OperationSpec[Any, ResponseT]

    async def execute(self) -> ResponseT:
        """Execute the operation and return its decoded result."""
        return (await self.response()).result

    async def response(self) -> OperationResponse[ResponseT, TransportResponseT]:
        """Execute the operation and include response metadata."""
        response = await self.transport_response()
        decoded = await self.transport.decode_response(response, self.spec.responses)
        return cast(OperationResponse[ResponseT, TransportResponseT], decoded)

    def transport_request(self) -> TransportRequestT:
        """Build and return a fresh native transport request."""
        return self.transport.build_request(self.spec.request)

    async def transport_response(self) -> TransportResponseT:
        """Send a fresh native request and return its native response."""
        return await self.transport.send(self.transport_request())


class StreamingOperation[ResponseT, TransportRequestT, TransportResponseT](
    Operation[ResponseT, TransportRequestT, TransportResponseT]
):
    """An operation whose reusable body creates fresh content per execution."""


@dataclass(frozen=True, slots=True)
class NullableOperation[ResponseT, TransportRequestT, TransportResponseT]:
    """An operation that can translate selected problems into ``None``."""

    transport: Transport[TransportRequestT, TransportResponseT]
    spec: OperationSpec[Any, ResponseT]
    nullify: NullifySpec

    def _operation(self) -> Operation[ResponseT, TransportRequestT, TransportResponseT]:
        return Operation(self.transport, self.spec)

    async def execute(self) -> ResponseT:
        """Execute without nullifying problems."""
        return await self._operation().execute()

    async def response(self) -> OperationResponse[ResponseT, TransportResponseT]:
        """Execute without nullifying problems and include metadata."""
        return await self._operation().response()

    async def execute_or_none(self) -> ResponseT | None:
        """Return ``None`` when a configured problem is raised."""
        response = await self.response_or_none()
        return response.result if response is not None else None

    async def response_or_none(self) -> OperationResponse[ResponseT, TransportResponseT] | None:
        """Return ``None`` when a configured problem is raised."""
        try:
            return await self.response()
        except Problem as problem:
            if self._matches(problem):
                return None
            raise

    def transport_request(self) -> TransportRequestT:
        """Build and return a fresh native transport request."""
        return self._operation().transport_request()

    async def transport_response(self) -> TransportResponseT:
        """Send a fresh native request and return its native response."""
        return await self._operation().transport_response()

    def _matches(self, problem: Problem) -> bool:
        return (problem.status is not None and problem.status in self.nullify.statuses) or isinstance(
            problem,
            self.nullify.problem_types,
        )
