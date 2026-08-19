# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Compatibility surface for Python clients generated before the neutral transport target.

Current generated clients import transport-neutral APIs directly from :mod:`sunday`.
This module remains available for existing beta packages that still contain a
generated ``runtime.py`` shim.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from urllib.parse import quote

import httpx
from pydantic import BaseModel

from .headers import ResponseHeaders
from .httpx import HttpxTransport, as_httpx_transport
from .media import MediaType
from .operations import OperationResponse
from .specs import RequestSpec
from .streaming import StreamingBody

type Transport = HttpxTransport | httpx.AsyncClient
type TransportRequest = httpx.Request
type TransportResponse = httpx.Response


@dataclass(frozen=True, slots=True)
class Operation[ResponseT]:
    """Prepared compatibility operation backed by ``HttpxTransport``."""

    transport: HttpxTransport
    request: httpx.Request
    decode: Callable[[httpx.Response], ResponseT]

    async def execute(self) -> ResponseT:
        """Send the request and return the decoded body."""
        return (await self.response()).result

    async def response(self) -> OperationResponse[ResponseT, httpx.Response]:
        """Send the request and include native response metadata."""
        response = await self.transport_response()
        try:
            body = await response.aread()
            if not 200 <= response.status_code < 300:
                self.transport.raise_problem(response, body)
            return OperationResponse(
                self.decode(response),
                response,
                response.status_code,
                ResponseHeaders.from_items(response.headers.multi_items()),
            )
        finally:
            await response.aclose()

    def transport_request(self) -> httpx.Request:
        """Return the prepared native request."""
        return self.request

    async def transport_response(self) -> httpx.Response:
        """Send and return the native HTTPX response."""
        return await self.transport.send(self.request)


@dataclass(frozen=True, slots=True)
class StreamingOperation[ResponseT]:
    """Prepared compatibility operation that rebuilds streaming request bodies."""

    transport: HttpxTransport
    build_request: Callable[[], httpx.Request]
    decode: Callable[[httpx.Response], ResponseT]

    async def execute(self) -> ResponseT:
        """Send a fresh request and return the decoded body."""
        return (await self.response()).result

    async def response(self) -> OperationResponse[ResponseT, httpx.Response]:
        """Send a fresh request and include native response metadata."""
        response = await self.transport_response()
        try:
            body = await response.aread()
            if not 200 <= response.status_code < 300:
                self.transport.raise_problem(response, body)
            return OperationResponse(
                self.decode(response),
                response,
                response.status_code,
                ResponseHeaders.from_items(response.headers.multi_items()),
            )
        finally:
            await response.aclose()

    def transport_request(self) -> httpx.Request:
        """Return a fresh native request."""
        return self.build_request()

    async def transport_response(self) -> httpx.Response:
        """Send and return a fresh native HTTPX response."""
        return await self.transport.send(self.build_request())


class EventStream[EventT]:
    """Compatibility event stream accepting a prepared HTTPX request."""

    def __init__(
        self,
        transport: HttpxTransport,
        request: httpx.Request,
        decode: Callable[[str], EventT],
    ) -> None:
        excluded_headers = {"host", "content-length", "transfer-encoding"}
        headers = tuple(
            (name, value) for name, value in request.headers.multi_items() if name.lower() not in excluded_headers
        )
        request_spec = RequestSpec[None](request.method, str(request.url), headers=headers)
        self._event_stream = transport.event_stream(request_spec, lambda event: decode(event.data or ""))

    def __aiter__(self) -> AsyncIterator[EventT]:
        return self.events()

    async def events(self) -> AsyncIterator[EventT]:
        """Yield decoded events across reconnections."""
        async for event in self._event_stream:
            yield event

    async def aclose(self) -> None:
        """Close the active stream."""
        await self._event_stream.aclose()


def as_transport(transport: Transport) -> HttpxTransport:
    """Normalize the beta constructor's HTTPX client or transport input."""
    return as_httpx_transport(transport)


def json_body(body: object | None) -> object | None:
    """Convert a generated model into an HTTPX JSON request value."""
    if isinstance(body, BaseModel):
        return body.model_dump(mode="json", by_alias=True, exclude_unset=True)
    return body


def parameter_map(parameters: Mapping[str, object | None]) -> dict[str, str]:
    """Format legacy scalar request parameters while omitting absent values."""
    return {name: parameter_value(value) for name, value in parameters.items() if value is not None}


def path_template(path: str, path_parameters: Mapping[str, object]) -> str:
    """Expand legacy simple path parameters."""
    for name, value in path_parameters.items():
        path = path.replace("{" + name + "}", quote(parameter_value(value), safe=""))
    return path


def parameter_value(value: object) -> str:
    """Convert a legacy scalar request parameter to a wire string."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Enum):
        return parameter_value(value.value)
    return str(value)


__all__ = [
    "EventStream",
    "MediaType",
    "Operation",
    "OperationResponse",
    "ResponseHeaders",
    "StreamingBody",
    "StreamingOperation",
    "Transport",
    "TransportRequest",
    "TransportResponse",
    "as_transport",
    "json_body",
    "parameter_map",
    "path_template",
]
