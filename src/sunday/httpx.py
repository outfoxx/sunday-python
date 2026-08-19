# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import AsyncIterable, Awaitable, Callable, Iterable, Mapping, Sequence
from time import monotonic
from types import TracebackType
from typing import Any, Protocol, TypeVar

import anyio
import httpx

from .codecs import JsonCodec, MediaTypeDecoders, MediaTypeEncoders
from .errors import SundayError, UnexpectedResponse
from .headers import ResponseHeaders
from .media import MediaType
from .multipart import MultipartBody
from .observers import TransportEvent, TransportEventKind, TransportObserver, redact_headers
from .operations import OperationResponse
from .parameters import encode_parameters
from .problems import Problem, ProblemRegistry
from .specs import RequestSpec, ResponseSpec
from .sse import EventStreamOptions, ServerSentEvent
from .streaming import StreamingBody
from .transport import Transport

ResponseT = TypeVar("ResponseT")


class HttpxRequestAdapter(Protocol):
    """Adapts an HTTPX request before each send attempt."""

    async def adapt(self, transport: HttpxTransport, request: httpx.Request) -> httpx.Request:
        """Return the request to send."""
        ...


type HttpxRequestAdapterCallable = Callable[
    [HttpxTransport, httpx.Request],
    Awaitable[httpx.Request | None],
]


class HttpxTransport(Transport[httpx.Request, httpx.Response]):
    """Sunday transport implemented by an ``httpx.AsyncClient``."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        problem_registry: ProblemRegistry | None = None,
        encoders: MediaTypeEncoders | None = None,
        decoders: MediaTypeDecoders | None = None,
        adapters: Sequence[HttpxRequestAdapter | HttpxRequestAdapterCallable] = (),
        observers: Sequence[TransportObserver] = (),
        sensitive_headers: Sequence[str] = (),
        close_client: bool = False,
    ) -> None:
        self.client = client
        self.problem_registry = problem_registry or ProblemRegistry()
        self.encoders = encoders or MediaTypeEncoders.defaults()
        self.decoders = decoders or MediaTypeDecoders.defaults()
        self.adapters = tuple(adapters)
        self.observers = tuple(observers)
        self.sensitive_headers = tuple(sensitive_headers)
        self.close_client = close_client
        self._closed = False

    def register_problem(self, type_uri: str, problem_type: type[Problem]) -> None:
        """Register a generated problem exception for response decoding."""
        self.problem_registry.register(type_uri, problem_type)

    def build_request(self, spec: RequestSpec[Any]) -> httpx.Request:
        """Build and adapt an HTTPX request from a declarative specification."""
        parameters = encode_parameters(spec.parameters)
        path = parameters.expand_path(spec.path_template)
        if parameters.query:
            path += ("&" if "?" in path else "?") + parameters.query_string

        headers = httpx.Headers((*spec.headers, *parameters.headers))
        if parameters.cookies:
            cookie_value = "; ".join(f"{name}={value}" for name, value in parameters.cookies)
            existing_cookie = headers.get("cookie")
            headers["cookie"] = f"{existing_cookie}; {cookie_value}" if existing_cookie else cookie_value
        if spec.accept_types and "accept" not in headers:
            headers["accept"] = ", ".join(str(media_type) for media_type in spec.accept_types)

        content: Any = None
        body = spec.effective_body
        content_types = spec.effective_content_types
        if isinstance(body, MultipartBody):
            content = body.content()
            headers["content-type"] = str(body.content_type)
        elif isinstance(body, StreamingBody):
            streaming_content = body.content()
            if isinstance(streaming_content, (bytes, AsyncIterable)):
                content = streaming_content
            else:
                iterable_content: Iterable[bytes] = streaming_content

                async def async_content() -> AsyncIterable[bytes]:
                    for chunk in iterable_content:
                        yield chunk

                content = async_content()
            if content_types and "content-type" not in headers:
                headers["content-type"] = str(content_types[0])
        elif isinstance(body, (bytes, bytearray, memoryview)):
            content = bytes(body)
            content_type = content_types[0] if content_types else MediaType("application/octet-stream")
            if "content-type" not in headers:
                headers["content-type"] = str(content_type)
        elif body is not None:
            content_type = content_types[0] if content_types else MediaType("application/json")
            encoder = self.encoders.find(content_type)
            if encoder is None:
                raise SundayError(f"No request encoder supports {content_type}")
            content = encoder.encode(body)
            if "content-type" not in headers:
                headers["content-type"] = str(content_type)

        return self.client.build_request(spec.method.upper(), path, headers=headers, content=content)

    async def send(self, request: httpx.Request, *, stream: bool = False) -> httpx.Response:
        """Send a native HTTPX request."""
        if self._closed:
            raise RuntimeError("HttpxTransport is closed")
        for adapter in self.adapters:
            adapt = getattr(adapter, "adapt", None)
            adapted = await adapt(self, request) if adapt is not None else await adapter(self, request)  # type: ignore[operator]
            if adapted is not None:
                request = adapted

        started = monotonic()
        self._observe(
            TransportEvent(
                TransportEventKind.REQUEST,
                request.method,
                str(request.url),
                redact_headers(request.headers.multi_items(), self.sensitive_headers),
            )
        )
        try:
            response = await self.client.send(request, stream=stream)
        except BaseException as error:
            self._observe(
                TransportEvent(
                    TransportEventKind.FAILURE,
                    request.method,
                    str(request.url),
                    (),
                    elapsed=monotonic() - started,
                    error=error,
                )
            )
            raise
        self._observe(
            TransportEvent(
                TransportEventKind.RESPONSE,
                request.method,
                str(request.url),
                redact_headers(response.headers.multi_items(), self.sensitive_headers),
                status=response.status_code,
                elapsed=monotonic() - started,
            )
        )
        return response

    async def decode_response(
        self,
        response: httpx.Response,
        responses: Sequence[ResponseSpec[Any]],
    ) -> OperationResponse[Any, httpx.Response]:
        """Decode a successful response or raise its typed problem."""
        body = await response.aread()
        try:
            return self._decode_buffered_response(response, body, responses)
        finally:
            with anyio.CancelScope(shield=True):
                await response.aclose()

    def _decode_buffered_response(
        self,
        response: httpx.Response,
        body: bytes,
        responses: Sequence[ResponseSpec[Any]],
    ) -> OperationResponse[Any, httpx.Response]:
        if not 200 <= response.status_code < 300:
            self.raise_problem(response, body)

        headers = ResponseHeaders.from_items(response.headers.multi_items())
        content_type = headers.content_type
        candidates = [item for item in responses if item.status == response.status_code]
        if not candidates:
            candidates = [item for item in responses if item.status is None]
        if not candidates:
            raise self._unexpected(response, body, "No response specification accepts this status")

        if response.status_code in {204, 205} or not body:
            response_spec = next((item for item in candidates if not item.body_expected), None)
            if response_spec is None and response.status_code not in {204, 205}:
                raise self._unexpected(response, body, "Expected a response body")
            decoded_headers = self._decode_headers(response_spec, headers, response, body) if response_spec else {}
            return OperationResponse(None, response, response.status_code, headers, decoded_headers)

        response_spec = next((item for item in candidates if item.accepts(content_type)), None)
        if response_spec is None:
            raise self._unexpected(response, body, "No response specification accepts this media type")
        if content_type is None:
            raise self._unexpected(response, body, "A response body was returned without Content-Type")

        decoder = self.decoders.find(content_type)
        if decoder is None:
            raise self._unexpected(response, body, "No decoder supports the response media type")
        decoded = decoder.decode(body, content_type)
        result = response_spec.decoder(decoded) if response_spec.decoder is not None else decoded
        decoded_headers = self._decode_headers(response_spec, headers, response, body)
        return OperationResponse(result, response, response.status_code, headers, decoded_headers)

    def _decode_headers(
        self,
        spec: ResponseSpec[Any],
        headers: ResponseHeaders,
        response: httpx.Response,
        body: bytes,
    ) -> dict[str, object]:
        decoded: dict[str, object] = {}
        for header in spec.headers:
            values = headers.get_all(header.name)
            if not values:
                if header.required:
                    raise self._unexpected(response, body, f"Required response header is missing: {header.name}")
                continue
            try:
                converted = tuple(header.decoder(value) if header.decoder is not None else value for value in values)
            except (TypeError, ValueError) as error:
                raise self._unexpected(response, body, f"Invalid response header {header.name}: {error}") from error
            decoded[header.name] = converted if header.repeated else converted[0]
        return decoded

    def raise_problem(self, response: httpx.Response, body: bytes) -> None:
        """Raise a typed problem or an unexpected-response error."""
        content_type_value = response.headers.get("content-type")
        try:
            content_type = MediaType(content_type_value) if content_type_value else None
        except ValueError:
            content_type = None

        if content_type is not None and content_type.is_json and body:
            try:
                value = JsonCodec().decode(body, content_type)
            except (UnicodeDecodeError, ValueError):
                value = None
            if isinstance(value, Mapping) and (
                content_type.subtype == "problem+json" or "type" in value or "title" in value
            ):
                raise self.problem_registry.decode(value, response_status=response.status_code)

        raise self._unexpected(response, body, "HTTP request failed without a decodable problem response")

    def event_stream(
        self,
        spec: RequestSpec[None],
        decoder: Callable[[ServerSentEvent], ResponseT],
        *,
        options: EventStreamOptions | None = None,
    ) -> HttpxEventStream[ResponseT]:
        """Create a reconnecting typed HTTPX server-sent event stream."""
        from .httpx_sse import HttpxEventStream

        return HttpxEventStream(self, spec, decoder, options=options)

    def _unexpected(self, response: httpx.Response, body: bytes, message: str) -> UnexpectedResponse:
        return UnexpectedResponse(
            message,
            status=response.status_code,
            content_type=response.headers.get("content-type"),
            body=body,
            transport_response=response,
        )

    def _observe(self, event: TransportEvent) -> None:
        for observer in self.observers:
            observer.observe(event)

    async def aclose(self) -> None:
        """Close the transport and optionally its HTTPX client exactly once."""
        if self._closed:
            return
        self._closed = True
        if self.close_client:
            await self.client.aclose()

    async def __aenter__(self) -> HttpxTransport:
        """Enter an asynchronous transport lifecycle scope."""
        if self._closed:
            raise RuntimeError("HttpxTransport is closed")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close transport-owned resources when leaving a lifecycle scope."""
        del exc_type, exc_value, traceback
        await self.aclose()


def as_httpx_transport(value: HttpxTransport | httpx.AsyncClient) -> HttpxTransport:
    """Preserve the beta constructor contract by wrapping raw HTTPX clients."""
    return value if isinstance(value, HttpxTransport) else HttpxTransport(value)


from .httpx_sse import HttpxEventStream as HttpxEventStream  # noqa: E402
