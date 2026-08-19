# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import AsyncIterable, Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, TypeVar

import anyio
import httpx

from .codecs import JsonCodec, MediaTypeDecoders, MediaTypeEncoders
from .errors import SundayError, UnexpectedResponse
from .headers import ResponseHeaders
from .media import MediaType
from .operations import OperationResponse
from .parameters import encode_parameters
from .problems import Problem, ProblemRegistry
from .specs import RequestSpec, ResponseSpec
from .sse import EventStreamOptions, ServerSentEvent
from .streaming import StreamingBody

ResponseT = TypeVar("ResponseT")


class HttpxRequestAdapter(Protocol):
    """Adapts an HTTPX request before each send attempt."""

    async def adapt(self, transport: HttpxTransport, request: httpx.Request) -> httpx.Request:
        """Return the request to send."""
        ...


@dataclass(frozen=True, slots=True)
class TokenAuthorization:
    """A header token and its optional expiration time."""

    token: str
    expires_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class StaticHeaderTokenAuthorizingAdapter:
    """Adds a static token to a request header."""

    token: str
    header_name: str = "Authorization"
    scheme: str | None = "Bearer"

    async def adapt(self, transport: HttpxTransport, request: httpx.Request) -> httpx.Request:
        """Add the configured static token header to ``request``."""
        del transport
        request.headers[self.header_name] = _authorization_value(self.token, self.scheme)
        return request


class RefreshingHeaderTokenAuthorizingAdapter:
    """Caches and refreshes header-token authorization using an async provider."""

    def __init__(
        self,
        provider: Callable[[], Awaitable[TokenAuthorization]],
        *,
        header_name: str = "Authorization",
        scheme: str | None = "Bearer",
        refresh_skew: timedelta = timedelta(seconds=30),
    ) -> None:
        self._provider = provider
        self._header_name = header_name
        self._scheme = scheme
        self._refresh_skew = refresh_skew
        self._authorization: TokenAuthorization | None = None
        self._lock = anyio.Lock()

    async def adapt(self, transport: HttpxTransport, request: httpx.Request) -> httpx.Request:
        """Add a cached or freshly obtained token header to ``request``."""
        del transport
        authorization = await self._current_authorization()
        request.headers[self._header_name] = _authorization_value(authorization.token, self._scheme)
        return request

    async def _current_authorization(self) -> TokenAuthorization:
        if self._is_current(self._authorization):
            authorization = self._authorization
            assert authorization is not None
            return authorization
        async with self._lock:
            if not self._is_current(self._authorization):
                self._authorization = await self._provider()
            authorization = self._authorization
            assert authorization is not None
            return authorization

    def _is_current(self, authorization: TokenAuthorization | None) -> bool:
        if authorization is None:
            return False
        if authorization.expires_at is None:
            return True
        expires_at = authorization.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        return datetime.now(UTC) + self._refresh_skew < expires_at


class HttpxTransport:
    """Sunday transport implemented by an ``httpx.AsyncClient``."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        problem_registry: ProblemRegistry | None = None,
        encoders: MediaTypeEncoders | None = None,
        decoders: MediaTypeDecoders | None = None,
        adapters: Sequence[HttpxRequestAdapter] = (),
    ) -> None:
        self.client = client
        self.problem_registry = problem_registry or ProblemRegistry()
        self.encoders = encoders or MediaTypeEncoders.defaults()
        self.decoders = decoders or MediaTypeDecoders.defaults()
        self.adapters = tuple(adapters)

    def register_problem(self, type_uri: str, problem_type: type[Problem]) -> None:
        """Register a generated problem exception for response decoding."""
        self.problem_registry.register(type_uri, problem_type)

    async def build_request(self, spec: RequestSpec[Any]) -> httpx.Request:
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
        if isinstance(spec.body, StreamingBody):
            streaming_content = spec.body.content()
            if isinstance(streaming_content, (bytes, AsyncIterable)):
                content = streaming_content
            else:
                iterable_content: Iterable[bytes] = streaming_content

                async def async_content() -> AsyncIterable[bytes]:
                    for chunk in iterable_content:
                        yield chunk

                content = async_content()
            if spec.content_types and "content-type" not in headers:
                headers["content-type"] = str(spec.content_types[0])
        elif spec.body is not None:
            content_type = spec.content_types[0] if spec.content_types else MediaType("application/json")
            encoder = self.encoders.find(content_type)
            if encoder is None:
                raise SundayError(f"No request encoder supports {content_type}")
            content = encoder.encode(spec.body)
            if "content-type" not in headers:
                headers["content-type"] = str(content_type)

        request = self.client.build_request(spec.method.upper(), path, headers=headers, content=content)
        for adapter in self.adapters:
            request = await adapter.adapt(self, request)
        return request

    async def send(self, request: httpx.Request, *, stream: bool = False) -> httpx.Response:
        """Send a native HTTPX request."""
        return await self.client.send(request, stream=stream)

    async def decode_response(
        self,
        response: httpx.Response,
        responses: Sequence[ResponseSpec[Any]],
    ) -> OperationResponse[Any]:
        """Decode a successful response or raise its typed problem."""
        body = await response.aread()
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
            return OperationResponse(None, response, response.status_code, headers)

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
        return OperationResponse(result, response, response.status_code, headers)

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


def as_httpx_transport(value: HttpxTransport | httpx.AsyncClient) -> HttpxTransport:
    """Preserve the beta constructor contract by wrapping raw HTTPX clients."""
    return value if isinstance(value, HttpxTransport) else HttpxTransport(value)


def _authorization_value(token: str, scheme: str | None) -> str:
    return f"{scheme} {token}" if scheme else token


from .httpx_sse import HttpxEventStream  # noqa: E402
