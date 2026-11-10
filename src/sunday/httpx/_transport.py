# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from time import monotonic
from types import TracebackType
from typing import Any, Protocol, TypeVar

import httpx

from ..codecs import JsonCodec, MediaTypeDecoders, MediaTypeEncoders
from ..errors import (
    RequestEncodingError,
    ResponseDecodingError,
    TransportError,
    UnexpectedResponse,
)
from ..headers import ResponseHeaders
from ..media import MediaType
from ..multipart import MultipartBody
from ..observers import TransportEvent, TransportEventKind, TransportObserver, redact_headers
from ..operations import OperationResponse
from ..parameters import encode_parameters
from ..problems import Problem, ProblemRegistry
from ..request_cancellation_error import RequestCancellationError
from ..specs import RequestSpec, ResponseSpec
from ..sse import EventStreamOptions, ServerSentEvent
from ..streaming import StreamingBody
from ..transport import BaseTransport
from ..uri import URITemplate
from ._async import complete, run_io
from ._diagnostics import diagnostic_error, diagnostic_request
from ._requests import BodyStream, copy_request, is_credentialed, redirect_request
from ._response_cleanup import close_response, protect_response
from .request_adapter import RequestAdapterLike, RequestAdapters
from .request_security_bindings import _set_request_security_bindings, request_security_bindings
from .response_adapter import ResponseAdapterLike, ResponseAdapters

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

type HttpxEventSourceRequestFactory = Callable[
    [tuple[tuple[str, str], ...]],
    httpx.Request | Awaitable[httpx.Request],
]


class HttpxTransport(BaseTransport[httpx.Request, httpx.Response]):
    """Sunday transport using an owned internal or borrowed external ``httpx.AsyncClient``."""

    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
        *,
        base_url: str | httpx.URL | None = None,
        problem_registry: ProblemRegistry | None = None,
        encoders: MediaTypeEncoders | None = None,
        decoders: MediaTypeDecoders | None = None,
        adapters: Sequence[HttpxRequestAdapter | HttpxRequestAdapterCallable] = (),
        request_adapter: RequestAdapterLike | None = None,
        response_adapter: ResponseAdapterLike | None = None,
        observers: Sequence[TransportObserver] = (),
        sensitive_headers: Sequence[str] = (),
    ) -> None:
        if client is not None and base_url is not None:
            raise TransportError("base_url cannot be provided with a borrowed HTTPX client")
        self._owns_client = client is None
        self.client = client if client is not None else httpx.AsyncClient(base_url=base_url or "")
        self.problem_registry = problem_registry or ProblemRegistry()
        self.encoders = encoders or MediaTypeEncoders.defaults()
        self.decoders = decoders or MediaTypeDecoders.defaults()
        self.adapters = tuple(adapters)
        self.observers = tuple(observers)
        self.sensitive_headers = tuple(sensitive_headers)
        self._request_adapter = RequestAdapters(
            *((self._adapt_legacy_request,) if self.adapters else ()),
            *((request_adapter,) if request_adapter is not None else ()),
        )
        self._response_adapter = ResponseAdapters(*((response_adapter,) if response_adapter is not None else ()))
        self._closed = False
        self._cleanup: asyncio.Task[None] | None = None
        self._event_sources: set[HttpxEventSource] = set()

    def register_problem(self, type_uri: str, problem_type: type[Problem]) -> None:
        """Register a generated problem exception for response decoding."""
        self.problem_registry.register(type_uri, problem_type)

    async def transport_request(self, spec: RequestSpec[Any]) -> httpx.Request:
        """Materialize an optionally authorized request, handing its lifecycle to the consumer."""
        request = await self._prepare_request(spec)
        return _set_request_security_bindings(await self._request_adapter.adapt(request), ())

    async def _prepare_request(self, spec: RequestSpec[Any]) -> httpx.Request:
        """Build without adapting; execution and export each own their adaptation step."""
        if self._closed:
            raise TransportError("HttpxTransport is closed")
        return self._build_request(spec)

    async def execute_request(self, spec: RequestSpec[Any]) -> httpx.Response:
        """Execute with invocation-local security receipts and no request-attached state."""
        request = await self._prepare_request(spec)
        return await self._execute(request, stream=False)

    def _build_request(self, spec: RequestSpec[Any]) -> httpx.Request:
        try:
            return _set_request_security_bindings(self._build_validated_request(spec), spec.security or ())
        except (RequestEncodingError, RequestCancellationError):
            raise
        except (TypeError, ValueError) as error:
            raise RequestEncodingError(
                "Request encoding failed",
                details={"method": spec.method, "path_template": str(spec.path_template)},
            ) from error

    def _build_validated_request(self, spec: RequestSpec[Any]) -> httpx.Request:
        spec.validate_parameters()
        spec.validate_body()
        parameters = encode_parameters(spec.parameters)
        if isinstance(spec.path_template, URITemplate):
            template = spec.path_template.template
            placeholders: dict[str, str] = {}
            for index, (name, _value) in enumerate(parameters.path):
                placeholder = "{" + name + "}"
                if placeholder in template:
                    token = f"__SUNDAY_PATH_PARAMETER_{index}__"
                    template = template.replace(placeholder, token)
                    placeholders[token] = placeholder
            template_value = URITemplate(template, spec.path_template.parameters)
            supplied = set(spec.path_template.parameters) | set(spec.template_parameters)
            missing = set(template_value.variable_names) - supplied
            if missing:
                names = ", ".join(sorted(missing))
                raise RequestEncodingError(
                    f"URI template parameters are missing: {names}",
                    details={"parameters": tuple(sorted(missing))},
                )
            path = template_value.expand(spec.template_parameters)
            for token, placeholder in placeholders.items():
                path = path.replace(token, placeholder)
            path = parameters.expand_path(path)
        else:
            path = parameters.expand_path(spec.path_template)
        if parameters.query:
            path += ("&" if "?" in path else "?") + parameters.query_string

        headers = httpx.Headers((*spec.headers, *parameters.headers))
        if parameters.cookies:
            cookie_value = "; ".join(f"{name}={value}" for name, value in parameters.cookies)
            existing_cookie = headers.get("cookie")
            headers["cookie"] = f"{existing_cookie}; {cookie_value}" if existing_cookie else cookie_value
        if spec.accept_types and "accept" not in headers:
            supported = tuple(
                media_type
                for media_type in spec.accept_types
                if self.decoders.find(media_type) is not None
                or any(binary.matches(media_type) or media_type.matches(binary) for binary in spec.binary_accept_types)
            )
            if not supported:
                declared = ", ".join(str(media_type) for media_type in spec.accept_types)
                raise RequestEncodingError(
                    f"No response decoder supports any declared media type: {declared}",
                    details={"accept_types": tuple(str(item) for item in spec.accept_types)},
                )
            headers["accept"] = ", ".join(str(media_type) for media_type in supported)

        content: Any = None
        body = spec.effective_body
        content_types = spec.effective_content_types
        if isinstance(body, MultipartBody):
            existing_content_type = self._header_media_type(headers, "content-type")
            declared_content_type = existing_content_type or (content_types[0] if content_types else None)
            if declared_content_type is not None and not MediaType("multipart/form-data").matches(
                declared_content_type
            ):
                raise RequestEncodingError(
                    f"Multipart body cannot use Content-Type {declared_content_type}",
                    details={"content_type": str(declared_content_type)},
                )
            content = body.content()
            headers["content-type"] = str(body.content_type)
        elif isinstance(body, StreamingBody):
            content = BodyStream(body)
            content_type = self._raw_content_type(headers, content_types)
            if content_type is not None and "content-type" not in headers:
                headers["content-type"] = str(content_type)
        elif body is not None:
            default = (
                MediaType("application/octet-stream")
                if isinstance(body, (bytes, bytearray, memoryview))
                else MediaType("application/json")
            )
            try:
                content_type, encoder = self._encoded_content_type(headers, content_types, default)
            except RequestCancellationError:
                raise
            except RequestEncodingError:
                if not isinstance(body, (bytes, bytearray, memoryview)):
                    raise
                content_type = self._raw_content_type(headers, content_types) or default
                content = bytes(body)
            else:
                try:
                    content = encoder.encode(body)
                except RequestCancellationError:
                    raise
                except (TypeError, ValueError) as error:
                    raise RequestEncodingError(
                        "Request body encoding failed",
                        details={"content_type": str(content_type)},
                    ) from error
            if "content-type" not in headers:
                headers["content-type"] = str(content_type)

        request = self.client.build_request(spec.method.upper(), path, headers=headers, content=content)
        if isinstance(content, BodyStream):
            request.stream = content
        if spec.security and "authorization" not in headers:
            # A borrowed client's default credentials are not part of the selected alternative.
            request.headers.pop("authorization", None)
        return request

    def _header_media_type(self, headers: httpx.Headers, name: str) -> MediaType | None:
        value = headers.get(name)
        if value is None:
            return None
        try:
            return MediaType(value)
        except ValueError as error:
            raise RequestEncodingError(
                f"Invalid {name.title()} header: {value}",
                details={"header": name, "value": value},
            ) from error

    def _raw_content_type(
        self,
        headers: httpx.Headers,
        declared: Sequence[MediaType],
    ) -> MediaType | None:
        return self._header_media_type(headers, "content-type") or (declared[0] if declared else None)

    def _encoded_content_type(
        self,
        headers: httpx.Headers,
        declared: Sequence[MediaType],
        default: MediaType,
    ) -> tuple[MediaType, Any]:
        explicit = self._header_media_type(headers, "content-type")
        candidates = (explicit,) if explicit is not None else tuple(declared) or (default,)
        for media_type in candidates:
            encoder = self.encoders.find(media_type)
            if encoder is not None:
                return media_type, encoder
        choices = ", ".join(str(media_type) for media_type in candidates)
        raise RequestEncodingError(
            f"No request encoder supports any declared media type: {choices}",
            details={"content_types": tuple(str(item) for item in candidates)},
        )

    async def _adapt_legacy_request(self, request: httpx.Request) -> httpx.Request:
        for adapter in self.adapters:
            adapt = getattr(adapter, "adapt", None)
            adapted = await adapt(self, request) if adapt is not None else await adapter(self, request)  # type: ignore[operator]
            if adapted is not None:
                request = adapted
        return request

    async def _send(self, request: httpx.Request, *, stream: bool = False) -> httpx.Response:
        return await self._execute(request, stream=stream)

    async def _send_event_stream(
        self,
        request: httpx.Request,
    ) -> httpx.Response:
        return await self._execute(request, stream=True, follow_redirects=True)

    async def _execute(
        self,
        request: httpx.Request,
        *,
        stream: bool,
        follow_redirects: bool | None = None,
    ) -> httpx.Response:
        follow = self.client.follow_redirects if follow_redirects is None else follow_redirects
        prepared = request
        redirects = 0
        history: list[httpx.Response] = []
        while True:
            sent = copy_request(prepared)
            try:
                sent = await self._request_adapter.adapt(sent)
            except BaseException as error:
                raise diagnostic_error(error, sent) from None
            response = await self._send_httpx(sent)
            effective = sent
            try:
                response.history = [*history, *response.history]
                effective = copy_request(response.request)
                response = await self._response_adapter.adapt(response, effective)
                next_request = (
                    redirect_request(prepared, effective, response)
                    if follow and not request_security_bindings(effective)
                    else None
                )
                if next_request is not None:
                    self.client.cookies.set_cookie_header(next_request)
                    redirects += 1
                    if redirects > self.client.max_redirects:
                        raise httpx.TooManyRedirects("Exceeded maximum allowed redirects", request=effective)
                    # HTTPX buffers intermediate redirect bodies even when the final response streams.
                    await run_io(response.aread)
                    await close_response(response)
                    history = [*response.history, response]
                    prepared = next_request
                    continue
                response.request = diagnostic_request(effective)
                if not stream:
                    await run_io(response.aread)
                    await close_response(response)
                return response
            except BaseException as error:
                await close_response(response, error)
                raise diagnostic_error(error, effective) from None

    async def _send_httpx(
        self,
        request: httpx.Request,
    ) -> httpx.Response:
        if self._closed:
            raise TransportError("HttpxTransport is closed")
        started = monotonic()
        diagnostic = diagnostic_request(request)
        self._observe(
            TransportEvent(
                TransportEventKind.REQUEST,
                request.method,
                str(diagnostic.url),
                redact_headers(diagnostic.headers.multi_items(), self.sensitive_headers),
            )
        )
        try:
            if request_security_bindings(request) or is_credentialed(request):
                response = await run_io(
                    lambda: self.client.send(request, stream=True, follow_redirects=False, auth=None),
                    discard=close_response,
                )
            else:
                response = await run_io(
                    lambda: self.client.send(request, stream=True, follow_redirects=False),
                    discard=close_response,
                )
        except BaseException as error:
            error = diagnostic_error(error, request)
            try:
                self._observe(
                    TransportEvent(
                        TransportEventKind.FAILURE,
                        request.method,
                        str(diagnostic.url),
                        (),
                        elapsed=monotonic() - started,
                        error=error,
                    )
                )
            except BaseException:
                error.add_note("Reporting the HTTP request failure also failed")
            raise error from None
        protect_response(response)
        try:
            self._observe(
                TransportEvent(
                    TransportEventKind.RESPONSE,
                    request.method,
                    str(diagnostic.url),
                    redact_headers(response.headers.multi_items(), self.sensitive_headers),
                    status=response.status_code,
                    elapsed=monotonic() - started,
                )
            )
        except BaseException as error:
            await close_response(response, error)
            raise
        return response

    async def _decode_response(
        self,
        response: httpx.Response,
        responses: Sequence[ResponseSpec[Any]],
    ) -> OperationResponse[Any, httpx.Response]:
        """Decode a successful response or raise its typed problem."""
        body = response.content
        if not 200 <= response.status_code < 300:
            self.raise_problem(response, body)

        headers = ResponseHeaders.from_items(response.headers.multi_items())
        content_type = headers.content_type
        candidates = [item for item in responses if item.status == response.status_code]
        if not candidates:
            candidates = [item for item in responses if item.status is None]
        if not candidates:
            raise self._unexpected(response, body, "No response specification accepts this status")

        response_spec = next((item for item in candidates if item.accepts(content_type)), None)
        if response.status_code in {204, 205} or (
            not body and (response_spec is None or not response_spec.binary_body)
        ):
            response_spec = next((item for item in candidates if not item.body_expected), None)
            if response_spec is None and response.status_code not in {204, 205}:
                raise self._unexpected(response, body, "Expected a response body")
            decoded_headers = self._decode_headers(response_spec, headers, response, body) if response_spec else {}
            return OperationResponse(None, response, response.status_code, headers, decoded_headers)

        if response_spec is None:
            raise self._unexpected(response, body, "No response specification accepts this media type")
        if content_type is None:
            raise self._unexpected(response, body, "A response body was returned without Content-Type")

        decoder = None if response_spec.binary_body else self.decoders.find(content_type)
        if decoder is None and not response_spec.binary_body:
            raise self._unexpected(response, body, "No decoder supports the response media type")
        try:
            decoded = decoder.decode(body, content_type) if decoder is not None else body
            result = response_spec.decoder(decoded) if response_spec.decoder is not None else decoded
        except RequestCancellationError:
            raise
        except (TypeError, ValueError) as error:
            raise ResponseDecodingError(
                "Response body decoding failed",
                status=response.status_code,
                content_type=str(content_type),
                body=body,
                transport_response=response,
            ) from error
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
            except RequestCancellationError:
                raise
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
        decoder: Callable[[ServerSentEvent], ResponseT | None],
        *,
        options: EventStreamOptions | None = None,
    ) -> HttpxEventStream[ResponseT]:
        """Create a reconnecting typed HTTPX server-sent event stream."""
        from ._sse import HttpxEventStream

        return HttpxEventStream(self, spec, decoder, options=options)

    def event_source(
        self,
        spec: RequestSpec[None],
        *,
        options: EventStreamOptions | None = None,
    ) -> HttpxEventSource:
        """Create a callback-oriented HTTPX server-sent event source."""

        async def request_factory(headers: tuple[tuple[str, str], ...]) -> httpx.Request:
            return self._build_request(spec.with_headers(*headers))

        from ._sse import HttpxEventSource

        source = HttpxEventSource(self, request_factory, options=options, spec=spec)
        self._event_sources.add(source)
        return source

    def event_source_from(
        self,
        request_factory: HttpxEventSourceRequestFactory,
        *,
        options: EventStreamOptions | None = None,
    ) -> HttpxEventSource:
        """Create an event source from a reconnectable native request factory."""
        from ._sse import HttpxEventSource

        source = HttpxEventSource(self, request_factory, options=options)
        self._event_sources.add(source)
        return source

    def _event_source_closed(self, source: HttpxEventSource) -> None:
        self._event_sources.discard(source)

    def _event_source_started(self, source: HttpxEventSource) -> None:
        self._event_sources.add(source)

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
        """Close owned event sources and the internal HTTPX client exactly once.

        Calls from an owned EventSource's adapter, native hook, or response-cleanup
        callback request shutdown without joining their own run. External callers
        await completion.
        """
        if self._cleanup is None:
            self._closed = True
            sources = tuple(self._event_sources)
            for source in sources:
                source.close()
            self._cleanup = asyncio.create_task(self._close())
        if any(source._owns_current_work() for source in self._event_sources):
            return
        await complete(self._cleanup)

    async def _close(self) -> None:
        sources = tuple(self._event_sources)
        for source in sources:
            await source._wait_closed()
        if self._owns_client:
            await self.client.aclose()

    async def __aenter__(self) -> HttpxTransport:
        """Enter an asynchronous transport lifecycle scope."""
        if self._closed:
            raise TransportError("HttpxTransport is closed")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close transport-owned resources when leaving a lifecycle scope."""
        del exc_type, traceback
        try:
            await self.aclose()
        except BaseException:
            if exc_value is None:
                raise
            exc_value.add_note("Closing the HTTP transport also failed")


from ._sse import HttpxEventSource as HttpxEventSource  # noqa: E402
from ._sse import HttpxEventStream as HttpxEventStream  # noqa: E402
