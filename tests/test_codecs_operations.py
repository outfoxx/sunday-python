# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import json
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable, Sequence
from datetime import UTC, date, datetime
from enum import StrEnum
from types import TracebackType
from typing import Any
from uuid import UUID

import pytest
from pydantic import Field

from sunday import (
    BaseTransport,
    BinaryCodec,
    EventSource,
    EventSourceState,
    EventStreamOptions,
    FormUrlEncodedCodec,
    JsonCodec,
    MediaType,
    MediaTypeDecoders,
    MediaTypeEncoders,
    MergePatch,
    MultipartBody,
    MultipartPart,
    NullableOperation,
    NullifySpec,
    Operation,
    OperationResponse,
    OperationSpec,
    PatchDocument,
    PatchOperation,
    PatchOperationKind,
    Problem,
    ProblemPayload,
    RequestSpec,
    ResponseHeaders,
    ResponseSpec,
    ServerSentEvent,
    StreamingBody,
    StreamingOperation,
    SundayModel,
    TextCodec,
    TransportError,
    WireMode,
)
from sunday.cbor import CborCodec


class Payload(SundayModel):
    created_at: date = Field(alias="created-at")


class MissingProblem(Problem):
    pass


class State(StrEnum):
    ACTIVE = "active"


class EmptyEventStream[EventT]:
    def __aiter__(self) -> AsyncIterator[EventT]:
        return self.events()

    async def events(self) -> AsyncIterator[EventT]:
        if False:
            yield  # pragma: no cover

    async def aclose(self) -> None:
        pass

    async def __aenter__(self) -> EmptyEventStream[EventT]:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback


class EmptyEventSource:
    ready_state = EventSourceState.CLOSED
    retry_time = 0.5
    on_open: Callable[[], None] | None = None
    on_error: Callable[[BaseException | None], None] | None = None
    on_message: Callable[[ServerSentEvent], None] | None = None

    def add_event_listener(self, event: str, handler: Callable[[ServerSentEvent], None]) -> UUID:
        del event, handler
        return UUID(int=0)

    def remove_event_listener(self, event: str, listener: UUID) -> None:
        del event, listener

    def connect(self) -> None:
        pass

    def close(self) -> None:
        pass


class FakeTransport(BaseTransport[tuple[int, RequestSpec[Any]], tuple[tuple[int, RequestSpec[Any]], bool]]):
    def __init__(self, result: object = "result", problem: Problem | None = None) -> None:
        self.result_value = result
        self.problem = problem
        self.requests = 0
        self.calls: list[str] = []
        self.registered_problems: dict[str, type[Problem]] = {}

    def register_problem(self, type_uri: str, problem_type: type[Problem]) -> None:
        self.registered_problems[type_uri] = problem_type

    async def _prepare_request(self, spec: RequestSpec[Any]) -> tuple[int, RequestSpec[Any]]:
        self.calls.append("transport_request")
        self.requests += 1
        return self.requests, spec

    async def _send(
        self,
        request: tuple[int, RequestSpec[Any]],
        *,
        stream: bool = False,
    ) -> tuple[tuple[int, RequestSpec[Any]], bool]:
        self.calls.append("transport_response")
        return request, stream

    async def _decode_response(
        self,
        response: tuple[tuple[int, RequestSpec[Any]], bool],
        responses: Sequence[ResponseSpec[Any]],
    ) -> OperationResponse[Any, tuple[tuple[int, RequestSpec[Any]], bool]]:
        self.calls.append("response")
        del responses
        if self.problem is not None:
            raise self.problem
        return OperationResponse(
            self.result_value,
            response,
            200,
            ResponseHeaders.from_items([("Content-Type", "application/json")]),
        )

    def event_stream[EventT](
        self,
        spec: RequestSpec[None],
        decoder: Callable[[ServerSentEvent], EventT | None],
        *,
        options: EventStreamOptions | None = None,
    ) -> EmptyEventStream[EventT]:
        del spec, decoder, options
        return EmptyEventStream()

    def event_source(
        self,
        spec: RequestSpec[None],
        *,
        options: EventStreamOptions | None = None,
    ) -> EventSource:
        del spec, options
        return EmptyEventSource()

    async def aclose(self) -> None:
        pass

    async def __aenter__(self) -> FakeTransport:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback


def test_builtin_codecs_and_registries() -> None:
    payload = Payload(created_at=date(2026, 8, 18))
    json_codec = JsonCodec()
    json_bytes = json_codec.encode(payload)

    assert json_bytes == b'{"created-at":"2026-08-18"}'
    assert json_codec.encode({"state": State.ACTIVE, "date": date(2026, 8, 18)}) == (
        b'{"state":"active","date":"2026-08-18"}'
    )
    assert json_codec.encode({"bytes": b"value", "set": {"one", "two"}}) in {
        b'{"bytes":"dmFsdWU=","set":["one","two"]}',
        b'{"bytes":"dmFsdWU=","set":["two","one"]}',
    }
    assert json_codec.encode(datetime(2026, 8, 18, 12, 30, tzinfo=UTC)) == b'"2026-08-18T12:30:00+00:00"'
    assert json_codec.decode(json_bytes, MediaType("application/problem+json")) == {"created-at": "2026-08-18"}
    assert TextCodec().decode("héllo".encode(), MediaType("text/plain; charset=utf-8")) == "héllo"
    assert BinaryCodec().encode(memoryview(b"abc")) == b"abc"
    assert BinaryCodec().decode(b"abc", MediaType("application/octet-stream")) == b"abc"
    assert FormUrlEncodedCodec().encode({"tag": ["one", "two"]}) == b"tag=one&tag=two"
    assert FormUrlEncodedCodec().encode(payload) == b"created-at=2026-08-18"
    assert FormUrlEncodedCodec().decode(
        b"tag=one&tag=two&empty=",
        MediaType("application/x-www-form-urlencoded"),
    ) == {"tag": ["one", "two"], "empty": [""]}

    encoders = MediaTypeEncoders.defaults()
    decoders = MediaTypeDecoders.defaults()
    assert isinstance(encoders.find(MediaType("application/vnd.test+json")), JsonCodec)
    assert isinstance(decoders.find(MediaType("text/csv")), TextCodec)
    assert encoders.find(MediaType("application/unknown")) is None

    encoders.register(CborCodec(), first=True)
    decoders.register(CborCodec(), first=True)
    cbor = encoders.find(MediaType("application/cbor"))
    assert cbor is not None
    encoded = cbor.encode({"value": 1})
    decoder = decoders.find(MediaType("application/cbor"))
    assert decoder is not None
    assert decoder.decode(encoded, MediaType("application/cbor")) == {"value": 1}


def test_codecs_reject_incompatible_values() -> None:
    with pytest.raises(TypeError):
        BinaryCodec().encode("not bytes")
    with pytest.raises(TypeError):
        FormUrlEncodedCodec().encode("not a mapping")


def test_patch_and_multipart_bodies_preserve_wire_semantics() -> None:
    class Update(SundayModel):
        name: str | None = None
        description: str | None = None

    update = Update(name=None)
    assert JsonCodec(wire_mode=WireMode.PATCH).encode(MergePatch(update)) == b'{"name":null}'

    patch = PatchDocument(
        (
            PatchOperation(PatchOperationKind.REMOVE, "/old"),
            PatchOperation(PatchOperationKind.ADD, "/name", "new"),
        )
    )
    assert JsonCodec().encode(patch) == b'[{"op":"remove","path":"/old"},{"op":"add","path":"/name","value":"new"}]'

    body = MultipartBody(
        (
            MultipartPart("metadata", "value"),
            MultipartPart(
                "file", StreamingBody.bytes(b"data"), filename="value.txt", content_type=MediaType("text/plain")
            ),
        ),
        boundary="test-boundary",
    )

    async def collect() -> bytes:
        return b"".join([chunk async for chunk in body.content()])

    import asyncio

    encoded = asyncio.run(collect())
    assert b'name="metadata"\r\n\r\nvalue' in encoded
    assert b'filename="value.txt"\r\nContent-Type: text/plain\r\n\r\ndata' in encoded
    assert encoded.endswith(b"--test-boundary--\r\n")


def test_patch_and_multipart_validation() -> None:
    with pytest.raises(ValueError, match="paths"):
        PatchOperation(PatchOperationKind.REMOVE, "invalid")
    with pytest.raises(ValueError, match="from_path"):
        PatchOperation(PatchOperationKind.MOVE, "/target")
    with pytest.raises(ValueError, match="value"):
        PatchOperation(PatchOperationKind.TEST, "/target")
    assert (
        JsonCodec().encode(PatchDocument((PatchOperation(PatchOperationKind.COPY, "/target", from_path="/source"),)))
        == b'[{"op":"copy","path":"/target","from":"/source"}]'
    )

    with pytest.raises(ValueError, match="boundaries"):
        MultipartBody((), boundary='bad"boundary')

    body = MultipartBody((MultipartPart("value", b"bytes", headers=(("X-Test", "bad\nvalue"),)),), boundary="valid")

    async def collect() -> bytes:
        return b"".join([chunk async for chunk in body.content()])

    import asyncio

    with pytest.raises(ValueError, match="headers"):
        asyncio.run(collect())


@pytest.mark.anyio
async def test_operation_builds_fresh_requests_and_returns_metadata() -> None:
    transport = FakeTransport()
    spec: OperationSpec[None, str] = OperationSpec(RequestSpec("GET", "/projects"), (ResponseSpec(200),))
    operation = Operation[
        str,
        tuple[int, RequestSpec[Any]],
        tuple[tuple[int, RequestSpec[Any]], bool],
    ](transport, spec)

    assert await operation.execute() == "result"
    assert transport.calls == ["transport_request", "transport_response", "response"]
    assert (await operation.response()).content_type == MediaType("application/json")
    assert await operation.transport_request() == (3, spec.request)
    assert await operation.transport_response() == ((4, spec.request), False)


@pytest.mark.anyio
async def test_streaming_body_creates_fresh_sync_and_async_content() -> None:
    sync_body = StreamingBody.iterable(lambda: [b"one", bytearray(b"two")])

    first = sync_body.content()
    second = sync_body.content()
    assert isinstance(first, Iterable)
    assert isinstance(second, Iterable)
    assert list(first) == [b"one", b"two"]
    assert list(second) == [b"one", b"two"]

    async def chunks() -> AsyncIterable[bytes]:
        yield b"one"
        yield b"two"

    async_body = StreamingBody.async_iterable(chunks)
    content = async_body.content()
    assert isinstance(content, AsyncIterable)
    assert [chunk async for chunk in content] == [b"one", b"two"]
    assert StreamingBody.bytes(b"value").content() == b"value"

    transport = FakeTransport()
    operation = StreamingOperation[
        str,
        tuple[int, RequestSpec[Any]],
        tuple[tuple[int, RequestSpec[Any]], bool],
    ](
        transport,
        OperationSpec(RequestSpec("POST", "/upload", body=sync_body), (ResponseSpec(200),)),
    )
    await operation.execute()
    await operation.execute()
    assert transport.requests == 2


@pytest.mark.anyio
async def test_streaming_body_lifecycle_closes_once() -> None:
    closes = 0

    async def close() -> None:
        nonlocal closes
        closes += 1

    body = StreamingBody(lambda: b"value", close)
    async with body:
        assert body.content() == b"value"
    await body.aclose()

    assert closes == 1
    with pytest.raises(TransportError):
        body.content()


@pytest.mark.anyio
async def test_nullable_operation_matches_status_and_problem_type() -> None:
    status_problem = Problem(ProblemPayload(status=404, title="Missing"))
    status_operation = NullableOperation[
        str,
        tuple[int, RequestSpec[Any]],
        tuple[tuple[int, RequestSpec[Any]], bool],
    ](
        FakeTransport(problem=status_problem),
        OperationSpec(RequestSpec("GET", "/missing"), (ResponseSpec(200),)),
        NullifySpec(statuses=(404,)),
    )
    assert await status_operation.execute_or_none() is None
    assert await status_operation.response_or_none() is None

    typed_problem = MissingProblem(ProblemPayload(status=409))
    typed_operation = NullableOperation[
        str,
        tuple[int, RequestSpec[Any]],
        tuple[tuple[int, RequestSpec[Any]], bool],
    ](
        FakeTransport(problem=typed_problem),
        OperationSpec(RequestSpec("GET", "/missing"), (ResponseSpec(200),)),
        NullifySpec(problem_types=(MissingProblem,)),
    )
    assert await typed_operation.execute_or_none() is None

    unmatched = NullableOperation[
        str,
        tuple[int, RequestSpec[Any]],
        tuple[tuple[int, RequestSpec[Any]], bool],
    ](
        FakeTransport(problem=status_problem),
        OperationSpec(RequestSpec("GET", "/missing"), (ResponseSpec(200),)),
        NullifySpec(statuses=(409,)),
    )
    with pytest.raises(Problem):
        await unmatched.execute_or_none()


@pytest.mark.parametrize("wire_mode", list(WireMode))
def test_json_codec_preserves_nullable_fields_and_omits_absent_non_nullable_fields(wire_mode: WireMode) -> None:
    class Presence(SundayModel):
        required_nullable: str | None = Field(alias="requiredNullable")
        optional_nullable: str | None = Field(default=None, alias="optionalNullable")
        optional_text: str | None = Field(default=None, alias="optionalText", exclude_if=lambda value: value is None)
        text: str = ""
        count: int = 0
        flag: bool = False
        items: list[str] = Field(default_factory=list)

    value = Presence(requiredNullable=None, optionalNullable=None, text="", count=0, flag=False, items=[])
    expected = {"requiredNullable": None, "optionalNullable": None, "text": "", "count": 0, "flag": False, "items": []}
    codec = JsonCodec(wire_mode=wire_mode)
    assert json.loads(codec.encode(value)) == expected
    assert json.loads(codec.encode({"nested": [value]})) == {"nested": [expected]}
    value.optional_text = "main"
    assert json.loads(codec.encode(value)) == dict(expected, optionalText="main")
