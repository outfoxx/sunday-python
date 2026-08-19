# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import AsyncIterable, Iterable, Sequence
from datetime import date
from typing import Any

import pytest
from pydantic import Field

from sunday import (
    BinaryCodec,
    FormUrlEncodedCodec,
    JsonCodec,
    MediaType,
    MediaTypeDecoders,
    MediaTypeEncoders,
    NullableOperation,
    NullifySpec,
    Operation,
    OperationResponse,
    OperationSpec,
    Problem,
    ProblemPayload,
    RequestSpec,
    ResponseHeaders,
    ResponseSpec,
    StreamingBody,
    StreamingOperation,
    SundayModel,
    TextCodec,
)
from sunday.cbor import CborCodec


class Payload(SundayModel):
    created_at: date = Field(alias="created-at")


class MissingProblem(Problem):
    pass


class FakeTransport:
    def __init__(self, result: object = "result", problem: Problem | None = None) -> None:
        self.result = result
        self.problem = problem
        self.requests = 0

    async def build_request(self, spec: RequestSpec[Any]) -> tuple[int, RequestSpec[Any]]:
        self.requests += 1
        return self.requests, spec

    async def send(self, request: object, *, stream: bool = False) -> object:
        return request, stream

    async def decode_response(
        self,
        response: object,
        responses: Sequence[ResponseSpec[Any]],
    ) -> OperationResponse[Any]:
        del responses
        if self.problem is not None:
            raise self.problem
        return OperationResponse(
            self.result,
            response,
            200,
            ResponseHeaders.from_items([("Content-Type", "application/json")]),
        )


def test_builtin_codecs_and_registries() -> None:
    payload = Payload(created_at=date(2026, 8, 18))
    json_codec = JsonCodec()
    json_bytes = json_codec.encode(payload)

    assert json_bytes == b'{"created-at":"2026-08-18"}'
    assert json_codec.decode(json_bytes, MediaType("application/problem+json")) == {"created-at": "2026-08-18"}
    assert TextCodec().decode("héllo".encode(), MediaType("text/plain; charset=utf-8")) == "héllo"
    assert BinaryCodec().encode(memoryview(b"abc")) == b"abc"
    assert BinaryCodec().decode(b"abc", MediaType("application/octet-stream")) == b"abc"
    assert FormUrlEncodedCodec().encode({"tag": ["one", "two"]}) == b"tag=one&tag=two"
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


@pytest.mark.anyio
async def test_operation_builds_fresh_requests_and_returns_metadata() -> None:
    transport = FakeTransport()
    spec: OperationSpec[None, str] = OperationSpec(RequestSpec("GET", "/projects"), (ResponseSpec(200),))
    operation = Operation[str](transport, spec)

    assert await operation.execute() == "result"
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
    operation = StreamingOperation[str](
        transport,
        OperationSpec(RequestSpec("POST", "/upload", body=sync_body), (ResponseSpec(200),)),
    )
    await operation.execute()
    await operation.execute()
    assert transport.requests == 2


@pytest.mark.anyio
async def test_nullable_operation_matches_status_and_problem_type() -> None:
    status_problem = Problem(ProblemPayload(status=404, title="Missing"))
    status_operation = NullableOperation[str](
        FakeTransport(problem=status_problem),
        OperationSpec(RequestSpec("GET", "/missing"), (ResponseSpec(200),)),
        NullifySpec(statuses=(404,)),
    )
    assert await status_operation.execute_or_none() is None
    assert await status_operation.response_or_none() is None

    typed_problem = MissingProblem(ProblemPayload(status=409))
    typed_operation = NullableOperation[str](
        FakeTransport(problem=typed_problem),
        OperationSpec(RequestSpec("GET", "/missing"), (ResponseSpec(200),)),
        NullifySpec(problem_types=(MissingProblem,)),
    )
    assert await typed_operation.execute_or_none() is None

    unmatched = NullableOperation[str](
        FakeTransport(problem=status_problem),
        OperationSpec(RequestSpec("GET", "/missing"), (ResponseSpec(200),)),
        NullifySpec(statuses=(409,)),
    )
    with pytest.raises(Problem):
        await unmatched.execute_or_none()
