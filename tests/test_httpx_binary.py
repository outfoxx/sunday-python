# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import httpx
import pytest
from pydantic import Base64Bytes, TypeAdapter

from sunday import (
    MediaType,
    MediaTypeDecoders,
    Operation,
    OperationSpec,
    Problem,
    RequestEncodingError,
    RequestSpec,
    ResponseHeaderSpec,
    ResponseSpec,
    SundayModel,
    UnexpectedResponse,
)
from sunday.httpx import HttpxTransport


@pytest.mark.anyio
@pytest.mark.parametrize("wildcard", [False, True])
@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize(
    ("media_type", "body"),
    [
        ("application/octet-stream", b"\x00\xff"),
        ("image/png", b"\x89PNG\r\n\x1a\n"),
        ("application/pdf", b"%PDF-1.7\n\x00\xff"),
        ("audio/mpeg", b"\x00\xff"),
        ("video/mp4", b"\x00\xff"),
        ("application/vnd.example.asset", b"\x00\xff"),
        ("application/json", b'{ "asset": true }\n'),
        ("text/plain; charset=iso-8859-1", b"caf\xe9"),
    ],
)
async def test_binary_response_preserves_bytes_and_metadata(
    media_type: str, body: bytes, empty: bool, wildcard: bool
) -> None:
    if empty:
        body = b""
    content_types = (MediaType("application/octet-stream"), MediaType("*/*")) if wildcard else (MediaType(media_type),)

    def handler(_request: httpx.Request) -> httpx.Response:
        assert _request.headers["Accept"] == ", ".join(map(str, content_types))
        return httpx.Response(200, content=body, headers={"Content-Type": media_type, "X-Count": "42"})

    async with httpx.AsyncClient(base_url="https://example.test", transport=httpx.MockTransport(handler)) as client:
        operation: Operation[bytes, httpx.Request, httpx.Response] = Operation(
            HttpxTransport(client),
            OperationSpec(
                RequestSpec("GET", "/asset", accept_types=content_types),
                (
                    ResponseSpec(
                        200,
                        content_types,
                        TypeAdapter(bytes).validate_python,
                        headers=(ResponseHeaderSpec("X-Count", int, required=True),),
                        binary_body=True,
                    ),
                ),
            ),
        )
        result = await operation.execute()
        assert type(result) is bytes and result == body
        response = await operation.response()
        assert type(response.result) is bytes and response.result == body
        assert response.status == 200
        assert response.content_type == MediaType(media_type)
        assert response.get_header("X-Count") == "42"
        assert response.decoded_header("X-Count") == 42
        assert response.transport_response.content == body
        assert response.transport_response.is_closed


@pytest.mark.anyio
@pytest.mark.parametrize("body", [b"", b"\x00\xff"])
@pytest.mark.parametrize(
    ("status", "media_type", "message"),
    [
        (201, "image/png", "status"),
        (200, "application/json", "media type|Expected a response body"),
        (200, None, "media type|Expected a response body"),
    ],
)
async def test_binary_response_enforces_contract(
    status: int, media_type: str | None, message: str, body: bytes
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=body, headers={"Content-Type": media_type} if media_type else {})

    async with httpx.AsyncClient(base_url="https://example.test", transport=httpx.MockTransport(handler)) as client:
        operation: Operation[object, httpx.Request, httpx.Response] = Operation(
            HttpxTransport(client),
            OperationSpec(
                RequestSpec("GET", "/asset"),
                (ResponseSpec(200, (MediaType("image/png"),), binary_body=True),),
            ),
        )
        for call in (operation.execute, operation.response):
            with pytest.raises(UnexpectedResponse, match=message):
                await call()


@pytest.mark.anyio
async def test_binary_response_keeps_problem_and_structured_model_decoding() -> None:
    class Asset(SundayModel):
        data: Base64Bytes

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/error":
            return httpx.Response(
                404, json={"status": 404, "title": "Missing"}, headers={"Content-Type": "application/problem+json"}
            )
        return httpx.Response(200, json={"data": "Y2Fm6Q=="})

    async with httpx.AsyncClient(base_url="https://example.test", transport=httpx.MockTransport(handler)) as client:
        transport = HttpxTransport(client)
        binary: Operation[bytes, httpx.Request, httpx.Response] = Operation(
            transport,
            OperationSpec(
                RequestSpec("GET", "/error"),
                (ResponseSpec(None, (MediaType("*/*"),), TypeAdapter(bytes).validate_python, binary_body=True),),
            ),
        )
        for call in (binary.execute, binary.response):
            with pytest.raises(Problem) as raised:
                await call()
            assert raised.value.status == 404
        structured: Operation[Asset, httpx.Request, httpx.Response] = Operation(
            transport,
            OperationSpec(
                RequestSpec("GET", "/model"),
                (ResponseSpec(200, (MediaType("application/json"),), TypeAdapter(Asset).validate_python),),
            ),
        )
        assert (await structured.execute()).data == b"caf\xe9"


@pytest.mark.anyio
async def test_binary_selection_is_response_specific_and_preserves_adapters() -> None:
    calls = 0

    async def adapt(_transport: HttpxTransport, request: httpx.Request) -> httpx.Request:
        request.headers["Authorization"] = "Bearer test-token"
        return request

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        assert request.headers["Authorization"] == "Bearer test-token"
        assert request.headers["Accept"] == "image/png, application/json"
        calls += 1
        if calls % 2:
            return httpx.Response(200, content=b"\x00\xff", headers={"Content-Type": "image/png"})
        return httpx.Response(201, json={"value": 42})

    async with httpx.AsyncClient(base_url="https://example.test", transport=httpx.MockTransport(handler)) as client:
        operation: Operation[object, httpx.Request, httpx.Response] = Operation(
            HttpxTransport(client, adapters=(adapt,)),
            OperationSpec(
                RequestSpec("GET", "/asset", accept_types=(MediaType("image/png"), MediaType("application/json"))),
                (
                    ResponseSpec(200, (MediaType("image/png"),), binary_body=True),
                    ResponseSpec(201, (MediaType("application/json"),)),
                ),
            ),
        )
        for _ in range(2):
            assert await operation.execute() == b"\x00\xff"
            assert (await operation.response()).result == {"value": 42}


def test_operation_updates_binary_negotiation_when_responses_are_replaced() -> None:
    from dataclasses import replace

    request: RequestSpec[None] = RequestSpec("GET", "/asset", accept_types=(MediaType("application/pdf"),))
    operation: OperationSpec[None, object] = OperationSpec(request, (ResponseSpec(200, binary_body=True),))
    assert operation.request.binary_accept_types == (MediaType("*/*"),)
    assert request.binary_accept_types == ()
    structured = replace(operation, responses=(ResponseSpec(200),))
    assert structured.request.binary_accept_types == ()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("accept", "declared", "actual", "overlaps"),
    [
        ("image/*", "image/png", "image/png", True),
        ("*/*", "image/png", "image/png", True),
        ("image/png", "image/*", "image/png", True),
        ("image/png", "*/*", "image/png", True),
        ("image/*", "image/*", "image/png", True),
        ("application/*+json", "application/vnd.asset+json", "application/vnd.asset+json", True),
        ("application/vnd.asset+json", "application/*+json", "application/vnd.asset+json", True),
        ("application/*", "application/*+json", "application/vnd.asset+json", True),
        ("application/*+json", "application/*", "application/vnd.asset+json", True),
        ("image/*", "audio/mpeg", "audio/mpeg", False),
        ("image/png", "image/jpeg", "image/jpeg", False),
        ("application/*+json", "application/*+xml", "application/vnd.asset+xml", False),
    ],
)
async def test_binary_accept_negotiation_matches_overlapping_ranges(
    accept: str, declared: str, actual: str, overlaps: bool
) -> None:
    requests = 0
    body = b"\x00\xff"

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        assert request.headers["Accept"] == accept
        return httpx.Response(200, content=body, headers={"Content-Type": actual})

    async with httpx.AsyncClient(base_url="https://example.test", transport=httpx.MockTransport(handler)) as client:
        operation: Operation[bytes, httpx.Request, httpx.Response] = Operation(
            HttpxTransport(client, decoders=MediaTypeDecoders()),
            OperationSpec(
                RequestSpec("GET", "/asset", accept_types=(MediaType(accept),)),
                (ResponseSpec(200, (MediaType(declared),), TypeAdapter(bytes).validate_python, binary_body=True),),
            ),
        )
        if overlaps:
            assert await operation.execute() == body
            assert (await operation.response()).result == body
            assert requests == 2
        else:
            for call in (operation.execute, operation.response):
                with pytest.raises(RequestEncodingError, match="No response decoder"):
                    await call()
            assert requests == 0
