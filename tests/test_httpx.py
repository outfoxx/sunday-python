# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest
from pydantic import TypeAdapter

from sunday import (
    MediaType,
    Operation,
    OperationSpec,
    ParameterLocation,
    ParameterSpec,
    Problem,
    ProblemPayload,
    RequestSpec,
    ResponseSpec,
    StreamingBody,
    UnexpectedResponse,
)
from sunday.httpx import (
    HttpxTransport,
    RefreshingHeaderTokenAuthorizingAdapter,
    StaticHeaderTokenAuthorizingAdapter,
    TokenAuthorization,
    as_httpx_transport,
)


class ConflictPayload(ProblemPayload):
    type: str = "https://example.test/problems/conflict"
    conflicting_id: str


class ConflictProblem(Problem):
    payload_type = ConflictPayload


@pytest.mark.anyio
async def test_build_request_encodes_parameters_body_headers_and_cookies() -> None:
    async with httpx.AsyncClient(base_url="https://api.example.test") as client:
        transport = HttpxTransport(
            client,
            adapters=(StaticHeaderTokenAuthorizingAdapter("token"),),
        )
        request = await transport.build_request(
            RequestSpec(
                "POST",
                "/projects/{project-id}",
                parameters=(
                    ParameterSpec("project-id", "a/b", ParameterLocation.PATH),
                    ParameterSpec("tag", ["one", "two"], ParameterLocation.QUERY),
                    ParameterSpec("X-Trace", "trace-1", ParameterLocation.HEADER),
                    ParameterSpec("session", "abc", ParameterLocation.COOKIE),
                ),
                headers=(("X-Existing", "value"),),
                body={"name": "Roadmap"},
                content_types=(MediaType("application/json"),),
                accept_types=(MediaType("application/vnd.project+json"),),
            )
        )

    assert str(request.url) == "https://api.example.test/projects/a%2Fb?tag=one&tag=two"
    assert request.headers["x-trace"] == "trace-1"
    assert request.headers["cookie"] == "session=abc"
    assert request.headers["authorization"] == "Bearer token"
    assert request.headers["accept"] == "application/vnd.project+json"
    assert request.headers["content-type"] == "application/json"
    assert request.content == b'{"name":"Roadmap"}'


@pytest.mark.anyio
async def test_streaming_request_body_is_recreated_per_operation_attempt() -> None:
    bodies: list[bytes] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(await request.aread())
        return httpx.Response(204)

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        transport = HttpxTransport(client)
        operation = Operation[None](
            transport,
            OperationSpec(
                RequestSpec(
                    "POST",
                    "/upload",
                    body=StreamingBody.iterable(lambda: [b"one", b"two"]),
                    content_types=(MediaType("application/octet-stream"),),
                ),
                (ResponseSpec(204, body_expected=False),),
            ),
        )
        await operation.execute()
        await operation.execute()

    assert bodies == [b"onetwo", b"onetwo"]


@pytest.mark.anyio
async def test_response_decoding_selects_status_media_and_decoder() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"Content-Type": "application/vnd.project+json"}, json={"id": "one"})

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        transport = HttpxTransport(client)
        operation = Operation[dict[str, str]](
            transport,
            OperationSpec(
                RequestSpec("GET", "/projects/one"),
                (
                    ResponseSpec(
                        200,
                        (MediaType("application/*+json"),),
                        TypeAdapter(dict[str, str]).validate_python,
                    ),
                ),
            ),
        )
        response = await operation.response()

    assert response.result == {"id": "one"}
    assert response.status == 200
    assert response.get_header("content-type") == "application/vnd.project+json"


@pytest.mark.anyio
async def test_typed_problem_decoding_and_generic_fallback() -> None:
    responses = [
        httpx.Response(
            409,
            headers={"Content-Type": "application/problem+json"},
            json={
                "type": "https://example.test/problems/conflict",
                "title": "Conflict",
                "conflicting_id": "project-1",
            },
        ),
        httpx.Response(
            500,
            headers={"Content-Type": "application/problem+json"},
            json={"type": "https://example.test/problems/unknown", "title": "Unknown"},
        ),
    ]

    def handler(_request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        transport = HttpxTransport(client)
        transport.register_problem("https://example.test/problems/conflict", ConflictProblem)
        operation = Operation[None](
            transport,
            OperationSpec(RequestSpec("GET", "/projects/one"), (ResponseSpec(204, body_expected=False),)),
        )
        with pytest.raises(ConflictProblem) as conflict:
            await operation.execute()
        assert conflict.value.status == 409
        assert conflict.value.payload.conflicting_id == "project-1"  # type: ignore[attr-defined]

        with pytest.raises(Problem) as unknown:
            await operation.execute()
        assert type(unknown.value) is Problem
        assert unknown.value.status == 500


@pytest.mark.anyio
async def test_unexpected_responses_preserve_diagnostics() -> None:
    responses = [
        httpx.Response(200, content=b"value"),
        httpx.Response(200, headers={"Content-Type": "application/xml"}, content=b"<value />"),
        httpx.Response(400, headers={"Content-Type": "application/problem+json"}, content=b"not-json"),
    ]

    def handler(_request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        operation = Operation[str](
            HttpxTransport(client),
            OperationSpec(
                RequestSpec("GET", "/value"),
                (ResponseSpec(200, (MediaType("text/plain"),), str),),
            ),
        )
        with pytest.raises(UnexpectedResponse) as missing_type:
            await operation.execute()
        assert missing_type.value.status == 200
        assert missing_type.value.body == b"value"

        with pytest.raises(UnexpectedResponse) as unsupported:
            await operation.execute()
        assert unsupported.value.content_type == "application/xml"

        with pytest.raises(UnexpectedResponse):
            await operation.execute()


@pytest.mark.anyio
async def test_refreshing_authorization_caches_until_expiration() -> None:
    calls = 0

    async def provider() -> TokenAuthorization:
        nonlocal calls
        calls += 1
        return TokenAuthorization(f"token-{calls}", datetime.now(UTC) + timedelta(hours=1))

    async with httpx.AsyncClient(base_url="https://api.example.test") as client:
        adapter = RefreshingHeaderTokenAuthorizingAdapter(provider, scheme=None)
        transport = HttpxTransport(client, adapters=(adapter,))
        first = await transport.build_request(RequestSpec("GET", "/one"))
        second = await transport.build_request(RequestSpec("GET", "/two"))

    assert first.headers["authorization"] == "token-1"
    assert second.headers["authorization"] == "token-1"
    assert calls == 1


def test_as_httpx_transport_preserves_existing_transport() -> None:
    client = httpx.AsyncClient()
    transport = HttpxTransport(client)
    try:
        assert as_httpx_transport(transport) is transport
        assert as_httpx_transport(client).client is client
    finally:
        import asyncio

        asyncio.run(client.aclose())
