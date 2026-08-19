# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

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
    ResponseHeaderSpec,
    ResponseSpec,
    StreamingBody,
    TransportEvent,
    TransportEventKind,
    UnexpectedResponse,
)
from sunday.httpx import (
    HttpxTransport,
    as_httpx_transport,
)


class ConflictPayload(ProblemPayload):
    type: str = "https://example.test/problems/conflict"
    conflicting_id: str


class ConflictProblem(Problem):
    payload_type = ConflictPayload


class RecordingObserver:
    def __init__(self) -> None:
        self.events: list[TransportEvent] = []

    def observe(self, event: TransportEvent) -> None:
        self.events.append(event)


@pytest.mark.anyio
async def test_build_request_encodes_parameters_body_headers_and_cookies() -> None:
    async def bearer(_transport: HttpxTransport, request: httpx.Request) -> httpx.Request:
        request.headers["Authorization"] = "Bearer token"
        return request

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(204)

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        transport = HttpxTransport(
            client,
            adapters=(bearer,),
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
        assert "authorization" not in request.headers
        await transport.send(request)

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
    assert response.transport_response.is_closed


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
async def test_response_decodes_declared_headers() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers=[("Content-Type", "application/json"), ("X-Count", "42"), ("X-Tag", "a"), ("X-Tag", "b")],
            json={"ok": True},
        )

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        operation = Operation[dict[str, bool]](
            HttpxTransport(client),
            OperationSpec(
                RequestSpec("GET", "/value"),
                (
                    ResponseSpec(
                        200,
                        (MediaType("application/json"),),
                        TypeAdapter(dict[str, bool]).validate_python,
                        headers=(
                            ResponseHeaderSpec("X-Count", int, required=True),
                            ResponseHeaderSpec("X-Tag", repeated=True),
                        ),
                    ),
                ),
            ),
        )
        response = await operation.response()

    assert response.decoded_header("x-count") == 42
    assert response.decoded_header("X-Tag") == ("a", "b")


@pytest.mark.anyio
async def test_request_adapters_are_ordered_and_observation_is_redacted() -> None:
    observer = RecordingObserver()

    class FirstAdapter:
        async def adapt(self, _transport: HttpxTransport, request: httpx.Request) -> httpx.Request:
            request.headers["X-Order"] = "first"
            return request

    async def second(_transport: HttpxTransport, request: httpx.Request) -> None:
        request.headers["X-Order"] += ",second"
        request.headers["Authorization"] = "Bearer secret"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["X-Order"] == "first,second"
        assert request.headers["Authorization"] == "Bearer secret"
        return httpx.Response(204, headers={"Set-Cookie": "session=secret"})

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        transport = HttpxTransport(client, adapters=(FirstAdapter(), second), observers=(observer,))
        await transport.send(await transport.build_request(RequestSpec("GET", "/value")))
        await transport.aclose()
        await transport.aclose()
        with pytest.raises(RuntimeError):
            await transport.send(await transport.build_request(RequestSpec("GET", "/closed")))

    assert [event.kind for event in observer.events] == [TransportEventKind.REQUEST, TransportEventKind.RESPONSE]
    assert dict(observer.events[0].headers)["authorization"] == "<redacted>"
    assert dict(observer.events[1].headers)["set-cookie"] == "<redacted>"


def test_as_httpx_transport_preserves_existing_transport() -> None:
    client = httpx.AsyncClient()
    transport = HttpxTransport(client)
    try:
        assert as_httpx_transport(transport) is transport
        assert as_httpx_transport(client).client is client
    finally:
        import asyncio

        asyncio.run(client.aclose())
