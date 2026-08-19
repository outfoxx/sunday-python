# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from enum import StrEnum

import httpx
import pytest
from pydantic import BaseModel

from sunday.httpx import HttpxTransport
from sunday.httpx_compat import Operation, StreamingOperation, as_transport, json_body, parameter_map, path_template


class Payload(BaseModel):
    value: int


class State(StrEnum):
    READY = "ready"


@pytest.mark.anyio
async def test_compatibility_operation_and_helpers() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"Content-Type": "application/json"}, json={"value": 1})

    async with httpx.AsyncClient(
        base_url="https://api.example.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        transport = as_transport(client)
        request = transport.client.build_request("GET", "/value")
        operation = Operation(transport, request, lambda response: response.json()["value"])

        assert await operation.execute() == 1
        response = await operation.response()
        assert response.status == 200
        assert response.headers.get("content-type") == "application/json"
        assert operation.transport_request() is request
        assert (await operation.transport_response()).status_code == 200
        assert isinstance(as_transport(transport), HttpxTransport)

        streaming = StreamingOperation(
            transport,
            lambda: transport.client.build_request("POST", "/value", content=b"value"),
            lambda response: response.json()["value"],
        )
        assert streaming.transport_request().method == "POST"
        assert (await streaming.transport_response()).status_code == 200
        assert (await streaming.response()).result == 1
        assert await streaming.execute() == 1

    assert json_body(None) is None
    assert json_body(Payload(value=1)) == {"value": 1}
    assert parameter_map({"enabled": True, "state": State.READY, "missing": None}) == {
        "enabled": "true",
        "state": "ready",
    }
    assert path_template("/projects/{id}", {"id": "a/b"}) == "/projects/a%2Fb"
