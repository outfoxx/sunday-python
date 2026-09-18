# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from typing import Any

import pytest
from litestar import Litestar, Request, post
from litestar.testing import TestClient

from sunday.litestar import request_bytes


@pytest.mark.parametrize(
    ("media_types", "content_type", "status"),
    [
        (["*/*"], "image/png", 201),
        (["*/*"], "text/plain", 201),
        (["*/*"], None, 201),
        (["image/*"], "image/png", 201),
        (["image/*"], "IMAGE/JPEG", 201),
        (["image/*"], 'image/png; profile="example;profile"', 201),
        (["image/*"], "application/octet-stream", 415),
        (["image/*"], "text/plain", 415),
        (["image/*"], None, 415),
        (["application/octet-stream"], None, 201),
        (["application/octet-stream"], "image/png", 415),
        (["image/png", "image/jpeg"], "image/jpeg", 201),
        (["image/png", "image/jpeg"], "image/webp", 415),
        (["application/*+json"], "application/vnd.example+json", 201),
        ([], "text/plain", 201),
        (["*/*"], "invalid", 400),
        (["*/*"], "image/png/extra", 400),
        (["*/*"], "image/*", 400),
        (["*/*"], "image/png; invalid", 400),
        (["*/*"], 'image/png; profile="unterminated', 400),
    ],
)
def test_request_bytes_enforces_all_media_ranges(media_types: list[str], content_type: str | None, status: int) -> None:
    received: list[bytes] = []

    @post("/upload")
    async def upload(request: Request[Any, Any, Any]) -> dict[str, str | None]:
        received.append(await request_bytes(request, media_types))
        return {"content_type": request.headers.get("content-type")}

    body = b'\x00\xff{"raw": "bytes"}\r\n'
    with TestClient(Litestar(route_handlers=[upload])) as client:
        response = client.post("/upload", content=body, headers={"content-type": content_type} if content_type else {})

    assert response.status_code == status
    assert received == ([body] if status == 201 else [])
    if status == 201:
        assert response.json() == {"content_type": content_type}


@pytest.mark.parametrize("body", [b"", b'"YWJj"', b"null", b"[1,2,3]"])
def test_request_bytes_preserves_empty_and_json_looking_bodies(body: bytes) -> None:
    @post("/upload")
    async def upload(request: Request[Any, Any, Any]) -> str:
        return (await request_bytes(request, ["*/*"])).hex()

    with TestClient(Litestar(route_handlers=[upload])) as client:
        response = client.post("/upload", content=body, headers={"content-type": "application/json"})

    assert response.status_code == 201
    assert response.text == body.hex()


def test_request_bytes_preserves_litestar_body_size_limit() -> None:
    @post("/upload")
    async def upload(request: Request[Any, Any, Any]) -> None:
        await request_bytes(request, ["*/*"])

    with TestClient(Litestar(route_handlers=[upload], request_max_body_size=2)) as client:
        response = client.post("/upload", content=b"large")

    assert response.status_code == 413
