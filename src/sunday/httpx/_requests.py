# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Mapping, Sequence
from contextlib import suppress

import httpx

from ..streaming import StreamingBody


def copy_request(
    request: httpx.Request,
    *,
    method: str | None = None,
    url: str | httpx.URL | None = None,
    headers: httpx.Headers | Mapping[str, str] | Sequence[tuple[str, str]] | None = None,
    content: bytes | None = None,
) -> httpx.Request:
    """Copy native fields without consuming a body; unchanged streams remain shared.

    Pass ``content=b""`` to remove a body. Timeout and other native extensions are
    retained. Changing authority updates Host unless ``headers`` explicitly supplies
    it. Unchanged streams retain their framing unless explicitly overridden.
    A caller consuming a shared stream must replace it before returning.
    """
    target_url = request.url if url is None else httpx.URL(url)
    copied_headers = httpx.Headers(request.headers if headers is None else headers)
    # Preserving a stream bypasses HTTPX's automatic Host header population.
    if "host" not in copied_headers or (headers is None and target_url.netloc != request.url.netloc):
        copied_headers["host"] = target_url.netloc.decode("ascii")
    if content is not None:
        copied_headers.pop("content-length", None)
        copied_headers.pop("transfer-encoding", None)
    else:
        with suppress(httpx.RequestNotRead):
            content = request.content
    if content is not None:
        return httpx.Request(
            method or request.method,
            target_url,
            headers=copied_headers,
            content=content,
            extensions=dict(request.extensions),
        )
    if "content-length" not in copied_headers and "transfer-encoding" not in copied_headers:
        if "content-length" in request.headers:
            copied_headers["content-length"] = request.headers["content-length"]
        else:
            copied_headers["transfer-encoding"] = request.headers.get("transfer-encoding", "chunked")
    return httpx.Request(
        method or request.method,
        target_url,
        headers=copied_headers,
        stream=request.stream,
        extensions=dict(request.extensions),
    )


class BodyStream(httpx.AsyncByteStream):
    """Let HTTPX consume a fresh Sunday body factory for each transmission."""

    def __init__(self, body: StreamingBody) -> None:
        self.body = body

    async def __aiter__(self) -> AsyncIterator[bytes]:
        content = self.body.content()
        if isinstance(content, bytes):
            yield content
        elif isinstance(content, AsyncIterable):
            async for chunk in content:
                yield chunk
        else:
            for chunk in content:
                yield chunk


def is_credentialed(request: httpx.Request) -> bool:
    return "authorization" in request.headers or "cookie" in request.headers


def redirect_request(prepared: httpx.Request, sent: httpx.Request, response: httpx.Response) -> httpx.Request | None:
    """Apply HTTPX's method semantics to adapted redirect headers without consuming bodies."""
    if is_credentialed(sent) or response.status_code not in {301, 302, 303, 307, 308}:
        return None
    if "location" not in response.headers:
        return None
    url = sent.url.join(response.headers["location"])
    if url.scheme not in {"http", "https"}:
        return None
    if not url.fragment and sent.url.fragment:
        url = url.copy_with(fragment=sent.url.fragment)
    get = (response.status_code in {302, 303} and sent.method != "HEAD") or (
        response.status_code == 301 and sent.method == "POST"
    )
    if not get and (
        not isinstance(sent.stream, (httpx.ByteStream, BodyStream))
        or not isinstance(prepared.stream, (httpx.ByteStream, BodyStream))
    ):
        return None
    headers = httpx.Headers(prepared.headers)
    headers.pop("host", None)
    headers.pop("cookie", None)
    if get:
        for name in ("content-type", "content-length", "transfer-encoding"):
            headers.pop(name, None)
    return copy_request(
        prepared, method="GET" if get else prepared.method, url=url, headers=headers, content=b"" if get else None
    )
