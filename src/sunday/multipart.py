# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Iterable, Sequence
from dataclasses import dataclass
from uuid import uuid4

from .media import MediaType
from .streaming import StreamingBody

type MultipartContent = str | bytes | bytearray | memoryview | StreamingBody


@dataclass(frozen=True, slots=True)
class MultipartPart:
    """One reusable multipart field or file."""

    name: str
    content: MultipartContent
    filename: str | None = None
    content_type: MediaType | None = None
    headers: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True, init=False)
class MultipartBody:
    """A reusable multipart body with a stable boundary."""

    parts: tuple[MultipartPart, ...]
    boundary: str

    def __init__(self, parts: Sequence[MultipartPart], boundary: str | None = None) -> None:
        actual_boundary = boundary or f"sunday-{uuid4().hex}"
        if not actual_boundary or any(character in actual_boundary for character in '\r\n"'):
            raise ValueError("Multipart boundaries must not contain quotes or newlines")
        object.__setattr__(self, "parts", tuple(parts))
        object.__setattr__(self, "boundary", actual_boundary)

    @property
    def content_type(self) -> MediaType:
        """Return the multipart media type including this body's boundary."""
        return MediaType(f'multipart/form-data; boundary="{self.boundary}"')

    def content(self) -> AsyncIterable[bytes]:
        """Create a fresh asynchronous content stream."""
        return self._content()

    async def _content(self) -> AsyncIterator[bytes]:
        for part in self.parts:
            yield f"--{self.boundary}\r\n".encode()
            disposition = f'form-data; name="{_quoted(part.name)}"'
            if part.filename is not None:
                disposition += f'; filename="{_quoted(part.filename)}"'
            yield f"Content-Disposition: {disposition}\r\n".encode()
            if part.content_type is not None:
                yield f"Content-Type: {part.content_type}\r\n".encode()
            for name, value in part.headers:
                if "\r" in name or "\n" in name or "\r" in value or "\n" in value:
                    raise ValueError("Multipart headers must not contain newlines")
                yield f"{name}: {value}\r\n".encode()
            yield b"\r\n"
            async for chunk in _content_chunks(part.content):
                yield chunk
            yield b"\r\n"
        yield f"--{self.boundary}--\r\n".encode()


async def _content_chunks(content: MultipartContent) -> AsyncIterator[bytes]:
    if isinstance(content, str):
        yield content.encode()
        return
    if isinstance(content, (bytes, bytearray, memoryview)):
        yield bytes(content)
        return
    value = content.content()
    if isinstance(value, bytes):
        yield value
    elif isinstance(value, AsyncIterable):
        async for chunk in value:
            yield chunk
    else:
        iterable: Iterable[bytes] = value
        for chunk in iterable:
            yield chunk


def _quoted(value: str) -> str:
    if "\r" in value or "\n" in value:
        raise ValueError("Multipart names and filenames must not contain newlines")
    return value.replace("\\", "\\\\").replace('"', '\\"')
