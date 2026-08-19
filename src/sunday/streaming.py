# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import AsyncIterable, Callable, Iterable
from dataclasses import dataclass

type StreamingBodyChunk = bytes | bytearray | memoryview
type StreamingBodyContent = bytes | Iterable[bytes] | AsyncIterable[bytes]


@dataclass(frozen=True, slots=True)
class StreamingBody:
    """Reusable streaming request body backed by a fresh content factory."""

    factory: Callable[[], StreamingBodyContent]

    @classmethod
    def bytes(cls, data: StreamingBodyChunk) -> StreamingBody:
        """Create a reusable body from in-memory bytes."""
        content = bytes(data)
        return cls(lambda: content)

    @classmethod
    def iterable(cls, factory: Callable[[], Iterable[StreamingBodyChunk]]) -> StreamingBody:
        """Create a reusable body from a fresh synchronous iterable."""

        def content() -> Iterable[bytes]:
            return (bytes(chunk) for chunk in factory())

        return cls(content)

    @classmethod
    def async_iterable(cls, factory: Callable[[], AsyncIterable[StreamingBodyChunk]]) -> StreamingBody:
        """Create a reusable body from a fresh asynchronous iterable."""

        async def content() -> AsyncIterable[bytes]:
            async for chunk in factory():
                yield bytes(chunk)

        return cls(content)

    def content(self) -> StreamingBodyContent:
        """Create content for one request attempt."""
        return self.factory()
