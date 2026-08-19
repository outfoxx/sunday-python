# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import inspect
from collections.abc import AsyncIterable, Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from types import TracebackType

type StreamingBodyChunk = bytes | bytearray | memoryview
type StreamingBodyContent = bytes | Iterable[bytes] | AsyncIterable[bytes]
type StreamingBodyClose = Callable[[], Awaitable[None] | None]


@dataclass(slots=True)
class StreamingBody:
    """Reusable streaming request body backed by a fresh content factory."""

    factory: Callable[[], StreamingBodyContent]
    close_callback: StreamingBodyClose | None = None
    _closed: bool = field(default=False, init=False, repr=False)

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
        if self._closed:
            raise RuntimeError("StreamingBody is closed")
        return self.factory()

    async def aclose(self) -> None:
        """Close the body and its optional backing resource exactly once."""
        if self._closed:
            return
        self._closed = True
        if self.close_callback is not None:
            result = self.close_callback()
            if inspect.isawaitable(result):
                await result

    async def __aenter__(self) -> StreamingBody:
        """Enter an asynchronous body lifecycle scope."""
        if self._closed:
            raise RuntimeError("StreamingBody is closed")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the body when leaving an asynchronous lifecycle scope."""
        del exc_type, exc_value, traceback
        await self.aclose()
