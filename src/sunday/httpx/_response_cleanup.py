# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextvars import ContextVar

import httpx

from ._async import complete, in_operation, run_io

_cleanup_owners: ContextVar[tuple[_ClosingStream, ...]] = ContextVar("sunday_httpx_cleanup_owners", default=())


class _ClosingStream(httpx.AsyncByteStream):
    """Protect native cleanup, including closure initiated by HTTPX at EOF."""

    def __init__(self, stream: httpx.AsyncByteStream) -> None:
        self._stream = stream
        self._cleanup: asyncio.Task[None] | None = None

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self._stream.__aiter__() if in_operation() else self._iterate()

    async def _iterate(self) -> AsyncIterator[bytes]:
        iterator = self._stream.__aiter__()
        while True:
            try:
                chunk = await run_io(lambda: anext(iterator))
            except StopAsyncIteration:
                return
            yield chunk

    def owns_cleanup(self) -> bool:
        return self in _cleanup_owners.get() and (self._cleanup is None or not self._cleanup.done())

    async def aclose(self) -> None:
        if self.owns_cleanup():
            return
        if self._cleanup is None:
            self._cleanup = asyncio.create_task(self._close())
        await complete(self._cleanup)

    async def _close(self) -> None:
        # Trace callbacks (and their child tasks) cannot join the cleanup that
        # needs them to return. Completed cleanup still exposes its result.
        token = _cleanup_owners.set((*_cleanup_owners.get(), self))
        try:
            await self._stream.aclose()
        finally:
            _cleanup_owners.reset(token)


def in_response_cleanup(response: httpx.Response) -> bool:
    """Return whether this callback belongs to the response's active cleanup."""
    return isinstance(response.stream, _ClosingStream) and response.stream.owns_cleanup()


def protect_response(response: httpx.Response) -> None:
    """Keep explicit and automatic response closure on the same protected path."""
    if not response.is_closed and not isinstance(response.stream, _ClosingStream):
        assert isinstance(response.stream, httpx.AsyncByteStream)
        response.stream = _ClosingStream(response.stream)


async def close_response(response: httpx.Response, error: BaseException | None = None) -> None:
    """Finish response cleanup without replacing an existing failure."""
    try:
        protect_response(response)
        if response.is_closed and isinstance(response.stream, _ClosingStream):
            # HTTPX sets is_closed before awaiting the stream's cleanup.
            await response.stream.aclose()
        else:
            await response.aclose()
    except BaseException:
        if error is None:
            raise
        error.add_note("Closing the HTTP response also failed")
