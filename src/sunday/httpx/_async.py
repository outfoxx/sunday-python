# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import Any

import anyio

from ..request_cancellation_error import RequestCancellationError

_operation_task: ContextVar[asyncio.Task[Any] | None] = ContextVar("sunday_httpx_operation_task", default=None)


def in_operation() -> bool:
    """Return whether this task already owns a protected native I/O operation."""
    return _operation_task.get() is asyncio.current_task()


async def _wait_done(task: asyncio.Task[Any]) -> asyncio.CancelledError | None:
    failure: asyncio.CancelledError | None = None
    with anyio.CancelScope(shield=True):
        while not task.done():
            try:
                await asyncio.wait((task,))
            except asyncio.CancelledError as cancellation:
                if failure is None:
                    failure = cancellation
    try:
        await anyio.lowlevel.checkpoint_if_cancelled()
    except asyncio.CancelledError as cancellation:
        if failure is None:
            failure = cancellation
    return failure


async def complete[T](task: asyncio.Task[T]) -> T:
    """Join shared cleanup without forwarding cancellation, then rethrow the first cancellation."""
    failure = await _wait_done(task)
    try:
        result = task.result()
    except BaseException:
        if failure is None:
            raise
        failure.add_note("Cleanup also failed")
    if failure is not None:
        raise failure
    return result


async def run_io[T](
    operation: Callable[[], Awaitable[T]], *, discard: Callable[[T], Awaitable[None]] | None = None
) -> T:
    """Cancel native I/O through an AnyIO scope, allowing its shielded cleanup to finish.

    Raw repeated Task.cancel() calls bypass HTTPX/httpcore's AnyIO shields. The
    caller waits separately so only the scope reaches native I/O. Nested calls on
    the owner task run directly, keeping bulk reads free of per-chunk tasks.
    """
    if in_operation():
        return await operation()

    scope = anyio.CancelScope()

    async def run() -> T:
        token = _operation_task.set(asyncio.current_task())
        try:
            with scope:
                return await operation()
            raise asyncio.CancelledError()
        finally:
            _operation_task.reset(token)

    task = asyncio.create_task(run())
    try:
        await asyncio.wait((task,))
        return task.result()
    except RequestCancellationError:
        raise
    except asyncio.CancelledError as error:
        scope.cancel()
        with anyio.CancelScope(shield=True):
            await _wait_done(task)
            try:
                result = task.result()
            except RequestCancellationError:
                raise
            except asyncio.CancelledError:
                pass
            except BaseException:
                error.add_note("Native I/O also failed while cancelling")
            else:
                # Cancellation can race with successful response delivery. Ownership
                # must be released here when the caller will never receive the result.
                if discard is not None:
                    try:
                        await discard(result)
                    except BaseException:
                        error.add_note("Closing the undelivered result also failed")
        raise error
