# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

import anyio
import httpx

from ._response_cleanup import close_response, protect_response


class ResponseAdapter(ABC):
    """Adapt returned headers before Sunday reads the body or applies retry policy.

    Native recovery's intermediate responses are not delivered. The request is
    the effective request associated with the returned response. An adapter that
    consumes or replaces a resource must close it and provide its replacement.
    """

    @abstractmethod
    async def adapt(self, response: httpx.Response, request: httpx.Request) -> httpx.Response:
        """Return the response to handle; raising stops the remaining adapters."""

    def chain_to(self, next_adapter: ResponseAdapterLike) -> ResponseAdapters:
        """Compose response adapters independently of request adapters."""
        return ResponseAdapters(self, next_adapter)


type ResponseAdapterLike = ResponseAdapter | Callable[[httpx.Response, httpx.Request], Awaitable[httpx.Response]]


class ResponseAdapters(ResponseAdapter):
    """An immutable ordered chain of adapters or asynchronous functions."""

    def __init__(self, *adapters: ResponseAdapterLike) -> None:
        self.adapters: tuple[ResponseAdapterLike, ...] = tuple(
            child
            for adapter in adapters
            for child in (adapter.adapters if isinstance(adapter, ResponseAdapters) else (adapter,))
        )

    async def adapt(self, response: httpx.Response, request: httpx.Request) -> httpx.Response:
        """Run the chain, closing the current response if an adapter fails."""
        protect_response(response)
        try:
            for adapter in self.adapters:
                await anyio.lowlevel.checkpoint_if_cancelled()
                response = (
                    await adapter.adapt(response, request)
                    if isinstance(adapter, ResponseAdapter)
                    else await adapter(response, request)
                )
                protect_response(response)
            await anyio.lowlevel.checkpoint_if_cancelled()
            return response
        except BaseException as error:
            await close_response(response, error)
            raise
