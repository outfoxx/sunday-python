# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

import anyio
import httpx

from .request_security_bindings import _set_request_security_bindings, request_security_bindings


class RequestAdapter(ABC):
    """Adapt once before each Sunday-managed native call, or once for export.

    Consumed resources must be replaced before returning. Native client recovery
    inside a call may resend the adapted request without invoking this adapter.
    """

    @abstractmethod
    async def adapt(self, request: httpx.Request) -> httpx.Request:
        """Return the request to send; raising stops the remaining adapters."""

    def chain_to(self, next_adapter: RequestAdapterLike) -> RequestAdapters:
        """Compose adapters in order, flattening existing chains."""
        return RequestAdapters(self, next_adapter)


type RequestAdapterLike = RequestAdapter | Callable[[httpx.Request], Awaitable[httpx.Request]]


class RequestAdapters(RequestAdapter):
    """An immutable ordered chain of adapters or asynchronous functions."""

    def __init__(self, *adapters: RequestAdapterLike) -> None:
        self.adapters: tuple[RequestAdapterLike, ...] = tuple(
            child
            for adapter in adapters
            for child in (adapter.adapters if isinstance(adapter, RequestAdapters) else (adapter,))
        )

    async def adapt(self, request: httpx.Request) -> httpx.Request:
        """Run each adapter once, preserving native task cancellation."""
        for adapter in self.adapters:
            await anyio.lowlevel.checkpoint_if_cancelled()
            metadata = {key: value for key, value in request.extensions.items() if key.startswith("sunday.")}
            bindings = request_security_bindings(request)
            request = await adapter.adapt(request) if isinstance(adapter, RequestAdapter) else await adapter(request)
            for key, value in metadata.items():
                request.extensions.setdefault(key, value)
            _set_request_security_bindings(request, bindings)
        await anyio.lowlevel.checkpoint_if_cancelled()
        return request
