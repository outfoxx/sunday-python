"""Explicit standard authentication policy, composed outside transport execution."""

from collections.abc import Mapping
from typing import overload

import httpx

from ..token_provider import TokenProvider
from .authentication_adapter import AuthenticationAdapter
from .bearer_token_rejection_adapter import BearerTokenRejectionAdapter
from .request_security_bindings import request_security_bindings
from .response_adapter import ResponseAdapters


class StandardAuthenticationAdapter:
    """Attach credentials, report rejection, and cancel rejected bearer exchanges without replay.

    Supplied providers retain lifecycle ownership. Supply TokenManager explicitly for caching and refresh.
    Install the same instance in both request and response slots.
    """

    def __init__(self, providers: Mapping[str, TokenProvider]) -> None:
        """Compose policy without acquiring, wrapping, or taking ownership of providers."""
        self._authentication = AuthenticationAdapter(providers)
        self._responses = ResponseAdapters(self._authentication, BearerTokenRejectionAdapter())

    @overload
    async def __call__(self, value: httpx.Request) -> httpx.Request: ...

    @overload
    async def __call__(self, value: httpx.Response, request: httpx.Request) -> httpx.Response: ...

    async def __call__(
        self, value: httpx.Request | httpx.Response, request: httpx.Request | None = None
    ) -> httpx.Request | httpx.Response:
        """Leave unrelated responses and all bodies untouched; feedback precedes cancellation."""
        if request is None:
            assert isinstance(value, httpx.Request)
            return await self._authentication(value)
        assert isinstance(value, httpx.Response)
        if not request_security_bindings(request):
            return value
        return await self._responses.adapt(value, request)
