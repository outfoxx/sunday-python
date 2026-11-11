"""Explicit provider integration outside HTTP transport execution."""

from collections.abc import Mapping
from typing import TYPE_CHECKING, TypedDict, cast, overload

from ..client_settings import ClientSettings

if TYPE_CHECKING:
    from ._transport import HttpxTransport

import httpx

from ..request_cancellation_error import RequestCancellationError
from ..token_provider import TokenProvider, TokenProviderError, TokenRejectedError
from ._security import RequestSecurity
from .request_adapter import RequestAdapterLike, RequestAdapters
from .request_security_bindings import request_security_bindings
from .response_adapter import ResponseAdapterLike, ResponseAdapters


class AuthenticationAdapter:
    """Callable request/response adapter, without transport retries.

    Install the same instance in both slots. Compose using RequestAdapters and
    ResponseAdapters, whose independent chain types avoid ambiguous inheritance.
    """

    def __init__(self, providers: Mapping[str, TokenProvider]) -> None:
        self._providers = dict(providers)
        self._receipt = f"sunday.authentication.{id(self)}"

    @overload
    async def __call__(self, value: httpx.Request) -> httpx.Request: ...

    @overload
    async def __call__(self, value: httpx.Response, request: httpx.Request) -> httpx.Response: ...

    async def __call__(
        self, value: httpx.Request | httpx.Response, request: httpx.Request | None = None
    ) -> httpx.Request | httpx.Response:
        """Return a usable message; terminal provider failure cancels the exchange."""
        if request is not None:
            assert isinstance(value, httpx.Response)
            security = (
                cast(RequestSecurity | None, request.extensions.get(self._receipt))
                if request_security_bindings(request)
                else None
            )
            if security is not None:
                recoverable = True
                try:
                    for lease in security.rejected_leases(request, value):
                        recoverable = await lease.invalidate() and recoverable
                except TokenProviderError as error:
                    if error.reason != "temporary":
                        raise RequestCancellationError(str(error)) from error
                    raise httpx.TransportError("Credentials temporarily unavailable", request=request) from error
                if not recoverable:
                    raise TokenRejectedError()
            return value
        assert isinstance(value, httpx.Request)
        bindings = request_security_bindings(value)
        if not bindings:
            return value
        try:
            value.extensions[self._receipt] = await RequestSecurity(bindings).authorize(value, self._providers)
        except TokenProviderError as error:
            if error.reason != "temporary":
                raise RequestCancellationError(str(error)) from error
            raise httpx.TransportError("Credentials temporarily unavailable", request=value) from error
        return value


class AuthenticationAdapters(TypedDict):
    """Explicit request/response slots accepted by HttpxTransport."""

    request_adapter: RequestAdapterLike
    response_adapter: ResponseAdapterLike


def authentication_adapters(
    providers: Mapping[str, TokenProvider],
    request_adapter: RequestAdapterLike | None = None,
    response_adapter: ResponseAdapterLike | None = None,
) -> AuthenticationAdapters:
    """Compose provider integration explicitly before optional application adapters."""
    authentication = AuthenticationAdapter(providers)
    return {
        "request_adapter": RequestAdapters(authentication, *((request_adapter,) if request_adapter else ())),
        "response_adapter": ResponseAdapters(authentication, *((response_adapter,) if response_adapter else ())),
    }


def transport_from_settings(settings: ClientSettings, client: httpx.AsyncClient) -> "HttpxTransport":
    """Select standard attachment, rejection feedback, and cancellation using caller-owned providers/client."""
    from ._transport import HttpxTransport

    if str(client.base_url).rstrip("/") != str(httpx.URL(settings.base_url)).rstrip("/"):
        raise ValueError("HTTPX client base URL must match resolved settings")
    from .standard_authentication_adapter import StandardAuthenticationAdapter

    authentication = StandardAuthenticationAdapter(settings.token_providers)
    return HttpxTransport(client, request_adapter=authentication, response_adapter=authentication)
