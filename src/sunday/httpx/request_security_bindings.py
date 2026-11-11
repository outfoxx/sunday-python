"""Operation metadata for HTTP adapters, independent of token acquisition and storage."""

from collections.abc import Sequence
from typing import cast

import httpx

from ..security import SecurityBinding

_KEY = "sunday.security_bindings"


def request_security_bindings(request: httpx.Request) -> tuple[SecurityBinding, ...]:
    """Return the originating operation's bindings without credential receipts or token state."""
    return cast(tuple[SecurityBinding, ...], request.extensions.get(_KEY, ()))


def _set_request_security_bindings(request: httpx.Request, bindings: Sequence[SecurityBinding]) -> httpx.Request:
    if bindings:
        request.extensions[_KEY] = tuple(bindings)
    else:
        request.extensions.pop(_KEY, None)
    return request
