"""Private adapter state retained only during a managed exchange."""

from typing import cast

import httpx

_KEY = "sunday.request_context"


def request_context(request: httpx.Request) -> dict[object, object]:
    """Share adapter receipts through native request copies within an exchange."""
    return cast(dict[object, object], request.extensions.setdefault(_KEY, {}))


def clear_request_context(request: httpx.Request) -> httpx.Request:
    """Detach private adapter state without altering native extensions or shared copies."""
    request.extensions.pop(_KEY, None)
    return request
