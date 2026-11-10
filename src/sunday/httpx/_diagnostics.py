"""Redact operation-designated credential locations without acquiring or interpreting tokens."""

import httpx

from ..request_cancellation_error import RequestCancellationError
from ._requests import copy_request
from .request_security_bindings import request_security_bindings


def diagnostic_request(request: httpx.Request) -> httpx.Request:
    """Remove sensitive values from native response and failure diagnostics."""
    bindings = request_security_bindings(request)
    if not bindings:
        return request
    url, headers = request.url, httpx.Headers(request.headers)
    for binding in bindings:
        transport = binding.transport
        if transport.location == "query":
            url = url.copy_set_param(transport.name, "[redacted]")
        elif transport.location == "header":
            headers[transport.name] = "[redacted]"
        else:
            headers["cookie"] = "[redacted]"
    return copy_request(request, url=url, headers=headers)


def diagnostic_error(error: BaseException, request: httpx.Request) -> BaseException:
    """Preserve cancellation while removing sensitive request data from HTTPX failures."""
    if not request_security_bindings(request) or isinstance(error, RequestCancellationError):
        return error
    diagnostic = diagnostic_request(request)
    if isinstance(error, httpx.HTTPStatusError):
        error.response.request = diagnostic
        return type(error)("HTTP request failed", request=diagnostic, response=error.response)
    if isinstance(error, httpx.RequestError):
        return type(error)("HTTP request failed", request=diagnostic)
    return error
