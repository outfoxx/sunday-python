# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any


class SundayError(Exception):
    """Base error raised by the Sunday runtime."""

    def __init__(self, message: str, *, details: Mapping[str, object] | None = None) -> None:
        super().__init__(message)
        self.details = MappingProxyType(dict(details or {}))


class RequestEncodingError(SundayError):
    """A declarative request could not be encoded as a native request."""


class TransportError(SundayError):
    """A Sunday transport is misconfigured or cannot satisfy its lifecycle contract."""


class ResponseError(SundayError):
    """Base error carrying native response diagnostics."""

    def __init__(
        self,
        message: str,
        *,
        status: int,
        content_type: str | None = None,
        body: bytes = b"",
        transport_response: Any = None,
        details: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(message, details=details)
        self.status = status
        self.content_type = content_type
        self.body = body
        self.transport_response = transport_response


class ResponseDecodingError(ResponseError):
    """A native response matched an operation but its value could not be decoded."""


class ResponseValidationError(ResponseError):
    """A native response did not satisfy an operation response declaration."""


class UnexpectedResponse(ResponseValidationError):
    """An HTTP response that cannot be decoded using an operation specification."""

    def __init__(
        self,
        message: str,
        *,
        status: int,
        content_type: str | None = None,
        body: bytes = b"",
        transport_response: Any = None,
    ) -> None:
        super().__init__(
            message,
            status=status,
            content_type=content_type,
            body=body,
            transport_response=transport_response,
        )
