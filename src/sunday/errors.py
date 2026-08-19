# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from typing import Any


class SundayError(Exception):
    """Base error raised by the Sunday runtime."""


class UnexpectedResponse(SundayError):
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
        super().__init__(message)
        self.status = status
        self.content_type = content_type
        self.body = body
        self.transport_response = transport_response
