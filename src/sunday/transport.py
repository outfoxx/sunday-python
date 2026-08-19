# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import Sequence
from types import TracebackType
from typing import Any, Protocol

from .operations import OperationResponse
from .specs import RequestSpec, ResponseSpec


class Transport(Protocol):
    """Transport contract used by generated operations."""

    async def build_request(self, spec: RequestSpec[Any]) -> Any:
        """Build a native request from a declarative request specification."""
        ...

    async def send(self, request: Any, *, stream: bool = False) -> Any:
        """Send a native request and return the native response."""
        ...

    async def decode_response(
        self,
        response: Any,
        responses: Sequence[ResponseSpec[Any]],
    ) -> OperationResponse[Any]:
        """Decode a native response according to generated response specifications."""
        ...

    async def aclose(self) -> None:
        """Release transport-owned resources idempotently."""
        ...

    async def __aenter__(self) -> Transport:
        """Enter an asynchronous transport lifecycle scope."""
        ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release transport-owned resources when leaving a lifecycle scope."""
        ...
