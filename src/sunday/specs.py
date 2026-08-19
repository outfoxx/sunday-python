# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace

from .media import MediaType
from .parameters import ParameterSpec


@dataclass(frozen=True, slots=True)
class RequestSpec[RequestBodyT]:
    """Declarative HTTP request description consumed by a transport."""

    method: str
    path_template: str
    parameters: tuple[ParameterSpec, ...] = ()
    headers: tuple[tuple[str, str], ...] = ()
    body: RequestBodyT | None = None
    content_types: tuple[MediaType, ...] = ()
    accept_types: tuple[MediaType, ...] = ()

    def with_headers(self, *headers: tuple[str, str]) -> RequestSpec[RequestBodyT]:
        """Return a request specification with appended headers."""
        return replace(self, headers=(*self.headers, *headers))


@dataclass(frozen=True, slots=True)
class ResponseSpec[ResponseT]:
    """Expected response status, media types, and generated value decoder."""

    status: int | None
    content_types: tuple[MediaType, ...] = ()
    decoder: Callable[[object], ResponseT] | None = None
    body_expected: bool = True

    def accepts(self, media_type: MediaType | None) -> bool:
        """Return whether this response specification accepts ``media_type``."""
        if media_type is None:
            return not self.content_types
        return not self.content_types or any(expected.matches(media_type) for expected in self.content_types)


@dataclass(frozen=True, slots=True)
class OperationSpec[RequestBodyT, ResponseT]:
    """A generated request and the possible successful responses."""

    request: RequestSpec[RequestBodyT]
    responses: tuple[ResponseSpec[ResponseT], ...]


@dataclass(frozen=True, slots=True)
class NullifySpec:
    """Problems that a nullable operation should translate into ``None``."""

    statuses: tuple[int, ...] = ()
    problem_types: tuple[type[Exception], ...] = ()
