# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any

from pydantic import TypeAdapter

from .media import MediaType
from .parameters import ParameterSpec
from .security import SecurityBinding
from .uri import URITemplate


@dataclass(frozen=True, slots=True)
class RequestPayloadSpec[RequestBodyT]:
    """A request body and the media types available to encode it."""

    body: RequestBodyT
    content_types: tuple[MediaType, ...]
    body_adapter: TypeAdapter[Any] | None = None


@dataclass(frozen=True, slots=True)
class RequestSpec[RequestBodyT]:
    """Declarative HTTP request description consumed by a transport."""

    method: str
    path_template: str | URITemplate
    parameters: tuple[ParameterSpec, ...] = ()
    headers: tuple[tuple[str, str], ...] = ()
    body: RequestBodyT | None = None
    content_types: tuple[MediaType, ...] = ()
    accept_types: tuple[MediaType, ...] = ()
    payload: RequestPayloadSpec[RequestBodyT] | None = None
    security: tuple[SecurityBinding, ...] | None = None
    template_parameters: Mapping[str, object | None] = field(default_factory=dict)
    body_adapter: TypeAdapter[Any] | None = None
    parameter_validation: Callable[[], None] | None = None

    def __post_init__(self) -> None:
        if self.payload is not None and (self.body is not None or self.content_types):
            raise ValueError("RequestSpec payload cannot be combined with body or content_types")

    def with_headers(self, *headers: tuple[str, str]) -> RequestSpec[RequestBodyT]:
        """Return a request specification with appended headers."""
        return replace(self, headers=(*self.headers, *headers))

    def validate_parameters(self) -> None:
        """Revalidate typed parameters before conversion, including requests without bodies."""
        if self.parameter_validation is not None:
            self.parameter_validation()

    def validate_body(self) -> None:
        """Revalidate the participating payload with native request-mode rules before encoding."""
        adapter = self.payload.body_adapter if self.payload is not None else self.body_adapter
        if adapter is not None:
            adapter.validate_python(self.effective_body, strict=True, context={"mode": "request"})

    @property
    def effective_body(self) -> RequestBodyT | None:
        """Return the request body from the payload form or compatibility fields."""
        return self.payload.body if self.payload is not None else self.body

    @property
    def effective_content_types(self) -> tuple[MediaType, ...]:
        """Return the payload media types from either request representation."""
        return self.payload.content_types if self.payload is not None else self.content_types


@dataclass(frozen=True, slots=True)
class ResponseHeaderSpec:
    """A declared response header and its generated value decoder."""

    name: str
    decoder: Callable[[str], object] | None = None
    required: bool = False
    repeated: bool = False


@dataclass(frozen=True, slots=True)
class ResponseSpec[ResponseT]:
    """Expected response status, media types, and generated value decoder."""

    status: int | None
    content_types: tuple[MediaType, ...] = ()
    decoder: Callable[[object], ResponseT] | None = None
    body_expected: bool = True
    headers: tuple[ResponseHeaderSpec, ...] = ()

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
