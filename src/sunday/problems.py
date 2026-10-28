# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import Mapping
from contextlib import suppress
from http import HTTPStatus
from typing import Any, ClassVar, Self

from pydantic import (
    AliasChoices,
    AliasPath,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    model_serializer,
    model_validator,
)

from .models import SundayModel

_STANDARD_MEMBERS = frozenset({"type", "title", "status", "detail", "instance"})


class ProblemPayload(SundayModel):
    """RFC 9457 payload with fixed standard wire names and authored extensions."""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True, extra="allow")

    type: str = "about:blank"
    title: str | None = Field(default=None, exclude_if=lambda value: value is None)
    status: int | None = Field(default=None, exclude_if=lambda value: value is None)
    detail: str | None = Field(default=None, exclude_if=lambda value: value is None)
    instance: str | None = Field(default=None, exclude_if=lambda value: value is None)

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        """Reserve the RFC wire names while leaving extension aliases to Pydantic."""
        super().__pydantic_init_subclass__(**kwargs)
        for name, field in cls.model_fields.items():
            if name in _STANDARD_MEMBERS:
                if any(
                    alias is not None and alias != name
                    for alias in (field.alias, field.validation_alias, field.serialization_alias)
                ):
                    raise TypeError(f"Problem standard member {name!r} must use its RFC wire name")
                continue
            alias = field.validation_alias
            paths = (
                alias.convert_to_aliases()
                if isinstance(alias, AliasChoices)
                else [alias.path]
                if isinstance(alias, AliasPath)
                else [[alias]]
                if isinstance(alias, str)
                else []
            )
            names = {name, field.alias, field.serialization_alias, *(path[0] for path in paths)}
            if names & _STANDARD_MEMBERS:
                raise TypeError(f"Problem extension {name!r} cannot use a reserved RFC member name")

    @model_validator(mode="before")
    @classmethod
    def _normalize_standard_members(cls, value: Any) -> Any:
        return cls.normalize_standard_members(value)

    @classmethod
    def normalize_standard_members(cls, value: Any) -> Any:
        """Copy JSON input, discard wrong standard types, and supply the effective type.

        Generated union dispatch uses this same function before choosing a subtype.
        Correctly typed values, extensions, and nested contents remain untouched.
        """
        if not isinstance(value, Mapping):
            return value
        normalized = value.copy() if isinstance(value, dict) else dict(value)
        for name in _STANDARD_MEMBERS:
            if name not in normalized:
                continue
            member = normalized[name]
            valid = (
                isinstance(member, (int, float)) and not isinstance(member, bool)
                if name == "status"
                else isinstance(member, str)
            )
            if not valid:
                del normalized[name]
        normalized.setdefault("type", "about:blank")
        return normalized

    @model_serializer(mode="wrap")
    def _serialize_standard_members(self, handler: SerializerFunctionWrapHandler):  # type: ignore[no-untyped-def]
        # A return annotation replaces Pydantic's model JSON schema with that type's schema.
        # Redeclaring a field can replace its exclude_if; keep RFC omission on subclasses.
        result: dict[str, Any] = handler(self)
        for name in _STANDARD_MEMBERS:
            if result.get(name) is None:
                result.pop(name, None)
        return result


class Problem(Exception):
    """Catchable RFC problem backed by a typed Pydantic payload."""

    payload_type: ClassVar[type[ProblemPayload]] = ProblemPayload

    def __init__(self, payload: ProblemPayload) -> None:
        super().__init__(payload.title or payload.type)
        self.payload = payload

    @classmethod
    def model_validate(cls, value: object) -> Self:
        """Validate a wire value and construct this problem type."""
        return cls(cls.payload_type.model_validate(value))

    @property
    def type(self) -> str:
        """Return the problem type URI."""
        return self.payload.type

    @property
    def title(self) -> str | None:
        """Return the short, human-readable problem summary."""
        return self.payload.title

    @property
    def status(self) -> int | None:
        """Return the problem HTTP status."""
        return self.payload.status

    @property
    def detail(self) -> str | None:
        """Return the human-readable problem explanation."""
        return self.payload.detail

    @property
    def instance(self) -> str | None:
        """Return the URI identifying this occurrence."""
        return self.payload.instance

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        """Serialize the typed problem payload."""
        return self.payload.model_dump(**kwargs)


class ProblemRegistry:
    """Registry mapping problem type URIs to generated exception classes."""

    def __init__(self) -> None:
        self._problem_types: dict[str, type[Problem]] = {}

    def register(self, type_uri: str, problem_type: type[Problem]) -> None:
        """Register ``problem_type`` for an exact problem type URI."""
        if not issubclass(problem_type, Problem):
            raise TypeError("Registered problem types must extend Problem")
        self._problem_types[type_uri] = problem_type

    def register_problem(self, type_uri: str, problem_type: type[Problem]) -> None:
        """Register a problem through the transport-neutral registrar contract."""
        self.register(type_uri, problem_type)

    def decode(self, value: Mapping[str, object], *, response_status: int | None = None) -> Problem:
        """Decode a problem mapping, falling back to the base problem type."""
        type_uri = value.get("type")
        if not isinstance(type_uri, str):
            type_uri = "about:blank"
        problem_type = self._problem_types.get(type_uri, Problem)
        payload_type = problem_type.payload_type
        normalized = payload_type.normalize_standard_members(value)
        if response_status is not None:
            normalized.setdefault("status", response_status)
            if type_uri == "about:blank" and "title" not in normalized:
                with suppress(ValueError):
                    normalized["title"] = HTTPStatus(normalized["status"]).phrase
        payload = payload_type.model_validate(normalized)
        return problem_type(payload)

    def copy(self) -> ProblemRegistry:
        """Create an independent registry containing the same registrations."""
        registry = ProblemRegistry()
        registry._problem_types.update(self._problem_types)
        return registry
