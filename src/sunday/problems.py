# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import Mapping
from http import HTTPStatus
from typing import Any, ClassVar, Self

from pydantic import AliasChoices, ConfigDict, Field, model_validator

from .models import SundayModel


class ProblemPayload(SundayModel):
    """RFC 9457 problem details payload with extension-member preservation."""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True, extra="allow")

    type: str = "about:blank"
    title: str | None = Field(default=None, exclude_if=lambda value: value is None)
    status: int | None = Field(default=None, exclude_if=lambda value: value is None)
    detail: str | None = Field(default=None, exclude_if=lambda value: value is None)
    instance: str | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="before")
    @classmethod
    def _normalize_standard_members(cls, value: Any) -> Any:
        return cls.normalize_standard_members(value)

    @classmethod
    def normalize_standard_members(cls, value: Any) -> Any:
        """Discard wrong JSON categories before defaults and field validation.

        Only standard members are normalized; extension values and nested models
        retain their authored validation rules. Work on a copy for shared inputs.
        """
        if not isinstance(value, Mapping):
            return value
        normalized = value.copy() if isinstance(value, dict) else dict(value)
        for name in ("type", "title", "status", "detail", "instance"):
            keys = cls.standard_member_input_keys(name)
            for key in keys:
                if key not in normalized:
                    continue
                member = normalized[key]
                valid = (
                    isinstance(member, (int, float)) and not isinstance(member, bool)
                    if name == "status"
                    else isinstance(member, str)
                )
                if valid:
                    # Lower-priority names are extras once Pydantic selects this key.
                    break
                del normalized[key]
            if name == "type" and keys and not any(key in normalized for key in keys):
                # An inherited custom default must not turn an absent type into
                # a custom discriminator. Its constraints still validate blank.
                normalized[keys[0]] = "about:blank"
        return normalized

    @classmethod
    def standard_member_input_keys(cls, name: str) -> list[str]:
        """Return accepted input keys in Pydantic alias precedence order."""
        field = cls.model_fields[name]
        alias = field.validation_alias
        aliases = alias.choices if isinstance(alias, AliasChoices) else [alias]
        by_alias = cls.model_config.get("validate_by_alias", True)
        by_name = cls.model_config.get("validate_by_name", cls.model_config.get("populate_by_name", False))
        keys = [key for key in aliases if isinstance(key, str)] if by_alias else []
        if alias is None or by_name:
            extension_aliases = {
                key
                for field_name, extension in cls.model_fields.items()
                if field_name not in ("type", "title", "status", "detail", "instance")
                for key in (
                    extension.validation_alias.choices
                    if isinstance(extension.validation_alias, AliasChoices)
                    else [extension.validation_alias]
                )
                if by_alias and isinstance(key, str)
            }
            if name not in extension_aliases:
                keys.append(name)
        return list(dict.fromkeys(keys))


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
            status_keys = payload_type.standard_member_input_keys("status")
            status = next((normalized[key] for key in status_keys if key in normalized), response_status)
            if not any(key in normalized for key in status_keys):
                normalized[status_keys[0]] = response_status
            title_keys = payload_type.standard_member_input_keys("title")
            if type_uri == "about:blank" and not any(key in normalized for key in title_keys):
                try:
                    title = HTTPStatus(status).phrase
                except ValueError:
                    pass
                else:
                    normalized[title_keys[0]] = title
        payload = payload_type.model_validate(normalized)
        return problem_type(payload)

    def copy(self) -> ProblemRegistry:
        """Create an independent registry containing the same registrations."""
        registry = ProblemRegistry()
        registry._problem_types.update(self._problem_types)
        return registry
