# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar, Self

from pydantic import ConfigDict

from .models import SundayModel


class ProblemPayload(SundayModel):
    """RFC 9457 problem details payload with extension-member preservation."""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True, extra="allow")

    type: str = "about:blank"
    title: str | None = None
    status: int | None = None
    detail: str | None = None
    instance: str | None = None


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
        type_uri = value.get("type", "about:blank")
        problem_type = self._problem_types.get(type_uri, Problem) if isinstance(type_uri, str) else Problem
        payload = problem_type.payload_type.model_validate(value)
        if payload.status is None and response_status is not None:
            payload = payload.model_copy(update={"status": response_status})
        return problem_type(payload)

    def copy(self) -> ProblemRegistry:
        """Create an independent registry containing the same registrations."""
        registry = ProblemRegistry()
        registry._problem_types.update(self._problem_types)
        return registry
