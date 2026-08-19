# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from enum import StrEnum

import pytest
from pydantic import Field, TypeAdapter

from sunday import (
    MediaType,
    Problem,
    ProblemPayload,
    ProblemRegistry,
    ResponseHeaders,
    SundayModel,
    TolerantStrEnum,
)


class Project(SundayModel):
    project_id: str = Field(alias="project-id")


class State(TolerantStrEnum):
    ACTIVE = "active"
    UNKNOWN = "unknown"


class StrictState(StrEnum):
    ACTIVE = "active"


class NotFoundPayload(ProblemPayload):
    type: str = "https://example.test/problems/not-found"
    resource_id: str = Field(alias="resource-id")


class NotFoundProblem(Problem):
    payload_type = NotFoundPayload


def test_sunday_model_uses_wire_aliases() -> None:
    project = Project.model_validate({"project-id": "project-1"})

    assert project.project_id == "project-1"
    assert project.model_dump() == {"project-id": "project-1"}


def test_tolerant_enum_preserves_unknown_values() -> None:
    value = TypeAdapter(State).validate_python("future")

    assert value.name == "UNKNOWN"
    assert value.value == "future"
    assert TypeAdapter(State).dump_python(value, mode="json") == "future"
    with pytest.raises(ValueError):
        TypeAdapter(StrictState).validate_python("future")


def test_media_type_parsing_matching_and_parameters() -> None:
    actual = MediaType('Application/Vnd.Example+JSON; Charset="utf-8"; profile="a;b"')

    assert actual.type == "application"
    assert actual.subtype == "vnd.example+json"
    assert actual.suffix == "json"
    assert actual.is_json
    assert actual.parameter("CHARSET") == "utf-8"
    assert actual.parameter("profile") == "a;b"
    assert MediaType("application/*+json").matches(actual)
    assert MediaType("*/*").matches(actual)
    assert not MediaType("text/*").matches(actual)
    assert str(actual) == 'application/vnd.example+json; charset=utf-8; profile="a;b"'


@pytest.mark.parametrize("value", ["json", "/json", "application/", 'application/json; charset="unterminated'])
def test_media_type_rejects_invalid_values(value: str) -> None:
    with pytest.raises(ValueError):
        MediaType(value)


def test_response_headers_preserve_repeated_values() -> None:
    headers = ResponseHeaders.from_items(
        [("Set-Cookie", "a=1"), ("set-cookie", "b=2"), ("Content-Type", "application/json")]
    )

    assert headers.get_all("SET-cookie") == ("a=1", "b=2")
    assert headers.get("missing") is None
    assert headers.content_type == MediaType("application/json")
    assert ResponseHeaders.from_items([("Content-Type", "invalid")]).content_type is None


def test_problem_registry_decodes_typed_and_unknown_problems() -> None:
    registry = ProblemRegistry()
    registry.register("https://example.test/problems/not-found", NotFoundProblem)

    problem = registry.decode(
        {
            "type": "https://example.test/problems/not-found",
            "title": "Not found",
            "resource-id": "project-1",
            "trace": "trace-1",
        },
        response_status=404,
    )

    assert isinstance(problem, NotFoundProblem)
    assert problem.status == 404
    assert problem.title == "Not found"
    assert problem.detail is None
    assert problem.instance is None
    assert problem.model_dump(by_alias=True)["resource-id"] == "project-1"
    assert problem.model_dump()["trace"] == "trace-1"
    assert str(problem) == "Not found"

    unknown = registry.copy().decode({"type": "https://example.test/problems/future"}, response_status=500)
    assert type(unknown) is Problem
    assert unknown.status == 500


def test_problem_registry_rejects_non_problem_types() -> None:
    registry = ProblemRegistry()

    with pytest.raises(TypeError):
        registry.register("invalid", Project)  # type: ignore[arg-type]
