# Copyright 2026 Outfox, Inc.
# Licensed under the Apache License, Version 2.0.

import json
from copy import deepcopy
from typing import Literal

import pytest
from pydantic import ConfigDict, Field, TypeAdapter, ValidationError

from sunday import Problem, ProblemPayload, ProblemRegistry, SundayModel


@pytest.mark.parametrize("member", ["type", "title", "status", "detail", "instance"])
@pytest.mark.parametrize("wrong", [None, True, False, 42, "401", [], {"nested": [1, None]}])
def test_wrong_categories_are_missing(member: str, wrong: object) -> None:
    if member == "status" and isinstance(wrong, (int, float)) and not isinstance(wrong, bool):
        return
    if member != "status" and isinstance(wrong, str):
        return
    baseline = {"trace": None, "nested": {"detail": 42}}
    source = {**baseline, member: wrong}
    original = deepcopy(source)
    expected = ProblemPayload.model_validate(baseline)
    for actual in (
        ProblemPayload.model_validate(source),
        ProblemPayload.model_validate_json(json.dumps(source)),
        TypeAdapter(ProblemPayload).validate_python(source),
        TypeAdapter(ProblemPayload).validate_json(json.dumps(source)),
    ):
        assert actual == expected
        assert actual.model_fields_set == expected.model_fields_set
        assert actual.model_dump() == expected.model_dump()
    registry = ProblemRegistry()
    assert registry.decode(source).model_dump() == registry.decode(baseline).model_dump()
    assert (
        registry.decode(source, response_status=404).model_dump()
        == registry.decode(baseline, response_status=404).model_dump()
    )
    assert source == original


class CustomPayload(ProblemPayload):
    type: Literal["urn:custom"] = "urn:custom"
    detail: str | None = Field(default="default", alias="explanation")
    required: int


class CustomProblem(Problem):
    payload_type = CustomPayload


def test_inherited_defaults_aliases_and_discriminators() -> None:
    registry = ProblemRegistry()
    registry.register("urn:custom", CustomProblem)
    defaulted_cases: list[dict[str, object]] = [{}, {"explanation": None}, {"explanation": []}]
    for data in defaulted_cases:
        problem = registry.decode({"type": "urn:custom", "required": 2, **data})
        assert isinstance(problem, CustomProblem)
        assert problem.detail == "default"
    missing_types: list[dict[str, object]] = [{}, {"type": None}, {"type": 42}]
    for data in missing_types:
        with pytest.raises(ValidationError):
            CustomPayload.model_validate({"required": 2, **data})
        assert type(registry.decode({"required": 2, **data})) is Problem
    with pytest.raises(ValidationError):
        registry.decode({"type": "urn:custom"})


def test_valid_values_and_semantic_failures() -> None:
    value = ProblemPayload.model_validate({"title": "", "detail": "", "instance": "", "trace": None})
    assert value.model_dump() == {"type": "about:blank", "title": "", "detail": "", "instance": "", "trace": None}
    assert ProblemPayload.model_validate({}).model_dump() == {"type": "about:blank"}
    with pytest.raises(ValidationError):
        ProblemPayload.model_validate({"status": 401.5})
    registry = ProblemRegistry()
    assert registry.decode({"status": 409}, response_status=404).status == 409
    assert registry.decode({"status": 409}, response_status=404).title == "Conflict"
    assert registry.decode({"type": "urn:unknown"}, response_status=404).title is None


class Ordinary(SundayModel):
    detail: str


def test_ordinary_models_are_unchanged() -> None:
    with pytest.raises(ValidationError):
        Ordinary.model_validate({"detail": 42})


def test_transport_status_precedes_model_defaults_and_uses_aliases() -> None:
    class DefaultPayload(ProblemPayload):
        status: int | None = Field(default=400, alias="httpStatus")
        title: str | None = Field(default="Default", alias="summary")

    class DefaultProblem(Problem):
        payload_type = DefaultPayload

    registry = ProblemRegistry()
    registry.register("about:blank", DefaultProblem)
    missing_values: list[dict[str, object]] = [{}, {"httpStatus": None}, {"httpStatus": "404"}, {"summary": []}]
    for raw in missing_values:
        value = registry.decode(raw, response_status=409)
        assert value.status == 409
        assert value.title == "Conflict"
    assert registry.decode({"httpStatus": 404, "summary": ""}, response_status=409).title == ""
    assert registry.decode({"httpStatus": 404}, response_status=409).title == "Not Found"
    assert registry.decode({}).status == 400


def test_aliases_do_not_consume_extension_values() -> None:
    class AliasedPayload(ProblemPayload):
        detail: str | None = Field(default=None, alias="explanation")
        extension_detail: int = Field(alias="detail")

    raw = {"explanation": "shown", "detail": 42}
    value = AliasedPayload.model_validate(raw)
    assert value.detail == "shown"
    assert value.extension_detail == 42
    assert raw == {"explanation": "shown", "detail": 42}

    class AliasOnlyPayload(ProblemPayload):
        model_config = ConfigDict(validate_by_alias=True, validate_by_name=False)
        detail: str | None = Field(default=None, alias="explanation")

    value_with_extra = AliasOnlyPayload.model_validate({"explanation": "shown", "detail": None})
    assert value_with_extra.detail == "shown"
    assert value_with_extra.model_extra == {"detail": None}


def test_existing_aliased_payload_revalidates_by_name() -> None:
    class AliasedPayload(ProblemPayload):
        trace_id: str = Field(alias="traceId")
        note: str = Field(default="fallback", alias="wireNote")

    original = AliasedPayload.model_validate({"traceId": "trace", "wireNote": "kept", "extra": None})
    presence = original.model_fields_set.copy()
    for value in (
        AliasedPayload.model_validate(original, by_alias=False, by_name=True),
        TypeAdapter(AliasedPayload).validate_python(original, by_alias=False, by_name=True),
    ):
        assert value == original
        assert value.model_fields_set == presence
    assert original.model_fields_set == presence


def test_transport_defaults_use_enabled_validation_names() -> None:
    class NameOnlyPayload(ProblemPayload):
        model_config = ConfigDict(validate_by_alias=False, validate_by_name=True)
        status: int | None = Field(default=None, alias="httpStatus")
        title: str | None = Field(default=None, alias="summary")

    class NameOnlyProblem(Problem):
        payload_type = NameOnlyPayload

    registry = ProblemRegistry()
    registry.register("about:blank", NameOnlyProblem)
    value = registry.decode({}, response_status=409)
    assert value.status == 409
    assert value.title == "Conflict"
    assert value.payload.model_extra == {}
    assert registry.decode({"status": "404"}, response_status=409).status == 409
