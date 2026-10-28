# Copyright 2026 Outfox, Inc.
# Licensed under the Apache License, Version 2.0.

import json
import subprocess
import sys
import textwrap
from copy import deepcopy
from typing import Annotated, Literal

import pytest
from pydantic import ConfigDict, Field, TypeAdapter, ValidationError, field_validator

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
    detail: str | None = Field(default="default", alias="detail")
    required: int


class CustomProblem(Problem):
    payload_type = CustomPayload


def test_inherited_defaults_and_discriminators() -> None:
    registry = ProblemRegistry()
    registry.register("urn:custom", CustomProblem)
    defaulted_cases: list[dict[str, object]] = [{}, {"detail": None}, {"detail": []}]
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


def test_transport_status_precedes_model_defaults() -> None:
    class DefaultPayload(ProblemPayload):
        status: int | None = Field(default=400, alias="status")
        title: str | None = Field(default="Default", alias="title")

    class DefaultProblem(Problem):
        payload_type = DefaultPayload

    registry = ProblemRegistry()
    registry.register("about:blank", DefaultProblem)
    missing_values: list[dict[str, object]] = [{}, {"status": None}, {"status": "404"}, {"title": []}]
    for raw in missing_values:
        value = registry.decode(raw, response_status=409)
        assert value.status == 409
        assert value.title == "Conflict"
    assert registry.decode({"status": 404, "title": ""}, response_status=409).title == ""
    assert registry.decode({"status": 404}, response_status=409).title == "Not Found"
    assert registry.decode({}).status == 400


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
        status: int | None = Field(default=None, alias="status")
        title: str | None = Field(default=None, alias="title")

    class NameOnlyProblem(Problem):
        payload_type = NameOnlyPayload

    registry = ProblemRegistry()
    registry.register("about:blank", NameOnlyProblem)
    value = registry.decode({}, response_status=409)
    assert value.status == 409
    assert value.title == "Conflict"
    assert value.payload.model_extra == {}
    assert registry.decode({"status": "404"}, response_status=409).status == 409


@pytest.mark.parametrize("by_name", [False, True])
@pytest.mark.parametrize("member", ["type", "title", "status", "detail", "instance"])
@pytest.mark.parametrize("wrong", [None, True, [], {}, "wrong-category"])
def test_per_call_validation_modes_preserve_missing_semantics(by_name: bool, member: str, wrong: object) -> None:
    if member != "status" and isinstance(wrong, str):
        wrong = 42

    class AliasedPayload(ProblemPayload):
        model_config = ConfigDict(validate_by_alias=True, validate_by_name=False)
        type: str = Field(default="about:blank", alias="type")
        title: str | None = Field(default="default title", alias="title")
        status: int | None = Field(default=400, alias="status")
        detail: str | None = Field(default="default detail", alias="detail")
        instance: str | None = Field(default=None, alias="instance")

    class NameOnlyPayload(AliasedPayload):
        model_config = ConfigDict(validate_by_alias=False, validate_by_name=True)

    payload_type = AliasedPayload if by_name else NameOnlyPayload
    aliases = {
        "type": "type",
        "title": "title",
        "status": "status",
        "detail": "detail",
        "instance": "instance",
    }
    active = member if by_name else aliases[member]
    inactive = "extension_" + member
    baseline = {inactive: ["unused alias"], "extension": None}
    raw = {**baseline, active: wrong}
    original = deepcopy(raw)
    expected = payload_type.model_validate(baseline, by_alias=not by_name, by_name=by_name)
    adapter = TypeAdapter(payload_type)
    for actual in (
        payload_type.model_validate(raw, by_alias=not by_name, by_name=by_name),
        payload_type.model_validate_json(json.dumps(raw), by_alias=not by_name, by_name=by_name),
        adapter.validate_python(raw, by_alias=not by_name, by_name=by_name),
        adapter.validate_json(json.dumps(raw), by_alias=not by_name, by_name=by_name),
    ):
        assert actual == expected
        assert actual.model_fields_set == expected.model_fields_set
        assert actual.model_dump() == expected.model_dump()
        assert actual.model_extra == baseline
    assert raw == original


def test_validated_defaults_do_not_reenter_normalization() -> None:
    # Keep a regression in Pydantic's native default recursion from killing pytest.
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            textwrap.dedent("""
            from pydantic import ConfigDict, Field, TypeAdapter
            from sunday import ProblemPayload

            class ModelDefaults(ProblemPayload):
                model_config = ConfigDict(validate_default=True)

            class FieldDefaults(ProblemPayload):
                detail: str | None = Field(default=None, validate_default=True)

            for payload in (ModelDefaults, FieldDefaults):
                expected = payload()
                for value in ({}, {"detail": None}, {"detail": []}):
                    assert payload.model_validate(value) == expected
                    assert TypeAdapter(payload).validate_python(value) == expected
                    assert payload.model_validate(value).model_fields_set == expected.model_fields_set
        """),
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_authored_defaults_validate_once_and_keep_semantic_errors() -> None:
    calls: list[object] = []

    class Payload(ProblemPayload):
        detail: str | None = Field(default_factory=lambda: "default", validate_default=True)

        @field_validator("detail", mode="before")
        @classmethod
        def validate_detail(cls, value: object) -> object:
            calls.append(value)
            return value

    detail_cases: list[dict[str, object]] = [{}, {"detail": None}, {"detail": []}]
    for raw in detail_cases:
        calls.clear()
        assert Payload.model_validate(raw).detail == "default"
        assert calls == ["default"]

    class InvalidDefault(ProblemPayload):
        model_config = ConfigDict(validate_default=True)
        status: int | None = Field(default=400, ge=500)

    status_cases: list[dict[str, object]] = [{}, {"status": None}, {"status": []}]
    for raw in status_cases:
        with pytest.raises(ValidationError):
            InvalidDefault.model_validate(raw)


def test_default_serialization_and_schema_are_preserved() -> None:
    class Payload(ProblemPayload):
        detail: str | None = Field(default="fallback")
        title: str | None = Field(default_factory=lambda: "title")

    for raw in ({}, {"detail": "fallback", "title": "title"}):
        value = Payload.model_validate(raw)
        assert value.model_dump(exclude_defaults=True) == {}
        assert value.model_dump_json(exclude_defaults=True) == "{}"
    assert Payload.model_validate({"detail": "other"}).model_dump(exclude_defaults=True) == {"detail": "other"}
    assert Payload.model_json_schema()["properties"]["detail"]["default"] == "fallback"


def test_custom_payload_discriminated_unions() -> None:
    class First(ProblemPayload):
        type: Literal["urn:first"] = "urn:first"
        count: int

    class Second(ProblemPayload):
        type: Literal["urn:second"] = "urn:second"
        label: str

    class Envelope(SundayModel):
        problem: Annotated[First | Second, Field(discriminator="type")]

    adapter: TypeAdapter[First | Second] = TypeAdapter(Annotated[First | Second, Field(discriminator="type")])
    for raw, payload_type in (
        ({"type": "urn:first", "count": 2}, First),
        ({"type": "urn:second", "label": "x"}, Second),
    ):
        raw_with_wrong_detail = {**raw, "detail": []}
        for decoded in (
            adapter.validate_python(raw_with_wrong_detail),
            adapter.validate_json(json.dumps(raw_with_wrong_detail)),
            Envelope.model_validate({"problem": raw_with_wrong_detail}).problem,
            Envelope.model_validate_json(json.dumps({"problem": raw_with_wrong_detail})).problem,
        ):
            assert isinstance(decoded, payload_type)
            assert decoded.detail is None
            assert adapter.validate_json(adapter.dump_json(decoded)) == decoded
        with pytest.raises(ValidationError):
            adapter.validate_python({**raw, "type": "urn:other"})
        wrong: object
        for wrong in (None, [], 42):
            with pytest.raises(ValidationError):
                payload_type.model_validate({**raw, "type": wrong})
        with pytest.raises(ValidationError):
            payload_type.model_validate({key: value for key, value in raw.items() if key != "type"})
