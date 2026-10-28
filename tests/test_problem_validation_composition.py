# Copyright 2026 Outfox, Inc.
# Licensed under the Apache License, Version 2.0.

import json
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from typing import Annotated, Any, Literal

import pytest
from pydantic import (
    AliasChoices,
    AliasPath,
    BaseModel,
    BeforeValidator,
    Field,
    TypeAdapter,
    ValidationError,
    ValidationInfo,
    field_serializer,
    field_validator,
    model_validator,
)

from sunday import Problem, ProblemPayload, ProblemRegistry, SundayModel


def registry_for(payload: type[ProblemPayload], uri: str = "about:blank") -> ProblemRegistry:
    class CustomProblem(Problem):
        payload_type = payload

    registry = ProblemRegistry()
    registry.register(uri, CustomProblem)
    return registry


def test_authored_factories_and_validators_see_only_validated_fields() -> None:
    seen: list[dict[str, Any]] = []

    class Payload(ProblemPayload):
        detail: str | None = Field(default_factory=lambda data: json.dumps(data))
        snapshot: dict[str, Any] = Field(default_factory=lambda data: dict(data))

        @field_validator("snapshot", mode="before")
        @classmethod
        def inspect_data(cls, value: Any, info: ValidationInfo) -> Any:
            seen.append(dict(info.data or {}))
            return value

    expected = Payload.model_validate({})
    cases: list[dict[str, object]] = [{"detail": None}, {"detail": []}]
    for raw in cases:
        for value in (Payload.model_validate(raw), registry_for(Payload).decode(raw).payload):
            assert value == expected
            assert value.model_fields_set == expected.model_fields_set
            assert value.model_dump_json() == expected.model_dump_json()
    Payload.model_validate({"detail": None, "snapshot": {}})
    assert all(not key.startswith("_sunday") for data in seen for key in data)


@pytest.mark.parametrize("literal", [False, True])
def test_type_serializer_and_json_schema_survive_default_replacement(literal: bool) -> None:
    class Payload(ProblemPayload):
        @field_serializer("type")
        def serialize_type(self, value: str) -> str:
            return "SERIALIZED:" + value

    class LiteralPayload(Payload):
        type: Literal["urn:a"] = "urn:a"

    payload_type = LiteralPayload if literal else Payload
    value = payload_type.model_validate({"type": "urn:a"})
    assert value.model_dump()["type"] == "SERIALIZED:urn:a"
    assert json.loads(value.model_dump_json())["type"] == "SERIALIZED:urn:a"
    assert payload_type.model_json_schema()["properties"]["type"]["default"] == ("urn:a" if literal else "about:blank")


def test_blank_literal_unions_and_standalone_normalization() -> None:
    class Blank(ProblemPayload):
        type: Literal["about:blank"] = "about:blank"

    class Custom(ProblemPayload):
        type: Literal["urn:a"] = "urn:a"

    class Envelope(SundayModel):
        problem: Annotated[Blank | Custom, Field(discriminator="type")]

    adapter: TypeAdapter[Blank | Custom] = TypeAdapter(Annotated[Blank | Custom, Field(discriminator="type")])
    for uri, cls in (("about:blank", Blank), ("urn:a", Custom)):
        raw = {"type": uri, "detail": []}
        assert isinstance(adapter.validate_python(raw), cls)
        assert isinstance(adapter.validate_json(json.dumps(raw)), cls)
        assert isinstance(Envelope.model_validate({"problem": raw}).problem, cls)
    wrong_values: list[object] = [None, 42, [], {}, True]
    for wrong in wrong_values:
        assert Blank.model_validate({"type": wrong}) == Blank.model_validate({})
    with pytest.raises(ValidationError):
        Blank.model_validate({"type": "urn:other"})


@pytest.mark.parametrize("member", ["title", "status", "detail", "instance"])
def test_registry_discarded_fields_keep_missing_presence(member: str) -> None:
    class Payload(ProblemPayload):
        title: str | None = "default"
        status: int | None = 400
        detail: str | None = "default"
        instance: str | None = "default"

    for payload_type in (ProblemPayload, Payload):
        registry = registry_for(payload_type)
        expected = payload_type.model_validate({})
        for actual in (payload_type.model_validate({member: []}), registry.decode({member: []}).payload):
            assert actual == expected
            assert actual.model_fields_set == expected.model_fields_set
            assert actual.model_dump(exclude_unset=True) == expected.model_dump(exclude_unset=True)


def test_nested_failed_validation_and_concurrent_calls_do_not_share_presence() -> None:
    errors: list[ValidationError] = []

    class Child(ProblemPayload):
        required: int

    def nested_default(data: dict[str, Any]) -> dict[str, Any]:
        try:
            Child.model_validate({"detail": []})
        except ValidationError as error:
            errors.append(error)
        Child.model_validate({"required": 1, "detail": []})
        return dict(data)

    class Parent(ProblemPayload):
        snapshot: dict[str, Any] = Field(default_factory=nested_default)
        child: Child | None = None

    expected = Parent.model_validate({})
    actual = Parent.model_validate({"detail": None, "child": {"required": 2, "detail": None}})
    assert actual.snapshot == expected.snapshot
    assert actual.model_fields_set == {"type", "child"}
    assert actual.child is not None and actual.child.model_fields_set == {"type", "required"}
    assert errors

    def decode(index: int) -> set[str]:
        return Parent.model_validate({"detail": None if index % 2 else "present"}).model_fields_set

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert list(pool.map(decode, range(32))) == [
            {"type", "detail"} if index % 2 == 0 else {"type"} for index in range(32)
        ]


def test_context_and_strict_settings_survive_normalization() -> None:
    class Payload(ProblemPayload):
        detail: str | None = Field(default_factory=lambda data: data["type"], validate_default=True)
        required: int

        @field_validator("detail", mode="after")
        @classmethod
        def check_context(cls, value: str | None, info: ValidationInfo) -> str | None:
            assert info.context == {"request": 7}
            return value

    context = {"request": 7}
    adapter = TypeAdapter(Payload)
    value = adapter.validate_python({"detail": [], "required": 1}, strict=True, context=context)
    assert value.detail == "about:blank"
    assert value.model_fields_set == {"type", "required"}
    with pytest.raises(ValidationError):
        adapter.validate_python({"detail": [], "required": "1"}, strict=True, context=context)
    assert context == {"request": 7}


def test_generator_normalization_helper_preserves_union_and_wire_validation() -> None:
    class Blank(ProblemPayload):
        type: Literal["about:blank"] = "about:blank"
        trace: str | None = None

        @model_validator(mode="before")
        @classmethod
        def validate_wire(cls, raw: Any) -> Any:
            if not isinstance(raw, dict):
                return raw
            data = cls.normalize_standard_members(raw)
            for key in list(data):
                if key in {"detail", "instance"}:
                    assert isinstance(data[key], str)
                if key == "extension":
                    data[key] = int(data[key])
                if key == "drop":
                    data.pop(key)
            data.update(trace="updated")
            return data

    class Custom(ProblemPayload):
        type: Literal["urn:custom"] = "urn:custom"

    adapter: TypeAdapter[Blank | Custom] = TypeAdapter(
        Annotated[
            Annotated[Blank | Custom, Field(discriminator="type")],
            BeforeValidator(lambda raw: ProblemPayload.normalize_standard_members(raw).copy()),
        ]
    )
    raw: dict[str, Any] = {"type": None, "detail": [], "instance": False, "extension": "7", "drop": 3}
    original = deepcopy(raw)
    result = adapter.validate_python(raw)
    assert isinstance(result, Blank)
    assert result.detail is None and result.instance is None
    assert result.trace == "updated"
    assert result.model_extra == {"extension": 7}
    assert result.model_fields_set == {"type", "trace", "extension"}
    assert adapter.validate_json(json.dumps(raw)) == result
    assert raw == original


@pytest.mark.parametrize("operation", ["pop", "delete", "popitem", "clear"])
def test_normalization_view_preserves_dictionary_edits(operation: str) -> None:
    data = ProblemPayload.normalize_standard_members({})
    assert data.setdefault("type", "urn:other") == "about:blank"
    if operation == "pop":
        assert data.pop("type") == "about:blank"
    elif operation == "delete":
        del data["type"]
    elif operation == "popitem":
        assert data.popitem() == ("type", "about:blank")
    else:
        data.clear()
    assert data == {}
    data.update(iter([("detail", "updated")]))
    value = ProblemPayload.model_validate(data)
    assert value.type == "about:blank" and value.detail == "updated"
    assert value.model_extra == {}


def test_mixin_predecessor_preserves_mutable_defaults_and_literal_discriminators() -> None:
    class Mixin(BaseModel):
        items: list[int] = []
        kind: Literal["first"] = "first"

    class First(ProblemPayload, Mixin):
        type: Literal["about:blank"] = "about:blank"

    class Second(First):
        kind: Literal["second"] = "second"  # type: ignore[assignment]

    adapter: TypeAdapter[First | Second] = TypeAdapter(Annotated[First | Second, Field(discriminator="kind")])
    first = adapter.validate_python({"kind": "first", "type": None})
    second = adapter.validate_python({"kind": "second", "type": []})
    assert isinstance(first, First) and isinstance(second, Second)
    first.items.append(1)
    assert second.items == []
    assert first.type == second.type == "about:blank"


def test_mixin_static_defaults_keep_native_copy_and_schema_behavior() -> None:
    class Mixin(BaseModel):
        items: list[int] = []

    class Payload(ProblemPayload, Mixin):
        type: Literal["about:blank"] = "about:blank"

    first = Payload.model_validate({"type": None})
    second = Payload.model_validate({"type": None})
    first.items.append(1)
    assert second.items == []
    assert second.model_dump(exclude_defaults=True) == {}
    assert Payload.model_json_schema()["properties"]["items"]["default"] == []


def test_nested_failure_in_mixin_validator_restores_parent_lookup() -> None:
    held_errors: list[ValidationError] = []

    class Child(ProblemPayload):
        required: int

    class Mixin(BaseModel):
        token: str

        @field_validator("token", mode="before")
        @classmethod
        def validate_token(cls, value: str) -> str:
            try:
                Child.model_validate({"detail": []})
            except ValidationError as error:
                held_errors.append(error)
            return value

    class Payload(ProblemPayload, Mixin):
        type: Literal["about:blank"] = "about:blank"

    value = Payload.model_validate({"token": "x", "type": None, "detail": []})
    assert held_errors
    assert value.type == "about:blank"
    assert value.model_fields_set == {"type", "token"}


@pytest.mark.parametrize("name", ["type", "title", "status", "detail", "instance"])
@pytest.mark.parametrize("alias", ["renamed", AliasPath("type", "uri"), AliasChoices("renamed", "type")])
def test_standard_members_require_canonical_wire_names(name: str, alias: Any) -> None:
    from pydantic import create_model

    with pytest.raises(TypeError, match="must use its RFC wire name"):
        fields: dict[str, Any] = {name: (str, Field(validation_alias=alias))}
        create_model("InvalidProblem", __base__=ProblemPayload, **fields)


@pytest.mark.parametrize(
    "options",
    [
        {"alias": "detail"},
        {"validation_alias": AliasPath("type", "uri")},
        {"validation_alias": AliasChoices("code", "status")},
        {"serialization_alias": "title"},
    ],
)
def test_extension_aliases_cannot_claim_standard_member_names(options: dict[str, Any]) -> None:
    with pytest.raises(TypeError, match="cannot use a reserved RFC member name"):

        class InvalidProblem(ProblemPayload):
            extension: Any = Field(**options)


def test_extensions_keep_alias_paths_values_and_native_validation() -> None:
    class Payload(ProblemPayload):
        resource_id: int = Field(validation_alias=AliasPath("meta", "resource-id"))
        trace_id: str = Field(alias="trace-id")
        raw_detail: Any = Field(alias="raw-detail")

    raw: dict[str, Any] = {"meta": {"resource-id": 7}, "trace-id": "abc", "raw-detail": None, "detail": []}
    original = deepcopy(raw)
    direct = Payload.model_validate(raw)
    transported = registry_for(Payload).decode(raw).payload
    assert direct == transported
    assert direct.resource_id == 7 and direct.trace_id == "abc" and direct.raw_detail is None
    assert json.loads(direct.model_dump_json())["raw-detail"] is None
    assert direct.model_fields_set == {"type", "resource_id", "trace_id", "raw_detail"}
    assert raw == original
    with pytest.raises(ValidationError):
        Payload.model_validate({**raw, "meta": {"resource-id": "invalid"}})


@pytest.mark.parametrize("mode", ["validation", "serialization"])
def test_required_standard_members_keep_native_schema_and_validation(mode: Any) -> None:
    class Payload(ProblemPayload):
        title: str
        detail: str

    assert Payload.model_json_schema(mode=mode)["required"] == ["title", "detail"]
    cases: list[dict[str, Any]] = [{}, {"title": [], "detail": None}]
    for raw in cases:
        with pytest.raises(ValidationError) as error:
            Payload.model_validate(raw)
        assert {item["loc"] for item in error.value.errors()} == {("title",), ("detail",)}
    value = Payload.model_validate({"title": "", "detail": ""})
    assert value.model_dump() == {"type": "about:blank", "title": "", "detail": ""}


def test_redeclared_optional_members_omit_none_and_keep_nullable_extensions() -> None:
    class Payload(ProblemPayload):
        title: str | None = None
        detail: str | None = None
        extension: Any = None

    assert Payload.model_validate({"detail": []}).model_dump() == {"type": "about:blank", "extension": None}
