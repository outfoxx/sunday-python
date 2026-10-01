# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import pytest
from pydantic import ConfigDict, Field, TypeAdapter, ValidationError, field_validator

from sunday import ModelMode, SundayModel, TolerantStrEnum


class State(TolerantStrEnum):
    ACTIVE = "active"
    UNKNOWN = "unknown"


class OpenState(TolerantStrEnum):
    ACTIVE = "active"
    UNKNOWN = "unknown"
    __request_tolerant__ = True


class Item(SundayModel):
    state: State
    values: list[State] = Field(default_factory=list, alias="wire-values")
    optional: str | None = None
    next: Item | None = None

    @field_validator("optional")
    @classmethod
    def non_nullable(cls, value: str | None) -> str:
        if value is None:
            raise ValueError("optional is not nullable")
        return value


def test_native_enum_context_and_explicit_override() -> None:
    adapter = TypeAdapter(State)
    unknown = adapter.validate_python("future")
    assert adapter.validate_python(unknown, context={"mode": ModelMode.RESPONSE}) is unknown
    with pytest.raises(ValidationError) as error:
        adapter.validate_python(unknown, context={"mode": ModelMode.REQUEST})
    assert error.value.errors()[0]["type"] == "unknown_enum"
    assert TypeAdapter(OpenState).validate_python("future", context={"mode": "request"}).value == "future"
    assert adapter.validate_python("active", context={"mode": "request"}) is State.ACTIVE


def test_existing_instances_are_revalidated_with_wire_presence_and_paths() -> None:
    item = Item(state=State.ACTIVE)
    fields = item.model_fields_set.copy()
    assert Item.model_validate(item, context={"mode": "request"}) == item
    assert item.model_fields_set == fields
    item.values = [State.ACTIVE, State("future")]
    with pytest.raises(ValidationError) as error:
        Item.model_validate(item, context={"mode": "request"})
    assert error.value.errors()[0]["loc"] == ("wire-values", 1)
    assert item.values[1].value == "future"
    item.values.pop()
    Item.model_validate(item, context={"mode": "request"})
    item.optional = None
    with pytest.raises(ValidationError, match="optional is not nullable"):
        Item.model_validate(item, context={"mode": "request"})


def test_cycles_are_rejected_but_shared_references_are_rechecked() -> None:
    item = Item(state=State.ACTIVE)
    adapter = TypeAdapter(list[Item])
    assert adapter.validate_python([item, item], context={"mode": "request"}) == [item, item]
    item.next = item
    with pytest.raises(ValidationError) as error:
        Item.model_validate(item, context={"mode": "request"})
    assert error.value.errors()[0]["type"] in {"model_cycle", "recursion_loop"}
    item.next = None
    Item.model_validate(item, context={"mode": "request"})


@pytest.mark.anyio
async def test_transport_revalidates_deferred_payloads_before_encoding() -> None:
    from sunday import MediaType, RequestEncodingError, RequestPayloadSpec, RequestSpec
    from sunday.httpx import HttpxTransport

    item = Item(state=State.ACTIVE)
    adapter = TypeAdapter(Item)
    requests = [
        RequestSpec(
            method="PUT",
            path_template="/items",
            body=item,
            body_adapter=adapter,
            content_types=(MediaType("application/json"),),
        ),
        RequestSpec(
            method="PUT",
            path_template="/items",
            payload=RequestPayloadSpec(body=item, content_types=(MediaType("application/json"),), body_adapter=adapter),
        ),
    ]
    async with HttpxTransport(base_url="https://example.com") as transport:
        for spec in requests:
            await transport.transport_request(spec)
            item.values = [State("future")]
            with pytest.raises(RequestEncodingError) as error:
                await transport.transport_request(spec)
            assert isinstance(error.value.__cause__, ValidationError)
            item.values = []


def test_litestar_uses_request_mode_before_invocation_and_response_mode_afterward() -> None:
    from litestar import Litestar, post
    from litestar.testing import TestClient

    from sunday.litestar import SundayPlugin

    calls = 0

    @post("/items")
    async def update(data: Item) -> Item:
        nonlocal calls
        calls += 1
        data.state = State("future")
        return data

    with TestClient(Litestar([update], plugins=[SundayPlugin()])) as client:
        rejected = client.post("/items", json={"state": "future"})
        assert rejected.status_code == 400
        assert calls == 0
        accepted = client.post("/items", json={"state": "active"})
        assert accepted.status_code == 201
        assert accepted.json()["state"] == "future"
        assert calls == 1


@pytest.mark.parametrize("use_json", [False, True])
@pytest.mark.parametrize("use_alias", [False, True])
def test_native_input_name_selection_survives_revalidation_adapter(use_json: bool, use_alias: bool) -> None:
    import json

    class Named(SundayModel):
        model_config = ConfigDict(extra="allow")
        project_id: str | None = Field(default=None, alias="projectId")

        @field_validator("project_id")
        @classmethod
        def non_nullable(cls, value: str | None) -> str:
            if value is None:
                raise ValueError("project ID is not nullable")
            return value

    selected, ignored = ("projectId", "project_id") if use_alias else ("project_id", "projectId")
    data = {selected: "selected", ignored: None}
    if use_json:
        value = Named.model_validate_json(json.dumps(data), by_alias=use_alias, by_name=not use_alias)
    else:
        value = Named.model_validate(data, by_alias=use_alias, by_name=not use_alias)
    assert value.project_id == "selected"
    data[selected], data[ignored] = None, "ignored"
    with pytest.raises(ValidationError, match="not nullable"):
        if use_json:
            Named.model_validate_json(json.dumps(data), by_alias=use_alias, by_name=not use_alias)
        else:
            Named.model_validate(data, by_alias=use_alias, by_name=not use_alias)


def test_instance_adapter_preserves_schema_and_calls_native_predicates_once() -> None:
    calls = 0

    class Named(SundayModel):
        project_id: str = Field(alias="projectId")

        @field_validator("project_id")
        @classmethod
        def count(cls, value: str) -> str:
            nonlocal calls
            calls += 1
            return value

    model = Named(project_id="valid")
    assert calls == 1
    Named.model_validate(model, strict=True)
    assert calls == 2
    assert Named.model_json_schema()["properties"] == {"projectId": {"title": "Projectid", "type": "string"}}
    assert model.model_dump(by_alias=True) == {"projectId": "valid"}


@pytest.mark.parametrize("use_alias", [False, True])
@pytest.mark.parametrize("extra", ["forbid", "allow"])
def test_existing_instance_name_selection_preserves_presence_and_native_validation(use_alias: bool, extra: str) -> None:
    class Named(SundayModel):
        model_config = ConfigDict(extra=extra)  # type: ignore[typeddict-item]
        project_id: str | None = Field(default=None, alias="projectId")

        @field_validator("project_id")
        @classmethod
        def non_nullable(cls, value: str | None) -> str:
            if value is None:
                raise ValueError("project ID is not nullable")
            return value

    original = Named()
    before = original.model_dump(), original.model_fields_set.copy()
    adapter = TypeAdapter(Named)
    adapter.validate_python(original, strict=True, by_alias=use_alias, by_name=not use_alias)
    assert (original.model_dump(), original.model_fields_set) == before
    original.project_id = "valid"
    if extra == "allow":
        original.note = "extra"
    validated = adapter.validate_python(original, strict=True, by_alias=use_alias, by_name=not use_alias)
    assert validated.project_id == "valid"
    assert validated.model_fields_set == original.model_fields_set
    assert validated.__pydantic_extra__ == original.__pydantic_extra__
    original.project_id = None
    with pytest.raises(ValidationError, match="not nullable"):
        adapter.validate_python(original, strict=True, by_alias=use_alias, by_name=not use_alias)


@pytest.mark.parametrize("use_alias", [False, True])
def test_before_validator_copies_preserve_instance_lookup_and_updated_field_values(use_alias: bool) -> None:
    from pydantic import model_validator

    class Named(SundayModel):
        project_id: str = Field(alias="projectId")

        @model_validator(mode="before")
        @classmethod
        def normalize(cls, value: object) -> object:
            if not isinstance(value, dict):
                return value
            data = value.copy()
            key = "projectId" if "projectId" in data else "project_id"
            data[key] = data[key].strip()
            return data

    original = Named(project_id="valid")
    original.project_id = " changed "
    checked = Named.model_validate(original, strict=True, by_alias=use_alias, by_name=not use_alias)
    assert checked.project_id == "changed"
    assert original.project_id == " changed "
    assert checked.model_fields_set == original.model_fields_set == {"project_id"}


@pytest.mark.parametrize("initial_alias", [False, True])
@pytest.mark.parametrize("use_alias", [False, True])
def test_ambiguous_extra_revalidation_never_discards_a_value(initial_alias: bool, use_alias: bool) -> None:
    from typing import Annotated

    from pydantic import AfterValidator, model_validator

    checks: list[int] = []
    models: list[dict[str, int]] = []

    def checked(value: int) -> int:
        checks.append(value)
        return value

    class Named(SundayModel):
        model_config = ConfigDict(extra="allow")
        __pydantic_extra__: dict[str, Annotated[int, AfterValidator(checked)]] = Field(init=False)
        project_id: str = Field(alias="projectId")

        @model_validator(mode="after")
        def seen(self) -> Named:
            models.append(dict(self.__pydantic_extra__))
            return self

    selected, ignored = ("projectId", "project_id") if initial_alias else ("project_id", "projectId")
    original = Named.model_validate({selected: "field", ignored: 7}, by_alias=initial_alias, by_name=not initial_alias)
    before = original.__dict__.copy(), original.model_fields_set.copy(), dict(original.__pydantic_extra__)
    checks.clear()
    models.clear()
    if initial_alias and use_alias:
        checked_model = Named.model_validate(original, strict=True, by_alias=use_alias, by_name=not use_alias)
        assert checked_model.project_id == "field"
        assert checked_model.__pydantic_extra__ == {ignored: 7}
        assert checks == [7]
        assert models == [{ignored: 7}]
    else:
        with pytest.raises(ValidationError) as error:
            Named.model_validate(original, strict=True, by_alias=use_alias, by_name=not use_alias)
        assert error.value.errors()[0]["type"] == "model_key_conflict"
        assert error.value.errors()[0]["loc"] == (ignored,)
        assert checks == []
        assert models == []
    assert (original.__dict__, original.model_fields_set, original.__pydantic_extra__) == before
    for mode in ("validation", "serialization"):
        schema = Named.model_json_schema(mode=mode)
        assert schema["type"] == "object"
        assert schema["properties"]["projectId"]["type"] == "string"
        assert schema["additionalProperties"]["type"] == "integer"


@pytest.mark.parametrize("use_alias", [False, True])
def test_extra_cannot_be_promoted_into_an_omitted_field_during_revalidation(use_alias: bool) -> None:
    class OptionalNamed(SundayModel):
        model_config = ConfigDict(extra="allow")
        project_id: str | None = Field(default=None, alias="projectId")

    original = OptionalNamed.model_validate({"projectId": "extra"}, by_alias=False, by_name=True)
    before = original.__dict__.copy(), original.model_fields_set.copy(), dict(original.__pydantic_extra__)
    assert original.project_id is None
    with pytest.raises(ValidationError) as error:
        OptionalNamed.model_validate(original, by_alias=use_alias, by_name=not use_alias)
    assert error.value.errors()[0]["type"] == "model_key_conflict"
    assert error.value.errors()[0]["loc"] == ("projectId",)
    assert (original.__dict__, original.model_fields_set, original.__pydantic_extra__) == before


def test_nested_collisions_use_the_original_wire_path() -> None:
    class Named(SundayModel):
        model_config = ConfigDict(extra="allow")
        project_id: str = Field(alias="projectId")

    class Container(SundayModel):
        values: list[Named] = Field(alias="wire-values")

    item = Named.model_validate({"project_id": "field", "projectId": "extra"}, by_alias=False, by_name=True)
    with pytest.raises(ValidationError) as error:
        Container.model_validate({"wire-values": [item]})
    assert error.value.errors()[0]["type"] == "model_key_conflict"
    assert error.value.errors()[0]["loc"] == ("wire-values", 0, "projectId")
