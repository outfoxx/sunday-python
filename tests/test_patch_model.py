# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

import copy
import pickle
from typing import Annotated, Any

import pytest
from pydantic import Field, ValidationError

from sunday import UNSET, JsonCodec, SundayPatchModel, TolerantStrEnum, UnsetType, is_unset


class State(TolerantStrEnum):
    ACTIVE = "active"
    UNKNOWN = "unknown"


class Update(SundayPatchModel):
    title: Annotated[str, Field(min_length=2)] | UnsetType = Field(default_factory=lambda: UNSET, exclude_if=is_unset)
    description: str | UnsetType | None = Field(default_factory=lambda: UNSET, exclude_if=is_unset)
    state: State | UnsetType = Field(default_factory=lambda: UNSET, exclude_if=is_unset)
    count: Annotated[int, Field(ge=1)] | UnsetType = Field(default_factory=lambda: UNSET, exclude_if=is_unset)
    display_name: str | UnsetType = Field(default_factory=lambda: UNSET, exclude_if=is_unset, alias="display-name")


@pytest.mark.parametrize("values", [{}, {"title": "new"}, {"description": None}, {"description": "text", "count": 2}])
def test_patch_wire_states(values: dict[str, Any]) -> None:
    patch = Update.model_validate(values)
    assert patch.model_dump(mode="json") == values
    assert Update.model_validate_json(JsonCodec().encode(patch)).model_dump(mode="json") == values
    assert Update.model_validate(patch, context={"mode": "request"}).model_dump(mode="json") == values
    if "title" not in values:
        assert patch.title is UNSET


def test_unset_can_cancel_an_update_without_mutating_the_original_during_validation() -> None:
    patch = Update(title="set", display_name="old")
    patch.title = UNSET
    patch.display_name = UNSET
    fields = patch.model_fields_set.copy()
    assert patch.model_dump(mode="json") == {}
    assert Update.model_validate(patch, context={"mode": "request"}).model_dump(mode="json") == {}
    assert patch.model_fields_set == fields
    assert Update(title=UNSET, display_name=UNSET).model_dump(mode="json") == {}
    assert Update.model_validate({"display-name": UNSET}).model_dump(mode="json") == {}
    assert Update(title="UNSET").title == "UNSET"


@pytest.mark.parametrize("values", [{"title": None}, {"title": "x"}, {"count": 0}, {"count": None}])
def test_patch_fields_retain_native_constraints(values: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        Update.model_validate(values)


def test_mutated_patch_is_revalidated_in_request_mode() -> None:
    patch = Update(state=State("future"))
    with pytest.raises(ValidationError, match="Unknown enum"):
        Update.model_validate(patch, context={"mode": "request"})
    patch.state = UNSET
    Update.model_validate(patch, context={"mode": "request"})
    patch.title = "x"
    with pytest.raises(ValidationError):
        Update.model_validate(patch, context={"mode": "request"})


def test_unset_copy_pickle_and_schema() -> None:
    assert copy.copy(UNSET) is UNSET
    assert copy.deepcopy(UNSET) is UNSET
    assert pickle.loads(pickle.dumps(UNSET)) is UNSET
    assert copy.deepcopy(Update()).title is UNSET
    assert repr(UNSET) == "UNSET"
    schema = Update.model_json_schema()
    assert "required" not in schema
    assert schema["properties"]["title"]["type"] == "string"
    assert "default" not in schema["properties"]["title"]
    assert "UnsetType" not in str(schema)


def test_inheritance_aliases_and_nested_patch_models() -> None:
    class Child(Update):
        nested: Update | UnsetType = Field(default_factory=lambda: UNSET, exclude_if=is_unset)

    patch = Child.model_validate({"display-name": "wire", "nested": {"description": None}})
    assert Child.model_validate(patch, context={"mode": "request"}).model_dump(mode="json") == {
        "display-name": "wire",
        "nested": {"description": None},
    }
    patch.nested = UNSET
    assert patch.model_dump(mode="json") == {"display-name": "wire"}


def test_unset_preserves_native_alias_precedence_and_per_call_selection() -> None:
    fields = {"display-name": UNSET, "display_name": "name"}
    assert Update.model_validate(fields).display_name is UNSET
    assert Update.model_validate(fields, by_alias=False, by_name=True).display_name == "name"
    assert Update.model_validate({"display-name": "alias", "display_name": UNSET}).display_name == "alias"
    assert fields["display-name"] is UNSET
