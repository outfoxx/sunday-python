# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from typing import Annotated, Any

import pytest
from pydantic import Field, TypeAdapter, ValidationError, field_validator

from sunday import ModelIntersection, SundayModel


def test_native_intersection_retains_payload_type_aliases_and_presence_without_constructing_metadata() -> None:
    class Rules(SundayModel):
        count: Annotated[int, Field(le=5, alias="wire-count")]

        def __init__(self, **data: Any) -> None:
            raise AssertionError("Metadata model must never be constructed")

    class Payload(SundayModel):
        count: Annotated[int, Field(le=9, alias="wire-count")]
        note: str | None = None

    adapter: TypeAdapter[Payload] = TypeAdapter(Annotated[Payload, ModelIntersection(Rules)])
    payload = Payload(count=2)
    checked = adapter.validate_python(payload, strict=True)
    assert type(checked) is Payload
    assert checked.count == 2
    assert payload.model_fields_set == {"count"}
    assert "allOf" in adapter.json_schema()
    payload.count = 9
    for candidate in (payload, {"wire-count": 9}):
        with pytest.raises(ValidationError) as raised:
            adapter.validate_python(candidate)
        assert raised.value.errors()[0]["loc"] == ("wire-count",)
        assert raised.value.errors()[0]["type"] == "less_than_equal"
    assert payload.count == 9
    assert Payload.model_validate(payload).count == 9


def test_intersection_uses_native_field_callbacks_once() -> None:
    calls: list[str] = []

    class Rules(SundayModel):
        text: str

        @field_validator("text")
        @classmethod
        def checked(cls, value: str) -> str:
            calls.append(value)
            return value

    class Payload(SundayModel):
        text: str

    adapter: TypeAdapter[Payload] = TypeAdapter(Annotated[Payload, ModelIntersection(Rules)])
    checked = adapter.validate_python({"text": "first"})
    checked.text = "second"
    adapter.validate_python(checked)
    assert calls == ["first", "second"]
