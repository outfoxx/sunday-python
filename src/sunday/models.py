# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from enum import StrEnum
from typing import Any, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    GetCoreSchemaHandler,
    ValidationError,
    ValidationInfo,
)
from pydantic_core import CoreSchema, PydanticCustomError, core_schema

from .model_mode import ModelMode, model_mode


class SundayModel(BaseModel):
    """Base model configuration used by generated Sunday models."""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
        """Revalidate existing instances through the same native field schema as decoded input."""
        schema = dict(handler(source_type))
        reference = schema.pop("ref", None)
        # Keep the reference outside normalization so Pydantic's recursion guard sees
        # the original object identity before a fresh field view is created.
        return core_schema.no_info_before_validator_function(cls._model_field_view, schema, ref=reference)

    @classmethod
    def _model_field_view(cls, value: Any) -> Any:
        if not isinstance(value, cls):
            return value
        return _ModelFieldView(value)


class _ModelFieldView(dict[str, Any]):
    """Let native alias lookup select field keys before its extra-field pass.

    A real dictionary preserves strict model parsing. Only this temporary view changes;
    the application's model, presence set, and extras remain untouched.
    Conflicting keys cannot share a native lookup slot, so they fail before normalization
    can overwrite a field or promote an extra value into an omitted field.
    """

    def __init__(self, value: SundayModel) -> None:
        super().__init__(value.__pydantic_extra__ or {})
        self._model_name = type(value).__name__
        self._fields: dict[str, str] = {}
        self._keys: dict[str, str] = {}
        for name, field in type(value).model_fields.items():
            alias = field.validation_alias if isinstance(field.validation_alias, str) else field.alias or name
            self._fields[name] = name
            self._fields[alias] = name
            self._keys[name] = alias
            if alias in self:
                self._conflict(alias)
            if (field.is_required() or name in value.model_fields_set) and name in value.__dict__:
                self[alias] = value.__dict__[name]

    def copy(self) -> _ModelFieldView:
        # Generated before validators normalize a copy, preserving native lookup
        # options and leaving the original application's model untouched.
        result = dict.__new__(_ModelFieldView)
        dict.__init__(result, self)
        result._model_name = self._model_name
        result._fields = self._fields.copy()
        result._keys = self._keys.copy()
        return result

    def get(self, key: str, default: Any = None) -> Any:
        name = self._fields.get(key)
        if name is None:
            return super().get(key, default)
        previous = self._keys[name]
        if previous not in self:
            return default
        value = self[previous]
        if previous != key:
            if key in self:
                self._conflict(key)
            self.pop(previous, None)
            self[key] = value
            self._keys[name] = key
        return value

    def _conflict(self, key: str) -> None:
        raise ValidationError.from_exception_data(
            self._model_name,
            [
                {
                    "type": PydanticCustomError(
                        "model_key_conflict", "Model field and extra property have conflicting names"
                    ),
                    "loc": (key,),
                    "input": self[key],
                }
            ],
        )


class TolerantStrEnum(StrEnum):
    """String enum that preserves unknown wire values using a configured fallback name.

    Generated subclasses may set ``__unknown_member_name__`` to the declared fallback
    member name. The conventional fallback name is ``UNKNOWN``.
    """

    @classmethod
    def _missing_(cls, value: object) -> Self | None:
        if not isinstance(value, str):
            return None

        fallback_name = getattr(cls, "__unknown_member_name__", "UNKNOWN")
        if fallback_name not in cls.__members__:
            return None

        member = str.__new__(cls, value)
        member._name_ = fallback_name
        member._value_ = value
        return member

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
        """Apply directional tolerance through Pydantic's native enum schema."""
        return core_schema.with_info_after_validator_function(cls._validate_mode, handler(source_type))

    @classmethod
    def _validate_mode(cls, value: Self, info: ValidationInfo) -> Self:
        if (
            model_mode(info) == ModelMode.REQUEST
            and not getattr(cls, "__request_tolerant__", False)
            and value.name == getattr(cls, "__unknown_member_name__", "UNKNOWN")
        ):
            raise PydanticCustomError("unknown_enum", "Unknown enum value is not permitted in requests")
        return value
