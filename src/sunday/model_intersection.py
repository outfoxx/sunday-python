# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Native field-schema intersections that preserve application payload types."""

from dataclasses import dataclass
from typing import Any

from pydantic import GetCoreSchemaHandler, GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema, core_schema

from .models import SundayModel, _ModelFieldView


@dataclass(frozen=True)
class ModelIntersection:
    """Apply a generated field contract without constructing its metadata model.

    Use as ``Annotated[PayloadUnion, ModelIntersection(CommonFields)]``. CommonFields
    declares native Pydantic fields and validators; the original payload union still
    owns decoding, representation, and serialization. No application values are mutated.
    """

    fields: type[SundayModel]
    skip: tuple[type[Any], ...] = ()

    def __get_pydantic_core_schema__(self, source: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
        """Keep native field validators while removing metadata-model construction."""
        schema = handler.resolve_ref_schema(handler.generate_schema(self.fields))
        while schema["type"] == "function-before":
            schema = handler.resolve_ref_schema(schema["schema"])
        if schema["type"] != "model":
            raise TypeError("ModelIntersection requires a generated field metadata model")
        common = self._field_schema(schema["schema"])

        def check(value: Any, validate: core_schema.ValidatorFunctionWrapHandler) -> Any:
            if not isinstance(value, self.skip):
                validate(_ModelFieldView(value) if isinstance(value, SundayModel) else value)
            return value

        return core_schema.chain_schema([core_schema.no_info_wrap_validator_function(check, common), handler(source)])

    def _field_schema(self, schema: CoreSchema) -> CoreSchema:
        if schema["type"] == "function-before":
            return {**schema, "schema": self._field_schema(schema["schema"])}
        if schema["type"] != "model-fields":
            raise TypeError("ModelIntersection supports native field and before-model validators")
        fields = {
            name: core_schema.typed_dict_field(
                field["schema"],
                required=field["schema"]["type"] != "default",
                validation_alias=field.get("validation_alias"),
                serialization_alias=field.get("serialization_alias"),
                metadata=field.get("metadata"),
            )
            for name, field in schema["fields"].items()
        }
        return core_schema.typed_dict_schema(
            fields,
            extra_behavior=self.fields.model_config.get("extra", "ignore"),
            extras_schema=schema.get("extras_schema"),
            config=core_schema.CoreConfig(validate_by_alias=True, validate_by_name=True),
        )

    def __get_pydantic_json_schema__(self, schema: CoreSchema, handler: GetJsonSchemaHandler) -> JsonSchemaValue:
        """Document both the common contract and the original union alternatives."""
        if schema["type"] != "chain":
            return handler(schema)
        return {"allOf": [handler(step) for step in schema["steps"]]}
