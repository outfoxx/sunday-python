# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from typing import Any

from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, PydanticUseDefault, core_schema

from .models import SundayModel
from .unset import UNSET


class SundayPatchModel(SundayModel):
    """A typed partial update validated by Pydantic.

    Fields use ``T | UnsetType`` (plus ``None`` when deletion is permitted) and
    ``Field(default_factory=lambda: UNSET, exclude_if=is_unset)``. Values are
    validated normally; unchanged fields neither apply schema defaults nor
    participate in field validation. Assign ``UNSET`` to cancel an update.
    """

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
        """Handle unchanged fields after native alias selection and before field validators."""
        schema = super().__get_pydantic_core_schema__(source_type, handler)
        cls._patch_field_schemas(schema)
        return schema

    @classmethod
    def _patch_field_schemas(cls, schema: Any) -> None:
        if not isinstance(schema, dict):
            return
        if schema.get("type") == "model-fields":
            for field in schema["fields"].values():
                field_schema = field["schema"]
                if field_schema.get("type") == "default":
                    # Leave the default wrapper outside the adapter so Pydantic can
                    # select the field's UNSET factory without running value constraints.
                    field["schema"] = {
                        **field_schema,
                        "schema": core_schema.no_info_before_validator_function(
                            cls._unchanged_field, field_schema["schema"]
                        ),
                    }
        else:
            cls._patch_field_schemas(schema.get("schema"))

    @staticmethod
    def _unchanged_field(value: Any) -> Any:
        if value is UNSET:
            raise PydanticUseDefault
        return value
