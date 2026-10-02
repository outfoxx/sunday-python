# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from enum import Enum
from typing import Any

from pydantic import GetCoreSchemaHandler, GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema, PydanticOmit, core_schema


class UnsetType(Enum):
    """The unchanged state of a PATCH field, distinct from an explicit null."""

    UNSET = "UNSET"

    def __repr__(self) -> str:
        return "UNSET"

    @classmethod
    def __get_pydantic_core_schema__(cls, source: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
        """Accept the Python sentinel without reserving a JSON string value."""
        return core_schema.is_instance_schema(cls)

    @classmethod
    def __get_pydantic_json_schema__(cls, schema: CoreSchema, handler: GetJsonSchemaHandler) -> JsonSchemaValue:
        """Keep the application-only state out of the wire schema."""
        raise PydanticOmit


UNSET = UnsetType.UNSET
"""Leave a PATCH field unchanged; serialization omits this value."""


def is_unset(value: object) -> bool:
    """Return whether a value represents an unchanged PATCH field."""
    return value is UNSET
