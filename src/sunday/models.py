# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from enum import StrEnum
from typing import Self

from pydantic import BaseModel, ConfigDict


class SundayModel(BaseModel):
    """Base model configuration used by generated Sunday models."""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)


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
