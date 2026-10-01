# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from typing import Any, ClassVar, Self

from pydantic import RootModel, ValidationInfo, model_validator
from pydantic_core import PydanticCustomError

from .model_mode import ModelMode, model_mode


class UnknownModel(RootModel[dict[str, Any]]):
    """Native Pydantic base for a declared discriminator fallback preserving its wire payload."""

    __request_tolerant__: ClassVar[bool] = False

    @model_validator(mode="after")
    def _validate_model_mode(self, info: ValidationInfo) -> Self:
        if model_mode(info) == ModelMode.REQUEST and not self.__request_tolerant__:
            raise PydanticCustomError("unknown_union", "Unknown union variant is not permitted in requests")
        return self
