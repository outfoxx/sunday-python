# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from collections.abc import Mapping
from enum import StrEnum

from pydantic import ValidationInfo


class ModelMode(StrEnum):
    """Payload direction, independent of whether an application is a client or server."""

    REQUEST = "request"
    RESPONSE = "response"


def model_mode(info: ValidationInfo) -> ModelMode:
    """Resolve native Pydantic validation context, defaulting standalone codecs to responses."""
    context = info.context
    if isinstance(context, Mapping):
        return ModelMode(context.get("mode", ModelMode.RESPONSE))
    return ModelMode.RESPONSE
