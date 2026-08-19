# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from dataclasses import dataclass

import yaml

from .codecs import WireMode, _json_value
from .media import MediaType


@dataclass(frozen=True, slots=True)
class YamlCodec:
    """Optional safe YAML request and response codec."""

    media_types: tuple[MediaType, ...] = (
        MediaType("application/yaml"),
        MediaType("application/*+yaml"),
        MediaType("text/yaml"),
    )

    def encode(self, value: object) -> bytes:
        """Encode ``value`` as UTF-8 YAML without Python-specific tags."""
        return yaml.safe_dump(_json_value(value, WireMode.REQUEST), allow_unicode=True, sort_keys=False).encode()

    def decode(self, value: bytes, media_type: MediaType) -> object:
        """Safely decode a YAML response."""
        charset = media_type.parameter("charset") or "utf-8"
        return yaml.safe_load(value.decode(charset))
