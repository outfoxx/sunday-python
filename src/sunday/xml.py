# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import xmltodict

from .codecs import WireMode, _json_value
from .media import MediaType


@dataclass(frozen=True, slots=True)
class XmlCodec:
    """Optional XML mapping request and response codec."""

    media_types: tuple[MediaType, ...] = (
        MediaType("application/xml"),
        MediaType("application/*+xml"),
        MediaType("text/xml"),
    )

    def encode(self, value: object) -> bytes:
        """Encode a one-root mapping as UTF-8 XML."""
        normalized = _json_value(value, WireMode.REQUEST)
        if not isinstance(normalized, Mapping) or len(normalized) != 1:
            raise TypeError("XML request bodies must be mappings with exactly one root element")
        return xmltodict.unparse(normalized, full_document=True).encode()

    def decode(self, value: bytes, media_type: MediaType) -> object:
        """Decode XML without entity expansion or external entities."""
        charset = media_type.parameter("charset") or "utf-8"
        return xmltodict.parse(value.decode(charset), disable_entities=True)
