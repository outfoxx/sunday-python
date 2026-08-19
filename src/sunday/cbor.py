# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from dataclasses import dataclass

import cbor2

from .codecs import _json_value
from .media import MediaType


@dataclass(frozen=True, slots=True)
class CborCodec:
    """Optional CBOR request and response codec."""

    media_types: tuple[MediaType, ...] = (MediaType("application/cbor"), MediaType("application/*+cbor"))

    def encode(self, value: object) -> bytes:
        """Encode ``value`` as CBOR after JSON-compatible normalization."""
        return cbor2.dumps(_json_value(value))

    def decode(self, value: bytes, media_type: MediaType) -> object:
        """Decode CBOR bytes into Python values."""
        del media_type
        return cbor2.loads(value)
