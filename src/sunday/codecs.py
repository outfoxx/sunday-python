# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import base64
import json
from collections.abc import Mapping, Sequence, Set
from dataclasses import dataclass
from datetime import date, datetime, time
from enum import Enum, StrEnum
from typing import Protocol
from urllib.parse import urlencode
from uuid import UUID

from pydantic import BaseModel

from .media import MediaType


class WireMode(StrEnum):
    """Controls how models distinguish absent values from explicit ``None``."""

    REQUEST = "request"
    RESPONSE = "response"
    PATCH = "patch"


class MediaTypeEncoder(Protocol):
    """Encodes a request body for one or more media types."""

    @property
    def media_types(self) -> tuple[MediaType, ...]:
        """Return the supported media type ranges."""
        ...

    def encode(self, value: object) -> bytes:
        """Encode a request value into bytes."""
        ...


class MediaTypeDecoder(Protocol):
    """Decodes a response body for one or more media types."""

    @property
    def media_types(self) -> tuple[MediaType, ...]:
        """Return the supported media type ranges."""
        ...

    def decode(self, value: bytes, media_type: MediaType) -> object:
        """Decode response bytes into a Python value."""
        ...


class _MediaTypeCodec(MediaTypeEncoder, MediaTypeDecoder, Protocol):
    pass


class MediaTypeEncoders:
    """Ordered registry of request media type encoders."""

    def __init__(self, encoders: Sequence[MediaTypeEncoder] = ()) -> None:
        self._encoders = list(encoders)

    @classmethod
    def defaults(cls) -> MediaTypeEncoders:
        """Create a registry with built-in and installed optional encoders."""
        return cls(_default_codecs())

    def register(self, encoder: MediaTypeEncoder, *, first: bool = False) -> None:
        """Register an encoder, optionally before existing encoders."""
        if first:
            self._encoders.insert(0, encoder)
        else:
            self._encoders.append(encoder)

    def find(self, media_type: MediaType) -> MediaTypeEncoder | None:
        """Return the first encoder supporting ``media_type``."""
        return next(
            (
                encoder
                for encoder in self._encoders
                if any(supported.matches(media_type) for supported in encoder.media_types)
            ),
            None,
        )


class MediaTypeDecoders:
    """Ordered registry of response media type decoders."""

    def __init__(self, decoders: Sequence[MediaTypeDecoder] = ()) -> None:
        self._decoders = list(decoders)

    @classmethod
    def defaults(cls) -> MediaTypeDecoders:
        """Create a registry with built-in and installed optional decoders."""
        return cls(_default_codecs())

    def register(self, decoder: MediaTypeDecoder, *, first: bool = False) -> None:
        """Register a decoder, optionally before existing decoders."""
        if first:
            self._decoders.insert(0, decoder)
        else:
            self._decoders.append(decoder)

    def find(self, media_type: MediaType) -> MediaTypeDecoder | None:
        """Return the first decoder supporting ``media_type``."""
        return next(
            (
                decoder
                for decoder in self._decoders
                if any(supported.matches(media_type) for supported in decoder.media_types)
            ),
            None,
        )


@dataclass(frozen=True, slots=True)
class JsonCodec:
    """JSON request and response codec, including structured JSON suffixes."""

    media_types: tuple[MediaType, ...] = (MediaType("application/json"), MediaType("application/*+json"))
    wire_mode: WireMode = WireMode.REQUEST

    def encode(self, value: object) -> bytes:
        """Encode ``value`` as compact UTF-8 JSON."""
        return json.dumps(_json_value(value, self.wire_mode), separators=(",", ":"), ensure_ascii=False).encode()

    def decode(self, value: bytes, media_type: MediaType) -> object:
        """Decode UTF-8 JSON bytes into Python values."""
        del media_type
        return json.loads(value)


@dataclass(frozen=True, slots=True)
class TextCodec:
    """UTF text request and response codec."""

    media_types: tuple[MediaType, ...] = (MediaType("text/*"),)

    def encode(self, value: object) -> bytes:
        """Encode the string representation of ``value`` as UTF-8."""
        return str(value).encode()

    def decode(self, value: bytes, media_type: MediaType) -> object:
        """Decode text using its declared charset or UTF-8."""
        charset = media_type.parameter("charset") or "utf-8"
        return value.decode(charset)


@dataclass(frozen=True, slots=True)
class BinaryCodec:
    """Opaque binary request and response codec."""

    media_types: tuple[MediaType, ...] = (MediaType("application/octet-stream"),)

    def encode(self, value: object) -> bytes:
        """Normalize a bytes-like value to immutable bytes."""
        if not isinstance(value, (bytes, bytearray, memoryview)):
            raise TypeError("Binary request bodies must be bytes-like")
        return bytes(value)

    def decode(self, value: bytes, media_type: MediaType) -> object:
        """Return opaque response bytes unchanged."""
        del media_type
        return value


@dataclass(frozen=True, slots=True)
class FormUrlEncodedCodec:
    """URL-encoded form request and response codec."""

    media_types: tuple[MediaType, ...] = (MediaType("application/x-www-form-urlencoded"),)

    def encode(self, value: object) -> bytes:
        """Encode a mapping as an HTML form body."""
        encoded = _json_value(value, WireMode.REQUEST)
        if not isinstance(encoded, Mapping):
            raise TypeError("Form request bodies must be mappings")
        return urlencode(encoded, doseq=True).encode()

    def decode(self, value: bytes, media_type: MediaType) -> object:
        """Decode an HTML form body while preserving repeated values."""
        from urllib.parse import parse_qs

        charset = media_type.parameter("charset") or "utf-8"
        return parse_qs(value.decode(charset), keep_blank_values=True)


def _json_value(value: object, wire_mode: WireMode = WireMode.REQUEST) -> object:
    sunday_wire = getattr(value, "__sunday_wire__", None)
    if callable(sunday_wire):
        return sunday_wire()
    if isinstance(value, BaseModel):
        if wire_mode in {WireMode.REQUEST, WireMode.PATCH}:
            return value.model_dump(mode="json", by_alias=True, exclude_unset=True)
        return value.model_dump(mode="json", by_alias=True, exclude_none=True)
    if isinstance(value, Enum):
        return _json_value(value.value, wire_mode)
    if isinstance(value, (datetime, date, time, UUID)):
        return value.isoformat() if isinstance(value, (datetime, date, time)) else str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, Mapping):
        return {str(key): _json_value(item, wire_mode) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray, memoryview)):
        return [_json_value(item, wire_mode) for item in value]
    if isinstance(value, Set):
        return [_json_value(item, wire_mode) for item in value]
    return value


def _default_codecs() -> tuple[_MediaTypeCodec, ...]:
    codecs: list[_MediaTypeCodec] = [JsonCodec()]
    codecs.extend(_optional_codecs())
    codecs.extend((FormUrlEncodedCodec(), TextCodec(), BinaryCodec()))
    return tuple(codecs)


def _optional_codecs() -> tuple[_MediaTypeCodec, ...]:
    codecs: list[_MediaTypeCodec] = []
    try:
        from .cbor import CborCodec
    except ModuleNotFoundError as error:
        if error.name != "cbor2":
            raise
    else:
        codecs.append(CborCodec())

    try:
        from .xml import XmlCodec
    except ModuleNotFoundError as error:
        if error.name != "xmltodict":
            raise
    else:
        codecs.append(XmlCodec())

    try:
        from .yaml import YamlCodec
    except ModuleNotFoundError as error:
        if error.name != "yaml":
            raise
    else:
        codecs.append(YamlCodec())

    return tuple(codecs)
