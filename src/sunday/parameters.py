# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from enum import Enum, StrEnum
from typing import TypeGuard
from urllib.parse import quote

from pydantic import BaseModel


class ParameterLocation(StrEnum):
    """Location of an HTTP operation parameter."""

    PATH = "path"
    QUERY = "query"
    HEADER = "header"
    COOKIE = "cookie"


class ParameterStyle(StrEnum):
    """OpenAPI parameter serialization style."""

    SIMPLE = "simple"
    LABEL = "label"
    MATRIX = "matrix"
    FORM = "form"
    SPACE_DELIMITED = "spaceDelimited"
    PIPE_DELIMITED = "pipeDelimited"
    DEEP_OBJECT = "deepObject"


@dataclass(frozen=True, slots=True)
class ParameterSpec:
    """A parameter value and its wire serialization rules."""

    name: str
    value: object | None
    location: ParameterLocation
    style: ParameterStyle | None = None
    explode: bool | None = None
    allow_reserved: bool = False
    allow_empty_value: bool = False

    @property
    def effective_style(self) -> ParameterStyle:
        """Return the OpenAPI default style for this parameter location."""
        if self.style is not None:
            return self.style
        if self.location in {ParameterLocation.QUERY, ParameterLocation.COOKIE}:
            return ParameterStyle.FORM
        return ParameterStyle.SIMPLE

    @property
    def effective_explode(self) -> bool:
        """Return the OpenAPI default explode behavior for the effective style."""
        return self.explode if self.explode is not None else self.effective_style == ParameterStyle.FORM


@dataclass(frozen=True, slots=True)
class EncodedParameters:
    """Serialized operation parameters grouped by HTTP location."""

    path: tuple[tuple[str, str], ...] = ()
    query: tuple[tuple[str, str], ...] = ()
    headers: tuple[tuple[str, str], ...] = ()
    cookies: tuple[tuple[str, str], ...] = ()

    def expand_path(self, template: str) -> str:
        """Expand path placeholders with their encoded values."""
        result = template
        for name, value in self.path:
            placeholder = "{" + name + "}"
            if placeholder not in result:
                raise ValueError(f"Path parameter {name!r} is not present in template {template!r}")
            result = result.replace(placeholder, value)
        return result

    @property
    def query_string(self) -> str:
        """Return the already percent-encoded query string."""
        return "&".join(f"{name}={value}" for name, value in self.query)


def encode_parameters(parameters: Sequence[ParameterSpec]) -> EncodedParameters:
    """Serialize parameters according to their location, style, and explode rules."""
    path: list[tuple[str, str]] = []
    query: list[tuple[str, str]] = []
    headers: list[tuple[str, str]] = []
    cookies: list[tuple[str, str]] = []

    for parameter in parameters:
        if parameter.value is None:
            continue
        if parameter.location == ParameterLocation.PATH:
            path.append((parameter.name, _encode_path(parameter)))
        elif parameter.location == ParameterLocation.QUERY:
            query.extend(_encode_query(parameter))
        elif parameter.location == ParameterLocation.HEADER:
            headers.append((parameter.name, _encode_header(parameter)))
        elif parameter.location == ParameterLocation.COOKIE:
            cookies.extend(_encode_cookie(parameter))

    return EncodedParameters(tuple(path), tuple(query), tuple(headers), tuple(cookies))


def parameter_value(value: object) -> str:
    """Convert a scalar parameter value to its canonical wire representation."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Enum):
        return parameter_value(value.value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def parameter_object(value: object) -> Mapping[str, object]:
    """Convert a generated query object to its alias-preserving wire mapping."""
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json", by_alias=True, exclude_none=True)
    if isinstance(value, Mapping):
        return value
    raise TypeError("Object parameters must be mappings or Pydantic models")


def _encode_path(parameter: ParameterSpec) -> str:
    style = parameter.effective_style
    explode = parameter.effective_explode
    value = parameter.value

    if style not in {ParameterStyle.SIMPLE, ParameterStyle.LABEL, ParameterStyle.MATRIX}:
        raise ValueError(f"Style {style} is not valid for path parameters")

    if isinstance(value, Mapping):
        items = [
            (_quote(key, parameter.allow_reserved), _quote(item, parameter.allow_reserved))
            for key, item in value.items()
        ]
        if style == ParameterStyle.SIMPLE:
            if explode:
                return ",".join(f"{key}={item}" for key, item in items)
            return ",".join(part for item in items for part in item)
        if style == ParameterStyle.LABEL:
            if explode:
                return "." + ".".join(f"{key}={item}" for key, item in items)
            return "." + ",".join(part for item in items for part in item)
        if explode:
            return "".join(f";{key}={item}" for key, item in items)
        return f";{_quote(parameter.name, False)}=" + ",".join(part for item in items for part in item)

    if _is_sequence(value):
        values = [_quote(item, parameter.allow_reserved) for item in value]
        if style == ParameterStyle.SIMPLE:
            return ",".join(values)
        if style == ParameterStyle.LABEL:
            return "." + ("." if explode else ",").join(values)
        prefix = f";{_quote(parameter.name, False)}="
        return prefix + prefix.join(values) if explode else prefix + ",".join(values)

    encoded = _quote(value, parameter.allow_reserved)
    if style == ParameterStyle.LABEL:
        return "." + encoded
    if style == ParameterStyle.MATRIX:
        return f";{_quote(parameter.name, False)}={encoded}"
    return encoded


def _encode_query(parameter: ParameterSpec) -> list[tuple[str, str]]:
    style = parameter.effective_style
    explode = parameter.effective_explode
    value = parameter.value
    name = _quote(parameter.name, parameter.allow_reserved)

    if isinstance(value, Mapping):
        items: list[tuple[str, str]] = []
        for key, item in value.items():
            if item is None:
                continue
            encoded_key = _quote(key, parameter.allow_reserved)
            if _is_sequence(item):
                items.extend((encoded_key, _quote(member, parameter.allow_reserved)) for member in item)
            else:
                items.append((encoded_key, _quote(item, parameter.allow_reserved)))
        if style == ParameterStyle.DEEP_OBJECT:
            return [(f"{name}%5B{key}%5D", item) for key, item in items]
        if style != ParameterStyle.FORM:
            raise ValueError(f"Style {style} is not valid for object query parameters")
        if explode:
            return items
        grouped: list[str] = []
        for key, item in items:
            grouped.extend((key, item))
        return [(name, ",".join(grouped))]

    if _is_sequence(value):
        values = [_quote(item, parameter.allow_reserved) for item in value]
        if style == ParameterStyle.FORM:
            return [(name, item) for item in values] if explode else [(name, ",".join(values))]
        if style == ParameterStyle.SPACE_DELIMITED:
            return [(name, "%20".join(values))]
        if style == ParameterStyle.PIPE_DELIMITED:
            return [(name, "|".join(values))]
        raise ValueError(f"Style {style} is not valid for array query parameters")

    if style not in {ParameterStyle.FORM, ParameterStyle.SPACE_DELIMITED, ParameterStyle.PIPE_DELIMITED}:
        raise ValueError(f"Style {style} is not valid for scalar query parameters")
    return [(name, _quote(value, parameter.allow_reserved))]


def _encode_header(parameter: ParameterSpec) -> str:
    if parameter.effective_style != ParameterStyle.SIMPLE:
        raise ValueError(f"Style {parameter.effective_style} is not valid for header parameters")
    value = parameter.value
    if isinstance(value, Mapping):
        items = [(parameter_value(key), parameter_value(item)) for key, item in value.items()]
        if parameter.effective_explode:
            return ",".join(f"{key}={item}" for key, item in items)
        return ",".join(part for item in items for part in item)
    if _is_sequence(value):
        return ",".join(parameter_value(item) for item in value)
    return parameter_value(value)


def _encode_cookie(parameter: ParameterSpec) -> list[tuple[str, str]]:
    if parameter.effective_style != ParameterStyle.FORM:
        raise ValueError(f"Style {parameter.effective_style} is not valid for cookie parameters")
    value = parameter.value
    if isinstance(value, Mapping):
        items = [(parameter_value(key), parameter_value(item)) for key, item in value.items()]
        if parameter.effective_explode:
            return items
        return [(parameter.name, ",".join(part for item in items for part in item))]
    if _is_sequence(value):
        values = [parameter_value(item) for item in value]
        if parameter.effective_explode:
            return [(parameter.name, item) for item in values]
        return [(parameter.name, ",".join(values))]
    return [(parameter.name, parameter_value(value))]


def _quote(value: object, allow_reserved: bool) -> str:
    safe = ":/?#[]@!$&'()*+,;=" if allow_reserved else ""
    return quote(parameter_value(value), safe=safe)


def _is_sequence(value: object) -> TypeGuard[Sequence[object]]:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray, memoryview))
