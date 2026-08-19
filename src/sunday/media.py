# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True, init=False)
class MediaType:
    """A parsed media type with case-insensitive matching and wildcard support."""

    type: str
    subtype: str
    parameters: tuple[tuple[str, str], ...]

    def __init__(self, value: str) -> None:
        media_range, *parameter_parts = _split_quoted(value, ";")
        type_name, separator, subtype = media_range.strip().partition("/")
        if separator == "" or not type_name.strip() or not subtype.strip():
            raise ValueError(f"Invalid media type: {value!r}")

        parameters: list[tuple[str, str]] = []
        for part in parameter_parts:
            name, equals, parameter_value = part.strip().partition("=")
            if not equals or not name:
                raise ValueError(f"Invalid media type parameter: {part!r}")
            parameters.append((name.lower(), _unquote(parameter_value.strip())))

        object.__setattr__(self, "type", type_name.strip().lower())
        object.__setattr__(self, "subtype", subtype.strip().lower())
        object.__setattr__(self, "parameters", tuple(parameters))

    @property
    def suffix(self) -> str | None:
        """Return the structured syntax suffix, when present."""
        _, separator, suffix = self.subtype.rpartition("+")
        return suffix if separator else None

    def parameter(self, name: str) -> str | None:
        """Return a parameter value using case-insensitive lookup."""
        lower_name = name.lower()
        return next((value for key, value in self.parameters if key == lower_name), None)

    def matches(self, actual: MediaType) -> bool:
        """Return whether this media range accepts ``actual``."""
        if self.type not in {"*", actual.type}:
            return False
        if self.subtype == "*" or self.subtype == actual.subtype:
            return True
        if self.subtype.startswith("*+"):
            return actual.suffix == self.subtype[2:]
        return False

    @property
    def is_json(self) -> bool:
        """Return whether the media type carries JSON data."""
        return self.subtype == "json" or self.suffix == "json"

    def __str__(self) -> str:
        parameters = "".join(f"; {name}={_quote_parameter(value)}" for name, value in self.parameters)
        return f"{self.type}/{self.subtype}{parameters}"


def _split_quoted(value: str, separator: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    quoted = False
    escaped = False
    for character in value:
        if escaped:
            current.append(character)
            escaped = False
        elif character == "\\" and quoted:
            current.append(character)
            escaped = True
        elif character == '"':
            current.append(character)
            quoted = not quoted
        elif character == separator and not quoted:
            parts.append("".join(current))
            current = []
        else:
            current.append(character)
    if quoted:
        raise ValueError(f"Unterminated quoted value: {value!r}")
    parts.append("".join(current))
    return parts


def _unquote(value: str) -> str:
    if not (value.startswith('"') and value.endswith('"')):
        return value
    result: list[str] = []
    escaped = False
    for character in value[1:-1]:
        if escaped:
            result.append(character)
            escaped = False
        elif character == "\\":
            escaped = True
        else:
            result.append(character)
    if escaped:
        result.append("\\")
    return "".join(result)


def _quote_parameter(value: str) -> str:
    if all(character.isalnum() or character in "!#$%&'*+-.^_`|~" for character in value):
        return value
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
