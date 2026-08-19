# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from .media import MediaType


@dataclass(frozen=True, slots=True)
class ResponseHeaders:
    """HTTP response headers preserving repeated values and original order."""

    entries: tuple[tuple[str, str], ...]

    @classmethod
    def from_items(cls, items: Iterable[tuple[str, str]]) -> ResponseHeaders:
        """Create response headers from transport header items."""
        return cls(tuple(items))

    def get_all(self, name: str) -> tuple[str, ...]:
        """Return every value matching ``name`` case-insensitively."""
        lower_name = name.lower()
        return tuple(value for header_name, value in self.entries if header_name.lower() == lower_name)

    def get(self, name: str) -> str | None:
        """Return the first value matching ``name`` case-insensitively."""
        return next(iter(self.get_all(name)), None)

    @property
    def content_type(self) -> MediaType | None:
        """Return the parsed Content-Type header when valid and present."""
        value = self.get("content-type")
        if value is None:
            return None
        try:
            return MediaType(value)
        except ValueError:
            return None
