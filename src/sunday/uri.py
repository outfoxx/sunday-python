# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from urllib.parse import urljoin

import uritemplate


@dataclass(frozen=True, slots=True)
class URITemplate:
    """An RFC 6570 URI template with reusable default parameters."""

    template: str
    parameters: Mapping[str, object | None] = field(default_factory=dict)

    def expand(self, parameters: Mapping[str, object | None] | None = None, /, **values: object | None) -> str:
        """Expand the template after applying per-call parameter overrides."""
        merged = dict(self.parameters)
        if parameters is not None:
            merged.update(parameters)
        merged.update(values)
        expanded = {name: _template_value(value) for name, value in merged.items() if value is not None}
        return uritemplate.expand(self.template, expanded)

    def resolve(self, relative: str) -> URITemplate:
        """Return a template formed by resolving ``relative`` against this template."""
        return URITemplate(urljoin(self.template.rstrip("/") + "/", relative.lstrip("/")), self.parameters)

    def __str__(self) -> str:
        return self.expand()


def _template_value(value: object) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _template_value(item) for key, item in value.items() if item is not None}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_template_value(item) for item in value if item is not None]
    return value
