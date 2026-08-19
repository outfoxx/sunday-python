# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum

from .codecs import WireMode, _json_value

_UNSET = object()


@dataclass(frozen=True, slots=True)
class MergePatch:
    """A JSON Merge Patch document preserving explicit ``None`` values."""

    value: object

    def __sunday_wire__(self) -> object:
        """Return the JSON-compatible merge-patch representation."""
        return _json_value(self.value, WireMode.PATCH)


class PatchOperationKind(StrEnum):
    """Operations defined by RFC 6902 JSON Patch."""

    ADD = "add"
    REMOVE = "remove"
    REPLACE = "replace"
    MOVE = "move"
    COPY = "copy"
    TEST = "test"


@dataclass(frozen=True, slots=True)
class PatchOperation:
    """One RFC 6902 JSON Patch operation."""

    op: PatchOperationKind
    path: str
    value: object = _UNSET
    from_path: str | None = None

    def __post_init__(self) -> None:
        if not self.path.startswith("/") and self.path != "":
            raise ValueError("JSON Patch paths must be empty or start with '/'")
        if self.op in {PatchOperationKind.MOVE, PatchOperationKind.COPY} and self.from_path is None:
            raise ValueError(f"JSON Patch {self.op} operations require from_path")
        if (
            self.op in {PatchOperationKind.ADD, PatchOperationKind.REPLACE, PatchOperationKind.TEST}
            and self.value is _UNSET
        ):
            raise ValueError(f"JSON Patch {self.op} operations require value")

    def __sunday_wire__(self) -> Mapping[str, object]:
        """Return the JSON-compatible operation representation."""
        result: dict[str, object] = {"op": self.op.value, "path": self.path}
        if self.from_path is not None:
            result["from"] = self.from_path
        if self.value is not _UNSET:
            result["value"] = _json_value(self.value, WireMode.PATCH)
        return result


@dataclass(frozen=True, slots=True, init=False)
class PatchDocument:
    """An ordered reusable RFC 6902 JSON Patch document."""

    operations: tuple[PatchOperation, ...]

    def __init__(self, operations: Sequence[PatchOperation]) -> None:
        object.__setattr__(self, "operations", tuple(operations))

    def __sunday_wire__(self) -> list[Mapping[str, object]]:
        """Return the JSON-compatible patch representation."""
        return [operation.__sunday_wire__() for operation in self.operations]
