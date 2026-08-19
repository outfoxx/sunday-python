# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Verify the Sunday wheel's core and optional HTTPX package metadata."""

from __future__ import annotations

import sys
from email.parser import BytesParser
from pathlib import Path
from zipfile import ZipFile

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name


def main() -> None:
    wheel = Path(sys.argv[1])
    with ZipFile(wheel) as archive:
        names = set(archive.namelist())
        metadata_path = next(name for name in names if name.endswith(".dist-info/METADATA"))
        metadata = BytesParser().parsebytes(archive.read(metadata_path))

    expected_files = {
        "sunday/httpx/__init__.py",
        "sunday/httpx/_transport.py",
        "sunday/httpx/_sse.py",
        "sunday/httpx_compat.py",
        "sunday/httpx_sse.py",
    }
    assert expected_files <= names, expected_files - names

    requirements = [Requirement(value) for value in metadata.get_all("Requires-Dist", [])]
    for dependency in ("anyio", "httpx"):
        matching = [item for item in requirements if canonicalize_name(item.name) == dependency]
        assert matching, f"Missing {dependency} wheel metadata"
        assert all(item.marker is not None for item in matching)
        assert all(not item.marker.evaluate({"extra": ""}) for item in matching if item.marker is not None)
        assert any(item.marker.evaluate({"extra": "httpx"}) for item in matching if item.marker is not None)


if __name__ == "__main__":
    main()
