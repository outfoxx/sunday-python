# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import ast
import subprocess
import sys
import tomllib
from pathlib import Path

import sunday


def test_core_import_does_not_load_httpx_dependencies() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sunday, sys; assert 'httpx' not in sys.modules; assert 'anyio' not in sys.modules",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_httpx_import_explains_the_missing_extra() -> None:
    code = """
import importlib.abc
import sys

class BlockHttpx(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "httpx" or fullname.startswith("httpx."):
            raise ModuleNotFoundError("No module named 'httpx'", name="httpx")
        return None

sys.meta_path.insert(0, BlockHttpx())
import sunday
try:
    import sunday.httpx
except ImportError as error:
    assert str(error) == "sunday.httpx requires the 'httpx' extra; install 'sunday-python[httpx]'"
else:
    raise AssertionError("sunday.httpx imported without HTTPX")
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_httpx_import_preserves_unrelated_module_failures() -> None:
    code = """
import importlib.abc
import sys

class BlockTransport(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "sunday.httpx._transport":
            raise ModuleNotFoundError("unexpected dependency failure", name="unexpected_dependency")
        return None

sys.meta_path.insert(0, BlockTransport())
import sunday
try:
    import sunday.httpx
except ModuleNotFoundError as error:
    assert error.name == "unexpected_dependency"
else:
    raise AssertionError("unrelated import failure was hidden")
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_core_source_does_not_import_httpx_adapter() -> None:
    source_root = Path(sunday.__file__).parent
    violations: list[str] = []

    for source in source_root.glob("*.py"):
        tree = ast.parse(source.read_text(), filename=str(source))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported = {alias.name for alias in node.names}
                if imported & {"anyio", "httpx"}:
                    violations.append(f"{source.name}:{node.lineno}")
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module in {"anyio", "httpx"} or (node.level > 0 and module == "httpx"):
                    violations.append(f"{source.name}:{node.lineno}")

    assert violations == []


def test_httpx_dependencies_are_optional_metadata() -> None:
    project = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())["project"]

    assert all(not dependency.startswith(("anyio", "httpx")) for dependency in project["dependencies"])
    assert project["optional-dependencies"]["httpx"] == ["anyio>=4,<5", "httpx>=0.28,<1"]
