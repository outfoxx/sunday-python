"""Specification-linked fixtures shared by case ID with the companion runtimes."""

import json
from pathlib import Path
from typing import Any

import pytest

from sunday import TokenProviderError
from sunday.httpx._oauth_wire import DiscoveryMetadata, TokenErrorResponse, TokenSuccessResponse

CORPUS = json.loads((Path(__file__).parents[1] / "test-fixtures/oauth/cases.json").read_text())


@pytest.mark.parametrize("case", CORPUS["cases"], ids=lambda case: case["id"])
def test_wire_case(case: dict[str, Any]) -> None:
    assert CORPUS["formatVersion"] == 1

    def parse() -> object:
        if case["kind"] == "discovery":
            return DiscoveryMetadata.parse(case["body"])
        if case["kind"] == "error":
            return TokenErrorResponse.parse(case["body"])
        return TokenSuccessResponse.parse(case["body"]).tokens(
            set(case["context"]["scopes"]), case["context"]["clockMillis"] / 1000
        )

    if case["expected"] == "accept":
        parse()
    else:
        with pytest.raises((ValueError, TypeError, TokenProviderError)):
            parse()
