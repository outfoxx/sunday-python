# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from datetime import date
from enum import StrEnum

import pytest

from sunday import (
    ParameterLocation,
    ParameterSpec,
    ParameterStyle,
    SundayModel,
    encode_parameters,
    parameter_object,
    parameter_value,
)


class State(StrEnum):
    ACTIVE = "active"


class Query(SundayModel):
    state: State


def test_scalar_parameter_values() -> None:
    assert parameter_value(True) == "true"
    assert parameter_value(False) == "false"
    assert parameter_value(State.ACTIVE) == "active"
    assert parameter_value(date(2026, 8, 18)) == "2026-08-18"
    assert parameter_value(b"value") == "value"


def test_parameter_object_accepts_models_and_mappings() -> None:
    assert parameter_object(Query(state=State.ACTIVE)) == {"state": "active"}
    assert parameter_object({"state": "active"}) == {"state": "active"}
    with pytest.raises(TypeError):
        parameter_object("state=active")


def test_encodes_all_parameter_locations_and_omits_none() -> None:
    encoded = encode_parameters(
        (
            ParameterSpec("project-id", "a/b", ParameterLocation.PATH),
            ParameterSpec("tag", ["one", "two"], ParameterLocation.QUERY),
            ParameterSpec("filter", {"state": "active", "owner": "a/b"}, ParameterLocation.QUERY),
            ParameterSpec("", {"tags": ["one", "two"]}, ParameterLocation.QUERY),
            ParameterSpec("X-Flags", ["one", "two"], ParameterLocation.HEADER),
            ParameterSpec("session", "abc", ParameterLocation.COOKIE),
            ParameterSpec("missing", None, ParameterLocation.QUERY),
        )
    )

    assert encoded.expand_path("/projects/{project-id}") == "/projects/a%2Fb"
    assert encoded.query == (
        ("tag", "one"),
        ("tag", "two"),
        ("state", "active"),
        ("owner", "a%2Fb"),
        ("tags", "one"),
        ("tags", "two"),
    )
    assert encoded.query_string == "tag=one&tag=two&state=active&owner=a%2Fb&tags=one&tags=two"
    assert encoded.headers == (("X-Flags", "one,two"),)
    assert encoded.cookies == (("session", "abc"),)


def test_object_query_values_support_nulls_sequences_and_compact_form() -> None:
    exploded = encode_parameters(
        (
            ParameterSpec(
                "filter",
                {"ignored": None, "tags": ["one", "two"], "state": "active"},
                ParameterLocation.QUERY,
            ),
        )
    )
    compact = encode_parameters(
        (
            ParameterSpec(
                "filter",
                {"tags": ["one", "two"], "state": "active"},
                ParameterLocation.QUERY,
                ParameterStyle.FORM,
                False,
            ),
        )
    )
    assert exploded.query == (("tags", "one"), ("tags", "two"), ("state", "active"))
    assert compact.query == (("filter", "tags,one,tags,two,state,active"),)


@pytest.mark.parametrize(
    ("style", "explode", "expected"),
    [
        (ParameterStyle.SIMPLE, False, "role,admin,firstName,Alex"),
        (ParameterStyle.SIMPLE, True, "role=admin,firstName=Alex"),
        (ParameterStyle.LABEL, False, ".role,admin,firstName,Alex"),
        (ParameterStyle.LABEL, True, ".role=admin.firstName=Alex"),
        (ParameterStyle.MATRIX, False, ";id=role,admin,firstName,Alex"),
        (ParameterStyle.MATRIX, True, ";role=admin;firstName=Alex"),
    ],
)
def test_path_object_styles(style: ParameterStyle, explode: bool, expected: str) -> None:
    encoded = encode_parameters(
        (ParameterSpec("id", {"role": "admin", "firstName": "Alex"}, ParameterLocation.PATH, style, explode),)
    )

    assert encoded.expand_path("/{id}") == f"/{expected}"


def test_path_array_styles() -> None:
    matrix = encode_parameters((ParameterSpec("id", [3, 4], ParameterLocation.PATH, ParameterStyle.MATRIX, True),))
    label = encode_parameters((ParameterSpec("id", [3, 4], ParameterLocation.PATH, ParameterStyle.LABEL, True),))

    assert matrix.expand_path("/{id}") == "/;id=3;id=4"
    assert label.expand_path("/{id}") == "/.3.4"


def test_query_collection_and_object_styles() -> None:
    encoded = encode_parameters(
        (
            ParameterSpec("space", ["a", "b"], ParameterLocation.QUERY, ParameterStyle.SPACE_DELIMITED),
            ParameterSpec("pipe", ["a", "b"], ParameterLocation.QUERY, ParameterStyle.PIPE_DELIMITED),
            ParameterSpec(
                "filter",
                {"state": "active", "owner": "me"},
                ParameterLocation.QUERY,
                ParameterStyle.DEEP_OBJECT,
            ),
            ParameterSpec("compact", ["a", "b"], ParameterLocation.QUERY, ParameterStyle.FORM, False),
        )
    )

    assert encoded.query == (
        ("space", "a%20b"),
        ("pipe", "a|b"),
        ("filter%5Bstate%5D", "active"),
        ("filter%5Bowner%5D", "me"),
        ("compact", "a,b"),
    )


def test_cookie_object_styles() -> None:
    exploded = encode_parameters(
        (ParameterSpec("filter", {"state": "active", "owner": "me"}, ParameterLocation.COOKIE),)
    )
    compact = encode_parameters(
        (
            ParameterSpec(
                "filter",
                {"state": "active", "owner": "me"},
                ParameterLocation.COOKIE,
                explode=False,
            ),
        )
    )

    assert exploded.cookies == (("state", "active"), ("owner", "me"))
    assert compact.cookies == (("filter", "state,active,owner,me"),)


def test_allow_reserved_and_empty_values() -> None:
    encoded = encode_parameters(
        (
            ParameterSpec("redirect", "https://example.test/a?b=c", ParameterLocation.QUERY, allow_reserved=True),
            ParameterSpec("empty", "", ParameterLocation.QUERY),
        )
    )

    assert encoded.query == (("redirect", "https://example.test/a?b=c"), ("empty", ""))


def test_invalid_styles_and_missing_path_placeholder_fail() -> None:
    with pytest.raises(ValueError):
        encode_parameters((ParameterSpec("id", "one", ParameterLocation.PATH, ParameterStyle.FORM),))
    with pytest.raises(ValueError):
        encode_parameters((ParameterSpec("id", {"a": "b"}, ParameterLocation.QUERY, ParameterStyle.SIMPLE),))
    with pytest.raises(ValueError):
        encode_parameters((ParameterSpec("id", "one", ParameterLocation.HEADER, ParameterStyle.FORM),))
    with pytest.raises(ValueError):
        encode_parameters((ParameterSpec("id", "one", ParameterLocation.COOKIE, ParameterStyle.SIMPLE),))
    with pytest.raises(ValueError):
        encode_parameters((ParameterSpec("id", "one", ParameterLocation.PATH),)).expand_path("/projects")
