# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import AsyncIterable
from datetime import date
from enum import StrEnum
from typing import Any

import pytest
from litestar import Litestar, Request, get, post
from litestar.testing import TestClient
from pydantic import Field

from sunday import Problem, ProblemPayload, SundayModel
from sunday.litestar import ServerResponse, SundayPlugin, query_model, request_model, server_sent_events


class Project(SundayModel):
    project_id: str = Field(alias="project-id")


class ProjectQuery(SundayModel):
    tags: list[str]


class Configuration(SundayModel):
    name: str


class ConfigurationEnvelope(SundayModel):
    configuration: Configuration = Field(alias="Configuration")


class RevisionState(StrEnum):
    ACTIVE = "active"


@get("/project")
async def project() -> Project:
    return Project(project_id="project-1")


@get("/problem")
async def problem() -> None:
    raise Problem(ProblemPayload(title="Missing", status=404, detail="Project was not found"))


def test_sunday_plugin_configures_aliases_and_problem_handler() -> None:
    with TestClient(Litestar(route_handlers=[project, problem], plugins=[SundayPlugin()])) as client:
        assert client.get("/project").json() == {"project-id": "project-1"}
        response = client.get("/problem")

    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.json() == {
        "type": "about:blank",
        "title": "Missing",
        "status": 404,
        "detail": "Project was not found",
    }


async def generated_events() -> AsyncIterable[Project | bytes | str]:
    yield Project(project_id="project-1")
    yield b"raw"
    yield "text"


def test_server_sent_events_serializes_generated_models() -> None:
    import asyncio

    async def collect() -> list[str]:
        return [event async for event in server_sent_events(generated_events())]

    assert asyncio.run(collect()) == ['{"project-id":"project-1"}', "raw", "text"]


def test_server_response_and_query_model_preserve_wire_metadata() -> None:
    @get("/projects")
    async def projects(request: Request[Any, Any, Any]) -> object:
        query = query_model(ProjectQuery, request)
        return ServerResponse(
            Project(project_id=query.tags[0]),
            {"X-Tags": query.tags, "X-State": RevisionState.ACTIVE, "X-Date": date(2026, 8, 18)},
            status=202,
        ).to_response()

    with TestClient(Litestar(route_handlers=[projects], plugins=[SundayPlugin()])) as client:
        response = client.get("/projects?tags=one&tags=two&tags=three")

    assert response.status_code == 202
    assert response.headers["X-Tags"] == "one, two, three"
    assert response.headers["X-State"] == "active"
    assert response.headers["X-Date"] == "2026-08-18"
    assert response.json() == {"project-id": "one"}


def test_optional_yaml_and_xml_request_response_models() -> None:
    @post("/yaml")
    async def yaml_route(request: Request[Any, Any, Any]) -> object:
        configuration = await request_model(Configuration, request, "application/yaml")
        return ServerResponse(configuration, {}, media_type="application/yaml").to_response()

    @post("/xml")
    async def xml_route(request: Request[Any, Any, Any]) -> object:
        envelope = await request_model(ConfigurationEnvelope, request, "application/xml")
        return ServerResponse(envelope, {}, media_type="application/xml").to_response()

    @post("/unsupported")
    async def unsupported_route(request: Request[Any, Any, Any]) -> None:
        with pytest.raises(ValueError, match="does not support"):
            await request_model(Configuration, request, "text/csv")

    with TestClient(
        Litestar(route_handlers=[yaml_route, xml_route, unsupported_route], plugins=[SundayPlugin()])
    ) as client:
        yaml_response = client.post("/yaml", content="name: Roadmap", headers={"content-type": "application/yaml"})
        xml_response = client.post(
            "/xml",
            content="<Configuration><name>Roadmap</name></Configuration>",
            headers={"content-type": "application/xml"},
        )
        assert client.post("/unsupported", content="name,Roadmap").status_code == 201

    assert yaml_response.headers["content-type"].startswith("application/yaml")
    assert yaml_response.text == "name: Roadmap\n"
    assert xml_response.headers["content-type"].startswith("application/xml")
    assert "<Configuration><name>Roadmap</name></Configuration>" in xml_response.text


def test_server_sent_events_preserve_explicit_nulls() -> None:
    import asyncio

    class NullableEvent(SundayModel):
        required_nullable: str | None = Field(alias="requiredNullable")
        optional_text: str | None = Field(default=None, exclude_if=lambda value: value is None)

    async def events() -> AsyncIterable[NullableEvent]:
        yield NullableEvent(required_nullable=None)

    async def collect() -> list[str]:
        return [value async for value in server_sent_events(events())]

    assert asyncio.run(collect()) == ['{"requiredNullable":null}']


def test_problem_response_preserves_nullable_extension_fields() -> None:
    class NullableProblemPayload(ProblemPayload):
        required_nullable: str | None = Field(alias="requiredNullable")

    @get("/nullable-problem")
    async def nullable_problem() -> None:
        raise Problem(NullableProblemPayload(status=400, required_nullable=None))

    with TestClient(Litestar(route_handlers=[nullable_problem], plugins=[SundayPlugin()])) as client:
        response = client.get("/nullable-problem")
    assert response.status_code == 400
    assert response.json() == {"type": "about:blank", "status": 400, "requiredNullable": None}
