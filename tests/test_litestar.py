# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import AsyncIterable

from litestar import Litestar, get
from litestar.testing import TestClient
from pydantic import Field

from sunday import Problem, ProblemPayload, SundayModel
from sunday.litestar import SundayPlugin, server_sent_events


class Project(SundayModel):
    project_id: str = Field(alias="project-id")


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
