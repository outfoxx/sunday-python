# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator
from typing import Any

from litestar import Request
from litestar.config.app import AppConfig
from litestar.plugins.pydantic import PydanticPlugin
from litestar.response import Response
from pydantic import BaseModel

from .problems import Problem


class SundayPlugin(PydanticPlugin):
    """Configure alias-aware Pydantic responses and RFC problem handling."""

    def __init__(self) -> None:
        super().__init__(prefer_alias=True)

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        """Apply Sunday server integration to a Litestar application."""
        app_config = super().on_app_init(app_config)
        app_config.exception_handlers[Problem] = problem_exception_handler
        return app_config


def problem_exception_handler(_request: Request[Any, Any, Any], exc: Problem) -> Response[dict[str, Any]]:
    """Render a Sunday problem as an ``application/problem+json`` response."""
    status = exc.status if exc.status is not None and 100 <= exc.status <= 599 else 500
    return Response(
        content=exc.model_dump(mode="json", by_alias=True, exclude_none=True),
        status_code=status,
        media_type="application/problem+json",
    )


async def server_sent_events(events: AsyncIterable[BaseModel | str | bytes]) -> AsyncIterator[str]:
    """Serialize generated event models for Litestar's ``ServerSentEvent`` response."""
    async for event in events:
        if isinstance(event, BaseModel):
            yield event.model_dump_json(by_alias=True, exclude_none=True)
        elif isinstance(event, bytes):
            yield event.decode()
        else:
            yield event
