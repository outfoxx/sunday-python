# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from enum import Enum
from typing import Any

from litestar import Request
from litestar.config.app import AppConfig
from litestar.plugins.pydantic import PydanticPlugin
from litestar.response import Response
from pydantic import BaseModel, TypeAdapter

from .media import MediaType
from .problems import Problem


@dataclass(frozen=True, slots=True)
class ServerResponse[BodyT, HeadersT: Mapping[str, object]]:
    """A generated server result with an explicitly selected status and typed headers."""

    body: BodyT
    headers: HeadersT
    status: int | None = None
    media_type: str | None = None

    def to_response(self, *, default_status: int = 200, default_media_type: str | None = None) -> Response[BodyT]:
        """Convert this transport-neutral result into a Litestar response."""
        media_type = self.media_type or default_media_type
        content: Any = self.body
        if media_type is not None and not isinstance(content, (str, bytes, bytearray, memoryview)):
            if media_type.endswith("+xml") or media_type in {"application/xml", "text/xml"}:
                from .xml import XmlCodec

                content = XmlCodec().encode(content)
            elif media_type.endswith("+yaml") or media_type in {"application/yaml", "text/yaml"}:
                from .yaml import YamlCodec

                content = YamlCodec().encode(content)
        return Response(
            content=content,
            status_code=self.status or default_status,
            headers={name: _header_value(value) for name, value in self.headers.items()},
            media_type=media_type,
        )


def _header_value(value: object) -> str:
    if isinstance(value, Enum):
        return _header_value(value.value)
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray, memoryview)):
        return ", ".join(_header_value(item) for item in value)
    return str(value)


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


def query_model[ModelT](model_type: type[ModelT], request: Request[Any, Any, Any]) -> ModelT:
    """Decode a generated query-string model while preserving repeated values."""
    values: dict[str, object] = {}
    for name, value in request.query_params.multi_items():
        previous = values.get(name)
        if previous is None:
            values[name] = value
        elif isinstance(previous, list):
            previous.append(value)
        else:
            values[name] = [previous, value]
    return TypeAdapter(model_type).validate_python(values)


async def request_model[ModelT](
    model_type: type[ModelT],
    request: Request[Any, Any, Any],
    media_type: str,
) -> ModelT:
    """Decode an optional XML or YAML request body into a generated model."""
    body = await request.body()
    if media_type.endswith("+xml") or media_type in {"application/xml", "text/xml"}:
        from .xml import XmlCodec

        value = XmlCodec().decode(body, MediaType(media_type))
    elif media_type.endswith("+yaml") or media_type in {"application/yaml", "text/yaml"}:
        from .yaml import YamlCodec

        value = YamlCodec().decode(body, MediaType(media_type))
    else:
        raise ValueError(f"Sunday request_model does not support {media_type}")
    return TypeAdapter(model_type).validate_python(value)


async def server_sent_events(events: AsyncIterable[BaseModel | str | bytes]) -> AsyncIterator[str]:
    """Serialize generated event models for Litestar's ``ServerSentEvent`` response."""
    async for event in events:
        if isinstance(event, BaseModel):
            yield event.model_dump_json(by_alias=True, exclude_none=True)
        elif isinstance(event, bytes):
            yield event.decode()
        else:
            yield event
