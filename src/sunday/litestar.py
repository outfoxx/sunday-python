# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import re
from collections.abc import AsyncIterable, AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from enum import Enum
from typing import Any

from litestar import Request
from litestar.config.app import AppConfig
from litestar.exceptions import HTTPException, ValidationException
from litestar.plugins.pydantic import PydanticDIPlugin, PydanticInitPlugin, PydanticPlugin, PydanticSchemaPlugin
from litestar.response import Response
from pydantic import BaseModel, TypeAdapter, ValidationError

from .media import MediaType
from .models import SundayModel
from .problems import Problem
from .unknown_model import UnknownModel


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
        app_config.plugins.extend(
            [
                _SundayPydanticInitPlugin(prefer_alias=True),
                PydanticSchemaPlugin(prefer_alias=True),
                PydanticDIPlugin(),
            ]
        )
        app_config.exception_handlers[Problem] = problem_exception_handler
        return app_config


class _SundayPydanticInitPlugin(PydanticInitPlugin):
    @classmethod
    def decoders(cls, validate_strict: bool = False) -> list[tuple[Callable[[Any], bool], Callable[[Any, Any], Any]]]:
        def is_sunday_model(model_type: Any) -> bool:
            return isinstance(model_type, type) and issubclass(model_type, (SundayModel, UnknownModel))

        return [(is_sunday_model, _decode_request_model), *super().decoders(validate_strict)]

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        app_config = super().on_app_init(app_config)
        assert app_config.type_encoders is not None
        encode = app_config.type_encoders[BaseModel]

        def encode_response(value: BaseModel) -> Any:
            type(value).model_validate(value, strict=True, context={"mode": "response"})
            return encode(value)

        app_config.type_encoders = {
            **app_config.type_encoders,
            SundayModel: encode_response,
            UnknownModel: encode_response,
        }
        return app_config


def _decode_request_model(model_type: type[BaseModel], value: Any) -> BaseModel:
    try:
        return model_type.model_validate(value, context={"mode": "request"})
    except ValidationError as error:
        raise ValidationException(
            detail="Request entity is invalid", extra=error.errors(include_input=False, include_context=False)
        ) from error


def problem_exception_handler(_request: Request[Any, Any, Any], exc: Problem) -> Response[dict[str, Any]]:
    """Render a Sunday problem as an ``application/problem+json`` response."""
    status = exc.status if exc.status is not None and 100 <= exc.status <= 599 else 500
    return Response(
        content=exc.model_dump(mode="json", by_alias=True),
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
    try:
        return TypeAdapter(model_type).validate_python(values, context={"mode": "request"})
    except ValidationError as error:
        raise ValidationException(
            detail="Request entity is invalid", extra=error.errors(include_input=False, include_context=False)
        ) from error


async def request_bytes(request: Request[Any, Any, Any], media_types: Sequence[str]) -> bytes:
    """Read a binary body without JSON decoding, enforcing the declared media ranges.

    An absent Content-Type is treated as application/octet-stream. An empty list
    of ranges leaves the body unconstrained. Parameters do not affect matching;
    explicit header arguments retain their original value for application use.
    """
    try:
        actual = MediaType(request.headers.get("content-type", "application/octet-stream"))
        if any(re.fullmatch(r"[!#$%&'+.\^_`|~0-9A-Za-z-]+", token) is None for token in (actual.type, actual.subtype)):
            raise ValueError("Content-Type must be a concrete media type")
    except ValueError as error:
        raise HTTPException(status_code=400, detail="Invalid Content-Type header") from error
    if media_types and not any(MediaType(media_type).matches(actual) for media_type in media_types):
        raise HTTPException(status_code=415, detail="Unsupported request media type")
    return await request.body()


async def request_model[ModelT](
    model_type: type[ModelT] | TypeAdapter[ModelT],
    request: Request[Any, Any, Any],
    media_type: str,
) -> ModelT:
    """Decode JSON, XML, or YAML in request mode, including a union's native TypeAdapter."""
    body = await request.body()
    adapter = model_type if isinstance(model_type, TypeAdapter) else TypeAdapter(model_type)
    parsed_media_type = MediaType(media_type)
    if parsed_media_type.is_json:
        try:
            return adapter.validate_json(body, context={"mode": "request"})
        except ValidationError as error:
            raise ValidationException(
                detail="Request entity is invalid", extra=error.errors(include_input=False, include_context=False)
            ) from error
    if parsed_media_type.suffix == "xml" or (
        parsed_media_type.type in {"application", "text"} and parsed_media_type.subtype == "xml"
    ):
        from .xml import XmlCodec

        value = XmlCodec().decode(body, parsed_media_type)
    elif parsed_media_type.suffix == "yaml" or (
        parsed_media_type.type in {"application", "text"} and parsed_media_type.subtype == "yaml"
    ):
        from .yaml import YamlCodec

        value = YamlCodec().decode(body, parsed_media_type)
    else:
        raise ValueError(f"Sunday request_model does not support {media_type}")
    try:
        return adapter.validate_python(value, context={"mode": "request"})
    except ValidationError as error:
        raise ValidationException(
            detail="Request entity is invalid", extra=error.errors(include_input=False, include_context=False)
        ) from error


async def server_sent_events(events: AsyncIterable[BaseModel | str | bytes]) -> AsyncIterator[str]:
    """Serialize generated event models for Litestar's ``ServerSentEvent`` response."""
    async for event in events:
        if isinstance(event, BaseModel):
            yield event.model_dump_json(by_alias=True)
        elif isinstance(event, bytes):
            yield event.decode()
        else:
            yield event
