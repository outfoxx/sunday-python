# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Transport-neutral Sunday runtime APIs.

Optional adapters must be imported from :mod:`sunday.httpx`,
:mod:`sunday.litestar`, or :mod:`sunday.cbor`.
"""

from importlib.metadata import PackageNotFoundError, version

from .codecs import (
    BinaryCodec,
    FormUrlEncodedCodec,
    JsonCodec,
    MediaTypeDecoder,
    MediaTypeDecoders,
    MediaTypeEncoder,
    MediaTypeEncoders,
    TextCodec,
)
from .errors import SundayError, UnexpectedResponse
from .headers import ResponseHeaders
from .media import MediaType
from .models import SundayModel, TolerantStrEnum
from .operations import NullableOperation, Operation, OperationResponse, StreamingOperation
from .parameters import (
    EncodedParameters,
    ParameterLocation,
    ParameterSpec,
    ParameterStyle,
    encode_parameters,
    parameter_value,
)
from .problems import Problem, ProblemPayload, ProblemRegistry
from .specs import NullifySpec, OperationSpec, RequestSpec, ResponseSpec
from .sse import EventParser, EventStreamOptions, ServerSentEvent
from .streaming import StreamingBody, StreamingBodyChunk, StreamingBodyContent
from .transport import Transport

try:
    __version__ = version("sunday-python")
except PackageNotFoundError:  # pragma: no cover - source tree without an editable install
    __version__ = "0.0.0"

__all__ = [
    "BinaryCodec",
    "EncodedParameters",
    "EventParser",
    "EventStreamOptions",
    "FormUrlEncodedCodec",
    "JsonCodec",
    "MediaType",
    "MediaTypeDecoder",
    "MediaTypeDecoders",
    "MediaTypeEncoder",
    "MediaTypeEncoders",
    "NullableOperation",
    "NullifySpec",
    "Operation",
    "OperationResponse",
    "OperationSpec",
    "ParameterLocation",
    "ParameterSpec",
    "ParameterStyle",
    "Problem",
    "ProblemPayload",
    "ProblemRegistry",
    "RequestSpec",
    "ResponseHeaders",
    "ResponseSpec",
    "ServerSentEvent",
    "StreamingBody",
    "StreamingBodyChunk",
    "StreamingBodyContent",
    "StreamingOperation",
    "SundayError",
    "SundayModel",
    "TextCodec",
    "TolerantStrEnum",
    "Transport",
    "UnexpectedResponse",
    "__version__",
    "encode_parameters",
    "parameter_value",
]
