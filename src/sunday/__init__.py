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
    WireMode,
)
from .errors import (
    RequestEncodingError,
    ResponseDecodingError,
    ResponseError,
    ResponseValidationError,
    SundayError,
    TransportError,
    UnexpectedResponse,
)
from .event_source import (
    EventSource,
    EventSourceErrorHandler,
    EventSourceMessageHandler,
    EventSourceOpenHandler,
    EventSourceState,
)
from .headers import ResponseHeaders
from .media import MediaType
from .model_intersection import ModelIntersection
from .model_mode import ModelMode
from .models import SundayModel, TolerantStrEnum
from .multipart import MultipartBody, MultipartContent, MultipartPart
from .observers import LoggingTransportObserver, TransportEvent, TransportEventKind, TransportObserver
from .operations import NullableOperation, Operation, OperationResponse, StreamingOperation
from .parameters import (
    EncodedParameters,
    ParameterLocation,
    ParameterSpec,
    ParameterStyle,
    encode_parameters,
    parameter_object,
    parameter_value,
)
from .patch import MergePatch, PatchDocument, PatchOperation, PatchOperationKind
from .patch_model import SundayPatchModel
from .problems import Problem, ProblemPayload, ProblemRegistry
from .security import SecurityBinding, SecurityEndpoints, SecurityTransport
from .specs import NullifySpec, OperationSpec, RequestPayloadSpec, RequestSpec, ResponseHeaderSpec, ResponseSpec
from .sse import EventParser, EventStreamOptions, ServerSentEvent
from .streaming import StreamingBody, StreamingBodyChunk, StreamingBodyClose, StreamingBodyContent
from .token_manager import TokenLease, TokenManager
from .token_provider import (
    AuthorizationRequiredError,
    RefreshingTokenProvider,
    TokenConfiguration,
    TokenProvider,
    TokenProviderError,
    TokenRequest,
    TokenSet,
    TokenStore,
)
from .transport import BaseTransport, EventStream, ProblemRegistrar, Transport
from .unknown_model import UnknownModel
from .unset import UNSET, UnsetType, is_unset
from .uri import URITemplate

try:
    __version__ = version("sunday-python")
except PackageNotFoundError:  # pragma: no cover - source tree without an editable install
    __version__ = "0.0.0"

__all__ = [
    "UNSET",
    "AuthorizationRequiredError",
    "BaseTransport",
    "BinaryCodec",
    "EncodedParameters",
    "EventParser",
    "EventSource",
    "EventSourceErrorHandler",
    "EventSourceMessageHandler",
    "EventSourceOpenHandler",
    "EventSourceState",
    "EventStream",
    "EventStreamOptions",
    "FormUrlEncodedCodec",
    "JsonCodec",
    "LoggingTransportObserver",
    "MediaType",
    "MediaTypeDecoder",
    "MediaTypeDecoders",
    "MediaTypeEncoder",
    "MediaTypeEncoders",
    "MergePatch",
    "ModelIntersection",
    "ModelMode",
    "MultipartBody",
    "MultipartContent",
    "MultipartPart",
    "NullableOperation",
    "NullifySpec",
    "Operation",
    "OperationResponse",
    "OperationSpec",
    "ParameterLocation",
    "ParameterSpec",
    "ParameterStyle",
    "PatchDocument",
    "PatchOperation",
    "PatchOperationKind",
    "Problem",
    "ProblemPayload",
    "ProblemRegistrar",
    "ProblemRegistry",
    "RefreshingTokenProvider",
    "RequestEncodingError",
    "RequestPayloadSpec",
    "RequestSpec",
    "ResponseDecodingError",
    "ResponseError",
    "ResponseHeaderSpec",
    "ResponseHeaders",
    "ResponseSpec",
    "ResponseValidationError",
    "SecurityBinding",
    "SecurityEndpoints",
    "SecurityTransport",
    "ServerSentEvent",
    "StreamingBody",
    "StreamingBodyChunk",
    "StreamingBodyClose",
    "StreamingBodyContent",
    "StreamingOperation",
    "SundayError",
    "SundayModel",
    "SundayPatchModel",
    "TextCodec",
    "TokenConfiguration",
    "TokenLease",
    "TokenManager",
    "TokenProvider",
    "TokenProviderError",
    "TokenRequest",
    "TokenSet",
    "TokenStore",
    "TolerantStrEnum",
    "Transport",
    "TransportError",
    "TransportEvent",
    "TransportEventKind",
    "TransportObserver",
    "URITemplate",
    "UnexpectedResponse",
    "UnknownModel",
    "UnsetType",
    "WireMode",
    "__version__",
    "encode_parameters",
    "is_unset",
    "parameter_object",
    "parameter_value",
]
