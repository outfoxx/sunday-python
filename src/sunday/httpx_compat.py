# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Compatibility imports for clients generated before transport-neutral output."""

from .httpx._compat import (
    EventStream,
    MediaType,
    Operation,
    OperationResponse,
    ResponseHeaders,
    StreamingBody,
    StreamingOperation,
    Transport,
    TransportRequest,
    TransportResponse,
    as_transport,
    json_body,
    parameter_map,
    path_template,
)

__all__ = [
    "EventStream",
    "MediaType",
    "Operation",
    "OperationResponse",
    "ResponseHeaders",
    "StreamingBody",
    "StreamingOperation",
    "Transport",
    "TransportRequest",
    "TransportResponse",
    "as_transport",
    "json_body",
    "parameter_map",
    "path_template",
]
