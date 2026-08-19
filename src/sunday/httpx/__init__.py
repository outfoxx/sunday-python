# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""HTTPX transport adapter for Sunday.

Install ``sunday-python[httpx]`` before importing this module.
"""

try:
    from ._transport import (
        HttpxEventStream,
        HttpxRequestAdapter,
        HttpxRequestAdapterCallable,
        HttpxTransport,
        as_httpx_transport,
    )
except ModuleNotFoundError as error:
    if error.name not in {"anyio", "httpx"}:
        raise
    raise ImportError("sunday.httpx requires the 'httpx' extra; install 'sunday-python[httpx]'") from error

__all__ = [
    "HttpxEventStream",
    "HttpxRequestAdapter",
    "HttpxRequestAdapterCallable",
    "HttpxTransport",
    "as_httpx_transport",
]
