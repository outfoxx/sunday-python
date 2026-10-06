# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Application hook for the manager of resolved providers.

Factories must not acquire tokens or perform storage I/O. They may supply the native
manager's store, expiry_skew and now options. Called once for secured settings and
never for settings without selected providers. The application owns the manager,
store and session lifecycle on its asyncio event loop.
"""

from collections.abc import Callable, Mapping

from .token_manager import TokenManager
from .token_provider import TokenProvider

type TokenManagerFactory = Callable[[Mapping[str, TokenProvider]], TokenManager]
