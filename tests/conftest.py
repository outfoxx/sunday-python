# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

import pytest


@pytest.fixture
def anyio_backend() -> str:
    """Run adapter tests on the runtime's supported asyncio backend."""
    return "asyncio"
