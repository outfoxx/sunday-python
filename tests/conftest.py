# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from collections.abc import Iterator

import pytest

from oauth_support.provider import Provider


@pytest.fixture
def anyio_backend() -> str:
    """Run adapter tests on the runtime's supported asyncio backend."""
    return "asyncio"


@pytest.fixture(scope="session")
def oauth_provider(pytestconfig: pytest.Config) -> Iterator[Provider]:
    """Own provider provisioning and teardown for the interoperability suite."""
    import os
    import subprocess
    import sys
    from pathlib import Path

    from playwright.sync_api import sync_playwright

    mode = os.environ.get("SUNDAY_OAUTH_TEST_MODE", "replay")
    provider = Provider(mode, Path(str(pytestconfig.cache.mkdir("oauth-artifacts"))))
    try:
        if mode == "live":
            with sync_playwright() as browser:
                installed = Path(browser.chromium.executable_path).exists()
            if not installed:
                subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True, timeout=240)
        yield provider.start()
    finally:
        provider.close()
