# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import math
import random
from collections.abc import Callable


class _ReconnectPolicy:
    """Tracks the retry controls and consecutive connection failures for an SSE stream."""

    def __init__(
        self,
        retry: float,
        retry_max: float | None,
        *,
        jitter: Callable[[float, float], float] | None = None,
    ) -> None:
        self.retry = retry
        self.configured_max = retry_max if retry_max is not None and retry_max > 0 else None
        self.server_max: float | None = None
        self.failed_attempts = 0
        self._jitter = jitter or random.uniform

    @property
    def effective_max(self) -> float:
        """Return the active server, configured, or retry-derived maximum delay."""
        if self.server_max is not None:
            return self.server_max
        if self.configured_max is not None:
            return self.configured_max
        return self.retry * 30

    def update_retry(self, retry: float) -> None:
        """Apply a positive server-provided base retry interval."""
        if retry > 0:
            self.retry = retry

    def update_retry_max(self, retry_max: float) -> None:
        """Apply a positive server-provided maximum retry interval."""
        if retry_max > 0:
            self.server_max = retry_max

    def opened(self) -> None:
        """Reset escalation after a connection has successfully opened."""
        self.failed_attempts = 0

    def next_delay(self, *, failed: bool) -> float:
        """Return the next capped delay and record a failed connection when applicable."""
        attempt = self.failed_attempts
        delay = self._capped_delay(attempt)
        if attempt > 0:
            delay *= self._jitter(0.9, 1.0)
        if failed:
            self.failed_attempts += 1
        return delay

    def _capped_delay(self, attempt: int) -> float:
        maximum = self.effective_max
        if self.retry <= 0 or maximum <= 0:
            return 0.0
        if self.retry >= maximum:
            return maximum
        try:
            delay = math.ldexp(self.retry, attempt)
        except OverflowError:
            return maximum
        return min(delay, maximum)
