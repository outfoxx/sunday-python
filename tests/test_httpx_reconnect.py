# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from sunday.httpx._reconnect import _ReconnectPolicy


def test_reconnect_policy_uses_exponential_delays_and_resets_after_open() -> None:
    policy = _ReconnectPolicy(0.5, None, jitter=lambda _minimum, _maximum: 1.0)

    delays = [policy.next_delay(failed=True) for _ in range(7)]

    assert delays == [0.5, 1.0, 2.0, 4.0, 8.0, 15.0, 15.0]
    policy.opened()
    assert policy.next_delay(failed=False) == 0.5


def test_reconnect_policy_jitters_only_escalated_retries() -> None:
    policy = _ReconnectPolicy(0.5, None, jitter=lambda minimum, maximum: minimum)

    assert policy.next_delay(failed=True) == 0.5
    assert policy.next_delay(failed=True) == 0.9


def test_reconnect_policy_applies_dynamic_configured_and_server_caps() -> None:
    dynamic = _ReconnectPolicy(0.5, None)
    assert dynamic.effective_max == 15.0
    dynamic.update_retry(1.0)
    assert dynamic.effective_max == 30.0

    configured = _ReconnectPolicy(0.5, 10.0)
    configured.update_retry(1.0)
    assert configured.effective_max == 10.0
    configured.update_retry_max(2.0)
    configured.update_retry_max(0.0)
    assert configured.effective_max == 2.0


def test_reconnect_policy_saturates_extreme_attempt_counts() -> None:
    policy = _ReconnectPolicy(0.5, None, jitter=lambda _minimum, _maximum: 1.0)
    policy.failed_attempts = 1_000_000

    assert policy.next_delay(failed=True) == 15.0
