# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol


class TransportEventKind(StrEnum):
    """Observable transport lifecycle transitions."""

    REQUEST = "request"
    RESPONSE = "response"
    FAILURE = "failure"


@dataclass(frozen=True, slots=True)
class TransportEvent:
    """A redacted transport lifecycle event."""

    kind: TransportEventKind
    method: str
    url: str
    headers: tuple[tuple[str, str], ...]
    status: int | None = None
    elapsed: float | None = None
    error: BaseException | None = None


class TransportObserver(Protocol):
    """Observes redacted transport lifecycle events."""

    def observe(self, event: TransportEvent) -> None:
        """Receive one transport event."""
        ...


@dataclass(frozen=True, slots=True)
class LoggingTransportObserver:
    """Writes transport lifecycle events through the standard logging package."""

    logger: logging.Logger
    level: int = logging.DEBUG

    def observe(self, event: TransportEvent) -> None:
        """Log one transport event without including response bodies."""
        self.logger.log(
            self.level,
            "sunday.http %s method=%s url=%s status=%s elapsed=%s headers=%s error=%s",
            event.kind,
            event.method,
            event.url,
            event.status,
            event.elapsed,
            dict(event.headers),
            event.error,
        )


def redact_headers(
    headers: Iterable[tuple[str, str]],
    sensitive_headers: Iterable[str] = (),
) -> tuple[tuple[str, str], ...]:
    """Return header items with credentials and configured values redacted."""
    sensitive = {"authorization", "proxy-authorization", "cookie", "set-cookie"}
    sensitive.update(name.lower() for name in sensitive_headers)
    return tuple((name, "<redacted>" if name.lower() in sensitive else value) for name, value in headers)
