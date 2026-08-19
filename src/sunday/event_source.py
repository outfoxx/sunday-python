# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from .sse import ServerSentEvent


class EventSourceState(StrEnum):
    """Connection state of a callback-oriented server-sent event source."""

    CONNECTING = "connecting"
    OPEN = "open"
    CLOSED = "closed"


type EventSourceOpenHandler = Callable[[], None]
type EventSourceErrorHandler = Callable[[BaseException | None], None]
type EventSourceMessageHandler = Callable[[ServerSentEvent], None]


class EventSource(Protocol):
    """Transport-neutral callback-oriented server-sent event source."""

    on_open: EventSourceOpenHandler | None
    on_error: EventSourceErrorHandler | None
    on_message: EventSourceMessageHandler | None

    @property
    def ready_state(self) -> EventSourceState:
        """Return the current connection state."""
        ...

    @property
    def retry_time(self) -> float:
        """Return the current initial reconnect delay in seconds."""
        ...

    def add_event_listener(self, event: str, handler: EventSourceMessageHandler) -> UUID:
        """Register a handler for one event type and return its listener token."""
        ...

    def remove_event_listener(self, event: str, listener: UUID) -> None:
        """Remove a previously registered event listener."""
        ...

    def connect(self) -> None:
        """Begin connecting without blocking the calling event loop."""
        ...

    def close(self) -> None:
        """Close the source and interrupt active reads or reconnect waits."""
        ...
