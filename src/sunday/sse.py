# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import codecs
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ServerSentEvent:
    """A parsed server-sent event and Sunday reconnect control fields."""

    data: str | None = None
    event: str | None = None
    id: str | None = None
    retry: int | None = None
    retry_max: int | None = None
    keepalive: int | None = None


@dataclass(frozen=True, slots=True)
class EventStreamOptions:
    """Connection and retry policy for a server-sent event stream."""

    retry: float = 0.5
    retry_max: float = 15.0
    event_timeout: float | None = None


class EventParser:
    """Incremental UTF-8 parser for the server-sent event wire format."""

    def __init__(self) -> None:
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self._line: list[str] = []
        self._skip_lf = False
        self._first_character = True
        self._data: list[str] = []
        self._event: str | None = None
        self._id: str | None = None
        self._retry: int | None = None
        self._retry_max: int | None = None
        self._keepalive: int | None = None

    def feed(self, data: bytes) -> tuple[ServerSentEvent, ...]:
        """Consume a byte chunk and return every completed event frame."""
        return self._consume(self._decoder.decode(data))

    def finalize(self) -> tuple[ServerSentEvent, ...]:
        """Finish decoding and dispatch a final unterminated data frame."""
        events = list(self._consume(self._decoder.decode(b"", final=True)))
        if self._line:
            events.extend(self._process_line("".join(self._line)))
            self._line = []
        if self._has_frame():
            events.append(self._dispatch())
        return tuple(events)

    def _consume(self, text: str) -> tuple[ServerSentEvent, ...]:
        events: list[ServerSentEvent] = []
        for character in text:
            if self._first_character:
                self._first_character = False
                if character == "\ufeff":
                    continue
            if self._skip_lf:
                self._skip_lf = False
                if character == "\n":
                    continue
            if character == "\r":
                events.extend(self._process_line("".join(self._line)))
                self._line = []
                self._skip_lf = True
            elif character == "\n":
                events.extend(self._process_line("".join(self._line)))
                self._line = []
            else:
                self._line.append(character)
        return tuple(events)

    def _process_line(self, line: str) -> tuple[ServerSentEvent, ...]:
        if line == "":
            if self._has_frame():
                return (self._dispatch(),)
            return ()
        if line.startswith(":"):
            return ()

        field, separator, value = line.partition(":")
        if separator and value.startswith(" "):
            value = value[1:]
        if field == "data":
            self._data.append(value)
        elif field == "event":
            self._event = value
        elif field == "id" and "\x00" not in value:
            self._id = value
        elif field == "retry":
            self._retry = _milliseconds(value)
        elif field == "retry-max":
            retry_max = _milliseconds(value)
            self._retry_max = retry_max if retry_max and retry_max > 0 else None
        elif field == "keepalive":
            self._keepalive = _milliseconds(value)
        return ()

    def _has_frame(self) -> bool:
        return bool(
            self._data
            or self._event is not None
            or self._id is not None
            or self._retry is not None
            or self._retry_max is not None
            or self._keepalive is not None
        )

    def _dispatch(self) -> ServerSentEvent:
        event = ServerSentEvent(
            data="\n".join(self._data) if self._data else None,
            event=self._event,
            id=self._id,
            retry=self._retry,
            retry_max=self._retry_max,
            keepalive=self._keepalive,
        )
        self._data = []
        self._event = None
        self._id = None
        self._retry = None
        self._retry_max = None
        self._keepalive = None
        return event


def _milliseconds(value: str) -> int | None:
    if not value.isascii() or not value.isdigit():
        return None
    milliseconds = int(value)
    return milliseconds if milliseconds <= 2**53 - 1 else None
