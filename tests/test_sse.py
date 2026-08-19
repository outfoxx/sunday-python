# Copyright 2026 Outfox, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from sunday import EventParser, ServerSentEvent


def test_event_parser_handles_chunk_and_newline_boundaries() -> None:
    wire = (
        b"\xef\xbb\xbf: ping\rretry: 250\r\nretry-max: 2000\nkeepalive: 1000\n"
        b"id: event-1\nevent: update\ndata: one\ndata: two\r\n\r\n"
    )

    for chunk_size in range(1, len(wire) + 1):
        parser = EventParser()
        events: list[ServerSentEvent] = []
        for start in range(0, len(wire), chunk_size):
            events.extend(parser.feed(wire[start : start + chunk_size]))
        events.extend(parser.finalize())

        assert len(events) == 1
        event = events[0]
        assert event.data == "one\ntwo"
        assert event.event == "update"
        assert event.id == "event-1"
        assert event.retry == 250
        assert event.retry_max == 2000
        assert event.keepalive == 1000


def test_event_parser_dispatches_controls_without_data() -> None:
    parser = EventParser()

    events = parser.feed(b"retry: 100\nretry-max: 0\nkeepalive: 0\n\n")

    assert len(events) == 1
    assert events[0].data is None
    assert events[0].retry == 100
    assert events[0].retry_max is None
    assert events[0].keepalive == 0


def test_event_parser_ignores_invalid_controls_and_ids() -> None:
    parser = EventParser()

    events = parser.feed(b"retry: 1ms\nretry-max: -1\nid: a\x00b\ndata: value\n\n")

    assert len(events) == 1
    assert events[0].retry is None
    assert events[0].retry_max is None
    assert events[0].id is None
    assert events[0].data == "value"


def test_event_parser_flushes_unterminated_data_and_replaces_invalid_utf8() -> None:
    parser = EventParser()
    assert parser.feed(b"data: value \xff") == ()

    events = parser.finalize()

    assert len(events) == 1
    assert events[0].data == "value �"
