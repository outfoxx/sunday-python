# Changelog

## Unreleased

- Align HTTPX EventSource and EventStream reconnection with the other Sunday runtimes: 500 ms capped exponential retry,
  downward jitter after the first attempt, reset after opening, dynamic 30-times caps, persistent positive `retry-max:`,
  redirect following, and exact-once request adaptation.
- Make SSE stuck-stream detection server-controlled. Remove `EventStreamOptions.event_timeout`, disable HTTPX read
  timeouts for SSE, and enable per-connection silence detection only after a positive `keepalive:` control.
- Align the public transport contract with the other Sunday runtimes through `transport_request()`,
  `transport_response()`, `response()`, and `result()`; retain request preparation, sending, and decoding as protected
  `BaseTransport` hooks.
- Generate public client transport and media defaults, hoist immutable response declarations, and reuse generated
  Pydantic adapters to keep operation methods concise.
- Let `HttpxTransport` own an internally created client when none is supplied and borrow any supplied client explicitly.
- Add typed request-encoding, response-decoding, response-validation, and transport errors with native response
  diagnostics while preserving typed problems and native failures.
- Add transport-neutral callback `EventSource` support and the HTTPX implementation alongside typed `EventStream`
  support, including reconnect state, listeners, custom request factories, and transport-owned shutdown.
- Select installed request/response codecs during negotiation and automatically compose optional CBOR, XML, and YAML
  codecs into default registries.
- Add generated and runtime RFC 6570 expansion with raw template parameters and mixed OpenAPI path serialization.
- Preserve Python wire types for aware and naive datetimes, dates, times, URI formats, base64 bytes, binary bodies, and
  set collections.
- Generate async native request/response exchange methods, HTTP AsyncAPI publisher/subscriber verbs, and raw versus typed
  SSE APIs without transport-specific imports.
- Split the optional HTTPX implementation into a dedicated `sunday.httpx` package while keeping one `sunday-python`
  distribution.
- Remove the beta-only `sunday.httpx_compat` and `sunday.httpx_sse` modules; consumers import the supported adapter API
  from `sunday.httpx`.
- Establish the Sunday Python runtime, HTTPX transport, Litestar integration, and server-sent event support.
- Add URI templates, multipart and patch bodies, typed response headers, XML/YAML codecs, transport lifecycle and
  observation, and closure-based HTTPX request adapters.
- Remove runtime-owned token authorization helpers; authentication and operational policy remain consumer adapters.
- Add generic transport, event-stream, and problem-registration protocols so generated clients remain transport-neutral.
