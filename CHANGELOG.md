# Changelog

## Unreleased

- Split the optional HTTPX implementation into a dedicated `sunday.httpx` package while keeping one `sunday-python`
  distribution.
- Remove the beta-only `sunday.httpx_compat` and `sunday.httpx_sse` modules; consumers import the supported adapter API
  from `sunday.httpx`.
- Establish the Sunday Python runtime, HTTPX transport, Litestar integration, and server-sent event support.
- Add URI templates, multipart and patch bodies, typed response headers, XML/YAML codecs, transport lifecycle and
  observation, and closure-based HTTPX request adapters.
- Remove runtime-owned token authorization helpers; authentication and operational policy remain consumer adapters.
- Add generic transport, event-stream, and problem-registration protocols so generated clients remain transport-neutral.
