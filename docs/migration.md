# Beta migration guide

## Transport implementations

The public transport contract now uses `transport_request()`, `transport_response()`, `response()`, and `result()` to
match the other Sunday runtimes. Implementations should subclass `BaseTransport` and provide `_prepare_request()`,
`_send()`, and `_decode_response()`; the previous public `prepare_request()`, `send()`, and `decode_response()` methods
have no compatibility aliases.

## Generated clients

Regenerated clients expose `transport`, `default_content_types`, and `default_accept_types`. Code accessing `_transport`
must use `transport`. Client media defaults may be supplied as keyword arguments and are propagated by aggregate clients.

## HTTPX ownership

`HttpxTransport(base_url=...)` creates and owns its HTTPX client. `HttpxTransport(client)` borrows a configured client and
never closes it. The former `close_client` argument has been removed; supplying both `client` and `base_url` is invalid.

## Errors

Catch `RequestEncodingError`, `ResponseDecodingError`, `ResponseValidationError`, or `TransportError` for
Sunday-originated failures. `UnexpectedResponse` remains the validation subtype for unmatched HTTP responses and retains
the native response diagnostics. Generated `Problem` exceptions and native HTTPX, adapter, and cancellation exceptions
remain independent.

## Server-sent event reconnection

`EventStreamOptions.event_timeout` has been removed. SSE silence detection is disabled until the current connection sends
a positive `keepalive:` control; applications cannot enable it independently. HTTPX read timeouts are ignored for SSE
requests, including when the transport borrows a configured client.

Leaving `retry_max` unset now selects the cross-runtime dynamic cap of 30 times the current retry interval instead of a
fixed 15-second cap. Explicit positive client caps and positive server `retry-max:` controls remain supported.
