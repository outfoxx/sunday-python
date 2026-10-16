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

## Whole-body binary responses

Declare whole-body binary results with `ResponseSpec(binary_body=True)`. The transport still checks the declared status
and media types, then supplies the body bytes directly to the result decoder. This preserves JSON and non-UTF-8 text
assets as bytes and accepts empty binary bodies as `b""`. Bodyless 204/205 responses retain their existing semantics.
`OperationSpec` also carries binary response media types into request Accept negotiation; no catch-all codec is needed.

```python
ResponseSpec(
    status=200,
    content_types=(MediaType("application/octet-stream"), MediaType("*/*")),
    decoder=TypeAdapter(bytes).validate_python,
    binary_body=True,
)
```

Generated clients must be regenerated with a generator that emits this flag. Older generated clients retain their
existing media-decoding behavior. Encoded byte properties inside JSON models continue to use normal model decoding.
