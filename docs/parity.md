# Sunday runtime parity

This matrix defines the first `sunday-python` beta boundary. “Generator” means the shared Sunday IR and generated Python
surface, not every construct accepted by upstream OpenAPI or AsyncAPI specifications.

| Capability | Kotlin | Swift | TypeScript | Python beta |
| --- | --- | --- | --- | --- |
| Typed models, aliases, inheritance, unions, discriminators, constraints | Yes | Yes | Yes | In scope |
| Strict and raw-token-preserving enums | Yes | Yes | Yes | In scope |
| Generic transport-neutral clients, operations, and response metadata | Yes | Yes | Yes | In scope |
| Path, query, header, and cookie serialization | Yes | Yes | Yes | In scope |
| JSON, text, binary, form, CBOR, XML, and YAML extension codecs | Yes | Yes | Yes | In scope where represented by IR |
| JSON Merge Patch and JSON Patch | Yes | Yes | Yes | In scope |
| Multipart and reusable streaming request bodies | Yes | Yes | Yes | In scope |
| Typed problems and nullification | Yes | Yes | Yes | In scope |
| SSE parsing, reconnect controls, cancellation, and response ownership | Yes | Yes | Yes | In scope |
| Consumer request adapters | Yes | Yes | Yes | In scope as ordered async callables or adapter objects |
| Generated authentication, authorization, retries, rate limits, or circuit breakers | No | No | No | Excluded |
| Broker/message transports and bindings | Kotlin only | No | No | Excluded |
| Target-specific JAX-RS metadata | Kotlin only | No | No | Excluded |

The Python generator skips broker-only operations by default and reports an unsupported-target diagnostic when broker
generation is explicitly requested. Auth and policy IR metadata is intentionally not emitted; applications implement
those concerns with request adapters or custom transports.

Generated Python clients import only the core `sunday` package. Applications select and construct an adapter such as
`HttpxTransport`; generated constructors do not accept native HTTP clients directly.
