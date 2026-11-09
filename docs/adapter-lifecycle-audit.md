# Adapter refactor lifecycle audit

## Assessment

The repeated findings were not independent corner cases. Earlier fixes and reviews were too
focused on individual call sites, especially explicit response closure. The design needed an
inventory of all native I/O and ownership transitions, plus tests of actual connection-pool
release rather than only mock `aclose()` calls.

Comparison with the Python worktree's pre-refactor `HEAD` confirms inherited problems: an AnyIO
cancel scope spanned SSE yields; EventSource retirement read mutable `self._stream`; shutdown
blanket-suppressed `CancelledError`; and `_closed` indicated that shutdown had started, not that
it had finished. The refactor added its own incomplete fixes: the first cleanup helper missed
HTTPX's EOF close, the stream-close wrapper then missed httpcore's internal read-error cleanup,
and clearing `_response` before awaiting cleanup let concurrent closers return early. A later
response-only join still missed closure before header reception/response adaptation had transferred
response ownership to the stream. Those intermediate changes should not be treated as independent proof of lifecycle correctness.

This audit covers the Python implementation and the issue families identified during the
Swift/Kotlin/Python work in this thread. It does not claim a fresh source review of the other
runtimes or the generator.

## Recurring issue families and checks

| Issue family identified in the refactor | Shared cause | Python invariant and evidence |
| --- | --- | --- |
| Mandatory TokenManager wrapping, competing caches, duplicate authorization | Two owners for one credential lifecycle | External providers are called directly; TokenManager explicitly opts into caching. Export adapts once. Security tests cover rotation, logout, rejection and acquisition counts. |
| Request adapter implicitly supplying response behavior; an explicit observer disabling invalidation | Hidden composition | Independent request/response chains, explicit flattening and no inferred response adapter. Adapter tests verify order, independent invocation and replacement ownership. |
| Nested pipelines and request-attached execution state | Reconstructing state instead of defining its owner | One flattened chain per transport. Attempt/recovery state is local to execution, and exported requests carry no hidden recovery state. |
| Replay errors wrapped or replaced; cancellation swallowed by encoding, nullable results, OAuth or retry handling | Broad exception handling crosses the stop-signal contract | A RequestCancellationError remains the same object and bypasses retry/reconnect/fallback. Regression coverage includes body factories, codec selection, provider errors, RFC problems, diagnostics and response history. |
| Swift continuation registration/completion races; Kotlin constrained-dispatcher cancellation | Completion notification confused with completed ownership | Python uses ordinary task completion. Shared cleanup has one task and every closer waits for it; it does not depend on a flag, a discarded pointer, or a blocking bridge. |
| Adapter/delegate body replacement lost on redirects; unsafe replay of consumed bodies | Native request reconstruction loses information or duplicates side effects | `copy_request` retains extensions and unchanged streams; adapters own consumed resources. Managed attempts rebuild and reapply adapters without accumulating headers. Arbitrary one-shot bodies are not replayed. |
| Missing/stale Host, discarded streaming-body framing, missing cookies or redirect history | Native constructor/follow-up semantics were only partially reproduced | TCP framing/Host tests, native HTTPX cookie comparisons, merged native-auth/redirect history and buffered intermediate bodies cover these separately. |
| Status substitution observable by caches/events in OkHttp | Trying to hide a policy change from the native stack | No status sentinels. Native recovery inside a native call is allowed; only Sunday-managed attempts promise adapter invocation. |
| Byte-at-a-time buffered collection; framework trailer behavior | Forcing cross-runtime implementation parity rather than contract parity | HTTPX retains bulk `aread()` and native byte iteration. A multi-chunk TCP regression verifies one owner task for a whole buffered body. Kotlin trailer behavior remains a native-framework concern, not a Python emulation. |
| SSE early exit, cancellation leaking into consumer work, cleanup replacing the consumer's error | Cancel scopes and resource scopes crossed yielded values | Cancel scopes are bounded to active I/O. Context exit preserves the original error and joins cleanup; existing early-exit tests verify the consumer task remains usable. |
| Pending/repeated cancellation, EOF cleanup, read-error cleanup and shutdown | AnyIO shielding mistaken for protection from direct repeated `Task.cancel()` | Native sends/reads use `run_io`; cleanup uses `complete`. TCP tests gate real native cleanup and prove a one-connection pool is usable before client shutdown. |
| Immediate EventSource reconnect closes the replacement; retiring tasks disappear from shutdown | Shared mutable generation pointers | Each run owns its stream; callbacks and state updates check ownership. Retirement is tracked until all runs finish. Tests cover restart before start, restart while active, and old cleanup finishing after the replacement. |
| Concurrent `aclose()` returns early | “Closing” state treated as “closed” | Response references survive until shared cleanup completes. Transport shutdown has a shared completion task. Cancelled waiters cannot cancel that work. |
| Abandoned token persistence reported twice on Python 3.14 | Shield/wait ownership and failure observation were conflated | TokenManager uses `asyncio.wait()` without forwarding cancellation; its existing completion callback retrieves failures. A regression reproduces the former duplicate event-loop report. |

## Consolidated implementation

`httpx/_async.py` defines two distinct operations:

- `run_io` runs native I/O in an owner task with an AnyIO cancellation scope. Caller cancellation
  signals that scope; repeated raw cancellations stay in the waiting caller. Native shielded
  cleanup can therefore finish. The original caller cancellation is rethrown, while native
  RequestCancellationError objects remain unconditional stop signals. A result produced during
  cancellation is disposed when ownership cannot be transferred to the caller.
- `complete` waits for shared cleanup without forwarding cancellation. It joins the task before
  propagating the waiter's original cancellation. Cleanup failure does not replace an existing
  primary error at response or transport context boundaries.

The response stream wrapper delegates protected reads and owns one close task. Each active SSE
I/O block has a completion event paired with its cancel scope. External close callers join that
block before response cleanup, even when no response has yet been handed to the stream. I/O
ownership is cleared before yielding events, so close never joins application work between events.
Callbacks within the same I/O ownership chain only request closure to avoid waiting on themselves.
Each EventSource retains its active and retiring streams. Stream close and transport shutdown
use the same stream ownership check for active I/O and response cleanup, including native child
tasks. A startup checkpoint registers each run before callbacks can execute under an eager task
factory. Shutdown signals source closure immediately; reentrant callers do not join their own
run, while external callers still join the shared cleanup. The transport owns one shutdown task.
EventSource retirement uses `asyncio.wait()` to join source tasks without confusing independently
cancelled sources with cancellation of the shutdown caller. There is no
new public adapter API, request-attached state, retry policy, custom dispatcher or HTTP stack.

Bulk response collection uses one native I/O owner task for the complete `aread()`. A task-local
identity check avoids nested task creation for its raw chunks. SSE uses one operation per requested
chunk, without read-ahead queues or a persistent producer task. The identity check compares the
actual task, so child tasks inheriting context do not incorrectly bypass protection.

## Native I/O and ownership inventory

| Boundary | Owner / cancellation behavior | Coverage |
| --- | --- | --- |
| Upload and response-header wait | Native send owner; cancelled scope permits internal connection cleanup | TCP buffered/streaming upload and header cancellation |
| Response delivery races cancellation | Send owner disposes an undelivered response | TCP response-hook handoff race, buffered/streaming |
| Buffered body and redirect-history reads | Bulk read owner; no per-chunk tasks | TCP read cancellation, existing history/error tests, multi-chunk collection test |
| Response adapter consumes a body | Protected stream iteration when no outer native owner exists | TCP adapter reads and existing consume/replace tests |
| SSE body read, timeout, EOF, error and reconnect | Per-read native owner; consumer is outside cancel scopes between yields | TCP repeated cancellation, EOF, early exit, keepalive/reconnect tests |
| Concurrent stream/context closure | External callers join active I/O before shared response cleanup; response reference retained until completion | TCP header/response-adapter close before handoff, concurrent cancellation, context/decoder/stop identity, and pool reuse before waiting for the consumer |
| Close inside the stream's own adapter/native hook | Requests cancellation without joining its own callback | Adapter and native-hook self-close regressions |
| Decoding an operation response | Execution has already buffered and closed it; decoding uses `response.content` directly | TCP decoding verifies complete/closed response and preserves stop/cancellation identity |
| OAuth discovery, acquisition and refresh | Same native send boundary, preserving the isolated OAuth request contract | TCP cancellation in all three paths, OAuth interoperability replay suite |
| EventSource shutdown and immediate reconnect | Run-local streams; native task join includes retiring runs | TCP reconnect and retirement tests |
| Transport shutdown | Shared completion, owned client closed only after sources finish; active owned callbacks request shutdown without self-joining | TCP owned/borrowed client, concurrent shutdown, independently cancelled source, scope/raw cancellation, request/response adapter and native-hook reentrancy, retiring runs, stop and external cancellation identity |
| Transport context cleanup fails during an existing error | Cleanup adds a note; original object propagates | Stop, CancelledError and ordinary-error identity tests |
| Token acquisition and persistence | Waiter counting remains separate from the shared renewal task | Eager/deferred acquisition and refresh, coalescing, last-waiter cancellation, persistence-after-cancellation and duplicate-report tests |
| EventSource startup | Run registration precedes callbacks under eager and deferred task factories | TCP peer cleanup, reentrant startup shutdown, external close waiting, and stop/native cancellation identity |
| Managed diagnostic errors | One policy redacts HTTPX request and status errors; stop signals retain identity | Request adapters, native sends/hooks, response adapters and collection; buffered/streaming execution, credential redaction, status/body/metadata preservation, and cancellation/unmanaged identity |

The TCP cancellation cases use HTTPX's documented trace extension to hold an actual httpcore
cleanup operation open. They then cancel again and verify completion, original error identity or
original cancellation reason, server disconnect and pool reuse. Mock transports remain useful
for adapter transformations and error types, but are not evidence that a native socket was released.

## Carry-forward rules

Before porting the model, enumerate native cleanup triggered by send failure, read failure, EOF,
redirect/auth follow-ups, explicit close and cancellation. Test ownership handoff and concurrent
close at each asynchronous boundary. Do not infer completed cleanup from native “closed” flags.
Keep adapters independent and stop signals unconditional. Match each platform's native recovery,
body replay and response-metadata behavior instead of introducing invisible policy bridges.

Framework references: [HTTPX asynchronous streaming](https://www.python-httpx.org/async/),
[HTTPX trace extension](https://www.python-httpx.org/advanced/extensions/#trace),
[AnyIO cancellation semantics](https://anyio.readthedocs.io/en/stable/cancellation.html), and
[Python task waiting and cancellation](https://docs.python.org/3.14/library/asyncio-task.html).
