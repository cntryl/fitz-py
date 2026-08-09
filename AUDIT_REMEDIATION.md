# Audit remediation

This file records the disposition of the six-reviewer correctness and ergonomics audit against
the current Fitz broker and protocol source. The broker checkout used for verification was at
`f78a5b389bc4e65fa455673b43e06ecee3b08a03`; all TCP/WebSocket and anonymous/JWT conformance
legs passed after remediation.

## Correctness findings

| Finding | Disposition |
| --- | --- |
| C1 Lease errors are plain | Rejected. The current broker encodes numeric Lease codes. Live contention produced code 5001, and `lease_codec.rs` uses `encode_error_into`. Coded fixtures cover every Lease operation. |
| C2 empty Lease owner | Fixed. Each client generates a stable non-empty owner; callers may provide `owner_id`, and handles preserve it through extend/release. |
| C3 reverse KV bounds | Fixed and broker-tested with descending bounds. |
| C4 Queue NOTIFY has a payload length | Rejected for the current broker. `queue_codec::encode_notify` emits the route followed directly by three u64 counts. A fixture locks in that shape. |
| C5 Stream plain errors | Fixed for APPEND, COMMIT, ROLLBACK, LAST, GET_METADATA, SUBSCRIBE, and UNSUBSCRIBE. READ remains correctly coded. |
| C6 Schedule SUBSCRIBE plain error | Fixed. |
| C7 Schedule wildcard subscriptions | Fixed for strict whole-segment `*` and `**` patterns. |
| H1 retry codes cannot occur | Corrected premise. Live KV and Lease frames produced codes 1006 and 5001. Dead entries were removed; current transient broker codes remain. |
| H2 RPC worker errors are plain | Rejected for the current broker. `rpc_codec::encode_response_into` emits numeric codes; a 6012 fixture protects this. |
| H3 empty RPC terminal dropped | Fixed; empty terminal frames are delivered before iteration completes. |
| H4 frame default too small | Fixed; the transport-frame default is 1 MiB. TLV records retain their independent u16 payload limit. |
| H5 Notice first-subscribe race | Fixed with locked single-flight lifecycle and a concurrent regression. |
| H6 APPEND requires exactly eight bytes | Fixed; opaque APPEND metadata is tolerated, with an offset decoded only for the current eight-byte form. |
| M1 Lease wait serializes unrelated routes | Superseded by protocol proof: deferred ACQUIRE responses have no correlation ID, so the complete lifecycle is serialized. Cancellation retains a FIFO tombstone until the deferred reply or disconnect. |
| M2 equal KV delete range | Rejected. The live broker returns `Invalid request: start must be less than end`; the client guard matches it. |
| M3 worker exception waits for timeout | Fixed; an unhandled worker exception sends a terminal 6010 response. |
| M4 TCP fallback port | Fixed to 4091. |
| M5 multiplexer hides decoder errors | Fixed with an observability error callback; classifier failure no longer discards a pending response. |
| M6 WebSocket outbound size | Fixed. |
| M7 empty optional Stream values | Fixed; explicit empty metadata/discriminators are encoded as present. |
| M8 reconnect restoration race | Fixed after deterministic reproduction. Transport loss or close during restoration can no longer resurrect an authenticated state, and manual connect coalesces with automatic reconnect. |
| L1 dead retry entries | Fixed. |
| L2 Queue batch maximum | Fixed at 1024. |
| L3 speculative Schedule ID response | Removed; CREATE now rejects trailing success data. |
| L4 implicit Schedule delivery mode | Fixed; `delivery_mode` is required. |
| L5 missing Notice UNSUBSCRIBE_ALL | Fixed with graceful local consumer completion. |

## Python API findings

- Rollback failures are no longer swallowed.
- `AsyncSubscription`, `LazyAsyncContext`, and `LazyAsyncIterator` are public annotation types.
- `LeaseLifecycleError` is an `ExceptionGroup` while retaining the Fitz error contract.
- `KVTransaction.get()` returns `bytes | None`; `KVGetResult` was removed.
- The Queue README example now explains lease-expiry redelivery.
- Logging has a typed protocol and multiplexer/handler errors reach observability.
- Ruff still selects `ALL`. Broad BLE, SLF, and TRY-family exemptions were replaced by narrow
  boundary suppressions; only TRY003 remains globally excluded for operation-specific domain
  exception messages.
- `is_retryable` accepts `BaseException`, `StreamCommitMode` is a string enum, and dead
  `sleep_backoff` code was removed.

`FrameCodec.decode_frame` remains because unit/property tests and benchmarks exercise the
single-frame codec directly. The one-second authentication settle window also remains: CONNECT
has no positive acknowledgement, and the window is required to observe asynchronous auth
rejection. TCP heartbeat provides transport/OS liveness rather than inventing an unsupported
protocol ping.

## Follow-up lifecycle and domain audit

The follow-up audit added deterministic coverage for deferred Lease correlation, reconnect
restoration loss/cancellation, competing connect paths, stale transport receives, ambiguous send
timeouts, lazy close during startup, parent-client subscription termination, immediate NOTIFY
delivery, RPC response sequencing and saturation, global Queue selectors, global Stream decoding,
strict response consumption, string-enum coercion, and bytes-like inputs.

## Proof status and remaining gaps

- Ruff format, Ruff `ALL` with documented project exceptions, and strict Pyright on the shipped
  package pass. Async, exception-boundary, security, import, naming, and complexity checks are
  active; documentation, exception-message, boolean-argument, typing-import, and size families
  remain explicitly excluded where they conflict with the current API or documentation strategy.
- The local branch-aware unit suite contains 101 tests and reaches 70.27% aggregate coverage. The
  combined 109-test unit/integration run reaches about 76%. CI now fails below 70%, but the aspirational
  95% line/90% branch and 100% critical-module branch bars are not yet achieved.
- All four TCP/WebSocket by anonymous/JWT conformance legs pass 17/17 against the pinned broker
  image, and package smoke covers Python 3.11 through 3.14 in CI.
- Codec and fan-out reference bars pass locally. Multiplexer loopback remains above its reference
  bar, and statistically comparable before/after plus repeated broker percentile evidence has not
  yet been captured. Performance remains evidence-only, as required.
