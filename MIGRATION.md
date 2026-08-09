# Migrating to 0.3

Version 0.3 is a clean break. It intentionally provides no compatibility aliases.

- Construct clients with `Client(url, *, ...)`; `Client(ClientConfig(...))` is rejected.
- Close clients with `await client.aclose()`.
- Use acronym-preserving names such as `KVClient`, `KVTransaction`, `RPCClient`, and `RPCCall`.
- Use `async with client.kv.transaction(...) as tx`; `begin(...)` remains the explicitly named manual opener.
- Use `async with client.notice.subscribe(...) as notices` and the equivalent lazy subscription factories on other domains. `open_subscription(...)` is the manual Notice opener.
- Use `async with client.rpc.call(...) as responses`; `open_call(...)` is the manual opener.
- Use `async with client.rpc.worker(...)`; `register_worker(...)` remains the manual opener.
- Use `schedule.list_schedules(...)` instead of the ambiguous `schedule.list(...)`.
- Pass `delivery_mode=` explicitly to `schedule.create(...)`.
- Lease acquisitions now send a stable generated owner ID. Pass `owner_id=` to `acquire(...)` or
  `hold(...)` when an application-defined identity is required; handles reuse it for extend/release.
- `AsyncSubscription`, `LazyAsyncContext`, and `LazyAsyncIterator` are public annotation types.
- `ClientConfig`, `FitzLogger`, `FitzMeter`, `FitzTracer`, and `BytesLike` are public annotation types.
- `KVTransaction.get()` returns `bytes | None`; the redundant `KVGetResult` wrapper was removed.
- `StreamCommitMode` is a string enum (`"buffered"`/`"sync"`), consistent with other public modes.
- Payload and key inputs accept `bytes`, `bytearray`, or `memoryview`; decoded payloads remain immutable `bytes`.
- Queue and Lease durations must be integral seconds. Floats and booleans are rejected rather than truncated.
- Lease expiration values are timezone-aware UTC `datetime` instances.
- Removed exception/config aliases include `ConnectionError`, `TransportError`, `TimeoutError`,
  `KvScanResult`, and `ReconnectOptions`; import the explicitly named 0.3 types instead.
- The default maximum frame size is now the broker-default 1 MiB; override `max_frame_size=` when
  deploying against a stricter broker.

Lazy stream/context objects start at most once. Closing one before it starts performs no network I/O.
Closing during startup waits for the factory and immediately cleans up any resource it created.
