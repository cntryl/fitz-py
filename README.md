# fitz-py

`fitz-py` is the typed, asyncio-native Python client for the Fitz broker. Version 0.2.0 is a
deliberate clean break: clients are configured once, domain clients are cached properties, streamed
results are async iterators, and network/runtime queues are bounded.

```bash
python -m pip install cntryl-fitz
```

## Connect

Connection readiness waits for `SERVER_HELLO` within `request_timeout`, so the
first domain command uses the broker's advertised capabilities. An explicit
zero capability advertisement preserves legacy behavior; a missing advertisement
times out and closes the transport. `auth_settle_timeout` is retained as a
deprecated option.

```python
from fitz_py import Client

async with Client(
    "ws://localhost:4190/ws",
    token_provider=lambda: "",
    service_name="orders-worker",
) as client:
    async with client.kv.transaction("kv://example/app/users") as tx:
        await tx.put(b"alice", b"active")
        await tx.commit()
```

`service_name` is optional. New brokers record it on the active session after
advertising the `SESSION_METADATA` capability; surrounding whitespace is trimmed before it is
reported, and older brokers receive no metadata frame.

`Client.aclose()` is permanent and idempotent. Reconnect is enabled by default after the first
successful authentication; an authentication rejection permanently closes the client. Configure
timeouts, bounded concurrency, retry, heartbeat, logging, metrics, and lifecycle events with
keyword arguments and the frozen policy values exported by `fitz_py`.

## Domains

- `client.kv`: transactions, scans, range deletes, durability, and mutation subscriptions.
- `client.queue`: delayed enqueue, broker-native long-poll reserve, fenced items, and availability.
- `client.rpc`: streamed calls and wildcard worker registrations with bounded handler dispatch.
- `client.lease`: queued fenced acquisition, query, change subscriptions, and managed renewal.
- `client.notice`: fire-and-forget publish and one-wire/many-consumer subscriptions.
- `client.stream`: append sessions, filtered replay, global cursors/watermarks, and commit events.
- `client.schedule`: delivery modes, total-count pagination, cancel, and routed notifications.

Subscriptions are independently closable async iterators:

```python
async with client.notice.subscribe("notice://example/app/*") as notices:
    async for notice in notices:
        print(notice.route, notice.body)
```

Reserve waits are performed by the broker rather than local polling:

```python
items = await client.queue.reserve("queue://example/work/*", lease=30, wait=10)
for item in items:
    await process(item.body)
    await item.complete()
```

If processing raises before `complete()`, the item remains inflight and becomes available for
redelivery when its lease expires.

Managed leases renew at one third of their TTL and preserve both renewal and release failures:

```python
async with client.lease.hold("lease://example/jobs/leader", ttl=30, wait=10) as lease:
    await run_leader(lease.token)
```

## Errors and cancellation

All library failures derive from `FitzError`. Transport, connection, timeout, protocol, bounded
queue, stale-handle, and domain failures have stable string codes and structured context. Task
cancellation is preserved. Requests cancelled after transmission leave a FIFO tombstone so a late
reply cannot corrupt the next same-type request.

When the broker advertises `CAP_RPC_CANCELLATION`, RPC calls carry their remaining timeout budget.
Closing a started `client.rpc.call()` or cancelling its asyncio task sends a best-effort remote
cancellation; await `call.cancellation` for the broker result, or use `await call.cancel()` to
cancel and wait for that result. A worker receives `request.context.cancelled` and
`request.context.remaining_time_ms()`. Pass the remaining time to downstream RPC calls and wait
for the cancellation event while cleaning up request-owned work. Brokers without this capability
keep the legacy wire format, and local cancellation cannot stop work already admitted by a worker.
The deadline also expires when the caller is not polling its response stream.
Workers await request-owned cleanup before returning; the SDK acknowledges cleanup afterward.
A forwarded cancellation does not prove rollback or make a dispatched call safe to retry.

For an explicit A→B→C link, create B's downstream call from the inbound request:

```python
async with client.rpc.call_from_request(request, "rpc://realm/area/child", body) as child:
    async for frame in child:
        await process(frame.body)
```

Schedule backend unavailability and broker saturation use the distinct coded
error `ERR_SCHEDULE_BACKEND_ERROR` (`7010`). It is retryable subject to the
operation's replay safety; it is never reported as a cron or parse error.

## Schedule broker extensions

The canonical `schedule.create()` and offset-based `schedule.list_schedules()` remain the
portable operations. Brokers that expose Schedule 706 and 707 also support
`schedule.create_batch(entries)` and `schedule.list_v2(cursor=..., limit=...)`.
`create_batch` accepts `ScheduleEntry` values, including each entry's delivery mode.
`list_v2` returns a `ScheduleCursorPage` with `entries`, `has_more`, and an opaque
`continuation` to pass as the next cursor. These methods use the broker extension wire
formats and report coded broker errors as `ScheduleError` domain codes.

## Verification

```bash
python -m pip install -e ".[dev]"
python -m ruff format --check .
python -m ruff check .
python -m pyright
python -m pytest tests/unit

docker compose up -d
python -m pytest tests/integration
python -m pytest tests/conformance
docker compose down --volumes

python -m benchmarks.hotpath
python -m build
```

The repository owns its broker Compose stack and a vendored copy of the canonical 17-scenario
cross-language suite. CI runs Python 3.11-3.14, wheel smoke tests, TCP/WebSocket, and
anonymous/JWT broker legs. Canonical behavior remains owned by the Fitz server documentation.

See [MIGRATION.md](MIGRATION.md) for every 0.2.0 break and [PERFORMANCE.md](PERFORMANCE.md) for the
benchmark evidence policy. [AUDIT_REMEDIATION.md](AUDIT_REMEDIATION.md) records the disposition and
proof for the independent correctness review.
