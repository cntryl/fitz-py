# fitz-py

`fitz-py` is the typed, asyncio-native Python client for the Fitz broker. Version 0.2.0 is a
deliberate clean break: clients are configured once, domain clients are cached properties, streamed
results are async iterators, and network/runtime queues are bounded.

PyPI publication is deferred. With Python 3.11 or later, build and install a
local wheel from this checkout:

```bash
python -m pip install build
python -m build --wheel
python -m pip install dist/cntryl_fitz-0.2.0-py3-none-any.whl
```

If the release wheel has already been supplied, only the last command is needed.

## Connect

Connection readiness waits for Wire-2 `HELLO` within `request_timeout`, so the
first domain command uses the broker's negotiated message-size and
in-flight-request limits. Wire 2 carries correlation in the fixed frame header;
capability bits are reserved as zero. `auth_settle_timeout` is retained as a
deprecated option for observing `CONNECT_REJECTED`.

```python
import asyncio

from fitz_py import Client, KVDurability


async def main():
    async with Client(
        "ws://localhost:4190/ws",
        token_provider=lambda: "",
        service_name="orders-worker",
    ) as client:
        async with client.kv.transaction("kv://example/app/users") as tx:
            await tx.put(b"alice", b"active")
            await tx.commit(KVDurability.SYNC)


asyncio.run(main())
```

`service_name` is optional and is sent in the `CONNECT` payload with the client
build identity. Surrounding whitespace is trimmed before it is sent.

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
- `client.schedule`: delivery modes, cursor pagination, cancel, and routed notifications.

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

RPC calls carry their remaining timeout budget in Wire 2. Closing a started
`client.rpc.call()` or cancelling its asyncio task sends a best-effort remote
cancellation; await `call.cancellation` for the broker result, or use `await call.cancel()` to
cancel and wait for that result. A worker receives `request.context.cancelled` and
`request.context.remaining_time_ms()`. Pass the remaining time to downstream RPC calls and wait
for the cancellation event while cleaning up request-owned work. Local cancellation cannot stop
work already admitted by a worker.
The deadline also expires when the caller is not polling its response stream.
Workers await request-owned cleanup before returning; the SDK acknowledges cleanup afterward.
A forwarded cancellation does not prove rollback or make a dispatched call safe to retry. RPC
UUIDs continue to identify caller and worker work in domain payloads; frame correlations route
client-to-broker responses.

For an explicit A→B→C link, create B's downstream call from the inbound request:

```python
async with client.rpc.call_from_request(request, "rpc://realm/area/child", body) as child:
    async for frame in child:
        await process(frame.body)
```

Schedule saturation is `BUSY` (`7005`) with a not-applied effect. A backend
failure is `7006` and has an indeterminate effect, so a caller should not retry
it blindly. Neither is reported as a cron or parse error.

## Schedule pagination

Schedule LIST (702) uses cursor pagination. `schedule.list_schedules()` requests
the first page; `offset` accepts only `None` or `0`. `schedule.list_v2(cursor=...,
limit=...)` is the explicit cursor API name for the same message. Message 707 is
retired. `schedule.create_batch(entries)` uses Schedule message 706.
`create_batch` accepts `ScheduleEntry` values, including each entry's delivery mode.
`list_v2` returns a `ScheduleCursorPage` with `entries`, `has_more`, and an opaque
`continuation` to pass as the next cursor. They use the Wire-2 Schedule messages
and report coded broker errors as `ScheduleError` domain codes.

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


### Queue backpressure and acknowledgement retries

Queue capacity error `4005` means the request was rejected before admission.
Use bounded backoff and a cancellation or deadline budget. For a rejected ACK,
keep the existing reservation and retry its completion while the lease remains
valid; do not rerun the work handler just to retry the ACK. Successful completion
closes the reservation.

A timeout, disconnect, cancellation after sending, or error `4007` can leave the
outcome unknown. Do not blindly replay enqueue, reserve, or completion in those
cases. Reconnection invalidates old reservation handles. A retryable error is a
classification, not a guarantee of automatic retries or exactly-once processing.
