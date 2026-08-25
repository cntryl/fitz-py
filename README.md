# fitz-py

`fitz-py` is the typed, asyncio-native Python client for the Fitz broker. Version 0.3 is a
deliberate clean break: clients are configured once, domain clients are cached properties, streamed
results are async iterators, and network/runtime queues are bounded.

```bash
python -m pip install cntryl-fitz
```

## Connect

```python
from fitz_py import Client

async with Client(
    "ws://localhost:4190/ws",
    token_provider=lambda: "",
) as client:
    async with client.kv.transaction("kv://example/app/users") as tx:
        await tx.put(b"alice", b"active")
        await tx.commit()
```

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

Schedule backend unavailability and broker saturation use the distinct coded
error `ERR_SCHEDULE_BACKEND_ERROR` (`7010`). It is retryable subject to the
operation's replay safety; it is never reported as a cron or parse error.

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

See [MIGRATION.md](MIGRATION.md) for every 0.3 break and [PERFORMANCE.md](PERFORMANCE.md) for the
benchmark evidence policy. [AUDIT_REMEDIATION.md](AUDIT_REMEDIATION.md) records the disposition and
proof for the independent correctness review.
