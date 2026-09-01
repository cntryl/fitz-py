"""LeaseInventoryObserver: the race-safe, reconnect-aware Lease inventory
observer required by docs/clients/client-requirements.md REQ-API-004C
(background: https://github.com/cntryl/fitz/issues/219 §5).

Covers:
  (a) bootstrap ordering - subscribe before list, buffered notifications
      applied only after the first list installs the initial view.
  (b) steady-state full reconciliation on notify, preserving complete items.
  (c) periodic reconciliation firing on a jittered interval.
  (d) reconnect invalidating the view and re-running the full bootstrap.
  (e) close() cancels the observer's background tasks and unsubscribes,
      with no leaked tasks.
"""

from __future__ import annotations

import asyncio
import inspect
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable

import pytest

from fitz_py.domains.lease import LeaseClient
from fitz_py.protocol.buffer import BufferWriter
from fitz_py.protocol.messages import (
    MSG_LEASE_LIST,
    MSG_LEASE_NOTIFY,
    MSG_LEASE_SUBSCRIBE,
    MSG_LEASE_UNSUBSCRIBE,
)
from fitz_py.types import ClientConfig


class ObserverConnection:
    """A fake connection that queues per-message-type responses, tracks
    on_reconnect listeners so tests can trigger a reconnect deterministically,
    and lets tests inject LEASE_NOTIFY pushes directly.
    """

    def __init__(self) -> None:
        self.config = ClientConfig(url="tcp://localhost:1")
        self.generation = 1
        self.notifications: dict[int, Callable[[bytes], None]] = {}
        self.sent: list[int] = []
        self.reconnect_listeners: dict[tuple[str, str], Callable[[], object]] = {}
        self._responses: dict[int, deque[object]] = defaultdict(deque)

    def queue(self, message_type: int, response: object) -> None:
        self._responses[message_type].append(response)

    async def request(self, message_type: int, payload: bytes) -> bytes:
        self.sent.append(message_type)
        pending = self._responses[message_type]
        if not pending:
            raise AssertionError(f"no queued response for message type {message_type}")
        response = pending.popleft()
        if callable(response):
            return await response()
        return response  # type: ignore[return-value]

    async def send(self, message_type: int, payload: bytes) -> None:
        self.sent.append(message_type)

    def register_notification_handler(
        self, message_type: int, handler: Callable[[bytes], None]
    ) -> None:
        self.notifications[message_type] = handler

    def register_push_classifier(self, *_args: object) -> None: ...

    def on_reconnect(
        self,
        listener: Callable[[], object],
        *,
        domain: str = "unknown",
        registration: str = "unknown",
    ) -> Callable[[], None]:
        key = (domain, registration)
        self.reconnect_listeners[key] = listener

        def unregister() -> None:
            self.reconnect_listeners.pop(key, None)

        return unregister

    def on_disconnect(self, _listener: Callable[[], object]) -> Callable[[], None]:
        return lambda: None

    def dispatch_async(self, work: Callable[[], Awaitable[None]]) -> bool:
        asyncio.create_task(work())
        return True

    async def run_with_retry(
        self, operation: Callable[[], Awaitable[object]], *, replay_safe: bool
    ) -> object:
        assert replay_safe
        return await operation()

    async def trigger_reconnect(self) -> None:
        for listener in list(self.reconnect_listeners.values()):
            result = listener()
            if inspect.isawaitable(result):
                await result

    def push_notify(self, sub_id: int, route: str) -> None:
        handler = self.notifications[MSG_LEASE_NOTIFY]
        writer = BufferWriter()
        writer.write_u64_be(sub_id)
        writer.write_route(route)
        writer.write_u32_be(0)
        handler(writer.build())


def _subscribe_response(sub_id: int) -> bytes:
    writer = BufferWriter()
    writer.write_u8(0)
    writer.write_u64_be(sub_id)
    return writer.build()


def _unsubscribe_response() -> bytes:
    writer = BufferWriter()
    writer.write_u8(0)
    return writer.build()


def _allow_unsubscribe(connection: ObserverConnection) -> None:
    """Queue enough UNSUBSCRIBE acks that closing the observer never blocks
    on an unqueued response, regardless of how many times a test reconnects.
    """
    for _ in range(8):
        connection.queue(MSG_LEASE_UNSUBSCRIBE, _unsubscribe_response())


def _list_response(
    *, items: list[tuple[str, str, int, str, int, int]], next_cursor: tuple[int, int] | None = None
) -> bytes:
    writer = BufferWriter()
    writer.write_u8(0)
    writer.write_u32_be(len(items))
    for route, owner_id, incarnation, acquired_at, expires_in_secs, renewals in items:
        writer.write_route(route)
        writer.write_route(owner_id)
        writer.write_u64_be(incarnation)
        writer.write_route(acquired_at)
        writer.write_u64_be(expires_in_secs)
        writer.write_u32_be(renewals)
    if next_cursor is None:
        writer.write_u8(0)
    else:
        writer.write_u8(1)
        writer.write_u64_be(next_cursor[0])
        writer.write_u32_be(next_cursor[1])
    return writer.build()


async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition was not met in time")
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_observer_bootstrap_subscribes_before_list_and_drains_buffered_notify() -> None:
    connection = ObserverConnection()
    connection.queue(MSG_LEASE_SUBSCRIBE, _subscribe_response(1))
    _allow_unsubscribe(connection)

    list_gate = asyncio.Event()

    async def gated_first_page() -> bytes:
        await list_gate.wait()
        return _list_response(
            items=[("lease://acme/renderers/doc-1", "owner-a", 1, "t", 30, 0)],
        )

    connection.queue(MSG_LEASE_LIST, gated_first_page)
    connection.queue(
        MSG_LEASE_LIST,
        _list_response(
            items=[
                ("lease://acme/renderers/doc-1", "owner-a", 1, "t", 30, 0),
                ("lease://acme/renderers/doc-2", "owner-b", 1, "t", 30, 0),
            ],
        ),
    )

    client = LeaseClient(connection)
    observer = await client.observe("lease://acme/renderers/*", reconcile_interval=1000.0)
    try:
        await _wait_until(lambda: MSG_LEASE_LIST in connection.sent)
        assert connection.sent[:2] == [MSG_LEASE_SUBSCRIBE, MSG_LEASE_LIST]
        assert not observer.ready

        # A notification arrives while the first LIST is still pending: it
        # must be buffered rather than applied immediately.
        connection.push_notify(1, "lease://acme/renderers/doc-2")

        list_gate.set()
        await _wait_until(lambda: observer.ready)

        # The buffered notification must trigger exactly one more full
        # relist to reconcile it.
        await _wait_until(lambda: connection.sent.count(MSG_LEASE_LIST) >= 2)
        await _wait_until(
            lambda: (
                set(observer.view)
                == {"lease://acme/renderers/doc-1", "lease://acme/renderers/doc-2"}
            )
        )
    finally:
        await observer.close()


@pytest.mark.asyncio
async def test_observer_steady_state_relists_complete_items_on_notify() -> None:
    connection = ObserverConnection()
    connection.queue(MSG_LEASE_SUBSCRIBE, _subscribe_response(1))
    connection.queue(MSG_LEASE_LIST, _list_response(items=[]))
    _allow_unsubscribe(connection)

    client = LeaseClient(connection)
    observer = await client.observe("lease://acme/renderers/*", reconcile_interval=1000.0)
    try:
        await _wait_until(lambda: observer.ready)
        assert observer.view == {}

        connection.queue(
            MSG_LEASE_LIST,
            _list_response(
                items=[
                    (
                        "lease://acme/renderers/doc-1",
                        "owner-a",
                        77,
                        "2026-08-31T00:00:00Z",
                        30,
                        4,
                    )
                ]
            ),
        )
        connection.push_notify(1, "lease://acme/renderers/doc-1")
        await _wait_until(lambda: "lease://acme/renderers/doc-1" in observer.view)
        assert connection.sent.count(MSG_LEASE_LIST) == 2
        assert observer.view["lease://acme/renderers/doc-1"].owner_id == "owner-a"
        assert observer.view["lease://acme/renderers/doc-1"].holder_incarnation == 77
        assert observer.view["lease://acme/renderers/doc-1"].renewals == 4

        connection.queue(MSG_LEASE_LIST, _list_response(items=[]))
        connection.push_notify(1, "lease://acme/renderers/doc-1")
        await _wait_until(lambda: "lease://acme/renderers/doc-1" not in observer.view)
        assert connection.sent.count(MSG_LEASE_LIST) == 3
    finally:
        await observer.close()


@pytest.mark.asyncio
async def test_observer_periodic_reconciliation_fires_on_jittered_interval() -> None:
    connection = ObserverConnection()
    connection.queue(MSG_LEASE_SUBSCRIBE, _subscribe_response(1))
    connection.queue(MSG_LEASE_LIST, _list_response(items=[]))
    _allow_unsubscribe(connection)

    reconcile_gate = asyncio.Event()
    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        await reconcile_gate.wait()
        reconcile_gate.clear()

    client = LeaseClient(connection)
    observer = await client.observe(
        "lease://acme/renderers/*",
        reconcile_interval=5.0,
        jitter=0.2,
        sleep=fake_sleep,
        random_func=lambda: 0.5,  # midpoint -> exactly the base interval
    )
    try:
        await _wait_until(lambda: observer.ready)
        await _wait_until(lambda: len(sleep_calls) >= 1)
        assert sleep_calls[0] == pytest.approx(5.0)

        connection.queue(
            MSG_LEASE_LIST,
            _list_response(items=[("lease://acme/renderers/doc-1", "owner-a", 1, "t", 30, 0)]),
        )
        reconcile_gate.set()
        await _wait_until(lambda: "lease://acme/renderers/doc-1" in observer.view)
        assert connection.sent.count(MSG_LEASE_LIST) == 2
        await _wait_until(lambda: len(sleep_calls) >= 2)
    finally:
        await observer.close()


@pytest.mark.asyncio
async def test_observer_reconnect_invalidates_view_and_rebootstraps() -> None:
    connection = ObserverConnection()
    connection.queue(MSG_LEASE_SUBSCRIBE, _subscribe_response(1))
    connection.queue(
        MSG_LEASE_LIST,
        _list_response(items=[("lease://acme/renderers/doc-1", "owner-a", 1, "t", 30, 0)]),
    )
    _allow_unsubscribe(connection)

    client = LeaseClient(connection)
    observer = await client.observe("lease://acme/renderers/*", reconcile_interval=1000.0)
    try:
        await _wait_until(lambda: observer.ready)
        assert "lease://acme/renderers/doc-1" in observer.view

        # LeaseClient's own reconnect-restore listener also fires and
        # resubscribes the still-open wire subscription.
        connection.queue(MSG_LEASE_SUBSCRIBE, _subscribe_response(2))
        connection.queue(
            MSG_LEASE_LIST,
            _list_response(items=[("lease://acme/renderers/doc-2", "owner-b", 1, "t", 30, 0)]),
        )

        await connection.trigger_reconnect()

        await _wait_until(lambda: connection.sent.count(MSG_LEASE_LIST) >= 2)
        await _wait_until(lambda: set(observer.view) == {"lease://acme/renderers/doc-2"})
        # A fresh bootstrap fully replaces the view rather than merging.
        assert "lease://acme/renderers/doc-1" not in observer.view
        assert "lease://acme/renderers/doc-2" in observer.view
    finally:
        await observer.close()


@pytest.mark.asyncio
async def test_observer_close_cancels_background_tasks_and_unsubscribes() -> None:
    connection = ObserverConnection()
    connection.queue(MSG_LEASE_SUBSCRIBE, _subscribe_response(1))
    connection.queue(MSG_LEASE_LIST, _list_response(items=[]))
    connection.queue(MSG_LEASE_UNSUBSCRIBE, _unsubscribe_response())

    client = LeaseClient(connection)
    tasks_before = asyncio.all_tasks()
    observer = await client.observe("lease://acme/renderers/*", reconcile_interval=1000.0)
    await _wait_until(lambda: observer.ready)

    await observer.close()

    assert connection.sent[-1] == MSG_LEASE_UNSUBSCRIBE
    await asyncio.sleep(0)
    leaked = {
        task
        for task in asyncio.all_tasks() - tasks_before - {asyncio.current_task()}
        if not task.done()
    }
    assert not leaked


@pytest.mark.asyncio
async def test_observer_async_context_manager_closes_on_exit() -> None:
    connection = ObserverConnection()
    connection.queue(MSG_LEASE_SUBSCRIBE, _subscribe_response(1))
    connection.queue(MSG_LEASE_LIST, _list_response(items=[]))
    connection.queue(MSG_LEASE_UNSUBSCRIBE, _unsubscribe_response())

    client = LeaseClient(connection)
    observer = await client.observe("lease://acme/renderers/*", reconcile_interval=1000.0)
    await _wait_until(lambda: observer.ready)

    async with observer:
        pass

    assert connection.sent[-1] == MSG_LEASE_UNSUBSCRIBE
