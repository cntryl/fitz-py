from __future__ import annotations

import pytest

from fitz_py.domains.kv import KvClient
from fitz_py.domains.lease import LeaseClient
from fitz_py.domains.notice import NoticeClient
from fitz_py.domains.queue import QueueClient
from fitz_py.domains.rpc import RpcClient
from fitz_py.domains.schedule import ScheduleClient
from fitz_py.domains.stream import StreamClient
from fitz_py.protocol.messages import (
    MSG_KV_BEGIN,
    MSG_LEASE_ACQUIRE,
    MSG_LEASE_SUBSCRIBE,
    MSG_NOTICE_PUBLISH,
    MSG_NOTICE_SUBSCRIBE,
    MSG_QUEUE_ENQUEUE,
    MSG_QUEUE_SUBSCRIBE,
    MSG_RPC_REQUEST,
    MSG_RPC_SUBSCRIBE_WORKER,
    MSG_SCHEDULE_CREATE,
    MSG_SCHEDULE_SUBSCRIBE,
    MSG_STREAM_BEGIN,
    MSG_STREAM_READ,
    MSG_STREAM_SUBSCRIBE,
)


class _FakeMultiplexer:
    def expect_optional_response(self, _message_type: int):
        return lambda: None


class _FakeConnection:
    def __init__(self, response: bytes) -> None:
        self.response = response
        self.requests: list[tuple[int, bytes]] = []
        self.notification_handlers: dict[int, object] = {}
        self.reconnect_handlers: list[object] = []
        self._multiplexer = _FakeMultiplexer()

    async def request(self, message_type: int, payload: bytes) -> bytes:
        self.requests.append((message_type, payload))
        return self.response

    async def send_fire_and_forget(self, message_type: int, payload: bytes) -> None:
        self.requests.append((message_type, payload))

    def on_reconnect(self, handler) -> None:
        self.reconnect_handlers.append(handler)
        return None

    def register_notification_handler(self, message_type: int, handler) -> None:
        self.notification_handlers[message_type] = handler

    def get_multiplexer(self) -> _FakeMultiplexer:
        return self._multiplexer


@pytest.mark.asyncio
async def test_kv_begin_accepts_exact_three_segment_route() -> None:
    connection = _FakeConnection(b"\x00" + (42).to_bytes(8, "big"))
    client = KvClient(connection)

    transaction = await client.begin("kv://example/app/users")

    assert transaction is not None
    assert connection.requests[0][0] == MSG_KV_BEGIN
    assert len(connection.requests) == 1


@pytest.mark.asyncio
async def test_kv_begin_forwards_short_route_without_local_validation() -> None:
    connection = _FakeConnection(b"\x00" + (42).to_bytes(8, "big"))
    client = KvClient(connection)

    transaction = await client.begin("kv://example/app")

    assert transaction is not None
    assert connection.requests[0][0] == MSG_KV_BEGIN


@pytest.mark.asyncio
async def test_kv_begin_forwards_wrong_scheme_without_local_validation() -> None:
    connection = _FakeConnection(b"\x00" + (42).to_bytes(8, "big"))
    client = KvClient(connection)

    transaction = await client.begin("queue://example/app/users")

    assert transaction is not None
    assert connection.requests[0][0] == MSG_KV_BEGIN


@pytest.mark.asyncio
async def test_lease_acquire_accepts_exact_three_segment_route() -> None:
    connection = _FakeConnection(b"\x00\x01" + (42).to_bytes(8, "big"))
    client = LeaseClient(connection)

    lease = await client.acquire("lease://example/app/leader", 30)

    assert lease.route == "lease://example/app/leader"
    assert lease.token == 42
    assert connection.requests[0][0] == MSG_LEASE_ACQUIRE
    assert len(connection.requests) == 1


@pytest.mark.asyncio
async def test_lease_acquire_forwards_short_route_without_local_validation() -> None:
    connection = _FakeConnection(b"\x00\x01" + (42).to_bytes(8, "big"))
    client = LeaseClient(connection)

    lease = await client.acquire("lease://example/app", 30)

    assert lease.route == "lease://example/app"
    assert connection.requests[0][0] == MSG_LEASE_ACQUIRE


@pytest.mark.asyncio
async def test_lease_acquire_forwards_empty_segment_route_without_local_validation() -> None:
    connection = _FakeConnection(b"\x00\x01" + (42).to_bytes(8, "big"))
    client = LeaseClient(connection)

    lease = await client.acquire("lease://example//leader", 30)

    assert lease.route == "lease://example//leader"
    assert connection.requests[0][0] == MSG_LEASE_ACQUIRE


@pytest.mark.asyncio
async def test_lease_subscribe_forwards_wildcard_pattern_without_local_validation() -> None:
    connection = _FakeConnection(b"\x00\x01" + (42).to_bytes(8, "big"))
    client = LeaseClient(connection)

    subscription = await client.subscribe("lease://example/**", lambda notification: None)

    assert subscription.pattern == "lease://example/**"
    assert connection.requests[0][0] == MSG_LEASE_SUBSCRIBE


@pytest.mark.asyncio
async def test_queue_enqueue_forwards_wildcard_route_without_local_validation() -> None:
    connection = _FakeConnection(b"\x00")
    client = QueueClient(connection)

    message_id = await client.enqueue("queue://example/app/*", b"payload")

    assert message_id == 0
    assert connection.requests[0][0] == MSG_QUEUE_ENQUEUE


@pytest.mark.asyncio
async def test_queue_subscribe_accepts_realm_wildcard_pattern() -> None:
    connection = _FakeConnection(b"\x00\x01" + (7).to_bytes(8, "big"))
    client = QueueClient(connection)

    subscription = await client.subscribe("queue://example/**", lambda notification: None)

    assert subscription.pattern == "queue://example/**"
    assert connection.requests[0][0] == MSG_QUEUE_SUBSCRIBE


@pytest.mark.asyncio
async def test_notice_publish_forwards_wildcard_route_without_local_validation() -> None:
    connection = _FakeConnection(b"\x00")
    client = NoticeClient(connection)

    await client.publish("notice://example/**", b"payload")

    assert connection.requests[0][0] == MSG_NOTICE_PUBLISH


@pytest.mark.asyncio
async def test_notice_subscribe_accepts_realm_wildcard_pattern() -> None:
    connection = _FakeConnection(b"\x00\x01" + (7).to_bytes(8, "big"))
    client = NoticeClient(connection)

    subscription = await client.subscribe("notice://example/**", lambda notification: None)

    assert subscription.pattern == "notice://example/**"
    assert connection.requests[0][0] == MSG_NOTICE_SUBSCRIBE


@pytest.mark.asyncio
async def test_rpc_call_forwards_wildcard_route_without_local_validation() -> None:
    connection = _FakeConnection(b"\x00")
    client = RpcClient(connection)

    iterator = await client.call("rpc://example/*", b"payload")

    assert iterator is not None
    assert connection.requests[0][0] == MSG_RPC_REQUEST


@pytest.mark.asyncio
async def test_rpc_register_worker_forwards_wildcard_route_without_local_validation() -> None:
    connection = _FakeConnection(b"\x00")
    client = RpcClient(connection)

    subscription = await client.register_worker("rpc://example/**", lambda request, writer: None)

    assert subscription.route == "rpc://example/**"
    assert connection.requests[0][0] == MSG_RPC_SUBSCRIBE_WORKER


@pytest.mark.asyncio
async def test_stream_begin_forwards_wildcard_route_without_local_validation() -> None:
    connection = _FakeConnection(b"\x00\x01" + (42).to_bytes(8, "big"))
    client = StreamClient(connection)

    session = await client.begin("stream://example/app/*")

    assert session is not None
    assert connection.requests[0][0] == MSG_STREAM_BEGIN


@pytest.mark.asyncio
async def test_stream_subscribe_forwards_wildcard_pattern_without_local_validation() -> None:
    connection = _FakeConnection(b"\x00\x01" + (7).to_bytes(8, "big"))
    client = StreamClient(connection)

    subscription = await client.subscribe("stream://example/area/**", lambda notification: None)

    assert subscription.pattern == "stream://example/area/**"
    assert connection.requests[0][0] == MSG_STREAM_SUBSCRIBE


@pytest.mark.asyncio
async def test_stream_read_accepts_realm_wildcard_pattern() -> None:
    connection = _FakeConnection(b"\x00")
    client = StreamClient(connection)

    records = await client.read("stream://example/**", 0)

    assert records == []
    assert connection.requests[0][0] == MSG_STREAM_READ


@pytest.mark.asyncio
async def test_schedule_create_accepts_exact_four_segment_route() -> None:
    connection = _FakeConnection(b"\x00")
    client = ScheduleClient(connection)

    route = await client.create("schedule://example/app/jobs/run", "0 0 * * *")

    assert route == "schedule://example/app/jobs/run"
    assert connection.requests[0][0] == MSG_SCHEDULE_CREATE
    assert len(connection.requests) == 1


@pytest.mark.asyncio
async def test_schedule_create_forwards_short_route_without_local_validation() -> None:
    connection = _FakeConnection(b"\x00")
    client = ScheduleClient(connection)

    route = await client.create("schedule://example/app", "0 0 * * *")

    assert route == "schedule://example/app"
    assert connection.requests[0][0] == MSG_SCHEDULE_CREATE


@pytest.mark.asyncio
async def test_schedule_create_forwards_wrong_scheme_without_local_validation() -> None:
    connection = _FakeConnection(b"\x00")
    client = ScheduleClient(connection)

    route = await client.create("queue://example/app/jobs/run", "0 0 * * *")

    assert route == "queue://example/app/jobs/run"
    assert connection.requests[0][0] == MSG_SCHEDULE_CREATE


@pytest.mark.asyncio
async def test_schedule_create_forwards_empty_segment_route_without_local_validation() -> None:
    connection = _FakeConnection(b"\x00")
    client = ScheduleClient(connection)

    route = await client.create("schedule://example//jobs/run", "0 0 * * *")

    assert route == "schedule://example//jobs/run"
    assert connection.requests[0][0] == MSG_SCHEDULE_CREATE


@pytest.mark.asyncio
async def test_schedule_subscribe_forwards_wildcard_route_without_local_validation() -> None:
    connection = _FakeConnection(b"\x00\x01" + (7).to_bytes(8, "big"))
    client = ScheduleClient(connection)

    subscription = await client.subscribe("schedule://example/app/*", lambda notification: None)

    assert subscription.pattern == "schedule://example/app/*"
    assert connection.requests[0][0] == MSG_SCHEDULE_SUBSCRIBE
