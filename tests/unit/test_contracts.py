from __future__ import annotations

import asyncio

import pytest

import fitz_py
from fitz_py._runtime import AsyncSubscription, RequestGate
from fitz_py.domains.kv import KVTransaction
from fitz_py.domains.lease import LeaseClient
from fitz_py.domains.notice import NoticeClient
from fitz_py.domains.queue import QueueClient
from fitz_py.domains.rpc import RPCClient
from fitz_py.domains.schedule import DeliveryMode, ScheduleClient
from fitz_py.domains.stream import StreamClient, StreamSession
from fitz_py.errors import (
    FitzConnectionError,
    FitzTimeoutError,
    FitzTransportError,
    KVError,
    LeaseError,
    QueueError,
    RequestQueueFullError,
    RPCError,
    ScheduleError,
    StreamError,
    SubscriptionBackpressureError,
)
from fitz_py.multiplexer import Multiplexer
from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.frame import FrameCodec, FrameParser
from fitz_py.protocol.messages import (
    MSG_KV_GET,
    MSG_KV_INSERT,
    MSG_KV_SCAN,
    MSG_LEASE_ACQUIRE,
    MSG_LEASE_EXTEND,
    MSG_LEASE_QUERY,
    MSG_LEASE_RELEASE,
    MSG_LEASE_SUBSCRIBE,
    MSG_LEASE_UNSUBSCRIBE,
    MSG_NOTICE_SUBSCRIBE,
    MSG_NOTICE_UNSUBSCRIBE,
    MSG_NOTICE_UNSUBSCRIBE_ALL,
    MSG_QUEUE_AVAILABILITY_NOTIFY,
    MSG_QUEUE_ENQUEUE,
    MSG_QUEUE_RESERVE,
    MSG_QUEUE_SUBSCRIBE,
    MSG_RPC_REQUEST,
    MSG_RPC_SUBSCRIBE_WORKER,
    MSG_SCHEDULE_CANCEL,
    MSG_SCHEDULE_CREATE,
    MSG_SCHEDULE_LIST,
    MSG_SCHEDULE_NOTIFY,
    MSG_SCHEDULE_SUBSCRIBE,
    MSG_SCHEDULE_UNSUBSCRIBE,
    MSG_STREAM_APPEND,
    MSG_STREAM_BEGIN,
    MSG_STREAM_COMMIT,
    MSG_STREAM_GET_METADATA,
    MSG_STREAM_LAST,
    MSG_STREAM_READ,
    MSG_STREAM_ROLLBACK,
    MSG_STREAM_SUBSCRIBE,
    MSG_STREAM_UNSUBSCRIBE,
)
from fitz_py.protocol.response import parse_response
from fitz_py.transport.tcp import TcpTransport
from fitz_py.transport.websocket import WebSocketTransport
from fitz_py.types import ClientConfig, ConcurrencyLimits


class FakeConnection:
    def __init__(self, responses: dict[int, bytes] | None = None) -> None:
        self.config = ClientConfig(url="tcp://localhost:1")
        self.generation = 1
        self.responses = responses or {}
        self.sent: list[tuple[int, bytes]] = []
        self.notifications = {}
        self.request_started = asyncio.Event()
        self.two_requests = asyncio.Event()

    async def request(self, message_type: int, payload: bytes) -> bytes:
        self.sent.append((message_type, payload))
        self.request_started.set()
        if len(self.sent) >= 2:
            self.two_requests.set()
        return self.responses[message_type]

    async def send(self, message_type: int, payload: bytes) -> None:
        self.sent.append((message_type, payload))

    def register_notification_handler(self, message_type, handler) -> None:
        self.notifications[message_type] = handler

    def register_push_classifier(self, *_args) -> None: ...
    def on_reconnect(self, *_args, **_kwargs):
        return lambda: None

    def on_disconnect(self, *_args):
        return lambda: None

    def dispatch_async(self, work):
        asyncio.create_task(work())
        return True

    async def run_with_retry(self, operation, *, replay_safe):
        assert replay_safe
        return await operation()


def test_clean_break_public_surface() -> None:
    assert hasattr(fitz_py, "ClientConfig")
    assert hasattr(fitz_py, "FitzLogger")
    assert not hasattr(fitz_py, "ErrKvKeyNotFound")
    assert not hasattr(fitz_py, "ReconnectOptions")


def test_config_is_frozen_and_validated() -> None:
    config = ClientConfig(url="tcp://localhost:7777", limits=ConcurrencyLimits(max_in_flight=2))
    assert config.limits.max_in_flight == 2
    with pytest.raises(ValueError):
        ClientConfig(url="")
    with pytest.raises(ValueError, match=r"reconnect\.backoff"):
        ClientConfig(
            url="tcp://localhost:7777",
            reconnect=fitz_py.ReconnectPolicy(backoff=0),
        )


def test_buffer_round_trip() -> None:
    writer = BufferWriter(1)
    writer.write_u8(7)
    writer.write_u64_be(2**63)
    writer.write_string("hello")
    reader = BufferReader(writer.build())
    assert (reader.read_u8(), reader.read_u64_be(), reader.read_string()) == (7, 2**63, "hello")
    assert reader.is_eof()


def test_frame_parser_handles_fragmentation() -> None:
    frame = FrameCodec.encode_frame(42, b"payload")
    parser = FrameParser()
    assert parser.parse_frames(frame[:3]) == []
    parsed = parser.parse_frames(frame[3:])
    assert [(item.message_type, item.payload) for item in parsed] == [(42, b"payload")]


def test_response_envelopes_are_strict() -> None:
    assert parse_response(b"\0data").data == b"data"
    writer = BufferWriter()
    writer.write_u8(1)
    writer.write_u32_be(4005)
    writer.write_string("full")
    response = parse_response(writer.build())
    assert (response.error_code, response.error) == (4005, "full")


@pytest.mark.asyncio
async def test_request_gate_is_bounded() -> None:
    gate = RequestGate(1, 0)
    release = await gate.acquire()
    with pytest.raises(RequestQueueFullError):
        await gate.acquire()
    release()


@pytest.mark.asyncio
async def test_subscription_backpressure_is_explicit() -> None:
    async def close() -> None: ...

    subscription = AsyncSubscription("x", 1, close)
    assert subscription.push(1)
    assert not subscription.push(2)
    with pytest.raises(SubscriptionBackpressureError):
        await anext(subscription)


@pytest.mark.asyncio
async def test_multiplexer_tombstone_consumes_late_reply() -> None:
    mux = Multiplexer()
    mux.set_connected()
    sent = asyncio.Event()

    async def send(_frame: bytes) -> None:
        sent.set()

    task = asyncio.create_task(mux.request(9, b"x", send, 0.01))
    await sent.wait()
    with pytest.raises(FitzTimeoutError):
        await task
    mux.dispatch(9, b"late")
    next_task = asyncio.create_task(mux.request(9, b"x", send, 1))
    await asyncio.sleep(0)
    mux.dispatch(9, b"current")
    assert await next_task == b"current"


@pytest.mark.asyncio
async def test_multiplexer_ambiguous_send_timeout_keeps_tombstone() -> None:
    mux = Multiplexer()
    mux.set_connected()

    async def ambiguous_send(_frame: bytes) -> None:
        raise FitzTimeoutError("drain timed out")

    with pytest.raises(FitzTimeoutError, match="drain"):
        await mux.request(9, b"first", ambiguous_send, 1)

    sent = asyncio.Event()

    async def send(_frame: bytes) -> None:
        sent.set()

    next_task = asyncio.create_task(mux.request(9, b"second", send, 1))
    await sent.wait()
    mux.dispatch(9, b"late-first")
    mux.dispatch(9, b"second-response")
    assert await next_task == b"second-response"


@pytest.mark.asyncio
async def test_multiplexer_disconnect_fails_waiters() -> None:
    mux = Multiplexer()
    mux.set_connected()
    task = asyncio.create_task(mux.request(1, b"", lambda _: asyncio.sleep(0), 1))
    await asyncio.sleep(0)
    mux.set_disconnected()
    with pytest.raises(FitzConnectionError):
        await task


def test_multiplexer_reports_push_decoder_failures() -> None:
    failures: list[BaseException] = []
    mux = Multiplexer(failures.append)

    def fail(_payload: bytes) -> None:
        raise ValueError("malformed push")

    mux.register_notification_handler(7, fail)
    mux.dispatch(7, b"bad")

    assert len(failures) == 1
    assert str(failures[0]) == "malformed push"


@pytest.mark.asyncio
async def test_queue_reserve_encodes_broker_wait_and_route_results() -> None:
    body = BufferWriter()
    body.write_u8(0)
    body.write_u32_be(1)
    body.write_route("queue://r/a/one")
    body.write_u64_be(10)
    body.write_u64_be(11)
    body.write_u32_be(3)
    body.write_bytes(b"msg")
    connection = FakeConnection({MSG_QUEUE_RESERVE: body.build()})
    items = await QueueClient(connection).reserve("queue://r/a/*", lease=30, wait=5)
    assert (items[0].route, items[0].body) == ("queue://r/a/one", b"msg")
    reader = BufferReader(connection.sent[0][1])
    reader.read_route()
    reader.read_u64_be()
    assert reader.read_u8() == 1
    assert reader.read_u32_be() == 1
    assert reader.read_u8() == 1
    assert reader.read_u64_be() == 5


@pytest.mark.asyncio
async def test_rpc_call_is_fire_and_forget_without_ack() -> None:
    connection = FakeConnection()
    call = await RPCClient(connection).open_call("rpc://r/a/work", b"input")
    assert connection.sent[0][0] == MSG_RPC_REQUEST
    await call.aclose()


@pytest.mark.asyncio
async def test_schedule_create_encodes_delivery_mode() -> None:
    connection = FakeConnection({MSG_SCHEDULE_CREATE: b"\0"})
    route = "schedule://r/a/jobs/nightly"
    assert (
        await ScheduleClient(connection).create(
            route, "0 0 * * *", delivery_mode=DeliveryMode.BROADCAST
        )
        == route
    )
    reader = BufferReader(connection.sent[0][1])
    assert reader.read_route() == route
    assert reader.read_string() == "0 0 * * *"
    assert reader.read_u8() == 0


@pytest.mark.asyncio
async def test_schedule_create_coerces_string_delivery_mode() -> None:
    connection = FakeConnection({MSG_SCHEDULE_CREATE: b"\0"})
    await ScheduleClient(connection).create(
        "schedule://r/a/jobs/nightly",
        "0 0 * * *",
        delivery_mode="broadcast",
    )

    reader = BufferReader(connection.sent[0][1])
    reader.read_route()
    reader.read_string()
    assert reader.read_u8() == 0


@pytest.mark.asyncio
async def test_stream_commit_coerces_string_mode() -> None:
    connection = FakeConnection({MSG_STREAM_COMMIT: b"\0\0\0\0\0"})
    session = StreamSession(connection, 7)

    await session.commit("sync")

    reader = BufferReader(connection.sent[0][1])
    assert reader.read_u64_be() == 7
    assert reader.read_u8() == 1


@pytest.mark.asyncio
async def test_stream_session_decodes_canonical_begin_and_append_responses() -> None:
    begin = BufferWriter()
    begin.write_u8(0)
    begin.write_u64_be(5)
    begin.write_u32_be(0)
    append = BufferWriter()
    append.write_u8(0)
    append.write_u32_be(8)
    append.write_u64_be(12)
    connection = FakeConnection(
        {
            MSG_STREAM_BEGIN: begin.build(),
            MSG_STREAM_APPEND: append.build(),
        }
    )

    session = await StreamClient(connection).begin("stream://r/a/events")

    assert await session.append(11, b"event") == 12


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "selector",
    ["stream://*/area/resource", "stream://*/area/*", "stream://*/*/resource"],
)
async def test_stream_global_filters_decode_extended_cursor(selector: str) -> None:
    page = BufferWriter()
    page.write_u32_be(0)
    page.write_u64_be(9)
    page.write_u8(0)
    page.write_u8(0)
    page.write_u8(1)
    page.write_u64_be(12)
    page.write_u8(0)
    page.write_u8(1)
    page.write_u64_be(13)
    page.write_u8(1)
    page.write_u64_be(14)
    envelope = BufferWriter()
    envelope.write_u8(0)
    envelope.write_u8(0)
    envelope.write_u32_be(len(page.build()))
    envelope.write_bytes(page.build())
    connection = FakeConnection({MSG_STREAM_READ: envelope.build()})

    result = await StreamClient(connection).read_page(selector, 0)

    assert result.cursor.last_global_offset == 12
    assert result.cursor.cursor_fingerprint == 13
    assert result.cursor.captured_watermark == 14


@pytest.mark.asyncio
async def test_kv_reverse_scan_accepts_descending_bounds() -> None:
    connection = FakeConnection({MSG_KV_SCAN: b"\0\0\0\0\0\0"})
    transaction = KVTransaction(connection, "kv://r/a/items", 7)

    page = await transaction.scan_page(start_key=b"z", end_key=b"a", reverse=True)

    assert page.entries == ()
    reader = BufferReader(connection.sent[0][1])
    reader.read_u64_be()
    reader.read_route()
    assert reader.read_u8() == 1
    assert reader.read_bytes(reader.read_u32_be()) == b"z"
    assert reader.read_u8() == 1
    assert reader.read_bytes(reader.read_u32_be()) == b"a"
    assert reader.read_u8() == 0
    assert reader.read_u8() == 1


@pytest.mark.asyncio
async def test_kv_error_response_decodes_domain_code_and_message() -> None:
    connection = FakeConnection({MSG_KV_INSERT: _coded_error(1006, "Key already exists")})
    transaction = KVTransaction(connection, "kv://r/a/items", 7)

    with pytest.raises(KVError, match="Key already exists") as raised:
        await transaction.insert(b"key", b"value")

    assert raised.value.domain_code == 1006


@pytest.mark.asyncio
async def test_domain_success_decoders_reject_malformed_flags_and_lengths() -> None:
    transaction = KVTransaction(FakeConnection({MSG_KV_GET: b"\0\2"}), "kv://r/a/items", 7)
    with pytest.raises(KVError, match="found flag"):
        await transaction.get(b"key")

    with pytest.raises(QueueError, match="item id"):
        await QueueClient(FakeConnection({MSG_QUEUE_ENQUEUE: b"\0junk"})).enqueue(
            "queue://r/a/items", b"body"
        )

    with pytest.raises(LeaseError, match="held flag"):
        await LeaseClient(FakeConnection({MSG_LEASE_QUERY: b"\0\2"})).query("lease://r/a/item")

    schedule = BufferWriter()
    schedule.write_u8(0)
    schedule.write_u64_be(0)
    schedule.write_u8(2)
    with pytest.raises(ScheduleError, match="entry marker"):
        await ScheduleClient(FakeConnection({MSG_SCHEDULE_LIST: schedule.build()})).list_schedules()

    with pytest.raises(StreamError, match="optional u64 flag"):
        await StreamClient(FakeConnection({MSG_STREAM_GET_METADATA: b"\0\2"})).metadata(
            "stream://r/a/item"
        )


def _lease_success(response_type: int, token: int) -> bytes:
    writer = BufferWriter()
    writer.write_u8(0)
    writer.write_u8(response_type)
    writer.write_u64_be(token)
    return writer.build()


def _coded_error(code: int, message: str) -> bytes:
    writer = BufferWriter()
    writer.write_u8(1)
    writer.write_u32_be(code)
    writer.write_string(message)
    return writer.build()


def _plain_error(message: str) -> bytes:
    writer = BufferWriter()
    writer.write_u8(1)
    writer.write_string(message)
    return writer.build()


@pytest.mark.asyncio
async def test_lease_operations_decode_coded_error_envelopes() -> None:
    error = _coded_error(5001, "HeldByOther")
    connection = FakeConnection(
        dict.fromkeys(
            (
                MSG_LEASE_ACQUIRE,
                MSG_LEASE_EXTEND,
                MSG_LEASE_QUERY,
                MSG_LEASE_RELEASE,
                MSG_LEASE_SUBSCRIBE,
                MSG_LEASE_UNSUBSCRIBE,
            ),
            error,
        )
    )
    client = LeaseClient(connection)

    operations = (
        client.acquire("lease://r/a/leader", ttl=30),
        client.query("lease://r/a/leader"),
        client._extend("lease://r/a/leader", "owner", 1, 30),
        client._release("lease://r/a/leader", "owner", 1),
        client._subscribe_wire("lease://r/a/leader"),
        client._unsubscribe_wire("lease://r/a/leader"),
    )
    for operation in operations:
        with pytest.raises(LeaseError, match="HeldByOther") as raised:
            await operation
        assert raised.value.domain_code == 5001


@pytest.mark.asyncio
async def test_lease_uses_stable_nonempty_owner_for_handle_lifecycle() -> None:
    extended = BufferWriter()
    extended.write_u8(0)
    extended.write_u64_be(10)
    connection = FakeConnection(
        {
            MSG_LEASE_ACQUIRE: _lease_success(0, 9),
            MSG_LEASE_EXTEND: extended.build(),
            MSG_LEASE_RELEASE: b"\0",
        }
    )
    lease = await LeaseClient(connection).acquire("lease://r/a/leader", ttl=30)
    await lease.extend(30)
    await lease.release()

    owners = []
    for _, payload in connection.sent:
        reader = BufferReader(payload)
        reader.read_route()
        owners.append(reader.read_route())
    assert owners == [lease.owner_id] * 3
    assert lease.owner_id


@pytest.mark.asyncio
async def test_queued_lease_wait_serializes_uncorrelated_deferred_responses() -> None:
    connection = FakeConnection(
        {
            MSG_LEASE_ACQUIRE: _lease_success(2, 0),
        }
    )
    client = LeaseClient(connection)
    first = asyncio.create_task(client.acquire("lease://r/a/first", ttl=30, wait=30))
    second = asyncio.create_task(client.acquire("lease://r/a/second", ttl=30, wait=30))

    await asyncio.wait_for(connection.request_started.wait(), 1)
    await asyncio.sleep(0)
    assert len(connection.sent) == 1
    handler = connection.notifications[MSG_LEASE_ACQUIRE]
    handler(_lease_success(0, 10))
    assert (await first).token == 10
    await asyncio.wait_for(connection.two_requests.wait(), 1)
    handler(_lease_success(0, 11))
    assert (await second).token == 11


@pytest.mark.asyncio
async def test_cancelled_queued_lease_retains_fifo_tombstone() -> None:
    connection = FakeConnection({MSG_LEASE_ACQUIRE: _lease_success(2, 0)})
    client = LeaseClient(connection)
    first = asyncio.create_task(client.acquire("lease://r/a/first", ttl=30, wait=30))
    await asyncio.wait_for(connection.request_started.wait(), 1)
    await asyncio.sleep(0)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first

    second = asyncio.create_task(client.acquire("lease://r/a/second", ttl=30, wait=30))
    await asyncio.sleep(0)
    assert len(connection.sent) == 1
    handler = connection.notifications[MSG_LEASE_ACQUIRE]
    handler(_lease_success(0, 10))
    await asyncio.wait_for(connection.two_requests.wait(), 1)
    handler(_lease_success(0, 11))

    assert (await second).token == 11


@pytest.mark.asyncio
async def test_queued_lease_wait_is_unblocked_by_disconnect() -> None:
    connection = FakeConnection({MSG_LEASE_ACQUIRE: _lease_success(2, 0)})
    client = LeaseClient(connection)
    pending = asyncio.create_task(client.acquire("lease://r/a/first", ttl=30, wait=30))
    await asyncio.wait_for(connection.request_started.wait(), 1)

    client._disconnect()

    with pytest.raises(FitzConnectionError, match="interrupted"):
        await pending


@pytest.mark.asyncio
async def test_stream_operations_decode_plain_error_envelopes() -> None:
    error = _plain_error("stream conflict")
    connection = FakeConnection(
        dict.fromkeys(
            (
                MSG_STREAM_APPEND,
                MSG_STREAM_COMMIT,
                MSG_STREAM_ROLLBACK,
                MSG_STREAM_LAST,
                MSG_STREAM_GET_METADATA,
                MSG_STREAM_SUBSCRIBE,
                MSG_STREAM_UNSUBSCRIBE,
            ),
            error,
        )
    )
    client = StreamClient(connection)
    operations = (
        StreamSession(connection, 1).append(0, b"event"),
        StreamSession(connection, 2).commit(),
        StreamSession(connection, 3).rollback(),
        client.peek("stream://r/a/events"),
        client.metadata("stream://r/a/events"),
        client._subscribe_wire("stream://r/a/*"),
        client._unsubscribe_wire("stream://r/a/*"),
    )

    for operation in operations:
        with pytest.raises(StreamError, match="stream conflict"):
            await operation


@pytest.mark.asyncio
async def test_stream_append_accepts_opaque_non_offset_metadata() -> None:
    response = BufferWriter()
    response.write_u8(0)
    response.write_u32_be(3)
    response.write_bytes(b"new")
    connection = FakeConnection({MSG_STREAM_APPEND: response.build()})
    session = StreamSession(connection, 1)

    assert await session.append(0, b"event", metadata=b"", discriminator="") is None
    reader = BufferReader(connection.sent[0][1])
    reader.read_u64_be()
    reader.read_u64_be()
    reader.read_bytes(reader.read_u32_be())
    assert reader.read_u8() == 1
    assert reader.read_u32_be() == 0
    assert reader.read_u8() == 1
    assert reader.read_string() == ""
    assert reader.is_eof()


def test_schedule_subscription_accepts_wildcard_patterns() -> None:
    client = ScheduleClient(FakeConnection())

    client.subscribe("schedule://r/a/*/run")
    client.subscribe("schedule://**")


@pytest.mark.asyncio
async def test_schedule_subscribe_decodes_plain_error() -> None:
    connection = FakeConnection({MSG_SCHEDULE_SUBSCRIBE: _plain_error("bad pattern")})

    with pytest.raises(ScheduleError, match="bad pattern"):
        await ScheduleClient(connection)._subscribe_wire("schedule://r/a/*/run")


@pytest.mark.asyncio
async def test_schedule_crud_list_and_notification_wire_contracts() -> None:
    listing = BufferWriter()
    listing.write_u8(0)
    listing.write_u64_be(2)
    listing.write_u8(1)
    listing.write_string("schedule://r/a/jobs/nightly")
    listing.write_string("0 0 * * *")
    listing.write_u8(1)
    listing.write_u32_be(3)
    listing.write_bytes(b"job")
    listing.write_u8(0)
    subscribed = BufferWriter()
    subscribed.write_u8(0)
    subscribed.write_u8(1)
    subscribed.write_u64_be(44)
    connection = FakeConnection(
        {
            MSG_SCHEDULE_CANCEL: b"\0",
            MSG_SCHEDULE_LIST: listing.build(),
            MSG_SCHEDULE_SUBSCRIBE: subscribed.build(),
            MSG_SCHEDULE_UNSUBSCRIBE: b"\0",
        }
    )
    client = ScheduleClient(connection)

    await client.cancel("schedule://r/a/jobs/nightly")
    page = await client.list_schedules(offset=1, limit=5)
    subscription = await client._subscriptions.subscribe("schedule://r/a/*/nightly")
    notify = BufferWriter()
    notify.write_u64_be(44)
    notify.write_route("schedule://r/a/jobs/nightly")
    notify.write_u32_be(4)
    notify.write_bytes(b"fire")
    connection.notifications[MSG_SCHEDULE_NOTIFY](notify.build())

    assert page.total_count == 2
    assert page.entries[0].delivery_mode is DeliveryMode.SINGLE
    assert page.entries[0].payload == b"job"
    assert (await anext(subscription)).payload == b"fire"
    await subscription.aclose()


@pytest.mark.asyncio
async def test_schedule_rejects_invalid_pagination_and_decode_shapes() -> None:
    client = ScheduleClient(FakeConnection())
    with pytest.raises(ValueError, match="non-negative"):
        await client.list_schedules(offset=-1)
    with pytest.raises(ValueError, match="1000"):
        await client.list_schedules(limit=1001)

    malformed = BufferWriter()
    malformed.write_u8(0)
    malformed.write_u64_be(1)
    malformed.write_u8(1)
    malformed.write_string("schedule://r/a/jobs/nightly")
    malformed.write_string("0 0 * * *")
    malformed.write_u8(9)
    connection = FakeConnection({MSG_SCHEDULE_LIST: malformed.build()})
    with pytest.raises(ScheduleError, match="delivery mode"):
        await ScheduleClient(connection).list_schedules()


@pytest.mark.asyncio
async def test_notice_first_subscribe_is_single_flight() -> None:
    subscribe = BufferWriter()
    subscribe.write_u8(0)
    subscribe.write_u8(1)
    subscribe.write_u64_be(8)
    connection = FakeConnection(
        {
            MSG_NOTICE_SUBSCRIBE: subscribe.build(),
            MSG_NOTICE_UNSUBSCRIBE: b"\0",
        }
    )
    client = NoticeClient(connection)

    first, second = await asyncio.gather(
        client.open_subscription("notice://r/a/*"),
        client.open_subscription("notice://r/a/*"),
    )

    assert [kind for kind, _ in connection.sent].count(MSG_NOTICE_SUBSCRIBE) == 1
    await first.aclose()
    await second.aclose()


@pytest.mark.asyncio
async def test_notice_unsubscribe_all_finishes_local_consumers() -> None:
    subscribe = BufferWriter()
    subscribe.write_u8(0)
    subscribe.write_u8(1)
    subscribe.write_u64_be(8)
    connection = FakeConnection(
        {
            MSG_NOTICE_SUBSCRIBE: subscribe.build(),
            MSG_NOTICE_UNSUBSCRIBE_ALL: b"\0",
        }
    )
    client = NoticeClient(connection)
    subscription = await client.open_subscription("notice://r/a/*")

    await client.unsubscribe_all()

    with pytest.raises(StopAsyncIteration):
        await anext(subscription)


@pytest.mark.asyncio
async def test_queue_reserve_rejects_batch_above_broker_limit() -> None:
    with pytest.raises(ValueError, match="1024"):
        await QueueClient(FakeConnection()).reserve(
            "queue://r/a/jobs",
            lease=30,
            batch_size=1025,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("selector", ["queue://*/cats/*", "queue://**"])
async def test_queue_accepts_canonical_global_selectors(selector: str) -> None:
    connection = FakeConnection({MSG_QUEUE_RESERVE: b"\0"})

    assert await QueueClient(connection).reserve(selector, lease=30) == []


@pytest.mark.asyncio
async def test_queue_rejects_falsey_non_integral_delay() -> None:
    client = QueueClient(FakeConnection())

    with pytest.raises(TypeError, match="integer"):
        await client.enqueue("queue://r/a/jobs", b"x", delay=0.0)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="integer"):
        await client.enqueue("queue://r/a/jobs", b"x", delay=False)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_queue_notify_decodes_current_broker_count_payload() -> None:
    subscribe = BufferWriter()
    subscribe.write_u8(0)
    subscribe.write_u8(1)
    subscribe.write_u64_be(4)
    connection = FakeConnection({MSG_QUEUE_SUBSCRIBE: subscribe.build()})
    client = QueueClient(connection)
    subscription = await client._subscriptions.subscribe("queue://r/a/*")
    notify = BufferWriter()
    notify.write_u64_be(4)
    notify.write_route("queue://r/a/jobs")
    notify.write_u64_be(3)
    notify.write_u64_be(2)
    notify.write_u64_be(1)

    connection.notifications[MSG_QUEUE_AVAILABILITY_NOTIFY](notify.build())

    availability = await anext(subscription)
    assert (availability.ready, availability.delayed, availability.inflight) == (3, 2, 1)


@pytest.mark.asyncio
async def test_rpc_worker_subscribe_decodes_current_coded_error() -> None:
    connection = FakeConnection(
        {MSG_RPC_SUBSCRIBE_WORKER: _coded_error(6012, "invalid worker pattern")}
    )

    async def handler(_request, _writer) -> None: ...

    with pytest.raises(RPCError, match="invalid worker pattern") as raised:
        await RPCClient(connection).register_worker("rpc://r/**", handler)

    assert raised.value.domain_code == 6012


def test_transport_defaults_match_broker_contract() -> None:
    config = ClientConfig(url="tcp://localhost")
    transport = TcpTransport("tcp://localhost")

    assert config.max_frame_size == 1_048_576
    assert transport._port == 4091


@pytest.mark.asyncio
async def test_websocket_rejects_oversized_outbound_frame() -> None:
    class Socket:
        async def send(self, _data: bytes) -> None:
            raise AssertionError("oversized frame reached socket")

    transport = WebSocketTransport("ws://localhost", max_frame_size=8)
    transport._socket = Socket()  # type: ignore[assignment]

    with pytest.raises(FitzTransportError, match="exceeds max frame size"):
        await transport.send(b"x" * 9)


def test_invalid_queue_route_fails_before_io() -> None:
    connection = FakeConnection()
    with pytest.raises(QueueError):
        asyncio.run(QueueClient(connection).enqueue("queue://bad", b"x"))
