from __future__ import annotations

import asyncio

import pytest

import fitz_py
from fitz_py._runtime import AsyncSubscription, RequestGate
from fitz_py.domains.queue import QueueClient
from fitz_py.domains.rpc import RpcClient
from fitz_py.domains.schedule import DeliveryMode, ScheduleClient
from fitz_py.errors import (
    FitzConnectionError,
    FitzTimeoutError,
    QueueError,
    RequestQueueFullError,
    SubscriptionBackpressureError,
)
from fitz_py.multiplexer import Multiplexer
from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.frame import FrameCodec, FrameParser
from fitz_py.protocol.messages import MSG_QUEUE_RESERVE, MSG_RPC_REQUEST, MSG_SCHEDULE_CREATE
from fitz_py.protocol.response import parse_response
from fitz_py.types import ClientConfig, ConcurrencyLimits


class FakeConnection:
    def __init__(self, responses: dict[int, bytes] | None = None) -> None:
        self.config = ClientConfig(url="tcp://localhost:1")
        self.generation = 1
        self.responses = responses or {}
        self.sent: list[tuple[int, bytes]] = []
        self.notifications = {}

    async def request(self, message_type: int, payload: bytes) -> bytes:
        self.sent.append((message_type, payload))
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


def test_clean_break_public_surface() -> None:
    assert fitz_py.ClientConfig is ClientConfig
    assert not hasattr(fitz_py, "ErrKvKeyNotFound")
    assert not hasattr(fitz_py, "ReconnectOptions")


def test_config_is_frozen_and_validated() -> None:
    config = ClientConfig(url="tcp://localhost:7777", limits=ConcurrencyLimits(max_in_flight=2))
    assert config.limits.max_in_flight == 2
    with pytest.raises(ValueError):
        ClientConfig(url="")


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
async def test_multiplexer_disconnect_fails_waiters() -> None:
    mux = Multiplexer()
    mux.set_connected()
    task = asyncio.create_task(mux.request(1, b"", lambda _: asyncio.sleep(0), 1))
    await asyncio.sleep(0)
    mux.set_disconnected()
    with pytest.raises(FitzConnectionError):
        await task


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
    reader.read_u8()
    reader.read_u8()
    assert reader.read_u64_be() == 5


@pytest.mark.asyncio
async def test_rpc_call_is_fire_and_forget_without_ack() -> None:
    connection = FakeConnection()
    call = await RpcClient(connection).call("rpc://r/a/work", b"input")
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


def test_invalid_queue_route_fails_before_io() -> None:
    connection = FakeConnection()
    with pytest.raises(QueueError):
        asyncio.run(QueueClient(connection).enqueue("queue://bad", b"x"))
