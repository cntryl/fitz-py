from __future__ import annotations

import asyncio

import pytest

from fitz_py._runtime import AsyncSubscription
from fitz_py.connection import Connection
from fitz_py.errors import FitzConnectionError, FitzTransportError
from fitz_py.protocol.frame import FrameCodec
from fitz_py.transport.base import Transport
from fitz_py.types import (
    ClientConfig,
    ConnectionState,
    HeartbeatPolicy,
    ReconnectPolicy,
)


class FakeTransport(Transport):
    def __init__(self) -> None:
        self.connected = False
        self.closed = False
        self.sent: list[bytes] = []
        self.inbound: asyncio.Queue[bytes | BaseException] = asyncio.Queue()
        self.send_event = asyncio.Event()
        self.heartbeat_calls = 0

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.closed = True

    async def send(self, data: bytes) -> None:
        if self.closed:
            raise FitzTransportError("closed")
        self.sent.append(data)
        self.send_event.set()

    async def receive(self) -> bytes:
        item = await self.inbound.get()
        if isinstance(item, BaseException):
            raise item
        return item

    def get_url(self) -> str:
        return "tcp://fake:4091"

    async def heartbeat(self, _timeout: float) -> None:
        self.heartbeat_calls += 1


def config(**overrides: object) -> ClientConfig:
    values: dict[str, object] = {
        "url": "tcp://fake:4091",
        "auth_settle_timeout": 0,
        "heartbeat": HeartbeatPolicy(enabled=False),
        "reconnect": ReconnectPolicy(enabled=False),
    }
    values.update(overrides)
    return ClientConfig(**values)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_connection_request_push_send_and_close_lifecycle() -> None:
    transport = FakeTransport()
    connection = Connection(lambda: transport, config())
    pushes: list[bytes] = []
    connection.register_notification_handler(88, pushes.append)

    await connection.connect()

    assert connection.generation == 1
    assert connection.get_state() is ConnectionState.AUTHENTICATED
    assert connection.is_connected()
    assert FrameCodec.decode_frame(transport.sent[0]).message_type == 1

    request = asyncio.create_task(connection.request(77, b"request"))
    while len(transport.sent) < 2:  # noqa: ASYNC110 -- observing an external fake transport
        await asyncio.sleep(0)
    outbound = FrameCodec.decode_frame(transport.sent[-1])
    assert (outbound.message_type, outbound.payload) == (77, b"request")
    await transport.inbound.put(FrameCodec.encode_frame(77, b"response"))
    assert await request == b"response"

    await connection.send(78, b"one-way")
    assert FrameCodec.decode_frame(transport.sent[-1]).payload == b"one-way"
    await transport.inbound.put(FrameCodec.encode_frame(88, b"push"))
    async with asyncio.timeout(1):
        while pushes != [b"push"]:  # noqa: ASYNC110 -- observing receive-loop dispatch
            await asyncio.sleep(0)

    await connection.close()
    await connection.close()
    assert transport.closed
    assert connection.get_state() is ConnectionState.CLOSED
    with pytest.raises(FitzConnectionError, match="closed"):
        await connection.connect()


@pytest.mark.asyncio
async def test_connection_reconnects_same_instance_after_receive_loss() -> None:
    transports = [FakeTransport(), FakeTransport()]
    index = 0

    def factory() -> FakeTransport:
        nonlocal index
        transport = transports[index]
        index += 1
        return transport

    lifecycle: list[str] = []
    reconnect_config = config(
        reconnect=ReconnectPolicy(enabled=True, max_attempts=2, backoff=0.0001, max_backoff=0.0001),
    )
    connection = Connection(factory, reconnect_config)
    connection.on_reconnect(lambda: lifecycle.append("restored"), domain="test", registration="x")
    disconnected = asyncio.Event()
    connection.on_disconnect(disconnected.set)
    await connection.connect()

    await transports[0].inbound.put(FitzTransportError("network lost"))
    await asyncio.wait_for(disconnected.wait(), 1)
    async with asyncio.timeout(1):
        while connection.generation != 2:  # noqa: ASYNC110 -- observing reconnect generation
            await asyncio.sleep(0)

    assert lifecycle == ["restored"]
    assert transports[0].closed
    assert connection.is_connected()
    await connection.close()


@pytest.mark.asyncio
async def test_reconnect_retries_when_transport_drops_during_restore() -> None:
    transports = [FakeTransport(), FakeTransport(), FakeTransport()]
    index = 0

    def factory() -> FakeTransport:
        nonlocal index
        transport = transports[index]
        index += 1
        return transport

    restoring = asyncio.Event()
    release_restore = asyncio.Event()

    async def restore() -> None:
        if restoring.is_set():
            return
        restoring.set()
        await release_restore.wait()

    connection = Connection(
        factory,
        config(
            reconnect=ReconnectPolicy(
                enabled=True, max_attempts=3, backoff=0.0001, max_backoff=0.0001
            )
        ),
    )
    connection.on_reconnect(restore, domain="test", registration="blocked")
    await connection.connect()
    await transports[0].inbound.put(FitzTransportError("first loss"))
    await asyncio.wait_for(restoring.wait(), 1)
    await transports[1].inbound.put(FitzTransportError("restore loss"))
    await asyncio.sleep(0)
    release_restore.set()

    async with asyncio.timeout(1):
        while connection.generation != 3 or not connection.is_connected():  # noqa: ASYNC110
            await asyncio.sleep(0)

    assert connection._transport is transports[2]
    await connection.close()


@pytest.mark.asyncio
async def test_close_during_restore_cannot_resurrect_connection() -> None:
    transports = [FakeTransport(), FakeTransport()]
    index = 0

    def factory() -> FakeTransport:
        nonlocal index
        transport = transports[index]
        index += 1
        return transport

    restoring = asyncio.Event()

    async def restore() -> None:
        restoring.set()
        await asyncio.Event().wait()

    connection = Connection(
        factory,
        config(
            reconnect=ReconnectPolicy(
                enabled=True, max_attempts=1, backoff=0.0001, max_backoff=0.0001
            )
        ),
    )
    connection.on_reconnect(restore, domain="test", registration="blocked")
    await connection.connect()
    await transports[0].inbound.put(FitzTransportError("loss"))
    await asyncio.wait_for(restoring.wait(), 1)

    await connection.close()
    await asyncio.sleep(0)

    assert connection.get_state() is ConnectionState.CLOSED
    assert not connection.is_connected()
    assert connection._transport is None


@pytest.mark.asyncio
async def test_manual_connect_coalesces_with_automatic_reconnect() -> None:
    transports = [FakeTransport(), FakeTransport()]
    index = 0

    def factory() -> FakeTransport:
        nonlocal index
        transport = transports[index]
        index += 1
        return transport

    connection = Connection(
        factory,
        config(
            reconnect=ReconnectPolicy(
                enabled=True,
                max_attempts=1,
                backoff=0.01,
                max_backoff=0.01,
            )
        ),
    )
    await connection.connect()
    await transports[0].inbound.put(FitzTransportError("loss"))
    async with asyncio.timeout(1):
        while connection._reconnect_task is None:  # noqa: ASYNC110
            await asyncio.sleep(0)

    await connection.connect()

    assert index == 2
    assert connection._transport is transports[1]
    assert connection.generation == 2
    await connection.close()


@pytest.mark.asyncio
async def test_old_transport_frame_cannot_resolve_new_generation_request() -> None:
    transports = [FakeTransport(), FakeTransport()]
    index = 0

    def factory() -> FakeTransport:
        nonlocal index
        transport = transports[index]
        index += 1
        return transport

    connection = Connection(
        factory,
        config(
            reconnect=ReconnectPolicy(
                enabled=True, max_attempts=1, backoff=0.0001, max_backoff=0.0001
            )
        ),
    )
    await connection.connect()
    await connection._connection_lost(FitzTransportError("external loss"))
    async with asyncio.timeout(1):
        while connection.generation != 2:  # noqa: ASYNC110
            await asyncio.sleep(0)

    request = asyncio.create_task(connection.request(77, b"request"))
    await transports[1].send_event.wait()
    await transports[0].inbound.put(FrameCodec.encode_frame(77, b"stale"))
    await asyncio.sleep(0)
    assert not request.done()
    await transports[1].inbound.put(FrameCodec.encode_frame(77, b"current"))
    assert await request == b"current"
    await connection.close()


@pytest.mark.asyncio
async def test_connection_rejects_request_when_not_authenticated() -> None:
    connection = Connection(FakeTransport, config())

    with pytest.raises(FitzConnectionError, match="DISCONNECTED"):
        await connection.request(7, b"")


def test_connection_registration_unsubscribers_are_idempotent() -> None:
    connection = Connection(FakeTransport, config())

    reconnect = connection.on_reconnect(
        lambda: None,
        domain="domain",
        registration="registration",
    )
    disconnect = connection.on_disconnect(lambda: None)
    reconnect()
    reconnect()
    disconnect()
    disconnect()


@pytest.mark.asyncio
async def test_connection_dispatch_async_reports_queue_acceptance() -> None:
    errors: list[BaseException] = []
    connection = Connection(FakeTransport, config())
    connection._dispatcher._on_error = errors.append
    completed = asyncio.Event()

    async def work() -> None:
        completed.set()

    assert connection.dispatch_async(work)
    await asyncio.wait_for(completed.wait(), 1)
    await connection.close()


@pytest.mark.asyncio
async def test_connection_close_unblocks_registered_subscription_consumer() -> None:
    connection = Connection(FakeTransport, config())

    async def close_wire() -> None: ...

    subscription = AsyncSubscription[object]("route", 1, close_wire)
    connection.on_close(subscription.finish)
    waiter = asyncio.create_task(anext(subscription))
    await asyncio.sleep(0)

    await connection.close()

    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(waiter, 1)
