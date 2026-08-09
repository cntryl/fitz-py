from __future__ import annotations

import asyncio

import pytest

from fitz_py._runtime import AsyncSubscription, LazyAsyncContext, LazyAsyncIterator
from fitz_py.domains._subscriptions import SubscriptionRegistry
from fitz_py.domains.rpc import ResponseFrame, ResponseWriter, RPCClient
from fitz_py.errors import (
    FitzConnectionError,
    ReconnectRestoreError,
    RPCError,
    SubscriptionBackpressureError,
)
from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.messages import MSG_RPC_REQUEST, MSG_RPC_RESPONSE
from fitz_py.types import ClientConfig, ConcurrencyLimits


class StubConnection:
    def __init__(self, *, capacity: int = 2) -> None:
        self.config = ClientConfig(
            url="tcp://localhost:4091",
            limits=ConcurrencyLimits(subscription_buffer_size=capacity),
        )
        self.generation = 1
        self.sent: list[tuple[int, bytes]] = []
        self.handlers: dict[int, object] = {}
        self.send_error: BaseException | None = None
        self.accept_dispatch = True

    def register_push_classifier(self, *_args: object) -> None: ...

    def register_notification_handler(self, message_type: int, handler: object) -> None:
        self.handlers[message_type] = handler

    def on_disconnect(self, *_args: object) -> None: ...

    def on_reconnect(self, *_args: object, **_kwargs: object) -> None: ...

    async def send(self, message_type: int, payload: bytes) -> None:
        if self.send_error is not None:
            error, self.send_error = self.send_error, None
            raise error
        self.sent.append((message_type, payload))

    async def request(self, message_type: int, payload: bytes) -> bytes:
        self.sent.append((message_type, payload))
        return b"\0\0\0\0\0"

    def dispatch_async(self, work: object) -> bool:
        if not self.accept_dispatch:
            return False
        asyncio.create_task(work())  # type: ignore[operator]
        return True


@pytest.mark.asyncio
async def test_subscription_registry_shares_wire_lifecycle_and_fans_out() -> None:
    subscribed: list[str] = []
    unsubscribed: list[str] = []

    async def subscribe(registration: str) -> int:
        subscribed.append(registration)
        return 17

    async def unsubscribe(registration: str) -> None:
        unsubscribed.append(registration)

    registry = SubscriptionRegistry[str](2, subscribe, unsubscribe)
    first, second = await asyncio.gather(registry.subscribe("route"), registry.subscribe("route"))
    registry.publish(17, "event")

    assert subscribed == ["route"]
    assert await anext(first) == await anext(second) == "event"
    await first.aclose()
    assert unsubscribed == []
    await second.aclose()
    assert unsubscribed == ["route"]
    assert registry.registrations == ()


@pytest.mark.asyncio
async def test_subscription_registry_buffers_notify_in_subscribe_response_batch() -> None:
    registry: SubscriptionRegistry[str]

    async def subscribe(_registration: str) -> int:
        registry.publish(17, "immediate")
        return 17

    async def unsubscribe(_registration: str) -> None: ...

    registry = SubscriptionRegistry[str](2, subscribe, unsubscribe)
    subscription = await registry.subscribe("route")

    assert await anext(subscription) == "immediate"


@pytest.mark.asyncio
async def test_subscription_registry_cancellation_after_wire_subscribe_cleans_wire() -> None:
    subscribed = asyncio.Event()
    release = asyncio.Event()
    unsubscribed: list[str] = []

    async def subscribe(_registration: str) -> int:
        subscribed.set()
        await release.wait()
        return 17

    async def unsubscribe(registration: str) -> None:
        unsubscribed.append(registration)

    registry = SubscriptionRegistry[str](2, subscribe, unsubscribe)
    opening = asyncio.create_task(registry.subscribe("route"))
    await subscribed.wait()
    opening.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await opening
    assert unsubscribed == ["route"]
    assert registry.registrations == ()


@pytest.mark.asyncio
async def test_subscription_registry_failed_unsubscribe_retains_wire_state() -> None:
    async def subscribe(_registration: str) -> int:
        return 5

    async def unsubscribe(_registration: str) -> None:
        raise RuntimeError("broker refused unsubscribe")

    registry = SubscriptionRegistry[str](1, subscribe, unsubscribe)
    subscription = await registry.subscribe("route")

    with pytest.raises(RuntimeError, match="refused"):
        await subscription.aclose()
    assert registry.registrations == ("route",)
    replacement = await registry.subscribe("route")
    registry.publish(5, "still-live")
    assert await anext(replacement) == "still-live"


@pytest.mark.asyncio
async def test_subscription_registry_overflow_cleans_last_wire_consumer() -> None:
    unsubscribed = asyncio.Event()

    async def subscribe(_registration: str) -> int:
        return 5

    async def unsubscribe(_registration: str) -> None:
        unsubscribed.set()

    registry = SubscriptionRegistry[str](1, subscribe, unsubscribe)
    subscription = await registry.subscribe("route")
    registry.publish(5, "first")
    registry.publish(5, "overflow")

    await asyncio.wait_for(unsubscribed.wait(), 1)
    while registry.registrations:  # noqa: ASYNC110 - observe asynchronous wire cleanup
        await asyncio.sleep(0)
    assert registry.registrations == ()
    with pytest.raises(SubscriptionBackpressureError, match="fell behind"):
        await anext(subscription)


@pytest.mark.asyncio
async def test_subscription_registry_restore_rekeys_and_isolates_failures() -> None:
    attempts: dict[str, int] = {}
    failures: list[tuple[str, BaseException]] = []

    async def subscribe(registration: str) -> int:
        attempts[registration] = attempts.get(registration, 0) + 1
        if registration == "bad" and attempts[registration] == 2:
            raise RuntimeError("restore failed")
        return attempts[registration] * 10 + (registration == "bad")

    async def unsubscribe(_registration: str) -> None: ...

    registry = SubscriptionRegistry[str](2, subscribe, unsubscribe)
    good = await registry.subscribe("good")
    bad = await registry.subscribe("bad")
    await registry.restore(domain="test", on_error=lambda key, error: failures.append((key, error)))

    assert registry.registrations == ("good",)
    registry.publish(20, "restored")
    assert await anext(good) == "restored"
    with pytest.raises(ReconnectRestoreError, match="restore failed"):
        await anext(bad)
    assert failures[0][0] == "bad"


@pytest.mark.asyncio
async def test_lazy_context_close_during_start_closes_created_resource() -> None:
    factory_started = asyncio.Event()
    release_factory = asyncio.Event()
    closed: list[object] = []
    resource = object()

    async def factory() -> object:
        factory_started.set()
        await release_factory.wait()
        return resource

    context = LazyAsyncContext(factory, lambda value: _append_async(closed, value))
    start = asyncio.create_task(context.__aenter__())
    await factory_started.wait()
    close = asyncio.create_task(context.aclose())
    await asyncio.sleep(0)
    release_factory.set()

    with pytest.raises(FitzConnectionError, match="closed"):
        await start
    await close
    assert closed == [resource]


@pytest.mark.asyncio
async def test_lazy_iterator_close_during_start_closes_created_iterator() -> None:
    factory_started = asyncio.Event()
    release_factory = asyncio.Event()
    closed = asyncio.Event()

    class Iterator:
        def __aiter__(self) -> Iterator:
            return self

        async def __anext__(self) -> int:
            return 1

        async def aclose(self) -> None:
            closed.set()

    async def factory() -> Iterator:
        factory_started.set()
        await release_factory.wait()
        return Iterator()

    iterator = LazyAsyncIterator(factory)
    start = asyncio.create_task(anext(iterator))
    await factory_started.wait()
    close = asyncio.create_task(iterator.aclose())
    await asyncio.sleep(0)
    release_factory.set()

    with pytest.raises(FitzConnectionError, match="closed"):
        await start
    await close
    assert closed.is_set()


@pytest.mark.asyncio
async def test_subscription_close_failure_still_unblocks_waiter() -> None:
    async def close() -> None:
        raise RuntimeError("wire cleanup failed")

    subscription = AsyncSubscription[object]("route", 1, close)
    waiter = asyncio.create_task(anext(subscription))
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="cleanup"):
        await subscription.aclose()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(waiter, 1)


async def _append_async(values: list[object], value: object) -> None:
    values.append(value)


@pytest.mark.asyncio
async def test_rpc_response_writer_advances_only_after_successful_send() -> None:
    connection = StubConnection()
    writer = ResponseWriter(connection, b"c" * 16)  # type: ignore[arg-type]

    connection.send_error = FitzConnectionError("write failed")
    with pytest.raises(FitzConnectionError, match="write failed"):
        await writer.send(b"retry")
    await writer.send(b"one")
    await writer.send(b"", end=True)

    sequences = []
    for message_type, payload in connection.sent:
        assert message_type == MSG_RPC_RESPONSE
        reader = BufferReader(payload)
        reader.read_bytes(16)
        sequences.append(reader.read_u64_be())
    assert sequences == [0, 1]
    with pytest.raises(FitzConnectionError, match="stale"):
        await writer.send(b"late")


@pytest.mark.asyncio
async def test_rpc_response_writer_serializes_concurrent_sends() -> None:
    connection = StubConnection()
    writer = ResponseWriter(connection, b"c" * 16)  # type: ignore[arg-type]

    await asyncio.gather(writer.send(b"one"), writer.send(b"two", end=True))

    sequences = []
    for _, payload in connection.sent:
        reader = BufferReader(payload)
        reader.read_bytes(16)
        sequences.append(reader.read_u64_be())
    assert sequences == [0, 1]


@pytest.mark.asyncio
async def test_rpc_empty_terminal_response_is_delivered() -> None:
    connection = StubConnection()
    client = RPCClient(connection)  # type: ignore[arg-type]
    call = await client.open_call("rpc://realm/app/work", b"request")
    request = BufferReader(connection.sent[0][1])
    correlation_id = request.read_bytes(16)
    response = BufferWriter()
    response.write_bytes(correlation_id)
    response.write_u64_be(0)
    response.write_u8(1)
    response.write_u32_be(0)

    client._on_response(response.build())

    assert await anext(call) == ResponseFrame(b"", 0)
    with pytest.raises(StopAsyncIteration):
        await anext(call)


@pytest.mark.asyncio
async def test_rpc_terminal_application_body_starting_with_error_flag_is_data() -> None:
    connection = StubConnection()
    client = RPCClient(connection)  # type: ignore[arg-type]
    call = await client.open_call("rpc://realm/app/work", b"request")
    request = BufferReader(connection.sent[0][1])
    correlation_id = request.read_bytes(16)
    response = BufferWriter()
    response.write_bytes(correlation_id)
    response.write_u64_be(0)
    response.write_u8(1)
    response.write_u32_be(4)
    response.write_bytes(b"\x01abc")

    client._on_response(response.build())

    assert await anext(call) == ResponseFrame(b"\x01abc", 0)


@pytest.mark.asyncio
async def test_rpc_response_backpressure_fails_and_removes_call() -> None:
    connection = StubConnection(capacity=1)
    client = RPCClient(connection)  # type: ignore[arg-type]
    call = await client.open_call("rpc://realm/app/work", b"request")
    key = next(iter(client._pending))

    call.push(ResponseFrame(b"first", 0), False)
    call.push(ResponseFrame(b"second", 1), False)

    with pytest.raises(RPCError, match="fell behind"):
        await anext(call)
    await call.aclose()
    assert key not in client._pending


@pytest.mark.asyncio
async def test_rpc_disconnect_fails_pending_calls() -> None:
    connection = StubConnection()
    client = RPCClient(connection)  # type: ignore[arg-type]
    call = await client.open_call("rpc://realm/app/work", b"request")

    client._disconnect()

    with pytest.raises(FitzConnectionError, match="closed"):
        await anext(call)
    assert client._pending == {}
    assert connection.sent[0][0] == MSG_RPC_REQUEST


@pytest.mark.asyncio
async def test_rpc_disconnect_delivers_buffered_frames_before_failure() -> None:
    connection = StubConnection()
    client = RPCClient(connection)  # type: ignore[arg-type]
    call = await client.open_call("rpc://realm/app/work", b"request")
    call.push(ResponseFrame(b"buffered", 0), False)

    client._disconnect()

    assert await anext(call) == ResponseFrame(b"buffered", 0)
    with pytest.raises(FitzConnectionError, match="closed"):
        await anext(call)


@pytest.mark.asyncio
async def test_rpc_dispatch_saturation_sends_terminal_backpressure() -> None:
    connection = StubConnection()
    client = RPCClient(connection)  # type: ignore[arg-type]

    async def handler(_request: object, _response: object) -> None: ...

    await client.register_worker("rpc://realm/app/*", handler)  # type: ignore[arg-type]
    connection.accept_dispatch = False
    request = BufferWriter()
    request.write_bytes(b"c" * 16)
    request.write_route("rpc://realm/app/work")
    request.write_u32_be(4)
    request.write_bytes(b"body")

    client._on_request(request.build())
    async with asyncio.timeout(1):
        while not any(message == MSG_RPC_RESPONSE for message, _ in connection.sent):  # noqa: ASYNC110
            await asyncio.sleep(0)

    response = next(payload for message, payload in connection.sent if message == MSG_RPC_RESPONSE)
    reader = BufferReader(response)
    reader.read_bytes(16)
    assert reader.read_u64_be() == 0
    assert reader.read_u8() == 1
    error = BufferReader(reader.read_bytes(reader.read_u32_be()))
    assert error.read_u8() == 1
    assert error.read_u32_be() == 6003


def test_rpc_worker_rejects_request_trailing_bytes() -> None:
    connection = StubConnection()
    client = RPCClient(connection)  # type: ignore[arg-type]
    request = BufferWriter()
    request.write_bytes(b"c" * 16)
    request.write_route("rpc://realm/app/work")
    request.write_u32_be(4)
    request.write_bytes(b"body")
    request.write_u8(0)

    with pytest.raises(RPCError, match="trailing"):
        client._on_request(request.build())
