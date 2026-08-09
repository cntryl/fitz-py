from __future__ import annotations

import asyncio

import pytest

from fitz_py import Client, ConcurrencyLimits
from fitz_py._runtime import AsyncSubscription, LazyAsyncIterator, RequestGate
from fitz_py.domains.lease import _duration as lease_duration
from fitz_py.domains.queue import _duration as queue_duration
from fitz_py.domains.rpc import ResponseFrame, ResponseWriter, RPCCall
from fitz_py.errors import FitzConnectionError
from fitz_py.multiplexer import Multiplexer
from fitz_py.types import ClientConfig


def test_client_uses_direct_construction() -> None:
    client = Client(
        "tcp://localhost:7777",
        limits=ConcurrencyLimits(max_in_flight=3),
    )
    assert client.url == "tcp://localhost:7777"
    assert client.config.limits.max_in_flight == 3
    assert hasattr(client, "aclose")


def test_client_rejects_legacy_config_construction() -> None:
    with pytest.raises(TypeError):
        Client(ClientConfig(url="tcp://localhost:7777"))  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_request_gate_reserves_fifo_handoff_capacity() -> None:
    gate = RequestGate(1, 2)
    release_first = await gate.acquire()
    order: list[str] = []

    async def acquire(name: str) -> None:
        release = await gate.acquire()
        order.append(name)
        await asyncio.sleep(0)
        release()

    first = asyncio.create_task(acquire("first"))
    second = asyncio.create_task(acquire("second"))
    await asyncio.sleep(0)
    release_first()

    # A newcomer must not steal the slot reserved for the first FIFO waiter.
    newcomer = asyncio.create_task(acquire("newcomer"))
    await asyncio.gather(first, second, newcomer)
    assert order == ["first", "second", "newcomer"]
    assert gate.active == 0


@pytest.mark.asyncio
async def test_request_gate_transfers_cancelled_reservation() -> None:
    gate = RequestGate(1, 2)
    release = await gate.acquire()
    cancelled = asyncio.create_task(gate.acquire())
    successor = asyncio.create_task(gate.acquire())
    await asyncio.sleep(0)
    release()
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    successor_release = await asyncio.wait_for(successor, 1)
    successor_release()
    assert gate.active == 0


@pytest.mark.asyncio
async def test_request_gate_release_token_is_one_shot() -> None:
    gate = RequestGate(1, 0)
    release = await gate.acquire()
    release()
    release()
    assert gate.active == 0


@pytest.mark.asyncio
async def test_request_gate_close_fails_reserved_waiter() -> None:
    gate = RequestGate(1, 1)
    release = await gate.acquire()
    waiter = asyncio.create_task(gate.acquire())
    await asyncio.sleep(0)
    release()
    gate.close()
    with pytest.raises(FitzConnectionError):
        await waiter
    assert gate.active == 0


@pytest.mark.asyncio
async def test_subscription_close_unblocks_full_consumer() -> None:
    closed = False

    async def close_wire() -> None:
        nonlocal closed
        closed = True

    subscription = AsyncSubscription[int]("registration", 1, close_wire)
    assert subscription.push(1)
    await subscription.aclose()
    assert closed
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(anext(subscription), 1)


@pytest.mark.asyncio
async def test_subscription_close_unblocks_when_wire_cleanup_fails() -> None:
    async def close_wire() -> None:
        raise RuntimeError("unsubscribe failed")

    subscription = AsyncSubscription[int]("registration", 1, close_wire)
    with pytest.raises(RuntimeError, match="unsubscribe failed"):
        await subscription.aclose()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(anext(subscription), 1)


@pytest.mark.asyncio
async def test_lazy_stream_is_single_start_and_close_before_start_is_noop() -> None:
    starts = 0

    async def factory() -> AsyncSubscription[int]:
        nonlocal starts
        starts += 1

        async def close_wire() -> None: ...

        subscription = AsyncSubscription[int]("registration", 1, close_wire)
        subscription.push(1)
        return subscription

    stream = LazyAsyncIterator(factory)
    assert starts == 0
    async with stream:
        assert await anext(stream) == 1
    assert starts == 1

    unopened = LazyAsyncIterator(factory)
    await unopened.aclose()
    assert starts == 1


def test_broker_durations_reject_silent_float_truncation() -> None:
    with pytest.raises(TypeError, match="integer"):
        queue_duration(1.5, "lease")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="integer"):
        lease_duration(1.5, "ttl")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_rpc_terminal_frame_survives_capacity_one() -> None:
    class Pending:
        _pending: dict[bytes, RPCCall]

        def __init__(self) -> None:
            self._pending = {}

    client = Pending()
    call = RPCCall(client, b"key", 1, 1)  # type: ignore[arg-type]
    client._pending[b"key"] = call
    call.push(ResponseFrame(b"last", 0), end=True)
    assert await anext(call) == ResponseFrame(b"last", 0)
    with pytest.raises(StopAsyncIteration):
        await anext(call)


@pytest.mark.asyncio
async def test_rpc_empty_terminal_frame_is_delivered() -> None:
    class Pending:
        _pending: dict[bytes, RPCCall]

        def __init__(self) -> None:
            self._pending = {}

    client = Pending()
    call = RPCCall(client, b"key", 1, 1)  # type: ignore[arg-type]
    client._pending[b"key"] = call

    call.push(ResponseFrame(b"", 0), end=True)

    assert await anext(call) == ResponseFrame(b"", 0)
    with pytest.raises(StopAsyncIteration):
        await anext(call)


@pytest.mark.asyncio
async def test_rpc_response_sequence_advances_only_after_write() -> None:
    class Connection:
        generation = 1

        def __init__(self) -> None:
            self.attempts = 0

        async def send(self, _message_type: int, _payload: bytes) -> None:
            self.attempts += 1
            if self.attempts == 1:
                raise RuntimeError("write failed")

    connection = Connection()
    writer = ResponseWriter(connection, b"x" * 16)  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="write failed"):
        await writer.send(b"first")
    assert writer._sequence == 0
    await writer.send(b"retry")
    assert writer._sequence == 1


@pytest.mark.asyncio
async def test_multiplexer_cancellation_during_send_keeps_tombstone() -> None:
    mux = Multiplexer()
    entered = asyncio.Event()
    transmitted = asyncio.Event()

    async def send(_frame: bytes) -> None:
        entered.set()
        await transmitted.wait()

    cancelled = asyncio.create_task(mux.request(9, b"first", send, 1))
    await entered.wait()
    cancelled.cancel()
    transmitted.set()
    with pytest.raises(asyncio.CancelledError):
        await cancelled

    current = asyncio.create_task(mux.request(9, b"second", send, 1))
    await asyncio.sleep(0)
    mux.dispatch(9, b"late")
    mux.dispatch(9, b"current")
    assert await current == b"current"
