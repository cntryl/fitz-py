from __future__ import annotations

import asyncio

import pytest

from fitz_py.connection import Connection
from fitz_py.errors import AuthenticationError, TransportError
from fitz_py.protocol.frame import FrameCodec
from fitz_py.types import ConnectionState


class _FakeTransport:
    def __init__(self) -> None:
        self.connected = False
        self.sent: list[bytes] = []
        self._pending_read: asyncio.Future[bytes] | None = None

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        if self._pending_read is not None and not self._pending_read.done():
            self._pending_read.set_exception(TransportError("closed"))
        self.connected = False
        return None

    async def send(self, data: bytes) -> None:
        self.sent.append(data)

    async def receive(self) -> bytes:
        loop = asyncio.get_running_loop()
        self._pending_read = loop.create_future()
        return await self._pending_read

    def respond(self, data: bytes) -> None:
        if self._pending_read is not None and not self._pending_read.done():
            self._pending_read.set_result(data)
            self._pending_read = None

    def get_url(self) -> str:
        return "ws://example.test"


class _DelayedCloseTransport(_FakeTransport):
    async def receive(self) -> bytes:
        await asyncio.sleep(0.2)
        raise TransportError("TCP connection closed")


class _ControllableTransport(_FakeTransport):
    def __init__(self) -> None:
        super().__init__()
        self.connect_started = False
        self._pending_read: asyncio.Future[bytes] | None = None

    async def connect(self) -> None:
        self.connect_started = True
        await super().connect()

    async def receive(self) -> bytes:
        loop = asyncio.get_running_loop()
        self._pending_read = loop.create_future()
        return await self._pending_read

    def fail(self, exc: Exception) -> None:
        if self._pending_read is not None and not self._pending_read.done():
            self.connected = False
            self._pending_read.set_exception(exc)

    async def close(self) -> None:
        if self._pending_read is not None and not self._pending_read.done():
            self._pending_read.set_exception(TransportError("closed"))
        await super().close()


async def _empty_token() -> str:
    return ""


def test_connection_state_exposes_connected() -> None:
    assert ConnectionState.CONNECTED.value == "CONNECTED"


@pytest.mark.asyncio
async def test_connection_emits_connected_before_authenticated() -> None:
    seen: list[ConnectionState] = []
    connection = Connection(lambda: _FakeTransport(), _empty_token, auth_settle_delay_ms=0)
    original_set_state = connection._set_state

    def record(state: ConnectionState) -> None:
        seen.append(state)
        original_set_state(state)

    connection._set_state = record  # type: ignore[method-assign]

    try:
        await connection.connect()

        assert seen[:4] == [
            ConnectionState.CONNECTING,
            ConnectionState.CONNECTED,
            ConnectionState.AUTHENTICATING,
            ConnectionState.AUTHENTICATED,
        ]
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_connection_notifies_disconnect_listeners_on_close() -> None:
    connection = Connection(lambda: _FakeTransport(), _empty_token)
    seen: list[str] = []

    connection.on_disconnect(lambda: seen.append("disconnect"))

    await connection.close()

    assert seen == ["disconnect"]


@pytest.mark.asyncio
async def test_connection_surfaces_delayed_auth_close_as_authentication_error() -> None:
    connection = Connection(
        lambda: _DelayedCloseTransport(),
        _empty_token,
        auth_settle_delay_ms=500,
    )

    try:
        with pytest.raises(AuthenticationError, match="TCP connection closed"):
            await connection.connect()
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_connection_does_not_reconnect_after_close_during_reconnect_backoff() -> None:
    first = _ControllableTransport()
    second = _ControllableTransport()
    transports = [first, second]

    def factory() -> _ControllableTransport:
        return transports.pop(0)

    connection = Connection(
        factory,
        _empty_token,
        auth_settle_delay_ms=0,
        reconnect_enabled=True,
        reconnect_max_attempts=1,
        reconnect_backoff_ms=50,
        reconnect_max_backoff_ms=50,
    )

    await connection.connect()
    first.fail(TransportError("boom"))

    async def wait_for_reconnecting() -> None:
        while connection.get_state() is not ConnectionState.RECONNECTING:
            await asyncio.sleep(0.01)

    await asyncio.wait_for(wait_for_reconnecting(), timeout=1)

    await connection.close()
    await asyncio.sleep(0.075)

    assert second.connect_started is False
    assert connection.get_state() is ConnectionState.CLOSED


@pytest.mark.asyncio
async def test_connection_bounds_outbound_requests_to_configured_limit() -> None:
    transport = _FakeTransport()
    connection = Connection(
        lambda: transport,
        _empty_token,
        auth_settle_delay_ms=0,
        max_in_flight_requests=1,
    )

    await connection.connect()

    first = asyncio.create_task(connection.request(77, b"first"))
    await asyncio.wait_for(_wait_for_sent_count(transport, 2), timeout=1)

    second = asyncio.create_task(connection.request(77, b"second"))

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(second, timeout=0.05)

    assert len(transport.sent) == 2

    transport.respond(FrameCodec.encode_frame(77, b"ok"))
    assert await first == b"ok"

    await connection.close()


async def _wait_for_sent_count(transport: _FakeTransport, count: int) -> None:
    while len(transport.sent) < count:
        await asyncio.sleep(0.01)
