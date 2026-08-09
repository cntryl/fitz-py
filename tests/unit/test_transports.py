from __future__ import annotations

import asyncio

import pytest

from fitz_py.errors import FitzTransportError
from fitz_py.transport.factory import create_transport
from fitz_py.transport.tcp import TcpTransport
from fitz_py.transport.websocket import WebSocketTransport
from fitz_py.types import TransportType


class FakeWriter:
    def __init__(self) -> None:
        self.data = bytearray()
        self.closed = False

    def write(self, data: bytes) -> None:
        self.data.extend(data)

    async def drain(self) -> None: ...

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None: ...

    def is_closing(self) -> bool:
        return self.closed


class FakeSocket:
    def __init__(self, received: bytes | str = b"reply") -> None:
        self.received = received
        self.sent: list[bytes] = []
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    async def send(self, data: bytes) -> None:
        self.sent.append(data)

    async def recv(self) -> bytes | str:
        return self.received

    def ping(self) -> asyncio.Future[float]:
        future = asyncio.get_running_loop().create_future()
        future.set_result(0.001)
        return future


@pytest.mark.asyncio
async def test_tcp_transport_framing_receive_and_close() -> None:
    transport = TcpTransport("tcp://broker:5000", max_frame_size=16)
    writer = FakeWriter()
    reader = asyncio.StreamReader()
    reader.feed_data(b"\x00\x00\x00\x05reply")
    transport._writer = writer  # type: ignore[assignment]
    transport._reader = reader

    await transport.send(b"hello")
    await transport.heartbeat(0.1)
    assert bytes(writer.data) == b"\x00\x00\x00\x05hello"
    assert await transport.receive() == b"reply"
    assert transport.get_url() == "tcp://broker:5000"
    await transport.close()
    assert writer.closed


@pytest.mark.asyncio
async def test_tcp_transport_rejects_invalid_state_and_frame_sizes() -> None:
    transport = TcpTransport("localhost", max_frame_size=4)
    with pytest.raises(FitzTransportError, match="not connected"):
        await transport.send(b"x")
    with pytest.raises(FitzTransportError, match="not connected"):
        await transport.receive()
    with pytest.raises(FitzTransportError, match="not connected"):
        await transport.heartbeat(0.1)

    transport._writer = FakeWriter()  # type: ignore[assignment]
    with pytest.raises(FitzTransportError, match="exceeds"):
        await transport.send(b"12345")

    reader = asyncio.StreamReader()
    reader.feed_data(b"\x00\x00\x00\x05")
    transport._reader = reader
    with pytest.raises(FitzTransportError, match="exceeds"):
        await transport.receive()


@pytest.mark.asyncio
async def test_websocket_transport_binary_lifecycle_and_heartbeat() -> None:
    transport = WebSocketTransport("ws://broker", max_frame_size=8)
    socket = FakeSocket()
    transport._socket = socket

    await transport.send(b"hello")
    assert await transport.receive() == b"reply"
    await transport.heartbeat(0.1)
    assert socket.sent == [b"hello"]
    assert transport.get_url() == "ws://broker"
    await transport.close()
    assert socket.closed


@pytest.mark.asyncio
async def test_websocket_transport_rejects_text_and_disconnected_state() -> None:
    transport = WebSocketTransport("ws://broker")
    with pytest.raises(FitzTransportError, match="not connected"):
        await transport.send(b"x")
    with pytest.raises(FitzTransportError, match="not connected"):
        await transport.receive()
    with pytest.raises(FitzTransportError, match="not connected"):
        await transport.heartbeat(0.1)

    transport._socket = FakeSocket("text")
    with pytest.raises(FitzTransportError, match="text frame"):
        await transport.receive()


def test_transport_factory_resolves_auto_and_explicit_modes() -> None:
    websocket = create_transport(
        "wss://broker", TransportType.AUTO, timeout_ms=10, max_frame_size=100
    )
    tcp = create_transport("ws://broker", TransportType.TCP, timeout_ms=10, max_frame_size=100)

    assert isinstance(websocket, WebSocketTransport)
    assert isinstance(tcp, TcpTransport)
