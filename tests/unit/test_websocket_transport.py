from __future__ import annotations

import pytest

from fitz_py.errors import TransportError
from fitz_py.transport.websocket import WebSocketTransport


class _FakeSocket:
    def __init__(self, payload: bytes | str) -> None:
        self._payload = payload

    async def recv(self) -> bytes | str:
        return self._payload


@pytest.mark.asyncio
async def test_websocket_transport_returns_binary_frames_unchanged() -> None:
    transport = WebSocketTransport("ws://example.test")
    transport._socket = _FakeSocket(b"payload")

    data = await transport.receive()

    assert data == b"payload"


@pytest.mark.asyncio
async def test_websocket_transport_rejects_text_frames() -> None:
    transport = WebSocketTransport("ws://example.test")
    transport._socket = _FakeSocket("payload")

    with pytest.raises(TransportError, match="text frame"):
        await transport.receive()