from __future__ import annotations

import asyncio

from fitz_py.errors import TransportError
from fitz_py.transport.base import Transport


class WebSocketTransport(Transport):
    def __init__(self, url: str, timeout_ms: int = 30000, max_frame_size: int = 65535) -> None:
        self._url = url
        self._timeout = timeout_ms / 1000
        self._max_frame_size = max_frame_size
        self._socket = None

    async def connect(self) -> None:
        try:
            import websockets

            self._socket = await asyncio.wait_for(
                websockets.connect(self._url, max_size=self._max_frame_size),
                timeout=self._timeout,
            )
        except Exception as exc:  # pragma: no cover - transport boundary
            raise TransportError(f"WebSocket connect failed: {exc}") from exc

    async def close(self) -> None:
        socket = self._socket
        self._socket = None
        if socket is None:
            return
        try:
            await socket.close()
        except Exception:
            return

    async def send(self, data: bytes) -> None:
        socket = self._socket
        if socket is None:
            raise TransportError("WebSocket transport is not connected")
        try:
            await socket.send(data)
        except Exception as exc:  # pragma: no cover - transport boundary
            raise TransportError(f"WebSocket send failed: {exc}") from exc

    async def receive(self) -> bytes:
        socket = self._socket
        if socket is None:
            raise TransportError("WebSocket transport is not connected")
        try:
            data = await asyncio.wait_for(socket.recv(), timeout=self._timeout)
        except Exception as exc:  # pragma: no cover - transport boundary
            raise TransportError(f"WebSocket receive failed: {exc}") from exc

        if isinstance(data, str):
            return data.encode()
        return data

    def get_url(self) -> str:
        return self._url
