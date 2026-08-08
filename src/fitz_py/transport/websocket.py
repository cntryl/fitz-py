from __future__ import annotations

import asyncio

from fitz_py.errors import TransportError
from fitz_py.transport.base import Transport


class WebSocketTransport(Transport):
    def __init__(
        self,
        url: str,
        timeout_ms: int = 30000,
        max_frame_size: int = 65535,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._url = url
        self._timeout = timeout_ms / 1000
        self._max_frame_size = max_frame_size
        self._socket = None
        self._headers = headers or {}

    async def connect(self) -> None:
        try:
            import websockets

            self._socket = await asyncio.wait_for(
                websockets.connect(
                    self._url,
                    max_size=self._max_frame_size,
                    ping_interval=None,
                    additional_headers=self._headers or None,
                ),
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
            data = await socket.recv()
        except Exception as exc:  # pragma: no cover - transport boundary
            raise TransportError(f"WebSocket receive failed: {exc}") from exc

        if isinstance(data, str):
            raise TransportError("WebSocket transport received text frame")
        return data

    def get_url(self) -> str:
        return self._url

    async def heartbeat(self, timeout: float) -> None:
        socket = self._socket
        if socket is None:
            raise TransportError("WebSocket transport is not connected")
        try:
            pong = await socket.ping()
            await asyncio.wait_for(pong, timeout=timeout)
        except Exception as exc:
            raise TransportError(f"WebSocket heartbeat failed: {exc}") from exc
