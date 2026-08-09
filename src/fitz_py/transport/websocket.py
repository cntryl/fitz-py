from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Protocol, cast

from websockets.asyncio.client import connect  # pyright: ignore[reportUnknownVariableType]

from fitz_py.errors import FitzTransportError
from fitz_py.transport.base import Transport


class _WebSocket(Protocol):
    async def close(self) -> None: ...

    async def send(self, data: bytes) -> None: ...

    async def recv(self) -> bytes | str: ...

    def ping(self) -> Awaitable[float]: ...


_connect = cast(Callable[..., Awaitable[_WebSocket]], connect)


class WebSocketTransport(Transport):
    def __init__(
        self,
        url: str,
        timeout_ms: int = 30000,
        max_frame_size: int = 1_048_576,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._url = url
        self._timeout = timeout_ms / 1000
        self._max_frame_size = max_frame_size
        self._socket: _WebSocket | None = None
        self._headers = headers or {}

    async def connect(self) -> None:
        try:
            self._socket = await asyncio.wait_for(
                _connect(
                    self._url,
                    max_size=self._max_frame_size,
                    ping_interval=None,
                    additional_headers=self._headers or None,
                ),
                timeout=self._timeout,
            )
        except Exception as exc:  # pragma: no cover - transport boundary
            raise FitzTransportError(f"WebSocket connect failed: {exc}") from exc

    async def close(self) -> None:
        socket = self._socket
        self._socket = None
        if socket is None:
            return
        try:
            await socket.close()
        except Exception:  # noqa: BLE001
            return

    async def send(self, data: bytes) -> None:
        socket = self._socket
        if socket is None:
            raise FitzTransportError("WebSocket transport is not connected")
        if len(data) > self._max_frame_size:
            raise FitzTransportError(
                f"WebSocket frame length {len(data)} exceeds max frame size {self._max_frame_size}"
            )
        try:
            await asyncio.wait_for(socket.send(data), timeout=self._timeout)
        except Exception as exc:  # pragma: no cover - transport boundary
            raise FitzTransportError(f"WebSocket send failed: {exc}") from exc

    async def receive(self) -> bytes:
        socket = self._socket
        if socket is None:
            raise FitzTransportError("WebSocket transport is not connected")
        try:
            data = await socket.recv()
        except Exception as exc:  # pragma: no cover - transport boundary
            raise FitzTransportError(f"WebSocket receive failed: {exc}") from exc

        if isinstance(data, str):
            raise FitzTransportError("WebSocket transport received text frame")
        return data

    def get_url(self) -> str:
        return self._url

    async def heartbeat(self, timeout: float) -> None:
        socket = self._socket
        if socket is None:
            raise FitzTransportError("WebSocket transport is not connected")
        try:
            pong = socket.ping()
            await asyncio.wait_for(pong, timeout=timeout)
        except Exception as exc:
            raise FitzTransportError(f"WebSocket heartbeat failed: {exc}") from exc
