from __future__ import annotations

import asyncio
import socket
from urllib.parse import urlparse

from fitz_py.errors import TimeoutError, TransportError
from fitz_py.transport.base import Transport


class TcpTransport(Transport):
    def __init__(self, url: str, timeout_ms: int = 30000, max_frame_size: int = 65535) -> None:
        self._url = url
        self._timeout = timeout_ms / 1000
        self._max_frame_size = max_frame_size
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

        parsed = urlparse(url if "://" in url else f"tcp://{url}")
        self._host = parsed.hostname or "localhost"
        self._port = parsed.port or 4090

    async def connect(self) -> None:
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port),
                timeout=self._timeout,
            )
            sock = self._writer.get_extra_info("socket")
            if sock is not None:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        except TimeoutError:
            raise
        except asyncio.TimeoutError as exc:  # pragma: no cover - transport boundary
            raise TimeoutError(
                f"TCP connection timeout after {int(self._timeout * 1000)}ms"
            ) from exc
        except Exception as exc:  # pragma: no cover - transport boundary
            raise TransportError(f"TCP connect failed: {exc}") from exc

    async def close(self) -> None:
        writer = self._writer
        self._reader = None
        self._writer = None
        if writer is None:
            return
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            return

    async def send(self, data: bytes) -> None:
        writer = self._writer
        if writer is None:
            raise TransportError("TCP transport is not connected")
        if len(data) > self._max_frame_size:
            raise TransportError(
                f"TCP frame length {len(data)} exceeds max frame size {self._max_frame_size}"
            )

        full_message = len(data).to_bytes(4, "big") + data
        try:
            writer.write(full_message)
            await asyncio.wait_for(writer.drain(), timeout=self._timeout)
        except asyncio.TimeoutError as exc:  # pragma: no cover - transport boundary
            raise TimeoutError(f"TCP send timeout after {int(self._timeout * 1000)}ms") from exc
        except Exception as exc:  # pragma: no cover - transport boundary
            raise TransportError(f"TCP send failed: {exc}") from exc

    async def receive(self) -> bytes:
        reader = self._reader
        if reader is None:
            raise TransportError("TCP transport is not connected")

        try:
            header = await reader.readexactly(4)
            frame_length = int.from_bytes(header, "big")
            if frame_length > self._max_frame_size:
                raise TransportError(
                    f"TCP frame length {frame_length} exceeds max frame size {self._max_frame_size}"
                )
            return await reader.readexactly(frame_length)
        except asyncio.IncompleteReadError as exc:  # pragma: no cover - transport boundary
            raise TransportError("TCP connection closed") from exc
        except asyncio.TimeoutError as exc:  # pragma: no cover - transport boundary
            raise TimeoutError(f"TCP receive timeout after {int(self._timeout * 1000)}ms") from exc
        except TransportError:
            raise
        except Exception as exc:  # pragma: no cover - transport boundary
            raise TransportError(f"TCP receive failed: {exc}") from exc

    def get_url(self) -> str:
        return self._url

    async def heartbeat(self, timeout: float) -> None:
        _ = timeout
        writer = self._writer
        if writer is None or writer.is_closing():
            raise TransportError("TCP transport is not connected")
