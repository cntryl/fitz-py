from __future__ import annotations

import asyncio
from urllib.parse import urlparse

from fitz_py.errors import TransportError
from fitz_py.protocol.frame import FrameParser
from fitz_py.transport.base import Transport


class TcpTransport(Transport):
    def __init__(self, url: str, timeout_ms: int = 30000, max_frame_size: int = 65535) -> None:
        self._url = url
        self._timeout = timeout_ms / 1000
        self._max_frame_size = max_frame_size
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._parser = FrameParser()

        parsed = urlparse(url if "://" in url else f"tcp://{url}")
        self._host = parsed.hostname or "localhost"
        self._port = parsed.port or 4191

    async def connect(self) -> None:
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port),
                timeout=self._timeout,
            )
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
        try:
            writer.write(data)
            await writer.drain()
        except Exception as exc:  # pragma: no cover - transport boundary
            raise TransportError(f"TCP send failed: {exc}") from exc

    async def receive(self) -> bytes:
        reader = self._reader
        if reader is None:
            raise TransportError("TCP transport is not connected")

        while True:
            try:
                chunk = await asyncio.wait_for(reader.read(4096), timeout=self._timeout)
            except Exception as exc:  # pragma: no cover - transport boundary
                raise TransportError(f"TCP receive failed: {exc}") from exc

            if not chunk:
                raise TransportError("TCP connection closed")

            if len(chunk) > self._max_frame_size + 3:
                raise TransportError("TCP frame exceeds configured max_frame_size")

            frames = self._parser.parse_frames(chunk)
            if not frames:
                continue

            frame = frames[0]
            if len(frame.payload) > self._max_frame_size:
                raise TransportError("TCP frame exceeds configured max_frame_size")
            return chunk

    def get_url(self) -> str:
        return self._url
