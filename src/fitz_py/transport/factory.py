from __future__ import annotations

from fitz_py.transport.base import Transport
from fitz_py.transport.tcp import TcpTransport
from fitz_py.types import TransportType


def create_transport(
    url: str,
    transport: TransportType,
    *,
    timeout_ms: int,
    max_frame_size: int,
) -> Transport:
    resolved = transport
    if resolved == "auto":
        resolved = "ws" if url.startswith("ws://") or url.startswith("wss://") else "tcp"

    if resolved == "ws":
        from fitz_py.transport.websocket import WebSocketTransport

        return WebSocketTransport(url, timeout_ms=timeout_ms, max_frame_size=max_frame_size)

    return TcpTransport(url, timeout_ms=timeout_ms, max_frame_size=max_frame_size)
