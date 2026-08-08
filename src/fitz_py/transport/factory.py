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
    websocket_headers: dict[str, str] | None = None,
) -> Transport:
    resolved = transport
    if resolved is TransportType.AUTO:
        resolved = (
            TransportType.WEBSOCKET if url.startswith(("ws://", "wss://")) else TransportType.TCP
        )

    if resolved is TransportType.WEBSOCKET:
        from fitz_py.transport.websocket import WebSocketTransport

        return WebSocketTransport(
            url,
            timeout_ms=timeout_ms,
            max_frame_size=max_frame_size,
            headers=websocket_headers,
        )

    return TcpTransport(url, timeout_ms=timeout_ms, max_frame_size=max_frame_size)
