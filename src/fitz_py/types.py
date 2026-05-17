"""Shared SDK configuration, transport, token, and connection-state types."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import Literal, TypeAlias

TransportType: TypeAlias = Literal["ws", "tcp", "auto"]
TokenProvider: TypeAlias = Callable[[], str | Awaitable[str]]


class ConnectionState(str, Enum):
    """Lifecycle states for a Fitz client connection."""

    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    AUTHENTICATING = "AUTHENTICATING"
    AUTHENTICATED = "AUTHENTICATED"
    RECONNECTING = "RECONNECTING"
    CLOSED = "CLOSED"


@dataclass(slots=True)
class ReconnectOptions:
    """Reconnect policy settings for automatic transport recovery."""

    enabled: bool = False
    max_attempts: int | float = float("inf")
    backoff_ms: int = 250
    max_backoff_ms: int = 5000


@dataclass(slots=True)
class ClientConfig:
    """Configuration used to construct a high-level Fitz client."""

    url: str
    token_provider: TokenProvider | None = None
    timeout_ms: int = 30000
    transport: TransportType = "auto"
    reconnect: ReconnectOptions | None = None
    max_frame_size: int = 65535
    auth_settle_delay_ms: int = 500
    max_in_flight_requests: int = 256
