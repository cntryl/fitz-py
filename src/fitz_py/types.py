"""Public configuration, lifecycle, and observability contracts."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol, TypeAlias

TokenProvider: TypeAlias = Callable[[], str | bytes | Awaitable[str | bytes]]


class TransportType(StrEnum):
    AUTO = "auto"
    TCP = "tcp"
    WEBSOCKET = "ws"


class ConnectionState(StrEnum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    AUTHENTICATING = "AUTHENTICATING"
    AUTHENTICATED = "AUTHENTICATED"
    RECONNECTING = "RECONNECTING"
    CLOSED = "CLOSED"


@dataclass(frozen=True, slots=True)
class ReconnectPolicy:
    enabled: bool = True
    max_attempts: int | None = None
    backoff: float = 0.25
    max_backoff: float = 5.0


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    enabled: bool = True
    max_attempts: int = 3
    backoff: float = 0.1
    max_backoff: float = 1.0


@dataclass(frozen=True, slots=True)
class HeartbeatPolicy:
    enabled: bool = True
    interval: float = 10.0
    timeout: float = 30.0


@dataclass(frozen=True, slots=True)
class ConcurrencyLimits:
    max_in_flight: int = 256
    request_queue_size: int = 1024
    async_handler_concurrency: int = 256
    async_handler_queue_size: int = 1024
    subscription_buffer_size: int = 1024
    async_handler_timeout: float = 30.0


@dataclass(frozen=True, slots=True)
class LifecycleEvent:
    event: str
    state: ConnectionState
    url: str
    transport: str | None = None
    attempt: int | None = None
    domain: str | None = None
    registration: str | None = None
    error: str | None = None


class FitzTracer(Protocol):
    def start_span(self, name: str, attributes: Mapping[str, Any]) -> Any: ...


class FitzMeter(Protocol):
    def counter(self, name: str, value: int, attributes: Mapping[str, Any]) -> None: ...

    def histogram(self, name: str, value: float, attributes: Mapping[str, Any]) -> None: ...

    def gauge(self, name: str, value: int, attributes: Mapping[str, Any]) -> None: ...


@dataclass(frozen=True, slots=True)
class Observability:
    logger: Any | None = None
    tracer: FitzTracer | None = None
    meter: FitzMeter | None = None
    on_lifecycle_event: Callable[[LifecycleEvent], None] | None = None


@dataclass(frozen=True, slots=True)
class ClientConfig:
    url: str
    token_provider: TokenProvider | None = None
    transport: TransportType | str = TransportType.AUTO
    request_timeout: float = 30.0
    auth_settle_timeout: float = 1.0
    max_frame_size: int = 65_540
    reconnect: ReconnectPolicy = field(default_factory=ReconnectPolicy)
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    heartbeat: HeartbeatPolicy = field(default_factory=HeartbeatPolicy)
    limits: ConcurrencyLimits = field(default_factory=ConcurrencyLimits)
    observability: Observability = field(default_factory=Observability)
    websocket_headers: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.url:
            raise ValueError("url is required")
        if self.request_timeout <= 0 or self.auth_settle_timeout < 0:
            raise ValueError("timeouts must be positive")
        if self.max_frame_size < 5:
            raise ValueError("max_frame_size must include the five-byte frame header")
        if self.reconnect.max_attempts is not None and self.reconnect.max_attempts < 0:
            raise ValueError("reconnect.max_attempts must be non-negative or None")
        if self.retry.max_attempts < 1:
            raise ValueError("retry.max_attempts must be at least 1")
        if self.limits.max_in_flight < 1:
            raise ValueError("limits.max_in_flight must be at least 1")
        if self.limits.request_queue_size < 0:
            raise ValueError("limits.request_queue_size must be non-negative")
        if self.limits.async_handler_concurrency < 1:
            raise ValueError("limits.async_handler_concurrency must be at least 1")
        if self.limits.async_handler_queue_size < 0:
            raise ValueError("limits.async_handler_queue_size must be non-negative")
        if self.limits.subscription_buffer_size < 1:
            raise ValueError("limits.subscription_buffer_size must be at least 1")


# Short aliases used in annotations and migration docs.
ReconnectOptions = ReconnectPolicy
