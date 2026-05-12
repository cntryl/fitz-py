"""High-level Fitz SDK client and domain accessors."""

from __future__ import annotations

from fitz_py.connection import Connection
from fitz_py.domains.kv import KvClient
from fitz_py.domains.lease import LeaseClient
from fitz_py.domains.notice import NoticeClient
from fitz_py.domains.queue import QueueClient
from fitz_py.domains.rpc import RpcClient
from fitz_py.domains.schedule import ScheduleClient
from fitz_py.domains.stream import StreamClient
from fitz_py.errors import ConnectionError
from fitz_py.transport.factory import create_transport
from fitz_py.types import ClientConfig, ConnectionState, TokenProvider


class Client:
    """Top-level Fitz client that manages a connection and domain clients."""

    def __init__(self, config: ClientConfig) -> None:
        if not config.url:
            raise ValueError("url is required")

        self._config = ClientConfig(
            url=config.url,
            token_provider=config.token_provider,
            timeout_ms=config.timeout_ms,
            transport=config.transport,
            reconnect=config.reconnect,
            max_frame_size=config.max_frame_size,
            auth_settle_delay_ms=config.auth_settle_delay_ms,
        )
        self._connection: Connection | None = None
        self._kv_client: KvClient | None = None
        self._queue_client: QueueClient | None = None
        self._rpc_client: RpcClient | None = None
        self._lease_client: LeaseClient | None = None
        self._notice_client: NoticeClient | None = None
        self._stream_client: StreamClient | None = None
        self._schedule_client: ScheduleClient | None = None

    async def connect(self) -> None:
        if self._connection is not None and self._connection.is_connected():
            return

        token_provider = self._resolve_token_provider()
        reconnect = self._config.reconnect
        self._connection = Connection(
            lambda: create_transport(
                self._config.url,
                self._config.transport,
                timeout_ms=self._config.timeout_ms,
                max_frame_size=self._config.max_frame_size,
            ),
            token_provider,
            timeout_ms=self._config.timeout_ms,
            auth_settle_delay_ms=self._config.auth_settle_delay_ms,
            reconnect_enabled=reconnect.enabled if reconnect else False,
            reconnect_max_attempts=reconnect.max_attempts if reconnect else float("inf"),
            reconnect_backoff_ms=reconnect.backoff_ms if reconnect else 250,
            reconnect_max_backoff_ms=reconnect.max_backoff_ms if reconnect else 5000,
        )
        await self._connection.connect()

    async def __aenter__(self) -> "Client":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
        self._connection = None
        self._kv_client = None
        self._queue_client = None
        self._rpc_client = None
        self._lease_client = None
        self._notice_client = None
        self._stream_client = None
        self._schedule_client = None

    @property
    def state(self) -> ConnectionState:
        return (
            self._connection.get_state()
            if self._connection is not None
            else ConnectionState.DISCONNECTED
        )

    def kv(self) -> KvClient:
        if self._kv_client is None:
            self._kv_client = KvClient(self._ensure_connection())
        return self._kv_client

    def queue(self) -> QueueClient:
        if self._queue_client is None:
            self._queue_client = QueueClient(self._ensure_connection())
        return self._queue_client

    def rpc(self) -> RpcClient:
        if self._rpc_client is None:
            self._rpc_client = RpcClient(self._ensure_connection())
        return self._rpc_client

    def lease(self) -> LeaseClient:
        if self._lease_client is None:
            self._lease_client = LeaseClient(self._ensure_connection())
        return self._lease_client

    def notice(self) -> NoticeClient:
        if self._notice_client is None:
            self._notice_client = NoticeClient(self._ensure_connection())
        return self._notice_client

    def stream(self) -> StreamClient:
        if self._stream_client is None:
            self._stream_client = StreamClient(self._ensure_connection())
        return self._stream_client

    def schedule(self) -> ScheduleClient:
        if self._schedule_client is None:
            self._schedule_client = ScheduleClient(self._ensure_connection())
        return self._schedule_client

    def _resolve_token_provider(self) -> TokenProvider:
        return self._config.token_provider or (lambda: "")

    def _ensure_connection(self) -> Connection:
        if self._connection is None:
            raise ConnectionError("Not connected to Fitz server. Call connect() first.")
        return self._connection
