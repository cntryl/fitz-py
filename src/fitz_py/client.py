"""High-level asynchronous Fitz client facade."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Mapping

from fitz_py.connection import Connection
from fitz_py.domains.kv import KVClient
from fitz_py.domains.lease import LeaseClient
from fitz_py.domains.notice import NoticeClient
from fitz_py.domains.queue import QueueClient
from fitz_py.domains.rpc import RPCClient
from fitz_py.domains.schedule import ScheduleClient
from fitz_py.domains.stream import StreamClient
from fitz_py.errors import AuthenticationError, FitzConnectionError, FitzTransportError
from fitz_py.transport.factory import create_transport
from fitz_py.types import (
    ClientConfig,
    ConcurrencyLimits,
    ConnectionState,
    HeartbeatPolicy,
    Observability,
    ReconnectPolicy,
    RetryPolicy,
    TokenProvider,
    TransportType,
)


class Client:
    def __init__(
        self,
        url: str,
        *,
        token_provider: TokenProvider | None = None,
        transport: TransportType | str = TransportType.AUTO,
        reconnect: ReconnectPolicy | None = None,
        limits: ConcurrencyLimits | None = None,
        request_timeout: float = 30.0,
        auth_settle_timeout: float = 1.0,
        max_frame_size: int = 1_048_576,
        retry: RetryPolicy | None = None,
        heartbeat: HeartbeatPolicy | None = None,
        observability: Observability | None = None,
        websocket_headers: Mapping[str, str] | None = None,
    ) -> None:
        if not isinstance(url, str):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError("url must be a string; pass Client options as keyword arguments")
        config = ClientConfig(
            url=url,
            token_provider=token_provider,
            transport=transport,
            request_timeout=request_timeout,
            auth_settle_timeout=auth_settle_timeout,
            max_frame_size=max_frame_size,
            reconnect=ReconnectPolicy() if reconnect is None else reconnect,
            retry=RetryPolicy() if retry is None else retry,
            heartbeat=HeartbeatPolicy() if heartbeat is None else heartbeat,
            limits=ConcurrencyLimits() if limits is None else limits,
            observability=Observability() if observability is None else observability,
            websocket_headers={} if websocket_headers is None else websocket_headers,
        )
        self.config = config
        self._closed = False
        self._connection = Connection(
            lambda: create_transport(
                config.url,
                TransportType(config.transport),
                timeout_ms=int(config.request_timeout * 1000),
                max_frame_size=config.max_frame_size,
                websocket_headers=dict(config.websocket_headers),
            ),
            config,
        )
        self._kv = KVClient(self._connection)
        self._queue = QueueClient(self._connection)
        self._rpc = RPCClient(self._connection)
        self._lease = LeaseClient(self._connection)
        self._notice = NoticeClient(self._connection)
        self._stream = StreamClient(self._connection)
        self._schedule = ScheduleClient(self._connection)

    async def __aenter__(self) -> Client:
        await self.connect()
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()

    async def connect(self) -> None:
        if self._closed:
            raise FitzConnectionError("Client is closed")
        await self._connection.connect()

    async def connect_when_ready(
        self,
        *,
        timeout: float | None = None,
        backoff: float = 0.25,
        max_backoff: float = 2.0,
    ) -> None:
        if self._closed:
            raise FitzConnectionError("Client is closed")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive or None")
        if backoff <= 0 or max_backoff < backoff:
            raise ValueError("backoff must be positive and no greater than max_backoff")
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        delay = backoff
        while True:
            try:
                await self.connect()
                return  # noqa: TRY300
            except AuthenticationError:
                raise
            except (FitzTransportError, FitzConnectionError):
                if deadline is not None and loop.time() >= deadline:
                    raise
                sleep_for = delay * random.uniform(0.8, 1.2)
                if deadline is not None:
                    sleep_for = min(sleep_for, max(0, deadline - loop.time()))
                await asyncio.sleep(sleep_for)
                delay = min(delay * 2, max_backoff)

    async def aclose(self) -> None:
        self._closed = True
        await self._connection.close()

    @property
    def state(self) -> ConnectionState:
        return self._connection.get_state()

    @property
    def is_connected(self) -> bool:
        return self._connection.is_connected()

    @property
    def url(self) -> str:
        return self.config.url

    @property
    def kv(self) -> KVClient:
        return self._kv

    @property
    def queue(self) -> QueueClient:
        return self._queue

    @property
    def rpc(self) -> RPCClient:
        return self._rpc

    @property
    def lease(self) -> LeaseClient:
        return self._lease

    @property
    def notice(self) -> NoticeClient:
        return self._notice

    @property
    def stream(self) -> StreamClient:
        return self._stream

    @property
    def schedule(self) -> ScheduleClient:
        return self._schedule
