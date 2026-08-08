"""High-level asynchronous Fitz client facade."""

from __future__ import annotations

import asyncio
import random

from fitz_py.connection import Connection
from fitz_py.domains.kv import KvClient
from fitz_py.domains.lease import LeaseClient
from fitz_py.domains.notice import NoticeClient
from fitz_py.domains.queue import QueueClient
from fitz_py.domains.rpc import RpcClient
from fitz_py.domains.schedule import ScheduleClient
from fitz_py.domains.stream import StreamClient
from fitz_py.errors import AuthenticationError, FitzConnectionError, FitzTransportError
from fitz_py.transport.factory import create_transport
from fitz_py.types import ClientConfig, ConnectionState, TransportType


class Client:
    def __init__(self, config: ClientConfig) -> None:
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
        self._kv = KvClient(self._connection)
        self._queue = QueueClient(self._connection)
        self._rpc = RpcClient(self._connection)
        self._lease = LeaseClient(self._connection)
        self._notice = NoticeClient(self._connection)
        self._stream = StreamClient(self._connection)
        self._schedule = ScheduleClient(self._connection)

    async def __aenter__(self) -> Client:
        await self.connect()
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()

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
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        delay = backoff
        while True:
            try:
                await self.connect()
                return
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

    async def close(self) -> None:
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
    def kv(self) -> KvClient:
        return self._kv

    @property
    def queue(self) -> QueueClient:
        return self._queue

    @property
    def rpc(self) -> RpcClient:
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
