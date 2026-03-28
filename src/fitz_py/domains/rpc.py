from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass

from fitz_py.domains.base import DomainClient
from fitz_py.errors import ConnectionError, ErrRpcTimeout, TransportError, rpc_error
from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.messages import (
    MSG_RPC_ACK,
    MSG_RPC_REQUEST,
    MSG_RPC_RESPONSE,
    MSG_RPC_SUBSCRIBE_WORKER,
    MSG_RPC_UNSUBSCRIBE_WORKER,
)
from fitz_py.types import ConnectionState

RpcHandler = Callable[["InboundRpcRequest", "ResponseWriter"], None | Awaitable[None]]


@dataclass(slots=True)
class ResponseFrame:
    body: bytes
    sequence: int


@dataclass(slots=True)
class InboundRpcRequest:
    correlation_id: bytes
    route: str
    reply_route: str
    body: bytes


class ResponseWriter:
    def __init__(self, connection, correlation_id: bytes) -> None:
        self._connection = connection
        self._correlation_id = correlation_id
        self._sequence = 0

    async def send(self, body: bytes, is_end: bool) -> None:
        writer = BufferWriter()
        writer.write_u32_be(len(self._correlation_id))
        writer.write_bytes(self._correlation_id)
        writer.write_u64_be(self._sequence)
        self._sequence += 1
        writer.write_u32_be(len(body))
        writer.write_bytes(body)
        writer.write_u8(1 if is_end else 0)
        try:
            await self._connection.send(MSG_RPC_RESPONSE, writer.build())
        except Exception as exc:
            if _is_benign_shutdown_error(exc, self._connection):
                return
            raise


class RpcSubscription:
    def __init__(self, route: str, unsubscribe: Callable[[str], Awaitable[None]]) -> None:
        self.route = route
        self._unsubscribe = unsubscribe

    async def unsubscribe(self) -> None:
        await self._unsubscribe(self.route)


class RpcIterator(AsyncIterator[ResponseFrame]):
    def __init__(self, correlation_id: bytes, client: "RpcClient", timeout_ms: int) -> None:
        self._correlation_id = correlation_id
        self._client = client
        self._timeout_ms = timeout_ms
        self._buffer: list[ResponseFrame] = []
        self._done = False
        self._waiter: asyncio.Future[ResponseFrame | None] | None = None

    def push(self, frame: ResponseFrame) -> None:
        if self._waiter is not None and not self._waiter.done():
            self._waiter.set_result(frame)
            self._waiter = None
            return
        self._buffer.append(frame)

    def end(self) -> None:
        self._done = True
        if self._waiter is not None and not self._waiter.done():
            self._waiter.set_result(None)
            self._waiter = None

    async def __anext__(self) -> ResponseFrame:
        if self._buffer:
            return self._buffer.pop(0)
        if self._done:
            raise StopAsyncIteration
        self._waiter = asyncio.get_running_loop().create_future()
        try:
            frame = await asyncio.wait_for(self._waiter, timeout=self._timeout_ms / 1000)
        except TimeoutError as exc:
            self._client.cleanup_pending_rpc(self._correlation_id)
            self._done = True
            raise ErrRpcTimeout("RPC call timeout") from exc
        if frame is None:
            raise StopAsyncIteration
        return frame

    async def aclose(self) -> None:
        self._done = True
        self._client.cleanup_pending_rpc(self._correlation_id)


class RpcClient(DomainClient):
    def __init__(self, connection) -> None:
        super().__init__(connection)
        self._pending: dict[str, RpcIterator] = {}
        self._workers: dict[str, RpcHandler] = {}
        self._initialized = False
        self.connection.on_reconnect(self._restore_workers)

    async def call(self, route: str, body: bytes, *, timeout_ms: int = 30000) -> RpcIterator:
        self._init_handlers()
        correlation_id = os.urandom(16)
        iterator = RpcIterator(correlation_id, self, timeout_ms)
        self._pending[correlation_id.hex()] = iterator

        writer = BufferWriter()
        writer.write_u32_be(len(correlation_id))
        writer.write_bytes(correlation_id)
        writer.write_route(route)
        writer.write_route("")
        writer.write_u32_be(len(body))
        writer.write_bytes(body)
        try:
            reader = BufferReader(await self.request_frame(MSG_RPC_REQUEST, writer.build()))
            status = reader.read_u8()
            if status != 0:
                self._pending.pop(correlation_id.hex(), None)
                raise rpc_error(f"REQUEST failed with status {status}", status)
            return iterator
        except Exception:
            self._pending.pop(correlation_id.hex(), None)
            raise

    async def register_worker(self, route: str, handler: RpcHandler) -> RpcSubscription:
        self._init_handlers()
        writer = BufferWriter()
        writer.write_route(route)
        reader = BufferReader(await self.request_frame(MSG_RPC_SUBSCRIBE_WORKER, writer.build()))
        status = reader.read_u8()
        if status != 0:
            raise rpc_error(f"REGISTER_WORKER failed with status {status}", status)
        self._workers[route] = handler
        return RpcSubscription(route, self._unregister_worker)

    def cleanup_pending_rpc(self, correlation_id: bytes) -> None:
        self._pending.pop(correlation_id.hex(), None)

    async def _unregister_worker(self, route: str) -> None:
        self._workers.pop(route, None)
        writer = BufferWriter()
        writer.write_route(route)
        try:
            await self.request_frame(MSG_RPC_UNSUBSCRIBE_WORKER, writer.build())
        except Exception:
            return

    def _init_handlers(self) -> None:
        if self._initialized:
            return
        self._initialized = True

        def response_handler(payload: bytes) -> None:
            try:
                reader = BufferReader(payload)
                corr_len = reader.read_u32_be()
                correlation_id = reader.read_bytes(corr_len)
                sequence = reader.read_u64_be()
                body = reader.read_bytes(reader.read_u32_be())
                stream_end = not reader.is_eof() and reader.read_u8() == 1
                iterator = self._pending.get(correlation_id.hex())
                if iterator is None:
                    return
                if body:
                    iterator.push(ResponseFrame(body=body, sequence=sequence))
                if stream_end:
                    self._pending.pop(correlation_id.hex(), None)
                    iterator.end()
            except Exception:
                return

        def request_handler(payload: bytes) -> None:
            try:
                reader = BufferReader(payload)
                corr_len = reader.read_u32_be()
                correlation_id = reader.read_bytes(corr_len)
                route = reader.read_route()
                reply_route = reader.read_route()
                body = reader.read_bytes(reader.read_u32_be())
                handler = self._workers.get(route)
                if handler is None:
                    return
                request = InboundRpcRequest(
                    correlation_id=correlation_id,
                    route=route,
                    reply_route=reply_route,
                    body=body,
                )
                response_writer = ResponseWriter(self.connection, correlation_id)
                result = handler(request, response_writer)
                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception:
                return

        self.connection.register_notification_handler(MSG_RPC_RESPONSE, response_handler)
        self.connection.register_notification_handler(MSG_RPC_REQUEST, request_handler)
        self.connection.register_notification_handler(MSG_RPC_ACK, lambda _payload: None)

    async def _restore_workers(self) -> None:
        if not self._workers:
            return
        snapshot = list(self._workers.items())
        self._workers.clear()
        for route, handler in snapshot:
            await self.register_worker(route, handler)


def _is_benign_shutdown_error(error: Exception, connection) -> bool:
    if connection.get_state() is not ConnectionState.AUTHENTICATED:
        return True
    if isinstance(error, ConnectionError):
        return True
    if not isinstance(error, TransportError):
        return False
    lowered = str(error).lower()
    return "closed" in lowered or "not connected" in lowered or "reset" in lowered
