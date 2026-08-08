"""Streaming RPC calls and worker registrations."""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass

from fitz_py.domains.base import DomainClient
from fitz_py.errors import FitzConnectionError, FitzTimeoutError, RpcError, domain_error
from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.messages import (
    MSG_RPC_REQUEST,
    MSG_RPC_RESPONSE,
    MSG_RPC_SUBSCRIBE_WORKER,
    MSG_RPC_UNSUBSCRIBE_WORKER,
)
from fitz_py.protocol.response import parse_response


@dataclass(frozen=True, slots=True)
class ResponseFrame:
    body: bytes
    sequence: int


@dataclass(frozen=True, slots=True)
class InboundRequest:
    route: str
    body: bytes


class ResponseWriter:
    def __init__(self, connection, correlation_id: bytes) -> None:
        self._connection = connection
        self._correlation_id = correlation_id
        self._sequence = 0
        self._generation = connection.generation
        self._ended = False

    async def send(self, body: bytes, *, end: bool = False) -> None:
        if self._ended or self._generation != self._connection.generation:
            raise FitzConnectionError("RPC response writer is stale")
        writer = BufferWriter()
        writer.write_bytes(self._correlation_id)
        writer.write_u64_be(self._sequence)
        writer.write_u8(int(end))
        writer.write_u32_be(len(body))
        writer.write_bytes(body)
        self._sequence += 1
        await self._connection.send(MSG_RPC_RESPONSE, writer.build())
        self._ended = end


class RpcCall(AsyncIterator[ResponseFrame]):
    def __init__(self, client: RpcClient, key: bytes, timeout: float, capacity: int) -> None:
        self._client = client
        self._key = key
        self._timeout = timeout
        self._queue: asyncio.Queue[ResponseFrame | BaseException | None] = asyncio.Queue(capacity)
        self._closed = False

    def __aiter__(self) -> RpcCall:
        return self

    async def __anext__(self) -> ResponseFrame:
        if self._closed and self._queue.empty():
            raise StopAsyncIteration
        try:
            async with asyncio.timeout(self._timeout):
                item = await self._queue.get()
        except asyncio.CancelledError:
            await self.aclose()
            raise
        except TimeoutError as exc:
            await self.aclose()
            raise FitzTimeoutError("RPC response timed out") from exc
        if item is None:
            self._closed = True
            raise StopAsyncIteration
        if isinstance(item, BaseException):
            self._closed = True
            raise item
        return item

    async def __aenter__(self) -> RpcCall:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            self._client._pending.pop(self._key, None)

    def push(self, frame: ResponseFrame, end: bool) -> None:
        if self._closed:
            return
        try:
            if frame.body:
                self._queue.put_nowait(frame)
            if end:
                self._queue.put_nowait(None)
                self._client._pending.pop(self._key, None)
        except asyncio.QueueFull:
            self.fail(RpcError("RPC response consumer fell behind", "BACKPRESSURE"))

    def fail(self, error: BaseException) -> None:
        if self._closed:
            return
        self._closed = True
        while not self._queue.empty():
            with contextlib.suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()
        self._queue.put_nowait(error)


RpcHandler = Callable[[InboundRequest, ResponseWriter], Awaitable[None]]


@dataclass(slots=True)
class Worker:
    route: str
    _client: RpcClient
    _identity: object

    async def unsubscribe(self) -> None:
        await self._client._unregister(self.route, self._identity)

    async def aclose(self) -> None:
        await self.unsubscribe()


@dataclass(frozen=True, slots=True)
class _Registration:
    handler: RpcHandler
    max_concurrency: int
    identity: object


class RpcClient(DomainClient):
    def __init__(self, connection) -> None:
        super().__init__(connection)
        self._pending: dict[bytes, RpcCall] = {}
        self._workers: dict[str, _Registration] = {}
        connection.register_push_classifier(MSG_RPC_REQUEST, _looks_like_request)
        connection.register_push_classifier(MSG_RPC_RESPONSE, _looks_like_response)
        connection.register_notification_handler(MSG_RPC_REQUEST, self._on_request)
        connection.register_notification_handler(MSG_RPC_RESPONSE, self._on_response)
        connection.on_disconnect(self._disconnect)
        connection.on_reconnect(self._restore, domain="rpc", registration="workers")

    async def call(self, route: str, body: bytes, *, timeout: float = 30.0) -> RpcCall:
        _route(route, patterns=False)
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        correlation_id = os.urandom(16)
        call = RpcCall(
            self,
            correlation_id,
            timeout,
            self.connection.config.limits.subscription_buffer_size,
        )
        self._pending[correlation_id] = call
        writer = BufferWriter()
        writer.write_bytes(correlation_id)
        writer.write_route(route)
        writer.write_u32_be(len(body))
        writer.write_bytes(body)
        try:
            await self.connection.send(MSG_RPC_REQUEST, writer.build())
        except BaseException:
            self._pending.pop(correlation_id, None)
            raise
        return call

    async def register_worker(
        self, route: str, handler: RpcHandler, *, max_concurrency: int = 1
    ) -> Worker:
        _route(route, patterns=True)
        if not 1 <= max_concurrency <= 1024:
            raise ValueError("max_concurrency must be between 1 and 1024")
        identity = object()
        registration = _Registration(handler, max_concurrency, identity)
        await self._subscribe(route, registration)
        self._workers[route] = registration
        return Worker(route, self, identity)

    async def _subscribe(self, route: str, registration: _Registration) -> None:
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_u32_be(registration.max_concurrency)
        response = parse_response(
            await self.request_frame(MSG_RPC_SUBSCRIBE_WORKER, writer.build())
        )
        if not response.success:
            raise domain_error(RpcError, "SUBSCRIBE", response.error_code or 0, response.error)

    async def _unregister(self, route: str, identity: object) -> None:
        registration = self._workers.get(route)
        if registration is None or registration.identity is not identity:
            return
        writer = BufferWriter()
        writer.write_route(route)
        response = parse_response(
            await self.request_frame(MSG_RPC_UNSUBSCRIBE_WORKER, writer.build())
        )
        if not response.success:
            raise domain_error(RpcError, "UNSUBSCRIBE", response.error_code or 0, response.error)
        self._workers.pop(route, None)

    def _on_response(self, payload: bytes) -> None:
        reader = BufferReader(payload)
        key = reader.read_bytes(16)
        sequence = reader.read_u64_be()
        flags = reader.read_u8()
        if flags & ~1:
            raise RpcError("Unsupported RPC response flags", "INVALID_RESPONSE")
        body = reader.read_bytes(reader.read_u32_be())
        if not reader.is_eof():
            raise RpcError("RPC response has trailing bytes", "INVALID_RESPONSE")
        call = self._pending.get(key)
        if call is None:
            return
        if flags & 1 and body[:1] == b"\x01":
            error = parse_response(body)
            if (
                not error.success
                and error.error_code is not None
                and 6001 <= error.error_code <= 6013
            ):
                call.fail(domain_error(RpcError, "CALL", error.error_code, error.error))
                self._pending.pop(key, None)
                return
        call.push(ResponseFrame(body, sequence), bool(flags & 1))

    def _on_request(self, payload: bytes) -> None:
        reader = BufferReader(payload)
        correlation_id = reader.read_bytes(16)
        route = reader.read_route()
        body = reader.read_bytes(reader.read_u32_be())
        registration = self._best_worker(route)
        if registration is None:
            return
        request = InboundRequest(route, body)
        response = ResponseWriter(self.connection, correlation_id)
        self.connection.dispatch_async(lambda: registration.handler(request, response))

    def _best_worker(self, route: str) -> _Registration | None:
        matches = [
            (pattern, worker)
            for pattern, worker in self._workers.items()
            if _matches(route, pattern)
        ]
        if not matches:
            return None
        return max(matches, key=lambda pair: _specificity(pair[0]))[1]

    async def _restore(self) -> None:
        for route, registration in list(self._workers.items()):
            try:
                await self._subscribe(route, registration)
            except BaseException as exc:
                self._workers.pop(route, None)
                self.connection.report_restore_failure("rpc", route, exc)

    def _disconnect(self) -> None:
        for call in tuple(self._pending.values()):
            call.fail(FitzConnectionError("Connection closed while RPC response was pending"))
        self._pending.clear()


def _route(route: str, *, patterns: bool) -> None:
    if not route.startswith("rpc://"):
        raise RpcError(f"Invalid RPC route: {route}", "INVALID_ROUTE")
    parts = route[6:].split("/")
    invalid = not parts or any(not part for part in parts)
    invalid |= not patterns and any("*" in part for part in parts)
    invalid |= any("*" in part and part not in {"*", "**"} for part in parts)
    invalid |= "**" in parts and parts[-1] != "**"
    if invalid:
        raise RpcError(f"Invalid RPC route: {route}", "INVALID_ROUTE")


def _matches(route: str, pattern: str) -> bool:
    route_parts, pattern_parts = route[6:].split("/"), pattern[6:].split("/")
    for index, part in enumerate(pattern_parts):
        if part == "**":
            return True
        if index >= len(route_parts) or part not in {"*", route_parts[index]}:
            return False
    return len(route_parts) == len(pattern_parts)


def _specificity(pattern: str) -> tuple[int, int, int, int]:
    parts = pattern[6:].split("/")
    return (
        sum(p not in {"*", "**"} for p in parts),
        -parts.count("*"),
        -parts.count("**"),
        len(parts),
    )


def _looks_like_request(payload: bytes) -> bool:
    try:
        reader = BufferReader(payload)
        reader.read_bytes(16)
        reader.read_route()
        reader.read_bytes(reader.read_u32_be())
        return reader.is_eof()
    except BaseException:
        return False


def _looks_like_response(payload: bytes) -> bool:
    try:
        reader = BufferReader(payload)
        reader.read_bytes(16)
        reader.read_u64_be()
        flags = reader.read_u8()
        reader.read_bytes(reader.read_u32_be())
        return flags & ~1 == 0 and reader.is_eof()
    except BaseException:
        return False
