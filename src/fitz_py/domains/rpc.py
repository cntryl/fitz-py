"""Streaming RPC calls and worker registrations."""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass

from fitz_py._runtime import LazyAsyncContext, LazyAsyncIterator
from fitz_py.connection import Connection
from fitz_py.domains.base import DomainClient
from fitz_py.errors import (
    CodecError,
    FitzConnectionError,
    FitzTimeoutError,
    ProtocolError,
    RPCError,
    domain_error,
)
from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.messages import (
    MSG_RPC_REQUEST,
    MSG_RPC_RESPONSE,
    MSG_RPC_SUBSCRIBE_WORKER,
    MSG_RPC_UNSUBSCRIBE_WORKER,
)
from fitz_py.protocol.response import parse_response
from fitz_py.types import BytesLike


@dataclass(frozen=True, slots=True)
class ResponseFrame:
    body: bytes
    sequence: int


@dataclass(frozen=True, slots=True)
class InboundRequest:
    route: str
    body: bytes


class ResponseWriter:
    def __init__(self, connection: Connection, correlation_id: bytes) -> None:
        self._connection = connection
        self._correlation_id = correlation_id
        self._sequence = 0
        self._generation = connection.generation
        self._ended = False
        self._lock = asyncio.Lock()

    async def send(self, body: BytesLike, *, end: bool = False) -> None:
        async with self._lock:
            if self._ended or self._generation != self._connection.generation:
                raise FitzConnectionError("RPC response writer is stale")
            body = bytes(body)
            writer = BufferWriter()
            writer.write_bytes(self._correlation_id)
            writer.write_u64_be(self._sequence)
            writer.write_u8(int(end))
            writer.write_u32_be(len(body))
            writer.write_bytes(body)
            await self._connection.send(MSG_RPC_RESPONSE, writer.build())
            self._sequence += 1
            self._ended = end


class RPCCall(AsyncIterator[ResponseFrame]):
    def __init__(self, client: RPCClient, key: bytes, timeout: float, capacity: int) -> None:
        self._client = client
        self._key = key
        self._timeout = timeout
        self._queue: asyncio.Queue[ResponseFrame | BaseException | None] = asyncio.Queue(capacity)
        self._closed = False
        self._terminal = False
        self._failure: BaseException | None = None

    def __aiter__(self) -> RPCCall:
        return self

    async def __anext__(self) -> ResponseFrame:
        if self._failure is not None and self._queue.empty():
            failure, self._failure = self._failure, None
            raise failure
        if (self._closed or self._terminal) and self._queue.empty():
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
            self._failure = None
            raise item
        return item

    async def __aenter__(self) -> RPCCall:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            self._client._pending.pop(self._key, None)  # noqa: SLF001
            while not self._queue.empty():
                with contextlib.suppress(asyncio.QueueEmpty):
                    self._queue.get_nowait()
            self._queue.put_nowait(None)

    def push(self, frame: ResponseFrame, end: bool) -> None:
        if self._closed:
            return
        try:
            self._queue.put_nowait(frame)
            if end:
                self._terminal = True
                self._client._pending.pop(self._key, None)  # noqa: SLF001
        except asyncio.QueueFull:
            self.fail(
                RPCError("RPC response consumer fell behind", "BACKPRESSURE"),
                preserve_buffered=False,
            )

    def fail(self, error: BaseException, *, preserve_buffered: bool = True) -> None:
        if self._closed:
            return
        self._closed = True
        self._client._pending.pop(self._key, None)  # noqa: SLF001
        if not preserve_buffered:
            while not self._queue.empty():
                with contextlib.suppress(asyncio.QueueEmpty):
                    self._queue.get_nowait()
        self._failure = error
        try:
            self._queue.put_nowait(error)
            self._failure = None
        except asyncio.QueueFull:
            pass


RPCHandler = Callable[[InboundRequest, ResponseWriter], Awaitable[None]]


@dataclass(slots=True)
class Worker:
    route: str
    _client: RPCClient
    _identity: object

    async def unsubscribe(self) -> None:
        await self._client._unregister(self.route, self._identity)  # noqa: SLF001

    async def aclose(self) -> None:
        await self.unsubscribe()

    async def __aenter__(self) -> Worker:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()


@dataclass(frozen=True, slots=True)
class _Registration:
    handler: RPCHandler
    max_concurrency: int
    identity: object
    semaphore: asyncio.Semaphore


class RPCClient(DomainClient):
    def __init__(self, connection: Connection) -> None:
        super().__init__(connection)
        self._pending: dict[bytes, RPCCall] = {}
        self._workers: dict[str, _Registration] = {}
        self._worker_locks: dict[str, asyncio.Lock] = {}
        self._terminal_tasks: set[asyncio.Task[None]] = set()
        connection.register_push_classifier(MSG_RPC_REQUEST, _looks_like_request)
        connection.register_push_classifier(MSG_RPC_RESPONSE, _looks_like_response)
        connection.register_notification_handler(MSG_RPC_REQUEST, self._on_request)
        connection.register_notification_handler(MSG_RPC_RESPONSE, self._on_response)
        connection.on_disconnect(self._disconnect)
        connection.on_reconnect(self._restore, domain="rpc", registration="workers")

    def call(
        self, route: str, body: BytesLike, *, timeout: float = 30.0
    ) -> LazyAsyncIterator[ResponseFrame]:
        _route(route, patterns=False)
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        return LazyAsyncIterator(lambda: self.open_call(route, body, timeout=timeout))

    async def open_call(self, route: str, body: BytesLike, *, timeout: float = 30.0) -> RPCCall:
        _route(route, patterns=False)
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        body = bytes(body)
        correlation_id = os.urandom(16)
        call = RPCCall(
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

    def worker(
        self, route: str, handler: RPCHandler, *, max_concurrency: int = 1
    ) -> LazyAsyncContext[Worker]:
        return LazyAsyncContext(
            lambda: self.register_worker(route, handler, max_concurrency=max_concurrency),
            lambda worker: worker.aclose(),
        )

    async def register_worker(
        self, route: str, handler: RPCHandler, *, max_concurrency: int = 1
    ) -> Worker:
        _route(route, patterns=True)
        if not 1 <= max_concurrency <= 1024:
            raise ValueError("max_concurrency must be between 1 and 1024")
        identity = object()
        registration = _Registration(
            handler,
            max_concurrency,
            identity,
            asyncio.Semaphore(max_concurrency),
        )
        async with self._worker_lock(route):
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
            raise domain_error(RPCError, "SUBSCRIBE", response.error_code or 0, response.error)
        _expect_empty_rpc_success(response.data, "SUBSCRIBE")

    async def _unregister(self, route: str, identity: object) -> None:
        async with self._worker_lock(route):
            registration = self._workers.get(route)
            if registration is None or registration.identity is not identity:
                return
            writer = BufferWriter()
            writer.write_route(route)
            response = parse_response(
                await self.request_frame(MSG_RPC_UNSUBSCRIBE_WORKER, writer.build())
            )
            if not response.success:
                raise domain_error(
                    RPCError, "UNSUBSCRIBE", response.error_code or 0, response.error
                )
            _expect_empty_rpc_success(response.data, "UNSUBSCRIBE")
            if self._workers.get(route) is registration:
                self._workers.pop(route, None)

    def _on_response(self, payload: bytes) -> None:
        reader = BufferReader(payload)
        key = reader.read_bytes(16)
        sequence = reader.read_u64_be()
        flags = reader.read_u8()
        if flags & ~1:
            raise RPCError("Unsupported RPC response flags", "INVALID_RESPONSE")
        body = reader.read_bytes(reader.read_u32_be())
        if not reader.is_eof():
            raise RPCError("RPC response has trailing bytes", "INVALID_RESPONSE")
        call = self._pending.get(key)
        if call is None:
            return
        if flags & 1:
            error = _decode_terminal_error(body)
            if error is not None:
                call.fail(error)
                self._pending.pop(key, None)
                return
        call.push(ResponseFrame(body, sequence), bool(flags & 1))

    def _on_request(self, payload: bytes) -> None:
        reader = BufferReader(payload)
        correlation_id = reader.read_bytes(16)
        route = reader.read_route()
        body = reader.read_bytes(reader.read_u32_be())
        if not reader.is_eof():
            raise RPCError("RPC request has trailing bytes", "INVALID_RESPONSE")
        registration = self._best_worker(route)
        if registration is None:
            return
        request = InboundRequest(route, body)
        response = ResponseWriter(self.connection, correlation_id)
        if not self.connection.dispatch_async(
            lambda: self._run_worker(registration, request, response)
        ):
            self._schedule_terminal(response, 6003, "Worker is locally saturated")

    @staticmethod
    async def _run_worker(
        registration: _Registration,
        request: InboundRequest,
        response: ResponseWriter,
    ) -> None:
        async with registration.semaphore:
            try:
                await registration.handler(request, response)
            except asyncio.CancelledError:
                if not response._ended:  # noqa: SLF001
                    with contextlib.suppress(Exception):
                        await RPCClient._send_error(response, 6010, "Worker handler cancelled")
                raise
            except Exception as exc:  # noqa: BLE001
                if response._ended:  # noqa: SLF001
                    return
                await RPCClient._send_error(response, 6010, str(exc) or type(exc).__name__)

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
            async with self._worker_lock(route):
                if self._workers.get(route) is not registration:
                    continue
                try:
                    await self._subscribe(route, registration)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    if self._workers.get(route) is registration:
                        self._workers.pop(route, None)
                    self.connection.report_restore_failure("rpc", route, exc)

    def _disconnect(self) -> None:
        for call in tuple(self._pending.values()):
            call.fail(FitzConnectionError("Connection closed while RPC response was pending"))
        self._pending.clear()

    def _worker_lock(self, route: str) -> asyncio.Lock:
        return self._worker_locks.setdefault(route, asyncio.Lock())

    def _schedule_terminal(self, response: ResponseWriter, code: int, message: str) -> None:
        task = asyncio.create_task(self._send_error(response, code, message))
        self._terminal_tasks.add(task)
        task.add_done_callback(self._terminal_completed)

    def _terminal_completed(self, task: asyncio.Task[None]) -> None:
        self._terminal_tasks.discard(task)
        if not task.cancelled():
            task.exception()

    @staticmethod
    async def _send_error(response: ResponseWriter, code: int, message: str) -> None:
        error = BufferWriter()
        error.write_u8(1)
        error.write_u32_be(code)
        error.write_string(message)
        await response.send(error.build(), end=True)


def _route(route: str, *, patterns: bool) -> None:
    if not route.startswith("rpc://"):
        raise RPCError(f"Invalid RPC route: {route}", "INVALID_ROUTE")
    parts = route[6:].split("/")
    invalid = not parts or any(not part for part in parts)
    invalid |= not patterns and any("*" in part for part in parts)
    invalid |= any("*" in part and part not in {"*", "**"} for part in parts)
    invalid |= "**" in parts and parts[-1] != "**"
    if invalid:
        raise RPCError(f"Invalid RPC route: {route}", "INVALID_ROUTE")


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
        len(parts),
        -parts.count("**"),
        -parts.count("*"),
    )


def _looks_like_request(payload: bytes) -> bool:
    try:
        reader = BufferReader(payload)
        reader.read_bytes(16)
        reader.read_route()
        reader.read_bytes(reader.read_u32_be())
        return reader.is_eof()
    except BaseException:  # noqa: BLE001
        return False


def _looks_like_response(payload: bytes) -> bool:
    try:
        reader = BufferReader(payload)
        reader.read_bytes(16)
        reader.read_u64_be()
        flags = reader.read_u8()
        reader.read_bytes(reader.read_u32_be())
        return flags & ~1 == 0 and reader.is_eof()
    except BaseException:  # noqa: BLE001
        return False


def _decode_terminal_error(body: bytes) -> RPCError | None:
    if len(body) < 9 or body[0] != 1:
        return None
    try:
        response = parse_response(body)
    except (CodecError, ProtocolError, UnicodeDecodeError):
        return None
    if (
        response.success
        or response.error_code is None
        or not 6001 <= response.error_code <= 6013
        or response.data
    ):
        return None
    return RPCError(
        f"CALL failed: {response.error or f'status {response.error_code}'}",
        "CALL",
        response.error_code,
    )


def _expect_empty_rpc_success(data: bytes, operation: str) -> None:
    reader = BufferReader(data)
    payload = reader.read_bytes(reader.read_u32_be())
    if payload or not reader.is_eof():
        raise RPCError(f"{operation} response is malformed", "INVALID_RESPONSE")
