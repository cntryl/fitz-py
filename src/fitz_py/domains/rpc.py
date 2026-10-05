"""Streaming RPC calls and worker registrations."""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Literal

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
    CAP_RPC_CANCELLATION,
    MSG_RPC_CANCELLATION,
    MSG_RPC_LIFECYCLE,
    MSG_RPC_REQUEST,
    MSG_RPC_RESPONSE,
    MSG_RPC_SUBSCRIBE_WORKER,
    MSG_RPC_UNSUBSCRIBE_WORKER,
)
from fitz_py.protocol.response import parse_response
from fitz_py.types import BytesLike

_RPC_EXTENSION_VERSION = 1
_RPC_CANCELLATION_GRACE_SECONDS = 5.0
_MAX_RPC_BUDGET_MS = 86_400_000
RpcCancellationOutcome = Literal[
    "not_requested",
    "unsupported",
    "queued_removed",
    "forwarded",
    "worker_unsupported",
    "already_terminal",
    "unknown_or_unauthorized",
    "forwarding_failed",
    "unconfirmed",
    "connection_closed",
]


@dataclass(frozen=True, slots=True)
class ResponseFrame:
    body: bytes
    sequence: int


@dataclass(frozen=True, slots=True)
class InboundRequest:
    route: str
    body: bytes
    context: RpcHandlerContext | None = None


@dataclass(slots=True)
class RpcHandlerContext:
    """Cooperative cancellation and inherited deadline for one worker call."""

    cancelled: asyncio.Event
    _deadline: float | None = None

    def remaining_time_ms(self) -> int | None:
        if self._deadline is None:
            return None
        return max(0, int((self._deadline - asyncio.get_running_loop().time()) * 1000))


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

    @property
    def ended(self) -> bool:
        return self._ended


@dataclass(slots=True)
class _ActiveInvocation:
    context: RpcHandlerContext
    response: ResponseWriter
    correlation_id: bytes
    deadline_handle: asyncio.TimerHandle | None = None
    cancellation_requested: bool = False
    finished: bool = False
    started: bool = False


class RPCCall(AsyncIterator[ResponseFrame]):
    def __init__(
        self,
        client: RPCClient,
        key: bytes,
        deadline: float | None,
        capacity: int,
    ) -> None:
        self._client = client
        self._key = key
        self._deadline = deadline
        self._queue: asyncio.Queue[ResponseFrame | BaseException | None] = asyncio.Queue(capacity)
        self._closed = False
        self._terminal = False
        self._failure: BaseException | None = None
        self._cancellation: asyncio.Future[RpcCancellationOutcome] = (
            asyncio.get_running_loop().create_future()
        )
        self._parent_cancellation_task: asyncio.Task[None] | None = None
        self._deadline_task: asyncio.Task[None] | None = None

    def watch_deadline(self) -> None:
        if self._deadline is not None:
            self._deadline_task = asyncio.create_task(self._expire_at_deadline())

    async def _expire_at_deadline(self) -> None:
        if self._deadline is None:
            return
        await asyncio.sleep(max(0, self._deadline - asyncio.get_running_loop().time()))
        if not self._closed and not self._terminal:
            self._failure = FitzTimeoutError("RPC response timed out")
            await self._close_and_cancel(2)

    def watch_parent_cancellation(self, parent_cancellation: asyncio.Event) -> None:
        self._parent_cancellation_task = asyncio.create_task(
            self._cancel_when_parent_cancelled(parent_cancellation)
        )

    async def _cancel_when_parent_cancelled(self, parent_cancellation: asyncio.Event) -> None:
        await parent_cancellation.wait()
        await self._close_and_cancel(1)

    def _stop_parent_cancellation(self) -> None:
        task = self._parent_cancellation_task
        self._parent_cancellation_task = None
        if task is not None and task is not asyncio.current_task():
            task.cancel()
        deadline_task = self._deadline_task
        self._deadline_task = None
        if deadline_task is not None and deadline_task is not asyncio.current_task():
            deadline_task.cancel()

    @property
    def cancellation(self) -> asyncio.Future[RpcCancellationOutcome]:
        """Resolves with the broker's best-effort remote-cancellation result."""
        return self._cancellation

    def __aiter__(self) -> RPCCall:
        return self

    async def __anext__(self) -> ResponseFrame:
        if self._failure is not None and self._queue.empty():
            failure, self._failure = self._failure, None
            raise failure
        if (self._closed or self._terminal) and self._queue.empty():
            raise StopAsyncIteration
        remaining = (
            None if self._deadline is None else self._deadline - asyncio.get_running_loop().time()
        )
        if remaining is not None and remaining <= 0:
            await self._close_and_cancel(2)
            raise FitzTimeoutError("RPC response timed out")
        try:
            if remaining is None:
                item = await self._queue.get()
            else:
                async with asyncio.timeout(remaining):
                    item = await self._queue.get()
        except asyncio.CancelledError:
            await self._close_and_cancel(1)
            raise
        except TimeoutError as exc:
            await self._close_and_cancel(2)
            raise FitzTimeoutError("RPC response timed out") from exc
        if item is None:
            self._closed = True
            if self._failure is not None:
                failure, self._failure = self._failure, None
                raise failure
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
        await self._close_and_cancel(1)

    async def cancel(self) -> RpcCancellationOutcome:
        await self._close_and_cancel(1)
        return await self._cancellation

    async def _close_and_cancel(self, reason: Literal[1, 2]) -> None:
        if self._terminal:
            return
        if not self._closed:
            self._closed = True
            while not self._queue.empty():
                with contextlib.suppress(asyncio.QueueEmpty):
                    self._queue.get_nowait()
            self._queue.put_nowait(None)
            self._stop_parent_cancellation()
            await self._client._abandon_call(self, reason)  # noqa: SLF001

    def push(self, frame: ResponseFrame, end: bool) -> None:
        if self._closed:
            return
        try:
            self._queue.put_nowait(frame)
            if end:
                self._terminal = True
                self._stop_parent_cancellation()
                self._client._finish_call(self, "not_requested")  # noqa: SLF001
        except asyncio.QueueFull:
            self.fail(
                RPCError("RPC response consumer fell behind", "BACKPRESSURE"),
                preserve_buffered=False,
            )

    def fail(self, error: BaseException, *, preserve_buffered: bool = True) -> None:
        if self._closed:
            return
        self._closed = True
        self._stop_parent_cancellation()
        outcome: RpcCancellationOutcome = (
            "connection_closed" if isinstance(error, FitzConnectionError) else "not_requested"
        )
        self._client._finish_call(self, outcome)  # noqa: SLF001
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


class LazyRPCCall(LazyAsyncIterator[ResponseFrame]):
    """Lazy RPC stream with explicit cancellation outcome access."""

    def __init__(self, factory: Callable[[], Awaitable[RPCCall]]) -> None:
        self._cancellation: asyncio.Future[RpcCancellationOutcome] | None = None

        async def start() -> RPCCall:
            resource = await factory()
            self._cancellation = resource.cancellation
            return resource

        super().__init__(start)

    @property
    def cancellation(self) -> Awaitable[RpcCancellationOutcome]:
        return self._get_cancellation()

    async def cancel(self) -> RpcCancellationOutcome:
        await self.aclose()
        return await self._get_cancellation()

    async def _get_cancellation(self) -> RpcCancellationOutcome:
        if self._cancellation is None:
            return "not_requested"
        return await self._cancellation


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
        self._pending_cancellations: dict[
            bytes, tuple[asyncio.Future[RpcCancellationOutcome], asyncio.TimerHandle]
        ] = {}
        self._active_invocations: dict[bytes, _ActiveInvocation] = {}
        self._workers: dict[str, _Registration] = {}
        self._worker_locks: dict[str, asyncio.Lock] = {}
        self._terminal_tasks: set[asyncio.Task[None]] = set()
        connection.register_push_classifier(MSG_RPC_REQUEST, _looks_like_request)
        connection.register_push_classifier(MSG_RPC_RESPONSE, _looks_like_response)
        connection.register_notification_handler(MSG_RPC_REQUEST, self._on_request)
        connection.register_notification_handler(MSG_RPC_RESPONSE, self._on_response)
        connection.register_notification_handler(MSG_RPC_LIFECYCLE, self._on_lifecycle)
        connection.on_disconnect(self._disconnect)
        connection.on_reconnect(self._restore, domain="rpc", registration="workers")

    def call(self, route: str, body: BytesLike, *, timeout: float = 30.0) -> LazyRPCCall:
        _route(route, patterns=False)
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if timeout * 1000 > _MAX_RPC_BUDGET_MS:
            raise ValueError(f"timeout must be at most {_MAX_RPC_BUDGET_MS} milliseconds")
        return LazyRPCCall(lambda: self.open_call(route, body, timeout=timeout))

    def call_from_request(
        self, request: InboundRequest, route: str, body: BytesLike
    ) -> LazyRPCCall:
        """Creates a downstream call linked to an inbound request's budget and cancellation."""
        _route(route, patterns=False)
        context = request.context
        if context is None:
            raise ValueError("inbound RPC request has no handler context")

        async def open_downstream_call() -> RPCCall:
            if context.cancelled.is_set():
                raise asyncio.CancelledError
            remaining_ms = context.remaining_time_ms()
            if remaining_ms == 0:
                raise FitzTimeoutError("Inbound RPC request deadline elapsed")
            timeout = None if remaining_ms is None else remaining_ms / 1000
            return await self.open_call(
                route,
                body,
                timeout=timeout,
                parent_cancellation=context.cancelled,
            )

        return LazyRPCCall(open_downstream_call)

    async def open_call(
        self,
        route: str,
        body: BytesLike,
        *,
        timeout: float | None = 30.0,
        parent_cancellation: asyncio.Event | None = None,
    ) -> RPCCall:
        _route(route, patterns=False)
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        if timeout is not None and timeout * 1000 > _MAX_RPC_BUDGET_MS:
            raise ValueError(f"timeout must be at most {_MAX_RPC_BUDGET_MS} milliseconds")
        body = bytes(body)
        correlation_id = os.urandom(16)
        deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout
        call = RPCCall(
            self,
            correlation_id,
            deadline,
            self.connection.config.limits.subscription_buffer_size,
        )
        self._pending[correlation_id] = call
        writer = BufferWriter()
        writer.write_bytes(correlation_id)
        writer.write_route(route)
        writer.write_u32_be(len(body))
        writer.write_bytes(body)
        if self.connection.capabilities & CAP_RPC_CANCELLATION and deadline is not None:
            loop = asyncio.get_running_loop()
            remaining_ms = max(
                0,
                min(
                    _MAX_RPC_BUDGET_MS,
                    int((deadline - loop.time()) * 1000),
                ),
            )
            _write_request_budget(writer, remaining_ms)
        try:
            await self.connection.send(MSG_RPC_REQUEST, writer.build())
        except BaseException:
            self._pending.pop(correlation_id, None)
            raise
        call.watch_deadline()
        if parent_cancellation is not None:
            call.watch_parent_cancellation(parent_cancellation)
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
        if self.connection.capabilities & CAP_RPC_CANCELLATION:
            writer.write_u8(_RPC_EXTENSION_VERSION)
            writer.write_u8(1)
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

    def _on_lifecycle(self, payload: bytes) -> None:
        if len(payload) != 18:
            return
        kind = payload[0]
        correlation_id = payload[1:17]
        value = payload[17]
        if kind == 2 and 1 <= value <= 4:
            invocation = self._active_invocations.get(correlation_id)
            if invocation is None:
                return
            invocation.cancellation_requested = True
            invocation.context.cancelled.set()
            if invocation.finished or not invocation.started:
                self._schedule_cleanup_ack(invocation)
            return
        if kind != 4:
            return
        outcomes: dict[int, RpcCancellationOutcome] = {
            1: "queued_removed",
            2: "forwarded",
            3: "worker_unsupported",
            4: "already_terminal",
            5: "unknown_or_unauthorized",
            6: "forwarding_failed",
        }
        outcome = outcomes.get(value)
        if outcome is not None:
            self._settle_cancellation(correlation_id, outcome)

    async def _abandon_call(self, call: RPCCall, reason: Literal[1, 2]) -> None:
        correlation_id = call._key  # noqa: SLF001
        if self._pending.get(correlation_id) is call:
            self._pending.pop(correlation_id, None)
        if call._cancellation.done():  # noqa: SLF001
            return
        if not self.connection.capabilities & CAP_RPC_CANCELLATION:
            self._resolve_call_outcome(call, "unsupported")
            return
        if call._key in self._pending_cancellations:  # noqa: SLF001
            return
        loop = asyncio.get_running_loop()
        timeout = loop.call_later(
            _RPC_CANCELLATION_GRACE_SECONDS,
            self._settle_cancellation,
            correlation_id,
            "unconfirmed",
        )
        self._pending_cancellations[correlation_id] = (call._cancellation, timeout)  # noqa: SLF001
        writer = BufferWriter()
        writer.write_u8(1)
        writer.write_bytes(correlation_id)
        writer.write_u8(reason)
        try:
            await self.connection.send(MSG_RPC_CANCELLATION, writer.build())
        except (FitzConnectionError, FitzTimeoutError):
            self._settle_cancellation(correlation_id, "connection_closed")
        except Exception:  # noqa: BLE001
            self._settle_cancellation(correlation_id, "unconfirmed")

    def _finish_call(self, call: RPCCall, outcome: RpcCancellationOutcome) -> None:
        correlation_id = call._key  # noqa: SLF001
        if self._pending.get(correlation_id) is call:
            self._pending.pop(correlation_id, None)
        self._resolve_call_outcome(call, outcome)

    @staticmethod
    def _resolve_call_outcome(call: RPCCall, outcome: RpcCancellationOutcome) -> None:
        if not call._cancellation.done():  # noqa: SLF001
            call._cancellation.set_result(outcome)  # noqa: SLF001

    def _settle_cancellation(self, correlation_id: bytes, outcome: RpcCancellationOutcome) -> None:
        pending = self._pending_cancellations.pop(correlation_id, None)
        if pending is None:
            return
        future, timeout = pending
        timeout.cancel()
        if not future.done():
            future.set_result(outcome)

    def _forget_invocation(self, invocation: _ActiveInvocation) -> None:
        if self._active_invocations.get(invocation.correlation_id) is invocation:
            self._active_invocations.pop(invocation.correlation_id, None)
        if invocation.deadline_handle is not None:
            invocation.deadline_handle.cancel()
            invocation.deadline_handle = None

    def _schedule_cleanup_ack(self, invocation: _ActiveInvocation) -> None:
        task = asyncio.create_task(self._acknowledge_invocation(invocation))
        self._terminal_tasks.add(task)
        task.add_done_callback(self._terminal_completed)

    async def _acknowledge_invocation(self, invocation: _ActiveInvocation) -> None:
        self._forget_invocation(invocation)
        if not self.connection.capabilities & CAP_RPC_CANCELLATION:
            return
        with contextlib.suppress(Exception):
            await self.connection.send(
                MSG_RPC_CANCELLATION,
                b"\x03" + invocation.correlation_id,
            )

    def _on_request(self, payload: bytes) -> None:
        reader = BufferReader(payload)
        correlation_id = reader.read_bytes(16)
        route = reader.read_route()
        body = reader.read_bytes(reader.read_u32_be())
        remaining_budget_ms = _read_request_budget(reader)
        if not reader.is_eof():
            raise RPCError("RPC request has trailing bytes", "INVALID_RESPONSE")
        registration = self._best_worker(route)
        if registration is None:
            return
        deadline = (
            asyncio.get_running_loop().time() + remaining_budget_ms / 1000
            if remaining_budget_ms is not None
            else None
        )
        context = RpcHandlerContext(asyncio.Event(), deadline)
        active = _ActiveInvocation(
            context, ResponseWriter(self.connection, correlation_id), correlation_id
        )
        if deadline is not None:
            active.deadline_handle = asyncio.get_running_loop().call_later(
                max(0, deadline - asyncio.get_running_loop().time()), context.cancelled.set
            )
        self._active_invocations[correlation_id] = active
        request = InboundRequest(route, body, context)
        response = active.response
        if not self.connection.dispatch_async(
            lambda: self._run_worker(registration, request, active)
        ):
            self._forget_invocation(active)
            self._schedule_terminal(response, 6003, "Worker is locally saturated", active)

    async def _run_worker(
        self,
        registration: _Registration,
        request: InboundRequest,
        active: _ActiveInvocation,
    ) -> None:
        if self._active_invocations.get(active.correlation_id) is not active:
            active.finished = True
            return
        try:
            async with registration.semaphore:
                if self._active_invocations.get(active.correlation_id) is not active:
                    return
                if active.context.remaining_time_ms() == 0:
                    active.context.cancelled.set()
                    if not active.cancellation_requested:
                        await self._send_error(
                            active.response, 6001, "Inbound RPC request deadline elapsed"
                        )
                    return
                if active.context.cancelled.is_set():
                    return
                active.started = True
                try:
                    await registration.handler(request, active.response)
                except asyncio.CancelledError:
                    if not active.response.ended:
                        with contextlib.suppress(Exception):
                            await self._send_error(
                                active.response, 6010, "Worker handler cancelled"
                            )
                    raise
                except Exception as exc:  # noqa: BLE001
                    if active.response.ended:
                        return
                    await self._send_error(active.response, 6010, str(exc) or type(exc).__name__)
        finally:
            active.finished = True
            # Cancellation may already be ordered at the broker while its
            # worker notification is still in transit. A completed handler must
            # acknowledge cleanup even when it has not seen that notification.
            if active.cancellation_requested or active.response.ended:
                await self._acknowledge_invocation(active)

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
        for correlation_id, (future, timeout) in tuple(self._pending_cancellations.items()):
            timeout.cancel()
            if not future.done():
                future.set_result("connection_closed")
            self._pending_cancellations.pop(correlation_id, None)
        for invocation in tuple(self._active_invocations.values()):
            invocation.context.cancelled.set()
            if invocation.deadline_handle is not None:
                invocation.deadline_handle.cancel()
        self._active_invocations.clear()

    def _worker_lock(self, route: str) -> asyncio.Lock:
        return self._worker_locks.setdefault(route, asyncio.Lock())

    def _schedule_terminal(
        self,
        response: ResponseWriter,
        code: int,
        message: str,
        invocation: _ActiveInvocation | None = None,
    ) -> None:
        async def terminate() -> None:
            try:
                await self._send_error(response, code, message)
            finally:
                if invocation is not None:
                    await self._acknowledge_invocation(invocation)

        task = asyncio.create_task(terminate())
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
        _read_request_budget(reader)
        return reader.is_eof()
    except BaseException:  # noqa: BLE001
        return False


def _write_request_budget(writer: BufferWriter, remaining_budget_ms: int) -> None:
    if not 0 <= remaining_budget_ms <= _MAX_RPC_BUDGET_MS:
        raise ValueError(f"remaining budget must be in 0..={_MAX_RPC_BUDGET_MS} milliseconds")
    writer.write_u8(_RPC_EXTENSION_VERSION)
    writer.write_u8(1)
    writer.write_u32_be(remaining_budget_ms)


def _read_request_budget(reader: BufferReader) -> int | None:
    if reader.is_eof():
        return None
    if reader.remaining_bytes() != 6:
        raise RPCError("RPC request has trailing or truncated extension bytes", "INVALID_RESPONSE")
    version = reader.read_u8()
    flags = reader.read_u8()
    remaining_budget_ms = reader.read_u32_be()
    if version != _RPC_EXTENSION_VERSION or flags != 1:
        raise RPCError("Unsupported RPC request extension", "INVALID_RESPONSE")
    if remaining_budget_ms > _MAX_RPC_BUDGET_MS:
        raise RPCError("RPC remaining budget exceeds one day", "INVALID_RESPONSE")
    return remaining_budget_ms


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
