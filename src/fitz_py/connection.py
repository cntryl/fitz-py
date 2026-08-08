"""Connection lifecycle, multiplexing, reconnect, retry, and telemetry."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import random
import time
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from fitz_py._runtime import AsyncDispatcher, RequestGate
from fitz_py.errors import (
    AuthenticationError,
    FitzConnectionError,
    FitzTransportError,
    ReconnectRestoreError,
    is_retryable,
)
from fitz_py.multiplexer import Multiplexer
from fitz_py.protocol.frame import FrameCodec, FrameParser
from fitz_py.protocol.messages import MSG_CONNECT
from fitz_py.transport.base import Transport
from fitz_py.types import ClientConfig, ConnectionState, LifecycleEvent

TransportFactory = Callable[[], Transport]
ReconnectListener = Callable[[], None | Awaitable[None]]
DisconnectListener = Callable[[], None | Awaitable[None]]
T = TypeVar("T")


class Connection:
    def __init__(self, transport_factory: TransportFactory, config: ClientConfig) -> None:
        self._transport_factory = transport_factory
        self._config = config
        self._transport: Transport | None = None
        self._state = ConnectionState.DISCONNECTED
        self._generation = 0
        self._multiplexer = Multiplexer()
        self._parser = FrameParser()
        self._receive_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._reconnect_task: asyncio.Task[None] | None = None
        self._loss_task: asyncio.Task[None] | None = None
        self._connect_task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._connect_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._closed = False
        self._ever_authenticated = False
        self._restoring = False
        self._last_activity = time.monotonic()
        self._auth_error: asyncio.Future[None] | None = None
        self._reconnect_listeners: dict[tuple[str, str], ReconnectListener] = {}
        self._disconnect_listeners: set[DisconnectListener] = set()
        self._gate = self._new_gate()
        limits = config.limits
        self._dispatcher = AsyncDispatcher(
            limits.async_handler_concurrency,
            limits.async_handler_queue_size,
            limits.async_handler_timeout,
            self._handler_error,
        )

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def config(self) -> ClientConfig:
        return self._config

    async def connect(self) -> None:
        if self._closed:
            raise FitzConnectionError("Client is closed")
        if self.is_connected():
            return
        async with self._connect_lock:
            if self.is_connected():
                return
            if self._connect_task is None or self._connect_task.done():
                self._connect_task = asyncio.create_task(self._open_and_authenticate(False))
            task = self._connect_task
        await asyncio.shield(task)

    async def close(self) -> None:
        if self._close_task is not None:
            await asyncio.shield(self._close_task)
            return
        self._close_task = asyncio.create_task(self._close())
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._set_state(ConnectionState.CLOSED, "closed")
        self._gate.close()
        self._multiplexer.set_disconnected()
        await self._notify_disconnect()
        current = asyncio.current_task()
        for task in (self._reconnect_task, self._heartbeat_task, self._receive_task):
            if task is not None and task is not current:
                task.cancel()
        transport, self._transport = self._transport, None
        if transport is not None:
            with contextlib.suppress(Exception):
                await transport.close()
        await self._dispatcher.close()

    async def request(self, message_type: int, payload: bytes) -> bytes:
        self._ensure_authenticated()
        release = await self._gate.acquire()
        started = time.monotonic()
        try:
            return await self._request_without_gate(message_type, payload)
        finally:
            release()
            self._record_latency(message_type, started)

    async def send(self, message_type: int, payload: bytes) -> None:
        self._ensure_authenticated()
        release = await self._gate.acquire()
        try:
            await self._send_frame(FrameCodec.encode_frame(message_type, payload))
        finally:
            release()

    async def send_fire_and_forget(self, message_type: int, payload: bytes) -> None:
        await self.send(message_type, payload)

    async def request_without_admission(self, message_type: int, payload: bytes) -> bytes:
        self._ensure_authenticated()
        return await self._request_without_gate(message_type, payload)

    async def reserve_admission(self) -> Callable[[], None]:
        self._ensure_authenticated()
        return await self._gate.acquire()

    def register_notification_handler(
        self, message_type: int, handler: Callable[[bytes], None]
    ) -> None:
        self._multiplexer.register_notification_handler(message_type, handler)

    def unregister_notification_handler(self, message_type: int) -> None:
        self._multiplexer.unregister_notification_handler(message_type)

    def register_push_classifier(
        self, message_type: int, classifier: Callable[[bytes], bool]
    ) -> None:
        self._multiplexer.register_push_classifier(message_type, classifier)

    def dispatch_async(self, work: Callable[[], Awaitable[None]]) -> bool:
        accepted = self._dispatcher.dispatch(work)
        if not accepted:
            self._log("warning", "fitz.handlers.saturated")
        return accepted

    def on_reconnect(
        self,
        listener: ReconnectListener,
        *,
        domain: str = "unknown",
        registration: str = "unknown",
    ) -> Callable[[], None]:
        key = (domain, registration)
        if key == ("unknown", "unknown"):
            key = ("unknown", str(id(listener)))
        self._reconnect_listeners[key] = listener

        def unregister() -> None:
            self._reconnect_listeners.pop(key, None)

        return unregister

    def on_disconnect(self, listener: DisconnectListener) -> Callable[[], None]:
        self._disconnect_listeners.add(listener)

        def unregister() -> None:
            self._disconnect_listeners.discard(listener)

        return unregister

    def report_restore_failure(self, domain: str, registration: str, error: BaseException) -> None:
        self._emit(
            "reconnect_restore_failed",
            domain=domain,
            registration=registration,
            error=error,
        )

    def get_multiplexer(self) -> Multiplexer:
        return self._multiplexer

    def get_state(self) -> ConnectionState:
        return self._state

    def is_connected(self) -> bool:
        return self._state is ConnectionState.AUTHENTICATED

    def get_url(self) -> str:
        return self._config.url

    async def run_with_retry(
        self,
        operation: Callable[[], Awaitable[T]],
        *,
        replay_safe: bool,
    ) -> T:
        policy = self._config.retry
        attempts = policy.max_attempts if policy.enabled and replay_safe else 1
        delay = policy.backoff
        for attempt in range(1, attempts + 1):
            try:
                return await operation()
            except BaseException as exc:
                if attempt >= attempts or not is_retryable(exc):
                    raise
                await asyncio.sleep(delay * random.uniform(0.8, 1.2))
                delay = min(delay * 2, policy.max_backoff)
        raise AssertionError("retry loop exhausted")

    async def _open_and_authenticate(self, reconnect: bool) -> None:
        self._parser = FrameParser()
        self._gate = self._new_gate()
        transport = self._transport_factory()
        self._transport = transport
        self._set_state(
            ConnectionState.RECONNECTING if reconnect else ConnectionState.CONNECTING,
            "reconnecting" if reconnect else "connecting",
        )
        try:
            async with asyncio.timeout(self._config.request_timeout):
                await transport.connect()
            self._set_state(ConnectionState.CONNECTED, "connected")
            self._set_state(ConnectionState.AUTHENTICATING, "authenticating")
            self._auth_error = asyncio.get_running_loop().create_future()
            self._receive_task = asyncio.create_task(self._receive_loop(transport))
            await self._send_connect()
            try:
                async with asyncio.timeout(self._config.auth_settle_timeout):
                    await asyncio.shield(self._auth_error)
            except TimeoutError:
                pass
            if self._auth_error is not None and self._auth_error.done():
                self._auth_error.result()
            self._auth_error = None
            self._generation += 1
            self._multiplexer.set_connected()
            if reconnect:
                self._restoring = True
                try:
                    await self._restore_registrations()
                finally:
                    self._restoring = False
            self._ever_authenticated = True
            self._set_state(ConnectionState.AUTHENTICATED, "authenticated")
            self._start_heartbeat()
        except BaseException as exc:
            self._multiplexer.set_disconnected()
            if self._transport is transport:
                self._transport = None
            with contextlib.suppress(Exception):
                await transport.close()
            if isinstance(exc, AuthenticationError):
                self._closed = True
                self._gate.close()
                await self._dispatcher.close()
                self._set_state(ConnectionState.CLOSED, "auth_rejected", error=exc)
            elif not self._closed:
                self._set_state(ConnectionState.DISCONNECTED, "connect_failed")
            raise

    async def _send_connect(self) -> None:
        provider = self._config.token_provider
        provided = "" if provider is None else provider()
        token: str | bytes
        if inspect.isawaitable(provided):
            token = await provided
        else:
            token = provided
        payload = token.encode() if isinstance(token, str) else bytes(token)
        await self._send_frame(FrameCodec.encode_frame(MSG_CONNECT, payload))

    async def _request_without_gate(self, message_type: int, payload: bytes) -> bytes:
        frame = FrameCodec.encode_frame(message_type, payload)
        try:
            return await self._multiplexer.request(
                message_type, frame, self._send_frame, self._config.request_timeout
            )
        except (FitzTransportError, FitzConnectionError) as exc:
            self._schedule_connection_loss(exc)
            raise

    async def _send_frame(self, frame: bytes) -> None:
        transport = self._transport
        if transport is None:
            raise FitzConnectionError("No active transport")
        async with self._write_lock:
            await transport.send(frame)
            self._last_activity = time.monotonic()

    async def _receive_loop(self, transport: Transport) -> None:
        try:
            while not self._closed and self._transport is transport:
                data = await transport.receive()
                self._last_activity = time.monotonic()
                for frame in self._parser.parse_frames(data):
                    self._multiplexer.dispatch(frame.message_type, frame.payload)
        except asyncio.CancelledError:
            return
        except BaseException as exc:
            if self._state is ConnectionState.AUTHENTICATING and self._auth_error is not None:
                if not self._auth_error.done():
                    self._auth_error.set_exception(AuthenticationError(str(exc) or "auth rejected"))
                return
            if not self._closed and self._transport is transport:
                await self._connection_lost(exc)

    async def _connection_lost(self, cause: BaseException) -> None:
        if self._state in {
            ConnectionState.DISCONNECTED,
            ConnectionState.RECONNECTING,
            ConnectionState.CLOSED,
        }:
            return
        self._multiplexer.set_disconnected()
        self._gate.close()
        self._set_state(ConnectionState.DISCONNECTED, "disconnected", error=cause)
        transport, self._transport = self._transport, None
        if transport is not None:
            with contextlib.suppress(Exception):
                await transport.close()
        await self._notify_disconnect()
        if not self._ever_authenticated or not self._config.reconnect.enabled or self._closed:
            return
        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    def _schedule_connection_loss(self, cause: BaseException) -> None:
        if self._loss_task is None or self._loss_task.done():
            self._loss_task = asyncio.create_task(self._connection_lost(cause))

    async def _reconnect_loop(self) -> None:
        policy = self._config.reconnect
        delay = policy.backoff
        attempt = 0
        while not self._closed and (policy.max_attempts is None or attempt < policy.max_attempts):
            attempt += 1
            self._set_state(ConnectionState.RECONNECTING, "reconnect_attempt", attempt=attempt)
            await asyncio.sleep(delay * random.uniform(0.8, 1.2))
            try:
                await self._open_and_authenticate(True)
                return
            except AuthenticationError:
                self._closed = True
                self._set_state(ConnectionState.CLOSED, "auth_rejected")
                return
            except BaseException as exc:
                self._emit("reconnect_failed", attempt=attempt, error=exc)
                delay = min(delay * 2, policy.max_backoff)
        if not self._closed:
            self._set_state(ConnectionState.DISCONNECTED, "reconnect_exhausted")

    async def _restore_registrations(self) -> None:
        for (domain, registration), listener in list(self._reconnect_listeners.items()):
            try:
                result = listener()
                if inspect.isawaitable(result):
                    await result
            except BaseException as exc:
                error = ReconnectRestoreError(domain, registration, exc)
                self._emit(
                    "reconnect_restore_failed",
                    domain=domain,
                    registration=registration,
                    error=error,
                )

    async def _notify_disconnect(self) -> None:
        for listener in list(self._disconnect_listeners):
            try:
                result = listener()
                if inspect.isawaitable(result):
                    await result
            except BaseException as exc:
                self._handler_error(exc)

    def _start_heartbeat(self) -> None:
        if not self._config.heartbeat.enabled:
            return
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self) -> None:
        policy = self._config.heartbeat
        try:
            while not self._closed and self.is_connected():
                await asyncio.sleep(policy.interval)
                if time.monotonic() - self._last_activity < policy.interval:
                    continue
                transport = self._transport
                if transport is None:
                    return
                await transport.heartbeat(policy.timeout)
                self._last_activity = time.monotonic()
        except asyncio.CancelledError:
            return
        except BaseException as exc:
            await self._connection_lost(exc)

    def _new_gate(self) -> RequestGate:
        limits = self._config.limits
        return RequestGate(limits.max_in_flight, limits.request_queue_size)

    def _ensure_authenticated(self) -> None:
        if self._closed or (
            self._state is not ConnectionState.AUTHENTICATED and not self._restoring
        ):
            raise FitzConnectionError(
                f"Cannot use connection while state is {self._state.value}",
                {"state": self._state.value},
            )

    def _set_state(
        self,
        state: ConnectionState,
        event: str,
        *,
        attempt: int | None = None,
        error: BaseException | None = None,
    ) -> None:
        self._state = state
        self._emit(event, attempt=attempt, error=error)

    def _emit(
        self,
        event: str,
        *,
        attempt: int | None = None,
        domain: str | None = None,
        registration: str | None = None,
        error: BaseException | None = None,
    ) -> None:
        payload = LifecycleEvent(
            event=event,
            state=self._state,
            url=self._config.url,
            transport=type(self._transport).__name__ if self._transport else None,
            attempt=attempt,
            domain=domain,
            registration=registration,
            error=str(error) if error else None,
        )
        observer = self._config.observability
        if observer.on_lifecycle_event is not None:
            observer.on_lifecycle_event(payload)
        self._log("info", f"fitz.connection.{event}", lifecycle=payload)
        if observer.meter is not None:
            observer.meter.counter("fitz.connection.lifecycle", 1, {"event": event})

    def _record_latency(self, message_type: int, started: float) -> None:
        elapsed = time.monotonic() - started
        meter = self._config.observability.meter
        if meter is not None:
            meter.histogram("fitz.request.duration", elapsed, {"message_type": message_type})
        if elapsed > 1:
            self._log("warning", "fitz.request.slow", message_type=message_type, duration=elapsed)

    def _handler_error(self, error: BaseException) -> None:
        self._log("error", "fitz.handler.error", error=str(error))

    def _log(self, level: str, event: str, **fields: Any) -> None:
        logger = self._config.observability.logger
        if logger is None:
            return
        method = getattr(logger, level, None)
        if callable(method):
            method(event, extra={"fitz": fields})
