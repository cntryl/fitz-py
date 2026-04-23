from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable

from fitz_py.errors import AuthenticationError, ConnectionError, TransportError
from fitz_py.multiplexer import Multiplexer
from fitz_py.protocol.frame import FrameCodec, FrameParser
from fitz_py.protocol.messages import MSG_CONNECT
from fitz_py.transport.base import Transport
from fitz_py.types import ConnectionState, TokenProvider

TransportFactory = Callable[[], Transport]
ReconnectListener = Callable[[], None | Awaitable[None]]
DisconnectListener = Callable[[], None | Awaitable[None]]


async def _sleep_ms(delay_ms: int) -> None:
    await asyncio.sleep(delay_ms / 1000)


async def _resolve_token(token_provider: TokenProvider) -> str:
    token = token_provider()
    if asyncio.iscoroutine(token) or isinstance(token, Awaitable):
        return await token
    return token


class Connection:
    def __init__(
        self,
        transport_factory: TransportFactory,
        token_provider: TokenProvider,
        *,
        timeout_ms: int = 30000,
        auth_settle_delay_ms: int = 500,
        reconnect_enabled: bool = False,
        reconnect_max_attempts: int | float = float("inf"),
        reconnect_backoff_ms: int = 250,
        reconnect_max_backoff_ms: int = 5000,
    ) -> None:
        self._transport_factory = transport_factory
        self._token_provider = token_provider
        self._timeout_ms = timeout_ms
        self._auth_settle_delay_ms = auth_settle_delay_ms
        self._reconnect_enabled = reconnect_enabled
        self._reconnect_max_attempts = reconnect_max_attempts
        self._reconnect_backoff_ms = reconnect_backoff_ms
        self._reconnect_max_backoff_ms = reconnect_max_backoff_ms

        self._transport: Transport | None = None
        self._state = ConnectionState.DISCONNECTED
        self._multiplexer = Multiplexer()
        self._frame_parser = FrameParser()
        self._receive_task: asyncio.Task[None] | None = None
        self._reconnect_listeners: set[ReconnectListener] = set()
        self._disconnect_listeners: set[DisconnectListener] = set()
        self._auth_future: asyncio.Future[None] | None = None
        self._close_requested = False
        self._receive_loop_abort = False
        self._reconnect_task: asyncio.Task[None] | None = None

    async def connect(self) -> None:
        self._close_requested = False
        await self._open_and_authenticate(False)

    async def close(self) -> None:
        if self._state is ConnectionState.CLOSED and self._transport is None:
            return
        self._close_requested = True
        self._receive_loop_abort = True
        self._set_state(ConnectionState.CLOSED)
        if self._auth_future is not None and not self._auth_future.done():
            self._auth_future.set_exception(ConnectionError("Connection closed"))
        self._auth_future = None
        self._multiplexer.set_disconnected()
        await self._notify_disconnect_listeners()

        receive_task = self._receive_task
        self._receive_task = None
        if receive_task is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(receive_task, timeout=1)

        transport = self._transport
        self._transport = None
        if transport is not None:
            await transport.close()

    async def request(self, message_type: int, payload: bytes) -> bytes:
        self._ensure_authenticated()
        transport = self._ensure_transport()
        frame = FrameCodec.encode_frame(message_type, payload)
        try:
            return await self._multiplexer.request(
                message_type,
                frame,
                transport.send,
                self._timeout_ms,
            )
        except Exception as exc:
            self._handle_possible_transport_failure(exc)
            raise

    async def send(self, message_type: int, payload: bytes) -> None:
        self._ensure_authenticated()
        transport = self._ensure_transport()
        frame = FrameCodec.encode_frame(message_type, payload)
        try:
            await transport.send(frame)
        except Exception as exc:
            self._handle_possible_transport_failure(exc)
            raise

    async def send_fire_and_forget(self, message_type: int, payload: bytes) -> None:
        await self.send(message_type, payload)

    def register_notification_handler(
        self, message_type: int, handler: Callable[[bytes], None]
    ) -> None:
        self._multiplexer.register_notification_handler(message_type, handler)

    def unregister_notification_handler(self, message_type: int) -> None:
        self._multiplexer.unregister_notification_handler(message_type)

    def on_reconnect(self, listener: ReconnectListener) -> Callable[[], None]:
        self._reconnect_listeners.add(listener)

        def unregister() -> None:
            self._reconnect_listeners.discard(listener)

        return unregister

    def on_disconnect(self, listener: DisconnectListener) -> Callable[[], None]:
        self._disconnect_listeners.add(listener)

        def unregister() -> None:
            self._disconnect_listeners.discard(listener)

        return unregister

    def get_multiplexer(self) -> Multiplexer:
        return self._multiplexer

    def get_state(self) -> ConnectionState:
        return self._state

    def is_connected(self) -> bool:
        return self._state is ConnectionState.AUTHENTICATED

    def get_url(self) -> str:
        return self._ensure_transport().get_url()

    async def _open_and_authenticate(self, is_reconnect: bool) -> None:
        self._receive_loop_abort = False
        self._transport = self._transport_factory()
        self._set_state(
            ConnectionState.RECONNECTING if is_reconnect else ConnectionState.CONNECTING
        )
        await self._transport.connect()
        self._receive_task = asyncio.create_task(self._receive_loop())

        self._set_state(ConnectionState.CONNECTED)
        self._set_state(ConnectionState.AUTHENTICATING)
        loop = asyncio.get_running_loop()
        self._auth_future = loop.create_future()

        try:
            await self._send_connect()
            await asyncio.wait_for(
                asyncio.shield(self._auth_settle()), timeout=self._timeout_ms / 1000
            )
            if self._auth_future is not None and not self._auth_future.done():
                self._auth_future.set_result(None)
            self._auth_future = None
            self._set_state(ConnectionState.AUTHENTICATED)
            self._multiplexer.set_connected()
            if is_reconnect:
                await self._restore_reconnect_state()
        except Exception:
            self._auth_future = None
            self._multiplexer.set_disconnected()
            transport = self._transport
            self._transport = None
            if transport is not None:
                with contextlib.suppress(Exception):
                    await transport.close()
            self._set_state(ConnectionState.DISCONNECTED)
            raise

    async def _auth_settle(self) -> None:
        auth_future = self._auth_future
        if auth_future is None:
            return
        sleep_task = asyncio.create_task(_sleep_ms(self._auth_settle_delay_ms))
        done, pending = await asyncio.wait(
            {auth_future, sleep_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        for task in done:
            await task

    async def _send_connect(self) -> None:
        token = await _resolve_token(self._token_provider)
        frame = FrameCodec.encode_frame(MSG_CONNECT, token.encode())
        await self._ensure_transport().send(frame)

    async def _receive_loop(self) -> None:
        while not self._receive_loop_abort and not self._close_requested:
            try:
                transport = self._ensure_transport()
                data = await transport.receive()
                frames = self._frame_parser.parse_frames(data)
                for frame in frames:
                    self._multiplexer.dispatch(frame.message_type, frame.payload)
            except Exception as exc:
                if self._receive_loop_abort or self._close_requested:
                    return
                await self._handle_connection_loss(exc)
                return

    async def _handle_connection_loss(self, exc: Exception) -> None:
        self._multiplexer.set_disconnected()

        if (
            self._state is ConnectionState.AUTHENTICATING
            and self._auth_future is not None
            and not self._auth_future.done()
        ):
            self._auth_future.set_exception(
                AuthenticationError(self._describe_connection_loss(exc))
            )

        if self._close_requested:
            self._set_state(ConnectionState.CLOSED)
            return

        self._set_state(ConnectionState.DISCONNECTED)
        await self._notify_disconnect_listeners()
        if not self._reconnect_enabled:
            return

        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())
        await self._reconnect_task

    async def _reconnect_loop(self) -> None:
        attempts = 0
        delay_ms = self._reconnect_backoff_ms
        while not self._close_requested and attempts < self._reconnect_max_attempts:
            attempts += 1
            self._set_state(ConnectionState.RECONNECTING)
            await _sleep_ms(delay_ms)
            try:
                await self._open_and_authenticate(True)
                return
            except Exception:
                delay_ms = min(delay_ms * 2, self._reconnect_max_backoff_ms)
        self._set_state(ConnectionState.DISCONNECTED)

    async def _restore_reconnect_state(self) -> None:
        for listener in list(self._reconnect_listeners):
            result = listener()
            if asyncio.iscoroutine(result):
                await result

    async def _notify_disconnect_listeners(self) -> None:
        for listener in list(self._disconnect_listeners):
            result = listener()
            if asyncio.iscoroutine(result):
                await result

    def _ensure_transport(self) -> Transport:
        if self._transport is None:
            raise ConnectionError("No active transport")
        return self._transport

    def _ensure_authenticated(self) -> None:
        if self._close_requested or self._state is not ConnectionState.AUTHENTICATED:
            raise ConnectionError(f"Cannot use connection while state is {self._state.value}")

    def _set_state(self, state: ConnectionState) -> None:
        self._state = state

    def _handle_possible_transport_failure(self, exc: Exception) -> None:
        if self._close_requested:
            return
        if isinstance(exc, (TransportError, ConnectionError, AuthenticationError)):
            asyncio.create_task(self._handle_connection_loss(exc))

    @staticmethod
    def _describe_connection_loss(exc: Exception) -> str:
        return str(exc) if str(exc) else "connection closed during CONNECT"
