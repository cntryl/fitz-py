from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from fitz_py.errors import ConnectionError, TimeoutError
from fitz_py.types import ConnectionState

NotificationHandler = Callable[[bytes], None]


@dataclass(slots=True)
class PendingRequest:
    future: asyncio.Future[bytes]
    timeout_handle: asyncio.TimerHandle


class Multiplexer:
    def __init__(self) -> None:
        self._pending: dict[int, deque[PendingRequest]] = defaultdict(deque)
        self._notification_handlers: dict[int, NotificationHandler] = {}
        self._optional_responses: dict[int, int] = {}
        self._state = ConnectionState.DISCONNECTED

    def set_connected(self) -> None:
        self._state = ConnectionState.AUTHENTICATED

    def set_disconnected(self) -> None:
        self._state = ConnectionState.DISCONNECTED
        self._optional_responses.clear()
        self.cancel_all()

    def register_notification_handler(
        self, message_type: int, handler: NotificationHandler
    ) -> None:
        self._notification_handlers[message_type] = handler

    def unregister_notification_handler(self, message_type: int) -> None:
        self._notification_handlers.pop(message_type, None)

    def expect_optional_response(self, message_type: int) -> Callable[[], None]:
        self._optional_responses[message_type] = self._optional_responses.get(message_type, 0) + 1

        def cancel() -> None:
            current = self._optional_responses.get(message_type, 0)
            if current <= 1:
                self._optional_responses.pop(message_type, None)
            else:
                self._optional_responses[message_type] = current - 1

        return cancel

    async def request(
        self,
        message_type: int,
        frame_data: bytes,
        send: Callable[[bytes], Awaitable[None]],
        timeout_ms: int,
    ) -> bytes:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[bytes] = loop.create_future()

        def on_timeout() -> None:
            self._remove_pending_future(message_type, future)
            if not future.done():
                future.set_exception(
                    TimeoutError(
                        f"Request timeout for message type {message_type} after {timeout_ms}ms"
                    )
                )

        timeout_handle = loop.call_later(timeout_ms / 1000, on_timeout)
        self._pending[message_type].append(
            PendingRequest(future=future, timeout_handle=timeout_handle)
        )

        try:
            await send(frame_data)
            return await future
        except BaseException:
            timeout_handle.cancel()
            self._remove_pending_future(message_type, future)
            raise

    def _remove_pending_future(self, message_type: int, future: asyncio.Future[bytes]) -> None:
        queue = self._pending.get(message_type)
        if queue is None:
            return

        filtered = deque(item for item in queue if item.future is not future)
        if filtered:
            self._pending[message_type] = filtered
            return

        self._pending.pop(message_type, None)

    def dispatch(self, message_type: int, payload: bytes) -> None:
        queue = self._pending.get(message_type)
        if queue:
            pending = queue.popleft()
            if not queue:
                self._pending.pop(message_type, None)
            pending.timeout_handle.cancel()
            if not pending.future.done():
                pending.future.set_result(payload)
            return

        handler = self._notification_handlers.get(message_type)
        if handler is not None:
            try:
                handler(payload)
            except Exception:
                return
            return

        optional_count = self._optional_responses.get(message_type, 0)
        if optional_count > 0:
            if optional_count == 1:
                self._optional_responses.pop(message_type, None)
            else:
                self._optional_responses[message_type] = optional_count - 1
            return

        if self._state is not ConnectionState.AUTHENTICATED:
            return

    def cancel_all(self) -> None:
        for queue in self._pending.values():
            for pending in queue:
                pending.timeout_handle.cancel()
                if not pending.future.done():
                    pending.future.set_exception(ConnectionError("Connection closed or reset"))
        self._pending.clear()
