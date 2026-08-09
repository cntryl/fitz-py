"""Cancellation-safe FIFO response correlation and push-frame dispatch."""

from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from fitz_py.errors import FitzConnectionError, FitzTimeoutError

NotificationHandler = Callable[[bytes], None]
PushClassifier = Callable[[bytes], bool]


@dataclass(slots=True)
class PendingRequest:
    future: asyncio.Future[bytes] | None
    sent: bool = False


class Multiplexer:
    def __init__(self, on_error: Callable[[BaseException], None] | None = None) -> None:
        self._pending: dict[int, deque[PendingRequest]] = defaultdict(deque)
        self._notification_handlers: dict[int, NotificationHandler] = {}
        self._push_classifiers: dict[int, PushClassifier] = {}
        self._connected = False
        self._on_error = on_error

    def set_connected(self) -> None:
        self._connected = True

    def set_disconnected(self) -> None:
        self._connected = False
        self.cancel_all()

    def register_notification_handler(
        self, message_type: int, handler: NotificationHandler
    ) -> None:
        self._notification_handlers[message_type] = handler

    def unregister_notification_handler(self, message_type: int) -> None:
        self._notification_handlers.pop(message_type, None)

    def register_push_classifier(self, message_type: int, classifier: PushClassifier) -> None:
        self._push_classifiers[message_type] = classifier

    async def request(
        self,
        message_type: int,
        frame_data: bytes,
        send: Callable[[bytes], Awaitable[None]],
        timeout: float,
    ) -> bytes:
        future = asyncio.get_running_loop().create_future()
        pending = PendingRequest(future)
        self._pending[message_type].append(pending)
        try:
            send_future = asyncio.ensure_future(send(frame_data))
            try:
                await asyncio.shield(send_future)
            except asyncio.CancelledError:
                try:
                    await asyncio.shield(send_future)
                except BaseException:  # noqa: BLE001
                    self._remove(message_type, pending)
                else:
                    pending.sent = True
                    self._abandon(message_type, pending)
                raise
            pending.sent = True
            async with asyncio.timeout(timeout):
                return await future
        except TimeoutError as exc:
            self._abandon(message_type, pending)
            raise FitzTimeoutError(
                f"Request timeout for message type {message_type} after {timeout:g}s"
            ) from exc
        except asyncio.CancelledError:
            self._abandon(message_type, pending)
            raise
        except BaseException as exc:
            if pending.sent or isinstance(exc, (FitzConnectionError, FitzTimeoutError)):
                pending.sent = True
                self._abandon(message_type, pending)
            else:
                self._remove(message_type, pending)
            raise

    def _abandon(self, message_type: int, pending: PendingRequest) -> None:
        if pending.sent:
            pending.future = None  # tombstone consumes a possible late reply
        else:
            self._remove(message_type, pending)

    def _remove(self, message_type: int, pending: PendingRequest) -> None:
        queue = self._pending.get(message_type)
        if queue is None:
            return
        try:
            queue.remove(pending)
        except ValueError:
            return
        if not queue:
            self._pending.pop(message_type, None)

    def dispatch(self, message_type: int, payload: bytes) -> None:
        classifier = self._push_classifiers.get(message_type)
        if classifier is not None:
            try:
                if classifier(payload):
                    self._dispatch_push(message_type, payload)
                    return
            except Exception as exc:  # noqa: BLE001
                self._report_error(exc)

        queue = self._pending.get(message_type)
        if queue:
            pending = queue.popleft()
            if not queue:
                self._pending.pop(message_type, None)
            if pending.future is not None and not pending.future.done():
                pending.future.set_result(payload)
            return
        self._dispatch_push(message_type, payload)

    def _dispatch_push(self, message_type: int, payload: bytes) -> None:
        handler = self._notification_handlers.get(message_type)
        if handler is not None:
            try:
                handler(payload)
            except Exception as exc:  # noqa: BLE001
                self._report_error(exc)

    def _report_error(self, error: BaseException) -> None:
        if self._on_error is not None:
            self._on_error(error)

    def cancel_all(self) -> None:
        error = FitzConnectionError("Connection closed or reset")
        for queue in self._pending.values():
            for pending in queue:
                if pending.future is not None and not pending.future.done():
                    pending.future.set_exception(error)
        self._pending.clear()
