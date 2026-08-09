"""Fire-and-forget notices and bounded async subscriptions."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field

from fitz_py._runtime import AsyncSubscription, LazyAsyncIterator
from fitz_py.connection import Connection
from fitz_py.domains._routes import is_exact_route_shape, is_selector_route_shape
from fitz_py.domains.base import DomainClient
from fitz_py.errors import NoticeError, domain_error
from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.messages import (
    MSG_NOTICE_NOTIFY,
    MSG_NOTICE_PUBLISH,
    MSG_NOTICE_SUBSCRIBE,
    MSG_NOTICE_UNSUBSCRIBE,
    MSG_NOTICE_UNSUBSCRIBE_ALL,
)
from fitz_py.protocol.response import parse_response
from fitz_py.types import BytesLike


def _empty_consumers() -> set[AsyncSubscription[Notice]]:
    return set()


@dataclass(frozen=True, slots=True)
class Notice:
    route: str
    body: bytes


@dataclass(slots=True)
class _Wire:
    sub_id: int
    consumers: set[AsyncSubscription[Notice]] = field(default_factory=_empty_consumers)


class NoticeClient(DomainClient):
    def __init__(self, connection: Connection) -> None:
        super().__init__(connection)
        self._by_pattern: dict[str, _Wire] = {}
        self._pattern_by_id: dict[int, str] = {}
        self._lock = asyncio.Lock()
        self._cleanup_tasks: set[asyncio.Task[None]] = set()
        self._pending_notifications: deque[tuple[int, Notice]] = deque(
            maxlen=connection.config.limits.subscription_buffer_size
        )
        connection.register_notification_handler(MSG_NOTICE_NOTIFY, self._notify)
        connection.on_reconnect(self._restore, domain="notice", registration="subscriptions")
        on_close = getattr(connection, "on_close", None)
        if callable(on_close):
            on_close(self._terminate)

    async def publish(self, route: str, body: BytesLike) -> None:
        if not is_exact_route_shape(route, "notice", 3):
            raise NoticeError(f"Invalid notice route: {route}", "INVALID_ROUTE")
        body = bytes(body)
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_u32_be(len(body))
        writer.write_bytes(body)
        await self.connection.send(MSG_NOTICE_PUBLISH, writer.build())

    def subscribe(self, pattern: str) -> LazyAsyncIterator[Notice]:
        if not is_selector_route_shape(pattern, "notice", 3, allow_realm_wildcard=True):
            raise NoticeError(f"Invalid notice pattern: {pattern}", "INVALID_ROUTE")
        return LazyAsyncIterator(lambda: self.open_subscription(pattern))

    async def open_subscription(self, pattern: str) -> AsyncSubscription[Notice]:
        if not is_selector_route_shape(pattern, "notice", 3, allow_realm_wildcard=True):
            raise NoticeError(f"Invalid notice pattern: {pattern}", "INVALID_ROUTE")
        async with self._lock:
            state = self._by_pattern.get(pattern)
            if state is None:
                state = _Wire(await self._subscribe_wire(pattern))
                self._by_pattern[pattern] = state
                self._pattern_by_id[state.sub_id] = pattern
            subscription: AsyncSubscription[Notice]

            async def close() -> None:
                async with self._lock:
                    current = self._by_pattern.get(pattern)
                    if current is None:
                        return
                    current.consumers.discard(subscription)
                    if current.consumers:
                        return
                    writer = BufferWriter()
                    writer.write_u64_be(current.sub_id)

                    async def unsubscribe() -> None:
                        response = parse_response(
                            await self.request_frame(MSG_NOTICE_UNSUBSCRIBE, writer.build())
                        )
                        if not response.success:
                            raise domain_error(
                                NoticeError,
                                "UNSUBSCRIBE",
                                response.error_code or 0,
                                response.error,
                            )
                        if response.data:
                            raise NoticeError(
                                "UNSUBSCRIBE response has trailing bytes", "INVALID_RESPONSE"
                            )

                    task = asyncio.create_task(unsubscribe())
                    succeeded = False
                    try:
                        await asyncio.shield(task)
                        succeeded = True
                    except asyncio.CancelledError:
                        await task
                        succeeded = True
                        raise
                    finally:
                        if succeeded:
                            self._by_pattern.pop(pattern, None)
                            self._pattern_by_id.pop(current.sub_id, None)

            subscription = AsyncSubscription(
                pattern, self.connection.config.limits.subscription_buffer_size, close
            )
            state.consumers.add(subscription)
            self._flush_pending(state)
            return subscription

    async def _subscribe_wire(self, pattern: str) -> int:
        writer = BufferWriter()
        writer.write_route(pattern)
        response = parse_response(await self.request_frame(MSG_NOTICE_SUBSCRIBE, writer.build()))
        if not response.success:
            raise domain_error(NoticeError, "SUBSCRIBE", response.error_code or 0, response.error)
        reader = BufferReader(response.data)
        if reader.read_u8() != 1:
            raise NoticeError("SUBSCRIBE response omitted its id", "INVALID_RESPONSE")
        sub_id = reader.read_u64_be()
        if not reader.is_eof():
            raise NoticeError("SUBSCRIBE response has trailing bytes", "INVALID_RESPONSE")
        return sub_id

    async def unsubscribe_all(self) -> None:
        """Remove every Notice subscription owned by this client session."""
        async with self._lock:
            response = parse_response(await self.request_frame(MSG_NOTICE_UNSUBSCRIBE_ALL, b""))
            if not response.success:
                raise domain_error(
                    NoticeError,
                    "UNSUBSCRIBE_ALL",
                    response.error_code or 0,
                    response.error,
                )
            if response.data:
                raise NoticeError("UNSUBSCRIBE_ALL response has trailing bytes", "INVALID_RESPONSE")
            states = tuple(self._by_pattern.values())
            self._by_pattern.clear()
            self._pattern_by_id.clear()
            for state in states:
                for consumer in state.consumers:
                    consumer.finish()

    def _notify(self, payload: bytes) -> None:
        reader = BufferReader(payload)
        sub_id = reader.read_u64_be()
        notice = Notice(reader.read_route(), reader.read_bytes(reader.read_u32_be()))
        if not reader.is_eof():
            raise NoticeError("NOTICE_NOTIFY has trailing bytes", "INVALID_RESPONSE")
        pattern = self._pattern_by_id.get(sub_id)
        state = self._by_pattern.get(pattern) if pattern is not None else None
        if state is None:
            self._pending_notifications.append((sub_id, notice))
            return
        dead = {consumer for consumer in state.consumers if not consumer.push(notice)}
        state.consumers.difference_update(dead)
        for consumer in dead:
            task = asyncio.create_task(consumer.aclose())
            self._cleanup_tasks.add(task)
            task.add_done_callback(self._cleanup_completed)

    async def _restore(self) -> None:
        async with self._lock:
            for pattern, state in list(self._by_pattern.items()):
                old_id = state.sub_id
                try:
                    state.sub_id = await self._subscribe_wire(pattern)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    self._by_pattern.pop(pattern, None)
                    self._pattern_by_id.pop(old_id, None)
                    for consumer in state.consumers:
                        consumer.fail(exc)
                    self.connection.report_restore_failure("notice", pattern, exc)
                    continue
                self._pattern_by_id.pop(old_id, None)
                self._pattern_by_id[state.sub_id] = pattern
                self._flush_pending(state)

    def _terminate(self) -> None:
        states = tuple(self._by_pattern.values())
        self._by_pattern.clear()
        self._pattern_by_id.clear()
        self._pending_notifications.clear()
        for state in states:
            for consumer in state.consumers:
                consumer.finish()

    def _cleanup_completed(self, task: asyncio.Task[None]) -> None:
        self._cleanup_tasks.discard(task)
        if not task.cancelled():
            task.exception()

    def _flush_pending(self, state: _Wire) -> None:
        retained: deque[tuple[int, Notice]] = deque(maxlen=self._pending_notifications.maxlen)
        while self._pending_notifications:
            sub_id, notice = self._pending_notifications.popleft()
            if sub_id == state.sub_id:
                for consumer in tuple(state.consumers):
                    consumer.push(notice)
            else:
                retained.append((sub_id, notice))
        self._pending_notifications = retained
