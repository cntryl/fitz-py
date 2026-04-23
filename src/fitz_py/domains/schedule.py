from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from fitz_py.domains.base import DomainClient
from fitz_py.domains._routes import is_exact_route_shape
from fitz_py.errors import ScheduleError, schedule_error
from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.messages import (
    MSG_SCHEDULE_CANCEL,
    MSG_SCHEDULE_CREATE,
    MSG_SCHEDULE_LIST,
    MSG_SCHEDULE_NOTIFY,
    MSG_SCHEDULE_SUBSCRIBE,
    MSG_SCHEDULE_UNSUBSCRIBE,
)
from fitz_py.protocol.response import parse_standard_response

ScheduleHandler = Callable[["ScheduleNotification"], None | Awaitable[None]]


@dataclass(slots=True)
class ScheduleNotification:
    payload: bytes


@dataclass(slots=True)
class ScheduleEntry:
    id: str
    route: str
    cron: str
    payload: bytes


@dataclass(slots=True)
class _ScheduleSubscriptionState:
    sub_id: int
    handlers: dict[int, ScheduleHandler] = field(default_factory=dict)


class ScheduleSubscription:
    def __init__(
        self,
        sub_id: int,
        pattern: str,
        handler: ScheduleHandler,
        unsubscribe: Callable[[str, int], Awaitable[None]],
        handler_id: int,
    ) -> None:
        self.sub_id = sub_id
        self.pattern = pattern
        self.handler = handler
        self._unsubscribe = unsubscribe
        self._handler_id = handler_id

    async def unsubscribe(self) -> None:
        await self._unsubscribe(self.pattern, self._handler_id)


class ScheduleClient(DomainClient):
    def __init__(self, connection) -> None:
        super().__init__(connection)
        self._subscriptions_by_pattern: dict[str, _ScheduleSubscriptionState] = {}
        self._patterns_by_sub_id: dict[int, str] = {}
        self._initialized = False
        self._next_handler_id = 1
        self.connection.on_reconnect(self._restore_subscriptions)

    async def create(self, route: str, cron: str, payload: bytes = b"") -> str:
        _assert_schedule_route(route)
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_string(cron)
        writer.write_u32_be(len(payload))
        writer.write_bytes(payload)
        data = self._assert_success(
            await self.request_frame(MSG_SCHEDULE_CREATE, writer.build()), "CREATE"
        )
        reader = BufferReader(data)
        if not reader.is_eof() and reader.read_u8() == 1:
            return reader.read_string()
        return route

    async def cancel(self, route: str) -> None:
        _assert_schedule_route(route)
        writer = BufferWriter()
        writer.write_route(route)
        self._assert_success(
            await self.request_frame(MSG_SCHEDULE_CANCEL, writer.build()), "CANCEL"
        )

    async def list(
        self, *, offset: int | None = None, limit: int | None = None
    ) -> list[ScheduleEntry]:
        writer = BufferWriter()
        writer.write_optional_u64(offset)
        writer.write_optional_u64(limit)
        data = self._assert_success(
            await self.request_frame(MSG_SCHEDULE_LIST, writer.build()), "LIST"
        )
        reader = BufferReader(data)
        if reader.remaining_bytes() >= 8:
            reader.read_u64_be()
        entries: list[ScheduleEntry] = []
        while not reader.is_eof():
            if reader.read_u8() == 0:
                break
            route = reader.read_string()
            cron = reader.read_string()
            payload = reader.read_bytes(reader.read_u32_be())
            entries.append(ScheduleEntry(id=route, route=route, cron=cron, payload=payload))
        return entries

    async def subscribe(self, pattern: str, handler: ScheduleHandler) -> ScheduleSubscription:
        _assert_schedule_route(pattern)
        self._init_notify_handler()

        existing = self._subscriptions_by_pattern.get(pattern)
        if existing is None:
            sub_id = await self._subscribe_wire(pattern)
            existing = _ScheduleSubscriptionState(sub_id=sub_id)
            self._subscriptions_by_pattern[pattern] = existing
            self._patterns_by_sub_id[sub_id] = pattern

        handler_id = self._next_handler_id
        self._next_handler_id += 1
        existing.handlers[handler_id] = handler
        return ScheduleSubscription(
            existing.sub_id, pattern, handler, self._unsubscribe, handler_id
        )

    async def _subscribe_wire(self, pattern: str) -> int:
        writer = BufferWriter()
        writer.write_string(pattern)
        data = self._assert_success(
            await self.request_frame(MSG_SCHEDULE_SUBSCRIBE, writer.build()), "SUBSCRIBE"
        )
        reader = BufferReader(data)
        has_sub_id = reader.read_u8() if not reader.is_eof() else 0
        if has_sub_id != 1 or reader.remaining_bytes() < 8:
            raise ScheduleError("SUBSCRIBE response missing subscription id", "MISSING_SUB_ID")
        return reader.read_u64_be()

    async def _unsubscribe(self, pattern: str, handler_id: int) -> None:
        subscription = self._subscriptions_by_pattern.get(pattern)
        if subscription is None:
            return
        subscription.handlers.pop(handler_id, None)
        if subscription.handlers:
            return

        self._subscriptions_by_pattern.pop(pattern, None)
        self._patterns_by_sub_id.pop(subscription.sub_id, None)
        writer = BufferWriter()
        writer.write_string(pattern)
        self._assert_success(
            await self.request_frame(MSG_SCHEDULE_UNSUBSCRIBE, writer.build()), "UNSUBSCRIBE"
        )

    def _init_notify_handler(self) -> None:
        if self._initialized:
            return
        self._initialized = True

        def handler(payload: bytes) -> None:
            try:
                sub_id, actual_payload = _decode_schedule_notification(payload)
                pattern = self._patterns_by_sub_id.get(sub_id)
                if pattern is None:
                    return
                subscription = self._subscriptions_by_pattern.get(pattern)
                if subscription is None:
                    return
                notification = ScheduleNotification(payload=actual_payload)
                for callback in subscription.handlers.values():
                    result = callback(notification)
                    if asyncio.iscoroutine(result):
                        asyncio.create_task(result)
            except Exception:
                return

        self.connection.register_notification_handler(MSG_SCHEDULE_NOTIFY, handler)

    async def _restore_subscriptions(self) -> None:
        if not self._subscriptions_by_pattern:
            return
        snapshot = [
            (pattern, list(state.handlers.values()))
            for pattern, state in self._subscriptions_by_pattern.items()
        ]
        self._subscriptions_by_pattern.clear()
        self._patterns_by_sub_id.clear()
        for pattern, handlers in snapshot:
            for handler in handlers:
                await self.subscribe(pattern, handler)

    @staticmethod
    def _assert_success(payload: bytes, operation: str) -> bytes:
        result = parse_standard_response(payload)
        if result.success:
            return result.data
        error_message = result.error or f"{operation} failed"
        raise _map_schedule_protocol_error(f"{operation} failed: {error_message}")


def _assert_schedule_route(route: str) -> None:
    if not is_exact_route_shape(route, "schedule", 4):
        raise ScheduleError(
            f"Invalid schedule route: {route} (expected schedule://{{realm}}/{{area}}/{{resource}}/{{operation}}, no empty segments or wildcards)",
            "INVALID_ROUTE",
        )


def _decode_schedule_notification(payload: bytes) -> tuple[int, bytes]:
    reader = BufferReader(payload)
    sub_id = reader.read_u64_be()
    body = reader.read_bytes(reader.read_u32_be())
    return sub_id, body


def _map_schedule_protocol_error(message: str) -> ScheduleError:
    normalized = message.lower()
    if "not found" in normalized:
        return schedule_error(message, 1)
    if "task" in normalized and "not found" in normalized:
        return schedule_error(message, 2)
    if "cron" in normalized:
        return schedule_error(message, 3)
    if "delay" in normalized:
        return schedule_error(message, 4)
    if "timestamp" in normalized or "time" in normalized:
        return schedule_error(message, 5)
    if "invalid route" in normalized or "must be schedule://" in normalized:
        return ScheduleError(message, "INVALID_ROUTE")
    return ScheduleError(message, "ERROR")
