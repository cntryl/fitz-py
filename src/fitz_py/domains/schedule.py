"""Cron schedules, pagination, and delivery notifications."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from fitz_py._runtime import LazyAsyncIterator
from fitz_py.connection import Connection
from fitz_py.domains._routes import is_exact_route_shape, is_selector_route_shape
from fitz_py.domains._subscriptions import SubscriptionRegistry
from fitz_py.domains.base import DomainClient
from fitz_py.errors import ScheduleError, domain_error
from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.messages import (
    MSG_SCHEDULE_CANCEL,
    MSG_SCHEDULE_CREATE,
    MSG_SCHEDULE_LIST,
    MSG_SCHEDULE_NOTIFY,
    MSG_SCHEDULE_SUBSCRIBE,
    MSG_SCHEDULE_UNSUBSCRIBE,
)
from fitz_py.protocol.response import parse_response
from fitz_py.types import BytesLike


class DeliveryMode(StrEnum):
    BROADCAST = "broadcast"
    SINGLE = "single"


@dataclass(frozen=True, slots=True)
class ScheduleEntry:
    route: str
    cron: str
    delivery_mode: DeliveryMode
    payload: bytes

    @property
    def id(self) -> str:
        return self.route


@dataclass(frozen=True, slots=True)
class SchedulePage:
    entries: tuple[ScheduleEntry, ...]
    total_count: int


@dataclass(frozen=True, slots=True)
class ScheduleNotification:
    route: str
    payload: bytes


class ScheduleClient(DomainClient):
    def __init__(self, connection: Connection) -> None:
        super().__init__(connection)
        self._subscriptions = SubscriptionRegistry[ScheduleNotification](
            connection.config.limits.subscription_buffer_size,
            self._subscribe_wire,
            self._unsubscribe_wire,
        )
        connection.register_notification_handler(MSG_SCHEDULE_NOTIFY, self._notify)
        connection.on_reconnect(
            lambda: self._subscriptions.restore(
                domain="schedule",
                on_error=lambda registration, error: connection.report_restore_failure(
                    "schedule", registration, error
                ),
            ),
            domain="schedule",
            registration="notifications",
        )
        on_close = getattr(connection, "on_close", None)
        if callable(on_close):
            on_close(self._subscriptions.terminate)

    async def create(
        self,
        route: str,
        cron: str,
        *,
        delivery_mode: DeliveryMode | str,
        payload: BytesLike = b"",
    ) -> str:
        _route(route)
        if not cron:
            raise ValueError("cron must not be empty")
        mode = DeliveryMode(delivery_mode)
        payload = bytes(payload)
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_string(cron)
        writer.write_u8(0 if mode is DeliveryMode.BROADCAST else 1)
        writer.write_u32_be(len(payload))
        writer.write_bytes(payload)
        response = parse_response(
            await self.request_frame(MSG_SCHEDULE_CREATE, writer.build()), plain=True
        )
        if not response.success:
            raise ScheduleError(f"CREATE failed: {response.error}", "CREATE")
        if response.data:
            raise ScheduleError("CREATE response has trailing bytes", "INVALID_RESPONSE")
        return route

    async def cancel(self, route: str) -> None:
        _route(route)
        writer = BufferWriter()
        writer.write_route(route)
        response = parse_response(
            await self.request_frame(MSG_SCHEDULE_CANCEL, writer.build()), plain=True
        )
        if not response.success:
            raise ScheduleError(f"CANCEL failed: {response.error}", "CANCEL")
        if response.data:
            raise ScheduleError("CANCEL response has trailing bytes", "INVALID_RESPONSE")

    async def list_schedules(
        self, *, offset: int | None = None, limit: int | None = None
    ) -> SchedulePage:
        if offset is not None and offset < 0:
            raise ValueError("offset must be non-negative")
        if limit is not None and not 0 <= limit <= 1000:
            raise ValueError("limit must be between 0 and 1000")
        writer = BufferWriter()
        writer.write_optional_u64(offset)
        writer.write_optional_u64(limit)
        response = parse_response(await self.request_frame(MSG_SCHEDULE_LIST, writer.build()))
        if not response.success:
            raise domain_error(ScheduleError, "LIST", response.error_code or 0, response.error)
        reader = BufferReader(response.data)
        total_count = reader.read_u64_be()
        entries: list[ScheduleEntry] = []
        while True:
            marker = reader.read_u8()
            if marker == 0:
                break
            if marker != 1:
                raise ScheduleError("LIST response has an invalid entry marker", "INVALID_RESPONSE")
            route, cron = reader.read_string(), reader.read_string()
            mode_byte = reader.read_u8()
            if mode_byte not in {0, 1}:
                raise ScheduleError("Invalid delivery mode", "INVALID_RESPONSE")
            payload = reader.read_bytes(reader.read_u32_be())
            entries.append(
                ScheduleEntry(
                    route,
                    cron,
                    DeliveryMode.BROADCAST if mode_byte == 0 else DeliveryMode.SINGLE,
                    payload,
                )
            )
        if not reader.is_eof():
            raise ScheduleError("LIST response has trailing bytes", "INVALID_RESPONSE")
        return SchedulePage(tuple(entries), total_count)

    def subscribe(self, route: str) -> LazyAsyncIterator[ScheduleNotification]:
        _pattern(route)
        return LazyAsyncIterator(lambda: self._subscriptions.subscribe(route))

    async def _subscribe_wire(self, route: str) -> int:
        writer = BufferWriter()
        writer.write_route(route)
        response = parse_response(
            await self.request_frame(MSG_SCHEDULE_SUBSCRIBE, writer.build()), plain=True
        )
        if not response.success:
            raise ScheduleError(f"SUBSCRIBE failed: {response.error}", "SUBSCRIBE")
        reader = BufferReader(response.data)
        if reader.read_u8() != 1:
            raise ScheduleError("SUBSCRIBE response omitted its id", "INVALID_RESPONSE")
        sub_id = reader.read_u64_be()
        if not reader.is_eof():
            raise ScheduleError("SUBSCRIBE response has trailing bytes", "INVALID_RESPONSE")
        return sub_id

    async def _unsubscribe_wire(self, route: str) -> None:
        writer = BufferWriter()
        writer.write_route(route)
        response = parse_response(
            await self.request_frame(MSG_SCHEDULE_UNSUBSCRIBE, writer.build()), plain=True
        )
        if not response.success:
            raise ScheduleError(f"UNSUBSCRIBE failed: {response.error}", "UNSUBSCRIBE")
        if response.data:
            raise ScheduleError("UNSUBSCRIBE response has trailing bytes", "INVALID_RESPONSE")

    def _notify(self, payload: bytes) -> None:
        reader = BufferReader(payload)
        sub_id = reader.read_u64_be()
        notification = ScheduleNotification(
            reader.read_route(), reader.read_bytes(reader.read_u32_be())
        )
        if not reader.is_eof():
            raise ScheduleError("SCHEDULE_NOTIFY has trailing bytes", "INVALID_RESPONSE")
        self._subscriptions.publish(sub_id, notification)


def _route(route: str) -> None:
    if not is_exact_route_shape(route, "schedule", 4):
        raise ScheduleError(f"Invalid schedule route: {route}", "INVALID_ROUTE")


def _pattern(route: str) -> None:
    if not is_selector_route_shape(route, "schedule", 4, allow_realm_wildcard=True):
        raise ScheduleError(f"Invalid schedule pattern: {route}", "INVALID_ROUTE")
