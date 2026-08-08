"""Cron schedules, pagination, and delivery notifications."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from fitz_py._runtime import AsyncSubscription
from fitz_py.domains._routes import is_exact_route_shape
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
    def __init__(self, connection) -> None:
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

    async def create(
        self,
        route: str,
        cron: str,
        *,
        delivery_mode: DeliveryMode = DeliveryMode.SINGLE,
        payload: bytes = b"",
    ) -> str:
        _route(route)
        if not cron:
            raise ValueError("cron must not be empty")
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_string(cron)
        writer.write_u8(0 if delivery_mode is DeliveryMode.BROADCAST else 1)
        writer.write_u32_be(len(payload))
        writer.write_bytes(payload)
        response = parse_response(
            await self.request_frame(MSG_SCHEDULE_CREATE, writer.build()), plain=True
        )
        if not response.success:
            raise ScheduleError(f"CREATE failed: {response.error}", "CREATE")
        reader = BufferReader(response.data)
        if reader.is_eof():
            return route
        if reader.read_u8() != 1:
            raise ScheduleError("Invalid schedule id flag", "INVALID_RESPONSE")
        schedule_id = reader.read_string()
        if not reader.is_eof():
            raise ScheduleError("CREATE response has trailing bytes", "INVALID_RESPONSE")
        return schedule_id

    async def cancel(self, route: str) -> None:
        _route(route)
        writer = BufferWriter()
        writer.write_route(route)
        response = parse_response(
            await self.request_frame(MSG_SCHEDULE_CANCEL, writer.build()), plain=True
        )
        if not response.success:
            raise ScheduleError(f"CANCEL failed: {response.error}", "CANCEL")

    async def list(self, *, offset: int | None = None, limit: int | None = None) -> SchedulePage:
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
        while reader.read_u8() == 1:
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

    async def subscribe(self, route: str) -> AsyncSubscription[ScheduleNotification]:
        _route(route)
        return await self._subscriptions.subscribe(route)

    async def _subscribe_wire(self, route: str) -> int:
        writer = BufferWriter()
        writer.write_route(route)
        response = parse_response(await self.request_frame(MSG_SCHEDULE_SUBSCRIBE, writer.build()))
        if not response.success:
            raise domain_error(ScheduleError, "SUBSCRIBE", response.error_code or 0, response.error)
        reader = BufferReader(response.data)
        if reader.read_u8() != 1:
            raise ScheduleError("SUBSCRIBE response omitted its id", "INVALID_RESPONSE")
        return reader.read_u64_be()

    async def _unsubscribe_wire(self, route: str) -> None:
        writer = BufferWriter()
        writer.write_route(route)
        response = parse_response(
            await self.request_frame(MSG_SCHEDULE_UNSUBSCRIBE, writer.build()), plain=True
        )
        if not response.success:
            raise ScheduleError(f"UNSUBSCRIBE failed: {response.error}", "UNSUBSCRIBE")

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
