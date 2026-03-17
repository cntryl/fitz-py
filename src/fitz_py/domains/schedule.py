from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from fitz_py.domains.base import DomainClient
from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.messages import (
    MSG_SCHEDULE_CANCEL,
    MSG_SCHEDULE_CREATE,
    MSG_SCHEDULE_LIST,
    MSG_SCHEDULE_NOTIFY,
    MSG_SCHEDULE_SUBSCRIBE,
    MSG_SCHEDULE_UNSUBSCRIBE,
)
from fitz_py.protocol.response import assert_success

ScheduleHandler = Callable[[bytes], None | Awaitable[None]]


@dataclass(slots=True)
class ScheduleEntry:
    id: str
    route: str
    cron: str
    payload: bytes


class ScheduleSubscription:
    def __init__(
        self,
        sub_id: int | None,
        pattern: str,
        unsubscribe: Callable[[str], Awaitable[None]],
    ) -> None:
        self.sub_id = sub_id
        self.pattern = pattern
        self._unsubscribe = unsubscribe

    async def unsubscribe(self) -> None:
        await self._unsubscribe(self.pattern)


class ScheduleClient(DomainClient):
    def __init__(self, connection) -> None:
        super().__init__(connection)
        self._subscriptions: dict[str, tuple[int | None, ScheduleHandler]] = {}
        self._initialized = False
        self.connection.on_reconnect(self._restore_subscriptions)

    async def create(self, route: str, cron: str, payload: bytes) -> str | None:
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_string(cron)
        writer.write_u32_be(len(payload))
        writer.write_bytes(payload)
        data = assert_success(
            await self.request_frame(MSG_SCHEDULE_CREATE, writer.build()), "CREATE"
        )
        reader = BufferReader(data)
        if not reader.is_eof() and reader.read_u8() == 1:
            return reader.read_string()
        return None

    async def cancel(self, route: str) -> None:
        writer = BufferWriter()
        writer.write_route(route)
        assert_success(await self.request_frame(MSG_SCHEDULE_CANCEL, writer.build()), "CANCEL")

    async def list(
        self, *, offset: int | None = None, limit: int | None = None
    ) -> list[ScheduleEntry]:
        writer = BufferWriter()
        writer.write_optional_u64(offset)
        writer.write_optional_u64(limit)
        data = assert_success(await self.request_frame(MSG_SCHEDULE_LIST, writer.build()), "LIST")
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
        self._init_notify_handler()
        writer = BufferWriter()
        writer.write_string(pattern)
        data = assert_success(
            await self.request_frame(MSG_SCHEDULE_SUBSCRIBE, writer.build()),
            "SUBSCRIBE",
        )
        reader = BufferReader(data)
        sub_id = reader.read_u64_be() if not reader.is_eof() and reader.read_u8() == 1 else None
        self._subscriptions[pattern] = (sub_id, handler)
        return ScheduleSubscription(sub_id, pattern, self._unsubscribe)

    async def _unsubscribe(self, pattern: str) -> None:
        if pattern not in self._subscriptions:
            return
        self._subscriptions.pop(pattern, None)
        writer = BufferWriter()
        writer.write_string(pattern)
        assert_success(
            await self.request_frame(MSG_SCHEDULE_UNSUBSCRIBE, writer.build()),
            "UNSUBSCRIBE",
        )

    def _init_notify_handler(self) -> None:
        if self._initialized:
            return
        self._initialized = True

        def handler(payload: bytes) -> None:
            try:
                actual_payload = _decode_schedule_notification(payload)
                for _, callback in self._subscriptions.values():
                    result = callback(actual_payload)
                    if asyncio.iscoroutine(result):
                        asyncio.create_task(result)
            except Exception:
                return

        self.connection.register_notification_handler(MSG_SCHEDULE_NOTIFY, handler)

    async def _restore_subscriptions(self) -> None:
        if not self._subscriptions:
            return
        snapshot = list(self._subscriptions.items())
        self._subscriptions.clear()
        for pattern, (_, handler) in snapshot:
            await self.subscribe(pattern, handler)


def _decode_schedule_notification(payload: bytes) -> bytes:
    if len(payload) < 4:
        return b""
    bytes_only_length = int.from_bytes(payload[:4], "big")
    if bytes_only_length == len(payload) - 4:
        return payload[4:]
    reader = BufferReader(payload)
    reader.read_u64_be()
    return reader.read_bytes(reader.read_u32_be())
