from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from fitz_py.domains.base import DomainClient
from fitz_py.errors import NoticeError
from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.messages import (
    MSG_NOTICE_NOTIFY,
    MSG_NOTICE_PUBLISH,
    MSG_NOTICE_SUBSCRIBE,
    MSG_NOTICE_UNSUBSCRIBE,
)

NoticeHandler = Callable[["NoticeMessage"], None | Awaitable[None]]


@dataclass(slots=True)
class NoticeMessage:
    route: str
    body: bytes


class NoticeSubscription:
    def __init__(self, sub_id: int, pattern: str, unsubscribe: Callable[[int], Awaitable[None]]) -> None:
        self.sub_id = sub_id
        self.pattern = pattern
        self._unsubscribe = unsubscribe

    async def unsubscribe(self) -> None:
        await self._unsubscribe(self.sub_id)


class NoticeClient(DomainClient):
    def __init__(self, connection) -> None:
        super().__init__(connection)
        self._subscriptions: dict[int, tuple[str, NoticeHandler]] = {}
        self._initialized = False
        self.connection.on_reconnect(self._restore_subscriptions)

    async def publish(self, route: str, body: bytes) -> None:
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_u32_be(len(body))
        writer.write_bytes(body)
        cancel_optional = self.connection.get_multiplexer().expect_optional_response(MSG_NOTICE_PUBLISH)
        try:
            await self.connection.send_fire_and_forget(MSG_NOTICE_PUBLISH, writer.build())
        except Exception:
            cancel_optional()
            raise

    async def subscribe(self, pattern: str, handler: NoticeHandler) -> NoticeSubscription:
        self._init_notify_handler()
        writer = BufferWriter()
        writer.write_route(pattern)
        reader = BufferReader(await self.request_frame(MSG_NOTICE_SUBSCRIBE, writer.build()))
        status = reader.read_u8()
        if status != 0:
            raise NoticeError(f"SUBSCRIBE failed with status {status}", "SUBSCRIBE_FAILED", status)
        has_sub_id = reader.read_u8() if not reader.is_eof() else 0
        if has_sub_id != 1 or reader.remaining_bytes() < 8:
            raise NoticeError("SUBSCRIBE response missing subscription id", "MISSING_SUB_ID")
        sub_id = reader.read_u64_be()
        self._subscriptions[sub_id] = (pattern, handler)
        return NoticeSubscription(sub_id, pattern, self._unsubscribe)

    async def _unsubscribe(self, sub_id: int) -> None:
        subscription = self._subscriptions.pop(sub_id, None)
        if subscription is None:
            return
        writer = BufferWriter()
        writer.write_route(subscription[0])
        await self.request_frame(MSG_NOTICE_UNSUBSCRIBE, writer.build())

    def _init_notify_handler(self) -> None:
        if self._initialized:
            return
        self._initialized = True

        def handler(payload: bytes) -> None:
            try:
                reader = BufferReader(payload)
                sub_id = reader.read_u64_be()
                route = reader.read_route()
                body = reader.read_bytes(reader.read_u32_be())
                subscription = self._subscriptions.get(sub_id)
                if subscription is None:
                    return
                result = subscription[1](NoticeMessage(route=route, body=body))
                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception:
                return

        self.connection.register_notification_handler(MSG_NOTICE_NOTIFY, handler)

    async def _restore_subscriptions(self) -> None:
        if not self._subscriptions:
            return
        snapshot = list(self._subscriptions.values())
        self._subscriptions.clear()
        for pattern, handler in snapshot:
            await self.subscribe(pattern, handler)
