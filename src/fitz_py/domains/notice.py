"""Notice domain publish/subscribe client and notification models."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from fitz_py.domains._routes import is_exact_route_shape, is_selector_route_shape
from fitz_py.domains.base import DomainClient
from fitz_py.errors import NoticeError, notice_error
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
    """Notice payload delivered to a subscriber callback."""

    route: str
    body: bytes


@dataclass(slots=True)
class _NoticeSubscriptionState:
    """Internal handler registry for a single subscribed notice pattern."""

    sub_id: int
    handlers: dict[int, NoticeHandler] = field(default_factory=dict)


class NoticeSubscription:
    """Handle for an active notice pattern subscription."""

    def __init__(
        self,
        sub_id: int,
        pattern: str,
        handler: NoticeHandler,
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


class NoticeClient(DomainClient):
    """Notice domain operations for publish and pattern subscriptions."""

    def __init__(self, connection) -> None:
        super().__init__(connection)
        self._subscriptions_by_pattern: dict[str, _NoticeSubscriptionState] = {}
        self._patterns_by_sub_id: dict[int, str] = {}
        self._initialized = False
        self._next_handler_id = 1
        self.connection.on_reconnect(self._restore_subscriptions)

    async def publish(self, route: str, body: bytes) -> None:
        _assert_notice_route(route)
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_u32_be(len(body))
        writer.write_bytes(body)
        cancel_optional = self.connection.get_multiplexer().expect_optional_response(
            MSG_NOTICE_PUBLISH
        )
        try:
            await self.connection.send_fire_and_forget(MSG_NOTICE_PUBLISH, writer.build())
        except Exception:
            cancel_optional()
            raise

    async def subscribe(self, pattern: str, handler: NoticeHandler) -> NoticeSubscription:
        _assert_notice_pattern(pattern)
        self._init_notify_handler()
        existing = self._subscriptions_by_pattern.get(pattern)
        if existing is None:
            sub_id = await self._subscribe_wire(pattern)
            existing = _NoticeSubscriptionState(sub_id=sub_id)
            self._subscriptions_by_pattern[pattern] = existing
            self._patterns_by_sub_id[sub_id] = pattern

        handler_id = self._next_handler_id
        self._next_handler_id += 1
        existing.handlers[handler_id] = handler
        return NoticeSubscription(existing.sub_id, pattern, handler, self._unsubscribe, handler_id)

    async def _subscribe_wire(self, pattern: str) -> int:
        writer = BufferWriter()
        writer.write_route(pattern)
        reader = BufferReader(await self.request_frame(MSG_NOTICE_SUBSCRIBE, writer.build()))
        status = reader.read_u8()
        if status != 0:
            raise notice_error(f"SUBSCRIBE failed with status {status}", status)
        has_sub_id = reader.read_u8() if not reader.is_eof() else 0
        if has_sub_id != 1 or reader.remaining_bytes() < 8:
            raise NoticeError("SUBSCRIBE response missing subscription id", "MISSING_SUB_ID")
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
        writer.write_u64_be(subscription.sub_id)
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
                pattern = self._patterns_by_sub_id.get(sub_id)
                if pattern is None:
                    return
                subscription = self._subscriptions_by_pattern.get(pattern)
                if subscription is None:
                    return
                notification = NoticeMessage(route=route, body=body)
                for callback in subscription.handlers.values():
                    result = callback(notification)
                    if asyncio.iscoroutine(result):
                        asyncio.create_task(result)
            except Exception:
                return

        self.connection.register_notification_handler(MSG_NOTICE_NOTIFY, handler)

    async def _restore_subscriptions(self) -> None:
        if not self._subscriptions_by_pattern:
            return
        snapshot = [
            (pattern, dict(state.handlers))
            for pattern, state in self._subscriptions_by_pattern.items()
        ]
        self._subscriptions_by_pattern.clear()
        self._patterns_by_sub_id.clear()
        for pattern, handlers in snapshot:
            sub_id = await self._subscribe_wire(pattern)
            self._subscriptions_by_pattern[pattern] = _NoticeSubscriptionState(
                sub_id=sub_id,
                handlers=handlers,
            )
            self._patterns_by_sub_id[sub_id] = pattern


def _assert_notice_route(route: str) -> None:
    if not is_exact_route_shape(route, "notice", 3):
        raise NoticeError(
            f"Invalid notice route: {route} (expected notice://{{realm}}/{{area}}/{{resource}}, no empty segments or wildcards)",
            "INVALID_ROUTE",
        )


def _assert_notice_pattern(pattern: str) -> None:
    if not is_selector_route_shape(pattern, "notice", 3, allow_realm_wildcard=True):
        raise NoticeError(
            f"Invalid notice pattern: {pattern} (expected notice://{{realm}}/{{area}}/{{resource}}, notice://{{realm}}/{{area}}/*, or notice://{{realm}}/**)",
            "INVALID_ROUTE",
        )
