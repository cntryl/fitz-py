"""Queue domain client, queue items, and availability subscriptions."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from fitz_py.domains._routes import is_exact_route_shape, is_selector_route_shape
from fitz_py.domains.base import DomainClient
from fitz_py.errors import QueueError, queue_error
from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.messages import (
    MSG_QUEUE_AVAILABILITY_NOTIFY,
    MSG_QUEUE_COMPLETE,
    MSG_QUEUE_ENQUEUE,
    MSG_QUEUE_EXTEND,
    MSG_QUEUE_RESERVE,
    MSG_QUEUE_SUBSCRIBE,
    MSG_QUEUE_UNSUBSCRIBE,
)

QueueAvailabilityHandler = Callable[[str], None | Awaitable[None]]


class QueueSubscription:
    """Handle for an active queue availability subscription."""

    def __init__(
        self, sub_id: int, pattern: str, unsubscribe: Callable[[int], Awaitable[None]]
    ) -> None:
        self.sub_id = sub_id
        self.pattern = pattern
        self._unsubscribe = unsubscribe

    async def unsubscribe(self) -> None:
        await self._unsubscribe(self.sub_id)


@dataclass(slots=True)
class QueueItem:
    """Reserved queue item with completion and lease-extension helpers."""

    route: str
    id: int
    token: int
    body: bytes
    _client: "QueueClient"

    async def extend(self, lease_seconds: int) -> None:
        await self._client._extend(self.route, self.id, self.token, lease_seconds)

    async def complete(self) -> None:
        await self._client._complete(self.route, self.id, self.token)

    async def complete_with_token(self, token: int) -> None:
        await self._client._complete(self.route, self.id, token)


class QueueClient(DomainClient):
    """Queue domain operations for enqueue, reserve, and availability subscribe."""

    def __init__(self, connection) -> None:
        super().__init__(connection)
        self._subscriptions: dict[int, tuple[str, QueueAvailabilityHandler]] = {}
        self._initialized = False
        self.connection.on_reconnect(self._restore_subscriptions)

    async def enqueue(self, route: str, body: bytes, *, delay_ms: int | None = None) -> int:
        _assert_queue_route(route)
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_u32_be(len(body))
        writer.write_bytes(body)
        delay_seconds = (delay_ms or 0) // 1000
        writer.write_u8(1 if delay_seconds > 0 else 0)
        if delay_seconds > 0:
            writer.write_u64_be(delay_seconds)
        reader = BufferReader(await self.request_frame(MSG_QUEUE_ENQUEUE, writer.build()))
        status = reader.read_u8()
        if status != 0:
            raise queue_error(f"ENQUEUE failed with status {status}", status)
        return reader.read_u64_be() if not reader.is_eof() else 0

    async def reserve(
        self,
        route: str,
        lease_seconds: int,
        *,
        batch_size: int = 1,
        wait_seconds: int = 0,
    ) -> list[QueueItem]:
        _assert_queue_reserve_route(route)
        if wait_seconds <= 0:
            return await self._reserve_once(route, lease_seconds, batch_size)

        items = await self._reserve_once(route, lease_seconds, batch_size)
        if items:
            return items

        availability = asyncio.Event()
        subscription = await self.subscribe(route, lambda _route: availability.set())
        deadline = asyncio.get_running_loop().time() + wait_seconds

        try:
            while True:
                items = await self._reserve_once(route, lease_seconds, batch_size)
                if items:
                    return items

                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    return []

                try:
                    await asyncio.wait_for(availability.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    return []
                finally:
                    availability.clear()
        finally:
            with contextlib.suppress(Exception):
                await subscription.unsubscribe()

    async def _reserve_once(
        self,
        route: str,
        lease_seconds: int,
        batch_size: int,
    ) -> list[QueueItem]:
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_u64_be(lease_seconds)
        writer.write_u8(1 if batch_size > 0 else 0)
        if batch_size > 0:
            writer.write_u32_be(batch_size)
        reader = BufferReader(await self.request_frame(MSG_QUEUE_RESERVE, writer.build()))
        status = reader.read_u8()
        if status != 0:
            raise queue_error(f"RESERVE failed with status {status}", status)
        if reader.is_eof():
            return []
        count = reader.read_u32_be()
        items: list[QueueItem] = []
        for _ in range(count):
            item_id = reader.read_u64_be()
            token = reader.read_u64_be()
            body = reader.read_bytes(reader.read_u32_be())
            items.append(QueueItem(route=route, id=item_id, token=token, body=body, _client=self))
        return items

    async def subscribe(self, pattern: str, handler: QueueAvailabilityHandler) -> QueueSubscription:
        _assert_queue_subscription_pattern(pattern)
        self._init_notify_handler()
        writer = BufferWriter()
        writer.write_route(pattern)
        reader = BufferReader(await self.request_frame(MSG_QUEUE_SUBSCRIBE, writer.build()))
        status = reader.read_u8()
        if status != 0:
            raise queue_error(f"SUBSCRIBE failed with status {status}", status)
        has_sub_id = reader.read_u8() if not reader.is_eof() else 0
        if has_sub_id != 1 or reader.is_eof():
            raise QueueError("SUBSCRIBE response missing subscription id", "MISSING_SUB_ID")
        sub_id = reader.read_u64_be()
        self._subscriptions[sub_id] = (pattern, handler)
        return QueueSubscription(sub_id, pattern, self._unsubscribe)

    async def _complete(self, route: str, item_id: int, token: int) -> None:
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_u64_be(item_id)
        writer.write_u64_be(token)
        reader = BufferReader(await self.request_frame(MSG_QUEUE_COMPLETE, writer.build()))
        status = reader.read_u8()
        if status != 0:
            raise queue_error(f"COMPLETE failed with status {status}", status)

    async def _extend(self, route: str, item_id: int, token: int, lease_seconds: int) -> None:
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_u64_be(item_id)
        writer.write_u64_be(token)
        writer.write_u64_be(lease_seconds)
        reader = BufferReader(await self.request_frame(MSG_QUEUE_EXTEND, writer.build()))
        status = reader.read_u8()
        if status != 0:
            raise queue_error(f"EXTEND failed with status {status}", status)

    async def _unsubscribe(self, sub_id: int) -> None:
        subscription = self._subscriptions.pop(sub_id, None)
        if subscription is None:
            return
        writer = BufferWriter()
        writer.write_route(subscription[0])
        await self.request_frame(MSG_QUEUE_UNSUBSCRIBE, writer.build())

    def _init_notify_handler(self) -> None:
        if self._initialized:
            return
        self._initialized = True

        def handler(payload: bytes) -> None:
            try:
                reader = BufferReader(payload)
                sub_id = reader.read_u64_be()
                route = reader.read_route()
                if not reader.is_eof():
                    reader.read_bytes(reader.read_u32_be())
                subscription = self._subscriptions.get(sub_id)
                if subscription is None:
                    return
                result = subscription[1](route)
                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception:
                return

        self.connection.register_notification_handler(MSG_QUEUE_AVAILABILITY_NOTIFY, handler)

    async def _restore_subscriptions(self) -> None:
        if not self._subscriptions:
            return
        snapshot = list(self._subscriptions.values())
        self._subscriptions.clear()
        for pattern, handler in snapshot:
            await self.subscribe(pattern, handler)


def _assert_queue_route(route: str) -> None:
    if not is_exact_route_shape(route, "queue", 3):
        raise QueueError(
            f"Invalid queue route: {route} (expected queue://{{realm}}/{{area}}/{{resource}}, no empty segments or wildcards)",
            "INVALID_ROUTE",
        )


def _assert_queue_reserve_route(route: str) -> None:
    if not is_selector_route_shape(route, "queue", 3):
        raise QueueError(
            f"Invalid queue route: {route} (expected queue://{{realm}}/{{area}}/{{resource}} or queue://{{realm}}/{{area}}/*, no empty segments or wildcards)",
            "INVALID_ROUTE",
        )


def _assert_queue_subscription_pattern(pattern: str) -> None:
    if not is_selector_route_shape(pattern, "queue", 3, allow_realm_wildcard=True):
        raise QueueError(
            f"Invalid queue pattern: {pattern} (expected queue://{{realm}}/{{area}}/{{resource}}, queue://{{realm}}/{{area}}/*, or queue://{{realm}}/**)",
            "INVALID_ROUTE",
        )
