"""Queue operations and bounded async availability subscriptions."""

from __future__ import annotations

from dataclasses import dataclass

from fitz_py._runtime import LazyAsyncIterator
from fitz_py.connection import Connection
from fitz_py.domains._routes import is_exact_route_shape, is_selector_route_shape
from fitz_py.domains._subscriptions import SubscriptionRegistry
from fitz_py.domains.base import DomainClient
from fitz_py.errors import QueueError, StaleHandleError, domain_error
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
from fitz_py.protocol.response import parse_response
from fitz_py.types import BytesLike


@dataclass(frozen=True, slots=True)
class Availability:
    route: str
    ready: int
    delayed: int
    inflight: int


@dataclass(slots=True)
class QueueItem:
    route: str
    body: bytes
    _id: int
    _token: int
    _client: QueueClient
    _generation: int
    _closed: bool = False

    def _ensure_valid(self) -> None:
        if self._closed or self._generation != self._client.connection.generation:
            raise StaleHandleError("Queue item")

    async def extend(self, lease_seconds: int) -> None:
        self._ensure_valid()
        await self._client._extend(self.route, self._id, self._token, lease_seconds)  # noqa: SLF001

    async def complete(self) -> None:
        self._ensure_valid()
        await self._client._complete(self.route, self._id, self._token)  # noqa: SLF001
        self._closed = True


class QueueClient(DomainClient):
    def __init__(self, connection: Connection) -> None:
        super().__init__(connection)
        self._subscriptions = SubscriptionRegistry[Availability](
            connection.config.limits.subscription_buffer_size,
            self._subscribe_wire,
            self._unsubscribe_wire,
        )
        connection.register_notification_handler(MSG_QUEUE_AVAILABILITY_NOTIFY, self._notify)
        connection.on_reconnect(
            lambda: self._subscriptions.restore(
                domain="queue",
                on_error=lambda registration, error: connection.report_restore_failure(
                    "queue", registration, error
                ),
            ),
            domain="queue",
            registration="availability",
        )
        on_close = getattr(connection, "on_close", None)
        if callable(on_close):
            on_close(self._subscriptions.terminate)

    async def enqueue(self, route: str, body: BytesLike, *, delay: int | None = None) -> int:
        _exact(route)
        body = bytes(body)
        if delay is not None:
            _duration(delay, "delay")
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_u32_be(len(body))
        writer.write_bytes(body)
        seconds = delay or 0
        writer.write_u8(int(seconds > 0))
        if seconds:
            writer.write_u64_be(seconds)
        response = parse_response(await self.request_frame(MSG_QUEUE_ENQUEUE, writer.build()))
        if not response.success:
            raise domain_error(QueueError, "ENQUEUE", response.error_code or 0, response.error)
        reader = BufferReader(response.data)
        if reader.remaining_bytes() != 8:
            raise QueueError("ENQUEUE response must contain one item id", "INVALID_RESPONSE")
        return reader.read_u64_be()

    async def reserve(
        self,
        selector: str,
        *,
        lease: int,
        batch_size: int | None = None,
        wait: int | None = None,
    ) -> list[QueueItem]:
        _selector(selector)
        _duration(lease, "lease", positive=True)
        if wait is not None:
            _duration(wait, "wait")
        if batch_size is not None and not 1 <= batch_size <= 1024:
            raise ValueError("batch_size must be between 1 and 1024")
        writer = BufferWriter()
        writer.write_route(selector)
        writer.write_u64_be(lease)
        writer.write_u8(1)
        writer.write_u32_be(batch_size or 1)
        writer.write_u8(int(wait is not None and wait > 0))
        if wait is not None and wait > 0:
            writer.write_u64_be(wait)
        response = parse_response(await self.request_frame(MSG_QUEUE_RESERVE, writer.build()))
        if not response.success:
            raise domain_error(QueueError, "RESERVE", response.error_code or 0, response.error)
        reader = BufferReader(response.data)
        if reader.is_eof():
            return []
        wildcard = "*" in selector
        items: list[QueueItem] = []
        for _ in range(reader.read_u32_be()):
            route = reader.read_route() if wildcard else selector
            if not is_exact_route_shape(route, "queue", 3):
                raise QueueError("RESERVE returned an invalid route", "INVALID_RESPONSE")
            item_id = reader.read_u64_be()
            token = reader.read_u64_be()
            body = reader.read_bytes(reader.read_u32_be())
            items.append(
                QueueItem(
                    route,
                    body,
                    item_id,
                    token,
                    self,
                    self.connection.generation,
                )
            )
        if not reader.is_eof():
            raise QueueError("RESERVE response has trailing bytes", "INVALID_RESPONSE")
        return items

    def subscribe(self, selector: str) -> LazyAsyncIterator[Availability]:
        _selector(selector)
        return LazyAsyncIterator(lambda: self._subscriptions.subscribe(selector))

    async def _complete(self, route: str, item_id: int, token: int) -> None:
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_u64_be(item_id)
        writer.write_u64_be(token)
        response = parse_response(
            await self.request_frame(MSG_QUEUE_COMPLETE, writer.build()), plain=True
        )
        if not response.success:
            raise QueueError(f"COMPLETE failed: {response.error}", "COMPLETE")
        if response.data:
            raise QueueError("COMPLETE response has trailing bytes", "INVALID_RESPONSE")

    async def _extend(self, route: str, item_id: int, token: int, lease: int) -> None:
        _duration(lease, "lease", positive=True)
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_u64_be(item_id)
        writer.write_u64_be(token)
        writer.write_u64_be(lease)
        response = parse_response(
            await self.request_frame(MSG_QUEUE_EXTEND, writer.build()), plain=True
        )
        if not response.success:
            raise QueueError(f"EXTEND failed: {response.error}", "EXTEND")
        if response.data:
            raise QueueError("EXTEND response has trailing bytes", "INVALID_RESPONSE")

    async def _subscribe_wire(self, selector: str) -> int:
        writer = BufferWriter()
        writer.write_route(selector)
        response = parse_response(
            await self.request_frame(MSG_QUEUE_SUBSCRIBE, writer.build()), plain=True
        )
        if not response.success:
            raise QueueError(f"SUBSCRIBE failed: {response.error}", "SUBSCRIBE")
        reader = BufferReader(response.data)
        if reader.read_u8() != 1:
            raise QueueError("SUBSCRIBE response omitted its id", "INVALID_RESPONSE")
        sub_id = reader.read_u64_be()
        if not reader.is_eof():
            raise QueueError("SUBSCRIBE response has trailing bytes", "INVALID_RESPONSE")
        return sub_id

    async def _unsubscribe_wire(self, selector: str) -> None:
        writer = BufferWriter()
        writer.write_route(selector)
        response = parse_response(
            await self.request_frame(MSG_QUEUE_UNSUBSCRIBE, writer.build()), plain=True
        )
        if not response.success:
            raise QueueError(f"UNSUBSCRIBE failed: {response.error}", "UNSUBSCRIBE")
        if response.data:
            raise QueueError("UNSUBSCRIBE response has trailing bytes", "INVALID_RESPONSE")

    def _notify(self, payload: bytes) -> None:
        reader = BufferReader(payload)
        sub_id = reader.read_u64_be()
        item = Availability(
            reader.read_route(),
            reader.read_u64_be(),
            reader.read_u64_be(),
            reader.read_u64_be(),
        )
        if not reader.is_eof():
            raise QueueError("Availability notification has trailing bytes", "INVALID_RESPONSE")
        self._subscriptions.publish(sub_id, item)


def _exact(route: str) -> None:
    if not is_exact_route_shape(route, "queue", 3):
        raise QueueError(f"Invalid queue route: {route}", "INVALID_ROUTE")


def _selector(route: str) -> None:
    if not is_selector_route_shape(route, "queue", 3, allow_realm_wildcard=True):
        raise QueueError(f"Invalid queue selector: {route}", "INVALID_ROUTE")


def _duration(value: object, name: str, *, positive: bool = False) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer number of seconds")
    if value < (1 if positive else 0):
        qualifier = "positive" if positive else "non-negative"
        raise ValueError(f"{name} must be {qualifier}")
