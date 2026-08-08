"""Transactional KV operations and mutation subscriptions."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from enum import StrEnum

from fitz_py._runtime import AsyncSubscription
from fitz_py.domains._routes import is_exact_route_shape, is_selector_route_shape
from fitz_py.domains._subscriptions import SubscriptionRegistry
from fitz_py.domains.base import DomainClient
from fitz_py.errors import KvError, StaleHandleError, domain_error
from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.messages import (
    MSG_KV_BEGIN,
    MSG_KV_COMMIT,
    MSG_KV_DELETE,
    MSG_KV_DELETE_RANGE,
    MSG_KV_GET,
    MSG_KV_INSERT,
    MSG_KV_NOTIFY,
    MSG_KV_PUT,
    MSG_KV_ROLLBACK,
    MSG_KV_SCAN,
    MSG_KV_SUBSCRIBE,
    MSG_KV_UNSUBSCRIBE,
)


class KvMode(StrEnum):
    READ_ONLY = "read_only"
    READ_WRITE = "read_write"


class KvDurability(StrEnum):
    BUFFERED = "buffered"
    SYNC = "sync"


@dataclass(frozen=True, slots=True)
class KvGetResult:
    found: bool
    value: bytes | None = None


@dataclass(frozen=True, slots=True)
class KvPair:
    key: bytes
    value: bytes


@dataclass(frozen=True, slots=True)
class KvScanPage:
    entries: tuple[KvPair, ...]
    has_more: bool


@dataclass(frozen=True, slots=True)
class KvNotification:
    route: str
    mutation_count: int


class KvTransaction:
    def __init__(self, connection, route: str, tx_id: int) -> None:
        self._connection = connection
        self._route = route
        self._tx_id = tx_id
        self._generation = connection.generation
        self._closed = False
        self._lock = asyncio.Lock()
        self._disconnect = connection.on_disconnect(self._invalidate)

    async def __aenter__(self) -> KvTransaction:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.rollback()

    def _ensure_open(self) -> None:
        if self._generation != self._connection.generation:
            raise StaleHandleError("KV transaction")
        if self._closed:
            raise KvError("Transaction is closed", "TX_CLOSED")

    def _invalidate(self) -> None:
        self._closed = True

    async def get(self, key: bytes) -> KvGetResult:
        async with self._lock:
            self._ensure_open()

            async def operation() -> KvGetResult:
                writer = self._prefix()
                writer.write_u32_be(len(key))
                writer.write_bytes(key)
                reader = BufferReader(await self._connection.request(MSG_KV_GET, writer.build()))
                self._status(reader, "GET")
                found = not reader.is_eof() and reader.read_u8() == 1
                if not found:
                    return KvGetResult(False)
                return KvGetResult(True, reader.read_bytes(reader.read_u32_be()))

            return await self._connection.run_with_retry(operation, replay_safe=True)

    async def put(self, key: bytes, value: bytes) -> None:
        await self._write(MSG_KV_PUT, "PUT", key, value)

    async def insert(self, key: bytes, value: bytes) -> None:
        await self._write(MSG_KV_INSERT, "INSERT", key, value)

    async def delete(self, key: bytes) -> None:
        async with self._lock:
            self._ensure_open()
            writer = self._prefix()
            writer.write_u32_be(len(key))
            writer.write_bytes(key)
            self._status(
                BufferReader(await self._connection.request(MSG_KV_DELETE, writer.build())),
                "DELETE",
            )

    async def delete_range(self, start_key: bytes, end_key: bytes) -> None:
        if start_key >= end_key:
            raise KvError("Range start must be less than end", "INVALID_RANGE")
        async with self._lock:
            self._ensure_open()
            writer = self._prefix()
            for key in (start_key, end_key):
                writer.write_u32_be(len(key))
                writer.write_bytes(key)
            self._status(
                BufferReader(await self._connection.request(MSG_KV_DELETE_RANGE, writer.build())),
                "DELETE_RANGE",
            )

    async def scan_page(
        self,
        *,
        start_key: bytes | None = None,
        end_key: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
    ) -> KvScanPage:
        if start_key is not None and end_key is not None and start_key >= end_key:
            raise KvError("Range start must be less than end", "INVALID_RANGE")
        async with self._lock:
            self._ensure_open()

            async def operation() -> KvScanPage:
                writer = self._prefix()
                for value in (start_key, end_key):
                    writer.write_u8(0 if value is None else 1)
                    if value is not None:
                        writer.write_u32_be(len(value))
                        writer.write_bytes(value)
                writer.write_u8(0 if limit is None else 1)
                if limit is not None:
                    writer.write_u32_be(limit)
                writer.write_u8(1 if reverse else 0)
                reader = BufferReader(await self._connection.request(MSG_KV_SCAN, writer.build()))
                self._status(reader, "SCAN")
                count = 0 if reader.is_eof() else reader.read_u32_be()
                entries = tuple(
                    KvPair(
                        reader.read_bytes(reader.read_u32_be()),
                        reader.read_bytes(reader.read_u32_be()),
                    )
                    for _ in range(count)
                )
                return KvScanPage(entries, not reader.is_eof() and reader.read_u8() == 1)

            return await self._connection.run_with_retry(operation, replay_safe=True)

    async def scan(self, **options) -> AsyncIterator[KvPair]:
        page = await self.scan_page(**options)
        if page.has_more and options.get("limit") is None:
            raise KvError("Unbounded scan was truncated", "SCAN_TRUNCATED")
        for entry in page.entries:
            yield entry

    async def commit(self) -> None:
        await self._finish(MSG_KV_COMMIT, "COMMIT")

    async def rollback(self) -> None:
        if self._closed:
            return
        try:
            await self._finish(MSG_KV_ROLLBACK, "ROLLBACK")
        except Exception:
            self._closed = True

    async def _write(self, message_type: int, operation: str, key: bytes, value: bytes) -> None:
        async with self._lock:
            self._ensure_open()
            writer = self._prefix()
            for item in (key, value):
                writer.write_u32_be(len(item))
                writer.write_bytes(item)
            self._status(
                BufferReader(await self._connection.request(message_type, writer.build())),
                operation,
            )

    async def _finish(self, message_type: int, operation: str) -> None:
        async with self._lock:
            self._ensure_open()
            self._closed = True
            self._disconnect()
            reader = BufferReader(
                await self._connection.request(message_type, self._prefix().build())
            )
            self._status(reader, operation)

    def _prefix(self) -> BufferWriter:
        writer = BufferWriter()
        writer.write_u64_be(self._tx_id)
        writer.write_route(self._route)
        return writer

    @staticmethod
    def _status(reader: BufferReader, operation: str) -> None:
        status = reader.read_u8()
        if status != 0:
            message = reader.read_string() if reader.remaining_bytes() >= 4 else None
            raise domain_error(KvError, operation, status, message)


class KvClient(DomainClient):
    def __init__(self, connection) -> None:
        super().__init__(connection)
        capacity = connection.config.limits.subscription_buffer_size
        self._subscriptions = SubscriptionRegistry[KvNotification](
            capacity, self._subscribe_wire, self._unsubscribe_wire
        )
        connection.register_notification_handler(MSG_KV_NOTIFY, self._notify)
        connection.on_reconnect(
            lambda: self._subscriptions.restore(
                domain="kv",
                on_error=lambda registration, error: connection.report_restore_failure(
                    "kv", registration, error
                ),
            ),
            domain="kv",
            registration="mutations",
        )

    async def begin(
        self,
        route: str,
        *,
        durability: KvDurability | str = KvDurability.BUFFERED,
        mode: KvMode | str = KvMode.READ_WRITE,
    ) -> KvTransaction:
        if not is_exact_route_shape(route, "kv", 3):
            raise KvError(f"Invalid KV route: {route}", "INVALID_ROUTE")
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_u8(1 if KvMode(mode) is KvMode.READ_WRITE else 0)
        writer.write_u8(1 if KvDurability(durability) is KvDurability.SYNC else 0)
        reader = BufferReader(await self.request_frame(MSG_KV_BEGIN, writer.build()))
        KvTransaction._status(reader, "BEGIN")
        if reader.remaining_bytes() < 8:
            raise KvError("BEGIN response missing transaction id", "INVALID_RESPONSE")
        return KvTransaction(self.connection, route, reader.read_u64_be())

    async def subscribe(self, pattern: str) -> AsyncSubscription[KvNotification]:
        if not is_selector_route_shape(pattern, "kv", 3, True):
            raise KvError(f"Invalid KV pattern: {pattern}", "INVALID_ROUTE")
        return await self._subscriptions.subscribe(pattern)

    async def _subscribe_wire(self, pattern: str) -> int:
        writer = BufferWriter()
        writer.write_route(pattern)
        reader = BufferReader(await self.request_frame(MSG_KV_SUBSCRIBE, writer.build()))
        KvTransaction._status(reader, "SUBSCRIBE")
        if reader.remaining_bytes() != 8:
            raise KvError("SUBSCRIBE response missing id", "INVALID_RESPONSE")
        return reader.read_u64_be()

    async def _unsubscribe_wire(self, pattern: str) -> None:
        writer = BufferWriter()
        writer.write_route(pattern)
        KvTransaction._status(
            BufferReader(await self.request_frame(MSG_KV_UNSUBSCRIBE, writer.build())),
            "UNSUBSCRIBE",
        )

    def _notify(self, payload: bytes) -> None:
        reader = BufferReader(payload)
        sub_id = reader.read_u64_be()
        notification = KvNotification(reader.read_route(), reader.read_u64_be())
        if not reader.is_eof():
            return
        self._subscriptions.publish(sub_id, notification)


# Descriptive compatibility-free public spelling.
KVDurability = KvDurability
KVMode = KvMode
KvScanResult = KvScanPage
