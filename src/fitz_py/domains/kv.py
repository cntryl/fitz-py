"""Transactional KV operations and mutation subscriptions."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from enum import StrEnum

from fitz_py._runtime import LazyAsyncContext, LazyAsyncIterator
from fitz_py.connection import Connection
from fitz_py.domains._routes import is_exact_route_shape, is_selector_route_shape
from fitz_py.domains._subscriptions import SubscriptionRegistry
from fitz_py.domains.base import DomainClient
from fitz_py.errors import KVError, StaleHandleError, domain_error
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
from fitz_py.types import BytesLike


class KVMode(StrEnum):
    READ_ONLY = "read_only"
    READ_WRITE = "read_write"


class KVDurability(StrEnum):
    BUFFERED = "buffered"
    SYNC = "sync"


@dataclass(frozen=True, slots=True)
class KVPair:
    key: bytes
    value: bytes


@dataclass(frozen=True, slots=True)
class KVScanPage:
    entries: tuple[KVPair, ...]
    has_more: bool


@dataclass(frozen=True, slots=True)
class KVNotification:
    route: str
    mutation_count: int


class KVTransaction:
    def __init__(self, connection: Connection, route: str, tx_id: int) -> None:
        self._connection = connection
        self._route = route
        self._tx_id = tx_id
        self._generation = connection.generation
        self._closed = False
        self._lock = asyncio.Lock()
        self._disconnect = connection.on_disconnect(self._invalidate)

    async def __aenter__(self) -> KVTransaction:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.rollback()

    def _ensure_open(self) -> None:
        if self._generation != self._connection.generation:
            raise StaleHandleError("KV transaction")
        if self._closed:
            raise KVError("Transaction is closed", "TX_CLOSED")

    def _invalidate(self) -> None:
        self._closed = True

    async def get(self, key: BytesLike) -> bytes | None:
        key = bytes(key)
        async with self._lock:
            self._ensure_open()

            async def operation() -> bytes | None:
                self._ensure_open()
                writer = self._prefix()
                writer.write_u32_be(len(key))
                writer.write_bytes(key)
                reader = BufferReader(await self._connection.request(MSG_KV_GET, writer.build()))
                self._status(reader, "GET")
                found_flag = reader.read_u8()
                if found_flag not in {0, 1}:
                    raise KVError("GET response has an invalid found flag", "INVALID_RESPONSE")
                found = found_flag == 1
                if not found:
                    if not reader.is_eof():
                        raise KVError("GET response has trailing bytes", "INVALID_RESPONSE")
                    return None
                value = reader.read_bytes(reader.read_u32_be())
                if not reader.is_eof():
                    raise KVError("GET response has trailing bytes", "INVALID_RESPONSE")
                return value

            return await self._connection.run_with_retry(operation, replay_safe=True)

    async def put(self, key: BytesLike, value: BytesLike) -> None:
        await self._write(MSG_KV_PUT, "PUT", key, value)

    async def insert(self, key: BytesLike, value: BytesLike) -> None:
        await self._write(MSG_KV_INSERT, "INSERT", key, value)

    async def delete(self, key: BytesLike) -> None:
        key = bytes(key)
        async with self._lock:
            self._ensure_open()
            writer = self._prefix()
            writer.write_u32_be(len(key))
            writer.write_bytes(key)
            self._status(
                BufferReader(await self._connection.request(MSG_KV_DELETE, writer.build())),
                "DELETE",
            )

    async def delete_range(self, start_key: BytesLike, end_key: BytesLike) -> None:
        start_key, end_key = bytes(start_key), bytes(end_key)
        if start_key >= end_key:
            raise KVError("Range start must be less than end", "INVALID_RANGE")
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
        start_key: BytesLike | None = None,
        end_key: BytesLike | None = None,
        limit: int | None = None,
        reverse: bool = False,
    ) -> KVScanPage:
        start_key = None if start_key is None else bytes(start_key)
        end_key = None if end_key is None else bytes(end_key)
        if start_key is not None and end_key is not None:
            invalid_forward = not reverse and start_key > end_key
            invalid_reverse = reverse and start_key < end_key
            if invalid_forward or invalid_reverse:
                async with self._lock:
                    self._ensure_open()
                    return KVScanPage((), False)
        async with self._lock:
            self._ensure_open()

            async def operation() -> KVScanPage:
                self._ensure_open()
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
                count = reader.read_u32_be()
                entries = tuple(
                    KVPair(
                        reader.read_bytes(reader.read_u32_be()),
                        reader.read_bytes(reader.read_u32_be()),
                    )
                    for _ in range(count)
                )
                has_more_flag = reader.read_u8()
                if has_more_flag not in {0, 1} or not reader.is_eof():
                    raise KVError("SCAN response is malformed", "INVALID_RESPONSE")
                return KVScanPage(entries, has_more_flag == 1)

            return await self._connection.run_with_retry(operation, replay_safe=True)

    async def scan(
        self,
        *,
        start_key: BytesLike | None = None,
        end_key: BytesLike | None = None,
        limit: int | None = None,
        reverse: bool = False,
    ) -> AsyncIterator[KVPair]:
        page = await self.scan_page(
            start_key=start_key,
            end_key=end_key,
            limit=limit,
            reverse=reverse,
        )
        if page.has_more and limit is None:
            raise KVError("Unbounded scan was truncated", "SCAN_TRUNCATED")
        for entry in page.entries:
            yield entry

    async def commit(self) -> None:
        await self._finish(MSG_KV_COMMIT, "COMMIT")

    async def rollback(self) -> None:
        if self._closed:
            return
        await self._finish(MSG_KV_ROLLBACK, "ROLLBACK")

    async def _write(
        self, message_type: int, operation: str, key: BytesLike, value: BytesLike
    ) -> None:
        key, value = bytes(key), bytes(value)
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
            code = reader.read_u32_be()
            message = reader.read_string()
            if not reader.is_eof():
                raise KVError(f"{operation} error response has trailing bytes", "INVALID_RESPONSE")
            raise domain_error(KVError, operation, code, message)


class KVClient(DomainClient):
    def __init__(self, connection: Connection) -> None:
        super().__init__(connection)
        capacity = connection.config.limits.subscription_buffer_size
        self._subscriptions = SubscriptionRegistry[KVNotification](
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
        on_close = getattr(connection, "on_close", None)
        if callable(on_close):
            on_close(self._subscriptions.terminate)

    async def begin(
        self,
        route: str,
        *,
        durability: KVDurability | str = KVDurability.BUFFERED,
        mode: KVMode | str = KVMode.READ_WRITE,
    ) -> KVTransaction:
        if not is_exact_route_shape(route, "kv", 3):
            raise KVError(f"Invalid KV route: {route}", "INVALID_ROUTE")
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_u8(1 if KVMode(mode) is KVMode.READ_WRITE else 0)
        writer.write_u8(1 if KVDurability(durability) is KVDurability.SYNC else 0)
        reader = BufferReader(await self.request_frame(MSG_KV_BEGIN, writer.build()))
        KVTransaction._status(reader, "BEGIN")  # noqa: SLF001
        if reader.remaining_bytes() != 8:
            raise KVError("BEGIN response must contain one transaction id", "INVALID_RESPONSE")
        return KVTransaction(self.connection, route, reader.read_u64_be())

    def transaction(
        self,
        route: str,
        *,
        durability: KVDurability | str = KVDurability.BUFFERED,
        mode: KVMode | str = KVMode.READ_WRITE,
    ) -> LazyAsyncContext[KVTransaction]:
        return LazyAsyncContext(
            lambda: self.begin(route, durability=durability, mode=mode),
            lambda transaction: transaction.rollback(),
        )

    def subscribe(self, pattern: str) -> LazyAsyncIterator[KVNotification]:
        if not is_selector_route_shape(pattern, "kv", 3, True):
            raise KVError(f"Invalid KV pattern: {pattern}", "INVALID_ROUTE")
        return LazyAsyncIterator(lambda: self._subscriptions.subscribe(pattern))

    async def _subscribe_wire(self, pattern: str) -> int:
        writer = BufferWriter()
        writer.write_route(pattern)
        reader = BufferReader(await self.request_frame(MSG_KV_SUBSCRIBE, writer.build()))
        KVTransaction._status(reader, "SUBSCRIBE")  # noqa: SLF001
        if reader.remaining_bytes() != 8:
            raise KVError("SUBSCRIBE response missing id", "INVALID_RESPONSE")
        return reader.read_u64_be()

    async def _unsubscribe_wire(self, pattern: str) -> None:
        writer = BufferWriter()
        writer.write_route(pattern)
        KVTransaction._status(  # noqa: SLF001
            BufferReader(await self.request_frame(MSG_KV_UNSUBSCRIBE, writer.build())),
            "UNSUBSCRIBE",
        )

    def _notify(self, payload: bytes) -> None:
        reader = BufferReader(payload)
        sub_id = reader.read_u64_be()
        notification = KVNotification(reader.read_route(), reader.read_u64_be())
        if not reader.is_eof():
            raise KVError("KV_NOTIFY response has trailing bytes", "INVALID_RESPONSE")
        self._subscriptions.publish(sub_id, notification)
