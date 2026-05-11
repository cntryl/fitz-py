from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from fitz_py.domains._routes import is_exact_route_shape
from fitz_py.domains.base import DomainClient
from fitz_py.errors import ErrKvOperationNotAllowed, KvError, kv_error
from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.messages import (
    MSG_KV_BEGIN,
    MSG_KV_COMMIT,
    MSG_KV_DELETE,
    MSG_KV_DELETE_RANGE,
    MSG_KV_GET,
    MSG_KV_INSERT,
    MSG_KV_PUT,
    MSG_KV_ROLLBACK,
    MSG_KV_SCAN,
)

KVMode = Literal["read_only", "read_write"]
KVDurability = Literal["none", "async", "sync"]


@dataclass(slots=True)
class KvGetResult:
    found: bool
    value: bytes | None = None


@dataclass(slots=True)
class KvPair:
    key: bytes
    value: bytes


@dataclass(slots=True)
class KvScanResult:
    items: list[KvPair]
    has_more: bool = False


class KvTransaction:
    def __init__(self, connection, route: str, tx_id: int) -> None:
        self._connection = connection
        self._route = route
        self._tx_id = tx_id
        self._closed = False
        self._closed_reason: str | None = None
        on_disconnect = getattr(self._connection, "on_disconnect", None)
        self._disconnect_unregister = (
            on_disconnect(self._invalidate) if callable(on_disconnect) else None
        )

    async def __aenter__(self) -> "KvTransaction":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if not self._closed:
            await self.rollback()

    async def get(self, key: bytes) -> KvGetResult:
        self._ensure_open("GET")
        writer = BufferWriter()
        writer.write_u64_be(self._tx_id)
        writer.write_route(self._route)
        writer.write_u32_be(len(key))
        writer.write_bytes(key)
        reader = BufferReader(await self._connection.request(MSG_KV_GET, writer.build()))
        status = reader.read_u8()
        if status != 0:
            raise kv_error(f"GET failed with status {status}", status)
        found = not reader.is_eof() and reader.read_u8() == 1
        if not found or reader.is_eof():
            return KvGetResult(found=False)
        value = reader.read_bytes(reader.read_u32_be())
        return KvGetResult(found=True, value=value)

    async def put(self, key: bytes, value: bytes) -> None:
        await self._write(MSG_KV_PUT, key, value, "PUT")

    async def insert(self, key: bytes, value: bytes) -> None:
        await self._write(MSG_KV_INSERT, key, value, "INSERT")

    async def delete(self, key: bytes) -> None:
        self._ensure_open("DELETE")
        writer = BufferWriter()
        writer.write_u64_be(self._tx_id)
        writer.write_route(self._route)
        writer.write_u32_be(len(key))
        writer.write_bytes(key)
        await self._expect_status(MSG_KV_DELETE, writer.build(), "DELETE")

    async def delete_range(self, start_key: bytes, end_key: bytes) -> None:
        self._ensure_open("DELETE_RANGE")
        writer = BufferWriter()
        writer.write_u64_be(self._tx_id)
        writer.write_route(self._route)
        writer.write_u32_be(len(start_key))
        writer.write_bytes(start_key)
        writer.write_u32_be(len(end_key))
        writer.write_bytes(end_key)
        await self._expect_status(MSG_KV_DELETE_RANGE, writer.build(), "DELETE_RANGE")

    async def scan(
        self,
        *,
        start_key: bytes | None = None,
        end_key: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
    ) -> KvScanResult:
        self._ensure_open("SCAN")
        writer = BufferWriter()
        writer.write_u64_be(self._tx_id)
        writer.write_route(self._route)
        if start_key is not None:
            writer.write_u8(1)
            writer.write_u32_be(len(start_key))
            writer.write_bytes(start_key)
        else:
            writer.write_u8(0)
        if end_key is not None:
            writer.write_u8(1)
            writer.write_u32_be(len(end_key))
            writer.write_bytes(end_key)
        else:
            writer.write_u8(0)
        if limit is not None and limit > 0:
            writer.write_u8(1)
            writer.write_u32_be(limit)
        else:
            writer.write_u8(0)
        writer.write_u8(1 if reverse else 0)

        reader = BufferReader(await self._connection.request(MSG_KV_SCAN, writer.build()))
        status = reader.read_u8()
        if status != 0:
            raise kv_error(f"SCAN failed with status {status}", status)
        if reader.is_eof():
            return KvScanResult(items=[])
        count = reader.read_u32_be()
        items: list[KvPair] = []
        for _ in range(count):
            key = reader.read_bytes(reader.read_u32_be())
            value = reader.read_bytes(reader.read_u32_be())
            items.append(KvPair(key=key, value=value))
        has_more = not reader.is_eof() and reader.read_u8() == 1
        return KvScanResult(items=items, has_more=has_more)

    async def commit(self) -> None:
        await self._finalize(MSG_KV_COMMIT, "COMMIT")

    async def rollback(self) -> None:
        await self._finalize(MSG_KV_ROLLBACK, "ROLLBACK")

    async def _write(self, message_type: int, key: bytes, value: bytes, operation: str) -> None:
        self._ensure_open(operation)
        writer = BufferWriter()
        writer.write_u64_be(self._tx_id)
        writer.write_route(self._route)
        writer.write_u32_be(len(key))
        writer.write_bytes(key)
        writer.write_u32_be(len(value))
        writer.write_bytes(value)
        await self._expect_status(message_type, writer.build(), operation)

    async def _finalize(self, message_type: int, operation: str) -> None:
        self._ensure_open(operation)
        writer = BufferWriter()
        writer.write_u64_be(self._tx_id)
        writer.write_route(self._route)
        await self._expect_status(message_type, writer.build(), operation)
        self._closed = True
        self._closed_reason = "committed" if operation == "COMMIT" else "rolled back"
        self._clear_disconnect_listener()

    def _ensure_open(self, operation: str) -> None:
        if not self._closed:
            return

        reason = self._closed_reason or "closed"
        raise ErrKvOperationNotAllowed(f"{operation} not allowed: transaction already {reason}")

    def _invalidate(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._closed_reason = "disconnected"
        self._clear_disconnect_listener()

    def _clear_disconnect_listener(self) -> None:
        unregister = getattr(self, "_disconnect_unregister", None)
        if unregister is None:
            return
        self._disconnect_unregister = None
        unregister()

    async def _expect_status(self, message_type: int, payload: bytes, operation: str) -> None:
        reader = BufferReader(await self._connection.request(message_type, payload))
        status = reader.read_u8()
        if status != 0:
            raise kv_error(f"{operation} failed with status {status}", status)


class KvClient(DomainClient):
    async def begin(
        self,
        route: str,
        *,
        mode: KVMode = "read_write",
        durability: KVDurability = "async",
    ) -> KvTransaction:
        _assert_kv_route(route)
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_u8(1 if mode == "read_write" else 0)
        writer.write_u8(1 if durability == "sync" else 0)
        reader = BufferReader(await self.request_frame(MSG_KV_BEGIN, writer.build()))
        status = reader.read_u8()
        if status != 0:
            raise kv_error(f"BEGIN failed with status {status}", status)
        tx_id = reader.read_u64_be() if not reader.is_eof() else None
        if tx_id is None:
            raise KvError("BEGIN response missing transaction id", "MISSING_TX_ID")
        return KvTransaction(self.connection, route, tx_id)


def _assert_kv_route(route: str) -> None:
    if not is_exact_route_shape(route, "kv", 3):
        raise KvError(
            f"Invalid kv route: {route} (expected kv://{{realm}}/{{area}}/{{resource}}, no empty segments or wildcards)",
            "INVALID_ROUTE",
        )
