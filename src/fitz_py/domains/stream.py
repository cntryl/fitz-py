from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import IntEnum

from fitz_py.domains.base import DomainClient
from fitz_py.domains._routes import is_exact_route_shape, is_selector_route_shape
from fitz_py.errors import StreamError, stream_error
from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.messages import (
    MSG_STREAM_APPEND,
    MSG_STREAM_BEGIN,
    MSG_STREAM_COMMIT,
    MSG_STREAM_GET_METADATA,
    MSG_STREAM_LAST,
    MSG_STREAM_NOTIFY,
    MSG_STREAM_READ,
    MSG_STREAM_ROLLBACK,
    MSG_STREAM_SUBSCRIBE,
    MSG_STREAM_UNSUBSCRIBE,
)

StreamHandler = Callable[["StreamCommitNotification"], None | Awaitable[None]]


@dataclass(slots=True)
class StreamRecord:
    offset: int
    body: bytes


@dataclass(slots=True)
class StreamMetadata:
    first_offset: int
    last_offset: int
    record_count: int


class StreamCommitMode(IntEnum):
    BUFFERED = 0
    SYNC = 1


@dataclass(slots=True)
class StreamCommitNotification:
    route: str
    event: str = ""
    first_resource_offset: int = 0
    last_resource_offset: int = 0
    first_area_offset: int = 0
    last_area_offset: int = 0
    first_realm_offset: int = 0
    last_realm_offset: int = 0
    batch_size: int = 0


class StreamSubscription:
    def __init__(
        self, sub_id: int, pattern: str, unsubscribe: Callable[[str], Awaitable[None]]
    ) -> None:
        self.sub_id = sub_id
        self.pattern = pattern
        self._unsubscribe = unsubscribe

    async def unsubscribe(self) -> None:
        await self._unsubscribe(self.pattern)


class StreamSession:
    def __init__(self, connection, session_id: int) -> None:
        self._connection = connection
        self._session_id = session_id
        self._closed = False

    async def __aenter__(self) -> "StreamSession":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if not self._closed:
            await self.rollback()

    async def append(
        self, expected_offset: int, body: bytes, metadata: bytes | None = None
    ) -> int | None:
        writer = BufferWriter()
        writer.write_u64_be(self._session_id)
        writer.write_u64_be(expected_offset)
        writer.write_u32_be(len(body))
        writer.write_bytes(body)
        if metadata:
            writer.write_u8(1)
            writer.write_u32_be(len(metadata))
            writer.write_bytes(metadata)
        else:
            writer.write_u8(0)
        reader = BufferReader(await self._connection.request(MSG_STREAM_APPEND, writer.build()))
        status = reader.read_u8()
        if status != 0:
            raise stream_error(f"APPEND failed with status {status}", status)
        if not reader.is_eof():
            has_session = reader.read_u8()
            if has_session == 1 and reader.remaining_bytes() >= 8:
                reader.read_u64_be()
        if reader.is_eof():
            return None
        data = reader.read_bytes(reader.read_u32_be())
        if len(data) < 8:
            return None
        return BufferReader(data).read_u64_be()

    async def commit(self, mode: int | StreamCommitMode = StreamCommitMode.BUFFERED) -> None:
        writer = BufferWriter()
        writer.write_u64_be(self._session_id)
        writer.write_u8(int(mode))
        await self._expect_status(MSG_STREAM_COMMIT, writer.build(), "COMMIT")
        self._closed = True

    async def rollback(self) -> None:
        writer = BufferWriter()
        writer.write_u64_be(self._session_id)
        await self._expect_status(MSG_STREAM_ROLLBACK, writer.build(), "ROLLBACK")
        self._closed = True

    async def _expect_status(self, message_type: int, payload: bytes, operation: str) -> None:
        reader = BufferReader(await self._connection.request(message_type, payload))
        status = reader.read_u8() if not reader.is_eof() else 0
        if status != 0:
            raise stream_error(f"{operation} failed with status {status}", status)


class StreamClient(DomainClient):
    def __init__(self, connection) -> None:
        super().__init__(connection)
        self._subscriptions: dict[int, tuple[str, StreamHandler]] = {}
        self._initialized = False
        self.connection.on_reconnect(self._restore_subscriptions)

    async def begin(
        self, route: str, ingest_metadata: bytes | None = None
    ) -> StreamSession:
        _assert_stream_route(route)
        writer = BufferWriter()
        writer.write_route(route)
        if ingest_metadata:
            writer.write_u8(1)
            writer.write_u32_be(len(ingest_metadata))
            writer.write_bytes(ingest_metadata)
        else:
            writer.write_u8(0)
        reader = BufferReader(await self.request_frame(MSG_STREAM_BEGIN, writer.build()))
        status = reader.read_u8()
        if status != 0:
            raise stream_error(f"BEGIN failed with status {status}", status)
        has_session = reader.read_u8() if not reader.is_eof() else 0
        if has_session != 1 or reader.remaining_bytes() < 8:
            raise StreamError("BEGIN response missing session id", "MISSING_SESSION_ID")
        return StreamSession(self.connection, reader.read_u64_be())

    async def read(
        self,
        route: str,
        start_offset: int,
        limit: int = 100,
        max_bytes: int | None = None,
    ) -> list[StreamRecord]:
        _assert_stream_pattern(route)
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_u64_be(start_offset)
        writer.write_u64_be(limit)
        writer.write_u8(1 if max_bytes is not None else 0)
        if max_bytes is not None:
            writer.write_u64_be(max_bytes)
        reader = BufferReader(await self.request_frame(MSG_STREAM_READ, writer.build()))
        status, data = _read_wrapped_stream_response(reader)
        if status != 0:
            raise stream_error(f"READ failed with status {status}", status)
        if not data:
            return []
        inner = BufferReader(data)
        count = inner.read_u32_be()
        records: list[StreamRecord] = []
        for _ in range(count):
            offset = inner.read_u64_be()
            body = inner.read_bytes(inner.read_u32_be())
            records.append(StreamRecord(offset=offset, body=body))
        return records

    async def peek(self, route: str) -> StreamRecord | None:
        _assert_stream_route(route)
        writer = BufferWriter()
        writer.write_route(route)
        reader = BufferReader(await self.request_frame(MSG_STREAM_LAST, writer.build()))
        status, data = _read_wrapped_stream_response(reader)
        if status != 0:
            raise stream_error(f"LAST failed with status {status}", status)
        if not data:
            return None
        inner = BufferReader(data)
        return StreamRecord(offset=inner.read_u64_be(), body=inner.read_bytes(inner.read_u32_be()))

    async def metadata(self, route: str) -> StreamMetadata:
        _assert_stream_route(route)
        writer = BufferWriter()
        writer.write_route(route)
        reader = BufferReader(await self.request_frame(MSG_STREAM_GET_METADATA, writer.build()))
        status, data = _read_wrapped_stream_response(reader)
        if status != 0:
            raise stream_error(f"METADATA failed with status {status}", status)
        if not data:
            return StreamMetadata(first_offset=0, last_offset=0, record_count=0)
        inner = BufferReader(data)
        return StreamMetadata(
            first_offset=inner.read_u64_be(),
            last_offset=inner.read_u64_be(),
            record_count=inner.read_u64_be(),
        )

    async def subscribe(self, pattern: str, handler: StreamHandler) -> StreamSubscription:
        _assert_stream_pattern(pattern)
        self._init_notify_handler()
        writer = BufferWriter()
        writer.write_route(pattern)
        reader = BufferReader(await self.request_frame(MSG_STREAM_SUBSCRIBE, writer.build()))
        status = reader.read_u8()
        if status != 0:
            raise stream_error(f"SUBSCRIBE failed with status {status}", status)
        has_sub_id = reader.read_u8() if not reader.is_eof() else 0
        if has_sub_id != 1 or reader.is_eof():
            raise StreamError("SUBSCRIBE response missing subscription id", "MISSING_SUB_ID")
        sub_id = reader.read_u64_be()
        self._subscriptions[sub_id] = (pattern, handler)
        return StreamSubscription(sub_id, pattern, self._unsubscribe)

    async def _unsubscribe(self, pattern: str) -> None:
        for sub_id, (sub_pattern, _) in list(self._subscriptions.items()):
            if sub_pattern == pattern:
                self._subscriptions.pop(sub_id, None)
        writer = BufferWriter()
        writer.write_route(pattern)
        await self.request_frame(MSG_STREAM_UNSUBSCRIBE, writer.build())

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
                result = subscription[1](_decode_stream_commit_notification(route, body))
                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception:
                return

        self.connection.register_notification_handler(MSG_STREAM_NOTIFY, handler)

    async def _restore_subscriptions(self) -> None:
        if not self._subscriptions:
            return
        snapshot = list(self._subscriptions.values())
        self._subscriptions.clear()
        for pattern, handler in snapshot:
            await self.subscribe(pattern, handler)


def _read_wrapped_stream_response(reader: BufferReader) -> tuple[int, bytes]:
    status = reader.read_u8()
    if status != 0:
        return status, b""
    if not reader.is_eof():
        has_session = reader.read_u8()
        if has_session == 1 and reader.remaining_bytes() >= 8:
            reader.read_u64_be()
    if reader.is_eof():
        return 0, b""
    return 0, reader.read_bytes(reader.read_u32_be())


def _assert_stream_route(route: str) -> None:
    if not is_exact_route_shape(route, "stream", 3):
        raise StreamError(
            f"Invalid stream route: {route} (expected stream://{{realm}}/{{area}}/{{resource}}, no empty segments or wildcards)",
            "INVALID_ROUTE",
        )


def _assert_stream_pattern(pattern: str) -> None:
    if not is_selector_route_shape(pattern, "stream", 3, allow_realm_wildcard=True):
        raise StreamError(
            f"Invalid stream pattern: {pattern} (expected stream://{{realm}}/{{area}}/{{resource}}, stream://{{realm}}/{{area}}/*, or stream://{{realm}}/**)",
            "INVALID_ROUTE",
        )


def _decode_stream_commit_notification(route: str, payload: bytes) -> StreamCommitNotification:
    _assert_stream_route(route)

    notification = StreamCommitNotification(route=route)
    if not payload:
        return notification

    try:
        decoded = json.loads(payload)
        if not isinstance(decoded, dict):
            return notification

        event = decoded.get("event", "")
        if not isinstance(event, str):
            return notification

        notification.event = event
        notification.first_resource_offset = int(decoded.get("first_resource_offset", 0))
        notification.last_resource_offset = int(decoded.get("last_resource_offset", 0))
        notification.first_area_offset = int(decoded.get("first_area_offset", 0))
        notification.last_area_offset = int(decoded.get("last_area_offset", 0))
        notification.first_realm_offset = int(decoded.get("first_realm_offset", 0))
        notification.last_realm_offset = int(decoded.get("last_realm_offset", 0))
        notification.batch_size = int(decoded.get("batch_size", 0))
    except (TypeError, ValueError, json.JSONDecodeError):
        return StreamCommitNotification(route=route)

    return notification
