"""Stream domain client, sessions, filters, read models, and subscriptions."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum, IntEnum

from fitz_py._runtime import AsyncSubscription
from fitz_py.domains._routes import is_exact_route_shape, is_selector_route_shape
from fitz_py.domains._subscriptions import SubscriptionRegistry
from fitz_py.domains.base import DomainClient
from fitz_py.errors import StaleHandleError, StreamError, domain_error
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
from fitz_py.protocol.response import parse_response


@dataclass(slots=True)
class StreamRecord:
    """Single stream event record with offsets and payload data."""

    route: str
    offset: int
    area_offset: int | None = None
    realm_offset: int | None = None
    global_offset: int | None = None
    body: bytes = b""
    metadata: bytes | None = None
    timestamp: int = 0


@dataclass(slots=True)
class StreamMetadata:
    """Aggregate stream metadata returned by metadata queries."""

    first_offset: int
    last_offset: int
    record_count: int
    max_batch_events: int | None = None
    max_batch_bytes: int | None = None
    ttl_seconds: int | None = None
    area_watermark: int | None = None
    realm_watermark: int | None = None


@dataclass(slots=True)
class StreamFilterClause:
    """One stream filter clause used in read filter sets."""

    kind: str
    value: str = ""
    values: list[str] = field(default_factory=list)


@dataclass(slots=True)
class StreamFilterSet:
    """Collection of filter clauses applied to stream reads."""

    clauses: list[StreamFilterClause] = field(default_factory=list)


class StreamFilteredReason(str, Enum):
    """Reason a read item was filtered instead of returned as an event."""

    SERVER_FILTER = "server_filter"
    PERMISSION = "permission"
    PROJECTION = "projection"


class StreamReadItemKind(str, Enum):
    """Discriminator for stream read page item variants."""

    EVENT = "event"
    FILTERED = "filtered"
    FILTERED_RANGE = "filtered_range"


@dataclass(slots=True)
class StreamReadCursor:
    """Pagination cursor state for subsequent stream reads."""

    last_resource_offset: int = 0
    last_area_offset: int | None = None
    last_realm_offset: int | None = None
    last_global_offset: int | None = None
    cursor_fingerprint: int | None = None
    captured_watermark: int | None = None
    has_more: bool = False


@dataclass(slots=True)
class StreamReadItem:
    """One item in a stream read page, event or filtered marker."""

    kind: StreamReadItemKind
    route: str
    record: StreamRecord | None = None
    offset: int = 0
    from_offset: int = 0
    to_offset: int = 0
    reason: StreamFilteredReason | None = None


@dataclass(slots=True)
class StreamReadPage:
    """Paged stream read payload containing items and next-cursor state."""

    items: list[StreamReadItem] = field(default_factory=list)
    cursor: StreamReadCursor = field(default_factory=StreamReadCursor)


class StreamCommitMode(IntEnum):
    """Durability mode used when committing a stream session."""

    BUFFERED = 0
    SYNC = 1


@dataclass(slots=True)
class StreamCommitNotification:
    """Payload delivered for stream commit notifications."""

    route: str
    event: str = ""
    first_resource_offset: int = 0
    last_resource_offset: int = 0
    first_area_offset: int = 0
    last_area_offset: int = 0
    first_realm_offset: int = 0
    last_realm_offset: int = 0
    batch_size: int = 0


class StreamSession:
    """Mutable append session returned by stream begin operations."""

    def __init__(self, connection, session_id: int) -> None:
        self._connection = connection
        self._session_id = session_id
        self._closed = False
        self._closed_reason: str | None = None
        self._generation = connection.generation
        on_disconnect = getattr(self._connection, "on_disconnect", None)
        self._disconnect_unregister = (
            on_disconnect(self._invalidate) if callable(on_disconnect) else None
        )

    async def __aenter__(self) -> "StreamSession":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if not self._closed:
            await self.rollback()

    async def append(
        self,
        expected_offset: int,
        body: bytes,
        metadata: bytes | None = None,
        discriminator: str | None = None,
    ) -> int | None:
        self._ensure_open("APPEND")
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
        if discriminator:
            writer.write_u8(1)
            writer.write_string(discriminator)
        else:
            writer.write_u8(0)
        response = parse_response(
            await self._connection.request(MSG_STREAM_APPEND, writer.build()), plain=True
        )
        if not response.success:
            raise StreamError(f"APPEND failed: {response.error}", "APPEND")
        if not response.data:
            return None
        reader = BufferReader(response.data)
        length = reader.read_u32_be()
        if length != 8 or reader.remaining_bytes() != 8:
            raise StreamError("APPEND response has invalid payload length", "INVALID_RESPONSE")
        return reader.read_u64_be()

    async def commit(self, mode: int | StreamCommitMode = StreamCommitMode.BUFFERED) -> None:
        self._ensure_open("COMMIT")
        writer = BufferWriter()
        writer.write_u64_be(self._session_id)
        writer.write_u8(int(mode))
        await self._expect_status(MSG_STREAM_COMMIT, writer.build(), "COMMIT")
        self._closed = True
        self._closed_reason = "committed"
        self._clear_disconnect_listener()

    async def rollback(self) -> None:
        self._ensure_open("ROLLBACK")
        writer = BufferWriter()
        writer.write_u64_be(self._session_id)
        await self._expect_status(MSG_STREAM_ROLLBACK, writer.build(), "ROLLBACK")
        self._closed = True
        self._closed_reason = "rolled back"
        self._clear_disconnect_listener()

    async def _expect_status(self, message_type: int, payload: bytes, operation: str) -> None:
        response = parse_response(await self._connection.request(message_type, payload))
        if not response.success:
            raise domain_error(StreamError, operation, response.error_code or 0, response.error)

    def _ensure_open(self, operation: str) -> None:
        if not self._closed and self._generation == self._connection.generation:
            return

        reason = self._closed_reason or "closed"
        raise StaleHandleError(f"Stream session ({operation}, {reason})")

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


class StreamClient(DomainClient):
    """Stream domain operations for write sessions, reads, and notifications."""

    def __init__(self, connection) -> None:
        super().__init__(connection)
        self._subscriptions = SubscriptionRegistry[StreamCommitNotification](
            connection.config.limits.subscription_buffer_size,
            self._subscribe_wire,
            self._unsubscribe_wire,
        )
        connection.register_notification_handler(MSG_STREAM_NOTIFY, self._notify)
        connection.on_reconnect(
            lambda: self._subscriptions.restore(
                domain="stream",
                on_error=lambda registration, error: connection.report_restore_failure(
                    "stream", registration, error
                ),
            ),
            domain="stream",
            registration="commits",
        )

    async def begin(self, route: str, ingest_metadata: bytes | None = None) -> StreamSession:
        _assert_stream_route(route)
        writer = BufferWriter()
        writer.write_route(route)
        if ingest_metadata:
            writer.write_u8(1)
            writer.write_u32_be(len(ingest_metadata))
            writer.write_bytes(ingest_metadata)
        else:
            writer.write_u8(0)
        response = parse_response(
            await self.request_frame(MSG_STREAM_BEGIN, writer.build()), plain=True
        )
        if not response.success:
            raise StreamError(f"BEGIN failed: {response.error}", "BEGIN")
        reader = BufferReader(response.data)
        if reader.remaining_bytes() < 8:
            raise StreamError("BEGIN response missing session id", "MISSING_SESSION_ID")
        return StreamSession(self.connection, reader.read_u64_be())

    async def read(
        self,
        route: str,
        start_offset: int,
        limit: int = 100,
        stream_filter: StreamFilterSet | None = None,
        *,
        max_bytes: int | None = None,
        cursor_fingerprint: int | None = None,
        captured_watermark: int | None = None,
    ) -> list[StreamRecord]:
        page = await self.read_page(
            route,
            start_offset,
            limit=limit,
            stream_filter=stream_filter,
            max_bytes=max_bytes,
            cursor_fingerprint=cursor_fingerprint,
            captured_watermark=captured_watermark,
        )
        return _flatten_stream_read_items(page.items)

    async def read_page(
        self,
        route: str,
        start_offset: int,
        limit: int = 100,
        stream_filter: StreamFilterSet | None = None,
        *,
        max_bytes: int | None = None,
        cursor_fingerprint: int | None = None,
        captured_watermark: int | None = None,
    ) -> StreamReadPage:
        _assert_stream_pattern(route)
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_u64_be(start_offset)
        writer.write_u64_be(limit)
        if max_bytes is None:
            writer.write_u8(0)
        else:
            writer.write_u8(1)
            writer.write_u64_be(max_bytes)
        filter_bytes = _encode_stream_filter_set(stream_filter)
        writer.write_u8(1 if filter_bytes else 0)
        if filter_bytes:
            writer.write_u32_be(len(filter_bytes))
            writer.write_bytes(filter_bytes)
        writer.write_optional_u64(cursor_fingerprint)
        writer.write_optional_u64(captured_watermark)
        response = parse_response(await self.request_frame(MSG_STREAM_READ, writer.build()))
        if not response.success:
            raise domain_error(StreamError, "READ", response.error_code or 0, response.error)
        if not response.data:
            return StreamReadPage()
        envelope = BufferReader(response.data)
        if envelope.read_u8() != 0:
            raise StreamError("READ response has invalid session flag", "INVALID_RESPONSE")
        data = envelope.read_bytes(envelope.read_u32_be())
        if not envelope.is_eof():
            raise StreamError("READ response has trailing bytes", "INVALID_RESPONSE")
        return _read_stream_read_page(data, route)

    async def peek(self, route: str) -> StreamRecord | None:
        _assert_stream_route(route)
        writer = BufferWriter()
        writer.write_route(route)
        response = parse_response(await self.request_frame(MSG_STREAM_LAST, writer.build()))
        if not response.success:
            raise domain_error(StreamError, "LAST", response.error_code or 0, response.error)
        if not response.data:
            return None
        inner = BufferReader(response.data)
        return _read_stream_record(inner, route, False)

    async def metadata(self, route: str) -> StreamMetadata:
        _assert_stream_route(route)
        writer = BufferWriter()
        writer.write_route(route)
        response = parse_response(await self.request_frame(MSG_STREAM_GET_METADATA, writer.build()))
        if not response.success:
            raise domain_error(StreamError, "METADATA", response.error_code or 0, response.error)
        if not response.data:
            return StreamMetadata(first_offset=0, last_offset=0, record_count=0)
        inner = BufferReader(response.data)
        return StreamMetadata(
            first_offset=_read_optional_u64(inner) or 0,
            last_offset=_read_optional_u64(inner) or 0,
            record_count=inner.read_u64_be(),
            max_batch_events=inner.read_u64_be(),
            max_batch_bytes=inner.read_u64_be(),
            ttl_seconds=_read_optional_u64(inner),
            area_watermark=inner.read_u64_be(),
            realm_watermark=inner.read_u64_be(),
        )

    async def subscribe(self, pattern: str) -> AsyncSubscription[StreamCommitNotification]:
        _assert_stream_pattern(pattern)
        return await self._subscriptions.subscribe(pattern)

    async def _subscribe_wire(self, pattern: str) -> int:
        writer = BufferWriter()
        writer.write_route(pattern)
        response = parse_response(await self.request_frame(MSG_STREAM_SUBSCRIBE, writer.build()))
        if not response.success:
            raise domain_error(StreamError, "SUBSCRIBE", response.error_code or 0, response.error)
        reader = BufferReader(response.data)
        has_sub_id = reader.read_u8()
        if has_sub_id != 1 or reader.is_eof():
            raise StreamError("SUBSCRIBE response missing subscription id", "MISSING_SUB_ID")
        return reader.read_u64_be()

    async def _unsubscribe_wire(self, pattern: str) -> None:
        writer = BufferWriter()
        writer.write_route(pattern)
        response = parse_response(await self.request_frame(MSG_STREAM_UNSUBSCRIBE, writer.build()))
        if not response.success:
            raise domain_error(StreamError, "UNSUBSCRIBE", response.error_code or 0, response.error)

    def _notify(self, payload: bytes) -> None:
        reader = BufferReader(payload)
        sub_id, route = reader.read_u64_be(), reader.read_route()
        body = reader.read_bytes(reader.read_u32_be())
        self._subscriptions.publish(sub_id, _decode_stream_commit_notification(route, body))


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


def _read_optional_u64(reader: BufferReader) -> int | None:
    if reader.read_u8() == 0:
        return None
    return reader.read_u64_be()


def _read_optional_bytes(reader: BufferReader) -> bytes | None:
    if reader.read_u8() == 0:
        return None
    return reader.read_bytes(reader.read_u32_be())


def _read_stream_record(reader: BufferReader, route: str, extended: bool) -> StreamRecord:
    offset = reader.read_u64_be()
    area_offset = _read_optional_u64(reader)
    realm_offset = _read_optional_u64(reader)
    global_offset = _read_optional_u64(reader) if extended else None
    body = reader.read_bytes(reader.read_u32_be())
    metadata = _read_optional_bytes(reader)
    timestamp = reader.read_u64_be()
    return StreamRecord(
        route=route,
        offset=offset,
        area_offset=area_offset,
        realm_offset=realm_offset,
        global_offset=global_offset,
        body=body,
        metadata=metadata,
        timestamp=timestamp,
    )


def _read_stream_read_page(data: bytes, selector: str) -> StreamReadPage:
    reader = BufferReader(data)
    extended = selector in {"stream://**", "stream://*/*/*"}
    count = reader.read_u32_be()
    items = []
    for _ in range(count):
        route = reader.read_route()
        _assert_stream_route(route)
        items.append(_read_stream_read_item(reader, route, extended))
    cursor = StreamReadCursor(
        last_resource_offset=reader.read_u64_be(),
        last_area_offset=_read_optional_u64(reader),
        last_realm_offset=_read_optional_u64(reader),
        last_global_offset=_read_optional_u64(reader) if extended else None,
        has_more=_read_bool_u8(reader),
        cursor_fingerprint=_read_optional_u64(reader) if extended else None,
        captured_watermark=_read_optional_u64(reader) if extended else None,
    )
    if not reader.is_eof():
        raise StreamError("READ response has trailing bytes", "INVALID_RESPONSE")
    return StreamReadPage(items=items, cursor=cursor)


def _read_stream_read_item(reader: BufferReader, route: str, extended: bool) -> StreamReadItem:
    tag = reader.read_u8()
    if tag == 0:
        return StreamReadItem(
            kind=StreamReadItemKind.EVENT,
            route=route,
            record=_read_stream_record(reader, route, extended),
        )
    if tag == 1:
        return StreamReadItem(
            kind=StreamReadItemKind.FILTERED,
            route=route,
            offset=reader.read_u64_be(),
            reason=_read_filtered_reason(reader),
        )
    if tag == 2:
        return StreamReadItem(
            kind=StreamReadItemKind.FILTERED_RANGE,
            route=route,
            from_offset=reader.read_u64_be(),
            to_offset=reader.read_u64_be(),
            reason=_read_filtered_reason(reader),
        )
    raise StreamError(f"Unknown stream read item tag: {tag}", "INVALID_READ_ITEM")


def _read_filtered_reason(reader: BufferReader) -> StreamFilteredReason | None:
    tag = reader.read_u8()
    if tag == 0:
        return None
    if tag == 1:
        return StreamFilteredReason.SERVER_FILTER
    if tag == 2:
        return StreamFilteredReason.PERMISSION
    if tag == 3:
        return StreamFilteredReason.PROJECTION
    raise StreamError(f"Invalid filtered reason tag: {tag}", "INVALID_FILTERED_REASON")


def _read_bool_u8(reader: BufferReader) -> bool:
    value = reader.read_u8()
    if value not in (0, 1):
        raise StreamError(f"Invalid boolean flag: {value}", "INVALID_BOOLEAN_FLAG")
    return value == 1


def _flatten_stream_read_items(items: list[StreamReadItem]) -> list[StreamRecord]:
    return [
        item.record
        for item in items
        if item.kind is StreamReadItemKind.EVENT and item.record is not None
    ]


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


def _encode_stream_filter_set(stream_filter: StreamFilterSet | None) -> bytes:
    if stream_filter is None or not stream_filter.clauses:
        return b""

    writer = BufferWriter()
    writer.write_u8(0)
    writer.write_u8(0xF1)
    writer.write_u32_be(len(stream_filter.clauses))
    for clause in stream_filter.clauses:
        _encode_stream_filter_clause(writer, clause)
    return writer.build()


def _encode_stream_filter_clause(writer: BufferWriter, clause: StreamFilterClause) -> None:
    if clause.kind == "Equals":
        writer.write_u8(0)
        writer.write_string(clause.value)
    elif clause.kind == "NotEquals":
        writer.write_u8(1)
        writer.write_string(clause.value)
    elif clause.kind == "StartsWith":
        writer.write_u8(2)
        writer.write_string(clause.value)
    elif clause.kind == "AnyOf":
        writer.write_u8(3)
        writer.write_u32_be(len(clause.values))
        for value in clause.values:
            writer.write_string(value)
    else:
        raise ValueError(f"Unknown stream filter clause: {clause.kind}")
