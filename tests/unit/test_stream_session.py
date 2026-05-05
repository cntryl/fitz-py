from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable

import pytest

from fitz_py import (
    StreamCommitMode,
    StreamCommitNotification,
    StreamFilterClause,
    StreamFilterSet,
)
from fitz_py.domains.stream import StreamClient, StreamSession
from fitz_py.errors import ErrStreamSessionClosed
from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.messages import (
    MSG_STREAM_APPEND,
    MSG_STREAM_COMMIT,
    MSG_STREAM_NOTIFY,
    MSG_STREAM_READ,
    MSG_STREAM_SUBSCRIBE,
)


class _FakeConnection:
    def __init__(self) -> None:
        self.requests: list[tuple[int, bytes]] = []
        self.notification_handlers: dict[int, Callable[[bytes], None]] = {}
        self.reconnect_handlers: list[Callable[[], Awaitable[None]]] = []
        self.disconnect_handlers: list[Callable[[], None | Awaitable[None]]] = []

    async def request(self, message_type: int, payload: bytes) -> bytes:
        self.requests.append((message_type, payload))
        if message_type == MSG_STREAM_SUBSCRIBE:
            return b"\x00\x01" + (7).to_bytes(8, "big")
        return b"\x00"

    def register_notification_handler(
        self, message_type: int, handler: Callable[[bytes], None]
    ) -> None:
        self.notification_handlers[message_type] = handler

    def on_reconnect(self, _handler: Callable[[], Awaitable[None]]) -> None:
        self.reconnect_handlers.append(_handler)
        return None

    def on_disconnect(self, handler: Callable[[], None | Awaitable[None]]) -> None:
        self.disconnect_handlers.append(handler)
        return None

    def emit_disconnect(self) -> None:
        for handler in list(self.disconnect_handlers):
            result = handler()
            if asyncio.iscoroutine(result):
                asyncio.create_task(result)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode, expected_mode",
    [
        (None, 0),
        (StreamCommitMode.SYNC, 1),
    ],
)
async def test_stream_session_commit_encodes_mode(
    mode: StreamCommitMode | None, expected_mode: int
) -> None:
    connection = _FakeConnection()
    session = StreamSession(connection, 42)

    if mode is None:
        await session.commit()
    else:
        await session.commit(mode)

    assert len(connection.requests) == 1
    message_type, payload = connection.requests[0]
    assert message_type == MSG_STREAM_COMMIT

    reader = BufferReader(payload)
    assert reader.read_u64_be() == 42
    assert reader.read_u8() == expected_mode
    assert reader.is_eof()


@pytest.mark.asyncio
async def test_stream_session_append_encodes_discriminator() -> None:
    connection = _FakeConnection()
    session = StreamSession(connection, 42)

    await session.append(12, b"entry", b"meta", "proj.alpha")

    assert connection.requests[0][0] == MSG_STREAM_APPEND
    reader = BufferReader(connection.requests[0][1])
    assert reader.read_u64_be() == 42
    assert reader.read_u64_be() == 12
    assert reader.read_u32_be() == 5
    assert reader.read_bytes(5) == b"entry"
    assert reader.read_u8() == 1
    assert reader.read_u32_be() == 4
    assert reader.read_bytes(4) == b"meta"
    assert reader.read_u8() == 1
    assert reader.read_string() == "proj.alpha"


@pytest.mark.asyncio
async def test_stream_read_encodes_filter_payload() -> None:
    connection = _FakeConnection()
    client = StreamClient(connection)

    stream_filter = StreamFilterSet(clauses=[StreamFilterClause(kind="Equals", value="proj.alpha")])
    records = await client.read("stream://realm/area/resource", 5, 10, stream_filter)

    assert records == []
    assert connection.requests[0][0] == MSG_STREAM_READ
    reader = BufferReader(connection.requests[0][1])
    assert reader.read_route() == "stream://realm/area/resource"
    assert reader.read_u64_be() == 5
    assert reader.read_u64_be() == 10
    assert reader.read_u8() == 1
    filter_length = reader.read_u32_be()
    expected_filter = (
        (1).to_bytes(8, "little")
        + (0).to_bytes(4, "little")
        + (10).to_bytes(8, "little")
        + b"proj.alpha"
    )
    assert filter_length == len(expected_filter)
    assert reader.read_bytes(filter_length) == expected_filter
    assert reader.is_eof()


@pytest.mark.asyncio
async def test_stream_session_invalidates_on_disconnect() -> None:
    connection = _FakeConnection()
    session = StreamSession(connection, 42)

    connection.emit_disconnect()

    with pytest.raises(ErrStreamSessionClosed, match="already disconnected"):
        await session.append(0, b"payload")


@pytest.mark.asyncio
async def test_stream_subscribe_decodes_commit_notification() -> None:
    connection = _FakeConnection()
    client = StreamClient(connection)
    route = "stream://realm/area/resource"
    notifications: list[StreamCommitNotification] = []
    delivered = asyncio.Event()

    async def handler(notification: StreamCommitNotification) -> None:
        notifications.append(notification)
        delivered.set()

    subscription = await client.subscribe(route, handler)
    assert subscription.sub_id == 7

    writer = BufferWriter()
    writer.write_u64_be(subscription.sub_id)
    writer.write_route(route)
    payload = json.dumps(
        {
            "event": "committed",
            "first_resource_offset": 0,
            "last_resource_offset": 0,
            "first_area_offset": 0,
            "last_area_offset": 0,
            "first_realm_offset": 0,
            "last_realm_offset": 0,
            "batch_size": 1,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    writer.write_u32_be(len(payload))
    writer.write_bytes(payload)

    notification_handler = connection.notification_handlers[MSG_STREAM_NOTIFY]
    notification_handler(writer.build())

    await asyncio.wait_for(delivered.wait(), timeout=1)
    assert len(notifications) == 1

    notification = notifications[0]
    assert notification.route == route
    assert notification.event == "committed"
    assert notification.first_resource_offset == 0
    assert notification.last_resource_offset == 0
    assert notification.first_area_offset == 0
    assert notification.last_area_offset == 0
    assert notification.first_realm_offset == 0
    assert notification.last_realm_offset == 0
    assert notification.batch_size == 1
