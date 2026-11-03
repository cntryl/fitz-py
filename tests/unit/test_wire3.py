from __future__ import annotations

import asyncio
import struct
from pathlib import Path

import pytest

from fitz_py.connection import Connection
from fitz_py.domains.queue import QueueClient, QueueItem
from fitz_py.errors import CodecError, ProtocolError, QueueError, StaleHandleError
from fitz_py.protocol.extensions import decode_payload, encode_capabilities, encode_payload
from fitz_py.protocol.frame import FrameCodec, FrameParser
from tests.unit.test_connection_lifecycle import FakeTransport, config

VECTORS = dict(
    line.split() for line in Path("tests/fixtures/wire3_vectors.txt").read_text().splitlines()
)


def test_should_match_shared_handshake_vector() -> None:
    vectors = VECTORS
    frame = FrameCodec.decode_frame(bytes.fromhex(vectors["hello_wire3_queue_capabilities"]))
    core, capabilities = decode_payload(frame.payload, frame.message_type, frame.flags)
    assert struct.unpack(">HI IIQ", core) == (3, 0, 1048576, 64, 42)
    assert capabilities == (1, 2)


def test_should_preserve_core_only_bytes() -> None:
    core = b"hi"
    assert encode_payload(core, ()) == core
    assert decode_payload(core, 104, 0) == (core, ())


def test_should_skip_bounded_unknown_fields_in_any_order() -> None:
    payload = encode_payload(b"hi", ((4096, b"future"), (4097, b"later")))
    assert decode_payload(payload, 104, 4) == (b"hi", ())


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"\0\0\0\x10hi",
        b"\0\0\0\0",
        b"\0\0\0\0\0",
        struct.pack(">IHI", 0, 0, 0),
        struct.pack(">IHI", 0, 9, 0xFFFFFFFF),
    ],
)
def test_should_reject_malformed_envelopes(payload: bytes) -> None:
    with pytest.raises(CodecError):
        decode_payload(payload, 104, 4)


def test_should_reject_duplicate_handshake_singleton() -> None:
    field = struct.pack(">HI", 1, 2) + b"\0\0"
    with pytest.raises(CodecError):
        decode_payload(struct.pack(">I", 22) + bytes(22) + field + field, 2, 4)


@pytest.mark.parametrize("capabilities", [(0,), (2, 1), (1, 1)])
def test_should_reject_invalid_capability_lists(capabilities: tuple[int, ...]) -> None:
    with pytest.raises(CodecError):
        encode_capabilities(capabilities)


def test_should_require_explicit_hello_capabilities() -> None:
    with pytest.raises(CodecError):
        decode_payload(bytes(22), 2, 0)


def test_should_reject_every_truncated_prefix_of_wire3_hello() -> None:
    payload = FrameCodec.decode_frame(
        bytes.fromhex(VECTORS["hello_wire3_queue_capabilities"])
    ).payload
    for size in range(len(payload)):
        with pytest.raises(CodecError):
            decode_payload(payload[:size], 2, 4)


def test_should_skip_repeated_unknown_fields_in_any_order() -> None:
    payload = bytes.fromhex("000000026869100100000000100000000000100000000000")
    assert decode_payload(payload, 104, 4) == (b"hi", ())


async def complete_request(transport, operation, response):
    count = len(transport.sent)
    task = asyncio.create_task(operation)
    async with asyncio.timeout(1):
        while len(transport.sent) == count:  # noqa: ASYNC110
            await asyncio.sleep(0)
    frame = FrameCodec.decode_frame(transport.sent[-1])
    transport.inbound.put_nowait(
        FrameCodec.encode_frame(
            frame.message_type, response, flags=2, correlation_id=frame.correlation_id
        )
    )
    await task
    return transport.sent[-1]


@pytest.mark.asyncio
async def test_should_encode_ttl_exactly_like_shared_vector() -> None:
    transport = FakeTransport()
    connection = Connection(lambda: transport, config())
    await connection.connect()
    try:
        frame = await complete_request(
            transport,
            QueueClient(connection).enqueue("queue://acme/jobs/q", b"job", ttl_hours=2),
            b"\0" + (1).to_bytes(8, "big"),
        )
        vectors = VECTORS
        assert frame.hex() == vectors["enqueue_wire3_ttl2h_corr1"]
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_should_close_reservation_after_delayed_nack() -> None:
    transport = FakeTransport()
    connection = Connection(lambda: transport, config())
    await connection.connect()
    try:
        item = QueueItem(
            "queue://acme/jobs/q", b"job", 1, 9, QueueClient(connection), connection.generation
        )
        frame = await complete_request(transport, item.nack(60), b"\0")
        decoded = FrameCodec.decode_frame(frame)
        assert decoded.message_type == 210
        assert int.from_bytes(decoded.payload[-8:], "big") == 60
        with pytest.raises(StaleHandleError, match="Queue item"):
            await item.complete()
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_should_reject_ttl_without_negotiated_support_before_sending() -> None:
    transport = FakeTransport(send_hello=False)
    vectors = VECTORS
    transport.inbound.put_nowait(bytes.fromhex(vectors["hello_wire3_empty_capabilities_session42"]))
    connection = Connection(lambda: transport, config())
    await connection.connect()
    try:
        with pytest.raises(QueueError, match="not negotiated"):
            await QueueClient(connection).enqueue("queue://acme/jobs/q", b"job", ttl_hours=2)
        assert len(transport.sent) == 1
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_should_reject_repeated_hello_after_negotiating_session() -> None:
    transport = FakeTransport()
    connection = Connection(lambda: transport, config())
    await connection.connect()
    try:
        with pytest.raises(ProtocolError, match="Repeated HELLO"):
            await connection._dispatch_transport_frame(
                bytes.fromhex(VECTORS["hello_wire3_queue_capabilities"]), FrameParser()
            )
    finally:
        await connection.close()
