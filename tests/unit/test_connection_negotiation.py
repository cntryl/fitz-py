from __future__ import annotations

import asyncio

import pytest

from fitz_py.connection import Connection
from fitz_py.errors import FitzTransportError, ProtocolError, is_retryable
from fitz_py.protocol.buffer import BufferWriter
from fitz_py.protocol.frame import FrameCodec
from fitz_py.protocol.messages import MSG_CONNECT_REJECTED, MSG_SERVER_HELLO
from fitz_py.types import ReconnectPolicy
from tests.unit.test_connection_lifecycle import FakeTransport, config, hello_payload


@pytest.mark.asyncio
async def test_connect_waits_for_hello_beyond_authentication_settlement() -> None:
    transport = FakeTransport(send_hello=False)
    connection = Connection(lambda: transport, config(auth_settle_timeout=0.001, request_timeout=1))
    connected = asyncio.create_task(connection.connect())
    try:
        await asyncio.sleep(0.025)
        assert not connected.done()
        hello = hello_payload()
        await transport.inbound.put(FrameCodec.encode_frame(MSG_SERVER_HELLO, hello))
        await connected
        assert connection.protocol_version == 2
        assert connection.capabilities == 0
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_connect_rejects_absent_hello_within_configured_timeout() -> None:
    transport = FakeTransport(send_hello=False)
    connection = Connection(
        lambda: transport, config(auth_settle_timeout=0.001, request_timeout=0.025)
    )
    with pytest.raises(TimeoutError):
        await connection.connect()
    assert transport.closed
    await connection.close()


@pytest.mark.asyncio
async def test_version_mismatch_is_a_non_retryable_protocol_error() -> None:
    transport = FakeTransport(send_hello=False)
    payload = BufferWriter()
    payload.write_u16_be(50)
    payload.write_string("no common wire version")
    transport.inbound.put_nowait(FrameCodec.encode_frame(MSG_CONNECT_REJECTED, payload.build()))
    connection = Connection(lambda: transport, config(request_timeout=1))
    try:
        with pytest.raises(ProtocolError) as rejected:
            await connection.connect()
        assert rejected.value.domain_code == 50
        assert "no common wire version" in str(rejected.value)
        assert not is_retryable(rejected.value)
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_reconnect_stops_after_version_mismatch() -> None:
    first = FakeTransport()
    rejected = FakeTransport(send_hello=False)
    payload = BufferWriter()
    payload.write_u16_be(50)
    payload.write_string("no common wire version")
    rejected.inbound.put_nowait(FrameCodec.encode_frame(MSG_CONNECT_REJECTED, payload.build()))
    transports = [first, rejected]
    attempts = 0

    def factory() -> FakeTransport:
        nonlocal attempts
        attempts += 1
        return transports.pop(0) if transports else FakeTransport()

    connection = Connection(
        factory,
        config(
            reconnect=ReconnectPolicy(enabled=True, max_attempts=5, backoff=0.01, max_backoff=0.01)
        ),
    )
    try:
        await connection.connect()
        await first.inbound.put(FitzTransportError("transport lost"))
        async with asyncio.timeout(1):
            while connection._reconnect_task is None:  # noqa: ASYNC110
                await asyncio.sleep(0.001)
        await asyncio.wait_for(asyncio.shield(connection._reconnect_task), timeout=1)
        assert attempts == 2
        assert not connection.is_connected()
    finally:
        await connection.close()
