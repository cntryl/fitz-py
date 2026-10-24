from __future__ import annotations

import asyncio

import pytest

from fitz_py.connection import Connection
from fitz_py.protocol.frame import FrameCodec
from fitz_py.protocol.messages import MSG_SERVER_HELLO
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
