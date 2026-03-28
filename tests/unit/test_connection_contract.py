from __future__ import annotations

import asyncio

import pytest

from fitz_py.connection import Connection
from fitz_py.types import ConnectionState


class _FakeTransport:
    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def send(self, _data: bytes) -> None:
        return None

    async def receive(self) -> bytes:
        return await asyncio.Future()

    def get_url(self) -> str:
        return "ws://example.test"


async def _empty_token() -> str:
    return ""


def test_connection_state_exposes_connected() -> None:
    assert ConnectionState.CONNECTED.value == "CONNECTED"


@pytest.mark.asyncio
async def test_connection_emits_connected_before_authenticated() -> None:
    seen: list[ConnectionState] = []
    connection = Connection(lambda: _FakeTransport(), _empty_token, auth_settle_delay_ms=0)
    original_set_state = connection._set_state

    def record(state: ConnectionState) -> None:
        seen.append(state)
        original_set_state(state)

    connection._set_state = record  # type: ignore[method-assign]

    try:
        await connection.connect()

        assert seen[:4] == [
            ConnectionState.CONNECTING,
            ConnectionState.CONNECTED,
            ConnectionState.AUTHENTICATING,
            ConnectionState.AUTHENTICATED,
        ]
    finally:
        await connection.close()
