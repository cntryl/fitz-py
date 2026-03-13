from __future__ import annotations

import asyncio

import pytest

from fitz_py.multiplexer import Multiplexer


@pytest.mark.asyncio
async def test_multiplexer_matches_fifo_request_response() -> None:
    mux = Multiplexer()
    mux.set_connected()
    sent: list[bytes] = []

    async def send(data: bytes) -> None:
        sent.append(data)

    task = asyncio.create_task(mux.request(100, b"frame", send, 1000))
    await asyncio.sleep(0)
    mux.dispatch(100, b"response")
    assert await task == b"response"
    assert sent == [b"frame"]


def test_multiplexer_ignores_optional_response() -> None:
    mux = Multiplexer()
    mux.set_connected()
    mux.expect_optional_response(500)
    mux.dispatch(500, b"ok")
