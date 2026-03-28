from __future__ import annotations

import asyncio

import pytest

from fitz_py.errors import TimeoutError
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


@pytest.mark.asyncio
async def test_multiplexer_clears_pending_on_send_failure() -> None:
    mux = Multiplexer()
    mux.set_connected()

    async def send(_data: bytes) -> None:
        raise RuntimeError("send failed")

    with pytest.raises(RuntimeError, match="send failed"):
        await mux.request(101, b"frame", send, 1000)

    assert 101 not in mux._pending  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_multiplexer_times_out_and_clears_pending() -> None:
    mux = Multiplexer()
    mux.set_connected()

    async def send(_data: bytes) -> None:
        return None

    with pytest.raises(TimeoutError, match="Request timeout"):
        await mux.request(102, b"frame", send, 1)

    assert 102 not in mux._pending  # type: ignore[attr-defined]
