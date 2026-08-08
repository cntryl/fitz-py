from __future__ import annotations

import pytest

from fitz_py import KvDurability
from tests.integration.fixture.fixture import IntegrationFixture, unique_route


@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("auth_mode", ["anonymous", "valid_jwt"])
@pytest.mark.asyncio
async def test_connect_and_kv_round_trip(transport: str, auth_mode: str) -> None:
    fixture = await IntegrationFixture.connect_or_fail(transport, auth_mode)  # type: ignore[arg-type]
    try:
        route = unique_route("kv")
        async with await fixture.client.kv.begin(route, durability=KvDurability.SYNC) as tx:
            await tx.put(b"key", b"value")
            assert (await tx.get(b"key")).value == b"value"
            await tx.commit()
        async with await fixture.client.kv.begin(route, durability=KvDurability.BUFFERED) as tx:
            assert (await tx.get(b"key")).value == b"value"
    finally:
        await fixture.close()


@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.asyncio
async def test_queue_round_trip(transport: str) -> None:
    fixture = await IntegrationFixture.connect_or_fail(transport, "anonymous")
    try:
        route = unique_route("queue")
        await fixture.client.queue.enqueue(route, b"work")
        items = await fixture.client.queue.reserve(route, lease=30, wait=2)
        assert [item.body for item in items] == [b"work"]
        await items[0].complete()
    finally:
        await fixture.close()
