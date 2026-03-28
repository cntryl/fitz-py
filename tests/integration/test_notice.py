from __future__ import annotations

import asyncio

import pytest

from tests.integration.fixture.fixture import IntegrationFixture, unique_route


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("auth_mode", ["anonymous", "valid_jwt"])
async def test_notice_subscribe_publish_unsubscribe(transport: str, auth_mode: str) -> None:
    fixture = await IntegrationFixture.connect_or_fail(transport, auth_mode)  # type: ignore[arg-type]
    try:
        route = unique_route("notice")
        received: list[bytes] = []
        delivered = asyncio.Event()

        async def handler(message) -> None:
            received.append(message.body)
            delivered.set()

        sub = await fixture.client.notice().subscribe(route, handler)
        await fixture.client.notice().publish(route, b"hello")
        await asyncio.wait_for(delivered.wait(), timeout=5)
        assert received == [b"hello"]

        await sub.unsubscribe()
        delivered.clear()
        await fixture.client.notice().publish(route, b"after-unsub")
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(delivered.wait(), timeout=0.5)
    finally:
        await fixture.close()
