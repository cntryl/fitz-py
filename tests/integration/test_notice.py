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


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("auth_mode", ["anonymous", "valid_jwt"])
async def test_notice_multiple_local_subscribers_same_pattern(
    transport: str, auth_mode: str
) -> None:
    fixture = await IntegrationFixture.connect_or_fail(transport, auth_mode)  # type: ignore[arg-type]
    try:
        route = unique_route("notice")
        received_one: list[bytes] = []
        received_two: list[bytes] = []
        delivered_one = asyncio.Event()
        delivered_two = asyncio.Event()

        async def handler_one(message) -> None:
            received_one.append(message.body)
            delivered_one.set()

        async def handler_two(message) -> None:
            received_two.append(message.body)
            delivered_two.set()

        sub_one = await fixture.client.notice().subscribe(route, handler_one)
        sub_two = await fixture.client.notice().subscribe(route, handler_two)

        assert sub_one.sub_id == sub_two.sub_id
        assert sub_one.pattern == sub_two.pattern == route

        await fixture.client.notice().publish(route, b"fanout")
        await asyncio.wait_for(delivered_one.wait(), timeout=5)
        await asyncio.wait_for(delivered_two.wait(), timeout=5)

        assert received_one == [b"fanout"]
        assert received_two == [b"fanout"]

        delivered_two.clear()
        await sub_one.unsubscribe()
        await fixture.client.notice().publish(route, b"second")
        await asyncio.wait_for(delivered_two.wait(), timeout=5)

        assert received_one == [b"fanout"]
        assert received_two == [b"fanout", b"second"]

        await sub_two.unsubscribe()
    finally:
        await fixture.close()
