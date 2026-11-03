from __future__ import annotations

import pytest

from tests.integration.fixture.fixture import IntegrationFixture, unique_route


@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("auth_mode", ["anonymous", "valid_jwt"])
@pytest.mark.asyncio
async def test_should_relinquish_ttl_message_immediately_through_nack(transport, auth_mode):
    fixture = await IntegrationFixture.connect_or_fail(transport, auth_mode)
    try:
        route = unique_route("queue")
        await fixture.client.queue.enqueue(route, b"wire3", ttl_hours=1)
        first = await fixture.client.queue.reserve(route, lease=30)
        await first[0].nack()
        second = await fixture.client.queue.reserve(route, lease=30)
        assert [item.body for item in second] == [b"wire3"]
        await second[0].complete()
    finally:
        await fixture.aclose()


@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.asyncio
async def test_should_defer_visibility_after_delayed_nack(transport):
    fixture = await IntegrationFixture.connect_or_fail(transport, "anonymous")
    try:
        route = unique_route("queue")
        await fixture.client.queue.enqueue(route, b"delay")
        items = await fixture.client.queue.reserve(route, lease=30)
        await items[0].nack(60)
        assert await fixture.client.queue.reserve(route, lease=30) == []
    finally:
        await fixture.aclose()
