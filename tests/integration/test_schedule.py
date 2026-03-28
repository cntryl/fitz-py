from __future__ import annotations

import asyncio

import pytest

from tests.integration.fixture.fixture import IntegrationFixture, unique_route


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("auth_mode", ["anonymous", "valid_jwt"])
async def test_schedule_create_subscribe_cancel(transport: str, auth_mode: str) -> None:
    fixture = await IntegrationFixture.connect_or_fail(transport, auth_mode)  # type: ignore[arg-type]
    try:
        route = unique_route("schedule")

        async def handler(_notification) -> None:
            return None

        sub = await fixture.client.schedule().subscribe(route, handler)
        schedule_id = await fixture.client.schedule().create(route, "0 9 * * 1", b"payload")
        assert schedule_id == route or isinstance(schedule_id, str)

        await fixture.client.schedule().cancel(route)
        await sub.unsubscribe()
    finally:
        await fixture.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("auth_mode", ["anonymous", "valid_jwt"])
async def test_schedule_multiple_local_subscribers_same_pattern(
    transport: str, auth_mode: str
) -> None:
    fixture = await IntegrationFixture.connect_or_fail(transport, auth_mode)  # type: ignore[arg-type]
    try:
        route = unique_route("schedule")
        received_one: list[bytes] = []
        received_two: list[bytes] = []

        async def handler_one(notification) -> None:
            received_one.append(notification.payload)

        async def handler_two(notification) -> None:
            received_two.append(notification.payload)

        sub_one = await fixture.client.schedule().subscribe(route, handler_one)
        sub_two = await fixture.client.schedule().subscribe(route, handler_two)

        assert sub_one.sub_id == sub_two.sub_id
        assert sub_one.pattern == sub_two.pattern == route

        payload = len(b"fanout").to_bytes(4, "big") + b"fanout"
        fixture.client.schedule().connection.get_multiplexer().dispatch(  # type: ignore[attr-defined]
            705, sub_one.sub_id.to_bytes(8, "big") + payload
        )
        await asyncio.sleep(0)

        assert received_one == [b"fanout"]
        assert received_two == [b"fanout"]

        await sub_one.unsubscribe()
        fixture.client.schedule().connection.get_multiplexer().dispatch(  # type: ignore[attr-defined]
            705, sub_two.sub_id.to_bytes(8, "big") + payload
        )
        await asyncio.sleep(0)
        assert received_one == [b"fanout"]
        assert received_two == [b"fanout", b"fanout"]

        await sub_two.unsubscribe()
    finally:
        await fixture.close()
