from __future__ import annotations

import asyncio

import pytest

from fitz_py import StreamCommitMode, StreamCommitNotification
from tests.integration.fixture.fixture import IntegrationFixture, unique_route


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("auth_mode", ["anonymous", "valid_jwt"])
async def test_stream_round_trip(transport: str, auth_mode: str) -> None:
    fixture = await IntegrationFixture.connect_or_fail(transport, auth_mode)  # type: ignore[arg-type]
    try:
        route = unique_route("stream")
        session = await fixture.client.stream().begin(route)
        await session.append(0, b"first")
        await session.append(1, b"second")
        await session.commit()

        records = await fixture.client.stream().read(route, start_offset=0, limit=10)
        assert [record.body for record in records[:2]] == [b"first", b"second"]
    finally:
        await fixture.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("auth_mode", ["anonymous", "valid_jwt"])
async def test_stream_commit_notification_shape(transport: str, auth_mode: str) -> None:
    fixture = await IntegrationFixture.connect_or_fail(transport, auth_mode)  # type: ignore[arg-type]
    try:
        route = unique_route("stream")
        notifications: list[StreamCommitNotification] = []
        delivered = asyncio.Event()

        async def handler(notification: StreamCommitNotification) -> None:
            notifications.append(notification)
            delivered.set()

        subscription = await fixture.client.stream().subscribe(route, handler)

        session = await fixture.client.stream().begin(route)
        await session.append(0, b"notify")
        await session.commit(StreamCommitMode.SYNC)

        await asyncio.wait_for(delivered.wait(), timeout=5)
        assert len(notifications) == 1

        notification = notifications[0]
        assert notification.route == route
        assert notification.event == "committed"
        assert notification.first_resource_offset == 0
        assert notification.last_resource_offset == 0
        assert notification.first_area_offset == 0
        assert notification.last_area_offset == 0
        assert notification.first_realm_offset == 0
        assert notification.last_realm_offset == 0
        assert notification.batch_size == 1

        await subscription.unsubscribe()
    finally:
        await fixture.close()
