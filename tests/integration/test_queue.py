from __future__ import annotations

import pytest

from tests.integration.fixture.fixture import IntegrationFixture, unique_route


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("auth_mode", ["anonymous", "valid_jwt"])
async def test_queue_lifecycle(transport: str, auth_mode: str) -> None:
    fixture = await IntegrationFixture.connect_or_fail(transport, auth_mode)  # type: ignore[arg-type]
    try:
        route = unique_route("queue")
        msg_id = await fixture.client.queue().enqueue(route, b"payload")
        assert msg_id > 0

        items = await fixture.client.queue().reserve(route, 30, batch_size=1)
        assert len(items) == 1
        assert items[0].body == b"payload"

        await items[0].extend(30)
        await items[0].complete()

        assert await fixture.client.queue().reserve(route, 30, batch_size=1) == []
    finally:
        await fixture.close()
