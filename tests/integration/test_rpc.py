from __future__ import annotations

import pytest

from tests.integration.fixture.fixture import IntegrationFixture, unique_route


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("auth_mode", ["anonymous", "valid_jwt"])
async def test_rpc_worker_round_trip(transport: str, auth_mode: str) -> None:
    worker = await IntegrationFixture.connect_or_fail(transport, auth_mode)  # type: ignore[arg-type]
    caller = await IntegrationFixture.connect_or_fail(transport, auth_mode)  # type: ignore[arg-type]
    try:
        route = unique_route("rpc")

        async def handler(req, writer) -> None:
            await writer.send(req.body.upper(), is_end=True)

        sub = await worker.client.rpc().register_worker(route, handler)
        iterator = await caller.client.rpc().call(route, b"ping", timeout_ms=2000)
        frames = [frame async for frame in iterator]
        assert [frame.body for frame in frames] == [b"PING"]
        await sub.unsubscribe()
    finally:
        await worker.close()
        await caller.close()
