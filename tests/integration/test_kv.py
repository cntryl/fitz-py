from __future__ import annotations

import pytest

from tests.integration.fixture.fixture import IntegrationFixture, unique_route


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("auth_mode", ["anonymous", "valid_jwt"])
async def test_kv_round_trip(transport: str, auth_mode: str) -> None:
    fixture = await IntegrationFixture.connect_or_fail(transport, auth_mode)  # type: ignore[arg-type]
    try:
        route = unique_route("kv")
        tx = await fixture.client.kv().begin(route, durability="sync")
        await tx.put(b"hello", b"world")
        await tx.commit()

        rtx = await fixture.client.kv().begin(route, mode="read_only", durability="sync")
        result = await rtx.get(b"hello")
        await rtx.rollback()

        assert result.found is True
        assert result.value == b"world"
    finally:
        await fixture.close()
