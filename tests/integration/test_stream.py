from __future__ import annotations

import pytest

from tests.integration.fixture.fixture import IntegrationFixture, unique_route


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("auth_mode", ["anonymous", "valid_jwt"])
async def test_stream_round_trip(transport: str, auth_mode: str) -> None:
    fixture = await IntegrationFixture.connect_or_fail(transport, auth_mode)  # type: ignore[arg-type]
    try:
        route = unique_route("stream")
        session = await fixture.client.stream().begin(route, expected_offset=0)
        await session.append(b"first")
        await session.append(b"second")
        await session.commit()

        records = await fixture.client.stream().read(route, start_offset=0, limit=10)
        assert [record.body for record in records[:2]] == [b"first", b"second"]
    finally:
        await fixture.close()
