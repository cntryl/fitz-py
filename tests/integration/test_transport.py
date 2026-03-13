from __future__ import annotations

import pytest

from fitz_py import ConnectionState
from tests.integration.fixture.fixture import IntegrationFixture


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("auth_mode", ["anonymous", "valid_jwt"])
async def test_connects_to_broker(transport: str, auth_mode: str) -> None:
    fixture = await IntegrationFixture.connect_or_fail(transport, auth_mode)  # type: ignore[arg-type]
    try:
        assert fixture.client.state is ConnectionState.AUTHENTICATED
    finally:
        await fixture.close()
