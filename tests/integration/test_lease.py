from __future__ import annotations

import pytest

from fitz_py import LeaseError
from tests.integration.fixture.fixture import IntegrationFixture, unique_route


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("auth_mode", ["anonymous", "valid_jwt"])
async def test_lease_lifecycle(transport: str, auth_mode: str) -> None:
    fixture1 = await IntegrationFixture.connect_or_fail(transport, auth_mode)  # type: ignore[arg-type]
    fixture2 = await IntegrationFixture.connect_or_fail(transport, auth_mode)  # type: ignore[arg-type]
    try:
        route = unique_route("lease")
        lease = await fixture1.client.lease().acquire(route, 30)
        assert lease.token > 0

        with pytest.raises(LeaseError):
            await fixture2.client.lease().acquire(route, 30)

        await lease.extend(30)
        await lease.release()

        lease2 = await fixture2.client.lease().acquire(route, 30)
        assert lease2.token > 0
        await lease2.release()
    finally:
        await fixture1.close()
        await fixture2.close()
