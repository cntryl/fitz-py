import pytest

from fitz_py import KVDurability, StreamCommitMode
from tests.integration.fixture.fixture import IntegrationFixture, unique_route


@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("auth_mode", ["anonymous", "valid_jwt"])
@pytest.mark.parametrize("mode", [KVDurability.BUFFERED, KVDurability.SYNC])
async def test_both_domains_commit_with_explicit_persistence(transport, auth_mode, mode):
    fixture = await IntegrationFixture.connect_or_fail(transport, auth_mode)
    try:
        route = unique_route("kv")
        async with fixture.client.kv.transaction(route) as tx:
            await tx.put(b"key", b"value")
            await tx.commit(mode)
        async with fixture.client.kv.transaction(route) as tx:
            assert await tx.get(b"key") == b"value"
        session = await fixture.client.stream.begin(unique_route("stream"))
        await session.append(0, b"record")
        await session.commit(StreamCommitMode(mode.value))
    finally:
        await fixture.aclose()
