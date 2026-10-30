from __future__ import annotations

import pytest

from fitz_py import StreamError, StreamReadItemKind
from tests.integration.fixture.fixture import IntegrationFixture, unique_route


@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("auth_mode", ["anonymous", "valid_jwt"])
@pytest.mark.asyncio
async def test_event_metadata_distinguishes_empty_and_absent(
    transport: str, auth_mode: str
) -> None:
    fixture = await IntegrationFixture.connect_or_fail(transport, auth_mode)  # type: ignore[arg-type]
    try:
        route = unique_route("stream")
        session = await fixture.client.stream.begin(route)
        await session.append(0, b"empty", metadata=b"")
        await session.append(1, b"absent")
        await session.commit("sync")

        records = await fixture.client.stream.read(route, 0, limit=10)

        assert [record.metadata for record in records] == [b"", None]
    finally:
        await fixture.aclose()


@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("auth_mode", ["anonymous", "valid_jwt"])
@pytest.mark.asyncio
async def test_event_size_boundary_and_long_route_replay(transport: str, auth_mode: str) -> None:
    fixture = await IntegrationFixture.connect_or_fail(transport, auth_mode)  # type: ignore[arg-type]
    try:
        prefix = unique_route("stream")
        route = prefix + "r" * (4096 - len(prefix.encode()))
        metadata = b"0123456789abcdef"
        body = b"x" * (400 * 1024 - len(metadata))
        for offset in range(3):
            session = await fixture.client.stream.begin(route)
            if offset == 0:
                with pytest.raises(StreamError) as caught:
                    await session.append(0, body + b"x", metadata=metadata)
                assert caught.value.domain_code == 2008
                assert caught.value.effect == 0
            assert await session.append(offset, body, metadata=metadata) == offset
            await session.commit("sync")

        records = []
        next_offset = 0
        pages = 0
        while True:
            page = await fixture.client.stream.read_page(route, next_offset, limit=10)
            pages += 1
            records.extend(
                item.record for item in page.items if item.kind == StreamReadItemKind.EVENT
            )
            if not page.cursor.has_more:
                break
            assert page.cursor.last_resource_offset >= next_offset
            next_offset = page.cursor.last_resource_offset + 1

        assert pages > 1
        assert len(records) == 3
        for offset, record in enumerate(records):
            assert record is not None
            assert record.route == route
            assert record.offset == offset
            assert record.body == body
            assert record.metadata == metadata
    finally:
        await fixture.aclose()
