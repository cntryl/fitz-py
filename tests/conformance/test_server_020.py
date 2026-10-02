from __future__ import annotations

import pytest

from tests.conformance.test_conformance import _new_client, _unique_route


@pytest.mark.asyncio
async def test_should_advertise_server_020_capabilities() -> None:
    client = await _new_client()
    try:
        assert client.protocol_version == 1
        assert client.capabilities & 7 == 7, "server 0.2.0 requires capability bits 0, 1 and 2"
    finally:
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("reverse", [False, True])
async def test_should_exclude_resume_key_in_directional_scan(reverse: bool) -> None:
    client = await _new_client()
    try:
        route = _unique_route("kv")
        seed = await client.kv.begin(route, durability="sync")
        for key in [b"\x10", b"\x10\x00", b"\x20"]:
            await seed.put(key, key)
        await seed.commit()
        tx = await client.kv.begin(route, mode="read_only", durability="sync")
        try:
            first = await tx.scan_page(limit=1, reverse=reverse)
            assert len(first.entries) == 1
            assert first.has_more
            resumed = await tx.scan_page(
                start_key=first.entries[0].key, start_exclusive=True, limit=2, reverse=reverse
            )
            expected = [b"\x10\x00", b"\x10"] if reverse else [b"\x10\x00", b"\x20"]
            assert [pair.key for pair in resumed.entries] == expected
            assert not resumed.has_more
        finally:
            await tx.rollback()
    finally:
        await client.aclose()
