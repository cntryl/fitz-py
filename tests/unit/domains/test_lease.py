"""Lease wildcard subscribe/unsubscribe grammar and LIST wire/client behavior."""

from __future__ import annotations

import asyncio

import pytest

from fitz_py.domains.lease import (
    LeaseClient,
    LeaseListCursor,
    LeaseListItem,
    LeaseListPage,
)
from fitz_py.errors import LeaseError
from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.messages import (
    MSG_LEASE_LIST,
    MSG_LEASE_SUBSCRIBE,
)
from tests.unit.test_contracts import FakeConnection, _coded_error

VALID_SELECTORS = [
    "lease://acme/renderers/document-123",
    "lease://acme/renderers/*",
    "lease://acme/*/document-123",
    "lease://acme/*/*",
    "lease://*/renderers/*",
    "lease://*/*/*",
    "lease://acme/**",
    "lease://**",
]

INVALID_SELECTORS = [
    "lease://acme/renderers",
    "lease://acme/renderers/document-123/extra",
    "lease://acme/renderers/doc*",
    "notice://acme/renderers/*",
]


@pytest.mark.parametrize("selector", VALID_SELECTORS)
def test_lease_subscribe_accepts_full_wildcard_matrix(selector: str) -> None:
    client = LeaseClient(FakeConnection())
    # Should not raise while constructing the lazy iterator's validation path.
    client.subscribe(selector)


@pytest.mark.parametrize("selector", INVALID_SELECTORS)
def test_lease_subscribe_rejects_invalid_selectors(selector: str) -> None:
    client = LeaseClient(FakeConnection())
    with pytest.raises(LeaseError):
        client.subscribe(selector)


# Regression: `is_selector_route_shape(pattern, "lease", 3,
# allow_realm_wildcard=True)` alone (without `allow_interior_double_star`)
# rejects a non-trailing `**`, e.g. `lease://**/renderers/document-123`
# (2 fixed segments + 1 flexible `**` = the 3-segment-equivalent shape the
# broker's PatternDepth::CanMatch(3) grammar accepts per
# src/runtime/matcher.rs). Locking in that lease.py's `_pattern()` passes
# `allow_interior_double_star=True` so this shape is accepted, and that
# adjacent `**` segments (never valid per the broker grammar) stay rejected.
NON_TRAILING_DOUBLE_STAR_SELECTORS = [
    "lease://**/renderers/document-123",
    "lease://acme/**/document-123",
    "lease://acme/**/*",
]


@pytest.mark.parametrize("selector", NON_TRAILING_DOUBLE_STAR_SELECTORS)
def test_lease_subscribe_accepts_non_trailing_double_star(selector: str) -> None:
    client = LeaseClient(FakeConnection())
    # Should not raise while constructing the lazy iterator's validation path.
    client.subscribe(selector)


def test_lease_subscribe_rejects_adjacent_double_star() -> None:
    client = LeaseClient(FakeConnection())
    with pytest.raises(LeaseError):
        client.subscribe("lease://**/**")


@pytest.mark.parametrize("selector", VALID_SELECTORS)
@pytest.mark.asyncio
async def test_lease_subscribe_wire_accepts_full_wildcard_matrix(selector: str) -> None:
    subscribe = BufferWriter()
    subscribe.write_u8(0)
    subscribe.write_u64_be(8)
    connection = FakeConnection({MSG_LEASE_SUBSCRIBE: subscribe.build()})
    client = LeaseClient(connection)

    sub_id = await client._subscribe_wire(selector)

    assert sub_id == 8
    reader = BufferReader(connection.sent[0][1])
    assert reader.read_route() == selector


@pytest.mark.parametrize("selector", INVALID_SELECTORS)
@pytest.mark.asyncio
async def test_lease_unsubscribe_wire_is_reached_only_for_valid_selectors(selector: str) -> None:
    # Unsubscribe wire itself does not validate (it mirrors subscribe's already-
    # validated route through the SubscriptionRegistry), but the public
    # `subscribe()` entrypoint must reject these before any wire traffic occurs.
    client = LeaseClient(FakeConnection())
    with pytest.raises(LeaseError):
        client.subscribe(selector)


def test_acquire_query_extend_release_still_require_exact_routes() -> None:
    client = LeaseClient(FakeConnection())
    for wildcard in ("lease://acme/renderers/*", "lease://acme/**", "lease://**"):
        with pytest.raises(LeaseError):
            asyncio.run(client.acquire(wildcard, ttl=30))
        with pytest.raises(LeaseError):
            asyncio.run(client.query(wildcard))


def _list_success_page(
    *,
    items: list[tuple[str, str, int, str, int, int]],
    next_cursor: tuple[int, int] | None,
) -> bytes:
    writer = BufferWriter()
    writer.write_u8(0)
    writer.write_u32_be(len(items))
    for route, owner_id, incarnation, acquired_at, expires_in_secs, renewals in items:
        writer.write_route(route)
        writer.write_route(owner_id)
        writer.write_u64_be(incarnation)
        writer.write_route(acquired_at)
        writer.write_u64_be(expires_in_secs)
        writer.write_u32_be(renewals)
    if next_cursor is None:
        writer.write_u8(0)
    else:
        writer.write_u8(1)
        writer.write_u64_be(next_cursor[0])
        writer.write_u32_be(next_cursor[1])
    return writer.build()


@pytest.mark.asyncio
async def test_lease_list_page_round_trips_request_and_response() -> None:
    payload = _list_success_page(
        items=[
            ("lease://acme/renderers/doc-1", "owner-a", 42, "2026-08-29T00:00:00Z", 30, 2),
            ("lease://acme/renderers/doc-2", "owner-b", 43, "2026-08-29T00:01:00Z", 30, 0),
        ],
        next_cursor=(99, 2),
    )
    connection = FakeConnection({MSG_LEASE_LIST: payload})
    client = LeaseClient(connection)

    page = await client.list_page("lease://acme/renderers/*", limit=50)

    assert isinstance(page, LeaseListPage)
    assert page.items == (
        LeaseListItem("lease://acme/renderers/doc-1", "owner-a", 42, "2026-08-29T00:00:00Z", 30, 2),
        LeaseListItem("lease://acme/renderers/doc-2", "owner-b", 43, "2026-08-29T00:01:00Z", 30, 0),
    )
    assert page.cursor == LeaseListCursor(99, 2)

    reader = BufferReader(connection.sent[0][1])
    assert reader.read_route() == "lease://acme/renderers/*"
    assert reader.read_u8() == 0  # no cursor on first call
    assert reader.read_u32_be() == 50
    assert reader.is_eof()


@pytest.mark.asyncio
async def test_lease_list_page_encodes_cursor_when_provided() -> None:
    payload = _list_success_page(items=[], next_cursor=None)
    connection = FakeConnection({MSG_LEASE_LIST: payload})
    client = LeaseClient(connection)

    page = await client.list_page(
        "lease://**", cursor=LeaseListCursor(snapshot_id=7, offset=100), limit=None
    )

    assert page.items == ()
    assert page.cursor is None

    reader = BufferReader(connection.sent[0][1])
    reader.read_route()
    assert reader.read_u8() == 1
    assert reader.read_u64_be() == 7
    assert reader.read_u32_be() == 100
    assert reader.read_u32_be() == 0  # limit None -> server default (0)
    assert reader.is_eof()


@pytest.mark.asyncio
async def test_lease_list_page_rejects_invalid_pattern_before_io() -> None:
    connection = FakeConnection()
    client = LeaseClient(connection)

    with pytest.raises(LeaseError):
        await client.list_page("lease://acme/renderers/doc*")

    assert connection.sent == []


@pytest.mark.asyncio
async def test_lease_list_leases_pages_through_a_multi_page_scan() -> None:
    first_page = _list_success_page(
        items=[("lease://acme/renderers/doc-1", "owner-a", 1, "2026-08-29T00:00:00Z", 30, 0)],
        next_cursor=(5, 1),
    )
    second_page = _list_success_page(
        items=[("lease://acme/renderers/doc-2", "owner-b", 2, "2026-08-29T00:01:00Z", 30, 0)],
        next_cursor=None,
    )
    pages = [first_page, second_page]

    class PagingConnection(FakeConnection):
        async def request(self, message_type: int, payload: bytes) -> bytes:
            self.sent.append((message_type, payload))
            return pages.pop(0)

    connection = PagingConnection()
    client = LeaseClient(connection)

    items = [item async for item in client.list_leases("lease://acme/renderers/*")]

    assert [item.route for item in items] == [
        "lease://acme/renderers/doc-1",
        "lease://acme/renderers/doc-2",
    ]
    assert len(connection.sent) == 2
    second_request = BufferReader(connection.sent[1][1])
    second_request.read_route()
    assert second_request.read_u8() == 1
    assert second_request.read_u64_be() == 5
    assert second_request.read_u32_be() == 1


@pytest.mark.asyncio
async def test_lease_list_decodes_new_coded_errors() -> None:
    for code, message in ((5011, "bad cursor"), (5012, "bad pattern")):
        connection = FakeConnection({MSG_LEASE_LIST: _coded_error(code, message)})
        client = LeaseClient(connection)
        with pytest.raises(LeaseError, match=message) as raised:
            await client.list_page("lease://**")
        assert raised.value.domain_code == code
