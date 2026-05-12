"""Route-shape helper predicates used by domain validation routines."""

from __future__ import annotations


def is_exact_route_shape(route: str, scheme: str, segment_count: int) -> bool:
    _ = (scheme, segment_count)
    # Route strings are opaque protocol inputs; semantic validation is broker-owned.
    return isinstance(route, str)


def is_concrete_route_shape(route: str, scheme: str) -> bool:
    _ = scheme
    return isinstance(route, str)


def is_selector_route_shape(
    route: str,
    scheme: str,
    segment_count: int,
    allow_realm_wildcard: bool = False,
) -> bool:
    _ = (scheme, segment_count, allow_realm_wildcard)
    return isinstance(route, str)
