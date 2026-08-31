"""Strict, opaque route-shape validation."""

from __future__ import annotations

import itertools


def _parts(
    route: str,
    scheme: str,
    *,
    allow_interior_double_star: bool = False,
) -> list[str] | None:
    prefix = f"{scheme}://"
    if not isinstance(route, str) or not route.startswith(prefix):  # pyright: ignore[reportUnnecessaryIsInstance]
        return None
    parts = route[len(prefix) :].split("/")
    if not parts or any(not part for part in parts):
        return None
    if any("*" in part and part not in {"*", "**"} for part in parts):
        return None
    if not allow_interior_double_star and "**" in parts and parts[-1] != "**":
        return None
    return parts


def is_exact_route_shape(route: str, scheme: str, segment_count: int) -> bool:
    parts = _parts(route, scheme)
    return parts is not None and len(parts) == segment_count and all("*" not in p for p in parts)


def is_concrete_route_shape(route: str, scheme: str) -> bool:
    parts = _parts(route, scheme)
    return parts is not None and all("*" not in p for p in parts)


def is_selector_route_shape(
    route: str,
    scheme: str,
    segment_count: int,
    allow_realm_wildcard: bool = False,
    *,
    allow_interior_double_star: bool = False,
) -> bool:
    """Check a selector's shape for a domain's ``segment_count``-deep routes.

    By default ``**`` is only accepted in trailing position, matching every
    existing caller's (narrower-than-generic) selector vocabulary.

    When ``allow_interior_double_star`` is set, this instead mirrors the
    broker's generic ``compile_registration_pattern`` / ``PatternDepth::
    CanMatch`` grammar (``src/runtime/matcher.rs``): ``**`` may occupy any
    segment, not only the trailing one, and matches zero or more segments, so
    a pattern is valid when its count of non-``**`` ("fixed") segments does
    not exceed ``segment_count``, and equals ``segment_count`` exactly
    whenever no ``**`` is present at all. Adjacent ``**`` segments are never
    valid.
    """
    parts = _parts(route, scheme, allow_interior_double_star=allow_interior_double_star)
    if parts is None:
        return False
    if allow_interior_double_star:
        if any(a == "**" and b == "**" for a, b in itertools.pairwise(parts)):
            return False
        fixed = sum(1 for part in parts if part != "**")
        flexible = "**" in parts
        if fixed > segment_count or (not flexible and fixed != segment_count):
            return False
        return not (parts[0] in {"*", "**"} and not allow_realm_wildcard)
    if parts == ["**"]:
        return allow_realm_wildcard
    if len(parts) == 2 and parts[-1] == "**":
        return parts[0] != "*" or allow_realm_wildcard
    if len(parts) != segment_count or "**" in parts:
        return False
    return not (parts[0] == "*" and not allow_realm_wildcard)
