"""Strict, opaque route-shape validation."""

from __future__ import annotations


def _parts(route: str, scheme: str) -> list[str] | None:
    prefix = f"{scheme}://"
    if not isinstance(route, str) or not route.startswith(prefix):  # pyright: ignore[reportUnnecessaryIsInstance]
        return None
    parts = route[len(prefix) :].split("/")
    if not parts or any(not part for part in parts):
        return None
    if any("*" in part and part not in {"*", "**"} for part in parts):
        return None
    if "**" in parts and parts[-1] != "**":
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
) -> bool:
    parts = _parts(route, scheme)
    if parts is None:
        return False
    if parts == ["**"]:
        return allow_realm_wildcard
    if len(parts) == 2 and parts[-1] == "**":
        return parts[0] != "*" or allow_realm_wildcard
    if len(parts) != segment_count or "**" in parts:
        return False
    return not (parts[0] == "*" and not allow_realm_wildcard)
