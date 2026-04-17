from __future__ import annotations


def is_exact_route_shape(route: str, scheme: str, segment_count: int) -> bool:
    prefix = f"{scheme}://"
    if not route.startswith(prefix):
        return False

    remainder = route[len(prefix) :]
    if not remainder:
        return False

    segments = remainder.split("/")
    if len(segments) != segment_count:
        return False

    return all(segment and segment != "*" and segment != "**" for segment in segments)


def is_concrete_route_shape(route: str, scheme: str) -> bool:
    prefix = f"{scheme}://"
    if not route.startswith(prefix):
        return False

    remainder = route[len(prefix) :]
    if not remainder:
        return False

    return all(segment and segment != "*" and segment != "**" for segment in remainder.split("/"))


def is_selector_route_shape(
    route: str,
    scheme: str,
    segment_count: int,
    allow_realm_wildcard: bool = False,
) -> bool:
    prefix = f"{scheme}://"
    if not route.startswith(prefix):
        return False

    remainder = route[len(prefix) :]
    if not remainder:
        return False

    segments = remainder.split("/")
    if len(segments) == segment_count:
        if all(segment and segment not in ("*", "**") for segment in segments):
            return True

        if segments[-1] == "*" and all(segment and segment not in ("*", "**") for segment in segments[:-1]):
            return True

    if allow_realm_wildcard and len(segments) == 2:
        return segments[1] == "**" and segments[0] not in ("", "*", "**")

    return False
