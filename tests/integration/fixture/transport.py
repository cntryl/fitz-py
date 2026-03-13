from __future__ import annotations

from collections.abc import Callable, Iterable

TransportType = str


def run_with_transports_only() -> Iterable[TransportType]:
    return ("tcp", "ws")


def run_with_both_transports(fn: Callable[[TransportType], None]) -> None:
    for transport in run_with_transports_only():
        fn(transport)
