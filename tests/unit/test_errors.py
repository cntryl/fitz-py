from __future__ import annotations

from fitz_py import (
    ErrKvKeyNotFound,
    ErrLeaseHeld,
    ErrRpcTimeout,
    TimeoutError,
    TransportError,
    is_retryable,
)
from fitz_py.errors import kv_error, lease_error, rpc_error


def test_named_domain_errors_are_mapped() -> None:
    assert isinstance(kv_error("missing", 4), ErrKvKeyNotFound)
    assert isinstance(lease_error("held", 1), ErrLeaseHeld)
    assert isinstance(rpc_error("timeout", 1), ErrRpcTimeout)


def test_is_retryable_matches_domain_and_transport_rules() -> None:
    assert is_retryable(TimeoutError("timeout")) is True
    assert is_retryable(TransportError("transport")) is True
    assert is_retryable(kv_error("missing", 4)) is True
    assert is_retryable(kv_error("conflict", 3)) is False
