from __future__ import annotations

from collections.abc import Callable

import pytest

from fitz_py.domains.kv import KvTransaction
from fitz_py.errors import ErrKvOperationNotAllowed


class _FakeConnection:
    def __init__(self) -> None:
        self.requests: list[tuple[int, bytes]] = []
        self.disconnect_handlers: list[Callable[[], None]] = []

    async def request(self, message_type: int, payload: bytes) -> bytes:
        self.requests.append((message_type, payload))
        return b"\x00"

    def on_disconnect(self, handler: Callable[[], None]) -> None:
        self.disconnect_handlers.append(handler)

    def emit_disconnect(self) -> None:
        for handler in list(self.disconnect_handlers):
            handler()


@pytest.mark.asyncio
async def test_kv_transaction_rejects_mutation_after_commit() -> None:
    conn = _FakeConnection()
    tx = KvTransaction(conn, "kv://tests/app/resource", 42)

    await tx.commit()

    with pytest.raises(ErrKvOperationNotAllowed, match="already committed"):
        await tx.put(b"k", b"v")


@pytest.mark.asyncio
async def test_kv_transaction_rejects_commit_after_rollback() -> None:
    conn = _FakeConnection()
    tx = KvTransaction(conn, "kv://tests/app/resource", 42)

    await tx.rollback()

    with pytest.raises(ErrKvOperationNotAllowed, match="already rolled back"):
        await tx.commit()


@pytest.mark.asyncio
async def test_kv_transaction_invalidates_on_disconnect() -> None:
    conn = _FakeConnection()
    tx = KvTransaction(conn, "kv://tests/app/resource", 42)

    conn.emit_disconnect()

    with pytest.raises(ErrKvOperationNotAllowed, match="already disconnected"):
        await tx.put(b"k", b"v")
