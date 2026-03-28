from __future__ import annotations

import pytest

from fitz_py.domains.kv import KvTransaction
from fitz_py.errors import ErrKvOperationNotAllowed


class _FakeConnection:
    def __init__(self) -> None:
        self.requests: list[tuple[int, bytes]] = []

    async def request(self, message_type: int, payload: bytes) -> bytes:
        self.requests.append((message_type, payload))
        return b"\x00"


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
