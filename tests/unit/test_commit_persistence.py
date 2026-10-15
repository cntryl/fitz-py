import pytest

from fitz_py import KVDurability, KVMode, StreamCommitMode
from fitz_py.domains.kv import KVClient, KVTransaction
from fitz_py.domains.stream import StreamSession
from fitz_py.protocol.buffer import BufferReader
from fitz_py.protocol.messages import MSG_KV_BEGIN, MSG_KV_COMMIT, MSG_STREAM_COMMIT
from tests.unit.test_contracts import FakeConnection


async def test_begin_carries_only_scope_and_mode():
    connection = FakeConnection({MSG_KV_BEGIN: b"\x00" + (7).to_bytes(8, "big")})
    await KVClient(connection).begin("kv://r/a/x", mode=KVMode.READ_WRITE)
    reader = BufferReader(connection.sent[0][1])
    assert reader.read_string() == "kv://r/a/x"
    assert reader.read_u8() == 1
    assert reader.is_eof()


@pytest.mark.parametrize("mode,wire", [(KVDurability.BUFFERED, 0), (KVDurability.SYNC, 1)])
async def test_kv_commit_encodes_explicit_persistence(mode, wire):
    connection = FakeConnection({MSG_KV_COMMIT: b"\x00"})
    await KVTransaction(connection, "kv://r/a/x", 7).commit(mode)
    reader = BufferReader(connection.sent[0][1])
    assert reader.read_u64_be() == 7
    assert reader.read_string() == "kv://r/a/x"
    assert reader.read_u8() == wire
    assert reader.is_eof()


async def test_stream_commit_requires_explicit_persistence():
    connection = FakeConnection({MSG_STREAM_COMMIT: b"\x00\x00\x00\x00\x00"})
    with pytest.raises(TypeError):
        await StreamSession(connection, 7).commit()
    assert connection.sent == []


@pytest.mark.parametrize("mode,wire", [(StreamCommitMode.BUFFERED, 0), (StreamCommitMode.SYNC, 1)])
async def test_stream_commit_encodes_explicit_persistence(mode, wire):
    connection = FakeConnection({MSG_STREAM_COMMIT: b"\x00\x00\x00\x00\x00"})
    await StreamSession(connection, 7).commit(mode)
    assert connection.sent == [(MSG_STREAM_COMMIT, (7).to_bytes(8, "big") + bytes([wire]))]


async def test_kv_commit_requires_explicit_persistence():
    connection = FakeConnection({MSG_KV_COMMIT: b"\x00"})
    with pytest.raises(TypeError):
        await KVTransaction(connection, "kv://r/a/x", 7).commit()
    assert connection.sent == []


async def test_invalid_persistence_does_not_finalize_transaction():
    connection = FakeConnection({MSG_KV_COMMIT: b"\x00"})
    transaction = KVTransaction(connection, "kv://r/a/x", 7)
    with pytest.raises(ValueError):
        await transaction.commit("invalid")
    assert connection.sent == []
    await transaction.commit(KVDurability.SYNC)
    assert len(connection.sent) == 1
