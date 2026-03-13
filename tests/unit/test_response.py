import pytest

from fitz_py.errors import ProtocolError
from fitz_py.protocol.buffer import BufferWriter
from fitz_py.protocol.response import assert_success, parse_standard_response


def test_parse_standard_response_success() -> None:
    writer = BufferWriter()
    writer.write_u8(0)
    writer.write_string("ok")
    parsed = parse_standard_response(writer.build())
    assert parsed.success is True
    assert parsed.data


def test_assert_success_raises_on_error() -> None:
    writer = BufferWriter()
    writer.write_u8(1)
    writer.write_string("bad")
    with pytest.raises(ProtocolError):
        assert_success(writer.build(), "TEST")
