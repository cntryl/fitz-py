"""Property regressions for framing and primitive codecs."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from fitz_py.errors import CodecError
from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.frame import FrameCodec, FrameParser


@given(
    st.lists(
        st.tuples(
            st.integers(min_value=0, max_value=0xFFFF),
            st.binary(max_size=512),
        ),
        max_size=20,
    ),
    st.lists(st.integers(min_value=1, max_value=37), max_size=100),
)
def test_parser_accepts_arbitrary_fragmentation(
    frames: list[tuple[int, bytes]], cuts: list[int]
) -> None:
    encoded = b"".join(FrameCodec.encode_frame(kind, payload) for kind, payload in frames)
    parser = FrameParser()
    decoded = []
    offset = 0
    for size in cuts:
        decoded.extend(parser.parse_frames(encoded[offset : offset + size]))
        offset += size
    decoded.extend(parser.parse_frames(encoded[offset:]))
    assert [(frame.message_type, frame.payload) for frame in decoded] == frames


@given(st.integers(min_value=0, max_value=0xFFFF), st.binary(max_size=4096))
def test_frame_round_trip(message_type: int, payload: bytes) -> None:
    decoded = FrameCodec.decode_frame(FrameCodec.encode_frame(message_type, payload))
    assert (decoded.message_type, decoded.payload) == (message_type, payload)


@given(st.sampled_from([2, 3, 0x7F, 0xFF]))
def test_optional_u64_rejects_malformed_flags(flag: int) -> None:
    with pytest.raises(CodecError, match="optional u64 flag"):
        BufferReader(bytes([flag])).read_optional_u64()


@given(st.integers(min_value=0, max_value=0xFFFFFFFFFFFFFFFF))
def test_u64_round_trip(value: int) -> None:
    writer = BufferWriter()
    writer.write_u64_be(value)
    assert BufferReader(writer.build()).read_u64_be() == value


def test_frame_rejects_oversized_payload() -> None:
    with pytest.raises(CodecError, match="65535"):
        FrameCodec.encode_frame(1, b"x" * 65_536)


@given(st.integers(min_value=0, max_value=0xFFFF), st.binary(max_size=256))
def test_frame_decoder_rejects_every_truncation(message_type: int, payload: bytes) -> None:
    encoded = FrameCodec.encode_frame(message_type, payload)
    for end in range(len(encoded)):
        with pytest.raises(CodecError):
            FrameCodec.decode_frame(encoded[:end])


@given(st.integers(min_value=0, max_value=0xFFFF), st.binary(max_size=256))
def test_frame_decoder_rejects_trailing_bytes(message_type: int, payload: bytes) -> None:
    encoded = FrameCodec.encode_frame(message_type, payload)
    with pytest.raises(CodecError, match="trailing"):
        FrameCodec.decode_frame(encoded + b"x")


@given(
    st.integers(min_value=-10_000, max_value=-1) | st.integers(min_value=0x10000, max_value=0x1FFFF)
)
def test_frame_encoder_rejects_out_of_range_message_types(message_type: int) -> None:
    with pytest.raises(CodecError, match="message type"):
        FrameCodec.encode_frame(message_type, b"")


def test_parser_retains_partial_extended_header_without_reprocessing() -> None:
    frame = FrameCodec.encode_frame(0xFFFF, b"payload")
    parser = FrameParser()
    for byte in frame[:-1]:
        assert parser.parse_frames(bytes((byte,))) == []
    decoded = parser.parse_frames(frame[-1:])
    assert [(item.message_type, item.payload) for item in decoded] == [(0xFFFF, b"payload")]
    assert parser.parse_frames(b"") == []
