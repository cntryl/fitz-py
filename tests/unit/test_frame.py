from fitz_py.protocol.frame import FrameCodec, FrameParser


def test_frame_codec_round_trip() -> None:
    payload = b"abc123"
    encoded = FrameCodec.encode_frame(302, payload)
    decoded = FrameCodec.decode_frame(encoded)
    assert decoded.message_type == 302
    assert decoded.payload == payload


def test_frame_parser_handles_partial_input() -> None:
    encoded = FrameCodec.encode_frame(100, b"payload")
    parser = FrameParser()

    assert parser.parse_frames(encoded[:2]) == []
    frames = parser.parse_frames(encoded[2:])

    assert len(frames) == 1
    assert frames[0].message_type == 100
    assert frames[0].payload == b"payload"
