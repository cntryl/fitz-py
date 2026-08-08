"""Stable Python-native microbenchmarks for codec hot paths."""

from __future__ import annotations

import pyperf

from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.frame import FrameCodec, FrameParser

PAYLOAD = b"x" * 256
FRAME = FrameCodec.encode_frame(104, PAYLOAD)


def encode_buffer() -> bytes:
    writer = BufferWriter()
    writer.write_route("kv://benchmark/area/resource")
    writer.write_u64_be(42)
    writer.write_u32_be(len(PAYLOAD))
    writer.write_bytes(PAYLOAD)
    return writer.build()


ENCODED = encode_buffer()


def decode_buffer() -> int:
    reader = BufferReader(ENCODED)
    reader.read_route()
    reader.read_u64_be()
    return len(reader.read_bytes(reader.read_u32_be()))


def parse_frame() -> int:
    return len(FrameParser().parse_frames(FRAME)[0].payload)


def main() -> None:
    runner = pyperf.Runner()
    runner.bench_func("buffer_encode_256b", encode_buffer)
    runner.bench_func("buffer_decode_256b", decode_buffer)
    runner.bench_func("frame_parse_256b", parse_frame)


if __name__ == "__main__":
    main()
