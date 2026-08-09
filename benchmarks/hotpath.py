"""Stable Python-native benchmarks for codecs and client runtime hot paths."""

from __future__ import annotations

import pyperf

from fitz_py._runtime import AsyncSubscription, RequestGate
from fitz_py.multiplexer import Multiplexer
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


def parse_fragmented_frame() -> int:
    parser = FrameParser()
    parsed = []
    for byte in FRAME:
        parsed.extend(parser.parse_frames(bytes((byte,))))
    return len(parsed[0].payload)


async def multiplexer_loopback() -> bytes:
    multiplexer = Multiplexer()
    multiplexer.set_connected()

    async def send(_frame: bytes) -> None:
        multiplexer.dispatch(104, PAYLOAD)

    return await multiplexer.request(104, FRAME, send, 1)


async def admission_roundtrip() -> int:
    gate = RequestGate(16, 64)
    release = await gate.acquire()
    active = gate.active
    release()
    return active


async def subscription_fanout_100() -> int:
    async def close() -> None: ...

    consumers = [AsyncSubscription[bytes]("benchmark", 1, close) for _ in range(100)]
    return sum(consumer.push(PAYLOAD) for consumer in consumers)


def main() -> None:
    runner = pyperf.Runner()
    runner.bench_func("buffer_encode_256b", encode_buffer)
    runner.bench_func("buffer_decode_256b", decode_buffer)
    runner.bench_func("frame_parse_256b", parse_frame)
    runner.bench_func("frame_parse_fragmented_256b", parse_fragmented_frame)
    runner.bench_async_func("multiplexer_loopback_256b", multiplexer_loopback)
    runner.bench_async_func("admission_roundtrip", admission_roundtrip)
    runner.bench_async_func("subscription_fanout_100", subscription_fanout_100)


if __name__ == "__main__":
    main()
