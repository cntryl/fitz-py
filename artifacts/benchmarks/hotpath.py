from __future__ import annotations

import argparse
import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from time import perf_counter_ns

from fitz_py.domains.kv import KvClient
from fitz_py.domains.lease import LeaseClient
from fitz_py.domains.notice import NoticeClient
from fitz_py.domains.queue import QueueClient
from fitz_py.domains.rpc import RpcClient
from fitz_py.domains.stream import StreamClient, StreamFilterClause, StreamFilterSet
from fitz_py.multiplexer import Multiplexer
from fitz_py.protocol.frame import FrameCodec, FrameParser
from fitz_py.protocol.messages import (
    MSG_KV_BEGIN,
    MSG_LEASE_ACQUIRE,
    MSG_QUEUE_ENQUEUE,
    MSG_RPC_SUBSCRIBE_WORKER,
    MSG_STREAM_BEGIN,
    MSG_STREAM_READ,
)


@dataclass(slots=True)
class BenchmarkResult:
    name: str
    ns_per_op: float


class FakeConnection:
    def __init__(self, responses: dict[int, bytes] | None = None) -> None:
        self.requests: list[tuple[int, bytes]] = []
        self.notification_handlers: dict[int, Callable[[bytes], None]] = {}
        self.responses = responses or {}
        self._multiplexer = Multiplexer()

    async def request(self, message_type: int, payload: bytes) -> bytes:
        self.requests.append((message_type, payload))
        return self.responses.get(message_type, b"\x00")

    async def send_fire_and_forget(self, message_type: int, payload: bytes) -> None:
        self.requests.append((message_type, payload))
        self._multiplexer.dispatch(message_type, b"")

    def register_notification_handler(
        self, message_type: int, handler: Callable[[bytes], None]
    ) -> None:
        self.notification_handlers[message_type] = handler
        self._multiplexer.register_notification_handler(message_type, handler)

    def unregister_notification_handler(self, message_type: int) -> None:
        self.notification_handlers.pop(message_type, None)
        self._multiplexer.unregister_notification_handler(message_type)

    def on_reconnect(self, _handler: Callable[[], Awaitable[None]]) -> Callable[[], None]:
        return lambda: None

    def on_disconnect(self, _handler: Callable[[], None]) -> Callable[[], None]:
        return lambda: None

    def get_multiplexer(self) -> Multiplexer:
        return self._multiplexer


def _build_u64(value: int) -> bytes:
    return value.to_bytes(8, "big")


def _build_stream_read_page_response() -> bytes:
    inner = bytearray()
    inner.extend((3).to_bytes(4, "big"))
    inner.extend(b"\x00")
    inner.extend((41).to_bytes(8, "big"))
    inner.extend(b"\x01")
    inner.extend((51).to_bytes(8, "big"))
    inner.extend(b"\x00")
    inner.extend((5).to_bytes(4, "big"))
    inner.extend(b"alpha")
    inner.extend(b"\x00")
    inner.extend((111).to_bytes(8, "big"))
    inner.extend(b"\x01")
    inner.extend((42).to_bytes(8, "big"))
    inner.extend(b"\x01")
    inner.extend(b"\x02")
    inner.extend((43).to_bytes(8, "big"))
    inner.extend((45).to_bytes(8, "big"))
    inner.extend(b"\x02")
    inner.extend((45).to_bytes(8, "big"))
    inner.extend(b"\x01")
    inner.extend((52).to_bytes(8, "big"))
    inner.extend(b"\x00")
    inner.extend(b"\x01")

    response = bytearray(b"\x00\x00")
    response.extend(len(inner).to_bytes(4, "big"))
    response.extend(inner)
    return bytes(response)


def _benchmark_sync(
    name: str,
    iterations: int,
    operation: Callable[[], None],
) -> BenchmarkResult:
    start = perf_counter_ns()
    for _ in range(iterations):
        operation()
    elapsed = perf_counter_ns() - start
    return BenchmarkResult(name=name, ns_per_op=elapsed / iterations)


async def _benchmark_async(
    name: str,
    iterations: int,
    operation: Callable[[], Awaitable[None]],
) -> BenchmarkResult:
    start = perf_counter_ns()
    for _ in range(iterations):
        await operation()
    elapsed = perf_counter_ns() - start
    return BenchmarkResult(name=name, ns_per_op=elapsed / iterations)


def _build_frame_benchmarks(iterations: int) -> list[BenchmarkResult]:
    results: list[BenchmarkResult] = []

    small_payload = b"x" * 128
    large_payload = b"x" * 10_240
    encoded_small_frame = FrameCodec.encode_frame(101, small_payload)
    encoded_large_frame = FrameCodec.encode_frame(101, large_payload)

    results.append(
        _benchmark_sync(
            "frame encode (small payload)",
            iterations,
            lambda: FrameCodec.encode_frame(101, small_payload),
        )
    )
    results.append(
        _benchmark_sync(
            "frame encode (large payload)",
            iterations,
            lambda: FrameCodec.encode_frame(101, large_payload),
        )
    )
    results.append(
        _benchmark_sync(
            "frame decode (small payload)",
            iterations,
            lambda: FrameCodec.decode_frame(encoded_small_frame),
        )
    )
    results.append(
        _benchmark_sync(
            "frame decode (large payload)",
            iterations,
            lambda: FrameCodec.decode_frame(encoded_large_frame),
        )
    )

    parser = FrameParser()
    fragment_a = FrameCodec.encode_frame(302, b"response-a")
    fragment_b = FrameCodec.encode_frame(303, b"response-b")
    fragmented_stream = fragment_a + fragment_b
    chunks = [fragmented_stream[index : index + 3] for index in range(0, len(fragmented_stream), 3)]
    frame_count = 0

    def parse_fragmented_stream() -> None:
        nonlocal frame_count
        for chunk in chunks:
            frame_count += len(parser.parse_frames(chunk))

    results.append(
        _benchmark_sync("frame parser fragmented stream", iterations, parse_fragmented_stream)
    )
    if frame_count != iterations * 2:
        raise RuntimeError(f"unexpected parsed frame count: {frame_count}")

    multiplexer = Multiplexer()
    delivered = 0
    mux_payload = b"payload"

    def handle_notification(_payload: bytes) -> None:
        nonlocal delivered
        delivered += 1

    multiplexer.register_notification_handler(401, handle_notification)
    results.append(
        _benchmark_sync(
            "multiplexer notification dispatch",
            iterations,
            lambda: multiplexer.dispatch(401, mux_payload),
        )
    )
    if delivered != iterations:
        raise RuntimeError(f"unexpected notification count: {delivered}")

    async def request_round_trip() -> None:
        async def send(_frame_data: bytes) -> None:
            multiplexer.dispatch(302, b"ok")

        await multiplexer.request(302, mux_payload, send, timeout_ms=1000)

    async def async_benchmarks() -> list[BenchmarkResult]:
        request_result = await _benchmark_async(
            "multiplexer request round trip",
            iterations,
            request_round_trip,
        )

        notice_connection = FakeConnection()
        notice_client = NoticeClient(notice_connection)
        notice_route = "notice://bench/area/events"
        notice_body = b"benchmark-payload"
        notice_result = await _benchmark_async(
            "notice publish",
            iterations,
            lambda: notice_client.publish(notice_route, notice_body),
        )

        kv_connection = FakeConnection({MSG_KV_BEGIN: b"\x00" + _build_u64(42)})
        kv_client = KvClient(kv_connection)
        kv_result = await _benchmark_async(
            "kv begin",
            iterations,
            lambda: kv_client.begin("kv://bench/area/users"),
        )

        lease_connection = FakeConnection({MSG_LEASE_ACQUIRE: b"\x00\x01" + _build_u64(43)})
        lease_client = LeaseClient(lease_connection)
        lease_result = await _benchmark_async(
            "lease acquire",
            iterations,
            lambda: lease_client.acquire("lease://bench/area/leader", 30),
        )

        queue_connection = FakeConnection({MSG_QUEUE_ENQUEUE: b"\x00" + _build_u64(44)})
        queue_client = QueueClient(queue_connection)
        queue_result = await _benchmark_async(
            "queue enqueue",
            iterations,
            lambda: queue_client.enqueue("queue://bench/area/messages", b"benchmark-payload"),
        )

        rpc_connection = FakeConnection({MSG_RPC_SUBSCRIBE_WORKER: b"\x00"})
        rpc_client = RpcClient(rpc_connection)

        def rpc_handler(_request, _writer) -> None:
            return None

        rpc_result = await _benchmark_async(
            "rpc register_worker",
            iterations,
            lambda: rpc_client.register_worker("rpc://bench/**", rpc_handler),
        )

        stream_connection = FakeConnection({MSG_STREAM_BEGIN: b"\x00\x01" + _build_u64(45)})
        stream_client = StreamClient(stream_connection)
        stream_begin_result = await _benchmark_async(
            "stream begin",
            iterations,
            lambda: stream_client.begin("stream://bench/area/events"),
        )

        stream_read_connection = FakeConnection(
            {MSG_STREAM_READ: _build_stream_read_page_response()}
        )
        stream_read_client = StreamClient(stream_read_connection)
        stream_filter = StreamFilterSet(
            clauses=[StreamFilterClause(kind="Equals", value="proj.alpha")]
        )
        stream_read_result = await _benchmark_async(
            "stream read_page",
            iterations,
            lambda: stream_read_client.read_page(
                "stream://bench/area/events",
                0,
                10,
                stream_filter,
                max_bytes=1024,
            ),
        )

        return [
            request_result,
            notice_result,
            kv_result,
            lease_result,
            queue_result,
            rpc_result,
            stream_begin_result,
            stream_read_result,
        ]

    results.extend(asyncio.run(async_benchmarks()))
    return results


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run fitz-py hot-path benchmarks")
    parser.add_argument("--iterations", type=int, default=10_000)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.iterations <= 0:
        raise SystemExit("--iterations must be greater than zero")

    results = _build_frame_benchmarks(args.iterations)

    print(f"fitz-py hotpath benchmarks (iterations={args.iterations})")
    for result in results:
        print(f"{result.name:<34} {result.ns_per_op:>12.2f} ns/op")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
