from __future__ import annotations

from dataclasses import dataclass

from fitz_py.errors import CodecError

HEADER_LEN = 15
MAX_MESSAGE_SIZE = 1_048_576
ACK_REQUESTED = 1
RESPONSE_FINAL = 2
VALID_FLAGS = ACK_REQUESTED | RESPONSE_FINAL


@dataclass(slots=True)
class Frame:
    message_type: int
    flags: int
    correlation_id: int
    payload: bytes


class FrameCodec:
    @staticmethod
    def encode_frame(
        message_type: int,
        payload: bytes,
        *,
        flags: int = 0,
        correlation_id: int = 0,
    ) -> bytes:
        if not 0 <= message_type <= 0xFFFF:
            raise CodecError(f"Invalid message type: {message_type}")
        if flags & ~VALID_FLAGS:
            raise CodecError(f"Unsupported frame flags: {flags:#x}")
        if not 0 <= correlation_id <= 0xFFFFFFFFFFFFFFFF:
            raise CodecError("Correlation ID must fit in u64")
        if HEADER_LEN + len(payload) > MAX_MESSAGE_SIZE:
            raise CodecError(f"Message exceeds {MAX_MESSAGE_SIZE} bytes")
        return (
            message_type.to_bytes(2, "big")
            + bytes((flags,))
            + correlation_id.to_bytes(8, "big")
            + len(payload).to_bytes(4, "big")
            + payload
        )

    @staticmethod
    def decode_frame(data: bytes) -> Frame:
        if len(data) < HEADER_LEN:
            raise CodecError(f"Message header is truncated: {len(data)} bytes")
        message_type = int.from_bytes(data[:2], "big")
        flags = data[2]
        if flags & ~VALID_FLAGS:
            raise CodecError(f"Unsupported frame flags: {flags:#x}")
        correlation_id = int.from_bytes(data[3:11], "big")
        payload_length = int.from_bytes(data[11:15], "big")
        if len(data) != HEADER_LEN + payload_length:
            raise CodecError(
                f"Message length {payload_length} does not match frame size "
                f"{len(data) - HEADER_LEN}"
            )
        return Frame(message_type, flags, correlation_id, data[HEADER_LEN:])


class FrameParser:
    """Decode exactly one Fitz message from each transport frame."""

    def parse_frames(self, data: bytes) -> list[Frame]:
        if not data:
            return []
        return [FrameCodec.decode_frame(data)]
