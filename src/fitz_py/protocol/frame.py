from __future__ import annotations

from dataclasses import dataclass

from fitz_py.errors import CodecError
from fitz_py.protocol.buffer import BufferReader, BufferWriter


@dataclass(slots=True)
class Frame:
    message_type: int
    payload: bytes


class FrameCodec:
    @staticmethod
    def encode_message_type(message_type: int, writer: BufferWriter) -> None:
        if not 0 <= message_type <= 0xFFFF:
            raise CodecError(f"Invalid message type: {message_type}")
        if message_type <= 0xFE:
            writer.write_u8(message_type)
            return
        writer.write_u8(0xFF)
        writer.write_u16_be(message_type)

    @staticmethod
    def decode_message_type(reader: BufferReader) -> int:
        first = reader.read_u8()
        if first == 0xFF:
            return reader.read_u16_be()
        return first

    @classmethod
    def encode_frame(cls, message_type: int, payload: bytes) -> bytes:
        if len(payload) > 0xFFFF:
            raise CodecError("TLV payload exceeds 65535 bytes")
        writer = BufferWriter()
        cls.encode_message_type(message_type, writer)
        writer.write_u16_be(len(payload))
        writer.write_bytes(payload)
        return writer.build()

    @classmethod
    def decode_frame(cls, data: bytes) -> Frame:
        reader = BufferReader(data)
        message_type = cls.decode_message_type(reader)
        payload_length = reader.read_u16_be()
        if reader.remaining_bytes() < payload_length:
            raise CodecError(
                f"Frame incomplete: expected {payload_length} bytes, got {reader.remaining_bytes()}"
            )
        payload = reader.read_bytes(payload_length)
        if not reader.is_eof():
            raise CodecError("Frame contains trailing bytes")
        return Frame(message_type=message_type, payload=payload)


class FrameParser:
    def __init__(self) -> None:
        self._buffer = bytearray()
        self._offset = 0
        self._message_type: int | None = None
        self._payload_length: int | None = None
        self._state = "reading_type"

    def parse_frames(self, data: bytes) -> list[Frame]:
        if data:
            self._buffer.extend(data)

        frames: list[Frame] = []
        while True:
            if self._state == "reading_type":
                if not self._try_read_message_type():
                    break
                self._state = "reading_length"

            if self._state == "reading_length":
                if not self._try_read_length():
                    break
                self._state = "reading_payload"

            if self._state == "reading_payload":
                if not self._try_read_payload(frames):
                    break
                self._state = "reading_type"

        self._compact()
        return frames

    def _try_read_message_type(self) -> bool:
        if self._offset >= len(self._buffer):
            return False
        first = self._buffer[self._offset]
        if first == 0xFF:
            if self._offset + 3 > len(self._buffer):
                return False
            self._message_type = int.from_bytes(
                self._buffer[self._offset + 1 : self._offset + 3], "big"
            )
            self._offset += 3
            return True
        self._message_type = first
        self._offset += 1
        return True

    def _try_read_length(self) -> bool:
        if self._offset + 2 > len(self._buffer):
            return False
        self._payload_length = int.from_bytes(self._buffer[self._offset : self._offset + 2], "big")
        self._offset += 2
        return True

    def _try_read_payload(self, frames: list[Frame]) -> bool:
        if self._message_type is None or self._payload_length is None:
            return False
        end = self._offset + self._payload_length
        if end > len(self._buffer):
            return False
        frames.append(
            Frame(
                message_type=self._message_type,
                payload=bytes(self._buffer[self._offset : end]),
            )
        )
        self._offset = end
        self._message_type = None
        self._payload_length = None
        return True

    def _compact(self) -> None:
        if self._offset == 0:
            return
        del self._buffer[: self._offset]
        self._offset = 0
