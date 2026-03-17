from __future__ import annotations

import struct

from fitz_py.errors import CodecError


class BufferWriter:
    def __init__(self) -> None:
        self._parts: list[bytes] = []

    def write_u8(self, value: int) -> None:
        self._parts.append(struct.pack(">B", value))

    def write_u16_be(self, value: int) -> None:
        self._parts.append(struct.pack(">H", value))

    def write_u32_be(self, value: int) -> None:
        self._parts.append(struct.pack(">I", value))

    def write_u64_be(self, value: int) -> None:
        self._parts.append(struct.pack(">Q", value))

    def write_bytes(self, value: bytes | bytearray | memoryview) -> None:
        self._parts.append(bytes(value))

    def write_string(self, value: str) -> None:
        encoded = value.encode("utf-8")
        self.write_u32_be(len(encoded))
        self.write_bytes(encoded)

    def write_route(self, value: str) -> None:
        self.write_string(value)

    def write_optional_u64(self, value: int | None) -> None:
        if value is None:
            self.write_u8(0)
            return
        self.write_u8(1)
        self.write_u64_be(value)

    def build(self) -> bytes:
        return b"".join(self._parts)


class BufferReader:
    def __init__(self, data: bytes | bytearray | memoryview) -> None:
        self._buffer = memoryview(bytes(data))
        self._offset = 0

    def _read(self, size: int) -> memoryview:
        if self._offset + size > len(self._buffer):
            raise CodecError(f"Buffer overflow: cannot read {size} bytes")
        start = self._offset
        self._offset += size
        return self._buffer[start : self._offset]

    def read_u8(self) -> int:
        return struct.unpack(">B", self._read(1))[0]

    def read_u16_be(self) -> int:
        return struct.unpack(">H", self._read(2))[0]

    def read_u32_be(self) -> int:
        return struct.unpack(">I", self._read(4))[0]

    def read_u64_be(self) -> int:
        return struct.unpack(">Q", self._read(8))[0]

    def read_bytes(self, length: int) -> bytes:
        return self._read(length).tobytes()

    def read_string(self) -> str:
        return self.read_bytes(self.read_u32_be()).decode("utf-8")

    def read_route(self) -> str:
        return self.read_string()

    def read_optional_u64(self) -> int | None:
        if self.read_u8() == 0:
            return None
        return self.read_u64_be()

    def remaining(self) -> bytes:
        return self._buffer[self._offset :].tobytes()

    def remaining_bytes(self) -> int:
        return len(self._buffer) - self._offset

    def is_eof(self) -> bool:
        return self._offset >= len(self._buffer)
