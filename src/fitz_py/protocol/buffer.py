from __future__ import annotations

import struct

from fitz_py.errors import CodecError


class BufferWriter:
    def __init__(self, capacity: int = 128) -> None:
        self._buffer = bytearray(capacity)
        self._offset = 0

    def _reserve(self, size: int) -> int:
        start = self._offset
        end = start + size
        if end > len(self._buffer):
            self._buffer.extend(b"\0" * max(end - len(self._buffer), len(self._buffer)))
        self._offset = end
        return start

    def write_u8(self, value: int) -> None:
        struct.pack_into(">B", self._buffer, self._reserve(1), value)

    def write_u16_be(self, value: int) -> None:
        struct.pack_into(">H", self._buffer, self._reserve(2), value)

    def write_u32_be(self, value: int) -> None:
        struct.pack_into(">I", self._buffer, self._reserve(4), value)

    def write_u64_be(self, value: int) -> None:
        struct.pack_into(">Q", self._buffer, self._reserve(8), value)

    def write_bytes(self, value: bytes | bytearray | memoryview) -> None:
        view = memoryview(value)
        start = self._reserve(len(view))
        self._buffer[start : start + len(view)] = view

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
        return bytes(memoryview(self._buffer)[: self._offset])


class BufferReader:
    def __init__(self, data: bytes | bytearray | memoryview) -> None:
        self._buffer = memoryview(data)
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
        flag = self.read_u8()
        if flag == 0:
            return None
        if flag != 1:
            raise CodecError(f"Invalid optional u64 flag: {flag}")
        return self.read_u64_be()

    def remaining(self) -> bytes:
        return self._buffer[self._offset :].tobytes()

    def remaining_bytes(self) -> int:
        return len(self._buffer) - self._offset

    def is_eof(self) -> bool:
        return self._offset >= len(self._buffer)
