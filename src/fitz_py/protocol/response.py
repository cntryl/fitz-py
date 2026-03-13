from __future__ import annotations

from dataclasses import dataclass

from fitz_py.errors import ProtocolError
from fitz_py.protocol.buffer import BufferReader


@dataclass(slots=True)
class ParsedResponse:
    success: bool
    data: bytes
    error: str | None = None


def parse_standard_response(payload: bytes) -> ParsedResponse:
    if not payload:
        raise ProtocolError("Response payload is empty")
    reader = BufferReader(payload)
    status = reader.read_u8()
    if status == 0:
        return ParsedResponse(success=True, data=reader.remaining())
    if status == 1:
        if reader.is_eof():
            return ParsedResponse(success=False, data=b"", error="Unknown error (no message)")
        return ParsedResponse(success=False, data=b"", error=reader.read_string())
    raise ProtocolError(f"Unknown response status: {status}")


def assert_success(payload: bytes, operation: str) -> bytes:
    result = parse_standard_response(payload)
    if result.success:
        return result.data
    raise ProtocolError(f"{operation} failed: {result.error or 'Unknown error'}")
