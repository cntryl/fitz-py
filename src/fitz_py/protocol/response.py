"""Strict parsing for Fitz response envelopes."""

from __future__ import annotations

from dataclasses import dataclass

from fitz_py.errors import ProtocolError
from fitz_py.protocol.buffer import BufferReader


@dataclass(frozen=True, slots=True)
class Response:
    data: bytes = b""
    error_code: int | None = None
    error: str | None = None

    @property
    def success(self) -> bool:
        return self.error is None


def parse_response(payload: bytes, *, plain: bool = False, stream: bool = False) -> Response:
    if not payload:
        raise ProtocolError("Response payload is empty")
    reader = BufferReader(payload)
    status = reader.read_u8()
    if status == 0:
        return Response(data=reader.remaining())
    if status != 1 and not (stream and status == 2):
        raise ProtocolError(f"Unknown response status: {status}", status)
    code = None if plain and status == 1 else reader.read_u32_be()
    message = reader.read_string()
    if not reader.is_eof():
        raise ProtocolError("Error response has trailing data", code)
    return Response(error_code=code, error=message)


def parse_stream_response(payload: bytes, *, plain: bool = False) -> Response:
    """Decode status-2 Stream errors or legacy operation-specific status-1 errors."""
    return parse_response(payload, plain=plain, stream=True)


def parse_plain_or_coded_response(payload: bytes) -> Response:
    """Parse legacy plain domain errors and coded ingress errors without guessing."""
    if not payload or payload[0] != 1:
        return parse_response(payload, plain=True)
    plain = len(payload) >= 5 and int.from_bytes(payload[1:5], "big") == len(payload) - 5
    coded = len(payload) >= 9 and int.from_bytes(payload[5:9], "big") == len(payload) - 9
    if plain == coded:
        raise ProtocolError("Malformed or ambiguous error response")
    return parse_response(payload, plain=plain)
