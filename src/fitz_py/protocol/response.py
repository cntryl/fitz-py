"""Strict parsing for Fitz response envelopes."""

from __future__ import annotations

from dataclasses import dataclass

from fitz_py.errors import ProtocolError
from fitz_py.protocol.buffer import BufferReader


@dataclass(frozen=True, slots=True)
class Response:
    data: bytes = b""
    error_code: int | None = None
    error_class: int | None = None
    effect: int | None = None
    error: str | None = None

    @property
    def success(self) -> bool:
        return self.error is None


def parse_response(payload: bytes, *, plain: bool = False, stream: bool = False) -> Response:
    # These flags remain accepted for source compatibility; Wire 2 uses one envelope.
    del plain, stream
    if not payload:
        raise ProtocolError("Response payload is empty")
    reader = BufferReader(payload)
    status = reader.read_u8()
    if status == 0:
        return Response(data=reader.remaining())
    if status != 1:
        raise ProtocolError(f"Unknown response status: {status}", status)
    code = reader.read_u16_be()
    error_class = reader.read_u8()
    effect = reader.read_u8()
    if error_class > 10 or effect > 2:
        raise ProtocolError("Error response has an invalid class or effect", code)
    message = reader.read_string()
    if not reader.is_eof():
        raise ProtocolError("Error response has trailing data", code)
    return Response(error_code=code, error_class=error_class, effect=effect, error=message)


def parse_stream_response(payload: bytes, *, plain: bool = False) -> Response:
    """Decode the common wire-2 response envelope used by Stream."""
    del plain
    return parse_response(payload)


def parse_plain_or_coded_response(payload: bytes) -> Response:
    """Compatibility name for the single wire-2 response envelope."""
    return parse_response(payload)
