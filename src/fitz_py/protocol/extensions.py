"""Wire 3 length-delimited extensions and numeric capability registry."""

from __future__ import annotations

import struct

from fitz_py.errors import CodecError

EXTENSIONS_PRESENT = 4
QUEUE_MESSAGE_TTL = 1
QUEUE_NACK = 2
CLIENT_CAPABILITY_IDS = (QUEUE_MESSAGE_TTL, QUEUE_NACK)


def encode_capabilities(ids: tuple[int, ...]) -> bytes:
    if len(ids) > 65535 or any(
        value <= 0 or value > 0xFFFFFFFF or (index and value <= ids[index - 1])
        for index, value in enumerate(ids)
    ):
        raise CodecError("Capability IDs must be nonzero, ascending u32 values")
    return struct.pack(">H", len(ids)) + b"".join(struct.pack(">I", value) for value in ids)


def encode_payload(core: bytes, fields: tuple[tuple[int, bytes], ...]) -> bytes:
    if not fields:
        return core
    previous = 0
    entries = bytearray()
    for field_id, value in fields:
        if not previous < field_id <= 65535:
            raise CodecError("Extension fields must have ascending nonzero u16 IDs")
        entries += struct.pack(">HI", field_id, len(value))
        entries += value
        previous = field_id
    return struct.pack(">I", len(core)) + core + entries


def decode_payload(payload: bytes, message_type: int, flags: int) -> tuple[bytes, tuple[int, ...]]:
    handshake = message_type == 2
    if not flags & EXTENSIONS_PRESENT:
        if handshake:
            raise CodecError("HELLO requires an explicit capability list")
        return payload, ()
    view = memoryview(payload)
    if len(view) < 4:
        raise CodecError("Extension core length is truncated")
    core_end = 4 + struct.unpack_from(">I", view)[0]
    if core_end > len(view):
        raise CodecError("Extension core exceeds payload bounds")
    cursor = core_end
    capabilities: tuple[int, ...] | None = None
    fields_seen = False
    while cursor < len(view):
        if len(view) - cursor < 6:
            raise CodecError("Extension header is truncated")
        field_id, size = struct.unpack_from(">HI", view, cursor)
        cursor += 6
        if field_id == 0 or size > len(view) - cursor:
            raise CodecError("Invalid extension ID or value length")
        if handshake and field_id == 1:
            if capabilities is not None or size < 2:
                raise CodecError("Duplicate or malformed capability list")
            count = struct.unpack_from(">H", view, cursor)[0]
            if size != 2 + 4 * count:
                raise CodecError("Capability list length mismatch")
            capabilities = tuple(
                struct.unpack_from(">I", view, cursor + 2 + 4 * index)[0] for index in range(count)
            )
            encode_capabilities(capabilities)
        cursor += size
        fields_seen = True
    if not fields_seen or (handshake and capabilities is None):
        raise CodecError("Required extension fields are missing")
    return bytes(view[4:core_end]), capabilities or ()
