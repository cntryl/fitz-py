"""Shared base class for domain-specific Fitz clients."""

from __future__ import annotations

from fitz_py.connection import Connection


class DomainClient:
    """Provides request helpers for concrete domain client implementations."""

    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    async def request_frame(self, message_type: int, payload: bytes) -> bytes:
        return await self.connection.request(message_type, payload)
