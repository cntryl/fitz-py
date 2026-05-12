"""Lease domain client, lease handles, and lease change subscriptions."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from fitz_py.domains._routes import is_exact_route_shape
from fitz_py.domains.base import DomainClient
from fitz_py.errors import LeaseError, lease_error
from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.messages import (
    MSG_LEASE_ACQUIRE,
    MSG_LEASE_EXTEND,
    MSG_LEASE_NOTIFY,
    MSG_LEASE_QUERY,
    MSG_LEASE_RELEASE,
    MSG_LEASE_SUBSCRIBE,
    MSG_LEASE_UNSUBSCRIBE,
)
from fitz_py.protocol.response import assert_success

LeaseHandler = Callable[[str], None | Awaitable[None]]


@dataclass(slots=True)
class LeaseInfo:
    """Current lease ownership and TTL information for a route."""

    is_held: bool
    owner: str | None = None
    ttl_remaining_secs: int | None = None


@dataclass(slots=True)
class Lease:
    """Client-side lease handle with extend and release helpers."""

    route: str
    _token: int
    _client: "LeaseClient"

    async def extend(self, ttl_secs: int) -> None:
        new_token = await self._client.extend(self.route, self._token, ttl_secs)
        if new_token is not None:
            self._token = new_token

    async def release(self) -> None:
        await self._client.release(self.route, self._token)


class LeaseSubscription:
    """Handle for an active lease change subscription."""

    def __init__(
        self, sub_id: int, pattern: str, unsubscribe: Callable[[int], Awaitable[None]]
    ) -> None:
        self._sub_id = sub_id
        self.pattern = pattern
        self._unsubscribe = unsubscribe

    async def unsubscribe(self) -> None:
        await self._unsubscribe(self._sub_id)


class LeaseClient(DomainClient):
    """Lease domain operations for acquire, extend, release, query, and subscribe."""

    def __init__(self, connection) -> None:
        super().__init__(connection)
        self._subscriptions: dict[int, tuple[str, LeaseHandler]] = {}
        self._initialized = False
        self.connection.on_reconnect(self._restore_subscriptions)

    async def acquire(self, route: str, ttl_secs: int) -> Lease:
        _assert_lease_route(route)
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_route("")
        writer.write_u64_be(ttl_secs)
        reader = BufferReader(await self.request_frame(MSG_LEASE_ACQUIRE, writer.build()))
        status = reader.read_u8()
        if status != 0:
            raise lease_error(f"ACQUIRE failed with status {status}", status)
        if not reader.is_eof():
            reader.read_u8()
        token = reader.read_u64_be() if not reader.is_eof() else None
        if token is None:
            raise LeaseError("ACQUIRE response missing fencing token", "MISSING_TOKEN")
        return Lease(route=route, _token=token, _client=self)

    async def extend(self, route: str, token: int, ttl_secs: int) -> int | None:
        _assert_lease_route(route)
        data = await self._send_token_ttl(MSG_LEASE_EXTEND, route, token, ttl_secs, "EXTEND")
        if data and len(data) >= 8:
            return BufferReader(data).read_u64_be()
        return None

    async def release(self, route: str, token: int) -> None:
        _assert_lease_route(route)
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_route("")
        writer.write_u64_be(token)
        assert_success(await self.request_frame(MSG_LEASE_RELEASE, writer.build()), "RELEASE")

    async def query(self, route: str) -> LeaseInfo:
        _assert_lease_route(route)
        writer = BufferWriter()
        writer.write_route(route)
        reader = BufferReader(await self.request_frame(MSG_LEASE_QUERY, writer.build()))
        status = reader.read_u8()
        if status != 0:
            raise lease_error(f"QUERY failed with status {status}", status)
        has_holder = reader.read_u8()
        if has_holder == 0:
            if not reader.is_eof():
                reader.read_u32_be()
            return LeaseInfo(is_held=False)
        owner = reader.read_route()
        ttl_remaining_secs = reader.read_u64_be()
        if not reader.is_eof():
            reader.read_u32_be()
        return LeaseInfo(is_held=True, owner=owner, ttl_remaining_secs=ttl_remaining_secs)

    async def subscribe(self, pattern: str, handler: LeaseHandler) -> LeaseSubscription:
        _assert_lease_route(pattern)
        self._init_notify_handler()
        writer = BufferWriter()
        writer.write_route(pattern)
        reader = BufferReader(await self.request_frame(MSG_LEASE_SUBSCRIBE, writer.build()))
        status = reader.read_u8()
        if status != 0:
            raise lease_error(f"SUBSCRIBE failed with status {status}", status)
        sub_id = reader.read_u64_be() if not reader.is_eof() else None
        if sub_id is None:
            raise LeaseError("SUBSCRIBE response missing subscription id", "MISSING_SUB_ID")
        self._subscriptions[sub_id] = (pattern, handler)
        return LeaseSubscription(sub_id, pattern, self._unsubscribe)

    async def _send_token_ttl(
        self, message_type: int, route: str, token: int, ttl_secs: int, operation: str
    ) -> bytes:
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_route("")
        writer.write_u64_be(token)
        writer.write_u64_be(ttl_secs)
        payload = await self.request_frame(message_type, writer.build())
        try:
            return assert_success(payload, operation)
        except LeaseError:
            raise
        except Exception as exc:
            message = str(exc)
            raise _map_lease_protocol_error(message) from exc

    async def _unsubscribe(self, sub_id: int) -> None:
        subscription = self._subscriptions.pop(sub_id, None)
        if subscription is None:
            return
        writer = BufferWriter()
        writer.write_route(subscription[0])
        await self.request_frame(MSG_LEASE_UNSUBSCRIBE, writer.build())

    def _init_notify_handler(self) -> None:
        if self._initialized:
            return
        self._initialized = True

        def handler(payload: bytes) -> None:
            try:
                reader = BufferReader(payload)
                sub_id = reader.read_u64_be()
                route = reader.read_route()
                subscription = self._subscriptions.get(sub_id)
                if subscription is None:
                    return
                result = subscription[1](route)
                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception:
                return

        self.connection.register_notification_handler(MSG_LEASE_NOTIFY, handler)

    async def _restore_subscriptions(self) -> None:
        if not self._subscriptions:
            return
        snapshot = list(self._subscriptions.values())
        self._subscriptions.clear()
        for pattern, handler in snapshot:
            await self.subscribe(pattern, handler)


def _map_lease_protocol_error(message: str) -> LeaseError:
    normalized = message.lower()
    if "held" in normalized:
        return lease_error(message, 1)
    if "not found" in normalized:
        return lease_error(message, 2)
    if "invalid" in normalized or "token" in normalized or "fence" in normalized:
        return lease_error(message, 3)
    return LeaseError(message, "ERROR")


def _assert_lease_route(route: str) -> None:
    if not is_exact_route_shape(route, "lease", 3):
        raise LeaseError(
            f"Invalid lease route: {route} (expected lease://{{realm}}/{{area}}/{{resource}}, no empty segments or wildcards)",
            "INVALID_ROUTE",
        )
