"""Fenced leases, queued acquisition, and managed renewal."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import deque
from dataclasses import dataclass

from fitz_py._runtime import AsyncSubscription
from fitz_py.domains._routes import is_exact_route_shape
from fitz_py.domains._subscriptions import SubscriptionRegistry
from fitz_py.domains.base import DomainClient
from fitz_py.errors import (
    LeaseError,
    LeaseLifecycleError,
    LeaseLostError,
    StaleHandleError,
    domain_error,
)
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
from fitz_py.protocol.response import parse_response


@dataclass(frozen=True, slots=True)
class LeaseInfo:
    is_held: bool
    owner: str | None
    ttl_remaining: int | None
    pending_waiters: int
    expires_at: float | None


@dataclass(slots=True)
class Lease:
    route: str
    token: int
    expires_at: float
    _client: LeaseClient
    _generation: int
    _released: bool = False

    def _valid(self) -> None:
        if self._released or self._generation != self._client.connection.generation:
            raise StaleHandleError("Lease")

    async def extend(self, ttl: float) -> None:
        self._valid()
        new_token = await self._client._extend(self.route, self.token, ttl)
        if new_token is not None:
            self.token = new_token
        self.expires_at = time.time() + ttl

    async def release(self) -> None:
        self._valid()
        await self._client._release(self.route, self.token)
        self._released = True


class ManagedLease:
    def __init__(self, client: LeaseClient, route: str, ttl: float, wait: float) -> None:
        self._client, self._route, self._ttl, self._wait = client, route, ttl, wait
        self._lease: Lease | None = None
        self._renewal: asyncio.Task[None] | None = None
        self._lost: BaseException | None = None

    async def __aenter__(self) -> Lease:
        self._lease = await self._client.acquire(self._route, ttl=self._ttl, wait=self._wait)
        self._renewal = asyncio.create_task(self._renew())
        return self._lease

    async def __aexit__(self, *_args: object) -> None:
        failures: list[BaseException] = []
        if self._renewal is not None:
            self._renewal.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._renewal
        if self._lost is not None:
            failures.append(self._lost)
        if self._lease is not None and not self._lease._released:
            try:
                await self._lease.release()
            except BaseException as exc:
                failures.append(exc)
        if len(failures) == 1:
            raise failures[0]
        if failures:
            raise LeaseLifecycleError(failures)

    async def _renew(self) -> None:
        assert self._lease is not None
        try:
            while True:
                await asyncio.sleep(self._ttl / 3)
                await self._lease.extend(self._ttl)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._lost = LeaseLostError(str(exc))


class LeaseClient(DomainClient):
    def __init__(self, connection) -> None:
        super().__init__(connection)
        self._acquire_lock = asyncio.Lock()
        self._queued: deque[asyncio.Future[bytes]] = deque()
        self._subscriptions = SubscriptionRegistry[str](
            connection.config.limits.subscription_buffer_size,
            self._subscribe_wire,
            self._unsubscribe_wire,
        )
        connection.register_notification_handler(MSG_LEASE_ACQUIRE, self._queued_reply)
        connection.register_notification_handler(MSG_LEASE_NOTIFY, self._notify)
        connection.on_reconnect(
            lambda: self._subscriptions.restore(
                domain="lease",
                on_error=lambda registration, error: connection.report_restore_failure(
                    "lease", registration, error
                ),
            ),
            domain="lease",
            registration="changes",
        )

    async def acquire(self, route: str, *, ttl: float, wait: float = 0) -> Lease:
        _route(route)
        if ttl <= 0 or wait < 0 or wait > 2**32 - 1:
            raise ValueError("ttl must be positive and wait must fit u32 seconds")
        async with self._acquire_lock:
            queued = asyncio.get_running_loop().create_future()
            self._queued.append(queued)
            writer = BufferWriter()
            writer.write_route(route)
            writer.write_route("")
            writer.write_u64_be(int(ttl))
            writer.write_u32_be(int(wait))
            try:
                payload = await self.request_frame(MSG_LEASE_ACQUIRE, writer.build())
                response_type, token = self._decode_acquire(payload)
                if response_type in {2, 3}:
                    response_type, token = self._decode_acquire(await queued)
                    if response_type not in {0, 1}:
                        raise LeaseError(
                            "ACQUIRE returned a second queued response", "INVALID_RESPONSE"
                        )
                else:
                    self._queued.remove(queued)
            except BaseException:
                with contextlib.suppress(ValueError):
                    self._queued.remove(queued)
                raise
        return Lease(route, token, time.time() + ttl, self, self.connection.generation)

    def hold(self, route: str, *, ttl: float, wait: float = 0) -> ManagedLease:
        return ManagedLease(self, route, ttl, wait)

    async def query(self, route: str) -> LeaseInfo:
        _route(route)
        writer = BufferWriter()
        writer.write_route(route)
        response = parse_response(await self.request_frame(MSG_LEASE_QUERY, writer.build()))
        if not response.success:
            raise domain_error(LeaseError, "QUERY", response.error_code or 0, response.error)
        reader = BufferReader(response.data)
        held = reader.read_u8() == 1
        if not held:
            return LeaseInfo(False, None, None, reader.read_u32_be(), None)
        owner = reader.read_route()
        ttl = reader.read_u64_be()
        return LeaseInfo(True, owner, ttl, reader.read_u32_be(), time.time() + ttl)

    async def subscribe(self, route: str) -> AsyncSubscription[str]:
        _route(route)
        return await self._subscriptions.subscribe(route)

    async def _extend(self, route: str, token: int, ttl: float) -> int | None:
        if ttl <= 0:
            raise ValueError("ttl must be positive")
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_route("")
        writer.write_u64_be(token)
        writer.write_u64_be(int(ttl))
        response = parse_response(await self.request_frame(MSG_LEASE_EXTEND, writer.build()))
        if not response.success:
            raise domain_error(LeaseError, "EXTEND", response.error_code or 0, response.error)
        return BufferReader(response.data).read_u64_be() if len(response.data) >= 8 else None

    async def _release(self, route: str, token: int) -> None:
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_route("")
        writer.write_u64_be(token)
        response = parse_response(await self.request_frame(MSG_LEASE_RELEASE, writer.build()))
        if not response.success:
            raise domain_error(LeaseError, "RELEASE", response.error_code or 0, response.error)

    def _decode_acquire(self, payload: bytes) -> tuple[int, int]:
        response = parse_response(payload)
        if not response.success:
            raise domain_error(LeaseError, "ACQUIRE", response.error_code or 0, response.error)
        reader = BufferReader(response.data)
        response_type, token = reader.read_u8(), reader.read_u64_be()
        if response_type not in {0, 1, 2, 3} or not reader.is_eof():
            raise LeaseError("Invalid ACQUIRE response", "INVALID_RESPONSE")
        return response_type, token

    def _queued_reply(self, payload: bytes) -> None:
        while self._queued:
            future = self._queued.popleft()
            if not future.done():
                future.set_result(payload)
                return

    async def _subscribe_wire(self, route: str) -> int:
        writer = BufferWriter()
        writer.write_route(route)
        response = parse_response(await self.request_frame(MSG_LEASE_SUBSCRIBE, writer.build()))
        if not response.success:
            raise domain_error(LeaseError, "SUBSCRIBE", response.error_code or 0, response.error)
        return BufferReader(response.data).read_u64_be()

    async def _unsubscribe_wire(self, route: str) -> None:
        writer = BufferWriter()
        writer.write_route(route)
        response = parse_response(await self.request_frame(MSG_LEASE_UNSUBSCRIBE, writer.build()))
        if not response.success:
            raise domain_error(LeaseError, "UNSUBSCRIBE", response.error_code or 0, response.error)

    def _notify(self, payload: bytes) -> None:
        reader = BufferReader(payload)
        sub_id, route = reader.read_u64_be(), reader.read_route()
        if reader.read_bytes(reader.read_u32_be()) or not reader.is_eof():
            raise LeaseError("Invalid LEASE_NOTIFY payload", "INVALID_RESPONSE")
        self._subscriptions.publish(sub_id, route)


def _route(route: str) -> None:
    if not is_exact_route_shape(route, "lease", 3):
        raise LeaseError(f"Invalid lease route: {route}", "INVALID_ROUTE")
