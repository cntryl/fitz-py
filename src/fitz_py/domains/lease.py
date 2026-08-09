"""Fenced leases, queued acquisition, and managed renewal."""

from __future__ import annotations

import asyncio
import contextlib
import secrets
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from fitz_py._runtime import LazyAsyncIterator
from fitz_py.connection import Connection
from fitz_py.domains._routes import is_exact_route_shape
from fitz_py.domains._subscriptions import SubscriptionRegistry
from fitz_py.domains.base import DomainClient
from fitz_py.errors import (
    FitzConnectionError,
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
    expires_at: datetime | None


@dataclass(slots=True)
class Lease:
    route: str
    owner_id: str
    token: int
    expires_at: datetime
    _client: LeaseClient
    _generation: int
    _released: bool = False

    def _valid(self) -> None:
        if self._released or self._generation != self._client.connection.generation:
            raise StaleHandleError("Lease")

    async def extend(self, ttl: int) -> None:
        self._valid()
        new_token = await self._client._extend(self.route, self.owner_id, self.token, ttl)  # noqa: SLF001
        if new_token is not None:
            self.token = new_token
        self.expires_at = datetime.now(UTC) + timedelta(seconds=ttl)

    async def release(self) -> None:
        self._valid()
        await self._client._release(self.route, self.owner_id, self.token)  # noqa: SLF001
        self._released = True


class ManagedLease:
    def __init__(
        self,
        client: LeaseClient,
        route: str,
        owner_id: str | None,
        ttl: int,
        wait: int,
    ) -> None:
        self._client, self._route, self._ttl, self._wait = client, route, ttl, wait
        self._owner_id = owner_id
        self._lease: Lease | None = None
        self._renewal: asyncio.Task[None] | None = None
        self._lost: Exception | None = None

    async def __aenter__(self) -> Lease:
        self._lease = await self._client.acquire(
            self._route,
            owner_id=self._owner_id,
            ttl=self._ttl,
            wait=self._wait,
        )
        self._renewal = asyncio.create_task(self._renew())
        return self._lease

    async def __aexit__(self, *_args: object) -> None:
        failures: list[Exception] = []
        if self._renewal is not None:
            self._renewal.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._renewal
        if self._lost is not None:
            failures.append(self._lost)
        if self._lease is not None and not self._lease._released:  # noqa: SLF001
            try:
                await self._lease.release()
            except Exception as exc:  # noqa: BLE001
                failures.append(exc)
        if len(failures) == 1:
            raise failures[0]
        if failures:
            raise LeaseLifecycleError(failures)

    async def _renew(self) -> None:
        lease = self._lease
        if lease is None:
            return
        try:
            while True:
                await asyncio.sleep(self._ttl / 3)
                await lease.extend(self._ttl)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._lost = LeaseLostError(str(exc))


class LeaseClient(DomainClient):
    def __init__(self, connection: Connection) -> None:
        super().__init__(connection)
        self._owner_id = secrets.token_hex(16)
        self._acquire_lock = asyncio.Lock()
        self._queued: deque[asyncio.Future[bytes]] = deque()
        self._cancelled_acquires: set[asyncio.Task[None]] = set()
        self._subscriptions = SubscriptionRegistry[str](
            connection.config.limits.subscription_buffer_size,
            self._subscribe_wire,
            self._unsubscribe_wire,
        )
        connection.register_notification_handler(MSG_LEASE_ACQUIRE, self._queued_reply)
        connection.register_notification_handler(MSG_LEASE_NOTIFY, self._notify)
        connection.on_disconnect(self._disconnect)
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
        on_close = getattr(connection, "on_close", None)
        if callable(on_close):
            on_close(self._subscriptions.terminate)

    async def acquire(
        self,
        route: str,
        *,
        owner_id: str | None = None,
        ttl: int,
        wait: int = 0,
    ) -> Lease:
        _route(route)
        _duration(ttl, "ttl", positive=True)
        _duration(wait, "wait")
        if ttl <= 0 or wait < 0 or wait > 2**32 - 1:
            raise ValueError("ttl must be positive and wait must fit u32 seconds")
        resolved_owner_id = self._owner_id if owner_id is None else owner_id
        if not resolved_owner_id:
            raise ValueError("owner_id must not be empty")
        queued = asyncio.get_running_loop().create_future()
        await self._acquire_lock.acquire()
        release_lock = True
        try:
            self._queued.append(queued)
            writer = BufferWriter()
            writer.write_route(route)
            writer.write_route(resolved_owner_id)
            writer.write_u64_be(ttl)
            writer.write_u32_be(wait)
            payload = await self.request_frame(MSG_LEASE_ACQUIRE, writer.build())
            response_type, token = self._decode_acquire(payload)
            if response_type not in {2, 3}:
                self._queued.remove(queued)
            else:
                response_type, token = self._decode_acquire(await asyncio.shield(queued))
                _validate_final_acquire(response_type)
        except asyncio.CancelledError:
            if queued in self._queued:
                task = asyncio.create_task(self._drain_cancelled_acquire(queued))
                self._cancelled_acquires.add(task)
                task.add_done_callback(self._cancelled_acquire_completed)
                release_lock = False
            raise
        except BaseException:
            if queued in self._queued:
                with contextlib.suppress(ValueError):
                    self._queued.remove(queued)
            raise
        finally:
            if release_lock:
                self._acquire_lock.release()
        return Lease(
            route,
            resolved_owner_id,
            token,
            datetime.now(UTC) + timedelta(seconds=ttl),
            self,
            self.connection.generation,
        )

    def hold(
        self,
        route: str,
        *,
        owner_id: str | None = None,
        ttl: int,
        wait: int = 0,
    ) -> ManagedLease:
        return ManagedLease(self, route, owner_id, ttl, wait)

    async def query(self, route: str) -> LeaseInfo:
        _route(route)
        writer = BufferWriter()
        writer.write_route(route)
        response = parse_response(await self.request_frame(MSG_LEASE_QUERY, writer.build()))
        if not response.success:
            raise domain_error(LeaseError, "QUERY", response.error_code or 0, response.error)
        reader = BufferReader(response.data)
        held_flag = reader.read_u8()
        if held_flag not in {0, 1}:
            raise LeaseError("QUERY response has an invalid held flag", "INVALID_RESPONSE")
        held = held_flag == 1
        if not held:
            result = LeaseInfo(False, None, None, reader.read_u32_be(), None)
            if not reader.is_eof():
                raise LeaseError("QUERY response has trailing bytes", "INVALID_RESPONSE")
            return result
        owner = reader.read_route()
        ttl = reader.read_u64_be()
        result = LeaseInfo(
            True,
            owner,
            ttl,
            reader.read_u32_be(),
            datetime.now(UTC) + timedelta(seconds=ttl),
        )
        if not reader.is_eof():
            raise LeaseError("QUERY response has trailing bytes", "INVALID_RESPONSE")
        return result

    def subscribe(self, route: str) -> LazyAsyncIterator[str]:
        _route(route)
        return LazyAsyncIterator(lambda: self._subscriptions.subscribe(route))

    async def _extend(self, route: str, owner_id: str, token: int, ttl: int) -> int | None:
        _duration(ttl, "ttl", positive=True)
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_route(owner_id)
        writer.write_u64_be(token)
        writer.write_u64_be(ttl)
        response = parse_response(await self.request_frame(MSG_LEASE_EXTEND, writer.build()))
        if not response.success:
            raise domain_error(LeaseError, "EXTEND", response.error_code or 0, response.error)
        if len(response.data) != 8:
            raise LeaseError("EXTEND response must contain one fencing token", "INVALID_RESPONSE")
        return BufferReader(response.data).read_u64_be()

    async def _release(self, route: str, owner_id: str, token: int) -> None:
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_route(owner_id)
        writer.write_u64_be(token)
        response = parse_response(await self.request_frame(MSG_LEASE_RELEASE, writer.build()))
        if not response.success:
            raise domain_error(LeaseError, "RELEASE", response.error_code or 0, response.error)
        if response.data:
            raise LeaseError("RELEASE response has trailing bytes", "INVALID_RESPONSE")

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

    def _disconnect(self) -> None:
        error = FitzConnectionError("Lease acquisition interrupted by disconnect")
        while self._queued:
            future = self._queued.popleft()
            if not future.done():
                future.set_exception(error)

    async def _drain_cancelled_acquire(self, future: asyncio.Future[bytes]) -> None:
        try:
            with contextlib.suppress(BaseException):
                await asyncio.shield(future)
        finally:
            self._acquire_lock.release()

    def _cancelled_acquire_completed(self, task: asyncio.Task[None]) -> None:
        self._cancelled_acquires.discard(task)
        if not task.cancelled():
            task.exception()

    async def _subscribe_wire(self, route: str) -> int:
        writer = BufferWriter()
        writer.write_route(route)
        response = parse_response(await self.request_frame(MSG_LEASE_SUBSCRIBE, writer.build()))
        if not response.success:
            raise domain_error(LeaseError, "SUBSCRIBE", response.error_code or 0, response.error)
        if len(response.data) != 8:
            raise LeaseError("SUBSCRIBE response must contain one id", "INVALID_RESPONSE")
        return BufferReader(response.data).read_u64_be()

    async def _unsubscribe_wire(self, route: str) -> None:
        writer = BufferWriter()
        writer.write_route(route)
        response = parse_response(await self.request_frame(MSG_LEASE_UNSUBSCRIBE, writer.build()))
        if not response.success:
            raise domain_error(LeaseError, "UNSUBSCRIBE", response.error_code or 0, response.error)
        if response.data:
            raise LeaseError("UNSUBSCRIBE response has trailing bytes", "INVALID_RESPONSE")

    def _notify(self, payload: bytes) -> None:
        reader = BufferReader(payload)
        sub_id, route = reader.read_u64_be(), reader.read_route()
        if reader.read_bytes(reader.read_u32_be()) or not reader.is_eof():
            raise LeaseError("Invalid LEASE_NOTIFY payload", "INVALID_RESPONSE")
        self._subscriptions.publish(sub_id, route)


def _route(route: str) -> None:
    if not is_exact_route_shape(route, "lease", 3):
        raise LeaseError(f"Invalid lease route: {route}", "INVALID_ROUTE")


def _validate_final_acquire(response_type: int) -> None:
    if response_type not in {0, 1}:
        raise LeaseError("ACQUIRE returned a second queued response", "INVALID_RESPONSE")


def _duration(value: object, name: str, *, positive: bool = False) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer number of seconds")
    if value < (1 if positive else 0):
        qualifier = "positive" if positive else "non-negative"
        raise ValueError(f"{name} must be {qualifier}")
