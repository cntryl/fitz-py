"""Fenced leases, queued acquisition, and managed renewal."""

from __future__ import annotations

import asyncio
import contextlib
import random
import secrets
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from fitz_py._runtime import LazyAsyncIterator
from fitz_py.connection import Connection
from fitz_py.domains._routes import is_exact_route_shape, is_selector_route_shape
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
    MSG_LEASE_LIST,
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


@dataclass(frozen=True, slots=True)
class LeaseListCursor:
    """Opaque continuation token for a LIST scan. Pass back verbatim."""

    snapshot_id: int
    offset: int


@dataclass(frozen=True, slots=True)
class LeaseListItem:
    route: str
    owner_id: str
    holder_incarnation: int
    acquired_at: str
    expires_in_secs: int
    renewals: int


@dataclass(frozen=True, slots=True)
class LeaseListPage:
    items: tuple[LeaseListItem, ...]
    cursor: LeaseListCursor | None


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


_CHANGE_CLOSED = object()


class LeaseInventoryObserver:
    """Race-safe, reconnect-aware view of a Lease selector's held-lease
    inventory (docs/clients/client-requirements.md REQ-API-004C; background:
    https://github.com/cntryl/fitz/issues/219 §5). Created via
    ``LeaseClient.observe()``, never directly.

    Bootstrap: subscribes to the pattern and waits for its acknowledgement,
    then buffers incoming ``LEASE_NOTIFY`` events for it while a full
    ``LIST`` scan builds the initial view. Installing that view marks the
    observer ready; if any notifications arrived during the scan, repeated
    full ``LIST`` passes reconcile them until one completes without a raced
    invalidation (a notify carries no payload beyond
    "this route changed", so a full relist - not a diff - is the only safe
    reconciliation).

    Steady state: each subsequent notification schedules a coalesced full
    ``LIST`` reconciliation. ``QUERY`` cannot return holder incarnation,
    acquisition time, or renewal count, so it cannot safely rebuild an
    observed item. A jittered periodic reconciliation
    (``reconcile_interval`` seconds, default 60s, ±20% jitter by default) is
    a backstop against a missed or dropped notification.

    Reconnect: Lease subscriptions and state do not survive a broker-side
    disconnect, so a reconnect clears readiness and re-runs the full
    bootstrap from scratch.

    Convergence backoff: if a bootstrap ``LIST`` pass keeps racing fresh
    notifications (e.g. continuous churn on a hot selector), each retry
    waits a bounded, jittered exponential backoff (50ms-2s, using the same
    ``sleep``/``random_func`` as periodic reconciliation) before the next
    full-relist attempt, rather than hammering the broker back-to-back.

    Degraded state: if a background task (the notification consumer, the
    bootstrap loop, or the periodic reconciler) ever fails permanently, the
    failure is logged via the connection's observability hooks
    (``Connection.report_restore_failure``, the same convention Notice/RPC
    use for background-task failures) and recorded on ``error``/``degraded``
    for callers to detect - the observer does not silently degrade to
    poll-only with no visible signal.

    ``view`` returns the current snapshot, ``ready`` reports whether the
    first bootstrap has completed, ``changes()`` is an async-iterator change
    stream (``str`` for a single route, ``None`` when the whole view was
    replaced by a relist), and ``close()`` / ``async with`` stop the
    observer's background task and unsubscribe. ``changes()`` shares one
    internal queue, so only one concurrent consumer is supported. That queue
    is bounded (``changes_maxsize``, default 256): once full, the oldest
    pending entry is dropped to make room for the newest one, since ``view``
    is always the authoritative current state and a caller that only polls
    ``view`` (never draining ``changes()``) must not leak memory for the
    observer's lifetime.
    """

    _BOOTSTRAP_BACKOFF_BASE = 0.05
    _BOOTSTRAP_BACKOFF_MAX = 2.0

    def __init__(
        self,
        client: LeaseClient,
        pattern: str,
        *,
        reconcile_interval: float = 60.0,
        jitter: float = 0.2,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        random_func: Callable[[], float] = random.random,
        changes_maxsize: int = 256,
    ) -> None:
        self._client = client
        self._pattern = pattern
        self._reconcile_interval = reconcile_interval
        self._jitter = jitter
        self._sleep = sleep if sleep is not None else asyncio.sleep
        self._random = random_func
        self._view: dict[str, LeaseListItem] = {}
        self._ready = asyncio.Event()
        self._closed = False
        self._mode = "buffering"
        self._buffer: list[str] = []
        self._notification_generation = 0
        self._lock = asyncio.Lock()
        self._invalidate = asyncio.Event()
        self._invalidate.set()
        self._changes: asyncio.Queue[object] = asyncio.Queue(maxsize=max(1, changes_maxsize))
        self._subscription: LazyAsyncIterator[str] | None = None
        self._subscription_ready = asyncio.Event()
        self._subscription_generation = 0
        self._unregister_reconnect: Callable[[], None] | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._error: BaseException | None = None

    @property
    def ready(self) -> bool:
        return self._ready.is_set()

    @property
    def view(self) -> dict[str, LeaseListItem]:
        return dict(self._view)

    @property
    def error(self) -> BaseException | None:
        """The first fatal error from a background task, if any (see `degraded`)."""
        return self._error

    @property
    def degraded(self) -> bool:
        """Whether a background task has permanently failed.

        When true, this observer has stopped reacting to notifications
        and/or reconciling and will not recover on its own; `error` holds
        the cause. Close and re-`observe()` to recover.
        """
        return self._error is not None

    async def wait_ready(self) -> None:
        await self._ready.wait()

    async def changes(self) -> AsyncIterator[str | None]:
        while True:
            item = await self._changes.get()
            if item is _CHANGE_CLOSED:
                return
            yield item  # type: ignore[misc]

    async def __aenter__(self) -> LeaseInventoryObserver:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._invalidate.set()
        if self._unregister_reconnect is not None:
            self._unregister_reconnect()
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._subscription is not None:
            await self._subscription.aclose()
        self._enqueue_change(_CHANGE_CLOSED)

    async def _start(self) -> None:
        subscription = self._client._subscribe_observer(  # noqa: SLF001
            self._pattern,
            self._note_notification,
            self._note_subscription_failure,
        )
        self._subscription = subscription
        # Step 1: establish the subscription and wait for its ack before any
        # LIST traffic - the subsequent bootstrap task relies on the wire
        # subscription (and thus buffering) already being live.
        await subscription.__aenter__()
        self._subscription_generation += 1
        self._subscription_ready.set()
        self._unregister_reconnect = self._client.connection.on_reconnect(
            self._handle_reconnect,
            domain="lease",
            registration=f"observer:{id(self)}",
        )
        self._spawn(self._consume())
        self._spawn(self._bootstrap_loop())
        self._spawn(self._reconcile_loop())

    def _spawn(self, coro: Coroutine[object, object, None]) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        # A background task (consume/bootstrap/reconcile) failed permanently
        # - surface it rather than letting the observer silently degrade to
        # poll-only with no visible signal (issue #219 §5 follow-up).
        if self._error is None:
            self._error = exc
        self._client.connection.report_restore_failure("lease", f"observer:{id(self)}", exc)

    def _note_notification(self, _route: str) -> None:
        """Record invalidation synchronously before queue-based dispatch."""
        self._notification_generation += 1
        if self._mode != "buffering":
            self._ready.clear()
            self._invalidate.set()

    def _note_subscription_failure(self, error: BaseException) -> None:
        """Synchronously invalidate a view whose wire subscription failed."""
        self._subscription_generation += 1
        self._subscription_ready.clear()
        self._ready.clear()
        self._mode = "buffering"
        self._buffer.clear()
        self._invalidate.set()
        self._client.connection.report_restore_failure(
            "lease",
            f"observer:{id(self)}",
            error,
        )

    async def _handle_reconnect(self) -> None:
        self._ready.clear()
        async with self._lock:
            self._mode = "buffering"
            self._buffer.clear()
            # A LIST already in flight must not install a view assembled
            # across a broker generation boundary, even when no notification
            # happens to arrive during that pass.
            self._subscription_generation += 1
        self._invalidate.set()

    async def _bootstrap_loop(self) -> None:
        attempt = 0
        while True:
            await self._invalidate.wait()
            if self._closed:
                return
            self._invalidate.clear()
            await self._subscription_ready.wait()
            # A replacement subscription sets `_invalidate` while this loop
            # may already be waiting on `_subscription_ready`. The LIST below
            # covers every change before it starts, so consume that coalesced
            # wake-up now instead of immediately issuing a redundant second
            # LIST after the recovery pass.
            self._invalidate.clear()
            try:
                await self._bootstrap()
                attempt = 0
            except asyncio.CancelledError:
                raise
            except BaseException as error:  # noqa: BLE001 - retry transient LIST failures
                self._ready.clear()
                self._client.connection.report_restore_failure(
                    "lease",
                    f"observer:{id(self)}",
                    error,
                )
                attempt += 1
                await self._sleep(self._bootstrap_backoff(attempt))
                self._invalidate.set()

    async def _bootstrap(self) -> None:
        # Steps 2-3: buffer notifications while a full LIST scan builds the
        # view (`_consume` checks `_mode` on every notification).
        async with self._lock:
            self._mode = "buffering"
            self._buffer.clear()
        attempt = 0
        while True:
            await self._subscription_ready.wait()
            subscription_generation = self._subscription_generation
            start_generation = self._notification_generation
            view = await self._full_list()
            async with self._lock:
                raced = (
                    bool(self._buffer)
                    or self._notification_generation != start_generation
                    or not self._subscription_ready.is_set()
                    or self._subscription_generation != subscription_generation
                )
                if raced:
                    # An invalidation raced this complete pass. Stay in
                    # buffering mode and repeat; never expose this pass as a
                    # ready view in between.
                    self._buffer.clear()
                else:
                    self._view = view
                    self._mode = "steady"
                    self._buffer.clear()
                    self._ready.set()
            if raced:
                # Under continuous churn on a hot selector, retrying with no
                # delay would hammer the broker with back-to-back full LISTs
                # and never converge. Back off (bounded, jittered) between
                # attempts instead.
                attempt += 1
                await self._sleep(self._bootstrap_backoff(attempt))
                continue
            self._emit(None)
            return

    def _bootstrap_backoff(self, attempt: int) -> float:
        cap = min(
            self._BOOTSTRAP_BACKOFF_MAX,
            self._BOOTSTRAP_BACKOFF_BASE * (2 ** min(attempt, 10)),
        )
        # Equal jitter keeps every retry at or above the documented 50ms
        # floor while still spreading a fleet across the upper half of the
        # current exponential window.
        return max(self._BOOTSTRAP_BACKOFF_BASE, cap * (0.5 + (self._random() * 0.5)))

    async def _consume(self) -> None:
        attempt = 0
        while not self._closed:
            assert self._subscription is not None  # noqa: S101
            try:
                async for route in self._subscription:
                    async with self._lock:
                        buffering = self._mode == "buffering"
                        if buffering:
                            self._buffer.append(route)
                    if not buffering:
                        self._ready.clear()
                        self._invalidate.set()
            except asyncio.CancelledError:
                raise
            except BaseException as error:  # noqa: BLE001 - subscription failures are recoverable
                # `AsyncSubscription.fail` already invalidated synchronously
                # through `_note_subscription_failure`; report failures from
                # implementations that terminate by raising directly too.
                if self._subscription_ready.is_set():
                    self._note_subscription_failure(error)

            if self._closed:
                return

            old_subscription = self._subscription
            with contextlib.suppress(Exception):
                await old_subscription.aclose()

            while not self._closed:
                try:
                    replacement = self._client._subscribe_observer(  # noqa: SLF001
                        self._pattern,
                        self._note_notification,
                        self._note_subscription_failure,
                    )
                    await replacement.__aenter__()
                except asyncio.CancelledError:
                    raise
                except BaseException as error:  # noqa: BLE001 - retry transient wire failures
                    self._client.connection.report_restore_failure(
                        "lease",
                        f"observer:{id(self)}",
                        error,
                    )
                    attempt += 1
                    await self._sleep(self._bootstrap_backoff(attempt))
                    continue

                self._subscription = replacement
                self._subscription_generation += 1
                self._subscription_ready.set()
                self._invalidate.set()
                attempt = 0
                break

    async def _full_list(self) -> dict[str, LeaseListItem]:
        view: dict[str, LeaseListItem] = {}
        async for item in self._client.list_leases(self._pattern):
            view[item.route] = item
        return view

    async def _reconcile_loop(self) -> None:
        # Step 7: periodic full-relist backstop against a missed or dropped
        # notification, on a jittered interval so a fleet does not
        # reconcile in lockstep.
        while True:
            multiplier = (1 - self._jitter) + self._random() * 2 * self._jitter
            await self._sleep(self._reconcile_interval * multiplier)
            if self._closed:
                return
            self._ready.clear()
            self._invalidate.set()

    def _emit(self, route: str | None) -> None:
        self._enqueue_change(route)

    def _enqueue_change(self, item: object) -> None:
        # Bounded, drop-oldest: `view` is always the authoritative current
        # state, so a caller that never drains `changes()` must not leak
        # memory for the observer's lifetime (issue #219 §5 follow-up).
        while True:
            try:
                self._changes.put_nowait(item)
            except asyncio.QueueFull:
                with contextlib.suppress(asyncio.QueueEmpty):
                    self._changes.get_nowait()
            else:
                return


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
        queued: asyncio.Future[bytes] | None = None
        if wait > 0:
            queued = asyncio.get_running_loop().create_future()
            # A disconnect can fail this internal future while the primary
            # ACQUIRE request is still pending. Observe that failure from the
            # moment it can be published; awaiting the same future later still
            # preserves the public acquisition error.
            queued.add_done_callback(_observe_acquire_future)
        await self._acquire_lock.acquire()
        release_lock = True
        try:
            if queued is not None:
                self._queued.append(queued)
            writer = BufferWriter()
            writer.write_route(route)
            writer.write_route(resolved_owner_id)
            writer.write_u64_be(ttl)
            writer.write_u32_be(wait)
            payload = await self.request_frame(MSG_LEASE_ACQUIRE, writer.build())
            response_type, token = self._decode_acquire(payload)
            if response_type not in {2, 3}:
                if queued is not None:
                    self._queued.remove(queued)
            else:
                queued_response = _require_queued_acquire_future(queued)
                response_type, token = self._decode_acquire(await asyncio.shield(queued_response))
                _validate_final_acquire(response_type)
        except asyncio.CancelledError:
            if queued is not None and queued in self._queued:
                task = asyncio.create_task(self._drain_cancelled_acquire(queued))
                self._cancelled_acquires.add(task)
                task.add_done_callback(self._cancelled_acquire_completed)
                release_lock = False
            raise
        except BaseException:
            if queued is not None and queued in self._queued:
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
        _pattern(route)
        return LazyAsyncIterator(lambda: self._subscriptions.subscribe(route))

    def _subscribe_observer(
        self,
        route: str,
        on_push: Callable[[str], None],
        on_failure: Callable[[BaseException], None],
    ) -> LazyAsyncIterator[str]:
        return LazyAsyncIterator(
            lambda: self._subscriptions.subscribe(
                route,
                on_push=on_push,
                on_failure=on_failure,
            )
        )

    async def list_page(
        self,
        pattern: str,
        *,
        cursor: LeaseListCursor | None = None,
        limit: int | None = None,
    ) -> LeaseListPage:
        _pattern(pattern)
        _u32(limit, "limit")
        writer = BufferWriter()
        writer.write_route(pattern)
        writer.write_u8(0 if cursor is None else 1)
        if cursor is not None:
            writer.write_u64_be(cursor.snapshot_id)
            writer.write_u32_be(cursor.offset)
        writer.write_u32_be(0 if limit is None else limit)
        response = parse_response(await self.request_frame(MSG_LEASE_LIST, writer.build()))
        if not response.success:
            raise domain_error(LeaseError, "LIST", response.error_code or 0, response.error)
        reader = BufferReader(response.data)
        count = reader.read_u32_be()
        items = tuple(
            LeaseListItem(
                reader.read_route(),
                reader.read_string(),
                reader.read_u64_be(),
                reader.read_string(),
                reader.read_u64_be(),
                reader.read_u32_be(),
            )
            for _ in range(count)
        )
        has_next = reader.read_u8()
        if has_next not in {0, 1}:
            raise LeaseError("LIST response has an invalid cursor flag", "INVALID_RESPONSE")
        next_cursor = (
            LeaseListCursor(reader.read_u64_be(), reader.read_u32_be()) if has_next == 1 else None
        )
        if not reader.is_eof():
            raise LeaseError("LIST response has trailing bytes", "INVALID_RESPONSE")
        return LeaseListPage(items, next_cursor)

    async def observe(
        self,
        pattern: str,
        *,
        reconcile_interval: float = 60.0,
        jitter: float = 0.2,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        random_func: Callable[[], float] = random.random,
        changes_maxsize: int = 256,
    ) -> LeaseInventoryObserver:
        """Return a started `LeaseInventoryObserver` for `pattern`.

        Owns the race-safe subscribe-then-list bootstrap, steady-state full
        LIST reconciliation, periodic reconciliation, and reconnect recovery
        described on `LeaseInventoryObserver`, so callers never have to
        hand-roll it (docs/clients/client-requirements.md REQ-API-004C).

        `changes_maxsize` bounds the internal queue backing `changes()`; once
        full, the oldest pending entry is dropped to make room for the
        newest (see `LeaseInventoryObserver`'s docstring).
        """
        _pattern(pattern)
        if reconcile_interval <= 0:
            raise ValueError("reconcile_interval must be positive")
        if not 0 <= jitter < 1:
            raise ValueError("jitter must be in [0, 1)")
        observer = LeaseInventoryObserver(
            self,
            pattern,
            reconcile_interval=reconcile_interval,
            jitter=jitter,
            sleep=sleep,
            random_func=random_func,
            changes_maxsize=changes_maxsize,
        )
        await observer._start()  # noqa: SLF001
        return observer

    async def list_leases(
        self, pattern: str, *, limit: int | None = None
    ) -> AsyncIterator[LeaseListItem]:
        cursor: LeaseListCursor | None = None
        page = await self.list_page(pattern, limit=limit)
        for item in page.items:
            yield item
        cursor = page.cursor
        while cursor is not None:
            page = await self.list_page(pattern, cursor=cursor, limit=limit)
            for item in page.items:
                yield item
            cursor = page.cursor

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


def _pattern(pattern: str) -> None:
    # Lease's broker grammar is PatternDepth::CanMatch(3) with `**` allowed in
    # any segment (routing-design.md §4 / src/runtime/matcher.rs), not only
    # trailing, so this opts into the broader shape check.
    if not is_selector_route_shape(
        pattern, "lease", 3, allow_realm_wildcard=True, allow_interior_double_star=True
    ):
        raise LeaseError(f"Invalid lease pattern: {pattern}", "INVALID_ROUTE")


def _u32(value: object, name: str) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if not 0 <= value <= 2**32 - 1:
        raise ValueError(f"{name} must fit an unsigned 32-bit integer")


def _validate_final_acquire(response_type: int) -> None:
    if response_type not in {0, 1}:
        raise LeaseError("ACQUIRE returned a second queued response", "INVALID_RESPONSE")


def _observe_acquire_future(future: asyncio.Future[bytes]) -> None:
    if not future.cancelled():
        future.exception()


def _require_queued_acquire_future(
    future: asyncio.Future[bytes] | None,
) -> asyncio.Future[bytes]:
    if future is None:
        raise LeaseError("ACQUIRE queued without a positive wait", "INVALID_RESPONSE")
    return future


def _duration(value: object, name: str, *, positive: bool = False) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer number of seconds")
    if value < (1 if positive else 0):
        qualifier = "positive" if positive else "non-negative"
        raise ValueError(f"{name} must be {qualifier}")
