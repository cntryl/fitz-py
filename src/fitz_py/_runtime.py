"""Bounded asyncio runtime primitives shared by the client and domains."""

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Generic, TypeVar

from fitz_py.errors import (
    FitzConnectionError,
    RequestQueueFullError,
    SubscriptionBackpressureError,
)

T = TypeVar("T")
_END = object()


class LazyAsyncContext(Generic[T]):
    """Single-start lazy async context manager for manually openable handles."""

    def __init__(
        self,
        factory: Callable[[], Awaitable[T]],
        close: Callable[[T], Awaitable[None]],
    ) -> None:
        self._factory = factory
        self._close = close
        self._resource: T | None = None
        self._start_lock = asyncio.Lock()
        self._closed = False

    async def _start(self) -> T:
        async with self._start_lock:
            if self._closed:
                raise FitzConnectionError("Context is closed")
            if self._resource is None:
                resource = await self._factory()
                if self._closed:
                    await self._close(resource)
                    raise FitzConnectionError("Context is closed")
                self._resource = resource
        return self._resource

    async def __aenter__(self) -> T:
        return await self._start()

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        self._closed = True
        async with self._start_lock:
            if self._resource is not None:
                resource, self._resource = self._resource, None
                await self._close(resource)


class LazyAsyncIterator(AsyncIterator[T], Generic[T]):
    """Single-start lazy async iterator and context manager.

    The factory is not invoked until iteration or context entry. Closing an
    unstarted value is a no-op; closing a started value always delegates to its
    ``aclose`` method.
    """

    def __init__(self, factory: Callable[[], Awaitable[AsyncIterator[T]]]) -> None:
        self._factory = factory
        self._resource: AsyncIterator[T] | None = None
        self._start_lock = asyncio.Lock()
        self._closed = False

    async def _start(self) -> AsyncIterator[T]:
        async with self._start_lock:
            if self._closed:
                raise FitzConnectionError("Stream is closed")
            if self._resource is None:
                resource = await self._factory()
                if self._closed:
                    close = getattr(resource, "aclose", None)
                    if close is not None:
                        await close()
                    raise FitzConnectionError("Stream is closed")
                self._resource = resource
        return self._resource

    def __aiter__(self) -> LazyAsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        return await anext(await self._start())

    async def __aenter__(self) -> LazyAsyncIterator[T]:
        await self._start()
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        self._closed = True
        async with self._start_lock:
            resource, self._resource = self._resource, None
            if resource is not None:
                close = getattr(resource, "aclose", None)
                if close is not None:
                    await close()


class RequestGate:
    def __init__(self, maximum: int, queue_size: int) -> None:
        self._maximum = maximum
        self._queue_size = queue_size
        self._active = 0
        self._closed = False
        self._waiters: deque[asyncio.Future[None]] = deque()

    async def acquire(self) -> Callable[[], None]:
        if self._closed:
            raise FitzConnectionError("Connection is closed")
        # Queued callers own the next slots.  A newcomer may only enter
        # immediately when there is capacity and no older waiter.
        if self._active < self._maximum and not self._waiters:
            self._active += 1
            return self._release_token()
        if len(self._waiters) >= self._queue_size:
            raise RequestQueueFullError

        waiter = asyncio.get_running_loop().create_future()
        self._waiters.append(waiter)
        try:
            await waiter
        except BaseException:
            try:
                self._waiters.remove(waiter)
            except ValueError:
                # The slot was already reserved for this waiter. Transfer it
                # instead of losing capacity when cancellation wins the race.
                self._release_slot()
            raise
        if self._closed:
            self._release_slot()
            raise FitzConnectionError("Connection is closed")
        return self._release_token()

    @property
    def active(self) -> int:
        """Number of acquired or reserved slots (primarily for observability)."""

        return self._active

    def _release_token(self) -> Callable[[], None]:
        released = False

        def release() -> None:
            nonlocal released
            if released:
                return
            released = True
            self._release_slot()

        return release

    def _release_slot(self) -> None:
        while self._waiters:
            waiter = self._waiters.popleft()
            if not waiter.done():
                # Ownership transfers directly: _active intentionally stays
                # unchanged, so capacity cannot be stolen between wakeups.
                waiter.set_result(None)
                return
        if self._active > 0:
            self._active -= 1

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        error = FitzConnectionError("Connection is closed")
        for waiter in self._waiters:
            if not waiter.done():
                waiter.set_exception(error)
        self._waiters.clear()


class AsyncDispatcher:
    def __init__(
        self,
        maximum: int,
        queue_size: int,
        timeout: float,
        on_error: Callable[[BaseException], None],
    ) -> None:
        self._maximum = maximum
        self._queue_size = queue_size
        self._timeout = timeout
        self._on_error = on_error
        self._active: set[asyncio.Task[None]] = set()
        self._queued: deque[Callable[[], Awaitable[None]]] = deque()
        self._closed = False

    def dispatch(self, work: Callable[[], Awaitable[None]]) -> bool:
        if self._closed:
            return False
        if len(self._active) < self._maximum:
            self._start(work)
            return True
        if len(self._queued) >= self._queue_size:
            return False
        self._queued.append(work)
        return True

    def _start(self, work: Callable[[], Awaitable[None]]) -> None:
        task = asyncio.create_task(self._run(work))
        self._active.add(task)
        task.add_done_callback(self._completed)

    async def _run(self, work: Callable[[], Awaitable[None]]) -> None:
        try:
            async with asyncio.timeout(self._timeout):
                await work()
        except BaseException as exc:  # noqa: BLE001
            if not isinstance(exc, asyncio.CancelledError):
                self._on_error(exc)

    def _completed(self, task: asyncio.Task[None]) -> None:
        self._active.discard(task)
        if not self._closed and self._queued:
            self._start(self._queued.popleft())

    async def close(self) -> None:
        self._closed = True
        self._queued.clear()
        tasks = tuple(self._active)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


class AsyncSubscription(AsyncIterator[T], Generic[T]):
    """A bounded, independently closable local subscription consumer."""

    def __init__(
        self,
        registration: str,
        capacity: int,
        close_wire: Callable[[], Awaitable[None]],
        on_push: Callable[[T], None] | None = None,
        on_failure: Callable[[BaseException], None] | None = None,
    ) -> None:
        self.registration = registration
        self._queue: asyncio.Queue[T | BaseException | object] = asyncio.Queue(capacity)
        self._close_wire = close_wire
        self._on_push = on_push
        self._on_failure = on_failure
        self._closed = False
        self._wire_closed = False
        self._close_lock = asyncio.Lock()

    def __aiter__(self) -> AsyncSubscription[T]:
        return self

    async def __anext__(self) -> T:
        if self._closed and self._queue.empty():
            raise StopAsyncIteration
        item = await self._queue.get()
        if item is _END:
            raise StopAsyncIteration
        if isinstance(item, BaseException):
            self._closed = True
            raise item
        return item  # type: ignore[return-value]

    async def __aenter__(self) -> AsyncSubscription[T]:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()

    def push(self, item: T) -> bool:
        if self._closed:
            return False
        if self._on_push is not None:
            self._on_push(item)
        try:
            self._queue.put_nowait(item)
            return True  # noqa: TRY300
        except asyncio.QueueFull:
            self.fail(SubscriptionBackpressureError())
            return False

    def fail(self, error: BaseException) -> None:
        if self._closed:
            return
        if self._on_failure is not None:
            self._on_failure(error)
        self._closed = True
        while not self._queue.empty():
            with contextlib.suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()
        self._queue.put_nowait(error)

    def finish(self) -> None:
        """End local iteration without performing wire cleanup."""
        if self._closed:
            return
        self._closed = True
        while not self._queue.empty():
            with contextlib.suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()
        self._queue.put_nowait(_END)

    async def unsubscribe(self) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        async with self._close_lock:
            if self._wire_closed:
                return
            self._wire_closed = True
            try:
                await self._close_wire()
            finally:
                if not self._closed:
                    self.finish()
