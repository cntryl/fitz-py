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
        if self._active < self._maximum:
            self._active += 1
            return self._release
        if len(self._waiters) >= self._queue_size:
            raise RequestQueueFullError()

        waiter = asyncio.get_running_loop().create_future()
        self._waiters.append(waiter)
        try:
            await waiter
        except BaseException:
            with contextlib.suppress(ValueError):
                self._waiters.remove(waiter)
            raise
        if self._closed:
            raise FitzConnectionError("Connection is closed")
        self._active += 1
        return self._release

    def _release(self) -> None:
        if self._active > 0:
            self._active -= 1
        while self._waiters:
            waiter = self._waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)
                break

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
        except BaseException as exc:
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
    ) -> None:
        self.registration = registration
        self._queue: asyncio.Queue[T | BaseException | object] = asyncio.Queue(capacity)
        self._close_wire = close_wire
        self._closed = False

    def __aiter__(self) -> AsyncSubscription[T]:
        return self

    async def __anext__(self) -> T:
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
        try:
            self._queue.put_nowait(item)
            return True
        except asyncio.QueueFull:
            self.fail(SubscriptionBackpressureError())
            return False

    def fail(self, error: BaseException) -> None:
        if self._closed:
            return
        self._closed = True
        while not self._queue.empty():
            with contextlib.suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()
        self._queue.put_nowait(error)

    async def unsubscribe(self) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._close_wire()
        with contextlib.suppress(asyncio.QueueFull):
            self._queue.put_nowait(_END)


async def sleep_backoff(delay: float) -> None:
    await asyncio.sleep(delay)
