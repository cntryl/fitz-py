"""Shared one-wire/many-consumer subscription registry."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Generic, TypeVar, cast

from fitz_py._runtime import AsyncSubscription
from fitz_py.errors import ReconnectRestoreError

T = TypeVar("T")


@dataclass(slots=True)
class WireSubscription(Generic[T]):
    sub_id: int
    consumers: set[AsyncSubscription[T]] = field(
        default_factory=cast("Callable[[], set[AsyncSubscription[T]]]", set)
    )


class SubscriptionRegistry(Generic[T]):
    def __init__(
        self,
        capacity: int,
        subscribe_wire: Callable[[str], Awaitable[int]],
        unsubscribe_wire: Callable[[str], Awaitable[None]],
    ) -> None:
        self._capacity = capacity
        self._subscribe_wire = subscribe_wire
        self._unsubscribe_wire = unsubscribe_wire
        self._by_registration: dict[str, WireSubscription[T]] = {}
        self._by_id: dict[int, str] = {}
        self._lock = asyncio.Lock()
        self._cleanup_tasks: set[asyncio.Task[None]] = set()
        self._pending_notifications: deque[tuple[int, T]] = deque(maxlen=capacity)

    async def subscribe(
        self,
        registration: str,
        *,
        on_push: Callable[[T], None] | None = None,
        on_failure: Callable[[BaseException], None] | None = None,
    ) -> AsyncSubscription[T]:
        async with self._lock:
            state = self._by_registration.get(registration)
            if state is None:
                subscribe_task = asyncio.ensure_future(self._subscribe_wire(registration))
                try:
                    sub_id = await asyncio.shield(subscribe_task)
                except asyncio.CancelledError as cancelled:
                    try:
                        sub_id = await subscribe_task
                    except BaseException:  # noqa: BLE001 - preserve caller cancellation
                        raise cancelled from None
                    try:
                        await self._unsubscribe_wire(registration)
                    except BaseException:  # noqa: BLE001 - retain ambiguous live registration
                        state = WireSubscription[T](sub_id)
                        self._by_registration[registration] = state
                        self._by_id[sub_id] = registration
                    raise
                state = WireSubscription[T](sub_id)
                self._by_registration[registration] = state
                self._by_id[state.sub_id] = registration

        async def close() -> None:
            async with self._lock:
                current = self._by_registration.get(registration)
                if current is None:
                    return
                current.consumers.discard(subscription)
                if current.consumers:
                    return

                async def unsubscribe() -> None:
                    await self._unsubscribe_wire(registration)

                task = asyncio.create_task(unsubscribe())
                succeeded = False
                try:
                    await asyncio.shield(task)
                    succeeded = True
                except asyncio.CancelledError:
                    await task
                    succeeded = True
                    raise
                finally:
                    if succeeded:
                        self._by_registration.pop(registration, None)
                        self._by_id.pop(current.sub_id, None)

        subscription = AsyncSubscription[T](
            registration,
            self._capacity,
            close,
            on_push,
            on_failure,
        )
        state.consumers.add(subscription)
        self._flush_pending(state)
        return subscription

    def publish(self, sub_id: int, item: T) -> None:
        registration = self._by_id.get(sub_id)
        if registration is None:
            self._pending_notifications.append((sub_id, item))
            return
        state = self._by_registration.get(registration)
        if state is None:
            return
        dead = {consumer for consumer in state.consumers if not consumer.push(item)}
        state.consumers.difference_update(dead)
        for consumer in dead:
            self._schedule_cleanup(consumer)

    async def restore(
        self,
        *,
        domain: str,
        on_error: Callable[[str, BaseException], None] | None = None,
    ) -> None:
        async with self._lock:
            for registration, state in list(self._by_registration.items()):
                old_id = state.sub_id
                try:
                    new_id = await self._subscribe_wire(registration)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    error = ReconnectRestoreError(domain, registration, exc)
                    self.fail_registration(registration, error)
                    if on_error is not None:
                        on_error(registration, error)
                    continue
                state.sub_id = new_id
                self._by_id.pop(old_id, None)
                self._by_id[new_id] = registration
                self._flush_pending(state)

    def fail_registration(self, registration: str, error: BaseException) -> None:
        state = self._by_registration.pop(registration, None)
        if state is None:
            return
        self._by_id.pop(state.sub_id, None)
        for consumer in state.consumers:
            consumer.fail(error)

    def terminate(self, error: BaseException | None = None) -> None:
        states = tuple(self._by_registration.values())
        self._by_registration.clear()
        self._by_id.clear()
        self._pending_notifications.clear()
        for state in states:
            for consumer in state.consumers:
                if error is None:
                    consumer.finish()
                else:
                    consumer.fail(error)

    def _schedule_cleanup(self, consumer: AsyncSubscription[T]) -> None:
        task = asyncio.create_task(consumer.aclose())
        self._cleanup_tasks.add(task)
        task.add_done_callback(self._cleanup_completed)

    def _cleanup_completed(self, task: asyncio.Task[None]) -> None:
        self._cleanup_tasks.discard(task)
        if not task.cancelled():
            task.exception()

    def _flush_pending(self, state: WireSubscription[T]) -> None:
        retained: deque[tuple[int, T]] = deque(maxlen=self._capacity)
        while self._pending_notifications:
            sub_id, item = self._pending_notifications.popleft()
            if sub_id == state.sub_id:
                for consumer in tuple(state.consumers):
                    consumer.push(item)
            else:
                retained.append((sub_id, item))
        self._pending_notifications = retained

    @property
    def registrations(self) -> tuple[str, ...]:
        return tuple(self._by_registration)
