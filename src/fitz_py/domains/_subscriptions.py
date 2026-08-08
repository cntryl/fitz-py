"""Shared one-wire/many-consumer subscription registry."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Generic, TypeVar

from fitz_py._runtime import AsyncSubscription
from fitz_py.errors import ReconnectRestoreError

T = TypeVar("T")


@dataclass(slots=True)
class WireSubscription(Generic[T]):
    sub_id: int
    consumers: set[AsyncSubscription[T]] = field(default_factory=set)


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

    async def subscribe(self, registration: str) -> AsyncSubscription[T]:
        state = self._by_registration.get(registration)
        if state is None:
            state = WireSubscription(await self._subscribe_wire(registration))
            self._by_registration[registration] = state
            self._by_id[state.sub_id] = registration

        subscription: AsyncSubscription[T]

        async def close() -> None:
            current = self._by_registration.get(registration)
            if current is None:
                return
            current.consumers.discard(subscription)
            if current.consumers:
                return
            await self._unsubscribe_wire(registration)
            self._by_registration.pop(registration, None)
            self._by_id.pop(current.sub_id, None)

        subscription = AsyncSubscription(registration, self._capacity, close)
        state.consumers.add(subscription)
        return subscription

    def publish(self, sub_id: int, item: T) -> None:
        registration = self._by_id.get(sub_id)
        if registration is None:
            return
        state = self._by_registration.get(registration)
        if state is None:
            return
        dead = {consumer for consumer in state.consumers if not consumer.push(item)}
        state.consumers.difference_update(dead)

    async def restore(
        self,
        *,
        domain: str,
        on_error: Callable[[str, BaseException], None] | None = None,
    ) -> None:
        for registration, state in list(self._by_registration.items()):
            old_id = state.sub_id
            try:
                new_id = await self._subscribe_wire(registration)
            except BaseException as exc:
                error = ReconnectRestoreError(domain, registration, exc)
                self.fail_registration(registration, error)
                if on_error is not None:
                    on_error(registration, error)
                continue
            state.sub_id = new_id
            self._by_id.pop(old_id, None)
            self._by_id[new_id] = registration

    def fail_registration(self, registration: str, error: BaseException) -> None:
        state = self._by_registration.pop(registration, None)
        if state is None:
            return
        self._by_id.pop(state.sub_id, None)
        for consumer in state.consumers:
            consumer.fail(error)

    @property
    def registrations(self) -> tuple[str, ...]:
        return tuple(self._by_registration)
