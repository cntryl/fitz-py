"""Fire-and-forget notices and bounded async subscriptions."""

from __future__ import annotations

from dataclasses import dataclass, field

from fitz_py._runtime import AsyncSubscription
from fitz_py.domains._routes import is_exact_route_shape, is_selector_route_shape
from fitz_py.domains.base import DomainClient
from fitz_py.errors import NoticeError, domain_error
from fitz_py.protocol.buffer import BufferReader, BufferWriter
from fitz_py.protocol.messages import (
    MSG_NOTICE_NOTIFY,
    MSG_NOTICE_PUBLISH,
    MSG_NOTICE_SUBSCRIBE,
    MSG_NOTICE_UNSUBSCRIBE,
)
from fitz_py.protocol.response import parse_response


@dataclass(frozen=True, slots=True)
class Notice:
    route: str
    body: bytes


@dataclass(slots=True)
class _Wire:
    sub_id: int
    consumers: set[AsyncSubscription[Notice]] = field(default_factory=set)


class NoticeClient(DomainClient):
    def __init__(self, connection) -> None:
        super().__init__(connection)
        self._by_pattern: dict[str, _Wire] = {}
        self._pattern_by_id: dict[int, str] = {}
        connection.register_notification_handler(MSG_NOTICE_NOTIFY, self._notify)
        connection.on_reconnect(self._restore, domain="notice", registration="subscriptions")

    async def publish(self, route: str, body: bytes) -> None:
        if not is_exact_route_shape(route, "notice", 3):
            raise NoticeError(f"Invalid notice route: {route}", "INVALID_ROUTE")
        writer = BufferWriter()
        writer.write_route(route)
        writer.write_u32_be(len(body))
        writer.write_bytes(body)
        await self.connection.send(MSG_NOTICE_PUBLISH, writer.build())

    async def subscribe(self, pattern: str) -> AsyncSubscription[Notice]:
        if not is_selector_route_shape(pattern, "notice", 3, allow_realm_wildcard=True):
            raise NoticeError(f"Invalid notice pattern: {pattern}", "INVALID_ROUTE")
        state = self._by_pattern.get(pattern)
        if state is None:
            state = _Wire(await self._subscribe_wire(pattern))
            self._by_pattern[pattern] = state
            self._pattern_by_id[state.sub_id] = pattern
        subscription: AsyncSubscription[Notice]

        async def close() -> None:
            current = self._by_pattern.get(pattern)
            if current is None:
                return
            current.consumers.discard(subscription)
            if current.consumers:
                return
            writer = BufferWriter()
            writer.write_u64_be(current.sub_id)
            response = parse_response(
                await self.request_frame(MSG_NOTICE_UNSUBSCRIBE, writer.build())
            )
            if not response.success:
                raise domain_error(
                    NoticeError, "UNSUBSCRIBE", response.error_code or 0, response.error
                )
            self._by_pattern.pop(pattern, None)
            self._pattern_by_id.pop(current.sub_id, None)

        subscription = AsyncSubscription(
            pattern, self.connection.config.limits.subscription_buffer_size, close
        )
        state.consumers.add(subscription)
        return subscription

    async def _subscribe_wire(self, pattern: str) -> int:
        writer = BufferWriter()
        writer.write_route(pattern)
        response = parse_response(await self.request_frame(MSG_NOTICE_SUBSCRIBE, writer.build()))
        if not response.success:
            raise domain_error(NoticeError, "SUBSCRIBE", response.error_code or 0, response.error)
        reader = BufferReader(response.data)
        if reader.read_u8() != 1:
            raise NoticeError("SUBSCRIBE response omitted its id", "INVALID_RESPONSE")
        return reader.read_u64_be()

    def _notify(self, payload: bytes) -> None:
        reader = BufferReader(payload)
        sub_id = reader.read_u64_be()
        notice = Notice(reader.read_route(), reader.read_bytes(reader.read_u32_be()))
        if not reader.is_eof():
            raise NoticeError("NOTICE_NOTIFY has trailing bytes", "INVALID_RESPONSE")
        pattern = self._pattern_by_id.get(sub_id)
        state = self._by_pattern.get(pattern) if pattern is not None else None
        if state is None:
            return
        dead = {consumer for consumer in state.consumers if not consumer.push(notice)}
        state.consumers.difference_update(dead)

    async def _restore(self) -> None:
        for pattern, state in list(self._by_pattern.items()):
            old_id = state.sub_id
            try:
                state.sub_id = await self._subscribe_wire(pattern)
            except BaseException as exc:
                self._by_pattern.pop(pattern, None)
                for consumer in state.consumers:
                    consumer.fail(exc)
                self.connection.report_restore_failure("notice", pattern, exc)
                continue
            self._pattern_by_id.pop(old_id, None)
            self._pattern_by_id[state.sub_id] = pattern
