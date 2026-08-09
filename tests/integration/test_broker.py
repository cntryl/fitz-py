from __future__ import annotations

import asyncio

import pytest

from fitz_py import DeliveryMode, InboundRequest, KVDurability, ResponseWriter, RPCError
from tests.integration.fixture.fixture import IntegrationFixture, unique_route


@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("auth_mode", ["anonymous", "valid_jwt"])
@pytest.mark.asyncio
async def test_connect_and_kv_round_trip(transport: str, auth_mode: str) -> None:
    fixture = await IntegrationFixture.connect_or_fail(transport, auth_mode)  # type: ignore[arg-type]
    try:
        route = unique_route("kv")
        async with fixture.client.kv.transaction(route, durability=KVDurability.SYNC) as tx:
            await tx.put(b"key", b"value")
            assert await tx.get(b"key") == b"value"
            await tx.commit()
        async with fixture.client.kv.transaction(route, durability=KVDurability.BUFFERED) as tx:
            assert await tx.get(b"key") == b"value"
    finally:
        await fixture.aclose()


@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.asyncio
async def test_queue_round_trip(transport: str) -> None:
    fixture = await IntegrationFixture.connect_or_fail(transport, "anonymous")
    try:
        route = unique_route("queue")
        await fixture.client.queue.enqueue(route, b"work")
        items = await fixture.client.queue.reserve(route, lease=30, wait=2)
        assert [item.body for item in items] == [b"work"]
        await items[0].complete()
    finally:
        await fixture.aclose()


@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.asyncio
async def test_lease_notice_stream_rpc_and_schedule_workflows(transport: str) -> None:
    fixture = await IntegrationFixture.connect_or_fail(transport, "anonymous")
    client = fixture.client
    try:
        lease = await client.lease.acquire(unique_route("lease"), ttl=30)
        lease_info = await client.lease.query(lease.route)
        assert lease_info.owner is not None and lease_info.owner.endswith(lease.owner_id)
        await lease.extend(30)
        await lease.release()

        notice_route = unique_route("notice")
        async with client.notice.subscribe(notice_route) as notices:
            await client.notice.publish(notice_route, b"notice")
            assert (await asyncio.wait_for(anext(notices), 2)).body == b"notice"

        stream_route = unique_route("stream")
        session = await client.stream.begin(stream_route, ingest_metadata=b"")
        assert await session.append(0, b"event", metadata=b"", discriminator="") == 0
        await session.commit()
        assert [record.body for record in await client.stream.read(stream_route, 0)] == [b"event"]

        rpc_route = f"rpc://integration-realm/{unique_route('rpc').split('/')[-2]}/work"

        async def echo(request: InboundRequest, writer: ResponseWriter) -> None:
            await writer.send(request.body + b"!", end=True)

        async with (
            client.rpc.worker(rpc_route, echo),
            client.rpc.call(rpc_route, b"rpc") as responses,
        ):
            assert [frame.body async for frame in responses] == [b"rpc!"]

        failing_route = rpc_route.replace("/work", "/fail")

        async def fail(_request: InboundRequest, _writer: ResponseWriter) -> None:
            raise RuntimeError("worker failed")

        async with (
            client.rpc.worker(failing_route, fail),
            client.rpc.call(failing_route, b"rpc", timeout=2) as responses,
        ):
            with pytest.raises(RPCError, match="worker failed"):
                await anext(responses)

        schedule_route = unique_route("schedule")
        await client.schedule.create(
            schedule_route,
            "0 0 * * *",
            delivery_mode=DeliveryMode.SINGLE,
            payload=b"scheduled",
        )
        page = await client.schedule.list_schedules(limit=0)
        assert schedule_route in {entry.route for entry in page.entries}
        async with client.schedule.subscribe("schedule://integration-realm/**"):
            pass
        await client.schedule.cancel(schedule_route)
    finally:
        await fixture.aclose()
