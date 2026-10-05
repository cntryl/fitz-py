from __future__ import annotations

import asyncio

import pytest

from fitz_py import FitzTimeoutError, InboundRequest, ResponseWriter, RPCError
from tests.integration.fixture.fixture import IntegrationFixture, unique_route


@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("auth_mode", ["anonymous", "valid_jwt"])
@pytest.mark.parametrize("reason", ["cancel", "deadline"])
@pytest.mark.asyncio
async def test_rpc_sdk_chain_propagates_cancellation_and_inherited_deadline(
    transport: str, auth_mode: str, reason: str
) -> None:
    a, b, c = await asyncio.gather(
        *(IntegrationFixture.connect_or_fail(transport, auth_mode) for _ in range(3))  # type: ignore[arg-type]
    )
    middle, leaf = unique_route("rpc"), unique_route("rpc")
    started, middle_cleaned, leaf_cleaned = (asyncio.Event() for _ in range(3))
    unrelated_started, unrelated_release = asyncio.Event(), asyncio.Event()
    unrelated_contexts = []
    budgets: dict[str, int | None] = {}

    async def leaf_handler(request: InboundRequest, writer: ResponseWriter) -> None:
        assert request.context is not None
        if request.body == b"unrelated":
            unrelated_contexts.append(request.context)
            unrelated_started.set()
            await unrelated_release.wait()
            await writer.send(request.body, end=True)
        elif request.body == b"probe":
            await writer.send(request.body, end=True)
        else:
            budgets["leaf"] = request.context.remaining_time_ms()
            started.set()
            await request.context.cancelled.wait()
            leaf_cleaned.set()

    async def middle_handler(request: InboundRequest, writer: ResponseWriter) -> None:
        assert request.context is not None
        budgets["parent"] = request.context.remaining_time_ms()
        try:
            async with b.client.rpc.call_from_request(request, leaf, request.body) as child:
                async for frame in child:
                    await writer.send(frame.body)
                await writer.send(b"", end=True)
        finally:
            middle_cleaned.set()

    try:
        async with (
            c.client.rpc.worker(leaf, leaf_handler, max_concurrency=2),
            b.client.rpc.worker(middle, middle_handler),
            a.client.rpc.call(leaf, b"unrelated", timeout=10) as unrelated,
        ):
            await asyncio.wait_for(unrelated_started.wait(), 2)
            async with a.client.rpc.call(
                middle, b"target", timeout=0.75 if reason == "deadline" else 10
            ) as call:
                await asyncio.wait_for(started.wait(), 2)
                parent, child = budgets["parent"], budgets["leaf"]
                assert parent is not None and child is not None
                assert 0 < child <= parent
                if reason == "cancel":
                    assert await call.cancel() == "forwarded"
                else:
                    with pytest.raises((FitzTimeoutError, RPCError), match="timed out"):
                        await anext(call)
                await asyncio.wait_for(
                    asyncio.gather(middle_cleaned.wait(), leaf_cleaned.wait()), 3
                )
                assert not unrelated_contexts[0].cancelled.is_set()
            unrelated_release.set()
            assert (await anext(unrelated)).body == b"unrelated"
            async with a.client.rpc.call(middle, b"probe", timeout=2) as probe:
                assert (await anext(probe)).body == b"probe"
    finally:
        unrelated_release.set()
        await asyncio.gather(a.aclose(), b.aclose(), c.aclose())
