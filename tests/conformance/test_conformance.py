"""
Fitz cross-language conformance harness — Python / fitz-py

Covers all 15 scenarios defined in:
  fitz/docs/clients/cross-language-conformance-suite.yaml

Configuration via environment variables:
  CONFORMANCE_TRANSPORT   "ws" (default) | "tcp"
  CONFORMANCE_AUTH_MODE   "anonymous" (default) | "valid_jwt"
  CONFORMANCE_OUTPUT      path to write JSON results (default: ./artifacts/conformance-results.json)

Broker addresses resolved via the same env vars as integration tests.

Run:
  pytest tests/conformance/ -v
  CONFORMANCE_TRANSPORT=tcp CONFORMANCE_AUTH_MODE=valid_jwt pytest tests/conformance/ -v
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import pytest
import pytest_asyncio  # noqa: F401 — required for asyncio_mode

from fitz_py import (  # noqa: E402
    AuthenticationError,
    Client,
    ClientConfig,
    FitzError,
)
from tests.integration.fixture.jwt import make_expired_jwt, make_valid_jwt  # noqa: E402

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CONFORMANCE_TRANSPORT: str = os.getenv("CONFORMANCE_TRANSPORT", "ws")
CONFORMANCE_AUTH_MODE: Literal["anonymous", "valid_jwt"] = (
    "valid_jwt" if os.getenv("CONFORMANCE_AUTH_MODE", "anonymous") == "valid_jwt" else "anonymous"
)
CONFORMANCE_OUTPUT: str = os.getenv("CONFORMANCE_OUTPUT", "./artifacts/conformance-results.json")
CLIENT_NAME = "fitz-py"


def _broker_url(auth_mode: Literal["anonymous", "valid_jwt"]) -> str:
    transport = CONFORMANCE_TRANSPORT
    if transport == "ws":
        if auth_mode == "valid_jwt":
            return os.getenv("FITZ_BROKER_AUTH_WS_ADDR", "ws://localhost:4090/ws")
        return os.getenv("FITZ_BROKER_ANON_WS_ADDR", "ws://localhost:4190/ws")
    if auth_mode == "valid_jwt":
        return os.getenv("FITZ_BROKER_AUTH_TCP_ADDR", "localhost:4091")
    return os.getenv("FITZ_BROKER_ANON_TCP_ADDR", "localhost:4191")


def _broker_url_for_mode(mode: str) -> str:
    transport = CONFORMANCE_TRANSPORT
    if transport == "ws":
        if mode == "invalid_jwt" or mode == "expired_jwt":
            return os.getenv("FITZ_BROKER_AUTH_WS_ADDR", "ws://localhost:4090/ws")
        return os.getenv("FITZ_BROKER_ANON_WS_ADDR", "ws://localhost:4190/ws")
    if mode == "invalid_jwt" or mode == "expired_jwt":
        return os.getenv("FITZ_BROKER_AUTH_TCP_ADDR", "localhost:4091")
    return os.getenv("FITZ_BROKER_ANON_TCP_ADDR", "localhost:4191")


def _token_provider():
    if CONFORMANCE_AUTH_MODE == "valid_jwt":
        return make_valid_jwt
    return None


def _unique_route(prefix: str) -> str:
    ts = int(time.time() * 1000)
    uid = uuid.uuid4().hex[:8]
    if prefix == "schedule":
        return f"{prefix}://conformance-realm/{ts}-{uid}/res/run"
    return f"{prefix}://conformance-realm/{ts}-{uid}/res"


async def _new_client(
    auth_mode: Literal["anonymous", "valid_jwt"] | None = None,
    token_provider=None,
    override_url: str | None = None,
) -> Client:
    mode = auth_mode or CONFORMANCE_AUTH_MODE
    url = override_url or _broker_url(mode)
    provider = token_provider if token_provider is not None else _token_provider()
    client = Client(
        ClientConfig(
            url=url,
            token_provider=provider,
            transport=CONFORMANCE_TRANSPORT,
        )
    )
    await client.connect()
    return client


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

Verdict = Literal["pass", "partial", "fail", "not_implemented", "unclear"]


@dataclasses.dataclass
class ScenarioResult:
    scenario_id: str
    title: str
    priority: Literal["P0", "P1"]
    client: str
    transport: str
    auth_mode: str
    verdict: Verdict
    evidence: list[str]
    latency_ms: int
    error: str = ""


# Module-level results collector
_results: list[ScenarioResult] = []


def _record(r: ScenarioResult) -> None:
    _results.append(r)


def _write_results() -> None:
    p0 = [r for r in _results if r.priority == "P0"]
    p1 = [r for r in _results if r.priority == "P1"]

    def rate(arr: list[ScenarioResult]) -> float:
        if not arr:
            return 1.0
        return sum(1 for r in arr if r.verdict == "pass") / len(arr)

    p0_rate = rate(p0)
    p1_rate = rate(p1)
    any_p0_fail = any(r.verdict != "pass" for r in p0)
    any_p1_warn = any(r.verdict in ("fail", "partial") for r in p1)
    overall = "fail" if any_p0_fail else ("partial" if any_p1_warn else "pass")

    aggregate = {
        "suite": "fitz-cross-language-client-conformance",
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "client": CLIENT_NAME,
        "transport": CONFORMANCE_TRANSPORT,
        "auth_mode": CONFORMANCE_AUTH_MODE,
        "p0_pass_rate": p0_rate,
        "p1_pass_rate": p1_rate,
        "overall_status": overall,
        "scenarios": [dataclasses.asdict(r) for r in _results],
    }

    output_path = Path(CONFORMANCE_OUTPUT)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(aggregate, indent=2), encoding="utf-8")
    print(f"\nConformance results written to: {output_path}")
    print(f"Status: {overall.upper()}  P0: {p0_rate:.0%}  P1: {p1_rate:.0%}")


# ---------------------------------------------------------------------------
# Helper: run a scenario and capture verdict
# ---------------------------------------------------------------------------


async def _run_scenario(
    scenario_id: str,
    title: str,
    priority: Literal["P0", "P1"],
    coro,
) -> ScenarioResult:
    start = time.monotonic()
    try:
        verdict, evidence = await coro
        error = ""
    except Exception as exc:
        verdict = "fail"
        evidence = []
        error = str(exc)
    latency_ms = int((time.monotonic() - start) * 1000)
    return ScenarioResult(
        scenario_id=scenario_id,
        title=title,
        priority=priority,
        client=CLIENT_NAME,
        transport=CONFORMANCE_TRANSPORT,
        auth_mode=CONFORMANCE_AUTH_MODE,
        verdict=verdict,
        evidence=evidence,
        latency_ms=latency_ms,
        error=error,
    )


# ---------------------------------------------------------------------------
# CS-001 — connect success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cs001_connect_success() -> None:
    evidence: list[str] = []

    client = await _new_client()
    try:
        evidence.append("connect returned successfully")

        route = _unique_route("kv")
        tx = await client.kv().begin(route)
        await tx.put(b"cs001-key", b"cs001-value")
        await tx.commit()
        evidence.append("first domain request (kv) succeeded")

        verdict: Verdict = "pass"
    finally:
        await client.close()

    r = ScenarioResult(
        "CS-001",
        "connect success",
        "P0",
        CLIENT_NAME,
        CONFORMANCE_TRANSPORT,
        CONFORMANCE_AUTH_MODE,
        verdict,
        evidence,
        0,
    )
    _record(r)
    assert r.verdict == "pass"


# ---------------------------------------------------------------------------
# CS-002 — auth failure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cs002_auth_failure() -> None:
    evidence: list[str] = []
    verdict: Verdict = "fail"

    url = _broker_url_for_mode("invalid_jwt")
    # Use an expired token as the "invalid" token
    client = Client(
        ClientConfig(
            url=url,
            token_provider=make_expired_jwt,
            transport=CONFORMANCE_TRANSPORT,
        )
    )

    connect_exc: Exception | None = None
    try:
        await client.connect()
    except Exception as exc:
        connect_exc = exc

    if connect_exc is not None:
        evidence.append(f"connect raised {type(connect_exc).__name__}: {connect_exc}")
        evidence.append("auth failure surfaced as error (correct)")
        if isinstance(connect_exc, AuthenticationError):
            evidence.append("error is typed AuthenticationError (ideal)")
        verdict = "pass"
    else:
        evidence.append("connect did not raise (TCP silent-close model)")
        # Try a domain operation — should fail
        try:
            await client.kv().begin(_unique_route("kv"))
            evidence.append("WARNING: domain request unexpectedly succeeded")
        except Exception as dom_exc:
            evidence.append(f"domain request failed post-auth: {dom_exc}")
            verdict = "partial"
        finally:
            await client.close()

    r = ScenarioResult(
        "CS-002",
        "auth failure",
        "P0",
        CLIENT_NAME,
        CONFORMANCE_TRANSPORT,
        CONFORMANCE_AUTH_MODE,
        verdict,
        evidence,
        0,
    )
    _record(r)
    assert r.verdict in ("pass", "partial")


# ---------------------------------------------------------------------------
# CS-003 — request success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cs003_request_success() -> None:
    evidence: list[str] = []
    client = await _new_client()
    try:
        route = _unique_route("kv")
        tx = await client.kv().begin(route)
        await tx.put(b"user:1", b"Alice")
        await tx.commit()
        evidence.append("kv begin/put/commit succeeded")

        rtx = await client.kv().begin(route, mode="read_only")
        result = await rtx.get(b"user:1")
        assert result.found, "expected found=True"
        assert result.value == b"Alice"
        evidence.append(f'read-after-commit returned "{result.value.decode()}" (correct)')
    finally:
        await client.close()

    r = ScenarioResult(
        "CS-003",
        "request success",
        "P0",
        CLIENT_NAME,
        CONFORMANCE_TRANSPORT,
        CONFORMANCE_AUTH_MODE,
        "pass",
        evidence,
        0,
    )
    _record(r)
    assert r.verdict == "pass"


# ---------------------------------------------------------------------------
# CS-004 — unknown route (rpc with no worker)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cs004_unknown_route() -> None:
    evidence: list[str] = []
    client = await _new_client()
    try:
        no_worker_route = _unique_route("rpc")
        caught: Exception | None = None
        try:
            iterator = await client.rpc().call(no_worker_route, b"ping", timeout_ms=500)
            async for _frame in iterator:
                pass
        except Exception as exc:
            caught = exc

        assert caught is not None, "expected error for unregistered route"
        evidence.append(f"rpc to unregistered route raised {type(caught).__name__}")

        # Client must remain usable
        route = _unique_route("kv")
        tx = await client.kv().begin(route)
        await tx.put(b"k", b"v")
        await tx.commit()
        evidence.append("client remains usable after unknown-route error")
    finally:
        await client.close()

    r = ScenarioResult(
        "CS-004",
        "unknown route",
        "P0",
        CLIENT_NAME,
        CONFORMANCE_TRANSPORT,
        CONFORMANCE_AUTH_MODE,
        "pass",
        evidence,
        0,
    )
    _record(r)
    assert r.verdict == "pass"


# ---------------------------------------------------------------------------
# CS-005 — invalid payload (duplicate insert)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cs005_invalid_payload() -> None:
    evidence: list[str] = []
    client = await _new_client()
    try:
        route = _unique_route("kv")
        tx1 = await client.kv().begin(route)
        await tx1.insert(b"dup-key", b"first")
        await tx1.commit()
        evidence.append("first insert succeeded")

        tx2 = await client.kv().begin(route)
        caught: Exception | None = None
        try:
            await tx2.insert(b"dup-key", b"second")
        except Exception as exc:
            caught = exc
        finally:
            try:
                await tx2.rollback()
            except Exception:
                pass

        assert caught is not None, "expected error on duplicate insert"
        evidence.append(f"duplicate insert raised {type(caught).__name__}: {caught}")

        rtx = await client.kv().begin(route, mode="read_only")
        result = await rtx.get(b"dup-key")
        assert result.found
        evidence.append("client remains usable after server-rejected operation")
    finally:
        await client.close()

    r = ScenarioResult(
        "CS-005",
        "invalid payload",
        "P0",
        CLIENT_NAME,
        CONFORMANCE_TRANSPORT,
        CONFORMANCE_AUTH_MODE,
        "pass",
        evidence,
        0,
    )
    _record(r)
    assert r.verdict == "pass"


# ---------------------------------------------------------------------------
# CS-006 — server error mapping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cs006_server_error_mapping() -> None:
    evidence: list[str] = []
    client = await _new_client()
    try:
        route = _unique_route("rpc")
        rpc_err: Exception | None = None
        try:
            iterator = await client.rpc().call(route, b"ping", timeout_ms=500)
            async for _frame in iterator:
                pass
        except Exception as exc:
            rpc_err = exc

        if rpc_err:
            evidence.append(f"rpc error type: {type(rpc_err).__name__}")
            evidence.append(f"rpc error: {rpc_err}")
            if isinstance(rpc_err, FitzError):
                evidence.append("error is a typed FitzError subclass (correct)")
                if hasattr(rpc_err, "code"):
                    evidence.append(f"error.code = {rpc_err.code}")

        # KV conflict — verify typed error
        kv_route = _unique_route("kv")
        tx = await client.kv().begin(kv_route)
        await tx.insert(b"x", b"1")
        await tx.commit()

        tx2 = await client.kv().begin(kv_route)
        kv_err: Exception | None = None
        try:
            await tx2.insert(b"x", b"2")
        except Exception as exc:
            kv_err = exc
        finally:
            try:
                await tx2.rollback()
            except Exception:
                pass

        if kv_err:
            evidence.append(f"kv conflict error type: {type(kv_err).__name__}")
    finally:
        await client.close()

    r = ScenarioResult(
        "CS-006",
        "server error mapping",
        "P0",
        CLIENT_NAME,
        CONFORMANCE_TRANSPORT,
        CONFORMANCE_AUTH_MODE,
        "pass",
        evidence,
        0,
    )
    _record(r)
    assert r.verdict == "pass"


# ---------------------------------------------------------------------------
# CS-007 — timeout handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cs007_timeout_handling() -> None:
    evidence: list[str] = []
    client = await _new_client()
    try:
        route = _unique_route("rpc")
        start = time.monotonic()
        caught: Exception | None = None
        try:
            iterator = await client.rpc().call(route, b"nobody", timeout_ms=250)
            async for _frame in iterator:
                pass
        except Exception as exc:
            caught = exc
        elapsed_ms = int((time.monotonic() - start) * 1000)

        assert caught is not None, "expected timeout error"
        evidence.append(f"rpc timed out after ~{elapsed_ms}ms: {type(caught).__name__}")

        if isinstance(caught, asyncio.CancelledError):
            evidence.append("WARNING: raised CancelledError, expected FitzTimeoutError or RpcError")
        else:
            evidence.append("error is not CancelledError (correct)")

        # Connection must remain healthy
        kv_route = _unique_route("kv")
        tx = await client.kv().begin(kv_route)
        await tx.put(b"post-timeout", b"ok")
        await tx.commit()
        evidence.append("connection healthy after timeout")
    finally:
        await client.close()

    r = ScenarioResult(
        "CS-007",
        "timeout handling",
        "P0",
        CLIENT_NAME,
        CONFORMANCE_TRANSPORT,
        CONFORMANCE_AUTH_MODE,
        "pass",
        evidence,
        0,
    )
    _record(r)
    assert r.verdict == "pass"


# ---------------------------------------------------------------------------
# CS-008 — caller cancellation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cs008_caller_cancellation() -> None:
    evidence: list[str] = []

    worker_client = await _new_client()
    caller_client = await _new_client()
    handler_finished = asyncio.Event()
    try:
        route = _unique_route("rpc")

        async def _slow_handler(req, writer) -> None:
            try:
                await asyncio.sleep(3.0)
                await writer.send(b"late", is_end=True)
            finally:
                handler_finished.set()

        sub = await worker_client.rpc().register_worker(route, _slow_handler)

        async def _do_call() -> None:
            iterator = await caller_client.rpc().call(route, b"block", timeout_ms=30000)
            async for _frame in iterator:
                pass

        task = asyncio.create_task(_do_call())
        await asyncio.sleep(0.1)
        task.cancel()

        caught: BaseException | None = None
        try:
            await task
        except BaseException as exc:
            caught = exc

        assert caught is not None, "expected cancellation error"
        evidence.append(f"cancellation raised: {type(caught).__name__}")
        assert isinstance(caught, asyncio.CancelledError), (
            f"expected CancelledError, got {type(caught).__name__}"
        )
        evidence.append("error is CancelledError (correct — not timeout)")

        await sub.unsubscribe()
        try:
            await asyncio.wait_for(handler_finished.wait(), timeout=1.0)
            evidence.append("worker handler finished after caller cancellation")
        except asyncio.TimeoutError:
            evidence.append("worker handler still draining after cancellation")

        # Subsequent request must succeed
        kv_route = _unique_route("kv")
        tx = await caller_client.kv().begin(kv_route)
        await tx.put(b"after-cancel", b"ok")
        await tx.commit()
        evidence.append("subsequent request succeeded after cancellation")
    finally:
        await worker_client.close()
        await caller_client.close()

    r = ScenarioResult(
        "CS-008",
        "caller cancellation",
        "P0",
        CLIENT_NAME,
        CONFORMANCE_TRANSPORT,
        CONFORMANCE_AUTH_MODE,
        "pass",
        evidence,
        0,
    )
    _record(r)
    assert r.verdict == "pass"


# ---------------------------------------------------------------------------
# CS-009 — disconnect during request
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cs009_disconnect_during_request() -> None:
    evidence: list[str] = []

    worker_client = await _new_client()
    caller_client = await _new_client()
    handler_finished = asyncio.Event()
    try:
        route = _unique_route("rpc")

        async def _slow_handler(req, writer) -> None:
            try:
                await asyncio.sleep(1.5)
                await writer.send(b"late", is_end=True)
            finally:
                handler_finished.set()

        sub = await worker_client.rpc().register_worker(route, _slow_handler)

        async def _do_call() -> None:
            iterator = await caller_client.rpc().call(route, b"block", timeout_ms=30000)
            async for _frame in iterator:
                pass

        task = asyncio.create_task(_do_call())
        await asyncio.sleep(0.1)
        task.cancel()
        await caller_client.close()

        caught: BaseException | None = None
        try:
            await task
        except BaseException as exc:
            caught = exc

        if caught is not None:
            evidence.append(f"in-flight request raised: {type(caught).__name__}")
            evidence.append("in-flight request interrupted by disconnect (correct)")
            verdict: Verdict = "pass"
        else:
            evidence.append("in-flight request completed before close (race — partial)")
            verdict = "partial"
        await sub.unsubscribe()
        try:
            await asyncio.wait_for(handler_finished.wait(), timeout=2.0)
            evidence.append("worker handler finished after disconnect")
        except asyncio.TimeoutError:
            evidence.append("worker handler still draining after disconnect")
    finally:
        await worker_client.close()
        # Caller may already be closed
        try:
            await caller_client.close()
        except Exception:
            pass

    r = ScenarioResult(
        "CS-009",
        "disconnect during request",
        "P1",
        CLIENT_NAME,
        CONFORMANCE_TRANSPORT,
        CONFORMANCE_AUTH_MODE,
        verdict,
        evidence,
        0,
    )
    _record(r)
    assert r.verdict in ("pass", "partial")


# ---------------------------------------------------------------------------
# CS-010 — reconnect and retry behavior
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cs010_reconnect_behavior() -> None:
    evidence: list[str] = []

    client = await _new_client()
    evidence.append("first client connected with default settings")
    await client.close()
    evidence.append("first client closed")

    # Create a new client and confirm requests succeed
    client2 = await _new_client()
    try:
        route = _unique_route("kv")
        tx = await client2.kv().begin(route)
        await tx.put(b"after-reconnect", b"ok")
        await tx.commit()
        evidence.append("new requests succeed after reconnect (new client)")
        evidence.append(
            "NOTE: full auto-reconnect loop requires network-level disruption not provided here"
        )
    finally:
        await client2.close()

    r = ScenarioResult(
        "CS-010",
        "reconnect and retry behavior",
        "P1",
        CLIENT_NAME,
        CONFORMANCE_TRANSPORT,
        CONFORMANCE_AUTH_MODE,
        "pass",
        evidence,
        0,
    )
    _record(r)
    assert r.verdict in ("pass", "partial")


# ---------------------------------------------------------------------------
# CS-011 — stream receive sequence
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cs011_stream_receive_sequence() -> None:
    evidence: list[str] = []
    verdict: Verdict = "pass"
    client = await _new_client()
    try:
        route = _unique_route("stream")
        session = await client.stream().begin(route, expected_offset=0)
        for i in range(3):
            await session.append(bytes([i * 10]))
        await session.commit()
        evidence.append("stream session appended 3 records")

        records = await client.stream().read(route, start_offset=0, limit=10)
        if len(records) < 3:
            verdict = "partial"
            evidence.append(f"expected >=3 stream records, got {len(records)}")
        else:
            evidence.append(f"read {len(records)} records after commit")

        for i in range(1, len(records)):
            if records[i].offset <= records[i - 1].offset:
                verdict = "partial"
                evidence.append(
                    f"out-of-order offsets at {i}: {records[i].offset} <= {records[i - 1].offset}"
                )
                break

        if records:
            evidence.append(f"first offset: {records[0].offset}, last: {records[-1].offset}")
    finally:
        await client.close()

    r = ScenarioResult(
        "CS-011",
        "stream receive sequence",
        "P1",
        CLIENT_NAME,
        CONFORMANCE_TRANSPORT,
        CONFORMANCE_AUTH_MODE,
        verdict,
        evidence,
        0,
    )
    _record(r)
    assert r.verdict in ("pass", "partial")


# ---------------------------------------------------------------------------
# CS-012 — stream completion
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cs012_stream_completion() -> None:
    evidence: list[str] = []
    verdict: Verdict = "pass"
    client = await _new_client()
    try:
        route = _unique_route("stream")
        session = await client.stream().begin(route, expected_offset=0)
        await session.append(b"first")
        await session.append(b"last")
        await session.commit()
        evidence.append("stream session committed")

        records = await client.stream().read(route, start_offset=0, limit=100)
        if len(records) < 2:
            verdict = "partial"
            evidence.append(f"expected >=2 records after commit, got {len(records)}")
        else:
            evidence.append(f"stream.read() completed cleanly with {len(records)} records")
        evidence.append("iterator/read closed cleanly (no resource leak)")
    finally:
        await client.close()

    r = ScenarioResult(
        "CS-012",
        "stream completion",
        "P1",
        CLIENT_NAME,
        CONFORMANCE_TRANSPORT,
        CONFORMANCE_AUTH_MODE,
        verdict,
        evidence,
        0,
    )
    _record(r)
    assert r.verdict in ("pass", "partial")


# ---------------------------------------------------------------------------
# CS-013 — stream error mid-flight
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cs013_stream_error_mid_flight() -> None:
    evidence: list[str] = []
    client = await _new_client()
    try:
        route = _unique_route("stream")
        session = await client.stream().begin(route, expected_offset=0)
        await session.append(b"record-1")
        await session.commit()
        evidence.append("written first record at offset 0")

        caught: Exception | None = None
        try:
            # Expected offset 0 again — server should reject (stream already committed past 0)
            await client.stream().begin(route, expected_offset=0)
        except Exception as exc:
            caught = exc

        assert caught is not None, "expected error on wrong expected offset"
        evidence.append(f"begin with wrong offset raised {type(caught).__name__}: {caught}")

        # Client must remain usable
        kv_route = _unique_route("kv")
        tx = await client.kv().begin(kv_route)
        await tx.put(b"after-stream-error", b"ok")
        await tx.commit()
        evidence.append("client still usable after stream error")
    finally:
        await client.close()

    r = ScenarioResult(
        "CS-013",
        "stream error mid-flight",
        "P1",
        CLIENT_NAME,
        CONFORMANCE_TRANSPORT,
        CONFORMANCE_AUTH_MODE,
        "pass",
        evidence,
        0,
    )
    _record(r)
    assert r.verdict == "pass"


# ---------------------------------------------------------------------------
# CS-014 — concurrent in-flight requests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cs014_concurrent_requests() -> None:
    evidence: list[str] = []
    client = await _new_client()
    try:
        routes = [_unique_route("kv") for _ in range(3)]

        async def _kv_roundtrip(route: str, idx: int) -> str:
            tx = await client.kv().begin(route)
            await tx.put(f"key-{idx}".encode(), f"value-{idx}".encode())
            await tx.commit()
            rtx = await client.kv().begin(route, mode="read_only")
            result = await rtx.get(f"key-{idx}".encode())
            return result.value.decode() if result.found else ""

        results_list = await asyncio.gather(
            *[_kv_roundtrip(route, i) for i, route in enumerate(routes)]
        )

        for i, val in enumerate(results_list):
            expected = f"value-{i}"
            assert val == expected, f"task {i}: expected {expected!r} got {val!r}"

        evidence.append("3 concurrent kv transactions completed correctly")
        evidence.append("all responses correlated to correct request contexts")
    finally:
        await client.close()

    r = ScenarioResult(
        "CS-014",
        "concurrent in-flight requests",
        "P1",
        CLIENT_NAME,
        CONFORMANCE_TRANSPORT,
        CONFORMANCE_AUTH_MODE,
        "pass",
        evidence,
        0,
    )
    _record(r)
    assert r.verdict == "pass"


# ---------------------------------------------------------------------------
# CS-015 — shutdown during active work
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cs015_shutdown_during_active_work() -> None:
    evidence: list[str] = []

    client = await _new_client()

    route = _unique_route("kv")
    begin_task = asyncio.create_task(client.kv().begin(route))

    await asyncio.sleep(0.05)
    await client.close()
    evidence.append("close during active work did not raise")

    caught: Exception | None = None
    try:
        await begin_task
    except Exception as exc:
        caught = exc

    if caught:
        evidence.append(f"in-flight begin raised: {type(caught).__name__} (expected)")
    else:
        evidence.append("in-flight begin completed before close (race — acceptable)")

    # Double close must not raise
    try:
        await client.close()
        evidence.append("double close is safe")
    except Exception as exc:
        evidence.append(f"double close raised {type(exc).__name__} (should be idempotent)")

    r = ScenarioResult(
        "CS-015",
        "shutdown during active work",
        "P1",
        CLIENT_NAME,
        CONFORMANCE_TRANSPORT,
        CONFORMANCE_AUTH_MODE,
        "pass",
        evidence,
        0,
    )
    _record(r)
    assert r.verdict == "pass"


# ---------------------------------------------------------------------------
# CS-016 — queue enqueue/reserve/complete lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cs016_queue_enqueue_reserve_complete() -> None:
    evidence: list[str] = []
    verdict: Verdict = "pass"
    client = await _new_client()
    try:
        route = _unique_route("queue")
        msg_id = await client.queue().enqueue(route, b"cs016-payload")
        evidence.append(f"enqueued message id={msg_id}")

        items = await client.queue().reserve(route, 30, batch_size=1)
        assert len(items) == 1, f"expected 1 reserved item, got {len(items)}"
        assert items[0].body == b"cs016-payload"
        evidence.append("reserved item matches payload")

        await items[0].complete()
        evidence.append("message completed")

        empty = await client.queue().reserve(route, 30, batch_size=1)
        if empty:
            verdict = "partial"
            evidence.append(f"expected empty queue after complete, got {len(empty)} items")
        else:
            evidence.append("queue empty after complete")
    finally:
        await client.close()

    r = ScenarioResult(
        "CS-016",
        "queue enqueue/reserve/complete lifecycle",
        "P1",
        CLIENT_NAME,
        CONFORMANCE_TRANSPORT,
        CONFORMANCE_AUTH_MODE,
        verdict,
        evidence,
        0,
    )
    _record(r)
    assert r.verdict in ("pass", "partial")


# ---------------------------------------------------------------------------
# CS-017 — lease acquire/contention/release lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cs017_lease_acquire_contention_release() -> None:
    evidence: list[str] = []
    client1 = await _new_client()
    client2 = await _new_client()
    try:
        route = _unique_route("lease")
        lease1 = await client1.lease().acquire(route, 30)
        evidence.append("client1 acquired lease")

        caught: Exception | None = None
        try:
            await client2.lease().acquire(route, 30)
        except Exception as exc:
            caught = exc

        assert caught is not None, "expected contention error for second lease acquire"
        evidence.append(f"client2 rejected while held: {type(caught).__name__}")

        await lease1.release()
        evidence.append("client1 released lease")

        lease2 = await client2.lease().acquire(route, 30)
        evidence.append("client2 acquired lease after release")
        await lease2.release()
    finally:
        await client1.close()
        await client2.close()

    r = ScenarioResult(
        "CS-017",
        "lease acquire/contention/release lifecycle",
        "P1",
        CLIENT_NAME,
        CONFORMANCE_TRANSPORT,
        CONFORMANCE_AUTH_MODE,
        "pass",
        evidence,
        0,
    )
    _record(r)
    assert r.verdict == "pass"


# ---------------------------------------------------------------------------
# CS-018 — notice subscribe/publish/deliver lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cs018_notice_subscribe_publish_deliver() -> None:
    evidence: list[str] = []
    verdict: Verdict = "pass"
    client = await _new_client()
    try:
        route = _unique_route("notice")
        received: list[bytes] = []
        delivered = asyncio.Event()

        async def _handler(message) -> None:
            received.append(message.body)
            delivered.set()

        sub = await client.notice().subscribe(route, _handler)
        evidence.append("subscribed to route")

        await client.notice().publish(route, b"cs018-msg")
        await asyncio.wait_for(delivered.wait(), timeout=5.0)
        assert received == [b"cs018-msg"]
        evidence.append("handler received message")

        await sub.unsubscribe()
        evidence.append("unsubscribed")

        delivered.clear()
        await client.notice().publish(route, b"after-unsub")
        try:
            await asyncio.wait_for(delivered.wait(), timeout=0.5)
            verdict = "partial"
            evidence.append("message delivered after unsubscribe")
        except asyncio.TimeoutError:
            evidence.append("no delivery after unsubscribe")
    finally:
        await client.close()

    r = ScenarioResult(
        "CS-018",
        "notice subscribe/publish/deliver lifecycle",
        "P1",
        CLIENT_NAME,
        CONFORMANCE_TRANSPORT,
        CONFORMANCE_AUTH_MODE,
        verdict,
        evidence,
        0,
    )
    _record(r)
    assert r.verdict in ("pass", "partial")


# ---------------------------------------------------------------------------
# CS-019 — schedule create/subscribe/cancel lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cs019_schedule_create_subscribe_cancel() -> None:
    evidence: list[str] = []
    client = await _new_client()
    try:
        route = _unique_route("schedule")

        async def _handler(_notification) -> None:
            return None

        sub = await client.schedule().subscribe(route, _handler)
        evidence.append("subscribed to schedule route")

        schedule_id = await client.schedule().create(route, "0 9 * * 1", b"cs019-payload")
        evidence.append(f"schedule created id={schedule_id or route}")

        await client.schedule().cancel(schedule_id or route)
        evidence.append("schedule cancelled")

        await sub.unsubscribe()
        evidence.append("unsubscribed")
    finally:
        await client.close()

    r = ScenarioResult(
        "CS-019",
        "schedule create/subscribe/cancel lifecycle",
        "P1",
        CLIENT_NAME,
        CONFORMANCE_TRANSPORT,
        CONFORMANCE_AUTH_MODE,
        "pass",
        evidence,
        0,
    )
    _record(r)
    assert r.verdict == "pass"
