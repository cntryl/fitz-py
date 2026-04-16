# fitz-py

`fitz-py` is the async-first Python SDK for Fitz.

## Install

```bash
python -m pip install cntryl-fitz
```

## Quick Start

```python
from fitz_py import Client, ClientConfig

client = Client(
    ClientConfig(
        url="ws://localhost:4190/ws",
        token_provider=lambda: "",
    )
)

await client.connect()

tx = await client.kv().begin("kv://example/app")
result = await tx.get(b"key")
await tx.rollback()

await client.close()
```

The package is `asyncio`-first and does not provide a synchronous wrapper.

## Parity Goals

`fitz-py` tracks the Fitz client behavior implemented in `fitz-go` and `fitz-ts`.
The Python SDK now exposes the same seven domains, typed Fitz/domain errors, retryability
helpers via `is_retryable()`, reconnect-aware subscription restoration, and extended
integration/conformance coverage for queue, lease, notice, and schedule lifecycle flows.

## Verification

Fast local checks:

```bash
python -m pip install -e ".[dev]"
python -m ruff check .
python -m pytest tests/unit
```

Hot-path microbenchmarks:

```bash
python artifacts/benchmarks/hotpath.py --iterations 10000
# or
hatch run bench-hotpath
```

One-shot verification:

```bash
hatch run verify
```

Broker-backed verification:

```bash
docker compose -f ../fitz-go/compose.yml up -d
python -m pytest tests/integration -v
CONFORMANCE_TRANSPORT=ws CONFORMANCE_AUTH_MODE=anonymous \
CONFORMANCE_OUTPUT=artifacts/conformance-results.json \
python -m pytest tests/conformance -v
docker compose -f ../fitz-go/compose.yml down --volumes
```

Package smoke verification:

```bash
python -m build
python -m pip install dist/*.whl
```

The conformance harness writes JSON results to `artifacts/conformance-results.json` by default.

## Project Layout

- `src/fitz_py`: package code
- `tests/unit`: fast unit coverage
- `tests/integration`: broker-backed integration coverage
- `tests/conformance`: release-gate conformance coverage

## Canonical Docs

Canonical client behavior is defined by the server-owned docs in the Fitz repository:

- [CLIENT_SPEC.md](../fitz/docs/clients/CLIENT_SPEC.md)
- [CLIENT_ACCEPTANCE_CRITERIA.md](../fitz/docs/clients/CLIENT_ACCEPTANCE_CRITERIA.md)
- [CLIENT_IMPLEMENTATION_GUIDE.md](../fitz/docs/clients/CLIENT_IMPLEMENTATION_GUIDE.md)
- [CONNECTION_FLOW.md](../fitz/docs/clients/CONNECTION_FLOW.md)

## Documentation

- [`docs/README.md`](docs/README.md)
- [`CLIENT_SPEC.md`](CLIENT_SPEC.md)
- [`CLIENT_ACCEPTANCE_CRITERIA.md`](CLIENT_ACCEPTANCE_CRITERIA.md)
