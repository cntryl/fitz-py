# fitz-py

`fitz-py` is an async-first Python client for Fitz.

## Status

This package is structured as a modern `src/`-layout Python project and exposes a canonical public surface:

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

## Public API

- `Client`
- `ClientConfig`
- `ConnectionState`
- Fitz/domain error types
- Canonical domain accessors:
  - `client.kv()`
  - `client.queue()`
  - `client.rpc()`
  - `client.notice()`
  - `client.lease()`
  - `client.stream()`
  - `client.schedule()`

The package is `asyncio`-first. It does not provide a synchronous wrapper.

## Project Layout

- `src/fitz_py`: package code
- `tests/unit`: fast unit coverage
- `tests/integration`: reserved for broker-backed acceptance tests

## Testing

```powershell
python -m pytest tests/unit
```

Broker-backed integration testing will use the same Fitz broker environment contract as the Go and TypeScript clients.

The required repo-local spec gate is the conformance suite:

```powershell
python -m pytest tests/conformance -v
```

## Canonical Spec

Canonical client behavior is defined by the server-owned docs in the Fitz repository:

- `fitz/docs/clients/CLIENT_SPEC.md`
- `fitz/docs/clients/CLIENT_ACCEPTANCE_CRITERIA.md`
- `fitz/docs/clients/CLIENT_IMPLEMENTATION_GUIDE.md`
