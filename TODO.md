# Fitz Python World-Class TODO

You are working in fitz-py. The SDK is already close to the target bar, but the conformance harness still records partial outcomes and the repo still needs a final audit pass before it can be called world-class.

## Canonical Sources

- [../fitz/docs/clients/CLIENT_SPEC.md](../fitz/docs/clients/CLIENT_SPEC.md)
- [../fitz/docs/clients/CLIENT_ACCEPTANCE_CRITERIA.md](../fitz/docs/clients/CLIENT_ACCEPTANCE_CRITERIA.md)
- [../fitz/docs/clients/CLIENT_IMPLEMENTATION_GUIDE.md](../fitz/docs/clients/CLIENT_IMPLEMENTATION_GUIDE.md)
- [../fitz/docs/clients/CONNECTION_FLOW.md](../fitz/docs/clients/CONNECTION_FLOW.md)
- [README.md](README.md)
- [docs/README.md](docs/README.md)
- [tests/conformance/test_conformance.py](tests/conformance/test_conformance.py)
- [pyproject.toml](pyproject.toml)

## What Is Still Missing

- The conformance harness still marks several scenarios partial instead of proving them cleanly across the supported transports.
- Auth failure, disconnect, stream, queue, notice, and schedule edge cases still need stronger broker-backed proof.
- Reconnect and handle invalidation are implemented, but the remaining behavior needs to be validated against the real broker, not just unit fixtures.
- The release story should be tightened so the README, scripts, and conformance output describe the same bar.
- Keep route handling opaque and keep the current async-first API shape.

## Work In Order

1. Eliminate the remaining partial conformance results.
   - Make `CS-002` fail with a typed auth error on every supported transport/auth path, or update the harness so it no longer accepts a weaker silent-close result if the contract does not allow it.
   - Remove race-dependent partials from `CS-009`, `CS-011`, `CS-012`, `CS-016`, and `CS-018` by fixing the underlying behavior, not by loosening the assertions.
2. Audit lifecycle correctness end to end.
   - Prove disconnect listeners invalidate transactions and sessions immediately.
   - Prove reconnect restores subscriptions and worker registrations without leaking stale handles.
   - Prove shutdown during active work leaves the client in a clean, reusable closed state.
3. Tighten error and response mapping.
   - Keep the typed error hierarchy and status-code mapping exhaustive.
   - Make any remaining server-status or response-shape drift visible through tests.
4. Refresh docs and verification guidance.
   - Keep `README.md`, `docs/README.md`, and `pyproject.toml` scripts aligned with the actual release gate.
   - Ensure the conformance output and the repo docs use the same terminology and scenario counts.

## Concrete Gap Checklist

- `tests/conformance/test_conformance.py`: the remaining partial scenarios are the highest-signal backlog items.
- `src/fitz_py/connection.py`: prove the state machine, disconnect signaling, and reconnect behavior under the broker-backed scenarios.
- `src/fitz_py/errors.py`: keep the typed error mapping exhaustive and stable.
- `src/fitz_py/domains/kv.py` and `src/fitz_py/domains/stream.py`: continue to protect stateful handles from use-after-disconnect and other stale-state bugs.
- `README.md` and `docs/README.md`: keep the verification and parity story aligned with what the code and tests actually do.

## Definition Of Done

- `pytest tests/unit`, `pytest tests/integration -v`, and `pytest tests/conformance -v` are all green.
- The conformance run is free of avoidable `partial` verdicts.
- The README and local docs accurately describe the shipped surface and verification commands.

## Constraints

- Do not redesign the public API shape just to remove a test warning.
- Keep routes opaque and protocol semantics faithful to the Fitz contract.
- Prefer additive, non-breaking changes.
- Fix the behavior first; only relax a test if the canonical contract truly allows it.