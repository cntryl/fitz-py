# Design notes

The public API is intentionally asyncio-native and compatibility-free. See the root README for
usage. The implementation follows these invariants:

- one serialized transport writer and FIFO per-message response queues;
- tombstones for timed-out or cancelled requests that were already transmitted;
- bounded request admission, handler dispatch, and subscription buffers;
- generation-bound transactional, queue, lease, stream, and response-writer handles;
- best-effort reconnect restoration with per-registration failure isolation;
- one broker subscription shared by multiple independent local async iterators;
- exact route-bearing wildcard results and notifications;
- cryptographically random 16-byte RPC correlation identifiers;
- no response shim for fire-and-forget Notice publish or RPC call submission.

The vendored conformance YAML in `tests/conformance` is copied from the canonical Fitz server
suite. Changes to client-wide behavior must update the canonical server documentation first.
