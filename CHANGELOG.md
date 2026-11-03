# Changelog


## 0.4.0 (unreleased)

- Requires Fitz 0.4.0 / Wire 3 with bounded extension envelopes and numeric capability negotiation.
- Adds capability-gated per-message Queue TTL in hours and explicit Queue NACK with visibility delay.
- Preserves core domain semantics, response correlation, ephemeral sessions, and best-effort Queue persistence.
- Qualifies all SDKs against the latest broker container, recording its digest for qualification; no package publication or release tag.

## Unreleased

- Requires the server commit-time persistence contract from cntryl/fitz#471; the old KV wire format is incompatible. Validated against `fitz:develop`.

- Breaking: move KV persistence from BEGIN to the required COMMIT argument. Stream COMMIT also requires an explicit Buffered or Sync choice. See MIGRATION.md for updated calls and wire compatibility.
