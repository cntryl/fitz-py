# Changelog

## Unreleased

- Requires the server commit-time persistence contract from cntryl/fitz#471; the old KV wire format is incompatible. Validated against `fitz:develop`.

- Breaking: move KV persistence from BEGIN to the required COMMIT argument. Stream COMMIT also requires an explicit Buffered or Sync choice. See MIGRATION.md for updated calls and wire compatibility.
