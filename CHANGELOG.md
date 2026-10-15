# Changelog

## Unreleased

- Breaking: move KV persistence from BEGIN to the required COMMIT argument. Stream COMMIT also requires an explicit Buffered or Sync choice. See MIGRATION.md for updated calls and wire compatibility.
