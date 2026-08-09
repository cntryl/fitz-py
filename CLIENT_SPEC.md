# Canonical Client Spec

`fitz-py` does not define protocol behavior locally.

The canonical Fitz client specification lives in the server repository:

- `../../fitz/docs/clients/spec/`
- `../../fitz/docs/clients/cross-language-conformance-suite.yaml`
- `../../fitz/docs/clients/client-perfection-scoreboard.md`

Use `fitz-ts` as the forward implementation reference and cross-check wire-sensitive behavior
against the current broker source. When prose and the running codec disagree, record the drift and
follow the current broker contract.
