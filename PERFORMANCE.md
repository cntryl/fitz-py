# Performance evidence

Performance results are evidence, not CI pass/fail thresholds. CI executes the pyperf suite in
smoke mode; comparable reports must be captured on the same idle runner and retain raw JSON.

Reference bars for release analysis are 5 microseconds for 256-byte codec paths, 25 microseconds
for multiplexer loopback, 100 microseconds for common domain loopbacks, and 250 microseconds for
100-consumer fan-out. Any statistically significant regression above 10% requires an explanation.

Run `python -m benchmarks.hotpath -o artifacts/benchmarks/hotpath.json` for codec encode/decode,
whole and fragmented parsing, multiplexer loopback, admission, and 100-consumer fan-out samples.
Use `--tracemalloc` for allocation evidence. CI runs the suite on every supported Python version
and uploads the raw JSON. This checkout does not claim broker latency numbers until repeated
p50/p95/p99 and throughput runs are captured on a named runner.

The 2026-08-08 local smoke on CPython 3.12.13/macOS arm64 measured 0.93 microseconds encode,
0.71 microseconds decode, 0.61 microseconds whole-frame parse, 33.5 microseconds fragmented parse,
43.3 microseconds multiplexer loopback, 0.36 microseconds admission, and 99.0 microseconds for
100-consumer fan-out. This fast-mode sample is not a before/after comparison. In particular, the
multiplexer result is above its 25-microsecond reference bar and remains an explicit optimization
target after correctness.
