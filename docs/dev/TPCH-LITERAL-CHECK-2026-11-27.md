# TPC-H check of integer literal plan reuse

## Scope and answer checks

[Run 46032353197](https://github.com/C0fuge/kestrel/actions/runs/46032353197), revision
`1c83b826`, compares the same server binary with integer literal reuse off/on within
balanced and speed profiles. Each profile has three process pairs in A/B, B/A, A/B
order. Each configuration runs all 22 SF1 queries through one ADBC client, one warmup
pass and five measured passes. A plain DuckDB configuration checks the same reference.
Server containers have four CPUs and 8 GiB; inputs are shared immutable local Parquet.
Exact versions, arguments, input hashes, SQL and typed reference values are retained.

All 1,716 requested query slots match the generation oracle's row counts and v2
value checksums. All 22 typed reference files independently reconstruct to those
checksums. All 12 server JFR recordings pass readability/hash checks; all 13 labels
have complete process/cgroup evidence. The final retention step failed when host
derivation tried to write inside a container-owned plain-client directory. The ZIP
was still uploaded and its GitHub digest and CRC verified. The original SHA256SUMS
was absent: all 644 original files were hashed locally, outside the immutable raw
tree. Derivation replay on a working copy produced a verified 654-file manifest;
no measurement rerun was needed. Ownership is now restored only on each stopped
task's output before host derivation.

[Java run 46034754073](https://github.com/C0fuge/kestrel/actions/runs/46034754073),
revision `0dc9711d`, separately verifies all 264 requested slots with two concurrent
clients and reuse off/on. This follows the integer/Decimal and physical Arrow-date
checksum corrections. Its 192 original manifest hashes and complete JFR/GC/resource
evidence were verified. The [focused CI](https://github.com/C0fuge/kestrel/actions/runs/46034758772)
passed 37 tests, full test-source compilation and Checkstyle.

## Descriptive latency ratios

Below are medians of 15 matched-pass ON/OFF ratios per query, grouped into three
fresh-JVM process pairs per profile on one runner. Lower favors ON. These are descriptive short-run
measurements, not confidence intervals or steady-state performance guarantees.
Client timing includes answer consumption and canonical checksum work. Total is the
sum of all 22 query latencies in a pass.

| Query | Balanced | Speed |
| --- | --- | --- |
| q01 | 1.002 | 1.000 |
| q02 | 1.006 | 1.025 |
| q03 | 0.998 | 1.034 |
| q04 | 0.994 | 1.027 |
| q05 | 1.025 | 1.022 |
| q06 | 1.075 | 1.021 |
| q07 | 1.069 | 0.992 |
| q08 | 1.106 | 1.126 |
| q09 | 0.934 | 1.001 |
| q10 | 1.016 | 1.026 |
| q11 | 0.970 | 1.003 |
| q12 | 0.999 | 0.955 |
| q13 | 0.982 | 0.992 |
| q14 | 1.013 | 1.005 |
| q15 | 1.003 | 1.003 |
| q16 | 0.985 | 1.021 |
| q17 | 1.019 | 1.034 |
| q18 | 0.988 | 0.977 |
| q19 | 0.995 | 0.957 |
| q20 | 0.999 | 0.925 |
| q21 | 0.987 | 0.998 |
| q22 | 0.995 | 0.996 |
| Total | 1.0007 | 1.0077 |

Aggregation matters. Ratios of median configuration times for q08 are
0.959/0.922/1.025 in balanced and 1.216/1.161/0.946 in speed: direction varies across
process pairs, and can differ from pooling matched-pass ratios. Both estimators and
all 660 individual query ratios are retained. No aggregate TPC-H benefit is established.

## Server phases and limits

All 1,584 benchmark streams were uniquely aligned by client timestamps and row counts;
12 unmeasured duckdb_settings streams were excluded. For speed q08, paired server
execute medians OFF→ON are 84→103, 98→110 and 88→83 ms. First-batch time tracks
execution; logged millisecond send/backpressure medians are zero and client residual
time is about 4–6 ms. Measured target-query attempts do not overlap recorded GC pauses.
Some ON attempts overlap recorded compilations of at least 100 ms, but excluding
those does not establish a causal explanation.
Compilation remains active during these approximately ten-second measured windows.

Original q08 is rejected by the literal-shape helper. Source and truncated stream
prefixes strongly suggest the same fallback during actual execution, although full
stream SQL was not recorded. Extra fallback tokenization happens before streamPrepared's
executeMs clock, so it cannot directly explain changes inside that interval. Avoiding
repeated rejection work is a separate hypothesis, not a demonstrated fix for q08.

Process CPU ratios are coarse one-second sampling brackets, with 0.74–0.97 seconds
of boundary overhead. They must not be interpreted as exact per-query CPU or as the
exact step CPU/query measurements from the embedded small-query screening. Allocation
and native-residency samples do not establish CPU percentages.

Repeated fixed TPC-H texts already benefit from ordinary statement caching. This check
primarily validates answers and regression risk; the variable-literal short/mixed
workloads establish the candidate's throughput benefit. The experiment remains off by
default. No release package or production deployment was made.
