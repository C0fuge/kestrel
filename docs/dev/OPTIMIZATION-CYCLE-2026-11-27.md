# Small-query optimization cycle — 2026-11-27

## Scope and decision

This is an opt-in experiment, not a new server default. The switch is
`-Darrowflight.experimental.integerLiteralParameters=true`. The normal execution
path stays active unless the switch is set. Do not enable it as a default until
the pinned-core, mixed-load and tail-latency checks pass.

The baseline is `90542012`. Local screening uses synthetic Parquet with 400,000
rows in four files, alternating point and filtered-count queries through the
prepared Flight SQL lifecycle. Server and clients share one JVM; process CPU,
RSS and JFR measurements include both. The JVM is limited to a 2 GiB heap and
1 GiB direct memory; shared DuckDB uses two threads and a 512 MB memory limit.
The container is limited to four CPUs and 8 GiB, initially without CPU affinity.
These are screening results, not production or cluster throughput.

## Candidate

[DuckDB prepared statements](https://duckdb.org/docs/api/c/prepared) support repeated
execution with different bound values. The experiment reuses one statement for
queries whose small integer comparison literals differ, using the existing
`DescribeKey` lexer and explicit `CAST(? AS INTEGER)` markers. The original SQL
still passes the single-query/function checks before a statement is prepared;
parameters are cleared and rebound on every execution. Views synchronize as before.
Statements reading evictable local copies retain their uncached lifetime.

Existing parameter markers, oversized SQL, comments, backslashes and unsupported
literal shapes use the original path. Review found two lexer mismatches that
required these conservative exclusions: nested block comments and backslash-escaped
quotes in E-strings. No change to the shared lexer or its other callers was made.

## Preliminary local evidence

Three alternating pairs, eight seconds per concurrency step, with JFR and raw samples:

| Clients | Candidate / baseline QPS | p50 | p95 | p99 | CPU per query |
| --- | --- | --- | --- | --- | --- |
| 1 | 1.071 | 0.936 | 0.958 | 0.921 | 0.966 |
| 8 | 1.115 | 0.899 | 1.091 | 1.269 | 0.900 |
| 32 | 1.109 | 0.833 | 1.284 | 1.206 | 0.905 |

Ratios compare medians across trials. Higher QPS and lower other ratios are better.
The throughput gain comes with worse high-concurrency tails, so these trials do not
justify a default change. CPU-quota bursts across the host's cores are one hypothesis;
repeat on pinned cores before drawing that conclusion. Native-method JFR samples
measure native residency, including waits; they are not CPU percentages.

Overlapping answers, row counts, checksums, Arrow bytes and batches matched in the
initial trials. Five focused rewrite tests pass, including 432 original-versus-bound
type/operator/value comparisons. Existing adapter, shared-database and handle tests
also pass with the experiment enabled (148 reported tests, three skipped).
Compile, Checkstyle and SpotBugs passed for the server and its reactor dependencies
before the final telemetry-only edits. Remote checks validate the final branch.

## Verified cloud screening

[Run 46020524364](https://github.com/C0fuge/kestrel/actions/runs/46020524364) passed
three independent variants, each with three alternating paired rounds on four pinned
logical CPUs. A is `90542012`; B is `2df1ce6a`. The same measurement harness was used
on both sides. JVM/process metrics include the embedded clients.

| Variant | Clients | QPS ratio | p95 ratio | p99 ratio | CPU/query ratio |
| --- | --- | --- | --- | --- | --- |
| Integer literal reuse | 1 | 1.144 | 0.895 | 0.880 | 0.904 |
| Integer literal reuse | 8 | 1.112 | 0.920 | 0.938 | 0.891 |
| Integer literal reuse | 32 | 1.104 | 0.889 | 0.908 | 0.900 |
| Producer cap 2 | 8 | 0.892 | 1.055 | 1.091 | 1.021 |
| Producer cap 2 | 32 | 0.827 | 0.997 | 0.962 | 1.092 |
| Producer cap 4 | 8 | 0.989 | 1.003 | 0.984 | 1.012 |
| Producer cap 4 | 32 | 0.903 | 0.926 | 0.877 | 1.079 |

These are medians of paired candidate/baseline ratios, not comparisons of absolute
QPS across runners. Integer reuse improves both point-query and filtered-count p99
separately; its median peak process RSS ratio is 0.993. All 87,852 overlapping answers
of that variant match. All three variants pass their answer gates. Each variant's
193 raw files was downloaded and verified against its SHA256SUMS manifest.

The pinned result confirms the reuse candidate's benefit in this workload. It does
not prove why earlier local quota-only tails worsened: hardware and scheduling differ.
Neither producer cap justifies a universal default on short queries: cap 2 loses
throughput, while cap 4 trades throughput for some tail improvement at 32 clients.
Mixed-load and TPC-H checks remain necessary before changing defaults.

[Fast Dev CI 46020549681](https://github.com/C0fuge/kestrel/actions/runs/46020549681)
passed 148 focused tests with no skips and Checkstyle on the published screening branch.

## Measurement support and next experiments

`SmallQueryThroughputPerfTest` now accepts `-Dperf.output=<fresh-directory>` and
records per-query latency, ordered answer checksum, rows, Arrow bytes/batches,
per-step wall/process CPU, JVM heap/direct/mapped memory, RSS, GC, thread counts,
runtime flags and versions, lifecycle milestones, JFR and producer-gate waits.
Warm-up samples are retained. Resource samples and query steps have monotonic
anchors for alignment. Partial query samples survive a correctness failure.
External-server runs explicitly label JVM metrics as client-only.

`-Dperf.producingStreams=0|2|4` wires the existing producer gate into the test, so
admission can be compared without a SQL rewrite. `threadsUnderLoad` does not change
shared DuckDB's node-wide pool and is not a substitute for this experiment.

Cloud screening should alternate A/B order on one runner, pin equal resources,
retain every raw log/profile/sample as an artifact and fail on incorrect answers.
The next cycle interleaves a deterministic bounded customer-key self join with the
small queries using `-Dperf.heavyEvery=8`, identically on A and B. Point and count
slots alternate among non-heavy queries so client count does not introduce a parity
bias. Reports retain observed proportions and per-kind tails. Explicit failed-sample
status prevents execution or answer errors from being ranked as fast queries;
global deadline and synchronized snapshots preserve partial evidence.
TPC-H and slow-reader checks follow before promoting any default. No TPC-H or
full-scan run is claimed here.
