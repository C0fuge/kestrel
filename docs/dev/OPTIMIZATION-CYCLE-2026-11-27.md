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
quotes in E-strings. A separate correctness branch fixes nested block comments in
the shared lexer; this experiment retains its conservative exclusions.

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

## Verified mixed-load screening

[Run 46023313188](https://github.com/C0fuge/kestrel/actions/runs/46023313188)
passed two alternating paired rounds per variant, using the same bounded self join
every eighth query on both revisions. The other slots alternate point and count
queries. A is `90542012`; B is `59764eaf`. Resources and metric scope match the
short-query cloud screening above. Values below are medians of paired B/A ratios.
Two pairs establish a screening result, not a confidence interval or a production SLA.

| Variant | Clients | QPS | CPU/query | Point p99 | Count p99 | Heavy p99 |
| --- | --- | --- | --- | --- | --- | --- |
| Integer literal reuse | 1 | 1.143 | 0.923 | 0.959 | 0.876 | 0.834 |
| Integer literal reuse | 8 | 1.090 | 0.914 | 0.903 | 0.921 | 0.950 |
| Integer literal reuse | 32 | 1.075 | 0.928 | 0.934 | 0.991 | 1.008 |
| Producer cap 2 | 32 | 0.876 | — | 1.341 | 1.211 | 0.473 |
| Producer cap 4 | 32 | 0.932 | — | 1.566 | 1.393 | 0.606 |

All answer gates passed; 438 raw files across three immutable archives were downloaded
and checked against their SHA256SUMS manifests. The reuse candidate improves throughput
and CPU/query without the small-query tail regression seen with global caps. Its heavy
p99 ratio of 1.008 at 32 clients is effectively unchanged in this short experiment.
Global caps improve aggregate p99 while worsening point/count p99; aggregate latency
alone would produce the wrong default decision. They remain unsuitable as universal
defaults. The corresponding [fast CI](https://github.com/C0fuge/kestrel/actions/runs/46023493840)
passed focused regressions and Checkstyle.

## Join-only admission screening

[Run 46027018997](https://github.com/C0fuge/kestrel/actions/runs/46027018997)
compares A `90542012` with B `6593dada`, using the same mixed workload and two paired
rounds. B enables only `arrowflight.experimental.joinProducingStreams=1` or `2`;
integer reuse is off. Admission classification comes from the cached DuckDB JSON
query tree. Separate gate counters and raw samples retain queue waits and full
end-to-end query latency. The two caps run on separate runners: compare each cap
with its own baseline, not their absolute QPS against each other.

| Join cap | Clients | QPS | CPU/query | Point p99 | Count p99 | Heavy p99 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 8 | 0.926 | 0.976 | 0.839 | 0.846 | 1.911 |
| 1 | 32 | 0.926 | 0.983 | 0.424 | 0.426 | 5.875 |
| 2 | 8 | 0.972 | 1.021 | 0.942 | 0.908 | 1.207 |
| 2 | 32 | 0.954 | 1.041 | 0.750 | 0.704 | 4.619 |

Both answer gates and the [focused CI](https://github.com/C0fuge/kestrel/actions/runs/46027075589)
passed. All 308 raw files were downloaded and hash-verified. Join-only caps isolate
small-query tails, but queue heavy work, reduce throughput, and increase aggregate
p99 at 32 clients by 3.961x and 3.166x respectively. This is a workload-priority tradeoff,
not a general performance improvement. Both switches remain experimental and disabled.
CPU/query is a whole-process metric; no per-kind CPU attribution is claimed.

## TPC-H validation instrumentation

[Run 46028477490](https://github.com/C0fuge/kestrel/actions/runs/46028477490)
requested SF1, one ADBC client, a warm-up and two measured passes over all 22 queries,
comparing integer reuse off/on within both balanced and speed profiles on ref
`6110786a`, with a four-CPU/eight-GiB server container. It stopped before starting
queries because GitHub omitted an empty optional `baseline` input. Failed artifacts
were retained and hash-verified. The omitted-input handling is fixed with a regression
test; all fourteen telemetry tests pass, including replay of that exact input payload.

Within each requested profile pair the flag is the only setting change. TPC-H here
checks answer compatibility and regression risk: its query literals do not vary
between passes, so it does not measure the cross-literal reuse benefit established
by the small-query workload. The instrumented workflow preserves raw samples, full logs, process
and cgroup counters, JFR/GC, versions, query and input manifests. Exact requested
query/client/round coverage and readable recording evidence must pass before results
can be ranked. The later [complete TPC-H check](TPCH-LITERAL-CHECK-2026-11-27.md)
verified 1,716 ADBC and 264 concurrent-Java oracle slots. TPC-H total time is close
to neutral; per-query direction depends on the process pair and aggregation method.
The detailed report retains all 22 query ratios, server-phase correlation and the
retention-failure/recovery caveat. It establishes no aggregate TPC-H gain or universal
default promotion.
