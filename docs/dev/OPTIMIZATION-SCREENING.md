# Cloud optimization screening

`optimization-screen.yml` compares two server revisions on the same GitHub-hosted
runner. It runs manually or on pushes to `perf/optimization-screen-*`; it is separate
from Dev CI and does not publish a server release. Manual runs accept `source_ref`,
`baseline_ref`, candidate-only `variant_options`, paired `rounds`, seconds per step,
rising client counts and `cpu_mode=pinned|quota`. Push runs use three paired rounds,
15 seconds per step, clients 1/8/32 and pinned CPUs, comparing the pushed revision with
`90542012`. Pushes to `perf/optimization-screen-mixed-*` or
`perf/optimization-screen-joins-*` use two rounds and
`heavy_every=8`; other pushes use `heavy_every=0`. Manual dispatches default to three
rounds and `heavy_every=0` and accept a common interval from 0 through 64.
The workflow runs the Python runner's unit tests before building or measuring.
Ordinary and mixed-screen pushes start three independent paired jobs: `literal` enables
`arrowflight.experimental.integerLiteralParameters` only on B; `admission2` and
`admission4` set only B's `perf.producingStreams=2` or `4`. Join-screen pushes instead
start `join1` and `join2`, setting only B's
`arrowflight.experimental.joinProducingStreams=1` or `2`. Manual dispatches start one
`custom` paired job with explicit options. Each variant has its own raw artifact.
Compare A/B ratios within a job; absolute values from different runners are not comparable.
Use a full 40-character commit SHA for explicit commit inputs; branch and tag names
are also accepted by the checkout action.

Each round alternates A/B, B/A, A/B. Both revisions use the candidate revision's exact
`SmallQueryThroughputPerfTest` and `PerfRunArtifacts` collector, including when the
baseline predates these collectors. Only these test files are overlaid on the baseline;
production code is built from each selected commit. The workflow packages only the
server reactor and its dependencies, without unit/perf tests, Checkstyle or SpotBugs;
normal fast branch CI and final full CI remain separate requirements before merging.

The data set is synthetic: 400,000 orders-like rows, four Parquet files, alternating
point lookup and filtered-count queries over Flight SQL. An unranked preflight writes
and warms the common data set before either ranked side. With `heavy_every=N` greater
than zero, every Nth query is a bounded count self-join on `o_custkey` (query kind 2),
whose expected count is `min(key, 5000) * 400`. The remaining queries alternate point
lookups (kind 0) and filtered counts (kind 1). The workload interval is passed as
`-Dperf.heavyEvery` to both A and B, including preflight, and recorded in `screen.json`.
It is rejected as a candidate-only option. Each dedicated container has
8 GiB memory and a four-processor JVM (`-Xmx2g`, direct-memory limit 1 GiB). Pinned mode
uses four logical CPUs; quota mode applies `--cpus=4`. Embedded clients and the server
share one JVM and CPU allocation, so process CPU/RSS measurements include both. These
are screening ratios; hosted-runner absolute QPS is not a production estimate.

Containers run as the runner's numeric UID/GID. Read-only passwd/group files retain the
image's system entries and add a synthetic screening identity when needed, so Hadoop's
native Unix login works even when that UID is absent from the image. Outputs and the
Maven cache remain owned by the runner; no host accounts or credentials are copied.

Candidate options are structured arguments, never shell-expanded. Only experimental
server properties, effective `duckDb*` settings, `resourceProfile`, `perf.producingStreams`,
`perf.threads`, `perf.threadsUnderLoad` and `perf.shared` are accepted.
The helper overrides `duckDbThreads*`, `duckDbShared*`, `duckDbMemoryLimit`,
`duckDbWarmConnections` and `duckDbGroups`, so the workflow rejects those ineffective
options. Use the corresponding `perf.*` properties; their actual values are recorded
by the harness. For example, `-Dperf.threads=4 -Dperf.shared=4:1GB` changes the shared
DuckDB thread count and limit.
Never supply credentials, production host names or personal paths in public inputs.
For example, `-Dperf.producingStreams=2` screens admission control; associated `*-gate.txt`
files are retained automatically if the selected harness writes them.
The join experiment defaults to zero and uses the existing DuckDB parse result to cap
joins and unknown structural forms while other statements bypass it. An explicitly
configured global producer cap takes precedence and logs the join experiment inactive.
The harness uses an optional getter for `*-join-gate.txt` wait/permit deltas, so the same
harness builds on baseline `90542012` without adding production code to A. This is a
join-admission screen: expensive non-join queries can still run concurrently, and the
producer cap does not bound memory retained between batches.

Dispatches permit at most eight concurrency steps and fifteen planned measurement
minutes, including warm-up/setup allowances. The build/measurement step has a separate
35-minute timeout inside the 45-minute job, reserving ten minutes for publishing partial
artifacts if the work fails or exceeds its budget.

The answer gate compares rows, ordered value checksums, Arrow buffer bytes and batches
for every overlapping `(concurrency, client, sequence, query kind, key)` pair. The test
also asserts the known first value and row count. A failed gate prevents ranking.
Any raw row with `ok=0` prevents ranking, including execution/read errors and answer
mismatches; failed rows and partial attempts remain in the artifacts. Historical
samples without the status column are accepted and explicitly labelled legacy in
their summary. New trials overlay the same status-aware harness on both revisions.
The aggregate summary reports median paired ratios for QPS, p95, p99 and CPU time/query.
Each query kind also retains its count, fraction of observed queries, QPS, p50, p95,
p99 and maximum latency in every trial's `summary.json`. Kind QPS uses the full step's
wall time, so the kind QPS values add up to the aggregate. The Markdown summary shows
observed A/B counts and shares plus per-kind paired ratios for counts, QPS and all four
latency metrics; `paired-ratios.json` retains every paired ratio and its median.
Missing kinds across A/B or rounds fail the comparison. Inspect the short-query tails
independently: aggregate QPS gains can conceal a short-query p95/p99/max regression.
A QPS gain with worse tails is a tradeoff, never promoted automatically.
Concise phase-start messages appear in the Actions console; complete Maven logs stay
in each phase's artifact directory.

Every successful or failed attempt is uploaded for 30 days with SHA-256 checksums:

- all per-query samples, warm-up samples, wall times and p50/p95/p99/max summaries;
- typed answer checksums, rows, Arrow buffer bytes/batches and transfer MB/s;
- 100 ms JVM/process samples: RSS, heap, non-heap, direct/mapped buffers, GC and threads;
- JFR profiles, lifecycle stages, startup/first-query evidence, exact runtime flags,
  JDK/DuckDB versions, CPU inventory, image identity and source commit IDs;
- cgroup CPU, throttling, memory, process and I/O counters, container exit/OOM state;
- full build/test logs, Surefire reports, WARN/ERROR extracts and the original Parquet data.

Full-scan, TPC-H, real-service, distributed/failover and external-client experiments
remain separate workloads. They are not implied by passing this small-query screen.
Download artifacts before their retention expires when a run is used for a decision.
