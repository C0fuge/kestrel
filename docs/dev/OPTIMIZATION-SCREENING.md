# Cloud optimization screening

`optimization-screen.yml` compares two server revisions on the same GitHub-hosted
runner. It runs manually or on pushes to `perf/optimization-screen-*`; it is separate
from Dev CI and does not publish a server release. Manual runs accept `source_ref`,
`baseline_ref`, candidate-only `variant_options`, paired `rounds`, seconds per step,
rising client counts and `cpu_mode=pinned|quota`. Push runs use three paired rounds,
15 seconds per step, clients 1/8/32 and pinned CPUs, comparing the pushed revision with
`90542012` and enabling `arrowflight.experimental.integerLiteralParameters` only on B.
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
and warms the common data set before either ranked side. Each dedicated container has
8 GiB memory and a four-processor JVM (`-Xmx2g`, direct-memory limit 1 GiB). Pinned mode
uses four logical CPUs; quota mode applies `--cpus=4`. Embedded clients and the server
share one JVM and CPU allocation, so process CPU/RSS measurements include both. These
are screening ratios; hosted-runner absolute QPS is not a production estimate.

Candidate options are structured arguments, never shell-expanded. Only experimental
server properties, `duckDb*`, `resourceProfile` and `perf.producingStreams` are accepted.
Never supply credentials, production host names or personal paths in public inputs.
For example, `-Dperf.producingStreams=2` screens admission control; associated `*-gate.txt`
files are retained automatically if the selected harness writes them.

The answer gate compares rows, ordered value checksums, Arrow buffer bytes and batches
for every overlapping `(concurrency, client, sequence, query kind, key)` pair. The test
also asserts the known first value and row count. A failed gate prevents ranking.
The summary reports median paired ratios for QPS, p95, p99 and CPU time/query. A QPS
gain with worse tails is reported as a tradeoff, never promoted automatically.

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
