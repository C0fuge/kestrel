# TPC-H performance evidence

`tpch-perf.yml` retains an independent directory for every configuration (`default`,
`threads4`, `baseline`, `extra`, `moreN`, and optional `plain`). Its existing SF,
storage/S3 latency, client/concurrent clients, baseline, quick and extra-configuration
inputs keep their meanings. Every configuration keeps its complete client/Maven log;
later configurations never replace earlier logs. A telemetry gate runs before the
existing answer and timing checks and rejects incomplete or failed raw measurements.
`run-plan.json` is written from the requested inputs before any configuration starts.
The gate requires every planned label and exactly one sample per requested query,
client and round, including round zero; duplicate slots and truncated final rounds
fail even when the client exits zero. ADBC and plain DuckDB each have one effective
client, while Java uses the requested concurrency.

The shared artifact contains the dispatched inputs, source commits, build/install
logs, exact workflow, hardware/kernel/JDK/Docker/image identities, jar hashes, query
SQL and expected row references. The already-computed generation answers also retain
`qNN.checksum` references using the Python clients' rounded, order-independent checksum
algorithm; Java's existing checksum algorithm differs. The existing answer gates keep
their behavior. `input-manifest.json` identifies each immutable
generated Parquet input by relative path, SHA-256, size and row count, and records
DuckDB's version, dbgen call, default deterministic generation and row-group layout.
Large regenerable Parquet inputs are represented by this shared manifest rather
than uploaded or duplicated per configuration. Query/measurement evidence is retained
in full, including failed attempts. Download decision-making artifacts before their
30-day retention expires.

Each server configuration also retains:

- Exact server/client/container argv, selected container limits and exit/OOM state;
  generated S3 test keys are redacted from text artifacts. Environment-variable JFR
  events are disabled before recording so those keys never enter the recording.
- Container creation/start/readiness/finish timestamps, complete timestamped server
  logs, WARN/ERROR extracts, effective JVM flags and Java/DuckDB version evidence.
- Once-per-second timestamped raw cgroup CPU/throttling, memory/peak/events, process
  count and I/O counters, plus `/proc/1/stat`, `status`, `io` and `smaps_rollup`.
  Each file's read status is retained separately, with actual required field checks;
  a later successful read cannot conceal an earlier failure. A valid empty `io.stat`
  is allowed when there was no attributed I/O. Clock tick/page units are recorded.
  The gate requires at least one complete process/cgroup sample. A server heap snapshot from `jcmd GC.heap_info`
  is collected every fifth sample, including runs with no garbage collections.
- An unrotated complete GC/safepoint log, the original JFR recording and preserved
  disk repository chunks. The recording enables periodic direct-buffer and CPU
  statistics and GC heap/collection events. The server is stopped gracefully before
  host-JDK summary/JSON extraction. If no final recording exists, collection attempts
  `jfr assemble` recovery and preserves the original chunks regardless of success.
  Ranking requires a nonempty complete recording, successful summary and JSON event
  extraction bound to its SHA-256/size, readable summary/events, and actual JVM/direct
  buffer fields. GC initialization logs plus an actual successful heap snapshot are
  valid without any garbage collections; arbitrary nonempty files are insufficient.

Common server flags enable `streamTimingLog=true` before variant options. Every
finished result stream's INFO line retains epoch `startMs`, `executeMs` (gate wait,
execution and Arrow export), `firstBatchMs` (start to first nonempty batch, or -1),
`totalMs`, `duckMs` (batch loading), `sendMs` (handoff to gRPC), `backpressureMs`, rows
and batches. These server timings live in the complete server log and describe stream
execution and handoff, rather than network delivery time or wire bytes. Client raw
timestamps provide the corresponding query/round timeline.

The server image uses Temurin 21 JDK so diagnostic tools are available. All compared
configurations receive the same collectors. JFR, one-second Docker/proc sampling and
five-second heap diagnostics introduce overhead; cgroup totals also include the small
in-container diagnostic processes. Server `/proc/1` counters isolate the server
process. Compare configurations from the same instrumented workflow rather than
treating times as identical to historical uninstrumented runs.

`TPCH_RAW_DIR` optionally enables Python ADBC/plain-client evidence. Java uses
`-Dperf.rawDir=<fresh directory>` and retains its per-client samples. Every attempted
query includes warmup/measured round, timestamp, elapsed time, row count, checksum,
bytes/batches and explicit success/error status; files flush after each attempt.
The first-query evidence includes the warmup's first attempt. Client lifecycle files
identify complete runs. Measured per-query p50/p95/p99/max summaries exclude warmups
and failed queries while retaining their original rows. With only a few rounds, tail
percentiles are observed samples and can equal the maximum; the CSV remains the
authoritative evidence. Existing printed query/checksum lines remain compatible with
the existing checks.

For Python clients, `arrow_buffer_bytes` is Arrow `table.nbytes`, the referenced
buffer ranges, and `table_batches` is `len(table.to_batches())`, the actual table
chunk layout. These are **not wire bytes or network batch counts**. Java records
Arrow vector buffer bytes and Flight batches separately. The plain DuckDB reference
also retains its full install/client log, raw query evidence and container/process
resource samples; it has no server JVM/JFR.

The workflow uploads the entire metrics directory on success or failure with
SHA-256 checksums. Diagnostic-command failures, partial output and exit codes remain
visible. A hard runner termination can interrupt graceful shutdown; mounted JFR chunks
and flushed CSV rows retain whatever was written before termination.

The implementation follows the primary documentation for [JFR startup and repository
preservation](https://docs.oracle.com/en/java/javase/21/docs/specs/man/java.html),
[JFR configuration, summary and chunk assembly](https://docs.oracle.com/en/java/javase/21/docs/specs/man/jfr.html),
and [Arrow table byte and batch semantics](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html).
