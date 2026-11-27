# Mixed baseline CPU diagnostic, 2026-11-27

[Run 46046940838](https://github.com/C0fuge/kestrel/actions/runs/46046940838)
profiles safe source `56b4eba65c4c15393686aa27e64762ba79554959` using collector
`d6391de95f3fe0448460c91bae36ca170eff792c`. It is a diagnostic baseline,
with no A/B speedup claim. The earlier failed collection remains preserved.

Each concurrency uses a fresh embedded server/client JVM, four pinned logical CPUs,
2 GiB heap, 1 GiB direct-memory limit, two DuckDB threads and a mixed join every eight
queries. The legacy eight-client warmup lasts ten seconds; measurement lasts thirty.
Async-profiler 4.6 records explicit ctimer CPU samples at 10 ms with native DWARF stacks.
The target PID, actual JVM limits, native worker coverage and phase clocks pass their
independent gates. JVM/compiler/GC, embedded clients and collectors share the process.

| Measured observation | 8 clients | 32 clients |
|---|---:|---:|
| Completed queries | 14,818 | 15,642 |
| Diagnostic queries/second | 493.84 | 520.18 |
| Whole-process CPU ms/query | 7.566 | 7.392 |
| CPU samples | 11,198 | 11,510 |
| Snappy RawUncompress leaf samples | 1,453 | 1,556 |
| Native planning inclusive samples | 1,227 | 1,344 |
| Native execution inclusive samples | 7,265 | 7,700 |
| Compiler inclusive samples | 1,039 | 973 |
| Arrow IPC inclusive samples | 35 | 39 |
| Schema serialization inclusive samples | 3 | 2 |
| GC pause wall ms | 170.1 | 326.7 |
| Sampled RSS peak KiB | 918,552 | 1,000,644 |

Inclusive categories overlap. CPU samples are statistical estimates, and allocation
weights, method residency and overlapping compiler-event wall durations are separate
measurements. All 36,331 answers, including 5,871 warmup answers, pass the full synthetic
oracle. Ten milliseconds times measured sample counts agrees with independent process
CPU within 0.12% and 0.45%; this supports coverage, not per-query CPU attribution.

Five-second completion bins show remaining ramp effects. At eight clients the first
bin has 401.4 queries/second and 21.79% compiler samples; the last ten seconds have
540.5 queries/second and 2.84% compiler samples. The last two bins still differ. At
32 clients new server, gRPC and client threads appear after the eight-client warmup;
compiler activity and GC continue late in measurement. Neither trial establishes a
steady-state plateau or proves warmup caused the throughput change. Common opt-in
[warmup controls](WARMUP-CONTROLS.md) allow that hypothesis to be tested explicitly.

Snappy decoding, native joins and native planning merit focused experiments. Schema
serialization has little sampled CPU here, consistent with rejecting the earlier
general schema-cache experiment after mixed-load regressions. These observations
do not justify changing production defaults or removing correctness checks.

Host CPU some-pressure increases by 20.84/28.98 seconds and I/O pressure is nonzero.
Observed memory pressure, OOM, steal and throttling deltas are zero only over the
retained intervals. The shared runner is not established to be interference-free.
ctimer has timer-resolution limits and provides no kernel stacks.

The original artifact contains 494 manifest-checked files, including answer samples,
JFRs, all auxiliary recordings, native binaries, logs, source data and resource series.
Its ZIP CRC and exact file set pass; SHA-256 matches GitHub's digest:
`6f1c2e594e4ad0775a1c3ad7c64d4d92ffe95b5a8bfb140b87f4d17d772829da`.
A retained private copy and separate hashed derivations preserve evidence beyond
GitHub's artifact retention window. [Collection details](CPU-PROFILING.md).
