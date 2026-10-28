# Server tuning: measured settings and recommended defaults

Campaign 2026-10-25/26, server builds 4eaab052deaf (all sweeps) and c3b276054cc3 (CI re-check of the winners; same defaults in
`arrowflight.properties`); final phase on 57d252b1437f (HDFS, S3 and a 3-node cluster, below).
Goal: heavy loads must work reliably at acceptable speed, small queries must stay fast.

## How it was measured

- **Stand:** laptop, 16 cores, 30 GB RAM, single-replica HDFS (NameNode + one DataNode in containers, hadoopfs extension,
  short-circuit reads), MinIO S3, TPC-H SF1 and SF10 parquet (8 files per big table, 122880-row groups). One server node co-located
  with the DataNode, container 8 GB, `-Xmx3g -XX:MaxDirectMemorySize=2g`, `numServers=1`. Trino 455 + our connector, 6 GB heap.
- **CI screening:** GitHub 4-cpu runners, default (A) and variant (B) on the same runner, alternating rounds (ABBAAB), local files or
  an emulated S3 (`rclone serve s3`); results are ratios B/A. An A/A control job gives the noise band (0.93-1.09 local).
- **Workloads per round:** SMALL = metadata (GetTables/GetSchemas), point lookups on orders/lineitem, prepared point lookup, LIMIT 10,
  count with a filter, at 1/8/32 concurrent clients, Flight SQL JDBC and ADBC (Python); MEDIUM = TPC-H SF1 22 (JDBC complete mode,
  Trino); HEAVY = TPC-H SF10 22 via JDBC (complete mode) and via Trino, full `lineitem` scan (60 M rows, ~10 GB Arrow) by raw Flight
  at 1/4/8 concurrent clients, and q01 q09 q18 q21 at the same time (heavy4). Warm runs, median of the reps, 3 rounds per variant,
  variants alternated; a round only counts when the CPU load outside the measurement stayed low.
- **Correctness gate** for every variant: TPC-H answer checksums (JDBC SF10, Trino SF10, JDBC SF1), small-query answer
  signatures, scan row counts. A variant that changes results or fails is disqualified.

## Final comparison on build 57d252b1437f (2026-10-26, stand, medians of 3 rounds)

Build 57d252b1437f: `duckDbThreads` default 4, S3 connect retry, one outbound byte budget shared by all Flight streams of a node
(`Flight outbound budget: 536870912 bytes shared by all streams (per stream at most 268435456, at least 1048576)` at
`-XX:MaxDirectMemorySize=2g`), planning without waiting for the cluster to settle after a member left. Threads were set explicitly
in every variant. Two controls on the campaign build 4eaab052deaf (threads 4, single node and 3 nodes) ran interleaved in the same
session, because the host had less free memory and page cache than during the earlier phases; Trino SF10 is 2-3 s higher for both
builds than in the earlier phases for that reason. Same workloads as above; S3 = MinIO on the same host (no SF1 there); 3 nodes =
three servers on the same host and the same DataNode (`-Xmx2g`, `duckDbMemoryLimit=1GB`, 5 GB containers), raw scans in partial
mode (3 endpoints). "default" = build 57d252b1437f with its default 4 threads. Small latencies in ms (JDBC unless ADBC), TPC-H in s
(sum of the 22 warm medians), scan MB/s at 1/4/8 concurrent clients, RSS = peak of the server JVM (max over nodes).

### HDFS, single node

| profile | setting | small c1 p50/p95 | small c32 p50/p95 | ADBC c1 p50 / c32 p95 | SF1 JDBC | SF1 Trino | SF10 JDBC | SF10 Trino | scan 1/4/8 | heavy4 max s | RSS GB | errors |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **default** | threads 4 | 21.5 / 39.6 | 94.5 / 307 | 16.9 / 305 | 1.89 | 6.19 | 10.82 | 33.6 | 939 / 2749 / 3161 | 2.6 | 4.0 | 1 of 9 ADBC c32 runs: connect race (item 13d, fixed after this build) |
| best for small | threads 2 (campaign build) | 18.0 / 41.8 | 65.2 / 300 | 14.7 / 282 | 2.29 | 5.80 | 19.01 | 32.4 | 996 / 3078 / 3463 | 3.4 | 4.2 | none |
| (small, alternative) | threads 1 | 19.4 / 62.3 | 60.9 / 300 | 15.4 / 276 | 3.79 | 6.30 | 36.51 | 39.6 | 843 / 2870 / 3225 | 5.8 | 4.3 | none |
| **best balanced** | = default (threads 4) | 21.5 / 39.6 | 94.5 / 307 | 16.9 / 305 | 1.89 | 6.19 | 10.82 | 33.6 | 939 / 2749 / 3161 | 2.6 | 4.0 | as default |
| best for heavy | threads 8 | 24.3 / 45.9 | 112.2 / 367 | 20.2 / 335 | 1.54 | 6.41 | 7.95 | 37.6 | 901 / 2736 / 3028 | 2.5 | 4.6 | none |
| control | campaign build, threads 4 | 21.9 / 40.1 | 95.1 / 327 | 17.1 / 295 | 1.94 | 6.22 | 11.16 | 34.7 | 955 / 2877 / 3092 | 2.6 | 4.1 | none |

Build 57d252b1437f at 4 threads equals the campaign build at 4 threads within noise on every metric: no single-node regression.
1 thread only lowers the 32-client p50 (-36 %) and costs p95 (+57 % at 1 client), SF10 (3.4x) and heavy4 (2.2x); 2 threads stay the
best pure small-query setting.

### S3 (MinIO), single node

| profile | setting | small c1 p50/p95 | small c32 p50/p95 | ADBC c1 p50 / c32 p95 | SF10 JDBC | SF10 Trino | scan 1/4/8 | heavy4 max s | RSS GB | errors |
|---|---|---|---|---|---|---|---|---|---|---|
| **default** | threads 4 | 20.0 / 50.4 | 107.8 / 704 | 14.4 / 817 | 33.39 | 66.7 | 742 / 2441 / 2916 | 7.1 | 3.7 | none |
| best for small | threads 2 (campaign build) | 18.0 / 75.3 | 67.6 / 607 | 12.6 / 593 | 54.42 | 66.5 | 814 / 2789 / 3313 | 8.5 | 4.4 | none |
| **best balanced** | = default (threads 4) | 20.0 / 50.4 | 107.8 / 704 | 14.4 / 817 | 33.39 | 66.7 | 742 / 2441 / 2916 | 7.1 | 3.7 | none |
| best for heavy (few clients) | threads 8 | 20.3 / 35.5 | 130.5 / 794 | 14.5 / fails | **24.17** | 69.1 | 754 / 2488 / 2780 | 6.8 | 3.9 | 32 concurrent small queries: 32-64 ADBC and 0-1 JDBC failures in every round |
| before the fix | threads 8 (campaign build) | 21.1 / 172.5 | 122.1 / 625 | 16.9 / fails | **0/22** | 65.6 | 873 / 2770 / 3332, 1- and 4-client scans failed in 2 of 3 rounds | 6.9 | 4.3 | every SF10 JDBC query, ~800 ADBC c32 |

The S3 connect retry fixes the heavy path at 8 threads (SF10 JDBC 22/22 in 3/3 rounds, 1.38x faster than 4 threads, all scans
complete). Still failing at 8 threads: 32 concurrent small queries, "IO Error: Could not establish connection error for HTTP GET",
123-192 failures per round. Measured in the server's network namespace: sockets in TIME_WAIT toward the S3 endpoint reach the whole
ephemeral port range (28231 of 28232, range 32768-60999) during that step in 3/3 rounds; at 4 threads they peak at 20-22k (70-78 %
of the range), the SF10 JDBC step at 18-24k. httpfs opens a new connection for most range GETs, every DuckDB instance has its own,
and TIME_WAIT lasts 60 s, so a retry of a few seconds cannot bridge it (Next steps item 0g). S3 vs HDFS at 4 threads: SF10 JDBC
3.1x slower, Trino 2x, 32-client p95 2.3x, single-client scan -21 %.

### 3-node cluster, HDFS, partial-mode scans

| profile | setting | small c1 p50/p95 | small c32 p50/p95 | ADBC c1 p50 / c32 p95 | SF1 JDBC | SF1 Trino | SF10 JDBC | SF10 Trino | scan 1/4/8 | heavy4 max s | RSS GB (max node) | errors |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **default** | threads 4 | 23.6 / 45.4 | 96.3 / 355 | 17.7 / 325 | 2.15 | 7.73 | 13.74 | 38.9 | 1604 / 2527 / 2635 | 5.2 | 4.1 | scans 0; Trino q18 heap (Trino side) |
| best for small | threads 2 (campaign build) | 19.4 / 44.3 | 71.7 / 288 | 15.4 / 279 | 2.27 | 6.69 | 20.51 | 35.9 | 1830 / 3066 / 3085 * | 3.9 | 4.5 | campaign build: 7-8 failed endpoint reads at 8 clients |
| **best balanced** | = default (threads 4) | 23.6 / 45.4 | 96.3 / 355 | 17.7 / 325 | 2.15 | 7.73 | 13.74 | 38.9 | 1604 / 2527 / 2635 | 5.2 | 4.1 | as default |
| best for heavy | threads 8 | 25.3 / 48.3 | 119.9 / 414 | 21.2 / 393 | 1.65 | 7.48 | 11.12 | 38.6 | 1557 / 2626 / 2702 | 4.9 | 4.1 | scans 0; Trino q18 heap |
| control | campaign build, threads 4 | 21.9 / 41.5 | 101.5 / 343 | 17.4 / 331 | 1.89 | 7.64 | 15.32 | 37.8 | 1679 / 2590 / 2753 * | 4.4 | 4.6 | 5 / 7 / 10 failed endpoint reads at 8 clients |

\* rounds with failed endpoint reads: MB/s of the rows delivered, not comparable. 8 clients x 3 endpoints of lineitem SF10:
**0 failed reads in 6 of 6 rounds** on 57d252b1437f (4 and 8 threads), all 480 M rows, no node lost, max node RSS 4.1 GB; the
campaign build in the same session failed 5-10 reads per round (`OutOfMemoryError: Cannot reserve ... direct buffer memory` ->
`CANCELLED: Server sendMessage() failed`). On one host the cluster adds no hardware: only partial scans gain (1 client 1.6 GB/s over
3 endpoints vs 0.94 GB/s on one node); SF10 JDBC is 27 % slower than one node. heavy4 slowest query 5.2 s vs 4.4 s for the control
(rounds 4.9-5.3 vs 3.7-4.7), not seen on one node: possible small regression, to recheck. Trino q18 "Java heap space" (6 GB Trino
heap) whenever a query has more than one endpoint: Trino connector side.

Correction (2026-10-26): the earlier report of a node dying under 8 concurrent cluster scans was an artifact of the benchmark
harness. Its memory guard removes the server container when host free memory falls below 4.5 GB, and that scan needs ~10-11 GB of
host memory for three servers plus the client; the removals fell inside the scan steps. No server crash was observed.

### Choice

- Keep `duckDbThreads=4` as the default: balanced on HDFS, S3 and the cluster.
- 8 threads for heavy-only HDFS servers (SF10 1.36x faster, 32-client p95 +20 %); not on S3 with concurrent clients until item 0g
  is fixed.
- 2 threads for latency-only services with many concurrent small queries.
- S3 with many concurrent queries: until connections are reused, `net.ipv4.tcp_tw_reuse=1` and a wider `ip_local_port_range`
  on the server host, or fewer threads.

## Campaign comparison (stand, HDFS, single node, build 4eaab052deaf, medians of 3 rounds)

| profile | settings | small JDBC c1 p50/p95 ms | small JDBC c32 p50/p95 ms | ADBC c1 p50 / c32 p95 ms | SF1 JDBC s | SF1 Trino s | SF10 JDBC s | SF10 Trino s | scan MB/s 1/4/8 clients | heavy4 slowest s | RSS peak GB |
|---|---|---|---|---|---|---|---|---|---|---|---|
| default | `duckDbThreads=2` (build default until 2026-10-26, now 4) | 18.8 / 43.5 | 70 / 308 | 14.8 / 293 | 2.38 | 6.04 | 19.4 | 35.1 | 944 / 2942 / 3130 | 3.5 | 4.4 |
| best for small | = default | 18.8 / 43.5 | 70 / 308 | 14.8 / 293 | 2.38 | 6.04 | 19.4 | 35.1 | 944 / 2942 / 3130 | 3.5 | 4.4 |
| **best balanced** | `duckDbThreads=4` | 21.4 / 39.4 | 95 / 302 | 17.5 / 286 | 1.85 | 6.05 | **10.8** | 33.1 | 916 / 3004 / 3162 | 2.4 | 4.1 |
| best for heavy | `duckDbThreads=8` | 25.1 / 45.7 | 112 / 320 | 20.3 / 313 | 1.49 | 6.06 | **7.45** | 33.3 | 935 / 2943 / 3156 | 2.3 | 4.7 |
| (more threads) | `duckDbThreads=16` | 25.3 / 58.2 | 126 / 377 | 20.7 / 360 | 1.41 | 6.15 | 6.58 | 35.2 | 928 / 2804 / 3124 | 2.3 | 6.3 |

Trade-off: `duckDbThreads` applies to every DuckDB instance, i.e. to every query of every client. 4 threads make SF10 1.8x and
SF1 1.3x faster for +2.6 ms (+14 %) on a single point lookup and unchanged p95 at 32 clients; 8 threads make SF10 2.6x faster but
point lookups +30-60 % (32 clients x 8 threads oversubscribe 16 cores). 16 threads add little over 8 and cost 1.6 GB more RSS.
Trino SF10 does not move with any server setting (one stream per node; the time is spent in Trino). On 4-cpu CI runners (both builds)
4 and 8 threads give SF10 0.70-0.76 and SF1 0.84-0.91 of the default with small queries unchanged.

## One-factor results (from the default)

| setting | values | effect | verdict |
|---|---|---|---|
| `duckDbThreads` | 1 / 2 / 4 / 8 / 16 | see above; 1 thread: SF10 35.5 s (1.8x slower), heavy4 5.5 s | **4** (balanced), 8 for heavy-only servers |
| `duckDbMemoryLimit` | unset / 1 / 2 / 4 GB (also with 4 or 8 threads) | no speed change; RSS 0.8-1.0x | leave unset; 1 GB where memory is tight |
| `ioParallelism` | auto / 2 / 4 / 8 / 16 | within noise (local, HDFS, emulated S3) | auto |
| `flightBackpressureThresholdBytes` | 4 / 16 / 64 / 256 MiB | within noise for 1 endpoint per node | **16 MiB** (needed with several endpoints, below) |
| `batchSize` | 16k / 64k / 256k | within noise | 65536 |
| `duckDbStreamResults` | true / false | false: server OOM on the full scan (3/3 rounds) | true (disqualified: false) |
| `duckDbStreamingBufferSize` | default / 64 MiB | within noise | default |
| `duckDbPreserveInsertionOrder` | default / false | within noise (answers still correct with the ORDER BY handling) | default |
| `duckDbExternalFileCache` | false / true | HDFS: server grows to the container limit and is OOM-killed (8 GB); emulated S3: 40x faster until it is killed (RSS 8.2 GB), answers missing | false (disqualified: true) |
| `duckDbStatementCacheSize` | 0 / 64 / 256 | 256 = 64; 0 broke parameterized prepared statements (bug, fixed after these builds as item 0b) | 64 |
| `duckDbWarmConnections` | 0 / 8 / 32 | within noise | 8 |
| `resultMode` | complete / partial | partial gives per-file partial answers to JDBC (by design, for Trino re-aggregation) | complete |
| `endpointsPerServer` | 1 / 2 / 4 / 8 | single-client partial scan 916 -> 1600 (2) / 2230 (4) MB/s; JDBC and Trino SF10 unchanged; with the 256 MiB backpressure default, 4 and 8 parts exhaust direct memory (`OutOfMemoryError: Cannot reserve ... direct buffer memory`, streams CANCELLED, RSS up to 8.7 GB); with 16 MiB backpressure no errors; Trino q18 fails with "Java heap space" (6 GB Trino heap) at 2/4/8 in every round | 1 (opt-in for single-client bulk reads, together with 16 MiB backpressure) |
| HDFS short-circuit reads | on / off | off: SF10 JDBC +49 %, Trino +25 %, ADBC 32-client p95 3.5x | on (deployment) |
| GC | G1 / Parallel / ZGC | Parallel = G1; ZGC RSS 1.5x, no gain | G1 |
| `arrow.enable_unsafe_memory_access` | true / false | within noise | unchanged |
| data layout | row groups 32k / 122880 / 1M, files 8 per table / 64 MB / one file | 1M-row groups: point/selective queries 2.5-6x slower (no row-group pruning), heavy on slow S3 ~2x faster; 32k: small 1.3x slower, RSS 1.35x; one file per table: small at concurrency 1.2-2.3x slower, RSS 2x; 64 MB files: neutral | ~128k-row groups, several files per table |

## Recommended defaults

- `duckDbThreads=4` (was 2) — adopted as the build default 2026-10-26 (plain 4). Since 2026-10-27 (item 19) the default is
  size-aware: cores / 4, between 2 and 8 (16 cores: 4 as measured above; 4 cores: 2, which a local c32 point run favoured:
  p50 57 ms vs 64-69 ms at 4). An explicit value still wins. Since 2026-10-28 an S3/lakeFS data directory (`s3://`,
  `s3a://`, `s3n://`, `lakefs://`) keeps at least 4: small queries there wait on round trips, and 2 threads lengthened the
  tail (CI screening: p95 +12-21 % at c8/c32 on 4 vCPU; local fake S3 at 2 ms per request: c1 p95 48 -> 35 ms, c8 p95
  77 -> 67 ms at 4 threads). Tables of catalogs on S3 behind a local or HDFS data directory keep the size-aware value;
  set `duckDbThreads=4` there. Since item 19b (2026-10-28) the upper clamp is 16 and the default also follows memory:
  `memory_limit` per connection = (memory - `-Xmx` - direct - 15 %) / `duckDbMemoryConnections` (cores / threads, at
  least 2), at least 1 GiB, and threads at most one per GiB of it (never below 2). Large hosts are still to be measured
  on the stand (16 vs 32 as the clamp).
- `flightBackpressureThresholdBytes=16777216` (16 MiB, was 256 MiB): no loss measured, and per-stream buffering times the number of
  concurrent streams must fit `MaxDirectMemorySize`; with several endpoints per node the 256 MiB default fails.
- Keep: `duckDbStreamResults=true`, `duckDbExternalFileCache=false`, `resultMode=complete`, `endpointsPerServer=1`,
  `duckDbStatementCacheSize=64`, `duckDbWarmConnections=8`, `ioParallelism` auto, `batchSize=65536`, memory limit unset, G1.
- Deployment: enable HDFS short-circuit reads; parquet with ~128k-row groups and several files per table.

## Single-stream scan opt-ins on DuckDB 1.5.6 (local, 2026-10-27, STATUS item 16)

`ScanThroughputPerfTest` (6 M rows / 8 files / 7 columns, 122880-row groups, one client in the same JVM, 4 cores, complete
mode, median of 9 scans, MB/s of Arrow buffers; variants run in both orders):

| `duckDbThreads` | default | `PreserveInsertionOrder=false` | `StreamingBufferSize=64MiB` | both |
|---|---|---|---|---|
| 2 | 329 | 354 | 385 | 336 |
| 4 | 348 / 388 | 334 / 349 | 405 / 424 | 338 / 298 |
| 8 | 346 / 367 | 234 / 246 | 377 / 381 | 268 / 223 |

4 / 16 MiB buffers: 357-388 MB/s (noise). Same 92 batches in every variant (the slowdown with `false` is inside DuckDB,
not our batching); same rows and checksum. Verdict here: `preserve_insertion_order=false` does not help and costs up to
a third at 8 threads; a 64 MiB buffer is +5-15 %, about the noise, and adds up to 64 MiB of buffered result per open
stream (not checked whether DuckDB counts it against `memory_limit`). Defaults unchanged; the stand (native client, 16 cores, HDFS) decides.

## Open points

- S3 with 8 threads and 32 concurrent small queries: ephemeral-port exhaustion toward the S3 endpoint (above, STATUS item 0g).
- 3 nodes: heavy4 slowest query +18 % on 57d252b1437f vs the campaign build (3 rounds each), to recheck.
- ADBC (Go Flight SQL driver): rarely (about 3 of 45 runs with 8 clients connecting at once; 1 of 9 32-client runs on
  57d252b1437f) a connect fails with "failed to update autocommit: rpc error: code = Unimplemented"; fixed after that build
  (item 13d, shared empty buffer), stand recheck pending.
