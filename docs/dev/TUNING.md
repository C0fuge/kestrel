# Server tuning: measured settings and recommended defaults

Campaign 2026-10-25/26, server builds 4eaab052deaf (all sweeps) and c3b276054cc3 (CI re-check of the winners; same defaults in
`arrowflight.properties`). Goal: heavy loads must work reliably at acceptable speed, small queries must stay fast.

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

## Final comparison (stand, HDFS, single node, build 4eaab052deaf, medians of 3 rounds)

| profile | settings | small JDBC c1 p50/p95 ms | small JDBC c32 p50/p95 ms | ADBC c1 p50 / c32 p95 ms | SF1 JDBC s | SF1 Trino s | SF10 JDBC s | SF10 Trino s | scan MB/s 1/4/8 clients | heavy4 slowest s | RSS peak GB |
|---|---|---|---|---|---|---|---|---|---|---|---|
| default | `duckDbThreads=2` (build default) | 18.8 / 43.5 | 70 / 308 | 14.8 / 293 | 2.38 | 6.04 | 19.4 | 35.1 | 944 / 2942 / 3130 | 3.5 | 4.4 |
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

- `duckDbThreads=4` (was 2). A size-aware default (for example cores / 4, between 2 and 8) would keep the balance on bigger hosts.
- `flightBackpressureThresholdBytes=16777216` (16 MiB, was 256 MiB): no loss measured, and per-stream buffering times the number of
  concurrent streams must fit `MaxDirectMemorySize`; with several endpoints per node the 256 MiB default fails.
- Keep: `duckDbStreamResults=true`, `duckDbExternalFileCache=false`, `resultMode=complete`, `endpointsPerServer=1`,
  `duckDbStatementCacheSize=64`, `duckDbWarmConnections=8`, `ioParallelism` auto, `batchSize=65536`, memory limit unset, G1.
- Deployment: enable HDFS short-circuit reads; parquet with ~128k-row groups and several files per table.

## Open points

- Storage comparison HDFS vs S3 on the stand and the 3-node cluster run with the best settings were not measured in this campaign
  (stand unavailable for valid measurements); the emulated S3 in CI is too slow to compare storages.
- ADBC (Go Flight SQL driver): rarely (about 3 of 45 runs with 8 clients connecting at once) a connect fails with
  "failed to update autocommit: rpc error: code = Unimplemented" although the server advertises no transaction support.
