# Upstream sync review

Upstream: github.com/VadimSurpin/arrow-flight-server (read-only for us). Fetch:
`git fetch https://github.com/VadimSurpin/arrow-flight-server.git '+refs/heads/*:refs/remotes/upstream/*'`.
Rule: per feature try ours and theirs, keep the more effective variant; our features stay. One decision per row,
with numbers where we have them. Status values: **ported**, **ours already** (same effect, ours kept), **rejected**
(reason), **open** (still to evaluate; the next session takes the first open row).

Reviewed 2026-10-23 against upstream `feat/starrocks-native-integration` @ 2e31e1f, `feat/trino-flight-connector`
@ its head of 10-22, `perf/pushdown-statistics` (August). Upstream `main` is still dbacd8d (our base).

## feat/starrocks-native-integration — server side

| # | Upstream commit | Feature | Ours | Decision |
|---|---|---|---|---|
| S1 | 7b277f7 | Exit when the server fails to start (non-daemon Hazelcast/gRPC threads keep a dead process alive); join-timeout elapsed fix | Our `start()` exits on failures inside its serving `try`, but failures before it (storage open, cluster join timeout, Dagger build) and any `Error` escaped `main` and left the process alive. Elapsed-time fix was already in ours. | **ported** 2026-10-23: `main` → `startOrReport` → `System.exit(1)`; regression tests in `HadoopArrowFlightServerUnitTest`. |
| S2 | d0cdfc7 | Stream DuckDB results (`jdbc_stream_results`) | Already ours: `duckDbStreamResults` (default true, env `DUCKDB_STREAM_RESULTS`). | **ours already** |
| S3 | 2e31e1f | Spill directory (`duckDbTempDirectory`) + cap `duckDbMaxTempSize` (default 20GiB) | Ours has the spill root (`duckDbTempDirectory`, a sub-directory per connection, inside the lockdown) but no size cap: DuckDB's default cap is 90 % of free disk, per instance. | **ported** 2026-10-23 (cap only): `duckDbMaxTempSize` / `DUCKDB_MAX_TEMP_SIZE`, per connection (our instances are per connection, see S5), validated as a size at startup, refused as a plugin setting. Default unset (DuckDB's 90 % of free disk) instead of upstream's 20GiB: a fixed per-connection default would mean 20GiB × connections; size it on the stand as spill disk / expected concurrent connections. Tests: `DuckDbAdapterTest.connectionsTakeTheSpillSizeCap`, `ConfigAdapterTest.getConfigDuckDbMaxTempSize`. |
| S4 | cb45f1c | `preserve_insertion_order=false`, `streaming_buffer_size=64MiB` | Not set (DuckDB defaults: true / about 1 MB). | **ported as opt-in** 2026-10-23: `duckDbPreserveInsertionOrder` (default true) and `duckDbStreamingBufferSize` (default unset), per connection, refused as plugin settings. Order audit: per-endpoint SQL renders the query's own ORDER BY/LIMIT (DuckDB honours ORDER BY in both modes; LIMIT without ORDER BY has no defined rows anyway); complete-mode aggregate merge is order-free; the equality-delete barrier (`LIMIT 2^63-1` in `CatalogViews.icebergSource`) still keeps filters above the scan with the setting off (EXPLAIN checked on DuckDB 1.4.5, same count); full CI with both forced on (audit branch `claude/s4-order-audit`, see STATUS log) found no order-dependent test. Default stays DuckDB's until measured on the stand (TPC-H SF10 via JDBC and Trino, before/after, both settings). |
| S5 | f5f11b3 | One shared DuckDB database, worker connections are `duplicate()`s (memory_limit and threads become global) | Ours opens one in-memory DuckDB instance per worker thread: `duckDbMemoryLimit` and `duckDbThreads` apply per connection, so the real ceiling is limit × live connections. Our design depends on per-connection isolation: catalog views refreshed per connection (`CREATE OR REPLACE VIEW` in `CatalogViews`, not TEMP — they would be shared), per-connection temp directory, secrets, lockdown. A shared database would let concurrent queries overwrite each other's views → wrong results. | **rejected as-is** (correctness). Open question recorded: document the per-connection meaning now; a shared database would need per-query view names (or schema per session) first. |
| S6 | 1d3f331 | `endpointsPerServer`: split one node's files into several endpoints (by size) | Ours: one endpoint per node per query (with backup locations). | **ported as opt-in** 2026-10-24: `endpointsPerServer` / `FLIGHT_ENDPOINTS_PER_SERVER` (default 1 = ours unchanged; below 1 refused, upstream clamps). Applies after our node choice (locality, load, rendezvous ties) to both planners (inventory and listing, catalog tables included); each part is its own endpoint with its own backups (`Backups.forEndpoint`) and load accounting, ticket format unchanged. Fixed against upstream: queries with a `singleNodeFeature` (OFFSET, HAVING, DISTINCT aggregates) are never split — upstream split them on a one-node cluster, where our check (`servers > 1`) does not fire, giving wrong answers; joins stay one endpoint. Deterministic ties (by path), parts keep the files' order. Tests: `QueryPlannerTest.splitBySize*`, `planSplitsOneNodesFilesIntoSeveralEndpoints`, `splitEndpointsKeepTheirBackupLocations`, `singleNodeQueriesAreNeverSplit`, `withoutLocalitySplitsOneNodesFiles`, `ConfigAdapterTest.getConfigEndpointsPerServer`. Benefit unmeasured: DuckDB already scans a node's files with `duckDbThreads`; worth it only where one client stream is the bottleneck (Trino/StarRocks splits) — stand measurement in STATUS Open questions. |
| S7 | 6088b2d | `decimalWire=narrow` (DECIMAL p≤18 as int64/int32 + field metadata) | None. Changes the wire types clients see. | **open, wire change** — opt-in only; needs a client-side reader in our Trino/StarRocks/ADBC connectors before it is useful. Decision for the maintainer (Open questions). |
| S8 | 75f2d0c, 794ae7c | Client runtime IN-filters and bloom filters on plain scans (ticket blocks, field 15 / "ARF2") | None. Extends the ticket format. | **open, wire change** — useful for our StarRocks connector (join-heavy TPC-H). Ticket-format compatibility must hold within a release; write into Open questions first. |
| S9 | da6a357 | `arrowflight.table_statistics` action (row count, bounds, null counts from footers, NDV estimate) | Ours has footer statistics for planning, no client action. | **ported** 2026-10-24 (without NDV): one action for both connectors, `arrowflight.table_statistics` (`TableStatisticsService`, answered by `FlightSqlProducer.doAction`, listed in `ListActions`). Same name, request and answer fields as upstream (their StarRocks `TableStats` parses it unchanged, it ignores unknown fields), plus `rowsExact`, `bytes`, `files`. Fixed against upstream: upstream lists the data directory with its own Hadoop file system (bypasses our `DataStore`: lakeFS versions, S3 plugin) and on a multi-node local-disk cluster reports only the answering node's files — ours reads through the `DataStore` in one read scope and refuses that cluster shape (`UNIMPLEMENTED`); upstream has no catalog tables — ours reports the current snapshot's row count/size, an upper bound (`rowsExact: false`) with delete files; upstream checks access by building SQL from unvalidated names — ours validates names first; unsigned ints, TIME, NaN bounds handled. Not ported yet: upstream's background `approx_count_distinct` NDV estimate (`ndv` stays null) — needs a DuckDB connection outside a query and lockdown-safe paths; catalog column bounds (Iceberg manifests carry them). Tests: `TableStatisticsServiceTest` (bounds and nulls vs DuckDB over the same files), `FlightSqlProducerTest.tableStatistics*`, `listActionsIncludesTableStatistics`. |
| S10 | 9f66812 | S3 data dir via Hadoop s3a (listing/footers) + DuckDB httpfs | Ours: `flight-plugin-s3` (AWS SDK v2 `ListObjectsV2` listing through our `DataStore`, footers by ranged GETs in `S3ReadableFile`, httpfs credentials as DuckDB secrets, lockdown-aware). | **rejected as-is** 2026-10-24 (structure), speed comparison pending: upstream skips `allowed_paths` for an S3 data dir (the file lockdown is off: user SQL may read any path the process can), sets the keys as global `SET s3_*` per connection instead of scoped secrets, has one store per server and no catalog SPI, and adds hadoop-aws + AWS SDK v1/v2 bundles to the server. No MinIO in cloud sessions (no Docker daemon, dl.min.io blocked), so the listing/footer speed comparison (s3a vs our plugin on 1000+ objects, SF10 lineitem footers) goes to the stand (STATUS Open questions); port only a measured win (e.g. listing page size or footer read size) into our plugin. |
| S11 | a3fb99b→ed97986, ec2f494 | Flight batch compression: LZ4/ZSTD (removed: negative), then parallel ZSTD opt-in | None. | **deferred** 2026-10-24 — upstream's own bench was negative for LZ4 and mixed for ZSTD; it pays only on slow links, which the stand does not have, and compressed IPC buffers change what clients must decode (every client needs the codec: Trino plugin, StarRocks connector, ADBC). Not ported without a client need; question in STATUS Open questions. |
| S12 | f998bf7 | Parse cache: do not cache the parse of queries > 256 KB (join key reduction sends huge IN lists) | Our `ParquetQueryParser` parse cache (1024 entries, no size bound), `ExecutionService` schema cache (1024), `DuckDbAdapter.StatementCache` (`duckDbStatementCacheSize` per thread). | **ported** 2026-10-24 and widened to all three caches (`QueryCacheLimits.MAX_CACHED_QUERY_CHARS` = 262 144): upstream bounded only the parse cache, our schema and statement caches would still pin the same texts; a long statement keeps one uncached slot per thread (closed when the next long text comes). Counters `arrowflight_query_cache_oversize_total` / `_evictions_total` per cache (Next step 6). Tests `ParquetQueryParserUnitTest.doesNotCacheVeryLongQueries`, `DuckDbAdapterTest.statementCacheKeepsLongTextsOutOfTheLru` / `streamSqlRunsQueriesLongerThanTheCacheLimit`, `ExecutionServiceTest.resultSchemaCachesOnlyQueriesWithinTheLimit`, `MetricsServiceTest.exposesQueryCacheMetrics`. |

StarRocks fork patches (runtime filters to the server, aggregate predicate pushdown, partial aggregates with SUM cast
to DOUBLE, decimal AVG): connector-side, not this repo. Note for the coordinator in STATUS Open questions.

## feat/trino-flight-connector — server side only

| # | Feature | Decision |
|---|---|---|
| T1 | `QueryStatistics` (row count/size estimate) returned for a query (`FlightSqlProducer`, `QueryPlanner`, `SchemaConverter`) | **rejected** 2026-10-24 in favour of S9: action `arrow-flight-query-statistics` (query text → 16 bytes: total bytes, total rows) is a subset of S9's per-table answer (S9 also allocates no ticket and no node load); one statistics action for both connectors. Our Trino plugin (separate repo) should call `arrowflight.table_statistics` (note for the coordinator in STATUS). |
| T2 | Client pool identity and per-call headers in `flight-client-core` (f265ea3) | **ported** 2026-10-24: `Configuration.getConnectionString` hashes every setting a pooled client is built with (TLS verification, trust store, certificate, schema/routing, retries, timeout, allocation limit; length-prefixed), so a verifying configuration no longer gets a client built with `crtVerify=false`; `SCHEMA` / `ROUTING_TAG` / `ROUTING_QUEUE` go with every call (`Client.callOptions`) and the Kerberos refresh, not only the first handshake (our server ignores them; gateways may route on them). Upstream's `QueryStatistics` client action not ported (T1 rejected). Tests: `ClientHeadersTest` (in-process Flight server records headers per method; pool separation), `ClientRetryTest.connectionIdentity*` — all fail on the old code. |

The connector itself (`flight-trino`: Kerberos auth, statistics, partitioning, types) lives in a separate repo on
our side; comparison goes to the coordinator, not here.

## perf/pushdown-statistics (August)

No merge base with upstream `main` (rewritten history); Spark-side changes on the old single-module layout.

| # | Upstream commit | Feature | Decision |
|---|---|---|---|
| P1 | 88c0a16 | Q6: decline an empty aggregation (rendered `select  from`); Q17: advertise and cast pushed decimal SUM as `Decimal(min(38, p + 10), s)` | **ours already** (checked 2026-10-24): `SparkPushdown.translate` declines empty aggregations, `DuckDbAdapter.renderSum` / `MetadataService` use p + 10, with the same regression tests (`FlightScanBuilderTest`, `MetadataUtilsTest`, `DuckDbAdapterTest`). |
| P2 | 49a5a5c | Q17 benchmark query casts `AVG(l_quantity)` (Spark ANSI overflow in Spark's own arithmetic) | **not applicable**: benchmark script only, no connector or server code. |
| P3 | 482d24b | Spark scan reports post-pushdown statistics: pushed filter selectivity (textbook defaults) and global aggregation scale the server's raw rows/bytes so Spark can broadcast filtered dimensions (their Q2/Q3/Q5/Q7 lost to direct reads as sort-merge joins) | **ported** 2026-10-24 (`flight-spark`): a pushed global aggregation always reports 1 row (exact); the filter selectivity is opt-in, read option `statistics.filterSelectivity=true`, because a heuristic underestimate can make Spark broadcast a large table (no NDV/histograms behind it). Fixed upstream's NOT of an unknown predicate (1 - 1.0 = an empty scan → broadcast of the whole table): it now keeps every row. Tests `ScanSelectivityTest`, `TableTest.estimates*`. Stand measurement pending (Open questions). |
| P4 | 7cf1544 | Cost gate: decline aggregate pushdown when estimated post-filter rows < `aggregate.pushdown.minRows` (500k), raw row count cached per table name | **deferred to the stand**: it rests on the same heuristic selectivity as P3 and a per-name row cache; their only win was Q6 (1.02× before). Decide after the P3 measurement. |

## Dependabot upstream

hazelcast 5.7.0, netty 4.2.19, grpc 1.85.0 → folded into STATUS Next step 1b.

## Speed comparison 2026-10-26

Upstream `main` dbacd8d (our fork point) and `feat/starrocks-native-integration` 07631d7 (CI shaded jars) against ours
57d252b, one node, same machine and conditions: 8 GB container, `-Xmx3g -XX:MaxDirectMemorySize=2g`, JDK 21, server
co-located with the DataNode (short-circuit reads), metrics off, same TPC-H SF1/SF10 Parquet files on HDFS and on S3 (MinIO),
same clients (Flight SQL JDBC 18, ADBC Flight SQL, raw Flight scan, Trino 455 with our connector), 3 interleaved rounds with a
load gate. Variants: ours defaults (`duckDbThreads` 4) and 8 threads; upstream `main` defaults (2) and 8; upstream native
defaults (2), 4, 8, and 4 + `flightCompression=zstd` (4 compression threads). S3 has 1 valid round (the object store went
down during the run); upstream `main` has no S3 support.

To make upstream run at all (applied to every build): a tpch-only data directory (upstream aborts its catalog load on a
BINARY column), schema-qualified SQL (upstream has no default schema), its own DuckDB 1.5.6 extension set (it cannot load
1.4.x extensions; hadoopfs built from our patched sources for v1.5.6 — upstream's own image pairs a v1.4.1 hadoopfs with
DuckDB 1.5.6, which does not load), Hadoop 3.5.0 + hadoop-aws on its class path.

**Correctness (a wrong answer disqualifies that workload):**

| | upstream `main` | upstream native |
|---|---|---|
| JDBC | no prepared statements: every JDBC statement "Not implemented" | `AVG` not implemented (21/22 TPC-H fail); q06 wrong (DECIMAL(38,4) sum returned with scale 1) |
| `LIMIT n` without ORDER BY | ignored: the whole table is returned | ignored |
| parameterized prepared statement | — | not supported |
| small-query load (8/32 clients) | OOM-killed (the LIMIT full scans) | OOM-killed |
| full-table scan | OOM / stream errors (results not streamed) | correct |
| TPC-H SF10 via Trino | 4-7/22 | 19-21/22 ("Server sendMessage() failed") |
| TPC-H SF1 via Trino | 22/22 | 22/22 |
| ZSTD batches | — | not readable by Java clients without `arrow-compression` (JDBC 18, Trino connector); ADBC reads them |

**Speed (best variant per side; HDFS):**

| workload | ours | upstream | winner |
|---|---|---|---|
| TPC-H SF10 / SF1 via JDBC | 7.67 s / 1.47 s (8 threads) | n/a (fails / wrong) | ours |
| heavy4 (q01 q09 q18 q21 at once) | 2.79 s | n/a | ours |
| TPC-H SF10 via Trino | 32.5 s | n/a (19-21/22) | ours |
| TPC-H SF1 via Trino | 6.17 s | 5.14 s (native, 8 thr) | upstream (0.83x time) |
| small queries, aggregate p50/p95 | 21.9 ms JDBC c1 p50 | disqualified (LIMIT) | ours |
| point lookup c1 p50 JDBC / ADBC | 25.0 / 19.4 ms | 16.1 / 11.6 ms | upstream (~0.6x) |
| filtered count c1 p50 JDBC / ADBC | 37.4 / 33.5 ms | 18.9 / 19.0 ms | upstream (~0.55x) |
| metadata calls c1 p50 | 2.4-3.8 ms | 2.4-4.4 ms | tie / ours |
| raw scan 1 stream | 954 MB/s | 1510 MB/s (native, 8 thr) | upstream (1.6x) |
| raw scan 4 / 8 streams | 2963 / 3360 MB/s | 3100 / 3444 MB/s | tie |
| server RSS peak | 3.8-4.2 GB | 6.9-7.7 GB (then OOM) | ours |
| S3, 1 round: scan 1/4/8 streams | 746 / 2549 / 3203 MB/s | 729 / 3083 / 3324 MB/s | tie / upstream at 4 |

Defaults vs defaults gives the same picture. The exception is the single-stream scan, where ours wins at upstream's
2-thread default (954 vs 840 MB/s).

**Why, and what to port:**

- DuckDB 1.5.6 alone (same files, no server) answers the point lookup in 5.6 ms vs 10.6 ms and the filtered count in 9.6 vs
  15.8 ms. That is about half of the small-query gap. → STATUS Next step 14 (upgrade to 1.5.6; the hadoopfs build for 1.5.6
  works).
- The rest of the gap, and the Trino SF1 gap, is our per-query server overhead. Upstream `main` keeps per-connection DuckDB
  instances like ours and is still faster, so the shared database (S5) is not the reason. → Next step 15 (profile and trim the
  point-query path).
- Single-stream scan: upstream defaults to `preserve_insertion_order=false` and `streaming_buffer_size=64MiB` (our S4 opt-ins).
  → Next step 16 (measure them on the stand; propose them as defaults for plain scans only if answers stay unchanged).
- Not ported: ZSTD batches (S11 stays deferred: no Java client in our set reads them, and there is no network bottleneck),
  the shared database (S5, correctness), `decimalWire` (S7, changes the types clients see). Upstream's LIMIT and decimal
  behaviour are bugs, not features.
