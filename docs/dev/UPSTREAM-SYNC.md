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
| S9 | da6a357 | `arrowflight.table_statistics` action (row count, bounds, null counts from footers, NDV estimate) | Ours has footer statistics for planning, no client action. | **open** — compare with the Trino branch's statistics (T1); one action for both connectors. |
| S10 | 9f66812 | S3 data dir via Hadoop s3a (listing/footers) + DuckDB httpfs | Ours: `flight-plugin-s3` (own listing + httpfs secrets, lockdown-aware). | **open** — compare listing/footer speed on MinIO; expectation: ours stays (catalog SPI, lockdown), port only measured wins. |
| S11 | a3fb99b→ed97986, ec2f494 | Flight batch compression: LZ4/ZSTD (removed: negative), then parallel ZSTD opt-in | None. | **open, low** — upstream's own bench was negative for LZ4 and mixed for ZSTD; only for slow links. |
| S12 | f998bf7 | Parse cache: do not cache the parse of queries > 256 KB (join key reduction sends huge IN lists) | Our `ParquetQueryParser` parse cache — check its bound. Overlaps Next step 6 (statement cache bound). | **open** — do together with Next step 6. |

StarRocks fork patches (runtime filters to the server, aggregate predicate pushdown, partial aggregates with SUM cast
to DOUBLE, decimal AVG): connector-side, not this repo. Note for the coordinator in STATUS Open questions.

## feat/trino-flight-connector — server side only

| # | Feature | Decision |
|---|---|---|
| T1 | `QueryStatistics` (row count/size estimate) returned for a query (`FlightSqlProducer`, `QueryPlanner`, `SchemaConverter`) | **open** — evaluate together with S9. |
| T2 | Client retry / configuration tweaks in `flight-client-core` | **open, low**. |

The connector itself (`flight-trino`: Kerberos auth, statistics, partitioning, types) lives in a separate repo on
our side; comparison goes to the coordinator, not here.

## perf/pushdown-statistics (August)

No merge base with upstream `main` (rewritten history); Spark-side aggregate pushdown cost gate and Q6/Q17 fixes on
the old layout. **open, low** — check whether the Q6/Q17 aggregate-pushdown fixes (88c0a16, 49a5a5c) apply to
`flight-spark`.

## Dependabot upstream

hazelcast 5.7.0, netty 4.2.19, grpc 1.85.0 → folded into STATUS Next step 1b.
