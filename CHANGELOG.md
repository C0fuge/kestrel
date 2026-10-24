# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- Spark connector: a scan with a pushed global aggregation (no GROUP BY) reports one row to
  Spark's optimizer instead of the raw table size. Read option `statistics.filterSelectivity`
  (default `false`): `true` scales the reported rows and bytes by the estimated selectivity of the
  pushed filters (equality 0.1, range 1/3, IN 0.1 per value up to 0.5, AND/OR/NOT combined), so
  Spark can broadcast selectively filtered tables in joins. Heuristic: an underestimate can make
  Spark broadcast a large table (idea from upstream 482d24b; negating an unknown predicate keeps
  every row here instead of none).
- Setting `duckDbMaxTempSize` (`arrowflight.duckdb.maxTempSize`, env `DUCKDB_MAX_TEMP_SIZE`):
  cap of the DuckDB spill directory, e.g. `20GiB`. It holds per DuckDB connection (each is an
  instance of its own), so a node may spill up to the cap times its live connections. Unset =
  DuckDB's own cap (90 % of the free disk space, per connection). Plugins may not set
  `max_temp_directory_size` (idea from upstream 2e31e1f, whose cap is per server).
- Settings `duckDbPreserveInsertionOrder` (`arrowflight.duckdb.preserveInsertionOrder`, env
  `DUCKDB_PRESERVE_INSERTION_ORDER`, default `true`; only `true`/`false` accepted) and
  `duckDbStreamingBufferSize` (`arrowflight.duckdb.streamingBufferSize`, env
  `DUCKDB_STREAMING_BUFFER_SIZE`, e.g. `64MiB`; unset = DuckDB's own): `false` lets DuckDB return
  the rows of a query without ORDER BY in any order (parallel scan threads do not wait for each
  other), the buffer lets a streamed result run ahead of a slow Flight consumer. Both are opt-in
  until measured on the stand; plugins may not set them (upstream cb45f1c turns both on).
- Setting `endpointsPerServer` (`arrowflight.cluster.endpointsPerServer`, env
  `FLIGHT_ENDPOINTS_PER_SERVER`, default 1): partial results split one node's files into up to that
  many endpoints of similar size (each with its own backup locations and load accounting), so a
  client can read a node over several parallel streams. Queries that need every row in one place
  (OFFSET, HAVING, DISTINCT aggregates) and joins are never split; values below 1 are refused
  (idea from upstream 1d3f331, which also split those single-node queries on a one-node cluster).
- Flight action `arrowflight.table_statistics` (JSON `{schema, table, columns}`): row count,
  per-column bounds, null counts and sizes from the Parquet footers of a data-directory table
  (cached per file set), or the row count and size of a catalog table's current snapshot (an upper
  bound, `rowsExact: false`, with delete files); no ticket, no node load; access checked like
  `SELECT * FROM` the table; listed by `ListActions`. Same name and request as upstream da6a357,
  answer extended by `rowsExact`, `bytes`, `files`; unlike upstream it refuses a multi-node cluster
  on local disks (a node sees only its own files) and estimates no distinct counts yet.

### Fixed
- `flight-client-core`: pooled clients were keyed only by host, port, user and secret, so a
  configuration that asks for certificate verification could get a client built without it (or
  with another trust store, schema/routing headers, retries, timeouts or allocation limit). The pool
  key now covers every setting (hashed). Schema and routing headers (`SCHEMA`, `ROUTING_TAG`,
  `ROUTING_QUEUE`) went only with the handshake; they now go with every call and with the Kerberos
  session refresh (from upstream `feat/trino-flight-connector`, UPSTREAM-SYNC T2).
- A server whose start failed before it began serving (storage that cannot be opened, a
  cluster join timeout) or with an `Error` stayed alive without ever becoming ready: Hazelcast
  and gRPC threads are not daemons. `main` now logs the failure and exits with status 1, so a
  supervisor restarts it (ported from upstream 7b277f7).
- HDFS files whose path has escaped characters (Hive partitions such as `l_shipmode=REG AIR`,
  `tag=50%25`) failed with "Unable to get file info": DuckDB was handed the URI-escaped path,
  and the hadoopfs extension reads paths literally. Paths now reach DuckDB decoded.
- Iceberg tables with equality deletes failed in DuckDB 1.4.5 ("Equality deletes need the
  relevant columns to be selected", or an INTERNAL error that invalidated the connection on most
  other projections, also on queries of one non-key column when the table has position deletes
  and renamed columns). Views of such snapshots now read every column
  (`hash(<all columns>) IS NOT NULL`) below a `LIMIT` no table reaches, so the query's own
  filters stay above the scan: a filter pushed into the scan (also through a `MATERIALIZED` CTE)
  applied the deletes wrongly. Correct, but every query reads the whole snapshot.
- Iceberg tables written by Spark / Iceberg HadoopFileIO name their files `file:/...` and
  `s3a://...`; DuckDB's lockdown refused them ("file system operations are disabled"). Catalog
  roots now also allow their other scheme forms (`file:` for plain paths, `s3`/`s3a`/`s3n`; S3
  secrets are scoped to all three).
- Iceberg REST transport failures were reported as "null"; they name the URI and the exception,
  and a catalog outage during planning or describe is UNAVAILABLE. A REST table whose metadata
  lies outside the roots is hidden (WARN once, NOT_FOUND). iceberg-hadoop lists only
  directories in the Hadoop layout. `fs.file.impl` / `fs.hdfs.impl` are set unless configured
  (merged Hadoop class paths lost the registrations: "No FileSystem for scheme file").
- lakeFS: a partial-mode explicit JOIN re-listed the tables at execution and could mix two
  commits in one answer; it now reads the ticket's files. All tables of a view refresh or plan
  are listed in one read scope (one branch resolution), and complete-mode views refresh as soon
  as the branch head moves (were up to 60 s stale).
- lakeFS error texts: missing credentials, HTTP 401/403, unreachable or unknown hosts, unknown
  branches/tags/commits and repositories are named; a partial plan with storage down is
  UNAVAILABLE "Cannot read the table's files: ..."; complete-mode failures keep the store's
  context instead of a bare root cause.
- lakeFS tags and short commit ids are resolved once to the full commit id and read through it
  (the S3 gateway served tags 2-3x slower).
- A Hive table with a partition outside `arrowflight.hive.roots` was hidden from queries but
  still listed by GetTables until a query had planned it; the table list now checks partition
  locations too.
- A Hive Metastore that hangs (no answer) blocked queries for minutes: every view refresh asked
  the metastore once per schema and table, and every planning call waited for the timeouts again.
  A failing catalog is now asked once per view refresh, the metastore client fails at once for
  5 s after the metastore could not be reached and drops its idle connections after a broken one.
  The schema of a query on a table whose catalog is down is UNAVAILABLE with the catalog's error,
  not NOT_FOUND "Could not find Arrow schema for query".
- With a table catalog configured (Iceberg, Hive) and a table in the data directory, the first
  refresh of the table inventory after its 30 s TTL called itself until the stack overflowed;
  every later call failed with "Application error processing RPC".
- Wrong answers and crashes of large `GROUP BY` queries (TPC-H q04 at SF10: duplicate groups,
  SIGSEGV in `TupleDataCollection::Combine`, INTERNAL errors) with 2 DuckDB threads: DuckDB
  1.4.1-1.4.3 skipped repartitioning in the hash aggregate's combine (duckdb#20357, fixed in
  1.4.4). DuckDB is now 1.4.5.
- DuckDB connections of ended gRPC threads were never closed: memory grew with every new
  executor thread. They are now closed, results are streamed instead of buffered, the DuckDB
  external file cache is off by default and the prepared-statement cache is bounded.
- All DuckDB connections spilled into the same `.tmp` directory; each now has its own.
- The schema in `FlightInfo` now always equals the schema of the stream (column order, `SUM`
  types, join column names): it is taken from DuckDB for the same SQL.
- `MIN`/`MAX` from Parquet footers returned NULL for dates and failed for decimals; the footer
  path is now used only for integer and date columns.
- `GetTables` returned table schemas that stock drivers could not read.
- A client cancel now interrupts the DuckDB query; before, it ran to its end.
- On shared storage a dead node no longer makes its files unavailable: they go to live nodes.
  Nodes beat every 3 s and are left out after 10 s (was 15 s / 45 s).
- Partial results: a select item other than a column or a plain `count`/`sum`/`min`/`max`
  (`CAST(sum(x) AS DECIMAL(38,2))`, `avg(x)`, `sum(x) / count(*)`, `x + 1`) failed with
  "Binder Error: Referenced column ... not found", and `FILTER (WHERE ...)` or `OVER (...)` on
  an aggregate was silently dropped (wrong results). Such queries now get the complete answer
  from one endpoint (logged as `query=answeredCompletely`); a cluster that keeps its files on
  local disks refuses them with the reason.
- Partial results streamed unsigned integers unwidened (uint64) while `GetTables` and complete
  answers said `decimal128(20,0)`; with `widenUnsignedTypes` (default) both modes now stream the
  same wider signed types (uint8/16/32/64 as int16/int32/int64/decimal128(20,0)).
- A DuckDB statement closed by a failed run is prepared again instead of failing every later
  run of the same SQL with "Statement was closed".
- After a DuckDB error that invalidates its database (FATAL or INTERNAL, e.g. a hash-aggregate
  failure under memory pressure) the thread's DuckDB is closed and replaced; before, every
  later query on that thread failed with "database has been invalidated" until a restart.
- Releasing the load of an endpoint no longer puts a node that was removed as stale back into
  the registry (where, without heartbeats, it counted as a node that had just started).

### Added
- Development CI `dev-ci.yml`: on pushes to `main` and on demand, one full build with every test
  except `perf`/`real-services` (Iceberg and fake-S3 integration tests use the DuckDB 1.4.5
  extensions of the private `ci-assets` release), Checkstyle and SpotBugs; a manual `package` job
  publishes a linux-only jar, the plugins dir, START-NOTES and SHA256SUMS as a pre-release
  `server-<version>-<sha>`. Upstream's `ci.yml` is manual-only in this fork.
- `SparkJoinIntegrationTest` moved from `flight-server` (where the `flight` Spark source is not on
  the classpath, so it always failed) to `flight-spark`, reading the server's `test_db`.
- Endpoint failover for partial results: every endpoint lists up to `endpointBackupLocations`
  (default 2, 0 = off) other live nodes after its planned node, those that can read all its
  files (HDFS/S3: any node; local disks: replica holders), best first (local bytes, load,
  rendezvous hash). Tickets are unchanged and run on any node; a node running an endpoint
  planned elsewhere logs `execution=failover` and takes over its load accounting. The planning
  node lists itself as a backup only for clients that send `x-flight-location-failover: 1`
  (older Trino plugins read every endpoint listing it from it). Clients may send keepalive
  pings every 5 s. Complete answers are unchanged.
- `ORDER BY`, `LIMIT` and `OFFSET` (see README for multi-node semantics).
- Prepared statements without parameters, empty key metadata, metadata endpoints without a
  fixed `0.0.0.0` location: the stock Arrow Flight SQL JDBC driver works.
- Optional access token (`authToken` / `FLIGHT_AUTH_TOKEN`).
- Complete-answer mode, the default (`resultMode`): one endpoint with the complete answer of any
  single read-only DuckDB query over views of all tables; clients ask for the old per-node parts
  with the header `x-flight-result-mode: partial` (the Trino and Spark connectors do).
- DuckDB lockdown (`duckDbLockdown`): only the data and spill directories are readable, no
  extension installs or loads by itself, settings are locked.
- Unsigned integer columns shown as the next wider signed type in complete mode
  (`widenUnsignedTypes`).
- `?` parameters of prepared statements in complete mode: the server keeps nothing and answers
  the values with a new statement handle carrying the SQL and the values; DuckDB binds them.
- Settings `duckDbStreamResults`, `duckDbExternalFileCache`, `duckDbStatementCacheSize`,
  `duckDbTempDirectory`.
- Storage plugins: module `flight-spi` (plugin API: stores, typed DuckDB setup, namespaced
  settings), plugins loaded from `<arrowflight.plugins.dir>/<name>/` (`FLIGHT_PLUGINS_DIR`,
  default `../plugins` beside the jar) in their own class loader, only when the data directory
  needs them; `SHA256SUMS` checked (`arrowflight.plugins.requireChecksums`). Plugins describe
  their DuckDB setup; the server writes and checks the SQL and reads the lockdown back at
  startup (settings, allowed directories, secrets) before the node joins the cluster.
- lakeFS data directories (`lakefs://<repository>/<ref>/<prefix>`, plugin `lakefs`): read
  through the lakeFS S3 gateway; a branch is pinned to its head commit per listing, so all
  endpoints of a query read the same commit. Settings `arrowflight.lakefs.*` /
  `FLIGHT_LAKEFS_*`.
- Table catalogs besides the data directory (`catalog=directory,iceberg-hadoop,iceberg-rest`),
  from plugins; their schemas appear next to the data directory's (a name the data directory
  has wins). Plugin `iceberg`: Iceberg tables of a warehouse directory in the Hadoop layout
  (`iceberg-hadoop`) or of an Iceberg REST catalog such as Gravitino or Polaris (`iceberg-rest`,
  bearer token or OAuth2 client credential from `FLIGHT_ICEBERG_REST_*` only). Complete answers
  read the snapshot with DuckDB's `iceberg` extension (position and equality deletes, renamed
  columns); partial results spread the snapshot's data files over the nodes, read by Parquet
  field ID, with the snapshot pinned in the tickets; tables with delete files, nested column
  changes or no data files, and joins of catalog tables, get the complete answer instead.
  Needs the DuckDB `iceberg` and `avro` extensions (`duckDbIcebergExtension`,
  `duckDbAvroExtension` or `arrowflight.duckdb.extensionDir`).
- Hive Metastore catalog (`catalog=hive`, plugin `hive`, Thrift client of Hive 3.1.3 without
  Hadoop): databases are schemas, Parquet tables are tables (other formats, views and ACID tables
  are skipped with a log line); partitions, their locations and their values come from the
  metastore (never from directory names; `__HIVE_DEFAULT_PARTITION__` is NULL), columns are read
  by name with the metastore's types, partition columns are typed constants. Every table and
  partition location must lie under `arrowflight.hive.roots`, else the table is hidden. Partial
  results ship each file's partition values in the ticket (partitions read once, at planning) and
  leave out partitions that simple predicates on partition columns rule out. Iceberg tables
  registered in the metastore are read through the iceberg plugin when it is installed.
  Metadata is cached (`namesTtlSeconds`, `metadataTtlSeconds`); a metastore that cannot be
  reached is an error (GetTables: UNAVAILABLE), never an empty schema.
- Plugin SPI 1.1 (additive): `DataStore.files(dir, recursive)`, `Catalog.pinnedColumns`, table
  formats other plugins' catalogs use (`TableFormat`, descriptor `provides.format`).
- Apache-2.0 LICENSE
- `CONTRIBUTING.md`, `SECURITY.md`, `CHANGELOG.md`
- Issue and PR templates
- `dependabot.yml` for automated dependency updates
- Release workflow triggered on `v*` tags
- `CODEOWNERS` for automatic review routing
- Package renamed from `net.surpin` to `pro.surpin`

### Changed
- Metadata endpoints (catalogs, schemas, tables, SqlInfo, type info) carry no location by
  default: clients read them over their own connection (stock JDBC metadata 1.4-1.6x faster in
  plaintext, 3.2x with TLS). `metadataEndpointLocation=advertised`
  (`FLIGHT_METADATA_ENDPOINT_LOCATION`) restores the advertised server URI.
- DuckDB is pinned to 1.4.5.0 (was 1.4.1.0): the hadoopfs and httpfs extension files must be
  the v1.4.5 builds (`extensions/v1.4.5/linux_amd64/`). The server needs JDK 21 (Netty 4.2 + Arrow
  fail on JDK 24+).
- HDFS: the server sets the hadoopfs extension's DataNode timeouts, `duckDbHdfsConnectTimeoutMs`
  (default 5000) and `duckDbHdfsReadTimeoutMs` (default 15000), as `hdfs_connect_timeout_ms` /
  `hdfs_read_timeout_ms`; they win over `LIBHDFS3_CONF`, `0` leaves them to the extension. An
  extension built without these settings keeps its own timeouts (one warning).
- S3 moved into the plugin `s3` (`plugins/s3`, built by `tools/make-plugin-dir.sh`): the AWS
  SDK is no longer in the server jar, and an `s3://` data directory without the plugin is a
  startup error naming it. All S3 settings keep their names (also `arrowflight.s3.*`,
  `FLIGHT_S3_*`, `etc/plugins/s3.properties`). `arrowflight.duckdb.extensionDir` may replace
  the per-extension file settings (`<dir>/v1.4.5/linux_amd64/<name>.duckdb_extension`).
- The DuckDB lockdown is read back at startup also for HDFS (hadoopfs) data directories.
- Calcite and with it Arrow Gandiva are gone from the server jar (318 MB -> 166 MB): the DDL
  logged at startup names its types itself, with the same names. `-Plinux-only` leaves out the
  DuckDB, zstd-jni and netty-tcnative native libraries of other platforms than Linux
  x86-64/ARM64 (113 MB).
- GitHub Actions pinned to full commit SHAs (supply-chain hardening)
- `groupId` updated from `com.example` to `pro.surpin.arrowflight`
- SonarCloud project key and organization updated to `VadimSurpin`
- README badges and clone URLs updated to `VadimSurpin/arrow-flight-server`
- `commons-cli` updated from 1.5.0 to 1.9.0

### Removed
- `.gitlab-ci.yml` (GitHub Actions is the canonical CI)

---

## [1.0.0] — 2026-07-28

Initial public release. Forked from [nsu-fit/ArrowFlight](https://github.com/nsu-fit/ArrowFlight).

### Added
- Arrow Flight SQL server running as a sidecar on each Hadoop DataNode or Ozone storage node
- Parquet footer fast-path for `COUNT(*)`, `MIN`, `MAX` with zero column I/O
- DuckDB in-process aggregation engine receiving Arrow C streams from Parquet scanner
- Hazelcast-based cluster formation and node registry (zero manual configuration)
- Size-aware file scheduler: assigns Parquet files to nodes by total bytes, TTL-cached
- Prometheus metrics endpoint with per-query execution-path tracking
- Spark DataSource V2 connector with column projection, predicate pushdown, and aggregate pushdown
- Columnar read path (`FlightArrowColumnVector`) — no row conversion in Spark
- `FlightSessionCatalog` for Spark catalog integration
- `TypeConversionHelper` with explicit Arrow↔Spark type mapping (fixes INT_8 ClassCastException)
- Dagger 2 dependency injection replacing manual wiring
- Multi-stage Docker image (`maven:3.9.9` build + `eclipse-temurin:21-jre-jammy` runtime)
- `docker-compose.yml` orchestrating 8 services: 3 Flight nodes, Spark cluster, Grafana, Prometheus
- TPC-H and TPC-DS benchmark suite (`benchmarks/benchbase-spark/`)
- GitHub Actions CI: build, lint (Checkstyle + SpotBugs), integration tests, JaCoCo coverage
- Architecture Decision Records for distribution strategy, benchmark suite, DI choice, BenchBase bridge
- Architecture documentation: query execution flow, Parquet storage model, cluster service

### Changed
- All Russian-language comments translated to English
- Package restructured into `adapters/`, `services/`, `model/`, `metrics/` sub-packages
- Substrait filter pushdown replaced by DuckDB JDBC execution
- `HadoopFlightSqlService` God class decomposed into `FlightSqlProducer`, `ExecutionService`, `QueryPlanner`, `ClusterService`
- Java version bumped from 17 to 21
- Checkstyle and SpotBugs now fail the build on violations
- JaCoCo coverage reporting with SonarCloud integration

[Unreleased]: https://github.com/VadimSurpin/arrow-flight-server/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/VadimSurpin/arrow-flight-server/releases/tag/v1.0.0
