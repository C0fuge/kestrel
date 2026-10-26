# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- HDFS with Kerberos: optional settings `hdfsKerberosPrincipal` + `hdfsKerberosKeytab`
  (`HDFS_KERBEROS_*`, optional `hdfsKerberosTicketCache`) make the server log in from the keytab,
  write the ticket-granting ticket as a MIT file ticket cache for the hadoopfs extension (libhdfs3
  reads only a ticket cache) and replace it at 80 % of its lifetime: no external `kinit` + renewal
  (secure HA stand finding (b)). The cache defaults to the one the extension reads; unset settings
  change nothing. Tested with an in-process KDC; MIT `klist` / `kvno` read the cache in CI.
- S3: a startup WARN when `net.ipv4.tcp_tw_reuse` is not 1, with the port range and the reads per
  second it allows: DuckDB's httpfs opens a new connection for most range reads at 4+ threads and
  each closed one holds its port 60 s, so 32 concurrent small queries at 8 threads ran the stand
  out of local ports (item 0g). Workaround documented in `arrowflight.properties` and START-NOTES
  (`tcp_tw_reuse=1`, wider port range, `duckDbExternalFileCache=true` for repeated reads); no
  setting changes.
- HDFS: a startup WARN names every libhdfs3 NameNode RPC timeout (`rpc.client.connect.timeout`,
  `read`, `write`, `timeout`) the `LIBHDFS3_CONF` file leaves unset or over a minute; libhdfs3's
  defaults let a query hang ~170 s after the active NameNode host vanished (secure HA stand finding
  (d)). Recommended values in `arrowflight.properties` and START-NOTES; no setting changes.
- Flight `CancelFlightInfo` (and Flight SQL `CancelQuery`): releases the node-load reservations of
  a plan's endpoints at once instead of after their 10-minute expiry, so plans a client abandons no
  longer steer later endpoints away from a node (3-node S3 stand finding (b)); answers `CANCELLED`,
  running streams continue (README "Cancelling a plan"). Was `UNIMPLEMENTED`.
- Hive plugin: Kerberos metastores (`arrowflight.hive.authentication=kerberos`): SASL/GSSAPI
  transport as Hive's own client (`metastorePrincipal` with `_HOST`, `saslQop`), login from
  `clientPrincipal` + `clientKeytab` or from the ticket cache (`KRB5CCNAME` / `clientTicketCache`),
  renewed before the ticket runs out. Tested against an in-process KDC (Apache Kerby, test scope only).
- Iceberg time travel: `FROM ns.t FOR VERSION AS OF <snapshot id | 'tag'>` and `FOR TIMESTAMP AS OF
  TIMESTAMP '<time> <zone>'` read an earlier snapshot of an iceberg-hadoop / iceberg-rest table with
  its own files, deletes and columns (README "Iceberg time travel"); errors for points without a
  snapshot, refused for other tables; partial mode answers such queries completely. SPI 1.2 adds
  `Catalog.asOf` (default: refused); the iceberg plugin now needs SPI 1.2.
- Several S3 stores per server: `arrowflight.s3.stores=<name>,...` with `arrowflight.s3.<name>.root`
  and that store's own endpoint, region, URL style and credentials (`FLIGHT_S3_<NAME>_ACCESS_KEY`,
  `..._SECRET_KEY`, `..._SESSION_TOKEN` or `arrowflight.s3.<name>.credentialsFile`) let catalogs read
  roots outside the S3 data directory (another bucket, endpoint or account). Each store gets its own
  DuckDB secret (`flight_s3_<name>`) scoped to its root. A root that needs a second store of the
  main S3 configuration is refused as before; a named store never borrows the main credentials. SPI: `DataStore`
  gains `configName()` (default empty).
- Runtime libs set: the build copies the Hadoop client libraries the shaded server jar leaves to the
  deployment (Hadoop is `provided`) to `flight-server/target/libs`, mediated against the jar's own
  libraries (58 jars, Hadoop 3.5.0; none of the stale stand set's ZooKeeper 3.6, Netty 3, Jetty
  9.4.51, BouncyCastle 1.79, log4j 2.20 or second Netty/Jackson/protobuf). The manual `package`
  release of `dev-ci.yml` publishes it as `flight-server-libs-<version>-<sha>.tar.zst` (`libs/` with
  `DEPENDENCIES.txt` and `SHA256SUMS`); start with `java -cp '<jar>:libs/*' <main class>`.
  `LibsSetTest` checks the floors and starts the server on exactly this set.
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

### Security
- Server: Hazelcast 5.6.0 → 5.7.0 (critical/high advisories on the member port) and Netty 4.2.9 → 4.2.19
  (HTTP/2 and handler advisories) under one `netty-bom`. `netty-all` is replaced by the modules the gRPC server
  uses (HTTP/2, proxy, epoll for Linux x86-64/ARM64), which drops about 25 advisories in unused codecs (Redis,
  HAProxy, SMTP, XML, HTTP/3, OCSP, DNS …); macOS builds now use Netty's NIO transport (no kqueue). README: keep
  the Hazelcast port private. Upgrade every node of a cluster together (Hazelcast open source has no rolling
  upgrade between minor versions). `DependencyFloorTest` guards both floors. The Spark connector imports the same
  `netty-bom` (its shaded jar's Netty 4.2.9 → 4.2.19; its tests mixed 4.1 and 4.2 modules).
- Client core and StarRocks connector: one Netty 4.2.19 under `netty-bom` instead of Arrow's 4.2.9 mixed with
  grpc-netty's 4.1.130 (HTTP/2 codec, http, socks, handler-proxy, unix-common were 4.1.130), tcnative
  2.0.74 → 2.0.84. The provided Hadoop dependencies no longer bring `netty-all`, so no codec or native
  transport leaks into the connectors' runtime classpath. `NettyFloorTest` in both modules.
- Jackson 2.15.3 (databind/core, mixed with Arrow's 2.21.0 jsr310 and Ranger's 2.17.2 JAX-RS) → one 2.21.8 under
  `jackson-bom` in the server, client core, Spark and StarRocks connectors (databind DoS and
  PolymorphicTypeValidator advisories). The Spark connector's shaded jar relocates Jackson to
  `pro.surpin.data.arrowflight.shaded.jackson`: Spark 3.5 keeps its own 2.15, the connector runs on 2.21.8 (before,
  Spark's 2.15 shadowed the bundled copy). `DependencyFloorTest` / `JacksonFloorTest` guard one version >= 2.21.7.
  The Iceberg plugin (own class loader) bundles Jackson 2.22.3 instead of Iceberg 1.12's 2.22.2.
- Server: Parquet 1.13.1 → 1.18.1 (footer and statistics reads), which lifts aircompressor 0.21 → 2.0.3
  (CVE-2024-36114, CVE-2025-67721: crash / memory leak on crafted LZ4/Snappy/Zstd input) and snappy-java
  1.1.8.3 → 1.1.10.8 (CVE-2023-43642, CVE-2023-34455 and others: DoS on crafted chunks). Data pages are still
  read by DuckDB. `DependencyFloorTest` guards Parquet >= 1.15, aircompressor >= 2.0.3, snappy-java >= 1.1.10.4.
- Hive plugin: libthrift 0.9.3 → 0.24.0 for the metastore client (CVE-2026-43869, CVE-2026-43871,
  CVE-2020-13949, CVE-2019-0205, CVE-2018-1320: parser DoS from a hostile metastore, TLS host checks). Hive
  3.1.3's generated metastore classes and libfb303 0.9.3 run unchanged on it. Responses are capped at 1 GiB
  (Hive 4's default; libthrift's own 100 MB would refuse large partition lists, 0.9.3 had no cap); frames stay
  at 16 MB. The test metastore (Hive 3.1.3 server, which fails on libthrift 0.14+) now runs on 0.9.3 in its own
  class loader, so the tests cover a new client against an old server; a framed-transport test was added.

### Fixed
- Concurrent DoGet streams could corrupt each other's batches: Arrow Java's `ListVector` /
  `LargeListVector` set the writer index of the process-wide empty buffer to 4 (8) when an empty
  list is written to IPC, so another stream serialized its empty buffers with the wrong length
  (a stream CANCELLED with `IndexOutOfBoundsException`, or, when the length changes between the
  batch header and the body, a body the client decodes shifted).
  Every SqlInfo answer has empty list/map children: the likely cause of ADBC connects that read
  "transactions supported" and failed `failed to update autocommit: ... Unimplemented` (TUNING.md,
  item 13d). Every batch a ticket sends now gives its empty list vectors own offset buffers first
  (`EmptyListOffsets`). BeginTransaction stays `UNIMPLEMENTED`, now with a description and a WARN
  log line.
- Error texts sent to clients no longer carry Java stack frames: HDFS NameNode errors (e.g.
  `Permission denied`, through DuckDB or the Java Hadoop client) used to arrive with the NameNode's whole
  stack trace; a gRPC interceptor now drops `at ...(...)` and `... n more` lines from every error
  status (the message and `Caused by:` lines stay; the server log keeps the full text).
- Many concurrent streams no longer exhaust direct memory: every stream could queue up to
  `flightBackpressureThresholdBytes` (256 MiB) for a slow client, so 8 clients scanning a 3-node
  cluster got streams CANCELLED ("Server sendMessage() failed with Error", direct memory out) and a
  node killed. All open streams of a node now share one outbound budget, new setting
  `flightOutboundBudgetBytes` (default 0 = a quarter of the direct memory limit): each stream queues
  at most budget / open streams (at least 1 MiB) and then waits for its client. Regression test runs
  16 slow-read streams against a server JVM with 256 MiB direct memory.
- S3: DuckDB retries failed S3 requests, connect errors included, for about 6 s instead of
  DuckDB's own ~0.5 s (3 retries from 100 ms, backoff 4), which a busy endpoint refusing
  connections outlasted at `duckDbThreads=8` (every complete-mode TPC-H query failed on a MinIO
  store with "Could not establish connection"). New settings `arrowflight.s3.httpRetries` (default
  6, 0-20), `httpRetryWaitMs` (200, first wait) and `httpRetryBackoff` (2, 1-10) set
  `http_retries`, `http_retry_wait_ms` and `http_retry_backoff`; named stores inherit them (they
  are per DuckDB connection, so differing values are refused at start).
- Cluster: planning no longer waits ~10 s after a node stopped. The node loads (every plan) and, on
  shared storage, the node inventories are read by key for the current Hazelcast members instead of
  with a whole-map query, which blocks while the cluster settles after a member left whose address
  no longer answers (GetFlightInfo timed out on a survivor). A stopped server now leaves the
  Hazelcast cluster gracefully (Hazelcast's terminating shutdown hook is off; graceful wait at most
  5 s). A crashed node still costs up to the 10 s member heartbeat timeout.
- HDFS short-circuit reads are no longer forced on: the deployment's `dfs.client.read.shortcircuit`
  (hdfs-site.xml or `-D`) is kept, and without it short-circuit is on only when
  `dfs.domain.socket.path` is set. A client configuration without a domain socket (no local DataNode)
  used to stop the server at start.
- An `s3a://` or `s3n://` catalog root (`arrowflight.iceberg-hadoop.warehouse=s3a://b/wh`) stopped the
  server at start ("The store opened for s3a://b/wh does not hold it (roots [s3://b/wh/])"), and Iceberg
  tables whose metadata names `s3a://` / `s3n://` files under an `s3://` root were hidden. Catalog
  locations now treat the three S3 scheme forms as one (store lookup, roots check, `read_parquet` file
  lists and the data file paths inside position delete files); DuckDB already accepted all three forms.
- A local catalog root (`arrowflight.iceberg-hadoop.warehouse=/wh`, `file:/wh` or `file:///wh`, local
  Iceberg/Hive data file paths) stopped the server at start when the Hadoop configuration names a
  non-local `fs.defaultFS` (`hdfs://<ns>`): "The store opened for /wh does not hold it". Catalog
  locations without a scheme are local files (as DuckDB reads them) and now open a local store; a
  data directory given as a plain path still follows `fs.defaultFS`.
- `duckDbStatementCacheSize=0` (or 1) broke every parameterized prepared statement with "Invalid
  Input Error: Parameter count mismatch" (JDBC, ADBC): the parse check before streaming took a
  statement from the per-thread cache and closed the one the parameter values were bound to. The
  bound statement now streams directly; 0 or 1 keep only the statement in use.
- Runtime libs set: ZooKeeper (3.8.6 with zookeeper-jute) and lz4-java (`at.yawk.lz4` 1.10.4, Hadoop's
  coordinates) were missing, because Spark's test dependencies (ZooKeeper 3.6.3, `org.lz4` 1.8.0) took them
  out of the `provided` scope; HA NameNode failover with ZooKeeper, the ZooKeeper delegation-token and
  signer-secret managers, `ZKCuratorManager`, `SecurityUtil` reflection and Hadoop's `Lz4Codec` failed with
  `NoClassDefFoundError`. The same cause gave the shaded jar curator-framework 2.13.0 next to curator-client
  5.2.0 (and curator-recipes 2.13.0 in the set); Curator is now 5.2.0 throughout. `LibsSetTest` now loads
  every class of hadoop-common, hadoop-hdfs-client and hadoop-auth on the thin class path and resolves their
  member types (only jsch for SFTP / ssh fencing and the legacy protobuf-2.5 `ProtobufRpcEngine` are left
  out), checks the HA/Kerberos/ZooKeeper classes by name and round-trips data through `Lz4Codec`.
- Configuration precedence: environment variables (`FLIGHT_RESULT_MODE`, `FLIGHT_ENDPOINT_BACKUP_LOCATIONS`,
  `FLIGHT_METRICS_ENABLED`, `DUCKDB_*`, `HDFS_*`, `FLIGHT_S3_*`, ...) were ignored whenever the bundled
  `arrowflight.properties` set the same setting, and a `-Darrowflight.*` alias lost to the file's short key
  (`metricsEnabled=true` beat `-Darrowflight.metrics.enabled=false`). The order is now the documented one,
  for server and plugin settings alike: every `-D` name > environment > `etc/plugins/<ns>.properties` >
  `arrowflight.properties` > default. Upgrade note: an environment variable that was silently shadowed
  before now takes effect (for example a leftover `FLIGHT_RESULT_MODE=partial`), and a plugin environment
  variable now beats the same key in `etc/plugins/<ns>.properties`.
- Iceberg equality deletes are applied by the server with read_parquet (complete answers):
  snapshots with equality deletes were read through DuckDB's iceberg_scan with all their columns
  and every filter kept above the scan (~25–90 s per query on the stand), because DuckDB 1.4.5's
  iceberg_scan fails or mis-applies equality deletes under projections and pushed filters. Data
  files are now read by field ID and anti-joined with their equality delete files (null matches
  null) and position delete files (file path and row number), as the catalog's plan assigns them
  (sequence numbers), so queries read only the columns they use and filters reach the scans.
  Snapshots with deletion vectors, columns without field IDs or an equality column the schema no
  longer has keep the old read. Partial answers of such snapshots are still answered completely.
- README Quick Start and the release START-NOTES started the server with `java -jar` on the shaded jar,
  which fails with `NoClassDefFoundError: org/apache/hadoop/conf/Configuration` (Hadoop is not in the
  jar); START-NOTES also put the jar where its default `../plugins` did not point at the unpacked plugins.
- Query texts longer than 256 Ki characters (a join key list a connector sends as an IN list can
  be megabytes, and each one differs) are no longer kept in the parse cache, the result-schema
  cache or the per-thread prepared-statement cache: each cache could pin up to its entry limit
  of them (1024 parses, 1024 schemas, `duckDbStatementCacheSize` statements per worker thread) and
  evicted the short, repeating queries. A long text keeps one uncached statement slot per
  thread. New Prometheus counters `arrowflight_query_cache_oversize_total{cache}` and
  `arrowflight_query_cache_evictions_total{cache}` (`parse`, `schema`, `statement`). Idea from
  upstream f998bf7 (parse cache only).
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
- `duckDbThreads` defaults to 4 (was 2 in the bundled `arrowflight.properties`, 1 in code): SF10 1.8x and SF1
  1.3x faster on the stand for +2.6 ms on a point lookup (docs/dev/TUNING.md). Set 1 where compute shares the node.
- Partial results of Iceberg tables with delete files: a snapshot with position and/or equality
  deletes is no longer answered completely by one node. Its data files are spread over the nodes
  like other snapshots' (tickets unchanged: snapshot ID and file location), and the executing node
  takes each file's delete files from the pinned snapshot's plan and applies them with read_parquet
  anti-joins (as complete answers do). Such plans report an unknown row count (`-1`) instead of the
  files' recorded rows. Deletion vectors, columns without field IDs and equality columns the schema
  dropped still get the complete answer.
- Metadata endpoints (catalogs, schemas, tables, SqlInfo, type info) carry no location by
  default: clients read them over their own connection (stock JDBC metadata 1.4-1.6x faster in
  plaintext, 3.2x with TLS). `metadataEndpointLocation=advertised`
  (`FLIGHT_METADATA_ENDPOINT_LOCATION`) restores the advertised server URI.
- DuckDB is pinned to 1.4.5.0 (was 1.4.1.0): the hadoopfs and httpfs extension files must be
  the v1.4.5 builds (`extensions/v1.4.5/linux_amd64/`). The server needs JDK 21 (Netty 4.2 + Arrow
  fail on JDK 24+).
- HDFS: the server sets the hadoopfs extension's DataNode timeouts, `duckDbHdfsConnectTimeoutMs`
  (default 3000) and `duckDbHdfsReadTimeoutMs` (default 10000), as `hdfs_connect_timeout_ms` /
  `hdfs_read_timeout_ms`; they win over `LIBHDFS3_CONF`, `0` leaves them to the extension. An
  extension built without these settings keeps its own timeouts (one warning). The defaults were
  5000 / 15000 in earlier builds of this line: with the hadoopfs dead-DataNode list only the first
  read that hits a lost DataNode waits, and 3 s / 10 s cut that wait from ~15-22 s to ~4-11 s.
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
