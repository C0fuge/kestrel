# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- Hive plugin: large Avro data files and uncompressed text files (text, CSV, JSON tables without header lines) of a
  scan are read as byte ranges decoded in parallel (Avro from the next sync marker, text from the next line start, as
  Hadoop splits them; files over 8 MiB, like ORC files since P1), instead of one thread per file. Same rows in the
  same order; compressed text files and tables with `skip.header.line.count` are still read whole. Avro records
  decode only the requested columns (others are skipped through Avro's resolver, so a file that does not resolve
  still fails as in Hive). Text tables parse plain decimals (up to 18 digits), `yyyy-mm-dd` dates and ASCII strings
  without intermediate objects (about 2.5x faster on a Q1-style column set, same values) (P4 (d)).
- Setting `duckDbSharedDatabase` (`DUCKDB_SHARED_DATABASE`, default false = unchanged): every worker connection
  connects to one DuckDB of the node (one thread pool and one `memory_limit`; storage setup, secrets and lockdown run
  once and are checked at startup); each connection keeps the catalog views in an in-memory catalog of its own, and
  client SQL naming such a `flight_session_<n>` catalog or schema is refused. With it, `duckDbThreads` /
  `duckDbMemoryLimit` size the node (unset = cores / whole DuckDB budget); `duckDbThreadsUnderLoad`,
  `duckDbThreadsWhenIdle` and `duckDbStreamingBufferSize` are ignored. A DuckDB invalidated by a FATAL error is
  replaced as a whole. Manual workflow `tpch-perf.yml` takes `extra` server options, measured and answer-checked
  against the default (item 22 step 1).
- Queries of catalog views (Hive views with the opt-in `hive.views`; nothing changes without it): a view named with
  its schema (`FROM h_db.my_view`, also in joins, subqueries and other views) reads its translated query over the
  tables; such queries are always answered completely, also when parts are asked for (one endpoint). Access needs the
  view and every table and view it reads (invoker rights, stricter than Hive's definer rights - see Open questions in
  `docs/dev/PLUGINS-STATUS.md`). GetTables lists views as `VIEW` (only those the caller may read with their sources),
  GetTableTypes adds `VIEW` when a catalog shows views. Refused: a view read `FOR VERSION/TIMESTAMP AS OF`, views
  nested deeper than 16; a view named like a table of its schema is not shown (the table wins); an unqualified view
  name is a missing table (P4 views part 3).
- Setting `flightLoadReservationSeconds` (`FLIGHT_LOAD_RESERVATION_SECONDS`, default 600 = unchanged): how long a
  planned endpoint's bytes count as load on its node when the client neither reads it to the end nor sends
  `CancelFlightInfo`; a shorter value frees nodes sooner from plans clients abandon (3-node S3 stand finding (b),
  README "Cancelling a plan"). Values below 1 stop the start.
- SPI 1.6: `Catalog.views(schema)` (default none) and `CatalogView(name, sql, sources, columns, fingerprint)`. Hive
  plugin (descriptor now SPI 1.6): opt-in `hive.views` (default false) lists simple virtual views whose expanded text
  `HiveViewSql` translates and whose every source is a table (or a listed view) the catalog shows; casts, untranslated
  constructs, sources not shown (another format without its opt-in, a database outside `hive.databases`), cycles and
  materialized views keep a view skipped with a log reason. The server does not read catalog views yet, so nothing
  changes for clients (P4 views part 2).
- Iceberg time travel for Iceberg tables registered in a Hive Metastore (item 3 (b)): `FOR VERSION AS OF` /
  `FOR TIMESTAMP AS OF` after such a table name reads the snapshot through the iceberg plugin, as for the iceberg
  catalogs (both result modes); other Hive tables still refuse the clause. SPI 1.5: `TableFormat.asOf(table,
  metadataLocation, point)` (default refuses); the hive plugin descriptor now names SPI 1.5.
- Hive plugin: Avro tables whose schema is given by `avro.schema.url` (under `hive.avroTables`; such tables were
  skipped). The schema file is read through the server's stores only when it lies under `hive.roots` (an HTTP URL or
  a file elsewhere keeps the table skipped with a log reason), at most 4 MiB, kept per location and read again when
  its size or modification time changes; a literal still wins over a URL, as in Hive. A partition's own URL is read
  the same way; a changed schema file makes queries planned before it fail clearly (P4).
- Iceberg time travel to snapshots whose files cannot be read one by one (deletion vectors, renamed nested fields;
  item 3 (c)): such a snapshot is now read by DuckDB's `iceberg_scan(..., snapshot_from_id => <id>)` instead of
  being refused, and only when DuckDB reads it with the snapshot's own columns (names and types, nested field names
  included) - otherwise refused as before. DuckDB 1.5.6 reads both snapshots of a table before and after a nested
  rename with their own field names (`IcebergCatalogIntegrationTest`, complete and partial mode). Log line
  `views=icebergSnapshotRead`.
- Hive plugin: Avro tables (`AvroSerDe` + `AvroContainerInputFormat`), opt-in `hive.avroTables` (default false; such
  tables were and stay skipped without it), read by the plugin as Hive 3.1.3 reads them and handed over as Arrow
  streams (both result modes, partition pruning). Reader schema from `avro.schema.literal` (table properties over
  serde properties) or made from the columns as Hive's `TypeInfoToSchema`; the table is shown only when that schema
  gives the metastore's columns (`avro.schema.url`, unions other than with null, Avro types without a Hive column
  type: skipped with a log reason). Each file is resolved from its own schema to the table's (promotions, defaults);
  decimals at the file's scale, char/varchar cut to the file's maxLength, timestamps in the file's
  `writer.time.zone` (a file without it fails on its first timestamp: Hive shows them in its server's zone); a
  partition with another schema, a file cut short and records Hive fails on fail the query naming the file. Codecs
  null/deflate/snappy/bzip2/zstandard. New plugin dependencies: Avro 1.12.0, snappy-java, Jackson 2.17 (P4).
- `dataDirShared` (`FLIGHT_DATA_DIR_SHARED`, default false): declares a local data directory a mount every node reads
  (NFS), so a cluster plans complete answers and spreads files over all nodes as on HDFS. Distributed complete mode
  tested between two server processes with TLS and the token provider (`DistributedTwoProcessTlsTest`: answers equal
  DuckDB's, merged from 4 endpoints on 2 nodes; a call without the token and a forged internal identity are refused)
  (item 18, D2 (c) part 1).
- Startup line `duckdb=externalFileCache perConnection= expectedConnections= expectedMiB= memoryMiB=` when
  `duckDbExternalFileCache=true`, and a WARN `duckdb=externalFileCacheUnbounded` when it is on without a per-connection
  `memory_limit` (`duckDbMemoryLimit=none`). Measured on DuckDB 1.5.6: the cache stays within each connection's
  `memory_limit` (evicted like any buffer), so the node bound is that limit times the live connections; pinned in
  `S3IntegrationTest.theExternalFileCacheStaysWithinTheMemoryLimit` (item 13 (c)). Default unchanged (off).
- Distributed complete mode (opt-in `distributedComplete`): an endpoint answer that fails before its schema (peer's
  Flight service down, connection refused) is read from the endpoint's backup locations in order, as clients fail
  over, before the whole query falls back to the single-node path; `DistributedComplete.failovers()` and a
  `distributedComplete=failover` log line (item 18, D2 (c) part 2). A failure after the schema still falls back.
- Distributed complete mode: `DistributedCompletePerfTest` (`perf`, item 18 step 3, D2 (b)) - three nodes, Q1/Q6-like
  aggregates, wide DISTINCT, ORDER BY LIMIT, wide GROUP BY and wide rows with `distributedComplete` off vs on (wall
  time, peer MB, peer wait); `DistributedComplete.peerBytes()` / `peerWaitNanos()` and `peerBytes=` / `peerWaitMs=` in
  its `merged` log line.
- `duckDbThreadsWhenIdle` (item 19b step 2), opt-in: `arrowflight.duckdb.threadsWhenIdle` / `DUCKDB_THREADS_WHEN_IDLE`
  (default 0 = off). Each Flight worker gets, on first use, a further DuckDB connection with that many threads (at
  most the cores); a DoGet that starts while no other runs on the node uses it to its end. Values not above
  `duckDbThreads` are off. Startup line `duckdb=threads ... whenIdle=`.
- S3 / lakeFS read cache disk tier (item 17 (a)), opt-in: `arrowflight.s3.cacheDiskPath` (absolute directory, default
  empty = memory only) + `arrowflight.s3.cacheDiskMegabytes` (size cap; both needed, and `cacheMegabytes` > 0); same
  keys under `arrowflight.lakefs.`. Blocks read from S3 are also written (one background writer, queue of 64 blocks,
  dropped when full) to files under `<path>/<plugin>-cache/`, least recently used deleted beyond the cap; a block the
  memory tier no longer holds is read from its file before S3 and kept in memory again. Each file's CRC-32C is checked
  on read (a damaged, short or deleted file is a miss read from S3, never an answer); files of an earlier run are
  deleted at start and all at close; a lock file `<path>/<plugin>-cache.lock` refuses a second server on the same
  directory; directory mode 700. `cacheAdmission=reuse` applies to each tier with its own reach. Metrics
  `arrowflight_s3_cache_disk_*` (capacity/used bytes, blocks, hits, misses, writes, evictions, dropped, errors,
  not_admitted), `disk...=` counts in the `s3=cacheClosed` line, `diskPath=` / `diskCapacityBytes=` in `s3=cacheOpen`.
- Hive JSON tables (P4 step 2c) under `hive.textTables`: `org.apache.hive.hcatalog.data.JsonSerDe` and
  `org.apache.hadoop.hive.serde2.JsonSerDe` (TextInputFormat; OpenX's serde stays skipped), one JSON object per line
  parsed exactly as Hive 3.1.3's serde does, with the same parser (Jackson 1.9 `jackson-core-asl` 1.9.13, new plugin
  dependency): keys matched ignoring case, unknown keys skipped, a later key wins, HCatalog's `_col<n>` keys; numbers
  through Jackson's accessors (fractions cut for integer columns, tinyint up to 255 wraps), booleans true only for
  `true`, strings from any scalar, binary as the text's UTF-8 bytes, Hive's lenient `Date.valueOf` /
  `Timestamp.valueOf`, decimals rounded half up (unreadable or too wide NULL), varchar/char cut; arrays, maps (keys
  read as their type) and structs, nested values also where Hive's token walk goes astray. A line Hive's serde fails
  on (not an object, a quoted number for a number column, an unreadable date or timestamp, an empty line) fails the
  query naming file and line, never a silent NULL; so do rows Arrow cannot hold (NULL or duplicate map keys).
  Header lines and `.gz`/`.deflate`/`.bz2` as for other text tables; `timestamp.formats` and footer lines skip the
  table; a partition with the other JSON serde fails the query.
- Hive `OpenCSVSerde` tables (P4 step 2c) under `hive.textTables`: lines parsed with opencsv 2.3 (new plugin
  dependency, the version Hive 3.1.3 uses) exactly as Hive's serde does: `separatorChar` / `quoteChar` /
  `escapeChar` (default `,` `"` and, for the default escape `"`, opencsv's backslash), header lines, missing
  fields NULL, empty line all NULL, no NULL text. Every column is a string, as Hive reads them; tables that
  declare other column types stay skipped with a log reason, as do footer lines.
- Hive text tables with nested columns (P4 step 2): arrays, maps and structs of `LazySimpleSerDe` tables
  (`hive.textTables`) are read as Hive reads them: split at `collection.delim` (or the misspelt
  `colelction.delim`, default byte 2), `mapkey.delim` (default 3) and bytes 4-8 for deeper levels, up to Hive's 7
  nesting levels; an empty field is an empty array or map, a map entry without a key separator has a NULL value,
  entries with a NULL or unreadable key are dropped and of equal keys the first is kept; struct fields by
  position. Tables with unions, non-primitive map keys or deeper nesting stay skipped with a log reason. New
  end-to-end test `HiveTextIntegrationTest` (text vs Parquet of the same rows through the server, both result
  modes, partition pruning, joins). Text tables' schema IDs change (the separators are part of them).
- Hive text tables (P4 step 1), opt-in `hive.textTables` (default false; text tables were and stay skipped
  without it): `STORED AS TEXTFILE` tables of `LazySimpleSerDe` are read by the hive plugin and handed to DuckDB
  as Arrow streams (the ORC path, both result modes), as Hive reads them: `field.delim` / `serialization.format`,
  `escape.delim` (+ `serialization.escape.crlf`), `serialization.null.format`, `skip.header.line.count`,
  `serialization.last.column.takes.rest`, rows ending at LF/CR/CRLF, missing fields NULL, fields that do not read
  as their type NULL; `.gz`, `.deflate`, `.bz2` files. Primitive columns only for now; tables with nested
  columns, footer lines, `timestamp.formats`, another encoding or line delimiter, and other text serdes (CSV,
  JSON, Regex) stay skipped with a log reason; a partition with its own serde properties fails the query.
- Unity Catalog OSS as an Iceberg REST catalog (C1 step 4): `catalogs-ci.yml` entry `unity-catalog`
  (`unitycatalog/unitycatalog:v0.7.0`, managed tables on a local storage root) runs both REST conformance tests;
  answers equal the Hadoop and Hive metastore references. Unity Catalog refuses Iceberg `time` columns (new
  optional `ICEBERG_REST_IT_OMIT_COLUMNS` leaves them out of both sides) and its sample table
  `default.marksheet_uniform` answers 500 (the server test shows namespace `ice` only,
  `ICEBERG_REST_IT_NAMESPACES`). Test only.
- Apache Ozone as an S3 backend (C3): `catalogs-ci.yml` entry `iceberg-rest-fixture-ozone-s3` runs both REST
  conformance tests with the warehouse on Ozone 2.0.0's S3 gateway (`tools/ci/ozone-s3.sh`: SCM, OM, one datanode,
  S3 gateway; non-secure, replication ONE) instead of moto: Iceberg S3FileIO writer, the s3 plugin and DuckDB httpfs
  read it; answers equal the Hadoop reference. Test only.
- DuckDB delta extension probe (C3 step 1): `catalogs-ci.yml` job `delta` writes Delta tables with Spark 3.5.3 +
  delta-spark 3.3.2 (`tools/ci/delta-fixtures.py`: partitions, deletion vectors, column mapping by name and id,
  checkpoints, versions, schema evolution, types; Spark's answers in `expected.tsv`) and
  `tools/ci/DuckDbDeltaProbe.java` reads them with `delta` v1.5.6 under the server's lockdown, locally and on S3
  (moto), and records what is read correctly and whether paths outside `allowed_directories` are refused. Test only.
- DuckDB postgres / mysql scanner probe (C2 step 1): `catalogs-ci.yml` job `jdbc-scanners`
  (`tools/ci/DuckDbScannerProbe.java`) loads `postgres_scanner` / `mysql_scanner` v1.5.6 by path, attaches
  PostgreSQL 16 / MySQL 8.4 read-only with a temporary secret, applies the server's lockdown and records which
  reads, pushdowns and client statements get through (results in docs/dev/CATALOGS-STATUS.md). Test only.
- Iceberg REST catalog tables registered in a Hive metastore (C1 step 3c): on local warehouses
  `RestCatalogServerConformanceTest` registers the REST catalog's metadata files in the in-process metastore (schema
  `hms_ice`, hive plugin -> iceberg plugin); answers equal the REST catalog's, partial equal complete. Test only.
- Iceberg REST catalogs through the server on S3 warehouses (C1 step 3b): `RestCatalogServerConformanceTest` also
  runs for polaris-s3, iceberg-rest-fixture-s3 and lakekeeper-s3, the server reading the tables through the s3 plugin
  (static keys); the iceberg module's `fixture-libs` carry Iceberg's S3FileIO and the AWS SDK. Test only.
- Iceberg REST catalogs through the server (C1 step 3, local warehouses): `RestCatalogServerConformanceTest` runs the
  server with the iceberg plugin's iceberg-rest catalog on Polaris / the Iceberg REST fixture next to an iceberg-hadoop
  reference with the same tables; complete answers equal the reference, partial equal complete. Test only
  (`catalogs-ci.yml`; `IcebergFixtures.writeRest`).
- Iceberg REST catalog conformance on S3 (C1 step 2a): `catalogs-ci.yml` also runs `RestCatalogConformanceTest`
  with the warehouse in an S3 bucket (moto's S3 server; MinIO's images no longer pull anonymously) for Apache
  Polaris 1.2.0 (S3 storage without STS), the Iceberg REST fixture 1.10.0 (S3FileIO) and Lakekeeper 0.13.8
  (PostgreSQL backend, S3 warehouse); the plugin reads metadata through the s3 plugin's store and DuckDB reads the
  rows over httpfs. Static keys only (vended credentials are step 2b). Test only, no runtime change.
- Iceberg REST catalog conformance (C1 step 1): new workflow `catalogs-ci.yml` starts Apache Polaris 1.2.0 (OAuth2
  client credentials, prefix from `/v1/config`) and the Apache Iceberg REST fixture 1.10.0 as containers and runs
  `RestCatalogConformanceTest`: the fixture tables are written through each catalog with Iceberg Java's RESTCatalog
  and must look to the `iceberg-rest` catalog exactly as the same tables in a Hadoop-layout warehouse (names,
  columns, files, row and delete counts, partial-read problems, DuckDB `iceberg_scan` rows). Test only, no runtime
  change.
- Distributed complete mode, 3-node test (item 18 step 3, lane 3 D2): `DistributedClusterIntegrationTest` runs
  three complete-mode nodes on one Hazelcast cluster in one JVM over a shared data directory (6 files, two
  endpoints per node): merged answers equal single-node answers for 9 shapes (incl. wide GROUP BY and a wide
  unordered projection), a peer stream reset mid-way (TCP relay) and a peer whose Flight service is down fall back
  to the exact single-node answer (or fail; never a partial answer), and a node that left is not planned. Test only,
  no runtime change.
- Distributed complete mode (item 18 step 2, lane 3), opt-in `distributedComplete` / `FLIGHT_DISTRIBUTED_COMPLETE`
  (default false, unchanged): the planning node of a complete answer reads the query's partial-mode endpoints from the
  nodes (Flight DoGet as the client's user, header `x-kestrel-internal-identity` signed with the ticket secret,
  accepted from live members' addresses only) and merges them in DuckDB (`CompleteAnswerMerge`, columns renamed and
  checked against the single-node answer's schema). Refusal, unmergeable shapes, one-part plans and peer failures
  before the first batch keep the single-node path. Peer TLS trust: `distributedCompleteTrustedCertificates`, default
  the node's own certificate. With the setting on, the ticket secret allows impersonation (see README).
- OIDC / Knox JWT bearer tokens (P2 step 3), new optional module `flight-auth-jwt`, `FLIGHT_AUTH_PROVIDER=jwt`
  (existing providers unchanged; release asset `flight-auth-...` now also holds `auth/jwt/`): every call's bearer JWT
  is verified with nimbus-jose-jwt 10.10 against the issuer's JWKS (`FLIGHT_JWT_JWKS_URL`, HTTPS only, cached and
  re-fetched on an unknown key id, rate limited; or `FLIGHT_JWT_JWKS_FILE`), `FLIGHT_JWT_ISSUER`,
  `FLIGHT_JWT_AUDIENCE`, expiry / not-before with `FLIGHT_JWT_CLOCK_SKEW_SECONDS` (default 60); asymmetric
  algorithms only (no HS*, no `none`). Identity from `FLIGHT_JWT_USER_CLAIM` (default `sub`), optional Ranger groups
  from `FLIGHT_JWT_GROUPS_CLAIM` (dotted path, e.g. `realm_access.roles`), opt-in `FLIGHT_JWT_BASIC_PASSWORD` for
  tools with only user/password fields. Bad tokens `UNAUTHENTICATED`, unreachable keys `UNAVAILABLE`. No core or SPI
  change.
- Small-query perf tooling (maintainer's ADBC c8 tail bug): `SmallQueryThroughputPerfTest` prints p99 and max and
  takes `-Dperf.server=host:port` / `-Dperf.dataDir` to measure a separately started server; new manual workflow
  `small-query-perf.yml` runs a baseline build and the current ref, each alone in a container with 2 CPUs by quota
  (`--cpus=2`, like the CI screening) and pinned (`--cpuset-cpus`), client on the other cores. No runtime change.
- Zero-copy Flight writes for DuckDB result streams (item 22 (d)), opt-in: `flightZeroCopyWrites` /
  `arrowflight.flight.zeroCopyWrites` / `FLIGHT_ZERO_COPY_WRITES` (default false, unchanged; only `true`/`false`).
  gRPC sends the buffers DuckDB exported for a batch instead of copying each batch first; DuckDB exports every batch
  into new buffers and Flight keeps them until written, so a batch stays intact while DuckDB computes the next ones.
  Other streams (metadata, SqlInfo) always copy. Local A/B on 2 cores (`ParallelScanPerfTest`, in-process client on
  the same cores, 6 M-row table, two rounds each): 8 endpoints 243/234 -> 245/268 MB/s at 8.05/8.34 -> 7.26/7.15 s
  CPU per GB; 1 and 4 streams within noise. `ParallelScanPerfTest` takes `-Dperf.zeroCopyWrites`.
- LDAP groups for Ranger (P2 step 2), opt-in `FLIGHT_LDAP_GROUP_SEARCH_BASE` (+ `FLIGHT_LDAP_GROUP_SEARCH_FILTER`,
  default `(|(member={1})(uniqueMember={1})(memberUid={0}))`, and `FLIGHT_LDAP_GROUP_NAME_ATTRIBUTE`, default `cn`):
  the user's directory groups, read at the login and kept for the session lifetime (another node reads them once with
  the service account, `FLIGHT_LDAP_BIND_DN` now also allowed in template mode), are added to the Hadoop group
  mapping for Ranger checks. Unreadable groups fail the call `UNAVAILABLE` / deny, never fewer groups. Core: new
  `FlightAuthenticationProvider.groups(identity)` (default none, so the built-in providers are unchanged).
- LDAP / Active Directory login (P2 step 1), new optional module `flight-auth-ldap`, `FLIGHT_AUTH_PROVIDER=ldap`
  (existing providers unchanged): a Basic user name + password (what JDBC/ODBC/ADBC and BI tools send) is checked with
  a simple bind - DN template (`FLIGHT_LDAP_USER_DN_TEMPLATE`, e.g. an AD UPN `{0}@corp.example`) or search with a
  service account (`FLIGHT_LDAP_USER_SEARCH_BASE` / `_FILTER` / `FLIGHT_LDAP_BIND_DN`, identity optionally from
  `FLIGHT_LDAP_USER_ATTRIBUTE`); ldaps or StartTLS with host name checks (clear text only with
  `FLIGHT_LDAP_ALLOW_PLAINTEXT=true`), several servers in order; the answer carries a signed session bearer token
  (`FLIGHT_LDAP_SESSION_SECRET`, shared by all nodes, `FLIGHT_LDAP_SESSION_MINUTES` default 10) so later calls skip
  the directory. Empty passwords, names outside `[letters digits . _ - @]` and names matching several entries are
  refused; a directory outage is `UNAVAILABLE`, bad credentials `UNAUTHENTICATED`. Requires Flight TLS. The identity
  reaches Ranger as a Kerberos principal's short name does. Release asset `flight-auth-<v>-<sha>.tar.zst`
  (`auth/ldap/`).
- S3 / lakeFS read cache admission (item 17 (e)), opt-in: `arrowflight.s3.cacheAdmission` /
  `arrowflight.lakefs.cacheAdmission` = `always` (default, unchanged) or `reuse`: a missed block is kept only when it
  is read again while it is among the last N blocks not kept (N = the cache's blocks), so a scan larger than the cache
  passes through and leaves what small queries read again; repeated queries hit from their third run on. Metric
  `arrowflight_<kind>_cache_not_admitted_total`. Measured on the fake S3 why a size rule would not do: DuckDB 1.5.6
  reads a scan per column chunk (2 KiB - 3 MiB GETs on a lineitem-like table, 122880-row groups) while a
  `SELECT *` point lookup reads its whole row group (4.5 MiB) in one GET.
- Cap on the streams inside DuckDB at once (item 22 (c)), opt-in: `duckDbProducingStreams` /
  `arrowflight.duckdb.producingStreams` / `DUCKDB_PRODUCING_STREAMS` (default 0 = no cap, unchanged). A DoGet holds
  one of N turns (handed out in arrival order) only while DuckDB runs its query or computes its next batch, never while
  it waits for the client or the outbound budget, so endpoints a client reads in turn cannot block each other; a client
  that goes away while its stream waits ends the wait with CANCELLED. On 2 cores (`duckDbThreads=2`, in-process
  client on the same cores, 6 M-row table, two rounds) one partial-mode scan in 8 endpoints went 310/330 -> 381/397
  MB/s at value 2 (CPU 6.2/5.8 -> 5.0/4.9 s/GB), 4 endpoints 352/346 -> 326/371; value 1 was neutral.
  `ParallelScanPerfTest` takes `-Dperf.producingStreams` and prints the waits.
- `HiveOrcPerfTest` (perf tag) and the manual workflow `hive-orc-perf.yml` (P1 step 2 (e)): a lineitem-like Hive
  table (SF1 size by default) as Parquet and as ORC (zlib, zstd) through the server with the hive plugin, best of 3
  per query and result mode; complete answers of the ORC tables must equal the Parquet table's. Test only.
- `SmallQueryThroughputPerfTest` (perf tag) can read its table from the fake S3 through the real s3 plugin
  (`-Dperf.store=s3`, `-Ds3.perf.latencyMs`, needs `-Ds3.it.httpfs`); `-Dperf.threads` unset now means the server's
  own default instead of the test helper's 1 thread. Test only.
- DuckDB threads under load (item 22, also item 21 (b) option 3), opt-in: `duckDbThreadsUnderLoad` /
  `arrowflight.duckdb.threadsUnderLoad` / `DUCKDB_THREADS_UNDER_LOAD` (default 0 = off, unchanged). Each Flight
  worker gets a second DuckDB connection with that many threads, opened on first use; a DoGet that starts while the
  running DoGets times `duckDbThreads` exceed the cores runs on it (same setup, views and answers), a query alone keeps
  all threads. On 2 cores (`duckDbThreads=2`, value 1, pyarrow client on other cores) one partial-mode scan in 4
  endpoints went 712 -> 792 MB/s and in 8 endpoints 579 -> 611 MB/s, server CPU -13 % / -6 %. New perf test
  `ParallelScanPerfTest` (one scan in N partial-mode endpoints, or N concurrent complete-mode scans; optionally
  against a separately started server).
- `SmallQueryThroughputPerfTest` (perf tag) can run plain statements (`-Dperf.client=statement`: GetFlightInfo +
  DoGet) besides the stock prepared pattern, to compare the two client patterns (item 21 (d)). Test only.
- ORC tables in the Hive catalog, opt-in (P1 step 2 (c)): with `hive.orcTables=true`
  (`arrowflight.hive.orcTables`, default false: ORC tables stay skipped as before) the hive plugin shows ORC tables
  and reads their files itself (orc-core through the server's stores), one Arrow stream per table reference with only
  the columns the query names; partition values come from the metastore. The query's simple filters on integer,
  DOUBLE, DECIMAL, STRING/VARCHAR, BOOLEAN and DATE columns skip ORC stripes and row groups (the server still applies
  every filter). Transactional (ACID) ORC tables stay skipped with a log line. End-to-end server tests follow.
- End-to-end tests of ORC Hive tables through the server with the packaged hive plugin (P1 step 2 (d),
  `HiveOrcIntegrationTest`): both result modes answer ORC tables (ZLIB, ZSTD, SNAPPY, LZ4 and uncompressed files)
  exactly like the same rows in Parquet, incl. partition pruning, filters, group by a partition column and joins of
  ORC with Parquet and with ORC; the plugin's zstd-jni works next to the server's. Test only.
- Parallel stream compression (item 20, step (f)): the buffers of a large compressed batch are compressed side by
  side by the stream's thread and helper threads shared by all streams, largest first; the batch is sent once all
  are done (byte for byte the serial batch, no extra batches held). New setting `flightCompressionThreads` /
  `arrowflight.flight.compressionThreads` (default 0 = one per core, at most 8; 1 = serial as before). Only streams
  of clients that ask for compression are affected. New manual workflow `compression-perf.yml` and perf test
  `CompressionThroughputPerfTest`: one stream over loopback slowed by `tc netem` (10 ms, 1 Gbit), plain / zstd / lz4,
  serial vs parallel (Arrow MB/s, CPU, ratio).
- Compression answer checks for non-Java clients (item 20, step (e)): `CompressionClientsIntegrationTest` reads
  a mixed-type table through pyarrow Flight (C++) and the ADBC Flight SQL driver (Go), plain, `none`, `zstd` and
  `lz4`, in both result modes, and requires identical answers and server-side compressed batches per client
  (CI installs pyarrow 26.0.0 / adbc-driver-flightsql 1.13.0; skipped without `-Dpython.it.executable`).
  START-NOTES now describe the compression opt-in and the change our patched JDBC driver needs to ask for it
  (step (d)). No runtime change.
- Complete answers over catalog tables read as Arrow streams (P1 step 2 (b) part 2 (2); no plugin produces such
  tables yet): each reference of such a table named with its schema (`FROM hive.orders o`, joins, FROM lists,
  subqueries) reads a stream of its own with the columns the query names (all for `*`, `COLUMNS(...)` or a
  whole-row use of the alias); the WHERE clause's simple conditions go along as hints only when the query is one
  SELECT of that table alone. The statement is prepared for the run only, with its parameters bound
  (`DuckDbAdapter.streamCompleteOnce`); describing reads no data. These tables have no complete-mode view, so a
  reference without the schema fails as a missing table (never an empty answer). Other tables are unchanged.
- Partial answers over catalog tables read as Arrow streams (P1 step 2 (b) part 2 (1); no plugin produces such
  tables yet): the planner ships their files with partition values like Hive Parquet files, every endpoint asks
  the catalog for one stream with the columns the query names (all for `*`, one for `count(*)`) and the simple
  conditions of the WHERE clause's top-level AND as hints (`ArrowScanRequests`), runs the query once over it
  without the statement cache (`DuckDbAdapter.streamSqlOnce`) and drops the stream afterwards; describing such a
  query reads no data (declared columns).
- Flight SQL sessions with the option `arrow-compression` (item 20, step (c)): SetSessionOptions /
  GetSessionOptions / CloseSession (were UNIMPLEMENTED) keep options per `arrow_flight_session_id` cookie on the
  node; `arrow-compression` (`none`/`zstd`/`lz4`) is the codec of the session's DoGet calls without the header
  (header first, then the session, then `flightCompression`). Unknown cookies are ignored rather than failing the
  call (unlike Arrow's own session middleware); sessions are bound to the caller's identity, expire after
  `flightSessionIdleSeconds` (3600) idle and are capped at `flightSessionMax` (10000) per node. ListActions now
  lists the three session actions.
- SPI 1.4 (P1 step 2 (b), additive, no runtime change yet): a catalog may hand table data to the server as Arrow
  streams - `ReadSpec.ArrowStream`, `ArrowScan` (columns + filter hints) and `Catalog.exportArrowScan` (default
  refuses). Server side `ArrowStreamScans` registers one DuckDB Arrow stream per table reference and drops it after
  the query (DuckDB's registered streams are read once); read_parquet sources refuse such snapshots. Plugins built
  against SPI <= 1.3 load unchanged.
- LZ4 compression of result streams (item 20, step (b)): `arrow-compression: lz4` (also `lz4_frame`) and
  `flightCompression=lz4` send Arrow LZ4_FRAME batches, compressed by our own codec on lz4-java (JNI) from the
  runtime libs set straight between Arrow buffers (~510 MB/s single thread vs ZSTD level 1 ~330 MB/s, at half
  ZSTD's ratio on small longs). Was refused before; refused still when lz4-java is not on the class path. Java
  Flight clients decode it with `arrow-compression` + `commons-compress`.
- Opt-in Zstandard compression of result streams (item 20, step 1): a client that sends `arrow-compression: zstd`
  gets Arrow IPC batches with ZSTD body compression (level `flightCompressionLevel`, default 1); batches under
  `flightCompressionMinBytes` (64 KiB) and streams whose batches do not shrink go plain; `flightCompression`
  (default `none`) sets the codec for calls without the header. Clients that do not ask get plain batches as
  before. Unknown values and `lz4` (Arrow Java's LZ4 codec is far too slow) are refused with INVALID_ARGUMENT.
  Metrics `arrowflight_flight_compress*`. New dependency `arrow-compression` (zstd-jni kept at parquet's version).
- Plugin descriptors: `share` takes a comma separated list of `hadoop` and `arrow` (was empty or `hadoop`);
  with `arrow` the plugin's class loader takes `org.apache.arrow.*` from the server (one Arrow memory and C data
  library per JVM, so a plugin can hand Arrow streams to the server) and a jar of the plugin that carries Arrow
  classes is refused. The load line logs `shareArrow=`. Existing descriptors are unchanged.
- Hive plugin (P1 step 2 (a)): descriptor `share=hadoop,arrow` - the plugin now runs on the server's Hadoop and
  Arrow classes (needed by the ORC reader); the metastore client is unchanged (Hive classes stay the plugin's).
- Hive plugin, ORC step 1 (P1, no runtime change yet): `OrcArrowReader` reads one ORC file of a Hive table as Arrow
  batches through the server's stores (orc-core 2.1.3 over a one-file Hadoop file system wrapping the store's
  `ReadableFile`): columns by name (case-insensitive) or by position for Hive 1.x `_colN` files, missing columns
  NULL, ORC schema evolution for changed types, projection, stripe/row-group skipping by search argument,
  timestamps as wall clock, CHAR without trailing spaces; all Hive types incl. decimal, date, timestamp, binary,
  list/map/struct. ORC tables are still skipped by the catalog; the plugin directory gains orc-core and its libraries.
- Iceberg time travel in partial mode (item 3 (a)): a query that reads one iceberg-hadoop / iceberg-rest table
  `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF` and nothing else is planned in parts like a query of the current
  snapshot (endpoints with locations; each ticket names files of the earlier snapshot, read with that snapshot's
  columns and deletes) instead of one complete endpoint. Joins, subqueries, set operations, several clauses and
  snapshots that cannot be read file by file still get the complete answer. Ticket format unchanged.
- Perf test `ScanThroughputPerfTest` (tag `perf`, item 16): one client streams a full table scan over Flight SQL
  (complete mode) per variant of `duckDbPreserveInsertionOrder` / `duckDbStreamingBufferSize` and prints MB/s and
  batches (`-Dperf.variants=true:,true:64MiB`, `-Dperf.threads`, `-Dperf.rows`, `-Dperf.runs`). Defaults unchanged.
- lakeFS read cache (item 17 (b); opt-in): `arrowflight.lakefs.cacheMegabytes` (default 0 = off) and
  `arrowflight.lakefs.cacheCheckSeconds` (default 10) let DuckDB read the lakeFS S3 gateway through a node-wide,
  byte-bounded cache like the s3 plugin's (`arrowflight.s3.cacheMegabytes`), scoped to the repository; separate
  from the s3 cache (log `lakefs=cacheOpen` / `lakefs=cacheClosed`, metrics `arrowflight_lakefs_cache_*`). The lakefs
  plugin descriptor now says `spi=1.3`. New public s3 plugin class `S3ReadCache` for plugins built on it.

- S3 read cache metrics (item 17 (d)): while `arrowflight.s3.cacheMegabytes` > 0 the Prometheus endpoint shows
  `arrowflight_s3_cache_capacity_bytes`, `_used_bytes`, `_blocks` (gauges) and `arrowflight_s3_cache_hits_total`,
  `_misses_total`, `_evictions_total`, `_joined_total`, `_s3_reads_total`, `_s3_read_bytes_total`,
  `_s3_lookups_total` (counters; the last three count the GETs, bytes and HEADs the cache sent to S3). New SPI 1.3:
  `StoreContext.metrics()` (`PluginMetrics`, `MetricSource`, `MetricSample`) lets a plugin publish unlabelled
  `arrowflight_*` samples on the server's endpoint; a name the server or another source already shows is left out.
  The s3 plugin descriptor now says `spi=1.3` (it needs a server of this build).
- S3 read cache, single flight (item 17 (c)): concurrent requests that miss the same 1 MiB block read it from S3
  once; the later ones wait for the first read and take its bytes (or its failure: a refused `If-Match` makes each of
  them look the version up again and read the new one, again once for all). The `s3=cacheClosed` line counts these
  blocks as `joined=`. Only active with `arrowflight.s3.cacheMegabytes` > 0.
- S3 read cache (item 17, option A; opt-in): `arrowflight.s3.cacheMegabytes` (default 0 = off) starts one
  node-wide, byte-bounded cache (JVM heap, at most half of it) that every DuckDB connection reads `s3://` through: a
  127.0.0.1-only HTTP endpoint of the s3 plugin answers ranged GETs from 1 MiB blocks (least recently used dropped
  first) and reads misses from S3 with the store's own client and `If-Match` on the object's ETag, so an answer never
  mixes two versions of an object. DuckDB's secret then holds a random per-store key the endpoint checks as an AWS
  SigV4 signature (the S3 credentials stay in Java); a key reads only its own data directory; listings are not
  served. HEADs always reach S3; an object's version is trusted for `arrowflight.s3.cacheCheckSeconds` (default 10)
  between checks, so a file rewritten in place under the same key may be read in its old version for that long.
  Applies to the main store and named stores (`arrowflight.s3.stores`), not yet to lakeFS. Startup line
  `s3=cacheOpen`, shutdown line `s3=cacheClosed` with hits/misses/evictions.
- Distributed complete mode, step 1 (item 18): `CompleteAnswerMerge` decides whether a query's
  complete answer can be rebuilt from the answers its partial-mode endpoints give for their own
  files and renders the DuckDB merge (rows concatenated; counts and sums summed, min/max taken
  again per selected group; DISTINCT, ORDER BY over output columns and LIMIT applied again).
  Refused, so answered as today: joins, OFFSET, HAVING, DISTINCT aggregates, avg and other
  expressions, LIMIT on an aggregate, GROUP BY columns that are not selected, ORDER BY over
  anything but an output column. Checked against DuckDB's own answer over all rows
  (`CompleteAnswerMergeTest`). Not wired into query execution yet: no runtime change.
- Tests (S3 read path, item 17): `S3IntegrationTest.repeatedQueriesReadOnlyTheRowGroupsTheyNeed`
  pins the request pattern of the s3 plugin's defaults on DuckDB 1.5.6 (a repeated query: no HEAD,
  no footer read, one GET per row group it reads; a pruned point query at most 2 GETs; with
  `metadataCache=false` every query looks up and reads each footer again), so a DuckDB or setting
  change that adds round trips fails CI. `S3ReadPathPerfTest` (tag `perf`) prints time and
  requests per query for thread counts and read settings against the fake S3 with a latency per
  request (`FakeS3.latency`). No runtime change.
- S3: setting `arrowflight.s3.connectionCaching` (default true; named stores and lakeFS inherit it)
  turns on DuckDB 1.5's `httpfs_connection_caching`: DuckDB keeps its S3 connections and reads later
  ranges and queries over them. Without it httpfs opened a connection for about every range read
  at 2+ threads (8 files x 4 row groups at 8 threads: 40 then 31 connections for two queries; now
  8 then 0), and the closed ones ran a host out of local ports under 32 concurrent clients (item
  0g). The startup `tcp_tw_reuse` WARN now appears only with `connectionCaching=false`; the
  `s3=open` line names the setting. Answers unchanged.
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

### Changed
- Cluster: a node that crashed with its address (host down, container removed) no longer holds every plan on the other
  nodes until Hazelcast drops it (10 s). The planner reads node loads, heartbeats and inventories through a local cluster
  view: the first plan waits at most 1 s for the silent member, later plans not at all; its node is left out of plans
  (not removed from the pool) until any operation on its partitions answers or it leaves the cluster, and its keys take
  the last values read (refreshed at every heartbeat). `hazelcast.max.no.heartbeat.seconds` stays 10 (item D3).
- Cluster: writes no longer wait for such a node either (item D3 step 2). A write whose key lives on a member that stopped
  answering (its owner, or the synchronous backup the write waits for up to 5 s) runs in the background: load changes
  (counted in this node's plans at once), endpoint load reservations and their releases (in order per key), handles,
  prepared-statement parameters, pool removals and the heartbeat (one pending at a time). The heartbeat thread
  refreshes the cluster view first and skips repairs it cannot do now; a live node whose heartbeat entry sits on the
  silent member is no longer dropped as stale. Measured with an iptables DROP: 40 load-tracked tickets with their
  releases and heartbeats in 65-88 ms (before: the first write on the silent member's partitions waited until
  Hazelcast dropped it, up to 10 s; a write whose backup alone was silent waited the 5 s backup timeout).
- Cluster: the remaining waits for such a node (item D3 step 2b). A backup node moving an endpoint's load reservation
  whose entry sits on the silent member no longer holds the end of the stream (the move and the release queue in the
  background, in order); publishing this node's file inventory and the heartbeat's repairs of the inventory and ticket
  secret (maps kept on every member, whose puts wait for each one) wait in the background or for the next beat, and
  this node's own pending inventory counts as published; a node that stops while its registry entry sits on the
  silent member leaves without deregistering (the others drop it when its member leaves).
- Cluster: plans on local disks (each node its own data directory) no longer wait for such a node either (item D3
  step 3). They need every published inventory, so that files only a lost node held fail the plan instead of leaving
  the answer; the full read of the inventory map waited for the silent member. Each node now keeps a local copy of the
  inventories (full reads plus the map's entry events) and, while a member does not answer, reads the inventories on
  members that answer by key and takes the others from that copy; a member's inventory it never saw makes it read the
  whole map as before. Measured with an iptables DROP: local-disk planning reads well under 500 ms (before: until
  Hazelcast dropped the member, 10 s).
- DuckDB threads and memory by host size (item 19b): with `duckDbMemoryLimit` unset every DuckDB connection now gets
  `memory_limit` = the node's DuckDB budget (container `memory.max` or physical memory, minus `-Xmx`, direct memory
  and 15 % headroom) / the connections expected at once (`duckDbMemoryConnections`, default cores / threads, at
  least 2), at least 1 GiB and at most 80 % of the memory (was unset = DuckDB's 80 % of RAM *per instance*);
  `duckDbMemoryLimit=none` keeps DuckDB's default, an explicit size wins. The `duckDbThreads` default is cores / 4
  between 2 and 16 (was 8; S3/lakeFS at least 4 as before), capped at one thread per GiB of the connection's limit
  but never below 2. Hosts up to 32 cores keep their thread count. Startup line `duckdb=sizing` shows the inputs
  and the chosen values.
- Hive ORC tables (`hive.orcTables`): a file larger than 8 MiB is read as several ranges of whole stripes (planned
  from its footer; at least a quarter of the file's size per range when it is large enough, at most about 32 MiB),
  each a read-ahead unit next to the other files' (P1 step 2 (g)), so a table of one large file is decoded in
  parallel too; batches still reach DuckDB in file and stripe order, and the first and last ranges are open, so every
  stripe is read once whatever the footer or listed size say. Answers unchanged; SF1 lineitem ORC in one file on a
  4-core runner: Q1-style 0.73-0.83 s, Q6-style 0.37-0.46 s, as fast as the same table in 4 files.
- Hive ORC tables (`hive.orcTables`): batches of integer, date, double, decimal (<= 18 digits), string and binary
  columns are copied into Arrow in bulk instead of value by value (P1 step 2 (f)). Answers unchanged; local 2M-row
  lineitem ORC: `SELECT *` 2.3 -> 1.9 s, Q1-style 0.8 -> 0.7 s.
- Hive ORC tables (`hive.orcTables`): the files of a table scan are decoded up to 4 at once (at most the core count)
  on the plugin's `hive-orc-read-*` threads, each at most 2 batches ahead; batches still reach DuckDB in file order,
  closing the stream stops the decoding and frees what was read ahead (P1 step 2 (f)). Answers unchanged; local
  2M-row lineitem ORC in 4 files: Q6-style 0.45 -> 0.26 s, `SELECT *` 1.9 -> 1.26 s (Parquet 0.03 / 0.85 s).
- Hive plugin, ORC tables: decimals of up to 18 digits are decoded by ORC straight into longs (`USE_DECIMAL64` row
  batches) and copied to Arrow without a `BigDecimal` per value; Q1-style queries over ORC 5-30 % faster
  (SF1 on a CI runner ~5 %, 1M rows locally ~30 %; P1 step 2 (e)). Answers unchanged (new test `OrcArrowReaderTest.readsDecimalsOfEveryWidthAndWidenedScales`).
- Less CPU per small query (item 21, step 1): GetFlightInfo of a complete answer on a node alone in its cluster no
  longer reads the server registry and heartbeat maps (it checked the node count on every query over local
  directories); the per-DoGet `executionPath=` event is written as text instead of through a Jackson tree (same
  text); endpoint tickets reuse one HMAC instance per thread; binary ticket query ids are formatted without
  `String.format` (same ids). Local, 2 CPUs, point lookups + filtered counts over 4 local Parquet files, client in
  the same JVM (`SmallQueryThroughputPerfTest`): c8 ~159 -> ~188 queries/s, c32 ~222 -> ~250, CPU per query
  ~12.3 -> ~10.4 ms at c8. No wire, setting, answer or log-text change.
- Iceberg position deletes (item 2 (iii)): data files that share their delete files (as after a MERGE) are now read
  in one `read_parquet` whose position delete files are read once and matched by DuckDB's `file_index` and the row
  number, instead of one UNION ALL branch per data file that re-read every delete file. Local, 500 files x 2000 rows
  with one shared position delete file: prepare ~750 -> ~20 ms, run ~1200 -> ~50 ms, same answer. A single file, and
  any table with a column named `file_index`, keep the per-file form.
- `duckDbThreads` default is size-aware (item 19, approved in stage 2): unset or 0 = a quarter of the cores the JVM
  sees, between 2 and 8 (4 cores: 2, 16: 4, 32+: 8; was a fixed 4). Every DuckDB connection (one per busy worker
  thread) runs that many threads, so 32 concurrent sessions at 4 threads on 4 cores oversubscribed the CPU (local
  c32 point p50 64-69 ms at 4 threads vs 57 at 2). An explicit value is used as before; a negative one is refused
  at startup. The bundled `arrowflight.properties` leaves it empty. New startup line `duckdb=threads perConnection=
  availableProcessors=`.
- Complete-answer mode: less work per query (item 15). The result and parameter schema of a query
  are described once per views snapshot and shared by the client's prepare and its GetFlightInfo
  (before: three DuckDB describes and four parse checks per JDBC/ADBC query); the describe
  connection keeps its parse-check statement; closing a prepared statement no longer waits for the
  shared parameter cache. Local measurement (`PointQueryOverheadPerfTest`, tag `perf`): point
  query p50 14.0 -> 10.2 ms, 8 clients 31 -> 22 ms, 32 clients 94 -> 58 ms. New metric label
  `describe` on the cache counters. Answers, wire format and settings unchanged.
- Less work per DoGet (item 15 step 2): a query text is parsed for the single-query check once per
  node (the describe at prepare and the DoGet shared none of it before; the refused function names
  are still checked on every use, also names a plugin adds later); DEBUG-only log arguments
  (durations formatted with `String.format`) are no longer built when DEBUG is off; the per-DoGet
  execution-path event reuses its regular expressions and SHA-256 digest (same output). Local
  measurement: complete-mode point query DoGet p50 6.2 -> 5.4 ms, total 13.1 -> 11.9 ms. New metric
  label `parse_check` on the cache counters. Answers, wire format, log lines and settings unchanged.
- Partial-result mode: a new query text no longer lists the table's files to describe it (item 15
  step 2 (a)). The file a table's queries are bound against is remembered per table for 30 s (the
  metadata TTL); a describe that cannot read it (table rewritten) lists the table again once.
  Local measurement: partial point query prepare p50 28.5 -> 3.4 ms, total 38.4 -> 12.5 ms,
  8 clients 97 -> 28 ms. Answers, wire format and settings unchanged.
- The per-DoGet INFO line `executionPath=...` (and the optional `BENCHMARK_EXECUTION_PATH_LOG`
  evidence file) is written by one background thread instead of the DoGet thread (item 15 (b),
  approved by the maintainer); same level, text and order per query; when 8192 events are pending
  the DoGet writes its own; pending events are written at shutdown. Local measurement: complete-mode
  32 clients p50 90 -> 78 ms, partial 8 clients 28 -> 24 ms.
- Complete mode: a prepared query without `?` parameters gets the plain handle (its SQL text, as in
  partial mode) instead of one with a statement id, so its GetFlightInfo no longer reads the
  cluster map for parameter values and its close no longer deletes from it (item 15 step 3). A
  query with parameters keeps the id handle. Answers and settings unchanged; the only visible
  difference is for a client that sends values to a query without parameters and keeps using the
  first handle: the query now runs without them instead of failing with a count mismatch.
  Local: close p50 1.15 -> 1.05 ms, GetFlightInfo 2.4 -> 2.2 ms (one node; a remote partition
  owner saves a network round trip per query).

### Fixed
- `duckDbThreadsUnderLoad`: a query alone on the node now keeps its `duckDbThreads`, as documented, also when
  `duckDbThreads` exceeds the cores (the default floor of 4 threads on 1-3 core nodes); before, every query on such
  a node ran on the low-thread connection. `SmallQueryThroughputPerfTest` takes `-Dperf.threadsUnderLoad`.
- DuckDB thread default no longer drops below 4 (laptop stand p19b, 2026-10-28): since item 19b 4c/8g, 8c/16g and
  16c/24g containers got 2/2/3 threads per connection (cut by the one-thread-per-GiB cap) and TPC-H SF10 ran
  1.35-1.7x slower than with 4. Default now cores / 4 between 4 and 16, the per-GiB cap applies only above 4; the
  default per-connection `memory_limit` keeps its values (1024/2201/3942 MiB on those profiles). New manual workflow
  `tpch-perf.yml` (SF10 in a 4c/8g container, default vs `-DduckDbThreads=4`, fails when the default is > 10 % slower
  or answers differ) with the perf-tag client `TpchPerfTest`.
- S3 / lakeFS read cache took about twice its size in heap (item 17 (e), CI screening `COMPARE-CACHE-2026-10-27`:
  RSS ~5.7 GB with `cacheMegabytes=1024` vs ~2.3 GB off): each 1 MiB block was one array, a humongous object for G1
  that takes whole regions (256 MiB of blocks used 512 MiB of heap at `-Xmx3g`); a miss of up to 64 MiB was also
  buffered by the SDK, copied, and copied again per block. Blocks are now held in 256 KiB pieces (256 MiB of blocks =
  256 MiB of heap) and misses are streamed from S3 into them, no whole-GET buffer. Answers unchanged.
- Tests: `TestFlightServerHelper` ignored `endpointsPerServer`, so in-process partial-mode scans were always one
  endpoint (`ParallelScanPerfTest` endpoint runs and `ThreadsUnderLoadIntegrationTest` measured one stream); the
  integration tests now assert the split. Test only.
- Arrow-stream tables (Hive ORC, `hive.orcTables`): a multiplication in the query (`sum(a * b)`) counted as a
  `SELECT *`, so the catalog streamed every column of the table (P1 step 2 (f)). `ArrowScanRequests` now takes a
  star between two values for a product; a star that may select columns (`SELECT *`, `t.*`, `DISTINCT ON (a) *`,
  `* EXCLUDE`, a star next to a comment) still reads all. Answers unchanged; local 2M-row lineitem ORC: Q6-style
  1.8 -> 0.5 s, Q1-style 2.0 -> 0.8 s.
- S3 small queries: longer tail since the size-aware `duckDbThreads` default (item 19; 4 vCPU -> 2 threads). With
  `duckDbThreads` unset or 0 and `dataDir` on `s3://`, `s3a://`, `s3n://` or `lakefs://` the default is now at least
  4 (the value before item 19; larger hosts keep cores / 4, at most 8); local and HDFS data directories are unchanged,
  an explicit value still wins. Local A/B, 4 cores, fake S3 at 2 ms per request, prepared point/count queries:
  2 -> 4 threads c1 p95 48 -> 35 ms, c8 p95 77 -> 67 ms (10 ms per request: c1 107 -> 64, c8 123 -> 101), c32 q/s
  -18 % on this CPU-saturated in-process run.
- S3 read cache: concurrent requests for the same missing span now claim all its blocks under one lock, so the
  span is read with one GET; before, two requests could each claim part of it and read it in two GETs (answers
  were right). Suspected cause of the `S3CacheEndpointTest` count failures in CI runs 40586746160 and 40593759006.
- S3 read cache: a miss read whose answer carries another ETag than the version looked up is refused like a failed
  `If-Match`, so a store or gateway that ignores `If-Match` cannot mix two versions of an object in one answer.
- S3 (DuckDB 1.5.6): after an object was rewritten, the first query of every server connection
  that had read it before failed "HTTP Error: ETag on reading file ... was initially ... this likely
  means the remote file has changed" (DuckDB keeps a file's metadata per connection; the next query
  reads the new file). A query refused this way before anything was sent now runs once more and
  returns the new file's rows (INFO `duckdb=changedFileRetry`); a refusal after the first batch
  still fails the stream. Test `S3IntegrationTest.aQueryAfterAFileWasRewrittenReadsTheNewFile`.
- Cluster: why the first plan after a kill test waited 9-10 s on one stand and ~1 s on another
  (3-node S3 stand finding (c)): a crashed node whose address vanished with it holds every map
  operation on its partitions until Hazelcast drops it (`hazelcast.max.no.heartbeat.seconds`,
  default 10, `-D` overrides it); one whose port still refuses is dropped at once. Documented in
  START-NOTES; `ClusterMemberLossIntegrationTest` covers both cases and the override (the
  vanished-address case uses iptables as root or via `sudo -n`, skipped otherwise). No code change.
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
- DuckDB 1.5.6 (was 1.4.5; `duckdb_jdbc` 1.5.6.0): plain DuckDB answers a point lookup ~2x faster
  on the stand files. Deployments need the httpfs, iceberg, avro and hadoopfs extension files of
  v1.5.6 (the startup check refuses other versions); Dev CI takes them from `ci-assets` or, until a
  v1.5.6 set is there, the official httpfs/iceberg/avro builds.
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
