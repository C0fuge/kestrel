# Plugins and integrations lane - status

Second lane of development (see `CLAUDE.md`, "Second lane"). Basis: the maintainer's research of 2026-10-28 (summary
below). Production target: on-prem Hadoop 3.1.3 (HA, Kerberos), Hive Metastore, Ranger 2.4; BI and Spark users.

## Next steps (one per session, in this order)

P1. **ORC tables in the Hive catalog.** Today the Hive plugin reads only Parquet and Iceberg tables and skips ORC; most
    production Hive tables are ORC. DuckDB has no usable ORC reader (the community `orc` extension reads local files
    only, no pushdown, no 1.5.6 build). Build a Java reader: Apache `orc-core` (with `hive-storage-api`) reading from HDFS
    (Kerberos, HA nameservice) and S3 through the existing storage setup, converting stripes to Arrow and handing them to
    DuckDB as an Arrow stream / registered scan (projection, predicate pushdown via ORC search arguments, stripe
    statistics, partition columns from the Hive partition spec). Opt-in per catalog first; refuse clearly (no silent
    skip) for ORC ACID tables until P6. Tests: ORC files written in the test (all Hive types incl. decimal, timestamp,
    date, char/varchar, binary, list/map/struct), answers equal to the same data in Parquet; partitioned table; HDFS
    mini-cluster or the existing CI HDFS container. Check both partial and complete result modes. Perf: SF1 lineitem in
    ORC vs Parquet (note it in the Log).
    *Step 1 done 2026-10-27:* `OrcArrowReader` (hive plugin) - one ORC file -> Arrow batches (an `ArrowReader`, so
    `Data.exportArrayStream` can hand it to DuckDB), read through the store's `ReadableFile` via `ReadableFileSystem`;
    tests `OrcArrowReaderTest` (all types + nulls + nested, by name / by position, projection, SARG skipping).
    Dependencies: orc-core/orc-format `shaded-protobuf` (the test metastore needs protobuf 2.5), hive-storage-api 2.8.1
    (now compile; the test metastore runs on it), Arrow and Hadoop `provided`.
    *Step 2 (a) done 2026-10-27:* descriptor `share` is a list of `hadoop`/`arrow`; `share=arrow` takes
    `org.apache.arrow.*` from the server and refuses plugin jars carrying Arrow classes (core commit 08966aa, test
    `PluginSharingTest`); hive descriptor `share=hadoop,arrow` (df0328d) - `HiveCatalogIntegrationTest` passes with
    the server's Hadoop and Arrow (locally with a UTF-8 locale: its partition path has non-ASCII characters).
    *Step 2 (b) part 1 done 2026-10-27:* SPI 1.4 (6884223): `ReadSpec.ArrowStream(format)`, `ArrowScan(columns,
    predicates)` (columns non-empty - DuckDB refuses a stream without columns, "must have at least one column";
    predicates are hints, the server keeps every filter above the stream), `Catalog.exportArrowScan(snapshot, files,
    scan, streamAddress, Object allocator)` (default refuses; partition columns come as UTF-8 text, the server
    casts). Core e3e22bf: `ArrowStreamScans` (per query: one `ArrowArrayStream` + `registerArrowStream` view per table
    reference, `DROP VIEW` + release on close; test `ArrowStreamScansTest` 5/5: self-join over two streams, partition
    text -> INTEGER, unread and failed streams released with 0 bytes leaked), `SpiCatalog` delegation,
    `CatalogSources.filesSource` refuses ArrowStream snapshots (so complete-mode views skip such tables with a log
    line, partial queries fail clearly). Checked by experiment on duckdb_jdbc 1.5.6.0: `registerArrowStream` =
    `CREATE OR REPLACE TEMP VIEW` over a one-shot stream - a second read (second statement, second reference,
    even a re-prepare) fails "This stream has been released"; a view over it can be read once. duckdb_jdbc 1.5.6
    also has Java table functions (`DuckDBTableFunction`), considered and rejected: projection pushdown only, no
    filter pushdown, per-value JNI writes, no LIST/MAP/STRUCT/BLOB.
    *Step 2 (b) part 2 (1) done 2026-10-28:* partial mode reads ArrowStream snapshots (core commit, STATUS.md Log):
    `ExecutionService.streamArrowScan` (one `ArrowStreamScans` per endpoint call, closed in `finally`),
    `DuckDbAdapter.streamSqlOnce` (prepared per run, never cached, parse check not remembered, no changed-file
    retry), `catalog/ArrowScanRequests` (columns = declared columns named anywhere in the endpoint SQL, all for a
    star, the first for none; predicates = simple conditions of the WHERE's top-level AND with literals of the
    column's exact declared type - BIGINT/INTEGER/SMALLINT/TINYINT -> Long, DECIMAL -> BigDecimal, DOUBLE ->
    Double, VARCHAR, BOOLEAN, DATE, TIMESTAMP; FLOAT and others never), `ExecutionService.shipsPartitionValues`
    (planner + `ticketFiles`; partition pruning applies), describe -> `CatalogViews.declaredColumnsSource`. Tests
    `ArrowScanRequestsTest` 6/6, `ArrowStreamExecutionTest` 5/5 (whole snapshot, same text twice, count(*), group
    by partition, planner tickets -> endpoint read, outside-root refusal, describe reads nothing, failing catalog
    leaves no view/buffer).
    **Next step (step 2 (b) part 2 (2)):** complete mode - rewrite each reference of an ArrowStream table in the
    client SQL to its own stream source, the way `CatalogViews.timeTravel(sql, describe)` rewrites time-travel
    references (reuse its reference finder; `TimeTravelClause.find`), with `ArrowScanRequests.of(snapshot,
    clientSql, null)` per reference (columns from the whole client SQL; predicates only when the query is a single
    table - a filter of a join may not hold for this reference), run through `streamSqlOnce`; describe ->
    `declaredColumnsSource`; complete-mode views keep skipping the table for queries that do not name it. Watch:
    `streamComplete` applies views and binds parameters on a cached statement - the stream variant needs the same
    parameter binding on an uncached one. Then (c) hive `exportArrowScan` with `OrcArrowReader`
    (one reader per file chained; set the plugin's context class loader inside `loadNextBatch`, since DuckDB calls
    it on its own threads; predicates -> ORC SARG), `hive.orcTables` + ACID refusal, (d) end-to-end tests as planned
    above. Risk still open: the plugin ships zstd-jni 1.5.6-10 next to the server's copy (separate class loaders
    extract their own temp copy of the native library - expected to work, confirm in the end-to-end test).
P2. **Login for generic clients: LDAP/AD and JWT (OIDC/Knox) auth providers.** JDBC/ODBC/ADBC and BI tools can send only
    user/password or a bearer token; with Ranger on, today they need Kerberos. Add auth providers (plugin modules behind
    the existing auth SPI): LDAP bind (ldaps, StartTLS, user DN template or search, group lookup), JWT verification
    (JWKS URL with cache, issuer/audience, user and groups claims). The resulting identity must flow to Ranger checks
    exactly like a Kerberos principal. Tests: OpenLDAP container and a local JWKS in CI; negative cases (bad password,
    expired token, wrong audience). Document in START-NOTES. Wire behaviour: new providers only, existing ones unchanged.
P3. **Spark connector for production (`flight-spark`).** (a) Kerberos on YARN executors: a Spark
    `HadoopDelegationTokenProvider` that gets a server session on the driver and ships it in the job credentials (today
    every executor does its own SPNEGO and fails without a TGT); (b) build profiles for Spark 3.3/3.4 (check what Hadoop
    3.1.3 clusters usually run) and Spark 4.x (Scala 2.13, JDK 17/21); (c) a proper Spark catalog (namespaces/tables from
    the server's metadata). Look at upstream's `feat/starrocks-native-integration` branch (runtime/bloom filters, AQE
    guard) - port what is a clear win, ticket format changes go to Open questions first. Tests: Spark local mode (existing
    175), a Spark 4 job, a KDC container + keytab job.
P4. **Hive TEXTFILE/CSV, Avro and JSON tables, then simple Hive views.** DuckDB's readers over the existing HDFS/S3
    setup; SerDe properties (field/line delimiters, null format, escape, skip.header), partition columns. Views: plain
    HiveQL views whose text DuckDB can run after a small translation; refuse the rest clearly.
P5. **Plain Flight / command descriptors (ClickHouse `arrowFlight`, other generic Flight clients).** Accept a SQL text
    in a CMD `FlightDescriptor` (not wrapped in a Flight SQL command) for read queries, same auth and Ranger checks;
    test with a ClickHouse container (`arrowFlight` table function) and DuckDB's `adbc` path. This touches core request
    handling: keep it small and coordinate via `STATUS.md` Log; wire change -> Open questions first, opt-in setting.
P6. Next wave (order may change after the maintainer's answers): Ranger Hive-policy reuse with column masking / row
    filters; Hive ACID (insert-only first); Iceberg REST catalogs (Polaris, Nessie, Lakekeeper, Unity) with vended
    credentials; Postgres/MySQL federation; Ozone S3 gateway test; Delta via Delta Kernel Java.

## Open questions

- (maintainer, pending) share of ORC / Parquet / text / ACID tables in production; Spark versions and YARN keytab use;
  Ranger masking/row filters in use; AD/LDAP or Knox for BI logins; BI tools in use; Delta/Hudi/Paimon/Ozone; plan for
  DuckDB 2.0 (released 2026-10-21; community extensions move to it first).

## Log

- 2026-10-28 (maintainer): lane created; P1-P6 from the integration research.
- 2026-10-27 (plugins session): P1 step 1 - ORC file reader to Arrow in the hive plugin (`OrcArrowReader`,
  `ReadableFileSystem`, 5 unit tests); no catalog/runtime change, no core or SPI change. CI: run 40586746160 (9438f59)
  failed in the s3 plugin on a timing test (`S3CacheEndpointTest.aChangedObjectFailsEveryWaiterAndIsReadAgainOnce`, 3
  GETs instead of 2; passes locally 3/3; its re-run was cancelled by the core lane's push); run 40587941135 (6ca7352, on
  top of 9438f59): all plugin modules green incl. hive (OrcArrowReaderTest 5/5), server red only on `LibsSetTest`
  (commons-io dropped from the libs set by the core lane's `arrow-compression` dependency in 6ca7352, checked locally;
  noted in STATUS.md Log). Next session: confirm a green run once the core lane fixes LibsSetTest, then step 2.
- 2026-10-27 (plugins session): P1 step 1 confirmed green (run 40589347178 on 85aaf46). P1 step 2 (a): `share=arrow`
  in plugin descriptors (core commit 08966aa, additive; noted in STATUS.md Log) and `share=hadoop,arrow` for the hive
  plugin (df0328d); tests `PluginSharingTest` 3/3, `PluginLoadingTest` 6/6, `HiveCatalogIntegrationTest` 6/6 (+1
  iceberg skip) locally. CI: run 40593759006 (df0328d) red only on
  `S3CacheEndpointTest.aChangedObjectFailsEveryWaiterAndIsReadAgainOnce` (3 GETs instead of 2, second time after
  40586746160); fix 9c6267c - the s3 read cache claims a missing span's blocks under one lock (requests could split
  a span into two GETs; the failure's stats fit that) + stress test `concurrentMissesOfOneSpanAreReadWithOneGet`.
  Not reproduced locally (even pinned to one CPU), so the cause is suspected, not proven: if the count failure
  recurs, look again (next suspect: a reader arriving between the 412 and the version being forgotten). Run
  40594636655 (9c6267c, all modules) green. No package release: no runtime behaviour visible to
  clients changes (ORC still skipped). Step 2 (b) findings and proposal in Next steps.
- 2026-10-27 (plugins session): P1 step 2 (b) part 1 - SPI 1.4 (6884223: `ReadSpec.ArrowStream`, `ArrowScan`,
  `Catalog.exportArrowScan`) and core e3e22bf (`ArrowStreamScans`, `SpiCatalog` delegation, read_parquet refusal
  of ArrowStream snapshots; FYI line in STATUS.md Log). Local: `SpiTest` 7/7, `ArrowStreamScansTest` 5/5,
  `PluginLoadingTest` 6/6 (its SPI-version expectation now follows `SpiVersion.current()`), Checkstyle + SpotBugs
  clean. CI: run 40603510805 (e3e22bf) cancelled by the core lane's push; run 40603766088 (ba4139a, on top of
  e3e22bf) green, all modules. No package release: no client-visible change.
- 2026-10-28 (plugins session): P1 step 2 (b) part 2 (1) - partial mode wired for ArrowStream snapshots (core
  3f81556: `ArrowScanRequests`, `DuckDbAdapter.streamSqlOnce`, `ExecutionService.streamArrowScan` /
  `shipsPartitionValues`; SPI doc-only beb7349; FYI line in STATUS.md Log). Local: `ArrowScanRequestsTest` 6/6,
  `ArrowStreamExecutionTest` 5/5, `DuckDbAdapterTest` 90, `QueryPlannerTest` 58, `ExecutionServiceTest` 43,
  `HiveCatalogIntegrationTest` 7 (+1 skip), Checkstyle + SpotBugs clean. CI: run 40608463690 (31f16f5) green.
  No package release: no plugin serves ArrowStream yet, nothing client-visible changes. Next: part 2 (2), complete
  mode (see Next steps).
