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
    *Step 2 (b) part 2 (2) done 2026-10-28:* complete mode reads ArrowStream snapshots (core 53c4d71, STATUS.md
    Log): such tables get no view (marker in `CatalogViews.refresh`); `CatalogViews.arrowStreams(sql, scans)`
    (after `timeTravel`) replaces each schema-qualified reference in a FROM position (`services/TableReferences`
    on `TimeTravelClause`'s tokens: after FROM, JOIN or a comma of a FROM list; three-part names, table functions,
    literals, comments, column qualifiers left alone) by its own stream source; columns from the whole client SQL
    (all for `*`, `COLUMNS(...)`, or a whole-row use of the alias/name); predicates only when the SQL is one
    SELECT of that table alone (`singleSelect` + `ParquetQueryParser`); describe -> `declaredColumnsSource`;
    `DuckDbAdapter.streamCompleteOnce` (views applied, uncached prepare, parameters bound). Known limits: an
    unqualified name (`FROM orders`) or a reference after an ON condition's comma is not replaced and fails as a
    missing table (never an empty answer). Tests `ArrowStreamCompleteTest` 7/7, `TableReferencesTest` 4/4.
    *Step 2 (c) done 2026-10-28 (2e4d2d9):* `hive.orcTables` (default false; ORC tables were and stay skipped
    without it) -> ORC tables (`HiveTable.Kind.ORC`, serde/input format simple name containing `orc`) are shown
    with `ReadSpec.ArrowStream("orc")`; their schema IDs hash with a `hive-orc|` prefix (Parquet IDs unchanged);
    `pinnedColumns` keeps the read spec. `HiveCatalog.exportArrowScan` -> `OrcTableReader` (files one after the
    other, each opened only when the previous is done, through `Stores.locate` + `DataStore.open`, refusing files
    outside `hive.roots`; partition columns as UTF-8 text; context class loader set in `loadNextBatch` /
    `closeReadSource`; errors name the file). Hints -> `OrcSearchArguments` (integers -> LONG, DOUBLE, DECIMAL,
    STRING/VARCHAR, BOOLEAN, DATE via `java.sql.Date.valueOf`; CHAR, FLOAT, TIMESTAMP, nested, NULL literals and
    NaN left out). Found on the way: ORC evaluates a SARG leaf only on columns it reads, so a hint on a column
    outside the projection skipped nothing; `OrcArrowReader` now also reads the SARG's columns. Transactional ORC
    tables stay skipped with the log reason `transactional (ACID) ORC table: not read yet` (like ACID Parquet
    tables; a client sees no such table, never wrong rows). Tests: `HiveOrcTableTest` 6/6 (metastore; DuckDB reads
    the exported stream = the same 6000 rows computed in DuckDB SQL, all 12 types incl. decimal/date/timestamp/
    char/list/struct/map with NULLs, JVM zone America/Los_Angeles; partitions; partition-only scan; row-group
    skipping; broken / outside file), `OrcSearchArgumentsTest` 2/2.
    *Step 2 (d) done 2026-10-28 (86af8f9):* `HiveOrcIntegrationTest` (flight-server) runs the server with the
    packaged plugin (`plugins/hive`, `share=hadoop,arrow`) and `hive.orcTables=true`: ORC tables `hvorc.flat`
    (2 files) and `hvorc.items` (partitioned by region, a file per compression kind ZLIB/ZSTD/SNAPPY/LZ4/NONE, a
    NULL partition) next to Parquet tables of the same rows; whole-table fingerprints = DuckDB computing the rows;
    8 queries (star, count, group by partition, nested columns, filters on BIGINT/VARCHAR/DECIMAL/DATE, IS NULL,
    empty result) equal to Parquet in both modes (schema and rows); partition pruning (1 endpoint); joins ORC x
    Parquet, ORC x ORC, ORC self-join (complete). zstd-jni risk checked: the server's own `Zstd` (server class
    path) works before and after the plugin decodes ZSTD ORC files - no conflict. ORC files are written by
    `OrcTestFiles` (hive test fixture, shared with `HiveOrcTableTest`) in the metastore's class loader; orc-core,
    orc-format, orc-shims, aircompressor, threeten-extra, zstd-jni added to the hive module's `fixture-libs`.
    Pre-release `server-1.0.0-3fbf5b28b186` (run 40627574196, package=true).
    *Step 2 (e) measured 2026-10-28:* `HiveOrcPerfTest` (perf tag, flight-server; manual workflow
    `hive-orc-perf.yml`, inputs rows/files): lineitem-like table (16 columns, `OrcTestFiles.LINEITEM` SQL = the
    Java writer `writeLineitem`; DuckDB `tpch` not needed) as Parquet (DuckDB default snappy) and ORC zlib/zstd,
    4 files each, through the server with the packaged plugin; complete answers of ORC = Parquet (checked every
    run). SF1 (6M rows) on a 4-core runner, run 40631384599 (1f600b9), best of 3, seconds (complete / partial):
    sizes Parquet 136 MB, ORC zlib 69 MB, zstd 59 MB; count(*) 0.01 / 0.01 vs ORC 0.28-0.40 / 0.24-0.27; Q1-style
    0.15 / 0.17 vs 6.6-7.0; Q6-style 0.04 / 0.05 vs 6.6-6.9; `l_orderkey < 50000` (sorted key, SARG skips row
    groups) 0.01 vs 0.06-0.07; `SELECT *` 1.69 / 1.61 vs 7.7-8.1. So ORC works and pushdown helps, but full
    scans are ~45x slower than Parquet for aggregations (Parquet: DuckDB's parallel reader with its own
    pushdown/statistics; ORC: one single-threaded Java stream per table reference, per-value copy into Arrow).
    First fix ca47448: `USE_DECIMAL64` batches (decimals <= 18 digits decoded into longs) - Q1 at 1M rows locally
    2.0 -> 1.4 s, but at SF1 (run 40633767991) only ~5 %: Q1 6.26-6.58, Q6 6.20-6.47, `SELECT *`
    7.3-7.6 s, count(*) 0.37-0.51 - decimals are not the main cost.
    *Step 2 (f) done 2026-10-28:* profiled (JFR via `jcmd` on the surefire fork; the hive-module reader alone
    via an in-test `jdk.jfr.Recording`). Findings and fixes: (1) **the main cost** - `ArrowScanRequests` took any
    `*` for `SELECT *`, so Q1/Q6 (`l_extendedprice * l_discount`) streamed all 16 columns; a star between two
    values is now a product (core d5d5466, regression test `aProductReadsOnlyItsColumns`; stars that may select
    columns - `t.*`, `DISTINCT ON (a) *`, `* EXCLUDE`, next to a comment - still read all); (2) bulk copy of flat
    columns into Arrow (dfad426, `OrcArrowReader.copyFlat`; the per-value `setSafe` was ~60 % of the reader's
    time); (3) files of a scan decoded up to `min(4, cores)` at once, 2 batches ahead each, handed over in file
    order (cc1230c, `OrcTableReader.FileRead`, pool `hive-orc-read-*`; plugin only, no core `ArrowStreamScans`
    change needed). Also: the perf test's JVM ran with the jacoco agent and `-ea` (~40 % slower Java path, not
    Parquet's) - `hive-orc-perf.yml` now runs without them (9506620). SF1 on a 4-core runner, run 40641173349,
    best of 3, seconds (complete / partial), Parquet vs ORC zlib-zstd: count(*) 0.01 vs 0.08-0.27; Q1-style 0.12
    vs 0.72-0.84 (was 6.6); Q6-style 0.03-0.04 vs 0.36-0.45 (was 6.6); `orderkey < 50000` 0.01 vs 0.02; `SELECT *`
    1.19-1.25 vs 1.51-1.69 (was 7.7). Batch size (4) not tried.
    **Next step (step 2 (g), then close P1):** (a) a table of one large file gets no read-ahead (parallelism is
    per file) - split files into stripe ranges (`Reader.Options.range(offset, length)` per group of stripes from
    `Reader.getStripes()`) and treat each range as a read-ahead unit, keeping file/stripe order; test with one
    file of several stripes (write with a small `stripeSize`) and `HiveOrcPerfTest -Dhive.orc.perf.files=1`;
    (b) try `ORC_BATCH_ROWS` 8192/16384 on Q1 (where is the remaining 6x: JFR on Q1 alone - DuckDB's
    single-stream consumption vs ORC decode); (c) then close P1 (the pre-release of this session already carries
    (f)) and move to P2.
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
- 2026-10-28 (plugins session): P1 step 2 (b) part 2 (2) - complete mode over ArrowStream snapshots (core 53c4d71:
  `CatalogViews.arrowStreams`, `services/TableReferences`, `DuckDbAdapter.streamCompleteOnce`; FYI line in
  STATUS.md Log; CHANGELOG 9db19b0). Local: `ArrowStreamCompleteTest` 7/7, `TableReferencesTest` 4/4,
  `ArrowStreamExecutionTest` 5/5, `ArrowScanRequestsTest` 6/6, `DuckDbAdapterTest` 90 (+2 skip),
  `ExecutionServiceTest` 43, `QueryPlannerTest` 58, `FlightSqlProducerTest` 27, CatalogViews tests, Checkstyle +
  SpotBugs clean. CI: run 40616190366 (9db19b0, dispatched: the push's head commit was `[skip ci]`) green. No
  package release: no plugin serves ArrowStream yet, nothing client-visible changes. Next: step 2 (c), hive
  `exportArrowScan` + `hive.orcTables`.
- 2026-10-28 (plugins session): P1 step 2 (c) - hive `exportArrowScan` over ORC and `hive.orcTables` opt-in
  (2e4d2d9, plugin only; no SPI or core code change, a comment line in `arrowflight.properties`); fix: SARG columns
  are now read by `OrcArrowReader` (hints on unprojected columns were ignored by ORC). Local: hive module 29 tests
  (`HiveOrcTableTest` 6, `OrcSearchArgumentsTest` 2, `OrcArrowReaderTest` 5, `HiveCatalogTest` 11, Kerberos 5),
  Checkstyle + SpotBugs clean. CI: run 40621078865 (2e4d2d9) green, all modules. No package release yet: planned after the
  end-to-end tests of step 2 (d) (default behaviour unchanged).
- 2026-10-28 (plugins session): P1 step 2 (d) - ORC Hive tables end to end through the server (86af8f9, tests and
  test fixtures only: `HiveOrcIntegrationTest` 5/5, `OrcTestFiles`; hive `fixture-libs` gets orc-core & co.; no
  core, SPI or plugin main code change). Local: `HiveOrcIntegrationTest` 5/5, `HiveCatalogIntegrationTest` 7 (+1
  skip, UTF-8 locale), hive module 29/29 + Checkstyle + SpotBugs. CI: run 40627574196 (3fbf5b2, dispatched with
  package=true; the push's head was `[skip ci]`) green; pre-release `server-1.0.0-3fbf5b28b186` carries the
  `hive.orcTables` opt-in (default off). zstd-jni of server and plugin coexist. Next: P1 perf note (step 2 (e)).
- 2026-10-28 (plugins session): P1 step 2 (e) - perf note. `HiveOrcPerfTest` + workflow `hive-orc-perf.yml`
  (1f600b9, test only; CI run 40631441911 green; SF1 perf run 40631384599: ORC ~45x slower than Parquet on
  Q1/Q6-style aggregations, 4.7x on `SELECT *`, numbers in P1). First speed-up ca47448 (hive plugin:
  `USE_DECIMAL64` row batches, `serialize64` for fitting `HiveDecimalWritable`s; new test
  `readsDecimalsOfEveryWidthAndWidenedScales`; hive module 30/30 + Checkstyle + SpotBugs locally). CI: run
  40633748155 (ca47448) green, SF1 perf re-run 40633767991 green (~5 % gain at SF1, numbers in P1).
  No pre-release: `hive.orcTables` is opt-in and answers are unchanged; one is planned when P1 closes.

- 2026-10-28 (plugins session): P1 step 2 (f) - ORC scan speed. Profiled: Q1/Q6 streamed all 16 columns because
  `ArrowScanRequests` took a multiplication's `*` for `SELECT *` (core fix d5d5466, own commit, FYI line in
  STATUS.md Log); then bulk copy of flat columns into Arrow (dfad426) and per-file read-ahead in `OrcTableReader`
  (cc1230c, up to min(4, cores) files at once, file order kept); `hive-orc-perf.yml` without jacoco/`-ea`
  (9506620). Local: hive module 36/36 (new `OrcTableReaderTest` 4, `OrcArrowReaderTest` 8), `ArrowScanRequestsTest`
  7, ArrowStream* tests, `HiveOrcIntegrationTest` 5/5, `HiveCatalogIntegrationTest` 7 (+1 skip), Checkstyle +
  SpotBugs clean. CI: Dev CI run 40641171065 (9506620, package=true) green, pre-release `server-1.0.0-9506620bb170`
  (ORC opt-in default off; answers unchanged); push run 40640386236 (dfad426) cancelled by the newer push. SF1 perf
  runs 40640401475 (dfad426, still with jacoco: Q1 1.5, Q6 0.8, `SELECT *` 4.0 s) and 40641173349 (9506620:
  Q1 0.72-0.84 vs Parquet 0.12, Q6 0.36-0.45 vs 0.03, `SELECT *` 1.5-1.7 vs 1.2; before 6.6 / 6.6 / 7.7).
  Next: step 2 (g) stripe-range read-ahead for single-file tables, batch size, then close P1.
