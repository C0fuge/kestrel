# Lane 4 - catalogs and sources (new plugin modules on `main`)

See `CLAUDE.md`, "Third and fourth lanes". Basis: the maintainer's integration research 2026-10-28 (next wave).

## Next steps

C1. **Iceberg REST catalogs beyond Gravitino:** conformance of the iceberg plugin's REST path against Apache Polaris,
    Project Nessie and Lakekeeper (containers in CI), incl. vended credentials (S3 temporary credentials returned by the
    catalog, refreshed before expiry) and OAuth2 client credentials; Unity Catalog OSS if its Iceberg REST endpoint runs in
    CI. Answers equal to the same tables via HMS; partial and complete mode.
    - Step 1 done (2026-10-28): `RestCatalogConformanceTest` + workflow `catalogs-ci.yml` (matrix polaris,
      iceberg-rest-fixture; local `file://` warehouse bind-mounted at the same path, containers run as the runner user
      with `/etc/passwd` mounted). Polaris covers OAuth2 client credentials, `scope`, prefix from `/v1/config`.
    - Step 2a done (2026-10-28): S3 warehouses with static keys. `RestCatalogConformanceTest` takes
      `ICEBERG_REST_IT_ROOT=s3://...` + `ICEBERG_REST_IT_S3_*` (writer: Iceberg S3FileIO with the url-connection
      client - Iceberg 1.12's "apache" client is the SDK's Apache 5 client, absent from SDK 2.34.1; plugin: the s3
      plugin's store; DuckDB: httpfs + secret). Matrix entries polaris-s3 (`stsUnavailable`), iceberg-rest-fixture-s3,
      lakekeeper-s3 (`vakamo/lakekeeper` on Docker Hub, PostgreSQL, `tools/ci/lakekeeper-setup.sh`; its metrics
      port moved to 9100). S3 server: moto (`quay.io/minio/minio` refuses anonymous pulls; Docker Hub `minio/minio`
      is gone). All five pass locally (Docker in the session; quay.io and ghcr.io blobs are blocked there).
    - **Next (step 2b): vended credentials.** Findings so far: (1) DuckDB secrets are created per connection before
      lockdown (`DuckDbAdapter`, `DataStore.contributeDuckDb`), but `CREATE OR REPLACE TEMPORARY SECRET` and
      `DROP SECRET` still work after `enable_external_access=false` + `lock_configuration=true` (checked with
      duckdb_jdbc 1.5.6); client SQL cannot do it (only SELECTs pass the parse check), so the server could install
      a table's scoped temporary credentials on a connection before running a query on it. (2) Manifests are read
      through `Stores` (server credentials); with vended credentials only, the plugin needs its own S3 GET with
      SigV4 (copy the s3 plugin's small `S3SigV4`, no SDK) in `StoresFileIO` for locations under such a table.
      Needed core/SPI piece (lane-4 limit: additive, own commit, SPI bump, STATUS.md Log line): e.g.
      `TableSnapshot`/`ReadSpec` carrying optional scoped storage credentials (prefix, type, options, expiry) that
      the server validates like `createSecret` (scope below the catalog's roots) and installs with
      `CREATE OR REPLACE TEMPORARY SECRET` on the query's connection(s), never logged. Plugin: optional
      `iceberg-rest.vendedCredentials=false` (default off) sends `X-Iceberg-Access-Delegation: vended-credentials`,
      reads `config` / `storage-credentials`, refreshes before `s3.session-token-expires-at-ms`. CI: moto STS
      (AssumeRole) with Lakekeeper `sts-enabled` / Polaris without `stsUnavailable`; check what each returns.
      See Open questions before building the core part.
    - Step 3a done (2026-10-28): answers through the server, local warehouses. `RestCatalogServerConformanceTest`
      (flight-server, `rest-catalogs`, env-gated like the plugin test) runs the server with iceberg-rest (schemas
      `rest_*`) next to iceberg-hadoop over a reference warehouse with the same fixtures; complete answers equal the
      reference (count + md5 per table), partial equal complete, located partial endpoints. `IcebergFixtures.writeRest`
      (shared by both tests). `catalogs-ci.yml` runs it for polaris and iceberg-rest-fixture (passes locally on both).
    - Step 3b done (2026-10-28): the server test on the S3 entries too (polaris-s3, iceberg-rest-fixture-s3,
      lakekeeper-s3): writer Iceberg S3FileIO (the iceberg module's `fixture-libs` now include
      `software.amazon.awssdk`, `software.amazon.eventstream`, `org.reactivestreams`, `dev.failsafe`), the s3 plugin
      installed next to iceberg with `arrowflight.s3.endpoint/region/urlStyle=path` + `FLIGHT_S3_*` keys; the catalog
      root's store is opened lazily by `CatalogStores.locate`.
    - Step 3c done (2026-10-28): HMS as a further reference on local warehouses: the REST metadata files registered
      in the in-process metastore (`HmsFixture.registerIceberg`, schema `hms_ice`, hive plugin -> iceberg plugin);
      answers equal the REST catalog's (schema + rows), partial equal complete. Skipped on S3 entries (the in-process
      HMS has no S3 file system); the workflow expects 4 tests with exactly that one skip there.
    - Step 4 done (2026-10-28): Unity Catalog OSS 0.7.0 (`unitycatalog/unitycatalog:v0.7.0`, Docker Hub) serves a
      writable Iceberg REST endpoint (`/api/2.1/unity-catalog/iceberg`, warehouse `unity`, prefix `catalogs/unity`):
      create namespace/table and commits work once `storage-root.tables` is set (managed tables under
      `<root>/__unitystorage/tables/<uuid>`). Entry `unity-catalog` in `catalogs-ci.yml` (local warehouse; UC runs
      as the runner user in group 101 on writable copies of `etc/conf`, `etc/db`, `etc/logs`). Findings: UC refuses
      Iceberg `time` columns (`ICEBERG_REST_IT_OMIT_COLUMNS=tm` leaves `types.tm` out of both sides); its sample
      table `default.marksheet_uniform` answers 500 on loadTable, which makes the whole catalog unavailable in the
      server (see Q3), so the server test uses `ICEBERG_REST_IT_NAMESPACES=ice`. Plugin test 3/3, server test 4/4
      (HMS reference included) pass locally. No vended credentials checked (UC's `/credentials` endpoint is listed
      in `/v1/config`; belongs to step 2b).
    - C1 is complete apart from step 2b (vended credentials), which waits on Q1. Next session: C2.
C2. **JDBC federation plugin:** PostgreSQL and MySQL tables as a data source through DuckDB's `postgres` / `mysql`
    extensions (bundle for 1.5.6 offline like the others; check availability), read-only, pushdown of filters/projection,
    credentials from the plugin config (never logged), Ranger checks as for other tables. CI with postgres/mysql
    containers, type matrix, TPC-H SF0.1 answers.
    - Note (2026-10-28): ci-assets `duckdb-extensions-v1.5.6.tar.zst` holds only avro, hadoopfs, httpfs, iceberg -
      `postgres_scanner` / `mysql_scanner` are missing; extensions.duckdb.org answers 403 from the session (CI can
      download them, as catalogs-ci.yml's fallback does). First step: a CI job that downloads both for v1.5.6, checks
      they load in duckdb_jdbc 1.5.6 with `enable_external_access=false` after `ATTACH` (or what order works under
      the lockdown), and records the result.
    - Step 1 done (2026-10-28): `catalogs-ci.yml` job `jdbc-scanners` + `tools/ci/DuckDbScannerProbe.java`
      (PostgreSQL 16.4, MySQL 8.4 containers; extensions from extensions.duckdb.org, kept 14 days as workflow
      artifact `duckdb-scanner-extensions-v1.5.6`; the job looks for
      `<name>-v1.5.6-linux_amd64.duckdb_extension` in ci-assets first). Results with the server's order
      (in-memory secrets, `LOAD '<path>'`, `CREATE TEMPORARY SECRET`, `ATTACH '' AS src (TYPE ..., SECRET s,
      READ_ONLY)`, then allowed_directories + no autoload + `enable_external_access=false` +
      `lock_configuration=true`):
      - Works after the lockdown, both engines: reads, projection and filter pushdown (`Filters: id=2` in the
        POSTGRES_SCAN / MYSQL_SCAN plan), information_schema of the attached database, NULLs. PostgreSQL runs
        reads in `READ ONLY` transactions (`transaction_read_only=on`).
      - Types: pg json/jsonb/char -> VARCHAR, bytea -> BLOB, int[] -> INTEGER[], numeric(18,4) -> DECIMAL(18,4),
        timestamptz -> TIMESTAMP WITH TIME ZONE, uuid/interval/time native. MySQL: time -> VARCHAR
        (`mysql_time_as_time=false`), year -> INTEGER, bit(1)/tinyint(1) -> BOOLEAN, bigint unsigned -> UBIGINT,
        enum/json -> VARCHAR.
      - Refused after the lockdown: a new `ATTACH` (also with the existing secret), `INSERT` into the read-only
        attach, `postgres_execute` / `mysql_execute`, non-SELECT and multi-statement text in `*_query`.
      - **Gets through (must be refused in client SQL before a plugin ships):** `postgres_query('src', '<any
        SELECT>')` / `mysql_query(...)` run text on the remote database with the plugin's credentials (bypasses
        per-table checks; side-effect functions like `set_config` ran); `postgres_scan('<any dsn>', ...)`
        connects to any host with any credentials despite `enable_external_access=false` (network egress from
        the server); `duckdb_secrets()` shows host/user (already in the server's `REFUSED_FUNCTIONS`);
        `CREATE OR REPLACE SECRET` and `DETACH` pass DuckDB but not the server's SELECT-only parse check.
      - Functions the extensions add: postgres_scanner `pg_clear_cache, postgres_attach, postgres_configure_pool,
        postgres_execute, postgres_hstore_get, postgres_hstore_to_json, postgres_query, postgres_scan,
        postgres_scan_pushdown, read_postgres_binary`; mysql_scanner `mysql_clear_cache,
        mysql_debug_execution_plan, mysql_execute, mysql_explain_federated, mysql_query`.
    - **Next (step 2):** see Q4 first. If allowed: SPI (own commit, additive, version bump) lets a plugin name
      functions to refuse in client SQL (or: the server refuses every function a plugin's `LOAD` adds, from a
      `duckdb_functions()` diff), the server adds them to the parse check; then module `flight-plugin-jdbc`
      (config: per source type/host/port/database/user, password from env/file, never logged; `LOAD` + secret +
      `ATTACH ... READ_ONLY` via `contributeDuckDb`, tables as catalog views `jdbc_<source>.<schema>.<table>`),
      tests with Testcontainers-free CI containers as in `jdbc-scanners`. Also upload the two extensions to
      ci-assets (from the artifact) so CI does not depend on extensions.duckdb.org.
C3. **Delta Lake tables** via DuckDB's `delta` extension (check 1.5.6 build), from HDFS and S3; refuse clearly what it
    cannot read (deletion vectors / column mapping if unsupported). Then: Apache Ozone S3 gateway as an S3 backend test.
    - Step 1 done (2026-10-28): `catalogs-ci.yml` job `delta`: Delta tables written by Spark 3.5.3 + delta-spark
      3.3.2 (`tools/ci/delta-fixtures.py`, Spark's answers in `expected.tsv`), read by `tools/ci/DuckDbDeltaProbe.java`
      with `delta` v1.5.6 (extension `6059958`, from extensions.duckdb.org, kept 14 days as workflow artifact
      `duckdb-delta-extension-v1.5.6`; ci-assets checked first) after the server's lockdown, locally and on S3 (moto,
      scoped `TYPE s3` secret). Results:
      - **Read correctly after the lockdown, local and S3** (all equal to Spark): plain, partitioned (string/int,
        NULL partition), deletion vectors (DELETE + UPDATE), column mapping `name` (renamed + dropped column) and
        `id`, checkpoints (interval 2, delete after the last), schema evolution (added column NULL in old files),
        time travel `delta_scan(path, version := n)`. `ATTACH '<path>' AS x (TYPE delta)` works before and after
        the lockdown (also `PIN_SNAPSHOT`, `VERSION n`). Filter + projection pushdown (`DELTA_SCAN Filters: p='x',
        id>1`). Types: timestamp -> TIMESTAMP WITH TIME ZONE, timestamp_ntz -> TIMESTAMP, decimal(38,10), binary ->
        BLOB, array/struct/map native. No replacement scan (`FROM '<dir>'` is not a Delta read). HDFS not probed:
        delta-kernel has its own object store (local, s3, az, gs/r2 via secrets, http(s)), so `hdfs://` tables
        would need the hadoopfs extension, which delta-kernel does not use - expect "not readable" (step 2 checks).
      - **Gets through the lockdown (must never reach client SQL):** delta-kernel does its own I/O, which
        `allowed_directories` / `enable_external_access` only stop at DuckDB's Parquet reader: a table's
        `_delta_log` outside the allowed directories is read (`DESCRIBE delta_scan(...)` shows its schema,
        `delta_list_files` its file names, sizes, partition values, DVs), local and on S3 with any matching
        secret; `delta_scan('https://<host>/...')` connects to any host (10 retries), `s3://` outside every
        secret's scope goes to the instance metadata service (169.254.169.254). The extension also adds
        `write_blob(path, blob)` and `copy_dir(src, dst)`, which **write files anywhere the server user can**
        (checked: both wrote outside the allowed directories after `lock_configuration=true`). Full list of
        added functions: `copy_dir, delta_domain_metadata, delta_filter_pushdown_log,
        delta_filter_pushdown_log_tpcds, delta_get_transaction_version, delta_list_files, delta_scan,
        delta_set_transaction_version, get_delta_test_expression, parse_delta_filter_logline, write_blob`.
      - No server setup loads `delta` today (plugins request extensions by name through `DuckDbSetup`; none asks
        for it), so nothing is exposed now.
    - **Next (step 2), after Q5:** module `flight-plugin-delta` (catalog of Delta tables under configured roots:
      a directory with `_delta_log` is a table; local + S3 through the stores' roots/secrets), tables as
      server-written `delta_scan('<root>/<table>')` sources (or `ATTACH ... (TYPE delta)` before the lockdown),
      `DuckDbSetup.refuseFunctions` with every function above (the SPI already has it: no SPI/core change), and
      a unit test that the refused list equals the `duckdb_functions()` diff of the bundled extension, so a new
      extension version cannot add an unrefused function. Partial mode: file list from `delta_list_files`
      (DVs need the scan, so tables with DVs complete-mode only or refused in partial, to decide). Probe HDFS.
    - Ozone done (2026-10-28): Apache Ozone 2.0.0's S3 gateway as an S3 backend: `tools/ci/ozone-s3.sh` (SCM, OM,
      one datanode, S3 gateway as containers on a private network, gateway on localhost:9000; non-secure, so any
      key, as moto; replication ONE; ready in ~50 s locally) and `catalogs-ci.yml` entry
      `iceberg-rest-fixture-ozone-s3` (the fixture's S3FileIO, the test's writer, the s3 plugin and DuckDB httpfs
      all on Ozone). Plugin test 3/3, server test 4/4 (the HMS reference skipped as on every S3 entry) pass
      locally, unchanged code. Ozone's gateway answers unsigned requests with 403 (readiness check takes any HTTP
      answer).
    - Delta on HDFS probed (2026-10-28): `tools/ci/hdfs-single.sh` (Docker Hub `apache/hadoop:3.4.1`, NameNode +
      DataNode in one container on the host network, ready in ~20 s) holds the fixtures; `DuckDbDeltaProbe` with
      `PROBE_HDFS_ROOT` loads hadoopfs (`51b7220-afc3`, ci-assets) next to delta and httpfs. The probe now runs in
      `eclipse-temurin:17-jdk-focal` (+ libxml2): the hadoopfs build links `libssl.so.1.1`, absent on
      ubuntu-latest. Results: hadoopfs reads the tables' Parquet files and lists `_delta_log` (before and after
      the lockdown), but every delta function fails on `hdfs://` with `DeltaKernel ObjectStoreError (8): ...
      Unable to recognise URL "hdfs://localhost:8020/delta/t/"` (delta_scan, delta_list_files, time travel);
      `ATTACH ... (TYPE delta)` succeeds (lazy) and fails on the first read. Local and S3 results unchanged.
      Consequence for the plugin: refuse `hdfs://` (and any scheme but local / s3) Delta roots at configuration
      time with a clear message; reading Delta on HDFS would need a log reader of our own (JSON + checkpoint
      Parquet, the files then read by hadoopfs' `read_parquet`) - not planned, see Next.
    - **Next for C3:** step 2 (the plugin) after Q5. Nothing else in C3 is unblocked.
C4. Hive ACID insert-only tables (after the plugins lane's P4), then Paimon/Hudi read-only if DuckDB or a light Java
    reader allows.
    - Step 1 done (2026-10-28): `InsertOnlyDirectories` in `flight-plugin-hive` (new class, not wired yet): for one
      table/partition directory listing and a snapshot (`WriteIds`: high watermark, min open write id, open +
      aborted ids; `Txns` for `_v<txn>` visibility suffixes) it chooses what to read as Hive's
      AcidUtils.getAcidState does for MM tables: the newest usable base (`base_N` with nothing open at or below N;
      an insert-overwrite base only when write id N committed; a compacted one when its visibility txn is
      committed or its `_metadata_acid` marks it compacted), committed deltas above it (all statement ids of a
      write; a compacted `delta_x_y_vT` covers its sources), original files only without a base, hidden `_*` /
      `.*` entries never. Refused with a clear message (never read in part): `delete_delta_*` (full ACID), a
      compacted base newer than the snapshot ("not enough history"), a multi-id delta without `_v` holding an
      aborted id, a delta reaching past the snapshot, partially overlapping deltas. `InsertOnlyDirectoriesTest`
      (9 unit tests).
    - Step 2 done (2026-10-28): wired behind the new optional setting `hive.insertOnlyTables` (default false).
      `Metastore.snapshot` (`ThriftMetastore`: `get_open_txns`, then `get_valid_write_ids` with the
      `ValidReadTxnList` string `hwm:minOpen:open:aborted`); `HiveTable.classify` shows
      `transactional_properties=insert_only` tables when on (any format the plugin reads; full ACID stays skipped);
      `HiveCatalog.plan` takes one snapshot per plan and lists each table/partition directory recursively, keeping
      the files of the entries `InsertOnlyDirectories.select` chooses (directories from `DataStore.directories`,
      so an empty `base_N` of an INSERT OVERWRITE counts; `_metadata_acid` read only for a base that needs it);
      a refused layout fails the plan with an IOException naming the directory. `HmsFixture` now creates Hive's
      txn tables (`TxnDbUtil.prepDb`) and opens/allocates/commits/aborts through the HMS API.
      `HiveInsertOnlyTableTest` (4 tests: originals + committed/aborted/open deltas, a later commit becoming
      visible, INSERT OVERWRITE base of an open then committed write, partitions, the delete_delta refusal, the
      setting off).
    - Step 3 (a) + (b, ORC) done (2026-10-29): `HiveInsertOnlyIntegrationTest` (flight-server, packaged hive
      plugin, in-process HMS with txn tables): insert-only Parquet (originals, committed / aborted / open deltas,
      `_tmp`), partitioned and ORC tables answer the committed rows only in complete and partial mode (ORC equal to
      a plain ORC table holding exactly those rows); a commit is visible to the next partial query (complete
      answers read views refreshed at most once a minute: one consistent snapshot or the other); INSERT OVERWRITE
      base replaces older writes; `delete_delta` refused in both modes. **Bug found and fixed:** the refusal was an
      IOException, which `CatalogViews` treats as a metastore outage, so every table listed after the refused one
      (all schemas of the metastore) vanished from the views. Now `HiveCatalog.UnreadableTableException`
      (IllegalArgumentException): only that table is skipped; complete -> NOT_FOUND, partial -> INVALID_ARGUMENT
      with the reason. Small core change (own commit 63679fe): `catalogCompleteReason` catches RuntimeException.
    - Step 3 (b, text) done (2026-10-29): `HiveInsertOnlyIntegrationTest` also covers insert-only text tables
      (Hive's default serde; originals, committed plain and gzip deltas, aborted and open writes, a partitioned
      table) through the server: committed rows only, both modes, equal to DuckDB's rows; pruning; a join with the
      Parquet insert-only table. Unchanged plugin code.
    - Step 3 (c) done (2026-10-29): real Hive writer. `tools/ci/hive-mm-writer.sh` runs HiveServer2 4.0.1 (Docker
      Hub `apache/hive:4.0.1`, ~3 min) with an embedded Derby metastore on a mounted directory and DbTxnManager,
      writes insert-only Parquet / text / ORC (one partitioned) tables (inserts, failed inserts via
      `assert_true(rand() < 0)` - a check over a table scan passed on the runner because its map vertex did not
      run, INSERT OVERWRITE of a table and of one partition, gzip text output, `COMPACT 'major' AND
      WAIT`, `COMPACT 'minor' AND WAIT`), records Hive's `SELECT *` answers, stops HiveServer2 and starts a
      standalone metastore (`SERVICE_NAME=metastore`, `IS_RESUME=true`) on the same Derby on localhost:9083.
      `HiveRealWriterIntegrationTest` (env-gated `HIVE_MM_IT_*`) reads them through the server: equal to Hive's
      answers in both modes (2/2 locally; Catalogs CI job `hive-writer`). What Hive 4 really writes: a failed
      insert leaves no directory (write id aborted); major compaction -> `base_N_vT` **without** `_metadata_acid`
      (the plugin's `_v` + committed visibility txn rule applies); minor -> `delta_1_3_vT` next to its sources;
      INSERT OVERWRITE -> plain `base_N`; hidden `.crc` files beside every data file; gzip text as `000000_0.gz`.
      **Hive 4 quirk:** below a directory whose name starts with `_` (the runner's `/home/runner/work/_temp`)
      Hive answers no rows for insert-only tables and compactions write nothing (reproduced locally), so the job
      keeps the warehouse in `/tmp/hive-mm` and the script fails on an empty Hive answer. The plugin does not
      mirror that quirk (not checked what it answers there; Hive's behaviour looks like a hidden-path filter
      applied to the whole location, not a rule worth copying).
      Setup notes: `ALTER TABLE ... COMPACT` in HiveServer2 needs direct access to the metastore database
      (`AlterTableCompactOperation` inits a `MetaStoreCompactorThread`), so a HiveServer2 talking to a separate
      thrift metastore on its own Derby fails with "Version information not found in metastore"; a shared
      PostgreSQL would also do (Docker Hub answered 429 for `postgres` in the session).
    - Step 4 (b) probe done (2026-10-29): Catalogs CI job `paimon-hudi`. `tools/ci/paimon-hudi-fixtures.py` writes
      Paimon 1.2.0 (filesystem catalog: append, partitioned append + INSERT OVERWRITE, primary-key with 2 buckets and
      upserts / DELETE / UPDATE uncompacted, partitioned PK after `sys.compact`, PK and append tables with deletion
      vectors, added column) and Hudi 0.15.0 (table version 6) / 1.0.2 (version 8) tables (COW partitioned with
      upsert / delete / update, COW INSERT OVERWRITE = replacecommit, COW with the last commit left inflight, MOR
      uncompacted, MOR with inline compaction) with Spark 3.5.3 (bundle jars from Maven Central: Ivy cannot pass the
      session proxy). `tools/ci/paimon-hudi-probe.py` answers them with a light reader (formats' metadata in
      Python, data by DuckDB `read_parquet`) and with DuckDB's own extensions; wrong light-reader answers fail the
      job. Results (runs 40810022672, 40810359864):
      - **DuckDB has `paimon` and `hudi` extensions for v1.5.6** (download origin - core or community repository -
        is in the job summary of run 40810359864). `paimon` adds `paimon_scan, paimon_snapshots`; `hudi` adds
        `hudi_files, hudi_info, read_hudi`. Both answer **every** fixture equal to Spark, incl. uncompacted Hudi MOR
        (log merge) and Paimon deletion vectors. Not yet checked under the server's lockdown (own I/O outside
        `allowed_directories`, http(s) / metadata service, added write functions - as found for `delta`).
      - Light reader equal to Spark on all Paimon tables and on Hudi COW / compacted MOR of both versions; refuses
        Hudi MOR with log files in the latest slice. Paimon: latest `snapshot-N` (LATEST is only a hint); base +
        delta manifest lists, live = ADD minus DELETE; **manifests are zstd-compressed Avro, which DuckDB's avro
        extension cannot read** ("unknown codec") - a Java reader needs Avro + zstd-jni (both already in the hive
        plugin / server). PK tables (deduplicate engine): files carry `_KEY_<pk>` (partition keys left out),
        `_SEQUENCE_NUMBER` (per bucket), `_VALUE_KIND`; answer = newest row per (partition cols, key), kinds 0/2.
        Deletion vectors: index manifest in `manifest/`, files in `index/` (version byte 1; per range BE length,
        magic 1581511376, portable 32-bit RoaringBitmap, CRC), applied via `file_row_number`; a DELETE on a DV PK
        table writes only a DV (no -D row). DV-mode PK reads skip level-0 files (none occurred). Added columns:
        `union_by_name`; renames refused. Hudi: 0.x timeline `.hoodie/<ts>.<action>`, 1.x `.hoodie/timeline/
        <requested>_<completed>.<action>` with Avro instead of JSON metadata; latest committed base file per file
        group, minus `partitionToReplaceFileIds` of completed replacecommits; inflight base files skipped; pending
        compaction / clustering refused; log files named after the base instant (0.x) or the delta commit (1.x).
    - Step 4 (b) lockdown probe done (2026-10-29): `tools/ci/DuckDbPaimonHudiProbe.java` in the `paimon-hudi` job
      (duckdb_jdbc 1.5.6 in the Ubuntu 20.04 JDK container as for delta; `paimon` e49d491, `hudi` ea166bd, kept 14
      days as workflow artifact `duckdb-paimon-hudi-extensions-v1.5.6`, ci-assets checked first; moto S3, HDFS via
      `tools/ci/hdfs-single.sh`). Run 40821054886. Results:
      - Both load into duckdb_jdbc 1.5.6 and answer **every local fixture equal to Spark after the lockdown**
        (Paimon PK / DV / evolved, Hudi COW / MOR / inflight, both Hudi versions). Added: `paimon_scan` (overloads
        `(table_path, ...)` and `(warehouse, database, table, ...)`, named `snapshot_from_id` /
        `snapshot_from_timestamp`, `file_format`, `manifest_format`), `paimon_snapshots`, `read_hudi(path,
        hudi_option, start, end, as_of)`, scalar `hudi_info` / `hudi_files` (they return an error as a string
        value instead of failing), secret type `paimon`, storage type `ATTACH ... (TYPE paimon)` (a warehouse;
        works before and after the lockdown, tables as `<att>.<db>.<table>`). Hudi has no storage type. **No write
        functions** (unlike `delta`). `paimon_scan` needs the `<warehouse>/<db>.db/<table>` layout.
      - **S3:** paimon ignores the `s3` secret and reads with its own `TYPE paimon` secret (KEY_ID, SECRET, ENDPOINT
        `http://host:port`, REGION, SCOPE): all Paimon fixtures equal to Spark on S3; outside that secret's scope it
        fails "no credentials" (no metadata-service fallback seen). hudi ignores DuckDB secrets and goes straight to
        the instance metadata service (`PUT http://169.254.169.254/latest/api/token`), so no Hudi table on S3 can
        be read; `hudi_option` takes `k=v` entries (JSON refused) but `aws_*` object-store keys did not change that
        (option names unknown).
      - **HDFS:** neither reads `hdfs://` (paimon "invalid scheme hdfs for local file system", hudi "Unable to
        recognise URL"); hadoopfs itself works next to them.
      - **Gets past the lockdown:** both do their own local file I/O: `paimon_scan`, `paimon_snapshots`, a
        metadata-only table (schema), `file://` URIs and `ATTACH ... (TYPE paimon)` of a warehouse outside
        `allowed_directories` read it after `lock_configuration=true`; same for `read_hudi`, `hudi_info`, `hudi_files`
        (file names, sizes, commit times). hudi contacts metadata services for `s3://` (AWS IMDS) and `gs://` (GCE
        metadata) paths. Refused: http(s) URLs (paimon: path format; hudi: "feature for Http not enabled"),
        `az://` without an account, `/proc/self`; no replacement scan (`FROM '<dir>'` is not a read).
      - Consequence: a plugin on the extensions must refuse all five functions in client SQL
        (`DuckDbSetup.refuseFunctions`) and write the table sources itself, as planned for delta. Paimon via the
        extension covers local + S3 (server installs a scoped `TYPE paimon` secret); Hudi via the extension local
        only. The light reader (data through DuckDB's own `read_parquet`, so inside the lockdown, S3 secrets and
        hadoopfs work) covers HDFS and S3 for Paimon and Hudi COW / compacted MOR. Q7 updated.
    - Step 4 (b) Hudi on S3 done (2026-10-29): the lockdown probe's `hudi_option` checks used `,` and JSON, but
      duckdb-hudi 0.3.1 (alitrack/duckdb-hudi, hudi-rs 0.5.0 kernel) splits `hudi_option` on `;` (`rust/src/
      kernel.rs`); keys other than `hoodie.*` go to object_store as storage options. With `aws_endpoint_url=http://
      host:port; aws_access_key_id=..; aws_secret_access_key=..; aws_region=..; aws_allow_http=true` **every Hudi
      fixture on S3 answers equal to Spark after the lockdown** (both versions, COW, INSERT OVERWRITE, inflight,
      uncompacted and compacted MOR, partition filter) - run 40823753558. The README's `hoodie.s3.access_key_id` /
      `hoodie.aws.region` names are not honoured (back to the metadata service). Security: credentials travel in the
      SQL text (a plugin would write them into server-generated query text, so query logging / EXPLAIN / error
      messages must never show it), and the same options read `s3://lake/outside/...` outside `allowed_directories`
      (GETS THROUGH) and contact any endpoint client SQL names - so `read_hudi` must be refused in client SQL as
      already planned; DuckDB secrets are still ignored. Consequence for Q7: Hudi via the extension covers local +
      S3 too (MOR with logs included), only HDFS needs the light reader.
    - **Next for C4 (b):** waits on Q7 (route). Also store both extensions in ci-assets (from the artifact; the
      session cannot download them). (d) full ACID ORC still waits on Q6. With C1 2b, C2, C3 and C4 all waiting on
      answers (Q1, Q4-Q7), a next session without answers has no unblocked item; the Hudi-on-S3 question is answered
      (above). Possible test-only extras: `hudi_info` / `hudi_files` with options (their signatures take none),
      or whether credentials in `hudi_option` show up in DuckDB error text (they did not in run 40823753558's
      refusals - masked check only). Otherwise stop early.
## Open questions

- Q1 (2026-10-28, C1 step 2b): vended credentials need the server to install per-table temporary S3 secrets on
  DuckDB connections after lockdown (`CREATE OR REPLACE TEMPORARY SECRET`, scope checked below the catalog's
  roots, values redacted), fed by a new optional field on the catalog SPI. It touches the DuckDB lockdown
  (security), so: may lane 4 add this (opt-in, off by default, own commits), or should the core lane own the server
  part? Until answered, lane 4 continues with step 3 and C2 and leaves 2b's core part unbuilt.

- Q4 (2026-10-28, C2 step 2, security): the postgres / mysql scanner extensions add table functions that
  get through the DuckDB lockdown: `postgres_query` / `mysql_query` (any SELECT on the remote database with the
  plugin's credentials) and `postgres_scan('<dsn>', ...)` (connects to any host). A federation plugin needs them
  refused in client SQL. Proposal: the server refuses every function a plugin's `LOAD` adds (diff of
  `duckdb_functions()` before/after the plugin's DuckDB setup, joined to `REFUSED_FUNCTIONS`), opt-in only by
  installing such a plugin; no change for existing setups. May lane 4 add this to the parse check (own commit,
  STATUS.md Log line), or should the core lane own it? Until answered, lane 4 builds no plugin that loads these
  extensions into query connections.
  Addendum (2026-10-28): the SPI already has `DuckDbSetup.refuseFunctions(String...)` ("the server's own list
  can only grow"), so a plugin can refuse the functions its extension adds without any core change; the
  automatic `duckdb_functions()` diff would only be a safety net against a future extension version adding
  one. Option (a): the jdbc plugin refuses the scanners' functions itself (plugin-only, plus a test that its
  list equals the diff of the bundled build). Option (b): the core diff as proposed. Lane 4 would go with (a)
  unless told otherwise, but still waits for a yes because it loads an extension with known lockdown gaps.

- Q5 (2026-10-28, C3 step 2, security): DuckDB's `delta` extension v1.5.6 adds functions that get past the
  lockdown (C3 step 1 results): `write_blob` / `copy_dir` write files anywhere, and `delta_scan` /
  `delta_list_files` read Delta logs outside `allowed_directories` and connect to any http(s) host or the
  instance metadata service. A Delta plugin would load it into every query connection and refuse all eleven
  added functions in client SQL through the existing `DuckDbSetup.refuseFunctions` (server-written table
  sources only; plugin-only, no SPI/core change), with a test that the list covers the bundled build. May lane
  4 build `flight-plugin-delta` on that basis (opt-in: only active when the plugin is installed), or should the
  parse check refuse extension-added functions generically first (Q4 option b)? Until answered, lane 4 does
  not load `delta` into server connections.

- Q6 (2026-10-29, C4 (d)): after insert-only tables (done, checked against a real Hive 4), which next: (a) full
  ACID ORC tables (`delete_delta` handling by ROW__ID - (originalTransaction, bucket, rowId) - anti-join in the
  plugin or in DuckDB; a sizeable new read path in the plugins lane's hive module), or (b) Paimon / Hudi read-only
  (first a probe of what DuckDB 1.5.6 extensions or a light Java reader can do, as for Delta in C3 step 1)?
  Without an answer lane 4 starts with (b)'s probe (test/CI only), since (a) changes the hive module's scope.

- Q7 (2026-10-29, C4 (b)): Paimon / Hudi read-only - which route? (a) a plugin on DuckDB's `paimon` / `hudi`
  extensions (correct on every probed table incl. Hudi MOR; security as for `delta`, Q5: refuse their functions in
  client SQL, pending the lockdown probe), or (b) a light reader in a plugin (no extension: Paimon append / PK /
  DV tables and Hudi COW; Hudi MOR with logs refused; PK merge-on-read needs a dedup query over the files, which
  the SPI's file + delete-file model cannot express - would need either a `CatalogView`-like SQL over files (SPI
  change) or PK tables limited to DV mode). Lane 4 would prefer (a) if the lockdown probe is clean, else (b) for
  COW / append / DV tables. No plugin is built until answered.
  Update (2026-10-29, lockdown probe done, C4 notes): neither extension writes files, but both read local files
  outside `allowed_directories` (all five functions, plus `ATTACH ... (TYPE paimon)`), and hudi calls the cloud
  metadata services for `s3://` / `gs://`. Paimon via the extension reads local + S3 (own `paimon` secret type),
  Hudi via the extension local only; neither reads HDFS. Revised proposal: (a) for Paimon (plugin refuses the
  extension's functions in client SQL, writes `paimon_scan` sources itself, installs a scoped `paimon` secret for
  S3 roots), (b) light reader for Hudi COW / compacted MOR (works on S3 and HDFS; MOR with logs refused). Same
  security footing as Q5 (an extension with its own I/O in every query connection, closed only by the refused
  list).
  Update (2026-10-29): Hudi on S3 works through the extension with `hudi_option` storage options (`;`-separated;
  the earlier probe's separator was wrong), all fixtures equal to Spark. So route (a) covers Paimon and Hudi on
  local + S3 (incl. Hudi MOR with logs); only HDFS needs (b). Hudi's credentials go in the generated SQL text
  (keep it out of logs), not in a DuckDB secret.

- Q2 (2026-10-28, for lane 3 / maintainer, not lane 4's code): `DistributedClusterIntegrationTest` is flaky on
  `main` since D2 (c) part 2 (c7e436e): `peerStreamCutBeforeTheFirstBatch...:187` and
  `peerStreamCutDuringTheAnswer...:212` fail with `mergedAnswers()` 1 instead of 0 in 2 of 3 local runs and in Dev CI
  runs 40692394131 / 40693932375. Likely cause: a relay cut before the schema now fails over to the endpoint's backup location, which
  bypasses the cutting relay, so the answer is merged (correct rows) instead of falling back. Lane 4 leaves it to
  lane 3.

- Q3 (2026-10-28, for the core lane, not lane 4's code): one table a catalog cannot load makes all of that
  catalog's schemas unavailable. `CatalogViews` refresh: `getTables(schema)` fails on Unity Catalog's sample table
  `default.marksheet_uniform` (HTTP 500 from loadTable), the catalog entry goes into `failed`, so `rest_ice.*`
  (healthy tables in another namespace) answer `UNAVAILABLE ... marksheet_uniform` too. An outage of the whole
  catalog and one broken table are not told apart. Proposal: keep the catalog-wide memo for connection-level errors
  only and skip (log, list as unavailable) a single table whose load fails, so its siblings stay queryable. This
  changes existing behaviour, so it is the core lane's / the maintainer's call. Reproduce: `catalogs-ci.yml` entry
  `unity-catalog` without `ICEBERG_REST_IT_NAMESPACES`.
  Addendum (2026-10-29): the same pattern hit the hive plugin's refused insert-only layouts (fixed plugin-side in
  C4 step 3 by throwing a runtime exception, which `catalogTableViews` treats as a table-level skip). Other hive
  plugin table-level refusals are still IOExceptions and so still hide their siblings: a text / Avro partition
  with its own serde or format (`checkTextPartition`, `checkAvroPartition`). Lane 4 could switch those to the
  same `UnreadableTableException` (plugin-only), but that changes existing behaviour of the plugins lane's
  module -> maintainer / plugins lane to decide. Also: a skipped table answers complete queries with DuckDB's
  "Table with name X does not exist!" (NOT_FOUND); naming the reason would need CatalogViews to keep per-table
  reasons (core lane).

## Log

- 2026-10-28 (maintainer): lane created; C1-C4.
- 2026-10-28: C1 step 1 - Iceberg REST conformance test against Apache Polaris 1.2.0 (OAuth2) and the Iceberg REST
  fixture 1.10.0 in the new workflow `catalogs-ci.yml`; all checks equal to the Hadoop-layout reference (names,
  columns, files, row/delete counts, partial-read problems, DuckDB rows). Test only. Catalogs CI green: run
  40670485477 (earlier failures 40669539591, 40669821302, 40670292342 were CI setup: container uid/permissions,
  DuckDB setting). Dev CI green: run 40672085389 (head c258adf, includes these commits). No open questions.
- 2026-10-28: C1 step 2a - REST conformance on S3 warehouses (static keys): polaris-s3, iceberg-rest-fixture-s3,
  lakekeeper-s3 added to `catalogs-ci.yml` (S3 server: moto; `quay.io/minio/minio` now refuses anonymous pulls,
  first run 40677711018 failed on that). Test only. Catalogs CI green: run 40677907066 (all 5 entries, 3/3 tests
  each). Dev CI green: run 40677906918 (head bfa1912). Step 2b analysed (DuckDB accepts `CREATE OR REPLACE
  TEMPORARY SECRET` after lockdown); its core part waits on Open question Q1.
- 2026-10-28: C1 step 3a - answers through the server for REST catalog tables (local warehouses):
  `RestCatalogServerConformanceTest`, run by `catalogs-ci.yml` for polaris and iceberg-rest-fixture. Test only.
  Catalogs CI green: run 40692394116 (all 5 entries; the 2 file entries run the new test). Dev CI runs 40692394131 and
  40693932375 (head 207b74c): 912 tests, the only failure `DistributedClusterIntegrationTest` (lane 3, intermittent,
  not this change), see Open questions Q2.
- 2026-10-28: C1 steps 3b + 3c - `RestCatalogServerConformanceTest` on the S3 entries (s3 plugin in the server,
  S3FileIO writer) and an HMS reference (REST metadata registered in the in-process metastore, local warehouses).
  Test only. Catalogs CI green: runs 40712157558 (3b, all 5 entries) and 40712744584 (3c, all 5 entries). Dev CI:
  run 40712744776 green (head 2cfa23e). No new open questions.
- 2026-10-28: C1 step 4 - Unity Catalog OSS 0.7.0 as an Iceberg REST catalog: `catalogs-ci.yml` entry
  `unity-catalog` (managed tables on a local storage root); fixtures can leave columns out
  (`ICEBERG_REST_IT_OMIT_COLUMNS`, UC refuses `time`), server test can scope namespaces
  (`ICEBERG_REST_IT_NAMESPACES`). Test only. Catalogs CI green: run 40719031624 (all 6 entries). Dev CI: run
  40719031475 (2498dd4) cancelled by a newer push; run 40720127635 (head 9efe670, includes it) green. New open
  question Q3 (one unloadable table makes a whole catalog unavailable - core lane). C1 done apart from 2b (Q1);
  next: C2.
- 2026-10-28: C2 step 1 - probe of DuckDB's postgres / mysql scanners v1.5.6 under the server's lockdown
  (`catalogs-ci.yml` job `jdbc-scanners`, `tools/ci/DuckDbScannerProbe.java`). Test only. Reads, projection and
  filter pushdown work after the lockdown; `*_query` pass-through and `postgres_scan('<dsn>')` also get through
  -> new open question Q4 before a plugin is built. Catalogs CI: runs 40730338121 (probe bug: a failed check
  closed the shared statement), 40730719713, 40731100450 (final probe; all 7 jobs green).
- 2026-10-28: C3 step 1 - probe of DuckDB's delta extension v1.5.6 on Spark-written Delta tables under the
  server's lockdown (`catalogs-ci.yml` job `delta`, `tools/ci/delta-fixtures.py`, `tools/ci/DuckDbDeltaProbe.java`).
  Test only. Every Delta feature checked (DVs, column mapping name/id, checkpoints, time travel, partitions,
  evolution, types) reads equal to Spark, local and S3; but `write_blob` / `copy_dir` write anywhere and
  delta-kernel's own I/O reads logs / connects outside the lockdown -> new open question Q5; Q4 addendum (the SPI's
  existing `refuseFunctions`). Catalogs CI: runs 40750693235, 40751237504, 40751817639 (delta job green in each,
  later runs add checks; 40751817639 all 8 jobs green). Dev CI: push runs cancelled by newer pushes; run
  40751852380 (workflow_dispatch, head 0f8eb5e) still running at session end (change is tools/ci + workflow only).
- 2026-10-28: C3 Ozone part - Apache Ozone 2.0.0's S3 gateway as an S3 backend (`tools/ci/ozone-s3.sh`,
  `catalogs-ci.yml` entry `iceberg-rest-fixture-ozone-s3`): both REST conformance tests pass on it unchanged. Test
  only. Catalogs CI green: run 40770759266 (all 9 jobs). Dev CI run 40770759218 (head c29bb31) still running at
  session end (change is tools/ci + workflow only). Q1, Q4, Q5 still open (no answer yet), so C1 step 2b, C2 step 2
  and the Delta plugin stay unbuilt; C4 waits on P4 (plugins lane, views part (d) next). Next: Delta-on-HDFS probe
  (C3 note above).
- 2026-10-28: C3 Delta-on-HDFS probe - single-node HDFS in the `delta` job (`tools/ci/hdfs-single.sh`), probe with
  hadoopfs in an Ubuntu 20.04 JDK container: delta v1.5.6 cannot read `hdfs://` tables (delta-kernel has no HDFS
  object store; hadoopfs reads the Parquet files). Test only. Catalogs CI green: run 40784918475 (all 9 jobs). Dev
  CI run 40784918326 (head 77babce) still running at session end (change is tools/ci + workflow only). Q1, Q4, Q5
  still unanswered: C1 step 2b, C2 step 2 and C3 step 2 stay unbuilt. P4 (plugins lane) is closed, so C4 is
  unblocked: next item.
- 2026-10-28: C4 steps 1 + 2 - Hive insert-only (MM) tables in the hive plugin behind the new optional setting
  `hive.insertOnlyTables` (default false, additive): `InsertOnlyDirectories` (Hive's getAcidState rules, ambiguous
  layouts refused) + metastore snapshot + listing; `InsertOnlyDirectoriesTest` (9), `HiveInsertOnlyTableTest` (4,
  in-process HMS with txn tables). Module `flight-plugin-hive` (plugins lane's module; C4 is lane 4's item):
  100 tests, checkstyle, spotbugs clean locally (`HiveCatalogTest` needs a UTF-8 locale for its path test).
  CI green: Dev CI 40787036947 and Catalogs CI 40787036921 (head 9b231bf). Pre-release: dispatch 40788225372
  (package=true) was cancelled by the plugins lane's push 5e9ac98 (same concurrency group); the next package=true
  run on main carries C4 - dispatch one next session if none exists by then. Next: C4 step 3.
- 2026-10-29: C4 step 3 (a) + ORC part of (b) - `HiveInsertOnlyIntegrationTest` (insert-only Parquet, partitioned
  and ORC tables through the server, complete = partial = committed rows, commit / INSERT OVERWRITE visibility,
  `delete_delta` refused). It found a bug in step 2: a refused layout was an IOException, so the server's views
  refresh treated the metastore as down and left out every later table; fixed in the hive plugin
  (`UnreadableTableException`, only that table skipped) plus a small core commit (63679fe,
  `catalogCompleteReason` catches RuntimeException; STATUS.md Log line). Locally: hive plugin 100 tests +
  checkstyle + spotbugs, server Hive ITs + QueryPlannerTest 90 tests. CI green on head 01884e2: Dev CI 40798070064,
  Catalogs CI 40798070095. Pre-release: Dev CI dispatch 40799537000 (package=true) running at session end (also
  carries C4 step 2; if another lane's push cancels it, dispatch again). Q3 addendum (other hive refusals still
  hide siblings). Q1, Q4, Q5 unanswered. Next: C4 step 3 rest (text insert-only, real Hive writer in CI).
- 2026-10-29: C4 step 3 (b, text) + (c) - insert-only text tables in `HiveInsertOnlyIntegrationTest` (5/5
  locally), and tables written by a real Hive 4.0.1 (`tools/ci/hive-mm-writer.sh`: inserts, failed inserts,
  INSERT OVERWRITE, major / minor compaction, gzip text; metastore started on the same Derby) read through the
  server by `HiveRealWriterIntegrationTest`: equal to Hive's answers in both modes. Test + CI only, no plugin
  change needed. Catalogs CI: 40801018004 green (b); 40802647788 and 40804099011 failed in the new job
  `hive-writer` (Hive 4 answers nothing for MM tables below the runner's `_temp` directory - see C4 notes),
  40805065862 green (all 10 jobs). Dev CI green: 40805065855 (head 6558c3d). Pre-release (package=true, also
  carries C4 step 2): dispatch 40806577145 (head 7d0826f) running at session end - if another lane's push
  cancels it, dispatch again. Q1, Q4, Q5 still unanswered. Next: C4 (d) needs a decision (full ACID vs
  Paimon/Hudi) - see C4 "Next"; asked as Q6.
- 2026-10-29: C4 (b) probe (Q6 default) - Catalogs CI job `paimon-hudi` (`tools/ci/paimon-hudi-fixtures.py`,
  `tools/ci/paimon-hudi-probe.py`): Paimon 1.2.0 and Hudi 0.15.0 / 1.0.2 tables written by Spark 3.5.3. DuckDB
  v1.5.6 has `paimon` and `hudi` extensions, both equal to Spark on every table; a light reader (metadata +
  `read_parquet`) is equal on all Paimon tables and Hudi COW / compacted MOR, refuses MOR with logs. Test only.
  Catalogs CI: runs 40810022672 and 40810359864 (`paimon-hudi` green in both). Dev CI: push runs 40810022826 /
  later (tools/ci + workflow only). New open question Q7 (route for Paimon / Hudi). Q1, Q4, Q5, Q6 unanswered.
  Next: lockdown probe of the two extensions.
- 2026-10-29: C4 (b) lockdown probe - `tools/ci/DuckDbPaimonHudiProbe.java` in the `paimon-hudi` job: paimon / hudi
  v1.5.6 under the server's lockdown answer every local fixture equal to Spark; paimon reads S3 with its own
  `paimon` secret, hudi cannot read S3 (goes to the metadata service), neither reads HDFS; both read local files
  outside `allowed_directories`, no write functions. Test only. Catalogs CI: run 40820331670 (paimon-hudi green,
  probe flaw: a lazy paimon ATTACH on S3/HDFS broke later lookups, outside copies in the wrong layout), run
  40821054886 (fixed probe; all jobs green). Dev CI push runs 40820331658 / 40821054924 cancelled by newer pushes; dispatch 40821204741 (head 06833e7, includes this) running at session end (tools/ci + workflow only). Q7
  updated. No new pre-release needed: the last runtime change of lane 4 (hive plugin) is in pre-release
  `server-1.0.0-01884e287668`; later C4 work is test-only. Q1, Q4-Q7 unanswered.
- 2026-10-29: C4 (b) Hudi on S3 - the lockdown probe's `hudi_option` used the wrong separator; with `;`-separated
  object_store keys `read_hudi` answers every Hudi fixture on S3 equal to Spark after the lockdown, and the same
  options also reach S3 paths outside `allowed_directories` (refuse `read_hudi` in client SQL, as planned). Test
  only. Catalogs CI green: run 40823753558 (all jobs). Dev CI run 40823753684 (head 37fe039) running at session end
  (tools/ci only). Q7 updated. Q1, Q4-Q7 unanswered; no unblocked item left for lane 4.
