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
    - **Next (step 4):** Unity Catalog OSS: check whether `unitycatalog/unitycatalog` (Docker Hub) serves the Iceberg
      REST endpoint (`/api/2.1/unity-catalog/iceberg`) read-only for Delta-UniForm tables only - if it cannot create
      Iceberg tables through REST, `IcebergFixtures.writeRest` cannot feed it and the entry needs UniForm tables
      written another way (likely out of reach without Spark; then record that and close C1 apart from 2b).
      Step 2b (vended credentials) still waits on Q1. After step 4 move on to C2.
C2. **JDBC federation plugin:** PostgreSQL and MySQL tables as a data source through DuckDB's `postgres` / `mysql`
    extensions (bundle for 1.5.6 offline like the others; check availability), read-only, pushdown of filters/projection,
    credentials from the plugin config (never logged), Ranger checks as for other tables. CI with postgres/mysql
    containers, type matrix, TPC-H SF0.1 answers.
C3. **Delta Lake tables** via DuckDB's `delta` extension (check 1.5.6 build), from HDFS and S3; refuse clearly what it
    cannot read (deletion vectors / column mapping if unsupported). Then: Apache Ozone S3 gateway as an S3 backend test.
C4. Hive ACID insert-only tables (after the plugins lane's P4), then Paimon/Hudi read-only if DuckDB or a light Java
    reader allows.

## Open questions

- Q1 (2026-10-28, C1 step 2b): vended credentials need the server to install per-table temporary S3 secrets on
  DuckDB connections after lockdown (`CREATE OR REPLACE TEMPORARY SECRET`, scope checked below the catalog's
  roots, values redacted), fed by a new optional field on the catalog SPI. It touches the DuckDB lockdown
  (security), so: may lane 4 add this (opt-in, off by default, own commits), or should the core lane own the server
  part? Until answered, lane 4 continues with step 3 and C2 and leaves 2b's core part unbuilt.

- Q2 (2026-10-28, for lane 3 / maintainer, not lane 4's code): `DistributedClusterIntegrationTest` is flaky on
  `main` since D2 (c) part 2 (c7e436e): `peerStreamCutBeforeTheFirstBatch...:187` and
  `peerStreamCutDuringTheAnswer...:212` fail with `mergedAnswers()` 1 instead of 0 in 2 of 3 local runs and in Dev CI
  runs 40692394131 / 40693932375. Likely cause: a relay cut before the schema now fails over to the endpoint's backup location, which
  bypasses the cutting relay, so the answer is merged (correct rows) instead of falling back. Lane 4 leaves it to
  lane 3.

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
