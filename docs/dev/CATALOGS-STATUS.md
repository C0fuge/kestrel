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
    - **Next (step 2): S3 + vended credentials.** Add a MinIO service (`quay.io/minio/minio` works on GitHub runners;
      Docker Hub `minio/minio` is gone) and a Polaris S3 catalog with STS (MinIO supports AssumeRole), plus Lakekeeper
      (`quay.io/lakekeeper/catalog`, needs PostgreSQL + `POST /management/v1/bootstrap` + warehouse) and Nessie
      (`ghcr.io/projectnessie/nessie`, `nessie.catalog.warehouses.*` + S3 settings). Writer side: the test writes with
      Iceberg Java RESTCatalog; needs `iceberg-aws` + AWS SDK as test deps (or write via the catalog's vended creds).
      Plugin side (additive, optional setting e.g. `iceberg-rest.vendedCredentials=true`, default off): send
      `X-Iceberg-Access-Delegation: vended-credentials` on loadTable, read `config` / `storage-credentials`
      (`s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`, `s3.session-token-expires-at-ms`) and hand them
      to the S3 store / DuckDB secret per table prefix, refreshed before expiry. Check first how the s3 plugin's stores
      take per-prefix credentials (may need an additive SPI method: then its own commit + SPI version bump). Never log
      the credentials (SecretRedactor).
    - Step 3: answers through the server (partial/complete) for the REST tables vs HMS, Unity Catalog OSS if feasible.
C2. **JDBC federation plugin:** PostgreSQL and MySQL tables as a data source through DuckDB's `postgres` / `mysql`
    extensions (bundle for 1.5.6 offline like the others; check availability), read-only, pushdown of filters/projection,
    credentials from the plugin config (never logged), Ranger checks as for other tables. CI with postgres/mysql
    containers, type matrix, TPC-H SF0.1 answers.
C3. **Delta Lake tables** via DuckDB's `delta` extension (check 1.5.6 build), from HDFS and S3; refuse clearly what it
    cannot read (deletion vectors / column mapping if unsupported). Then: Apache Ozone S3 gateway as an S3 backend test.
C4. Hive ACID insert-only tables (after the plugins lane's P4), then Paimon/Hudi read-only if DuckDB or a light Java
    reader allows.

## Log

- 2026-10-28 (maintainer): lane created; C1-C4.
- 2026-10-28: C1 step 1 - Iceberg REST conformance test against Apache Polaris 1.2.0 (OAuth2) and the Iceberg REST
  fixture 1.10.0 in the new workflow `catalogs-ci.yml`; all checks equal to the Hadoop-layout reference (names,
  columns, files, row/delete counts, partial-read problems, DuckDB rows). Test only. Catalogs CI green: run
  40670485477 (earlier failures 40669539591, 40669821302, 40670292342 were CI setup: container uid/permissions,
  DuckDB setting). Dev CI green: run 40672085389 (head c258adf, includes these commits). No open questions.
