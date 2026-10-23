# Server development status (cloud + local)

## Where things stand (2026-10-23)

- `main` = branch integration-3: integration-2 (our engine on upstream layout, cluster fixes for member loss and the
  ticket secret, DuckDB 1.4.5, hadoopfs timeout settings, extension version check) + catalog SPI + plugins s3, lakefs,
  iceberg (hadoop + REST/Gravitino), hive (HMS 3.1.3 Thrift) + stand fixes (lakeFS L-02..05, Iceberg I-06..09,
  Hive HV-07..12). Local build "int7" = this main at 4b2aa29 (eq deletes: every query reads all columns of a snapshot with equality deletes, filters kept outside the scan — correct, ~40 s per query on 1.5 M rows; see Next steps 2). Full offline build green: server 638 tests.
- Verified on the maintainer's stand (HDFS, MinIO S3, lakeFS, HMS+PostgreSQL, Gravitino 1.3.1): TPC-H SF10 22/22 via
  JDBC and the Trino plugin (checksums 9e2af63e8711 / 05bc0dc2713a), 3-node cluster kill tests, lakeFS 22/22 per
  branch/commit/tag, Hive 76/89 reference queries (rest explained), Gravitino 15 tables = DuckDB; clients: Trino plugin,
  StarRocks native connector, ADBC (separate repos).
- Build: `./mvnw -B clean package` (JDK 21); linux-only jar: `-Plinux-only`; plugins dir: `tools/make-plugin-dir.sh`.
  Integration tests that need real DuckDB extensions take `-Diceberg.it.extensionDir=<dir with v1.4.5/linux_amd64>`
  and `-Dhadoopfs.it.extension=<file>` (hadoopfs needs libkrb5 etc. — run that one in an AlmaLinux 8 container or
  skip it); `perf` tests are opt-in.

## Next steps (one per session)

1. **CI** `dev-ci.yml`: on push to main (code paths) and manual: full build + all tests (excluding perf / real
   services) + Checkstyle + SpotBugs, with the iceberg/avro/httpfs extensions from `ci-assets`
   (`duckdb-extensions-v1.4.5.tar.zst`); a second manual job "package": linux-only jar + plugins dir as a GitHub
   Release `server-<version>-<sha>` (assets: jar, plugins tar, START-NOTES, SHA256SUMS). Keep it under ~25 min.
1b. **Dependency security upgrades** (audit 2026-10-23: `docs/dev/DEPENDENCY-AUDIT.md`; native/C++ items there are
   unverified): Hazelcast 5.6.0 → 5.7.0 (critical/high advisories on the member port; also keep the port private and
   document it); Netty → 4.2.17+ under one `netty-bom`, replace `netty-all` by the modules in use; Jackson 2.15.3 →
   2.18.x/2.22.x aligned via `jackson-bom`; aircompressor 0.21 → 2.0.3 and snappy-java → 1.1.10.12 (Parquet read path);
   Hive plugin libthrift 0.9.3 → 0.24.0 (run the hive tests); then produce a fresh runtime libs set from the poms
   (`dependency:copy-dependencies`, only what the server needs) as a release asset `flight-server-libs-<sha>.tar.zst`
   replacing the stale stand set (Arrow 18, gRPC 1.65, BouncyCastle 1.79, ZooKeeper 3.6.3, Netty 3.10.6, Jetty 9.4,
   lz4-java 1.8.0). Full test suite green after each group of upgrades.
2. **Iceberg equality deletes are correct but slow** (full scan of all columns per query, 25–90 s; DuckDB 1.4.5's
   iceberg extension refuses or mis-applies eq deletes when filters/projections are pushed). Make them fast and
   correct: e.g. read data files + equality delete files yourself (anti-join on the equality columns with sequence-
   number rules), in complete and partial mode (partial: per-file anti-join on the node). Prove with a test table
   with eq + position deletes vs an independent answer.
3. **Iceberg time travel**: expose snapshot reads (`FOR VERSION AS OF` / a `table@snapshot` name form or a session
   setting — propose in Open questions first which form clients can send), pinned per query like the current snapshot.
4. **Second S3 store**: today one S3 store per server (a second S3 root is refused). Allow several S3 stores
   (different endpoints/credentials/buckets) per server for catalogs (e.g. Iceberg warehouse on another S3).
5. **Config precedence bug (upstream)**: env vars such as `FLIGHT_RESULT_MODE`, `FLIGHT_ENDPOINT_BACKUP_LOCATIONS`,
   `DUCKDB_*` are shadowed by values in the bundled `arrowflight.properties`; documented precedence is env > file.
   Fix with tests (and keep `-D` highest).
6. **Prepared-statement cache bound**: is `DuckDbAdapter.stmtCache` bounded? Dynamic filters in Trino produce a new SQL
   text per query. Bound it (LRU, size setting, metrics) if not.
7. **Kerberos for the HMS client** (SASL/GSSAPI in `flight-plugin-hive` `ThriftMetastore.open`) and check HDFS
   Kerberos with the hadoopfs extension: CI with a KDC container (e.g. MIT krb5 in a container) + a kerberized HMS if
   feasible; otherwise unit-level with a fake SASL server; docs.
8. **Ranger hardening** (security): details are kept outside this public repo; ask the maintainer before starting. (The maintainer wants Ranger/
   Kerberos finished before production; implement here, the stand check comes later.)
9. **Per-user identity** (needed for Ranger/Kerberos): JWT/bearer pass-through of the end user to Ranger checks;
   design in Open questions first.

## Open questions

(none yet)

## Log

- 2026-10-23 (maintainer's local coordinator): repo created from local integration-3 (e9ef57b); heavy upstream
  workflows disabled; CI assets uploaded.
