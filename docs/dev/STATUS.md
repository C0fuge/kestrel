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
- CI: `.github/workflows/dev-ci.yml` (push to main on code paths + manual; ~6 min): `clean verify checkstyle:check
  spotbugs:check -DexcludedGroups=perf,real-services` with the DuckDB 1.4.5 extensions from `ci-assets` (Iceberg and
  fake-S3 integration tests run; hadoopfs IT skipped). Manual dispatch with input `package=true` adds the release
  job (linux-only jar + plugins tar + START-NOTES + SHA256SUMS as pre-release `server-<version>-<sha>`); dispatch via
  the GitHub MCP `actions_run_trigger` (`gh` is not authenticated in cloud sessions). Upstream `ci.yml` is
  manual-only. Locally the JVM needs a UTF-8 locale (`LC_ALL=C.UTF-8`) or the Hive tests fail on non-ASCII paths.
- Build: `./mvnw -B clean package` (JDK 21); linux-only jar: `-Plinux-only`; plugins dir: `tools/make-plugin-dir.sh`.
  Integration tests that need real DuckDB extensions take `-Diceberg.it.extensionDir=<dir with v1.4.5/linux_amd64>`
  and `-Dhadoopfs.it.extension=<file>` (hadoopfs needs libkrb5 etc. — run that one in an AlmaLinux 8 container or
  skip it); `perf` tests are opt-in.

## Next steps (one per session)

0. **Upstream is active again (checked 2026-10-23)** — upstream `main` is still dbacd8d (our base), but new work sits on
   public upstream branches (read-only for us: `git fetch https://github.com/VadimSurpin/arrow-flight-server.git
   '+refs/heads/*:refs/remotes/upstream/*'`; never push there). Review per feature and port what beats ours (rule:
   try ours and theirs, keep the more effective variant per feature, our features stay; record the decision per item
   in docs/dev/UPSTREAM-SYNC.md with numbers where possible):
   - `feat/starrocks-native-integration` (86 commits, 10-16..10-23): server — stream DuckDB results instead of
     materializing (compare with ours), bounded DuckDB read-ahead + unordered scan output, apply client runtime
     IN-filters and bloom filters to plain scans, table-statistics Flight action, optional parallel ZSTD batch
     compression + wire-format matrix, narrow DECIMAL wire form (`decimalWire=narrow`), DuckDB spill to a temp dir,
     S3 via s3a + httpfs (compare with our S3 plugin), join key reduction / parse cache limit, exit on failed start;
     plus their StarRocks fork patches (runtime filters to the Flight server, aggregate predicate pushdown) — note
     anything our StarRocks/Trino connectors should adopt in Open questions (the coordinator forwards it).
   - `feat/trino-flight-connector` (11 commits, 10-16..10-22): their own Trino connector (`flight-trino`: Kerberos
     auth, statistics, routing/partitioning, types) — compare with our plugin (separate repo); server-side bits only here.
   - `perf/pushdown-statistics` (3 commits, August): check if anything is still useful.
   - Dependabot PRs upstream: hazelcast 5.7.0, netty 4.2.19, grpc 1.85.0 — fold into 1b.
   - **Progress:** triage of every server-side feature is in `docs/dev/UPSTREAM-SYNC.md` (rows S1..S12, T1..T2).
     S1 (exit on failed start) ported, S2 ours already, S5 (shared DuckDB database) rejected as-is (breaks our
     per-connection catalog views). Next session: the first **open** row there (S3: spill size cap), then S4
     (needs an order-dependence audit + stand measurement), S12 together with item 6.
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

- Upstream's `.github/workflows/ci.yml` now runs only on manual dispatch (it ran on every push and Dependabot PR,
  failed, and doubled the minutes; dev-ci.yml covers the same checks). Revert the `on:` block if you want it back.
- Dependabot PRs (#4..#7 on this repo) no longer get CI. Close them or fold the bumps into item 1b?
- `duckDbMemoryLimit` / `duckDbThreads` apply per DuckDB connection (one in-memory instance per worker thread), so a
  node can use limit × live connections (upstream hit an OOM kill this way and switched to one shared database,
  f5f11b3). We keep per-connection instances because catalog views are per connection; options: (a) document it and
  size the limit as node budget / expected concurrent connections, (b) per-query view names so a shared database becomes safe (bigger
  change). Which one?
- Upstream wire extensions (UPSTREAM-SYNC S7 `decimalWire=narrow`, S8 runtime IN/bloom filters in the ticket): want
  them (opt-in) for our StarRocks/Trino connectors? Both change what clients see/send; for the coordinator: upstream's
  StarRocks fork pushes runtime filters and partial aggregates (SUM cast to DOUBLE because integer SUM streams as
  DECIMAL(38,0)) to the server.

## Log

- 2026-10-23 (maintainer's local coordinator): repo created from local integration-3 (e9ef57b); heavy upstream
  workflows disabled; CI assets uploaded.
- 2026-10-23 (cloud session): item 1 CI done. `dev-ci.yml` added; first run green on c4ca0bf (run 39990624546: build
  5 min, server 642 tests / 2 skipped, client-core 149, spark 162, starrocks 7). Fixes needed for green:
  `SparkJoinIntegrationTest` moved from flight-server (no `flight` data source on its classpath, it always failed)
  to flight-spark; Hive tests need a UTF-8 JVM locale (set in the workflow). Package dispatch run 39991672183 did
  not reach the package job: `HiveCatalogTest.anOutageIsAnErrorNotAnEmptyAnswer` failed once (`schemas()` after the
  metastore proxy closed did not throw); 10 local runs pass. The test proxy had a race (a connection accepted while
  `close()` ran stayed open), fixed in HmsFixture (96b878a); root cause not confirmed: if that test fails again,
  root-cause it before other work. Package dispatch run 39993691816 on 96b878a green (build 5 min + package 3 min):
  pre-release `server-1.0.0-96b878a93be8` (linux jar 133 MB, plugins tar 40 MB, START-NOTES.md, SHA256SUMS).
