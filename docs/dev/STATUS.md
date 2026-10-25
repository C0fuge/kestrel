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

> **Maintainer, 2026-10-25:** the production Hadoop cluster has HA NameNode with ZooKeeper (automatic failover),
> Kerberos and Ranger. Versions (2026-10-25): Hadoop 3.1.3 (vendor build) on the cluster side; HA with 2 NameNodes,
> automatic failover, ConfiguredFailoverProxyProvider, RPC 8020; ZooKeeper 3.9.2 ensemble, SASL/Kerberos; Ranger 2.4.0
> (HDFS authorizer on the NameNodes; plugins also for Hive). Our client libs must work against a Hadoop 3.1 cluster
> (RPC/SASL compatibility of the newer hadoop-client), and Ranger checks must target the 2.4 REST API. Consequences: item 11 (ZooKeeper and lz4-java missing from the new
> runtime libs set) is now required — put the ZooKeeper client (and its Curator/Jute deps as Hadoop needs them) and
> lz4-java back into the libs set, with a startup/link check that the HA/ZK and Kerberos code paths load. Keep items
> 7 (Kerberos for HMS and HDFS), 8 (Ranger subquery bypass) and 9 (per-user identity) — they are needed for production.

0a. ~~**hadoopfs timeout defaults 3 s / 10 s**~~ — done 2026-10-24 (732e238, CI run 40140133221 green).

> **Stand, 3-node S3 cluster on int7 (2026-10-25)** (unnumbered; sessions keep taking the numbered items, see Open questions): all correct (JDBC 22/22, Trino 22/22, B2, B5, mid-stream kill, ticket matrix, MinIO outage + recovery without restart). Server follow-ups, none give wrong results: (a) during an S3 outage each read needs 20–40 s to fail (DuckDB httpfs retries + DNS); make S3 retry/timeout settings explicit and shorter when the endpoint is unreachable; (b) plans that are never read keep their node load reservation up to 10 min, skewing later plans (one node got no endpoints ~5 min) — release reservations on ticket expiry/cancel or shorten the TTL; (c) after B5 the first plan on S3 waits 9–10 s while dead members are dropped (HDFS cluster: ~1 s) — find why.

> **Stand regression, single node, build 4eaab052deaf with the new runtime libs set (2026-10-25)** (unnumbered): the
> release jar + `flight-server-libs` (60 jars, Hadoop 3.5.0) instead of the old 335-jar libs dir, against int7 on the
> same node, alternating runs. HDFS (HA nameservice, hadoopfs 51b7220-afc3): startup log shows timeouts 3000/10000,
> extension versions and `storageSetupVerified` (lockdown check); TPC-H SF10 JDBC 22/22 `9e2af63e8711` x3, Trino 455
> + plugin v10 22/22 `05bc0dc2713a` x3, jdbc.sh 27/2, safety 82/82. S3 plugin on MinIO (read-only user): JDBC 22/22
> x2, 27/2 + 82/82. HMS catalog (hdfs + s3 roots, libthrift 0.24.0): reference queries 76 OK / 2 expected / 11
> time travel in complete and partial mode (= int7). Iceberg local warehouse: 9/9 answers = int7, refusals intact.
> Timings equal within noise (HDFS JDBC warm 24.6 vs 24.7 s, Trino 43.9 vs 43.7 s, S3 JDBC 33.0 vs 32.9 s); JVM
> RSS ~50 MB lower idle, same peak (2.1–2.5 GB after Trino). No missing class on any tested path; a Kerberos keytab
> login (no KDC) reaches JAAS. Follow-ups: items 10 and 11.

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
     per-connection catalog views), S3 (spill size cap) ported as per-connection `duckDbMaxTempSize`, S4
     ported as opt-in `duckDbPreserveInsertionOrder` / `duckDbStreamingBufferSize` (order audit clean; stand
     measurement pending, see Open questions), S6 ported as opt-in `endpointsPerServer` (fixed upstream's
     split of single-node queries; stand measurement pending), S9 ported as action
     `arrowflight.table_statistics` (no NDV yet), T1 rejected in favour of S9, S10 rejected as-is
     (upstream drops the file lockdown for S3; speed comparison moved to the stand, Open questions),
     T2 ported (client pool identity covers every setting; routing headers on every call), perf
     branch: P1 (Q6/Q17) ours already, P3 ported (Spark post-pushdown statistics, filter part
     opt-in), P4 cost gate deferred to the stand, S11 (batch compression) deferred (Open
     questions), S12 ported to all three query-text caches together with item 6. The remaining
     item 0 work is only S7/S8 (wait on Open questions) and the stand measurements: 1b is next.
1b. **Dependency security upgrades** (audit 2026-10-23: `docs/dev/DEPENDENCY-AUDIT.md`; native/C++ items there are
   unverified). **Done 2026-10-24 (server module):** Hazelcast 5.7.0, Netty 4.2.19 under `netty-bom`, `netty-all`
   replaced (README notes the private Hazelcast port; `DependencyFloorTest`); `flight-spark` imports the same
   `netty-bom`. **Done 2026-10-24 (client group):** `flight-client-core` / `flight-starrocks` on the same BOM
   (Netty 4.2.19, tcnative 2.0.84, no `netty-all` from the provided Hadoop deps; `NettyFloorTest`).
   **Done 2026-10-24 (Jackson):** one Jackson 2.21.8 (`jackson-bom`) in server, client-core, spark and
   starrocks; the Spark connector's shaded jar relocates Jackson (Spark 3.5 keeps its 2.15); Iceberg plugin
   2.22.3; floor tests in every module. Stand: run one Spark read with the new shaded connector jar.
   Dev CI runs again (2026-10-24 22:49, repo public). **Done 2026-10-25 (Parquet read path):** server Parquet
   1.13.1 → 1.18.1, bringing aircompressor 2.0.3 and snappy-java 1.1.10.8 (`DependencyFloorTest.parquetCodecs*`).
   **Done 2026-10-25 (Hive plugin):** libthrift 0.9.3 → 0.24.0 for the metastore client (1 GiB response cap);
   the test metastore (Hive 3.1.3 server, broken on libthrift 0.14+) runs on 0.9.3 in its own class loader.
   Stand: one Hive reference run (framed and unframed if the HMS has both) with the new plugin dir.
   **Done 2026-10-25 (runtime libs set):** the build copies the Hadoop client libraries the shaded jar leaves to
   the deployment (`provided`) to `flight-server/target/libs` (58 jars, 42 MB, Hadoop 3.5.0, mediated against
   the jar's own libraries); the `package` dispatch publishes it as `flight-server-libs-<version>-<sha>.tar.zst`;
   `LibsSetTest` checks floors and starts the server on jar classpath + libs (Parquet read) and without libs
   (must fail). START-NOTES / README now start with `java -cp '<jar>:libs/*'` (`java -jar` never worked without
   Hadoop on the class path). Stand: replace the old libs dir with the new asset (first `package` dispatch
   pending, see Log) and run one HDFS + one S3 query. Follow-up (optional, after that stand run): prune what only
   Hadoop daemons use from the set — MapReduce/YARN client, Jetty 9.4.58 (EOL line), Jersey 2/hk2, leveldbjni,
   commons-daemon, jline, `hadoop-hdfs` (server) — the server's own code uses hadoop-common + hdfs-client only;
   the API jars overlapping the shaded jar (javax.servlet 4.0.0 vs jakarta.servlet-api 4.0.4, JAX-RS 1.1 in
   Ranger's `jersey-bundle` vs jakarta.ws.rs-api 2.1.6, javax.annotation/inject) would go with them.
   Item 1b is done apart from the stand checks named above.
2. **Iceberg equality deletes are correct but slow** — **complete mode done 2026-10-25** (5466f86): snapshots with
   equality deletes are read by `CatalogSources.deletesAppliedSource` (read_parquet by field ID, ANTI JOINs on the
   equality columns with IS NOT DISTINCT FROM, position deletes on normalised path + `file_row_number`; delete
   assignment = the plugin's `FileScanTask.deletes()`, i.e. Iceberg's sequence-number rules); deletion vectors,
   columns without field ID, equality columns the schema dropped, a column named `file_row_number` keep the old
   iceberg_scan read. **Partial mode done 2026-10-25** (see Log): `completeReason` accepts deletes that
   `CatalogSources.deletesApplicable` covers; the executing node re-derives each ticket file's delete files from
   the pinned snapshot (tickets unchanged) and reads them with `deletesAppliedSource`. **Remaining:** (ii) stand: time the int7 eq-delete
   queries (was 25–90 s) on the new source and check answers vs the old ones, in complete mode (JDBC) and partial
   mode (Trino, StarRocks: these tables now get one endpoint per node instead of one complete endpoint); (iii) many data files with position
   deletes become one UNION ALL branch each — measure planning time on a table after a large MERGE. Original
   text: Iceberg equality deletes are correct but slow (full scan of all columns per query, 25–90 s; DuckDB 1.4.5's
   iceberg extension refuses or mis-applies eq deletes when filters/projections are pushed). Make them fast and
   correct: e.g. read data files + equality delete files yourself (anti-join on the equality columns with sequence-
   number rules), in complete and partial mode (partial: per-file anti-join on the node). Prove with a test table
   with eq + position deletes vs an independent answer.
3. **Iceberg time travel**: expose snapshot reads (`FOR VERSION AS OF` / a `table@snapshot` name form or a session
   setting — propose in Open questions first which form clients can send), pinned per query like the current snapshot.
4. ~~**Second S3 store**~~ — done 2026-10-25 (see Log): named stores `arrowflight.s3.stores=<name>,...`, each
   with `arrowflight.s3.<name>.root` and its own endpoint/region/URL style/credentials, own DuckDB secret
   `flight_s3_<name>`. Stand: one Iceberg or Hive catalog in a second MinIO bucket with another (read-only)
   user next to the S3 data directory; check both secrets in the startup log and one query per store.
5. ~~**Config precedence bug (upstream)**~~ — done 2026-10-25 (see Log): every `-D` name > environment >
   `etc/plugins/<ns>.properties` > `arrowflight.properties` > default, server and plugin settings alike.
   Stand: check that no leftover environment variable (e.g. `FLIGHT_RESULT_MODE`, `DUCKDB_*`) now changes
   behaviour that the bundled file used to hide.
6. ~~**Prepared-statement cache bound**~~ — done 2026-10-24 with UPSTREAM-SYNC S12: the statement cache already was
   an LRU of `duckDbStatementCacheSize` (default 64) per worker thread; now texts over 256 Ki chars stay out of it
   (and out of the parse and schema caches), evictions and skips are Prometheus counters.
7. **Kerberos for the HMS client** (SASL/GSSAPI in `flight-plugin-hive` `ThriftMetastore.open`) and check HDFS
   Kerberos with the hadoopfs extension: CI with a KDC container (e.g. MIT krb5 in a container) + a kerberized HMS if
   feasible; otherwise unit-level with a fake SASL server; docs.
8. **Ranger hardening** (security): details are kept outside this public repo; ask the maintainer before starting. (The maintainer wants Ranger/
   Kerberos finished before production; implement here, the stand check comes later.)
9. **Per-user identity** (needed for Ranger/Kerberos): JWT/bearer pass-through of the end user to Ranger checks;
   design in Open questions first.
10. **Local catalog roots under an HDFS `fs.defaultFS`** (stand 2026-10-25, int7 and 4eaab05 alike): with the HDFS
   client config on the class path, `-Darrowflight.iceberg-hadoop.warehouse=file:///wh` (or `/wh`) fails at start:
   "The store opened for /wh does not hold it (roots [hdfs://<ns>/wh])" — `CatalogLocations.normalize` drops
   `file://` and the Hadoop store resolves the bare path against `fs.defaultFS`. Keep the scheme (or qualify bare
   paths with `file:`) for catalog roots and Hive/Iceberg locations; test with an HDFS default FS configured.
11. ~~**Runtime libs set gaps**~~ — done 2026-10-25 (see Log): the set now carries ZooKeeper 3.8.6 (+ jute,
   audience-annotations) and lz4-java (`at.yawk.lz4` 1.10.4), Curator is 5.2.0 in jar and set alike (the jar had
   curator-framework 2.13.0 with curator-client 5.2.0). `LibsSetTest` links every class of hadoop-common,
   hadoop-hdfs-client and hadoop-auth on the thin class path. Stand: replace the libs dir with release `server-1.0.0-c3b276054cc3`'s
   `flight-server-libs` asset and run one HDFS query against the HA nameservice; a real Kerberos login (KDC)
   is still item 7. Note: lz4-java 1.12.1 exists (we follow Hadoop 3.5.0's 1.10.4).

12. **`s3a://` / `s3n://` catalog roots are not matched by the open S3 stores** (found 2026-10-25 while
   testing item 4): `CatalogLocations.normalize` keeps the scheme, S3 stores name their roots `s3://`, so
   e.g. `arrowflight.iceberg-hadoop.warehouse=s3a://b/wh` fails at start ("The store opened for s3a://b/wh
   does not hold it (roots [s3://b/wh/])"); the Hive plugin maps s3a/s3n itself, Iceberg does not. Match
   the S3 scheme forms as one in `StoreRegistry` (`holder`, `locate`) or normalise them; test with an
   s3a warehouse and s3a data file paths in Iceberg metadata.

## Open questions

- Production versions (maintainer, 2026-10-25) vs our client side, not yet checked: the libs set carries the
  ZooKeeper 3.8.6 client (Hadoop 3.5.0's; talks to a 3.9.2 ensemble, SASL client code is the same line) and
  hadoop-client 3.5.0 against a Hadoop 3.1.3 cluster (HDFS RPC and SASL are wire-compatible across 3.x, but
  e.g. the 3.5 client's defaults — `dfs.client.*` timeouts, observer reads off — should be confirmed on the
  stand); the embedded Ranger evaluator is ranger-plugins-common 2.8.0 against a Ranger 2.4.0 admin (policy
  download REST). Should the libs set follow the cluster (ZooKeeper 3.9.x, or Hadoop 3.3.x client), or stay on
  Hadoop 3.5.0 unless the stand shows a problem? Proposal: stay, and test HA failover + Kerberos on the stand
  with release `server-1.0.0-c3b276054cc3` first.

- **Item 3 (Iceberg time travel): ANSWERED 2026-10-25 — yes, implement the proposed `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF` clause.** (proposal 2026-10-25:) Recommended: the
  SQL clause Trino and Spark use, after a catalog table name: `FROM ns.t FOR VERSION AS OF
  8213491011553364004` (snapshot ID) and `FOR TIMESTAMP AS OF TIMESTAMP '2026-10-01 00:00:00 UTC'`
  (latest snapshot committed at or before; a time zone is required, no server-local time). The
  server's parser takes the clause out before DuckDB and pins that snapshot for the query exactly as
  it pins the current one; tickets already carry the snapshot ID, so no ticket or header change, and
  partial mode works the same. An unknown snapshot ID or a time before the first snapshot is an
  error, never the current snapshot; data directory and Hive tables refuse the clause. Alternatives:
  (a) a `t@<snapshot>` name form (no grammar for clients, but clashes with quoting and Trino's Iceberg
  connector dropped it), (b) a session header such as `x-flight-iceberg-snapshot` (new header, one
  table per query). For the coordinator: the Trino plugin / StarRocks connector would have to
  forward the clause (or their own time-travel syntax translated to it). OK to implement the clause?

- Upstream's `.github/workflows/ci.yml` now runs only on manual dispatch (it ran on every push and Dependabot PR,
  failed, and doubled the minutes; dev-ci.yml covers the same checks). Revert the `on:` block if you want it back.
- Dependabot PRs (#4..#7 on this repo) no longer get CI. Close them or fold the bumps into item 1b?
- `duckDbMaxTempSize` (new, UPSTREAM-SYNC S3) is unset by default = DuckDB's cap of 90 % of free disk for every
  connection. Upstream defaults to 20GiB per server; ours is per connection. Set it on the stand (spill disk /
  expected concurrent connections), or want a default?
- `duckDbMemoryLimit` / `duckDbThreads` apply per DuckDB connection (one in-memory instance per worker thread), so a
  node can use limit × live connections (upstream hit an OOM kill this way and switched to one shared database,
  f5f11b3). We keep per-connection instances because catalog views are per connection; options: (a) document it and
  size the limit as node budget / expected concurrent connections, (b) per-query view names so a shared database becomes safe (bigger
  change). Which one?
- UPSTREAM-SYNC S4 on the stand: measure TPC-H SF10 (JDBC and Trino) with `DUCKDB_PRESERVE_INSERTION_ORDER=false`
  and `DUCKDB_STREAMING_BUFFER_SIZE=64MiB` (each alone and both) against the defaults. If they win, should they
  become the defaults? (The order audit found nothing relying on row order without ORDER BY; clients that read
  a plain `SELECT` in file order would see a different order.)
- GitHub Pages: the Actions list shows a `pages build and deployment` run on branch `gh-pages`
  (2026-10-24 18:50 UTC, success); no `gh-pages` branch exists now. CLAUDE.md wants no website: please
  check Settings → Pages is off (sessions cannot change repo settings).
- UPSTREAM-SYNC S6 on the stand: measure TPC-H SF10 partial mode (Trino, StarRocks) with
  `FLIGHT_ENDPOINTS_PER_SERVER=2` and `4` against 1. DuckDB already uses `duckDbThreads` per
  endpoint, and each extra endpoint takes another DuckDB connection (memory limit per connection), so
  a win is expected only where one client stream is the bottleneck. Keep default 1 unless it wins?
- For the coordinator (client repos): the server now answers `arrowflight.table_statistics`
  (README "Table statistics action"; upstream's StarRocks fork patch and `TableStats` parse it
  unchanged). Our Trino plugin / StarRocks connector could feed row counts and column bounds to their
  optimizers from it; `ndv` is always null so far. Want the background NDV estimate (upstream runs
  `approx_count_distinct` per column once per file set; costs a full column scan per table)?
- UPSTREAM-SYNC S10 on the stand (MinIO): compare listing + footer time of a 1000+ object data
  directory and SF10 lineitem through our S3 plugin vs upstream's s3a reader (9f66812) to find any
  win worth porting into our plugin (page size, footer read size). Cloud sessions have no MinIO.
- UPSTREAM-SYNC P3 on the stand (Spark, TPC-H SF10 with `FlightSessionCatalog`): run Q2/Q3/Q5/Q7
  with the read option `statistics.filterSelectivity=true` vs default and check the plans switch to
  broadcast joins without driver/executor memory trouble. If it wins, make it the default? (It is a
  heuristic, no column statistics; P4 cost gate decided after this.)
- UPSTREAM-SYNC S11 (Flight batch compression, LZ4/ZSTD IPC buffers) deferred: upstream's own benchmark was
  negative (LZ4) or mixed (ZSTD), it helps only on slow links, and every client must then decode compressed buffers.
  Is there a slow link (WAN client, cross-site StarRocks) that wants it? If so it becomes an opt-in setting.
- The stand findings (a)–(c) in Next steps (S3 outage read latency, unread plans keeping node load
  reservations up to 10 min, slow first plan after B5 on S3) are not numbered, so sessions keep going down the
  numbered list (1b next). Want one of them taken before 1b's remaining groups? (b) skews endpoint placement
  for ~5 min; none gives wrong results.
- Hazelcast phone-home is on: a started node tries to reach `phonehome.hazelcast.com` (version, cluster size;
  seen 2026-10-25 from the cloud sandbox, where the proxy refused it). Nothing in the config sets
  `hazelcast.phone.home.enabled=false`. Turn it off by default (no client-visible change)? Until then the stand
  can pass `-Dhazelcast.phone.home.enabled=false` or `HZ_PHONE_HOME_ENABLED=false`.
- Item 5 (config precedence, done 2026-10-25) changes which value wins when a setting is set twice:
  environment variables now beat `arrowflight.properties` (both the bundled one and one an operator puts on
  the class path) and `etc/plugins/<ns>.properties`. Before upgrading the stand/production, check the
  service environment for leftover variables that used to be ignored (most visible: `FLIGHT_RESULT_MODE`,
  `FLIGHT_ENDPOINT_BACKUP_LOCATIONS`, `FLIGHT_METRICS_ENABLED`, `DUCKDB_LOCKDOWN`). No wire change by itself.
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
- 2026-10-23 (cloud session): item 0 started. Triage of upstream's server-side work written to
  `docs/dev/UPSTREAM-SYNC.md` (S1..S12, T1..T2, decisions per row). Ported S1: `main` exits with status 1 when the
  start fails before serving or throws an `Error` (regression tests in `HadoopArrowFlightServerUnitTest`); CI run
  40005871418 on 379f72f green. S5 (shared DuckDB database) rejected as-is, question added to Open questions.
- 2026-10-23 (cloud session): item 0, UPSTREAM-SYNC S3 ported: setting `duckDbMaxTempSize` /
  `DUCKDB_MAX_TEMP_SIZE` sets `max_temp_directory_size` on every DuckDB connection (validated as a size, refused as a
  plugin setting, default unset). Tests in `DuckDbAdapterTest`, `ConfigAdapterTest`, `DuckDbSetupValidatorTest`.
  CI run 40022303101 on 2dae6ce green (~8 min).
- 2026-10-23 (cloud session): item 0, UPSTREAM-SYNC S4 ported as opt-in settings `duckDbPreserveInsertionOrder`
  (default true, strict true/false) and `duckDbStreamingBufferSize` (default unset), per connection, refused as
  plugin settings; tests in `ConfigAdapterTest`, `DuckDbAdapterTest`, `DuckDbSetupValidatorTest`. Order audit:
  CI runs 40036505609 / 40037380505 on branch `claude/s4-order-audit` (both settings forced on, audit only, not
  merged) failed only the 3 tests that assert the defaults / golden SET lines (server 649, client-core 149, spark
  162, starrocks 7, plugins green); EXPLAIN on DuckDB 1.4.5 shows the equality-delete LIMIT barrier still keeps
  filters above the scan. CI run 40037403277 on e984ff5 green.
- 2026-10-24 (cloud session): item 0, UPSTREAM-SYNC S6 ported as opt-in `endpointsPerServer` /
  `FLIGHT_ENDPOINTS_PER_SERVER` (default 1; below 1 refused): partial results split each node's files
  into up to N endpoints of similar size, each with its own backups and load accounting; queries
  with a single-node feature (OFFSET, HAVING, DISTINCT aggregates) and joins never split (upstream
  split them on a one-node cluster: wrong answers). Tests in `QueryPlannerTest`, `ConfigAdapterTest`.
  Commit 930e6db. Dev CI run 40046662476 could not start (2 attempts, no runner, see Open questions);
  full local `clean verify checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services`
  green instead (server 649 / 9 skipped — Iceberg ITs need the ci-assets extensions — client-core 149,
  spark 162, starrocks 7, plugins green).
- 2026-10-24 (cloud session): Dev CI still cannot start (dispatch run 40055199702 on 7d4cebe: failed
  in 2 s, no runner). Item 0, UPSTREAM-SYNC S9 ported as Flight action `arrowflight.table_statistics`
  (`TableStatisticsService`: footer row count, bounds, nulls, sizes for data-directory tables through
  the DataStore; catalog snapshot rows/size, upper bound with delete files; refused on a multi-node
  local-disk cluster; no NDV yet); T1 rejected in favour of it. Tests `TableStatisticsServiceTest`
  (vs DuckDB over the same files), `FlightSqlProducerTest.tableStatistics*`. Commit 89c9b95. Full local
  `clean verify checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services` green (server
  660 / 9 skipped, client-core 149, spark 162, starrocks 7, plugins green).
- 2026-10-24 (cloud session): Dev CI still cannot start (dispatch run 40063344609 on 6bbc09b: 5 s,
  no runner). Item 0: UPSTREAM-SYNC S10 rejected as-is (upstream turns the file lockdown off for S3
  data dirs; speed comparison needs MinIO, moved to the stand), T2 ported: `flight-client-core` pool
  identity now hashes every client setting (a `crtVerify=false` client was reused for a verifying
  configuration, likewise trust store / routing / retries / timeout / allocation limit), schema and
  routing headers go with every call and the Kerberos refresh. Tests `ClientHeadersTest` (in-process
  Flight server), `ClientRetryTest.connectionIdentity*` (fail on the old code). Full local `clean
  verify checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services` green (server 660 / 9
  skipped, client-core 154, spark 162, starrocks 7, plugins green).
- 2026-10-24 (cloud session): Dev CI still cannot start (dispatch run 40071167062 on fd6fb30: 5 s, no
  runner). Item 0, perf branch rows P1..P4 in UPSTREAM-SYNC: Q6/Q17 aggregate fixes (88c0a16) ours
  already, 49a5a5c benchmark-only, 482d24b ported to `flight-spark` (pushed global aggregate reports
  1 row; filter selectivity scaling behind read option `statistics.filterSelectivity`, default off;
  NOT of an unknown predicate keeps every row, upstream estimated 0), 7cf1544 deferred to the stand.
  Tests `ScanSelectivityTest`, `TableTest.estimates*`. Full local `clean verify checkstyle:check
  spotbugs:check -DexcludedGroups=perf,real-services` green on d264273 (server
  660 / 9 skipped, client-core 154, spark 173, starrocks 7, plugins green).
- 2026-10-24 (cloud session): Dev CI still cannot start (dispatch run 40079012105 on ce71eec and push
  run 40080400054 on d8237a1: 4 s each, no runner). Item 0: S11 (batch compression) deferred (Open
  questions); S12 ported and widened, closing Next step 6: query texts over 256 Ki chars stay out of
  the parse cache, the result-schema cache and the per-thread statement cache (which was already an
  LRU of `duckDbStatementCacheSize`); one uncached statement slot per thread; counters
  `arrowflight_query_cache_oversize_total` / `arrowflight_query_cache_evictions_total{cache}`. Tests
  in `ParquetQueryParserUnitTest`, `DuckDbAdapterTest`, `ExecutionServiceTest`, `MetricsServiceTest`.
  Commit d8237a1. Full local `clean verify checkstyle:check spotbugs:check
  -DexcludedGroups=perf,real-services` green (server 665 / 9 skipped, client-core 154, spark 173,
  starrocks 7, plugins green). Note: `spotbugs-exclude.xml` still matches the old package
  `net.surpin` (stale, harmless).
- 2026-10-24 (cloud session): Dev CI still cannot start (dispatch run 40086887735 on fb6e840: 3 s, no
  runner). Item 1b, first group (server): Hazelcast 5.6.0 → 5.7.0, Netty 4.2.9 → 4.2.19 under
  `netty-bom`, `netty-all` replaced by codec-http2 + handler-proxy + epoll (x86-64/ARM64); netty-all
  excluded from the provided Hadoop and test Spark deps, Spark's kqueue natives kept out of the jar
  (the BOM manages native modules to runtime scope). Shaded jar checked: no Redis/HAProxy/SMTP/STOMP/
  MQTT/XML/HTTP3/OCSP/DNS/kqueue/io_uring classes. `flight-spark` needed the same BOM (its tests failed
  with `NoSuchMethodError` in the epoll transport on mixed 4.1/4.2 modules). README: Hazelcast port
  must stay private. Test `DependencyFloorTest` (one Netty version >= 4.2.17, unused codecs absent,
  Hazelcast >= 5.7.0). Full local `clean verify checkstyle:check spotbugs:check
  -DexcludedGroups=perf,real-services` green (server 669 / 9 skipped, client-core 154, spark 173,
  starrocks 7, plugins green). Commit 69d50f8; its push run 40088691573 also failed in 3 s, no runner.
  Stand: run the 3-node kill tests once with this jar and
  restart all nodes together (no rolling upgrade 5.6 → 5.7 in Hazelcast open source).
- 2026-10-24 (cloud session): Dev CI still cannot start (dispatch run 40094973520 on 1445f68: 3 s, no
  runner). Item 1b, client group: `flight-client-core` and `flight-starrocks` import `netty-bom` 4.2.19
  (they ran Arrow's 4.2.9 mixed with grpc-netty's 4.1.130 HTTP/2/http/socks/proxy/unix-common;
  tcnative 2.0.74 → 2.0.84); client-core's provided hadoop-client/hadoop-hdfs exclude `netty-all`
  (under the BOM its natives and codecs would become runtime deps of the connectors). Runtime
  classpath diff vs main: only the Netty versions change (+ codec-compression, − the 4.1 codec jar).
  `NettyFloorTest` in both modules (fails on the old poms). Full local `clean verify checkstyle:check
  spotbugs:check -DexcludedGroups=perf,real-services` green (server 669 / 9 skipped, client-core 156,
  spark 173, starrocks 9, plugins green). Commit d59b974; push run 40095535435 failed in 3 s, no runner.
- 2026-10-24 (cloud session): Dev CI still cannot start (dispatch run 40112719429 on e6ff83b: 3 s, no
  runner). Item 1b, Jackson group (commit fbb2aca): 2.15.3 (mixed with Arrow's 2.21.0 jsr310, Ranger's 2.17.2 JAX-RS)
  → one 2.21.8 under `jackson-bom` in flight-server, client-core, spark, starrocks (2.21 = Arrow 19's line;
  the BOM also lifts the Scala module of the test Spark, so the Spark tests run on it). The Spark connector's
  shaded jar relocates `com.fasterxml.jackson` to `pro.surpin.data.arrowflight.shaded.jackson` (checked: no
  class in the jar references the unrelocated package; service files relocated). Iceberg plugin bundles
  2.22.3 (Iceberg 1.12 brought 2.22.2). Floor tests `DependencyFloorTest.jackson*` and `JacksonFloorTest`
  (client-core, spark, starrocks, iceberg; the StarRocks and Iceberg ones fail on the old poms).
  `PluginLoadingTest` now expects the server's 2.21.8. Full local `clean verify checkstyle:check
  spotbugs:check -DexcludedGroups=perf,real-services` green (server 670 / 9 skipped, client-core 158,
  spark 175, starrocks 11, plugins green).
- 2026-10-24 (cloud session): Dev CI runs again (the Actions history restarted with the public repo;
  the earlier "cannot start" question is closed). Dispatch run 40139808951 on f354d38 was cancelled by
  the newer push run. Item 0a: hadoopfs DataNode timeout defaults 5000 / 15000 → 3000 / 10000
  (`AppConfig`, `arrowflight.properties`, startup warning hint, CHANGELOG; golden setup SQL and
  `ConfigAdapterTest` updated, their override cases moved to 4000 so they still differ from the default).
  Commit 732e238: push run 40140133221 green (build 6 min) — first CI run covering 89c9b95..fbb2aca too.
  Full local `clean verify checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services` green
  (server 670 / 9 skipped, client-core 158, spark 175, starrocks 11, plugins green). Note: `./mvnw` got
  HTTP 429 from Maven Central for the Maven 3.10.0 download; `/opt/maven` 3.9.11 works.
- 2026-10-25 (cloud session): item 1b, Parquet read path group (commit 6314b72): server Parquet 1.13.1 →
  1.18.1 (`parquet.version`), which brings aircompressor 0.21 → 2.0.3 and snappy-java 1.1.8.3 → 1.1.10.8
  (zstd-jni 1.5.0-1 → 1.5.7-15); the server only reads footers/statistics with it, data pages stay DuckDB's.
  Spark keeps its provided Parquet 1.13.1; plugins unaffected (Iceberg already on aircompressor 2.0.3).
  `DependencyFloorTest.parquetCodecsAreAtTheFloor` (fails on the old pom). Full local `clean verify
  checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services` green (server 671 / 9 skipped,
  client-core 158, spark 175, starrocks 11, plugins green); push run 40148323187 green (6 min). Next: libthrift.
- 2026-10-25 (cloud session): item 1b, Hive plugin group (commit cf060fe): libthrift 0.9.3 → 0.24.0 for the
  metastore client (`TSocket` with a `TConfiguration` capping responses at 1 GiB like Hive 4 — libthrift's
  default 100 MB would refuse large partition lists; frames 16 MB as before; the read budget resets on every
  flush, so pooled connections do not run into it); `TFramedTransport` moved to `transport.layered`. Hive 3.1.3's
  generated classes and libfb303 0.9.3 run unchanged on it. The Hive 3.1.3 metastore server fails on libthrift
  0.14+ (`TFramedTransport$Factory` gone), so the test metastore now runs on 0.9.3 in a child-first class loader
  over target/fixture-libs in the plugin's tests too (`IsolatedHms`; the server's `HiveTestTables` already did):
  the tests are a new client against an old server. New tests in `HiveCatalogTest`: framed transport through a
  framing proxy (same snapshot as unframed), libthrift floor >= 0.24 and the 1 GiB cap, fixture on 0.9.3. Full
  local `clean verify checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services` green (server 671 / 9
  skipped, client-core 158, spark 175, starrocks 11, hive 11, plugins green); push run 40156159318 green.
  Next: the runtime libs set (1b's last group).
- 2026-10-25 (cloud session): item 1b, runtime libs set (commit 4eaab05). Finding: the shaded jar never carried
  Hadoop (`provided`, "comes from the deployment"), so `java -jar` (README Quick Start, START-NOTES) failed with
  `NoClassDefFoundError: org/apache/hadoop/conf/Configuration`; the stand ran on its stale libs dir. The build now
  copies the provided closure (mediated against the jar's libraries: 58 jars, 42 MB, Hadoop 3.5.0, Jetty 9.4.58,
  no ZooKeeper / Netty 3 / log4j / BouncyCastle / second Netty-Jackson-protobuf) to `flight-server/target/libs`;
  the `package` dispatch publishes `flight-server-libs-<version>-<sha>.tar.zst` (`libs/` + DEPENDENCIES.txt +
  SHA256SUMS). New `LibsSetTest` (floors, one version per jar, child-JVM start on server classpath + libs
  reading Parquet, and a start without libs that must fail). README / START-NOTES start with
  `java -cp '<jar>:libs/*'`; START-NOTES' layout also fixed (`../plugins` beside the jar). Found on the way: the
  bundled properties shadow `-Darrowflight.*` too (added to item 5), Hazelcast phone-home is on (Open questions).
  Full local `clean verify checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services` green (server 675
  / 9 skipped, client-core 158, spark 175, starrocks 11, hive 11, plugins green); push run 40163635118 cancelled by
  dispatch run 40163642740 (`package=true`) on the same commit: green, pre-release `server-1.0.0-4eaab052deaf`
  with the libs asset (38.9 MB). Next: item 2 (Iceberg equality deletes), unless the maintainer picks a stand
  finding (a)–(c) first.
- 2026-10-25 (cloud session): item 2, complete mode (commit 5466f86). Snapshots with equality deletes no longer go
  through iceberg_scan with every column read: `CatalogSources.deletesAppliedSource` reads the data files by field
  ID and applies the plan's delete files itself (ANTI JOIN per set of equality columns, IS NOT DISTINCT FROM; position
  deletes on `file_row_number` and the delete file's path normalised in SQL like `CatalogLocations.normalize`); data
  files with the same deletes share one read_parquet, a file with position deletes is its own UNION ALL branch.
  EXPLAIN on DuckDB 1.4.5: plain hash anti joins, the data scan projects only the used + equality columns and takes
  the query's filters. Deletion vectors, columns without field ID, dropped equality columns, a `file_row_number`
  column keep the old read. New `CatalogSourcesDeletesTest` (3000 rows, renamed column, pos deletes with `file:` /
  `file://` paths and rows of a file they do not apply to, equality deletes on 1 and 2 columns with nulls; answers
  computed in Java; fails with `=` instead of IS NOT DISTINCT FROM); `CatalogViewsIcebergSourceTest` updated. Local
  server tests green (681 / 9 skipped); push run 40170480666 green (incl. the Iceberg ITs on Iceberg-Java tables).
  Next: item 2 partial mode (see Next steps 2 (i)), then the stand timing (ii).
- 2026-10-25 (cloud session): item 2 (i), partial mode (commit f518695). Iceberg snapshots with position/equality
  deletes that `CatalogSources.deletesApplicable` covers (new; same checks `deletesAppliedSource` makes, they hold
  for every subset of files) are no longer answered completely in partial mode: `completeReason` lets them through,
  the plan spreads their files like any snapshot's (tickets unchanged: snapshot ID + location), and the executing
  node, which re-plans the pinned snapshot anyway, reads its ticket files with their delete files through
  `deletesAppliedSource` (`ExecutionService.catalogFilesSource`). Plans with delete files report records `-1`
  (recorded rows overcount). Deletion vectors, columns without field ID, dropped equality columns stay complete.
  Client-visible: such tables now give one endpoint per node in partial mode instead of one complete endpoint
  (same as every other catalog table; no ticket or header change). Tests: `CatalogSourcesDeletesTest` (each file
  alone with its own deletes adds up to the independent answer), `CatalogTablesTest` (reasons; plan in parts with
  unknown row count; DV still complete), `IcebergCatalogIntegrationTest` (Iceberg-Java table, hadoop + REST: located
  partial endpoint, a ticket per data file returns exactly that file's surviving rows, eq delete older than the
  second file leaves it alone; the iceberg_scan-workaround queries now also run partially). Local server verify:
  692 tests, only `LakeFsPluginTest` erred for the unbuilt lakeFS plugin in a single-module run (green after building
  it); Checkstyle/SpotBugs clean. CI: dispatch run 40178274322 green (the push had a [skip ci] head). Next: item 2 (ii)/(iii) are stand work; next code item
  is 3 (time travel; propose the form in Open questions first).
- 2026-10-25 (maintainer's local agent): single-node stand regression of release `server-1.0.0-4eaab052deaf`
  with the new libs set vs int7 — all checks pass, timings and memory equal (note under Next steps); new items 10
  (file:// catalog roots under an HDFS default FS, also in int7) and 11 (zookeeper / lz4-java absent from the set).
- 2026-10-25 (cloud session): item 3 needs the maintainer first: proposal for the time-travel form (SQL
  clause `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF`) added to Open questions, no code. Item 4 done (commit
  a4d0e63): named S3 stores `arrowflight.s3.stores` / `arrowflight.s3.<name>.root` with their own endpoint,
  region, URL style and credentials (`FLIGHT_S3_<NAME>_ACCESS_KEY`, ...; never the main ones; CA file, TLS
  check, metadata cache and listing settings default to the main store's, since DuckDB holds them per
  connection), own DuckDB secret `flight_s3_<name>` scoped to the store root; `StoreRegistry` now refuses a
  second store only for the same provider *and* configuration name (SPI `DataStore.configName()`); a broken
  store list stops the start. Tests: plugin `S3NamedStoresTest` (names, longest root, settings view) and
  server `S3NamedStoresTest` (real s3 + iceberg plugins: Iceberg warehouse in a second bucket next to an S3
  data directory → two scoped secrets, locate per store; refusal names the remedy; missing credentials;
  secret as property refused unprinted). Found on the way: `s3a://` catalog roots never match S3 stores
  (new item 12). Full local `clean verify checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services`
  green (server 687 / 9 skipped, client-core 158, spark 175, starrocks 11, hive 11, plugins green); push
  run 40186896468 green. Next: item 3 once the form is confirmed, else item 5 (config precedence).
- 2026-10-25 (cloud session): item 3 still waits on the maintainer (Open questions). Item 5 done (commit 1da3703):
  configuration precedence is now every `-D` name (short key and `arrowflight.*` alias) > environment >
  `etc/plugins/<ns>.properties` > `arrowflight.properties` > default. Before, the bundled file hid environment
  variables (`FLIGHT_RESULT_MODE`, `FLIGHT_ENDPOINT_BACKUP_LOCATIONS`, `FLIGHT_METRICS_ENABLED`, `DUCKDB_*`,
  `FLIGHT_S3_REGION` via `s3Region`, ...) and the file's short key beat a `-Darrowflight.*` alias. `ConfigSource`
  (`lookup`, new `firstSystemProperty` / `firstEnvironment` / `firstFileProperty`) carries the order;
  `ConfigAdapter` reads through it (`getConfig(ConfigSource)`), `PropertiesPluginConfig` puts the plugin file
  between environment and bundled file; SPI `PluginConfig` Javadoc, README, properties comments updated. Tests:
  `ConfigAdapterTest` (env beats bundled file, every -D beats env/file, blanks fall through),
  `PropertiesPluginConfigTest` and `DuckDbExtensionsTest` rewritten to the new order, `LibsSetTest` now starts
  the child server with `-Darrowflight.metrics.enabled=false`. Upgrade note in CHANGELOG and Open questions.
  Full local `clean verify checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services` green (server
  691 / 9 skipped, client-core 158, spark 175, starrocks 11, hive 11, plugins green); push run 40195045583
  green. Next: item 3 once the form is confirmed, else item 7 (Kerberos for the HMS client; 8/9 need the
  maintainer first) or items 10–12.
- 2026-10-25 (cloud session): item 11 done (commit c3b2760), taken before item 7 per the maintainer's note.
  Cause: Spark's test dependencies (zookeeper 3.6.3, org.lz4 1.8.0, curator 2.13.0) are nearer in the tree
  than Hadoop's, so ZooKeeper and lz4-java became test-only and left the libs set, and the shaded jar got
  curator-framework 2.13.0 next to curator-client 5.2.0. Fix: zookeeper 3.8.6 and at.yawk.lz4:lz4-java 1.10.4
  declared `provided` (Hadoop 3.5.0's versions, hadoop-project exclusions: no Netty, logback, slf4j, jline),
  Curator managed at 5.2.0, org.lz4 excluded from spark-core. Set: 62 jars. `LibsSetTest`: floors, one
  Curator version, link check of all hadoop-common / hdfs-client / hadoop-auth classes (left out: jsch for
  SFTP and ssh fencing, the protobuf-2.5 `ProtobufRpcEngine`), HA/Kerberos/ZK classes by name, `Lz4Codec`
  round trip; proven to fail with ZooKeeper and lz4-java removed from the set. Full local `clean verify
  checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services` green (server 694 / 9 skipped,
  client-core 158, spark 175); push run 40204556893 green; package dispatch run 40205062825 green: pre-release `server-1.0.0-c3b276054cc3` (jar, plugins, `flight-server-libs-1.0.0-c3b276054cc3.tar.zst`) for the stand. Next: item 3 once the form is confirmed, else
  item 7 (Kerberos for the HMS client) or items 10, 12.
