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

> **Stage 2, approved by the maintainer 2026-10-26 - take these in this order before the other open items** (basis: `docs/dev/TUNING.md`
> and the speed comparison in `docs/dev/UPSTREAM-SYNC.md`; each one: code + tests + CI green + a `package=true` pre-release when it
> changes runtime behaviour, so the coordinator can measure it on the stand):
> 1. item 14 - DuckDB 1.5.6 (hadoopfs for 1.5.6 already builds in the hadoopfs repo; keep answers identical, TPC-H checksums).
> 2. item 15 - per-query overhead (target <= 3 ms over plain DuckDB for a point query).
> 3. item 0g - S3 connections: reuse/pool connections so 32 clients x 8 threads do not exhaust local ports (TIME_WAIT).
> 4. item 17 (new) - S3 read path: a memory-bounded DuckDB external file cache (unbounded it was ~40x faster on S3 but OOM-killed
>    the server), parallel range reads / prefetch, metadata (footer, listing) cache. Goal: close the 2-3x gap to HDFS.
> 5. item 18 (new) - distributed complete mode: in a cluster the planning node computes a complete answer alone; let each node
>    compute its part (partial aggregates / filtered scans) in parallel and merge on the planning node, for queries where this is
>    provably correct; fall back to today's path otherwise. Opt-in setting first; must keep the complete-mode wire contract.
> 6. item 16 - single-stream scan opt-ins as defaults for plain scans (only if answers unchanged).
> 7. item 19 (new) - size-aware `duckDbThreads` default (cores/4, clamped 2..8); default change approved as part of stage 2.

0g. ~~**Bug (stand, 2026-10-26, build 57d252b1437f): S3 + `duckDbThreads=8`, 32 concurrent small queries fail with
    ephemeral-port exhaustion.**~~ — **cause found, workaround + startup WARN done 2026-10-26 (9f7eb4f, see Log); the
    server-side fix needs a decision (Open questions, "S3 connection churn").** DuckDB 1.4.5 httpfs keeps HTTP
    clients per open file handle only, and at 2+ threads a thread opens a file anew for each row group it takes, so
    about every range read opens a new connection (`http_keep_alive`, the curl client and the prefetch settings change
    nothing; measured 78 connections / 96 requests per query at 4-16 threads vs 32 at 1 thread, 16 files x 4 row
    groups). Only `duckDbExternalFileCache=true` (DuckDB default, ours off) stops it for repeated reads: a repeated
    query on the same connection opens 0 connections. **Stand:** rerun the 32-client small-query step at 8 threads
    with `--sysctl net.ipv4.tcp_tw_reuse=1` (expect 0 failures, check the startup WARN is gone), and once with
    `duckDbExternalFileCache=true` (count TIME_WAIT as before; note RSS). Original report: Every round (3/3): 32-64 ADBC and 0-1 JDBC queries of the 32-client small-query step fail
    "IO Error: Could not establish connection error for HTTP GET to '<s3 endpoint>/.../data_N.parquet'" (123-192 server
    log lines per round). Measured in the server's network namespace every 5 s: sockets in TIME_WAIT toward the S3
    endpoint reach 28231 = the whole `ip_local_port_range` (32768-60999) during that step; 4 threads peak at 20-22k
    (70-78 %, no failures yet), the SF10 JDBC step 18-24k at both. TIME_WAIT lasts 60 s, so the ~6 s retry of 0e cannot
    bridge it. Fix: reuse HTTP connections (httpfs keep-alive across queries / one connection cache for all DuckDB
    instances of a node, check `http_keep_alive` and whether each worker's instance opens its own), or fewer, larger
    range reads; test: count new connections per query at 8 threads x 32 clients against a local S3 (rclone/MinIO) and
    assert it stays bounded. Document the deployment workaround (`net.ipv4.tcp_tw_reuse=1`, wider port range).
    (MinIO's `MINIO_API_REQUESTS_MAX` not checked; the failures are TCP connect errors, not 503 throttling.)

0h. **Recheck (stand): 3-node heavy4 slower on 57d252b1437f?** 4 concurrent heavy JDBC queries (q01 q09 q18 q21, SF10,
    complete mode, 4 threads): slowest 5.2 s (rounds 4.9-5.3) vs 4.4 s (3.7-4.7) for 4eaab052deaf in the same session;
    single node unchanged (2.6 vs 2.6 s), cluster SF10 JDBC not slower (13.7 vs 15.3 s). Possibly the outbound budget
    (per-stream share = budget / open streams) or the planner change; rerun with more rounds before acting.

0f. ~~**Bug (stand, 2026-10-26): 3-node cluster, 8 concurrent partial-mode full scans -> streams CANCELLED and a node
    dies.**~~ — done 2026-10-26 (dd31117, see Log): all streams of a node share one outbound budget
    (`flightOutboundBudgetBytes`, default a quarter of the direct memory limit; each stream budget / open streams,
    at least 1 MiB). **Stand:** rerun the 8-client x 3-endpoint lineitem SF10 rounds (1 and 8 DuckDB threads) on
    a pre-release with this commit; check the startup line `Flight outbound budget: ...` and that no endpoint read
    fails and no node dies; note the timings against the single-server run. If a node still dies without a Java
    error, it is native memory (DuckDB `duckDbMemoryLimit` x live connections, see Open questions), not Flight.
    Original report: 3 servers (HDFS, `-Xmx2g`, `duckDbMemoryLimit=1GB`, 4 GB containers, server 4eaab052deaf), 8 clients each
    reading all 3 endpoints of lineitem SF10: 7-10 endpoint reads per round fail "CANCELLED: Server sendMessage()
    failed" in every round (1 and 8 DuckDB threads); with 8 threads the node serving the coordinator role was gone
    after the scan in 2 of 2 valid rounds (container removed, no exit info). A single server with the same clients has
    0 errors. Same mechanism as item 13b (256 MiB backpressure per stream exhausts direct memory): bound the per-stream
    and total outbound buffer (13b default 16 MiB and/or a global budget), fail a stream instead of the process, and
    add a test with many concurrent streams under a small `MaxDirectMemorySize`.
    **Correction (2026-10-26, stand):** the "node dies" part was a benchmark-harness artifact, not a server crash: the
    harness memory guard removes the server container when host MemAvailable falls below 4.5 GB, and this scan needs
    ~10-11 GB of host memory (3 servers + client); both removals fell inside the scan step. The CANCELLED errors were
    real. **Stand result on 57d252b1437f:** 8 clients x 3 endpoints, 0 failed endpoint reads in 6/6 rounds (4 and 8
    threads), all 480 M rows, no node lost, max node RSS 4.1 GB, startup line `Flight outbound budget: 536870912 bytes
    shared by all streams (per stream at most 268435456, at least 1048576)`; control 4eaab052deaf in the same session:
    5/7/10 failed reads per round (direct-memory OOM -> CANCELLED). Fixed. Timings in `docs/dev/TUNING.md`.

0e. ~~**Bug (stand, 2026-10-26): S3 + `duckDbThreads=8` -> every complete-mode query fails**~~ "Could not
    establish connection" — done 2026-10-26 (c71ee2e, see Log): DuckDB now retries S3 requests (connect errors
    included) for ~6 s instead of its own ~0.5 s; settings `arrowflight.s3.httpRetries` / `httpRetryWaitMs` /
    `httpRetryBackoff`. **Stand:** rerun SF10 JDBC on MinIO at `duckDbThreads=8` (and 4 as control, timings). If
    it still fails, the endpoint refuses for longer than 6 s: count client sockets in TIME_WAIT during the round
    (`ss -tan state time-wait | wc -l` vs `net.ipv4.ip_local_port_range`) and MinIO's
    `MINIO_API_REQUESTS_MAX` — httpfs opens a new connection for most range GETs at 8+ threads (measured: 144
    connections / 352 requests at 8 threads, 322 at 32, 32 at 1), so ephemeral-port exhaustion is the likely
    limit; then raise `httpRetries` or report the numbers. Also recheck the S3-outage time (3-node stand (a)):
    a read now retries ~6 s before failing.
    **Stand result on 57d252b1437f (2026-10-26):** SF10 JDBC on MinIO at 8 threads 22/22 in 3/3 rounds (24.2 s; 4 threads
    33.4 s), all raw scans complete -> heavy path fixed. The 32-client small-query step still fails at 8 threads:
    ephemeral-port exhaustion, measured -> new item 0g.

0d. ~~**Intermittent `GetFlightInfo` TIMED_OUT with a cluster member down (StarRocks connector CI)**~~ — done
    2026-10-26 (8e755fb, see Log): the planner's node-load read (and, on shared storage, the inventory read) was a
    Hazelcast whole-map query (`entrySet`), which waits ~10 s after a member left whose address stops answering
    (container gone); now key lookups of the current members. A stopped server leaves Hazelcast gracefully
    (Hazelcast's terminating shutdown hook off, graceful wait max 5 s). Left: a *crashed* node (kill -9, host gone)
    still blocks every map operation on its partitions until Hazelcast drops the member (10 s, see Open questions).
    StarRocks repo: rerun marten m4 check 6 on the next pre-release before moving its default off 20b887839edf.

0c. ~~**Maintainer decision 2026-10-26: make `duckDbThreads` default 4**~~ — done 2026-10-26 (60e888b, CI run
    40245198713 green, pre-release `server-1.0.0-60e888bc26ec`). Items 13b-d stay as proposals (behaviour/default
    changes there still need approval).

0b. ~~**Bug: `duckDbStatementCacheSize=0` breaks parameterized prepared statements**~~ — done 2026-10-25 (see Log):
    `streamComplete` streams its bound statement directly; the parse check in `streamSql` used to evict and close it
    from a one-statement cache (0 is clamped to 1). Stand: rerun the perf screening's cache-size-0 case (JDBC + ADBC).

> **Maintainer, 2026-10-25:** the production Hadoop cluster has HA NameNode with ZooKeeper (automatic failover),
> Kerberos and Ranger. Versions (2026-10-25): Hadoop 3.1.3 (vendor build) on the cluster side; HA with 2 NameNodes,
> automatic failover, ConfiguredFailoverProxyProvider, RPC 8020; ZooKeeper 3.9.2 ensemble, SASL/Kerberos; Ranger 2.4.0
> (HDFS authorizer on the NameNodes; plugins also for Hive). Our client libs must work against a Hadoop 3.1 cluster
> (RPC/SASL compatibility of the newer hadoop-client), and Ranger checks must target the 2.4 REST API. Consequences: item 11 (ZooKeeper and lz4-java missing from the new
> runtime libs set) is now required — put the ZooKeeper client (and its Curator/Jute deps as Hadoop needs them) and
> lz4-java back into the libs set, with a startup/link check that the HA/ZK and Kerberos code paths load. Keep items
> 7 (Kerberos for HMS and HDFS), 8 (Ranger subquery bypass) and 9 (per-user identity) — they are needed for production.

0a. ~~**hadoopfs timeout defaults 3 s / 10 s**~~ — done 2026-10-24 (732e238, CI run 40140133221 green).

> **Stand, 3-node S3 cluster on int7 (2026-10-25)** (unnumbered; sessions keep taking the numbered items, see Open questions): all correct (JDBC 22/22, Trino 22/22, B2, B5, mid-stream kill, ticket matrix, MinIO outage + recovery without restart). Server follow-ups, none give wrong results: (a) during an S3 outage each read needs 20–40 s to fail (DuckDB httpfs retries + DNS); make S3 retry/timeout settings explicit and shorter when the endpoint is unreachable; (b) ~~plans that are never read keep their node load reservation up to 10 min~~ — partly fixed 2026-10-26 (9168a44, see Log): `CancelFlightInfo` / `CancelQuery` release a plan's reservations at once; clients that abandon a plan without cancelling still hold them 10 min (shorter TTL = default change, Open questions). Original: skewing later plans (one node got no endpoints ~5 min) — release reservations on ticket expiry/cancel or shorten the TTL; (c) ~~after B5 the first plan on S3 waits 9–10 s while dead members are dropped (HDFS cluster: ~1 s) — find why~~ — cause found 2026-10-27 (9934d3b, see Log): a crashed node whose *address* vanished (container removed) holds every map operation on its partitions until `hazelcast.max.no.heartbeat.seconds` (10 s); one whose port still refuses is dropped in ~0.35 s — so the two stands most likely differed in how the nodes died. Same mechanism as the 0d crash Open question (faster detection = maintainer's call); `-D` override documented in START-NOTES. **Stand:** note how B5 kills the nodes on both stands (process vs container/host); optionally rerun B5 on S3 with `-Dhazelcast.max.no.heartbeat.seconds=5`.

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

> **Stand, secure HA HDFS (production profile), build 4eaab052deaf (2026-10-25)** (unnumbered): on-demand stand with an
> MIT Kerberos KDC, ZooKeeper 3.9.2 x3 (SASL client auth required + quorum SASL), Hadoop 3.1.3 with 2 NameNodes + ZKFC
> (automatic failover, ConfiguredFailoverProxyProvider, RPC 8020), 3 JournalNodes, 3 DataNodes (Kerberos, block access
> tokens, HTTPS_ONLY), Ranger Admin 2.4.0 (PostgreSQL) with the Ranger HDFS plugin on both NameNodes (policy download via
> SPNEGO; test policies: one user reads `tpch` only, another nothing, the server principal reads all). Data SF1 tpch +
> types. Results:
> - **Works:** Kerberos Flight authentication (`FLIGHT_KERBEROS_*` + TLS) with a real KDC: keytab login, SPNEGO
>   `Negotiate` handshake from a Java client, bearer session; the Java Hadoop client of the new libs set (3.5.0) against
>   the 3.1.3 cluster: Kerberos RPC, HA nameservice and failover — no ZooKeeper class is needed on this client path
>   (item 11 is still right for the elector/ZK code paths, not hit here). Queries with hadoopfs (libhdfs3, Kerberos from
>   a ticket cache) read correctly once DataNodes do not require SASL on data transfer (secure DataNodes on privileged
>   ports). Ranger: disabling the server principal's policy makes queries fail with HDFS `Permission denied` within the
>   10 s policy poll; enabling it again restores them.
> - **(a) hadoopfs cannot read from DataNodes with SASL data transfer protection** (`dfs.data.transfer.protection`, the
>   usual Hadoop 3 secure setup without privileged ports) nor with `dfs.encrypt.data.transfer=true`: NameNode RPC works,
>   every block read fails (`Read 8 bytes failed from <dn>:9866`; DataNode: `InvalidMagicNumberException`, the client never
>   sends the SASL handshake). The Java client reads fine. Production must use privileged-port DataNodes, or the
>   extension needs the SASL data-transfer handshake (libhdfs3) — check which one production uses.
> - ~~**(b) hadoopfs needs a ticket cache:**~~ done 2026-10-26 (2b2cac8, see Log): optional `hdfsKerberosPrincipal` +
>   `hdfsKerberosKeytab` make the server write and renew the MIT ticket cache the extension reads (default: LIBHDFS3_CONF's
>   `hadoop.security.kerberos.ticket.cache.path`, else `KRB5CCNAME`, else `/tmp/krb5cc_<uid>`). **Stand:** on the secure
>   HA stand drop the external `kinit`, set both settings (server keytab), check the startup line
>   `hdfs=kerberosTicketCache ... ticketEnd=...` and no cache WARN, run queries; with a short `ticket_lifetime` (e.g.
>   10 min) check that queries keep working past the first lifetime (a renewal line every ~8 min). Original: with only the server keytab (no `KRB5CCNAME` cache) the Java side starts and
>   lists, every query fails `IO Error: Unable to connect to HDFS: hdfs://<ns>`. The server does not hand its keytab
>   login to the extension; today an external `kinit -kt` (+ renewal before expiry) is required. Options: the server
>   writes and renews a private ccache from its keytab, or the extension takes keytab + principal settings.
> - ~~**(c) Short-circuit is forced on:**~~ fixed 2026-10-26 (see Log): the deployment's value is kept, otherwise on only with a `dfs.domain.socket.path`. Original: `HadoopDataStoreProvider.open` sets `dfs.client.read.shortcircuit=true`; a client
>   config without `dfs.domain.socket.path` (no local DataNode) stops the server at start (`The short-circuit local reads
>   feature is enabled but dfs.domain.socket.path is not set`). Keep the deployment's value / make it a setting.
> - **(d) NameNode host crash with libhdfs3 defaults:** partly done 2026-10-26 (8e0307f, see Log): the hadoopfs
>   extension (afc3) has no RPC timeout setting, so the server cannot set them in DuckDB; it now WARNs at startup on HDFS
>   for each of the four `rpc.client.*` timeouts LIBHDFS3_CONF leaves unset or over 60 s, and the values below are
>   documented (`arrowflight.properties`, START-NOTES). **Stand:** put them into the stand's/production LIBHDFS3_CONF.
>   Making the server supply them needs extension settings (`hdfs_rpc_*`, hadoopfs repo) — ask if wanted. Original: killing the active NameNode *process* costs the running query
>   ~10 s (60/60 OK). Killing the whole active NameNode *host* (its address disappears; new active after ~27 s): one
>   query hung 170 s and failed (`Failed to invoke RPC call "getFileInfo" on server <old active>:8020 ... Connection reset
>   by peer`), later queries OK. With `rpc.client.connect.timeout=5000`, `rpc.client.read.timeout=15000`,
>   `rpc.client.write.timeout=15000`, `rpc.client.timeout=20000`, `rpc.client.connect.retry=2` in the libhdfs3 conf: 80/80
>   OK, the slowest query 32.6 s (= the failover itself). Consider shipping these as defaults next to the DataNode
>   timeouts (item 0a) or documenting them.
> - ~~**(e)**~~ fixed 2026-10-26 (see Log): stack frames are dropped from every client error text. Original: HDFS errors reach the Flight client with the NameNode's Java stack trace in the message (e.g. the
>   `Permission denied` above, `FSPermissionChecker...`); trim to the first line.
> - As expected without item 9, every authenticated user reads what the server principal may read (Ranger sees only
>   the server principal). Kerberized HMS on this stand: prepared (principal + keytab, auth_to_local, proxyuser), not run.

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
3. ~~**Iceberg time travel**~~ — done 2026-10-25 (see Log): `FOR VERSION AS OF <id | 'tag'>` / `FOR TIMESTAMP AS OF
   TIMESTAMP '<time> <zone>'` after an iceberg-hadoop / iceberg-rest table name, complete mode (rewritten to a read of
   the snapshot's own files, deletes and columns); partial mode answers such queries completely. Follow-ups (optional):
   (a) partial mode in parts (plan the travelled snapshot's files; the executor already pins any snapshot ID via
   `pinned`, but describe and the `fileUris == null` path read `current`); (b) Iceberg tables registered in a Hive
   Metastore (`TableFormat` needs an `asOf`); (c) snapshots that cannot be read file by file (deletion vectors,
   renamed nested fields) are refused — `iceberg_scan(..., snapshot_from_id => )` could read them if DuckDB 1.4.5 picks
   the snapshot's schema (check first). For the coordinator: Trino plugin / StarRocks connector may forward the clause.
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
7. ~~**Kerberos for the HMS client**~~ — done 2026-10-25 (see Log): `arrowflight.hive.authentication=kerberos`,
   SASL/GSSAPI as Hive's client (`metastorePrincipal` with `_HOST`, `saslQop` default
   `auth-conf,auth-int,auth`), login from `clientPrincipal` + `clientKeytab` or the ticket cache
   (`KRB5CCNAME` / `clientTicketCache`), renewed at 80 % of the TGT lifetime; framed + Kerberos refused.
   Tested with an in-process Kerby KDC and a SASL front for the test metastore (no kerberized real HMS in CI).
   Stand: the prepared kerberized HMS (principal, keytab, auth_to_local, proxyuser) — reference queries with
   `authentication=kerberos`, once from the keytab and once from a `kinit` cache, with the metastore's
   `hadoop.rpc.protection` = authentication and = privacy. HDFS Kerberos with hadoopfs: see the secure HA
   stand findings (a) above (data-transfer SASL) — still open, not numbered; (b) (ticket cache) done 2026-10-26.
8. **Ranger hardening** (security): details are kept outside this public repo; ask the maintainer before starting. (The maintainer wants Ranger/
   Kerberos finished before production; implement here, the stand check comes later.)
9. **Per-user identity** (needed for Ranger/Kerberos): JWT/bearer pass-through of the end user to Ranger checks;
   design in Open questions first. **Design written 2026-10-26** (Open questions, "Item 9"): phase A trusted-proxy
   header, phase B JWT provider, phase C storage-level identity; waits on the maintainer's answers.
10. ~~**Local catalog roots under an HDFS `fs.defaultFS`**~~ — done 2026-10-25 (see Log): a catalog location
   without a scheme (`/wh`, `file:/wh`, `file:///wh` all normalise to `/wh`) now opens a local store
   (`file:/wh`) instead of resolving against `fs.defaultFS`; the data directory as a plain path still follows
   `fs.defaultFS`. Stand: the int7 case (`-Darrowflight.iceberg-hadoop.warehouse=file:///wh` with the HDFS
   client config on the class path) must start and answer.
11. ~~**Runtime libs set gaps**~~ — done 2026-10-25 (see Log): the set now carries ZooKeeper 3.8.6 (+ jute,
   audience-annotations) and lz4-java (`at.yawk.lz4` 1.10.4), Curator is 5.2.0 in jar and set alike (the jar had
   curator-framework 2.13.0 with curator-client 5.2.0). `LibsSetTest` links every class of hadoop-common,
   hadoop-hdfs-client and hadoop-auth on the thin class path. Stand: replace the libs dir with release `server-1.0.0-c3b276054cc3`'s
   `flight-server-libs` asset and run one HDFS query against the HA nameservice; a real Kerberos login (KDC)
   is still item 7. Note: lz4-java 1.12.1 exists (we follow Hadoop 3.5.0's 1.10.4).

12. ~~**`s3a://` / `s3n://` catalog roots are not matched by the open S3 stores**~~ — done 2026-10-26 (514782d,
   see Log): `CatalogLocations.normalize` (server) and `Locations.normalize` (iceberg plugin) map s3a/s3n to
   s3, so store lookup (`holder`, both `locate`s), the roots checks, `read_parquet` lists and the position
   delete `file_path` match treat the three forms as one. Stand: one Iceberg table written by Spark through S3A
   (`s3a://` paths in its metadata) under an `s3://` or `s3a://` warehouse, complete and partial mode.

13. **Tuning defaults from the perf campaign** (stand + CI screening 2026-10-25/26, `docs/dev/TUNING.md`):
    (a) ~~`duckDbThreads` default 2 -> 4~~ done as item 0c (plain 4) (SF10 JDBC 1.8x, SF1 1.3x faster; single point lookup +14 %, 32-client p95
    unchanged), or a size-aware default (cores / 4, between 2 and 8); (b) `flightBackpressureThresholdBytes` default
    256 MiB -> 16 MiB (no measured loss; with `endpointsPerServer` 4/8 the 256 MiB per stream exhausts
    `MaxDirectMemorySize` and streams are CANCELLED); (c) `duckDbExternalFileCache=true` has no memory bound and gets the
    server OOM-killed on SF10 (HDFS and S3) - bound it or document it as unsafe; (d) ~~ADBC Go driver connects rarely fail
    with "failed to update autocommit: rpc error: code = Unimplemented"~~ — fixed 2026-10-26 (4939fb7, see Log):
    the call is BeginTransaction (ADBC sends it only when it decoded SqlInfo TRANSACTION as supported); concurrent
    DoGet streams corrupted each other's empty buffers through Arrow's shared empty buffer (`EmptyListOffsets`).
    **Stand:** rerun the 8-client ADBC connect rounds (45 runs); expect no autocommit error and no
    `beginTransaction refused` WARN line in the server log; if one appears, note the ADBC version. Connector side (forward to the Trino
    connector repo): Trino q18 fails "Java heap space" (6 GB heap) as soon as `endpointsPerServer` > 1.

14. **DuckDB 1.5.6** (upstream speed comparison 2026-10-27, `docs/dev/UPSTREAM-SYNC.md` "Speed comparison 2026-10-26"): both
    upstream builds bundle DuckDB 1.5.6; on the same HDFS files, without a Flight server, 1.5.6 answers a point lookup in
    5.6 ms vs 10.6 ms and a filtered count in 9.6 vs 15.8 ms (1.4.5). The hadoopfs extension from our patched sources builds and
    loads for v1.5.6 (footer `51b7220-afc3` / `v1.5.6`). Upgrade `duckdb_jdbc` to 1.5.6.0 and the CI extension set (httpfs, iceberg,
    avro, hadoopfs), run the full suite, then the stand rerun (P4 small + TPC-H SF1/SF10 + Trino). Expected: 5-6 ms less per
    small query (25-45 % of the small-query p50).
15. **Per-query overhead** (same comparison): a point query costs ~15-20 ms over Flight vs ~6-11 ms in plain DuckDB; upstream
    (even its fork-point `main`, which keeps per-connection instances like ours) answers point/count queries 35-50 % faster
    and TPC-H SF1 via Trino 17 % faster. Profile GetFlightInfo -> first batch for a point query (per-connection catalog view
    refresh, inventory planning, load accounting) and cut what is not needed for a single-node plain query; target <= +3 ms
    over raw DuckDB. Regression test with a latency budget is not reliable in CI: measure on the stand.
16. **Single-stream scan throughput** (same comparison): upstream native (defaults `preserve_insertion_order=false`,
    `streaming_buffer_size=64MiB`, DuckDB 1.5.6) streams one full `lineitem` scan at 1510 MB/s with 8 threads vs ~950 MB/s
    for ours at any thread count (4 and 8 concurrent scans: equal). Measure our opt-ins `duckDbPreserveInsertionOrder=false`
    + `duckDbStreamingBufferSize=64MiB` on the stand (scan x1, Trino splits) before and after item 14; if answers are unchanged
    and x1 improves, propose them as defaults for plain scans without ORDER BY (default change -> Open questions first).

## Open questions

- **S3 connection churn (item 0g, 2026-10-26).** DuckDB's httpfs opens a new TCP connection for about every S3
  range read at 2+ threads (per-file-handle client cache, a new handle per row group and thread); each closed one
  holds a local port 60 s. Today: startup WARN + documented host workaround (`tcp_tw_reuse=1`, wider port range).
  Options needing your call: (1) turn `duckDbExternalFileCache` on by default for S3 data stores only (repeated
  reads need no connection; costs up to `duckDbMemoryLimit` of cache per live connection — the reason it is off);
  (2) an in-process HTTP forward proxy with pooled keep-alive connections for DuckDB's S3 reads (`http_proxy`,
  plain-HTTP endpoints only; HTTPS would tunnel 1:1 and gain nothing; touches the lockdown); (3) a pooled client
  in httpfs itself (upstream DuckDB, or our own build like hadoopfs). Recommendation: decide after the stand rerun
  with `tcp_tw_reuse=1`; if that is clean, keep (0) and document it as a deployment requirement.

- **(2026-10-26, item 0d follow-up) Faster detection of a crashed member?** With kill -9 / host gone, every
  Hazelcast map operation touching the dead member's partitions (heartbeat puts, getAll, ticket reservations)
  waits until the member is dropped: `hazelcast.max.no.heartbeat.seconds=10` (our default). Measured in one JVM
  with an iptables DROP: first plan 10.4 s, then ~10 ms. Side effect seen: a survivor's own heartbeat put can block
  as long, so other nodes briefly drop that live node as stale (back within 3 s; no wrong answers). Options: lower
  it to 5 s (risk: members dropped on GC pauses / busy runners), or plan from a local cluster snapshot refreshed by
  the heartbeat thread. Both change existing behaviour — maintainer's call. (2026-10-27: this is also 3-node S3 finding (c); only a
  vanished address waits — a refused port is detected in < 1 s. A deployment can already set
  `-Dhazelcast.max.no.heartbeat.seconds` on every node, now documented in START-NOTES.)

- **Item 9 (per-user identity): design proposal 2026-10-26 — please answer the questions at the end.** Today the
  embedded Ranger gate (`FLIGHT_AUTHORIZATION_PROVIDER=ranger`, service type `arrowflight`, schema/table policies)
  already decides per Flight peer identity on every call that names data (GetFlightInfo, DoGet re-check of the
  ticket's query, catalog listings, `arrowflight.table_statistics`). The gap: when the Flight client is a shared
  service (Trino coordinator + workers, StarRocks BEs, a gateway) the peer identity is that service's principal,
  so every end user gets the service's rights; and storage (HDFS, HMS) is always read as the server principal, so
  the cluster's own Ranger HDFS/Hive policies see only the server. Proposal in three phases, each opt-in and off
  by default (no change for existing deployments):
  - **A. Trusted-proxy impersonation (server only).** New settings `FLIGHT_IMPERSONATION_PROXIES=<principal or
    short name>,...` and optional `FLIGHT_IMPERSONATION_USERS` / `FLIGHT_IMPERSONATION_GROUPS` (allow lists, same
    idea as Hadoop's `hadoop.proxyuser.<svc>.users/groups`). A call may carry `x-flight-effective-user: <name>`;
    when the authenticated peer is a listed proxy and the target user/group is allowed, the effective user becomes
    the call's identity — implemented as a wrapper around the configured `CallHeaderAuthenticator`, so every
    existing `peerIdentity()` check uses it without further changes. The header from a non-proxy, or for a user
    outside the allow list, is rejected (`PERMISSION_DENIED`), never silently ignored. Ranger audit records the
    effective user and names the proxy in the request data; the server log line too. Partial mode: DoGet already
    re-checks with the caller's identity, so the connector's workers must send the same header on each endpoint
    call (connector work, for the coordinator); tickets stay unchanged (alternative: bind the effective user into
    the signed ticket — ticket format change, not proposed). Requires an identity-establishing provider (Kerberos
    or B); the static bearer token (identity `token`) cannot be a proxy, `FlightSecurity` already enforces that.
  - **B. JWT authentication provider** `FLIGHT_AUTH_PROVIDER=jwt` (TLS required): verifies signed JWTs (JWKS URL
    or public key file, required issuer and audience, clock skew limit, configurable user claim — `sub` or
    `preferred_username`); identity = that claim. Fits clients where users log in through OAuth2/OIDC and the
    client forwards the user's token (Trino with OAuth2 can pass it through); then no impersonation is needed.
    Library: nimbus-jose-jwt (already in the dependency tree through hadoop-auth) as a direct dependency at a
    current version. Session tokens of the Kerberos provider are unaffected.
  - **C. Storage-level identity (bigger, separate decision).** Read HDFS and query HMS as the effective user via
    Hadoop proxy users: the Java side (listing, footers, statistics) under `UserGroupInformation.createProxyUser
    (user, login).doAs`, the HMS client with `set_ugi` / SASL proxy; needs `hadoop.proxyuser.<server>.hosts/users`
    on the cluster. The hard part is DuckDB's hadoopfs reads: one DuckDB instance per worker thread with one
    libhdfs3 connection cache, so per-user reads need per-user connections (or an effective-user setting per
    query) — whether libhdfs3 supports a proxy user over Kerberos is unverified; plus every server cache (listing,
    footer, statistics, external file cache) must be keyed by user or filled only after the user's own check.
    Only worth it if the cluster's Ranger HDFS/Hive policies must be the single source of truth.
  Questions: (1) Which clients reach production and how do their users authenticate to them (Trino with
  Kerberos/LDAP/OAuth2, StarRocks, BI tools)? That picks A, B or both. (2) Is the embedded `arrowflight` Ranger
  service (schema/table policies on the server) enough, or must the cluster's HDFS/Hive Ranger policies decide per
  user (phase C)? (3) Header name `x-flight-effective-user` and reject-on-misuse OK? Proposal: implement A first
  (one session, plus connector header forwarding), B if (1) shows OAuth2/OIDC, C only on explicit request.

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
- **(2026-10-26, 3-node S3 finding (b)) Reservation TTL of unread endpoints.** `CancelFlightInfo` now releases a
  plan's node-load reservations at once (additive). A client that drops a plan without cancelling still holds
  them 10 min (tickets carry no expiry). Options: (1) connectors (Trino plugin, StarRocks connector) send
  `CancelFlightInfo` when a query is abandoned / finished early (LIMIT) — for the coordinator; (2) a shorter
  reservation TTL (e.g. 2 min, or a new setting with the 10 min default kept); (3) both. Proposal: (1) plus an
  opt-in setting. Which?
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
- 2026-10-25 (maintainer's local agent, stand): secure HA HDFS stand (KDC, ZooKeeper 3.9.2 SASL, Hadoop 3.1.3 HA + ZKFC,
  Ranger 2.4.0 HDFS plugin) and first smoke of 4eaab052deaf on it — see the note "Stand, secure HA HDFS" under Next steps
  (hadoopfs: no SASL data transfer, needs a ticket cache; forced short-circuit; libhdfs3 RPC timeouts for NameNode host loss).
- 2026-10-25 (cloud session): item 3 done (commit f652985), as approved: Iceberg time travel with `FOR VERSION AS
  OF <snapshot id | 'tag/branch'>` and `FOR TIMESTAMP AS OF TIMESTAMP '<time> <zone>'` (zone required; latest
  snapshot-log entry at or before). `TimeTravelClause` finds the clause (skips literals, quoted identifiers,
  comments, dollar quotes) and `CatalogViews.timeTravel` replaces each reference in complete mode with a read of
  that snapshot's own files, deletes and columns (never `iceberg_scan`, which reads the metadata file's current
  snapshot; snapshots not readable file by file are refused); the table name stays the alias unless the client
  gave one. Partial mode answers such queries completely (no ticket or header change). Errors: unknown/expired
  snapshot, unknown ref, time before the first commit, missing zone → INVALID_ARGUMENT; missing table →
  NOT_FOUND; data-directory and Hive tables refuse. Access checks, `readsOnlyCatalogTables` see the SQL with the
  clauses removed. SPI 1.2: `AsOf` + `Catalog.asOf` (default refused), iceberg plugin `spi=1.2` (needs a server
  of this build). Tests: `TimeTravelClauseTest`, `FlightAuthorizationTimeTravelTest`, plugin
  `IcebergTimeTravelTest` (id, tag, branch, instants, schema of the old snapshot, metadata cached before a
  commit), `IcebergCatalogIntegrationTest.timeTravelReadsTheFirstSnapshotWithItsColumns` (hadoop + REST,
  complete and partial, joins old vs current, deletes snapshot, refusals; runs in CI only: the ci-assets
  download is refused from the cloud sandbox). Local verify: server 699 tests, the last failure fixed and the
  touched tests rerun; push run 40214412156 green. Next: item 7 (Kerberos for the HMS client), or 10 / 12;
  item 3 follow-ups (a)-(c) are optional.
- 2026-10-25 (cloud): **item 0b done** (cd389ff). Root cause: `streamComplete` bound the parameter values to the
  cached statement, then called `streamSql`, whose parse check took `PARSE_CHECK_SQL` from the per-thread LRU; with
  `duckDbStatementCacheSize` 0 or 1 (clamped to 1) that closed the bound statement and the query text was prepared
  again without values → "Parameter count mismatch". Now `streamSql` = parse check + `streamPrepared`, and
  `streamComplete` calls `streamPrepared` with its bound statement (also saves a second parse check per
  complete query). Regression test `DuckDbAdapterTest.streamCompleteBindsParametersWithAnyStatementCacheSize`
  (sizes 0, 1, 2, 64; parameterized, plain, parameterized again, wrong value count) failed before the fix with the
  reported error. Local: DuckDbAdapterTest 87/87, checkstyle + spotbugs clean; push run 40222858195 green.
  Next: item 7 (Kerberos for the HMS client), or 10 / 12.
- 2026-10-25 (cloud): **item 7 done** (d326dd6): Kerberos (SASL/GSSAPI) for the Hive Metastore client, new
  `KerberosLogin` (JAAS Krb5LoginModule, keytab or ticket cache, renewal) and settings `metastorePrincipal`,
  `clientPrincipal`, `clientKeytab`, `clientTicketCache`, `saslQop` (additive; `authentication=none` unchanged).
  `KerberosMetastoreTest`: Kerby 2.0.3 KDC in the JVM (test scope), a `TSaslServerTransport` front as
  `hive/localhost` forwarding to the real Hive 3.1.3 test metastore; keytab login at auth-conf, ticket cache at
  auth / auth-int (catalog answers = plain metastore), unknown service principal, no common protection layer,
  missing cache, wrong keytab principal, plain client against SASL. Local: hive module 16/16 + checkstyle +
  spotbugs clean; push run 40232501858 green. Next: item 10 or 12 (8 needs the maintainer first, 9 design first).
- 2026-10-25 (cloud): **item 10 done** (67d809c): `StoreRegistry.CatalogStores.locate` opens a catalog location
  without a scheme as `file:<path>` (normalised `/wh`, `file:/wh`, `file:///wh`), so a local warehouse no longer
  resolves against a non-local `fs.defaultFS`; the data directory as a plain path is unchanged (follows
  `fs.defaultFS`). Regression test `CatalogRegistryTest.localWarehouseUnderANonLocalDefaultFileSystem` (viewfs
  default FS standing in for HDFS; four warehouse spellings; table files located in a local store) failed before the
  fix with the stand's error. Local: CatalogRegistryTest + StoreRegistryTest, checkstyle + spotbugs clean (local
  Maven: the wrapper's 3.10.0 download fails, `/opt/maven/bin/mvn` works); push run 40241864311 green.
  Next: item 12 (s3a/s3n catalog roots); 8 needs the maintainer first, 9 design first.
- 2026-10-26 (local perf campaign): `docs/dev/TUNING.md` (stand single node, HDFS, build 4eaab052deaf; CI screening on
  4eaab052deaf and c3b276054cc3); new item 13 (default changes). Storage HDFS vs S3 and 3-node runs with the best
  settings not measured yet (stand not available for valid measurements).
- 2026-10-26 (cloud session): item 0c done (60e888b): `duckDbThreads` default 4 in code (was 1) and in the bundled
  `arrowflight.properties` (was 2); unit test `ConfigAdapterTest.duckDbThreadsDefaultsToFourWithoutAnySetting`;
  CHANGELOG, TUNING.md, START-NOTES, user guides. Package dispatch run 40245198713 green (build + package; the push
  run 40245189332 was cancelled in its favour): pre-release `server-1.0.0-60e888bc26ec`. Stand: rerun with the new
  default where it ran 2. Next: item 1b (dependency security upgrades).
- 2026-10-26 (cloud session): item 12 done (514782d): `s3a://` / `s3n://` catalog locations are matched as
  `s3://` — `CatalogLocations.normalize`, the SQL `normalizedLocation` for position delete paths,
  `StoreRegistry.locate` and the iceberg plugin's `Locations.normalize` (an s3a warehouse used to stop the
  start; s3a files under an s3 root were hidden). Regression tests in `CatalogTablesTest`,
  `CatalogSourcesDeletesTest`, `S3NamedStoresTest`, `PluginPartsTest`. CI run 40251162808 green. Next
  numbered items: 8 (ask the maintainer first), 9 (design in Open questions first), 13 (proposals awaiting
  approval); stand checks listed per item.
- 2026-10-26 (cloud session): numbered items left are stand checks, 8 (ask first), 9 (design first) and 13
  (proposals), so: item 9 design written to Open questions (phase A trusted-proxy header
  `x-flight-effective-user`, B JWT provider, C storage-level identity; questions for the maintainer). Fixed
  secure-HA stand finding (c) (6b20c3a): `HadoopDataStoreProvider` keeps the deployment's
  `dfs.client.read.shortcircuit`, otherwise turns it on only with a `dfs.domain.socket.path` (was forced on and stopped
  the start without a socket); `HadoopDataStoreProviderTest`. CI run 40260783151 green. Stand: int7 HDFS with local
  DataNodes must still read short-circuit (socket path set). Next: answers to item 9 / 13, or the other unnumbered
  stand findings (secure HA (d)/(e), 3-node S3 (a)-(c)).
- 2026-10-26 (cloud session): item 0d done (8e755fb). Reproduced in one JVM (3 Hazelcast members, iptables DROP on
  the leaving member's port to mimic a gone container): after a member left, even gracefully, `entrySet` on the
  survivor took 10.3 s while key ops took ms; `ClusterService.allServerLoads` (every plan) used it. Now
  `getAll(memberUris)` (entrySet only when members carry no URI), and `fileLocations(true)` reads members'
  inventories by key on shared storage (local disks keep every inventory so a lost node's files fail the plan).
  Measured after the fix: 11 ms. Hazelcast's shutdown hook (TERMINATE = looks like a crash) disabled; the server's
  hook leaves gracefully, `hazelcast.graceful.shutdown.max.wait` 5 s (was 600 s: a graceful leave against an
  unreachable peer hung 363 s). Tests: `ClusterServiceTest` (key reads, inventories kept on local disks),
  `ClusterMemberLossIntegrationTest.survivorPlansAtOnceAfterAMemberLeft`, `HazelcastAdapterTest`. Crash case
  (10 s) → Open questions. CI run 40272329184 green. Next: answers to the Open questions (0d crash case, 9, 13) or the unnumbered stand findings.
- 2026-10-26 (cloud session): item 0e done (c71ee2e + golden 8b4ffe7). Measured with DuckDB 1.4.5 + the ci-assets
  httpfs against a local S3 stand-in (16 Parquet files, 352 range requests per query): httpfs opened 32 TCP
  connections at 1 thread, 144 at 8, 322 at 32 (keep-alive reuse collapses with threads), and against a
  refusing port its default retries (3 from 100 ms, backoff 4) gave up after 0.5 s; with 6 retries from 200 ms,
  backoff 2 a query rode out a 3 s refusal. The s3 plugin now sets `http_retries` / `http_retry_wait_ms` /
  `http_retry_backoff` (new settings `httpRetries` 6, `httpRetryWaitMs` 200, `httpRetryBackoff` 2, ~6 s in
  all; inherited by named stores; validator allowlist extended). Tests: `S3SettingsTest`, `S3DataStoreTest`,
  `S3NamedStoresTest`, `S3PluginTest`, golden `duckdb-setup.txt`, and
  `S3IntegrationTest.connectRefusalsAreRetried` (forwarder refuses connections 1.5 s during an 8-thread read;
  retries 0 fails as control). CI run 40282571105 red (golden file not updated), 40283144913 on 8b4ffe7 green.
  Root cause on the stand not confirmed (likely client ephemeral ports / endpoint limits from the connection
  churn) — stand check listed in item 0e. Next: stand rerun of 0e; answers to the Open questions (0d crash case,
  9, 13) or the unnumbered stand findings.
- 2026-10-26 (cloud session): item 0f done. Reproduced the stand error in a test JVM: a Flight server with 256 MiB
  direct memory streaming 1 MiB batches to 16 clients that read late fails every stream with "CANCELLED: Server
  sendMessage() failed with Error" (`OutOfMemoryError: Cannot reserve ... direct buffer memory` in gRPC's
  MessageFramer; peak direct 2.1 GB at `-XX:MaxDirectMemorySize=2g`, i.e. each stream filled its 256 MiB
  threshold). Some streams of such a server never finish (no error, no data) — likely what clients saw as hangs.
  Fix: new gRPC interceptor `OutboundBudget` (installed via the Flight builder's `grpc.builderConsumer`) caps
  each streaming call's onReady threshold at budget / open streams (min 1 MiB, max the requested
  `flightBackpressureThresholdBytes`, rebalanced when streams open/close; gRPC reads the threshold on every
  readiness check). New setting `flightOutboundBudgetBytes` (default 0 = MaxDirectMemorySize / 4, else heap /
  4); per-stream threshold default unchanged (13b stays a proposal). Measured: same 16 streams, budget 16 MiB,
  peak direct ~130 MB of 256 MiB, all rows; budget 8-32 MiB at 2 GB: 0.37-0.42 GB peak, all rows.
  Tests: `OutboundBudgetTest` (units + forked-JVM regression, also asserts the unbounded control fails),
  `ConfigAdapterTest`, `HadoopArrowFlightServerUnitTest`. CI run 40297852948 on dd31117 green.
  Next: stand rerun of 0f and 0e; answers to the Open questions (0d crash case, 9, 13).
- 2026-10-26 (cloud session): no numbered item open without a maintainer answer or a stand run (0d-0f, 1b, 2,
  item 0 stand measurements; 8, 9, 13 wait on Open questions), so took the secure HA stand finding (e). Hadoop
  IPC sends the NameNode exception stringified with its stack trace, which reached Flight clients as the error
  description. New gRPC interceptor `StackTraceTrimmer` (installed next to `OutboundBudget` in `grpcOptions`)
  drops Java frame lines (`at a.B.c(B.java:1)`, module/`app//` forms) and `... n more` from every closing
  status; message and `Caused by:` lines stay, multi-line DuckDB errors (Binder `LINE 1:` carets) are
  unchanged; `failureDescription` and the "Cannot read the table's files" text trim before the 1024-char cut.
  Server logs unchanged. Tests `StackTraceTrimmerTest` (incl. an in-process Flight server/client),
  `HadoopArrowFlightServerUnitTest`. Commit 5e573c1, CI run 40310036063 green. Next: stand reruns of 0e/0f; answers to the Open
  questions (0d crash case, 9, 13); other unnumbered findings (secure HA (b) ticket cache from the keytab,
  (d) libhdfs3 RPC timeouts as defaults = maintainer's call, 3-node S3 (b) reservation release, (c)).
- 2026-10-26 (cloud session): 3-node S3 stand finding (b) partly fixed (9168a44): Flight `CancelFlightInfo` (and Flight
  SQL `CancelQuery`, which Arrow maps to it) was `UNIMPLEMENTED`; now it resolves each endpoint's signed ticket and
  releases its node-load reservation at once (idempotent through the reservation key; running streams continue; a
  later read of the ticket releases nothing twice), answers `CANCELLED`, `INVALID_ARGUMENT` for unsigned tickets.
  Tests `FlightSqlProducerTest` (action wiring, tracked/untracked, forged and non-SQL tickets),
  `EndpointFailoverIntegrationTest.cancelFlightInfoReleasesTheLoadOfAnUnreadPlanAtOnce` (Flight client, real
  Hazelcast). README "Cancelling a plan". Local: those tests + checkstyle + spotbugs clean; CI run 40325762742 green.
  Clients that never cancel still hold reservations 10 min → Open questions (connectors should cancel; TTL).
  Next: stand reruns of 0e/0f; answers to the Open questions (0d crash case, 9, 13, reservation TTL); other
  unnumbered findings (secure HA (b) ticket cache from the keytab, (d) libhdfs3 RPC timeouts, 3-node S3 (c)).
- 2026-10-26 (cloud session): item 13(d) (ADBC "failed to update autocommit: ... Unimplemented"). ADBC's Go driver
  sends BeginTransaction only when its connect-time SqlInfo read decoded FLIGHT_SQL_SERVER_TRANSACTION as
  TRANSACTION/SAVEPOINT, so the client had read a wrong answer. Root cause found in Arrow Java (19.0.0, also arrow-java
  main): `ListVector`/`LargeListVector.getFieldBuffers` set `writerIndex(4/8)` on the offset buffer even when empty,
  and an unallocated vector's buffer is the allocation manager's process-wide empty `ArrowBuf`; every other empty
  vector sets it to 0, so concurrent streams serialize empty buffers with the wrong length (a CANCELLED stream with
  `IndexOutOfBoundsException` in `ArrowMessage.asInputStream`, or a header/body mismatch the client decodes shifted).
  Every SqlInfo answer has empty list/map union children. Fix: `EmptyListOffsets` wraps every DoGet listener
  (`FlightSqlProducer.getStream`) and allocates own offset buffers for empty list vectors before each batch;
  BeginTransaction stays UNIMPLEMENTED with a description + WARN line (to match on the stand). Repro:
  `FlightSqlProducerTest.concurrentClientsAllReadNoTransactionSupport` (16 clients x 25 SqlInfo reads over Flight)
  failed 3/3 without the guard, passes 3/3 with it; `EmptyListOffsetsTest`. Not reproduced against a locally started
  server with ADBC Go 1.8.0 / Python ADBC 1.13.0 (~13 000 concurrent connects, insecure and TLS+bearer, CPU
  load) — the stand rerun (13d in Next steps) decides. CI run 40348114333 on 4939fb7 green. Next: stand reruns of 0e/0f/13d; answers
  to the Open questions (0d crash case, 9, 13, reservation TTL); other unnumbered findings (secure HA (b) ticket cache
  from the keytab, (d) libhdfs3 RPC timeouts, 3-node S3 (c)).
- 2026-10-26 (cloud session): secure HA stand finding (d) (libhdfs3 NameNode RPC timeouts). The hadoopfs
  extension (v1.4.5-afc3, ci-assets) has settings only for DataNode connect/read and dead-node TTL, none for
  `rpc.client.*`, so the server cannot ship them as DuckDB defaults. Added (additive, log only):
  `HadoopArrowFlightServer.longHdfsRpcTimeouts` + startup WARN on HDFS naming each of
  `rpc.client.{connect.timeout,read.timeout,write.timeout,timeout}` LIBHDFS3_CONF leaves unset or > 60 s with the
  stand's value; values documented in `arrowflight.properties` and START-NOTES. Test
  `HadoopArrowFlightServerTest.reportsLongHdfsRpcTimeouts`. CI run 40362074150 on 8e0307f green. Note: the extension's
  `hdfs_dead_node_ttl_ms` is not exposed as a server setting yet (optional follow-up). Next: secure HA (b) (ticket
  cache from the keytab), 3-node S3 (c), and the stand reruns of 0e/0f/13d.
- 2026-10-26 (local perf campaign, final phase on 57d252b1437f, 3 rounds, HDFS threads 1/4/8, S3 4/8, 3 nodes 4/8 + two
  4eaab052deaf controls): `docs/dev/TUNING.md` final comparison (default 4 threads = balanced on HDFS, S3 and 3 nodes; same
  speed as 4eaab at 4). 0f confirmed fixed (0 failed reads in 6/6 rounds) and corrected (the node death was the harness
  memory guard); 0e heavy path fixed (SF10 JDBC 22/22 at 8 threads on S3); new 0g (S3 ephemeral-port exhaustion at 8 threads
  x 32 clients), 0h (3-node heavy4 +18 %, recheck); ADBC autocommit race still seen once on 57d252b1437f (before 4939fb7).
- 2026-10-26 (cloud session): item 0g (S3 ephemeral-port exhaustion). Cause measured with DuckDB 1.4.5 + the
  ci-assets httpfs against a counting HTTP server/forwarder: httpfs caches HTTP clients per open file handle only
  and at 2+ threads every row group read gets a new handle, so ~1 connection per range read (16 files x 4 row
  groups: 78 conns/96 requests per query at 4-16 threads, 32 at 1 thread; `http_keep_alive`, curl client,
  prefetch settings no help; metadata caches only save footers/HEADs). `duckDbExternalFileCache=true` makes a
  repeated query open 0 connections. Added (additive, log only): S3 plugin WARNs once at startup when
  `net.ipv4.tcp_tw_reuse` != 1 with port count and reads/s (`S3DataStoreProvider.ephemeralPortWarning`,
  `S3DataStoreProviderTest`); workaround in `arrowflight.properties` and START-NOTES; regression test
  `S3IntegrationTest.connectionsPerQueryAreBoundedAndCachedReadsOpenNone` (8 files x 4 row groups at 8 threads:
  39/31 connections for the first/repeated query, with the cache 40/0). Server-side fix -> Open questions
  ("S3 connection churn"). CI run 40399232182 on 9f7eb4f green. Next: 0h (stand recheck), stand rerun of 0g
  with `tcp_tw_reuse=1`, then secure HA (b) (ticket cache from the keytab), 3-node S3 (c).
- 2026-10-26 (cloud session): secure HA stand finding (b) (hadoopfs needs a ticket cache). libhdfs3 (afc3) reads
  only a ticket cache (`hadoop.security.kerberos.ticket.cache.path` in LIBHDFS3_CONF, else `KRB5CCNAME`, else the
  default). Added (additive, off unless set): `hdfsKerberosPrincipal` + `hdfsKerberosKeytab` (`HDFS_KERBEROS_*`,
  optional `hdfsKerberosTicketCache`) — `HdfsTicketCache` logs in from the keytab (JAAS), writes the TGT as a MIT
  file cache v4 (owner-only, atomic replace) before the data store opens, and logs in again at 80 % of the
  lifetime (retry every minute on failure; a failed first login stops the start); WARN when the cache is not the
  one the extension reads. Test `HdfsTicketCacheTest` (in-process Kerby KDC; the cache is read by the JDK, Kerby
  and MIT `klist`/`kvno`, which also gets a service ticket with it — verified locally; Dev CI now installs
  krb5-user so that case runs there too). CI run 40415521254 on 2b2cac8 green. Next: 0h (stand recheck), stand
  reruns (0g with `tcp_tw_reuse=1`, secure HA (b) without `kinit`), then 3-node S3 (c) (first plan after B5 waits
  9-10 s), optional: expose the extension's `hdfs_dead_node_ttl_ms` as a server setting.
- 2026-10-27 (cloud session): 3-node S3 stand finding (c) (first plan after B5 waits 9-10 s on S3, ~1 s on HDFS)
  explained, no code change. Measured in one JVM (doomed member on 127.0.0.2): crash with its port refusing ->
  member dropped and first plan's cluster ops done in ~0.35 s; crash with the address made silent (iptables DROP)
  -> 9.96 s = `hazelcast.max.no.heartbeat.seconds`. S3 and HDFS stores are both `shared` and plan through the same
  key lookups since 0d, so the storage is not the difference; how the nodes died is. Tests in
  `ClusterMemberLossIntegrationTest` (refused port < 5 s; vanished address with the timeout at 3 s via `-D` takes
  2-8 s — needs iptables as root or `sudo -n`, skipped otherwise); START-NOTES documents the window and the `-D`
  override. Decision on faster detection stays in Open questions (0d follow-up). CI run 40428800760 on 9934d3b
  green (server 764 tests / 2 skipped = the usual two; the iptables case runs on the runner via sudo -n).
  Next: 0h (stand recheck), stand reruns (0g with `tcp_tw_reuse=1`, secure HA (b) without `kinit`, B5 kill mode),
  optional: expose the extension's `hdfs_dead_node_ttl_ms` as a server setting; 3-node S3 (a) (S3 outage read
  latency) partly covered by 0e's retry settings.
- 2026-10-27 (maintainer's local agent): speed comparison upstream `main` dbacd8d / `feat/starrocks-native-integration`
  07631d7 vs ours 57d252b on the same single-node stand (3 rounds, same container/JVM/data/clients): results in
  `docs/dev/UPSTREAM-SYNC.md` "Speed comparison 2026-10-26". Upstream wrong answers (LIMIT ignored, decimal scale lost in q06,
  no AVG / prepared statements / parameters) disqualify it on JDBC TPC-H, heavy and small aggregates; upstream wins point/count
  latency, TPC-H SF1 via Trino and single-stream scan. Next steps 14-16 added.
