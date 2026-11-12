# Lane 3 - distributed (branch `lane/distributed`)

See `CLAUDE.md`, "Third and fourth lanes". Moved here from `STATUS.md` stage 3 on 2026-10-28 (maintainer).

## Next steps

> **Maintainer, 2026-11-11 21:10 - answers + D14.** **Q3: yes** - default `arrowAllocatorLimitBytes` from the JVM's
> direct memory (e.g. 75 % of MaxDirectMemorySize, at least 256 MiB; an explicit setting wins; start line shows it).
> **Q4: yes**, keep `distributedCompletePointLookups=local` as the default. **D14 (stand on `29f1267b995f`, DataNodes
> `authentication,privacy`, direct SASL reads):** 117/117 exact on 3 servers speed+dc; DATE point mix dc 2301 vs speed
> 2227 q/s (1.03); but the small-query mix at 32 clients is dc 1133 vs speed 2072 q/s - one-row aggregates (count /
> sum with a filter, no GROUP BY) are still merged across nodes. Treat filtered one-row aggregates over a pruned file
> set like point lookups (local) when pruning keeps <= N files. As ITEM 48, merge when green, "ITEM 48 DONE <sha>".

ITEM 48 DONE 97ad4f22 + Q3 DONE (2026-11-12, see Log; branch commits 5bacb483..97ad4f22, fast-forwarded into
`main`). Item 48: under dc with `distributedCompletePointLookups=local`, a filtered one-row aggregate (count / sum /
min / avg with a WHERE clause, no GROUP BY) whose footer statistics keep at most
`distributedCompleteLocalAggregateMaxFiles` (default 4, 0 = merge) files, all readable here, is computed on the
planning node. It needs footer pruning, so with a local, unshared `dataDir` it applies only when
`fileStatsPruningMaxFiles` is set (Q-40b-1); HDFS / S3 / shared stores prune by default. Q3:
`arrowAllocatorLimitBytes` unset = 75 % of direct memory, at least 256 MiB; start line `arrowAllocator limitMiB=...
source=directMemory|set`. Service cluster 43145797836 13/13 + summary green on the branch (point gate with `agg_range`;
dc / speed point q/s hdfs 1.22, hdfs-ha 1.29, s3 0.81). For the stand (on the `package=true` pre-release of 97ad4f22):
rerun the small-query mix at 32 clients under dc; expect count / sum with a selective filter not `merged`, and with
`-XX:MaxDirectMemorySize=8g` the start line `limitMiB=6144 source=directMemory`, 48 peer streams.

ITEM 45 DONE b8613fc4 (2026-11-10, see Log): dc point lookups with typed literals (`= DATE '...'`,
`= CAST('...' AS DATE)`, `TIMESTAMP '...'`, exponents, parenthesised literals) run on the planning node. Service
cluster 42901104843 (12/12 green, mix now includes `point_date` with a DATE literal): speed-dc 0 merged / 0 admission
waits on srv1 in all three stores; speed-dc / speed q/s hdfs 48.2 / 37.7, hdfs-ha 36.4 / 38.1, s3 49.1 / 37.4.
For the stand: rerun the dc point mix with the DATE form; expect no `distributedComplete=merged` for it.

> **Maintainer, 2026-11-11 00:05 - ITEM 45 (item 43 follow-up, stand on `2ff221447b5d`).** The point-lookup
> pattern (`POINT_EQUALITY` in `DistributedComplete`) matches only a string or number after `=`: `SELECT count(*) FROM
> tpch.orders WHERE o_orderdate = DATE '1992-01-01'` (and `CAST('1992-01-01' AS DATE)`) is still `merged
> endpoints=8`, p50 73 ms vs 14 ms under speed; with `= '1992-01-01'` the mix reaches 0.92 of speed. The real
> cluster's benchmark mix uses the DATE form. Accept typed literals (DATE / TIMESTAMP / CAST(... AS type) / negative
> numbers / decimals). Overall dc 32-client point mix was 899 q/s vs speed 2198 (0.41; target >= 0.8). CI: the point
> gate with a DATE literal. Merge when green, "ITEM 45 DONE <sha>".

> **Maintainer, 2026-11-10 11:10 - FIRST in this lane: ITEM 43 (stand bug (stand check of `894afb9b48c4`, ~500 000 hash-checked answers, none wrong; logs in the maintainer's stand)).** distributedComplete point
> lookups queue for all peer streams: each takes `permits=4` (all 4 peer streams of a node with 512 MiB direct), so
> 61-63 % of dc queries wait for admission; at 32 clients dc does 217 q/s vs speed's 1348, dc small-query p99 297 ms
> vs speed 54 ms. A query whose answer is one row / a few rows (point lookup, count) needs no merge budget: take
> permits by the expected result size (or none for single-row answers), keep the item 35 bound for large scans.
> CI: the cluster cell with 32 clients of point queries under dc, q/s within ~20 % of speed. Branch
> `lane/distributed`, merge when green, "ITEM 43 DONE <sha>".

D17 DONE (2026-11-10, see Log): Service cluster on `main` 42789267919 was red only on item 43's summary point gate
(hdfs speed-dc / speed 0.62). Same server code across runs gave 0.62-1.77 (the two cells run on different runners),
so the gate now checks the deterministic part in the speed-dc cell (point lookups on node 1: 0 merged, 0 admission
waits) and the cross-runner ratio warns < 0.8, fails < 0.5. Watch: a speed-dc cell failing with `point load under dc`.

ITEM 43 DONE 502e295d (2026-11-10, see Log; commits 85df974d, b3d9d38f, 502e295d): dc point lookups no longer fan
out or queue. Service cluster 42786623655: 32 clients of point queries on node 1, speed-dc / speed q/s hdfs 43.5 / 51.7
(0.84), hdfs-ha 36.2 / 37.0 (0.98), s3 28.2 / 28.7 (0.98); 12/12 cells green; Dev CI 42786620428 green. Changes a dc
default: see Q4. For the stand (on the core lane's next pre-release, which carries it): rerun the dc small-query
classes at 8 / 32 clients; expect the start line `distributedComplete point lookups: local ...`, no
`admissionWait` for point_orders / point_lineitem / count_filter, q/s close to speed.

ITEM 39(d) DONE a952f415 (2026-11-09, see Log): merge budget from min(direct memory, Arrow allocator limit);
one-row merges skip the queue. For the stand: rerun the dc small-query classes (8 / 32 clients) and the 8
concurrent full scans on the core lane's item 39 pre-release; expect `peer streams: 16 (... Arrow allocator limit
2048 MiB)` at start, 0 allocation errors, and `permits=0` on the count/sum merges.

> **Maintainer, 2026-11-09 16:30 - FIRST, before anything else in this lane: ITEM 39(d).** dc small-query p99 tails and the 2 GiB allocator cap on concurrent dc scans (item 39 (d)). Full description
> with the real-cluster numbers: `docs/dev/STATUS.md`, notes "ITEM 39 IS SPLIT ..." and "BUG FIRST, ITEM 39". Work on branch
> `lane/distributed` (create it from `origin/main` if missing; rebase on `origin/main` each session), tests + CI green on the branch,
> then merge into `main` (pull --rebase, keep both sides of CHANGELOG conflicts) and write "ITEM 39(d) DONE <merge sha>"
> in this file's Log. Maintainer exception to this lane's scope limits for this item only. No `package=true` here: the
> core lane packages when all four parts are merged.

> **Maintainer, 2026-11-02 18:30 - SERVICE MATRIX, cluster part (take it first; this item goes straight to main, no
> branch).** A real Hadoop cluster refused every HDFS query with `f06b5834e76e` (core lane fixes it). Add
> `.github/workflows/service-cluster.yml`: a **3-node cluster** of the server on **HDFS** (NameNode + 3 DataNodes, each
> server next to a DataNode with short-circuit reads, as `docker-compose.ci.yml` lays out) and on **S3** (MinIO), in
> balanced, speed and speed + `distributedComplete=true`; plus one node stopped mid-query (answers complete or a
> repeatable error, never a partial answer). Own CI workflow of its own (only this lane edits it), triggered on `workflow_dispatch` with an input `server` (a kestrel
> pre-release id; default: build from the checked-out commit) and on push to main touching its files. Each cell: a real
> container of the service, the server in **balanced and `-DresourceProfile=speed`**, TPC-H SF1 22/22 with answers
> checked against in-process DuckDB + Flight SQL metadata + one prepared query; job summary = table service x profile
> -> pass/fail + first error line. A failure in a cell is a bug for the lane that owns the code (write it as bug-first
> into that lane's STATUS). Then write "SERVICE MATRIX <lane> DONE" at the start of a line.

SERVICE MATRIX lane 3 DONE (2026-11-02, see Log): `.github/workflows/service-cluster.yml`, 8 cells green (run 41469353046).

D15 DONE 7c54dfde + ace241be (2026-11-09, see Log): distributed complete merges queries with plain table names
    (they all fell back to one node). For the stand: TPC-H with plain names + `-DdefaultSchema=tpch` under dc should
    log `distributedComplete=merged` for q01 / q06 and no `fallback reason='Name the table with its schema'`.

D16 DONE 97a2b7bb (2026-11-09, see Log): the core lane's standby retry (`2429782f`) holds in the hdfs-ha loss
    cell, which now fails over twice and logs per-server retry counts; run 42642657223 shows one refusal retried
    (500 ms) and answered, 0 errors. Watch: a `retry failed` count > 0 or a red hdfs-ha loss cell in later runs.

D13 (c) DONE 74971b9a + 60c36431 (2026-11-09, see Log): the loss cells stream a slow merged scan across the node
    kill; `peerLost` UNAVAILABLE after the first batch is now seen in CI (2 of 3 loss cells per run), every retry
    exact. Watch: a red loss cell, or `scan_peer_lost=0` in all three loss cells of several runs.

D13 (b) DONE 3a595503 + dd9090a8 (2026-11-09, see Log): the hdfs-ha loss cell notes the reads each NameNode
    answered per failover window; queries planned after each failover reach the new active NameNode (~2 700-3 000
    reads per window, standby ~0). Watch: a `NameNode not reached` warning in later runs.

D14. (Point lookups: done by item 43, computed on one node.) After the stand's item 39 rerun: if dc point queries (row-returning, e.g. `point_orders`) still have p99 tails
    behind scans, charge them less (per-shape) or compute small plans on one node (threshold on planned bytes).
    Optional (changes a default): derive `arrowAllocatorLimitBytes` from direct memory - written up as Q3, no code
    until answered.

D13. Follow-ups of the service matrix (cluster), optional:
    - (a) short-circuit reads: *measured 2026-11-02 (see Log)* - **not used**. *(a1) done 2026-11-02 (19th session):
      the layout supports it (Java probe in dn1 reads short-circuit), so it is hadoopfs/libhdfs3 - handed to the core
      lane (`STATUS.md` Log); the HDFS cells emit a `short-circuit unused` warning, not a failure (answers are right).* With short-circuit configured (native
      libhadoop loads, DataNode domain socket mounted, `LIBHDFS3_CONF` with `dfs.client.read.shortcircuit` +
      `dfs.domain.socket.path`), every DataNode has `ReadBlockOpNumOps == BlocksRead` (all block reads over TCP) and
      ~60 % of them come from remote clients. Answers are right; only speed is lost. Next: (a1) check the layout with
      the Java client inside a DataNode container (`hdfs dfs -cat` of a local block, JMX before/after: no new
      READ_BLOCK = the layout supports short-circuit) - if it does, it is a hadoopfs/libhdfs3 matter for the core lane
      (noted in `STATUS.md` Log); then make the cell fail when short-circuit is configured but unused. ~~(a2) why 60 %
      remote reads~~ *measured 2026-11-02 (20th session, see Log): not a planning bug.* Partial endpoints run on
      the node holding their files' blocks (balanced: 10/10 endpoints read only local blocks); the remote share is
      complete mode: an unmerged complete answer (every join, most TPC-H queries) is computed by the one node the
      client called, which reads all files, so ~2/3 of its blocks are remote with 3 DataNodes and replication 1,
      plus the planning node reading the Parquet footers of every file of the table (6-10 block reads per plan).
      Only merged answers (`distributedComplete`, 2 of 22 TPC-H queries) read locally. A wider merge (joins) is
      the only way to cut it - not planned.
    - ~~(b) an HA nameservice URI (`hdfs://<ns>/...`) for the cluster cells~~ *done 2026-11-02 (1646ed4f, see Log):*
      store `hdfs-ha`, 12/12 cells green. ~~Follow-up (optional): the hdfs-ha loss cell's NameNode failover (nn2 -> nn1,
      ~30 s into the load) saw 0 errors, but the cell does not show that a query planned after it reached the
      NameNode (speed caches file listings) - count NameNode RPCs per NameNode (JMX) after the failover, or fail
      over under `balanced`.~~ *done 2026-11-09 (D13 (b) DONE above).*
    - ~~(c) the loss cells saw no retryable error (kills land between or before peer reads: fallbacks 1-2, 0 wrong) -
      a longer load or a kill timed inside a merged answer would exercise `peerLost`.~~ *done 2026-11-09 (D13 (c)
      DONE above).*
    - ~~(d) HDFS cells with `-Darrowflight.duckdb.extensionDir` only~~ *done 2026-11-02 (ff4b3310, see Log):* the gate
      now fails `server-1.0.0-fca6512a8046` (start refused) and passes `server-1.0.0-c4cd985f37e3`.

D1. ~~**Item 18 step 2 - distributed complete mode.**~~ *Done 2026-10-28 (738d1e7, see Log).* Opt-in `distributedComplete`
    (default false). Open follow-ups, folded into D2: (a) TLS + a real auth provider between two processes (the
    internal identity is unit-tested; the in-process test runs without auth); (b) peer streams are read one after the
    other by DuckDB's scan (all DoGets start at once, gRPC buffers the rest) - measure in D2 whether a parallel
    prefetch is worth it; (c) failover to an endpoint's backup location instead of the single-node fallback.
D2. Item 18 step 3 - cluster integration test, then a perf test.
    - ~~(a) 3-node correctness test~~ *Done 2026-10-28 (see Log):* `DistributedClusterIntegrationTest`.
    - ~~(b) Perf test~~ *Done 2026-10-28 (see Log):* `DistributedCompletePerfTest`. Decision on D1 follow-up (b):
      no parallel prefetch for now - peer wait is 1-3% of the merged query for the aggregate shapes and 14% for the
      wide ones (one JVM); revisit with a real multi-host run (`-Dperf.rows`, numbers in the Log).
    - ~~(c) TLS + a real auth provider between two processes~~ *done 2026-10-28 (see Log):*
      `DistributedTwoProcessTlsTest`. ~~Failover to an endpoint's backup location before the single-node
      fallback~~ *done 2026-10-28 (see Log).*
    - Observed: a peer stream cut mid-way hit before the first client batch in both cut tests (DuckDB's arrow scan
      reads peers before emitting), so the after-first-batch error path is only asserted as "error or exact",
      not forced; forcing it needs a cut after DuckDB emitted, e.g. a larger wide projection with a later cut.
D3. Faster detection of a crashed member (`STATUS.md` Open question, item 0d follow-up): plan from a local cluster snapshot
    refreshed by the heartbeat thread instead of blocking map reads; keep `hazelcast.max.no.heartbeat.seconds` as is.
    - ~~Step 1: planning reads~~ *done 2026-10-28 (a110a22, see Log):* `ClusterView`.
    - ~~Step 2: writes~~ *done 2026-10-28 (aef0a7a, see Log):* deferred writes in `ClusterService`.
    - ~~Step 2b: remaining writes~~ *done 2026-10-28 (see Log):* claim, inventory/secret puts, `close()`.
    - ~~Step 3: `fileLocations(false)` (local disks)~~ *done 2026-10-28 (a9f6def, see Log):* local inventory copy.
D4. ~~Hazelcast test-port race~~ *done 2026-10-28 (256bddd, see Log):* `TestPorts`.
D5. ~~D2 observed gap: force the after-first-batch cut path~~ *done 2026-10-29 (see Log):*
    `DistributedCompleteCutAfterFirstBatchTest`.
D6. ~~**Q1 answered (maintainer 2026-10-29): yes**~~ *done 2026-10-29 (fd6084f, see Log).* Rethrow as `UNAVAILABLE` ("peer lost during distributed complete
    answer; retry") when a peer stream ended with UNAVAILABLE/transport error after the first batch; other failures
    stay INTERNAL; opt-in path only; test + doc line.
D7. ~~Perf of distributed complete on 3 containers in CI (heavy TPC-H shapes that merge: q01, q06, q12-like aggregates,
    filtered scans) on vs off, rows/s and wall time; decide the D2 (b) prefetch question from it.~~ *Done 2026-10-29.*
    - ~~Step 1: workflow~~ *done 2026-10-29 (see Log):* `distributed-perf.yml` (manual).
    - ~~Step 2: run it~~ *done 2026-10-29 (see Log):* on/off 0.39-0.59 for the aggregates, 0.88 for the filtered
      scan; peer wait 13-15 % of merged time. Decision on D2 (b): still no parallel prefetch.
    - Optional: a run with `cpus=2` needs a runner with >= 6 cores (ubuntu-latest has 4: three 2-CPU nodes would
      share 4 cores, while "off" gets its 2 alone); a netem latency input between the nodes.
D8. ~~Rolling restart under load: 3 nodes, a client loop of mixed queries, restart nodes one by one (graceful and
    kill -9): no wrong answers, failures only as retryable codes, recovery time per node in the Log.~~ *Done
    2026-10-29 (see Log):* `distributed-restart.yml` + `RollingRestartLoadTest`; found and fixed (opt-in) the
    graceful-stop `INTERNAL`: `shutdownDrainSeconds`. Follow-ups: Q2 (default of `shutdownDrainSeconds`); the
    client-side recovery (~6-9 s from "started" to the first answer planned on the node) is mostly the load
    client's gRPC reconnect backoff - measure with a fresh channel per retry if it matters.
D9. ~~Node join under load: a 4th node joins mid-run; plans start using it without errors; inventory/heartbeat
    timings.~~ *Done 2026-10-29 (see Log):* `NodeJoinUnderLoadIntegrationTest` + `distributed-join.yml`; found and
    fixed a permanent node-load leak with concurrent identical queries (`LoadReservations`). Follow-up: the leave half
    of the workflow sends calls to the stopped node until the client drops it (load client only, retryable).
D10. ~~Found in D7: `AVG` is not mergeable (`AggFunc` has COUNT/SUM/MIN/MAX only), so TPC-H q01 itself is computed by one
    node; D7 measures q01 without its AVG columns. Candidate (additive, opt-in path only): endpoints answer sum + count
    for an AVG, the merge divides (type as DuckDB's avg: DOUBLE, DECIMAL avg -> DOUBLE); test equality vs single node.~~
    *Done 2026-10-29 (ab8cc3d, see Log):* `AverageMerge`. Follow-up (optional): a bit-exact merge for unscaled sums
    >= 2^53 would need DuckDB's long-double division (x86: 80-bit) - not worth it unless a client compares bits.

D11. ~~Q2 answered (maintainer 2026-10-29): `shutdownDrainSeconds` default 20 s, START-NOTES, `docker stop -t 25`.~~
    *Done 2026-10-29 (see Log).*
D12. *Decided 2026-11-09 (lane 3): acceptable, no change* - `LoadReservations` is in every pre-release since
    2026-10-29 (the stand runs November builds), so only an upgrade from a build of 2026-10-23..28 crosses it, and
    there only identical concurrent plans write the new value; the old node then fails to read that key until it is
    restarted with the new build (one rolling-upgrade window). Reopen if a cluster still runs an October build.
    Original candidate (found in D9, needs no approval: test/infra only unless it changes behaviour): mixed-version clusters
    during a rolling upgrade - a node before `LoadReservations` cannot read a reservation key holding several
    reservations (only identical concurrent plans produce one); measure with the restart workflow across two
    builds, or decide it is acceptable for an upgrade window (loads then expire after `flightLoadReservationSeconds`
    only if the key still exists).

## Open questions

Q1. (2026-10-29, lane 3, **answered: yes, see D6**) **Status code when a peer is lost after the client's first batch** (opt-in
    `distributedComplete` only). Today the client gets `INTERNAL` with DuckDB's `arrow_scan: get_next failed(): ...`
    text that wraps the peer's `UNAVAILABLE` (asserted by `DistributedCompleteCutAfterFirstBatchTest`). The answer is
    never silently partial; only the code differs. Proposal: when the merge fails after the first batch and a peer
    stream ended with `UNAVAILABLE` (or a transport error), `DistributedComplete.stream` rethrows as `UNAVAILABLE`
    ("peer lost during distributed complete answer; retry"), so clients and the JDBC/ADBC drivers that retry on
    `UNAVAILABLE` re-run the query (the retry plans without the lost member). Other failures stay `INTERNAL`. This is
    wire-visible (status code), hence a question: (a) map to `UNAVAILABLE` as proposed, (b) keep `INTERNAL`.
    No code until answered.

Q2. (2026-10-29, lane 3, D8; **answered by the maintainer 2026-10-29: (a) default 20 s**, document it in START-NOTES and
    note that `docker stop` needs `-t 25` or more; **done, D11**) **Default of `shutdownDrainSeconds`.** Without a drain (default 0, today's behaviour) a
    node stopped with SIGTERM cuts the answers it is still sending with `INTERNAL` (HTTP/2 RST_STREAM), which clients
    and drivers do not retry - seen once per graceful stop under load. With a drain the node leaves the pool, new calls
    get `UNAVAILABLE` and running answers finish (D8 run: 0 non-retryable failures over 3 graceful + 3 kill -9
    restarts). Proposal: default 20 s (below Kubernetes' 30 s grace period; `docker stop` waits 10 s unless `-t`).
    Changes stop behaviour (stops take up to the longest running answer), hence a question: (a) default 20, (b) keep 0.

Q3. (2026-11-10, lane 3, D14; **answered by the maintainer 2026-11-11: yes, 75 %, at least 256 MiB; done 2026-11-12**) **Default of `arrowAllocatorLimitBytes`** (`arrowflight.arrow.allocatorLimitBytes`,
    today a fixed 2 GiB). The root Arrow allocator holds every outgoing answer and, under `distributedComplete`, every
    peer stream of a merge. On the stand (`-XX:MaxDirectMemorySize=8g`) 8 concurrent dc full scans hit `Unable to
    allocate buffer ... Current allocation: 2147231193`: 6 GiB of direct memory were unusable. 39(d) (a952f415) now
    admits peer streams by min(direct memory, allocator limit), so dc no longer overruns the cap, but it waits instead
    (16 streams at 2 GiB). Proposal: when the setting is not given, default to 75 % of `MaxDirectMemorySize` (at least
    2 GiB, i.e. never below today's value; the rest left for Netty/gRPC buffers outside the allocator); log
    `arrowAllocator limitMiB= source=directMemory|set`; an explicit value keeps today's meaning. Effect: on 8g direct
    6 GiB and 48 peer streams; on hosts with <= 2.67 GiB direct nothing changes. Risk: with a larger limit a burst of
    answers can use more direct memory before the allocator refuses (the JVM's own direct limit still holds, giving
    `OutOfDirectMemoryError` instead of the allocator's clear `OutOfMemoryException` if Netty needs the remaining 25 %).
    Changes a default, hence a question: (a) derive as proposed, (b) another share, (c) keep 2 GiB (stand sets it).
    No code until answered.
Q4. (2026-11-10, lane 3, item 43; **answered by the maintainer 2026-11-11: keep `local`**) **`distributedCompletePointLookups` default `local`** (`FLIGHT_DISTRIBUTED_COMPLETE_POINT_LOOKUPS`,
    new). Under `distributedComplete=true`, a query with `column = literal` (no OR) and no aggregate or a one-row
    aggregate (point_orders, point_lineitem, count_filter) is now computed by the planning node alone when it can read
    the files, as without dc; `merge` restores merging (on probation, without permits until 16 MiB read). Reason: with
    probation alone (no admission waits at all) CI dc still did 0.27-0.57 of speed's q/s (run 42781188876): each point
    lookup cost six part queries on three nodes. Cost of the default: a one-row aggregate with an unselective equality
    (`count(*) ... WHERE l_returnflag = 'R'` on a large table) runs on one node instead of all. Answers are unchanged
    either way. Keep `local` as the default, or default to `merge` (then the stand / clients set `local`)? Implemented
    as `local` because item 43's acceptance (dc within ~20 % of speed) needs it; flipping is a one-word change.

## Log

- 2026-10-28 (maintainer): lane created; D1-D3.
- 2026-10-28 (lane 3 session): D1 done - item 18 step 2 (738d1e7, branch `lane/distributed`, rebase-merged into `main`).
  `DistributedComplete`: on the planning node's complete-answer DoGet, mergeable shapes (`CompleteAnswerMerge.refusal`
  == null, no parameters, no time travel) with >= 2 planned partial endpoints are read from the endpoints' first
  locations (DoGet with header `x-kestrel-internal-identity` = user + expiry + HMAC-SHA256 with the ticket secret,
  60 s), concatenated into one Arrow stream registered in the node's DuckDB and merged; the merged columns are renamed
  to the single-node answer's names (found by the test: partial endpoints name `count(*)`, the complete path
  `count_star()`) and the types checked before the client's stream starts (start is deferred to the first batch, so
  any failure before it falls back cleanly). Peers: `InternalCallAuthenticator` wraps the configured authenticator,
  a gRPC interceptor refuses the header from addresses that are not live members' hosts; peer TLS trust
  `distributedCompleteTrustedCertificates`, else `FLIGHT_TLS_CERT`. Tests: `DistributedCompleteIntegrationTest`
  (one node, 4 files, `endpointsPerServer=4`: 7 shapes identical schema + rows on vs off, 2 refused shapes untouched,
  unreachable peer -> fallback), `InternalCallIdentityTest`, `InternalCallAuthenticatorTest`, `ConfigAdapterTest`.
  CI run 40670031954 green on the branch; package run 40671772031 on 738d1e7 was cancelled by the plugins lane's push; their package dispatch 40672085389 on c258adf contains 738d1e7 and was running at session end (its pre-release server-1.0.0-c258adf27dc9 carries this change - next session: confirm it is green, else dispatch package=true). Next: D2 (3-node CI test).
- 2026-10-28 (lane 3 session, 2nd): D2 (a) done - 3-node correctness test of distributed complete mode (8d2006c,
  rebase-merged into `main`; test only, no runtime change, so no pre-release). Lock taken 11:28Z after the previous
  session's package run 40672085389 (c258adf, contains 738d1e7) was confirmed green. `DistributedClusterIntegrationTest`:
  3 complete-mode nodes, one Hazelcast cluster (seeded members with their Flight URIs), one JVM, a shared data
  directory (`TestFlightServerHelper.sharedStorage()` presents the local store as shared, else complete mode refuses
  a local-disk cluster), 6 files, `endpointsPerServer=2`. Merges read 6 endpoints on 3 nodes (4 on 2 after a node
  left); 9 shapes equal the single-node answers (ordered row by row, unordered as row sets); a peer stream reset
  after 32 KB through `CuttingRelay` (TCP relay, RST) and a peer whose Flight port is closed fall back to the exact
  answer; a node that left is not planned. Helper: `hazelcastSeeds(...)`, `sharedStorage()`. Local run 5/5 green
  (~8 s); CI run 40681311340 green on the branch (before the rebase onto acb87cb, plugins-lane commits only).
  Next: D2 (b) perf test on vs off, D2 (c) two-process TLS + auth.
  On `main` 8d2006c has no CI run yet: the merge push ended in a `[skip ci]` commit, and a dispatch (40682318542)
  was cancelled by me so as not to cancel the plugins lane's run 40681896929 (same concurrency group). Next session:
  confirm a green Dev CI run on `main` containing 8d2006c (else dispatch one).
- 2026-10-28 (lane 3 session, 3rd): lock 12:31Z. Confirmed 8d2006c green on `main` (Dev CI 40682965605 on 177dfe3).
  D2 (b) done (ffcbabf): `DistributedCompletePerfTest` (`perf`; 3 nodes in one JVM, shared lineitem-like table,
  12 files, `endpointsPerServer=2`, off vs on, answers asserted equal) + `DistributedComplete.peerBytes()` /
  `peerWaitNanos()` (also in the `merged` log line). Local run, 4 cores, 6M rows, median of 3, off -> on ms (peer
  MB/query, peer wait share of on): Q1-like GROUP BY 215 -> 117 (0.00, 1%); Q6-like 117 -> 97 (0.00, 1%); wide
  DISTINCT 252 -> 344 (9.8, 3%); ORDER BY LIMIT 146 -> 94 (0.01, 1%); wide GROUP BY 442 -> 464 (38.9, 14%); wide
  rows 793 -> 785 (27.7, 14%). At 1.2M rows "on" is ~1.8x slower (fixed cost of the fan-out ~30-50 ms). So no
  parallel prefetch yet (D1 follow-up (b)). Found by the test: DOUBLE sums merged vs single-node differ in the last
  digits (summation order; DuckDB's own parallel sum varies the same way) - not a bug, the perf table uses DECIMAL as
  TPC-H does; noted in the test's Javadoc. D2 (c) part 2 done (c7e436e): an endpoint answer that fails before its
  schema is read from the endpoint's next location (backup), `failovers()`; tests: peer Flight down -> merged via
  backups (was: single-node fallback), all locations down -> fallback. CI 40689624446 green on the branch (64f531e,
  both commits; 40689301840 cancelled by it); rebase-merged into `main`, package=true run 40691235109 on
  f73beb2 was cancelled by the core lane's push fd9f62f, whose Dev CI run 40691933422 (contains both commits) was
  running at session end; not cancelled, so as not to cancel theirs. Next session: confirm a green run and a
  pre-release containing c7e436e (else dispatch package=true on `main`). Next: D2 (c) part 1, two-process TLS + token auth.
- 2026-10-28 (lane 3 session, 4th): lock 14:44Z (a plain `git fetch origin` hung on the tags in this container;
  `git fetch --no-tags origin main lane/distributed` works). Confirmed c7e436e in pre-release
  server-1.0.0-94ad61021d93 (Dev CI 40695407602, package green). D2 (c) part 1 done (eb82020, rebase-merged into
  `main`): `DistributedTwoProcessTlsTest` (`integration`) starts two `HadoopArrowFlightServer` JVMs with
  `FLIGHT_TLS_CERT`/`KEY` (self-signed for localhost, generated in the test), `FLIGHT_AUTH_PROVIDER=token`,
  `distributedComplete=true`, `endpointsPerServer=2`, one Hazelcast cluster; 4 shapes over TLS with the bearer token
  equal DuckDB's answers over the same files, node 1's log shows `distributedComplete=merged endpoints=4` for each and
  no fallback/failover (so the peer DoGets went over TLS trusting the node's own certificate, admitted by the
  internal identity in front of the token authenticator); a call without the token and a forged
  `x-kestrel-internal-identity` from a member's address are UNAUTHENTICATED (`internalCall=refused`). ~15 s; local
  5/5 green. Needed and added (additive, default unchanged): `dataDirShared` / `FLIGHT_DATA_DIR_SHARED` declares a
  local data directory a mount every node reads (NFS), so a multi-process cluster on one directory plans complete
  answers (the in-JVM tests use a proxy for that). CI 40709875961 green on the branch (859f039, before the rebase onto
  plugins/catalogs commits); Dev CI 40712744776 green on `main` at 2cfa23e (contains eb82020); package=true run
  40715095069 on 2cfa23e green, pre-release server-1.0.0-2cfa23eb62f9. Checked the core lane's 94ad610 (cut tests accept a failover + merge): matches
  the intent. D2 is complete. Next: D3.
- 2026-10-28 (maintainer): lane-3 session 16:25Z took LOCK-LANE3 and ended within 2 minutes without changes; lock released by the maintainer.
- 2026-10-28 (lane 3 session, 5th): lock 17:29Z. D3 step 1 done (a110a22, rebase-merged into `main`): planning reads
  (`allServerLoads`, `filterLiveServers`, `fileLocations(true)`, `hasFileInventory`) go through `ClusterView`
  (`HazelcastAdapter.view()`): `getAsync` per key plus one probe key per other member (a key whose partition that member
  owns), waiting at most 1 s; a member whose reads do not answer is unresponsive: its keys take the last values read
  (the heartbeat thread refreshes loads and heartbeats every 3 s, `refreshView`), its server is left out of plans but
  not removed from the pool, later reads do not wait for it, and it is back as soon as any read of its partitions
  answers or it leaves the cluster. This node's own inventory falls back to its local copy; an unknown inventory on a
  silent partition counts as published (nodes publish before they register). `max.no.heartbeat` stays 10 s.
  Measured locally (iptables DROP on 127.0.0.2, default 10 s): first plan < 3 s (was >= 10 s), later plans < 500 ms
  while the member is still in the cluster. Tests: `ClusterViewTest` (6), `ClusterServiceTest` (+1),
  `ClusterMemberLossIntegrationTest.crashedMemberWhoseAddressVanishedHoldsOnlyTheFirstPlanForTheReadWait` (replaces
  the 3 s-heartbeat variant; runs where iptables works: root or sudo -n). Known gap: a member's entry on the silent
  member's partitions that this node never read is absent (refreshView makes that the first 3 s of a node only).
  CI 40739303087 green on the branch (3871f94). On `main`: my package=true dispatch 40741178454 on a110a22 cancelled
  the plugins lane's dispatch 40739820098 on 929b544 (same concurrency group; a110a22 contains 929b544); it is
  green, pre-release `server-1.0.0-a110a2279437` (contains 929b544 too). Next: D3 step 2 (writes).
- 2026-10-28 (lane 3 session, 6th): lock 18:25Z. D3 step 2 done (aef0a7a, rebase-merged into `main`): cluster map
  writes whose key lives on a member the cluster view found unresponsive go to a background writer
  (`ClusterService.writeOrDefer`, thread `flight-cluster-write`; `deferredWrites()`): `addLoad`/`subtractLoad`
  (pending deltas counted in `allServerLoads` at once), ticket load reservations and releases (same key keeps its
  order: later writes of a key with deferred writes queue too), `storeHandle`, `removeHandle`, `rememberParameters`,
  pool removals, the heartbeat (`beat()`, at most one pending, writes the time it runs). The heartbeat thread runs
  `refreshView` before the beat; `repairClusterState` skips entries whose owner is silent; `filterLiveServers` keeps
  a member whose heartbeat entry is on the silent member (its value ages) instead of dropping it as stale. Found by
  the iptables test: a put whose *backup* replica is the silent member waits Hazelcast's backup timeout (5.5 s for
  one ticket, its release 3 s) - the public API names the owner only, so `HazelcastAdapter` reads the partition
  replicas from the internal partition table (`InternalPartitionService`, Hazelcast 5.7.0) and
  `ClusterView.replicaUnresponsive(key, backupCount)` checks owner + first backup (map default backupCount 1).
  Measured locally (iptables DROP, 3 runs): 40 load-tracked tickets + addLoad + release + beat in 65-88 ms, ~106
  writes deferred; loads back to 0 after the member was dropped. Tests: `ClusterServiceTest` (+4),
  `ClusterViewTest` (+1), `ClusterMemberLossIntegrationTest.crashedMemberWhoseAddressVanishedHoldsNoWritesOfPlansOrHeartbeats`.
  Rebased once over the core lane's a29a0a4 (`flightLoadReservationSeconds`, conflict in `createEndpointHandle`,
  kept both). CI green on the branch: 40748318034 (044256e), 40750101323 (ab93926, after the rebase). On `main`:
  package=true run 40751852380 on 0f8eb5e (contains aef0a7a; my first dispatch 40751788500 was cancelled by the
  catalogs lane's push) green, pre-release `server-1.0.0-0f8eb5e5f65c`. Next: D3 step
  2b is optional; D3 otherwise complete - next lane-3 item to be set by the maintainer (or D2 observed gap: force
  the after-first-batch cut path).
- 2026-10-28 (lane 3 session, 7th): lock 20:26Z. D3 step 2b done (9044653, rebase-merged into `main`): the remaining
  waits for a silent member. `claimEndpointLoad` (a backup node moving an endpoint's load reservation) goes through
  `writeOrDefer` on the reservation key, so `claimEndpointLoadLater` completes at once and the end of the stream
  (`FlightSqlProducer` joins the claim before the release) no longer waits; the release of the same key queues behind
  the move and finds the moved reservation. The inventory / ticket-secret maps keep a copy on every member, so their
  puts wait for any silent member: `registerLocalFiles` defers with `ALL_MEMBERS_BACKUP_COUNT`
  (`HazelcastAdapter.ALL_MEMBERS_BACKUP_COUNT` now public), the heartbeat's inventory repair and secret
  `putIfAbsent` are skipped while any replica is silent (adopting the cluster's secret, a read, still runs), and this
  node's own pending inventory counts as published in `hasFileInventory` (else plans failed with "not published").
  `close()` skips the deregistration whose entries sit on a silent member (the others drop the node when its member
  leaves). Tests: `ClusterServiceTest` +3 (all three fail on the old code). Local: `ClusterServiceTest` 50/50,
  `ClusterViewTest`, `ClusterMemberLossIntegrationTest` (iptables, root) 2/2, `DistributedClusterIntegrationTest`,
  `DistributedCompleteIntegrationTest` green. Seen once locally: `ClusterMemberLossIntegrationTest` failed to bind
  a Hazelcast port from `freePort()` (port taken between release and bind, as the plugins lane saw in
  `DistributedClusterIntegrationTest`) - test-side race, not this change; candidate fix: retry the member start on a
  BindException. CI 40764754357 green on the branch (35085cc); on `main` package=true run 40766473788 on 9044653 green
  (it cancelled my push run 40766410121 and the plugins lane's push run 40765524515 on c62e0bb, which 9044653
  contains), pre-release `server-1.0.0-9044653333cd`. Next: D3 step 3 (optional, `fileLocations(false)`), or the D2
  observed gap (force the after-first-batch cut path), or the Hazelcast test-port race; next lane-3 item otherwise
  to be set by the maintainer.
- 2026-10-28 (lane 3 session, 8th): lock 21:28Z. D3 step 3 done (a9f6def, rebase-merged into `main`): plans on local
  disks (`fileLocations(false)`) read the whole inventory map (`entrySet`), which waited for a silent member until
  Hazelcast dropped it. `ClusterService` now keeps a local copy of every inventory (`inventoryMirror`: each full read
  plus the map's entry events, `InventoryListener`) and, while the cluster view knows a silent member, reads the
  inventories on members that answer by key (`readKeys`) and takes those on the silent member's partitions from the
  copy (this node's own from `localInventory`); a member's inventory on a silent partition that was never seen here
  falls back to the full read, so no node's files drop from a plan (files only the silent node holds still fail the
  plan, "No live Flight node owns required shard", now at once). Tests: `ClusterServiceTest` +3 (two fail on the old
  code), `ClusterMemberLossIntegrationTest.crashedMemberWhoseAddressVanishedDoesNotHoldLocalDiskPlans` (iptables; on
  the old code the read waited until the member was dropped). Local: `ClusterServiceTest` 53/53, `ClusterViewTest`,
  `ClusterMemberLossIntegrationTest` 7/7 (root), checkstyle + spotbugs clean. CI 40773266160 green on the branch
  (a17f3d8); on `main` my push run and package dispatch on a9f6def were cancelled by the plugins lane's pushes (same
  concurrency group); Dev CI 40776549317 green on 80aa61a (contains a9f6def); my package=true dispatch 40778284369 on 80aa61a was
  cancelled by another lane's simultaneous dispatch on the same commit, whose run 40778373787 is green: pre-release
  `server-1.0.0-80aa61a15557` contains a9f6def. D3 is complete. Next (no maintainer item yet): the Hazelcast test-port race (retry a member start on a
  BindException from `freePort()`), or the D2 observed gap (force the after-first-batch cut path).
- 2026-10-28 (lane 3 session, 9th): lock 23:28Z. D4 done (256bddd, rebase-merged into `main`; test-only, no
  pre-release): the Hazelcast test-port race. Cause: `freePort()` (`ServerSocket(0)`) returns an *ephemeral* port,
  which any outgoing connection of the JVM (Hazelcast members dialling seeds, gRPC clients) can get as its source port
  before the test server binds it. New `TestPorts.free()` hands out ports below the system's ephemeral range (start
  read from `/proc/sys/net/ipv4/ip_local_port_range`, default 32768; JVM-wide distinct, probed by a bind); used by
  `TestFlightServerHelper`, `ClusterMemberLossIntegrationTest`, `DistributedClusterIntegrationTest`,
  `DistributedCompletePerfTest`, `DistributedTwoProcessTlsTest`. Found on the way: in this container
  `Files.readString` returned the /proc file truncated ("3"), so the first version silently fell back to ephemeral
  ports - now a line read, and `TestPortsTest` asserts the system range is used where readable. Local: those tests +
  `MultiNodeFailoverTest`, `DistributedCompleteIntegrationTest`, `HadoopArrowFlightServerTest` 33/33 green, all
  Hazelcast members on 237xx-238xx; checkstyle clean. CI 40788631312 green on the branch (a488311, before the rebase
  onto core-lane commits). Not changed: `LibsSetTest` and `flight-spark`'s benchmark keep their own `freePort()`
  (other lanes; single servers). On `main` 256bddd has no CI run yet (the push ended in a `[skip ci]` commit; not
  dispatched so as not to cancel the core lane's run 40789295791 on 881bbf7, same concurrency group). Next session:
  confirm a green Dev CI run on `main` containing 256bddd (else dispatch one). Next: D5, or a maintainer item.
- 2026-10-29 (lane 3 session, 10th): lock 00:25Z. Confirmed 256bddd green on `main` (Dev CI 40794638660 on f082b73).
  D5 done (test only, no pre-release; rebase-merged into `main`): `DistributedCompleteCutAfterFirstBatchTest`
  (`integration`, one node, 4 files x 100k rows, `endpointsPerServer=4`, `batchSize=1024`) reads the endpoint
  answers through `CuttingRelay` at 1 MB/s and calls the new `cutNow()` as soon as the client has its first batch:
  the client gets an error after 1024 of 320000 rows (INTERNAL, `arrow_scan: get_next failed(): ... UNAVAILABLE`, not
  INVALID_ARGUMENT), `mergedAnswers()` and `fallbacks()` stay 0, and a later healthy run merges the full answer.
  DuckDB read ~2.3-2.6 MB of ~7 MB before its first emission (read-ahead), so the cut lands mid-stream with margin.
  Mutation check: `stream()` returning true instead of rethrowing after the first batch makes the test fail
  (silently partial answer). Local: 3/3 green, plus `DistributedClusterIntegrationTest`,
  `DistributedCompleteIntegrationTest`, `TestPortsTest`, checkstyle. CI 40795299170 green on the branch (a9e1807).
  Merge push to `main` ends in this `[skip ci]` commit so as not to cancel the other lane's dispatch 40794647875;
  next session: confirm a green Dev CI run on `main` containing the D5 commit. Next: D6 (maintainer item).
- 2026-10-29 (lane 3 session, 11th): lock 01:28Z. Confirmed the D5 commit b693ad2 green on `main` (Dev CI 40798070064
  on 01884e2). The only open item, D6, waits for the maintainer to choose it; wrote the status-code candidate as Q1
  (Open questions, proposal (a) `UNAVAILABLE`) so it can be answered. No code change. Next: D6 / Q1 answer.
- 2026-10-29 (lane 3 session, 12th): lock 02:28Z. D6 done (fd6084f, rebase-merged into `main`): when a merged answer
  fails after the client's first batch, `DistributedComplete.stream` checks the first failure of an endpoint stream
  (recorded by `EndpointAnswers`, `failure()`); if it is `UNAVAILABLE` or has an `IOException` among its causes
  (`peerLost`), the client gets `UNAVAILABLE` "peer lost during distributed complete answer; retry: <peer error>"
  (counter `peersLost()`, log `distributedComplete=peerLost`); any other failure, or a merge that fails without an
  endpoint failure, keeps its error (`INTERNAL`). Tests: `DistributedCompletePeerLostTest` (unit, classifier),
  `DistributedCompleteCutAfterFirstBatchTest` now asserts `UNAVAILABLE` + the text + `peersLost()==1` (it got
  `INTERNAL` before). `EndpointAnswers` counters are `AtomicLong` now (read by the caller after DuckDB's scan thread;
  SpotBugs). README + CHANGELOG (Changed). Local: those + `DistributedCompleteIntegrationTest`,
  `DistributedClusterIntegrationTest` 12/12, checkstyle + spotbugs clean. CI 40807468661 green on the branch
  (cc0867a, before the rebase onto other lanes' commits). On `main` the merge push ends in this `[skip ci]` commit;
  package=true dispatches on `main` were cancelled by other lanes' pushes (same concurrency group): 40808931423 on
  95158856, 40811713278 on f8929cf; Dev CI 40810360247 green on 54181e9 (contains fd6084f). No pre-release with D6
  yet - next session: confirm a pre-release containing fd6084f, else dispatch package=true on `main` when no other
  run is in progress. Next: D7 (perf on 3 containers in CI).
- 2026-10-29 (lane 3 session, 13th): lock 04:26Z. D7 done: new manual workflow `distributed-perf.yml` (14d5cb6, fix
  6ab7764; additive CI tooling, no runtime change, so no pre-release of its own): TPC-H SF10 lineitem (59,986,052 rows)
  in 24 Parquet files on one directory all nodes mount (`dataDirShared`), three server containers of the ref on the
  host network (one Hazelcast cluster, 1 CPU / 4 GB each, `endpointsPerServer=2`), `distributedComplete` off vs on,
  TpchPerfTest against node 1 (warm-up + 2 passes); fails when an answer (rows + checksum, and DuckDB's row count)
  differs or an "on" answer was not merged. Shapes: q01 without its AVG columns (AVG is not mergeable -> D10), q06,
  q12 without the orders join, GROUP BY l_partkey (2M groups), a filtered scan (1.87M rows), DISTINCT, ORDER BY LIMIT.
  Run 40819283390 green (40818498423: same numbers class, its check miscounted the `total` line - fixed in 6ab7764).
  Best pass off -> on ms (on/off): q01 5216 -> 2128 (0.41), q06 1603 -> 773 (0.48), q12 1517 -> 614 (0.40), wide
  GROUP BY 25202 -> 14929 (0.59), filtered scan 3166 -> 2797 (0.88), DISTINCT 1627 -> 786 (0.48), ORDER BY LIMIT 1605
  -> 633 (0.39); 21/21 merged, 0 fallbacks, peers 1.95 GB per 21 answers, peer wait 13 % of merged time (run
  40818498423: 0.39-0.49 for the aggregates, 0.45 wide GROUP BY, 0.95 scan, wait 15 %). Absolute times vary ~1.6x
  between runners; the ratios are stable. So 3 nodes give 2-2.6x on scan-bound aggregates (ideal 3x: fan-out +
  merge), the wide GROUP BY is limited by shipping 2M partial groups through the planning node, and a scan returning
  many rows gains little (the client's one stream is the bottleneck). D2 (b) decision kept: no parallel prefetch -
  at most the 13-15 % peer wait to gain, concentrated in the wide shapes. Local check before CI (3 JVMs, 3M-row
  lineitem-like table): same 6 checksums on vs off, all merged. Package: the pre-release with D6 (fd6084f) is still
  missing - my package=true dispatch 40818496135 was cancelled by the core lane's push cbdc14b; redispatched on
  06833e7 after their run: Dev CI package=true 40821204741 green (it superseded the catalogs lane's push run
  40821054924 on f227d98, which 06833e7 contains), pre-release `server-1.0.0-06833e736572` contains fd6084f (D6). Next: D8 (rolling restart under load), D10 (AVG merge) is additive and small.
- 2026-10-29 (lane 3 session, 14th): lock 05:27Z. D8 done (95b6b01 + 2a2b8e0, rebase-merged into `main`). New manual
  workflow `distributed-restart.yml` + `RollingRestartLoadTest` (`perf`; external cluster): reference answers (rows +
  order-independent checksum, rows checked against DuckDB) on the healthy cluster, then N clients run 7 shapes (q01,
  q06, q12, DISTINCT, ORDER BY LIMIT, a filtered scan in complete and in partial mode) each call on the next node,
  reading endpoints from their locations in order; fails on any wrong answer or a code outside `UNAVAILABLE`. The
  workflow restarts the 3 containers one by one, graceful (`docker stop`) then kill -9, 15 s down, 20 s settle.
  Found by the first local run: a SIGTERM'd planning node cut its running answer with `INTERNAL` (RST_STREAM;
  `stop()` called `server.shutdown()` and closed DuckDB/Hazelcast at once). Fix, opt-in (default 0 = unchanged, Q2
  asks for a default): `shutdownDrainSeconds` / `FLIGHT_SHUTDOWN_DRAIN_SECONDS` - `ClusterService.deregister()`
  (stops the heartbeat, removes the server from the pool, member stays up), `server.shutdown()`, `awaitTermination`
  up to the setting, then the old close order (log `shutdownDrain=done|timeout`). Tests: `ShutdownDrainTest`
  (integration; a running answer read across the drain arrives in full, the drain waits for it - mutation-checked -,
  new calls `UNAVAILABLE`, server out of `liveServerUris`), `ConfigAdapterTest` +1; README + CHANGELOG. Local:
  those + `ClusterServiceTest` 53, `ClusterViewTest`, `DistributedClusterIntegrationTest`,
  `DistributedCompleteIntegrationTest`, `MultiNodeFailoverTest`, checkstyle + spotbugs green; local 3-JVM run with
  the drain: 2 graceful restarts, 0 non-retryable failures (without it: 1 `INTERNAL`). CI: Dev CI 40824909097 green
  on the branch; Distributed restart 40826279722 green on `main` 2a2b8e0 (SF1, 12 files, 3 clients,
  `distributedComplete=true`, drain 20): 0 wrong answers, 464 failures all `UNAVAILABLE` (463 "io exception" = calls
  to the node that was down, 1 "Network closed"), none of the merged answers planned on healthy nodes failed while a
  peer died. Per restart (stop s / start -> "started" s / start -> first ok planned on the node s / last error
  after down s): graceful 1.2-1.7 / 5.0-6.1 / 12.0-12.7 / 26.6-28.4; kill -9 0.1-0.5 / 5.6-6.2 / 12.5-15.6 /
  27.9-30.0. Package=true Dev CI 40826302585 green, pre-release `server-1.0.0-2a2b8e08191f`. Next: D9 (node join
  under load; the restart workflow is the base), D10 (AVG merge), Q2.
- 2026-10-29 (lane 3 session, 15th): lock 06:25Z. D9 done, plus a bug it found, plus Q2/D11 (rebase-merged into
  `main`: 16edbb7 fix, fd96f59 D9, d4a136f D11, 7b988c7 workflow fix). **Bug (fixed, runtime change):** node loads
  leaked for good under concurrent identical queries - endpoint tickets are deterministic, so identical plans share
  one load-reservation key; the second `put` overwrote the first reservation and one release found nothing (the
  expiry listener cannot help: the key was removed). In the in-JVM test loads climbed to MBs per node after a few
  hundred queries and a node got no endpoints at all for > 60 s (`assignWithoutLocality` picks the least loaded).
  Fix in `ClusterService`: a reservation key holds one reservation per planned endpoint (`HandleState` as before when
  one, new `LoadReservations` when several), updated by `putIfAbsent` / `get` + `replace` / `remove(k, v)` CAS loops,
  `setTtl` after a `replace` (Hazelcast 5.7 `replace` drops the entry's TTL - checked); a release takes this node's
  reservation first (the one a claim moved here), a claim moves one. Ticket format unchanged. Tests:
  `ClusterServiceTest` +2 (`identicalConcurrentPlansAreEachReleased`, `claimMovesOneOfIdenticalReservations`; the
  mocks of the reservation calls now use a map), 55/55. **D9:** `NodeJoinUnderLoadIntegrationTest` (integration; 2
  nodes + 1 joining, 3 clients, merged complete / single-node complete / partial read endpoint by endpoint): 0 wrong,
  0 failures, the joined node planned 0.1-0.7 s after its member is up, loads back to 0 after the load (fails on the
  old code), merged answers on 3 nodes; local 8/8. `TestFlightServerHelper` now registers a node after its Flight
  server listens, as `HadoopArrowFlightServer` does (before, a joining test node was planned while its port was not
  open: partial reads failed, merges fell back). `distributed-join.yml` (manual): 3 containers + a 4th that joins
  then leaves (`docker stop`, drain 20); `RollingRestartLoadTest` takes `-Dload.joinServers` / `-Dload.joinFile` and
  prints `location` lines. Run 40834551789 green on 7b988c7 (SF1, 12 files, 3 clients, 0.75 CPU per node): join
  phase 129 ok / 0 errors, leave phase 125 ok + 40 `UNAVAILABLE` "io exception" (all calls planned on the stopped
  node by the load client, retryable), merged 308 answers, 0 fallbacks, 0 peerLost; node 4: inventory registered
  8.1 s after `docker start`, "started" 11.1 s, first endpoint planned on it 11.6 s, first answer planned on it
  19.0 s. (First run 40834082260 failed at startup: a node waits for every member in `--hosts`; fixed in 7b988c7.)
  **D11:** `shutdownDrainSeconds` default 20 (`ConfigAdapter.DEFAULT_SHUTDOWN_DRAIN_SECONDS`), README, START-NOTES
  (`docker stop -t 25`), CHANGELOG (Changed); `ConfigAdapterTest`, `ShutdownDrainTest`,
  `HadoopArrowFlightServerTest` green. CI: Dev CI 40832718976 green on the branch (619e2f9, all three commits);
  package=true Dev CI on `main`: 40834079848 on d4a136f built green but its release step got HTTP 403 ("Resource
  not accessible by integration", a package run 25 min earlier had published); redispatched once: 40835849089 on
  7b988c7 green, pre-release `server-1.0.0-7b988c720821`. Lock released 07:46Z. Next: D10 (AVG merge), D12.
- 2026-10-29 (lane 3 session, 16th): lock 08:25Z (the branch copy of LOCK-LANE3 was stale; main's was released). D10
  done (ab8cc3d + 4edb98d, rebase-merged into `main`): distributed complete mode merges plain `avg`. `AverageMerge`
  rewrites the query with jOOQ into a part query (`sum(x) AS __kestrel_avg_sum_i, count(x) AS __kestrel_avg_count_i`
  per average, no ORDER BY) that `DistributedComplete` plans and reads instead of the client query; the merge sums
  both and divides as DuckDB's avg finalizes: the exact unscaled sum (integer part and fraction scaled apart, no
  DECIMAL overflow) as DOUBLE over count * 10^scale (scale from the endpoint answers' Arrow types; floating-point
  sums over the count; NULL when the count is 0); the complete answer's names, then the client's ORDER BY over them.
  Measured before coding (DuckDB 1.4.1, 5000 groups): naive `CAST(sum AS DOUBLE)/count` differs from avg in 31 % of
  DECIMAL(15,2) groups; this formula 0 differences while the unscaled sum is < 2^53 (DuckDB divides INT64/INT128
  sums in long double, so beyond that ~25 % differ in the last bit) - documented in README like DOUBLE sums.
  Refused (single-node path): avg DISTINCT / FILTER / OVER, `ORDER BY avg(...)`, HAVING, LIMIT on aggregates, joins.
  Partial mode and the wire unchanged (the part query is only used between nodes). Tests: `AverageMergeTest`
  (unit: bit-exact vs DuckDB over 4 endpoint parts for 7 shapes incl. DECIMAL(15,2)/(18,4), BIGINT, DOUBLE, an
  all-NULL group, ORDER BY alias/ordinal; mutation-checked), `DistributedCompleteIntegrationTest` +3 merged AVG
  shapes (DECIMAL column added to its files). `distributed-perf.yml` q01 has its AVG columns again. CI: Dev CI
  40843407670 green on the branch; Distributed perf 40843471013 green on the branch (SF10, 3 x 1 CPU): same
  checksums off vs on, 21/21 merged, full q01 5814 -> 2388 ms (0.41), others as in D7 (0.40-0.54, scan 0.92).
  Package=true Dev CI 40845151877 on `main` b3bbcce (contains the plugins lane's b3bbcce, whose push run 40845149128
  it cancelled, and supersedes the dispatch 40844289066 on cc191fb) was running at session end - next session:
  confirm it green with a pre-release containing ab8cc3d, else dispatch package=true when no other run is in
  progress. Next: D12 (mixed-version reservations during a rolling upgrade).
- 2026-11-02 (lane 3 session, 17th): lock 15:05Z. SERVICE MATRIX, cluster part, on `main` (c84f8065):
  `.github/workflows/service-cluster.yml` (dispatch with input `server` = a pre-release id, else builds the commit;
  push to `main` touching its files) + `tools/ci/service-cluster-data.py` (TPC-H SF1 Parquet, lineitem/orders in 6
  files, DuckDB's 22 queries, reference answers by in-process DuckDB over the same files) +
  `tools/ci/service-cluster-check.py` (ADBC Flight SQL, complete mode: 22 answers compared column names + rows as a
  multiset, floats rel 1e-9; GetObjects / GetTableSchema on every node; one prepared query with a bound parameter on
  every node; `loss` mode: 3 clients for 60 s on nodes 1-2, node 3 `docker kill -s KILL` after 15 s, every answer
  exact or UNAVAILABLE whose retry is exact). Layout: docker network 172.30.0.0/24; HDFS = apache/hadoop 3.4.1
  NameNode + 3 DataNodes (replication 1), each server in its DataNode's network namespace with the DataNode's domain
  socket mounted (short-circuit configured: native libhadoop loads); S3 = MinIO (`pgsty/minio`; the official images
  are gone from Docker Hub/quay, moto is the fallback and the summary names the service); servers in an Ubuntu 20.04
  image with the runner's JDK 21 (our hadoopfs links libssl 1.1), `DUCKDB_HDFS_EXTENSION` / httpfs from ci-assets,
  1 CPU / 3 GB each, `-DendpointsPerServer=2`. Cells: {hdfs, s3} x {balanced, speed, speed-dc, loss(speed-dc)}. Run
  41469353046 (dispatched on 2f307313 = the core lane's hadoopfs fix; it cancelled my push run 41469126621): **8/8
  pass**, 22/22 + metadata + prepared everywhere; speed-dc merged answers on 2 of 3 nodes; loss: hdfs 190 ok, s3 274
  ok, 0 retryable, 0 wrong, 0 errors (fallbacks 2 / 1). Pre-release input path: run 41470187507 on
  `server-1.0.0-fca6512a8046` (before the hadoopfs fix; the
  workflow sets `DUCKDB_HDFS_EXTENSION` explicitly, the setting the maintainer found working) - **8/8 pass** too (prepare downloaded the release assets). Next: D12, D13.
- 2026-11-02 (lane 3 session, 18th): lock 16:00Z. D13 (d) done, (a) measured, on `main` (ff4b3310, db1d4aa6,
  4a9ae387; workflow only, no runtime change, so no pre-release). `service-cluster.yml`: the HDFS cells load hadoopfs
  only through `-Darrowflight.duckdb.extensionDir` (a `<dir>/v1.5.6/linux_amd64/` copy of the ci-assets extensions; no
  `DUCKDB_HDFS_EXTENSION`), as the maintainer's cluster runs; S3 cells keep `DUCKDB_HTTPFS_EXTENSION`. Gate check:
  run 41477414245 on `server-1.0.0-fca6512a8046` (before the core lane's fix) - 4 HDFS cells **fail** (node start
  refused: lockdown `allowed_directories` mismatch, the 2026-11-02 bug), 4 S3 cells pass; run 41479197754 on
  `server-1.0.0-c4cd985f37e3` (contains 2f307313) **8/8 pass**; build runs 41477353037 (ff4b3310) and 41479189384
  (4a9ae387) 8/8 pass, loss cells 0 wrong / 0 errors. HDFS notes now list each DataNode's JMX read counters (the
  image does not log clienttrace, even with log4j set - run 41478120223, dropped again): `ReadBlockOpNumOps ==
  BlocksRead` on all 3 DataNodes in every run (e.g. 328/356/919 block reads, 131/96/321 from a local client), so
  the servers never read short-circuit although it is configured - see D13 (a). Next: D13 (a1), then D12.
- 2026-11-02 (lane 3 session, 19th): lock 17:16Z. D13 (a1) done on `main` (6218df4c, f8ba588a; workflow only, no
  runtime change, so no pre-release). `service-cluster.yml` HDFS cells probe short-circuit before the servers start:
  the Java client inside dn1 writes a 4 MB file (block on dn1), reads it back (byte count checked), and dn1's JMX
  `ReadBlockOpNumOps` is unchanged = short-circuit. Result in every HDFS cell of runs 41489637645 (6218df4c) and
  41490875832 (f8ba588a), both **8/8 pass**: the layout supports short-circuit, the servers never use it
  (`ReadBlockOpNumOps >= BlocksRead` on all 3 DataNodes) -> hadoopfs/libhdfs3, written for the core lane in
  `STATUS.md` Log. Decision: a `::warning` annotation ("short-circuit unused") instead of failing the cell - it is a
  speed issue, and a red cell would hide correctness failures of the matrix; the warning disappears once hadoopfs
  reads short-circuit. My push cancelled the core lane's Dev CI on 48090377 (contained in 6218df4c / f8ba588a); Dev
  CI 41490875833 on f8ba588a was running at session end. Next: D13 (a2) (why ~60 % remote reads: planning vs block
  locations, per query), then D12.
- 2026-11-02 (lane 3 session, 20th): lock 17:56Z. D13 (a2) done on `main` (29903c01, dcb4c942; workflow and CI
  script only, no runtime change, so no pre-release). `service-cluster-check.py`: `check` prints the DataNodes'
  block reads (local / remote client, JMX) over the 22 queries; new `locality` mode plans a lineitem and an orders
  query in partial mode on node 1 (pyarrow.flight), reads every endpoint from its first location and prints the
  DataNode reads per endpoint; the HDFS cells also list which DataNode holds each file and whether a server
  resolves the DataNodes' host names (yes). Found on the way: a DataNode's JMX values are a cache refreshed once
  per metrics period (10 s), so per-query deltas were zero but one (run 41495736199, cancelled by the next push
  after 7/8 cells passed); snapshots now wait 11 s (`SC_JMX_SETTLE`). Run 41496592054 **8/8 pass**: balanced -
  10/10 partial endpoints read only local blocks (94 local, 0 remote), planning on node 1 read 6-10 footer blocks
  (4-8 remote); the 22 complete queries 1299 local / 2682 remote (67 %); speed / speed-dc - endpoints mostly read
  no blocks at all (speed's DuckDB caches), the rest local. So the remote share is complete mode on one node, not
  endpoint planning (D13 (a2) note). Lock released 18:10Z. Next: D12, D13 (b)/(c).
- 2026-11-02 (lane 3 session, 21st): lock 18:26Z. D13 (b) done on `main` (1646ed4f; workflow only, no runtime change,
  so no pre-release). `service-cluster.yml` has a third store `hdfs-ha`: the same 3-node cluster on an HA nameservice
  `hdfs://ns1` (nn1 172.30.0.9 standby / nn2 172.30.0.10 active, shared edits in a bind-mounted directory, manual
  failover, as the core lane's `service-hdfs-cell.sh`); the servers get the nameservice keys in `/conf/hdfs-site.xml`
  (Java client on the classpath and `LIBHDFS3_CONF`), so both clients try the standby first and fail over. Its loss
  cell also fails the NameNodes over (`haadmin -failover nn2 nn1`) 15 s after node 3 is killed. Summary now expects
  12 cells. Run 41500837184 (push) **12/12 pass**: hdfs-ha balanced / speed / speed-dc 22/22 + metadata + prepared;
  hdfs-ha loss: ok 300, 0 retryable, 0 wrong, 0 errors, merged 14 + 27, states after failover nn1=active
  nn2=standby; locality as on plain HDFS (balanced 11/11 endpoints local-only); short-circuit still unused (warning).
  Dev CI 41500837154 on 1646ed4f was running at session end (workflow-only change). Next: D12, D13 (c), the hdfs-ha
  failover follow-up.
- 2026-11-09 (lane 3 session, 22nd): lock 12:53Z. **ITEM 39(d) DONE a952f415** (rebase-merged into `main`; no
  `package=true`, the core lane packages item 39). `lane/distributed` had no common history with `main` (main was
  rewritten after 2026-10-29): its old tip 50038cc9 is kept as `archive/lane-distributed-2026-10-29`, the lane
  branch was recreated from `origin/main` (force-with-lease). Cause of the 2 GiB cap: item 35's admission sized its
  budget from direct memory only (8 GiB -> 64 endpoint streams), but peer streams are read into the root Arrow
  allocator, `arrowAllocatorLimitBytes` default 2 GiB, so eight concurrent full scans of several endpoints filled it
  (`Current allocation: 2147231193`); item 35's CI ran with 2g direct = 2 GiB allocator and could not see it. Fix:
  default budget one stream per 128 MiB of min(direct memory, effective allocator limit incl. parents) - 16 on the
  cluster; start line names both limits. Small-query tails: every merge took one permit per endpoint and queued
  FIFO behind full scans; one-row merges (aggregates without GROUP BY / DISTINCT, AVG included) now take no permits
  and never wait (`admitUnmetered`, `merged ... permits=0`). Not changed: row-returning point queries
  (`point_orders`) still take permits - their size is unknown before they run; if the stand still shows dc tails for
  them, a per-shape charge or a local-execution threshold is the next step (D14). The `INTERRUPT Error:
  Interrupted!` of the stand was not reproduced; it is most likely the cancel that follows an allocation failure (the
  merge cancels its endpoint streams) - recheck on the stand. Tests: `PeerStreamAdmissionTest` (+2),
  `DistributedCompleteIntegrationTest.oneRowMergesDoNotWaitForRoom` (room held, 3 one-row merges exact, 0 waits).
  Dev CI concurrency step now also runs with `-XX:MaxDirectMemorySize=8g` and asserts budget 16: Dev CI
  42621419498 green on the branch (2g and 8g each 48/48 exact, 0 failed executions); push Dev CI 42624035293 on
  a952f415 was running at session end. Next: D14 (stand results), D12, D13 (c).
- 2026-11-09 (lane 3 session, 23rd): lock 14:05Z. **D15 DONE 7c54dfde + ace241be** (rebase-merged into `main`; no
  `package=true`: the core lane packages item 39). Found while adding a dc test for item 39 (a) part 2's session
  schema: partial plans need `schema.table` (`ParquetAdapter.requireSchema`), so under `distributedComplete` every
  query with a plain table name was planned, refused and computed by one node (`distributedComplete=fallback
  reason='Name the table with its schema ...'`), which is how the stand runs TPC-H since item 39 (a). Answers were
  right, nothing merged. Fix: `DistributedComplete.stream` plans from `ExecutionService.qualifyAsResolved(query)`, the
  plain names qualified with the schema DuckDB reads them from here (the one schema with the name, or
  `defaultSchema`); the expected schema still comes from the client's text. The header / session option `schema`
  was already written into the text by item 39 (a) part 2. Test `DistributedCompleteSchemaIntegrationTest`
  (`tpch.orders` next to `orders.orders`, 4 endpoints: header, session option + prepared, default schema; 4 merged
  shapes, each equal to the single-node answer of that schema's table). `service-cluster.yml`: speed-dc cells also
  run the 22 queries with plain names (`SC_PLAIN=1`) and fail on that fallback line. Dev CI 42632283286 green on the
  branch; Service cluster 42632461815 on the branch: 11/12 cells green incl. the three speed-dc with plain names
  (merged 13 + 12 on hdfs-ha), **hdfs-ha loss red twice** (attempt 1 and the rerun of the failed job): NameNode
  failover -> `INTERNAL ... Operation category READ is not supported in state standby` on 2 single-node join answers,
  0 wrong; written as bug-first in `STATUS.md` (D16). Push runs on `main`: Dev CI of ace241be cancelled by the
  catalogs lane's dispatch 42635429611 on 63a3ec52 (includes D15) **green**; Service cluster 42635146358 on ace241be
  **12/12 green** (hdfs-ha loss passed there: the failover bug is intermittent, 2 of 3 runs).
  Next: D16 (after the core lane), D14 (stand results), D12, D13 (c).
- 2026-11-09 15:05-15:30Z (lane 3 session): **D16 DONE 97a2b7bb** (c1e68574 + 97a2b7bb, rebase-merged into `main`).
  The core lane's green run 42638482163 did not show whether `retryOnStandby` fired (notes had no server-log
  counts; artifacts are not downloadable from the session). `service-cluster.yml` hdfs-ha loss: a second NameNode
  failover (nn1 -> nn2, 12 s after the first) in the same 60 s load, and per server `standby: retried N, retry
  failed N, standby lines N; first: ...`. Service cluster on the branch: 42641106647 12/12 green (hdfs-ha loss: ok
  213, 0 errors, retried 0 - both windows missed the queries; the 1 standby line per server was the Java client's
  `RetryInvocationHandler` failover), 42642657223 12/12 green (hdfs-ha loss: ok 189, 0 errors, srv1 `standbyRetry
  run=2 delayMs=500` once, answered). With the core run: 3 green runs, 5 failover windows, 0 errors (before the fix
  red in 2 of 3). Workflow-only change (no Dev CI needed); the merge push to `main` ended on a `[skip ci]` commit, so no push run.
  Next: D14 (stand results), D12, D13 (c).
- 2026-11-09 19:03-19:50Z (lane 3 session): lock 19:03Z. D14 waits for the stand (item 39 rerun), so took D13 (c).
  **D13 (c) DONE** (74971b9a + 60c36431, rebase-merged into `main`): `service-cluster-check.py loss` runs one more
  client that streams `SELECT l_orderkey, l_linenumber FROM tpch.lineitem` (merged under dc) at a fixed row rate
  and checks rows + sums against `expected/scan.txt` (new, `service-cluster-data.py`); `peer lost` UNAVAILABLE after
  the first batch is counted (`scan_peer_lost`) and retried on the other node. Service cluster on the branch:
  42680042903 (250k rows/s) 12/12 green, peerLost in hdfs loss only (after 5767168 of 6001215 rows, retry exact);
  42681427105 (150k rows/s) 12/12 green, peerLost in s3 and hdfs-ha loss (after 4325376 / 4259840 rows, retries
  exact; first time CI saw the D6 path outside unit tests), hdfs loss had read node 3's endpoints before the kill.
  0 wrong, 0 non-retryable over both runs. Test/workflow only (no Dev CI needed, no `package=true`). D12 decided
  acceptable (see D12). Lock released. Next: D14 (stand results).
- 2026-11-09 20:03-20:40Z (lane 3 session): lock 20:03Z. D14 still waits for the stand (no stand results after the
  maintainer's 16:30 note), so took the D13 (b) follow-up. **D13 (b) DONE** (3a595503 + dd9090a8, rebase-merged into
  `main`): the `hdfs-ha loss` cell snapshots each NameNode's JMX `NameNodeActivity` (FileInfoOps / GetBlockLocations /
  GetListingOps, new `service-cluster-check.py nnsnap|nnreads`) at load start and right after each failover; notes
  per window, warning `NameNode not reached` when a window's active NameNode answered none (not a cell failure). The
  logic lives in the Python script because the cell's `run` block hit GitHub's 21000-character expression limit
  (dispatch refused 422) - keep new cell logic out of the workflow. Service cluster on the branch: 42689843545 12/12
  green (snapshot after the HA probe blurred the windows: nn1 829 reads "while standby"), 42691698496 12/12 green
  with the snapshot before the probe: nn2 active 5965/5955/5, nn1 40/41/0; after failover 1 nn1 active
  2951/2924/20, nn2 60/49/0; after failover 2 nn2 active 2680/2678/2, nn1 1/0/0 (residue = the ~1 s between
  failover and snapshot); 244 answers, 0 wrong, 0 errors. Under `speed` listings stay cached (GetListingOps ~0)
  but every file open reaches the active NameNode. Test/workflow only (no Dev CI, no `package=true`). Lock
  released. Next: D14 (stand results).
- 2026-11-10 04:03-04:15Z (lane 3 session): lock 04:03Z. D14 still waits for the stand: item 39 is not packaged yet
  (core lane's package Dev CI 42751144566 running at 04:03Z), so no stand rerun exists. Wrote D14's optional part as
  Q3 (default of `arrowAllocatorLimitBytes` from direct memory); no code, no CI. Lock released. Next: D14 (stand
  results of the item 39 pre-release), Q3 answer.
- 2026-11-10 07:04Z (lane 3 session): no change. ITEM 39 is packaged (`server-1.0.0-894afb9b48c4`, core lane 06:34Z),
  but no stand results on it yet; D14 waits for them, Q3 still unanswered. No code, no CI, lock not taken.
- 2026-11-10 08:08-09:20Z (lane 3 session): ITEM 43 DONE 502e295d. Lane branch restarted from `origin/main` (its old
  content was all merged). (1) `PeerStreamAdmission.admitOnProbation` / `Admitted.charge`: merges expected small
  (equality filter without OR, or LIMIT <= 10 000 without aggregate) open their streams without permits and take
  them once they read > 16 MiB (`EndpointAnswers` byte meter); at most max(32, 8 x permits) on probation. Service
  cluster 42781188876: 0 admission waits, but dc point q/s 0.27-0.57 of speed (fan-out cost). (2) New setting
  `distributedCompletePointLookups` (`local` default / `merge`): point lookups (no aggregate or one-row aggregate) are
  computed on the planning node when it can read the files; top-N LIMITs stay merged (DistributedTwoProcessTlsTest
  caught it, Dev CI 42784392012). (3) `service-cluster.yml`: speed and speed-dc cells run 32 clients of
  `tools/ci/point-query-load.py` on node 1; the summary fails when dc q/s < 0.8 x speed. Final: Service cluster
  42786623655 green (ratios 0.84 / 0.98 / 0.98), Dev CI 42786620428 green. Q4 (default `local`). Not packaged: the
  core lane's next package carries it (STATUS note on item 40(b)). Lock released. Next: Q3 / Q4 answers, D14 after
  the stand's rerun.
- 2026-11-10 10:03-10:35Z (lane 3 session): lock 10:03Z. **D17 DONE** (workflow/test only, no Dev CI or
  `package=true` needed). Orchestrator flagged Service cluster on `main` 42789267919 (502e295d) red: all 13 cells pass,
  only the summary's item 43 gate (dc / speed point q/s >= 0.8) failed with hdfs 0.62, hdfs-ha 0.78; srv1 showed 0
  admission waits, 0 probation merges. The branch run of the same code had 0.84 / 0.98 / 0.98; a rerun on `main`
  (42796003792, green) gave 1.06 / 1.10 / 0.81 and the branch run 42796297705 1.77 / 0.88 / 0.76 - cross-runner noise,
  not a dc cost (the local path adds only two parses). Fix: point load moved to `tools/ci/service-cluster-point.sh`
  (the cell's run block was over GitHub's 21000-character limit again, dispatch 422); the speed-dc cell fails when a
  point lookup on srv1 was merged or waited for admission; the summary ratio warns < 0.8 (s3 0.76 warned in
  42796297705), fails < 0.5 (pre-item-43 fan-out was 0.27-0.57). Branch Service cluster 42796297705 green, merged
  into `main`. Lock released. Next: Q3 / Q4 answers, D14 after the stand's rerun.
- 2026-11-10 12:04-12:35Z (lane 3 session): lock 12:04Z. No open item to take: D14 waits for the stand's rerun on
  the item 39/43 pre-release, Q3 / Q4 unanswered. Found: `service-cluster.yml`'s push trigger did not cover D17's
  `tools/ci/service-cluster-point.sh` (nor `point-query-load.py`), so D17's merge never ran the matrix on `main`.
  Fixed (33d4d2dc, workflow only): the trigger also takes `tools/ci/service-cluster-*.sh` and
  `tools/ci/point-query-load.py`. Service cluster 42812785652 on `main` green (13/13 cells + summary: first `main` run
  with the D17 point gate). Lock released. Next: Q3 / Q4 answers, D14 after the stand's rerun.
- 2026-11-10 15:04-15:16Z (lane 3 session): lock 15:04Z. No open item (Q3 / Q4 unanswered, D14 waits for the stand).
  Guard check only, no code: the catalogs lane's 40(b) pruning cuts (3a01265e, 4228c733, 135e6dac) changed the
  point-query path that item 43's dc `local` point lookups take, with no Service cluster run since 33d4d2dc. Dispatched
  Service cluster 42840719877 on `main` (0b467b80): 13/13 cells + summary green, dc / speed point q/s hdfs 1.12, hdfs-ha
  0.94, s3 0.82, no warnings. Lock released. Next: Q3 / Q4 answers, D14 after the stand's rerun.
- 2026-11-10 (lane 3 session): ITEM 45 DONE b8613fc4 (branch commit ff9d1b3b, fast-forwarded into `main`).
  `DistributedComplete.POINT_EQUALITY` now accepts typed literals (DATE / TIME / TIMESTAMP[ WITH TIME ZONE] /
  INTERVAL '...'), `[TRY_]CAST(<literal> AS <type>[(p[, s])])`, numbers with exponent, and literals in parentheses;
  `CAST(<column> AS ...)`, `>=` / `<=` / `<>` and ORs still merge. Test `typedLiteralsArePointLookups` (12 positive,
  6 negative shapes through `ParquetQueryParser`). CI: `point-query-load.py` takes `POINT_QUERY_CLASSES` (default
  unchanged) and a `point_date` class; `service-cluster-point.sh` runs it with `point_date`, so the deterministic
  speed-dc gate (0 merged, 0 waits) covers the DATE form. Dev CI 42901100322 green, Service cluster 42901104843
  12/12 green (numbers above). No `package=true`: the core lane's next pre-release carries it.
- 2026-11-11 10:03-10:10Z (lane 3 session): no open item (Q3 / Q4 unanswered, D14 waits for the stand's rerun). Guard
  check only, no code: Service cluster 42955011424 on `main` (29f1267b: item 44 (2) table naming in
  `ExecutionService`, hadoopfs afc8 default) green, so the dc paths are covered on the morning build
  `server-1.0.0-29f1267b995f`. Lock not taken (no work). Next: Q3 / Q4 answers, D14 after the stand's rerun.
- 2026-11-11 18:03-18:20Z (lane 3 session): no open item (Q3 / Q4 unanswered, D14 waits for the stand's rerun). Guard
  check only, no code: item 46 (c96a1272, `DuckDbSizing` connection limits) touches the dc merge path; Service cluster
  43074009015 on `main` (74464701, dispatched by another lane) 13/13 cells + summary green, deterministic speed-dc
  point gate passed. Ratio warnings only: dc / speed point q/s hdfs 0.71, hdfs-ha 0.79, s3 0.67 - dc 31.9-36.8 q/s is
  within its earlier range (31-49), speed was high (45-55): cross-runner noise as in D17. Watch: dc point q/s < 30 in
  later runs. Lock not taken (no work). Next: Q3 / Q4 answers, D14 after the stand's rerun.
- 2026-11-12 03:03-04:10Z (lane 3 session): lock 03:03Z. The 2026-11-11 18:07Z session had implemented item 48 and Q3
  on `lane/distributed` (CI green: Dev CI 43079501100, Service cluster 43077850882) but ended before merging and
  logging; its lock had expired. Rebased the branch on `main` (CHANGELOG conflict with Q-40b-1, kept both). Q-40b-1
  (2f31c071) turns footer pruning off for a local, unshared `dataDir` unless set, which item 48 relies on: the
  in-process test now sets `fileStatsPruningMaxFiles` (97ad4f22), CHANGELOG notes it; the cluster stores are
  unaffected. Dev CI 43145794759 and Service cluster 43145797836 (13/13 + summary) green on the branch;
  fast-forwarded into `main` = ITEM 48 DONE 97ad4f22, Q3 DONE. `package=true` Dev CI dispatched on `main` (Q3 changes
  a default). Lock released. Next: the stand's rerun of the dc small-query mix on that pre-release.
