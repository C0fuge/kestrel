# Lane 3 - distributed (branch `lane/distributed`)

See `CLAUDE.md`, "Third and fourth lanes". Moved here from `STATUS.md` stage 3 on 2026-10-28 (maintainer).

## Next steps

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
D12. Candidate (found in D9, needs no approval: test/infra only unless it changes behaviour): mixed-version clusters
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
