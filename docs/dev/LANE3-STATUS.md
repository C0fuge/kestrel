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
D5. (open, no maintainer item yet) D2 observed gap: force the after-first-batch cut path of distributed complete mode
    (a cut after DuckDB emitted, e.g. a larger wide projection with a later cut) and assert the error, not "error or
    exact".

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
