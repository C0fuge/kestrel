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
    - (c) TLS + a real auth provider between two processes (separate JVMs, e.g. a test that starts
      `HadoopArrowFlightServer` twice with `FLIGHT_TLS_*` and the token provider). ~~Failover to an endpoint's
      backup location before the single-node fallback~~ *done 2026-10-28 (see Log).*
    - Observed: a peer stream cut mid-way hit before the first client batch in both cut tests (DuckDB's arrow scan
      reads peers before emitting), so the after-first-batch error path is only asserted as "error or exact",
      not forced; forcing it needs a cut after DuckDB emitted, e.g. a larger wide projection with a later cut.
D3. Faster detection of a crashed member (`STATUS.md` Open question, item 0d follow-up): plan from a local cluster snapshot
    refreshed by the heartbeat thread instead of blocking map reads; keep `hazelcast.max.no.heartbeat.seconds` as is.

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
