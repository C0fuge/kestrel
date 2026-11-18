# Lane 5 (temporary) - see CLAUDE.md "Fifth and sixth lanes"

## Next steps

> **Lane 5 next step (2026-11-18T04:40Z):** waiting for the maintainer. Package run 44231671868 green; pre-release
> `server-1.0.0-5f02e5b35611` (`main` 5f02e5b3) is the one for the real-cluster rerun of 55 (a) (goal p99 <= 1.5x
> speed, scans >= 0.9x; CI-cell numbers in the 01:20Z note below). When the maintainer's numbers meet the goal write
> `ITEM 55(a) DONE <sha>` and `LANE 5 DONE`; otherwise look at the cell they name. Until then later sessions stop early
> (first check that the latest full Dev CI on `main` touching lane 5 code is green).

> ~~**Lane 5 next step (2026-11-18T03:40Z):**~~ (done: package green) FIRST read the package run 44231671868 (`package=true` on branch
> `pkg/5f02e5b3` = `main` 5f02e5b3, the newest code commit; includes the 55 (a) defaults a3a7f029). The earlier package
> run 44214383193 was cancelled at 01:25Z by lane 4's push to `main` (same concurrency group), hence the `pkg/` branch;
> `main`'s push run 44215691965 on 5f02e5b3 is green. Red = fix it first; green = its pre-release is the one for the
> maintainer's real-cluster rerun of 55 (a) - say so in a `STATUS.md` Log line so the core lane / maintainer see it.
> The rest of the 01:20Z note below still holds.

> ~~**Lane 5 next step (2026-11-18T01:20Z):**~~ (package run cancelled, redone as 44231671868) FIRST read the package run 44214383193 (`package=true`, `main` 55adfbfa,
> includes the defaults commit a3a7f029; its push run was skipped by a `[skip ci]` head); red = fix it first, green =
> its pre-release is the one for the maintainer's rerun. Then the
> item waits for the **maintainer's real-cluster rerun** of item 55 (a) on a pre-release with the new defaults (goal p99
> <= 1.5x speed, scans >= 0.9x). On the CI cell (DC tail perf 44210328551, 3 x 1 CPU, SF1, `fix/55a` = defaults
> commit) the goal is met: p99 dc / speed ms - c1 sum_date 74 / 82, avg_date 72 / 81, limob 85 / 90; c8 sum_date 387 /
> 389, avg_date 355 / 395, limob 521 / 790; q/s dc >= speed (sum_date c8 45.5 vs 37.5, limob 27.4 vs 19.2); point /
> count_all / agg_range equal; lineitem scans MB/s dc / speed x1 435 / 484 and 398 / 437 (0.90x), x4 412 / 416, x8
> 439 / 414 (export 217 / 249, 0.87x - not in the goal). When the maintainer's numbers meet the goal write `ITEM 55(a)
> DONE <sha>` and `LANE 5 DONE` (backlog empty); otherwise look at the cell they name. Nothing else open in this lane.

> ~~**Maintainer, 2026-11-18 03:45 - answers + one red run.**~~ (done 2026-11-18T01:30Z: red run was SpotBugs, fixed by lane 6; both defaults changed) **Q-55a-nl: default false** (merge plans live for ms; tails matter more than cross-coordinator balancing of them). **Q-55a-mc: take your recommended default** (refused addresses still checked every call, signed identity header every call). FIRST: full Dev CI 44207492475 dispatched on `main` f06e6f25 at 00:24Z failed in the build step with `Cluster join timeout after 60s: only 1 of 3 nodes connected` - check whether the member-check / load-tracking changes can delay a join or it is a runner flake (rerun once); fix if ours. Then the real-cluster rerun is the maintainer's: write `ITEM 55(a) DONE <sha>` with the CI cell numbers once p99 / scans meet the goal in the cell.

> ~~**Lane 5 next step (2026-11-18T00:30Z):**~~ (done: dc-nl run 44207273404 was cancelled, measured with the new defaults instead) FIRST check full Dev CI 44203849956 (push, `main` 9866bb15: green at
> 00:22Z) is still the latest lane-5 run; red = fix it first. Restart `fix/55a` with `git checkout -B fix/55a origin/main`.
> Then read DC tail perf 44207273404 (`fix/55a` = 9866bb15, variants speed / dc / dc-nl, dc-nl =
> `-DdistributedCompleteMergeLoadTracking=false`): if dc-nl's c8 p99 of sum_date / avg_date / limob is clearly below dc's
> (this session's 44203845486, same code: dc 692 / 716 / 1209 ms vs speed 608 / 510 / 906; c1 179 / 113 / 140 vs 85 / 87
> / 103; scans dc x1 / x4 / x8 267 / 295 / 306 vs speed 300 / 266 / 275 MB/s), add the numbers to Q-55a-nl in STATUS.md
> (recommend false) - the default stays true until the maintainer answers. Note when reading: a push to `fix/55a`
> cancels a running dispatched DC tail perf (same concurrency group) - dispatch perf runs only after the last push.
> Remaining at c8 on the 1-CPU cell: `plan` p50 130-200 ms and `open` 140-210 ms of the merged shapes; at c1 the p99
> outliers (sum_date 179 vs 85). Then ask for the real-cluster rerun (goal p99 <= 1.5x speed, scans >= 0.9x).

> ~~**Lane 5 next step (2026-11-17T23:15Z):**~~ (done: CI green; (a) answered - the extra addLoad were batch-collected calls logged under the same tag; (b) implemented as an option) FIRST read full Dev CI 44196365448 (push, `main` 49f44a18: plan write
> batch); red = fix it first. Restart `fix/55a` with `git checkout -B fix/55a origin/main`. Step (2) of the earlier note
> is answered: on the 1-CPU CI cell the cluster writes ARE the c8 planning cost - `hazelcast.planWrites` (3 endpoint
> reservations + one load change per node) p50 ~90 / p99 ~250 ms at c8, 1-2 ms at c1 (DC tail perf 44194780093);
> running them side by side on a pool made it worse (44193153607: c8 p99 limob 1386 ms), so they run serially on the
> planning thread. Next: (a) `hazelcast.addLoad` counts ~5.7 per merged query on node 1 with 3 endpoints although a plan
> now writes <= 3: find the other callers (claims? the peers' endpoint-internal plans?); (b) consider not
> load-tracking merge-internal endpoints at all (the coordinator reads them at once and releases them within ms; the
> snapshot already counts this node's own planned load) - that changes load balancing between nodes, so as an optional
> setting with default off, or ask the maintainer (Open question); (c) then the real-cluster rerun (p99 <= 1.5x speed).
> Current CI-cell numbers (44194780093, a faster runner than earlier runs: point qps ~2x): c8 p99 sum_date / avg_date /
> limob 457 / 466 / 679 ms, c1 p99 91 / 86 / 96, scans x1 / x4 / x8 380-405 / 377 / 426 MB/s. A speed variant was not
> run in this session; compare speed and dc in one run next time (`variants` empty = speed, dc, dc-4ep).

> ~~**Lane 5 next step (2026-11-17T21:50Z):**~~ (done: CI 44180696125 green, step (2) tried) FIRST read full Dev CI 44180696125 (push, `main` 66b19368: cluster
> snapshot cache, file-plan refresh-ahead, optional member-check memory); red = fix it first. `fix/55a` still holds
> the pre-rebase commits: restart it with `git checkout -B fix/55a origin/main` (force-with-lease) instead of rebasing.
> State of 55 (a) on the CI cell (DC tail perf 44178943268, 3 x 1 CPU, SF1, timing): c8 p99 dc / dc-mc / speed -
> sum_date 512 / 405 / 410, avg_date 568 / 463 / 479, limob 721 / 639 / 896 ms (was 1697 / 1592 / 2203 vs ~510 /
> 510 / 1008 in 44174076316); c1 p99 sum_date 131 / 165 / 84, avg_date 92 / 81 / 85, limob 97 / 93 / 97 (c1 p99 is
> a few outliers of ~300 ms: check what they are, e.g. GC or the first plan after the snapshot / file-plan refresh);
> scans dc / speed MB/s x1 327-378 / 383-387, x4 358 / 344, x8 363 / 345. dc-mc = `distributedCompleteMemberCheckMillis=250`,
> default 0 until the maintainer answers Q-55a-mc in STATUS.md. Remaining in the merged-shape phases at c8: open
> (endpoint execution on the peers) 93-195 ms p50, plan 40-106 ms p50 (`hazelcast.addLoad` per endpoint: next
> step (2) of the earlier note - does a merge-internal plan need the synchronous load write? check
> `claimEndpointLoadLater` first). Then ask the maintainer for a real-cluster rerun (p99 <= 1.5x speed).

> **Lane 5, 2026-11-17T20:10Z: done** - the item 35 check now runs with
> `distributedCompleteSmallAnswerEndpointsPerNode=0` (one node: 55 (a) computes one-row answers there alone, as
> `DistributedCompleteSchemaIntegrationTest` already did); full Dev CI 44162578438 on `main` 4662fa4f green.
> ~~**Maintainer, 2026-11-17 22:45 - FIRST: `main` is red after the 55(a) merge.** Full Dev CI 44154778833 on `main` dfaac51c, step 'Distributed complete under concurrency (items 35, 39 (d))', direct 2g: `48 of 48 exact; merged 32, fallback 0, admission waits 29` -> `only 32 of 48 answers merged from endpoint answers`. Either 55(a) (one endpoint per node for merged one-row aggregates / small LIMITs) changed what counts as a merge - then update the check's expectation in `.github/workflows/dev-ci.yml` with the reason - or merges are really lost under concurrency (29 admission waits) - then fix it. Answers were exact. Get the full Dev CI on `main` green before anything else.

> **Maintainer, 2026-11-17 19:20 - ITEM 55(a): dc tails (moved here from lane 3, which works on 55(b)).** Real cluster run 2026-11-16 on `server-1.0.0-d2e27119fda4` (3 nodes, 80 cores / 754 GB, server on 16 cores, -Xmx8g, direct 8g, HDFS Kerberos, DataNodes `authentication,privacy`, direct SASL reads, TPC-H SF10 + a 1.27 TB table; full context in `STATUS.md` item 52 and `LANE3-STATUS.md` item 55).
> dc vs speed: same q/s on the point load (0.94-1.10), but small-query p99 c1 287 ms vs 36, c8 1125 vs 85, c32 4825 vs 755
> (p50 equal or better), and raw Flight scans of tpch.lineitem with 4 / 8 concurrent streams 1154 / 964 vs 1975 / 2171 MB/s
> (1 stream 646 vs 722). Find the tail source under dc (peer stream setup per query, merge queue, admission waits, Hazelcast
> lookups, a periodic stall such as the 30 s file inventory refresh?) with per-query timing on the CI cluster cell (3 servers,
> HDFS SASL) and fix it; goal p99 <= 1.5x speed and scans >= 0.9x speed. Branch `fix/55a`. Write `ITEM 55(a) DONE <sha>` with numbers.

> ~~**Lane 5 next step (2026-11-17T20:10Z):**~~ (done: CI green, (1) done, tail found in file-plan expiry and member checks) FIRST read full Dev CI 44166060698 (push, `main` cf9f2d29, the inventory cache); red = fix it first. Then: the merged-shape tail is planning, then opening the endpoints. DC tail perf
> 44163827273 (`variants=dc=true:`, `timing=true`, 3 x 1 CPU, SF1): at c8 the merged line's phases (p50/p99 ms) are
> plan 102/1175, open 119/285, merge 11/168, everything else < 1 ms; inside plan `planning.validateServers` 48/178
> (was 88/251 before this session's inventory cache), `hazelcast.allServerLoads` 16/111, `hazelcast.filterLiveServers`
> 15/107, `hazelcast.addLoad` 4/124 (x2-3 per plan), endpoint release `hazelcast.subtractLoad` 13/177. At c1 all of
> them are 1-5 ms p50, so it is CPU queueing of the Hazelcast calls on the saturated 1-CPU containers. Next: (1) take
> the cluster snapshot (registry loads + live servers) of a merge-internal plan from a short-lived local cache
> (~250 ms, plus this node's own pending load) instead of two cluster-map reads per plan; (2) look at addLoad /
> reservation writes per endpoint for merge-internal plans (the coordinator reads the endpoints at once: does the load
> reservation need a synchronous write?) - check the failover claim path (`claimEndpointLoadLater`) before changing
> it; (3) `open` (endpoint execution on the peers, ~120 ms p50 at c8) is DuckDB CPU on the same saturated nodes.
> Then rerun DC tail perf with defaults (speed, dc, dc-4ep) and ask for a real-cluster rerun (p99 <= 1.5x speed).
> Workflow tip: `-f inputs[timing]=true -f inputs[variants]=dc=true:` prints p50/p99 per Timing tag per merged class.

> ~~Lane 5 next step (2026-11-17T19:10Z):~~ (done: CI read, item 35 fixed, timings added) read the full Dev CI 44154760020 (push) and the package run 44154778833 on
> `main` dfaac51c; a red run is the first thing to fix. Then the remaining tail: merged one-row aggregates / ORDER BY
> LIMIT on the CI cell are still 2-3.5x speed (sum_date c1 p99 201 vs 84 ms, c8 1789 vs 508; client p50 77 vs merge
> p50 50, so ~25 ms is spent before the merge: localReason / prunedHere / planner.plan / completeSchema - add
> `LogUtil.logTiming` around them and print them in dc-tail-perf, artifacts cannot be downloaded from sessions, so
> put the numbers in the job log). At c8 ~30 % of merged-shape queries logged no decision line (one endpoint after
> load balancing -> computed here): check. Then a real-cluster rerun for the item's numbers (p99 <= 1.5x speed).

## Log

- 2026-11-17T15:15-16:40Z (lane 5, session 1, no log written then; reconstructed): on `fix/55a` - merged plain scans
  forward endpoint batches without DuckDB (`distributedCompleteForwardScans`), dc's local size rule takes file sizes
  from the catalog views instead of listing the table per query, new workflow `dc-tail-perf.yml`. Dev CI 44123833392
  green, DC tail perf 44123837246: count_all / agg_range / point classes under dc now equal speed.
- 2026-11-17T18:36-19:10Z (lane 5): merged one-row aggregates and small LIMITs planned with one endpoint per node
  (`distributedCompleteSmallAnswerEndpointsPerNode`, default 1, 0 = endpointsPerServer as before). DC tail perf
  44150307344 (3 x 1 CPU, SF1, 12 files), dc vs dc-4ep (old) vs speed: sum_date c1 q/s 12.2 / 5.7 / 27.0, p99 201 /
  478 / 84 ms; c8 p99 1789 / 1855 / 508; limob c1 p99 139 / 384 / 103; lineitem scans dc / speed x1 263 / 289, x4
  282 / 261, x8 296 / 272 MB/s (>= 0.9x). Fast tier 44151537756 green (all flight-server tests). Merged `fix/55a`
  into main (dfaac51c, includes session 1's commits); full Dev CI 44154760020 and package 44154778833 dispatched,
  results for the next session. Item 55 (a) not done: merged small answers still 2-3.5x speed's p99.
- 2026-11-17T19:07-20:15Z (lane 5): package run 44154778833 on `main` dfaac51c red in the item 35 step (32 of 48
  merged: one-row answers on one node are computed there since 55 (a)) -> the step sets
  `distributedCompleteSmallAnswerEndpointsPerNode=0`. New: `phasesUs=` on `distributedComplete=merged` lines, DEBUG
  `distributedComplete=local reason='1 endpoints'` (the c8 merged-shape queries without a decision line: load balancing
  put the plan on one node), dc-tail-perf `timing` input. Found the tail in planning (Hazelcast reads per plan under
  CPU load); on shared storage the planner now remembers each member's published inventory. CI: fast tier 44155722752
  and 44163831594 green, DC tail perf 44155712825 (all variants, phases), 44162414933 / 44163827273 (dc, timing,
  before / after the inventory cache), full Dev CI 44162578438 on `main` 4662fa4f green. Merged `fix/55a` into main.
  Item 55 (a) not done: c8 p99 of merged one-row aggregates still ~1.1 s vs speed ~0.6 s on the 1-CPU CI cell.
- 2026-11-17T20:36-21:50Z (lane 5): full Dev CI 44166060698 on `main` cf9f2d29 green. Timing run 44171909394 showed
  three remaining tail sources: (a) the merge plans' registry + heartbeat reads, (b) the same reads in the internal
  calls' member address check on every peer call (`InternalCallAuthenticator.memberAddressCheck`), (c) the 30 s file
  plan expiry, at which all concurrent plans reloaded together (`planning.fileLocations` p99 1.2-2.1 s at c8). Fixed:
  merge plans reuse a cluster snapshot for `distributedCompleteClusterSnapshotMillis` (default 250 ms, plus the load
  planned since; one plan renews it at a time), one plan reloads a file plan in the last 3 s of its TTL, new optional
  `distributedCompleteMemberCheckMillis` (default 0; Q-55a-mc). Fast tier 44171418695, 44174010663, 44178948114
  (all 1386 flight-server tests) green; DC tail perf 44174076316 (before the file-plan fix) and 44178943268 (all
  fixes, numbers in Next steps). Merged into `main` 66b19368; full Dev CI 44180696125 pending. Item 55 (a) not done:
  c8 p99 now <= 1.25x speed on the CI cell, c1 p99 1.5x (outliers); real-cluster numbers pending.
- 2026-11-17T22:43-23:15Z (lane 5): full Dev CI 44180696125 on `main` 66b19368 green. Plan write batch
  (`ClusterService.writeBatch()`): a plan's endpoint load reservations and its added load (one summed change per node)
  are written when the plan is made, before it returns. Side by side on a pool first (fast tier 44193148712 green,
  DC tail perf 44193153607: c8 p99 worse, planWrites p50 ~95 ms), then serially on the planning thread (fast tier
  44194776591 green, DC tail perf 44194780093: c8 p99 457 / 466 / 679 ms sum_date / avg_date / limob, no
  regression). Merged into `main` 49f44a18; full Dev CI 44196365448 pending. Item 55 (a) not done.
- 2026-11-17T23:41-2026-11-18T00:30Z (lane 5): full Dev CI 44196365448 on `main` 49f44a18 green. Step (a): the ~5.7
  `hazelcast.addLoad` per merged query were the planner's per-endpoint calls collected by the write batch (no cluster
  write, ~0 ms) plus the batch's per-node writes, logged under the same tag; collected ones now log `batch.addLoad`.
  Step (b): new optional `distributedCompleteMergeLoadTracking` (default true = unchanged; false = merge plans write no
  reservations / added load and release nothing, the load counts in this node's snapshot only; Q-55a-nl). Fast tier
  44201981927 green; DC tail perf 44203845486 (speed / dc / dc-4ep, numbers in Next steps); dc-nl run 44207273404
  pending. Merged into `main` 9866bb15; full Dev CI 44203849956 green. Item 55 (a) not done.
- 2026-11-18T00:42-01:30Z (lane 5): full Dev CI 44207492475 on `main` f06e6f25 was red in SpotBugs (NP in lane 6's
  `ParquetQueryParser` key functions; the "Cluster join timeout" there is an expected test exception, not a join
  problem); fixed on `fix/55a`, lane 6 pushed the same fix (6c8f66de) meanwhile, so theirs was kept. Defaults per the
  maintainer: `distributedCompleteMergeLoadTracking` false, `distributedCompleteMemberCheckMillis` 250. Full tier on
  `fix/55a` 44210325758 green (before the rebase), fast tier on the rebased tip 44212641334, DC tail perf 44210328551
  (speed / dc, timing; goal met on the CI cell, numbers in Next steps). Merged into `main` a3a7f029; package run 44214383193 dispatched on `main` 55adfbfa (pre-release for the real-cluster rerun). Item 55 (a) waits for the
  real-cluster rerun.
- 2026-11-18T03:36-03:40Z (lane 5): package run 44214383193 was cancelled by lane 4's push to `main`; `main`'s push
  run 44215691965 (5f02e5b3, includes 55 (a)) green. Re-dispatched `package=true` on `pkg/5f02e5b3` as 44231671868
  (pending). Item 55 (a) waits for that pre-release and the maintainer's real-cluster rerun.
- 2026-11-18T04:35-04:45Z (lane 5): package run 44231671868 on `pkg/5f02e5b3` green (build, service gate,
  package); pre-release `server-1.0.0-5f02e5b35611` is the one for the maintainer's real-cluster rerun (noted in
  `STATUS.md` Log). Item 55 (a) waits for that rerun.
