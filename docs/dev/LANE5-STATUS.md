# Lane 5 (temporary) - see CLAUDE.md "Fifth and sixth lanes"

## Next steps

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

> **Lane 5 next step (2026-11-17T20:10Z):** FIRST read full Dev CI 44166060698 (push, `main` cf9f2d29, the inventory cache); red = fix it first. Then: the merged-shape tail is planning, then opening the endpoints. DC tail perf
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
