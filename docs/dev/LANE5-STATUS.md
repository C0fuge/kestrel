# Lane 5 (temporary) - see CLAUDE.md "Fifth and sixth lanes"

## Next steps

> **Maintainer, 2026-11-17 22:45 - FIRST: `main` is red after the 55(a) merge.** Full Dev CI 44154778833 on `main` dfaac51c, step 'Distributed complete under concurrency (items 35, 39 (d))', direct 2g: `48 of 48 exact; merged 32, fallback 0, admission waits 29` -> `only 32 of 48 answers merged from endpoint answers`. Either 55(a) (one endpoint per node for merged one-row aggregates / small LIMITs) changed what counts as a merge - then update the check's expectation in `.github/workflows/dev-ci.yml` with the reason - or merges are really lost under concurrency (29 admission waits) - then fix it. Answers were exact. Get the full Dev CI on `main` green before anything else.

> **Maintainer, 2026-11-17 19:20 - ITEM 55(a): dc tails (moved here from lane 3, which works on 55(b)).** Real cluster run 2026-11-16 on `server-1.0.0-d2e27119fda4` (3 nodes, 80 cores / 754 GB, server on 16 cores, -Xmx8g, direct 8g, HDFS Kerberos, DataNodes `authentication,privacy`, direct SASL reads, TPC-H SF10 + a 1.27 TB table; full context in `STATUS.md` item 52 and `LANE3-STATUS.md` item 55).
> dc vs speed: same q/s on the point load (0.94-1.10), but small-query p99 c1 287 ms vs 36, c8 1125 vs 85, c32 4825 vs 755
> (p50 equal or better), and raw Flight scans of tpch.lineitem with 4 / 8 concurrent streams 1154 / 964 vs 1975 / 2171 MB/s
> (1 stream 646 vs 722). Find the tail source under dc (peer stream setup per query, merge queue, admission waits, Hazelcast
> lookups, a periodic stall such as the 30 s file inventory refresh?) with per-query timing on the CI cluster cell (3 servers,
> HDFS SASL) and fix it; goal p99 <= 1.5x speed and scans >= 0.9x speed. Branch `fix/55a`. Write `ITEM 55(a) DONE <sha>` with numbers.

> **Lane 5 next step (2026-11-17T19:10Z):** read the full Dev CI 44154760020 (push) and the package run 44154778833 on
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

