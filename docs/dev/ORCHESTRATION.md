# Orchestration status

Status as of **2026-11-03 08:57 UTC** (hourly orchestrator; it only watches, it does not change code).

Not ready yet: **IMPROVEMENTS DONE** in marten (q19 fix in CI, step (b)) and an egret FRESH result for the newest
pre-release (`server-1.0.0-413485ba80fd`; run 41604206524 in progress since 08:33Z).
Done: **ITEM 26 DONE**, **ITEM 27 DONE**, **ITEM 28 DONE** (kestrel), **IMPROVEMENTS DONE** (heron).

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | none: item 28 (docs only: libhadoop how-to, row order under `speed`) done 08:42Z; no open core item | 08:42Z (2aa9e4c8, 15 min) | free | Dev CI 41589351305 (main 413485ba) green; 28 was docs only, no run |
| plugins (`PLUGINS-STATUS.md`) | paused; no new bug-first note | 2026-11-02 | free | - |
| catalogs (`CATALOGS-STATUS.md`) | paused; no new bug-first note | 2026-11-02 | free | - |
| lane 3 (`LANE3-STATUS.md`) | paused; no new bug-first note | 2026-11-02 | free | - |

No stale locks (kestrel all free; marten taken 08:40Z, active; heron free). Dev CI on main: newest run green, nothing to rerun.

## Service matrix

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 41592454233 (`ci/core-27`) | 413485ba | pass |
| service-hdfs.yml | 41592458259 (`ci/core-27`) | 413485ba | pass |
| service-security.yml | 41592462281 (`ci/core-27`) | 413485ba | pass |
| service-stores.yml | 41592466574 (`ci/core-27`) | 413485ba | pass |

## Pre-releases (newest 3)

- `server-1.0.0-413485ba80fd` (2026-11-03 06:21Z): item 27 (speed: `preserve_insertion_order=false`; `scan-perf.yml`).
- `server-1.0.0-25aa7eec0f0f` (2026-11-03 04:21Z): item 26 (hadoopfs short-circuit from LIBHDFS3_CONF / hdfs-site.xml).
- `server-1.0.0-f8ba588a8295` (2026-11-02 17:24Z): HDFS fix + ranger table check fix.

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: items 1, 3 and 2(a) done; SF1 compare 41598906259: ours/up common-19 0.84 default, 0.85 speed. Step (b) q19:
  rule matches at SF1 (~2 170 keys) but SF10 needs ~21 600, so `flight.pushdown.join_keys` limit raised 10000 -> 50000
  (2a23272); FE 41605517022 + BE 41605520176 (force) running, then m4 checks 1-9 + compare SF10 dispatched (2b3bd20).
  Session active (lock 08:40Z). Not DONE.
- heron: **IMPROVEMENTS DONE**; session 99 added the `speed` profile proof (run 41599165320): SF1 60.0 vs 71.2 s,
  SF10 19 common 250.8 vs 311.4 s, every answer exact. Lock free.

## egret

- Newest: `results/FRESH-25aa7eec0f0f.md` (run 41584018828, upstream `6e264c10f403` unchanged; pre item 27): speed /
  upstream S3 scan x1 / x4 / x8 1.02 / 0.87 / 0.89, S3 ADBC export 0.87; upstream still fails the TPC-H / small sets.
- Running: FRESH three-way on `413485ba80fd` (balanced vs speed vs fresh upstream), run 41604206524 since 08:33Z;
  this is also the item 27 stand rerun the core lane asked for.

## For the maintainer

- marten raised its default `flight.pushdown.join_keys` 10000 -> 50000 (for q19 at SF10); heron switched
  `arrow-flight.split-count` 4 -> 1 earlier. Both are connector default changes; review when back.
- Core lane has no open item after 26-28 (Open questions: item 27 stand rerun = egret run above; item 23 speed FYI).
- Nothing blocking.

## History

- 2026-11-03 08:57Z: ITEM 28 DONE (docs only); core idle; marten q19 step (join_keys 50000) in FE/BE CI; heron speed proof added; egret FRESH 413485ba80fd running (41604206524); no stale locks, Dev CI green.
- 2026-11-03 08:00Z: ITEM 26 + 27 DONE (pre-release 413485ba80fd, service gate 4/4 green); core idle, no open item; heron IMPROVEMENTS DONE (session 98); marten join-key fix green, q19 + SF1 compare left; egret FRESH-25aa7eec0f0f done (not yet on 413485ba); no stale locks, no red Dev CI.
- 2026-11-03 07:00Z: core on ITEM 27 (scan-perf workflow green 41589080058; 413485ba speed profile `duckDbPreserveInsertionOrder=false`, Dev CI green; service gate on `ci/core-27` running); marten fixing q12/q14 regression (58a0995, fe/be running); heron split-count default 1, proof compare 41586926044 running; egret fresh run 41584018828 still running; no stale locks, no red Dev CI.
- 2026-11-03 06:00Z: core on ITEM 27 (locked 05:40Z, scan-perf workflow added); pre-release 25aa7eec0f0f out, service matrix 4/4 green; egret fresh run 41584018828 on it; marten q01/q17 closed but q12/q14 regressed (item 2 fix pending); heron full compare green, lane working; no stale locks, no red Dev CI.
- 2026-11-03 05:00Z: ITEM 26 DONE written (package run 41577668888 on `ci/core-sc` with service gate in progress); service-hdfs 8/8 green on main; marten improvements steps 1-2 green, step 3 compare dispatched; heron found cause (split-count), waits on full compare / split_count question; no stale locks, no red CI.
- 2026-11-03 04:00Z: core on ITEM 26 (locked 03:41Z); marten improvements steps 1-2 pushed, heron started; all CI green; newest pre-release 57e054887f7a; no stale locks.
- 2026-11-02 18:00Z: plugins tag-policy fix pushed (security run in progress); core free, ITEM 25 not started; lane 3 on D13 (a2); pre-release f8ba588a8295; no stale locks, no red CI.
- 2026-11-02 17:30Z: ranger cell added and green (8/8) after the core table-check fix; plugins lane on the tag-policy bug; core lane locked, ITEM 25 next; no service-hdfs.yml yet.
- 2026-11-02 16:25Z: first orchestrator status. HDFS FIX DONE; matrix: catalogs and lane 3 DONE, plugins at 3/4 services; core ITEM 25 open; both connector comparisons DONE.
