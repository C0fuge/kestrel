# Orchestration status

Status as of **2026-11-03 08:00 UTC** (hourly orchestrator; it only watches, it does not change code).

Not ready yet: **IMPROVEMENTS DONE** in marten (item 2 join-key fix green; SF1 compare + q19 check left) and an egret
FRESH result for the newest pre-release (`server-1.0.0-413485ba80fd`; egret's newest FRESH is `25aa7eec0f0f`).
Done: **ITEM 26 DONE**, **ITEM 27 DONE** (kestrel), **IMPROVEMENTS DONE** (heron).

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | none: items 26 and 27 done; 07:45Z session found no open item (remaining items are stand checks or wait on Open questions) | 07:41Z (fe73038d, 15 min) | free | Dev CI 41589351305 (main 413485ba) green |
| plugins (`PLUGINS-STATUS.md`) | paused; no open bug-first note | 2026-11-02 18:23Z | free | - |
| catalogs (`CATALOGS-STATUS.md`) | paused; no open bug-first note | 2026-11-02 16:18Z | free | - |
| lane 3 (`LANE3-STATUS.md`) | paused; no open bug-first note | 2026-11-02 18:39Z | free | - |

No stale locks (kestrel all free; marten free; heron taken 07:51Z, valid). Dev CI on main: newest run green, nothing to rerun.

## Service matrix

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 41592454233 (`ci/core-27`) | 413485ba | pass |
| service-hdfs.yml | 41592458259 (`ci/core-27`) | 413485ba | pass |
| service-security.yml | 41592462281 (`ci/core-27`) | 413485ba | pass |
| service-stores.yml | 41592466574 (`ci/core-27`) | 413485ba | pass |

## Pre-releases (newest 3)

- `server-1.0.0-413485ba80fd` (2026-11-03 06:21Z): item 27 (S3 scan/export; `preserve_insertion_order` cause, new `scan-perf.yml`).
- `server-1.0.0-25aa7eec0f0f` (2026-11-03 04:21Z): item 26 (hadoopfs short-circuit from LIBHDFS3_CONF / hdfs-site.xml).
- `server-1.0.0-f8ba588a8295` (2026-11-02): HDFS fix + ranger table check fix.

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: items 1 (`avg()` pushdown) and 3 done; item 2 join-key regression fix (series `fla21c2a`): FE 41591877770
  green, m4 41593203647 checks 1-9 PASS, compare SF10 41593206366: ours/up 1.14 default, 0.97 speed; q12 3.01 vs 2.95
  (was 4.15), q14 2.04 vs 1.68, q19 4.63 vs 2.85 still slower. Next: read the SF1 compare dispatched 07:49Z, then q19
  (only if cheap), then IMPROVEMENTS DONE. Lock free. Not DONE.
- heron: **IMPROVEMENTS DONE** (session 98): proof run 41586926044 with default `split-count=1`: SF1 warm sum 60.8 vs
  upstream 74.8 s; SF10 19 common 252.1 vs 320.9 s. A new session took the lock 07:51Z.

## egret

- `results/FRESH-25aa7eec0f0f.md` (run 41584018828, upstream native `6e264c10f403`, unchanged): upstream still fails the
  TPC-H / small-query sets (interface gap). Speed / upstream: S3 scan x1 / x4 / x8 1.02 / 0.87 / 0.89, S3 ADBC export
  0.87, local load 0.92-1.08; balanced / upstream S3 export 0.73. This is the build *before* item 27; a FRESH run on
  `413485ba80fd` is still needed (README 'In progress' has no entry for it yet).

## For the maintainer

- heron switched the Trino connector default `arrow-flight.split-count` 4 -> 1 (a default change; evidence in
  heron `docs/CONNECTOR-COMPARE.md`). Review when back.
- Core lane has no open item after 26-27; it asked for a stand rerun (Open questions in STATUS.md). New items needed.
- Nothing blocking.

## History

- 2026-11-03 08:00Z: ITEM 26 + 27 DONE (pre-release 413485ba80fd, service gate 4/4 green); core idle, no open item; heron IMPROVEMENTS DONE (session 98); marten join-key fix green, q19 + SF1 compare left; egret FRESH-25aa7eec0f0f done (not yet on 413485ba); no stale locks, no red Dev CI.
- 2026-11-03 07:00Z: core on ITEM 27 (scan-perf workflow green 41589080058; 413485ba speed profile `duckDbPreserveInsertionOrder=false`, Dev CI green; service gate on `ci/core-27` running); marten fixing q12/q14 regression (58a0995, fe/be running); heron split-count default 1, proof compare 41586926044 running; egret fresh run 41584018828 still running; no stale locks, no red Dev CI.
- 2026-11-03 06:00Z: core on ITEM 27 (locked 05:40Z, scan-perf workflow added); pre-release 25aa7eec0f0f out, service matrix 4/4 green; egret fresh run 41584018828 on it; marten q01/q17 closed but q12/q14 regressed (item 2 fix pending); heron full compare green, lane working; no stale locks, no red Dev CI.
- 2026-11-03 05:00Z: ITEM 26 DONE written (package run 41577668888 on `ci/core-sc` with service gate in progress); service-hdfs 8/8 green on main; marten improvements steps 1-2 green, step 3 compare dispatched; heron found cause (split-count), waits on full compare / split_count question; no stale locks, no red CI.
- 2026-11-03 04:00Z: core on ITEM 26 (locked 03:41Z); marten improvements steps 1-2 pushed, heron started; all CI green; newest pre-release 57e054887f7a; no stale locks.
- 2026-11-02 18:00Z: plugins tag-policy fix pushed (security run in progress); core free, ITEM 25 not started; lane 3 on D13 (a2); pre-release f8ba588a8295; no stale locks, no red CI.
- 2026-11-02 17:30Z: ranger cell added and green (8/8) after the core table-check fix; plugins lane on the tag-policy bug; core lane locked, ITEM 25 next; no service-hdfs.yml yet.
- 2026-11-02 16:25Z: first orchestrator status. HDFS FIX DONE; matrix: catalogs and lane 3 DONE, plugins at 3/4 services; core ITEM 25 open; both connector comparisons DONE.
