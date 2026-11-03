# Orchestration status

Status as of **2026-11-03 09:57 UTC** (hourly orchestrator; it only watches, it does not change code).

Not ready yet: **ITEM 29** (maintainer's bug-first, SASL DataNodes; part 1 of 2 done) and an egret FRESH result for
the newest pre-release (`server-1.0.0-413485ba80fd`; run 41604206524 in progress since 08:33Z).
Done: **ITEM 26 DONE**, **ITEM 27 DONE**, **ITEM 28 DONE** (kestrel), **IMPROVEMENTS DONE** (marten and heron).

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | ITEM 29 in progress: part 1 done (footers through the Java client, a bad table skipped, warning text); next part 2 (planner: files only to nodes holding every block; SASL service cell; `package=true`) | 09:15Z (00e3e896, 42 min) | taken 08:57Z (1 h, active) | Dev CI 41607831413 (75e63681) **red** = SpotBugs, fixed by dcc8f919: 41609483775 green; dispatch 41612114451 (00e3e896) running |
| plugins (`PLUGINS-STATUS.md`) | paused; no new bug-first note | 2026-11-02 | free | - |
| catalogs (`CATALOGS-STATUS.md`) | paused; no new bug-first note | 2026-11-02 | free | Catalogs CI 41609482721 green |
| lane 3 (`LANE3-STATUS.md`) | paused; no new bug-first note | 2026-11-02 | free | - |

No stale locks (kestrel core lock active, others free; marten and heron free). Newest completed Dev CI on main green,
nothing to rerun.

## Service matrix

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 41614141178 (main) | 00e3e896 | queued (previous 41592454233 on 413485ba: pass) |
| service-hdfs.yml | 41614145511 (main) | 00e3e896 | running (previous 41592458259: pass) |
| service-security.yml | 41614149731 (main) | 00e3e896 | running (previous 41592462281: pass) |
| service-stores.yml | 41614153424 (main) | 00e3e896 | running (previous 41592466574: pass) |

## Pre-releases (newest 3)

- `server-1.0.0-413485ba80fd` (2026-11-03 06:21Z): item 27 (speed: `preserve_insertion_order=false`; `scan-perf.yml`).
- `server-1.0.0-25aa7eec0f0f` (2026-11-03 04:21Z): item 26 (hadoopfs short-circuit from LIBHDFS3_CONF / hdfs-site.xml).
- `server-1.0.0-f8ba588a8295` (2026-11-02 17:24Z): HDFS fix + ranger table check fix.

No pre-release with item 29 yet.

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: **IMPROVEMENTS DONE** (09:53Z). Join-key IN list up to 50 000 (series `4a9848e-fl1eb792`, FE 41605517022,
  BE 41605520176, m4 41606940223 S3 checks 1-9 PASS); compare SF10 41606943229: q19 2.89 vs up 2.91 s; q01/q12/q14/q17/q19
  at parity; SF10 common-20 ours / up 1.15 default, 0.93 speed. Not released. Lock free.
- heron: **IMPROVEMENTS DONE** (session 98); session 100: nothing open. Lock free.

## egret

- Newest: `results/FRESH-25aa7eec0f0f.md` (run 41584018828, upstream `6e264c10f403`; pre item 27): speed / upstream
  S3 scan x1 / x4 / x8 1.02 / 0.87 / 0.89, S3 ADBC export 0.87.
- Running: FRESH three-way on `413485ba80fd` (balanced vs speed vs fresh upstream), run 41604206524 since 08:33Z
  (also the item 27 stand rerun the core lane asked for).

## For the maintainer

- New Open question (core, item 29): how to read non-local HDFS blocks under SASL - options (1) SASL-capable libhdfs3,
  (2) Java read path as Arrow stream, (3) local copy/cache of non-local files (lane's suggestion), (4) operating advice:
  replication = number of server nodes. Part 2 (routing to nodes with all blocks) proceeds without the answer.
- marten raised its default `flight.pushdown.join_keys` 10000 -> 50000; heron switched `arrow-flight.split-count`
  4 -> 1 earlier. Both connector default changes; review when back.

## History

- 2026-11-03 09:57Z: core on bug-first ITEM 29 (part 1 done 75e63681/dcc8f919; Dev CI red on 75e63681 = SpotBugs, fixed by dcc8f919, green 41609483775); new Open question item 29 (non-local SASL reads); marten IMPROVEMENTS DONE; heron idle; egret FRESH 413485ba80fd still running; no stale locks.
- 2026-11-03 08:57Z: ITEM 28 DONE (docs only); core idle; marten q19 step (join_keys 50000) in FE/BE CI; heron speed proof added; egret FRESH 413485ba80fd running (41604206524); no stale locks, Dev CI green.
- 2026-11-03 08:00Z: ITEM 26 + 27 DONE (pre-release 413485ba80fd, service gate 4/4 green); core idle, no open item; heron IMPROVEMENTS DONE (session 98); marten join-key fix green, q19 + SF1 compare left; egret FRESH-25aa7eec0f0f done (not yet on 413485ba); no stale locks, no red Dev CI.
- 2026-11-03 07:00Z: core on ITEM 27 (scan-perf workflow green 41589080058; 413485ba speed profile `duckDbPreserveInsertionOrder=false`, Dev CI green; service gate on `ci/core-27` running); marten fixing q12/q14 regression (58a0995, fe/be running); heron split-count default 1, proof compare 41586926044 running; egret fresh run 41584018828 still running; no stale locks, no red Dev CI.
- 2026-11-03 06:00Z: core on ITEM 27 (locked 05:40Z, scan-perf workflow added); pre-release 25aa7eec0f0f out, service matrix 4/4 green; egret fresh run 41584018828 on it; marten q01/q17 closed but q12/q14 regressed (item 2 fix pending); heron full compare green, lane working; no stale locks, no red Dev CI.
- 2026-11-03 05:00Z: ITEM 26 DONE written (package run 41577668888 on `ci/core-sc` with service gate in progress); service-hdfs 8/8 green on main; marten improvements steps 1-2 green, step 3 compare dispatched; heron found cause (split-count), waits on full compare / split_count question; no stale locks, no red CI.
- 2026-11-03 04:00Z: core on ITEM 26 (locked 03:41Z); marten improvements steps 1-2 pushed, heron started; all CI green; newest pre-release 57e054887f7a; no stale locks.
- 2026-11-02 18:00Z: plugins tag-policy fix pushed (security run in progress); core free, ITEM 25 not started; lane 3 on D13 (a2); pre-release f8ba588a8295; no stale locks, no red CI.
- 2026-11-02 17:30Z: ranger cell added and green (8/8) after the core table-check fix; plugins lane on the tag-policy bug; core lane locked, ITEM 25 next; no service-hdfs.yml yet.
- 2026-11-02 16:25Z: first orchestrator status. HDFS FIX DONE; matrix: catalogs and lane 3 DONE, plugins at 3/4 services; core ITEM 25 open; both connector comparisons DONE.
