# Orchestration status

Status as of **2026-11-03 07:00 UTC** (hourly orchestrator; it only watches, it does not change code).

Not ready yet: **ITEM 27 DONE** (core, S3 single-stream scan/export; in progress), **IMPROVEMENTS DONE** in marten (item 2
q12/q14 regression fix 58a0995 in CI) and heron (proof compare running), and an egret FRESH result for the newest
pre-release (`server-1.0.0-25aa7eec0f0f`, run 41584018828 in progress). **ITEM 26 DONE** is written.

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | ITEM 27 (S3 scan/export vs upstream): `scan-perf.yml` working; 413485ba speed profile offers `duckDbPreserveInsertionOrder=false` | 06:21Z (413485ba, 35 min) | taken 05:40Z (valid, 75 min) | Dev CI 41589351305 (main 413485ba) green; Scan perf 41589080058 green (three earlier first-try failures fixed); service gate on `ci/core-27` = 413485ba running (cluster 41592454233, hdfs 41592458259, security 41592462281, stores 41592466574) |
| plugins (`PLUGINS-STATUS.md`) | paused; no open bug-first note | 2026-11-02 18:23Z | free | - |
| catalogs (`CATALOGS-STATUS.md`) | paused; no open bug-first note | 2026-11-02 16:18Z | free | - |
| lane 3 (`LANE3-STATUS.md`) | paused; no open bug-first note | 2026-11-02 18:39Z | free | - |

No stale locks (kestrel core 05:40Z; marten 06:40Z; heron 06:51Z). Dev CI on main: newest run green, nothing to rerun.

## Service matrix

| Workflow | Newest run | Commit | Result | Last completed |
|---|---|---|---|---|
| service-cluster.yml | 41592454233 (`ci/core-27`) | 413485ba | queued | 41579846717 25aa7eec pass |
| service-hdfs.yml | 41592458259 (`ci/core-27`) | 413485ba | queued | 41577631525 25aa7eec pass (8 cells) |
| service-security.yml | 41592462281 (`ci/core-27`) | 413485ba | queued | 41579850394 25aa7eec pass |
| service-stores.yml | 41592466574 (`ci/core-27`) | 413485ba | in progress | 41579854352 25aa7eec pass |

## Pre-releases (newest 3)

- `server-1.0.0-25aa7eec0f0f` (2026-11-03): item 26 (hadoopfs short-circuit from LIBHDFS3_CONF / hdfs-site.xml).
- `server-1.0.0-f8ba588a8295` (2026-11-02): HDFS fix + ranger table check fix.
- `server-1.0.0-fca6512a8046` (2026-11-02).

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: items 1 (`avg()` pushdown) and 3 (compare "after") done; item 2 (join-key pushdown) fix for the q12/q14
  regression pushed 58a0995 (06:45Z, key pushdown only from a smaller source table); fe 41591877770 and be (force)
  41591880762 running. Lane locked 06:40Z. Not DONE.
- heron: default `arrow-flight.split-count` 4 -> 1 (aa608f3); full suite 41579205198: SF10 ours 325.4 s (was 402.1),
  upstream-common 243.8 vs 313.8 s; build 41586915975 and e2e 41586916003 green; proof compare 41586926044 (SF1 + SF10,
  ours vs up) running. Lane locked 06:51Z. Not DONE.

## egret

- Fresh three-way on `25aa7eec0f0f` still running: run 41584018828, wave `fresh-up-25aa7eec0f0f` (A balanced, B speed,
  C upstream native `6e264c10f403`), started 05:27Z. No `results/FRESH-*.md` yet (previous wave results are under
  `results/fresh-up-f06b5834e76e-41437099791`). Item 27 baseline: S3 scan x1 0.80, ADBC export 0.73 of upstream.

## For the maintainer

- heron switched the Trino connector default `arrow-flight.split-count` 4 -> 1 (a default change; evidence in
  heron `docs/CONNECTOR-COMPARE.md`). Review when back.
- Nothing blocking.

## History

- 2026-11-03 07:00Z: core on ITEM 27 (scan-perf workflow green 41589080058; 413485ba speed profile `duckDbPreserveInsertionOrder=false`, Dev CI green; service gate on `ci/core-27` running); marten fixing q12/q14 regression (58a0995, fe/be running); heron split-count default 1, proof compare 41586926044 running; egret fresh run 41584018828 still running; no stale locks, no red Dev CI.
- 2026-11-03 06:00Z: core on ITEM 27 (locked 05:40Z, scan-perf workflow added); pre-release 25aa7eec0f0f out, service matrix 4/4 green; egret fresh run 41584018828 on it; marten q01/q17 closed but q12/q14 regressed (item 2 fix pending); heron full compare green, lane working; no stale locks, no red Dev CI.
- 2026-11-03 05:00Z: ITEM 26 DONE written (package run 41577668888 on `ci/core-sc` with service gate in progress); service-hdfs 8/8 green on main; marten improvements steps 1-2 green, step 3 compare dispatched; heron found cause (split-count), waits on full compare / split_count question; no stale locks, no red CI.
- 2026-11-03 04:00Z: core on ITEM 26 (locked 03:41Z); marten improvements steps 1-2 pushed, heron started; all CI green; newest pre-release 57e054887f7a; no stale locks.
- 2026-11-02 18:00Z: plugins tag-policy fix pushed (security run in progress); core free, ITEM 25 not started; lane 3 on D13 (a2); pre-release f8ba588a8295; no stale locks, no red CI.
- 2026-11-02 17:30Z: ranger cell added and green (8/8) after the core table-check fix; plugins lane on the tag-policy bug; core lane locked, ITEM 25 next; no service-hdfs.yml yet.
- 2026-11-02 16:25Z: first orchestrator status. HDFS FIX DONE; matrix: catalogs and lane 3 DONE, plugins at 3/4 services; core ITEM 25 open; both connector comparisons DONE.
