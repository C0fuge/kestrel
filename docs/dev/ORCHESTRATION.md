# Orchestration status

Status as of **2026-11-03 06:00 UTC** (hourly orchestrator; it only watches, it does not change code).

Not ready yet: **ITEM 27 DONE** (core, S3 single-stream scan/export; in progress), **IMPROVEMENTS DONE** in marten (item 2
needs one fix: q12/q14 regressed) and heron (fix pending), and an egret FRESH result for the newest pre-release
(`server-1.0.0-25aa7eec0f0f`, run 41584018828 in progress). **ITEM 26 DONE** is written (pre-release `server-1.0.0-25aa7eec0f0f`).

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | ITEM 27 (S3 scan/export vs upstream): new `scan-perf.yml` (92c83c8d, 6d694810) | 05:45Z (6d694810, 10 min) | taken 05:40Z (valid, 15 min) | Dev CI 41577631438 (main 25aa7eec) green; Scan perf 41585602654 failed (first try of the new workflow, fixed by 6d694810), 41585815577 in progress |
| plugins (`PLUGINS-STATUS.md`) | paused; no open bug-first note | 2026-11-02 18:23Z | free | - |
| catalogs (`CATALOGS-STATUS.md`) | paused; no open bug-first note | 2026-11-02 16:18Z | free | - |
| lane 3 (`LANE3-STATUS.md`) | paused; no open bug-first note | 2026-11-02 18:39Z | free | - |

No stale locks (kestrel core 05:40Z; marten free; heron 04:51Z, 65 min). Dev CI on main: newest run green, nothing to rerun.

## Service matrix (newest runs)

| Workflow | Run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 41579846717 | 25aa7eec | pass |
| service-hdfs.yml | 41577631525 | 25aa7eec | pass (8 cells incl. `ha-krb-sasl`) |
| service-security.yml | 41579850394 | 25aa7eec | pass |
| service-stores.yml | 41579854352 | 25aa7eec | pass |

## Pre-releases (newest 3)

- `server-1.0.0-25aa7eec0f0f` (2026-11-03 04:59Z): item 26 (hadoopfs short-circuit from LIBHDFS3_CONF / hdfs-site.xml).
- `server-1.0.0-f8ba588a8295` (2026-11-02 17:43Z): HDFS fix + ranger table check fix.
- `server-1.0.0-c4cd985f37e3` (2026-11-02 16:11Z): first with the HDFS fix.

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: items 1 (`avg()` pushdown) and 3 (compare "after") done; item 2 (join-key pushdown) needs one fix. BE
  41579557200, m4 41580199568 S3 checks 1-9 PASS; compare SF1 41580201961, SF10 41582285095: q01 6.08 -> 2.69 s (up 2.60),
  q17 3.19 -> 1.03 s (up 1.06) closed; q12 1.88 -> 4.15 s and q14 1.28 -> 2.44 s regressed (lineitem as key source, >10k
  keys); SF10 default common-20 ours 68.3 vs up 56.2 s, speed 52.7 vs 53.3 s. Not DONE.
- heron: session 96 found the cause (4 key-range splits per scan; `split_count=1` 80.0 s vs 115.3 default vs 91.9
  upstream, SF10 join queries); full compare 41579205198 green (1h19m), lane locked 04:51Z, next step switch the default
  unless the maintainer answers the split_count Open question. Not DONE.

## egret

- Fresh three-way on `25aa7eec0f0f` running: run 41584018828, wave `fresh-up-25aa7eec0f0f` (A balanced, B speed, C upstream
  native `6e264c10f403`), started 05:27Z. No `results/FRESH-*.md` yet; previous waves `fresh-up-f06b5834e76e` (41437099791).
  Item 27 baseline (upstream 6e264c10f403): S3 scan x1 0.80, ADBC export 0.73 of upstream.

## For the maintainer

- heron: changing the Trino connector default `arrow-flight.split-count` 4 -> 1 is a default change; the lane will do it
  unless you answer its split_count Open question otherwise.
- Nothing else blocking.

## History

- 2026-11-03 06:00Z: core on ITEM 27 (locked 05:40Z, scan-perf workflow added); pre-release 25aa7eec0f0f out, service matrix 4/4 green; egret fresh run 41584018828 on it; marten q01/q17 closed but q12/q14 regressed (item 2 fix pending); heron full compare green, lane working; no stale locks, no red Dev CI.

- 2026-11-03 05:00Z: ITEM 26 DONE written (package run 41577668888 on `ci/core-sc` with service gate in progress); service-hdfs 8/8 green on main; marten improvements steps 1-2 green, step 3 compare dispatched; heron found cause (split-count), waits on full compare / split_count question; no stale locks, no red CI.
- 2026-11-03 04:00Z: core on ITEM 26 (locked 03:41Z); marten improvements steps 1-2 pushed, heron started; all CI green; newest pre-release 57e054887f7a; no stale locks.
- 2026-11-02 18:00Z: plugins tag-policy fix pushed (security run in progress); core free, ITEM 25 not started; lane 3 on D13 (a2); pre-release f8ba588a8295; no stale locks, no red CI.
- 2026-11-02 17:30Z: ranger cell added and green (8/8) after the core table-check fix; plugins lane on the tag-policy bug; core lane locked, ITEM 25 next; no service-hdfs.yml yet.
- 2026-11-02 16:25Z: first orchestrator status. HDFS FIX DONE; matrix: catalogs and lane 3 DONE, plugins at 3/4 services; core ITEM 25 open; both connector comparisons DONE.
