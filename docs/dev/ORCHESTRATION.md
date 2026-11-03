# Orchestration status

Status as of **2026-11-03 05:00 UTC** (hourly orchestrator; it only watches, it does not change code).

Not ready yet: **ITEM 27 DONE** (core, S3 single-stream scan/export; not started), **IMPROVEMENTS DONE** in marten (step 3
running) and heron (fix pending), and an egret FRESH result for the newest pre-release (the item 26 pre-release is still
being packaged). **ITEM 26 DONE** is written (2026-11-03, e016818c / ad141bf5 / 78c7e875).

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | ITEM 26 done, `package=true` pending; ITEM 27 next | 04:21Z (25aa7eec) | taken 03:41Z (valid, 79 min) | Dev CI 41577631438 (main 25aa7eec) green; package run 41577668888 (`ci/core-sc`) in progress |
| plugins (`PLUGINS-STATUS.md`) | paused; no open bug-first note | 2026-11-02 18:23Z (cdc4c5df) | free | Dev CI green |
| catalogs (`CATALOGS-STATUS.md`) | paused; no open bug-first note | 2026-11-02 16:18Z | free | Catalogs CI 41577070039 green |
| lane 3 (`LANE3-STATUS.md`) | paused; no open bug-first note | 2026-11-02 18:39Z (fbfced79) | free | - |

No stale locks (kestrel, marten 04:40Z, heron 04:51Z). Dev CI on main: newest run green, nothing to rerun. Item 26's
service-hdfs pushes 41575245509 / 41576424084 / 41577070056 failed while the lane iterated; fixed by 25aa7eec (41577631525 green).

## Service matrix (newest runs)

| Workflow | Run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 41579846717 | 25aa7eec (`ci/core-sc`) | pass |
| service-hdfs.yml | 41577631525 | 25aa7eec (main) | pass (8 cells incl. `ha-krb-sasl`) |
| service-security.yml | 41579850394 | 25aa7eec (`ci/core-sc`) | in progress |
| service-stores.yml | 41579854352 | 25aa7eec (`ci/core-sc`) | pass |

## Pre-releases (newest 3)

- `server-1.0.0-57e054887f7a` (2026-11-02 18:12Z): item 25 core part, first through the service gate.
- `server-1.0.0-f8ba588a8295` (2026-11-02 17:24Z): HDFS fix + ranger table check fix.
- `server-1.0.0-c4cd985f37e3` (2026-11-02 15:51Z): first with the HDFS fix.
- Expected next: `server-1.0.0-25aa7eec...` (item 26) from run 41577668888.

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: steps 1 (`avg()` pushdown) and 2 (join-key pushdown, IS NOT NULL fix 17b6cc9) green (fe 41578204766, be
  41579557200); step 3 under way: m4 41580199568 and compare 41580201961 (SF1 ours/up) dispatched 04:48Z. Not DONE.
- heron: cause found - the 4 key-range statements per big scan (`split-count=4`); `split_count=1` 80.0 s vs ours 115.3 vs
  upstream 91.9 (SF10, 5 join queries). Next: full compare run 41579205198, then switch the default to 1 unless the
  maintainer answers the split_count Open question otherwise. Not DONE.

## egret

- Newest fresh-upstream waves: `fresh-up-f06b5834e76e` (41437099791) and `fresh-up-fc-f06b5834e76e` (41437982824); no
  `results/FRESH-*.md` file yet. Item 27 numbers (upstream 6e264c10f403): S3 scan x1 0.80, ADBC export 0.73 of upstream.
- README 'In progress': idle 03:26Z, "no kestrel pre-release newer than 57e054887f7a"; waits for the item 26 pre-release.

## For the maintainer

- heron: changing the Trino connector default `arrow-flight.split-count` 4 -> 1 is a default change; the lane will do it
  after the full compare unless you answer its split_count Open question otherwise.
- Nothing else blocking.

## History

- 2026-11-03 05:00Z: ITEM 26 DONE written (package run 41577668888 on `ci/core-sc` with service gate in progress); service-hdfs 8/8 green on main; marten improvements steps 1-2 green, step 3 compare dispatched; heron found cause (split-count), waits on full compare / split_count question; no stale locks, no red CI.
- 2026-11-03 04:00Z: core on ITEM 26 (locked 03:41Z); marten improvements steps 1-2 pushed, heron started; all CI green; newest pre-release 57e054887f7a; no stale locks.
- 2026-11-02 18:00Z: plugins tag-policy fix pushed (security run in progress); core free, ITEM 25 not started; lane 3 on D13 (a2); pre-release f8ba588a8295; no stale locks, no red CI.
- 2026-11-02 17:30Z: ranger cell added and green (8/8) after the core table-check fix; plugins lane on the tag-policy bug; core lane locked, ITEM 25 next; no service-hdfs.yml yet.
- 2026-11-02 16:25Z: first orchestrator status. HDFS FIX DONE; matrix: catalogs and lane 3 DONE, plugins at 3/4 services; core ITEM 25 open; both connector comparisons DONE.
