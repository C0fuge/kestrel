# Orchestration status

Status as of **2026-11-03 04:00 UTC** (hourly orchestrator; it only watches, it does not change code).

Not ready yet: **ITEM 26 DONE** (core, hadoopfs short-circuit; session started 03:41Z), **ITEM 27 DONE** (core, S3
single-stream scan/export), **IMPROVEMENTS DONE** in marten (step 2 of 3) and heron (investigation started), and an
egret FRESH result for the newest pre-release `server-1.0.0-57e054887f7a` (newest fresh-upstream wave is for
`f06b5834e76e`).

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | ITEM 26 (hadoopfs short-circuit; SASL data-transfer cell), then ITEM 27 | 03:41Z (3310864e, lock only) | taken 03:41Z (valid) | Dev CI 41500837154 (main 1646ed4f) green |
| plugins (`PLUGINS-STATUS.md`) | paused; no open bug-first note | 2026-11-02 18:23Z (cdc4c5df) | free | Dev CI 41497330129 green |
| catalogs (`CATALOGS-STATUS.md`) | paused; no open bug-first note | 2026-11-02 | free | Catalogs CI 41497330175 green |
| lane 3 (`LANE3-STATUS.md`) | paused (D13 (b) hdfs-ha store done); no open bug-first note | 2026-11-02 18:39Z (fbfced79) | free | service-cluster 41500837184 green |

No stale locks (kestrel, marten, heron). Dev CI on main: newest run green, nothing to rerun.

## Service matrix (newest runs)

| Workflow | Run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 41500837184 | 1646ed4f (main) | pass |
| service-hdfs.yml | 41500352375 | 57e05488 (`ci/core-gate`) | pass 6/6 |
| service-security.yml | 41500357307 | 57e05488 (`ci/core-gate`) | pass |
| service-stores.yml | 41500362497 | 57e05488 (`ci/core-gate`) | pass |

## Pre-releases (newest 3)

- `server-1.0.0-57e054887f7a` (2026-11-02 18:40Z): item 25 core part, first through the `package=true` service gate.
- `server-1.0.0-f8ba588a8295` (2026-11-02 17:43Z): HDFS fix + ranger table check fix.
- `server-1.0.0-c4cd985f37e3` (2026-11-02 16:11Z): first with the HDFS fix.

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: step 1 (`avg()` pushdown, 2c9597e) and step 2 (join-key pushdown, c614cda) committed 03:39-03:46Z; fe run
  41574168661 pending (41573471776 in progress); step 3 (compare re-run) open. Lock 03:35Z (valid). Not DONE.
- heron: investigation started - `connector-compare` now logs plans and planning time (62cb748, 03:52Z). Lock 03:50Z
  (valid). Not DONE.

## egret

- Newest fresh-upstream waves: `fresh-up-f06b5834e76e` (run 41437099791) and `fresh-up-fc-f06b5834e76e` (41437982824).
  speed vs balanced S3 SF10 ADBC 0.66 / JDBC 0.68; the up-native side fails its correctness gate in several jobs (sf1/sf10
  S3). Maintainer's item 27 numbers (upstream 6e264c10f403): S3 scan x1 0.80, ADBC export 0.73 of upstream.
- README 'In progress': idle 03:26Z, "no kestrel pre-release newer than 57e054887f7a"; no FRESH wave for 57e054887f7a yet.

## For the maintainer

- Nothing blocking. The core Open question on hadoopfs short-circuit is now covered by your ITEM 26.
- egret has no fresh three-way result for 57e054887f7a (it reports idle); expect one with the item 26/27 pre-release.

## History

- 2026-11-03 04:00Z: core on ITEM 26 (locked 03:41Z); marten improvements steps 1-2 pushed, heron started; all CI green; newest pre-release 57e054887f7a; no stale locks.
- 2026-11-02 18:00Z: plugins tag-policy fix pushed (security run in progress); core free, ITEM 25 not started; lane 3 on D13 (a2); pre-release f8ba588a8295; no stale locks, no red CI.
- 2026-11-02 17:30Z: ranger cell added and green (8/8) after the core table-check fix; plugins lane on the tag-policy bug; core lane locked, ITEM 25 next; no service-hdfs.yml yet.
- 2026-11-02 16:25Z: first orchestrator status. HDFS FIX DONE; matrix: catalogs and lane 3 DONE, plugins at 3/4 services; core ITEM 25 open; both connector comparisons DONE.
