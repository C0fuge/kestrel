# Orchestration status

Status as of **2026-11-03 11:58 UTC** (hourly orchestrator; it only watches, it does not change code).

Not ready yet: **ITEM 30** (maintainer's decision: local copy of non-local HDFS files under SASL; acceptance = CI cells
shaped like the target cluster incl. `ha-krb-sasl`, then `package=true`) - not started; egret has no FRESH report for
the newest pre-release `5122cea1f045` (egret skips HDFS-SASL-only builds per its note, so ALL DONE waits for item 30's
pre-release and a FRESH run on it). Done: **ITEM 26 DONE**, **ITEM 27 DONE**, **ITEM 28 DONE**, **ITEM 29 DONE**
(kestrel), **IMPROVEMENTS DONE** (marten and heron).

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | ITEM 29 DONE (11:49Z, pre-release `5122cea1f045`, whole service matrix green); next: ITEM 30 (local copy), not started | 11:49Z (eeacc053, 9 min) | free | Dev CI 41628818006 (26f24f1e) **green** |
| plugins (`PLUGINS-STATUS.md`) | paused; no new bug-first note | 2026-11-02 | free | Catalogs CI 41628818011 green |
| catalogs (`CATALOGS-STATUS.md`) | paused; no new bug-first note | 2026-11-02 | free | - |
| lane 3 (`LANE3-STATUS.md`) | paused; no new bug-first note | 2026-11-02 | free | - |

No stale locks (all kestrel lanes free; marten and heron free). Newest Dev CI on main green, nothing to rerun.

## Service matrix

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 41628772790 (`ci/core-29`) | 5122cea1 | pass (incl. hdfs-ha / loss, which failed once on 00e3e896) |
| service-hdfs.yml | 41624568709 (main) | 5122cea1 | pass (incl. new `sasl-cluster` cell) |
| service-security.yml | 41628777405 (`ci/core-29`) | 5122cea1 | pass |
| service-stores.yml | 41628781826 (`ci/core-29`) | 5122cea1 | pass |

Note (core lane, Log): the service gate had a bug (exit 0 on a failed workflow, subshell); fixed in 26f24f1e.

## Pre-releases (newest 3)

- `server-1.0.0-5122cea1f045` (2026-11-03 11:04Z): item 29 part 2 (SASL: files only to nodes holding every block,
  clear errors naming files, per-table view binding, sasl-cluster CI cell). Whole matrix green.
- `server-1.0.0-00e3e896b31e` (2026-11-03 09:15Z): item 29 part 1 (footers through the Java client, bad table skipped).
- `server-1.0.0-413485ba80fd` (2026-11-03 06:21Z): item 27 (speed: `preserve_insertion_order=false`).

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: **IMPROVEMENTS DONE** (10:00Z); 10:41Z session: no new maintainer items, finish mode. Lock free.
- heron: **IMPROVEMENTS DONE** (session 98); session 100 (09:51Z): nothing open, no newer server build in
  `e2e-assets`. Lock free.

## egret

- Newest report: `results/FRESH-413485ba80fd.md` (run 41604206524, 10/10 green, upstream `6e264c10f403`, unchanged).
  Speed vs balanced: scan x4/x8 1.30-1.46 (local and S3), S3 TPC-H 0.14-0.19, load +41-48 % at 128 clients; speed vs
  upstream native: scans 1.00-1.04 (x1/x4), S3 ADBC export 0.92, load 0.92-0.98; upstream native still 0/22 ADBC on
  TPC-H. Ours 22/22, 0 errors.
- README "In progress" (11:29Z): idle; newer pre-releases are HDFS-SASL-only, no FRESH wave for them.

## For the maintainer

- `server-1.0.0-5122cea1f045` (item 29 complete) is ready for the SASL stand when it is back: queries over files held
  whole by the server's node should pass; files no node holds whole fail clearly until item 30.
- Open question "item 27 stand rerun" is effectively answered by egret FRESH-413485ba80fd on runners (S3 x1 1.02, x4
  1.00, export 0.92 of upstream); the stand rerun is still the maintainer's call.
- Connector default changes to review when back: marten `flight.pushdown.join_keys` 10000 -> 50000; heron
  `arrow-flight.split-count` 4 -> 1.

## History

- 2026-11-03 11:58Z: ITEM 29 DONE (pre-release 5122cea1f045, service matrix 4/4 green incl. hdfs-ha / loss and sasl-cluster); core free, ITEM 30 next (not started); Dev CI green; egret FRESH 413485ba80fd published; no stale locks.
- 2026-11-03 10:57Z: core on ITEM 29 part 2 (routing, view binding per table, sasl-cluster cell; Dev CI 41620905057 green, service-hdfs running); service-cluster 41614141178 on 00e3e896 **red** (hdfs-ha loss q18 "READ not supported in state standby"); egret FRESH 413485ba run green, report pending; marten/heron idle; no stale locks.
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
