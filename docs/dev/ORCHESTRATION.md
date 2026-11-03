# Orchestration status

Status as of **2026-11-03 10:57 UTC** (hourly orchestrator; it only watches, it does not change code).

Not ready yet: **ITEM 29** (maintainer's bug-first, SASL DataNodes; part 2 in progress), then **ITEM 30** (maintainer's
decision: local copy of non-local files under SASL); egret FRESH report for `server-1.0.0-413485ba80fd` (run green,
report pending). Done: **ITEM 26 DONE**, **ITEM 27 DONE**, **ITEM 28 DONE** (kestrel), **IMPROVEMENTS DONE** (marten
and heron).

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | ITEM 29 part 2: planner sends files only to nodes holding every block (8ec89fe9), views bind on a whole-local file (ed7cd026), one failing view no longer fails others (df3c23f1), service-hdfs sasl-cluster cell (73970819, 56283139); maintainer stand finding 13:45 (+03) addressed by ed7cd026/df3c23f1, not yet re-checked | 10:55Z (56283139, 1 min) | taken 10:14:57Z (42 min, active) | Dev CI 41620905057 (df3c23f1) **green**; 3 earlier runs cancelled by newer pushes |
| plugins (`PLUGINS-STATUS.md`) | paused; no new bug-first note | 2026-11-02 | free | - |
| catalogs (`CATALOGS-STATUS.md`) | paused; no new bug-first note | 2026-11-02 | free | - |
| lane 3 (`LANE3-STATUS.md`) | paused; no new bug-first note | 2026-11-02 | free | - |

No stale locks (kestrel core lock active, other lanes free; marten and heron free). Newest Dev CI on main green, nothing
to rerun.

## Service matrix

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 41614141178 (dispatch, main) | 00e3e896 | **fail**: cell hdfs-ha / loss, q18 non-retryable during the NameNode failover: `Unable to get file info: hdfs://ns1/.../lineitem/part-004.parquet: Operation category READ is not supported in state standby` (other 11 cells pass; previous 41592454233 on 413485ba: pass). Not in STATUS yet - see below |
| service-hdfs.yml | 41623262313 (main) | 56283139 | running (sasl-cluster cell, item 29) |
| service-security.yml | 41614149731 (main) | 00e3e896 | pass |
| service-stores.yml | 41614153424 (main) | 00e3e896 | pass |

## Pre-releases (newest 3)

- `server-1.0.0-00e3e896b31e` (2026-11-03 09:15Z): item 29 part 1 (footers through the Java client, bad table skipped).
- `server-1.0.0-413485ba80fd` (2026-11-03 06:21Z): item 27 (speed: `preserve_insertion_order=false`).
- `server-1.0.0-25aa7eec0f0f` (2026-11-03 04:21Z): item 26 (hadoopfs short-circuit from LIBHDFS3_CONF / hdfs-site.xml).

No pre-release with item 29 part 2 yet.

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: **IMPROVEMENTS DONE** (10:00Z); 10:41Z session: no new maintainer items, finish mode. Lock free.
- heron: **IMPROVEMENTS DONE** (session 98); session 100 (09:51Z): nothing open. Lock free.

## egret

- Newest report: `results/FRESH-25aa7eec0f0f.md` (run 41584018828, upstream `6e264c10f403`, unchanged): ours 22/22
  everywhere; upstream native 0/22 ADBC, 1/22 JDBC, errors on small queries.
- FRESH three-way on `413485ba80fd`: run 41604206524 **green**, report pending (README "In progress", 10:56Z).
  The newest pre-release is now `00e3e896b31e`, so ALL DONE also waits on a FRESH run for the item 29 build.

## For the maintainer

- **service-cluster hdfs-ha loss failed on 00e3e896** (run 41614141178): q18 got a non-retryable error during the
  NameNode failover (standby answered a file-info call, the HA client did not fail over). First failure of this cell;
  could be the item 29 part 1 Java footer path or a timing flake. The core lane's next `package=true` service gate will
  show whether it repeats; if it does, it is a core bug-first.
- Item 30 decision (option 3, local copy) is recorded in STATUS; option (1) stays a long-term Open question.
- Connector default changes to review when back: marten `flight.pushdown.join_keys` 10000 -> 50000; heron
  `arrow-flight.split-count` 4 -> 1.

## History

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
