# Orchestration status

Status as of **2026-11-03 21:58 UTC** (hourly orchestrator; it only watches, it does not change code).

Not ready yet: **ITEM 30** (local copy of non-local HDFS files under SASL). Done so far: parts 1, 2a, 2b steps 1-3 and
the copy-disk guard 336f9607 (`hdfsLocalCopyMinFreeBytes`; Dev CI 41715910689 green). New: 0418b441 (21:43Z) adds a
small copy-disk stage to service-hdfs; Dev CI 41724478294 on it is green, but service-hdfs 41724478510 is **red in the
new stage only** (cell sasl-cluster/balanced, "small disk": `chunks.lineitem` partial refused with
`hdfs=localCopyNoSpace`, 13 MB free vs a 52 MB copy - the stage's expectations do not match its disk size yet). The
core session holds the lock (21:41Z, active), so this is its own in-flight work; no bug-first note written. Left after
that: joins / complete over the cache (Open question below), `package=true`. egret has no FRESH report for the newest
pre-release `5122cea1f045` (ALL DONE waits for item 30's pre-release and a FRESH run on it).
Done: **ITEM 26 DONE**, **ITEM 27 DONE**, **ITEM 28 DONE**, **ITEM 29 DONE** (kestrel), **IMPROVEMENTS DONE** (marten
and heron).

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | ITEM 30 part 2b (small copy-disk CI stage) | 0418b441 21:43Z (~15 min) | held 21:41Z (active) | Dev CI 41724478294 green; service-hdfs 41724478510 red (new small-disk stage, sasl-cluster balanced; other 9 cells pass). |
| plugins (`PLUGINS-STATUS.md`) | paused; no new bug-first note | 2026-11-03 11:04Z | free | - |
| catalogs (`CATALOGS-STATUS.md`) | paused; no new bug-first note | 2026-11-03 11:04Z | free | - |
| lane 3 (`LANE3-STATUS.md`) | paused; no new bug-first note | 2026-11-03 11:04Z | free | - |

No stale locks (marten and heron free).

## Service matrix

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 41628772790 | 5122cea1 | pass |
| service-hdfs.yml | 41724478510 (main) | 0418b441 | **fail** (small-disk stage, sasl-cluster balanced) |
| service-security.yml | 41628777405 | 5122cea1 | pass |
| service-stores.yml | 41628781826 | 5122cea1 | pass |

## Pre-releases (newest 3)

- `server-1.0.0-5122cea1f045` (2026-11-03 11:04Z): item 29 part 2 (SASL routing, clear errors, per-table views).
- `server-1.0.0-00e3e896b31e` (2026-11-03 09:15Z): item 29 part 1.
- `server-1.0.0-413485ba80fd` (2026-11-03 06:21Z): item 27 (speed: `preserve_insertion_order=false`).

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: **IMPROVEMENTS DONE** (10:00Z). Session 21:40-22:00Z: lakeFS M4 41713886604 on server `0e457de30f25` PASS,
  full M4 suite green, `0e457de30f25` is now the M4 default. Lock free.
- heron: **IMPROVEMENTS DONE** (session 98); session 102: nothing open. Lock free.

## egret

- Newest report: `results/FRESH-413485ba80fd.md` (run 41604206524, 10/10 green, upstream `6e264c10f403`, unchanged).
  Speed vs upstream native: scans x1/x4 1.00-1.04, S3 ADBC export 0.92 (local 0.98), slow-link ZSTD export 0.96-1.06,
  load 0.92-0.98; upstream native 0-1/22 on TPC-H, small-query errors. Ours 22/22, 0 errors.
- README "In progress" (21:28Z): idle; newest tracked 413485ba80fd (no pre-release after 5122cea1).

## For the maintainer

- **Decision wanted (Open question, item 30):** joins / non-mergeable `complete` answers (TPC-H joins, windows,
  HAVING / LIMIT over groups) hold all their copies at once, so the copy dir can grow to the query's input size.
  Options: (1) refuse up front beyond `hdfsLocalCopyMaxBytes`; (2) `hdfsLocalCopyHardMaxBytes` + free-space check;
  (3) keep and document. Still unanswered. The free-space guard (`hdfsLocalCopyMinFreeBytes`) already keeps the disk
  from filling; the remaining choice is whether such queries should run up to the disk or stop at a cache-tied bound.
- Open question (core lane, FYI): flake `EndpointFailoverIntegrationTest.ticketPlannedForAnotherNodeGivesTheSameAnswerAndMovesItsLoad`
  (Dev CI 41644812393); no repeat seen.
- Open question "item 27 stand rerun" is still listed (egret runner numbers: S3 export 0.92 of upstream).
- Connector default changes to review: marten `flight.pushdown.join_keys` 10000 -> 50000; heron `arrow-flight.split-count` 4 -> 1.

## History

- 2026-11-03 21:58Z: core lock 21:41Z, pushed 0418b441 (service-hdfs small copy-disk stage): Dev CI 41724478294 green, service-hdfs 41724478510 red only in the new small-disk stage (sasl-cluster balanced, `localCopyNoSpace`), lane active; 336f9607 Dev CI green; marten M4 default now server 0e457de30f25 (lakeFS 41713886604 pass); heron/egret idle; no stale locks.
- 2026-11-03 20:58Z: core lock 20:41Z, pushed 336f9607 (item 30 part 2b: `hdfsLocalCopyMinFreeBytes`, copies never fill their disk; query refused with `hdfs=localCopyNoSpace`); a4bbbab0 green (Dev CI 41707251465, service-hdfs 41707251290); service-hdfs 41715910681 on 336f9607 green, Dev CI 41715910689 running; marten session active (lock 19:43Z, M4 checks 6,7 and lakeFS 1,3,4,10 dispatched); heron/egret idle; no stale locks, no red CI.
- 2026-11-03 19:58Z: core lock 19:41Z, pushed a4bbbab0 (item 30 part 2b: LIMIT aggregates, DISTINCT / LIMIT scans over copy batches), Dev CI 41707251465 / service-hdfs 41707251290 running; marten session active (lock 19:43Z, M4 S3/iceberg rerun dispatched on server 0e457de30f25); heron/egret idle; no stale locks, no red CI.
- 2026-11-03 19:05Z: core footer-answered count(*) and `hdfsLocalCopyDir` (842f4a88) green (Dev CI 41696991827, service-hdfs 41696991867); new Open question (joins over the copy cache); core lock released; no new pre-release; marten/heron/egret idle; no stale locks, no red CI.
- 2026-11-03 18:05Z: core ITEM 30 part 2b step 2 green (Dev CI 41679739414); new footer-count commit 712047f8 red in Dev CI 41687057083 (its new unit test), lane's fix 4c74c89f running (41689345981); core lock 17:41Z; no new pre-release; marten/heron/egret idle; no stale locks.
- 2026-11-03 16:57Z: core ITEM 30 part 2b step 1 green (Dev CI 41671843220, service-hdfs 41669461010); core lock retaken 16:41Z; no new pre-release; marten/heron/egret idle; no stale locks, no red CI.
- 2026-11-03 15:57Z: core on ITEM 30 part 2b (part 2a green: Dev CI 41659194274, service-hdfs 41659194422; c00a0fba pushed 15:51Z, Dev CI / service-hdfs running; lock 15:41Z); no new pre-release; marten/heron/egret idle; no stale locks, no red CI.
- 2026-11-03 14:57Z: core on ITEM 30 part 2 (690becd9 pushed 14:45Z, lock 14:41Z); service-hdfs 41656768629 on it red only in the 2 sasl-cluster cells (all answers ok, TCP-read bound exceeded), Dev CI 41656768520 running; no new pre-release; marten/heron/egret idle; no stale locks.
- 2026-11-03 13:57Z: core ITEM 30 part 1 green (Dev CI 41644812393 after one flake rerun, service-hdfs 41641719428 10/10); part 2 open; core LOCK still holds 12:41Z although the Log says released (not stale by rule); no new pre-release; marten/heron/egret idle; no red CI.
- 2026-11-03 12:56Z: core on ITEM 30 (part 1 a523356e pushed, Dev CI / service-hdfs running; part 2 open); no new pre-release; marten/heron idle (IMPROVEMENTS DONE); egret idle; no stale locks, no red CI.
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
