# Orchestration status

Status as of **2026-11-04 14:58 UTC** (hourly orchestrator; it only watches, it does not change code).

Not written: **ALL DONE.** Every lane condition is met except one: egret has no FRESH report for the newest
pre-release `server-1.0.0-d2313a9eb651` (item 30). egret skips it on purpose: the maintainer's 2026-11-03 note in
egret `ci/BUILD-TRACK-TASK.md` says not to start a FRESH wave for pre-releases that carry only items 29/30 (HDFS SASL
path, which those waves do not exercise). Newest FRESH is `FRESH-413485ba80fd.md`. **Maintainer:** either accept
FRESH-413485ba80fd as the final FRESH result (then the orchestrator writes ALL DONE), or ask egret for a FRESH wave on
`d2313a9eb651`.
Done: **ITEM 26 DONE**, **ITEM 27 DONE**, **ITEM 28 DONE**, **ITEM 29 DONE**, **ITEM 30 DONE** (kestrel),
**IMPROVEMENTS DONE** (marten and heron).

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | none open (idles until the maintainer adds an item / answers) | ce6c56e1 13:42Z (~75 min, lock/unlock, no open item) | free | green: Dev CI 41770973165 (5253945e); package=true 41774819696 (d2313a9e on `ci/core-item30`). Later main commits `[skip ci]` only. |
| plugins (`PLUGINS-STATUS.md`) | paused; no new bug-first note | 2026-11-03 14:59Z | free | - |
| catalogs (`CATALOGS-STATUS.md`) | paused; no new bug-first note | 2026-11-03 14:59Z | free | - |
| lane 3 (`LANE3-STATUS.md`) | paused; no new bug-first note | branch 2026-10-29 | free | - |

No stale locks (marten and heron free).

## Service matrix

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 41776324282 | d2313a9e | pass |
| service-hdfs.yml | 41776328578 | d2313a9e | pass |
| service-security.yml | 41776332023 | d2313a9e | pass |
| service-stores.yml | 41776335441 | d2313a9e | pass |

## Pre-releases (newest 3)

- `server-1.0.0-d2313a9eb651` (2026-11-04 04:42Z): ITEM 30 (SASL node-local copies of non-local HDFS files; known
  limits in START-NOTES: joins / non-mergeable complete answers / ORDER BY-only scans bounded by the free-disk guard).
- `server-1.0.0-5122cea1f045` (2026-11-03 11:04Z): item 29 part 2 (SASL routing, clear errors, per-table views).
- `server-1.0.0-413485ba80fd` (2026-11-03 06:21Z): newest with a FRESH report (egret).

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: **IMPROVEMENTS DONE** (2026-11-03 10:00Z). Finish mode; last session 12:41Z (newest): no new server jar, no
  maintainer items. M4 default server `0e457de30f25`. Lock free.
- heron: **IMPROVEMENTS DONE** (session 98); session 109 (12:52Z, newest): nothing open, no new server build. Lock free.

## egret

- Newest report: `results/FRESH-413485ba80fd.md` (run 41604206524, 10/10 green, upstream `6e264c10f403`).
  Speed vs upstream native: scans x1/x4 1.00-1.04, S3 ADBC export 0.92 (local 0.98), slow-link ZSTD export 0.96-1.06,
  load 0.92-0.98; upstream native 0-1/22 on TPC-H, small-query errors. Ours 22/22, 0 errors.
- README "In progress" (13:26Z): idle; newest tracked 413485ba80fd; no FRESH wave for d2313a9eb651 (items 29/30 only).

## For the maintainer

- **ALL DONE decision:** accept `FRESH-413485ba80fd` as final, or request a FRESH wave on `d2313a9eb651` (see top).
- **Item 30 Open question (copies of joins / non-mergeable answers):** unanswered; item 30 closed with the free-disk
  guard (`hdfsLocalCopyMinFreeBytes`) as the bound. An answer (option 1 or a hard bound) would be a follow-up item.
- Core lane idles: remaining numbered items are stand checks (2, 13, 15, 27 rerun) or wait on you (0 S7/S8, 8, 9,
  1b libs pruning, libhadoop for the Java client).
- Open question (FYI): flake `EndpointFailoverIntegrationTest.ticketPlannedForAnotherNodeGivesTheSameAnswerAndMovesItsLoad`
  (Dev CI 41644812393); no repeat seen.
- Connector default changes to review: marten `flight.pushdown.join_keys` 10000 -> 50000; heron `arrow-flight.split-count` 4 -> 1.

## History

- 2026-11-04 14:58Z: no change: no core session since 13:42Z, locks free, Dev CI on main green (41770973165), service gate 4/4 green; marten (12:41Z)/heron (session 109)/egret (13:26Z) idle; ALL DONE still waits on the maintainer's FRESH decision.
- 2026-11-04 13:57Z: no change: core session 13:42Z found no open item; locks free, Dev CI on main green (41770973165), service gate 4/4 green; marten (12:41Z)/heron (session 109)/egret (13:26Z) idle; ALL DONE still waits on the maintainer's FRESH decision.
- 2026-11-04 12:57Z: no change: no core session since 09:42Z, locks free, Dev CI on main green (41770973165), service gate 4/4 green; marten (12:41Z)/heron (session 109)/egret idle; ALL DONE still waits on the maintainer's FRESH decision.
- 2026-11-04 11:57Z: no change: no core session since 09:42Z, locks free, Dev CI on main green (41770973165), service gate 4/4 green; marten (11:40Z)/heron (session 108)/egret idle; ALL DONE still waits on the maintainer's FRESH decision.
- 2026-11-04 10:58Z: no change: no core session since 09:42Z, locks free, Dev CI on main green (41770973165), service gate 4/4 green; marten/heron (session 107)/egret idle; ALL DONE still waits on the maintainer's FRESH decision.
- 2026-11-04 09:58Z: no change: core session 09:41-09:42Z found no open item; locks free, Dev CI on main green (41770973165), service gate 4/4 green; marten/heron (session 106)/egret idle; ALL DONE still waits on the maintainer's FRESH decision.
- 2026-11-04 08:58Z: no change since 07:57Z; all lanes idle, locks free, CI/services green; ALL DONE still waits on the maintainer's FRESH decision (d2313a9eb651).
- 2026-11-04 07:57Z: no change: core idle (lock free), Dev CI on main green (41770973165), service gate 4/4 green on d2313a9e; marten/heron (session 105)/egret idle; no stale locks; ALL DONE still waits on the maintainer (FRESH for d2313a9eb651 or accept FRESH-413485ba80fd).
- 2026-11-04 06:57Z: no change: core idle (lock free), Dev CI on main green (41770973165), service gate 4/4 green on d2313a9e; marten/heron (session 104)/egret idle; no stale locks; ALL DONE still waits on the maintainer (FRESH for d2313a9eb651 or accept FRESH-413485ba80fd).
- 2026-11-04 05:57Z: ITEM 30 DONE (pre-release d2313a9eb651, Dev CI package=true 41774819696, service gate 4/4 green on d2313a9e); core idle (no open item, lock free); Dev CI on main green (41770973165); egret skips FRESH for d2313a9eb651 per the maintainer's 2026-11-03 note, so ALL DONE waits on the maintainer; no stale locks.
- 2026-11-04 04:57Z: core lock 04:41Z (active), closing ITEM 30 with known limits (d2313a9e docs: START-NOTES/CHANGELOG; "ITEM 30 DONE" and `package=true` not yet written); e9cdd42a/5253945e green (Dev CI 41770973165, service-hdfs 41769492301, Catalogs 41769492328); marten/heron/egret idle; no stale locks.
- 2026-11-04 03:57Z: core lock 03:40Z (active), pushed e9cdd42a (item 30 part 2b: HAVING / DISTINCT aggregates merged from copy batches), Dev CI 41769492302 / service-hdfs 41769492301 running; 4a1d354f green (Dev CI 41726874635, service-hdfs 41726885857: small-disk stage fixed); lane proposes ITEM 30 DONE with the copy-join limit unless the maintainer answers; marten finish-mode session 03:40Z (nothing to do); heron/egret idle; no stale locks.
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
