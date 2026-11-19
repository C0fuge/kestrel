# Orchestration status

ALL DONE 2026-11-18T17:58Z (restored; withdrawn 2026-11-17T15:58Z for items 52-55, items 56 and 57 added 2026-11-18). Done: 52(a),
53, 54 (f1ad5b22), 55(b), 55(c) (e8afe351, LANE 6 DONE), 56 (36f33023), 57 (06d6468b). marten and heron both run
06d6468b17c5 as default; egret FRESH 06d6468b17c5 DONE. Still waiting only for the maintainer's real-cluster rerun:
52(b) (merged bdeda437) and 55(a) (merged, defaults per answers a3a7f029).

Status as of **2026-11-19T12:57Z UTC** (hourly orchestrator; it only watches, it does not change code).

Points for the maintainer:
- **egret FRESH 06d6468b17c5 DONE** (44335959845 + rereport 44362838876; upstream native `47c6c7d95db0`, new head):
  item 57 reverts the Q54-1 balanced S3 scan gain as expected (speed/balanced S3 x4/x8 1.25/1.22, was 1.13/1.14;
  balanced/upstream S3 x4 0.85); balanced S3 ADBC export vs upstream 0.83 (was 0.66); balanced scan RSS -20 %; TPC-H
  balanced S3 mixed (SF1 ADBC -18 %, SF10 JDBC +15 % on the same CPU type). **Flag: speed S3 SF10 ADBC 38.1 s vs
  32.3 s in f4feb0d5e404 (+18 %, same 9V74 type, one run; item 57 does not touch speed) - egret suggests a re-check.**
- heron: session 335 closed the q09/q10/q12 SF10 watch (44338448083) and q21 SF1 (44352121022) at parity; e2e kill
  delays fixed (push e2e 44352452531 green); sessions 336-348 finish mode, nothing open (348 at 12:51Z).
- marten: finish mode, 06d6468b17c5 default (unchanged since 16:42Z).
- Q-57a (core): `speed` still drops the insertion order (options: leave / keep order / per-query second connection;
  lane recommends leave).
- Open questions otherwise unchanged: lane 3 Q5, Q54-2 (stand numbers), Q-46, catalogs Q11, plugins Q-40a follow-up,
  Q-53 stand check; stand checks of items 44 (2) / 46 / 50 / 51 / 55(b) / 56 / 57.
- Branches `pkg/06d6468b`, `pkg/36f33023`, `pkg/f4feb0d5`, `pkg/6f0aa5a0`, `pkg/d2e27119`, `pkg/7ab48cd6`,
  `pkg/2c169a53`, `pkg/74464701`, `pkg/29f1267b`, `pkg/2ff22144`, `pkg/37bf9401` can be deleted.

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | ITEM 57 DONE 06d6468b; nothing open (Q-57a) | 13:53Z | free | main 44311547842 green |
| plugins (`PLUGINS-STATUS.md`) | nothing open | 12:13Z (note) | free | - |
| catalogs (`CATALOGS-STATUS.md`) | nothing open (Q54-2 = stand numbers) | 12:21Z (note) | free | - |
| lane 3 (`LANE3-STATUS.md`) | ITEM 56 DONE; nothing open | 18:03Z (note) | free | - |
| lane 5 (`LANE5-STATUS.md`) | 55(a) waits for the maintainer's rerun | 05:03Z | free | - |
| lane 6 (`LANE6-STATUS.md`) | LANE 6 DONE; 52(b) waits for cluster numbers | 05:03Z | free | - |

Main CI: newest Dev CI on main 44311547842 (06d6468b) green; no new runs since. No stale locks (all kestrel, marten
and heron locks empty). No new bug-first notes in the paused lanes.

## Service matrix (newest run per workflow)

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 44314762330 | pkg/06d6468b | pass |
| service-hdfs.yml | 44314767562 | pkg/06d6468b | pass |
| service-security.yml | 44314772794 | pkg/06d6468b | pass |
| service-stores.yml | 44314777714 | pkg/06d6468b | pass |

## Pre-releases (newest 3)

- `server-1.0.0-06d6468b17c5` (2026-11-18 13:52Z): item 57; marten + heron default; egret ACCEPT SHIP + FRESH DONE.
- `server-1.0.0-36f3302321ba` (2026-11-18 07:17Z): egret ACCEPT SHIP 44258093085, no FRESH; has the q14 regression.
- `server-1.0.0-5f02e5b35611` (2026-11-18 04:09Z): egret ACCEPT SHIP + FRESH DONE; first build with the q14 regression.

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: IMPROVEMENTS DONE (2026-11-03). 06d6468b17c5 m4 / compare default after full regression PASS; finish mode
  (6637d7e 17:41Z), lock free.
- heron: IMPROVEMENTS DONE (session 98). 06d6468b17c5 default; sessions 335-348 nothing open (12:51Z), lock
  free; last runs green (e2e 44352452531, connector-compare 44352121022).

## egret

- Newest FRESH: 06d6468b17c5 (results/FRESH-06d6468b17c5.md; upstream native `47c6c7d95db0`), see points above.
- 06d6468b17c5: ACCEPT SHIP (44321703096).

## History
- 2026-11-19T12:57Z: ALL DONE holds; only plugins (12:13Z) and catalogs (12:21Z) no-open-item notes; main green (44311547842), no new CI, services or pre-releases; marten finish-mode 10:40Z, heron session 348 (12:51Z) nothing open; egret idle 11:26Z (FRESH 06d6468b17c5 newest); no stale locks.
- 2026-11-19T11:56Z: ALL DONE holds; only plugins (11:12Z) and catalogs (11:21Z) no-open-item notes; main green (44311547842), no new CI, services or pre-releases; marten finish-mode 10:40Z, heron session 347 (11:51Z) nothing open; egret idle 11:26Z (FRESH 06d6468b17c5 newest); no stale locks.
- 2026-11-19T10:56Z: ALL DONE holds; only catalogs no-open-item note (10:21Z); main green (44311547842), no new CI, services or pre-releases; marten finish-mode 10:40Z, heron session 346 (10:51Z) nothing open; egret idle 09:26Z (FRESH 06d6468b17c5 newest); no stale locks.
- 2026-11-19T06:57Z: ALL DONE holds; only plugins (06:11Z) and catalogs (06:21Z) no-open-item notes; main green (44311547842), no new CI, services or pre-releases; marten finish-mode 03:41Z, heron session 342 (06:52Z) nothing open; egret idle 05:26Z (FRESH 06d6468b17c5 newest); no stale locks.
- 2026-11-19T05:56Z: ALL DONE holds; only catalogs no-open-item note (05:21Z); main green (44311547842), no new CI, services or pre-releases; marten finish-mode 03:41Z, heron session 341 (05:51Z) nothing open; egret idle 05:26Z (FRESH 06d6468b17c5 newest); no stale locks.
- 2026-11-19T04:56Z: ALL DONE holds; no kestrel commits since 03:58Z; main green (44311547842), no new CI, services or pre-releases; marten finish-mode 03:41Z, heron session 340 (04:51Z) nothing open; egret idle 03:26Z (FRESH 06d6468b17c5 newest); no stale locks.
- 2026-11-19T03:58Z: ALL DONE holds; only lane 3 (03:03Z) and plugins (03:12Z) no-open-item notes; main green (44311547842), no new CI, services or pre-releases; marten finish-mode 03:41Z and heron session 339 (03:51Z) nothing new; egret idle 03:26Z (FRESH 06d6468b17c5 newest); no stale locks.
- 2026-11-18T21:58Z: ALL DONE holds; only core no-open-item lock/unlock (21:41Z) and catalogs no-open-item note (21:21Z); main green (44311547842), no new CI, services or pre-releases; marten finish-mode session 21:41Z nothing new; heron/egret unchanged (egret idle 21:26Z); no stale locks.
- 2026-11-18T20:58Z: ALL DONE holds; only catalogs no-open-item note (20:21Z); main green (44311547842), no new CI, services or pre-releases; heron session 338 nothing open; marten/egret unchanged; no stale locks.
- 2026-11-18T19:58Z: ALL DONE holds; only lane no-open-item notes (plugins 19:11Z, catalogs 19:20Z); main green (44311547842), no new CI, services or pre-releases; heron session 337 finish mode nothing open; marten/egret unchanged; no stale locks.
- 2026-11-18T18:58Z: ALL DONE holds; only lane no-open-item notes (plugins, catalogs, lane 3); main green (44311547842), no new CI, services or pre-releases; marten/heron/egret unchanged since 17:51Z; no stale locks.
- 2026-11-18T17:58Z: **ALL DONE restored**: egret FRESH 06d6468b17c5 DONE (44335959845; item 57 reverts Q54-1 S3 scan gain as expected, speed S3 SF10 ADBC +18 % flagged for re-check); heron closed the SF10 q09/q10/q12 watch at parity, e2e kill delays fixed (44352452531 green), finish mode; marten finish mode; kestrel only lane no-open-item notes, main green, no new CI or pre-releases; no stale locks.
- 2026-11-18T16:58Z: marten made 06d6468b17c5 the m4/compare default after full regression PASS (check 13 44339957571); heron pinned it as default (e2e PASS), session 335 active (lock 16:51Z) re-checking compare 44338448083; egret ACCEPT 06d6468b17c5 SHIP, FRESH 44335959845 running; kestrel only lane no-open-item notes, main green, no new CI or pre-releases; no stale locks.
- 2026-11-18T16:00Z: marten confirms ITEM 57 fix (compare 44326063071: default q14 1.42 -> 1.39 s vs 6f0aa5a026fa, 22/22, no query > 10 % slower; m4 S3/iceberg PASS, lakefs + check 13 running); heron connector-compare 44338448083 running; egret ACCEPT 44321703096 + FRESH 44335959845 on 06d6468b17c5 running; kestrel only lane no-open-item notes, main green, no new CI or pre-releases; no stale locks.
- 2026-11-18T14:58Z: 06d6468b17c5 in marten m4-assets / heron e2e-assets (maintainer 17:29 MSK); marten dispatched compare + m4 S3 on it (bba5755), heron session active (both locks 14:30Z); kestrel only lane no-open-item notes, main green, no new CI or pre-releases; egret not yet on 06d6468b17c5; no stale locks.
- 2026-11-18T13:58Z: **ITEM 57 DONE 06d6468b** (cause Q54-1 insertion order; balanced keeps file order again), main Dev CI 44311547842 + package 44311669381 + 4 service runs green -> pre-release server-1.0.0-06d6468b17c5 (13:52Z); marten bisect agrees (5f02e5b35611, cd26b0e), waits for m4-assets; new Q-57a (speed's order); no stale locks.
- 2026-11-18T12:58Z: maintainer added **ITEM 57** (core, 15:50 MSK): TPC-H q14 +102 % in balanced via StarRocks on 36f3302321ba vs 6f0aa5a026fa (marten 44300586688, same EPYC 7763); core lane lock 12:53Z active; marten told to wait (6f0aa5a026fa stays default) and bisect over f4feb0d5e404 / 5f02e5b35611 (lock 12:53Z); no new CI runs or pre-releases, main green; no stale locks.
- 2026-11-18T11:57Z: heron closed SF10 q05/q07 watch (44289327005, noise), nothing open; marten dispatched same-CPU compare baseline 6f0aa5a026fa + repeat 36f3302321ba (lock 11:40Z); kestrel only no-op notes (plugins, catalogs, core lock/unlock 11:41Z), main green, no new CI or pre-releases; no stale locks.
- 2026-11-18T10:56Z: heron pinned 36f3302321ba as default after full regression (e2e 44274803788/44274809554 PASS, A/B 44274814805 within noise, watch SF10 q05/q07); marten functional regression all PASS, compare 44285073710 pending; kestrel only lane 3 note 10:03Z, no new CI runs or pre-releases, main green; no stale locks.
- 2026-11-18T09:58Z: maintainer note (12:20 MSK) in marten + heron: full regression on 36f3302321ba then default switch; marten m4 S3 1-9 PASS 44274880025, lakefs dispatched; heron session active; kestrel no new CI runs or pre-releases (only catalogs note 09:21Z), main green; no stale locks.
- 2026-11-18T08:58Z: egret ACCEPT 36f3302321ba SHIP (44258093085), no FRESH (DC-only change); no new CI runs, commits only lane no-open-item notes; main green (44246657183); 52(b)/55(a) wait for stand rerun; marten/heron idle; no stale locks.
- 2026-11-18T07:57Z: package pkg/36f33023 green (44253203263 + 4 service runs) -> pre-release server-1.0.0-36f3302321ba (07:17Z, first with item 56) for the stand rerun; egret FRESH 5f02e5b35611 DONE (balanced S3 ADBC export 0.66 of upstream, SF1 +20 % flagged), ACCEPT 36f3302321ba 44258093085 running; main CI green; lanes nothing open; marten/heron idle; no stale locks.
- 2026-11-18T06:56Z: item 56 main full Dev CI 44246657183 green; core lane dispatched package pkg/36f33023 (Dev CI 44253203263 running) for the first pre-release with item 56; egret FRESH 5f02e5b35611 (44244388077) still running; lanes nothing open; marten/heron idle; no stale locks.
- 2026-11-18T05:58Z: maintainer added ITEM 56 (dc scan stream fails under memory pressure, lane 3, low priority); lane 3 fixed it same hour: ITEM 56 DONE 36f33023 (merged plain scans open 2 endpoint streams per node, `distributedCompleteScanStreamsPerNode`, 0 = old), branch fast CI 44243247868 + service cluster 44243265348 green; full Dev CI on main 44246657183 running; needs a package for the stand; egret ACCEPT 5f02e5b35611 SHIP, FRESH 44244388077 running; marten/heron idle; no stale locks.
- 2026-11-18T04:58Z: package pkg/5f02e5b3 green (Dev CI 44231671868 + 4 service runs) -> pre-release server-1.0.0-5f02e5b35611 published 04:09Z, the build for the maintainer's rerun (52(b)/55(a)/Q54-2); main CI 44215691965 green, no new code; lanes nothing open; marten/heron idle; egret FRESH 5f02e5b35611 pending; no stale locks.
- 2026-11-18T03:57Z: main green (push Dev CI 44215691965 on 5f02e5b3); maintainer answers taken (Q-52a c9d9b4f4, Q-55a defaults a3a7f029, Q54-1 f1ad5b22); ITEM 54 DONE, ITEM 55(c) DONE + LANE 6 DONE; red 44207492475 was SpotBugs, fixed 6c8f66de; package pkg/5f02e5b3 (44231671868 + 4 service runs) running; 52(b)/55(a) wait for stand rerun; marten/heron/egret idle; no stale locks.
- 2026-11-17T21:58Z: main green (full Dev CI 44175118886 / 44180696125 green; 44179759902 cancelled, superseded); lane 5 merged 55(a) cluster-snapshot reuse / file-plan refresh-ahead / member-check memory (66b19368), new Q-55a-mc; lane 6 55(c) step 3 merged 5b751015 (not DONE); catalogs 54 step 3 part 4 (HDFS point perf 44178894320 green, unread); egret FRESH f4feb0d5e404 DONE; marten/heron idle; no stale locks.
- 2026-11-17T20:57Z: main green again (Dev CI 44162578438 / 44166060698 / 44169154810 green; item 35 check fixed by lane 5); lane 6 55(c) step 1 merged 72edab3d/e3d14331 (not DONE); lane 5 fix/55a cluster-snapshot work (Dev CI 44174010663 green, DC tail perf 44174076316 running); catalogs 54 step 3 part 3 (HDFS point perf 44172681969 running); egret FRESH f4feb0d5e404 still running; marten/heron finish mode idle; no stale locks.
- 2026-11-17T19:58Z: main Dev CI red after 55(a) merge (item 35 check, 44154778833 / 44158521271; maintainer note in LANE5), lane 5 fix d50d4b63 on main, Dev CI 44162578438 running; lane 6 52(b) merged bdeda437 (not DONE, needs cluster numbers); catalogs 54 step 3 run 44159553651; egret FRESH f4feb0d5e404 running (upstream d05bf9c52129); marten compare 44160944676 22/22; heron 323 idle; no stale locks.
- 2026-11-17T18:57Z: new pre-release server-1.0.0-f4feb0d5e404 (55(b), package + 4 service runs green); catalogs 54 step 2 (HDFS point perf 44147368932 running); lane 5 fix/55a Dev CI 44151537756 green, DC tail perf running; lane 6 lock 16:16Z quiet since 15:24Z; marten GO done (RF default, 6f0aa5a026fa default, compare sf=10 vs jdbc dispatched); heron 322 idle; egret not yet on f4feb0d5e404; main effectively green; no stale locks.
- 2026-11-17T15:58Z: **ALL DONE withdrawn** for items 52-55 (maintainer 2026-11-17, lanes 5/6 added). Done: 52(a) 183a1775, 53 7f53357e, 55(b) b5c677a9; open: 52(b)/55(c) lane 6, 54 catalogs (lock 14:59Z), 55(a) lane 5 (lock 15:15Z). Main Dev CI 44117935189 green (87cd3c00), 44118413074 running; c74e9018 red 44115291931 superseded. marten: maintainer GO on RF default, regression running. New Qs: Q-52a, lane 3 Q5. No stale locks.
- 2026-11-17T14:57Z: ALL DONE kept; no change: kestrel only lane no-open-item notes (lane 3 14:04Z, catalogs 14:22Z), no new CI runs or pre-releases (newest 6f0aa5a026fa), no red CI; marten 14:41Z still waits for the maintainer (RF default proposal); heron session 320 idle; egret idle (13:28Z); all locks empty.
- 2026-11-17T13:57Z: ALL DONE kept; no change: kestrel only lane no-open-item notes (lane 3 13:04Z, plugins 13:16Z, catalogs 13:24Z), no new CI runs or pre-releases (newest 6f0aa5a026fa), no red CI; marten 13:41Z still waits for the maintainer (RF default proposal); heron session 319 idle; egret idle (13:28Z); all locks empty.
- 2026-11-17T12:57Z: ALL DONE kept; no change: kestrel only lane no-open-item notes (lane 3 12:03Z, catalogs 12:22Z), no new CI runs or pre-releases (newest 6f0aa5a026fa), no red CI; marten 12:41Z still waits for the maintainer (RF default proposal); heron session 318 idle; egret idle (11:26Z); all locks empty.
- 2026-11-17T11:57Z: ALL DONE kept; no change: kestrel only lane no-open-item notes (lane 3 11:03Z, plugins 11:11Z, catalogs 11:21Z), no new CI runs or pre-releases (newest 6f0aa5a026fa), no red CI; marten 11:41Z still waits for the maintainer (RF default proposal); heron sessions 316/317 idle; egret idle (11:26Z); all locks empty.
- 2026-11-17T09:57Z: no change: lanes only "no open item" notes (plugins 09:11Z, catalogs 09:21Z); marten 09:40Z / heron session 315 idle (marten still waits for the maintainer on the RF default proposal); no new kestrel CI runs or pre-releases (newest 6f0aa5a026fa), no red CI, no stale locks; egret idle (09:26Z). ALL DONE kept.
- 2026-11-17T06:58Z: ALL DONE kept; no change: kestrel only catalogs no-open-item note (06:21Z), no new CI runs or pre-releases, no red CI; marten/heron/egret idle; all locks empty; marten RF default proposal still waits for the maintainer.
- 2026-11-17T05:58Z: ALL DONE kept; no change: kestrel only lane no-open-item notes (lane 3, plugins, catalogs 05:04-05:21Z), no new CI runs or pre-releases; marten waiting for the maintainer; heron session 312 idle; egret idle; no stale locks.
- 2026-11-17T04:58Z: ALL DONE kept; no change: kestrel only catalogs session 89 no-open-item note, no new CI runs or pre-releases; marten still waiting for the maintainer; heron session 310 cleared SF1 watch (44013068299, noise), 311 idle; no stale locks.
- 2026-11-17T03:58Z: ALL DONE kept; no new kestrel commits except lane no-open-item notes, no new CI runs or pre-releases; marten waiting for the maintainer (runtime-filter default proposal, 03:40Z); heron session active (lock 03:50Z); no stale locks.
