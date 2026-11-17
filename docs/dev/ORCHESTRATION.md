# Orchestration status

ALL DONE withdrawn 2026-11-17T15:58Z for items 52-55 (maintainer notes 2026-11-17). Package once 52(a)/(b), 53, 54,
55(a)/(b)/(c) are each DONE. Done: 52(a), 53, 55(b). Open: 52(b) (merged, waits for cluster numbers) + 55(c) (lane 6),
54 (catalogs, step 3), 55(a) (lane 5).

Status as of **2026-11-17T19:58Z UTC** (hourly orchestrator; it only watches, it does not change code).

Points for the maintainer:
- **main Dev CI red** since the 55(a) merge dfaac51c: step 'Distributed complete under concurrency (items 35, 39 (d))',
  `48 of 48 exact; merged 32` -> `only 32 of 48 answers merged` (package run 44154778833, and again on bdeda437 push run
  44158521271 - same check, not infrastructural; answers exact). Your bug-first note is at the top of LANE5-STATUS (no
  extra note written). Lane 5's fix d50d4b63 "Dev CI item 35 step: keep one-row answers merged on its one node" is on
  main; full Dev CI **44162578438** (4662fa4f) running.
- **Item 52(b) merged** (bdeda437, lane 6: DuckDB-per-connection session whose external file cache exceeds its share is
  recycled, `duckdb=fileCacheRecycled`), but lane 6 does not write `ITEM 52(b) DONE` without real-cluster numbers (RSS with
  `duckDbSharedDatabase=false` + cache on HDFS) or a CI HDFS cell run; it needs main green first.
- New pre-release `server-1.0.0-f4feb0d5e404` (55(b)) still waits for the stand check (`-DdistributedAggregateMinBytes=8GiB`).
- Open questions unchanged: Q-52a, lane 3 Q5, Q54-1, Q-51a, Q-46, catalogs Q11, plugins Q-40a follow-up; stand checks of
  items 44 (2) / 46 / 50 / 51 / 55(b).
- Branches `pkg/f4feb0d5`, `pkg/6f0aa5a0`, `pkg/d2e27119`, `pkg/7ab48cd6`, `pkg/2c169a53`, `pkg/74464701`, `pkg/29f1267b`,
  `pkg/2ff22144`, `pkg/37bf9401` can be deleted.

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | idle (52(a) DONE 183a1775); notes main red, lane 5 fixing | 19:41Z | free | main Dev CI 44162578438 running |
| plugins (`PLUGINS-STATUS.md`) | ITEM 53 DONE 7f53357e; nothing open | 16:13Z | free | - |
| catalogs (`CATALOGS-STATUS.md`) | ITEM 54 step 3 of 3 (run 44159553651: serverCpus, plain/sasl, speed/balanced) | 19:32Z | free | - |
| lane 3 (`LANE3-STATUS.md`) | ITEM 55(b) DONE b5c677a9 (f4feb0d5e404); nothing else open | 19:04Z | free | - |
| lane 5 (`LANE5-STATUS.md`) | FIRST: main red (item 35 check); then ITEM 55(a) phase timings (724a1f99 merged, fix/55a 8cd73695) | 19:55Z | 19:07Z (active) | fix/55a Dev CI 44163831594 + DC tail perf 44163827273 running |
| lane 6 (`LANE6-STATUS.md`) | 52(b) merged bdeda437 (not DONE, needs cluster numbers); next 55(c) on fix/55c after main is green | 19:26Z | free | fast tier 44156451682 green; full 44158521271 red (item 35, lane 5's) |

Main CI: 44154778833 (dfaac51c, package) red and 44158521271 (bdeda437) red in 'Distributed complete under concurrency'
(merged 32 of 48 - expectation vs 55(a)'s one-endpoint-per-node merges); 44154760020 cancelled; 44162578438 (4662fa4f,
with lane 5's check fix) running. No rerun (not infrastructural). No stale locks (lane 5 19:07Z active; marten / heron
empty).

## Service matrix (newest run per workflow)

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 44146698684 | pkg/f4feb0d5 | pass |
| service-hdfs.yml | 44146705728 | pkg/f4feb0d5 | pass |
| service-security.yml | 44146711823 | pkg/f4feb0d5 | pass |
| service-stores.yml | 44146719328 | pkg/f4feb0d5 | pass |

## Pre-releases (newest 3)

- `server-1.0.0-f4feb0d5e404` (2026-11-17 18:03Z): items 52(a), 53, 55(b); egret FRESH running.
- `server-1.0.0-6f0aa5a026fa` (2026-11-16): item 51; egret FRESH DONE; marten m4 / compare default, heron pinned.
- `server-1.0.0-d2e27119fda4` (2026-11-16): items 49 + 50; the real cluster run of 2026-11-16 (items 52-55) used it.

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: IMPROVEMENTS DONE (2026-11-03). Finish mode; m4 regression on 6f0aa5a026fa all PASS (18:40-19:50Z); repeat
  compare SF10 ours speed 44160944676 22/22, 39.35 / 47.83 s warm / cold (earlier q01 connect error transient). Lock free.
- heron: IMPROVEMENTS DONE (session 98); finish mode, pinned 6f0aa5a026fa; session 323 idle (19:51Z). Lock free.

## egret

- Newest FRESH: **6f0aa5a026fa** (`results/FRESH-6f0aa5a026fa.md`; upstream `28d6ea713b8e`): speed/balanced 22/22, 0
  errors; load +34-41 % at 128 clients. README 'In progress': FRESH **f4feb0d5e404 RUNNING** (19:28Z, compare run
  44158872708 vs upstream `d05bf9c52129` - new upstream head; ACCEPT run 44154946019).

## History
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
- 2026-11-16T21:58Z: ALL DONE kept; marten m4 S3 1-9 without RF 43950963745 PASS (measurements complete, default switch waits for the maintainer); heron pinned 6f0aa5a026fa (e2e green, compare running, lock 20:51Z active); kestrel lanes nothing open, no new CI runs or pre-releases; no stale locks.
- 2026-11-16T20:58Z: **ALL DONE** restored: kestrel ITEM 51 DONE 6f0aa5a0 (20:50Z); marten ITEM 51 measured (SF1 ours first 43950959245 88/88; m4 S3 1-9 with RF PASS 43942242083) and runtime-filter default switch proposed (maintainer decision); heron new session (lock 20:51Z) running e2e + compare; no kestrel CI runs or pre-releases; no stale locks.
- 2026-11-16T19:58Z: marten SF10 ours vs ours_rf on 6f0aa5a026fa done (43899589063 / 43899594632, 22/22): ours_rf faster in both orders (warm 0.937 / 0.995, ~3 % order-corrected; q21 -16/-17 %, q11 -20/-30 %) -> numbers for kestrel `ITEM 51 DONE` are in; marten session (lock 19:41Z) running m4 S3 1-9 with/without RF + compare sf=1; heron session 308 idle; kestrel lanes nothing open, no new CI runs or pre-releases; no stale locks.
- 2026-11-16T15:58Z: marten unblocked: compare sf=10 ours/ours_rf both orders on 6f0aa5a026fa running (43899589063/43899594632, lock 15:40Z); heron UPSTREAM 2026-11 DONE a11a36c (patched series frozen by the maintainer); kestrel lanes nothing open, no new CI runs or pre-releases; egret idle; no stale locks.
- 2026-11-16T14:57Z: no change: 6f0aa5a026fa still not in marten m4-assets (no marten session since 12:41Z); heron idle on the patched-Trino question; lane 3 / catalogs (session 83) nothing open; no new CI runs or pre-releases; locks free; egret idle.
- 2026-11-16T13:58Z: no change in blockers: 6f0aa5a026fa still not in marten m4-assets (no marten session since 12:41Z); heron session 305 closed the q01 watch (43870753170, no regression), 306 idle on the patched-Trino question; plugins/catalogs nothing open; no new CI runs or pre-releases; locks free; egret idle.
- 2026-11-16T12:58Z: egret FRESH 6f0aa5a026fa DONE (43840086547 + rereport 43861228344; 22/22, 0 errors, no item 51 cost; local small ADBC c1 speed/balanced 1.26 carried over, larger); marten finish-mode session 12:41Z stopped again (6f0aa5a026fa still not in m4-assets); heron session 304 A/B: d2e27119fda4 = deafc861cce3, new session lock 12:53Z, connector-compare 43870753170 running; kestrel no code/CI change; no stale locks.
- 2026-11-16T11:58Z: no kestrel code/CI change (catalogs session 80 nothing open); marten finish-mode session 11:42Z again stopped: 6f0aa5a026fa still not in m4-assets; heron session 304 server A/B (43852765801) still running, lock 10:51Z active; egret FRESH 6f0aa5a026fa run 43840086547 finished, raw results committed 11:49Z, FRESH-6f0aa5a026fa.md not yet written; no stale locks.
- 2026-11-16T10:57Z: no kestrel code/CI change; marten finish-mode session (10:40Z) still without 6f0aa5a026fa in m4-assets; heron session 303 3-server agg check: partial not faster, Open question freeze/remove, session 304 server A/B running (lock 10:51Z); egret FRESH 6f0aa5a026fa still running; no stale locks.
- 2026-11-16T10:00Z: core ITEM 51 packaged -> pre-release server-1.0.0-6f0aa5a026fa (pkg Dev CI 43834254344 + 4 service runs green), core lock released; not yet in marten m4-assets (maintainer pinged); marten item (1) DONE (d2e27119fda4 m4 default); heron session 302 patched Trino not faster, Open question, 3-server agg check running (lock 09:51Z); egret FRESH 6f0aa5a026fa running; no stale locks, main CI green.
- 2026-11-16T09:00Z: maintainer 11:15 MSK notes taken: core ITEM 51 (runtime join filters from the ticket) implemented e2a3838f/6f0aa5a0, main Dev CI 43830148409 red -> 43831943883 green, package Dev CI 43834254344 on pkg/6f0aa5a0 running (core lock 08:08Z active); marten running d2e27119fda4 default-switch regression (m4 43829097466/43829093697 green, lock 08:08Z); heron patched-Trino measurement (connector-compare 43829210540 / 43835156106, e2e 43834715534 running, lock 08:51Z); ALL DONE still withdrawn; no stale locks.
- 2026-11-16T07:58Z: heron patched-Trino item progressing (80ebbbf..a11a36c; patched-trino 43825784443 green, e2e 43826068308 red, lane iterating; lock 06:51Z active); marten finish mode (07:41Z); catalogs session 76 nothing open; ALL DONE still withdrawn; kestrel unchanged, no new CI runs or pre-releases, no stale locks.
- 2026-11-16T07:00Z: marten UPSTREAM 2026-11 DONE flbcef73 (runtime filters no gain, opt-in); heron took maintainer's patched-Trino correction (80ebbbf Trino 483 patch series, CI running, lock 06:51Z); ALL DONE still withdrawn (waits for heron); kestrel unchanged, no new CI runs or pre-releases, no stale locks.
- 2026-11-16T05:57Z: egret FRESH d2e27119fda4 DONE (43798340706; speed/balanced 22/22, 0 errors; local small c1 p50 speed/balanced 1.15 ADBC / 1.12 JDBC carried over; load +42-43 %; upstream 28d6ea713b8e still 0-1/22); maintainer unblocked marten/heron with d2e27119fda4: marten compare sf=1,10 ours vs ours_rf running (43809296573/43809299659), heron pinned it (e2e 43808937358/43808940518 green), connector-compare 43808943657 SF10 running; ALL DONE still withdrawn; kestrel unchanged, locks free (marten/heron locks 05:13Z active).
- 2026-11-16T04:57Z: no change: ALL DONE still withdrawn; marten (04:41Z) / heron (session 299) blocked on the 2c169a53db3b upload; egret FRESH d2e27119fda4 (43798340706) still running; main Dev CI gate-only red, no new CI runs or pre-releases; locks free.
- 2026-11-16T03:58Z: ALL DONE withdrawn (items 49/50, pre-releases 7ab48cd6675c and d2e27119fda4; marten/heron blocked on 2c169a53db3b upload; egret FRESH d2e27119fda4 running; main Dev CI gate-only red).
- 2026-11-15T21:56Z: ALL DONE kept; no change (lanes nothing open, locks free, no new CI runs or pre-releases; marten/heron/egret idle).
- 2026-11-15T20:01Z: ALL DONE kept; no change (lanes nothing open, locks free, no new CI runs or pre-releases; marten/heron/egret idle).
- 2026-11-15T14:56Z: ALL DONE kept; no change (lanes nothing open, locks free, no new CI runs or pre-releases; marten/heron/egret idle).
- 2026-11-15T13:01Z: ALL DONE kept; no change (lanes nothing open, locks free, no new CI runs or pre-releases; marten/heron/egret idle).
- 2026-11-15T11:56Z: ALL DONE kept; no change (lanes nothing open, locks free, no new CI runs or pre-releases; marten/heron/egret idle).
- 2026-11-15T10:56Z: ALL DONE kept; no change (lanes nothing open, locks free, no new CI runs or pre-releases; marten/heron/egret idle).
- 2026-11-15T07:57Z: ALL DONE kept; no change (lanes nothing open, locks free, no new CI runs or pre-releases; marten/heron/egret idle).
- 2026-11-15T06:57Z: ALL DONE kept; no change (lanes nothing open, locks free, no new CI runs or pre-releases; marten/heron/egret idle).
- 2026-11-14T16:57Z: ALL DONE kept; no change (lanes nothing open, locks free, no new CI runs or pre-releases; marten/heron/egret idle).
- 2026-11-14T14:56Z: ALL DONE kept; no change (lanes nothing open, locks free, no new CI runs or pre-releases; marten/heron/egret idle).
- 2026-11-14T11:56Z: ALL DONE kept; no change (lanes nothing open, locks free, no new CI runs or pre-releases; marten/heron/egret idle).
- 2026-11-14T10:57Z: ALL DONE kept; no change (lanes nothing open, locks free, no new CI runs or pre-releases; marten/heron/egret idle).
