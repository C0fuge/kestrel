# Orchestration status

ALL DONE withdrawn 2026-11-17T15:58Z for items 52-55 (maintainer notes 2026-11-17). Package once 52(a)/(b), 53, 54,
55(a)/(b)/(c) are each DONE. Done: 52(a), 53, 54 (f1ad5b22), 55(b), 55(c) (e8afe351, LANE 6 DONE). Open, both waiting
for the maintainer's real-cluster rerun: 52(b) (merged bdeda437) and 55(a) (merged, defaults per answers a3a7f029).

Status as of **2026-11-18T04:58Z UTC** (hourly orchestrator; it only watches, it does not change code).

Points for the maintainer:
- **Build for the real-cluster rerun is ready:** pre-release `server-1.0.0-5f02e5b35611` (published 04:09Z; package
  Dev CI 44231671868 green, service cluster 44233398192 / hdfs 44233401931 / security 44233407122 / stores 44233410601
  all green). `main` 5f02e5b3 is still the newest code commit, so it holds every runtime change (52(a)/(b), 53, 54 +
  Q54-1, 55(a) defaults, 55(b), 55(c), Q-52a). 52(b), 55(a) (goal p99 <= 1.5x speed, scans >= 0.9x) and the Q54-2 stand
  numbers wait for your run on it; lanes 5/6 then write their DONE lines. Not yet in marten m4-assets / heron
  e2e-assets (your coordinator distributes it).
- Lane 6 note for the core lane (unchanged): `Distributed perf` (44210800053 / 44211990773) stays red by design of its
  check (it expects every `on` answer merged; small-aggregate / point-lookup local decisions keep one query on one
  node). Not a main Dev CI failure; the check's expectation may need your call.
- Open questions otherwise unchanged: lane 3 Q5, Q54-2 (stand numbers), Q-46, catalogs Q11, plugins Q-40a follow-up,
  Q-53 stand check; stand checks of items 44 (2) / 46 / 50 / 51 / 55(b).
- Branches `pkg/f4feb0d5`, `pkg/6f0aa5a0`, `pkg/d2e27119`, `pkg/7ab48cd6`, `pkg/2c169a53`, `pkg/74464701`, `pkg/29f1267b`,
  `pkg/2ff22144`, `pkg/37bf9401` can be deleted.

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | nothing open (04:50Z: 5f02e5b3 already packaged; package only if code lands after it) | 04:41Z | free | main Dev CI 44215691965 green |
| plugins (`PLUGINS-STATUS.md`) | nothing open (ITEM 53 DONE 7f53357e) | 03:11Z | free | - |
| catalogs (`CATALOGS-STATUS.md`) | ITEM 54 DONE f1ad5b22; nothing open (Q54-2 = stand numbers) | 04:20Z | free | - |
| lane 3 (`LANE3-STATUS.md`) | ITEM 55(b) DONE b5c677a9; nothing open | 2026-11-17 23:06Z | free | - |
| lane 5 (`LANE5-STATUS.md`) | 55(a) waits for the maintainer's rerun on 5f02e5b35611 | 04:36Z | free | package 44231671868 green |
| lane 6 (`LANE6-STATUS.md`) | **LANE 6 DONE**; 52(b) waits for cluster numbers | 01:01Z | free | Distributed perf red by its check (see above) |

Main CI: newest Dev CI on main 44215691965 (5f02e5b3, full tier, push) green; no new runs since; no rerun needed. No
stale locks (all kestrel locks empty; marten / heron empty). No bug-first notes open in any lane.

## Service matrix (newest run per workflow)

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 44233398192 | pkg/5f02e5b3 | pass |
| service-hdfs.yml | 44233401931 | pkg/5f02e5b3 | pass |
| service-security.yml | 44233407122 | pkg/5f02e5b3 | pass |
| service-stores.yml | 44233410601 | pkg/5f02e5b3 | pass |

## Pre-releases (newest 3)

- `server-1.0.0-5f02e5b35611` (published 2026-11-18 04:09Z): 52(b), 54 + Q54-1, 55(a) defaults, 55(c), Q-52a; build
  for the real-cluster rerun; egret FRESH not yet.
- `server-1.0.0-f4feb0d5e404` (2026-11-17 18:03Z): items 52(a), 53, 55(b); egret ACCEPT SHIP + FRESH DONE.
- `server-1.0.0-6f0aa5a026fa` (2026-11-16 08:31Z): item 51; egret FRESH DONE; marten m4 / compare default, heron pinned.

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: IMPROVEMENTS DONE (2026-11-03). Finish mode; session 04:40-04:45Z nothing to do. Lock free.
- heron: IMPROVEMENTS DONE (session 98); finish mode, pinned 6f0aa5a026fa; session 326 idle (04:51Z). Lock free.

## egret

- Newest FRESH: **f4feb0d5e404** (`results/FRESH-f4feb0d5e404.md`, compare 44158872708 + rereport 44183288832; upstream
  native `d05bf9c52129`): speed/balanced 22/22, 0 errors; upstream 0/22 ADBC / 1/22 JDBC. Balanced matches speed under
  load. Last session 03:27Z idle (before 5f02e5b35611 was published); FRESH for 5f02e5b35611 expected next session.
  README 'In progress': no running task.

## History
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
