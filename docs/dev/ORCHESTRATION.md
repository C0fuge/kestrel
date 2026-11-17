# Orchestration status

ALL DONE withdrawn 2026-11-17T15:58Z for items 52-55 (maintainer notes 2026-11-17; previous ALL DONE 07:56Z). Package
once 52(a)/(b), 53, 54, 55(a)/(b)/(c) are each DONE.

Status as of **2026-11-17T15:58Z UTC** (hourly orchestrator; it only watches, it does not change code).

Points for the maintainer:
- New Open questions: **Q-52a** (STATUS.md: default memory budget on big hosts without a cgroup limit - keep WARN only,
  cap the host budget per core, or budget RSS overshoot ~1.5x) and **lane 3 Q5** (LANE3-STATUS.md: default of
  `distributedAggregateMinBytes`, now 0 = off). Lane 6 plans a new `duckDbExternalFileCacheMaxBytes` with a proposed
  default (min(DuckDB budget / 4, 8 GiB)) that it will put into Open questions.
- Still open: Q-51a (runtime filters in complete mode / ARF2 bloom filters), Q-46, catalogs Q11, plugins Q-40a follow-up;
  stand checks of items 44 (2) / 46 / 50 / 51 / 55(b) (`-DdistributedAggregateMinBytes=8GiB` on the 1.27 TB table).
- Branches `pkg/6f0aa5a0`, `pkg/d2e27119`, `pkg/7ab48cd6`, `pkg/2c169a53`, `pkg/74464701`, `pkg/29f1267b`, `pkg/2ff22144`,
  `pkg/37bf9401` can be deleted.

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | ITEM 52(a) DONE 183a1775; nothing else open (package after 52-55) | 15:30Z | free | main Dev CI 44111204306 green (183a1775) |
| plugins (`PLUGINS-STATUS.md`) | ITEM 53 DONE 7f53357e (balanced uses the shared DuckDB) | 15:43Z | free | fix/53-balanced Dev CI 44113887980 green |
| catalogs (`CATALOGS-STATUS.md`) | ITEM 54 (single-stream HDFS scan vs upstream), branches fix/54-* | 15:02Z | 14:59Z (active) | HDFS point perf 44116044127 running (fix/54-buffer) |
| lane 3 (`LANE3-STATUS.md`) | ITEM 55(b) DONE b5c677a9 (not measured at scale; Q5); branch `lane/distributed-55a-scans` offered to lane 5 | 15:48Z | free | Dev CI 44114130596, Service cluster 44113741833 green |
| lane 5 (`LANE5-STATUS.md`) | ITEM 55(a) dc tails, branch fix/55a | 15:53Z (CI) | 15:15Z (active) | fix/55a Dev CI 44119431410 + DC tail perf 44119432833 running; Distributed perf 44113635774 red (perf tool, lane working) |
| lane 6 (`LANE6-STATUS.md`) | ITEM 52(b) diagnosed (external file cache), fix next; then 55(c) | 15:24Z | free | - |

Main CI: newest completed Dev CI 44117935189 green (87cd3c00, fast-tier workflow change); 44118413074 (28ebd7e5) running.
44115291931 on c74e9018 (build failed with the first fast-tier version) is superseded by the green 87cd3c00 run - no
bug-first note needed. No stale locks (kestrel catalogs 14:59Z / lane 5 15:15Z active; marten 14:59Z active, commit
15:41Z; heron empty).

## Service matrix (newest run per workflow)

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 44113741833 | lane/distributed 6efb56d2 | pass |
| service-hdfs.yml | 43836560693 | pkg/6f0aa5a0 | pass |
| service-security.yml | 43836564596 | pkg/6f0aa5a0 | pass |
| service-stores.yml | 43836569443 | pkg/6f0aa5a0 | pass |

## Pre-releases (newest 3)

- `server-1.0.0-6f0aa5a026fa` (2026-11-16): item 51; egret FRESH DONE; marten m4 / compare default candidate.
- `server-1.0.0-d2e27119fda4` (2026-11-16): items 49 + 50; the real cluster run of 2026-11-16 (items 52-55) used it.
- `server-1.0.0-7ab48cd6675c` (2026-11-15): item 49.

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: IMPROVEMENTS DONE (2026-11-03). Maintainer 2026-11-17 18:30 GO on both: runtime filters default
  (`max_keys` 4096, `wait_ms` 500; series fl234f9b) and 6f0aa5a026fa as m4 / compare default. Regression running: m4 S3
  1-9 PASS 44111873484, iceberg 1,11,12 and lakefs 1,3,4,10 dispatched (15:13Z / 15:41Z). Lock 14:59Z active.
- heron: IMPROVEMENTS DONE (session 98); UPSTREAM 2026-11 DONE a11a36c, series frozen; finish mode, pinned 6f0aa5a026fa;
  session 321 idle (15:51Z).

## egret

- Newest FRESH: **6f0aa5a026fa** (`results/FRESH-6f0aa5a026fa.md`; upstream `28d6ea713b8e`): speed/balanced 22/22, 0
  errors; local small ADBC c1 p50 speed/balanced 1.26 (regression by the rule, carried over); load +34-41 % at 128
  clients (0.95-0.96 of upstream native). No newer pre-release. README 'In progress': idle 15:26Z, newest tracked
  6f0aa5a026fa.

## History
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
- 2026-11-12T09:56Z: **ALL DONE** again: egret FRESH 2c169a53db3b DONE (43175544843; 22/22, 0 errors; local small c1 speed/balanced 1.09 JDBC / 1.14 ADBC carried over; speed local SF10 +5 %); core 09:41Z / marten 09:41Z / heron 229 nothing open; all CI green, no stale locks.
- 2026-11-12 08:57Z: egret ACCEPT 2c169a53db3b **SHIP** (43175547634, all 6 checks, 4 cells); FRESH 43175544843 still running, so ALL DONE still suspended; catalogs session 13 (08:25Z) no open item; marten/heron idle; all CI green, no stale locks.
