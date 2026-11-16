# Orchestration status

ALL DONE (2026-11-12T09:56Z) **withdrawn**: new work since 2026-11-15 22:00Z (kestrel items 49 / 50, two new pre-releases,
new upstream items in marten and heron); egret FRESH for the newest pre-release is still running.

Status as of **2026-11-16T04:57Z UTC** (hourly orchestrator; it only watches, it does not change code).

Points for the maintainer:
- **marten and heron are blocked on you:** your 2026-11-16 01:45 notes need server `server-1.0.0-2c169a53db3b` for the
  before/after times, but it is not in marten's `m4-assets` (newest there: deafc861cce3) nor in heron's `e2e-assets`.
  Upload it (or name the newest pre-release `server-1.0.0-d2e27119fda4` instead). heron also asks (Open questions):
  move production to a self-built Trino 455 with upstream's partial-aggregation engine patch, or close the item as
  not applicable to stock Trino; and what the unfilled `{rule}` placeholder in your 2026-11-16 note meant.
- **Pre-release `server-1.0.0-d2e27119fda4`** (items 49 + 50) is packaged and green; egret ACCEPT **SHIP** for both
  7ab48cd6675c and d2e27119fda4 (runs 43786698707, 43790722012). The stand check of item 50 on the real cluster is yours.
- Main's own Dev CI 43784755805 (cf60940e) is red **only in the service gate** (main moved during the build; build job
  green; the same code d2e27119 is green on `pkg/d2e27119`, 43786741509). Not a test failure, no bug-first note; not
  rerun because the gate would wait for service runs that do not exist for that commit. The next code push on main re-runs it.
- Q-46 (hadoopfs negotiated QOP / cipher) still open; catalogs Q11 still open.
- Branches `pkg/d2e27119`, `pkg/7ab48cd6`, `pkg/2c169a53`, `pkg/74464701`, `pkg/29f1267b`, `pkg/2ff22144`, `pkg/37bf9401` can be deleted.

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | ITEM 50 PACKAGED (02:05Z); no open core item until your next note | 02:05Z (~3 h) | free | main Dev CI 43784755805 red (gate only); pkg 43786741509 green |
| plugins (`PLUGINS-STATUS.md`) | ITEM 49(b) DONE f0adaf52; nothing open | 03:11Z | free | - |
| catalogs (`CATALOGS-STATUS.md`) | ITEM 49(c) DONE df3dae71; session 73 nothing open | 04:21Z | free | - |
| lane 3 (`LANE3-STATUS.md`) | ITEM 49(d) DONE 64d566cb, ITEM 50 DONE d2e27119; nothing open (04:03Z status) | 04:03Z | free | Dev CI main 43774021600 (a69848c9) green |

No bug-first notes in any lane. No stale locks (all kestrel locks and marten/heron `docs/LOCK` empty).

## Service matrix (newest run per workflow)

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 43788205329 | pkg/d2e27119 | pass |
| service-hdfs.yml | 43788208920 | pkg/d2e27119 | pass |
| service-security.yml | 43788212571 | pkg/d2e27119 | pass |
| service-stores.yml | 43788216221 | pkg/d2e27119 | pass |

## Pre-releases (newest 3)

- `server-1.0.0-d2e27119fda4` (2026-11-16): item 50 (dc small scans / count(*) on the planning node) + item 49; egret ACCEPT SHIP (43790722012), FRESH running (43798340706).
- `server-1.0.0-7ab48cd6675c` (2026-11-15): item 49 (upstream server changes since cec626bbb54a); egret ACCEPT SHIP (43786698707); FRESH skipped (contained in d2e27119fda4).
- `server-1.0.0-2c169a53db3b` (2026-11-12): Q-40b-1, items 47 / 48; egret ACCEPT SHIP, FRESH DONE 43175544843.

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: IMPROVEMENTS DONE (2026-11-03). New maintainer item (2026-11-16 01:45, upstream StarRocks connector changes):
  join runtime filters adopted (series `4a9848e-flbcef73`; FE 43771325687 52/52, BE 43771314584, be-ut 43771314581 24/24
  green); measurement blocked (still at 04:41Z: 2c169a53db3b not in `m4-assets`).
- heron: IMPROVEMENTS DONE. New maintainer item (upstream Trino partial aggregation pushdown): reviewed (session 297),
  blocked on maintainer input (session 299, 03:51Z).

## egret

- Newest FRESH result: still **2c169a53db3b** (`results/FRESH-2c169a53db3b.md`, run 43175544843; upstream `cfaa929746cb`):
  22/22 speed and balanced, 0 errors; small c1 p50 speed/balanced 1.09 JDBC / 1.14 ADBC; S3 TPC-H 0.13-0.16; load +39-42 %.
- README 'In progress': EGRET ACCEPT 7ab48cd6675c / d2e27119fda4 DONE (SHIP); **EGRET FRESH d2e27119fda4 RUNNING**
  (run 43798340706 since 03:27Z, still in progress at 04:56Z; upstream now `28d6ea713b8e`).

## History

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
- 2026-11-12 07:58Z: egret started FRESH 43175544843 + ACCEPT 43175547634 on `server-1.0.0-2c169a53db3b` (07:28Z); ALL DONE still suspended; no kestrel lane commit since 05:03Z; marten/heron idle; all CI green, no stale locks.
- 2026-11-12 06:58Z: no change: no kestrel lane commit since 05:03Z; egret has not started FRESH/ACCEPT on `server-1.0.0-2c169a53db3b`; marten (06:42Z)/heron (session 226) idle; all CI green, no stale locks.
- 2026-11-12T05:57Z: egret FRESH 74464701e372 DONE (43148390757; 22/22, local small c1 speed/balanced 1.12 JDBC / 1.23 ADBC carried over); egret not yet on newest pre-release 2c169a53db3b, so ALL DONE still suspended; lanes nothing open, locks free, no new CI runs or pre-releases, no red CI.
- 2026-11-12T04:57Z: pre-release **server-1.0.0-2c169a53db3b** published (Q-40b-1, items 47/48, Q3; pkg Dev CI 43150534816 + 4 service runs green); egret ACCEPT 74464701e372 SHIP (43148393964), FRESH 43148390757 still running; ALL DONE suspended (no FRESH on 2c169a53db3b); lanes nothing open, locks free, no red CI.
- 2026-11-12T03:58Z: ALL DONE suspended (egret FRESH on newest pre-release 74464701e372 running: 43148390757 / ACCEPT 43148393964); maintainer's 21:10 answers implemented: Q-40b-1 2f31c071, ITEM 47 DONE 2c169a53 (catalogs), ITEM 48 DONE 97ad4f22 + Q3 (lane 3); core lane (lock 03:46Z) packaging on pkg/2c169a53 (Dev CI 43150534816 running); main Dev CI 43147654545 red only at the service gate (main moved), no rerun; no stale locks.
- 2026-11-11T17:57Z: ALL DONE kept; plugins ITEM 46 DONE c96a1272 (main 74464701; Dev CI 43064709644, Service hdfs 43064709693, Catalogs CI 43064709784 green); core lane (lock 17:44Z) packaging on pkg/74464701 (Dev CI 43071198734 running); no new pre-release; marten/heron/egret idle; no stale locks, no red CI.
- 2026-11-11T16:57Z: ALL DONE kept; plugins ITEM 46 on fix/46 progressing: Dev CI 43056505119 green after mirror fixes, Service hdfs 43056510473 red (apt infra), lane pushed bea11172 and 43060538104 is running (no orchestrator rerun); main unchanged, no new pre-release; marten/heron/egret idle; no stale locks.
- 2026-11-11T15:57Z: ALL DONE kept, but plugins ITEM 46 (maintainer stand note 18:40: sizing budget, qid=null in denials, QOP log; Q-46 for the extension repo) on fix/46, unmerged: TPC-H perf 43047430684 green, Dev CI / Service hdfs red on runner ubuntu.com DNS (infra), failed job of 43050095655 re-run once; main unchanged, locks free, no new pre-release; marten/heron/egret idle.
- 2026-11-11T12:57Z: ALL DONE kept; no change (lanes nothing open, plugins state line 12:20Z; locks free; no new CI runs or pre-releases; marten 12:40Z / heron session 218 / egret 11:29Z README idle).
- 2026-11-11T09:58Z: ALL DONE kept; no change (lanes nothing open, locks free, no new CI runs or pre-releases; marten/heron idle; egret idle on 29f1267b995f).
- 2026-11-11T07:57Z: **ALL DONE** again: egret FRESH 29f1267b995f DONE (run 42965886044, balanced TPC-H OOM fixed, 22/22 everywhere; local small c1 speed/balanced 1.19 JDBC / 1.16 ADBC regression by the rule) and ACCEPT 29f1267b995f SHIP (42965889054 + rerun 42972471705); lanes nothing open, locks free, no new kestrel CI runs or pre-releases; marten/heron idle.
- 2026-11-11T06:57Z: no change in lanes (all nothing open, locks free, no new kestrel CI runs or pre-releases); egret ACCEPT 42965889054 on 29f1267b995f hit apt mirror failures in 4 cells (infra, harness retry 95395a4), rerun 42972471705 + FRESH 42965886044 running; ALL DONE still suspended.
- 2026-11-11T05:58Z: no change in lanes (core/plugins/catalogs/lane 3 nothing open; plugins 05:15Z note: 29f1267b995f is the stand build for item 44 / Q-40a); egret FRESH 42965886044 + ACCEPT 42965889054 on 29f1267b995f running (upstream head `46d565e93c19`); ALL DONE still suspended; no new CI runs, no stale locks, no red code CI.
- 2026-11-11T04:59Z: MORNING BUILD DONE: pre-release `server-1.0.0-29f1267b995f` (hadoopfs afc8 default, items 44+45) from pkg/29f1267b, all package runs green (Dev CI 42956935228, Service cluster/hdfs/security/stores, Catalogs CI); main Dev CI 42955017367 red only at the service gate (main moved by the 03:57Z orchestrator commit during the build; not a test failure, no rerun); ALL DONE suspended until egret FRESH on 29f1267b995f; no stale locks.
- 2026-11-11T03:58Z: ALL DONE kept; plugins ITEM 44 DONE e75705c9 (part (2) Ranger staleness recheck merged); core lane on the morning note: afc8 default 29f1267b, package=true Dev CI 42955017367 running (two superseded Dev CI runs cancelled); Service cluster 42955011424 green, Service hdfs 42955011583 running, Catalogs CI green; marten/heron idle; no stale locks, no red CI, no new pre-release yet.
- 2026-11-10T21:58Z: ALL DONE kept; balanced TPC-H OOM (Q-43m) fixed as plugins ITEM 44 (1)(3) on main 2335ddc6 (tpch-perf 42903634429 fix/44 balanced 22/22, red only on the baseline's OOM), part (2) Ranger staleness open; lane 3 ITEM 45 DONE b8613fc4 (Dev CI 42901100322, Service cluster 42901104843 green); main Dev CI 42906724548 green; maintainer's morning note (afc8 default + package=true) waits for the core session; no stale locks, no new pre-release.
- 2026-11-10 20:58Z: ALL DONE kept; core lane filed Q-43m (balanced OOM = Q-42(a) cap, options A/B/C, needs maintainer); egret HDFS acceptance rerun 42885490049 SHIP; no new CI runs or pre-releases; marten/heron nothing new; no stale locks.
- 2026-11-10T19:58Z: egret FRESH 2ff221447b5d DONE (run 42865250390, results/FRESH-2ff221447b5d.md): **default balanced profile fails TPC-H with DuckDB OOM at 128 MiB memory_limit** (new vs 37bf9401d77c; errors, not silent; speed 22/22); local ADBC c1 speed/balanced 1.11; ALL DONE conditions met again (with this finding). Gizmo wave 42888240521 + maintainer ACCEPT 42885490049 running. Plugins lane 19:14Z note (Q-40a build exists); no new CI runs, no locks, no red CI.
- 2026-11-10T18:56Z: egret ACCEPT 2ff221447b5d SHIP (42865242528, check 6 passes); FRESH 42865250390 still running. Core/marten/heron sessions: nothing open. No new CI runs, no locks, no stale locks, no red CI.
- 2026-11-10T17:58Z: real-cluster pre-release `server-1.0.0-2ff221447b5d` published, all package runs green; egret ACCEPT 42865242528 + FRESH 42865250390 running on it; catalogs/marten/heron idle; no locks held, no stale locks, no red CI.
- 2026-11-10T16:57Z: refresh (ALL DONE). **ITEM 40(b) DONE 135e6dac** written 16:06Z by the catalogs lane (c1 p50 ~10-15 % below deafc861, HDFS point +2..6 %; Q-40b-1 proposed to close without a default change, needs maintainer OK). Core lane took the lock 16:41Z and runs the real-cluster package: Dev CI 42857169249 on `pkg/2ff22144` (`package=true`) in progress; no new pre-release yet (newest `server-1.0.0-37bf9401d77c`). main Dev CI 42837979902 green; service matrix unchanged (cluster 42840719877 pass, hdfs 42794862424, security/stores 42777504125/42777507448 pass). marten/heron: nothing new, locks empty; egret idle, newest FRESH 37bf9401d77c. No stale locks, no red CI.
- 2026-11-10T15:57Z: refresh (ALL DONE). Catalogs 40(b) session active (lock 15:22Z): Small query perf 42843856272 (main) + 42843860772 (perf/40b-final) running, HDFS point perf 42843864824 (main) / 42843869104 green; ITEM 40(b) DONE not yet written; lane 3 Service cluster 42840719877 green on main; main Dev CI 42837979902 (34e24d3b) green; plugins session 15:16Z nothing open; no new pre-release or egret FRESH; no stale locks, no red CI.
- 2026-11-10T14:57Z: refresh (ALL DONE). Catalogs 40(b) session (8) merged 135e6dac (pruning shapes for decimal / long / date literals; branch Dev CI 42835145513 green, Small query perf 42835176027 within noise), lock released; ITEM 40(b) DONE not yet written (next: JFR check, gizmo vs speed on HDFS point cell); Q-40b-1 proposed for closing without a default change (needs maintainer OK); main Dev CI 42828677361 (6abf6264) green, 42837979902 (34e24d3b) running; core package for the real cluster still waits for 40(b) DONE; all locks free, no red CI, no new pre-release or egret FRESH.
- 2026-11-10T13:58Z: refresh (ALL DONE). Catalogs 40(b) cut 5 (3a01265e, one pruning per DoGet) on main, Dev CI 42822143053 green, c1 p50 back at baseline without a default change, so Q-40b-1 is now optional; next cut 4228c733 (one jOOQ parse per point-query shape) on main, Dev CI 42828677361 running; lock catalogs 13:24Z active; no stale locks, no red CI.
