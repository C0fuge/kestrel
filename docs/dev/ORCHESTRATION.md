# Orchestration status

ALL DONE (2026-11-12T09:56Z) **withdrawn** since 2026-11-15 22:00Z. Remaining for ALL DONE: kestrel `ITEM 51 DONE`
(packaged, waits for marten's SF10 `ours,ours_rf` numbers), marten ITEM 51 re-measure (item (2)), heron's patched-Trino
item (measured, waits for the maintainer's freeze/remove answer), egret FRESH write-up on `server-1.0.0-6f0aa5a026fa`
(run finished, raw results in, `FRESH-6f0aa5a026fa.md` pending).

Status as of **2026-11-16T11:58Z UTC** (hourly orchestrator; it only watches, it does not change code).

Points for the maintainer:
- **Still blocking marten (and so ITEM 51 DONE):** pre-release `server-1.0.0-6f0aa5a026fa` (ITEM 51) is not in marten's
  `m4-assets`; marten's 11:42Z finish-mode session checked again and stopped (newest there still d2e27119fda4). Please
  upload it (jar, flight-plugins, flight-server-libs, flight-auth, SHA256SUMS).
- **heron Open question: freeze or remove the patched-Trino series** (proposal: freeze; sessions 302/303: patched not
  faster on 1 server or a 3-server cluster). Session 304 (lock 10:51Z) runs a same-job server A/B deafc861cce3 vs
  d2e27119fda4 (connector-compare 43852765801, in progress).
- egret raw numbers for 6f0aa5a026fa (run 43840086547): speed/balanced local small ADBC c1 p50 **1.26** (JDBC 1.11; the
  known local c1 regression, d2e27119fda4 had 1.15 / 1.12), S3 small c32 p95 0.06, S3 SF10 ADBC 0.11, local TPC-H
  0.95-1.03; ours has no errors in any job. Upstream native still fails TPC-H answers, died after small (local and S3).
- Still open: Q-51a (does marten's connector read in partial mode), Q-46 (hadoopfs negotiated QOP / cipher), catalogs
  Q11; stand checks of items 50 / 51.
- Branches `pkg/6f0aa5a0`, `pkg/d2e27119`, `pkg/7ab48cd6`, `pkg/2c169a53`, `pkg/74464701`, `pkg/29f1267b`, `pkg/2ff22144`,
  `pkg/37bf9401` can be deleted.

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | ITEM 51 PACKAGED (waits for numbers); items 26 / 27 DONE 2026-11-03 | 09:27Z | free | main Dev CI 43831943883 green (6f0aa5a0); pkg 43834254344 green |
| plugins (`PLUGINS-STATUS.md`) | nothing open | 10:11Z | free | - |
| catalogs (`CATALOGS-STATUS.md`) | session 80 nothing open | 11:22Z | free | - |
| lane 3 (`LANE3-STATUS.md`) | ITEM 49(d), ITEM 50 DONE; nothing open | 04:03Z | free | lane/distributed Dev CI 43782908034 green |

No new bug-first notes in any lane. No stale locks: kestrel locks all empty; marten `docs/LOCK` empty (released 11:42Z);
heron `docs/LOCK` 10:51Z (active, commit 11:05Z). No new kestrel CI runs since 09:05Z.

## Service matrix (newest run per workflow)

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 43836555978 | pkg/6f0aa5a0 | pass |
| service-hdfs.yml | 43836560693 | pkg/6f0aa5a0 | pass |
| service-security.yml | 43836564596 | pkg/6f0aa5a0 | pass |
| service-stores.yml | 43836569443 | pkg/6f0aa5a0 | pass |

## Pre-releases (newest 3)

- `server-1.0.0-6f0aa5a026fa` (2026-11-16): item 51 (ticket runtime filters ARF1 in partial mode); egret FRESH run 43840086547 finished (write-up pending); not in marten m4-assets.
- `server-1.0.0-d2e27119fda4` (2026-11-16): item 50 + item 49; egret ACCEPT SHIP (43790722012), FRESH DONE (43798340706); marten m4 default.
- `server-1.0.0-7ab48cd6675c` (2026-11-16): item 49; egret ACCEPT SHIP (43786698707); FRESH skipped (contained in d2e27119fda4).

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: IMPROVEMENTS DONE (2026-11-03); UPSTREAM 2026-11 DONE `flbcef73`. Maintainer 11:15 MSK items: (1) DONE 09:45Z
  (m4 default d2e27119fda4); (2) waits for 6f0aa5a026fa in `m4-assets` (finish-mode sessions 10:40Z and 11:42Z stopped early).
- heron: IMPROVEMENTS DONE (session 98). Patched-Trino item: sessions 302/303 measured (1 server and 3-server cluster):
  patched not faster, verdict stock Trino; Open question freeze/remove. Session 304 server A/B (43852765801) running.

## egret

- Newest FRESH write-up: **d2e27119fda4** (`results/FRESH-d2e27119fda4.md`, run 43798340706; upstream `28d6ea713b8e`):
  speed/balanced 22/22, 0 errors; local small c1 p50 speed/balanced 1.15 ADBC / 1.12 JDBC; load +42-43 % at 128 clients;
  speed/upstream load 0.94-0.96, scans 1.07-1.09.
- FRESH 6f0aa5a026fa: run 43840086547 finished; raw results `results/fresh-up-6f0aa5a026fa-43840086547/` committed 11:49Z
  (numbers above); README 'In progress' still RUNNING; next egret step: load rereport + `FRESH-6f0aa5a026fa.md`.

## History

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
