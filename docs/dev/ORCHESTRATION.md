# Orchestration status

Status as of **2026-11-09 09:56Z UTC** (hourly orchestrator; it only watches, it does not change code).

Not ALL DONE: **ITEM 38** (minor logs/hints) is done in code/CI and packaged as pre-release `server-1.0.0-b69660b9260a`
(19:05Z); it waits for the maintainer's stand check and "ITEM 38 DONE". The routine's original criterion (items 26/27,
marten/heron IMPROVEMENTS DONE) is long met; no egret FRESH on the newest pre-release (egret runs ACCEPT, not FRESH, on
log-only builds; **ACCEPT b69660b9260a SHIP**).

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | ITEM 38 done in CI, waits for the stand | 56557104, 2026-11-09 08:42Z lock/unlock only (no work) | free (released 08:42Z) | main Dev CI 42403022881 (b69660b9) green; package Dev CI 42403052289 (pkg/b69660b9) green |
| plugins (`PLUGINS-STATUS.md`) | paused; no new bug-first note | 2026-11-06 | free | - |
| catalogs (`CATALOGS-STATUS.md`) | paused; no new bug-first note | 2026-11-06 | free | - |
| lane 3 (`LANE3-STATUS.md`) | paused; no new bug-first note | branch 2026-10-29 | free | - |

No stale locks (marten and heron locks free). Newest Dev CI on main green, nothing to rerun.

## Service matrix (newest run per workflow)

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 42403042241 | b69660b9 (pkg) | pass |
| service-hdfs.yml | 42403044908 | b69660b9 (pkg) | pass |
| service-security.yml | 42403047248 | b69660b9 (pkg) | pass |
| service-stores.yml | 42403049750 | b69660b9 (pkg) | pass |

## Pre-releases (newest 3)

- `server-1.0.0-b69660b9260a` (2026-11-07 ~18:50-19:05Z): item 38 (1)-(4); waits for the stand; egret ACCEPT SHIP (42406253240).
- `server-1.0.0-06c5adc7d7c9` (2026-11-07 07:02Z): item 37 (a)-(d); **ITEM 37 DONE on the stand**; egret ACCEPT SHIP.
- `server-1.0.0-deafc861cce3` (2026-11-06 11:30Z): third in the release list (the API lists it before 6384bc55b173 now); egret FRESH DONE; marten/heron CHECKED.

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: **IMPROVEMENTS DONE** (2026-11-03); SERVER deafc861cce3 CHECKED; session 2026-11-09 09:40Z "finish mode, nothing new".
  Open question (no decision needed to continue): TPC-H q16 SF1 ~0.5 s slowdown from join-key IN-list pushdown - fix or leave.
- heron: **IMPROVEMENTS DONE**; SERVER deafc861cce3 CHECKED; session 181 (2026-11-09 09:53Z): nothing open.
- Neither has checked a server build newer than `deafc861cce3`.

## egret

- Newest FRESH: **FRESH deafc861cce3 DONE** (run 42223833724): speed vs balanced scans x4/x8 1.30-1.58, load +42-45 %
  at 128 clients, 22/22, 0 errors; upstream native `9e4787906b07` still fails TPC-H/small.
- **ACCEPT b69660b9260a DONE: SHIP** (42406253240, all 6 checks in all 4 profile x servers cells, room wait 30 s).

## For the maintainer

- Stand check of `server-1.0.0-b69660b9260a` for ITEM 38 (stop under load: no `runningCalls=1` on a `done` line;
  keytab-less KEYRING start WARN; one-line INFO fallback), then "ITEM 38 DONE".
- **Open question (item 35):** default budget `distributedCompletePeerStreams` (one per 128 MiB direct memory).
- **marten open question:** TPC-H q16 SF1 slowdown from join-key pushdown - fix or leave.

## History

- 2026-11-09 09:56Z: no change: no core session since 08:42Z (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases since 2026-11-07; marten (09:40Z)/heron (session 181)/egret (09:26Z README, idle) idle; no stale locks.
- 2026-11-09 08:56Z: no change: core session 08:42Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases since 2026-11-07; marten (08:41Z)/heron (session 180)/egret (07:30Z README, idle) idle; no stale locks.
- 2026-11-09 07:57Z: no change: no core session since 05:41Z (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases since 2026-11-07; marten (07:41Z)/heron (session 179)/egret (07:30Z README, idle) idle; no stale locks.
- 2026-11-09 06:58Z: no change: no core session since 05:41Z (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases since 2026-11-07; marten (06:41Z)/heron (session 178)/egret (05:27Z README, idle) idle; no stale locks.
- 2026-11-09 05:57Z: no change: core session 05:41Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases since 2026-11-07; marten (05:41Z)/heron (session 177)/egret (05:27Z README, idle) idle; no stale locks.
- 2026-11-09 04:57Z: no change: core session 04:41Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases since 2026-11-07; marten (04:41Z)/heron (session 176)/egret (03:27Z README, idle) idle; no stale locks.
- 2026-11-09 03:56Z: no change: core session 03:41Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases since 2026-11-07; marten (03:41Z)/heron (session 175)/egret (03:27Z README, idle) idle; no stale locks; no orchestrator status 22:56Z-02:56Z (no runs).
- 2026-11-08 21:56Z: no change: core session 21:41Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (21:41Z)/heron (session 174)/egret (21:26Z README, idle) idle; no stale locks.
- 2026-11-08 20:56Z: no change: no core session since 19:41Z (lock free; item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (20:41Z)/heron (session 173)/egret (19:27Z README, idle) idle; no stale locks.
- 2026-11-08 19:56Z: no change: core session 19:41Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (19:40Z)/heron (session 172)/egret (19:27Z README, idle) idle; no stale locks.
- 2026-11-08 18:56Z: no change: core session 18:41Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (18:40Z)/heron (session 171)/egret (17:26Z README, idle) idle; no stale locks.
- 2026-11-08 17:57Z: no change: core session 17:41Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (17:41Z)/heron (session 170)/egret (17:26Z README, idle) idle; no stale locks.
- 2026-11-08 16:56Z: no change: core session 16:43Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (16:40Z)/heron (session 169)/egret (15:26Z README, idle) idle; no stale locks.
- 2026-11-08 15:56Z: no change: core session 15:41Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (15:41Z)/heron (session 168)/egret (15:26Z README, idle) idle; no stale locks.
- 2026-11-08 14:56Z: no change: core session 14:41Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (14:41Z)/heron (session 167)/egret (13:27Z README, idle) idle; no stale locks.
- 2026-11-08 13:56Z: no change: no core commit since 10:42Z (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (12:41Z)/heron (session 166)/egret (13:27Z README, idle) idle; no stale locks.
- 2026-11-08 12:57Z: no change: no core commit since 10:42Z (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (12:41Z)/heron (session 165)/egret (11:26Z README, idle) idle; no stale locks.
- 2026-11-08 11:57Z: no change: no core commit since 10:42Z (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (11:40Z)/heron (session 164)/egret (11:26Z README, idle) idle; no stale locks.
- 2026-11-08 10:57Z: no change: core session 10:42Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (10:42Z)/heron (session 163)/egret (09:27Z README, idle) idle; no stale locks.
- 2026-11-08 09:57Z: no change: core session 09:41Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (09:41Z)/heron (session 162)/egret (09:27Z README, idle) idle; no stale locks.
- 2026-11-08 08:57Z: no change: core session 08:41Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (08:41Z)/heron (session 161)/egret (07:27Z README, idle) idle; no stale locks.
- 2026-11-08 07:57Z: no change: core session 07:41Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (07:41Z)/heron (session 160)/egret (07:27Z README, idle) idle; no stale locks.
- 2026-11-08 06:57Z: no change: core session 06:41Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (06:41Z)/heron (session 159)/egret (05:27Z README, idle) idle; no stale locks.
- 2026-11-08 05:56Z: no change: core session 05:41Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (05:41Z)/heron (session 158)/egret (05:27Z README, idle) idle; no stale locks.
- 2026-11-08 04:57Z: no change: core session 04:41Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (04:41Z)/heron (session 157)/egret (03:27Z README, idle) idle; no stale locks.
- 2026-11-08 03:57Z: no change: core session 03:41Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (03:40Z)/heron (session 156)/egret (03:27Z README, idle) idle; no stale locks.
- 2026-11-07 21:56Z: no change: core session 21:41Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (21:40Z)/heron (session 155)/egret (21:27Z README, idle) idle; no stale locks.
- 2026-11-07 20:57Z: egret ACCEPT b69660b9260a DONE: SHIP (42406253240); core session 20:40Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases; marten (20:40Z)/heron (session 154) idle; no stale locks.
- 2026-11-07 19:58Z: item 38 packaged: pre-release `server-1.0.0-b69660b9260a` (pkg/b69660b9: Dev CI 42403052289 + 4 service runs green; main Dev CI 42403022881 green); core unlocked 19:05Z, waits for the stand ("ITEM 38 DONE"); egret ACCEPT b69660b9260a running (42406253240); marten (19:41Z)/heron (session 153) idle; no stale locks.
- 2026-11-07 18:57Z: maintainer wrote ITEM 37 DONE (stand, 06c5adc7d7c9, ~30k answers none wrong) and opened ITEM 38 (minor log/hint follow-ups); core session (lock 18:41Z, active) pushed b69660b9 (item 38 (1)-(4)) and dispatched package runs on pkg/b69660b9 (Dev CI 42403052289 + 4 service runs) plus main Dev CI 42403022881, all running; egret ACCEPT 06c5adc7d7c9 recheck SHIP (42396888303, after harness fix fcf1eb3); marten (18:40Z)/heron (session 152) idle; no stale locks.
- 2026-11-07 17:58Z: egret: ACCEPT 06c5adc7d7c9 rerun 42390647941 lost 3 speed cells to a harness layout flake (report file now says DO NOT SHIP, setup-only); harness fix fcf1eb3, reruns 42396888303/42398263063 running. Kestrel unchanged since 13:41Z (item 37 waits for the stand); no new CI/pre-releases; marten (17:41Z)/heron (session 151) idle; no stale locks.
- 2026-11-07 16:56Z: no change: no core commits since the 13:41Z unlock (item 37 waits for the stand re-check of server-1.0.0-06c5adc7d7c9); no new CI runs or pre-releases; marten (16:41Z)/heron (session 150)/egret (15:26Z) idle; no stale locks.
- 2026-11-07 15:56Z: no change: no core commits since the 13:41Z unlock (item 37 waits for the stand re-check of server-1.0.0-06c5adc7d7c9); no new CI runs or pre-releases; marten (15:40Z)/heron (session 149)/egret (15:26Z) idle; no stale locks.
- 2026-11-07 14:56Z: no change: no core commits since the 13:41Z unlock (item 37 waits for the stand re-check of server-1.0.0-06c5adc7d7c9); no new CI runs or pre-releases; marten (14:41Z)/heron (session 148)/egret (13:27Z) idle; no stale locks.
- 2026-11-07 13:56Z: no change: core session 13:40Z only re-confirmed item 37 waits for the stand re-check of server-1.0.0-06c5adc7d7c9; no new CI runs or pre-releases; marten (13:41Z)/heron (session 147)/egret (13:27Z) idle; no stale locks.
- 2026-11-07 12:57Z: no change: no core commits since 07:42Z unlock (item 37 waits for the stand re-check of server-1.0.0-06c5adc7d7c9); no new CI runs or pre-releases; marten (12:40Z)/heron (session 146)/egret (11:27Z) idle; no stale locks.
- 2026-11-07 11:56Z: no change: no core commits since 07:42Z unlock (item 37 waits for the stand re-check of server-1.0.0-06c5adc7d7c9); no new CI runs or pre-releases; marten (11:42Z)/heron (session 145)/egret (11:27Z) idle; no stale locks.
- 2026-11-07 10:56Z: no change since 09:56Z: no core commits since 07:42Z unlock (item 37 waits for the stand re-check of server-1.0.0-06c5adc7d7c9); no new CI runs or pre-releases; marten (10:41Z)/heron (session 144)/egret idle; no stale locks.
- 2026-11-07 09:56Z: no change: no kestrel lane commits since 07:42Z (core waits for the stand on `server-1.0.0-06c5adc7d7c9`); no new CI runs or pre-releases; marten (09:41Z)/heron (session 143)/egret idle; no stale locks.
- 2026-11-07 08:57Z: egret ACCEPT 06c5adc7d7c9 DONE: SHIP (42351380956, all 6 checks x 4 cells); no kestrel lane commits since 07:42Z (core waits for the stand on `server-1.0.0-06c5adc7d7c9`); no new CI runs; marten (08:41Z)/heron (session 142) idle; no stale locks.
