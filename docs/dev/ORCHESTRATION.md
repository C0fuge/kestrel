# Orchestration status

Status as of **2026-11-09 20:58Z UTC** (hourly orchestrator; it only watches, it does not change code).

Not ALL DONE: **ITEM 39** (bug first, real-cluster benchmark of build `35c15ab226b9`): all four parts are on `main` -
39(a) core, 39(b) 39f84617 plugins, 39(c) **DONE bf72114c** (catalogs, 20:44Z; parts 1-4: file pruning by footer
statistics incl. aliased single-table queries; HDFS point perf 42689843600: NameNode opens per point query ~20 -> ~11),
39(d) a952f415 lane 3. Remaining: main Dev CI 42695876926 (bf72114c, running), then the core lane's `package=true` and
"ITEM 39 DONE". ITEM 38 still waits for the stand check on `server-1.0.0-b69660b9260a`. The routine's original
criterion (items 26/27, marten/heron IMPROVEMENTS DONE) is long met.

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | package item 39 (`package=true`) + "ITEM 39 DONE"; item 38 waits for the stand | 2026-11-09 20:41Z (status: waits for 39(c)) | free | main Dev CI 42695876926 (bf72114c) running; 42690037660 (32ebda26) + Service cluster 42690037488 + Catalogs CI 42690037957 green |
| plugins (`PLUGINS-STATUS.md`) | nothing open (39(b) DONE, finish mode) | 2026-11-09 14:30Z | free | - |
| catalogs (`CATALOGS-STATUS.md`) | 39(c) DONE; follow-ups (a)-(d) non-blocking; Q10/Q11 open | 2026-11-09 20:44Z | free (released) | fix/39c-point Dev CI 42693013191 + HDFS point perf 42693005244 green |
| lane 3 (`LANE3-STATUS.md`) | D13 (b) DONE; next D14 (stand results) | 2026-11-09 20:27Z (branch) | free | Service cluster 42689843545 + 42691698496 green (42689694253 red: workflow-file run on 8a468214, superseded) |

No red Dev CI on main; nothing to rerun. No new bug-first notes in the paused-lane files.

## Service matrix (newest run per workflow)

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 42691698496 | 48d9f99c (lane/distributed) | pass (main: 42690037488 32ebda26 pass) |
| service-hdfs.yml | 42403044908 | b69660b9 (pkg) | pass |
| service-security.yml | 42403047248 | b69660b9 (pkg) | pass |
| service-stores.yml | 42403049750 | b69660b9 (pkg) | pass |

## Pre-releases (newest 3)

- `server-1.0.0-b69660b9260a` (2026-11-07 18:47Z): item 38; waits for the stand; egret ACCEPT SHIP (42406253240).
- `server-1.0.0-06c5adc7d7c9` (2026-11-07 05:57Z): item 37; ITEM 37 DONE on the stand.
- `server-1.0.0-deafc861cce3` (2026-11-06 11:04Z): egret FRESH DONE; marten/heron CHECKED.

No item 39 pre-release yet (next core session).

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: **IMPROVEMENTS DONE** (2026-11-03); SERVER deafc861cce3 CHECKED; session 20:40Z "finish mode, nothing new".
  Open question: TPC-H q16 SF1 ~0.5 s slowdown from join-key IN-list pushdown - fix or leave.
- heron: **IMPROVEMENTS DONE**; SERVER deafc861cce3 CHECKED; session 189 (20:51Z): nothing open.

## egret

- Newest FRESH for a pre-release: **FRESH deafc861cce3 DONE** (run 42223833724): speed vs balanced scans x4/x8
  1.30-1.58, load +42-45 % at 128 clients, 22/22, 0 errors; upstream native `9e4787906b07` still fails TPC-H/small.
- **EGRET GIZMO DONE** (20:34Z, `results/FRESH-gizmo-deafc861cce3.md`; runs 42632367579, 42683525682, 42684180966):
  GizmoSQL v1.42.2 22/22 everywhere, 0 errors, small-set gate fails on `meta_schemas` only, no result compression.
  Speed / gizmo: local TPC-H 1.01-1.07, raw scans 0.99-1.01, load 0.85-0.91 at 1 client; **S3 TPC-H SF10 1.48-1.51
  (gizmo ~34 s vs ~51 s)**; S3 small-mix at 8/32 clients 0.76-0.93 (speed ahead). **Local small c1 p50 regression
  speed vs balanced confirmed: 1.16 JDBC / 1.18 ADBC.**
- ACCEPT b69660b9260a DONE: SHIP. Next: FRESH/ACCEPT on the item 39 pre-release.

## For the maintainer

- ITEM 39: all four parts on `main`; packaging (`package=true`) and "ITEM 39 DONE" by the next core session.
- Stand check of `server-1.0.0-b69660b9260a` for ITEM 38, then "ITEM 38 DONE".
- egret: S3 TPC-H SF10 ~1.5x slower than GizmoSQL; local small-query c1 p50 speed-profile regression (1.16-1.18) -
  candidates for a later item.
- **Open questions:** item 35 default `distributedCompletePeerStreams`; item 33 default `hdfsLocalCopyRoomWaitMillis`
  (0 vs 30000); catalogs Q10 (footer-stats pruning on by default) and Q11 (balanced fd requests per point query);
  marten q16 SF1 pushdown slowdown.

## History

- 2026-11-09 20:58Z: ITEM 39(c) DONE bf72114c (catalogs, parts 1-4 on main, lock released); main Dev CI 42695876926 on bf72114c running (32ebda26: 42690037660 green); core lane next packages (`package=true`, ITEM 39 DONE); lane 3 D13 (b) DONE (42689843545, 42691698496 green); egret EGRET GIZMO DONE (S3 SF10 speed ~1.5x slower than gizmo, local small c1 p50 regression 1.16-1.18); no red CI on main, no stale locks.
- 2026-11-09 19:57Z: catalogs lane active again (lock 19:22Z): 39(c) file pruning 9f4f901f on fix/39c-point, Dev CI 42684272737 + HDFS point perf 42684276829 green, not merged; lane 3 D13 (c) merged (Service cluster 42680042903/42681427105 green); egret gizmo scan rerun 42684180966 running after harness login fix, draft report written; no red CI, no stale locks.
- 2026-11-09 18:58Z: released stale LOCK-CATALOGS (15:23:58Z); ITEM 39 waits only for 39(c) (parts 1+2 on main, 42641096583 green; next pruning by footer stats, Q9 answered); lane 3 D16 DONE 97a2b7bb (hdfs-ha fix confirmed, 3 green runs); egret GizmoSQL wave 42632367579 aggregated (SF10 S3 speed 1.51x slower than gizmo; gizmo fails scan/small gates), report file pending; no red CI; no orchestrator status 15:56Z-17:56Z (no runs).
- 2026-11-09 14:59Z: ITEM 39(a) DONE (Dev CI 42630578494); 39(c) part 1 merged to main (42635429611 green), part 2 b08fdb19 in CI on fix/39c-point; lane 3 D15 DONE 7c54dfde, found intermittent hdfs-ha NameNode failover bug (42632461815 red), core fix 2429782f with Dev CI 42638389703 + Service cluster 42638482163 running; egret GizmoSQL wave 42632367579 running; no red CI on main, no stale locks.
- 2026-11-09 13:59Z: ITEM 39(b) DONE 39f84617 and 39(d) DONE a952f415 on main; core 39(a) part 2 53647b1a pushed, main Dev CI 42628870401 running (42624035293 green); catalogs 39(c) part 1 green on fix/39c-point (42627268633, 42627272084), not merged; egret compare run 42629228261 (GizmoSQL fourth server) running; no red CI on main, no stale locks.
- 2026-11-09 12:58Z: ITEM 39 (real-cluster bugs, maintainer 2026-11-09) split across all four lanes; all four locks held (12:44-12:54Z); 39(a) ffe0b1d2 and 39(b) 39f84617 pushed to main by the core lane, main Dev CI 42620776092 running; lane3 Dev CI 42621419498 queued; catalogs hdfs-point-perf 42621322306 running on fix/39c-point; no red CI, no stale locks.
- 2026-11-09 11:56Z: no change: no core session since 10:41Z (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases since 2026-11-07; marten (11:41Z)/heron (session 183)/egret (11:26Z README, idle) idle; no stale locks.
- 2026-11-09 10:56Z: no change: core session 10:41Z lock/unlock only (item 38 waits for the stand on `server-1.0.0-b69660b9260a`); no new CI runs or pre-releases since 2026-11-07; marten (10:40Z)/heron (session 182)/egret (09:26Z README, idle) idle; no stale locks.
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
