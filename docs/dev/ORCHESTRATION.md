# Orchestration status

Status as of **2026-11-10 03:57Z UTC** (hourly orchestrator; it only watches, it does not change code).

Not ALL DONE: **ITEM 39** is in packaging. The maintainer's last step (hadoopfs afc4 default, new setting
`hdfsMetadataCacheTtlMs`, Q11 = external file cache on HDFS by default) is on `main` as 877cf100 (Dev CI 42700518633,
Service cluster 42700518286, Service hdfs 42700518331, Catalogs CI 42700518131, HDFS point perf 42700537172 all green).
The core lane (lock 03:41Z) runs the package build: Dev CI 42751144566 on `ci/core-39pkg` (71959a39), running; then
"ITEM 39 DONE". **ITEM 40** (close the gap to GizmoSQL) is open: 40(a) S3 in the plugins lane (active), 40(b)
small-query overhead in the core lane after packaging. ITEM 38 still waits for the stand. The routine's original
criterion (items 26/27, marten/heron IMPROVEMENTS DONE) is long met.

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | package item 39 + "ITEM 39 DONE"; then 40(b); item 38 waits for the stand | 2026-11-10 03:41Z (lock); last code 877cf100 2026-11-09 21:13Z | held 03:41Z (active) | main Dev CI 42700518633 (877cf100) green; package Dev CI 42751144566 running |
| plugins (`PLUGINS-STATUS.md`) | ITEM 40(a) S3 SF10 vs gizmo, branch `fix/40a-s3` | 2026-11-10 03:46Z (branch, 468ec1e9) | held 03:10Z (active) | tpch-perf 42751636105 running; 42748064489 red (branch harness run, superseded); 42700198697 green |
| catalogs (`CATALOGS-STATUS.md`) | no open item (39(c) DONE, follow-ups (a)/(b) done by core) | 2026-11-10 03:21Z (unlock) | free | - |
| lane 3 (`LANE3-STATUS.md`) | 39(d) DONE; D13 (b) DONE; next D14 (stand results) | 2026-11-09 20:27Z | free | Service cluster 42691698496 green (branch) |

No red Dev CI on main; nothing to rerun. No new bug-first notes in the paused-lane files.

## Service matrix (newest run per workflow)

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 42700518286 | 877cf100 (main) | pass |
| service-hdfs.yml | 42700518331 | 877cf100 (main) | pass |
| service-security.yml | 42403047248 | b69660b9 (pkg) | pass |
| service-stores.yml | 42403049750 | b69660b9 (pkg) | pass |

## Pre-releases (newest 3)

- `server-1.0.0-b69660b9260a` (2026-11-07 18:47Z): item 38; waits for the stand; egret ACCEPT SHIP (42406253240).
- `server-1.0.0-06c5adc7d7c9` (2026-11-07 05:57Z): item 37; ITEM 37 DONE on the stand.
- `server-1.0.0-6384bc55b173` (2026-11-06 18:04Z) / `server-1.0.0-deafc861cce3` (2026-11-06 11:04Z, egret FRESH DONE).

Item 39 pre-release: package run 42751144566 in progress.

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: **IMPROVEMENTS DONE** (2026-11-03); SERVER deafc861cce3 CHECKED; session 03:40Z "finish mode, nothing new".
  Open question: TPC-H q16 SF1 slowdown (series effect since `fl1eb792`) - fix or leave.
- heron: **IMPROVEMENTS DONE**; SERVER deafc861cce3 CHECKED; session 190 (03:51Z): nothing open.

## egret

- Newest FRESH for a pre-release: **FRESH deafc861cce3 DONE** (run 42223833724): speed vs balanced scans x4/x8
  1.30-1.58, load +42-45 % at 128 clients, 22/22, 0 errors; upstream native `9e4787906b07` still fails TPC-H/small.
- **EGRET GIZMO DONE** (`results/FRESH-gizmo-deafc861cce3.md`): S3 TPC-H SF10 speed ~1.5x slower than GizmoSQL;
  local small c1 p50 regression speed vs balanced 1.16-1.18 (basis of ITEM 40).
- README 'In progress': idle, newest tracked `b69660b9260a` (03:26Z). Next: FRESH/ACCEPT on the item 39 pre-release.

## For the maintainer

- Nothing blocking. ITEM 39 package run in progress (core lane); then the stand + the independent cluster check.
- Stand check of `server-1.0.0-b69660b9260a` for ITEM 38, then "ITEM 38 DONE".
- **Open questions:** item 35 default `distributedCompletePeerStreams`; item 33 default `hdfsLocalCopyRoomWaitMillis`
  (0 vs 30000); catalogs Q10 (footer-stats pruning on by default); marten q16 SF1 pushdown slowdown.

## History

- 2026-11-10 03:57Z: item 39 last step 877cf100 on main (hadoopfs afc4, hdfsMetadataCacheTtlMs, external file cache on HDFS; Dev CI 42700518633 + 4 service/perf runs green); core lane (lock 03:41Z) packaging: Dev CI 42751144566 on ci/core-39pkg running; maintainer opened ITEM 40 (gizmo gap): plugins lane on 40(a) fix/40a-s3 (tpch-perf 42751636105 running, 42748064489 red on the branch); no red CI on main, no stale locks.
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
