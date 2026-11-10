# Orchestration status

Status as of **2026-11-10T08:57Z UTC** (hourly orchestrator; it only watches, it does not change code).

Not ALL DONE. **ITEM 42 DONE db35b57a** (plugins lane: speed caps DuckDB at the budget; point-query pruning on SASL
DataNodes). Item 41's package runs (`pkg/37bf9401`: Dev CI 42777511119, Service cluster/hdfs/security/stores) are all
green and pre-release `server-1.0.0-37bf9401d77c` is published, but "ITEM 41 DONE" is not written yet: the core lane
(lock 08:41Z, active) is on the maintainer's 11:25 answers (`hdfs=reads` on the direct path, `authentication,privacy`
CI cell c48c78d9) and the too-old-hadoopfs fix 004d844e. Main Dev CI 42785951498 (db35b57a) + Service hdfs
42785951630 running. Catalogs lane (lock 08:21Z) on ITEM 40(b) (`fix/40b-small` Dev CI 42784271237 green); lane 3
(lock 08:08Z) on ITEM 43, its branch Dev CI is red (own work in progress, see below).

For the maintainer:
- New open question **Q-42** in `PLUGINS-STATUS.md` (other memory limits above the budget; point queries without copies).
- The cluster build (your 11:30 note) waits for "ITEM 40(b) DONE" (catalogs lane, in progress).
- egret ACCEPT re-run 42778646542 (after harness fix 6d7671d) and FRESH 42775953599 on `894afb9b48c4` still running.

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | ITEM 41: answers to the 11:25 note, then "ITEM 41 DONE"; then 40(b) package | 2026-11-10 08:48Z (c48c78d9) | held 08:41Z (active) | main Dev CI 42785951498 (db35b57a) running; 42785235869 (004d844e) cancelled/superseded; newest completed green 42777385100 (37bf9401) |
| plugins (`PLUGINS-STATUS.md`) | ITEM 42 DONE db35b57a; Q-42 open | 2026-11-10 08:52Z (unlock) | free | Dev CI 42781954321 (fix/42-sizing-pruning) green |
| catalogs (`CATALOGS-STATUS.md`) | ITEM 40(b) small/point query overhead (`fix/40b-small`) | 2026-11-10 08:38Z (branch) | held 08:21Z (active) | Dev CI 42784271237 (fix/40b-small) green |
| lane 3 (`LANE3-STATUS.md`) | ITEM 43 dc point lookups (`lane/distributed` d0fbb186) | 2026-11-10 08:39Z | held 08:08Z (active) | Dev CI 42784392012 red: `DistributedTwoProcessTlsTest.mergedAnswersOverTlsWithTokenEqualDuckDb:168` (1 of 1267); Service cluster 42784396437 red; previous 9f492e7c green - lane branch WIP, not on main |

No red completed Dev CI on main. No new bug-first notes (the "FIRST" notes in the lane files are the maintainer's item
42/43/40(b) assignments). No stale locks (marten/heron `docs/LOCK` empty; LOCK-PLUGINS empty).

## Service matrix (newest run per workflow)

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 42784396437 | lane/distributed d0fbb186 | fail (lane 3 WIP); pkg 42777497907 pass |
| service-hdfs.yml | 42785951630 | main db35b57a | running; pkg 42777501115 pass |
| service-security.yml | 42777504125 | 37bf9401 (pkg) | pass |
| service-stores.yml | 42777507448 | 37bf9401 (pkg) | pass |

## Pre-releases (newest 3)

- `server-1.0.0-37bf9401d77c` (2026-11-10 07:44Z): item 41 (afc6, direct SASL reads); all package runs green; "ITEM 41 DONE" pending.
- `server-1.0.0-894afb9b48c4` (2026-11-10 05:48Z): ITEM 39 DONE; egret ACCEPT re-run 42778646542 + FRESH 42775953599 running.
- `server-1.0.0-b69660b9260a` (2026-11-07 18:47Z): item 38; egret ACCEPT SHIP (42406253240).

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: **IMPROVEMENTS DONE** (2026-11-03); session 08:40Z "finish mode, nothing new". Open question: TPC-H q16 SF1
  slowdown - fix or leave.
- heron: **IMPROVEMENTS DONE**; session 195 (08:51Z): nothing open.

## egret

- Newest FRESH for a pre-release: **FRESH deafc861cce3 DONE** (run 42223833724); none yet for `894afb9b48c4` /
  `37bf9401d77c`.
- README 'In progress': on `894afb9b48c4`: ACCEPT re-run 42778646542 (harness fix 6d7671d) running; FRESH 42775953599
  (upstream `cec626bbb54a`) running.

## History

- 2026-11-10T08:57Z: ITEM 42 DONE db35b57a (plugins, Q-42 open); item 41 package runs all green, pre-release server-1.0.0-37bf9401d77c published, ITEM 41 DONE not yet written (core lane active on the 11:25 answers, 004d844e too-old-hadoopfs fix, c48c78d9 authentication,privacy cell); main Dev CI 42785951498 running; catalogs 40(b) fix/40b-small green; lane 3 branch red (DistributedTwoProcessTlsTest, WIP); egret ACCEPT rerun + FRESH on 894afb9b48c4 running; no stale locks.
- 2026-11-10 07:58Z: ITEM 41 steps (1)-(3) on main (afc6 default, hdfsSaslDataTransfer, sasl-direct cell; Dev CI 42774303168 green, Service hdfs 42775949664 12/12 green), package=true on pkg/37bf9401 running (Dev CI 42777511119 + 4 service runs); plugins lane 40(a) Q-40a (C) on main d295fe36; catalogs lane (lock 07:27Z) on ITEM 40(b); maintainer added too-old-hadoopfs note; egret ACCEPT 894afb9b48c4 DO NOT SHIP (harness loaded afc3, every server failed setup; harness fixed 6d7671d), FRESH 42775953599 running; no red Dev CI on main, no stale locks.
- 2026-11-10 06:58Z: ITEM 39 DONE (pre-release server-1.0.0-894afb9b48c4); core lane (lock 06:41Z) on ITEM 41: 39edad76 on main (afc6 default, hdfsSaslDataTransfer), Dev CI 42770827832 + Service hdfs/cluster running; Catalogs CI 42770828168 polaris docker-pull reset (infra) -> failed jobs rerun; plugins lane logged Q-40a as unanswered though (C) is on main (lock taken before the answer); egret not yet on 894afb9b48c4; no stale locks.
- 2026-11-10 05:56Z: core lane (lock 05:41Z, active): package Dev CI 42757195073 red at the service gate (apt errors in `docker build` of the server images); CI-only mirror fixes 90b3def5 + 894afb9b on main (Dev CI 42763742753 on 90b3def5 red in the hadoopfs-libraries apt step, superseded), main Dev CI 42764375712 + package rerun 42764387193 on ci/core-39pkg2 running; maintainer answered **Q-40a (C)** and opened **ITEM 41** (hadoopfs afc6, SASL DataNodes read directly); plugins lane (lock 05:11Z) tpch-perf 42760596461 running; no stale locks.
- 2026-11-10 04:57Z: core lane (lock 03:41Z, active): package Dev CI 42751144566 on ci/core-39pkg red only at the service gate (Service hdfs 42752652430/42755619561 red: KDC setup stalled on the Ubuntu mirror); CI fix 98574893 on main (Dev CI 42757164357 + Catalogs CI green, Service hdfs 42757164355 running), package rerun Dev CI 42757195073 running; plugins lane: 40(a) cause found (speed's DuckDB memory limit bounds the external file cache; speed 15.9 s vs plain DuckDB 16.9 s with 1g JVM), docs step done, **Q-40a open**; lane 3 Q3 open; no red Dev CI on main, no stale locks.
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
