ALL DONE 2026-11-10T19:58Z, refreshed 2026-11-11T03:58Z (all conditions met; the balanced TPC-H OOM found by egret FRESH 2ff221447b5d is fixed on main by plugins ITEM 44, DONE e75705c9; the core lane is packaging the next pre-release now)

# Orchestration status

Status as of **2026-11-11T03:58Z UTC** (hourly orchestrator; it only watches, it does not change code).

**Core session running the maintainer's morning note** (lock 03:46:09Z): (1) hadoopfs `51b7220-afc8` as CI / release
default - **done** 29f1267b; (2) fix/44 merge - done (2335ddc6, and part (2) 8ea12800 + e75705c9); (3) Dev CI
`package=true` on `main` - **in progress** (Dev CI 42955017367, workflow_dispatch on 29f1267b). No new pre-release yet.

Points for the maintainer:
- **ITEM 44 DONE e75705c9** (plugins lane, 03:38Z): part (2) bounds Ranger / HDFS revoke staleness with an access
  recheck (8ea12800) and names a query's tables with the token finder instead of DuckDB's parse lock (e75705c9). The
  maintainer's bug-first note is still at the top of PLUGINS-STATUS Next steps but the item is marked DONE there.
- Branches `pkg/2ff22144`, `pkg/37bf9401` can be deleted (sessions could not delete them).
- Open questions still listed: Q-40b-1 (catalogs, close without a default change - needs OK), Q8 / Q10 / Q11
  (catalogs); lane 3 Q3 / Q4; marten TPC-H q16 SF1 slowdown; `ci-assets` extension set refresh (sha256 in STATUS.md).

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | maintainer morning note: afc8 default done 29f1267b, package=true running | 03:47Z (29f1267b) | held 03:46:09Z (fresh) | main Dev CI 42955017367 in progress; two earlier Dev CI runs (42955011412, 42954112590) cancelled = superseded, not red |
| plugins (`PLUGINS-STATUS.md`) | ITEM 44 DONE e75705c9; nothing open | 03:38Z (unlock) | free | covered by main Dev CI above |
| catalogs (`CATALOGS-STATUS.md`) | none open (40(b) DONE 135e6dac) | 2026-11-10 17:21Z | free | Catalogs CI 42955011409 (29f1267b) green |
| lane 3 (`LANE3-STATUS.md`) | ITEM 45 DONE b8613fc4; nothing open | 2026-11-10 21:19Z | free | Service cluster 42955011424 (main 29f1267b) green |

No red Dev CI on main (last completed green: 42906724548). No new bug-first notes. No stale locks (core lock is 12 min
old; other kestrel locks and marten/heron `docs/LOCK` empty).

## Service matrix (newest run per workflow)

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 42955011424 | main 29f1267b | pass |
| service-hdfs.yml | 42955011583 | main 29f1267b | running |
| service-security.yml | 42860284292 | pkg/2ff22144 | pass |
| service-stores.yml | 42860290386 | pkg/2ff22144 | pass |

## Pre-releases (newest 3)

- `server-1.0.0-2ff221447b5d` (2026-11-10 17:17Z): build for the real cluster; egret ACCEPT SHIP (42865242528, HDFS rerun 42885490049 SHIP); FRESH 42865250390: balanced TPC-H OOM (fixed on main by item 44, next package running).
- `server-1.0.0-37bf9401d77c` (2026-11-10 08:17Z): ITEM 41; egret ACCEPT DO NOT SHIP (check 6 only), FRESH 42791002963.
- `server-1.0.0-894afb9b48c4` (2026-11-10 06:34Z): ITEM 39; egret ACCEPT SHIP (42778646542), FRESH 42775953599.

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: **IMPROVEMENTS DONE** (2026-11-03); session 03:43-03:50Z "finish mode, nothing new".
- heron: **IMPROVEMENTS DONE**; session 209 (03:51Z): nothing open.

## egret

- Newest FRESH: **2ff221447b5d** (run 42865250390; upstream native `0ecca4f9b01e`). Speed vs balanced: S3 small p95 0.04-0.06, load +36-44 % at 128 clients (0.94-0.97 of upstream), scans x4/x8 1.31-1.45, slow-link ZSTD 1.02; balanced TPC-H OOM (fixed by item 44); local ADBC c1 speed/balanced 1.11.
- README 'In progress': idle (03:26Z), newest tracked 2ff221447b5d. A FRESH run on the coming pre-release is expected next.

## History

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
- 2026-11-10T12:58Z: catalogs lane bisected the local c1 regression to footer-stats file pruning (b0f82a47) and asked **Q-40b-1** (default change: prune only for remote stores); 40(b) cut 5 on fix/40b-small-5 (Dev CI 42818682745 + Small query perf 42818686506 running); main Dev CI 42812785328 (33d4d2dc) green, Service cluster 42812785652 green; lock catalogs 12:24Z active; no stale locks, no red CI.
- 2026-11-10T11:58Z: ALL DONE conditions met (egret FRESH 37bf9401d77c done, run 42791002963); egret ACCEPT 37bf9401d77c DO NOT SHIP on check 6 only (maintainer decision); catalogs 40(b) cuts a4e14e03 + ac2055c8 on main, Dev CI 42811292025 running; lock catalogs 11:20Z active; no stale locks, no red CI.
- 2026-11-10T10:58Z: Service cluster red resolved (D17 DONE: lane 3 point gate fix ed81bfc8/f567baa4, runs 42796003792 main + 42796297705 branch green); item 41 follow-up (2) DONE (hdfs=reads lines on direct SASL, Dev CI 42794847349 + Service hdfs 42794862424 green); 40(b) 3rd improvement 388b81f5 on main, Dev CI 42802352922 running; egret ACCEPT 37bf9401d77c PARTIAL (check 6: no hdfs=reads lines, added after that build), rerun + FRESH running; lock catalogs 10:21Z active; no stale locks.
- 2026-11-10T09:58Z: ITEM 41 DONE (core) + Q-42 DONE 15220a77 (plugins) + 40(b) 2nd improvement merged; Service cluster on main 42789267919 red on item 43's point q/s gate (0.62/0.78 < 0.8, cells pass); main Dev CI green 42789413482, newer queued; egret FRESH 894afb9b48c4 done (small c1 regression), ACCEPT+FRESH on 37bf9401d77c running; no stale locks.
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
