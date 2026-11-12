# Orchestration status

Status as of **2026-11-12T04:57Z UTC** (hourly orchestrator; it only watches, it does not change code).

**ALL DONE suspended:** ITEM 26 / 27 DONE (kestrel core) and IMPROVEMENTS DONE (marten, heron) still hold, but the newest
pre-release **`server-1.0.0-2c169a53db3b`** (2026-11-12, Q-40b-1 + items 47 / 48 + Q3) has no egret FRESH result yet;
egret is still on the previous one (FRESH 74464701e372, run 43148390757, running since 03:28Z).

Points for the maintainer:
- **New pre-release `server-1.0.0-2c169a53db3b`** (7 assets) with your 2026-11-11 21:10 answers: Q-40b-1 (file pruning
  off for a local, unshared `dataDir`), item 47 (attached databases hidden from DuckDB metadata functions), item 48 + Q3
  (dc local one-row aggregates over <= 4 pruned files; default `arrowAllocatorLimitBytes` = 75 % of direct memory).
  Package Dev CI 43150534816 + Service cluster 43152195437 / hdfs 43152198658 / security 43152201884 / stores
  43152204958 all green. **Stand check is yours** (lane 3 notes: small-query mix at 32 clients under dc; start line
  `arrowAllocator limitMiB=... source=directMemory`). No open core item until your next note.
- egret ACCEPT on `server-1.0.0-74464701e372` (item 46): **SHIP** (run 43148393964, all 6 checks in 4 cells).
- **Q-46** (ask the hadoopfs extension repo for the negotiated QOP / cipher) still open.
- Branches `pkg/2c169a53`, `pkg/74464701`, `pkg/29f1267b`, `pkg/2ff22144`, `pkg/37bf9401` can be deleted (sessions could not).
- Open questions still listed: Q-40a follow-up (plugins), Q-46; marten TPC-H q16 SF1 slowdown, `_iceberg` schema
  listing; heron speed-profile SF10 peak memory, endpoint splits; `ci-assets` extension set refresh (sha256 in STATUS.md).

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | nothing open (MAIN PACKAGED 04:28Z -> `server-1.0.0-2c169a53db3b`) | 04:41Z (unlock) | free | main Dev CI 43149903188 (00da2257) green; pkg 43150534816 green |
| plugins (`PLUGINS-STATUS.md`) | nothing open (ITEM 46 DONE c96a1272; Q-46) | 03:11Z | free | - |
| catalogs (`CATALOGS-STATUS.md`) | nothing open (ITEM 47 DONE 2c169a53) | 03:41Z | free | Dev CI 43147991919, Catalogs CI 43147994336 green |
| lane 3 (`LANE3-STATUS.md`) | nothing open (ITEM 48 DONE 97ad4f22, Q3) | 03:21Z | free | Service cluster 43152195437 (pkg/2c169a53) green |

No bug-first notes in any lane. No stale locks (all kestrel locks and marten/heron `docs/LOCK` empty). Newest Dev CI
on main (43149903188, 00da2257) green; the older red 43147654545 was the service gate only (main moved), superseded.

## Service matrix (newest run per workflow)

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 43152195437 | pkg/2c169a53 | pass |
| service-hdfs.yml | 43152198658 | pkg/2c169a53 | pass |
| service-security.yml | 43152201884 | pkg/2c169a53 | pass |
| service-stores.yml | 43152204958 | pkg/2c169a53 | pass |

## Pre-releases (newest 3)

- `server-1.0.0-2c169a53db3b` (2026-11-12): Q-40b-1, items 47 / 48, Q3; egret not started yet.
- `server-1.0.0-74464701e372` (2026-11-11): plugins item 46; egret ACCEPT SHIP (43148393964), FRESH 43148390757 running.
- `server-1.0.0-29f1267b995f` (2026-11-11): morning build (hadoopfs afc8, items 44/45); egret ACCEPT SHIP, FRESH 42965886044.

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: **IMPROVEMENTS DONE** (2026-11-03); session 04:41Z "finish mode, nothing new".
- heron: **IMPROVEMENTS DONE**; session 224 (04:53Z): nothing open.

## egret

- Newest FRESH result: **29f1267b995f** (run 42965886044): balanced TPC-H OOM fixed; local small c1 p50 speed/balanced
  1.19 JDBC / 1.16 ADBC (Q-40b-1 in 2c169a53db3b addresses it).
- README 'In progress': ACCEPT 74464701e372 DONE (SHIP); **FRESH 74464701e372 RUNNING** - compare 43148390757
  (upstream native head `cfaa929746cb`). Nothing yet on 2c169a53db3b.

## History

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
