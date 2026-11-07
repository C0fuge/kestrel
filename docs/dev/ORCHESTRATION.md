# Orchestration status

Status as of **2026-11-07 21:56Z UTC** (hourly orchestrator; it only watches, it does not change code).

Not ALL DONE: **ITEM 38** (minor logs/hints) is done in code/CI and packaged as pre-release `server-1.0.0-b69660b9260a`
(19:05Z); it waits for the maintainer's stand check and "ITEM 38 DONE". The routine's original criterion (items 26/27,
marten/heron IMPROVEMENTS DONE) is long met; no egret FRESH on the newest pre-release (egret runs ACCEPT, not FRESH, on
log-only builds; **ACCEPT b69660b9260a SHIP**).

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | ITEM 38 done in CI, waits for the stand | 05584fe7, 21:41Z lock/unlock only (~15 min) | free (released 21:41Z) | main Dev CI 42403022881 (b69660b9) green; package Dev CI 42403052289 (pkg/b69660b9) green |
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
- `server-1.0.0-6384bc55b173` (2026-11-06 18:36Z): items 35, 36; egret ACCEPT SHIP.

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: **IMPROVEMENTS DONE** (2026-11-03); SERVER deafc861cce3 CHECKED; session 21:40Z "finish mode, nothing new".
  Open question (no decision needed to continue): TPC-H q16 SF1 ~0.5 s slowdown from join-key IN-list pushdown - fix or leave.
- heron: **IMPROVEMENTS DONE**; SERVER deafc861cce3 CHECKED; session 155 (21:51Z): nothing open.
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
- 2026-11-07 07:58Z: item 37 pre-release server-1.0.0-06c5adc7d7c9 published 07:02Z (pkg/06c5adc7: Dev CI 42348141046 + all 4 service runs green); core unlocked 07:42Z waiting for the stand; egret ACCEPT 06c5adc7 running (42351380956); marten (07:40Z)/heron (session 141) idle; no stale locks.
- 2026-11-07 06:57Z: core session (lock 06:41Z) answered the gate note: package-release.sh on a fixed pkg/<sha> branch (aaefbd08), item 37 package dispatched on pkg/06c5adc7 (Dev CI 42348141046 running; cluster/stores green, hdfs/security running); no new pre-release; marten (06:40Z)/heron (session 140)/egret idle; no stale locks.
- 2026-11-07 05:55Z: core did item 37 (a)-(d) in code/CI (33347f10, 35d9420a; Dev CI 42342174052 + Service hdfs 42342173100 green, all 10 cells) and unlocked 05:41Z waiting for the stand; but its `package=true` Dev CI 42343340513 on 4264bed7 failed at the service gate (no service runs on 4264bed7, main moved) -> no pre-release; orchestrator bug-first note at the top of core Next steps; marten (05:41Z)/heron (session 139)/egret idle; no stale locks.
- 2026-11-07 04:57Z: core session (lock 03:41Z, active) pushed 761bad23 (item 37 (b) Flight call executor), 88ccac09/602c1927 (Service hdfs item 37 stage), d9e6449b (item 37 (a) storage-refused table); Dev CI green on 602c1927, 42339761044 running; Service hdfs red x3 on the lane's own new stage (ha-krb/ha-krb-sasl balanced), being fixed, 42339761143 running; no new pre-release; marten/heron (session 138)/egret idle; no stale locks.
- 2026-11-07 03:58Z: maintainer accepted items 35 and 36 (1)(2)(5) on the stand and opened ITEM 37 (b9277a77: Ranger denial code + view retry, node stop order under load, refusal SQL once, keytab-without-ticket-cache WARN); core session (lock 03:41Z) pushed 04637d9e, e04ba1ef, 691cd394 (all four parts in code), Dev CI 42336009247 running (2 earlier runs cancelled by newer pushes); no new pre-release; marten/heron/egret idle; no stale locks.
- 2026-11-06 21:56Z: no change: core session 21:41Z lock/unlock only (waits for the stand on `server-1.0.0-6384bc55b173`); Dev CI/service gate green on 6384bc55; egret idle (21:26Z README, newest tracked 6384bc55b173); marten (17:41Z)/heron (session 137) idle; no stale locks.
- 2026-11-06 20:57Z: no kestrel lane commits since 18:41Z (core waits for the stand on `server-1.0.0-6384bc55b173`); Dev CI/service gate green on 6384bc55; egret ACCEPT 6384bc55b173 SHIP (42278048164); marten/heron idle; no stale locks.
- 2026-11-06 19:56Z: no kestrel commits since 18:58Z (core idle, waits for the stand on `server-1.0.0-6384bc55b173`); Dev CI/service gate green on 6384bc55; egret ACCEPT 6384bc55b173 running (42278048164); marten (17:41Z)/heron (session 137) idle; no stale locks.
- 2026-11-06 18:58Z: new pre-release `server-1.0.0-6384bc55b173` (18:36Z; ITEM 36 packaged incl. (4) rest; package Dev CI 42267369022 green, service gate 4/4 green on 6384bc55); core unlocked 18:41Z, waits for the stand; egret ACCEPT 19efa29b1b57 SHIP (42261777686); marten/heron idle; no stale locks.
- 2026-11-06 17:58Z: new pre-release `server-1.0.0-5a8e831eade2` (17:02Z; items 35, 36 (1) part 1 + CI cell, (2) keytab-only start, (4) one-line logs; Dev CI 42257953849 green, service gate 4/4 green on 5a8e831e); core session (lock 17:41Z) pushed e7b4bfa6 (rest of 36 (4)), Dev CI 42264944379 running; egret ACCEPT 19efa29b1b57 running (42261777686); marten/heron idle; no stale locks.
- 2026-11-06 16:56Z: pre-release `server-1.0.0-19efa29b1b57` (items 35, 36 (3)/(5), 36 (1) part 1; package Dev CI 42249857195 green, service gate 4/4 green); core (lock 16:41Z) pushed 36 (1) CI cell, 36 (2), 36 (4) part, Dev CI 42255936543 running; marten SERVER deafc861cce3 CHECKED; egret idle.
- 2026-11-06 15:58Z: core did ITEM 35 + ITEM 36 (3), (5) in code/CI (Dev CI 42241541621 green on 4ca7be01); both package runs (42238168755 fd67ac4a, 42244135498 cd3c0ea6) built green but failed the service gate (main moved) -> no pre-release; core session (lock 15:40Z) on ITEM 36 (1) (ece08961, Dev CI 42247203245 running); egret FRESH deafc861cce3 DONE (speed 1.10-1.15 of upstream 9e4787906b07 on S3 scans, JDBC c1 p50 1.10 flag); marten deafc861cce3 regression lakefs + check 13 PASS, A/B 3,4 running; heron session 137 idle; no stale locks.
- 2026-11-06 14:58Z: maintainer accepted ITEM 34 on the stand and opened ITEM 36 (0b076bf6: DC small-disk room leak, keytab-only error, Ranger code, log noise, NN timeouts; after item 35); item 35 `package=true` Dev CI 42233371052 cancelled in the new item 35 step, core session (lock 14:41Z) changed the step (fd67ac4a, DuckDB wheel) and re-dispatched Dev CI 42238168755 (running); egret ACCEPT deafc861cce3 SHIP, FRESH 42223833724 still running; marten M4 on deafc861cce3 part 1 (S3 1-9, iceberg pass), pair 2 running; heron session 136 idle; no stale locks.
- 2026-11-06 13:56Z: maintainer opened ITEM 35 (concurrent distributedComplete scans fail on the Arrow memory limit, 5c93983c) after the ITEM 34 stand check; core session (lock 13:28Z) pushed 2e128c2e (merge admission, RESOURCE_EXHAUSTED), Dev CI 42228594645 + Service hdfs/cluster + Catalogs CI running; egret FRESH deafc861cce3 (42223833724) + ACCEPT (42223763930) running; marten dispatched M4 on deafc861cce3; heron pinned deafc861cce3 for e2e; no stale locks.
- 2026-11-06 12:57Z: no change since 11:56Z: core session 12:40Z lock/unlock only (waits for the stand check of ITEM 34 on deafc861cce3); Dev CI/service gate green on deafc861; egret idle (11:28Z), no FRESH for deafc861cce3; marten (12:42Z)/heron (session 133) idle; no stale locks.
- 2026-11-06 11:56: ITEM 34 done in code/CI, pre-release server-1.0.0-deafc861cce3 (Dev CI 42207423706), all 4 service workflows green on deafc861; waits for the stand; egret ACCEPT 35c15ab226b9 SHIP with room wait; marten/heron idle; no stale locks.
- 2026-11-06 10:58Z: maintainer accepted ITEM 33 on the stand (d94e4068) and opened ITEM 34 (log hygiene); core session (lock 10:41Z) pushed 85293332, Dev CI 42205205027 running, Service hdfs/Catalogs green; egret ACCEPT 35c15ab226b9 (42196478105) SHIP, all cells; marten (10:41Z)/heron (session 131) idle; no stale locks.
- 2026-11-06 09:59Z: egret accept-hdfs 42196478105 on 35c15ab226b9 dispatched by the maintainer 09:37Z, running (presumably the room-wait re-check for ITEM 33); no kestrel lane commits; Dev CI/service gate green on 35c15ab2, no new pre-release; marten (09:40Z)/heron (session 130) idle; no stale locks.
- 2026-11-06 08:56Z: no change since 07:57Z: no kestrel lane commits; Dev CI/service gate green on 35c15ab2, no new pre-release; egret idle, room-wait ACCEPT for 35c15ab226b9 still not dispatched; marten (08:41Z)/heron (session 129) idle; no stale locks.
- 2026-11-06 07:57Z: no change since 06:56Z: core session 07:41Z lock/unlock only (waits for the egret room-wait re-check of item 33); Dev CI/service gate green on 35c15ab2, no new pre-release; egret idle (07:27Z), no new run; marten (07:40Z)/heron (session 129) idle; no stale locks.
- 2026-11-06 06:56Z: no change since 05:56Z: no kestrel lane commits since 21:41Z, Dev CI/service gate green on 35c15ab2, no new pre-release; egret idle (03:26Z) waiting for the maintainer's room-wait ACCEPT dispatch for 35c15ab226b9; marten (06:41Z)/heron (session 128) idle; no stale locks.
- 2026-11-06 05:56Z: no change since 04:56Z: no kestrel lane commits since 21:41Z, Dev CI/service gate green on 35c15ab2, no new pre-release; egret idle (03:26Z) waiting for the maintainer's room-wait ACCEPT dispatch for 35c15ab226b9; marten (05:41Z)/heron (session 127) idle; no stale locks.
- 2026-11-06 04:56Z: no change since 03:56Z: no kestrel lane commits since 21:41Z, Dev CI/service gate green on 35c15ab2, no new pre-release; egret still idle waiting for the maintainer's room-wait ACCEPT dispatch for 35c15ab226b9; marten (04:40Z)/heron (session 126) idle; no stale locks.
- 2026-11-06 03:56Z: egret ACCEPT 35c15ab226b9 PARTIAL (42113544966, 22:33Z): checks 1/2/3/5/6 pass all cells, check 4 now passes in both default cells; speed cells still fail check 4 because the run had no room wait -> egret waits for the maintainer to confirm/dispatch the `smalldisk-opts=-DhdfsLocalCopyRoomWaitMillis=30000` run; no kestrel commits since 21:57Z, CI/service gate green on 35c15ab2; marten/heron (session 125) idle; no stale locks.
- 2026-11-05 21:57Z: core: package Dev CI 42108450187 on 35c15ab2 green, service gate 4/4 green -> pre-release `server-1.0.0-35c15ab226b9` (item 33 complete in code/CI); core unlocked 21:41Z, waits for stand/egret; egret ACCEPT 35c15ab226b9 running (42113544966); marten/heron (session 124) idle; no stale locks.
- 2026-11-05 20:57Z: core: room wait CI green (70ffac12: Dev CI 42096491904, Service hdfs 42096491844 all cells incl. speed); `package=true` Dev CI 42099508904 on 5092f2c5 red in flaky EndpointFailoverIntegrationTest.ticketPlannedForAnotherNodeGivesTheSameAnswerAndMovesItsLoad (not item-33 code); core session (lock 20:41Z) fixed it in bbc8e7c1, Dev CI 42106064754 running - no bug-first note; no new pre-release; marten/heron (session 124)/egret idle; no stale locks.
