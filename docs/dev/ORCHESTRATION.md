# Orchestration status

Status as of **2026-11-07 04:57Z UTC** (hourly orchestrator; it only watches, it does not change code).

Not ALL DONE: the core lane is on **ITEM 37** (rest of item 36 after the 2026-11-07 02:20 stand re-check). The core
session (lock 03:41Z, last commit 04:44Z, active) pushed (b) own Flight call executor + drain report (761bad23),
(a) failed views retried by queries that name them (691cd394) and an undescribed storage-refused table as
PERMISSION_DENIED (d9e6449b), plus a new item 37 stage in the Kerberos one-server cells of Service hdfs (88ccac09,
602c1927). Dev CI green on 88ccac09, 761bad23, 602c1927; Dev CI 42339761044 on d9e6449b running. Service hdfs is red
on the lane's own new stage (ha-krb / ha-krb-sasl balanced cells, 3 runs: 42336170913, 42337610848, 42338931140; the
stop-under-load part passes, the cell then fails) and the session is iterating on it (run 42339761143 on d9e6449b
running) - no bug-first note, the lane is working on exactly this. No new pre-release yet. No bug-first notes in any
lane.

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | ITEM 37 (a)-(d) in code; Service hdfs item 37 stage being fixed; `package=true` + stand re-check pending | d9e6449b, 2026-11-07 04:44Z | 03:41Z (active, not stale) | Dev CI green 42338931197 (602c1927); 42339761044 (d9e6449b) running; Service hdfs red x3 on the new stage, 42339761143 running; Catalogs CI green |
| plugins (`PLUGINS-STATUS.md`) | paused; no new bug-first note | 2026-11-06 | free | - |
| catalogs (`CATALOGS-STATUS.md`) | paused; no new bug-first note | 2026-11-06 | free | Catalogs CI 42339761145 green |
| lane 3 (`LANE3-STATUS.md`) | paused; no new bug-first note | branch 2026-10-29 | free | - |

No stale locks (marten and heron locks free).

## Service matrix (newest run per workflow)

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 42269501072 | 6384bc55 | pass |
| service-hdfs.yml | 42339761143 | d9e6449b | running (previous 42338931140 on 602c1927: fail, ha-krb / ha-krb-sasl balanced, item 37 stage) |
| service-security.yml | 42269591550 | 6384bc55 | pass |
| service-stores.yml | 42269597085 | 6384bc55 | pass |

## Pre-releases (newest 3)

- `server-1.0.0-6384bc55b173` (2026-11-06 18:04Z): items 35, 36 (1)-(5); stand re-checked 2026-11-07: item 35 done, rest -> item 37.
- `server-1.0.0-5a8e831eade2` (2026-11-06 17:02Z): items 35, 36 (1) part 1 + CI cell, (2), (3), (5), (4) one-line part.
- `server-1.0.0-19efa29b1b57` (2026-11-06 16:10Z): items 35, 36 (3), (5), 36 (1) part 1; egret ACCEPT SHIP.

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: **IMPROVEMENTS DONE** (2026-11-03); SERVER deafc861cce3 CHECKED; last session 2026-11-06 17:41Z "finish mode,
  nothing new". Open question (no decision needed to continue): fix TPC-H q16 SF1 ~0.5 s slowdown from the join-key
  IN-list pushdown or leave it.
- heron: **IMPROVEMENTS DONE**; SERVER deafc861cce3 CHECKED (session 135); session 138 (2026-11-07 04:51Z): nothing open.
- Neither has checked `19efa29b1b57` / `5a8e831eade2` / `6384bc55b173`.

## egret

- Newest FRESH: **FRESH deafc861cce3 DONE** (run 42223833724): speed vs balanced scans x4/x8 1.30-1.58, load +42-45 %
  at 128 clients, 22/22, 0 errors; upstream native `9e4787906b07` still fails TPC-H/small.
- **EGRET ACCEPT 6384bc55b173 DONE** (run 42278048164): **SHIP**, all 6 checks in all 4 cells (check 4 with room wait).
- README "In progress": idle 2026-11-07 03:26Z, newest tracked `6384bc55b173`; next ACCEPT on the item 37 pre-release.

## For the maintainer

- Nothing blocks you now: ITEM 37 is in progress in the core lane; after its `package=true` pre-release, re-check on
  the stand: Ranger denial (expect PERMISSION_DENIED + one `query=denied` WARN, node answers right after the policy is
  back), node stop under load (expect `result=stopping`, no Netty/`Filesystem closed` stacks, no allocator leak),
  refusal WARN with the SQL once, keytab-only start without ticket cache (WARN at start). Then "ITEM 37 DONE".
- **Open question (item 35):** default budget `distributedCompletePeerStreams` (one per 128 MiB direct memory).
- **marten open question:** TPC-H q16 SF1 slowdown from join-key pushdown - fix or leave.
- ALL DONE criterion note: this routine's original items 26/27 are long done; the core lane is now on item 37.

## History

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
- 2026-11-05 19:56Z: core (lock 18:41Z, active) on ITEM 33 speed cells: room wait reworked (95d0234f, 1e82115a, 70ffac12); Dev CI 42093460272 on 1e82115a red in LocalCopyCacheTest.ticketsOfOneQueryTakeTheRoomInTurnsWhileCopying (own new test, fixed by 70ffac12, Dev CI 42096491904 running); Service hdfs plain-speed cell red (known open, reported as such); no new pre-release; marten/heron/egret idle; no stale locks.
- 2026-11-05 18:57Z: core batched sp-group (avg) / sp-distinct (2x count DISTINCT) (ed4b26d1; Dev CI 42082455680 + Service hdfs 42082455107 green); package run 42084966934 refused at service gate (main moved); speed cells as opt-in hdfsLocalCopyRoomWaitMillis (f53b9709, CI running); new Open question on its default.
- 2026-11-05 17:56Z: core lane analysed the egret check-4 failure: no pin leak (18219b9c, the refusal counted the query's own reused copies; Dev CI 42073731097 + Service hdfs 42073730677 green); maintainer gave the exact SQL of the 4 unbatched queries (cb29d02f: avg, two count DISTINCT, speed partial per-ticket); core session (lock 17:40Z) pushed 5b84e0a7, Dev CI 42078627145 running; ITEM 33 not done; marten/heron/egret idle; no stale locks.
- 2026-11-05 16:56Z: egret ACCEPT 50fe065af50c DONE: DO NOT SHIP (check 4 fails in all 4 cells: copies of finished queries stay pinned, sp-group/sp-distinct refused, speed partial 1-copy queries regress) -> orchestrator bug-first note at the top of core Next steps; core idle since 15:41Z unlock; Dev CI on main green, service gate 4/4 green on 50fe065a; marten/heron idle; no stale locks.
- 2026-11-05 15:57Z: core session 15:41Z made no code change (ITEM 33 waits for the stand re-check of 50fe065af50c); Dev CI on main green (42041121733, package 42043316941), service gate 4/4 green on 50fe065a; egret ACCEPT 50fe065af50c running (42055012677); marten (15:41Z)/heron (session 123) idle; no stale locks.
- 2026-11-05 14:58Z: core ITEM 33 done in code and CI ((A)-(D) a744ef50/75de58bc, (E) aeb87cdf/8e60638e/9dbde204; Dev CI 42041121733 green); `package=true` Dev CI 42043316941 green, service gate 4/4 green on 50fe065a -> pre-release `server-1.0.0-50fe065af50c`, waits for the stand re-check; core lock released 14:48Z; egret has no ACCEPT/FRESH for 50fe065af50c yet; marten (14:40Z)/heron (session 123) idle; no stale locks.
- 2026-11-05 13:58Z: core session (lock 13:41Z) on ITEM 33 (E): aeb87cdf (partial subquery -> complete answer on SASL HDFS) + 8e60638e (copy refusals log hdfs=reads); Dev CI 42039372619 + Service hdfs 42039028971 running; `package=true` 42031858690 on bfe09a23 cancelled by the new push (build green); service matrix on bfe09a23: cluster/security/hdfs green, stores running; egret ACCEPT 5f60db1ee304 DONE: DO NOT SHIP (check 4 only, item 33 scope); marten/heron (session 122) idle; no stale locks.
- 2026-11-05 12:58Z: maintainer accepted items 31/32 on the stand (f546f828) and opened ITEM 33 (small copy disk: count(*) / GROUP BY 1 batching, copy disk fills, distributedComplete subquery, minor logs); core session (lock 12:14Z) pushed a744ef50 + 75de58bc, Dev CI 42027060883 + Service hdfs 42027060707 green on 75de58bc, dispatch 42029413029 running; egret ACCEPT 5f60db1ee304 first report DO NOT SHIP (check 4 small disk, now in item 33 scope), harness fix a5a0469 + only rerun 42027069331 running; marten/heron idle.
- 2026-11-05 11:56Z: pre-release `server-1.0.0-5f60db1ee304` (ITEM 32) published from Dev CI 42012727794; service matrix on 5f60db1e all green (42014348488/42014352876/42014357596/42014361466); egret ACCEPT 5f60db1ee304 run 42018218548 in progress; core session 11:42Z: no open item, waits for the re-check; marten/heron (session 121) idle; no locks.
- 2026-11-05 10:56Z: maintainer accepted ITEM 31 for the stand (47764f75) and opened ITEM 32 (copy refusal code, relative reserve, hdfs=reads on complete path); core session 10:06-10:41Z did it in code (580dbdfa, 23063397, 04c70af7): Dev CI 42010391188 green (580dbdfa red 42008636974 = PeerReadGateTest race, fixed by 04c70af7), service-hdfs 42010059761 green; `package=true` Dev CI 42012727794 + service matrix running on 5f60db1e; egret will re-ACCEPT on the item 32 build; marten/heron (session 120) idle; no locks held.
- 2026-11-05 09:57Z: egret final ACCEPT 8060179bce0c (41995351608): DO NOT SHIP (check 4 INTERNAL vs RESOURCE_EXHAUSTED + 1 GiB reserve, check 6 missing hdfs=reads); core session 09:41Z answered in 5c83dd13 (check 4a = ADBC client mapping, wire status is RESOURCE_EXHAUSTED; 4b = new Open question; check 6 log fix), Dev CI 42005115898 running; marten/heron (session 119) idle.
- 2026-11-05 08:57Z: egret preliminary SASL HDFS acceptance of 8060179bce0c (41986696064): DO NOT SHIP - check 4 copy-disk refusal INTERNAL not RESOURCE_EXHAUSTED (all cells), check 6 missing hdfs=reads lines; final run 41995351608 running; kestrel no change (CI/service matrix green); marten/heron (session 118) idle; no locks held.
- 2026-11-05 07:57Z: core session 07:42Z found nothing open (ITEM 31 waits for the stand re-check); CI and service matrix green on 8060179b; egret started the maintainer's SASL HDFS acceptance task (accept-hdfs.yml on 8060179bce0c: run 41986549800 failed in setup, fixed, 41986696064 running); marten (06:41Z)/heron (session 117) idle; no locks held.
- 2026-11-05 06:56Z: no change: no commits on kestrel main since 21:41Z; ITEM 31 still waits for the stand re-check of `server-1.0.0-8060179bce0c`; CI and service matrix green on 8060179b; marten (06:41Z)/heron (session 116)/egret (05:27Z) idle; no locks held.
- 2026-11-05 05:56Z: no change: no commits on kestrel main since 21:41Z; ITEM 31 still waits for the stand re-check of `server-1.0.0-8060179bce0c`; CI and service matrix green on 8060179b; marten (05:40Z)/heron (session 115)/egret (05:27Z) idle; no locks held.
- 2026-11-05 04:56Z: no change: no commits on kestrel main since 21:41Z; ITEM 31 still waits for the stand re-check of `server-1.0.0-8060179bce0c`; CI and service matrix green on 8060179b; marten (04:40Z)/heron (session 115)/egret idle; no locks held.
- 2026-11-05 03:56Z: no change since 21:57Z: no commits on kestrel main; ITEM 31 still waits for the stand re-check of `server-1.0.0-8060179bce0c`; CI and service matrix green on 8060179b; marten/heron (session 114)/egret idle; no locks held.
