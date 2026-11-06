# Orchestration status

Status as of **2026-11-06 10:58 UTC** (hourly orchestrator; it only watches, it does not change code).

Not ALL DONE: the maintainer accepted ITEM 33 on the stand (`35c15ab226b9`, d94e4068) and opened **ITEM 34** (minor,
log hygiene: one-line WARN for expected partial-mode refusals and copy-disk `NoRoomException`, qid + `hdfs=reads` on
refusals). Core session (lock 10:41Z, active) pushed 85293332; Dev CI 42205205027 running. ITEM 34 DONE not written yet.
**egret ACCEPT 35c15ab226b9 (run 42196478105): SHIP** - all checks pass in all 4 cells, incl. check 4 small disk in
both speed cells.

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | ITEM 34 (log hygiene) in progress | 85293332, 2026-11-06 10:46Z | held 10:41Z (active) | Dev CI 42205205027 running; Catalogs 42205204951 + Service hdfs 42205205124 green on 85293332; last finished Dev CI green (42108450187 package on 35c15ab2) |
| plugins (`PLUGINS-STATUS.md`) | paused; no new bug-first note | 2026-11-05 14:02Z | free | - |
| catalogs (`CATALOGS-STATUS.md`) | paused; no new bug-first note | 2026-11-05 14:02Z | free | - |
| lane 3 (`LANE3-STATUS.md`) | paused; no new bug-first note | branch 2026-10-29 | free | - |

No stale locks (marten and heron free).

## Service matrix (newest run per workflow)

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 42110845591 | 35c15ab2 | pass |
| service-hdfs.yml | 42205205124 | 85293332 | pass |
| service-security.yml | 42110857408 | 35c15ab2 | pass |
| service-stores.yml | 42110862958 | 35c15ab2 | pass |

## Pre-releases (newest 3)

- `server-1.0.0-35c15ab226b9` (2026-11-05 20:59Z): ITEM 33; accepted on the stand by the maintainer; egret ACCEPT **SHIP**.
- `server-1.0.0-50fe065af50c` (2026-11-05 14:15Z): ITEM 33 first cut. egret: DO NOT SHIP (check 4); superseded.
- `server-1.0.0-5f60db1ee304` (2026-11-05 10:40Z): ITEM 32; accepted by the maintainer for the stand.

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: **IMPROVEMENTS DONE** (2026-11-03). Finish mode; last session 10:41Z: no new server jar, no maintainer items. Lock free.
- heron: **IMPROVEMENTS DONE**; session 131 (10:51Z): nothing open. Lock free.

## egret

- Newest FRESH report: `results/FRESH-413485ba80fd.md` (upstream `6e264c10f403`), accepted as final for item 30.
  No FRESH for `5f60db1ee304` / `50fe065af50c` / `35c15ab226b9`.
- Newest acceptance: `results/ACCEPT-35c15ab226b9.md` (run 42196478105, 10:37Z): **SHIP**, checks 1-6 pass in
  default x1/x3 and speed x1/x3.

## For the maintainer

- Nothing blocking. ITEM 34 is in progress in the core lane; a `package=true` pre-release follows it.
- **Open question (item 33, speed cells):** default of `hdfsLocalCopyRoomWaitMillis` (opt-in, default 0); the stand
  says speed partial passes with and without room wait - may be closable.
- **Open question (item 31):** honour the client's Flight `SCHEMA` header as the first schema for plain names, and
  on by default?
- **Item 30 Open question (copies of joins / non-mergeable answers):** unanswered; free-disk guard is the bound.

## History

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
- 2026-11-04 21:57Z: no change: core session 21:41Z found nothing open (ITEM 31 waits for the stand re-check of `server-1.0.0-8060179bce0c`); CI and service matrix green on 8060179b; marten/heron (session 113)/egret idle; no stale locks.
- 2026-11-04 20:57Z: core ITEM 31 (7) done (1f3866d2, Dev CI 41902472862 green); `package=true` Dev CI 41905031992 green with service gate 4/4 green -> pre-release `server-1.0.0-8060179bce0c`; waits for the stand re-check, then ITEM 31 DONE; core lock released 20:48Z; marten/heron (session 112)/egret idle (egret has no FRESH for 8060179bce0c yet); no stale locks.
- 2026-11-04 19:57Z: core on ITEM 31: part 2 (ea0f1d73) green (Dev CI 41893438040, service-hdfs 41893438019, Catalogs 41893438065); two `package=true` dispatches (41895426530 on ea0f1d73, 41897670516 on d7e84969) red only in `service-gate` (main moved by later [skip ci] commits, service matrix not run on that sha; build green) - process, not a test failure, no bug-first note; core lock 19:41Z (active), (7) direct-memory OOM next; marten/heron (session 111)/egret idle; no stale locks.
- 2026-11-04 18:56Z: core on ITEM 31: part 1 fa7f901a green (Dev CI 41883760528, service-hdfs 41883760502, Catalogs 41883760547); part 2 37cbf500 (complete one-table queries batched over copies) pushed 18:47Z, Dev CI 41890561380 / service-hdfs 41890561200 running, Catalogs 41890561182 green; new Open question (client SCHEMA header for plain names); lock 18:41Z active; marten/heron (session 110)/egret idle; no stale locks.
- 2026-11-04 17:57Z: ALL DONE withdrawn: maintainer opened bug-first ITEM 31 (75edc566, plain-name SASL copies, wrong 0-row join answers); core lock 17:52Z, working; Dev CI on main green, service gate 4/4 green; marten 17:40Z/heron/egret idle; no stale locks.
- 2026-11-04 16:56Z: ALL DONE written: maintainer accepted FRESH-413485ba80fd as final in egret (dac5040); core session 16:42Z found no open item; locks free, Dev CI on main green (41770973165), service gate 4/4 green; marten 16:41Z/heron session 109 idle.
- 2026-11-04 15:57Z: no change: no core session since 13:42Z, locks free, Dev CI on main green (41770973165), service gate 4/4 green; marten (15:40Z)/heron (session 109)/egret (15:26Z) idle; ALL DONE still waits on the maintainer's FRESH decision.
- 2026-11-04 14:58Z: no change: no core session since 13:42Z, locks free, Dev CI on main green (41770973165), service gate 4/4 green; marten (12:41Z)/heron (session 109)/egret (13:26Z) idle; ALL DONE still waits on the maintainer's FRESH decision.
- 2026-11-04 13:57Z: no change: core session 13:42Z found no open item; locks free, Dev CI on main green (41770973165), service gate 4/4 green; marten (12:41Z)/heron (session 109)/egret (13:26Z) idle; ALL DONE still waits on the maintainer's FRESH decision.
- 2026-11-04 12:57Z: no change: no core session since 09:42Z, locks free, Dev CI on main green (41770973165), service gate 4/4 green; marten (12:41Z)/heron (session 109)/egret idle; ALL DONE still waits on the maintainer's FRESH decision.
- 2026-11-04 11:57Z: no change: no core session since 09:42Z, locks free, Dev CI on main green (41770973165), service gate 4/4 green; marten (11:40Z)/heron (session 108)/egret idle; ALL DONE still waits on the maintainer's FRESH decision.
