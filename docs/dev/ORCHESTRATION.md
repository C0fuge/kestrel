# Orchestration status

Status as of **2026-11-08 13:56Z UTC** (hourly orchestrator; it only watches, it does not change code).

Not ALL DONE: **ITEM 38** (minor logs/hints) is done in code/CI and packaged as pre-release `server-1.0.0-b69660b9260a`
(19:05Z); it waits for the maintainer's stand check and "ITEM 38 DONE". The routine's original criterion (items 26/27,
marten/heron IMPROVEMENTS DONE) is long met; no egret FRESH on the newest pre-release (egret runs ACCEPT, not FRESH, on
log-only builds; **ACCEPT b69660b9260a SHIP**).

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | ITEM 38 done in CI, waits for the stand | c914e752, 10:42Z lock/unlock only (~135 min) | free (released 10:42Z) | main Dev CI 42403022881 (b69660b9) green; package Dev CI 42403052289 (pkg/b69660b9) green |
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

- marten: **IMPROVEMENTS DONE** (2026-11-03); SERVER deafc861cce3 CHECKED; session 12:41Z "finish mode, nothing new".
  Open question (no decision needed to continue): TPC-H q16 SF1 ~0.5 s slowdown from join-key IN-list pushdown - fix or leave.
- heron: **IMPROVEMENTS DONE**; SERVER deafc861cce3 CHECKED; session 166 (13:51Z): nothing open.
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
