# Orchestration status

Status as of **2026-11-05 10:56 UTC** (hourly orchestrator; it only watches, it does not change code).

Not ALL DONE: the maintainer accepted item 31 for the stand (`47764f75`, 13:05 local note) and opened small
**ITEM 32** (two leftovers from egret's acceptance). The core lane finished it in code and CI (10:41Z); the
`package=true` pre-release is being built now. Remaining: that pre-release, then egret's re-ACCEPT -> "ITEM 32 DONE".

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | ITEM 32: relative copy reserve min(1 GiB, 10 % of disk) by default, `hdfs=reads` on every complete answer (`580dbdfa`, `23063397`, test fix `04c70af7`); waits for the pre-release + egret re-check | 5f60db1e, 15 min ago | free (released 10:41Z) | Dev CI 42010391188 on 04c70af7 **green**; 580dbdfa red (42008636974, `PeerReadGateTest` allocator race, fixed by 04c70af7 - no bug-first note needed); service-hdfs 42010059761 / Catalogs 42010059722 green on 23063397; `package=true` Dev CI 42012727794 **running** on 5f60db1e |
| plugins (`PLUGINS-STATUS.md`) | paused; no new bug-first note | 2026-11-03 | free | - |
| catalogs (`CATALOGS-STATUS.md`) | paused; no new bug-first note | 2026-11-03 | free | - |
| lane 3 (`LANE3-STATUS.md`) | paused; no new bug-first note | branch 2026-10-29 | free | - |

No stale locks (marten and heron free).

## Service matrix (gate of the `package=true` run on 5f60db1e, started 10:52Z)

| Workflow | Newest run | Commit | Result |
|---|---|---|---|
| service-cluster.yml | 42014348488 | 5f60db1e | running (previous 41907447179 on 8060179b pass) |
| service-hdfs.yml | 42014352876 | 5f60db1e | queued (42010059761 on 23063397 pass) |
| service-security.yml | 42014357596 | 5f60db1e | running (previous 41907457509 pass) |
| service-stores.yml | 42014361466 | 5f60db1e | queued (previous 41907462979 pass) |

## Pre-releases (newest 3)

- `server-1.0.0-8060179bce0c` (2026-11-04 20:16Z): ITEM 31 parts 1-3; accepted by the maintainer for the stand.
- `server-1.0.0-d2313a9eb651` (2026-11-04 04:42Z): ITEM 30.
- `server-1.0.0-5122cea1f045` (2026-11-03 11:04Z): item 29 part 2.
- Next: `server-1.0.0-5f60db1e...` (ITEM 32) once Dev CI 42012727794 and the service gate are green.

## marten / heron: IMPROVEMENTS from the connector comparison

- marten: **IMPROVEMENTS DONE** (2026-11-03). Finish mode; last session 10:41Z: no new server jar. Lock free.
- heron: **IMPROVEMENTS DONE**; session 120 (10:51Z): nothing open. Lock free.

## egret

- Newest FRESH report: `results/FRESH-413485ba80fd.md` (upstream `6e264c10f403`), accepted as final for item 30.
  No FRESH for `8060179bce0c`.
- `results/ACCEPT-8060179bce0c.md`: DO NOT SHIP (checks 4, 6) - superseded by the maintainer's item 31 acceptance
  and item 32. egret's track task (`e056c14`) now reads the ADBC status as `(ResourceExhausted; ...` and plans the
  next ACCEPT on the item 32 build. README "In progress": nothing running.

## For the maintainer

- Nothing blocking. Item 32 needs no decision; the `hdfsLocalCopyMinFreeBytes` question is answered and built.
- **Open question (item 31):** honour the client's Flight `SCHEMA` header as the first schema for plain names, and
  on by default?
- **Item 30 Open question (copies of joins / non-mergeable answers):** unanswered; free-disk guard is the bound.
- Connector default changes to review: marten `flight.pushdown.join_keys` 10000 -> 50000; heron `arrow-flight.split-count` 4 -> 1.

## History

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
- 2026-11-04 10:58Z: no change: no core session since 09:42Z, locks free, Dev CI on main green (41770973165), service gate 4/4 green; marten/heron (session 107)/egret idle; ALL DONE still waits on the maintainer's FRESH decision.
- 2026-11-04 09:58Z: no change: core session 09:41-09:42Z found no open item; locks free, Dev CI on main green (41770973165), service gate 4/4 green; marten/heron (session 106)/egret idle; ALL DONE still waits on the maintainer's FRESH decision.
- 2026-11-04 08:58Z: no change since 07:57Z; all lanes idle, locks free, CI/services green; ALL DONE still waits on the maintainer's FRESH decision (d2313a9eb651).
- 2026-11-04 07:57Z: no change: core idle (lock free), Dev CI on main green (41770973165), service gate 4/4 green on d2313a9e; marten/heron (session 105)/egret idle; no stale locks; ALL DONE still waits on the maintainer (FRESH for d2313a9eb651 or accept FRESH-413485ba80fd).
- 2026-11-04 06:57Z: no change: core idle (lock free), Dev CI on main green (41770973165), service gate 4/4 green on d2313a9e; marten/heron (session 104)/egret idle; no stale locks; ALL DONE still waits on the maintainer (FRESH for d2313a9eb651 or accept FRESH-413485ba80fd).
- 2026-11-04 05:57Z: ITEM 30 DONE (pre-release d2313a9eb651, Dev CI package=true 41774819696, service gate 4/4 green on d2313a9e); core idle (no open item, lock free); Dev CI on main green (41770973165); egret skips FRESH for d2313a9eb651 per the maintainer's 2026-11-03 note, so ALL DONE waits on the maintainer; no stale locks.
- 2026-11-04 04:57Z: core lock 04:41Z (active), closing ITEM 30 with known limits (d2313a9e docs: START-NOTES/CHANGELOG; "ITEM 30 DONE" and `package=true` not yet written); e9cdd42a/5253945e green (Dev CI 41770973165, service-hdfs 41769492301, Catalogs 41769492328); marten/heron/egret idle; no stale locks.
- 2026-11-04 03:57Z: core lock 03:40Z (active), pushed e9cdd42a (item 30 part 2b: HAVING / DISTINCT aggregates merged from copy batches), Dev CI 41769492302 / service-hdfs 41769492301 running; 4a1d354f green (Dev CI 41726874635, service-hdfs 41726885857: small-disk stage fixed); lane proposes ITEM 30 DONE with the copy-join limit unless the maintainer answers; marten finish-mode session 03:40Z (nothing to do); heron/egret idle; no stale locks.
- 2026-11-03 21:58Z: core lock 21:41Z, pushed 0418b441 (service-hdfs small copy-disk stage): Dev CI 41724478294 green, service-hdfs 41724478510 red only in the new small-disk stage (sasl-cluster balanced, `localCopyNoSpace`), lane active; 336f9607 Dev CI green; marten M4 default now server 0e457de30f25 (lakeFS 41713886604 pass); heron/egret idle; no stale locks.
- 2026-11-03 20:58Z: core lock 20:41Z, pushed 336f9607 (item 30 part 2b: `hdfsLocalCopyMinFreeBytes`, copies never fill their disk; query refused with `hdfs=localCopyNoSpace`); a4bbbab0 green (Dev CI 41707251465, service-hdfs 41707251290); service-hdfs 41715910681 on 336f9607 green, Dev CI 41715910689 running; marten session active (lock 19:43Z, M4 checks 6,7 and lakeFS 1,3,4,10 dispatched); heron/egret idle; no stale locks, no red CI.
- 2026-11-03 19:58Z: core lock 19:41Z, pushed a4bbbab0 (item 30 part 2b: LIMIT aggregates, DISTINCT / LIMIT scans over copy batches), Dev CI 41707251465 / service-hdfs 41707251290 running; marten session active (lock 19:43Z, M4 S3/iceberg rerun dispatched on server 0e457de30f25); heron/egret idle; no stale locks, no red CI.
- 2026-11-03 19:05Z: core footer-answered count(*) and `hdfsLocalCopyDir` (842f4a88) green (Dev CI 41696991827, service-hdfs 41696991867); new Open question (joins over the copy cache); core lock released; no new pre-release; marten/heron/egret idle; no stale locks, no red CI.
- 2026-11-03 18:05Z: core ITEM 30 part 2b step 2 green (Dev CI 41679739414); new footer-count commit 712047f8 red in Dev CI 41687057083 (its new unit test), lane's fix 4c74c89f running (41689345981); core lock 17:41Z; no new pre-release; marten/heron/egret idle; no stale locks.
- 2026-11-03 16:57Z: core ITEM 30 part 2b step 1 green (Dev CI 41671843220, service-hdfs 41669461010); core lock retaken 16:41Z; no new pre-release; marten/heron/egret idle; no stale locks, no red CI.
- 2026-11-03 15:57Z: core on ITEM 30 part 2b (part 2a green: Dev CI 41659194274, service-hdfs 41659194422; c00a0fba pushed 15:51Z, Dev CI / service-hdfs running; lock 15:41Z); no new pre-release; marten/heron/egret idle; no stale locks, no red CI.
- 2026-11-03 14:57Z: core on ITEM 30 part 2 (690becd9 pushed 14:45Z, lock 14:41Z); service-hdfs 41656768629 on it red only in the 2 sasl-cluster cells (all answers ok, TCP-read bound exceeded), Dev CI 41656768520 running; no new pre-release; marten/heron/egret idle; no stale locks.
- 2026-11-03 13:57Z: core ITEM 30 part 1 green (Dev CI 41644812393 after one flake rerun, service-hdfs 41641719428 10/10); part 2 open; core LOCK still holds 12:41Z although the Log says released (not stale by rule); no new pre-release; marten/heron/egret idle; no red CI.
- 2026-11-03 12:56Z: core on ITEM 30 (part 1 a523356e pushed, Dev CI / service-hdfs running; part 2 open); no new pre-release; marten/heron idle (IMPROVEMENTS DONE); egret idle; no stale locks, no red CI.
- 2026-11-03 11:58Z: ITEM 29 DONE (pre-release 5122cea1f045, service matrix 4/4 green incl. hdfs-ha / loss and sasl-cluster); core free, ITEM 30 next (not started); Dev CI green; egret FRESH 413485ba80fd published; no stale locks.
- 2026-11-03 10:57Z: core on ITEM 29 part 2 (routing, view binding per table, sasl-cluster cell; Dev CI 41620905057 green, service-hdfs running); service-cluster 41614141178 on 00e3e896 **red** (hdfs-ha loss q18 "READ not supported in state standby"); egret FRESH 413485ba run green, report pending; marten/heron idle; no stale locks.
- 2026-11-03 09:57Z: core on bug-first ITEM 29 (part 1 done 75e63681/dcc8f919; Dev CI red on 75e63681 = SpotBugs, fixed by dcc8f919, green 41609483775); new Open question item 29 (non-local SASL reads); marten IMPROVEMENTS DONE; heron idle; egret FRESH 413485ba80fd still running; no stale locks.
