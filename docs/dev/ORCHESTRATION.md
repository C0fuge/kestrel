# Orchestration status

Status as of **2026-11-02 18:00 UTC** (hourly orchestrator; it only watches, it does not change code).

Not ready yet: **ITEM 25 DONE** (core: `service-hdfs.yml` + pre-release gate; not started, workflow does not exist)
and **SERVICE MATRIX plugins DONE** (tag-policy fix 3f71355e + cell change 201df3b2 pushed, service-security run
41494830118 in progress) are still missing.

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | RANGER TABLE CHECK FIX DONE, pre-release f8ba588a8295; next: ITEM 25 core part (`service-hdfs.yml`: plain + HA + Kerberos + short-circuit, and the `package=true` gate) | 17:44Z (695f4b1e) | free | Dev CI 41491152401 (`ci/core-authz-fix` = f8ba588a) and 41490875833 (main f8ba588a) green; later main push runs cancelled by newer pushes; 41495736054 (29903c01) running |
| plugins (`PLUGINS-STATUS.md`) | bug first: `ranger-hive` tag policies (fix 3f71355e: restrictive tag policies refuse only tagged tables; 201df3b2: cell creates the `EXPIRES_ON` tag); then SERVICE MATRIX plugins DONE | 17:49Z (201df3b2) | taken 17:33Z (valid) | service-security 41494830118 (201df3b2): ldap + jwt 4/4 green, hive + ranger running |
| catalogs (`CATALOGS-STATUS.md`) | SERVICE MATRIX catalogs DONE; no open item | 16:18Z (fe62f3dc) | free | Catalogs CI 41494830138 (201df3b2) green |
| lane 3 (`LANE3-STATUS.md`) | D13 (a2): per-query DataNode block reads + partial-mode locality probe in the HDFS cells, then D12 | 17:55Z (29903c01) | taken 17:53Z (valid) | service-cluster 41495736199 (29903c01) queued; 41490875832 (f8ba588a) green |

No stale locks. Dev CI on main: no red run.

## Service matrix (newest finished runs)

| Workflow | Run | Commit | Cells |
|---|---|---|---|
| service-security.yml | 41488081427 | 48090377 | ldap, jwt, hive, ranger x balanced/speed: 8/8 pass (41494830118 on 201df3b2 running) |
| service-stores.yml | 41473218719 (pre-release fca6512a8046) | 880caeba | s3, iceberg-hadoop, iceberg-rest, ozone, lakefs-branch, lakefs-commit, postgres, mysql x balanced/speed: 16/16 pass |
| service-cluster.yml | 41490875832 | f8ba588a | hdfs/s3 x balanced, speed, speed-dc, loss: 8/8 pass |
| service-hdfs.yml | - | - | does not exist yet (core lane, ITEM 25) |

Markers: HDFS FIX DONE - yes (STATUS.md). SERVICE MATRIX catalogs DONE - yes. SERVICE MATRIX lane 3 DONE - yes.
SERVICE MATRIX plugins DONE - no. ITEM 25 DONE - no.

## Pre-releases (newest 3)

- `server-1.0.0-f8ba588a8295` (2026-11-02 17:43Z): HDFS fix + ranger table check fix. service-cluster green on
  f8ba588a; security/stores not run on this commit; no service-hdfs.yml. Not READY FOR CLUSTER TEST.
- `server-1.0.0-c4cd985f37e3` (2026-11-02 16:11Z): first with the HDFS fix.
- `server-1.0.0-fca6512a8046` (2026-11-02 14:30Z).

The ranger-hive tag-policy fix (3f71355e) is not in a pre-release yet.

## marten / heron: connector comparison

- marten: CONNECTOR COMPARE DONE (2026-11-02 15:50Z), `docs/CONNECTOR-COMPARE.md` is there; no change since 16:05Z.
- heron: CONNECTOR COMPARE DONE (session 94), `docs/CONNECTOR-COMPARE.md` is there; last commit 17:35Z (session 95 housekeeping).

## For the maintainer

- Stand: re-run the HA + Kerberos cluster on `server-1.0.0-f8ba588a8295` (or c4cd985f37e3) with only `extensionDir`
  set; the startup line must show `duckdb=hdfsExtensionLoaded` (core lane's request).
- Production note (plugins lane): with Ranger tag-based policies, `ranger-hive` in current pre-releases refuses every
  query (default `EXPIRES_ON` deny); fix 3f71355e on main, verification run in progress, not yet packaged. Ranger 2.4
  script conditions cannot run on Java 21 (no Nashorn).
- Lane 3 short-circuit finding (handed to core): hadoopfs/libhdfs3 does not use short-circuit reads although the
  layout supports them; answers are right, only speed is lost.
- No new blocking Open questions in any lane.

## History

- 2026-11-02 18:00Z: plugins tag-policy fix pushed (security run in progress); core free, ITEM 25 not started; lane 3 on D13 (a2); pre-release f8ba588a8295; no stale locks, no red CI.
- 2026-11-02 17:30Z: ranger cell added and green (8/8) after the core table-check fix; plugins lane on the tag-policy bug; core lane locked, ITEM 25 next; no service-hdfs.yml yet.
- 2026-11-02 16:25Z: first orchestrator status. HDFS FIX DONE; matrix: catalogs and lane 3 DONE, plugins at 3/4 services; core ITEM 25 open; both connector comparisons DONE.
