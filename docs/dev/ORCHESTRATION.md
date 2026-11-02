# Orchestration status

Status as of **2026-11-02 17:30 UTC** (hourly orchestrator; it only watches, it does not change code).

Not ready yet: **ITEM 25 DONE** (core: `service-hdfs.yml` + pre-release gate) and **SERVICE MATRIX plugins DONE**
(all 8 cells green, but the lane's tag-policy bug comes first) are still missing.

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | RANGER TABLE CHECK FIX DONE (48090377); next: ITEM 25 (`service-hdfs.yml`: plain + HA + Kerberos + short-circuit, and the pre-release gate) | 17:20Z (27cfce5c) | taken 17:02Z (valid) | Dev CI on main: 41490875833 (f8ba588a) running; earlier main runs cancelled by newer pushes; last finished non-cancelled 41479189505 (4a9ae387) green |
| plugins (`PLUGINS-STATUS.md`) | bug first: `ranger-hive` refuses every table when the Hive service has a tag service (Ranger's default `EXPIRES_ON` deny); then SERVICE MATRIX plugins DONE | 17:20Z (fcf97038) | free | service-security 41488081427 (48090377): 8/8 cells green, ranger 59/59 |
| catalogs (`CATALOGS-STATUS.md`) | SERVICE MATRIX catalogs DONE; no open item | 16:18Z (fe62f3dc) | free | Catalogs CI 41486142794 green |
| lane 3 (`LANE3-STATUS.md`) | D13 (a1) short-circuit check with the Java client inside a DataNode, then D12 | 17:24Z (f8ba588a) | taken 17:16Z (valid) | service-cluster 41490875832 (f8ba588a) green |

No stale locks.

## Service matrix (newest finished runs)

| Workflow | Run | Commit | Cells |
|---|---|---|---|
| service-security.yml | 41488081427 | 48090377 | ldap, jwt, hive, ranger x balanced/speed: 8/8 pass |
| service-stores.yml | 41473218719 (pre-release fca6512a8046) | 880caeba | s3, iceberg-hadoop, iceberg-rest, ozone, lakefs-branch, lakefs-commit, postgres, mysql x balanced/speed: 16/16 pass |
| service-cluster.yml | 41490875832 | f8ba588a | hdfs/s3 x balanced, speed, speed-dc, loss: 8/8 pass |
| service-hdfs.yml | - | - | does not exist yet (core lane, ITEM 25) |

Markers: HDFS FIX DONE - yes (STATUS.md). SERVICE MATRIX catalogs DONE - yes. SERVICE MATRIX lane 3 DONE - yes.
SERVICE MATRIX plugins DONE - no. ITEM 25 DONE - no.

## Pre-releases (newest 3)

- `server-1.0.0-c4cd985f37e3` (2026-11-02 15:51Z): contains the HDFS fix; no service-hdfs.yml run (workflow missing). Not READY FOR CLUSTER TEST.
- `server-1.0.0-fca6512a8046` (2026-11-02 14:11Z)
- `server-1.0.0-f06b5834e76e` (2026-10-30)

The ranger table check fix (48090377) is not in a pre-release yet.

## marten / heron: connector comparison

- marten: CONNECTOR COMPARE DONE (2026-11-02 15:50Z), `docs/CONNECTOR-COMPARE.md` is there; no open item.
- heron: CONNECTOR COMPARE DONE (session 94), `docs/CONNECTOR-COMPARE.md` is there.

## For the maintainer

- Stand: re-run the HA + Kerberos cluster on `server-1.0.0-c4cd985f37e3` with only `extensionDir` set; the startup line must show `duckdb=hdfsExtensionLoaded` (core lane's request).
- Production note (plugins lane): with Ranger tag-based policies in use, `ranger-hive` currently refuses every query (default `EXPIRES_ON` deny policy); fix in progress in the plugins lane. Ranger 2.4 script conditions cannot run on Java 21 (no Nashorn).
- No new blocking Open questions in any lane.

## History

- 2026-11-02 17:30Z: ranger cell added and green (8/8) after the core table-check fix; plugins lane on the tag-policy bug; core lane locked, ITEM 25 next; no service-hdfs.yml yet.
- 2026-11-02 16:25Z: first orchestrator status. HDFS FIX DONE; matrix: catalogs and lane 3 DONE, plugins at 3/4 services; core ITEM 25 open; both connector comparisons DONE.
