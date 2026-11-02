# Orchestration status

Status as of **2026-11-02 16:25 UTC** (hourly orchestrator; it only watches, it does not change code).

Not ready yet: **ITEM 25 DONE** (core gate + `service-hdfs.yml`) and **SERVICE MATRIX plugins DONE** are still missing.

## Lanes

| Lane | Current item | Last commit | Lock | CI |
|---|---|---|---|---|
| core (`STATUS.md`) | HDFS FIX DONE (pre-release `server-1.0.0-c4cd985f37e3`); next: ITEM 25 (`service-hdfs.yml`: plain + HA + Kerberos + short-circuit, and the pre-release gate) | 16:12Z (30a6f6c1) | free | Dev CI on main: latest run 41479189505 (4a9ae387) still running; earlier main runs were cancelled by other lanes' pushes; c4cd985f green on `ci/core-hdfs-fix` (41475681887); last finished non-cancelled main run 41459673523 (fca6512a) green |
| plugins (`PLUGINS-STATUS.md`) | SERVICE MATRIX, security part: LDAP, JWT, hive cells green; ranger cell still to do | 16:03Z (3297d041) | taken 15:27Z (58 min old, valid) | service-security 41477683286 green; 41480086139 running |
| catalogs (`CATALOGS-STATUS.md`) | SERVICE MATRIX catalogs DONE; no open item | 16:18Z (fe62f3dc) | free | Catalogs CI 41477683108 green |
| lane 3 (`LANE3-STATUS.md`) | SERVICE MATRIX lane 3 DONE; D13 (a1) short-circuit check, then D12 | 16:19Z (90ceae03) | free | service-cluster 41479197754 green |

No stale locks.

## Service matrix (newest finished runs)

| Workflow | Run | Commit | Cells |
|---|---|---|---|
| service-security.yml | 41477683286 | 3297d041 | ldap, jwt, hive x balanced/speed: 6/6 pass (ranger cell not added yet) |
| service-stores.yml | 41473218719 (pre-release fca6512a8046) | 880caeba | s3, iceberg-hadoop, iceberg-rest, ozone, lakefs-branch, lakefs-commit, postgres, mysql x balanced/speed: 16/16 pass |
| service-cluster.yml | 41479197754 | 4a9ae387 | hdfs/s3 x balanced, speed, speed-dc, loss: 8/8 pass |
| service-hdfs.yml | - | - | does not exist yet (core lane, ITEM 25) |

Markers: HDFS FIX DONE - yes (STATUS.md). SERVICE MATRIX catalogs DONE - yes. SERVICE MATRIX lane 3 DONE - yes.
SERVICE MATRIX plugins DONE - no. ITEM 25 DONE - no.

## Pre-releases (newest 3)

- `server-1.0.0-c4cd985f37e3` (2026-11-02 15:51Z): contains the HDFS fix; no service-hdfs.yml run yet. The cluster cells pass with it and fail with fca6512a8046 when hadoopfs comes only from `extensionDir`.
- `server-1.0.0-fca6512a8046` (2026-11-02 14:11Z)
- `server-1.0.0-f06b5834e76e` (2026-10-30)

## marten / heron: connector comparison

- marten: CONNECTOR COMPARE DONE (2026-11-02 15:50Z), `docs/CONNECTOR-COMPARE.md` is there; routine paused, no open item.
- heron: CONNECTOR COMPARE DONE (session 94), `docs/CONNECTOR-COMPARE.md` is there.

## For the maintainer

- Stand: re-run the HA + Kerberos cluster on `server-1.0.0-c4cd985f37e3` with only `extensionDir` set; the startup line must show `duckdb=hdfsExtensionLoaded` (core lane's request).
- Lane 3 found that HDFS short-circuit reads are never used in CI (all reads go over TCP). Answers are right; it only costs speed. Under investigation (D13 a1).
- No new blocking Open questions in any lane today.

## History

- 2026-11-02 16:25Z: first orchestrator status. HDFS FIX DONE; matrix: catalogs and lane 3 DONE, plugins at 3/4 services; core ITEM 25 open; both connector comparisons DONE.
