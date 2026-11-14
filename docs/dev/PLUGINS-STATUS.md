# Plugins and integrations lane - status

Second lane of development (see `CLAUDE.md`, "Second lane"). Basis: the maintainer's research of 2026-10-28 (summary
below). Production target: on-prem Hadoop 3.1.3 (HA, Kerberos), Hive Metastore, Ranger 2.4; BI and Spark users.

## Next steps (one per session, in this order)

> **Maintainer, 2026-11-11 18:40 - ITEM 46 (minor, stand on `29f1267b995f`; no wrong answers, package shipped).**
> (1) balanced in 6 GB / -Xmx3g / direct 2g logs `duckdb=memoryLimitAboveJvmLimits memoryLimit=1024MiB instances=4
> budgetMiB=102`: the budget still uses the full -Xmx + direct while the floors use the real reservation - make both
> use the same number (no limits that add up above the container). (2) Every denial WARN carries `qid=null` - pass the
> query id. (3) Log once at start which data-transfer QOP / cipher the extension negotiated (needs a hadoopfs
> setting or function from the extension repo - ask there if not available). Merge when green, "ITEM 46 DONE <sha>".

ITEM 46 DONE c96a1272 (2026-11-11, plugins lane; `fix/46` rebase-merged into `main` at 74464701, see Log 17:10Z).
Parts (1) and (2) complete; part (3) logs the requested QOP / cipher (`negotiated=unknown`) - the negotiated value
waits on the extension repo (Q-46). Packaging is left to the core lane's `package=true`. **Next for the maintainer's
stand:** on the next build, check the `duckdb=sizing` line in the 6 GB / -Xmx3g / direct 2g shape (expected
`memoryLimit=665MiB budgetMiB=2662`-ish, no limits above the container) and a `qid=` on denial WARNs. Nothing else
open in this lane besides Q-46 and the Q-40a follow-up.

**State 2026-11-11 12:20Z: nothing open in this lane.** Every maintainer note below is DONE (ITEM 44 e75705c9,
ITEM 42 db35b57a, Q-42 15220a77, 39(b), service matrix); they stay as the record. Waiting on the maintainer only:
the stand's repeat of `ranger-policy ... off` on `server-1.0.0-29f1267b995f` (ITEM 44 (2)) and the Q-40a gizmo wave
(then "ITEM 40(a) DONE"). Put a new item above this line.

> **Maintainer, 2026-11-11 00:05 - BUG FIRST in this lane, ITEM 44 (regression from Q-42, real-cluster build
> `2ff221447b5d`).** Performance repo `results/FRESH-2ff221447b5d.md`: **balanced fails TPC-H heavy queries with DuckDB
> Out of Memory at a 128 MiB `memory_limit`** - local SF10 q09/q10/q13 every round, S3 SF1 3-5 and SF10 5-8 queries
> (harness container 2 CPU / 6 GB, -Xmx3g, direct 2g: the budget after the JVM reservation is ~100 MiB, so Q-42's cap
> put the per-connection limit at the 128 MiB floor). `37bf9401d77c` balanced was 22/22. A memory cap must never make
> queries fail that ran before: for balanced use the item 40(a) reservation (what the JVM really uses, not the full
> -Xmx + direct) to compute the budget, keep a usable floor (e.g. 512 MiB per connection or the old 1 GiB when the
> container has the room), and WARN at start when the configured heap+direct leave less than that. Test with the
> harness shape (6 GB, -Xmx3g, direct 2g): TPC-H SF10 22/22 in balanced. Also from the stand (same build):
> (2) **Ranger revoke delayed 45-70 s on the direct SASL path** (`ranger-policy ... off`: queries keep answering at
> +25 s and +45 s, also 3 tables never read before; denial only between +45 and +70 s; with
> `-DhdfsSaslDataTransfer=off` or `-DhdfsMetadataCacheTtlMs=0` denial at +25 s): with the metadata/listing caches a
> repeat query makes no NameNode call, so no Ranger check. Bound the authorization staleness: re-validate access with
> one cheap NameNode call per table at most every N s (default 10 s, setting), or cap the cache TTL for permissions.
> (3) plain-name denial status: after the revoke, plain `q01` (via defaultSchema) gets `UNAVAILABLE: Permission
> denied: user=flight, access=READ_EXECUTE, inode="/data/parquet/types"...` - should be UNAUTHORIZED, no HDFS path in
> the client text, a `result=denied` WARN. Branch `fix/44`, merge when green, "ITEM 44 DONE <sha>".

ITEM 44 DONE e75705c9 (2026-11-11, plugins lane; parts (1), (3) in 2335ddc6, part (2) in 8ea12800 + e75705c9; see
Log 03:40Z). (2): new setting `hdfsAccessRecheckMs` (default 10000 on `hdfs://`, 0 = off) - each query asks the
NameNode (`FileSystem.access`, READ_EXECUTE on the table directory) once per data-directory table at most every
interval; a denial is UNAUTHORIZED with the generic text. Limits (no action unless the stand shows them): a revoke on
files only (directory still readable) is not seen by the recheck, only by the next real NameNode read; tables of
catalog plugins (hive, iceberg) are not rechecked. **Next for the maintainer's stand:** repeat the `ranger-policy ...
off` check on the next build - expected denial within Ranger's refresh + 10 s. Nothing else open in this lane besides
the Q-40a follow-up.

> **Maintainer, 2026-11-10 11:10 - FIRST in this lane: ITEM 42 (two stand bugs (stand check of `894afb9b48c4`, ~500 000 hash-checked answers, none wrong; logs in the maintainer's stand)).**
> (1) **speed sizes DuckDB above the budget in small cgroups.** 3g container, `-Xmx768m -XX:MaxDirectMemorySize=512m
> -DresourceProfile=speed`, no `duckDbMemoryLimit`, a self-join GROUP BY of lineitem: start log `duckdb=sizing
> threads=16 memoryLimit=2457MiB ... budgetMiB=1331` (2457 + 768 + 512 > 3072) -> OOM kill (exit 137) in 5-13 s,
> client `UNAVAILABLE: Network closed for unknown reason`. Cause: `DuckDbSizing.compute` gives each of 4 connections
> at least 1 GiB, capped at 80 % of memory instead of the budget. The DuckDB limit must never exceed the budget
> (budget = cgroup/host memory - JVM reservation of item 40(a)); test with small budgets (1-3 GiB).
> (2) **file pruning by footer statistics (item 39 (c) part 3) never runs with SASL DataNodes.** One server, debug
> log for `...server.services`, `SELECT ... FROM tpch.orders WHERE o_orderkey = 49378`: no `views=prunedFiles` line,
> `-DfileStatsPruningMaxFiles=0` gives the same rate; with privileged DataNodes the same query logs
> `views=prunedFiles table=tpch.orders kept=1 files=8`. The real cluster has SASL DataNodes. Find the SASL /
> short-circuit-only (or direct SASL, item 41) path that skips pruning and apply it there; CI: the sasl cells must
> show `views=prunedFiles` for a point query.
> Branch `fix/42-sizing-pruning`, maintainer exception to the lane scope, merge into `main` when green, "ITEM 42 DONE <sha>".

*ITEM 42 DONE db35b57a (2026-11-10, plugins lane; see Log 09:05Z).* (1) speed caps the shared DuckDB's default limit at
the budget (stand shape: 2457 -> 1331 MiB). (2) Cause: on short-circuit-only SASL nodes with node-local copies the
complete answer went through `completeFromCopies`, which replaced the table by all its files before `prunedFiles`
ran; pruning now happens first there (and in `completeInCopyBatches`). Follow-ups for the maintainer: Q-42 below.
*Q-42 DONE 15220a77 (2026-11-10): (a) balanced / shared / fallback floors capped at the budget, WARN below 1 GiB;
(b) pruning before `requireReadableHere` with copies off. Nothing open in this lane's Next steps besides the
Q-40a follow-up (gizmo wave, waits for the maintainer).*

> **Core lane, 2026-11-09 13:10Z - note on ITEM 39(b) (no action of this lane lost):** the core session had already
> pushed a fix for (b) to `main` (`39f84617`) before it saw the split: `HostLimits` now reads the memory limit of the
> process's own cgroup (`/proc/self/cgroup`) and of every parent up to the mount point (smallest wins, v2 then v1),
> start log `memoryBudget source=cgroup|host memoryMiB= hostMiB= cgroupMiB= cgroup=`; unit test in `DuckDbSizingTest`;
> Dev CI step "Server in a memory-limited systemd scope (item 39 (b))" (speed server, `MemoryMax=3G` scope, 100 M-group
> aggregate, exact answer, no OOM kill). Its CI result is in `STATUS.md` Log. This lane: check it against the
> stand's description (dc nodes, heap/direct share), extend on `fix/39b-memory` if anything is missing, and write
> "ITEM 39(b) DONE" in this Log; no need to redo the cgroup reading.

*Item 39(b) done 2026-11-09 (plugins lane check of the core lane's `39f84617`, nothing to extend; see Log).*

> **Maintainer, 2026-11-10 00:25 - NEXT in this lane: ITEM 40(a)** (S3 TPC-H SF10 1.5x behind GizmoSQL on the same
> DuckDB): see `STATUS.md` "ITEM 40". Branch `fix/40a-s3`, maintainer exception to the lane scope for this item,
> merge into `main` when green, "ITEM 40(a) DONE <sha>" in this Log.
>
> *Item 40(a) step 1 done 2026-11-10 (plugins lane): cause found, fix needs Q-40a below.* The S3 gap is DuckDB's
> memory limit under speed, not the S3 read path: speed sizes DuckDB as memory - `-Xmx` - `MaxDirectMemorySize` - 15 %;
> the external file cache lives in that limit, so with a large reserved heap the warm passes re-read S3 (q09/q10/q12/
> q14/q20 7-20x plain). `tpch-perf.yml` (S3, SF10, 4 CPU / 8 GB container, ADBC): speed with -Xmx3g, direct 2g
> (DuckDB 2048 MiB) 75.8 s; same with `-DduckDbMemoryLimit=5120MiB` 19.4 s; with `-Xmx1g -XX:MaxDirectMemorySize=1g`
> (auto-sized 4914 MiB, peak 4.1 GB of 8) **15.9 s vs plain DuckDB 1.5.6 (GizmoSQL-like) 16.9 s**, same answers;
> `endpointsPerServer=1` no effect. **Next step:** after Q-40a, apply the chosen sizing (profile code + test, or
> START-NOTES/harness only), ask the performance repo for a gizmo wave on the harness shape, then "ITEM 40(a) DONE".
> *Step 2 done 2026-11-10 (plugins lane, docs only):* option (A) documented in START-NOTES "Sizing `speed` for S3"
> (no default changes). The settings-diff side issue was already fixed by 72740e75: run 42751636105 prints the diffs.
> They show no S3-path setting that explains the gap: under `speed` (`extra`, `more1`) `enable_external_file_cache`
> equals plain's (true); the differences are the memory limit (4.7 vs 6.3 GiB), lockdown settings, HTTP retries
> (6 / 200 ms / backoff 2 vs 3 / 100 ms / 4, only on failures), `httpfs_connection_caching` and
> `enable_http_metadata_cache` / `parquet_metadata_cache` on (ours), `preserve_insertion_order=false`.
> *2026-11-10 06:45Z (plugins lane):* memory-pressure check for option (B) dispatched while Q-40a waits:
> `tpch-perf.yml` run **42760596461** on `main` (S3 SF10, 8 GB container, -Xmx3g/direct 2g, **4 concurrent Java
> clients**, configs default | speed | speed `duckDbMemoryLimit=4608MiB` (~ (B): half of heap+direct reserved) |
> speed 5120MiB); still in Measure after 90 min. **Next session: read its summary** (client failed = OOM kill;
> peak MiB per config) and add the result to Q-40a.
> *Step 3 done 2026-11-10 (plugins lane, Q-40a (C)): d295fe36 on `main`.* speed sizes DuckDB with heap
> min(-Xmx, max(1 GiB, heap in use at startup x 2)) and direct min(direct, 1 GiB + 25 %); tpch-perf's 8 GB / -Xmx3g /
> direct 2g -> 4402 MiB (was 2048). Dev CI 42775184825 green on the branch incl. the new memory-pressure step.
> **Next step (open):** a gizmo wave in the performance repo on the harness's JVM flags (this lane cannot touch
> other repositories - see the open question "Q-40a follow-up"), then "ITEM 40(a) DONE". tpch-perf 42760596461
> (4 clients) read: see Log 08:00Z.
> *(Old text:)* Then: (B) if chosen - profile code + `DuckDbSizingTest` case + a memory-pressure
> check (speed with the raised limit in a `MemoryMax` scope, as item 39(b)'s CI step); in any case ask the
> performance repo for a gizmo wave on the harness's -Xmx/direct, then "ITEM 40(a) DONE".

> **Maintainer, 2026-11-09 16:30 - FIRST, before anything else in this lane: ITEM 39(b).** speed / dc memory budget from the cgroup / container limit, not host RAM (item 39 (b)). Full description
> with the real-cluster numbers: `docs/dev/STATUS.md`, notes "ITEM 39 IS SPLIT ..." and "BUG FIRST, ITEM 39". Work on branch
> `fix/39b-memory` (create it from `origin/main` if missing; rebase on `origin/main` each session), tests + CI green on the branch,
> then merge into `main` (pull --rebase, keep both sides of CHANGELOG conflicts) and write "ITEM 39(b) DONE <merge sha>"
> in this file's Log. Maintainer exception to this lane's scope limits for this item only. No `package=true` here: the
> core lane packages when all four parts are merged.

> **Maintainer, 2026-11-02 18:30 - FINISH MODE lifted for this item: SERVICE MATRIX, plugins part (take it first).**
> A real Hadoop cluster refused every HDFS query with `f06b5834e76e` (core lane fixes it); no CI run talks to real
> services end to end. Add `.github/workflows/service-security.yml`: **LDAP** login (OpenLDAP container, a user + a
> group mapped into Ranger), **JWT** login (a JWKS issuer container, e.g. Keycloak or a static JWKS + signed token),
> **Ranger** (Ranger admin container if it fits in a runner, else the in-process Ranger but through the real server
> process: table policy + a column mask + a row filter, answers checked), **Hive Metastore** tables of the hive plugin
> (Parquet, ORC, text, Avro, a view) over HDFS or local. Own CI workflow of its own (only this lane edits it), triggered on `workflow_dispatch` with an input `server` (a kestrel
> pre-release id; default: build from the checked-out commit) and on push to main touching its files. Each cell: a real
> container of the service, the server in **balanced and `-DresourceProfile=speed`**, TPC-H SF1 22/22 with answers
> checked against in-process DuckDB + Flight SQL metadata + one prepared query; job summary = table service x profile
> -> pass/fail + first error line. A failure in a cell is a bug for the lane that owns the code (write it as bug-first
> into that lane's STATUS). Then write "SERVICE MATRIX <lane> DONE" at the start of a line.

*Service matrix, step 1 done 2026-11-02 (90da3772):* `.github/workflows/service-security.yml` - job `server` (build
of the commit, or input `server` = pre-release tag: jar + libs + auth tars), matrix `cell` service x profile
(balanced, `-DresourceProfile=speed`), job `summary` (table service | profile | pass/fail | first failed check).
Each cell: TPC-H SF1 Parquet + DuckDB's in-process answers (`tools/ci/service-tpch-gen.py`), a test CA and a
certificate for 127.0.0.1 (Flight TLS; both auth providers need TLS), the server on the runner, then
`tools/ci/service-check.py` (ADBC Flight SQL 1.13): refused anonymous / wrong credential, GetSqlInfo, GetTables
(exactly the 8 tables), lineitem schema (16 columns), 22 queries complete (rows as sorted multisets, numbers rel.
1e-9), one prepared query with 2 bound parameters - 28 checks. Cells **ldap** (osixia/openldap 1.5.0, plaintext
`ldap://` with `FLIGHT_LDAP_ALLOW_PLAINTEXT`, search mode + service account + group search, user `alice` in
`analysts`) and **jwt** (Keycloak 26.4.2 `start-dev` over HTTPS with the test cert, JWKS URL + PKCS12 trust store,
password grant of public client `flight`, audience mapper, `preferred_username`, groups claim `analysts`). Runs:
push 41468664530 (90da3772) 4/4 cells 28/28; dispatch with `server=server-1.0.0-fca6512a8046` 41469307322 4/4.
*Step 2 done 2026-11-02 (d566c427, e6b6ddca, 3297d041): cell **hive**.* `tools/ci/service-hive-data.py`
copies the TPC-H Parquet files and writes '|' text files (DuckDB, no quoting) under `/tmp/hive-svc` (not under
`$RUNNER_TEMP`: Hive skips paths below a `_` directory) plus `ddl.sql`: databases `tpch_parquet`, `tpch_text`,
`tpch_orc`, `tpch_avro` (external tables, columns = the Parquet files' types in Hive types), `INSERT OVERWRITE`
into the ORC and Avro tables from the text tables (so those files are **Hive's own**), and view
`tpch_views.segment_lines` (joins customer Parquet x nation Avro x orders text x lineitem ORC, filter, group by;
DuckDB's answer in `queries/view.json`). `tools/ci/service-hive.sh`: apache/hive:3.1.3 HiveServer2 (embedded
Derby, Tez local, `-Xmx4g`) runs `ddl.sql` (SF1: 71-113 s), then a standalone metastore on the same Derby
(readiness probed inside the container - docker's port proxy accepts early, which made a server start fail with
"Socket is closed by peer"). Server: packaged hive plugin (`tools/make-plugin-dir.sh`, or the pre-release's
plugins tar), `-Darrowflight.catalog=directory,hive`, `hive.orcTables/textTables/
avroTables/views=true`, `arrowflight.duckdb.extensionDir` from ci-assets (the server refuses to start with Avro
tables without DuckDB's avro extension); login = static bearer token (insecure mode refuses TLS).
`service-check.py` gets `--schema` (repeatable; `tpch.` -> `<schema>.`, checks named `<schema>:<check>`) and
`--view`. Hive cell: 104 checks (refusals, sql-info, 4 x (tables, lineitem schema, 22 queries, prepared), view).
Runs: push 41477683286 (3297d041) 6/6 cells (hive 104/104 in both profiles); dispatch with
`server=server-1.0.0-fca6512a8046` (plugins tar of the pre-release) 41480086139 6/6. Earlier runs 41474257754
(insecure mode + TLS refused) and 41475888176 (balanced: metastore not yet listening) were CI-setup faults, no
server or plugin bug found.
*Step 3 done 2026-11-02 (d733dc7a, 496ef3c7, 629c36f7): cell **ranger** - red because of two bugs it found.*
`tools/ci/service-ranger.sh start|setup`: Ranger admin 2.7.0 + `apache/ranger-db:2.7.0` (PostgreSQL) on network
`ranger-svc`, container/host names `ranger-db` / `ranger`, `127.0.0.1:6080`, no Solr, started first (ready by the time
Hive is done); setup waits for service `dev_hive`, creates group `analysts` and per database (`tpch_parquet`,
`tpch_orc`) a select policy (all tables/columns), a `MASK_SHOW_LAST_4` mask on `customer.c_phone` and a row filter
`o_orderstatus = 'F'` on `orders`; prints what the plugin downloads. The cell reuses the ldap cell's OpenLDAP (login
`alice` in `analysts`) and the hive cell's Hive tables; server with `FLIGHT_AUTHORIZATION_PROVIDER=ranger-hive`,
`auth/ldap/*:auth/ranger-hive/*`. `tools/ci/service-ranger-data.py`: DuckDB computes the 22 answers + prepared under
the same mask and filter, plus `extra.json` probes (masked phones, order count per status, `tpch_text` refused);
`service-check.py --extra`. Result (run 41486142813, both profiles): 35/59 - refusals, sql-info, listings, schemas,
mask, row filter, refusal of an unpoliced database and the 11 queries the table check can name (q1, 2, 4, 6, 11,
15-18, 20, 21; q4/q18/q21 read the filtered orders) equal DuckDB's restricted answers. Failing: **(core bug,
written bug-first into STATUS.md)** the server's table check under any authorization provider refuses comma joins,
FROM subqueries and `?` parameters (q3, 5, 7-10, 12-14, 19, 22, prepared). **(our bug, item below)** Ranger's
default tag policy refused every table until the cell deleted it.
*Core fix confirmed:* run 41488081427 (dispatch on 48090377, the core lane's fix of the table check): **8/8 cells
green, ranger 59/59 in both profiles** (with the tag-policy deletion still in place).

*Step 4 done 2026-11-02 (3f71355e, 201df3b2, 1ea39a30): tag policies.* `ranger-hive` refused every table when the
Hive service has a tag service with a restrictive tag policy (always: Ranger's default `EXPIRES_ON` deny).
`HiveRestrictivePolicies` keeps one entry per restrictive tag policy (its tag names; null = any tag for exclusions,
patterns, no values) and `probe(database, table, tags)` refuses a table only when a tag on the database, the table
or one of its columns is one of them, or when the tags are unknown (no tag enricher / tags not loaded) - conditions
are not evaluated (an `EXPIRES_ON` tag refuses whether expired or not; Ranger 2.x script conditions need a JS
engine Java 21 lacks). `HiveRangerPlugin.tags` asks Ranger's own `RangerTagEnricher` (ANY access,
`SELF_OR_DESCENDANTS`: ancestors' and columns' tags). Tests `HiveTagPoliciesTest` 4/4 (real tag enricher with
`ServiceTags`: column / table / database tags, untagged table read, tags not loaded, excluded tag = any tag,
allow-only tag policies). Ranger cell: the default tag policy stays; setup creates tag type + tag `EXPIRES_ON`
(expiry 2020/01/01) on `tpch_avro.nation` through `/service/tags/tagdefs|tags|resources|tagresourcemaps`
(`importservicetags` answered 400) and waits until it is served; analysts may also select in `tpch_avro`; extra
checks `tag-expired` (refused) and `tag-untagged` (`tpch_avro.region` count 5). Run 41496378967 (1ea39a30):
**8/8 cells green, ranger 63/63 in both profiles.**

SERVICE MATRIX plugins DONE (2026-11-02, run 41496378967: ldap, jwt, hive, ranger x balanced, speed all green).
Nothing of the service matrix item is open for this lane; the FINISH MODE note below applies again (no new items).

> **Maintainer, 2026-10-29 13:15 - FINISH MODE:** finish P6 completely (masks + row filters wired into queries, tests,
> docs), then write "PLUGINS LANE DONE" and stop; no new items.

> **Maintainer, 2026-10-28 15:00 - native first.** Integrations must talk Arrow Flight / Flight SQL natively (Arrow batches
> end to end, parallel endpoints, pushdown), not through a JDBC/ODBC layer, wherever the consumer allows it - as done for
> StarRocks (native catalog instead of the JDBC bridge), Trino and Spark. JDBC/ODBC stay supported for tools that only
> speak them. For P5 this means ClickHouse's native `arrowFlight` path, not its JDBC bridge.

P1. **ORC tables in the Hive catalog.** *(Done 2026-10-28; opt-in `hive.orcTables`.)* Today the Hive plugin reads only Parquet and Iceberg tables and skips ORC; most
    production Hive tables are ORC. DuckDB has no usable ORC reader (the community `orc` extension reads local files
    only, no pushdown, no 1.5.6 build). Build a Java reader: Apache `orc-core` (with `hive-storage-api`) reading from HDFS
    (Kerberos, HA nameservice) and S3 through the existing storage setup, converting stripes to Arrow and handing them to
    DuckDB as an Arrow stream / registered scan (projection, predicate pushdown via ORC search arguments, stripe
    statistics, partition columns from the Hive partition spec). Opt-in per catalog first; refuse clearly (no silent
    skip) for ORC ACID tables until P6. Tests: ORC files written in the test (all Hive types incl. decimal, timestamp,
    date, char/varchar, binary, list/map/struct), answers equal to the same data in Parquet; partitioned table; HDFS
    mini-cluster or the existing CI HDFS container. Check both partial and complete result modes. Perf: SF1 lineitem in
    ORC vs Parquet (note it in the Log).
    *Step 1 done 2026-10-27:* `OrcArrowReader` (hive plugin) - one ORC file -> Arrow batches (an `ArrowReader`, so
    `Data.exportArrayStream` can hand it to DuckDB), read through the store's `ReadableFile` via `ReadableFileSystem`;
    tests `OrcArrowReaderTest` (all types + nulls + nested, by name / by position, projection, SARG skipping).
    Dependencies: orc-core/orc-format `shaded-protobuf` (the test metastore needs protobuf 2.5), hive-storage-api 2.8.1
    (now compile; the test metastore runs on it), Arrow and Hadoop `provided`.
    *Step 2 (a) done 2026-10-27:* descriptor `share` is a list of `hadoop`/`arrow`; `share=arrow` takes
    `org.apache.arrow.*` from the server and refuses plugin jars carrying Arrow classes (core commit 08966aa, test
    `PluginSharingTest`); hive descriptor `share=hadoop,arrow` (df0328d) - `HiveCatalogIntegrationTest` passes with
    the server's Hadoop and Arrow (locally with a UTF-8 locale: its partition path has non-ASCII characters).
    *Step 2 (b) part 1 done 2026-10-27:* SPI 1.4 (6884223): `ReadSpec.ArrowStream(format)`, `ArrowScan(columns,
    predicates)` (columns non-empty - DuckDB refuses a stream without columns, "must have at least one column";
    predicates are hints, the server keeps every filter above the stream), `Catalog.exportArrowScan(snapshot, files,
    scan, streamAddress, Object allocator)` (default refuses; partition columns come as UTF-8 text, the server
    casts). Core e3e22bf: `ArrowStreamScans` (per query: one `ArrowArrayStream` + `registerArrowStream` view per table
    reference, `DROP VIEW` + release on close; test `ArrowStreamScansTest` 5/5: self-join over two streams, partition
    text -> INTEGER, unread and failed streams released with 0 bytes leaked), `SpiCatalog` delegation,
    `CatalogSources.filesSource` refuses ArrowStream snapshots (so complete-mode views skip such tables with a log
    line, partial queries fail clearly). Checked by experiment on duckdb_jdbc 1.5.6.0: `registerArrowStream` =
    `CREATE OR REPLACE TEMP VIEW` over a one-shot stream - a second read (second statement, second reference,
    even a re-prepare) fails "This stream has been released"; a view over it can be read once. duckdb_jdbc 1.5.6
    also has Java table functions (`DuckDBTableFunction`), considered and rejected: projection pushdown only, no
    filter pushdown, per-value JNI writes, no LIST/MAP/STRUCT/BLOB.
    *Step 2 (b) part 2 (1) done 2026-10-28:* partial mode reads ArrowStream snapshots (core commit, STATUS.md Log):
    `ExecutionService.streamArrowScan` (one `ArrowStreamScans` per endpoint call, closed in `finally`),
    `DuckDbAdapter.streamSqlOnce` (prepared per run, never cached, parse check not remembered, no changed-file
    retry), `catalog/ArrowScanRequests` (columns = declared columns named anywhere in the endpoint SQL, all for a
    star, the first for none; predicates = simple conditions of the WHERE's top-level AND with literals of the
    column's exact declared type - BIGINT/INTEGER/SMALLINT/TINYINT -> Long, DECIMAL -> BigDecimal, DOUBLE ->
    Double, VARCHAR, BOOLEAN, DATE, TIMESTAMP; FLOAT and others never), `ExecutionService.shipsPartitionValues`
    (planner + `ticketFiles`; partition pruning applies), describe -> `CatalogViews.declaredColumnsSource`. Tests
    `ArrowScanRequestsTest` 6/6, `ArrowStreamExecutionTest` 5/5 (whole snapshot, same text twice, count(*), group
    by partition, planner tickets -> endpoint read, outside-root refusal, describe reads nothing, failing catalog
    leaves no view/buffer).
    *Step 2 (b) part 2 (2) done 2026-10-28:* complete mode reads ArrowStream snapshots (core 53c4d71, STATUS.md
    Log): such tables get no view (marker in `CatalogViews.refresh`); `CatalogViews.arrowStreams(sql, scans)`
    (after `timeTravel`) replaces each schema-qualified reference in a FROM position (`services/TableReferences`
    on `TimeTravelClause`'s tokens: after FROM, JOIN or a comma of a FROM list; three-part names, table functions,
    literals, comments, column qualifiers left alone) by its own stream source; columns from the whole client SQL
    (all for `*`, `COLUMNS(...)`, or a whole-row use of the alias/name); predicates only when the SQL is one
    SELECT of that table alone (`singleSelect` + `ParquetQueryParser`); describe -> `declaredColumnsSource`;
    `DuckDbAdapter.streamCompleteOnce` (views applied, uncached prepare, parameters bound). Known limits: an
    unqualified name (`FROM orders`) or a reference after an ON condition's comma is not replaced and fails as a
    missing table (never an empty answer). Tests `ArrowStreamCompleteTest` 7/7, `TableReferencesTest` 4/4.
    *Step 2 (c) done 2026-10-28 (2e4d2d9):* `hive.orcTables` (default false; ORC tables were and stay skipped
    without it) -> ORC tables (`HiveTable.Kind.ORC`, serde/input format simple name containing `orc`) are shown
    with `ReadSpec.ArrowStream("orc")`; their schema IDs hash with a `hive-orc|` prefix (Parquet IDs unchanged);
    `pinnedColumns` keeps the read spec. `HiveCatalog.exportArrowScan` -> `OrcTableReader` (files one after the
    other, each opened only when the previous is done, through `Stores.locate` + `DataStore.open`, refusing files
    outside `hive.roots`; partition columns as UTF-8 text; context class loader set in `loadNextBatch` /
    `closeReadSource`; errors name the file). Hints -> `OrcSearchArguments` (integers -> LONG, DOUBLE, DECIMAL,
    STRING/VARCHAR, BOOLEAN, DATE via `java.sql.Date.valueOf`; CHAR, FLOAT, TIMESTAMP, nested, NULL literals and
    NaN left out). Found on the way: ORC evaluates a SARG leaf only on columns it reads, so a hint on a column
    outside the projection skipped nothing; `OrcArrowReader` now also reads the SARG's columns. Transactional ORC
    tables stay skipped with the log reason `transactional (ACID) ORC table: not read yet` (like ACID Parquet
    tables; a client sees no such table, never wrong rows). Tests: `HiveOrcTableTest` 6/6 (metastore; DuckDB reads
    the exported stream = the same 6000 rows computed in DuckDB SQL, all 12 types incl. decimal/date/timestamp/
    char/list/struct/map with NULLs, JVM zone America/Los_Angeles; partitions; partition-only scan; row-group
    skipping; broken / outside file), `OrcSearchArgumentsTest` 2/2.
    *Step 2 (d) done 2026-10-28 (86af8f9):* `HiveOrcIntegrationTest` (flight-server) runs the server with the
    packaged plugin (`plugins/hive`, `share=hadoop,arrow`) and `hive.orcTables=true`: ORC tables `hvorc.flat`
    (2 files) and `hvorc.items` (partitioned by region, a file per compression kind ZLIB/ZSTD/SNAPPY/LZ4/NONE, a
    NULL partition) next to Parquet tables of the same rows; whole-table fingerprints = DuckDB computing the rows;
    8 queries (star, count, group by partition, nested columns, filters on BIGINT/VARCHAR/DECIMAL/DATE, IS NULL,
    empty result) equal to Parquet in both modes (schema and rows); partition pruning (1 endpoint); joins ORC x
    Parquet, ORC x ORC, ORC self-join (complete). zstd-jni risk checked: the server's own `Zstd` (server class
    path) works before and after the plugin decodes ZSTD ORC files - no conflict. ORC files are written by
    `OrcTestFiles` (hive test fixture, shared with `HiveOrcTableTest`) in the metastore's class loader; orc-core,
    orc-format, orc-shims, aircompressor, threeten-extra, zstd-jni added to the hive module's `fixture-libs`.
    Pre-release `server-1.0.0-3fbf5b28b186` (run 40627574196, package=true).
    *Step 2 (e) measured 2026-10-28:* `HiveOrcPerfTest` (perf tag, flight-server; manual workflow
    `hive-orc-perf.yml`, inputs rows/files): lineitem-like table (16 columns, `OrcTestFiles.LINEITEM` SQL = the
    Java writer `writeLineitem`; DuckDB `tpch` not needed) as Parquet (DuckDB default snappy) and ORC zlib/zstd,
    4 files each, through the server with the packaged plugin; complete answers of ORC = Parquet (checked every
    run). SF1 (6M rows) on a 4-core runner, run 40631384599 (1f600b9), best of 3, seconds (complete / partial):
    sizes Parquet 136 MB, ORC zlib 69 MB, zstd 59 MB; count(*) 0.01 / 0.01 vs ORC 0.28-0.40 / 0.24-0.27; Q1-style
    0.15 / 0.17 vs 6.6-7.0; Q6-style 0.04 / 0.05 vs 6.6-6.9; `l_orderkey < 50000` (sorted key, SARG skips row
    groups) 0.01 vs 0.06-0.07; `SELECT *` 1.69 / 1.61 vs 7.7-8.1. So ORC works and pushdown helps, but full
    scans are ~45x slower than Parquet for aggregations (Parquet: DuckDB's parallel reader with its own
    pushdown/statistics; ORC: one single-threaded Java stream per table reference, per-value copy into Arrow).
    First fix ca47448: `USE_DECIMAL64` batches (decimals <= 18 digits decoded into longs) - Q1 at 1M rows locally
    2.0 -> 1.4 s, but at SF1 (run 40633767991) only ~5 %: Q1 6.26-6.58, Q6 6.20-6.47, `SELECT *`
    7.3-7.6 s, count(*) 0.37-0.51 - decimals are not the main cost.
    *Step 2 (f) done 2026-10-28:* profiled (JFR via `jcmd` on the surefire fork; the hive-module reader alone
    via an in-test `jdk.jfr.Recording`). Findings and fixes: (1) **the main cost** - `ArrowScanRequests` took any
    `*` for `SELECT *`, so Q1/Q6 (`l_extendedprice * l_discount`) streamed all 16 columns; a star between two
    values is now a product (core d5d5466, regression test `aProductReadsOnlyItsColumns`; stars that may select
    columns - `t.*`, `DISTINCT ON (a) *`, `* EXCLUDE`, next to a comment - still read all); (2) bulk copy of flat
    columns into Arrow (dfad426, `OrcArrowReader.copyFlat`; the per-value `setSafe` was ~60 % of the reader's
    time); (3) files of a scan decoded up to `min(4, cores)` at once, 2 batches ahead each, handed over in file
    order (cc1230c, `OrcTableReader.FileRead`, pool `hive-orc-read-*`; plugin only, no core `ArrowStreamScans`
    change needed). Also: the perf test's JVM ran with the jacoco agent and `-ea` (~40 % slower Java path, not
    Parquet's) - `hive-orc-perf.yml` now runs without them (9506620). SF1 on a 4-core runner, run 40641173349,
    best of 3, seconds (complete / partial), Parquet vs ORC zlib-zstd: count(*) 0.01 vs 0.08-0.27; Q1-style 0.12
    vs 0.72-0.84 (was 6.6); Q6-style 0.03-0.04 vs 0.36-0.45 (was 6.6); `orderkey < 50000` 0.01 vs 0.02; `SELECT *`
    1.19-1.25 vs 1.51-1.69 (was 7.7). Batch size (4) not tried.
    *Step 2 (g) done 2026-10-28 (77a8db9, 3a78772):* a file larger than a quarter of `ORC_RANGE_BYTES` (32 MiB,
    so 8 MiB) is planned from its footer (`OrcArrowReader.stripes`, on the stream's thread, once per file) into
    groups of whole stripes of at least `min(32 MiB, size / readAhead)`; each group is a read-ahead unit
    (`Reader.Options.range`, ORC reads the stripes that start in it), handed over in file and stripe order. The
    first range starts at 0 and the last is open-ended, so every stripe is read once whatever the footer or listed
    size say; an unreadable footer leaves the file one range, whose read reports the failure naming the file.
    Tests `OrcTableReaderTest` 6/6 (new: one file of >= 8 stripes, ranges per group, listed size right and short,
    read-ahead 1/3/8; a large broken file). SF1 one file per table, run 40643095396: count(*) 0.07-0.17, Q1-style
    0.73-0.83, Q6-style 0.37-0.46, `orderkey < 50000` 0.02, `SELECT *` 1.51-1.77 s (Parquet 0.01 / 0.24 / 0.08 /
    0.01 / 1.23-1.30) = the 4-file numbers of step 2 (f). Batch size: 4096 / 8192 / 16384 rows tried locally
    (2M rows, 4 files, 4 cores) - differences within run-to-run noise (Q1 0.41-0.53 / 0.45-0.68 / 0.40-0.60 s),
    kept 4096. The rest of the gap to Parquet (~3-10x on aggregations) is one Arrow stream per table reference
    consumed by DuckDB (no parallel scan of a stream) plus ORC decoding; closing it would need several streams per
    table reference (a union of streams, core `ArrowStreamScans`) - not planned; revisit if production queries
    need it. **P1 closed.**
P2. **Login for generic clients: LDAP/AD and JWT (OIDC/Knox) auth providers.** JDBC/ODBC/ADBC and BI tools can send only
    user/password or a bearer token; with Ranger on, today they need Kerberos. Add auth providers (plugin modules behind
    the existing auth SPI): LDAP bind (ldaps, StartTLS, user DN template or search, group lookup), JWT verification
    (JWKS URL with cache, issuer/audience, user and groups claims). The resulting identity must flow to Ranger checks
    exactly like a Kerberos principal. Tests: OpenLDAP container and a local JWKS in CI; negative cases (bad password,
    expired token, wrong audience). Document in START-NOTES. Wire behaviour: new providers only, existing ones unchanged.
    *Step 1 done 2026-10-28 (LDAP login):* module `flight-auth-ldap` (after `flight-server`, which it takes as
    `provided` and imports as BOM for gRPC/Netty versions; runs on the server class path, `auth/ldap/*`, not in the
    plugin loader - the auth SPI is `FlightAuthenticationProviderFactory` via ServiceLoader). `FLIGHT_AUTH_PROVIDER=
    ldap`: Basic user:password -> UnboundID LDAP SDK 7.0.3 simple bind (DN template, or search with optional service
    account + `FLIGHT_LDAP_USER_ATTRIBUTE`), ldaps / StartTLS with host name verification, clear text only with
    `FLIGHT_LDAP_ALLOW_PLAINTEXT`, servers tried in order; reply carries a signed session bearer (`LdapSessionToken`,
    HMAC like the Kerberos sessions, own `ldap1` version tag, `FLIGHT_LDAP_SESSION_SECRET` shared by nodes,
    `FLIGHT_LDAP_SESSION_MINUTES` default 10); outage -> UNAVAILABLE, bad credentials -> UNAUTHENTICATED; login
    names restricted to `[\p{L}\p{N}._@-]{1,256}`, empty passwords refused. Identity = login name (or attribute) ->
    Ranger via `createRemoteUser(name).getShortUserName()` (= the name) and Hadoop group mapping. Tests (25, unit,
    in-process UnboundID `InMemoryDirectoryServer` with keytool-made certs instead of an OpenLDAP container):
    `LdapDirectoryTest` 11 (template/StartTLS, search/LDAPS, ambiguous, AD-style filter, anonymous search, bad
    service account, failover + unreachable, untrusted cert, wrong host name with a positive control by IP, server
    without StartTLS, plaintext opt-in), `LdapFlightLoginTest` 3 (real TLS FlightServer via `FlightSecurity.
    fromSettings`: `authenticateBasicToken` -> bearer -> peer identity; bad/empty password, forged bearer, no
    credential; TLS required, no mixing with `FLIGHT_BEARER_TOKEN`), `LdapSettingsTest` 5, `LdapSessionTokenTest` 3,
    `LdapFlightAuthenticatorTest` 3. Docs: `docs/security/providers.md` (settings table), START-NOTES; release asset
    `flight-auth-<v>-<sha>.tar.zst` (dev-ci `package`).
    *Step 2 done 2026-10-28 (groups into Ranger):* core 6af7b87 (own commit, STATUS.md Log line):
    `FlightAuthenticationProvider.groups(identity)` (default `Set.of()`); `RangerAuthorization.union(provider,
    mapping)` = provider groups + Hadoop group mapping (decided: union; a throwing provider makes the check deny,
    `RangerAuthorization` already fails closed on runtime exceptions); wired via `SecurityProviders.authorization(env,
    groups)` and a new 5-arg `ServerModule` constructor fed by `flightSecurity.authentication()::groups` (4-arg kept).
    Plugin e70d80b: opt-in `FLIGHT_LDAP_GROUP_SEARCH_BASE`, `_FILTER` (default `(|(member={1})(uniqueMember={1})
    (memberUid={0}))`, `{0}` identity, `{1}` user DN, both escaped; filter validated at start),
    `FLIGHT_LDAP_GROUP_NAME_ATTRIBUTE` (default `cn`); `FLIGHT_LDAP_BIND_DN` now also allowed in template mode when a
    group search is set. Groups read at the login (service account if set, else as the user), cached per identity
    for the session lifetime (`LdapGroupCache`, max 10000 entries); a bearer on a node without the entry triggers one
    lookup (identity -> DN: template, or search by filter / `FLIGHT_LDAP_USER_ATTRIBUTE`; service account or
    anonymous) in the authenticator, failure -> UNAVAILABLE "LDAP group lookup failed"; > 1000 groups, no single
    entry, missing base -> failure, never fewer groups. Tests: module 35 (`LdapDirectoryTest` 15 incl. groupOfNames /
    groupOfUniqueNames / posixGroup (test schema extended with RFC 2307 posixGroup), custom filter + attribute,
    failures; `LdapGroupCacheTest` 3; `LdapFlightAuthenticatorTest` 4 incl. other-node lookup and outage;
    `LdapSettingsTest` 6; `LdapFlightLoginTest` 4 incl. groups through `FlightSecurity` + `union`), core
    `RangerAuthorizationTest` 6 (union, provider failure denies, LDAP name `alice` = Kerberos `alice@EXAMPLE.COM`
    decision). Docs: `docs/security/providers.md`, START-NOTES, CHANGELOG.
    *Step 3 done 2026-10-28 (JWT, 1b3a7bb):* module `flight-auth-jwt` (same layout as `flight-auth-ldap`: server
    `provided` + BOM import, `auth/jwt/*` on the server class path, dev-ci `package` adds `auth/jwt/` to the
    `flight-auth-...tar.zst` asset). `FLIGHT_AUTH_PROVIDER=jwt`: every call's `Bearer` JWT is verified with
    nimbus-jose-jwt 10.10 (only runtime jar; its deps are optional) - `JwtVerifier`: key set from
    `FLIGHT_JWT_JWKS_URL` (https only, `JWKSourceBuilder` cache `FLIGHT_JWT_JWKS_CACHE_MINUTES` default 15,
    re-fetch on unknown `kid` rate limited by `FLIGHT_JWT_JWKS_REFRESH_LIMIT_SECONDS` default 30, own trust store,
    512 KiB limit) or `FLIGHT_JWT_JWKS_FILE` (public parts only); `typ` absent/`JWT`/`at+jwt`; algorithms RS/PS/ES
    256-512 only (`FLIGHT_JWT_ALGORITHMS` subset; HS*/none/EdDSA refused at start); required `iss` (exact
    `FLIGHT_JWT_ISSUER`), `aud` (one of `FLIGHT_JWT_AUDIENCE`), `exp`, user claim; skew
    `FLIGHT_JWT_CLOCK_SKEW_SECONDS` default 60. Identity = `FLIGHT_JWT_USER_CLAIM` (default `sub`; same character
    rule as LDAP; a numeric `sub` is read as text by nimbus, other non-string claims refused). Groups =
    `FLIGHT_JWT_GROUPS_CLAIM` (dotted path, array or one string, > 1000 or other shapes refuse the token) stored per
    identity until that token's expiry (`JwtGroups`, max 10000) -> `groups(identity)`; unseen/expired identity
    throws -> Ranger denies. Known limit (documented): two tokens of one user with different groups at the same
    moment may see each other's groups (never another user's). Opt-in `FLIGHT_JWT_BASIC_PASSWORD`: Basic password =
    token, reply carries it as bearer. Bad token UNAUTHENTICATED, key fetch failure UNAVAILABLE. Tests 16 (unit):
    `JwtVerifierTest` 6 (RS256/ES256/at+jwt, claim choice, audiences; groups shapes/nesting/limits; expiry with
    and beyond skew, nbf, iss, aud, missing exp, bad identities; plain/HS256 with a kid/tampered/unknown kid/wrong
    key under a known kid/`typ` JOSE/garbage/algorithm subset; HTTPS JWKS with trust store, cache = 1 fetch, key
    rotation -> 1 more fetch, old key then refused; untrusted cert and unreachable URL -> unavailable),
    `JwtSettingsTest` 3, `JwtGroupsTest` 3, `JwtFlightLoginTest` 4 (real TLS FlightServer through `FlightSecurity`:
    identity + per-token groups, `RangerAuthorization.union`, Basic password, expired/foreign/forged/missing,
    TLS requirement, no mixing with `FLIGHT_BEARER_TOKEN`). Docs: `docs/security/providers.md`, START-NOTES,
    CHANGELOG. **P2 closed** (optional OpenLDAP container test: see Open questions).
P3. **(moved 2026-10-28 to its own repository - do not change `flight-spark` here; skip this item)** Spark connector for production (`flight-spark`). (a) Kerberos on YARN executors: a Spark
    `HadoopDelegationTokenProvider` that gets a server session on the driver and ships it in the job credentials (today
    every executor does its own SPNEGO and fails without a TGT); (b) build profiles for Spark 3.3/3.4 (check what Hadoop
    3.1.3 clusters usually run) and Spark 4.x (Scala 2.13, JDK 17/21); (c) a proper Spark catalog (namespaces/tables from
    the server's metadata). Look at upstream's `feat/starrocks-native-integration` branch (runtime/bloom filters, AQE
    guard) - port what is a clear win, ticket format changes go to Open questions first. Tests: Spark local mode (existing
    175), a Spark 4 job, a KDC container + keytab job.
P4. **Hive TEXTFILE/CSV, Avro and JSON tables, then simple Hive views.** DuckDB's readers over the existing HDFS/S3
    setup; SerDe properties (field/line delimiters, null format, escape, skip.header), partition columns. Views: plain
    HiveQL views whose text DuckDB can run after a small translation; refuse the rest clearly.
    *Step 1 done 2026-10-28 (c258adf, text tables):* decided to read text in the plugin (Java) through the ORC
    path (`ReadSpec.ArrowStream("text")`, both modes, partition pruning, tickets) instead of DuckDB's `read_csv`:
    Hive's lazy semantics (unreadable field -> NULL, no quoting, escapes, `\N`, positional fields, missing ->
    NULL) cannot be expressed with `read_csv` options without core SQL changes, and no SPI/core change is needed
    this way. Opt-in `hive.textTables` (default false) shows `TextInputFormat` + `LazySimpleSerDe` tables.
    `TextFormat` (serde params, table params override, as Hive merges them): `field.delim` / `serialization.format`
    (byte number or first char; ASCII only), `escape.delim`, `serialization.escape.crlf`,
    `serialization.null.format` (default `\N`), `skip.header.line.count` (per file),
    `serialization.last.column.takes.rest`; refused (table skipped, log reason): `line.delim` other than LF,
    `serialization.encoding` other than UTF-8, `skip.footer.line.count`, `timestamp.formats`, escape = delimiter.
    The format is part of the schema ID (`hive-text|<format>|...`). `TextArrowReader`: lines end at LF/CR/CRLF
    (Hadoop's LineReader), 64 MiB line limit, `.gz` (multi-member) / `.deflate` / `.bz2` (Hadoop BZip2Codec),
    `.snappy/.lz4/.zst/.lzo/.br` refused naming the file. `TextValues`: integers with blanks/sign/fraction
    (dropped), out of range NULL; booleans true/false any case; float/double as Java; decimals HALF_UP to scale,
    NULL above precision; dates `yyyy-M-d` (rest after a blank ignored), timestamps `yyyy-M-d[ |T]H:m[:s[.f]]`
    (micros), impossible days NULL; binary Base64-decoded when all bytes are Base64; text UTF-8 with U+FFFD for
    malformed bytes, VARCHAR/CHAR cut to length, CHAR right-trimmed (as the ORC reader). `TableFilesReader` (was
    `OrcTableReader`) takes a `Format` strategy (ORC: stripe ranges + SARG; text: one range per file, no hints).
    A partition whose own serde properties give another format (or another storage format) fails the query
    naming the partition (Hive reads it with its own; never silently mis-split). Primitive columns only: nested
    columns skip the table. Tests: `TextValuesTest` 4, `TextArrowReaderTest` 6, `HiveTextTableTest` 3
    (metastore: opt-in, DuckDB reads the stream as the declared types incl. a gzip file, partitions with header
    and NULL partition, partition with its own serde refused, footer/nested skipped).
    *Step 2 (a)+(b) done 2026-10-28:* (a) dd9175e `HiveTextIntegrationTest` (server, packaged plugin, real
    metastore): text tables (Hive defaults with a gzip file; partitioned, `,` + header + `''` NULL) answer like
    the same rows in Parquet, both modes, filters, GROUP BY partition, joins text/Parquet, partition pruning (1
    endpoint). (b) acb87cb nested columns: `TextFormat` separators per level (`collection.delim`, else Hive's
    misspelt `colelction.delim`, default 2; `mapkey.delim` default 3; levels 3-7 bytes 4-8; part of the schema
    ID; refused when non-ASCII, CR/LF or equal to the escape); `TextArrowReader` splits recursively as
    `LazyArray` / `LazyMap` / `LazyStruct`: array/struct use their level's separator and children level+1, a
    map entries at its level, key/value at level+1, children level+2; empty field = empty array/map; entry
    without key separator = NULL value; NULL / unreadable keys dropped, first of equal keys kept (DuckDB
    refuses both anyway); struct fields by position, missing NULL, `last.column.takes.rest` also applies to
    nested structs (Hive passes the serde params to every struct). Skipped: unions, non-primitive map keys,
    > 7 levels (Hive's `extend.nesting.levels` not read). Tests: `TextArrowReaderTest` 7 (new nested test),
    `HiveTextTableTest` 4 (DuckDB reads list/map/struct from the stream), `HiveTextIntegrationTest` 4 with
    array/struct/map columns.
    *Step 2 (c) part 1 done 2026-10-28 (OpenCSVSerde):* `CsvLayout` (opencsv 2.3 `CSVReader` per line, the
    serde's constructor choice: 3-arg when escapeChar is the default `"`, so the escape is the backslash);
    `TextFormat.csv` (+ `csv` component, in the schema ID; header lines, footer refused; table params win);
    `HiveTable.csv(sd)`, a partition with the other serde fails the query; non-string columns skip the table
    (Hive-created tables store `string` for all columns). Tests: `TextArrowReaderTest` 8 (opencsv semantics:
    quotes, doubled quotes, escapes dropped before other chars, unclosed quote drops the field, empty line,
    U+FFFD), `HiveTextTableTest` 5, `HiveTextIntegrationTest` 5 (`openCsvSerdeTablesAnswerInBothModes`).
    *Step 2 (c) part 2 done 2026-10-28 (b17866c, JsonSerDe):* read from Hive 3.1.3's own sources (hcatalog
    `JsonSerDe`, serde2 `JsonSerDe`, `HCatSchema`, `Date`/`Timestamp`, `HiveBaseChar`, Java object inspectors):
    both serdes are the same Jackson 1.9 token walk (not the newer `HiveJsonStructReader` of Hive 4). Ported
    one to one in `JsonRows` with the same parser (`jackson-core-asl` 1.9.13, new plugin dependency, no deps) so
    token quirks match: first token must be `{`, keys lower-cased and matched (hcatalog: against lower-cased
    names; serde2: against declared names), unknown keys skipped with their children, later key wins,
    hcatalog `_col<n>` keys (`_col01` fails, out of range fails; serde2 fails on them); ints via Jackson
    (`1.9` -> 1, tinyint 200 -> -56, quoted number / bool / object fail), boolean = `Boolean.valueOf(getText)`,
    string = `getText` of any token (an object gives `{` and its keys then count for the enclosing object -
    reproduced), binary UTF-8 bytes, date/timestamp Hive's LENIENT formatters copied (`2020-2-30` ->
    03-01, a date-only timestamp FAILS in Hive 3.1.3 - reproduced), decimal `HiveDecimal.create` +
    `enforcePrecisionScale` (storage-api), varchar/char cut by code points, char stripped; map keys via
    `Integer.valueOf` & co. Hive fails the query on any serde exception, so the plugin fails the read
    naming file and line (also for unprojected columns - the serde is eager). Arrow-only failures: NULL map
    key (unreadable decimal key), duplicate keys after conversion (binary keys, sub-microsecond timestamps),
    dates/timestamps out of range. `HiveTable.json(sd)` by full serde class (OpenX's `JsonSerDe` stays
    skipped), `TextFormat.json` (+ `json` component in schema ID; header lines; footer and any
    `timestamp.formats` skip the table), partitions with the other JSON serde fail the query. Tests:
    `TextArrowReaderTest` 11 (+3: every type and quirk, 23 failing lines, formats/types),
    `HiveTextTableTest` 6 (+1 metastore: hcatalog + serde2 + gzip + NULL partition, OpenX/formats skipped,
    partition with other serde), `HiveTextIntegrationTest` 6 (+1: DuckDB `to_json` rows with `S`/`_col0`/
    unknown keys = Parquet in both modes, fingerprint, join on struct/map).
    *Step 2 (c) part 3 done 2026-10-28 (deb0a99, Avro tables):* opt-in `hive.avroTables` (default false) shows
    `AvroSerDe` + `AvroContainerInputFormat` tables, read in the plugin (`ReadSpec.ArrowStream("avro")`, both modes,
    pruning) - DuckDB's `avro` extension not used: Hive's semantics (schema resolution per file, decimal scale from
    the file's schema, writer time zone) need the Java reader, and no SPI/core change is needed this way. Ported
    from Hive 3.1.3's sources (`AvroSerDe`, `AvroSerdeUtils.determineSchemaOrThrowException`, `SchemaToTypeInfo`,
    `TypeInfoToSchema`, `AvroDeserializer`, `AvroGenericRecordReader`). `AvroFormat`: reader schema =
    `avro.schema.literal` (table params over serde params, `none` = unset) else the columns' schema
    (`TypeInfoToSchema`: `baseRecord`, fields `[null, T]` default null, inner records `record_<n>`, tinyint/
    smallint -> int); its Hive types (`SchemaToTypeInfo`: `[null, T]` -> T, enum -> string, fixed -> binary,
    bytes+decimal -> decimal(p,s), int+date, long+timestamp-millis, string+char/varchar(maxLength), records
    structs, maps `map<string,..>`) must equal the metastore columns (position, name any case, type), else the
    table is skipped with a log reason (also: `avro.schema.url`, other unions, null type, recursive records -
    a record reused outside itself is fine, bad decimal/maxLength props). Schema ID `hive-avro|<schema>|...`.
    `AvroArrowReader` (Avro 1.12.0 `DataFileReader` + `GenericDatumReader.setExpected(table schema)` over the
    store's `ReadableFile` through a 1 MiB window): fields by position; strings/enums text; bytes/fixed binary;
    decimal = unscaled bytes at the scale of the file's own field schema (Hive's `fileSchema`), then
    `enforcePrecisionScale` to the column (NULL when too wide); char/varchar cut to the file schema's maxLength
    then the column's (char stripped); date days; timestamp millis UTC -> local time of the file's
    `writer.time.zone` (Hive 3.1.3 writes it; a file without it fails on its first non-NULL timestamp, see Open
    questions); a decimal/char/varchar field missing in the file schema fails (Hive does); map key text, HashMap
    order as Hive. An empty (0-byte) file has no rows. Found on the way: Avro's `DataFileStream.hasNext` returns
    false on any EOF, so a file cut inside a block was read as shorter - the reader checks
    `previousSync() == length` at the end and fails otherwise. A partition of another storage format, or (unless a
    table property names the schema) whose serde literal / columns give another schema (`Schema.equals`, docs
    ignored), fails the query naming the partition. Codecs null/deflate/snappy (snappy-java 1.1.10.7)/bzip2/
    zstandard. Dependencies: avro 1.12.0 (jackson excluded), Jackson 2.17.2 now compile (was test-scoped 2.12.0
    for the test metastore; it runs on 2.17.2), snappy-java. Tests: `AvroFormatTest` 4, `AvroArrowReaderTest` 5
    (every type + NULLs, projections incl. empty, resolution: int->long, dropped/added/defaulted fields, decimal
    at the file's scale 3 -> (10,2) rounding and NULL above precision, varchar at the file's maxLength, required
    field without default, decimal without scale; writer time zones UTC/Tokyo/none/bad; 5 codecs x 5000 rows in
    many blocks; empty, not-Avro, cut by 20/10/2 bytes), `HiveAvroTableTest` 3 (metastore: opt-in, literal and
    column tables, tinyint/url/stale skipped, DuckDB reads all 17 types, partitions with NULL partition,
    partition-only scan, partition with another schema refused).
    End to end (cdc0889, test only): `HiveAvroIntegrationTest` (server, packaged plugin, real metastore): `hvavro.flat`
    (schema as table literal; deflate + uncompressed files) and `hvavro.items` (columns' schema, partitioned by
    region with a NULL partition, zstandard) answer like the same rows in Parquet in both modes (schema and rows;
    star, count, GROUP BY partition, nested columns, filters), fingerprints = DuckDB computing the rows, partition
    pruning (1 endpoint), joins Avro x Parquet / Avro x Avro / self-join. Files written by `AvroTestFiles` (hive
    test fixture) in the metastore class loader; avro, commons-compress, commons-io added to `fixture-libs`.
    *Step 2 (c) part 4 done 2026-10-28 (8d95ce8, `avro.schema.url`):* the schema file is read through the server's
    stores when it lies under `hive.roots` (4 MiB at most), kept per location and read again when its size or
    modification time changes; HTTP URLs and files elsewhere keep the table skipped with a log reason; a literal still
    wins, as in Hive. Tests in `AvroFormatTest`, `HiveAvroTableTest`.
    *Step 2 (c) views, part 1 done 2026-10-28 (translator, not wired):* `HiveViewSql.translate(viewExpandedText,
    declared columns, database -> server schema)` turns a simple view into DuckDB SQL
    `SELECT CAST("_v0" AS <declared>) AS "<col>", ... FROM (<translated>) AS "_view"("_v0", ...)` plus the list of
    (database, table) it reads. Whitelist only: backtick identifiers -> double-quoted; tables only as `db`.`t` right
    after FROM / JOIN / a comma of a FROM list (anything else there - string literal, table function, VALUES, unknown
    or hidden database, unqualified name - refused: DuckDB would read files or table functions named in FROM), or a
    parenthesised subquery starting with SELECT; keywords SELECT DISTINCT ALL FROM WHERE GROUP BY HAVING AS AND OR NOT
    IS NULL TRUE FALSE IN BETWEEN CASE WHEN THEN ELSE END JOIN INNER LEFT RIGHT FULL OUTER CROSS ON UNION LIKE;
    operators `= <> != == < > <= >= + - ||`, `*` only in `count(*)`; LIKE only with a literal pattern, written with
    `ESCAPE '\'` (Hive's escape); functions count sum min max coalesce nvl lower/lcase upper/ucase abs year month
    day/dayofmonth; string literals '...' and "..." with escapes `\' \" \\ \n \t \r \% \_`; integer literals only.
    Refused (reason in the message): casts (Hive truncates double -> int, DuckDB rounds; text formats of doubles
    differ), `/ % div` (result types differ), `*` (decimal scale rules differ), decimal/suffixed literals (literal
    types differ), `[]` (0- vs 1-based), ORDER BY / LIMIT (NULL order differs), WITH, LATERAL VIEW, windows, other
    functions and keywords, adjacent string literals, `;`, comments left open. Tests: `HiveViewSqlTest` 3 (DuckDB runs
    the translations: filter + LIKE escape, grouping with declared types, LEFT JOIN + UNION ALL + FROM subquery + IN /
    BETWEEN, CASE / coalesce / nvl / `||` / `==` / comma join; 30+ refusals; FROM-position safety; quoting of
    identifiers with quotes and backticks).
    *Step 2 (c) views, part 2 done 2026-10-28 (5ea70fe SPI, 8410789 hive):* SPI 1.6 (own commit, additive):
    `Catalog.views(schema)` default `List.of()`, record `CatalogView(name, sql, sources, columns, fingerprint)`
    (sources/columns non-empty, copied). Hive (descriptor `spi=1.6`): opt-in `hive.views` (default false); with it
    `VIRTUAL_VIEW` tables are collected in `tablesOf` (same metastore pass, same names TTL) instead of being logged
    as skipped; `MATERIALIZED_VIEW` stays skipped (its stored rows may differ from its text). `views(schema)`
    translates each with `HiveViewSql` (schema function = prefix + database when that database is shown) and shows
    it only when every source is in `tables(schema)` or is itself a shown view (followed recursively, at most 16
    deep, a view on the current path refused = cycles); declared columns -> `ColumnSpec(name, HiveTypes.duckDb,
    nullable, no field ID)` (unsupported types skip); fingerprint = SHA-256 of `hive-view|<sql>` (first 16 bytes).
    Skips are logged once (`hive=viewSkipped view= reason=`). The server does not call `views` yet: no client-visible
    change. Tests: `HiveViewCatalogTest` 3 (metastore: off -> none; prefix `h_` -> sources/columns/fingerprints,
    the SQL runs in DuckDB over `h_hvw.people` and over the inner view; cross-database view, cast, ORC source
    without `orcTables` (shown with it), missing table, view over a skipped view, two-view cycle, union column type,
    database outside `hive.databases`).
    *Step 2 (c) views, parts 3 + 4 done 2026-10-28 (core 74cc484, 4ca22c3; test c62e0bb):* `SpiCatalog.views`
    delegates; `CatalogTables.views` drops views with invalid names or a source in another catalog.
    `CatalogViews.catalogViews(sql)` -> `Expanded(sql, sources)`: a schema-qualified view in a FROM position
    (`TableReferences`) becomes `(<view sql>) AS "<name>"` (alias kept), repeated for views over views (at most 16
    rounds, then refused); `FOR VERSION/TIMESTAMP AS OF` on a view refused; SQL without a reference in a catalog
    schema returns at once without listing views. The view map is its own 60 s cache read from the catalogs only
    (no DuckDB work, so partial queries cannot fail on a view refresh): a view named like a table of its schema is
    left out (table wins), as is one whose SQL names (in a FROM position) a table it does not declare as a source
    (keeps the Ranger check honest); a failing catalog keeps its views. Expansion runs before `timeTravel` in the
    three complete calls of `ExecutionService`. `FlightSqlProducer.requireQuery(principal, query)` (all 7 call
    sites): `authorization.requireQuery` as before plus `canReadTable` on every source of every expanded view
    (invoker rights, see Open questions); the ticket path runs the very text it checked (no second map read) and
    skips the distributed merge for views. Partial requests: `completeAnswerReason` = "the query reads a catalog
    view" -> one complete endpoint (same path as other queries endpoints cannot answer in parts). GetTables: views
    as `VIEW` when no type or `VIEW` is asked, only if the caller may read the view and its sources, schema from
    the complete describe; GetTableTypes adds `VIEW` when a catalog shows views. Tests: `CatalogViewQueryTest` 5
    (views over an Arrow-stream table: joins/subqueries/comma lists/view over view, sources listed, idempotent,
    describe, table wins, undeclared source hidden, time travel/cycle refused), `FlightSqlProducerTest` +4 (source
    denied -> UNAUTHORIZED before describe, time travel -> INVALID_ARGUMENT, ticket runs the checked text, partial ->
    complete plan, listing with type filters and denied source), `MetadataUtilsTest` +1, `HiveViewIntegrationTest` 4
    (packaged plugin, real metastore: views over ORC/Parquet/text, ORC x text join, view over view, both modes,
    declared types, GetTables/GetTableTypes, time travel refused). Not done: a Ranger end-to-end test of a denied
    view source (unit-tested in the producer only).
    *Step 2 (d) done 2026-10-28 (4690319, 5152e7f, 8216228, 80aa61a):* (1) large Avro and uncompressed text files
    are split like ORC files (inside the stream, not into endpoints - one Arrow stream per table reference stays):
    a file over 8 MiB is planned into equal byte ranges of `min(32 MiB, size / readAhead)`, decoded up to 4 at once
    and handed over in order; an Avro range reads the blocks whose preceding sync marker starts in it
    (`DataFileReader.sync` + `pastSync`, Hadoop's rule), a text range the lines that start in it (a range after
    the first starts a byte early and drops its first, partial line; `\n`, `\r`, `\r\n` ends). Compressed text
    (by suffix) and tables with `skip.header.line.count` stay one range. Tests: every split point and even range
    sizes give the whole file's rows once and in order (`TextArrowReaderTest`, `AvroArrowReaderTest`, incl. a cut
    file noticed by the last range), `SplitFilesReaderTest` through `TableFilesReader` (read-ahead 1/3/8, range
    sizes, open counts, gz and header files one range). (2) `HiveOrcPerfTest` / `hive-orc-perf.yml` also write the
    lineitem-like table as text (\001, DuckDB `COPY`) and deflate Avro (Java rows) and check their complete
    answers equal Parquet's. (3) Profile-driven speedups: Avro decodes only projected top-level fields (others
    skipped through the resolving decoder, so unresolvable files still fail; float/double and nested-record
    fields still read - a skip cannot widen promotions or follow a nested writer order; the new projection test
    caught a first version that also skipped fields inside nested records); text fast paths for plain decimals
    <= 18 digits (unscaled long), `yyyy-mm-dd` dates, all-ASCII strings (equivalence tests against the general
    parsers, 20k inputs each). SF1, 4 files per table, 4-core runner, best of 3, seconds (complete mode),
    Parquet / ORC zstd / text / Avro: run 40774271172 (before (3)): count(*) 0.01 / 0.22 / 2.80 / 4.59; Q1-style
    0.15 / 0.88 / 8.43 / 10.03; Q6-style 0.04 / 0.45 / 6.79 / 8.40; `orderkey < 50000` 0.01 / 0.02 / 3.68 / 6.11;
    `SELECT *` 1.55 / 1.95 / 14.82 / 12.22. Run 40776750625 (80aa61a, same runner speed - Parquet equal):
    count(*) 0.01 / 0.19 / 2.70 / 3.44; Q1 0.15 / 0.85 / 4.30 / 9.25; Q6 0.04 / 0.44 / 3.56 / 7.07; orderkey
    0.01 / 0.02 / 2.95 / 5.01; `SELECT *` 1.58 / 2.05 / 7.42 / 12.71 (partial mode within 5 % of complete).
    Sizes: Parquet 136 MB, text 798 MB, Avro deflate 201 MB. So text and Avro are 25-60x Parquet on aggregations
    (no pushdown or statistics, every row parsed): fine for occasional queries, not for heavy BI - converting hot
    tables to Parquet/ORC is the answer. (Run 40775865607 on 8216228 had a faster runner - not comparable.)
    *Optional, only if production needs it:* Avro is now dominated by deflate and Avro's generic decoding (profile
    it like text: JFR through an in-test `jdk.jfr.Recording`, the surefire `argLine` is not passed to the fork);
    text: the rest is line splitting and Arrow `setSafe`. A Ranger end-to-end test of a denied view source is
    still open (unit-tested in the producer only). **P4 closed.**
P5. **Plain Flight / command descriptors (ClickHouse `arrowFlight`, other generic Flight clients).** Accept a SQL text
    in a CMD `FlightDescriptor` (not wrapped in a Flight SQL command) for read queries, same auth and Ranger checks;
    test with a ClickHouse container (`arrowFlight` table function) and DuckDB's `adbc` path. This touches core request
    handling: keep it small and coordinate via `STATUS.md` Log; wire change -> Open questions first, opt-in setting.
    *Step 1 done 2026-10-28 (core 5e9ac98, test a802c41):* opt-in `plainFlightCommands` (`FLIGHT_PLAIN_COMMANDS`,
    default false = such descriptors refused as before; additive, see Open questions). ClickHouse's client read from
    its sources (`StorageArrowFlight`, `ArrowFlightSource`, `ArrowFlightConnection`, master 2026-10): GetSchema, then
    GetFlightInfo and DoGet on every endpoint over its one connection (locations ignored); descriptor
    `Path({dataset})` by default, `Command("SELECT * FROM " + dataset)` with `arrow_flight_request_descriptor_type =
    'command'`; 4 arguments / named collection -> `AuthenticateBasicToken` and the returned bearer on every call.
    Server: `PlainFlightCommands.query(descriptor)` - a command whose bytes parse as a protobuf `Any` with a Flight
    SQL type URL stays Flight SQL (null); otherwise strict UTF-8 SQL text (blank / malformed -> INVALID_ARGUMENT); a
    path = 1-3 plain identifiers (`[A-Za-z_][A-Za-z0-9_]{0,127}`, dotted or as elements) -> `SELECT * FROM a.b`
    (anything else INVALID_ARGUMENT, so a path cannot inject SQL or name a table function). `FlightSqlProducer`
    overrides `getFlightInfo` / `getSchema`: plain query -> `requireQuery` (Ranger + view sources, as Flight SQL
    statements) -> `flightInfoForQuery(..., complete)` with complete unless the client sends
    `x-flight-result-mode: partial` (a generic client concatenates endpoints, so partial answers of an aggregate
    would be wrong rows); tickets are the usual statement tickets (DoGet unchanged). Tests: `PlainFlightCommandsTest`
    3, `FlightSqlProducerTest` +2 (off -> refused without a Ranger call; Ranger denies text and path in both calls
    before describing; complete by default on a `partial` server, parts on request, Flight SQL keeps the default),
    `ConfigAdapterTest` +1, `PlainFlightCommandsIntegrationTest` 5 (Java FlightClient: aggregate -> 1 row on a
    partial-default server, path dotted/elements/SQL = whole table, GetSchema = FlightInfo schema, parts on request,
    refusals, Flight SQL unchanged, off), `GenericFlightClientsIntegrationTest` 2 (pyarrow plain Flight: path,
    SQL text, aggregate; ClickHouse 26.8.36.3 `clickhouse local` in Docker: path and command mode with a WHERE in
    the dataset; answers = DuckDB over the same files, incl. NULLs, DECIMAL, DATE). Dev CI pulls the ClickHouse
    image (failed pull skips that test). Docs: README, `arrowflight.properties`, START-NOTES, CHANGELOG.
    *Step 2 done 2026-10-28 (test only):* `ClickHouseLdapLoginTest` (flight-auth-ldap, integration, needs
    `-Dclickhouse.it.image`): a TLS Flight server through `FlightSecurity` with `FLIGHT_AUTH_PROVIDER=ldap`
    (in-memory directory, keytool certificate with SAN localhost) and a producer that answers with the caller's
    identity; ClickHouse 26.8 `clickhouse local` with a named collection (`use_basic_authentication`, `enable_ssl`,
    `ssl_ca` = the mounted PEM) reads `alice  42`: GetSchema, GetFlightInfo (path `[sales.orders]`) and DoGet all
    arrive as alice on the session bearer from the Basic handshake; a wrong password fails in ClickHouse ("Failed to
    authenticate") before any call reaches the producer. No code change was needed.
    *Next step (3):* DuckDB as a client: (a) the ADBC path is Flight SQL already (ADBC Flight SQL driver is checked by
    `CompressionClientsIntegrationTest`); (b) DuckDB's community `airport` extension speaks plain Flight
    (`airport_take_flight('grpc://host:port', 'SELECT ...')` / a path) - check in CI whether it reads plain
    descriptors (community-extensions.duckdb.org is not reachable from the cloud session, so add it to the Python
    step of Dev CI and a case to `GenericFlightClientsIntegrationTest`, skipped when the extension cannot be
    installed); it may need Airport-specific actions/metadata - if so, note it and do not add them. Then close
    P5. ClickHouse `INSERT` (DoPut) stays unsupported (read-only server; it fails as before).
    *Step 3 done 2026-10-29 (0de9086, test + docs only):* (b) DuckDB `airport` (community build, first DuckDB of
    2.0.0 / 1.5.6 / 1.4.5 with one; Dev CI step "DuckDB airport extension", `-Dairport.it.python`, skipped with a
    warning when none installs): `airport_take_flight` gets the schema of a path (dotted / elements) or SQL-text
    descriptor, but reads rows only through Airport's own `endpoints` DoAction (msgpack parameters), which the
    server does not implement -> the query fails with `Unrecognized request: endpoints`, never with wrong rows.
    `GenericFlightClientsIntegrationTest.duckdbAirportGetsSchemaButNeedsItsEndpointsAction` pins that; README and
    CHANGELOG say airport is unsupported (use DuckDB's ADBC Flight SQL path). Not added (Open questions). **P5
    closed.**
P6. **(Done 2026-10-29)** Ranger Hive-policy reuse with column masking / row filters (catalogs, federation, Delta, ACID moved to lane 4,
    `docs/dev/CATALOGS-STATUS.md`).
    *Plan (2026-10-29):* module `flight-auth-ranger-hive` (server class path `auth/ranger-hive/*`, like the auth
    modules), provider `FLIGHT_AUTHORIZATION_PROVIDER=ranger-hive` evaluating the cluster's **Hive** Ranger service
    (service type `hive`, resources database/table/column, ranger-plugins-common 2.8.0 from the server) for the
    Hive catalog's schemas (server schema = prefix + database). Fail closed everywhere: a table whose mask or row
    filter cannot be written exactly is denied, never read unmasked. Steps: (1) masks -> DuckDB SQL; (2) policy
    evaluator (`_any` for database visibility as Hive's `SHOW DATABASES`, `select` **per column**, row filter
    and mask lookups via `evalRowFilterPolicies` / `evalDataMaskPolicies`), tests with a real `RangerBasePlugin`
    fed with `ServicePolicies` + Ranger's own `ranger-servicedef-hive.json` (no admin); (3) core hooks (own
    commits): `FlightAuthorizationProviderFactory.create(env, providerGroups)` default (custom providers get no
    LDAP/JWT groups today), and a per-table restriction hook - the server must tell the provider the referenced
    columns (a table-level `select` check would let a column-level deny through: Ranger matches a request without
    column only against `*` policies) and their Hive types (CHAR(n) masks work on padded values), and must
    replace each reference by `(SELECT <masks> FROM t WHERE <filter>)` in both modes, views and tickets; (4)
    row filters: Hive expressions through the `HiveViewSql` whitelist (else deny); (5) end-to-end with the hive
    plugin. Wire/security design goes to Open questions before step 3 is merged.
    *Step 1 done 2026-10-29 (a1aa21f):* `HiveMask.parse(maskType, transformer)` + `sql(column, hiveType,
    duckDbType)`, ported from Hive 3.1.3's `GenericUDFMask` / `MaskShowFirstN` / `MaskShowLastN` / `MaskHash` /
    `BaseMaskUDF` sources: MASK_NULL -> typed NULL, MASK_NONE -> column; transformers `mask`,
    `mask_show_first_n`, `mask_show_last_n`, `mask_hash` with `{col}` and literal arguments (Hive's argument
    rules: int literal = code point, string = first char, '' = default, number 0-9 else 1, day 1-31 / month
    0-11 corrections, negative count 0); strings per char by Unicode category (`\p{Lu}`/`\p{Ll}`/`\p{Nd}`, a
    supplementary character = two "other" chars as in Java), show-first/last by position (a value with
    characters outside the BMP that is masked in part fails the query - Hive counts UTF-16 chars), CHAR(n)
    masked padded then shown trimmed; integers by digit arithmetic with Java overflow per width (the minimum
    value computed by `HiveMaskNumbers`, a port of the transformers); dates with fixed/kept parts
    (`mask_show_*_n` on a date gives 0000-01-01 like Hive); `mask_hash` SHA-256 hex of text, NULL for other
    types; other primitive types NULL (Hive's unsupported-type adapter); complex columns, CUSTOM, other
    functions and arguments Hive would refuse -> `IllegalArgumentException` (the provider will deny). Tests
    `HiveMaskTest` 6/6 (all mask types of Ranger's Hive service definition; DuckDB vs Hive's algorithms over
    Unicode strings, CHAR(8), tinyint..bigint incl. MIN/MAX and random values, dates incl. impossible days).
    Not wired: no runtime change.
    *Step 2 done 2026-10-29 (f455b87):* `HivePolicies(RangerBasePlugin, groups)` - read from Ranger 2.8.0's
    `RangerHiveAuthorizer` sources: `canSeeDatabase` / `canSeeTable` = `_any` access on the database / table
    resource (as Hive's SHOW DATABASES / SHOW TABLES filters; a column-level policy inside makes them visible),
    `readTable(principal, HiveTable(database, table, owner), referencedColumns)` -> `TableAccess(allowed,
    rowFilter, masks, reason)`: `select` per referenced column (lower case, as Hive), table-level `select` only
    when no column is referenced (count(*) - needs a policy on column `*`; a column-level deny is not seen by
    it, hence per-column checks); row filter = `evalRowFilterPolicies` on the table (Hive's raw expression,
    translated in step 4); masks = `evalDataMaskPolicies` per referenced column -> `HiveMask` (service
    definition transformer; MASK_NULL; CUSTOM value parsed as a mask function, null value = NULL; any other
    custom expression or a `{colType}` transformer denies the table). `{OWNER}` policies through the table owner
    (`setOwnerUser`), Ranger roles from `getRolesFromUserAndGroups`, user = UGI short name (as the core
    `RangerAuthorization`). Every failure (no identity, group lookup error, policies not loaded, evaluation
    error) denies. `hadoop-common` added as `provided`. Tests `HivePoliciesTest` 5/5 with a real
    `RangerBasePlugin` fed with `ServicePolicies` + `ranger-servicedef-hive.json` (column-level allow, table
    `*`, group policy, column deny, `{OWNER}`, row filter, mask types incl. CUSTOM and MASK_NONE, fail closed).
    Not wired: no runtime change.
    *Step 3 (a) done 2026-10-29 (093d6a8, core, own commit):* `FlightAuthorizationProviderFactory.create(env,
    providerGroups)` default -> `create(env)`; `SecurityProviders` passes the LDAP/JWT groups to custom providers.
    Restriction-hook design written into Open questions ("P6 step 3, security design").
    *Step 3 (b) done 2026-10-29 (d1f0c78, module only):* provider `ranger-hive` registered
    (`RangerHiveAuthorizationProviderFactory`, services file, `create(env, providerGroups)`; dev-ci `package` adds
    `auth/ranger-hive/` (jar only) to `flight-auth-...tar.zst`). Settings: `RANGER_HIVE_SERVICE_NAME`,
    `RANGER_ADMIN_URL` (required), `RANGER_POLICY_REST_USERNAME/_PASSWORD`, `RANGER_POLICY_CACHE_DIR`,
    `RANGER_HIVE_SCHEMA_PREFIX` (schema = prefix + database; other schemas refused). Not named `RANGER_SERVICE_NAME`,
    which core reserves for `ranger`. Groups = provider groups union Hadoop mapping. The server passes no column
    names, so `canReadTable` = `HivePolicies.readWholeTable`: table-level select (column `*`), no row filter, then
    `HiveRestrictivePolicies` (built per policy engine by `HiveRangerPlugin`, a `RangerBasePlugin` subclass that
    lists resource policies of the default and every zone seen in `setPolicies`, and tag policies) finds the
    restrictive policies (deny items, allow exceptions, masks, row filters) whose database/table resources match
    (Ranger's `RangerDefaultPolicyResourceMatcher`, `SELF_OR_DESCENDANT`, also additional resource sets) and
    collects their literal column names; those plus a probe column only `*` policies match (`"\u0000any column"`)
    go through `readTable` - any deny or mask (except MASK_NONE) refuses the table. Refused without evaluation:
    `{OWNER}` or conditions in restrictive items (owner unknown, request has no client context), column patterns /
    `{...}` macros / excludes, any restrictive tag policy (all tables). Note: Hive's mask resources take no
    wildcards (service definition), so Ranger reads `sal*` there literally and so do we. No Ranger audit from
    this provider yet. Tests: `HivePoliciesTest` +2 (7), `RangerHiveAuthorizationTest` 3 (schema mapping,
    `requireQuery` incl. joins, factory via ServiceLoader, settings, union, close).
    *Step 3 (c) partly done 2026-10-29 (848f11a):* (i) Ranger audit: `HivePolicies(plugin, groups, audit)` audits
    each `readWholeTable` decision once as Ranger's result of a table-level `select` (no probe column; a denial
    by a column restriction / mask / row filter / inexact policy is logged denied with its reason and policy id
    -1, `isAudited` forced); `ranger-hive` passes a `RangerDefaultAuditHandler` (destinations from
    `ranger-hive-audit.xml` on the class path); `canReadTable` evaluates the table before `canSeeDatabase` so
    denials are audited; schema visibility not audited (as Hive's SHOW filters). Note: `GetTables` filters
    through `canReadTable`, so listings are audited per table - as the core `ranger` provider does. (ii) zone
    test: `HivePoliciesTest.wholeTableFollowsSecurityZones` (zone `pii` on database `hr`: the zone's column
    deny and mask are found, a default-zone deny on `hr.teams` does not apply inside the zone, as Ranger does;
    checked to fail with zone listing removed).
    *Step 3 (c iii) done 2026-10-29 (0b94507):* `RangerHiveFlightSqlTest` (integration, module test; server test-jar
    and `hadoop-client` as test dependencies) runs `RangerHiveAuthorization` with in-memory policies in a real Flight
    SQL server (`TestFlightServerHelper`, basic login) over DuckDB schemas named like Hive databases: a mask
    (orders), a row filter (payments) and a column deny (customers) refuse the table for that user in plain
    queries, count(*), joins and quoted joins, in partial and complete mode, while a user without the restriction
    is answered; subqueries / CTE / UNION refused; GetTables lists only whole-readable tables; a database without a
    policy is neither readable nor listed. **It found a core authorization bypass** (any provider): tables read only
    inside subqueries or joined with quoted names were never checked - fixed in core commit f411c13 (see
    `STATUS.md` Log, CHANGELOG Security; subqueries now refused under a provider).
    *Step 3 (c iv) done 2026-10-29 (test only):* `RangerHiveCatalogTest` (integration, module test; the server
    test-jar's `HiveTestTables` + `StoreRegistry.open` with the packaged hive plugin and the in-process metastore,
    `hive.orcTables/textTables/views=true`): database `hvranger` with an ORC table (Arrow stream reads), a Parquet
    and a text table, views `v_orc`, `v_pq`, `v_join` (ORC x text). An unrestricted reader gets every table and
    view in full (both modes); a column deny on the ORC table refuses it and `v_orc` / `v_join`; a mask (Parquet)
    and a row filter (text) refuse those tables and `v_pq` / `v_join`, in plain queries, count(*), joins and
    IN-subqueries, both modes; a view granted without its source table is refused (invoker rights, see Open
    questions "P4, views"); GetTables lists only readable tables and views; the database is visible to a user
    with a policy only on a view inside it (`_any`). No code change was needed.
    **Found on the way (core, not fixed here):** a Flight SQL server started second in one JVM answers a
    partial-mode join with quoted names (`... JOIN "sales"."orders" t ...`) with INVALID_ARGUMENT `Catalog Error:
    Table with name "sales.orders" does not exist because schema "sales" does not exist` - reproduced with two
    copies of `RangerHiveFlightSqlTest` (each alone passes; the same query unquoted passes). Process-wide state
    suspected (not `ParquetQueryParser`'s parse cache, its parses are immutable; not `PluginRefusedFunctions`);
    an error, never wrong rows, and one server per JVM in production - but a race at startup is not ruled out.
    The module now runs one JVM per test class (`reuseForks=false`). Noted in `STATUS.md` Log for the core lane.
    *Step 3 (A, B) done 2026-10-29 (52446bad, core, own commit, additive):* `TableRestriction(allowed, rowFilter,
    masks)` (`UNRESTRICTED`, `DENIED`), `FlightAuthorization.restrictsTables()` (default false: nothing changes for
    existing providers) and `restrictTable(principal, schema, table, columns)` (default from `canReadTable`).
    `services/QueryRestrictions`: after view expansion every table of the query (DuckDB's parse, `QueryTables`) is
    asked; each `TableReferences` reference of a restricted table becomes `(SELECT * REPLACE (CAST((<mask>) AS
    <DuckDB type>) AS c, ...) FROM s.t WHERE (<filter>)) AS <alias or t>`; a check rewrite (sources under a
    placeholder schema) must leave no read of the restricted table, else UNAUTHORIZED (missed references, `TABLE
    s.t`, time travel on a restricted table, unknown mask column, provider failure). Producer: `checkQuery` in every
    path; restricted queries are planned and described complete (one endpoint, also when asked in parts); DoGet
    derives the restriction again for the redeeming user and refuses a partial ticket that is now restricted;
    table statistics of a restricted table refused; distributed complete skipped (rewritten text). Columns =
    DuckDB `DESCRIBE` of the table (`ExecutionService.tableColumns` / `DuckDbAdapter.describeColumns`). Tests
    `QueryRestrictionsTest` 4, `TableRestrictionIntegrationTest` (fake provider, real server: masks + filter, plain,
    count, join, quoted self-join, IN-subquery, group by, prepared statement, both modes, unrestricted schema, one
    endpoint, bob's partial ticket refused for alice).
    *Step 3 (C) done 2026-10-29 (19703370):* `ranger-hive` `restrictsTables()`; `restrictTable` =
    `HivePolicies.readColumns` (inexact-policy probe as before + `select` per column of **all** columns + row
    filter + masks, audited once) -> masks via `HiveMask.sql`; `canReadTable` is now Hive's SHOW TABLES visibility
    (any access, not audited) - listings show tables the caller has any access to (views with their sources).
    *Step 3 (D) done 2026-10-29 (8f10de6f):* `HiveRowFilter` - typed recursive-descent translation of the filter's
    Hive expression: columns, string / integer (<= 9 digits) literals, TRUE/FALSE/NULL, comparisons of one kind
    (text, number, date, boolean), IS [NOT] NULL, [NOT] IN, [NOT] BETWEEN, [NOT] LIKE literal (ESCAPE '\\'),
    AND/OR/NOT, lower/upper/lcase/ucase, `current_user()` (UGI short name); anything else (other functions,
    arithmetic, casts, subqueries, qualified names, `||`, mixed-kind comparisons such as date vs string) denies.
    Test `HiveRowFilterTest` (15 filters evaluated in DuckDB, 24 refusals).
    *Hive types (SPI 1.8, 5d9e5c12 SPI own commit; 6479bb38 hive; 0e6bcb10 core; c80d71cf ranger-hive):*
    `ColumnSpec.sourceType` (catalog's own type, 4-arg constructor kept); the hive plugin declares metastore types
    (descriptor `spi=1.8`, schema IDs unchanged); `restrictTable` takes `List<ColumnSpec>` (DESCRIBE + catalog
    type); ranger-hive masks use the Hive type (CHAR(n) padded), and refuses show-last masks on text only when no
    Hive type is known.
    *Step 3 (E) done 2026-10-29:* `RangerHiveFlightSqlTest` 6 (masked orders.ssn in plain / join / nested / CTE,
    both modes, one endpoint, same schema; row filter on payments incl. join and IN-subquery; untranslatable filter
    refuses), `RangerHiveCatalogTest` 6 (hive plugin + metastore: MASK on the ORC table's string = Hive's
    `x`/`n` per char, also through view `v_orc`; MASK_NULL on Parquet and its view; MASK_HASH of a char(4) = SHA-256
    of the padded value; row filter `i >= 900` on the text table and through the join view `v_join`; column deny
    still refuses table and views; listings). Docs `docs/security/providers.md`, START-NOTES, README, CHANGELOG.

PLUGINS LANE DONE (2026-10-29): P6 finished as the maintainer's FINISH MODE asked (masks and row filters wired
into queries, tests, docs); no new items are taken. Open questions below stay for the maintainer.

## Open questions

- **(Q-46, 2026-11-11, plugins lane) negotiated data-transfer QOP / cipher (item 46 (3)).** The server cannot see
  what DataNodes negotiate: the hadoopfs extension (afc8) exposes no setting or function for it. `fix/46` logs what
  the client asks for in the `hdfs=saslDataTransfer` start line (`requestedQop= encryptDataTransfer= cipherSuites=
  negotiated=unknown`, from LIBHDFS3_CONF). Please ask the extension repo (this lane does not touch other repos) for
  e.g. a table function `hdfs_data_transfer_security()` or a read-only setting with the QOP and cipher of the last
  (or first) DataNode connection; the server will then query it once after the first block read and fill
  `negotiated=`.

- *Q-42 DONE 15220a77 (2026-11-10, plugins lane; see Log 10:30Z).*
- **Answer Q-42 (maintainer, 2026-11-10 12:05): yes to both.** (a) cap `balanced`'s per-connection floor and the
  shared-database floor at the budget too (floor 128 MiB; a WARN at start when the budget is below the old 1 GiB
  floor, naming the setting to raise it); (b) prune before `requireReadableHere` when copies are off. Small follow-up
  item in this lane, merge into `main` when green ("Q-42 DONE <sha>").
- **(Q-42, 2026-11-10, plugins lane) other memory limits above the budget, and point queries without copies.**
  (a) Item 42 caps only `speed`'s node-wide limit. `balanced` (one DuckDB per connection) keeps its 1 GiB floor per
  connection and `duckDbSharedDatabase=true` without speed its 1 GiB floor for the node, even when the budget is
  smaller (the screening container 2 CPU / 6 GB / -Xmx3g / direct 2g has a budget of ~100 MiB and keeps 1024 MiB x 2
  connections; a test pins it). Cap those at the budget too (floor 128 MiB)? It changes `balanced` defaults on small
  hosts, so not done without your word. Also speed's private fallback DuckDB (sized as balanced) runs beside the
  shared one. (b) With local copies off (`hdfsLocalCopyMaxBytes=0`) on short-circuit-only SASL nodes a point query is
  still refused by `requireReadableHere` when any file of the table is not held here, although the kept file may be;
  pruning before that check would answer it. Additive in effect (answers instead of refusals); do it?

- **(Q-40a follow-up, 2026-11-10) gizmo wave and the harness's JVM flags.** (B) is on `main` (d295fe36). This lane must
  not touch other repositories, so it could neither read `ci/ab.sh` of the performance repo nor ask there for a
  gizmo wave: please have the coordinator run one on the next kestrel build that contains d295fe36 (a `package=true`
  pre-release - the core lane's or the maintainer's call) and say which -Xmx/direct the harness uses; then this lane
  writes "ITEM 40(a) DONE". *Update 2026-11-10 19:15Z:* that build exists - pre-release `server-1.0.0-2ff221447b5d`
  (core lane, 17:18Z, `main` 2ff22144 contains d295fe36); the gizmo wave can run on it. Two notes on the formula: "heap after warm-up" is the heap in use when speed sizes DuckDB
  (at startup, typically 30-150 MiB, so the 1 GiB floor applies); a re-size after warm-up would need a runtime
  `SET memory_limit` - say if wanted. "Never less than 39(b)'s cgroup bound" is read as: the memory the limit is cut
  from stays the cgroup's / host's (item 39(b)), only the JVM reservation shrinks.

- **Answer Q-40a (maintainer, 2026-11-10 08:20): (C).** Keep the START-NOTES text (A), and do (B): speed reserves
  for the JVM what it can really use, not the full `-Xmx` + direct (e.g. reserve = min(-Xmx, max(1 GiB, measured
  heap after warm-up x 2)) + min(direct, 1 GiB + 25 % of direct), never less than item 39(b)'s cgroup bound), with
  `DuckDbSizingTest` cases and the memory-pressure check in a `MemoryMax` scope (heap filled to 90 %, DuckDB at its
  limit, no OOM kill). The harness runs servers with the JVM defaults of its container (check `ci/ab.sh` in the
  performance repo); after the change ask there for a gizmo wave, then "ITEM 40(a) DONE".
- **(Q-40a, item 40(a), 2026-11-10) speed's DuckDB memory vs the JVM reservation.** On S3 the warm TPC-H passes
  are only as fast as GizmoSQL when DuckDB's limit holds the external file cache (see the 40(a) note in Next steps:
  75.8 s -> 15.9 s with a 1g heap / 1g direct in an 8 GB container, plain DuckDB 16.9 s). Options: (A) no code change:
  START-NOTES and the harness give speed a small `-Xmx` / `MaxDirectMemorySize` (or set `duckDbMemoryLimit`);
  (B) speed counts only part of `-Xmx`/direct as reserved (e.g. half), raising the default limit - risk: the RSS
  overshoot of item 39(b) if the heap really fills; (C) both. Which -Xmx/direct does the harness run speed with?
  Plugins lane suggests (A) now, (B) only after a memory-pressure check. Waiting for the maintainer.

- (P2, optional) JWT: only one issuer per server and keys as JWKS (URL or file); Knox without a JWKS endpoint would
  need a PEM public key setting, and several issuers (e.g. Keycloak + Entra) a list - add if production needs them.
  An OpenLDAP container test in CI (`real-services`) for the LDAP provider - only if wanted (UnboundID in-memory
  directory covers the protocol paths today).

- (P4, OpenCSVSerde) tables whose metastore columns are typed (`int`, ...) are skipped: Hive reads every column of
  the serde as string, so the plugin would have to announce strings for declared ints (Trino refuses such tables,
  Hive-created ones store `string`). Read them as strings instead, or cast like Spark does? Skipped until decided.

- (P4, JsonSerDe) an empty line in a JSON table fails the query: Hive 3.1.3's serde fails on it at a file's
  start and otherwise repeats the previous row (it re-parses the reused line buffer - a Hive bug). Skip empty
  lines instead (no row), if production files have them?

- (P4, Avro) timestamps of Avro files without `writer.time.zone` metadata (written by Hive before 3.1 or by other
  tools) fail the query: Hive 3.1.3 shows them in the time zone of the HiveServer2 that reads them (or UTC with
  `hive.avro.timestamp.skip.conversion=true`), which the plugin cannot know. Add a setting (e.g.
  `hive.avroTimestampZone`, default unset = fail) if production has such files?

- (P4, views) Ranger semantics for Hive views: Hive (and Ranger's Hive plugin) let a user who may select from a view
  read it without rights on the tables it reads (definer rights). The server (since part 3, 74cc484) requires the
  view **and** every table and view it reads, for queries and for GetTables (invoker rights - stricter, never more
  access than today). Switch to Hive's semantics
  (view only) if production relies on views to hide tables?

- (P5, decided as additive - tell if not) `plainFlightCommands` accepts descriptors that today are refused
  (INVALID_ARGUMENT), only when the setting is on; Flight SQL commands are recognised by their protobuf type URL and
  unchanged. Plain answers default to complete regardless of `resultMode`, since generic clients concatenate
  endpoints. Should a path also accept quoted / non-ASCII table names (today plain identifiers only)?

- (P5, optional) DuckDB's `airport` extension needs Airport-specific DoActions (`endpoints`, msgpack-encoded,
  maybe more for catalogs) to read rows; not added per P5 (DuckDB users have the ADBC Flight SQL path). Add an
  opt-in Airport compatibility layer if DuckDB users want `airport_take_flight` / `ATTACH ... (TYPE AIRPORT)`?

- (P6, implemented 2026-10-29 with the proposal's conservative answers - tell if any should change) (a) the server
  passes **all** columns of a table, so a Ranger column-level deny on any column refuses every query of that table
  (Hive allows queries that avoid the column); passing only the referenced columns would need the server to name
  them reliably (`ArrowScanRequests` does so for streams) - worth doing? (b) the restriction is derived again at
  DoGet for the user redeeming the ticket (stricter than proposed: a policy change between GetFlightInfo and DoGet
  applies; no extra cost beyond the existing per-DoGet check). (c) `ranger-hive` listings now follow Hive's SHOW
  TABLES (any access) instead of "readable whole", and listings are no longer audited (this answers the former
  "P6, audit" question); a listed table can still be refused for a query. (d) row filters outside the translated
  subset refuse the table - most common gap: a date column compared with a string literal (`dt >= '2024-01-01'`),
  which Hive converts differently from DuckDB; add such conversions if production filters use them.

- (maintainer, pending) share of ORC / Parquet / text / ACID tables in production; Spark versions and YARN keytab use;
  Ranger masking/row filters in use; AD/LDAP or Knox for BI logins; BI tools in use; Delta/Hudi/Paimon/Ozone; plan for
  DuckDB 2.0 (released 2026-10-21; community extensions move to it first).

## Log

- 2026-10-28 (maintainer): lane created; P1-P6 from the integration research.
- 2026-10-27 (plugins session): P1 step 1 - ORC file reader to Arrow in the hive plugin (`OrcArrowReader`,
  `ReadableFileSystem`, 5 unit tests); no catalog/runtime change, no core or SPI change. CI: run 40586746160 (9438f59)
  failed in the s3 plugin on a timing test (`S3CacheEndpointTest.aChangedObjectFailsEveryWaiterAndIsReadAgainOnce`, 3
  GETs instead of 2; passes locally 3/3; its re-run was cancelled by the core lane's push); run 40587941135 (6ca7352, on
  top of 9438f59): all plugin modules green incl. hive (OrcArrowReaderTest 5/5), server red only on `LibsSetTest`
  (commons-io dropped from the libs set by the core lane's `arrow-compression` dependency in 6ca7352, checked locally;
  noted in STATUS.md Log). Next session: confirm a green run once the core lane fixes LibsSetTest, then step 2.
- 2026-10-27 (plugins session): P1 step 1 confirmed green (run 40589347178 on 85aaf46). P1 step 2 (a): `share=arrow`
  in plugin descriptors (core commit 08966aa, additive; noted in STATUS.md Log) and `share=hadoop,arrow` for the hive
  plugin (df0328d); tests `PluginSharingTest` 3/3, `PluginLoadingTest` 6/6, `HiveCatalogIntegrationTest` 6/6 (+1
  iceberg skip) locally. CI: run 40593759006 (df0328d) red only on
  `S3CacheEndpointTest.aChangedObjectFailsEveryWaiterAndIsReadAgainOnce` (3 GETs instead of 2, second time after
  40586746160); fix 9c6267c - the s3 read cache claims a missing span's blocks under one lock (requests could split
  a span into two GETs; the failure's stats fit that) + stress test `concurrentMissesOfOneSpanAreReadWithOneGet`.
  Not reproduced locally (even pinned to one CPU), so the cause is suspected, not proven: if the count failure
  recurs, look again (next suspect: a reader arriving between the 412 and the version being forgotten). Run
  40594636655 (9c6267c, all modules) green. No package release: no runtime behaviour visible to
  clients changes (ORC still skipped). Step 2 (b) findings and proposal in Next steps.
- 2026-10-27 (plugins session): P1 step 2 (b) part 1 - SPI 1.4 (6884223: `ReadSpec.ArrowStream`, `ArrowScan`,
  `Catalog.exportArrowScan`) and core e3e22bf (`ArrowStreamScans`, `SpiCatalog` delegation, read_parquet refusal
  of ArrowStream snapshots; FYI line in STATUS.md Log). Local: `SpiTest` 7/7, `ArrowStreamScansTest` 5/5,
  `PluginLoadingTest` 6/6 (its SPI-version expectation now follows `SpiVersion.current()`), Checkstyle + SpotBugs
  clean. CI: run 40603510805 (e3e22bf) cancelled by the core lane's push; run 40603766088 (ba4139a, on top of
  e3e22bf) green, all modules. No package release: no client-visible change.
- 2026-10-28 (plugins session): P1 step 2 (b) part 2 (1) - partial mode wired for ArrowStream snapshots (core
  3f81556: `ArrowScanRequests`, `DuckDbAdapter.streamSqlOnce`, `ExecutionService.streamArrowScan` /
  `shipsPartitionValues`; SPI doc-only beb7349; FYI line in STATUS.md Log). Local: `ArrowScanRequestsTest` 6/6,
  `ArrowStreamExecutionTest` 5/5, `DuckDbAdapterTest` 90, `QueryPlannerTest` 58, `ExecutionServiceTest` 43,
  `HiveCatalogIntegrationTest` 7 (+1 skip), Checkstyle + SpotBugs clean. CI: run 40608463690 (31f16f5) green.
  No package release: no plugin serves ArrowStream yet, nothing client-visible changes. Next: part 2 (2), complete
  mode (see Next steps).
- 2026-10-28 (plugins session): P1 step 2 (b) part 2 (2) - complete mode over ArrowStream snapshots (core 53c4d71:
  `CatalogViews.arrowStreams`, `services/TableReferences`, `DuckDbAdapter.streamCompleteOnce`; FYI line in
  STATUS.md Log; CHANGELOG 9db19b0). Local: `ArrowStreamCompleteTest` 7/7, `TableReferencesTest` 4/4,
  `ArrowStreamExecutionTest` 5/5, `ArrowScanRequestsTest` 6/6, `DuckDbAdapterTest` 90 (+2 skip),
  `ExecutionServiceTest` 43, `QueryPlannerTest` 58, `FlightSqlProducerTest` 27, CatalogViews tests, Checkstyle +
  SpotBugs clean. CI: run 40616190366 (9db19b0, dispatched: the push's head commit was `[skip ci]`) green. No
  package release: no plugin serves ArrowStream yet, nothing client-visible changes. Next: step 2 (c), hive
  `exportArrowScan` + `hive.orcTables`.
- 2026-10-28 (plugins session): P1 step 2 (c) - hive `exportArrowScan` over ORC and `hive.orcTables` opt-in
  (2e4d2d9, plugin only; no SPI or core code change, a comment line in `arrowflight.properties`); fix: SARG columns
  are now read by `OrcArrowReader` (hints on unprojected columns were ignored by ORC). Local: hive module 29 tests
  (`HiveOrcTableTest` 6, `OrcSearchArgumentsTest` 2, `OrcArrowReaderTest` 5, `HiveCatalogTest` 11, Kerberos 5),
  Checkstyle + SpotBugs clean. CI: run 40621078865 (2e4d2d9) green, all modules. No package release yet: planned after the
  end-to-end tests of step 2 (d) (default behaviour unchanged).
- 2026-10-28 (plugins session): P1 step 2 (d) - ORC Hive tables end to end through the server (86af8f9, tests and
  test fixtures only: `HiveOrcIntegrationTest` 5/5, `OrcTestFiles`; hive `fixture-libs` gets orc-core & co.; no
  core, SPI or plugin main code change). Local: `HiveOrcIntegrationTest` 5/5, `HiveCatalogIntegrationTest` 7 (+1
  skip, UTF-8 locale), hive module 29/29 + Checkstyle + SpotBugs. CI: run 40627574196 (3fbf5b2, dispatched with
  package=true; the push's head was `[skip ci]`) green; pre-release `server-1.0.0-3fbf5b28b186` carries the
  `hive.orcTables` opt-in (default off). zstd-jni of server and plugin coexist. Next: P1 perf note (step 2 (e)).
- 2026-10-28 (plugins session): P1 step 2 (e) - perf note. `HiveOrcPerfTest` + workflow `hive-orc-perf.yml`
  (1f600b9, test only; CI run 40631441911 green; SF1 perf run 40631384599: ORC ~45x slower than Parquet on
  Q1/Q6-style aggregations, 4.7x on `SELECT *`, numbers in P1). First speed-up ca47448 (hive plugin:
  `USE_DECIMAL64` row batches, `serialize64` for fitting `HiveDecimalWritable`s; new test
  `readsDecimalsOfEveryWidthAndWidenedScales`; hive module 30/30 + Checkstyle + SpotBugs locally). CI: run
  40633748155 (ca47448) green, SF1 perf re-run 40633767991 green (~5 % gain at SF1, numbers in P1).
  No pre-release: `hive.orcTables` is opt-in and answers are unchanged; one is planned when P1 closes.

- 2026-10-28 (plugins session): P1 step 2 (f) - ORC scan speed. Profiled: Q1/Q6 streamed all 16 columns because
  `ArrowScanRequests` took a multiplication's `*` for `SELECT *` (core fix d5d5466, own commit, FYI line in
  STATUS.md Log); then bulk copy of flat columns into Arrow (dfad426) and per-file read-ahead in `OrcTableReader`
  (cc1230c, up to min(4, cores) files at once, file order kept); `hive-orc-perf.yml` without jacoco/`-ea`
  (9506620). Local: hive module 36/36 (new `OrcTableReaderTest` 4, `OrcArrowReaderTest` 8), `ArrowScanRequestsTest`
  7, ArrowStream* tests, `HiveOrcIntegrationTest` 5/5, `HiveCatalogIntegrationTest` 7 (+1 skip), Checkstyle +
  SpotBugs clean. CI: Dev CI run 40641171065 (9506620, package=true) green, pre-release `server-1.0.0-9506620bb170`
  (ORC opt-in default off; answers unchanged); push run 40640386236 (dfad426) cancelled by the newer push. SF1 perf
  runs 40640401475 (dfad426, still with jacoco: Q1 1.5, Q6 0.8, `SELECT *` 4.0 s) and 40641173349 (9506620:
  Q1 0.72-0.84 vs Parquet 0.12, Q6 0.36-0.45 vs 0.03, `SELECT *` 1.5-1.7 vs 1.2; before 6.6 / 6.6 / 7.7).
  Next: step 2 (g) stripe-range read-ahead for single-file tables, batch size, then close P1.
- 2026-10-28 (plugins session): P1 step 2 (g) - large ORC files read as stripe-group ranges ahead (77a8db9,
  3a78772; hive plugin only, no core or SPI change; CHANGELOG). Local: hive module 38/38 (`OrcTableReaderTest`
  6), Checkstyle + SpotBugs clean; local perf 2M rows (batch size 4096/8192/16384: no difference beyond noise,
  4096 kept). CI: Dev CI run 40643089615 (77a8db9) green; Dev CI run 40644229233 (36b2177, package=true) green,
  pre-release `server-1.0.0-36b2177d51b4` (ORC opt-in default off; answers unchanged); SF1 one-file perf run
  40643095396 (77a8db9) = 4-file speed. **P1 closed**; next session starts P2 (LDAP/JWT auth providers).
- 2026-10-28 (plugins session): P2 step 1 - LDAP / AD login provider, new module `flight-auth-ldap` (857af1d; no
  SPI or core code change; root `pom.xml` module line, dev-ci `package` assembles `flight-auth-<v>-<sha>.tar.zst`;
  CHANGELOG, `docs/security/providers.md`, START-NOTES). Local: module 25/25 (`LdapDirectoryTest` 11,
  `LdapFlightLoginTest` 3, `LdapSettingsTest` 5, `LdapSessionTokenTest` 3, `LdapFlightAuthenticatorTest` 3),
  Checkstyle + SpotBugs clean. CI: Dev CI run 40651658732 (7f308a6, package=true) green, pre-release
  `server-1.0.0-7f308a6cdb4d` with the `flight-auth` asset (opt-in provider; existing auth unchanged). That dispatch
  cancelled the core lane's run 40651083243 (78a2720, same concurrency group); 40651658732 includes 78a2720 and is
  green. Next: step 2 (provider groups into Ranger, small core hook), then step 3 (JWT).
- 2026-10-28 (plugins session): P2 step 2 - provider groups into Ranger. Core 6af7b87 (own commit, FYI line in
  STATUS.md Log: `FlightAuthenticationProvider.groups(identity)`, default none; Ranger groups = provider groups +
  Hadoop group mapping, a failing provider denies) and plugin e70d80b (opt-in `FLIGHT_LDAP_GROUP_SEARCH_BASE` /
  `_FILTER` / `FLIGHT_LDAP_GROUP_NAME_ATTRIBUTE`, `LdapGroupCache`; CHANGELOG, providers.md, START-NOTES). Local:
  flight-auth-ldap 35/35, `RangerAuthorizationTest` 6, `SecurityProvidersTest` 4, `FlightSecurityTest` 3,
  Checkstyle + SpotBugs clean (server and module). CI: Dev CI run 40656572265 (e70d80b, package=true) green,
  pre-release `server-1.0.0-e70d80bb7995` (group search opt-in; existing auth unchanged); push run 40656558867
  cancelled by that dispatch. Next: step 3 (JWT provider).
- 2026-10-28 (plugins session): P2 step 3 - OIDC / Knox JWT provider, new module `flight-auth-jwt` (1b3a7bb; no
  SPI or core change; root `pom.xml` module line, dev-ci `package` loops over `ldap jwt` for the `flight-auth`
  asset; CHANGELOG, `docs/security/providers.md`, START-NOTES). Local: module 16/16 (`JwtVerifierTest` 6,
  `JwtFlightLoginTest` 4, `JwtSettingsTest` 3, `JwtGroupsTest` 3), Checkstyle + SpotBugs clean. CI: Dev CI run
  40665594250 (1b3a7bb, package=true) green (build + package), pre-release `server-1.0.0-1b3a7bbad4bc` (opt-in
  provider; existing auth unchanged); push run 40665576701 cancelled by that dispatch (it also superseded the core
  lane's push run 40665228585 for 4b47b0c, which 40665594250 includes). **P2 closed**; next session starts P3
  (Spark connector: Kerberos delegation tokens on YARN executors first).
- 2026-10-28 (plugins session): P3 skipped (moved to its own repository). P4 step 1 - Hive text tables
  (`LazySimpleSerDe`) read by the hive plugin, opt-in `hive.textTables` (c258adf; plugin only, no SPI or core code
  change, a comment line in `arrowflight.properties`; CHANGELOG). Local: hive module 51 tests (new `TextValuesTest` 4,
  `TextArrowReaderTest` 6, `HiveTextTableTest` 3; `OrcTableReaderTest` 6 on the renamed `TableFilesReader`; UTF-8
  locale), Checkstyle + SpotBugs clean. CI: Dev CI run 40672085389 (c258adf, package=true; it also covers lane 3's
  738d1e7, whose dispatch 40671772031 my push cancelled - same concurrency group) green (build + package), pre-release `server-1.0.0-c258adf27dc9` (text tables opt-in, default off; existing behaviour unchanged). Next: P4 step 2 (a) end-to-end server test, then (b)-(d).
- 2026-10-28 (plugins session): P4 step 2 (a) dd9175e `HiveTextIntegrationTest` (text tables end to end through
  the server vs Parquet, both modes, pruning, joins; test only); (b) acb87cb nested text columns (array/map/struct
  at Hive's level separators, `collection.delim`/`colelction.delim`/`mapkey.delim`; unions, nested map keys, > 7
  levels skipped; text schema IDs change); (c part 1) cdfbe8d OpenCSVSerde tables via opencsv 2.3 (new plugin
  dependency, Hive 3.1.3's version; string columns only, typed ones skipped - Open question). Plugin only, no SPI or
  core code change (comment lines in `arrowflight.properties`); CHANGELOG. Local: hive module 55/55 + Checkstyle +
  SpotBugs, `HiveTextIntegrationTest` 5/5, `HiveOrcIntegrationTest` 5/5, `HiveCatalogIntegrationTest` 7 (+1 skip).
  CI: Dev CI run 40682965605 (177dfe3, package=true) green (build + package), pre-release `server-1.0.0-177dfe3e5716`
  (all behind the `hive.textTables` opt-in, default off). My push runs 40681698511 / 40682819503 and dispatch
  40682828658 were cancelled by me so as not to cancel other lanes' runs in the shared `dev-ci-main` concurrency
  group (lane runs 40681442479, 40682559133 were cancelled anyway; 40682965605 covers their commits). Next: P4 step 2
  (c) JsonSerDe, then Avro, views; (d) perf.
- 2026-10-28 (plugins session): P4 step 2 (c) part 2 - Hive JsonSerDe tables (hcatalog + serde2) under
  `hive.textTables` (b17866c; plugin only + tests, comment lines in `arrowflight.properties`; new plugin dependency
  `jackson-core-asl` 1.9.13 = Hive's parser; CHANGELOG). Local: hive module 59/59 + Checkstyle + SpotBugs,
  `HiveTextIntegrationTest` 6/6. CI: Dev CI run 40689263636 (f73de96, package=true; push head was `[skip ci]`)
  green (build + package), pre-release `server-1.0.0-881bbf73e946` (JSON tables behind the `hive.textTables` opt-in,
  default off). Next: P4 step 2 (c) Avro, then views; (d) perf.
- 2026-10-28 (plugins session): P4 step 2 (c) part 3 - Hive Avro tables under the new opt-in `hive.avroTables`
  (default false; deb0a99 plugin + unit/metastore tests, cdc0889 server end-to-end test; plugin only, no SPI or core
  code change, comment lines in `arrowflight.properties`; new plugin dependencies Avro 1.12.0 + snappy-java, Jackson
  2.17.2 now compile; CHANGELOG). No local build (Maven Central rate-limited the session: HTTP 429). CI: Dev CI run
  40712744776 (2cfa23e, push from the catalogs lane, contains both commits) green (build; all modules SUCCESS). My
  push runs 40710493366 / 40711359997 were superseded in the shared `dev-ci-main` group (and 40710493366 cancelled
  the catalogs lane's dispatch 40710223962 for 411e804 - covered by the green run above). Pre-release
  `server-1.0.0-2cfa23eb62f9` (catalogs lane's dispatch 40715095069, package=true, green; contains both commits;
  Avro tables behind the `hive.avroTables` opt-in, default off). Next: P4 `avro.schema.url`, views, perf.
- 2026-10-28 (maintainer): plugins session 16:18Z ended after 8d95ce8 (avro.schema.url, CI 40725913763 green) without updating this file or releasing LOCK-PLUGINS; lock released by the maintainer. Next session: add the Log line for 8d95ce8 and continue P4.
- 2026-10-28 (plugins session, recorded by the next session): P4 step 2 (c) part 4 - `avro.schema.url` schema files
  read through the stores under `hive.roots` (8d95ce8; plugin only, CHANGELOG). CI: run 40725913763 green.
- 2026-10-28 (plugins session): P4 step 2 (c) views part 1 - `HiveViewSql` translator of simple Hive views to DuckDB
  SQL, whitelist only, not wired into the catalog yet (5011e8d; plugin only, no SPI or core change, no runtime
  behaviour change, so no CHANGELOG line or pre-release). Local (Maven through the GCS mirror of Central, since
  `repo.maven.apache.org` answered 429): `HiveViewSqlTest` 3/3, Checkstyle + SpotBugs clean. CI: Dev CI run
  40737753002 cancelled by a later push; Dev CI run 40737948663 (27d9b32, contains 5011e8d): SPI, all plugin
  modules incl. hive (`HiveViewSqlTest`) SUCCESS; server red on two failures not from this change -
  `PluginLoadingTest.newerSpiVersionsAreRefused` (fa3dc0c bumps the SPI to 1.5, the test's "newer" version is
  presumably 1.5 now; that lane's lock is held, left to it) and `DistributedClusterIntegrationTest.startCluster`
  (Hazelcast port already in use, runner flake). Open question added: Ranger semantics for views (plan: view and its source tables). Next: views part 2
  (SPI 1.6 `Catalog.views` + `hive.views` opt-in), see Next steps.
- 2026-10-28 (plugins session): P4 step 2 (c) views part 2 - SPI 1.6 `Catalog.views` / `CatalogView` (5ea70fe, own
  commit, additive; FYI line in STATUS.md Log) and hive `hive.views` opt-in listing translatable views over shown
  tables (8410789; descriptor `spi=1.6`; comment lines in `arrowflight.properties`; CHANGELOG). No client-visible
  change (the server does not read catalog views yet). Local (GCS mirror of Central): `SpiTest` 9/9, hive module
  79/79 (new `HiveViewCatalogTest` 3; UTF-8 locale), Checkstyle + SpotBugs clean. CI: Dev CI run 40744052381
  (8410789) green (build, all modules). No pre-release (no runtime change). Next: views part 3 (core), detailed in Next steps.
- 2026-10-28 (plugins session): P4 step 2 (c) views parts 3 + 4 - queries of catalog views (core commits 74cc484
  expansion + source access check + partial -> complete, 4ca22c3 GetTables `VIEW` / GetTableTypes; FYI line in
  STATUS.md Log) and `HiveViewIntegrationTest` (c62e0bb, with docs in `arrowflight.properties` and CHANGELOG). Only
  servers with `hive.views=true` see a change. Local (GCS mirror of Central): `CatalogViewQueryTest` 5/5,
  `FlightSqlProducerTest` 31/31, `MetadataUtilsTest`, `HiveViewIntegrationTest` 4/4, Hive Text/Orc/Avro/Catalog
  integration tests green (Catalog needs `LC_ALL=C.UTF-8`), Checkstyle + SpotBugs clean. CI: push runs 40764016829,
  40764736076, 40765524515 cancelled by later pushes; Dev CI run 40766473788 (9044653, contains all three commits)
  green incl. package -> pre-release `server-1.0.0-9044653333cd` (dispatched by another lane; carries this change).
  Next: P4 (d) perf note + splitting large Avro/text files, see Next steps.
- 2026-10-28 (plugins session): P4 step 2 (d) - large Avro/uncompressed text files read as parallel byte ranges
  (4690319), text/Avro tables in the Hive perf test (5152e7f), Avro projection skip (8216228), text fast paths
  (80aa61a); plugin only (plus the perf test/workflow), no SPI or core change; CHANGELOG. Local (GCS mirror of
  Central): hive module 88/88, Checkstyle + SpotBugs clean; `HiveOrcPerfTest` at 400k rows (answers equal Parquet).
  CI: Dev CI push runs 40773505710, 40774236756, 40775370618 cancelled by later pushes; Dev CI run 40776549317
  (80aa61a, contains all four commits) green; perf runs 40774271172, 40775865607, 40776750625 (numbers under P4);
  package run 40778373787 (package=true, 80aa61a) green -> pre-release `server-1.0.0-80aa61a15557`. P4 closed;
  next session: P5.
- 2026-10-28 (plugins session): P5 steps 1 + 2 - plain Flight descriptors (SQL text command, table path) behind the
  new opt-in `plainFlightCommands` (core 5e9ac98, own commit, STATUS.md Log line; Open question noted as additive),
  pyarrow + ClickHouse 26.8 `arrowFlight` end-to-end tests and a ClickHouse image step in Dev CI (a802c41),
  ClickHouse TLS + LDAP Basic login test (7c88ccf, test only). Local (GCS mirror of Central; Docker started in the
  session, image via mirror.gcr.io): `PlainFlightCommandsTest` 3/3, `PlainFlightCommandsIntegrationTest` 5/5,
  `FlightSqlProducerTest` 33/33, `ConfigAdapterTest` 48/48, `GenericFlightClientsIntegrationTest` 2/2,
  flight-auth-ldap 36/36 incl. `ClickHouseLdapLoginTest`; Checkstyle + SpotBugs clean. CI: push runs 40788411531,
  40788854220, 40789242301 cancelled by later pushes; Dev CI run 40789295791 (881bbf7, dispatched by the core lane,
  contains all three commits) green incl. the ClickHouse image step and package -> pre-release
  `server-1.0.0-881bbf73e946`. Next: P5 step 3 (DuckDB `airport` in CI), then close P5.
- 2026-10-29 (plugins session): P5 step 3 - DuckDB `airport` extension against plain descriptors (0de9086, test,
  Dev CI step, README/CHANGELOG; no code change): schema works, reading needs Airport's own `endpoints` action, which
  is not added (Open questions); the test pins the clear refusal. Probe runs on temporary branch
  `probe/airport-plugins` (40792710735 found the `endpoints` action, 40793004644 green); the session could not
  delete that branch (proxy 403) - maintainer: please delete it. Local (GCS mirror of Central): flight-server
  test-compile + Checkstyle clean (the test skips without the extension; airport hosts are blocked in the cloud
  session). CI: Dev CI push run 40793321181 (0de9086) cancelled by a later push; Dev CI run 40794638660 (f082b73,
  contains 0de9086) green incl. the airport step. No pre-release (no runtime change). **P5 closed**; next: P6.
- 2026-10-29 (plugins session): P6 started - plan written under P6 (provider `ranger-hive` on the cluster's Hive
  Ranger service, fail closed; core hooks for referenced columns / Hive types / table replacement still to design in
  Open questions before merging). Step 1: new module `flight-auth-ranger-hive` with `HiveMask` (Ranger Hive mask
  types -> DuckDB expressions equal to Hive 3.1.3's mask UDFs) and `HiveMaskNumbers` (a1aa21f; nothing registered,
  no runtime change, so no CHANGELOG line or pre-release). Local (GCS mirror of Central): `HiveMaskTest` 6/6,
  Checkstyle + SpotBugs clean. CI: Dev CI push runs 40801711288, 40802647736, 40804099018 cancelled by other lanes'
  pushes; Dev CI run 40805065855 (6558c3d, contains a1aa21f) green (build). Next: P6 step 2 (policy evaluator).
- 2026-10-29 (plugins session): P6 step 2 - `HivePolicies` (Ranger Hive service evaluator: visibility by `_any`,
  select per column, row filter, masks via `HiveMask`, fail closed; f455b87, module `flight-auth-ranger-hive` only,
  nothing registered, so no CHANGELOG line or pre-release). Local (Maven Central): `HivePoliciesTest` 5/5,
  `HiveMaskTest` 6/6, Checkstyle + SpotBugs clean. CI: Dev CI run 40812949194 (f455b87) green (build). Next: P6 step 3
  (core hook design into Open questions, then the provider-groups factory default).
- 2026-10-29 (plugins session): P6 step 3 (a) - core 093d6a8 (own commit, FYI line in STATUS.md, CHANGELOG line):
  `FlightAuthorizationProviderFactory.create(env, providerGroups)` default, groups passed to custom providers;
  test `SecurityProvidersTest.passesProviderGroupsToCustomAuthorization`. Local: `SecurityProvidersTest` 5/5,
  Checkstyle + SpotBugs clean. CI: Dev CI run 40816724947 (093d6a8) green. Restriction-hook design (masks / row
  filters by query rewrite, tickets, caches, fail closed) written into Open questions for the maintainer. No
  pre-release (no runtime change for existing providers). Next: P6 step 3 (b) unwired `ranger-hive` provider
  (deny on any restriction) while the design question is open.
- 2026-10-29 (plugins session): P6 step 3 (b) - provider `ranger-hive` registered (d1f0c78, module
  `flight-auth-ranger-hive` + dev-ci packaging + docs `docs/security/providers.md`, START-NOTES, CHANGELOG):
  Hive catalog tables checked against the cluster's Hive Ranger service, any mask / row filter / column deny for the
  caller (or a policy the server cannot evaluate exactly) refuses the table. Additive (new opt-in provider name).
  Local (Maven Central): module tests 16/16 (`HiveMaskTest` 6, `HivePoliciesTest` 7, `RangerHiveAuthorizationTest`
  3), Checkstyle + SpotBugs clean. CI: Dev CI push run 40824308828 (d1f0c78) and my
  package dispatch 40824332344 (560b1ed) cancelled by other lanes' pushes; the core lane's package dispatch
  40824399521 (7867822, contains d1f0c78) green (build + package); pre-release `server-1.0.0-786782219b1a` carries
  `auth/ranger-hive/` (jar + SHA256SUMS, checked). Next: P6 step 3 (c), see Next steps.
- 2026-10-29 (plugins session): P6 step 3 (c) (i)+(ii) (848f11a): `ranger-hive` audits each table decision to
  Ranger (one table-level `select` result per decision, denials by column restrictions with reason and policy id
  -1; `canReadTable` evaluates the table first so denials are audited) and security zones are covered by a test
  (zone deny/mask found, default-zone policies not applied inside a zone). Docs `docs/security/providers.md`,
  CHANGELOG. Local: module tests 18/18 (`HivePoliciesTest` 9), Checkstyle + SpotBugs clean. CI: Dev CI push run
  40828739290 (848f11a) green; package dispatch 40830166120 cancelled by the core lane's push, re-dispatched after
  its run 40830188389 finished: 40831727414 (85a7908) green, pre-release `server-1.0.0-85a790826156`. New Open
  question (P6, audit of GetTables listings). Next: P6 step 3 (c iii) end-to-end, see Next steps.
- 2026-10-29 (plugins session): P6 step 3 (c iii) - end-to-end test `RangerHiveFlightSqlTest` (0b94507, 4 tests). It
  exposed a core authorization bypass for every provider (subquery tables and quoted join tables never checked;
  denied rows returned in complete mode, scalar subqueries in both); fixed in its own core commit f411c13
  (`FlightAuthorization.requireQuery` + `ParquetQueryParser.checkedTables` / `TableReferences.nestedQuery`,
  regression test `FlightAuthorizationSubqueryTest`; CHANGELOG Security; `STATUS.md` Log; maintainer notified).
  Local: module tests 22/22; flight-server authorization tests (incl. `SharedDatabaseAuthorizationIntegrationTest`,
  `FlightSqlProducerTest`) green; Checkstyle + SpotBugs clean. CI: Dev CI push run 40842467785 (0b94507) green; package dispatch 40844289066 cancelled by later pushes.
  New Open question (subquery refusal). Next: hive plugin variant of the end-to-end test.
- 2026-10-29 (plugins session, end): the subquery refusal of f411c13 was replaced by the core lane in b3bbcce (tables of
  nested queries named from DuckDB's `json_serialize_sql` and checked; `RangerHiveFlightSqlTest` adjusted there), so
  that Open question is gone; its CI / pre-release is the core lane's run 40845151877. Lock released.
- 2026-10-29 (maintainer): lane 4 (catalogs) starts full ACID ORC tables in the hive module (CATALOGS-STATUS Q6) - new classes, small commits; plugins lane please rebase often.
- 2026-10-29 (plugins session): P6 step 3 (c iv) - `RangerHiveCatalogTest` (46ef1517, test only: hive plugin +
  metastore, ORC/Parquet/text tables and views, column deny / mask / row filter refuse tables and their views, invoker
  rights, listings, both modes). Found: a second Flight SQL server in one JVM fails a partial-mode quoted-name join
  ("schema does not exist") - module now forks per test class; FYI in `STATUS.md` Log for the core lane. Local:
  module `verify` + Checkstyle + SpotBugs green, 26/26 tests. CI: Dev CI runs on 46ef1517 and the lanes' later
  commits (40847672478, 40847783421, 40848268191, 40848841146) were all cancelled by newer pushes; run 40849110613
  (ecbd3c7d, contains 46ef1517) was still running at session end - **next session: confirm it green first.**
  Maintainer's FINISH MODE read as go-ahead for the restriction hook; plan (A)-(E) in P6 Next steps. Lock released.
- 2026-10-29 (plugins session, FINISH MODE): P6 finished - core restriction hook (52446bad, 0e6bcb10; additive
  `restrictsTables` / `restrictTable` -> `TableRestriction`, `QueryRestrictions` rewrite, restricted queries
  answered complete and re-derived at DoGet), SPI 1.8 `ColumnSpec.sourceType` (5d9e5c12; hive plugin 6479bb38),
  ranger-hive masks (19703370, c80d71cf; Hive types, CHAR(n) padded) and row filters (8f10de6f, typed
  `HiveRowFilter`); `ranger-hive` listings now Hive SHOW semantics, not audited. Earlier run 40849110613 was
  cancelled; its content is covered by the green runs below. Local: flight-server unit suite 864/864, ranger-hive
  module 34/34, hive plugin catalog tests, Checkstyle + SpotBugs clean. CI: Dev CI push run 40858061955 (c80d71cf)
  green; package dispatch 40860291863 (146e6757) green, pre-release `server-1.0.0-146e6757a01f`. Open questions
  updated (P6 conservative answers; former "P6, audit" answered). PLUGINS LANE DONE. Lock released.
- 2026-11-02 (plugins session): SERVICE MATRIX step 1 - `service-security.yml` with LDAP (OpenLDAP) and JWT
  (Keycloak) cells x balanced/speed, Flight TLS, TPC-H SF1 22/22 vs DuckDB + metadata + prepared query
  (`tools/ci/service-check.py`, `service-tpch-gen.py`; CI only, no code change). Runs 41468664530 (push, 90da3772)
  and 41469307322 (dispatch, pre-release server-1.0.0-fca6512a8046): 4/4 cells, 28/28 checks each - no bug found
  for either lane. Next: hive cell, then ranger cell (plan in Next steps). Not yet "SERVICE MATRIX plugins DONE".
  Lock still held until Dev CI ends.
- 2026-11-02 (plugins session): SERVICE MATRIX step 2 - cell `hive` in `service-security.yml`: real Hive 3.1.3
  writes ORC and Avro TPC-H SF1 tables itself (Parquet/text external tables over DuckDB files) plus a view over all
  four formats; the server with the packaged hive plugin answers 22/22 + metadata + prepared per format and the view
  = DuckDB (104/104 checks, balanced and speed). `service-check.py` `--schema`/`--view`; new
  `tools/ci/service-hive-data.py`, `service-hive.sh`. Runs 41477683286 (push, 3297d041) and 41480086139 (dispatch,
  server-1.0.0-fca6512a8046) 6/6 cells. No bug for either lane. Found for step 3: Ranger admin images are amd64 only
  from 2.7.0; 2.7.0 admin + ranger-db works without Solr, REST recipe in Next steps. FYI lane 4:
  `tools/ci/hive-mm-writer.sh` waits for the metastore on the host port, which docker's proxy accepts before the
  metastore listens (fixed here by probing inside the container). Not yet "SERVICE MATRIX plugins DONE". Lock released.
- 2026-11-02 (plugins session): SERVICE MATRIX step 3 - cell `ranger` (Ranger admin 2.7.0 + PostgreSQL, Hive tables,
  OpenLDAP; mask, row filter, unpoliced database; answers by DuckDB under the same restrictions):
  `tools/ci/service-ranger.sh`, `service-ranger-data.py`, `service-check.py --extra`. Runs 41483268562 (d733dc7a,
  cancelled by the next push after the ranger cells failed), 41484542642 (496ef3c7), 41486142813 (629c36f7):
  ldap/jwt cells green; ranger 35/59 in both profiles. Two bugs found: (1) core - the table check under any
  authorization provider refuses comma joins, FROM subqueries and `?` parameters (written bug-first into STATUS.md
  Next steps); (2) this lane - `ranger-hive` refuses every table while the Hive service has a tag service (Ranger's
  default `EXPIRES_ON` deny), next item. The core lane fixed (1) in 48090377 within the session; its dispatch run
  41488081427: 8/8 cells green, ranger 59/59 in both profiles. Only (2) remains before "SERVICE MATRIX plugins
  DONE" (the cell still deletes the tag policy). Lock released.
- 2026-11-02 (plugins session): service matrix step 4 - `ranger-hive` tag policies (bug found by the ranger cell):
  restrictive tag policies refuse only tables carrying their tags (Ranger's tag enricher), unknown tags still refuse
  all; `HiveTagPoliciesTest` 4/4; ranger cell keeps Ranger's default `EXPIRES_ON` policy and checks an expired tag
  (3f71355e; cell setup fixes 201df3b2, 1ea39a30 - runs 41493587087 and 41494830118 failed in setup on Ranger's tag
  REST, no server bug). Run 41496378967: 8/8 cells green, ranger 63/63. SERVICE MATRIX plugins DONE. Dev CI run 41497330129
  (f5579621, includes these commits): green. `package=true` pre-release dispatched on 7fa1c246: run 41500117177
  (its service gate runs every service-*.yml; next session: check it published `server-1.0.0-7fa1c246...`).
  Lock released.
- 2026-11-09 (plugins lane): ITEM 39(b) DONE 39f84617 (fix by the core session on `main` before the split; no
  `fix/39b-memory` branch needed). Checked against the stand's description: the per-connection limit and speed's
  node-wide shared limit (`DuckDbSizing.compute` / `shared`) both take the cgroup-derived memory minus max heap, max
  direct memory and the headroom (48 GB scope, -Xmx8g, 8g direct -> about 30 GB for DuckDB instead of ~738 GB); every
  server process, dc nodes included, sizes itself through `HostLimits.detect()` (ServerModule / ConfigAdapter), so dc
  is covered. Dev CI run 42620776092 (39f84617): green, incl. step "Server in a memory-limited systemd scope (item 39
  (b))". No `package=true` (the core lane packages when all four parts are on `main`). Lock released.
- 2026-11-09 13:20Z (plugins lane): nothing open (ITEM 39(b) DONE, SERVICE MATRIX plugins DONE, FINISH MODE). The
  11-02 follow-up is answered: package run 41500117177 (7fa1c246) was cancelled and published nothing, but later
  pre-releases contain those commits (latest `server-1.0.0-b69660b9260a`, 2026-11-07, has 7fa1c246 as an
  ancestor). No new release needed. Lock not taken (docs only).
- 2026-11-10 (plugins lane): ITEM 40(a) step 1 - measurement. `tpch-perf.yml` (branch `fix/40a-s3`, now on `main`:
  c0fe458f, 3ba03976, 72740e75; tooling only, no server/plugin code): input `plain` (plain in-process DuckDB 1.5.6
  as the GizmoSQL reference + settings dumps), `more` ('|'-separated configurations, answers checked against the
  default's), `quick` (skip threads4). Runs (S3 SF10, adbc): 42700198697 (previous session; speed 63.3 s vs plain
  12.3 s), 42748064489 (red only on the label bug fixed in 72740e75; speed 75.8 s, `duckDbMemoryLimit=5120MiB` 19.4 s,
  plain 20.3 s), 42751636105 green (speed -Xmx1g/direct 1g 15.9 s, plain 16.9 s, same answers). Cause: DuckDB memory
  limit bounds the external file cache; decision Q-40a open. Force-push of the rebased branch was refused by the
  session's permission check, so the branch got a merge of `main` instead. Lock released.
- 2026-11-10 (plugins lane): ITEM 40(a) step 2 - START-NOTES "Sizing `speed` for S3" (option (A), docs only) +
  CHANGELOG line; settings-diff side issue found already fixed (run 42751636105 prints the diffs; nothing S3-path
  specific besides the memory limit). Q-40a still open; no CI run needed (docs only). Lock released.
- 2026-11-10 06:45Z (plugins lane): Q-40a still unanswered; dispatched the (B) memory-pressure check `tpch-perf.yml`
  run 42760596461 (S3 SF10, 4 concurrent clients, speed at default / 4608 / 5120 MiB DuckDB limit in an 8 GB
  container); not finished at session end, result to be read next session. No code change. Lock released.
- 2026-11-10 07:50Z (plugins lane): ITEM 40(a) step 3 - Q-40a (C) implemented on `fix/40a-s3`, cherry-picked to `main`
  (d295fe36 code + tests + CHANGELOG, 59ddcd60 START-NOTES). `HostLimits` gains `usedHeapBytes` (4-arg constructor
  kept; start log `usedHeapMiB=`), `ResourceProfile.speedReservation` (tests in `ResourceProfileTest`, new case
  `speedReservesWhatTheJvmUsesNotItsLimits`; SMALL/MEDIUM/LARGE/egret expectations raised: 3174/24780/209715/2662 MiB).
  Dev CI step "speed's JVM reservation under memory pressure" (`tools/ci/HeapFillLauncher.java`: server in a 6G
  `MemoryMax` scope, -Xmx3g / direct 1g, heap filled to 90 % after start, 100 M-group aggregate). Run **42775184825**
  green: limit 3174 MiB (was 2048), heap 2765/3072 MiB, answer exact in 3.9 s, scope peak 5.73 of 6 GiB, oom_kill 0;
  caveat: the aggregate spilled only 1 MiB, so DuckDB was near but not proven at its limit. tpch-perf run 42760596461
  still running (Measure). Gizmo wave not requested (other repository) - see Open questions. Lock released.
- 2026-11-10 08:00Z (plugins lane): Dev CI dispatch on `main` 37bf9401 run **42777385100** green. tpch-perf run
  **42760596461** (S3 SF10, 8 GB container, -Xmx3g/direct 2g, 4 concurrent Java clients, success = answers agree,
  no client lost): balanced best pass 1830 s (peak 3497 MiB); speed default 2048 MiB 1110 s (3669 MiB); speed
  4608 MiB 81.4 s (5725 MiB); speed 5120 MiB 101.3 s (5977 MiB) - no OOM kill, raised limits 13.6x faster under
  concurrency; the new default for this shape (4402 MiB) sits just below the 4608 MiB cell. Lock stays released.
- 2026-11-10 09:05Z (plugins lane): **ITEM 42 DONE db35b57a** (branch `fix/42-sizing-pruning` rebased and merged into
  `main`). (1) `ResourceProfile.speedMemoryBytes` caps the shared DuckDB's default limit at the DuckDB budget
  (`DuckDbSizing.withinBudget`, floor 128 MiB); test `speedNeverSizesDuckDbAboveTheBudgetInSmallContainers` (stand
  shape 16 cores / 3 GiB / -Xmx768m / direct 512m -> 1331 MiB, sweep 1-3 GiB), `smallHostGetsTheMinimumMemoryLimit` now
  1228 MiB (was 1638 MiB + 512 MiB JVM in 2 GiB). (2) `CatalogViews.pruning` / `prunedFiles(sql, pruning)`;
  `completeFromCopies` prunes first (kept files all held here -> pruned SQL, `CopiedQuery.pruned`, no
  `requireReadableHere`; else copies only kept files), `completeInCopyBatches` too; `PrunedLocalCopiesTest`. CI:
  `service-hdfs-cell.sh` stage `pointPruning` (sasl cells: `point.orders_r`, tpch.orders in 8 key ranges, 3 point
  queries complete mode on one server, CatalogViews debug log; requires kept=1 x2 and kept=0 x1;
  `tools/ci/service-hdfs-point-check.py`). Branch runs: Dev CI **42781954321** green, Service hdfs **42781957575** 12/12
  (all six sasl cells: prunedFiles 3, kept=1: 2, kept=0: 1; sasl-cluster balanced via completeFromCopies). On `main`:
  Dev CI 42785951498, Service hdfs 42785951630 dispatched by the push (not awaited). New Q-42. Lock released.
- 2026-11-10 10:30Z (plugins lane): **Q-42 DONE 15220a77** (branch `fix/q42-floors-b`, cherry-picked onto `main`; the first
  branch `fix/q42-floors` is stale - its CI 42789460785 failed only on the golden DuckDB setup that the core lane fixed
  in 7b36459b; a force-push of the rebase was refused, so the rebased commit went to a new branch). (a)
  `DuckDbSizing.compute` (per connection: balanced and speed's private fallback) and `DuckDbSizing.shared` cap their
  1 GiB floor at the budget (`withinBudget`, floor 128 MiB): screening shape 2 CPU / 6 GB / -Xmx3g / direct 2g 1024 ->
  128 MiB per connection (speed's shared limit there stays 2662 MiB, Q-40a reservation); 8 GB / 4g / 2g 1024 -> 819 MiB;
  budgets >= 1 GiB unchanged. Start WARN `duckdb=memoryLimitBelowFloor memoryLimit= floorMiB=1024 hint=` (only for a
  default limit; `ConfigAdapter.duckDbMemoryLimitSet` on the source without profile defaults). Tests:
  `DuckDbSizingTest.smallBudgetsCapTheOneGibFloors` (1-3 GiB sweep, both modes, warning cases) and updated expectations
  in `DuckDbSizingTest`, `ResourceProfileTest`, `ConfigAdapterTest`. (b) `completeFromCopies` with local copies off
  returns a point query pruned (`CopiedQuery.pruned`) when every kept file is held here, so `requireReadableHere` does
  not refuse it; a kept file not held here is refused as before; test
  `PrunedLocalCopiesTest.withoutCopiesAPointQueryOnHeldFilesIsPrunedNotRefused`. CI: Dev CI **42791671758** green on the
  branch; on `main` Dev CI 42794664721 was cancelled by the core lane's next push; Dev CI **42794847349** on a8ded50d (core lane's logging follow-up on top of 15220a77) green. README (shared sizing) and CHANGELOG updated. Lock released.
- 2026-11-10 10:30Z (plugins lane): Q-42 on `main` confirmed. Dev CI 42794664721 (15220a77) was cancelled because a
  newer run superseded it; Dev CI **42794847349** (a8ded50d, contains 15220a77) green. Service hdfs **42794862424**
  12/12 green on attempt 2. On attempt 1 only `ha-krb-sasl` balanced was red, at setup: `hadoop checknative` in the
  image failed before any server started (a CI-setup fault; the same cell was green under speed). Item 42's Service
  hdfs on `main`, 42785951630, is also green. Nothing open in this lane besides the Q-40a follow-up (gizmo wave,
  waits for the maintainer). No code change. Lock released.
- 2026-11-10 19:15Z (plugins lane): nothing open. The build for the Q-40a follow-up exists (`server-1.0.0-2ff221447b5d`,
  contains d295fe36; noted in Open questions); "ITEM 40(a) DONE" waits for the gizmo wave and the harness's JVM flags
  from the maintainer. No code change, no CI run.
- 2026-11-10 21:15Z (plugins lane): **ITEM 44 step 1 - parts (1) and (3)** on `fix/44` (7641ebfd), rebased into `main` as 2335ddc6.
  (1) `DuckDbSizing.compute` (per connection, also speed's private fallback) and `DuckDbSizing.shared` (without speed)
  cap their default limit at `reservedBudget` (memory - item 40(a) reservation - 15 %) instead of the full-limit
  budget, floor `MIN_USABLE_BYTES` 512 MiB (speed's shared limit unchanged, floor 128 MiB); harness shape 2 CPU / 6 GB /
  -Xmx3g / direct 2g: 128 -> 1024 MiB per connection and shared. New start WARN `duckdb=memoryLimitAboveJvmLimits`
  (`aboveJvmLimitsWarning`: default limit x instances above the full-limit budget; not under speed, not for an
  operator's limit). Tests: `DuckDbSizingTest.harnessContainerKeepsOneGibAndWarnsAboutTheJvmLimits`, updated
  expectations in `DuckDbSizingTest`, `ResourceProfileTest`, `ConfigAdapterTest`. (3) `FlightSqlProducer.unavailableOrDenied`
  on the four resolving paths (`qualifyPlainNames`, `qualifyForSchema`, `catalogViews`, `tableColumns`): a storage denial
  is UNAUTHORIZED with `STORAGE_DENIED_MESSAGE` (no HDFS path) and one `query=denied result=denied` WARN; test
  `plainNameRefusedByTheStorageIsDeniedWithoutThePath`. README sizing text updated, CHANGELOG lines. CI: Dev CI
  **42903630231** green on the branch; `tpch-perf.yml` **42903634429** (local SF10, 2 CPU / 6 GB, -Xmx3g / direct 2g,
  baseline 2ff22144, quick, 1 round): fix/44 balanced **22/22**, 42.1 s, `memoryLimit=1024MiB`, peak 1756 MiB;
  baseline q09/q10/q13 DuckDB Out of Memory at 128 MiB both rounds (the stand's failure reproduced) - the run is red
  only because of the baseline's failures. Part (2) (Ranger staleness) open: plan in Next steps. Lock released.
- 2026-11-11 03:40Z (plugins lane): **ITEM 44 DONE e75705c9** - part (2), Ranger revoke staleness. `fix/44` 8ea12800 +
  e75705c9, fast-forwarded into `main`. New optional setting `hdfsAccessRecheckMs` (`arrowflight.hdfs.accessRecheckMs`,
  `HDFS_ACCESS_RECHECK_MS`, default 10000 on `hdfs://`, else 0 = off). `storage.AccessRecheck` remembers each table
  directory's last positive answer (a denial or a NameNode with no answer is not remembered). The server's
  `storage.DataStore` gets the defaults `checksAccess()` / `checkReadable(dir)` (not the SPI); `HadoopDataStore` calls
  `FileSystem.access(dir, READ_EXECUTE)`, which goes through the NameNode permission checker and Ranger's enforcer.
  Only "Permission denied" fails a query; a missing dir or a NameNode that does not answer does not.
  `ParquetAdapter.recheckAccess(schema, table)` covers data-directory tables, not catalog schemas.
  `ExecutionService.recheckAccess(sql)` finds a query's tables with the token finder (`TableReferences`; plain names
  through `CatalogViews.plainOwner`). It does not use DuckDB's parse, which takes one global lock per query.
  `FlightSqlProducer.checkQuery` calls it on every query path; a denial goes through `unavailableOrDenied` and comes
  back as UNAUTHORIZED with `STORAGE_DENIED_MESSAGE` plus a `result=denied` WARN. Tests: `AccessRecheckTest` 4/4,
  `TableReferencesTest.accessRecheckNamesEveryTableOfAQuery`, `FlightSqlProducerTest.revokeSeenByTheAccessRecheckIsDenied`
  and `ConfigAdapterTest.hdfsAccessRecheckDefaultsTo10sOnHdfsOnly`, all run locally.
  Service hdfs, ha-krb cells: a new stage after item 37 (a)'s `restored` does `chmod 700 /data/tpch/nation` while the
  server runs and has just read the table, then requires the denial within 15 s and the table readable again after
  `chmod 755`. CI on the branch: Dev CI **42952000343** green; Service hdfs **42952002696** 12/12 green, with the
  denial after 3.6 s on ha-krb balanced and after 0.0 s on ha-krb-sasl balanced. No baseline run without the
  recheck. README and CHANGELOG updated. Packaging is left to the core lane's morning `package=true` on `main`; noted
  in STATUS.md Log. Lock released.
- 2026-11-11 05:15Z (plugins lane): nothing open. The core lane's pre-release `server-1.0.0-29f1267b995f` contains
  item 44 (e75705c9) and d295fe36: it is the build for the stand's repeat of `ranger-policy ... off` (expected denial
  within Ranger's refresh + 10 s) and for the Q-40a gizmo wave. Dev CI 42955017367 on `main` red only at the service
  gate (explained in STATUS.md Log, repackaged green as 42956935228). No code change, no CI run, lock not taken.
- 2026-11-11 10:15Z (plugins lane): nothing open. egret FRESH 29f1267b995f (run 42965886044, see ORCHESTRATION.md)
  confirms ITEM 44 (1): balanced TPC-H 22/22 on every job, no DuckDB OOM. Still waiting for the maintainer's stand:
  ITEM 44 (2) `ranger-policy ... off` repeat and the Q-40a gizmo wave. No code change, no CI run, lock not taken.
- 2026-11-11 12:20Z (plugins lane): nothing open. Added a state line at the top of Next steps (all maintainer notes
  there are DONE; answers the orchestrator's 11:56Z remark). No code change, no CI run, lock not taken.
- 2026-11-11 15:45Z (plugins lane): **ITEM 46, all three parts on `fix/46`, not merged (CI blocked by the
  runner-wide `*.ubuntu.com` DNS outage).** (1) `DuckDbSizing.compute`: each connection gets at most its share
  (budget / connections) of the budget left by the JVM's real reservation, at least 512 MiB
  (`withinReservedShare`); `reservedBudgetBytes()` is the number in `duckdb=sizing budgetMiB=` (the full-limit one
  is now `limitsBudgetMiB=`) and in the `memoryLimitAboveJvmLimits` WARN, which now fires only when the 512 MiB
  floor exceeds the share. Speed's shared limit is unchanged; speed's private fallback follows balanced as before.
  Stand shape (8 cores, 6 GB, -Xmx3g, direct 2g, 4 connections): 4 x 1024 -> 4 x 665 MiB within 2662 MiB; harness
  (2 CPU): unchanged 2 x 1024 MiB, no WARN. (2) `FlightSqlProducer.getFlightInfoStatement` binds the handle's qid
  while `checkQuery` runs; `logDenial` falls back to an id of its own, never `qid=null`. (3) Start line names the
  requested QOP / encryption / cipher suites, `negotiated=unknown` (Q-46). Tests: `DuckDbSizingTest` 19 (new
  `connectionLimitsAddUpWithinTheBudgetTheLogShows`, `harnessContainerKeepsOneGibWithinTheReservedBudget`),
  `FlightSqlProducerTest` 47 (qid asserts in `plainNameRefusedByTheStorageIsDeniedWithoutThePath`),
  `HdfsSaslDataTransferTest` 7, `ResourceProfileTest` 14, `ConfigAdapterTest` 62, all locally. CI: TPC-H perf
  **43047430684** green (local SF10, 2 CPU / 6 GB, -Xmx3g / direct 2g, quick, 1 round): default `memoryLimit=1024MiB
  connections=2 budgetMiB=2662`, 31.97 s, peak 1722 MiB; `-DduckDbMemoryConnections=4` `memoryLimit=665MiB`, 22/22
  same answers, 31.94 s, peak 1637 MiB. Dev CI 43047425944 (twice), 43048145637 and 43050095655 (15:47Z), Service hdfs 43047453329: red
  before any build/test step, `Could not resolve host: *.ubuntu.com` (curl of the hadoopfs test libs, apt-get
  krb5-user, apt-get in the service images). 5cfbedba adds `mirrors.edge.kernel.org` as a third mirror for the test
  libs (that step then passed); apt-get still needs ubuntu.com. CHANGELOG / README lines are on the branch. Lock
  released.
- 2026-11-11 17:10Z (plugins session): ITEM 46 merged - `fix/46` rebase-merged into `main` (item 46 c96a1272, tip
  74464701). CI was blocked all afternoon because runners could not resolve any `*.ubuntu.com` host; added
  `mirrors.kernel.org` as apt fallback in Dev CI (Kerberos tools step: switch apt sources when azure.archive is
  unresolvable), `tools/ci/server-image.sh` (try 3) and the Service hdfs KDC container (`service-hdfs-cell.sh` apt
  try 3). Green on the branch: Dev CI 43056505119 (c6f08354), Service hdfs 43060538104 (bea11172; 11/12 cells first
  time, `ha-krb-sasl speed` hit one connect failure to kernel.org during KDC setup and was green on the re-run of the
  failed job); TPC-H SF10 43047430684 earlier. Service cluster / security / stores and Catalogs CI also build
  Ubuntu images: if they fail the same way before ubuntu.com resolves again, give them the same kernel.org try
  (not done here: not needed for this item). Lock released.
- 2026-11-11 17:25Z (plugins session): no open item in this lane (waiting on the maintainer: Q-46, Q-40a follow-up,
  stand checks of items 44/46). Post-merge CI on `main` 74464701 (item 46) green: Dev CI 43064709644, Service hdfs
  43064709693, Catalogs CI 43064709784. Lock released.
- 2026-11-12 15:15Z (plugins session): no open item (no new maintainer note since 2026-11-11 18:40; waiting on the
  maintainer: Q-46, Q-40a follow-up, stand checks of items 44 (2) / 46). No code change, no CI run, lock not taken.
- 2026-11-12 18:15Z (plugins session): no open item (no new maintainer note since 2026-11-11 18:40; waiting on the
  maintainer: Q-46, Q-40a follow-up, stand checks of items 44 (2) / 46). No code change, no CI run; lock released.
- 2026-11-13 04:15Z (plugins session): no open item (no new maintainer note since 2026-11-11 18:40; waiting on the
  maintainer: Q-46, Q-40a follow-up, stand checks of items 44 (2) / 46). No code change, no CI run; lock released.
- 2026-11-13 07:15Z (plugins session): no open item (no new maintainer note since 2026-11-11 18:40; waiting on the
  maintainer: Q-46, Q-40a follow-up, stand checks of items 44 (2) / 46). No code change, no CI run; lock released.
- 2026-11-13 11:15Z (plugins session): no open item (no new maintainer note since 2026-11-11 18:40; waiting on the
  maintainer: Q-46, Q-40a follow-up, stand checks of items 44 (2) / 46). No code change, no CI run; lock not taken.
- 2026-11-13 14:55Z (plugins session): no open item (no new maintainer note since 2026-11-11 18:40; waiting on the
  maintainer: Q-46, Q-40a follow-up, stand checks of items 44 (2) / 46). No code change, no CI run; lock not taken.
- 2026-11-13 16:35Z (plugins session): no open item (no new maintainer note since 2026-11-11 18:40; waiting on the
  maintainer: Q-46, Q-40a follow-up, stand checks of items 44 (2) / 46). No code change, no CI run; lock not taken.
- 2026-11-13 17:15Z (plugins session): no open item (no new maintainer note since 2026-11-11 18:40; waiting on the
  maintainer: Q-46, Q-40a follow-up, stand checks of items 44 (2) / 46). No code change, no CI run; lock not taken.
- 2026-11-14 03:15Z (plugins session): no open item (no new maintainer note since 2026-11-11 18:40; waiting on the
  maintainer: Q-46, Q-40a follow-up, stand checks of items 44 (2) / 46). No code change, no CI run; lock not taken.
- 2026-11-14 06:15Z (plugins session): no open item (no new maintainer note since 2026-11-11 18:40; waiting on the
  maintainer: Q-46, Q-40a follow-up, stand checks of items 44 (2) / 46). No code change, no CI run; lock not taken.
- 2026-11-14 08:15Z (plugins session): no open item (no new maintainer note since 2026-11-11 18:40; waiting on the
  maintainer: Q-46, Q-40a follow-up, stand checks of items 44 (2) / 46). No code change, no CI run; lock not taken.
- 2026-11-14 10:15Z (plugins session): no open item (no new maintainer note since 2026-11-11 18:40; waiting on the
  maintainer: Q-46, Q-40a follow-up, stand checks of items 44 (2) / 46). No code change, no CI run; lock not taken.
