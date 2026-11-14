# Lane 4 - catalogs and sources (new plugin modules on `main`)

See `CLAUDE.md`, "Third and fourth lanes". Basis: the maintainer's integration research 2026-10-28 (next wave).

## Next steps

> **Maintainer, 2026-11-11 21:10 - answers.** **Q-40b-1: yes** - turn file pruning off by default for a local
> `dataDir` (keep it on for HDFS / S3 / plugin stores; an explicit setting wins); the performance repo still sees speed
> c1 p50 1.16-1.32x balanced locally, this is the cost you found. **Q10: keep pruning on** for HDFS/S3 (the stand ran
> with it on SASL HDFS: ~860k answers exact, NameNode calls per point query 0). **Q8 (security): yes, fix it** - metadata
> calls (`information_schema`, `duckdb_tables()/columns()`, Flight SQL GetTables) must list only the configured tables
> of an attached database, never the remote catalog's other names. As ITEM 47 in this lane, merge when green,
> "ITEM 47 DONE <sha>".

ITEM 47 DONE 2c169a53 (2026-11-12, session 12; merged with the Q8 fix of session 11's branch). Q-40b-1 DONE
2f31c071 (session 11, Dev CI on main 43077861504 green). Item 47: every DuckDB connection gets temporary macros of
DuckDB's metadata functions (`duckdb_databases/schemas/tables/columns/views/constraints/indexes/sequences/types()`,
`pragma_database_size()`) that leave the plugin-attached databases out, so `information_schema`, `pg_catalog`,
`sqlite_master`, `SHOW ALL TABLES` list only the server's views; while a database is attached, a qualified call
(`system.main.duckdb_tables()`) and the name-string functions `pragma_table_info` / `pragma_storage_info` /
`pragma_show` / `pragma_metadata_info` are refused in client queries (session 12 added `pragma_database_size` and the
name-string refusal: a guessed `pragma_table_info('jdbc_attached_pg.public.x')` passed the qualifier check). Flight SQL
GetTables already listed only configured tables. Tests: `HiddenAttachmentsTest` (3), `JdbcFederationIntegrationTest
.metadataQueriesListNoRemoteNames` against real PostgreSQL / MySQL with an unconfigured remote table. CI on
`fix/47-attached-metadata-2`: Dev CI 43147991919, Catalogs CI 43147994336 green. Next: no open item in this lane
(C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 open). A `package=true` pre-release is
for the core lane's next package (item 47 changes runtime behaviour only for setups with a jdbc attachment).

> **Maintainer, 2026-11-10 11:10 - 40(b) is the priority: merge each finished improvement into `main` as soon as its CI
> is green (do not hold them for one big merge); the build for the real cluster is the core lane's package right after "ITEM 40(b) DONE" - it waits for 40(b).**
>
> **Maintainer, 2026-11-10 13:45 - for 40(b): a measured speed c1 regression to look at first.** Performance repo
> `results/FRESH-894afb9b48c4.md` (same CPU type as `deafc861cce3`'s run): local small queries at 1 client, speed p50
> **1.19x balanced over JDBC (15.4 -> 18.4 ms) and 1.28x over ADBC (10.3 -> 13.2 ms)**, all 3 rounds; deafc had
> 1.10 / 1.08. p95/p99 and 8/32 clients are fine, S3 c1 is 0.53/0.82 - a fixed per-query cost of speed on local data at
> low concurrency, appeared between `deafc861cce3` and `894afb9b48c4` (suspects: item 39 (b) sizing / the shared
> DuckDB's settings, the external file cache on local files (+0.8 ms seen in your profiling), item 40(a)'s reservation).
> Bisect with the harness pairs if cheap (the performance repo runs a wave on request), fix within 40(b).

> **Maintainer, 2026-11-10 10:40 - FIRST in this lane: ITEM 40(b)** (moved here from the core lane, which works on item
> 41). Per-query overhead of small / point queries: on the real cluster point queries are the main gap to upstream
> (there 460 q/s on 16 cores), and GizmoSQL (same DuckDB) takes 0.70-0.85 of speed's time on the small-query mix
> locally (performance repo `results/FRESH-gizmo-deafc861cce3.md`); speed c1 p50 is 1.16-1.18x balanced. Profile one
> point query end to end on the HDFS point cell and locally (JFR / async-profiler: parse + plan + view/describe +
> DuckDB prepare/execute + Arrow conversion + Flight send), find what the Java/Flight path adds beyond DuckDB's own
> time, and cut it (statement/plan caching per SQL text, avoid re-describing, fewer copies, smaller first batch,
> the speed c1 regression). Branch `fix/40b-small`, maintainer exception to the lane scope, merge into `main` when
> green with the point cell + small-query numbers before/after, "ITEM 40(b) DONE <sha>".

ITEM 40(b) DONE 135e6dac (2026-11-10, session 9; main head bc7aa004 = 135e6dac + docs). Before/after numbers on
`main` (no code change this session):
- Small query perf, prepared client (like JDBC), 2-CPU server container, 3 rounds, baseline deafc861cce3, run
  42843856272: c1 p50 ms quota balanced 3.62 / 3.72 / 3.81 -> 3.27 / 3.31 / 3.29, quota speed 3.68 / 3.68 / 3.75 ->
  3.27 / 3.29 / 3.38, pinned balanced 4.12 / 4.20 / 4.24 -> 3.75 / 3.88 / 3.74, pinned speed 4.23 / 4.22 / 4.20 ->
  3.85 / 3.89 / 3.81; q/s c32 quota balanced 388 / 368 / 369 -> 422 / 411 / 414, quota speed 473 / 466 / 453 ->
  521 / 502 / 484, pinned speed 473 / 478 / 474 -> 504 / 507 / 518. **The maintainer's c1 regression is gone and
  c1 p50 is ~10 % below deafc861** in both profiles; server CPU per query equal or lower.
- Same, statement client (GetFlightInfo + DoGet, like ADBC), run 42843860772: c1 p50 quota balanced 2.22 / 2.23 /
  2.23 -> 1.91 / 1.92 / 1.91, quota speed 2.21 / 2.21 / 2.21 -> 1.93 / 1.91 / 1.93, pinned balanced 2.65 / 2.67 /
  2.89 -> 2.27 / 2.30 / 2.36, pinned speed 2.70 / 2.62 / 2.67 -> 2.36 / 2.25 / 2.26 (-13..15 %); q/s c8 quota
  balanced 568 / 571 / 559 -> 660 / 644 / 664, c32 quota speed 673 / 678 / 662 -> 761 / 780 / 757, pinned speed
  697 / 691 / 710 -> 796 / 799 / 792 (+12..15 %).
- HDFS point cell (`hdfs-point-perf.yml`, SF1, point_orders / point_lineitem / count_filter, q/s c1 / c8 / c32),
  before = 894afb9b (item 39 package, branch `perf/bisect-894afb9b`) run 42843869104, after = main run 42843864824,
  0 errors everywhere: before balanced 67.2 / 93.7 / 95.2 and 67.0 / 95.5 / 97.0, speed 66.4 / 105.6 / 113.1 and
  69.0 / 107.8 / 114.0; after, runner a (same speed class as both "before" runners) balanced 69.3 / 97.7 / 96.5,
  speed 71.9 / 111.1 / 119.6; runner b was a faster machine (balanced 83.9 / 117.5 / 114.0, speed 100.9 / 150.5 /
  158.0, not comparable). +2..6 % on the comparable runner; the 2-CPU cell stays DuckDB-CPU-bound. Caveat: the
  "before" workflow pins hadoopfs afc4, "after" afc6 (listing cache + SASL read path; no SASL in this cell).
- JFR (1 ms, `PointQueryOverheadPerfTest`, main): DoGet pruning samples 62 of 2491 (285 of 3446 in session 7), jOOQ
  under DoGet 12 - the `count` texts with decimal literals no longer parse per text (session 8's cut confirmed).
- Not done within 40(b) (follow-ups, not blocking): gizmo vs speed per call on the same box and c32 JFR on the HDFS
  point cell (old step 4) - best measured by the performance repo on the next package; Q-40b-1 (proposed: close
  without a default change).
- **Core lane: the package for the real cluster (maintainer 11:30) can go now.** Throwaway branch `perf/40b-final`
  (perf dispatch ref only, = bc7aa004) may be deleted by the maintainer with the `perf/bisect-*` ones.

ITEM 40(b) state after session 2026-11-10 (8) - pruning shapes for decimal / long integer / date literals (135e6dac):
- Done: next step (b) of session (7). `DescribeKey.pruningShape` (pruning only; `DescribeKey.of` / the describe key
  unchanged) also leaves out integers of 10-18 digits, decimals `[0-9]+.[0-9]+` and the string of `DATE 'yyyy-mm-dd'`
  in the same WHERE positions; kinds are separate key characters (`k = 5` and `k = 5.5` are different shapes).
  `CatalogViews.shapedQuery` parses with stand-ins of the same kind (`3000000001+i`, `3000000001+i.25`, date
  `2999-01-01` + i days); `FileStatsPruner.literal` reads stand-ins via `Map<Object, Object>` binds (a
  `java.sql.Date` key is read as `LocalDate`). jOOQ reads `DATE '...'` as a date value (a mutation test showed the
  cast branch is never hit). Invalid dates (`2021-01-32`) and stand-in clashes fall back to a per-text parse.
  Tests: `FileStatsPrunerTest.shapeParsesKeepWhatEachTextKeeps` +13 shapes (decimals incl. negative, 11-12 digit
  keys, dates incl. invalid ones, clash texts); mutations of the decimal and date binds fail it.
  `DescribeKeyTest.pruningShapesLeaveOutMoreLiterals`.
- Numbers: `small-query-perf.yml` 42835176027 (prepared, c1, 3 rounds, baseline 4228c733, head option
  `-DfileStatsPruningMaxFiles=0`), p50 ms: quota base 4.87 / 4.32 / 4.64, head 5.16 / 4.59 / 4.96, head pruning off
  4.60 / 4.84 / 4.91; pinned base 5.35 / 6.28 / 5.50, head 5.34 / 5.78 / 5.47, off 5.46 / 5.82 / 5.61. Within noise
  (quota head vs base +0.3 ms but equal to pruning off, which does not run this code; pinned head <= base). The
  workflow's mix may not contain decimal filters; the saving is the per-text jOOQ parse of `count`-style queries
  (~100 of the 285 remaining pruning JFR samples in session (7)) - not re-measured with JFR this session.
  Dev CI branch 42835145513 green; Dev CI on main dispatched at session end (merge push was `[skip ci]`).
- Next (in order): (a) check Dev CI on main for 135e6dac; (b) JFR on `PointQueryOverheadPerfTest` (count mix) to
  confirm the `count` texts no longer parse per text (pruning samples should drop from ~285); (c) gizmo vs speed per
  call and c32 JFR on the HDFS point cell (old step 4). Next branch from `origin/main` as `fix/40b-small-8`.

ITEM 40(b) state after session 2026-11-10 (7) - the pruned-vs-view gap on local files cut (4228c733), no default change:
- Where the gap was: not DuckDB. Plain DuckDB JDBC with the server's settings (threads 4/1, `parquet_metadata_cache`,
  external file cache on/off, `allowed_directories` + `enable_external_access=false` + `lock_configuration`) runs the
  pruned text `(SELECT * FROM read_parquet(['one file'])) AS t WHERE k = ?` 0.1-0.2 ms *faster* than the 4-file view.
  JFR (1 ms, `PointQueryOverheadPerfTest`): `CatalogViews.pruning` was 391 of 3446 DoGet samples, ~300 of them a full
  jOOQ parse of a never-seen text (`ParquetQueryParser.parse` via `singleTableQuery`; the key differs each query, so
  the parse cache never hits). With pruning off, complete-mode DoGet parses nothing - that was the ~0.25 ms.
- Cut: `CatalogViews.shapedQuery` parses a point query's shape (`DescribeKey.shape`: the literals the describe key
  leaves out, with their spans) once, with ten-digit stand-in literals `3000000001 + i` (unique in the text, else
  per-text parse as before), cached per shape (512); `FileStatsPruner.keep(..., binds)` reads each stand-in (and its
  negation, jOOQ folds a unary minus) as the text's value. Test `FileStatsPrunerTest.shapeParsesKeepWhatEachTextKeeps`
  (25 shapes x 15-33 values: kept files equal a per-text parse, answers equal the view; a broken bind fails it) and
  `DescribeKeyTest.shapeNamesTheLeftOutLiterals`.
- Numbers: JFR pruning samples 391 -> 285, the rest are the `count` queries with decimal literals (`o_totalprice >
  1234.5`, no shape). `small-query-perf.yml` 42826465868 (prepared, c1, 3 rounds, baseline e62006cc, head options
  `-DfileStatsPruningMaxFiles=0`): quota balanced head 4.08 / 4.00 / 4.70 ms p50 vs head pruning off 4.15 / 4.05 /
  4.10, speed head 4.11 / 4.21 / 4.05; pinned head 5.68 / 5.00 / 4.96 vs off 5.78 / 5.20 / 5.01 (base 4.41-5.04 and
  5.23-5.57, noisy runner). **Pruning on is now level with pruning off on local files.** Dev CI branch 42826441121
  green; Dev CI on main 42828677361 dispatched (merge push was `[skip ci]`).
- Next (in order): (a) done: Dev CI on main 42828677361 green for 4228c733; (b) a pruning shape for decimal / date
  literals too (a separate key: `DescribeKey` must keep decimals in its describe key, DuckDB types them by digits;
  for pruning only the value matters) - the `count_filter` mix uses decimals; (c) gizmo vs speed per call and c32 JFR
  on the HDFS point cell (old step 4). Next branch from `origin/main` as `fix/40b-small-7`. `fix/40b-small-6` on the
  remote is the pre-rebase copy (force-push not used); safe to delete.

ITEM 40(b) state after session 2026-11-10 (6) - the c1 regression's cost found and cut (3a01265e), no default change:
- What the pruned local point query paid (JFR at 1 ms sampling on `PointQueryOverheadPerfTest`, pruning on vs
  `-DfileStatsPruningMaxFiles=0`): not DuckDB's read of the kept file (plain DuckDB: `read_parquet(['one file'])` is
  as fast as the 4-file view) but the server's DoGet: on a node without local copies `completeFromCopies` prunes
  first (Q-42 (b) path, `prunedHere` -> `CatalogViews.prunedFiles`, which skipped `acceptRewrite`), so (1) DuckDB's
  single-query parse check ran in full on the pruned text (~66 of ~800 DoGet samples) and (2) `readComplete` ran the
  pruning again on the pruned / declined text (tokenize, references, footer keep). Fix: `prunedHere` uses
  `ExecutionService.prunedFiles(sql, pruning)` (with `acceptRewrite`), `CopiedQuery.pruningDone` (true when the
  no-copies path pruned or declined the text; a pruning with a kept file this node lacks stays with DoGet as before),
  `readComplete(..., pruningDone)` skips the pruning for an unchanged text. Test `FileStatsPrunerTest
  .doGetUsesThePruningDecidedBeforeIt`.
- Local: complete point DoGet p50 4.17 -> 3.76 ms (3.42 with pruning off). `small-query-perf.yml` run 42818686506
  (prepared client, c1, 3 rounds, baseline deafc861cce3, ms p50 / CPU ms per query): quota balanced base 4.39-4.48 /
  2.17-2.40, head 4.23-4.27 / 2.36-2.51, head pruning off 3.96-4.01 / 2.09-2.27; quota speed base 4.35-4.43 /
  2.25-2.29, head 4.14-4.30 / 2.17-2.56; pinned balanced base 5.12-5.16, head 4.93-5.19, off 4.70-4.82; pinned speed
  base 5.10-5.24, head 4.89-5.06. **c1 p50 is back at or below deafc861 in both profiles**; pruning off is still
  ~0.25 ms faster and ~0.1-0.2 ms CPU cheaper on local files. Dev CI on the branch 42818682745 green; Dev CI on main
  dispatched at session end (the merge push was `[skip ci]`).
- Next (in order): (a) done: Dev CI on main 42822143053 green for 3a01265e;
  (b) the rest of the pruned-vs-view gap on local files: JFR still shows DuckDB execute ~+47 native samples per
  ~800 DoGets with pruning (plain DuckDB does not), and create/GetFlightInfo may still evaluate pruning - look whether
  the shared DuckDB binds `read_parquet([...])` differently from the view (settings: `parquet_metadata_cache`, file
  cache, threads) and whether one pruning per text can serve the whole request; Q-40b-1 stays open as the simpler
  alternative; (c) gizmo vs speed per call and c32 JFR on the HDFS point cell (old step 4). Next branch from
  `origin/main` as `fix/40b-small-6`. JFR at 1 ms: `jfr configure --input profile jdk.ExecutionSample#period=1ms
  jdk.NativeMethodSample#period=1ms --output <dir>/fast.jfc`, then `settings=<dir>/fast.jfc`; `jfr print
  --stack-depth 200 --json` (the default depth 5 hides the server frames).

ITEM 40(b) state after session 2026-11-10 (5) - speed c1 regression found (bisected); two more cuts merged:
- **Cause of the c1 regression (maintainer's 13:45 note): footer-statistics file pruning (item 39 (c), b0f82a47,
  default `fileStatsPruningMaxFiles=1024`) on local data**, in both profiles - not speed-specific, not 39 (b) sizing
  (same with a 6g container limit), not 40 (a) (after 894afb9b). `small-query-perf.yml` (2-CPU container, prepared
  client, 4 files x 100k rows): main vs deafc861 c1 p50 +0.3..0.5 ms, server CPU +0.25..0.45 ms/query (runs
  42806968672, 42806974143 with 6g); bisect (each vs deafc861, same run): 06e3c0e5 / 35d9420a / 53647b1a /
  c71b71d2 / 2429782f = baseline, 894afb9b +0.27..0.47 ms (runs 42810133238 .. 42810174324, 42811189839);
  **main with `-DfileStatsPruningMaxFiles=0` = baseline** (quota 3.21 vs 3.23 ms, pinned 3.87 vs 3.88, CPU 1.39 vs
  1.44 ms/query; with pruning 3.55 / 4.30 ms, CPU 1.67; run 42811208123). On local files a pruned point query pays
  the rewrite + a `read_parquet([...])` bind and saves only cheap page-cache reads; on HDFS/S3 pruning pays off
  (fewer NameNode opens, 39 (c) numbers).
- Proposed fix (needs the maintainer, Q-40b-1 below, it is a default change): prune by default only for stores whose
  file opens are remote (HDFS, S3 / lakeFS, other plugin stores), off for a local `dataDir`; an explicit
  `fileStatsPruningMaxFiles` still wins. Alternatively find what the pruned local query pays (JFR on
  `PointQueryOverheadPerfTest` with 4 local files, pruning on vs off) and cut that instead.
- Merged this session (Dev CI 42808268971 on `fix/40b-small-4` green): a4e14e03 file pruning reads the parser's WHERE
  condition (no second jOOQ `parseCondition`); ac2055c8 describe cache key `DescribeKey` - point queries differing
  only in small integer literals after a comparison in WHERE share one describe (parse check still per text).
  Small-query perf branch vs main (run 42810177524): within noise on this cell.
- Next (in order): (a) Q-40b-1 answer, then the fix + regression perf run (`small-query-perf.yml` baseline
  deafc861cce3, `bothOptions=-DresourceProfile=speed`); (b) check Dev CI on main for ac2055c8; (c) gizmo vs speed per
  call and c32 JFR on the HDFS point cell (old step 4). `small-query-perf.yml` has a new input `bothOptions` (options
  measured on the baseline and on the ref). Branches `perf/bisect-*` and `perf/40b-c1-mem` are throwaway (perf
  dispatch refs only) - deleting them from the session is refused (HTTP 403): the maintainer may delete them.

ITEM 40(b) state after session 2026-11-10 (4) - third improvement (repeated parses of one text) merged (388b81f5):
- Measured where jOOQ parses run (debug print on every parse-cache miss, `PointQueryOverheadPerfTest`, 4 vCPU): each
  query text is parsed by jOOQ **once** already (complete mode: first in DoGet's `CatalogViews.pruning`; partial:
  in create's `completeAnswerReason`); the later `ParquetQueryParser.parse` calls of the same request hit the parse
  cache. So step (2) "one jOOQ parse per request" was already true; what the calls repeated was the preparation in
  front of the cache lookup (`castTypedLiterals`, which built a new regex Matcher at every character, and
  `flattenSubqueryWrapper`, which compiled its pattern each call): JFR 15 of 39 parser samples.
- Cut: cache by the raw text in `parse()` (same capacity/size limit, refused texts never cached) + one matcher tried
  only at `t`/`T` in `castTypedLiterals` + `FROM_PAREN` compiled once. JFR samples under `parse()` 39 -> 25 (of ~700),
  preparation 15 -> 0; end to end within noise (c1 point p50 complete 9.6-9.8 ms before, 10.2 after; partial 7.4-7.7
  vs 7.9; DuckDB-bound VM).
- Other per-query costs seen in the same JFR (for the next cuts): `ExecutionPathRecorder` (`queryDigest` SHA-256 +
  JSON line + write, ~18 samples, as many as the jOOQ parse), `FileStatsPruner.keep` parses its filter with jOOQ
  again per query (`parseCondition`, 2 samples), and in partial mode `ClusterService.allServerLoads` takes the
  `entrySet` query path (~45 samples) because the test helper (one of its constructors) starts Hazelcast without the
  `arrowflight.uri` member attribute; `ServerModule` always sets it, so production takes the key-lookup path
  (test-only, not a server cost). `ExecutionPathRecorder` is already async with a per-thread digest; making the
  per-query INFO line cheaper than that means turning it off by default = a behaviour change (ask first).
- Next (in order): (a) check Dev CI on main for 388b81f5 (run 42802352922, started at session end); (b) one jOOQ
  `parseCondition` per filter text in `FileStatsPruner.keep` (it re-parses the filter the query parser just
  rendered; small, ~2 samples); (c) literal-insensitive describe cache key (old step 3) - the bigger remaining
  server-side cut for point queries; (d) gizmo vs speed per call and c32 JFR on the HDFS point cell (old step 4).
  Start the next branch from `origin/main` as `fix/40b-small-4`. JFR recipe that works here: append
  `-XX:FlightRecorderOptions=stackdepth=256 -XX:StartFlightRecording=filename=<dir>/x-%p.jfr,settings=profile` to
  `JAVA_TOOL_OPTIONS` and run `mvn -pl flight-server test-compile surefire:test -Dgroups=perf -DexcludedGroups=
  -Dtest=PointQueryOverheadPerfTest -Djacoco.skip=true` (`-DargLine` is not passed to the fork).

ITEM 40(b) state after session 2026-11-10 (3) - second improvement (one parse check per pruned query) merged:
- Done: next step (1) of session (2). DoGet's pruned text is accepted without a second `json_serialize_sql` when the
  parse check of the unpruned text (the describe's) found a DuckDB base table reference (`query_location`, UTF-8
  byte offset) exactly at the replaced span and the span is a plain table name (`DuckDbAdapter.acceptRewrite`,
  `ExecutionService.prunedFiles`, `CatalogViews.prunedReplacement`). Derived names = original's + `read_parquet` +
  `list_value` (the `[...]` file list parses as `list_value`; the test over the 40 pruning queries found it).
  Only on the node that described the text (cache per node); the local-copy path (item 42) still checks in full.
- HDFS point perf (balanced / speed, c1/c8/c32 q/s, 2 runners each): branch 42791070774 b 56.4/79.7/80.1 and
  55.5/77.0/77.4, s 54.6/88.3/97.3 and 55.3/89.6/96.0; main baseline 42791075306 b 53.0/78.5/79.5 and 45.2/63.1/64.8
  (slower runner), s 56.0/90.0/95.5 and 46.5/71.3/78.0. Within noise on the comparable runner (the 2-CPU cell is
  DuckDB-CPU-bound); 0 errors. Expected saving ~0.15 ms CPU per point query.
- Next (in order): (2) one jOOQ parse per request (or a parse cache per SQL text for create -> GetFlightInfo -> DoGet);
  (3) literal-insensitive describe cache key; (4) gizmo vs speed per call and c32 JFR on the HDFS point cell, as in
  the session (2) note. Branch for the next cut: `fix/40b-small-2` was this cut's (`fix/40b-small` cannot be rebased:
  force-push refused, and merging main into it conflicts with the cherry-picked 48adb2ac) - start the next one from
  `origin/main` as `fix/40b-small-3`.

ITEM 40(b) state after session 2026-11-10 (2) - first improvement (parse check name scan, see Log) merged:
- Local cell rebuilt (same shape; this time data sorted by key, 93 750-row groups for orders, 375 000 for lineitem,
  so DuckDB's own point time is smaller): c1 p50 ms, prepared flow point_orders create 1.45-1.55, GetFlightInfo
  0.60, DoGet 2.1-2.3, close 0.5 (= round trip); `SELECT k` create 0.95, DoGet 0.9. So per table point query the
  server adds ~0.5 ms in create over `SELECT k` (describe of a text never seen: the key differs every query, so
  the describe cache and the parse-check cache never hit) and ~1.2 ms in DoGet (parse check + DuckDB prepare +
  execute).
- JFR (executor threads, point_orders only, 6 000 queries): DuckDB execute ~27 % of samples, DuckDB prepare
  ~13 %, the parse check ~19 % (DuckDB `json_serialize_sql` + the Java regex over its JSON), jOOQ parsing ~7 %
  (checkQuery / ParquetQueryParser, three times per prepared query: create, GetFlightInfo, DoGet), the rest small.
  Micro-benchmark of the parse check for a point query (2.2 KB JSON): `json_serialize_sql` ~140 us, the regex 20 us
  (now 0.6 us); both run twice per query (describe text and the pruned DoGet text differ).
- End to end the regex fix is within noise (c1 and c8 ADBC load: base 108.8 / 107.4 q/s, new 106.0 / 108.3 q/s; the
  4-vCPU VM is DuckDB-CPU-bound at c8 by `count_filter`).
- Next (in order): (1) skip the second `json_serialize_sql` per query: DoGet's text is the describe's text with one
  table reference replaced by server SQL (`prunedFiles`); accept it when the unpruned text passed the check (needs a
  careful argument + test that the substitution cannot add a statement or a refused name; ~0.15 ms + one DuckDB
  parse per query); (2) one jOOQ parse per request instead of several (checkQuery, qualifiedPlainNames,
  ParquetQueryParser, TableReferences), or cache the parse per SQL text for the GetFlightInfo/DoGet that follow
  create; (3) a literal-insensitive describe cache key (WHERE-clause literals only; the schema does not depend on
  them) so create stops describing every new key - needs a proof that output columns cannot depend on the replaced
  literals; (4) gizmo vs speed per call on the same box, and c32 CPU per query with JFR on the HDFS point cell, as
  before. JFR here needs `-XX:FlightRecorderOptions=stackdepth=256` *before* `StartFlightRecording` (stacks were cut
  at 64 frames this session).

ITEM 40(b) state after session 2026-11-10 (1) - profiling only, no code change yet (branch `fix/40b-small` = main +
the tool `tools/ci/small-query-rpc.py`):
- Local cell (4 vCPU VM, local disk, synthetic TPC-H-shaped orders 1.5 M / lineitem 6 M rows, 4 key-range files per
  table, 4 row groups per file; DuckDB's tpch extension cannot be downloaded in the session), c1, complete mode, p50 ms:
  ADBC point_orders ~13-14, point_lineitem ~12-13, count_filter ~17, `SELECT k` ~5. Per call (small-query-rpc.py):
  point_orders statement flow GetFlightInfo 1.6-1.8 + DoGet 8.2-9.3; prepared flow (what ADBC sends) create 1.7-1.8,
  GetFlightInfo 0.6-0.7, DoGet 8.8-9.3, close 0.5-0.7 (= one round trip, no server work). `SELECT k`: create 1.1-1.3,
  DoGet 1.0-1.2.
- DuckDB's own time is most of it: the pruned DoGet SQL (one file kept by `FileStatsPruner`) run alone over JDBC
  (same jar, same settings incl. lockdown) takes 6.1-7.4 ms (the matching row group is decoded); DoGet adds ~1-1.5 ms
  on top. Server time per query beyond DuckDB and round trips: ~0.6-1.2 ms in the describe (create /
  GetFlightInfo; table queries +0.5 ms over `SELECT k`) and ~1 ms in DoGet. JFR (1 ms sampling, `SELECT k`): server
  CPU ~0.3 ms per query; in DoGet the parts are DuckDB fetch, `putNext`/`start` (Flight write), the
  `ExecutionPathRecorder` JSON line (one INFO line per query) and ticket sign/verify; nothing dominant.
- External file cache (speed default): +0.8 ms (+13 %) per point query on local files in plain JDBC, but no
  difference end to end on the server (noise +-1 ms), and speed vs balanced is equal here at c1 (the 1.16-1.18x of
  the harness does not reproduce on this VM) - not changed.
- Next (in order): (1) get the harness numbers for gizmo vs speed per call on the same box (the gap is likely
  per-call fixed cost x 4 calls of ADBC's flow): run `small-query-rpc.py` against gizmo too (ask the performance
  repo or run gizmo in CI); (2) cut the second DuckDB prepare per query: the describe at CreatePreparedStatement
  runs in the describe DuckDB and DoGet prepares the same text again - cache the describe result per SQL text for
  the GetFlightInfo(prepared) that follows, and measure skipping the describe DuckDB for queries whose schema the
  execution DuckDB gives at prepare time; (3) make the per-query `ExecutionPathRecorder` line and the debug
  formatting free when off; (4) measure c32 CPU per query (that is where the cluster's 460 q/s gap is) with JFR on
  the HDFS point cell (`hdfs-point-perf.yml` with a JFR variant).

> **Maintainer, 2026-11-09 16:30 - FIRST, before anything else in this lane: ITEM 39(c).** balanced point/small-query collapse on HDFS and the speed point-query rate (item 39 (c)); the small-query path, ExecutionService, DuckDbAdapter and catalog views are allowed for this item. Full description
> with the real-cluster numbers: `docs/dev/STATUS.md`, notes "ITEM 39 IS SPLIT ..." and "BUG FIRST, ITEM 39". Work on branch
> `fix/39c-point` (create it from `origin/main` if missing; rebase on `origin/main` each session), tests + CI green on the branch,
> then merge into `main` (pull --rebase, keep both sides of CHANGELOG conflicts) and write "ITEM 39(c) DONE <merge sha>"
> in this file's Log. Maintainer exception to this lane's scope limits for this item only. No `package=true` here: the
> core lane packages when all four parts are merged.

ITEM 39(c) state after session 2026-11-09 (4) - see the Log for "ITEM 39(c) DONE":
- Part 1 (catalog views refresh without making queries wait), part 2 (`FooterStubCache`), part 3 (file pruning by
  footer statistics, `fileStatsPruningMaxFiles`, Q10) and part 4 (pruning also with a table alias,
  `FROM s.t x WHERE x.k = 5`) are in `main`.
- Measured (HDFS point perf 42689843600, SF1, key-range files, point_orders / point_lineitem / count_filter mix, two
  runners): NameNode opens per query ~20 -> ~11 in every profile (lineitem files ~2.0 -> ~0.5 opens each; orders
  files ~3.2 -> ~2.1: `count_filter` on `o_custkey` still reads every orders file), speed c32 ~10 -> ~5.7.
  q/s +3..15% against the `-nopruning` variants (balanced c32 58.6 -> 65.6 and 45.7 -> 52.8; speed c32
  97.9 -> 106.3 and 77.9 -> 82.5); the 2-CPU cell stays CPU-bound in `duckdb_jdbc_execute`, so the real cluster's
  round-trip-bound collapse is expected to gain more (fewer Kerberos/SASL RPCs per query). Verify on the stand once
  the core lane has packaged.
- Follow-ups of 39 (c), not blocking DONE ((a) and (b) done by the core lane in 877cf100: hadoopfs afc4 in CI,
  `hdfsMetadataCacheTtlMs`, external file cache default on HDFS): (a) Q9 answer: when the hadoopfs build `51b7220-afc4` appears in
  `ci-assets` (only afc3 on 2026-11-09 20:30Z), take it, set `hdfs_metadata_cache_ttl_ms` from a new optional
  server setting and measure opens per query again (`hdfs-point-perf.yml`). (b) Q11: balanced fd requests per query
  (~5-6; file cache variant 0-0.2 and +10..15% q/s). (c) A cell like the cluster (several schemas, SF10), speed vs
  upstream. (d) Pruning for plain names on non-HDFS stores and partial mode (QueryPlanner) - only if the stand
  shows it matters.

> **Maintainer, 2026-11-02 18:30 - SERVICE MATRIX, catalogs and stores part (take it first).** A real Hadoop
> cluster refused every HDFS query with `f06b5834e76e` (core lane fixes it); no CI run talks to real services end to
> end. Add `.github/workflows/service-stores.yml`: **S3** (MinIO, s3 plugin, 2 stores), **lakeFS** (branch + commit),
> **Ozone** (S3 gateway), **Iceberg** Hadoop catalog on S3 + one REST catalog (Polaris or Lakekeeper, vended
> credentials on), **PostgreSQL** and **MySQL** through the jdbc plugin. Own CI workflow of its own (only this lane edits it), triggered on `workflow_dispatch` with an input `server` (a kestrel
> pre-release id; default: build from the checked-out commit) and on push to main touching its files. Each cell: a real
> container of the service, the server in **balanced and `-DresourceProfile=speed`**, TPC-H SF1 22/22 with answers
> checked against in-process DuckDB + Flight SQL metadata + one prepared query; job summary = table service x profile
> -> pass/fail + first error line. A failure in a cell is a bug for the lane that owns the code (write it as bug-first
> into that lane's STATUS). Then write "SERVICE MATRIX <lane> DONE" at the start of a line.

SERVICE MATRIX catalogs DONE (2026-11-02). `.github/workflows/service-stores.yml`: 8 services x 2 profiles = 16 cells,
all green on this commit's build (run 41473152168) and on pre-release `server-1.0.0-fca6512a8046` (dispatch run
41473218719). Cells (`tools/ci/service-stores-cell.sh <service> <profile>`): `s3` (MinIO, data directory on bucket
`lake` + named store `wh` on bucket `warehouse`), `iceberg-hadoop` (pyiceberg-written Hadoop-layout tables on
`s3://warehouse/iceberg`, read through the named store), `iceberg-rest` (Lakekeeper v0.13.8 on MinIO, STS on as a
MinIO user, `vendedCredentials=true`; the server's own S3 key is a random one MinIO does not know, so every read
uses the vended credentials), `ozone` (S3 gateway, Ozone 2.0.0), `lakefs-branch` / `lakefs-commit` (lakeFS 1.50.0
quickstart; the commit cell pins the first commit after a second commit emptied `nation` on main), `postgres`
(16.4) / `mysql` (8.4) through the jdbc plugin (TPC-H loaded with COPY / LOAD DATA). Check
(`tools/ci/service-tpch.py check`, ADBC Flight SQL): 22 answers equal in-process DuckDB 1.5.6 over the same Parquet
files (columns, rows as a multiset, numbers within 1e-6 rel / 0.005 abs), GetSqlInfo, GetObjects (8 tables + their
columns), GetTableSchema, one prepared query bound twice; complete mode. MinIO: Docker Hub / quay.io refuse the
pulls, the cell takes `cgr.dev/chainguard/minio` (the job log names the image). No cell failure, so no bug for
another lane.
Optional follow-ups (not required by the item): a `partial`-mode pass per cell; Polaris as a second REST catalog;
SF10 on dispatch (`sf` input exists, runner disk/time not checked above SF1).


> **Maintainer, 2026-10-29 13:15 - FINISH MODE:** finish only what is started: C1 step 2b (vended credentials, per
> Q1 answer) and C2 (the JDBC federation plugin, per Q4 answer). Delta, Paimon/Hudi and full ACID ORC stay as written
> backlog (not started now). Then write "CATALOGS LANE DONE" and stop.

**CATALOGS LANE DONE (2026-10-29).** C1 and C2 are finished (see below); C3 step 2, C4 (b) / (d) stay written backlog
as the maintainer asked. Open follow-ups for whoever picks the lane up again: Q8 (needs a decision), C2 "Next"
items (2)-(3) optional, C1 Polaris-with-STS optional.

C1. **Iceberg REST catalogs beyond Gravitino:** conformance of the iceberg plugin's REST path against Apache Polaris,
    Project Nessie and Lakekeeper (containers in CI), incl. vended credentials (S3 temporary credentials returned by the
    catalog, refreshed before expiry) and OAuth2 client credentials; Unity Catalog OSS if its Iceberg REST endpoint runs in
    CI. Answers equal to the same tables via HMS; partial and complete mode.
    - Step 1 done (2026-10-28): `RestCatalogConformanceTest` + workflow `catalogs-ci.yml` (matrix polaris,
      iceberg-rest-fixture; local `file://` warehouse bind-mounted at the same path, containers run as the runner user
      with `/etc/passwd` mounted). Polaris covers OAuth2 client credentials, `scope`, prefix from `/v1/config`.
    - Step 2a done (2026-10-28): S3 warehouses with static keys. `RestCatalogConformanceTest` takes
      `ICEBERG_REST_IT_ROOT=s3://...` + `ICEBERG_REST_IT_S3_*` (writer: Iceberg S3FileIO with the url-connection
      client - Iceberg 1.12's "apache" client is the SDK's Apache 5 client, absent from SDK 2.34.1; plugin: the s3
      plugin's store; DuckDB: httpfs + secret). Matrix entries polaris-s3 (`stsUnavailable`), iceberg-rest-fixture-s3,
      lakekeeper-s3 (`vakamo/lakekeeper` on Docker Hub, PostgreSQL, `tools/ci/lakekeeper-setup.sh`; its metrics
      port moved to 9100). S3 server: moto (`quay.io/minio/minio` refuses anonymous pulls; Docker Hub `minio/minio`
      is gone). All five pass locally (Docker in the session; quay.io and ghcr.io blobs are blocked there).
    - Step 2b (i) done (2026-10-29, Q1 answered yes): vended credentials for DuckDB's data reads.
      SPI 1.7 (own commit 64c03a9): `StorageCredential(prefix, type, options, expiresAt)`,
      `TableSnapshot.credentials` (old 7-argument constructor kept), `TableSnapshot.withFiles`. Core (own
      commits 5efec89, 9e7bd88; STATUS.md Log line): `TableSecrets` - `SpiCatalog` registers the credentials of every
      snapshot it returns; one `CREATE OR REPLACE TEMPORARY SECRET table_secret_<hash>_<i>` per credential, scoped
      to the deepest directory holding the snapshot's data / delete / metadata files below the prefix (so a
      warehouse-wide prefix still gives the table directory), checked like `createSecret` (type s3 / gcs / r2,
      allowlisted options, scope below the catalog's roots; outside -> IllegalArgumentException = that table is
      skipped), URL_COMPATIBILITY_MODE forced on, expired credentials fail the table; `DuckDbAdapter.session()`
      brings each DuckDB (per session, or once per shared database) to the current set; refreshed -> replaced,
      no longer returned -> `DROP TEMPORARY SECRET IF EXISTS`; a refused statement is logged without literals
      and does not fail other tables' queries. Plugin (8ed1a8d): opt-in `iceberg-rest.vendedCredentials`
      (default false) sends `X-Iceberg-Access-Delegation: vended-credentials`, maps the S3 entries of
      `storage-credentials` (or `config`, for the table location) to KEY_ID / SECRET / SESSION_TOKEN / REGION /
      ENDPOINT / USE_SSL / URL_STYLE + `s3.session-token-expires-at-ms`, reloads a table 5 min before expiry;
      descriptor SPI 1.7. Tests: `TableSecretsTest`, `SpiCatalogTest`, `S3IntegrationTest
      .tableSecretsReadOneTableWithItsOwnCredentials` (real httpfs after the lockdown, FakeS3: the table's reads
      are signed with the vended key + token, refresh switches the token, drop falls back to the store key,
      `?s3_endpoint=` reaches no other host, values redacted in `duckdb_secrets()`), `VendedCredentialsTest`.
      Catalogs CI entry `lakekeeper-s3-vended` (Lakekeeper `sts-enabled` against moto's AssumeRole; moto ignores
      session policies, so Lakekeeper's downscope check fails and the entry sets
      `LAKEKEEPER__SKIP_STORAGE_VALIDATION=true`): every table carries its own temporary key + token, server
      answers equal the reference with `vendedCredentials=true`.
      Security note: secrets live in DuckDB's in-memory secret manager (database-wide), client SQL cannot list
      them (`duckdb_secrets` / `which_secret` refused) or create / drop secrets (SELECT-only parse check); the
      worst a misbehaving catalog can do is have its credentials used for other tables in the same directory
      tree below its own roots.
    - Step 2b (ii) done (2026-10-29): manifests with the vended credentials. `VendedS3` (plugin, no SDK): SigV4-signed
      HEAD / ranged GET on the JDK HTTP client with one vended credential (KEY_ID / SECRET / SESSION_TOKEN signed as
      `x-amz-security-token` / REGION / ENDPOINT / USE_SSL / URL_STYLE; AWS endpoints `s3[.<region>].amazonaws.com`,
      vhost unless path style); `StoresFileIO.withCredentials` routes files below a credential's prefix (longest
      first) and below the catalog's roots to it, everything else to the stores; `IcebergCatalog` plans with that
      FileIO (`SnapshotPlanner.plan(..., fileIo)`). Core checked: catalog-table files are never read Java-side
      (`TableStatisticsService` uses the catalog's counts, footer reads touch the data directory only), so no core
      change. Tests: `VendedManifestReadsTest` (InMemoryCatalog tables copied to FakeS3, REST catalog vending
      per-table credentials, stores refusing every read; every request's signature verified with the s3 plugin's
      `S3SigV4`), `RestCatalogConformanceTest` gives the `lakekeeper-s3-vended` entry a refusing store (Catalogs CI
      run 40862720044 green: Lakekeeper's STS credentials read every manifest). Not done (optional): a Polaris entry
      without `stsUnavailable` (STS endpoint to moto) - Lakekeeper already covers vended credentials end to end.
    - Step 3a done (2026-10-28): answers through the server, local warehouses. `RestCatalogServerConformanceTest`
      (flight-server, `rest-catalogs`, env-gated like the plugin test) runs the server with iceberg-rest (schemas
      `rest_*`) next to iceberg-hadoop over a reference warehouse with the same fixtures; complete answers equal the
      reference (count + md5 per table), partial equal complete, located partial endpoints. `IcebergFixtures.writeRest`
      (shared by both tests). `catalogs-ci.yml` runs it for polaris and iceberg-rest-fixture (passes locally on both).
    - Step 3b done (2026-10-28): the server test on the S3 entries too (polaris-s3, iceberg-rest-fixture-s3,
      lakekeeper-s3): writer Iceberg S3FileIO (the iceberg module's `fixture-libs` now include
      `software.amazon.awssdk`, `software.amazon.eventstream`, `org.reactivestreams`, `dev.failsafe`), the s3 plugin
      installed next to iceberg with `arrowflight.s3.endpoint/region/urlStyle=path` + `FLIGHT_S3_*` keys; the catalog
      root's store is opened lazily by `CatalogStores.locate`.
    - Step 3c done (2026-10-28): HMS as a further reference on local warehouses: the REST metadata files registered
      in the in-process metastore (`HmsFixture.registerIceberg`, schema `hms_ice`, hive plugin -> iceberg plugin);
      answers equal the REST catalog's (schema + rows), partial equal complete. Skipped on S3 entries (the in-process
      HMS has no S3 file system); the workflow expects 4 tests with exactly that one skip there.
    - Step 4 done (2026-10-28): Unity Catalog OSS 0.7.0 (`unitycatalog/unitycatalog:v0.7.0`, Docker Hub) serves a
      writable Iceberg REST endpoint (`/api/2.1/unity-catalog/iceberg`, warehouse `unity`, prefix `catalogs/unity`):
      create namespace/table and commits work once `storage-root.tables` is set (managed tables under
      `<root>/__unitystorage/tables/<uuid>`). Entry `unity-catalog` in `catalogs-ci.yml` (local warehouse; UC runs
      as the runner user in group 101 on writable copies of `etc/conf`, `etc/db`, `etc/logs`). Findings: UC refuses
      Iceberg `time` columns (`ICEBERG_REST_IT_OMIT_COLUMNS=tm` leaves `types.tm` out of both sides); its sample
      table `default.marksheet_uniform` answers 500 on loadTable, which makes the whole catalog unavailable in the
      server (see Q3), so the server test uses `ICEBERG_REST_IT_NAMESPACES=ice`. Plugin test 3/3, server test 4/4
      (HMS reference included) pass locally. No vended credentials checked (UC's `/credentials` endpoint is listed
      in `/v1/config`; belongs to step 2b).
    - **C1 done** (2026-10-29). Optional extra: Polaris with STS (see step 2b (ii)).
C2. **JDBC federation plugin:** PostgreSQL and MySQL tables as a data source through DuckDB's `postgres` / `mysql`
    extensions (bundle for 1.5.6 offline like the others; check availability), read-only, pushdown of filters/projection,
    credentials from the plugin config (never logged), Ranger checks as for other tables. CI with postgres/mysql
    containers, type matrix, TPC-H SF0.1 answers.
    - Note (2026-10-28): ci-assets `duckdb-extensions-v1.5.6.tar.zst` holds only avro, hadoopfs, httpfs, iceberg -
      `postgres_scanner` / `mysql_scanner` are missing; extensions.duckdb.org answers 403 from the session (CI can
      download them, as catalogs-ci.yml's fallback does). First step: a CI job that downloads both for v1.5.6, checks
      they load in duckdb_jdbc 1.5.6 with `enable_external_access=false` after `ATTACH` (or what order works under
      the lockdown), and records the result.
    - Step 1 done (2026-10-28): `catalogs-ci.yml` job `jdbc-scanners` + `tools/ci/DuckDbScannerProbe.java`
      (PostgreSQL 16.4, MySQL 8.4 containers; extensions from extensions.duckdb.org, kept 14 days as workflow
      artifact `duckdb-scanner-extensions-v1.5.6`; the job looks for
      `<name>-v1.5.6-linux_amd64.duckdb_extension` in ci-assets first). Results with the server's order
      (in-memory secrets, `LOAD '<path>'`, `CREATE TEMPORARY SECRET`, `ATTACH '' AS src (TYPE ..., SECRET s,
      READ_ONLY)`, then allowed_directories + no autoload + `enable_external_access=false` +
      `lock_configuration=true`):
      - Works after the lockdown, both engines: reads, projection and filter pushdown (`Filters: id=2` in the
        POSTGRES_SCAN / MYSQL_SCAN plan), information_schema of the attached database, NULLs. PostgreSQL runs
        reads in `READ ONLY` transactions (`transaction_read_only=on`).
      - Types: pg json/jsonb/char -> VARCHAR, bytea -> BLOB, int[] -> INTEGER[], numeric(18,4) -> DECIMAL(18,4),
        timestamptz -> TIMESTAMP WITH TIME ZONE, uuid/interval/time native. MySQL: time -> VARCHAR
        (`mysql_time_as_time=false`), year -> INTEGER, bit(1)/tinyint(1) -> BOOLEAN, bigint unsigned -> UBIGINT,
        enum/json -> VARCHAR.
      - Refused after the lockdown: a new `ATTACH` (also with the existing secret), `INSERT` into the read-only
        attach, `postgres_execute` / `mysql_execute`, non-SELECT and multi-statement text in `*_query`.
      - **Gets through (must be refused in client SQL before a plugin ships):** `postgres_query('src', '<any
        SELECT>')` / `mysql_query(...)` run text on the remote database with the plugin's credentials (bypasses
        per-table checks; side-effect functions like `set_config` ran); `postgres_scan('<any dsn>', ...)`
        connects to any host with any credentials despite `enable_external_access=false` (network egress from
        the server); `duckdb_secrets()` shows host/user (already in the server's `REFUSED_FUNCTIONS`);
        `CREATE OR REPLACE SECRET` and `DETACH` pass DuckDB but not the server's SELECT-only parse check.
      - Functions the extensions add: postgres_scanner `pg_clear_cache, postgres_attach, postgres_configure_pool,
        postgres_execute, postgres_hstore_get, postgres_hstore_to_json, postgres_query, postgres_scan,
        postgres_scan_pushdown, read_postgres_binary`; mysql_scanner `mysql_clear_cache,
        mysql_debug_execution_plan, mysql_execute, mysql_explain_federated, mysql_query`.
    - Step 2 done (2026-10-29, Q4 answered): **SPI 1.9** `DuckDbSetup.loadExtensionRefusingFunctions` + core diff of
      `duckdb_functions()` around that LOAD (Q4 (b), opt-in per extension so existing plugins keep their functions);
      **SPI 1.10** `DuckDbSetup.attachDatabase` (secret + `ATTACH '' AS n (TYPE postgres|mysql, SECRET n,
      READ_ONLY)`, allowlisted options, only after the scanner was loaded refusing, name refused as qualifier in
      client SQL) and `ReadSpec.AttachedTable` (views / describe read the attachment, complete only, statistics
      unknown); a catalog that attaches a database may have no roots. Module **`flight-plugin-jdbc`**
      (`catalog=jdbc`, see CHANGELOG): only configured `schema.table` entries are shown, refused lists of its own
      (Q4 (a)) checked against the bundled builds by `ScannerFunctionsTest`. `JdbcFederationIntegrationTest` in
      Catalogs CI job `jdbc-scanners` (PostgreSQL 16.4, MySQL 8.4, type matrices): answers equal DuckDB's own scan
      (all values as DuckDB text), partial = complete (one endpoint), cross-database join, scanner functions /
      attachments / ATTACH / DETACH / `duckdb_secrets()` refused in both modes, password never in errors - 3/3 and
      2/2 in run 40865845835 (job 123420311726). Plugin added to the release plugin set (`dev-ci.yml`).
    - Step 3 done (2026-10-29): TPC-H SF0.1 generated by DuckDB's `tpch` extension, copied into PostgreSQL through a
      writable scanner attach, the 22 queries through the server (unqualified names, complete) equal to DuckDB over
      the generated data (numbers within 1e-9 relative) - `JdbcFederationIntegrationTest` 4/4, Catalogs CI
      40868732813 green; Dev CI 40868732917 green (head 1e7b43a4).
    - **C2 done.** Follow-ups (not required by the item as written): (1) Q8; (2) a Ranger-style restriction on a
      jdbc table (`TableRestrictionIntegrationTest` pattern) - restrictions apply to catalog tables generically, not
      checked for attached ones; (3) TPC-H on MySQL too; (4) `postgres_scanner` / `mysql_scanner` / `tpch` into
      ci-assets (the session cannot download Actions artifacts - blob storage blocked).
C3. **Delta Lake tables** via DuckDB's `delta` extension (check 1.5.6 build), from HDFS and S3; refuse clearly what it
    cannot read (deletion vectors / column mapping if unsupported). Then: Apache Ozone S3 gateway as an S3 backend test.
    - Step 1 done (2026-10-28): `catalogs-ci.yml` job `delta`: Delta tables written by Spark 3.5.3 + delta-spark
      3.3.2 (`tools/ci/delta-fixtures.py`, Spark's answers in `expected.tsv`), read by `tools/ci/DuckDbDeltaProbe.java`
      with `delta` v1.5.6 (extension `6059958`, from extensions.duckdb.org, kept 14 days as workflow artifact
      `duckdb-delta-extension-v1.5.6`; ci-assets checked first) after the server's lockdown, locally and on S3 (moto,
      scoped `TYPE s3` secret). Results:
      - **Read correctly after the lockdown, local and S3** (all equal to Spark): plain, partitioned (string/int,
        NULL partition), deletion vectors (DELETE + UPDATE), column mapping `name` (renamed + dropped column) and
        `id`, checkpoints (interval 2, delete after the last), schema evolution (added column NULL in old files),
        time travel `delta_scan(path, version := n)`. `ATTACH '<path>' AS x (TYPE delta)` works before and after
        the lockdown (also `PIN_SNAPSHOT`, `VERSION n`). Filter + projection pushdown (`DELTA_SCAN Filters: p='x',
        id>1`). Types: timestamp -> TIMESTAMP WITH TIME ZONE, timestamp_ntz -> TIMESTAMP, decimal(38,10), binary ->
        BLOB, array/struct/map native. No replacement scan (`FROM '<dir>'` is not a Delta read). HDFS not probed:
        delta-kernel has its own object store (local, s3, az, gs/r2 via secrets, http(s)), so `hdfs://` tables
        would need the hadoopfs extension, which delta-kernel does not use - expect "not readable" (step 2 checks).
      - **Gets through the lockdown (must never reach client SQL):** delta-kernel does its own I/O, which
        `allowed_directories` / `enable_external_access` only stop at DuckDB's Parquet reader: a table's
        `_delta_log` outside the allowed directories is read (`DESCRIBE delta_scan(...)` shows its schema,
        `delta_list_files` its file names, sizes, partition values, DVs), local and on S3 with any matching
        secret; `delta_scan('https://<host>/...')` connects to any host (10 retries), `s3://` outside every
        secret's scope goes to the instance metadata service (169.254.169.254). The extension also adds
        `write_blob(path, blob)` and `copy_dir(src, dst)`, which **write files anywhere the server user can**
        (checked: both wrote outside the allowed directories after `lock_configuration=true`). Full list of
        added functions: `copy_dir, delta_domain_metadata, delta_filter_pushdown_log,
        delta_filter_pushdown_log_tpcds, delta_get_transaction_version, delta_list_files, delta_scan,
        delta_set_transaction_version, get_delta_test_expression, parse_delta_filter_logline, write_blob`.
      - No server setup loads `delta` today (plugins request extensions by name through `DuckDbSetup`; none asks
        for it), so nothing is exposed now.
    - **Next (step 2), after Q5:** module `flight-plugin-delta` (catalog of Delta tables under configured roots:
      a directory with `_delta_log` is a table; local + S3 through the stores' roots/secrets), tables as
      server-written `delta_scan('<root>/<table>')` sources (or `ATTACH ... (TYPE delta)` before the lockdown),
      `DuckDbSetup.refuseFunctions` with every function above (the SPI already has it: no SPI/core change), and
      a unit test that the refused list equals the `duckdb_functions()` diff of the bundled extension, so a new
      extension version cannot add an unrefused function. Partial mode: file list from `delta_list_files`
      (DVs need the scan, so tables with DVs complete-mode only or refused in partial, to decide). Probe HDFS.
    - Ozone done (2026-10-28): Apache Ozone 2.0.0's S3 gateway as an S3 backend: `tools/ci/ozone-s3.sh` (SCM, OM,
      one datanode, S3 gateway as containers on a private network, gateway on localhost:9000; non-secure, so any
      key, as moto; replication ONE; ready in ~50 s locally) and `catalogs-ci.yml` entry
      `iceberg-rest-fixture-ozone-s3` (the fixture's S3FileIO, the test's writer, the s3 plugin and DuckDB httpfs
      all on Ozone). Plugin test 3/3, server test 4/4 (the HMS reference skipped as on every S3 entry) pass
      locally, unchanged code. Ozone's gateway answers unsigned requests with 403 (readiness check takes any HTTP
      answer).
    - Delta on HDFS probed (2026-10-28): `tools/ci/hdfs-single.sh` (Docker Hub `apache/hadoop:3.4.1`, NameNode +
      DataNode in one container on the host network, ready in ~20 s) holds the fixtures; `DuckDbDeltaProbe` with
      `PROBE_HDFS_ROOT` loads hadoopfs (`51b7220-afc3`, ci-assets) next to delta and httpfs. The probe now runs in
      `eclipse-temurin:17-jdk-focal` (+ libxml2): the hadoopfs build links `libssl.so.1.1`, absent on
      ubuntu-latest. Results: hadoopfs reads the tables' Parquet files and lists `_delta_log` (before and after
      the lockdown), but every delta function fails on `hdfs://` with `DeltaKernel ObjectStoreError (8): ...
      Unable to recognise URL "hdfs://localhost:8020/delta/t/"` (delta_scan, delta_list_files, time travel);
      `ATTACH ... (TYPE delta)` succeeds (lazy) and fails on the first read. Local and S3 results unchanged.
      Consequence for the plugin: refuse `hdfs://` (and any scheme but local / s3) Delta roots at configuration
      time with a clear message; reading Delta on HDFS would need a log reader of our own (JSON + checkpoint
      Parquet, the files then read by hadoopfs' `read_parquet`) - not planned, see Next.
    - **Next for C3:** step 2 (the plugin) after Q5. Nothing else in C3 is unblocked.
C4. Hive ACID insert-only tables (after the plugins lane's P4), then Paimon/Hudi read-only if DuckDB or a light Java
    reader allows.
    - Step 1 done (2026-10-28): `InsertOnlyDirectories` in `flight-plugin-hive` (new class, not wired yet): for one
      table/partition directory listing and a snapshot (`WriteIds`: high watermark, min open write id, open +
      aborted ids; `Txns` for `_v<txn>` visibility suffixes) it chooses what to read as Hive's
      AcidUtils.getAcidState does for MM tables: the newest usable base (`base_N` with nothing open at or below N;
      an insert-overwrite base only when write id N committed; a compacted one when its visibility txn is
      committed or its `_metadata_acid` marks it compacted), committed deltas above it (all statement ids of a
      write; a compacted `delta_x_y_vT` covers its sources), original files only without a base, hidden `_*` /
      `.*` entries never. Refused with a clear message (never read in part): `delete_delta_*` (full ACID), a
      compacted base newer than the snapshot ("not enough history"), a multi-id delta without `_v` holding an
      aborted id, a delta reaching past the snapshot, partially overlapping deltas. `InsertOnlyDirectoriesTest`
      (9 unit tests).
    - Step 2 done (2026-10-28): wired behind the new optional setting `hive.insertOnlyTables` (default false).
      `Metastore.snapshot` (`ThriftMetastore`: `get_open_txns`, then `get_valid_write_ids` with the
      `ValidReadTxnList` string `hwm:minOpen:open:aborted`); `HiveTable.classify` shows
      `transactional_properties=insert_only` tables when on (any format the plugin reads; full ACID stays skipped);
      `HiveCatalog.plan` takes one snapshot per plan and lists each table/partition directory recursively, keeping
      the files of the entries `InsertOnlyDirectories.select` chooses (directories from `DataStore.directories`,
      so an empty `base_N` of an INSERT OVERWRITE counts; `_metadata_acid` read only for a base that needs it);
      a refused layout fails the plan with an IOException naming the directory. `HmsFixture` now creates Hive's
      txn tables (`TxnDbUtil.prepDb`) and opens/allocates/commits/aborts through the HMS API.
      `HiveInsertOnlyTableTest` (4 tests: originals + committed/aborted/open deltas, a later commit becoming
      visible, INSERT OVERWRITE base of an open then committed write, partitions, the delete_delta refusal, the
      setting off).
    - Step 3 (a) + (b, ORC) done (2026-10-29): `HiveInsertOnlyIntegrationTest` (flight-server, packaged hive
      plugin, in-process HMS with txn tables): insert-only Parquet (originals, committed / aborted / open deltas,
      `_tmp`), partitioned and ORC tables answer the committed rows only in complete and partial mode (ORC equal to
      a plain ORC table holding exactly those rows); a commit is visible to the next partial query (complete
      answers read views refreshed at most once a minute: one consistent snapshot or the other); INSERT OVERWRITE
      base replaces older writes; `delete_delta` refused in both modes. **Bug found and fixed:** the refusal was an
      IOException, which `CatalogViews` treats as a metastore outage, so every table listed after the refused one
      (all schemas of the metastore) vanished from the views. Now `HiveCatalog.UnreadableTableException`
      (IllegalArgumentException): only that table is skipped; complete -> NOT_FOUND, partial -> INVALID_ARGUMENT
      with the reason. Small core change (own commit 63679fe): `catalogCompleteReason` catches RuntimeException.
    - Step 3 (b, text) done (2026-10-29): `HiveInsertOnlyIntegrationTest` also covers insert-only text tables
      (Hive's default serde; originals, committed plain and gzip deltas, aborted and open writes, a partitioned
      table) through the server: committed rows only, both modes, equal to DuckDB's rows; pruning; a join with the
      Parquet insert-only table. Unchanged plugin code.
    - Step 3 (c) done (2026-10-29): real Hive writer. `tools/ci/hive-mm-writer.sh` runs HiveServer2 4.0.1 (Docker
      Hub `apache/hive:4.0.1`, ~3 min) with an embedded Derby metastore on a mounted directory and DbTxnManager,
      writes insert-only Parquet / text / ORC (one partitioned) tables (inserts, failed inserts via
      `assert_true(rand() < 0)` - a check over a table scan passed on the runner because its map vertex did not
      run, INSERT OVERWRITE of a table and of one partition, gzip text output, `COMPACT 'major' AND
      WAIT`, `COMPACT 'minor' AND WAIT`), records Hive's `SELECT *` answers, stops HiveServer2 and starts a
      standalone metastore (`SERVICE_NAME=metastore`, `IS_RESUME=true`) on the same Derby on localhost:9083.
      `HiveRealWriterIntegrationTest` (env-gated `HIVE_MM_IT_*`) reads them through the server: equal to Hive's
      answers in both modes (2/2 locally; Catalogs CI job `hive-writer`). What Hive 4 really writes: a failed
      insert leaves no directory (write id aborted); major compaction -> `base_N_vT` **without** `_metadata_acid`
      (the plugin's `_v` + committed visibility txn rule applies); minor -> `delta_1_3_vT` next to its sources;
      INSERT OVERWRITE -> plain `base_N`; hidden `.crc` files beside every data file; gzip text as `000000_0.gz`.
      **Hive 4 quirk:** below a directory whose name starts with `_` (the runner's `/home/runner/work/_temp`)
      Hive answers no rows for insert-only tables and compactions write nothing (reproduced locally), so the job
      keeps the warehouse in `/tmp/hive-mm` and the script fails on an empty Hive answer. The plugin does not
      mirror that quirk (not checked what it answers there; Hive's behaviour looks like a hidden-path filter
      applied to the whole location, not a rule worth copying).
      Setup notes: `ALTER TABLE ... COMPACT` in HiveServer2 needs direct access to the metastore database
      (`AlterTableCompactOperation` inits a `MetaStoreCompactorThread`), so a HiveServer2 talking to a separate
      thrift metastore on its own Derby fails with "Version information not found in metastore"; a shared
      PostgreSQL would also do (Docker Hub answered 429 for `postgres` in the session).
    - Step 4 (b) probe done (2026-10-29): Catalogs CI job `paimon-hudi`. `tools/ci/paimon-hudi-fixtures.py` writes
      Paimon 1.2.0 (filesystem catalog: append, partitioned append + INSERT OVERWRITE, primary-key with 2 buckets and
      upserts / DELETE / UPDATE uncompacted, partitioned PK after `sys.compact`, PK and append tables with deletion
      vectors, added column) and Hudi 0.15.0 (table version 6) / 1.0.2 (version 8) tables (COW partitioned with
      upsert / delete / update, COW INSERT OVERWRITE = replacecommit, COW with the last commit left inflight, MOR
      uncompacted, MOR with inline compaction) with Spark 3.5.3 (bundle jars from Maven Central: Ivy cannot pass the
      session proxy). `tools/ci/paimon-hudi-probe.py` answers them with a light reader (formats' metadata in
      Python, data by DuckDB `read_parquet`) and with DuckDB's own extensions; wrong light-reader answers fail the
      job. Results (runs 40810022672, 40810359864):
      - **DuckDB has `paimon` and `hudi` extensions for v1.5.6** (download origin - core or community repository -
        is in the job summary of run 40810359864). `paimon` adds `paimon_scan, paimon_snapshots`; `hudi` adds
        `hudi_files, hudi_info, read_hudi`. Both answer **every** fixture equal to Spark, incl. uncompacted Hudi MOR
        (log merge) and Paimon deletion vectors. Not yet checked under the server's lockdown (own I/O outside
        `allowed_directories`, http(s) / metadata service, added write functions - as found for `delta`).
      - Light reader equal to Spark on all Paimon tables and on Hudi COW / compacted MOR of both versions; refuses
        Hudi MOR with log files in the latest slice. Paimon: latest `snapshot-N` (LATEST is only a hint); base +
        delta manifest lists, live = ADD minus DELETE; **manifests are zstd-compressed Avro, which DuckDB's avro
        extension cannot read** ("unknown codec") - a Java reader needs Avro + zstd-jni (both already in the hive
        plugin / server). PK tables (deduplicate engine): files carry `_KEY_<pk>` (partition keys left out),
        `_SEQUENCE_NUMBER` (per bucket), `_VALUE_KIND`; answer = newest row per (partition cols, key), kinds 0/2.
        Deletion vectors: index manifest in `manifest/`, files in `index/` (version byte 1; per range BE length,
        magic 1581511376, portable 32-bit RoaringBitmap, CRC), applied via `file_row_number`; a DELETE on a DV PK
        table writes only a DV (no -D row). DV-mode PK reads skip level-0 files (none occurred). Added columns:
        `union_by_name`; renames refused. Hudi: 0.x timeline `.hoodie/<ts>.<action>`, 1.x `.hoodie/timeline/
        <requested>_<completed>.<action>` with Avro instead of JSON metadata; latest committed base file per file
        group, minus `partitionToReplaceFileIds` of completed replacecommits; inflight base files skipped; pending
        compaction / clustering refused; log files named after the base instant (0.x) or the delta commit (1.x).
    - Step 4 (b) lockdown probe done (2026-10-29): `tools/ci/DuckDbPaimonHudiProbe.java` in the `paimon-hudi` job
      (duckdb_jdbc 1.5.6 in the Ubuntu 20.04 JDK container as for delta; `paimon` e49d491, `hudi` ea166bd, kept 14
      days as workflow artifact `duckdb-paimon-hudi-extensions-v1.5.6`, ci-assets checked first; moto S3, HDFS via
      `tools/ci/hdfs-single.sh`). Run 40821054886. Results:
      - Both load into duckdb_jdbc 1.5.6 and answer **every local fixture equal to Spark after the lockdown**
        (Paimon PK / DV / evolved, Hudi COW / MOR / inflight, both Hudi versions). Added: `paimon_scan` (overloads
        `(table_path, ...)` and `(warehouse, database, table, ...)`, named `snapshot_from_id` /
        `snapshot_from_timestamp`, `file_format`, `manifest_format`), `paimon_snapshots`, `read_hudi(path,
        hudi_option, start, end, as_of)`, scalar `hudi_info` / `hudi_files` (they return an error as a string
        value instead of failing), secret type `paimon`, storage type `ATTACH ... (TYPE paimon)` (a warehouse;
        works before and after the lockdown, tables as `<att>.<db>.<table>`). Hudi has no storage type. **No write
        functions** (unlike `delta`). `paimon_scan` needs the `<warehouse>/<db>.db/<table>` layout.
      - **S3:** paimon ignores the `s3` secret and reads with its own `TYPE paimon` secret (KEY_ID, SECRET, ENDPOINT
        `http://host:port`, REGION, SCOPE): all Paimon fixtures equal to Spark on S3; outside that secret's scope it
        fails "no credentials" (no metadata-service fallback seen). hudi ignores DuckDB secrets and goes straight to
        the instance metadata service (`PUT http://169.254.169.254/latest/api/token`), so no Hudi table on S3 can
        be read; `hudi_option` takes `k=v` entries (JSON refused) but `aws_*` object-store keys did not change that
        (option names unknown).
      - **HDFS:** neither reads `hdfs://` (paimon "invalid scheme hdfs for local file system", hudi "Unable to
        recognise URL"); hadoopfs itself works next to them.
      - **Gets past the lockdown:** both do their own local file I/O: `paimon_scan`, `paimon_snapshots`, a
        metadata-only table (schema), `file://` URIs and `ATTACH ... (TYPE paimon)` of a warehouse outside
        `allowed_directories` read it after `lock_configuration=true`; same for `read_hudi`, `hudi_info`, `hudi_files`
        (file names, sizes, commit times). hudi contacts metadata services for `s3://` (AWS IMDS) and `gs://` (GCE
        metadata) paths. Refused: http(s) URLs (paimon: path format; hudi: "feature for Http not enabled"),
        `az://` without an account, `/proc/self`; no replacement scan (`FROM '<dir>'` is not a read).
      - Consequence: a plugin on the extensions must refuse all five functions in client SQL
        (`DuckDbSetup.refuseFunctions`) and write the table sources itself, as planned for delta. Paimon via the
        extension covers local + S3 (server installs a scoped `TYPE paimon` secret); Hudi via the extension local
        only. The light reader (data through DuckDB's own `read_parquet`, so inside the lockdown, S3 secrets and
        hadoopfs work) covers HDFS and S3 for Paimon and Hudi COW / compacted MOR. Q7 updated.
    - Step 4 (b) Hudi on S3 done (2026-10-29): the lockdown probe's `hudi_option` checks used `,` and JSON, but
      duckdb-hudi 0.3.1 (alitrack/duckdb-hudi, hudi-rs 0.5.0 kernel) splits `hudi_option` on `;` (`rust/src/
      kernel.rs`); keys other than `hoodie.*` go to object_store as storage options. With `aws_endpoint_url=http://
      host:port; aws_access_key_id=..; aws_secret_access_key=..; aws_region=..; aws_allow_http=true` **every Hudi
      fixture on S3 answers equal to Spark after the lockdown** (both versions, COW, INSERT OVERWRITE, inflight,
      uncompacted and compacted MOR, partition filter) - run 40823753558. The README's `hoodie.s3.access_key_id` /
      `hoodie.aws.region` names are not honoured (back to the metadata service). Security: credentials travel in the
      SQL text (a plugin would write them into server-generated query text, so query logging / EXPLAIN / error
      messages must never show it), and the same options read `s3://lake/outside/...` outside `allowed_directories`
      (GETS THROUGH) and contact any endpoint client SQL names - so `read_hudi` must be refused in client SQL as
      already planned; DuckDB secrets are still ignored. Consequence for Q7: Hudi via the extension covers local +
      S3 too (MOR with logs included), only HDFS needs the light reader.
    - **Next for C4 (b):** waits on Q7 (route). Also store both extensions in ci-assets (from the artifact; the
      session cannot download them). (d) full ACID ORC still waits on Q6. With C1 2b, C2, C3 and C4 all waiting on
      answers (Q1, Q4-Q7), a next session without answers has no unblocked item; the Hudi-on-S3 question is answered
      (above). Possible test-only extras: `hudi_info` / `hudi_files` with options (their signatures take none),
      or whether credentials in `hudi_option` show up in DuckDB error text (they did not in run 40823753558's
      refusals - masked check only). Otherwise stop early.


## Open questions

- **Closed (Q-40b-1 done 2f31c071, maintainer yes 2026-11-11).** Q-40b-1 (2026-11-10, item 40 (b)): the speed (and balanced) c1 point-query regression on local data is file pruning
  by footer statistics (on by default since item 39 (c)): +0.3..0.5 ms p50 / +0.3 ms CPU per point query at c1 on a
  local `dataDir`, gone with `-DfileStatsPruningMaxFiles=0` (run 42811208123). Turn pruning off by default for a local
  `dataDir` and keep it on for HDFS / S3 / plugin stores (an explicit setting wins)? Same answers either way; it is a
  default change, so not done without you.
  Update 2026-11-10 (6): most of the cost was the server's DoGet pruning twice plus a second parse check (fixed in
  3a01265e without a default change): c1 p50 is now at or below deafc861 in both profiles (run 42818686506); pruning
  off is still ~0.25 ms p50 faster on local files, so the question stays open but is no longer needed to remove the
  regression.
  Update 2026-11-10 (7): the rest was one jOOQ parse per point text in DoGet's pruning, now one per shape (4228c733):
  pruning on is level with pruning off at c1 on local files (run 42826465868). Proposal: close Q-40b-1 without a
  default change unless you see a gap on the harness.
  Update 2026-11-10 (9): with pruning on (default) main is ~10 % below deafc861 at c1 p50 on local files (runs
  42843856272, 42843860772); still proposed for closing without a default change.

- Q11 (2026-11-09, item 39 (c)): in the HDFS point cell the balanced profile asks the DataNode for short-circuit fds
  ~5-6 times per point query (each per-thread DuckDB's libhdfs3 asks again); `duckDbExternalFileCache=true` brings
  that to 0-0.2 and q/s +10..15% (run 42689843600: balanced c32 65.6 -> 75.9 and 52.8 -> 60.6). Make the external
  file cache the balanced default on HDFS (it is a default change, so not done without you), after checking that
  hadoopfs reports real modification times (stale cached bytes otherwise; afc4 promises correct mtimes)?
- **Closed (maintainer 2026-11-11: keep pruning on for HDFS/S3).** Q10 (2026-11-09, item 39 (c) part 3): file pruning by footer statistics is **on by default**
  (`fileStatsPruningMaxFiles=1024`). Answers are unchanged by design (40-query equality test), but it changes which
  files a complete answer reads. Keep it on, or make it opt-in (`0` default) until the stand has run with it?
- **Answer Q9 (maintainer, 2026-11-09 18:20): yes.** The hadoopfs side is now its own item in the extension's repo
  (P1: process-wide file-status + located-blocks cache with a TTL setting `hdfs_metadata_cache_ttl_ms`, reuse of the
  open stream across row-group reads, correct modification times; build for DuckDB v1.5.6 as `51b7220-afc4`,
  published as a release there). Do both: the server-side file pruning by footer statistics here (your next step 1)
  now, and when the afc4 build exists, take it into `ci-assets`, set `hdfs_metadata_cache_ttl_ms` from a server
  setting (default e.g. 30 s on HDFS) and measure opens/RPCs per query again on the cell.
- Q9 (2026-11-09, item 39 (c)): DuckDB's hadoopfs extension does one NameNode `getfileinfo` and one `open`
  (getBlockLocations) per Parquet file per row-group scan (~5 per file per point query in the HDFS cell, run
  42637704787). Should the extension (our build in `ci-assets`) cache file status and located blocks per file for a
  short time? It is outside this repository's sources; the server-side lever (file pruning by footer statistics) is
  next in this lane either way.

- **Maintainer answers 2026-10-29 (Q1, Q4-Q7):**
  - Q1: yes, lane 4 adds the per-table temporary S3 secrets for vended credentials (opt-in, off by default, own
    commits, a short security note in the Log, tests that a secret's scope cannot reach outside its table's root and
    values never appear in logs); a `STATUS.md` Log line so the core lane knows.
  - Q4: (a) yes - the jdbc plugin refuses the scanners' functions itself, with the test against the bundled build.
    Also add (b) as a safety net, active only when a plugin loads an extension (diff of `duckdb_functions()`
    before/after, refused in client SQL), own commit + `STATUS.md` Log line. Federation plugin may then be built.
  - Q5: yes, build `flight-plugin-delta` on that basis (opt-in plugin, all added functions refused, the test that the
    list covers the bundled build, plus the Q4 (b) net).
  - Q6: **(a) full ACID ORC first** - production Hive 3 managed tables are full ACID ORC by default, so this matters
    more than Paimon/Hudi. It touches the hive module of the plugins lane: keep it in new classes, small commits,
    `git pull --rebase` often, and a line in PLUGINS-STATUS Log when you start. Then (b).
  - Q7: route (a) for Paimon and Hudi on local + S3 through the extensions with refused functions (as Q5); HDFS via
    (b) only later if asked. After Q6 (a).

- **Closed (maintainer 2026-11-11: fix it -> item 47, done 2c169a53).** Q8 (2026-10-29, C2, security): an attached database is a DuckDB catalog, so client queries of
  `information_schema.tables` / `information_schema.columns` / `duckdb_tables()` / `duckdb_columns()` probably list
  every table and column of the remote database the plugin's user can see, also the ones not configured in
  `<s>.tables` (names only - their rows stay unreachable: the attachment's name is refused as a qualifier and the
  scanners' functions are refused). Not yet checked in CI. Options: (a) accept (document: give the plugin a
  database user that sees only the configured tables - the usual practice anyway); (b) the core filters attached
  catalogs out of those system views for client queries (wider change to the parse check). Lane 4 proposes (a)
  plus a CI assertion of what is visible; tell me if (b) is wanted.

- Q1 (2026-10-28, C1 step 2b): vended credentials need the server to install per-table temporary S3 secrets on
  DuckDB connections after lockdown (`CREATE OR REPLACE TEMPORARY SECRET`, scope checked below the catalog's
  roots, values redacted), fed by a new optional field on the catalog SPI. It touches the DuckDB lockdown
  (security), so: may lane 4 add this (opt-in, off by default, own commits), or should the core lane own the server
  part? Until answered, lane 4 continues with step 3 and C2 and leaves 2b's core part unbuilt.

- Q4 (2026-10-28, C2 step 2, security): the postgres / mysql scanner extensions add table functions that
  get through the DuckDB lockdown: `postgres_query` / `mysql_query` (any SELECT on the remote database with the
  plugin's credentials) and `postgres_scan('<dsn>', ...)` (connects to any host). A federation plugin needs them
  refused in client SQL. Proposal: the server refuses every function a plugin's `LOAD` adds (diff of
  `duckdb_functions()` before/after the plugin's DuckDB setup, joined to `REFUSED_FUNCTIONS`), opt-in only by
  installing such a plugin; no change for existing setups. May lane 4 add this to the parse check (own commit,
  STATUS.md Log line), or should the core lane own it? Until answered, lane 4 builds no plugin that loads these
  extensions into query connections.
  Addendum (2026-10-28): the SPI already has `DuckDbSetup.refuseFunctions(String...)` ("the server's own list
  can only grow"), so a plugin can refuse the functions its extension adds without any core change; the
  automatic `duckdb_functions()` diff would only be a safety net against a future extension version adding
  one. Option (a): the jdbc plugin refuses the scanners' functions itself (plugin-only, plus a test that its
  list equals the diff of the bundled build). Option (b): the core diff as proposed. Lane 4 would go with (a)
  unless told otherwise, but still waits for a yes because it loads an extension with known lockdown gaps.

- Q5 (2026-10-28, C3 step 2, security): DuckDB's `delta` extension v1.5.6 adds functions that get past the
  lockdown (C3 step 1 results): `write_blob` / `copy_dir` write files anywhere, and `delta_scan` /
  `delta_list_files` read Delta logs outside `allowed_directories` and connect to any http(s) host or the
  instance metadata service. A Delta plugin would load it into every query connection and refuse all eleven
  added functions in client SQL through the existing `DuckDbSetup.refuseFunctions` (server-written table
  sources only; plugin-only, no SPI/core change), with a test that the list covers the bundled build. May lane
  4 build `flight-plugin-delta` on that basis (opt-in: only active when the plugin is installed), or should the
  parse check refuse extension-added functions generically first (Q4 option b)? Until answered, lane 4 does
  not load `delta` into server connections.

- Q6 (2026-10-29, C4 (d)): after insert-only tables (done, checked against a real Hive 4), which next: (a) full
  ACID ORC tables (`delete_delta` handling by ROW__ID - (originalTransaction, bucket, rowId) - anti-join in the
  plugin or in DuckDB; a sizeable new read path in the plugins lane's hive module), or (b) Paimon / Hudi read-only
  (first a probe of what DuckDB 1.5.6 extensions or a light Java reader can do, as for Delta in C3 step 1)?
  Without an answer lane 4 starts with (b)'s probe (test/CI only), since (a) changes the hive module's scope.

- Q7 (2026-10-29, C4 (b)): Paimon / Hudi read-only - which route? (a) a plugin on DuckDB's `paimon` / `hudi`
  extensions (correct on every probed table incl. Hudi MOR; security as for `delta`, Q5: refuse their functions in
  client SQL, pending the lockdown probe), or (b) a light reader in a plugin (no extension: Paimon append / PK /
  DV tables and Hudi COW; Hudi MOR with logs refused; PK merge-on-read needs a dedup query over the files, which
  the SPI's file + delete-file model cannot express - would need either a `CatalogView`-like SQL over files (SPI
  change) or PK tables limited to DV mode). Lane 4 would prefer (a) if the lockdown probe is clean, else (b) for
  COW / append / DV tables. No plugin is built until answered.
  Update (2026-10-29, lockdown probe done, C4 notes): neither extension writes files, but both read local files
  outside `allowed_directories` (all five functions, plus `ATTACH ... (TYPE paimon)`), and hudi calls the cloud
  metadata services for `s3://` / `gs://`. Paimon via the extension reads local + S3 (own `paimon` secret type),
  Hudi via the extension local only; neither reads HDFS. Revised proposal: (a) for Paimon (plugin refuses the
  extension's functions in client SQL, writes `paimon_scan` sources itself, installs a scoped `paimon` secret for
  S3 roots), (b) light reader for Hudi COW / compacted MOR (works on S3 and HDFS; MOR with logs refused). Same
  security footing as Q5 (an extension with its own I/O in every query connection, closed only by the refused
  list).
  Update (2026-10-29): Hudi on S3 works through the extension with `hudi_option` storage options (`;`-separated;
  the earlier probe's separator was wrong), all fixtures equal to Spark. So route (a) covers Paimon and Hudi on
  local + S3 (incl. Hudi MOR with logs); only HDFS needs (b). Hudi's credentials go in the generated SQL text
  (keep it out of logs), not in a DuckDB secret.

- Q2 (2026-10-28, for lane 3 / maintainer, not lane 4's code): `DistributedClusterIntegrationTest` is flaky on
  `main` since D2 (c) part 2 (c7e436e): `peerStreamCutBeforeTheFirstBatch...:187` and
  `peerStreamCutDuringTheAnswer...:212` fail with `mergedAnswers()` 1 instead of 0 in 2 of 3 local runs and in Dev CI
  runs 40692394131 / 40693932375. Likely cause: a relay cut before the schema now fails over to the endpoint's backup location, which
  bypasses the cutting relay, so the answer is merged (correct rows) instead of falling back. Lane 4 leaves it to
  lane 3.

- Q3 (2026-10-28, for the core lane, not lane 4's code): one table a catalog cannot load makes all of that
  catalog's schemas unavailable. `CatalogViews` refresh: `getTables(schema)` fails on Unity Catalog's sample table
  `default.marksheet_uniform` (HTTP 500 from loadTable), the catalog entry goes into `failed`, so `rest_ice.*`
  (healthy tables in another namespace) answer `UNAVAILABLE ... marksheet_uniform` too. An outage of the whole
  catalog and one broken table are not told apart. Proposal: keep the catalog-wide memo for connection-level errors
  only and skip (log, list as unavailable) a single table whose load fails, so its siblings stay queryable. This
  changes existing behaviour, so it is the core lane's / the maintainer's call. Reproduce: `catalogs-ci.yml` entry
  `unity-catalog` without `ICEBERG_REST_IT_NAMESPACES`.
  Addendum (2026-10-29): the same pattern hit the hive plugin's refused insert-only layouts (fixed plugin-side in
  C4 step 3 by throwing a runtime exception, which `catalogTableViews` treats as a table-level skip). Other hive
  plugin table-level refusals are still IOExceptions and so still hide their siblings: a text / Avro partition
  with its own serde or format (`checkTextPartition`, `checkAvroPartition`). Lane 4 could switch those to the
  same `UnreadableTableException` (plugin-only), but that changes existing behaviour of the plugins lane's
  module -> maintainer / plugins lane to decide. Also: a skipped table answers complete queries with DuckDB's
  "Table with name X does not exist!" (NOT_FOUND); naming the reason would need CatalogViews to keep per-table
  reasons (core lane).

## Log

- 2026-10-28 (maintainer): lane created; C1-C4.
- 2026-10-28: C1 step 1 - Iceberg REST conformance test against Apache Polaris 1.2.0 (OAuth2) and the Iceberg REST
  fixture 1.10.0 in the new workflow `catalogs-ci.yml`; all checks equal to the Hadoop-layout reference (names,
  columns, files, row/delete counts, partial-read problems, DuckDB rows). Test only. Catalogs CI green: run
  40670485477 (earlier failures 40669539591, 40669821302, 40670292342 were CI setup: container uid/permissions,
  DuckDB setting). Dev CI green: run 40672085389 (head c258adf, includes these commits). No open questions.
- 2026-10-28: C1 step 2a - REST conformance on S3 warehouses (static keys): polaris-s3, iceberg-rest-fixture-s3,
  lakekeeper-s3 added to `catalogs-ci.yml` (S3 server: moto; `quay.io/minio/minio` now refuses anonymous pulls,
  first run 40677711018 failed on that). Test only. Catalogs CI green: run 40677907066 (all 5 entries, 3/3 tests
  each). Dev CI green: run 40677906918 (head bfa1912). Step 2b analysed (DuckDB accepts `CREATE OR REPLACE
  TEMPORARY SECRET` after lockdown); its core part waits on Open question Q1.
- 2026-10-28: C1 step 3a - answers through the server for REST catalog tables (local warehouses):
  `RestCatalogServerConformanceTest`, run by `catalogs-ci.yml` for polaris and iceberg-rest-fixture. Test only.
  Catalogs CI green: run 40692394116 (all 5 entries; the 2 file entries run the new test). Dev CI runs 40692394131 and
  40693932375 (head 207b74c): 912 tests, the only failure `DistributedClusterIntegrationTest` (lane 3, intermittent,
  not this change), see Open questions Q2.
- 2026-10-28: C1 steps 3b + 3c - `RestCatalogServerConformanceTest` on the S3 entries (s3 plugin in the server,
  S3FileIO writer) and an HMS reference (REST metadata registered in the in-process metastore, local warehouses).
  Test only. Catalogs CI green: runs 40712157558 (3b, all 5 entries) and 40712744584 (3c, all 5 entries). Dev CI:
  run 40712744776 green (head 2cfa23e). No new open questions.
- 2026-10-28: C1 step 4 - Unity Catalog OSS 0.7.0 as an Iceberg REST catalog: `catalogs-ci.yml` entry
  `unity-catalog` (managed tables on a local storage root); fixtures can leave columns out
  (`ICEBERG_REST_IT_OMIT_COLUMNS`, UC refuses `time`), server test can scope namespaces
  (`ICEBERG_REST_IT_NAMESPACES`). Test only. Catalogs CI green: run 40719031624 (all 6 entries). Dev CI: run
  40719031475 (2498dd4) cancelled by a newer push; run 40720127635 (head 9efe670, includes it) green. New open
  question Q3 (one unloadable table makes a whole catalog unavailable - core lane). C1 done apart from 2b (Q1);
  next: C2.
- 2026-10-28: C2 step 1 - probe of DuckDB's postgres / mysql scanners v1.5.6 under the server's lockdown
  (`catalogs-ci.yml` job `jdbc-scanners`, `tools/ci/DuckDbScannerProbe.java`). Test only. Reads, projection and
  filter pushdown work after the lockdown; `*_query` pass-through and `postgres_scan('<dsn>')` also get through
  -> new open question Q4 before a plugin is built. Catalogs CI: runs 40730338121 (probe bug: a failed check
  closed the shared statement), 40730719713, 40731100450 (final probe; all 7 jobs green).
- 2026-10-28: C3 step 1 - probe of DuckDB's delta extension v1.5.6 on Spark-written Delta tables under the
  server's lockdown (`catalogs-ci.yml` job `delta`, `tools/ci/delta-fixtures.py`, `tools/ci/DuckDbDeltaProbe.java`).
  Test only. Every Delta feature checked (DVs, column mapping name/id, checkpoints, time travel, partitions,
  evolution, types) reads equal to Spark, local and S3; but `write_blob` / `copy_dir` write anywhere and
  delta-kernel's own I/O reads logs / connects outside the lockdown -> new open question Q5; Q4 addendum (the SPI's
  existing `refuseFunctions`). Catalogs CI: runs 40750693235, 40751237504, 40751817639 (delta job green in each,
  later runs add checks; 40751817639 all 8 jobs green). Dev CI: push runs cancelled by newer pushes; run
  40751852380 (workflow_dispatch, head 0f8eb5e) still running at session end (change is tools/ci + workflow only).
- 2026-10-28: C3 Ozone part - Apache Ozone 2.0.0's S3 gateway as an S3 backend (`tools/ci/ozone-s3.sh`,
  `catalogs-ci.yml` entry `iceberg-rest-fixture-ozone-s3`): both REST conformance tests pass on it unchanged. Test
  only. Catalogs CI green: run 40770759266 (all 9 jobs). Dev CI run 40770759218 (head c29bb31) still running at
  session end (change is tools/ci + workflow only). Q1, Q4, Q5 still open (no answer yet), so C1 step 2b, C2 step 2
  and the Delta plugin stay unbuilt; C4 waits on P4 (plugins lane, views part (d) next). Next: Delta-on-HDFS probe
  (C3 note above).
- 2026-10-28: C3 Delta-on-HDFS probe - single-node HDFS in the `delta` job (`tools/ci/hdfs-single.sh`), probe with
  hadoopfs in an Ubuntu 20.04 JDK container: delta v1.5.6 cannot read `hdfs://` tables (delta-kernel has no HDFS
  object store; hadoopfs reads the Parquet files). Test only. Catalogs CI green: run 40784918475 (all 9 jobs). Dev
  CI run 40784918326 (head 77babce) still running at session end (change is tools/ci + workflow only). Q1, Q4, Q5
  still unanswered: C1 step 2b, C2 step 2 and C3 step 2 stay unbuilt. P4 (plugins lane) is closed, so C4 is
  unblocked: next item.
- 2026-10-28: C4 steps 1 + 2 - Hive insert-only (MM) tables in the hive plugin behind the new optional setting
  `hive.insertOnlyTables` (default false, additive): `InsertOnlyDirectories` (Hive's getAcidState rules, ambiguous
  layouts refused) + metastore snapshot + listing; `InsertOnlyDirectoriesTest` (9), `HiveInsertOnlyTableTest` (4,
  in-process HMS with txn tables). Module `flight-plugin-hive` (plugins lane's module; C4 is lane 4's item):
  100 tests, checkstyle, spotbugs clean locally (`HiveCatalogTest` needs a UTF-8 locale for its path test).
  CI green: Dev CI 40787036947 and Catalogs CI 40787036921 (head 9b231bf). Pre-release: dispatch 40788225372
  (package=true) was cancelled by the plugins lane's push 5e9ac98 (same concurrency group); the next package=true
  run on main carries C4 - dispatch one next session if none exists by then. Next: C4 step 3.
- 2026-10-29: C4 step 3 (a) + ORC part of (b) - `HiveInsertOnlyIntegrationTest` (insert-only Parquet, partitioned
  and ORC tables through the server, complete = partial = committed rows, commit / INSERT OVERWRITE visibility,
  `delete_delta` refused). It found a bug in step 2: a refused layout was an IOException, so the server's views
  refresh treated the metastore as down and left out every later table; fixed in the hive plugin
  (`UnreadableTableException`, only that table skipped) plus a small core commit (63679fe,
  `catalogCompleteReason` catches RuntimeException; STATUS.md Log line). Locally: hive plugin 100 tests +
  checkstyle + spotbugs, server Hive ITs + QueryPlannerTest 90 tests. CI green on head 01884e2: Dev CI 40798070064,
  Catalogs CI 40798070095. Pre-release: Dev CI dispatch 40799537000 (package=true) running at session end (also
  carries C4 step 2; if another lane's push cancels it, dispatch again). Q3 addendum (other hive refusals still
  hide siblings). Q1, Q4, Q5 unanswered. Next: C4 step 3 rest (text insert-only, real Hive writer in CI).
- 2026-10-29: C4 step 3 (b, text) + (c) - insert-only text tables in `HiveInsertOnlyIntegrationTest` (5/5
  locally), and tables written by a real Hive 4.0.1 (`tools/ci/hive-mm-writer.sh`: inserts, failed inserts,
  INSERT OVERWRITE, major / minor compaction, gzip text; metastore started on the same Derby) read through the
  server by `HiveRealWriterIntegrationTest`: equal to Hive's answers in both modes. Test + CI only, no plugin
  change needed. Catalogs CI: 40801018004 green (b); 40802647788 and 40804099011 failed in the new job
  `hive-writer` (Hive 4 answers nothing for MM tables below the runner's `_temp` directory - see C4 notes),
  40805065862 green (all 10 jobs). Dev CI green: 40805065855 (head 6558c3d). Pre-release (package=true, also
  carries C4 step 2): dispatch 40806577145 (head 7d0826f) running at session end - if another lane's push
  cancels it, dispatch again. Q1, Q4, Q5 still unanswered. Next: C4 (d) needs a decision (full ACID vs
  Paimon/Hudi) - see C4 "Next"; asked as Q6.
- 2026-10-29: C4 (b) probe (Q6 default) - Catalogs CI job `paimon-hudi` (`tools/ci/paimon-hudi-fixtures.py`,
  `tools/ci/paimon-hudi-probe.py`): Paimon 1.2.0 and Hudi 0.15.0 / 1.0.2 tables written by Spark 3.5.3. DuckDB
  v1.5.6 has `paimon` and `hudi` extensions, both equal to Spark on every table; a light reader (metadata +
  `read_parquet`) is equal on all Paimon tables and Hudi COW / compacted MOR, refuses MOR with logs. Test only.
  Catalogs CI: runs 40810022672 and 40810359864 (`paimon-hudi` green in both). Dev CI: push runs 40810022826 /
  later (tools/ci + workflow only). New open question Q7 (route for Paimon / Hudi). Q1, Q4, Q5, Q6 unanswered.
  Next: lockdown probe of the two extensions.
- 2026-10-29: C4 (b) lockdown probe - `tools/ci/DuckDbPaimonHudiProbe.java` in the `paimon-hudi` job: paimon / hudi
  v1.5.6 under the server's lockdown answer every local fixture equal to Spark; paimon reads S3 with its own
  `paimon` secret, hudi cannot read S3 (goes to the metadata service), neither reads HDFS; both read local files
  outside `allowed_directories`, no write functions. Test only. Catalogs CI: run 40820331670 (paimon-hudi green,
  probe flaw: a lazy paimon ATTACH on S3/HDFS broke later lookups, outside copies in the wrong layout), run
  40821054886 (fixed probe; all jobs green). Dev CI push runs 40820331658 / 40821054924 cancelled by newer pushes; dispatch 40821204741 (head 06833e7, includes this) running at session end (tools/ci + workflow only). Q7
  updated. No new pre-release needed: the last runtime change of lane 4 (hive plugin) is in pre-release
  `server-1.0.0-01884e287668`; later C4 work is test-only. Q1, Q4-Q7 unanswered.
- 2026-10-29: C4 (b) Hudi on S3 - the lockdown probe's `hudi_option` used the wrong separator; with `;`-separated
  object_store keys `read_hudi` answers every Hudi fixture on S3 equal to Spark after the lockdown, and the same
  options also reach S3 paths outside `allowed_directories` (refuse `read_hudi` in client SQL, as planned). Test
  only. Catalogs CI green: run 40823753558 (all jobs). Dev CI run 40823753684 (head 37fe039) running at session end
  (tools/ci only). Q7 updated. Q1, Q4-Q7 unanswered; no unblocked item left for lane 4.
- 2026-10-29 (06:5x UTC): no answers to Q1, Q4-Q7 yet, so no unblocked item; stopped early without code changes.
  Maintainer notified once that lane 4 is idle until they are answered; later sessions: stop early silently until
  an answer appears (no further notifications).
- 2026-10-29 (FINISH MODE session): **C1 step 2b (ii) done** - iceberg-rest reads manifests with the vended S3
  credentials (`VendedS3` SigV4 reader in the plugin, af1fb7ff); Catalogs CI 40862720044 green (12 jobs, the vended
  Lakekeeper entry with a store refusing every read). **C2 step 2 done** - SPI 1.9 (14e86e09) + core 22e90670
  (extension functions refused by diff, Q4 (b)); SPI 1.10 (f4bb3699) + core 04b60f7b, 19acce48 (attached databases,
  `ReadSpec.AttachedTable`, root-less catalogs that attach); new module `flight-plugin-jdbc` (dbef061e); CI
  3768feba: Catalogs CI 40865845835 job `jdbc-scanners` green with `JdbcFederationIntegrationTest` 3/3 and
  `ScannerFunctionsTest` 2/2 against PostgreSQL 16.4 / MySQL 8.4. STATUS.md Log lines for the core lane. Dev CI
  40865845870 (head 3768feba) running at the time of writing - see the next Log line. New Q8 (remote table names in
  system views). Pre-release (package=true) pending a green Dev CI.
- 2026-10-29 (same session, end): Dev CI 40865845870 failed on `GoldenDuckDbSetupTest` (my LockdownCheck added a
  query for every setup); fixed by a6a6c868 (the name query runs only when a declared secret has no scope - setups
  without attachments run exactly the old statements); the core lane's golden regeneration c75715ab crossed it and
  was reverted by them (6ddebf26). TPC-H SF0.1 added (C2 step 3). Final: Dev CI 40868732917 and Catalogs CI
  40868732813 green on 1e7b43a4. Pre-release: Dev CI dispatch 40870896985 (package=true, head 1e7b43a4, plugin set
  now includes `jdbc`) green: pre-release `server-1.0.0-1e7b43a4e0c7` (7 assets).
  **CATALOGS LANE DONE.**
- 2026-11-02: **SERVICE MATRIX, stores part** - `service-stores.yml` + `tools/ci/service-tpch.py` (TPC-H data,
  reference answers, ADBC check), `service-stores-cell.sh`, `service-iceberg.py`. Runs: 41470097544 (first, 2/12:
  AWS CLI 2.23+ trailer checksums stored raw by Ozone / refused by lakeFS -> `AWS_REQUEST_CHECKSUM_CALCULATION=
  when_required`; MinIO bucket name too short; MySQL entrypoint chowns the CSV mount), 41471591465 (14/14),
  41473152168 (16/16 with iceberg-rest, build of 880caeba), 41473218719 (dispatch, pre-release
  server-1.0.0-fca6512a8046, 16/16). Catalogs CI's `paimon-hudi` job broke on the runners' Python 3.14
  (`backports.zstd` has no 3.14 build; fastavro uses the stdlib `compression.zstd` there): requirement marker,
  Catalogs CI 41471784761 green. No server, plugin or SPI change. Lock released.
- 2026-11-09: **ITEM 39(c) part 1** (branch `fix/39c-point`, merged into `main`): catalog views refresh without
  blocking other queries; setting `duckDbParquetMetadataCache` (default off); workflow `hdfs-point-perf.yml`. Unit
  tests `CatalogViewsTest.oldViewsServeOtherQueriesWhileOneRefreshes`, `ConfigAdapterTest.duckDbParquetMetadataCache*`.
  Dev CI 42623741690 green on the branch (6bc58cda). HDFS point perf: 42621322306 / 42621870860 (cell setup: the
  server JVM crashed with the native Hadoop library, cell runs without it), 42622519461 (baseline: balanced 42/57/53,
  speed 71/111/119 q/s at c1/c8/c32, both CPU-bound on 2 CPUs), 42623710991 (one runner per variant: too noisy),
  42624683955 (every variant on two runners, c32 a/b: balanced 42.2/49.4, +file cache 50.1/60.4, +metadata cache
  45.8/53.5, speed 72.5/84.9, +metadata cache 76.9/88.3), 42627272084 (rebased head 3b5512be, per-query HDFS
  counters: ~20 NameNode opens per query, balanced 7-8 short-circuit fd requests per query, speed 0). Dev CI
  42627268633 green on the rebased head; 42630358181 red only on the core lane's
  `PlainNameSchemaIntegrationTest` (red on `main` too, fixed there by 655bec3a); 42632180687 green after rebasing on it. Merged into `main` (fast-forward, head 63a3ec52); Dev CI
  42635429611 (dispatch on `main`) green. Not DONE: see the ITEM 39(c) note in Next steps. Lock released.
- 2026-11-09 (2): **ITEM 39(c) part 2** - `FooterStubCache`: partial-mode describes of remote tables reuse a local
  footer stub per sample file version (SASL describes use it too). Tests `FooterStubCacheTest`,
  `ExecutionServiceTest.describesOfRemoteTablesReuseOneFooterStub` (+ the SASL describe test now expects the stub
  kept). `hdfs-point-cell.sh` prints NameNode audit commands per query and opens per file. Branch runs: Dev CI
  42637718051 green (b08fdb19), HDFS point perf 42637704787 (all opens are DuckDB's: ~5 per file per query + one
  getfileinfo each; balanced c32 a/b 45.0/52.9 q/s, speed 74.8/87.9; complete mode, so part 2 not visible there).
  Merged into `main` by cherry-pick (34b7911a, 2b61650d; no force push to the branch). Dev CI on main: 42641096583 green.
  Not DONE: next is file pruning by footer statistics (Next steps). Lock released.

- 2026-11-09 18:58Z (orchestrator): released stale `LOCK-CATALOGS` (15:23:58Z, no catalogs commit since then). ITEM 39(c) still open (next: file pruning by footer statistics; Q9 answered).
- 2026-11-09 (3): **ITEM 39(c) part 3** - file pruning by footer statistics for complete answers of plain
  single-table queries over directory tables (`FileStatsPruner`, `CatalogViews.prunedFiles`; setting
  `fileStatsPruningMaxFiles`, default 1024, Q10). Core files touched (39 (c) exception): `CatalogViews`,
  `ExecutionService.readComplete`, `ConfigAdapter`. Tests `FileStatsPrunerTest` (5), `ConfigAdapterTest` (+1).
  Branch `fix/39c-point` (merged origin/main, no force push): Dev CI 42684272737 green, HDFS point perf 42684276829
  green but no change in opens (mod-split data, see Next steps); data script `range` split + `-nopruning` variants,
  HDFS point perf 42689843600 green (opens per query ~20 -> ~11, balanced c32 +12-16 % q/s; see Next steps).
  Cherry-picked into `main` (b0f82a47, 32ebda26); Dev CI on main 42690037660 green. Not DONE yet. Lock released.
- 2026-11-09 (4): **ITEM 39(c) part 4** - file pruning also for aliased single-table queries (`FROM s.t x WHERE
  x.k = 5`, `AS x`, quoted aliases; `CatalogViews.singleTableFilter(sql, reference, aliases)` + `aliasOf`; stream
  hints unchanged). Tests `FileStatsPrunerTest` (+8 queries). Read HDFS point perf 42689843600: opens per query
  ~20 -> ~11, q/s +3..15% vs `-nopruning` (Next steps). Branch `fix/39c-point` (merged origin/main, no force push):
  Dev CI 42693013191 green; merged into `main` as bf72114c. New Open question Q11 (balanced external file cache).
ITEM 39(c) DONE bf72114c (parts 1-4 on `main`: b0f82a47, 32ebda26, 34b7911a and part 1 before; follow-ups (a)-(d) in
  Next steps, none blocking). The core lane can package (`package=true`) and measure on the stand. Lock released.
- 2026-11-09 21:25Z: no open item for this lane. ITEM 39(c) follow-ups (a) Q9/afc4 and (b) Q11 were done by the core
  lane (877cf100); (c)/(d) wait for the stand. Item 40 is split between the plugins and core lanes. Lane stays in
  finish mode (CATALOGS LANE DONE); no code change, lock not taken.
- 2026-11-10 (1): **ITEM 40(b)** started (profiling): local c1 per-call numbers, DuckDB vs server time,
  JFR breakdown, external file cache check (see the 40(b) state note in Next steps). New tool
  `tools/ci/small-query-rpc.py` on branch `fix/40b-small` (68dc9ea9). No server change, so no CI run. Lock released.
- 2026-11-10 (2): **ITEM 40(b)** first improvement: the parse check finds query names with one `indexOf` scan instead
  of two regular expressions (20 us -> 0.6 us per new text, same names/order/refusals; equivalence test against the
  old expressions over DuckDB's own JSON). Branch `fix/40b-small` Dev CI green: run 42784271237 (29dbaf18); merged
  into `main` as 48adb2ac (cherry-pick on top of item 42), Dev CI on main run 42786787681 (head 5fc352be) is red, but only on `GoldenDuckDbSetupTest` from core-lane commit 004d844e (written bug-first into `STATUS.md` Log); every other test passed, and the branch run with the same change was green. End to end within noise
  (c1 per call and c8 ADBC load, see the 40(b) state note). New profile numbers and the next three cuts are in that
  note. Not DONE yet. Branch `fix/40b-small` now = main + `tools/ci/small-query-rpc.py` (merge commit f8f39197; a
  force-push to rebase it was refused, so main was merged in instead). Lock released.
- 2026-11-10 (3): **ITEM 40(b)** second improvement: DoGet accepts the pruned text of a point query without a second
  DuckDB parse check when DuckDB's parse of the described text put a table reference at the replaced span
  (`query_location`); tests `DuckDbAdapterTest` (+2), `FileStatsPrunerTest` (every pruned query: accepted, one
  statement, names within original + `read_parquet` + `list_value`). Branch `fix/40b-small-2` Dev CI 42791067194 green;
  HDFS point perf branch 42791070774 vs main 42791075306 within noise (state note). Merged into `main` as 345627f1
  (Dev CI on main: 42794210815 / 42794491688 / 42794664721 cancelled by newer pushes of other lanes; 42794847349 on
  head a8ded50d, which includes it and Q-42, green). Not DONE yet. Lock released.
- 2026-11-10 (4): **ITEM 40(b)** third improvement: measured that each point query is parsed by jOOQ once already
  (the later parses of a request hit the parse cache), but every `ParquetQueryParser.parse` call re-ran the text
  preparation (`castTypedLiterals` built a regex Matcher per character, `flattenSubqueryWrapper` compiled its
  pattern). Now a raw-text cache in front of it + one matcher tried at `t`/`T` only; JFR samples under `parse()`
  39 -> 25, end to end within noise (DuckDB-bound VM). Tests in `ParquetQueryParserUnitTest` (+2: equivalence with
  the old rewrite over 5 000 random texts, raw-cache identity / refusals repeated). Branch `fix/40b-small-3` Dev CI
  42800078539 green; merged into `main` as 388b81f5, Dev CI on main 42802352922 green. Not DONE yet; next cuts in
  the state note. Lock released.
- 2026-11-10 (5): **ITEM 40(b)** speed c1 regression bisected with `small-query-perf.yml` (new input `bothOptions`):
  cause is footer-statistics file pruning on local data (b0f82a47), both profiles; `-DfileStatsPruningMaxFiles=0`
  restores deafc861's c1; b0f82a47 alone +0.65..0.95 ms, its parent 34b7911a = baseline (runs 42811204309, 42811196902,
  42806968672, 42806974143, 42810133238-42810174324, 42811189839, 42811208123). Fix is a
  default change -> Q-40b-1. Merged two cuts from `fix/40b-small-4` (Dev CI 42808268971 green): a4e14e03 pruning reuses
  the parser's WHERE condition; ac2055c8 `DescribeKey` (one describe for point queries differing in their key; tests
  `DescribeKeyTest` incl. a bind check over every column type, `FileStatsPrunerTest` +2). CHANGELOG lines. Not DONE.
  Lock released.
- 2026-11-10 (6): **ITEM 40(b)** the speed / balanced c1 regression of footer-statistics pruning on local data cut
  without a default change (3a01265e, branch `fix/40b-small-5`): DoGet's no-local-copies path pruned without
  `acceptRewrite` (full DuckDB parse check on the pruned text) and `readComplete` pruned again; now one pruning and the
  rewrite accepted (`CopiedQuery.pruningDone`). Found with JFR at 1 ms sampling; `PointQueryOverheadPerfTest` complete
  point DoGet p50 4.17 -> 3.76 ms; `small-query-perf.yml` run 42818686506: c1 p50 at or below deafc861 in both
  profiles (balanced quota 4.24 vs 4.44 ms). Dev CI branch 42818682745 green, Dev CI on main 42822143053 green. Test
  `FileStatsPrunerTest` +1, CHANGELOG line, Q-40b-1 updated (still open, optional now). Not DONE yet. Lock released.
- 2026-11-10 (7): item 40(b) - pruned-vs-view gap on local files cut (4228c733, merged into `main` fast-forward
  after Dev CI on `fix/40b-small-6` 42826441121 green). Plain-DuckDB microbenchmark with the server's settings showed
  the pruned `read_parquet` text is faster than the view; JFR showed DoGet's pruning parsing every point text with jOOQ
  (~300 of 3446 DoGet samples). Now one parse per `DescribeKey` shape with stand-in literals, values bound when
  footer bounds are compared. `small-query-perf.yml` 42826465868: c1 p50 pruning on = pruning off within noise (was
  ~0.25 ms slower). Tests: `FileStatsPrunerTest` +1, `DescribeKeyTest` +1; CHANGELOG line; Q-40b-1 updated (proposal:
  close without default change). Dev CI on main for 4228c733: 42828677361 green. Not DONE yet. Lock released.
- 2026-11-10 (8): item 40(b) - file pruning shapes for decimal, 10-18 digit integer and `DATE 'yyyy-mm-dd'` literals
  (135e6dac; describe key unchanged). Tests `FileStatsPrunerTest` (+13 shapes), `DescribeKeyTest` +1; CHANGELOG line.
  Dev CI branch 42835145513 green; `small-query-perf.yml` 42835176027 c1 within noise. Merged to `main`
  (fast-forward). Not DONE yet. Lock released.
- 2026-11-10 (session 9): **ITEM 40(b) DONE 135e6dac.** Final before/after: Small query perf vs deafc861 prepared
  42843856272 (c1 p50 -10 %, c32 q/s +8..10 %) and statement 42843860772 (c1 p50 -13..15 %, c8/c32 q/s +12..16 %),
  HDFS point perf main 42843864824 vs 894afb9b 42843869104 (+2..6 % on the comparable runner, 0 errors); Dev CI on
  main 42837979902 (34e24d3b) green; JFR confirms one parse per shape for `count` texts. No code change. Lock released.
- 2026-11-10 17:25Z (session 10): no open item (40(b) DONE, C3 step 2 / C4 (b) / (d) stay backlog per the 2026-10-29 finish-mode note, Q-40b-1 / Q8 / Q10 / Q11 unanswered); no code change, stopped early. Lock released.
- 2026-11-12 03:20-03:50Z (session 12): stale lock of session 11 (18:07Z) taken over. **ITEM 47 DONE 2c169a53**:
  rebased session 11's `fix/47-attached-metadata` (Catalogs CI 43079733256 green) onto main as
  `fix/47-attached-metadata-2` (force-push refused), added `pragma_database_size` hiding and the refusal of name-string
  metadata functions while attached (+ tests), Dev CI 43147991919 / Catalogs CI 43147994336 green, rebase-merged.
  Q-40b-1 (2f31c071, Dev CI 43077861504), Q10 and Q8 closed. `STATUS.md` Log line for the core lane. Lock released.
- 2026-11-12 08:25Z (session 13): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered). Confirmed Dev CI on main 43149903188 (00da2257, includes item 47) green. No code change, lock not taken.
- 2026-11-12 10:25Z (session 14): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 13). No code change, lock not taken.
- 2026-11-12 15:21Z (session 15): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 14). No code change, lock not taken.
- 2026-11-12 16:21Z (session 16): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 15). No code change, lock not taken.
- 2026-11-12 18:21Z (session 17): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 16). No code change, lock not taken.
- 2026-11-12 19:21Z (session 18): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 17). No code change, lock not taken.
- 2026-11-12 20:21Z (session 19): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 18). No code change, lock not taken.
- 2026-11-12 21:21Z (session 20): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 19). No code change, lock not taken.
- 2026-11-13 03:21Z (session 21): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 20). No code change, lock not taken.
- 2026-11-13 04:21Z (session 22): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 21). No code change, lock not taken.
- 2026-11-13 05:21Z (session 23): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 22). No code change, lock not taken.
- 2026-11-13 06:21Z (session 24): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 23). No code change, lock not taken.
- 2026-11-13 07:21Z (session 25): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 24). No code change, lock not taken.
- 2026-11-13 08:21Z (session 26): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 25). No code change, lock not taken.
- 2026-11-13 10:26Z (session 27): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 26). No code change, lock not taken.
- 2026-11-13 11:21Z (session 28): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 27). No code change, lock not taken.
- 2026-11-13 12:22Z (session 29): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 28). No code change, lock not taken.
- 2026-11-13 14:40Z (session 30): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 29). No code change, lock not taken.
- 2026-11-13 16:37Z (session 31): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 30). No code change, lock not taken.
- 2026-11-13 17:25Z (session 32): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 31). No code change, lock not taken.
- 2026-11-13 18:21Z (session 33): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 32). No code change, lock not taken.
- 2026-11-13 21:21Z (session 34): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 33). No code change, lock not taken.
- 2026-11-14 03:21Z (session 35): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 34). No code change, lock not taken.
- 2026-11-14 05:21Z (session 36): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 35). No code change, lock not taken.
- 2026-11-14 06:21Z (session 37): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 36). No code change, lock not taken.
- 2026-11-14 07:21Z (session 38): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 37). No code change, lock not taken.
- 2026-11-14 08:21Z (session 39): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 38). No code change, lock not taken.
- 2026-11-14 09:21Z (session 40): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 39). No code change, lock not taken.
- 2026-11-14 10:21Z (session 41): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 40). No code change, lock not taken.
- 2026-11-14 11:21Z (session 42): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 41). No code change, lock not taken.
- 2026-11-14 12:21Z (session 43): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 42). No code change, lock not taken.
- 2026-11-14 13:21Z (session 44): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 43). No code change, lock not taken.
- 2026-11-14 14:21Z (session 45): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 44). No code change, lock not taken.
- 2026-11-14 15:21Z (session 46): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 45). No code change, lock not taken.
- 2026-11-14 16:21Z (session 47): no open item (item 47 DONE; C3 step 2 / C4 (b) / (d) backlog per the 2026-10-29 finish-mode note; Q11 unanswered; no new maintainer note since session 46). No code change, lock not taken.
