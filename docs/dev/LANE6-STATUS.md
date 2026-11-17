# Lane 6 (temporary) - see CLAUDE.md "Fifth and sixth lanes"

## Next steps

> **Lane 6, 2026-11-17 21:50 - ITEM 55(c) step 3 merged into main 5b751015 (not DONE).** Batches of local copies
> (`CopyBatchMerge`) now refuse on `mergeReason`, so computed GROUP BY keys merge there too (ordinal, expression, alias;
> averages next to them through `AverageMerge`). New guard: an alias GROUP BY key is accepted only when the table's footer
> columns are known and none has that name (`CopyBatchMerge.shadowedAliasKey`, `ExecutionService.copyBatchMerge`); the
> static `notBatchableReason` / `mergeable` know no columns, so the complete-mode batched path declines alias keys (with the
> reason) - ordinals / expressions go. Full Dev CI 44175118886 (924baf8f) was green; fast tier 44179162271 green; full Dev CI
> on main: run 44179759902 (head 8025e7f5, includes 5b751015).
> **Next session:** (0) read full Dev CI 44179759902 (red = fix first). (1) If the maintainer gave the exact
> GROUP BY / the INFO `distributedComplete=single reason=` line, handle that shape. (2) `date_trunc` / `strftime` keys:
> keep the client's own select-item text (jOOQ renders `date_trunc` wrongly, cannot parse `strftime`), with a DuckDB
> equivalence test in `CompleteAnswerMergeTest.computedGroupKeysMergeIntoTheCompleteAnswer`. (3) Optional: let
> `notBatchableReason` take the footer columns so alias keys also go the complete-mode batched path. (4) Per-node endpoints
> for big GROUP BYs if still slow at scale.

> **Lane 6, 2026-11-17 21:05 - ITEM 55(c) step 2 merged into main 728b5df0 (not DONE).** dc now merges a GROUP BY on a
> selected expression: `SELECT id % 6 AS g, count(*) ... GROUP BY g | 1 | id % 6`. The parser models the key as a
> `COLUMN` item with `computed=true` (`inputColumn` = output name, `inputExpression` = quoted SQL), endpoints render
> `<expr> AS "g" ... GROUP BY <expr>`, the merge groups by the output column. Only whitelisted jOOQ nodes (columns,
> literals, `+ - * / %`, neg, CAST, extract year/month/day/hour, lower, upper, floor, ceil, abs, substring, coalesce):
> jOOQ renders `date_trunc('day', ts)` as `trunc("ts", null)` (silently wrong), `random()` is not deterministic, so they
> stay single-node. `GROUP BY g` where `g` is also a table column stays single-node (`shadowedAliasKey`; DuckDB binds the
> column first - checked). Partial mode unchanged: `completeAnswerReason` still set, the merge reads the new `mergeReason`.
> avg next to a computed key works through `AverageMerge` (integration test). Fast tier 44174563888 green; full Dev CI on
> main: run 44175118886 (head 924baf8f, includes 728b5df0).
> **Next session:** (0) read full Dev CI 44175118886 (red = fix first). (1) If the maintainer gave the
> exact GROUP BY or the INFO `distributedComplete=single reason=` line, handle that shape. (2) Otherwise: `date_trunc`
> (and `strftime`, which jOOQ cannot parse) as computed keys need the client's own text instead of jOOQ's rendering -
> e.g. keep the select item's source text (parse positions) or a DuckDB-side check; do not widen the whitelist without
> a DuckDB equivalence test in `CompleteAnswerMergeTest.computedGroupKeysMergeIntoTheCompleteAnswer`. (3) Batches of
> copies (`CopyBatchMerge`) still refuse computed keys (it checks `completeAnswerReason`): switch it to `mergeReason`
> with a test. (4) Then per-node endpoints for big GROUP BYs if still slow at scale (see step 1 note below).

> **Lane 6, 2026-11-17 20:25 - ITEM 55(c) step 1 merged into main 72edab3d + e3d14331 (not DONE).** Finding: dc's 890 s vs
> speed's 1001 s means that GROUP BY was most likely never merged, so one node computed it alone as under speed (count/min/max/sum
> got 20x because it *was* merged). `DistributedComplete.mergeable` silently refuses (DEBUG only) these GROUP BY shapes:
> `ORDER BY <aggregate>` (e.g. `ORDER BY count(*) DESC`), a GROUP BY expression (`id % 6`, `date_trunc(...)`, `GROUP BY 1`
> on an expression), `avg` next to other aggregates (`AverageMerge.of` returns null for `SELECT t, count(*), avg(v)`),
> `count(DISTINCT)` and HAVING. Plain-column groups with count/sum/min/max (+ ORDER BY alias / ordinal) do merge.
> Fixed now: an ORDER BY aggregate that is also selected renders as its position (parser `aggregateOrdinal`; also helps
> batches of copies), so it merges. New INFO line `distributedComplete=single reason='...'` for every aggregate kept on one
> node. Fast tier 44167850389 + 44168551211 green; full Dev CI 44169154810 on main e3d14331 running at session end.
> **Next session:** (0) read 44169154810; red = fix first. (1) The maintainer's exact GROUP BY (or the new INFO reason
> from a stand run on a build with e3d14331) decides what is left; meanwhile (2) merge the next most likely shape: a GROUP BY expression that is also selected (`SELECT id % 6 AS g, count(*) ...
> GROUP BY g / 1 / id % 6`): parser must keep the select item as a group "column" with its quoted expression
> (`SelectExpr.inputExpression`), the endpoints' partial path must compute it (check `ExecutionService` ~3099 and
> `MetadataService` ~405-483, which assume `inputColumn` is a physical column), and the merge groups by the output column
> (`appendAggregateMerge` already does). (3) `avg` mixed with other aggregates in `AverageMerge`. (4) Only if the merged GROUP BY
> is still slow at scale: per-node endpoints (`mergeEndpointsPerNode` is 0 for GROUP BY, many statements per node).

> **Maintainer, 2026-11-17 19:20 - ITEM 52(b) then 55(c): memory and speed of big aggregates (moved here from core / lane 3).** Real cluster run 2026-11-16 on `server-1.0.0-d2e27119fda4` (3 nodes, 80 cores / 754 GB, server on 16 cores, -Xmx8g, direct 8g, HDFS Kerberos, DataNodes `authentication,privacy`, direct SASL reads, TPC-H SF10 + a 1.27 TB table; full context in `STATUS.md` item 52 and `LANE3-STATUS.md` item 55).
> (52b) Balanced on a 754 GB host sized DuckDB `memoryLimit=160188MiB` and its RSS passed 48 GB, then 96 GB, on
> `SELECT count(id), min(id), max(id), sum(id) FROM <1.27 TB table>` - a streaming aggregate that should need little memory.
> Reproduce with a big generated Parquet table and a low memoryLimit (CI or HDFS cell): what holds the memory (DuckDB buffer
> pool / external file cache keeping read blocks, Parquet metadata cache, Arrow buffers, glibc arenas)? Cap it within the
> budget (core item 52 adds `memoryBudgetBytes`; use it) so RSS stays near the budget and the query still answers.
> (55c) Under dc a GROUP BY (6 groups) over the same table took 890 s vs 1001 s on speed while count/min/max/sum got 20x
> (31.7 s vs 638 s): find why (one merge node, partial aggregates not pushed, single-threaded scan per node?) and fix.
> Branch `fix/52b` then `fix/55c`. Write `ITEM 52(b) DONE <sha>` / `ITEM 55(c) DONE <sha>` with numbers.

> **Lane 6, 2026-11-17 - ITEM 52(b) diagnosed, fix not yet written (branch `fix/52b` f17d006d holds the probe only).**
> Cause: balanced turns `duckDbExternalFileCache=true` on for an `hdfs://` data dir (item 39), and DuckDB keeps every block
> read through an extension file system (httpfs, hadoopfs; not local files) in that cache, evicting only at `memory_limit`.
> So one streaming aggregate fills its connection's cache up to `memory_limit` (160188 MiB in the cluster run), and an idle
> connection keeps it until its thread ends. Probe `tools/file-cache-probe.py` (DuckDB 1.5.6, httpfs from ci-assets, 2.6 GiB
> file over local HTTP, limit 2 GB, separate processes): count/min/max/sum(id) cache 153 MiB / RSS 345 MiB with the cache vs
> 0 / 147 without; wide scan cache 942-1257 MiB (fills to the limit) / RSS 2502 vs 1214 MiB. With limit 3 GB RSS peaked at
> 3945 MiB (above the limit). A DuckDB's cache drops at once with `SET enable_external_file_cache=false` (then `true`), also
> concurrently with a running query (3 scans exact, 41 trims, peak cache 736 MiB, RSS 2599 vs 3945 MiB) - BUT
> `duckDbLockdown=true` (default) sets `lock_configuration`, which refuses that SET, so a runtime toggle is no fix.
> With 52(a)'s `memoryBudgetBytes=48g` the per-connection limits already add up within the budget (RSS ~ budget).
> **Next step (52(b) fix, lockdown-safe), in this order:** (1) new setting `duckDbExternalFileCacheMaxBytes`
> (`DUCKDB_EXTERNAL_FILE_CACHE_MAX`, node-wide, split over the expected connections; proposed default min(DuckDB budget / 4,
> 8 GiB), `0` = today's unbounded behaviour - write the default into Open questions of STATUS.md); in `DuckDbAdapter.session()`
> (owner thread, no query running) at most once per second per own-DuckDB session read `duckdb_memory()` tag
> `EXTERNAL_FILE_CACHE` and, above the share, close the session and open a fresh one (log `duckdb=fileCacheRecycled
> cacheMiB=`); the shared DuckDB is one instance - there recycle nothing, just log. (2) In-query bound: run a query whose
> local scan bytes (`QueryPlanner.localScanBytes`) exceed the share on a per-thread session opened with `fileCache=false`
> (as `openPrivateSession` does), so a big scan never fills the cache. (3) Unit tests for the share/decision; an
> integration test with httpfs from ci-assets serving a generated file (as the probe does) showing the cache stays under
> the share. Then 55(c).

> **Lane 6, 2026-11-17 19:30 - ITEM 52(b) fix on `fix/52b` (steps 1 and 3 of the plan above).** New setting
> `duckDbExternalFileCacheMaxBytes` (`DUCKDB_EXTERNAL_FILE_CACHE_MAX`; default min(DuckDB budget / 4, 8 GiB), `0` = no
> cap; Q-52b in STATUS.md) split over the expected connections; when a query's slot closes (owner thread, nothing
> running, nobody holds the connection), a DuckDB-per-connection session whose `EXTERNAL_FILE_CACHE` exceeds the share
> (checked at most once a second) is closed and the next query opens a fresh one (`duckdb=fileCacheRecycled`). Shared
> DuckDB (balanced default since item 53) unchanged - its one memory limit bounds the cache. Step (2) (no-cache session
> for scans above the share) not done: during a query `memory_limit` already bounds the cache; judge after a cluster run.
> **Merged into main bdeda437** (fast tier 44156451682 green; full Dev CI 44158521271 on main dispatched by the push -
> **next session: read it first, a red run is the first thing to fix**). Not yet `ITEM 52(b) DONE`: needs the
> maintainer's real-cluster numbers (RSS with `duckDbSharedDatabase=false` + cache on HDFS) or a CI HDFS cell run.
> **Next step:** if the full run is green, start ITEM 55(c) on branch `fix/55c` (dc GROUP BY over the 1.27 TB table 890 s
> vs speed 1001 s while count/min/max/sum got 20x): read `DistributedComplete` merge planning for GROUP BY (one merge
> node? partial aggregates pushed? threads per endpoint scan), reproduce on `service-cluster.yml` at the smallest sf.

## Log
- 2026-11-17 15:15-16:00Z (lane 6): ITEM 52(b) diagnosis (see Next steps): the external file cache, filled by one scan up to
  `memory_limit` per connection; runtime toggle blocked by the lockdown; probe f17d006d on `fix/52b` ([skip ci], no CI run;
  local runs only). No code change yet, no merge. Next: the lockdown-safe cap (recycle + no-cache session for big scans).
- 2026-11-17 19:06-19:45Z (lane 6): ITEM 52(b) cap implemented (`duckDbExternalFileCacheMaxBytes`, recycle after the
  query's slot; Q-52b), unit tests (DuckDbAdapterTest, DuckDbSizingTest, ConfigAdapterTest) and S3IntegrationTest with
  real httpfs + lockdown (25 MiB cached, share 12 MiB -> replaced, fresh cache 0). Fast tier 44156451682 green; merged
  into main bdeda437; full Dev CI 44158521271 pending. Next: check it, then 55(c).
- 2026-11-17 20:12-20:25Z (lane 6): full Dev CI 44158521271 (bdeda437) was red in the item 35 step - lane 5's, fixed by
  them (44162578438 green). ITEM 55(c) step 1: probe of the GROUP BY shapes dc refuses (see Next steps); ORDER BY a
  selected aggregate now merges (dc and batches of copies), INFO reason for unmerged aggregates. Tests: CompleteAnswerMergeTest,
  CopyBatchMergeTest, DistributedCompleteMergeableTest, ExecutionServiceTest. Fast tier 44167850389, 44168551211 green;
  merged into main 72edab3d + e3d14331; full Dev CI 44169154810 pending. Local full unit run: only HiveCatalogIntegrationTest
  (non-UTF-8 file names in the container, fails without the change too) and LakeFsPluginTest (plugin not built) errored.
- 2026-11-17 20:44-21:05Z (lane 6): full Dev CI 44169154810 (e3d14331) green. ITEM 55(c) step 2: computed GROUP BY
  keys merged under dc (see Next steps). Tests: DistributedCompleteMergeableTest, CompleteAnswerMergeTest (DuckDB
  equivalence with endpoint SQL built from the parse), DistributedCompleteIntegrationTest (4 merged shapes + an alias
  shadowing a column kept single-node). Fast tier 44174563888 green; merged into main 728b5df0; full Dev CI 44175118886 (924baf8f) pending.
- 2026-11-17 21:16-21:50Z (lane 6): full Dev CI 44175118886 (924baf8f) green. ITEM 55(c) step 3: batches of copies merge
  computed GROUP BY keys, alias keys guarded against shadowing a table column (see Next steps). Tests: CopyBatchMergeTest
  (DuckDB equivalence incl. avg + LIMIT), ExecutionServiceTest (mergeable lists; end-to-end batches with alias and ordinal
  keys). Fast tier 44179162271 green; merged into main 5b751015; full Dev CI 44179759902 (8025e7f5) pending.
