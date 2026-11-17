# Lane 6 (temporary) - see CLAUDE.md "Fifth and sixth lanes"

## Next steps

> **Maintainer, 2026-11-17 19:20 - ITEM 52(b) then 55(c): memory and speed of big aggregates (moved here from core / lane 3).** Real cluster run 2026-11-16 on `server-1.0.0-d2e27119fda4` (3 nodes, 80 cores / 754 GB, server on 16 cores, -Xmx8g, direct 8g, HDFS Kerberos, DataNodes `authentication,privacy`, direct SASL reads, TPC-H SF10 + a 1.27 TB table; full context in `STATUS.md` item 52 and `LANE3-STATUS.md` item 55).
> (52b) Balanced on a 754 GB host sized DuckDB `memoryLimit=160188MiB` and its RSS passed 48 GB, then 96 GB, on
> `SELECT count(id), min(id), max(id), sum(id) FROM <1.27 TB table>` - a streaming aggregate that should need little memory.
> Reproduce with a big generated Parquet table and a low memoryLimit (CI or HDFS cell): what holds the memory (DuckDB buffer
> pool / external file cache keeping read blocks, Parquet metadata cache, Arrow buffers, glibc arenas)? Cap it within the
> budget (core item 52 adds `memoryBudgetBytes`; use it) so RSS stays near the budget and the query still answers.
> (55c) Under dc a GROUP BY (6 groups) over the same table took 890 s vs 1001 s on speed while count/min/max/sum got 20x
> (31.7 s vs 638 s): find why (one merge node, partial aggregates not pushed, single-threaded scan per node?) and fix.
> Branch `fix/52b` then `fix/55c`. Write `ITEM 52(b) DONE <sha>` / `ITEM 55(c) DONE <sha>` with numbers.

## Log
