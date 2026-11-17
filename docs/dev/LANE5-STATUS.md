# Lane 5 (temporary) - see CLAUDE.md "Fifth and sixth lanes"

## Next steps

> **Maintainer, 2026-11-17 19:20 - ITEM 55(a): dc tails (moved here from lane 3, which works on 55(b)).** Real cluster run 2026-11-16 on `server-1.0.0-d2e27119fda4` (3 nodes, 80 cores / 754 GB, server on 16 cores, -Xmx8g, direct 8g, HDFS Kerberos, DataNodes `authentication,privacy`, direct SASL reads, TPC-H SF10 + a 1.27 TB table; full context in `STATUS.md` item 52 and `LANE3-STATUS.md` item 55).
> dc vs speed: same q/s on the point load (0.94-1.10), but small-query p99 c1 287 ms vs 36, c8 1125 vs 85, c32 4825 vs 755
> (p50 equal or better), and raw Flight scans of tpch.lineitem with 4 / 8 concurrent streams 1154 / 964 vs 1975 / 2171 MB/s
> (1 stream 646 vs 722). Find the tail source under dc (peer stream setup per query, merge queue, admission waits, Hazelcast
> lookups, a periodic stall such as the 30 s file inventory refresh?) with per-query timing on the CI cluster cell (3 servers,
> HDFS SASL) and fix it; goal p99 <= 1.5x speed and scans >= 0.9x speed. Branch `fix/55a`. Write `ITEM 55(a) DONE <sha>` with numbers.

## Log
