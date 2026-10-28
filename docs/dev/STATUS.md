# Server development status (cloud + local)

## Where things stand (2026-10-23)

- `main` = branch integration-3: integration-2 (our engine on upstream layout, cluster fixes for member loss and the
  ticket secret, DuckDB 1.4.5, hadoopfs timeout settings, extension version check) + catalog SPI + plugins s3, lakefs,
  iceberg (hadoop + REST/Gravitino), hive (HMS 3.1.3 Thrift) + stand fixes (lakeFS L-02..05, Iceberg I-06..09,
  Hive HV-07..12). Local build "int7" = this main at 4b2aa29 (eq deletes: every query reads all columns of a snapshot with equality deletes, filters kept outside the scan — correct, ~40 s per query on 1.5 M rows; see Next steps 2). Full offline build green: server 638 tests.
- Verified on the maintainer's stand (HDFS, MinIO S3, lakeFS, HMS+PostgreSQL, Gravitino 1.3.1): TPC-H SF10 22/22 via
  JDBC and the Trino plugin (checksums 9e2af63e8711 / 05bc0dc2713a), 3-node cluster kill tests, lakeFS 22/22 per
  branch/commit/tag, Hive 76/89 reference queries (rest explained), Gravitino 15 tables = DuckDB; clients: Trino plugin,
  StarRocks native connector, ADBC (separate repos).
- CI: `.github/workflows/dev-ci.yml` (push to main on code paths + manual; ~6 min): `clean verify checkstyle:check
  spotbugs:check -DexcludedGroups=perf,real-services` with the DuckDB 1.4.5 extensions from `ci-assets` (Iceberg and
  fake-S3 integration tests run; hadoopfs IT skipped). Manual dispatch with input `package=true` adds the release
  job (linux-only jar + plugins tar + START-NOTES + SHA256SUMS as pre-release `server-<version>-<sha>`); dispatch via
  the GitHub MCP `actions_run_trigger` (`gh` is not authenticated in cloud sessions). Upstream `ci.yml` is
  manual-only. Locally the JVM needs a UTF-8 locale (`LC_ALL=C.UTF-8`) or the Hive tests fail on non-ASCII paths.
- Build: `./mvnw -B clean package` (JDK 21); linux-only jar: `-Plinux-only`; plugins dir: `tools/make-plugin-dir.sh`.
  Integration tests that need real DuckDB extensions take `-Diceberg.it.extensionDir=<dir with v1.4.5/linux_amd64>`
  and `-Dhadoopfs.it.extension=<file>` (hadoopfs needs libkrb5 etc. — run that one in an AlmaLinux 8 container or
  skip it); `perf` tests are opt-in.

## Next steps (one per session)

> ~~**Bug first (maintainer, 2026-10-28 16:30, laptop stand p19b): item 19b defaults make heavy queries 1.35-1.7x slower.**~~
> *Fixed 2026-10-28 (9efe670, see Log):* default threads cores / 4 in 4..16, the per-GiB cap only above 4, per-connection
> `memory_limit` unchanged (1024/2201/3942 MiB on the three profiles, `DuckDbSizingTest`). New manual workflow
> `tpch-perf.yml` (run 40722015834, SF10, 4c/8g container): default 21.82 s, `-DduckDbThreads=4` 21.38 s, pre-fix
> 2d93081 (2 threads) 29.48 s; all 22 row counts = DuckDB's own. **Screening/stand:** re-measure the pre-release
> (see Log) against `7562a5bdb7b9`; the CI perf check of the note is `tpch-perf.yml`, not in Dev CI (SF10 takes ~10 min).
> Original report:
> Under container limits 4c/8g, 8c/16g, 16c/24g (ADBC + raw Flight, HDFS SF10, 3 rounds, 22/22, 0 OOM): `duckdb=sizing`
> correctly reports container cores/memory, but chooses threads 2/2/3 (16c/24g cut from 4 to 3 by the 1-thread-per-GiB
> cap at 3942 MiB) -> TPC-H SF10 17.95/17.94/13.43 s vs 7562a5bdb7b9 (4 threads) 10.71/10.73/9.92. With 4 threads and the
> 1 GiB limit: 10.11 s -> the memory limit is neutral (RSS 3.2-3.9 GB both), the thread count is the cause. t8/t16: SF10
> -23/-20 % but small queries at 8/32 clients p50 1.6-3x worse, RSS +27 % at t16. Fix: thread floor 4 by default
> (max(4, cores/4), at most 16), drop or soften the per-GiB thread cap (e.g. only when memory_limit < 1 GiB x 4), keep
> the default per-connection memory_limit; more threads for heavy queries only via the adaptive step 2 (opt-in). Add
> a unit test for the three profiles above and a CI perf check that SF10 at 4 vCPU is not slower than with
> `-DduckDbThreads=4`. Package pre-release; the screening repo and the stand re-measure it.

> ~~**Bug first (maintainer, 2026-10-28 11:30, CI build tracking `track-4bb6441569be`): small local ADBC tail at 8 clients.**~~
> *Investigated 2026-10-28 (cloud session, see Log): not a server regression - a CPU-quota effect.* Under `docker --cpus=2`
> (CFS quota, as the screening runs) c8 latency is bimodal in **both** builds: p50 11-15 ms, p95 59-63 ms (queries that
> span a throttled 100 ms period); pinned to 2 cores (`--cpuset-cpus`) p95 is 37-42 ms. 4b47b0c vs 7562a5b, same runner
> (workflow `small-query-perf.yml`, run 40665245865, plain statements = ADBC's path, client on other cores, 2 rounds):
> quota c8 p50 0.72/0.80x, p95 0.94/0.93x, p99 0.95/0.95x, q/s +22/+29 %; pinned c8 p50 0.89/0.79x, p95 0.91/0.87x.
> The p95 is set by the throttle stall, so it stays ~1.0x while p50 falls; the screening's 1.07 is within that stall's
> run-to-run spread. `duckDbThreads=4` on head does not change the quota tail (p95 61.0/61.6). No code change; for the
> screening: read c8 tails under `--cpus` as throttling, or add a `--cpuset-cpus` run (see Open question "Quota tails").
> Original report:
> **Small local ADBC tail at 8 clients.**
> `4bb6441569be` vs `7562a5bdb7b9`, same runner, 3/3 rounds: ADBC c8 p95 1.07 (1.10 1.06 1.12), p99 1.15 (1.14 1.16 1.15)
> while p50 is 0.75; JDBC and 32 clients not affected. Small, but in every round. Look at it together with item 21 (same
> path): reproduce with `SmallQueryThroughputPerfTest` (ADBC-like prepare/execute, 8 clients, 2 CPUs), check whether a
> stage-3 change (item 21 step 1 per-DoGet event, item 22 thread/stream caps, compression code path when not asked)
> adds a stall for some queries; keep the p50 gain. Then continue with the open stage-3 item.

> ~~**Bug (fixed in 4bb6441569be, confirmed by CI tracking)**~~ **Bug first (maintainer, 2026-10-28, from the CI screening's build tracking, `results/BUILDS.md` /
> `track-d22cdaccb4e1`): S3 small queries got slower in the tail.** `d22cdaccb4e1` vs `7562a5bdb7b9`, same runner, 4 vCPU,
> 3/3 rounds: p95 at 8 and 32 clients +12-21 % (JDBC c32 p95 1.21, JDBC c8 p95 1.12, ADBC c8 p95 1.20) while p50 is
> 0.65-0.91 and local disk is not affected. Prime suspect: the size-aware `duckDbThreads` default (item 19: 4 vCPU -> 2
> threads vs 4 before) - S3 reads are round-trip bound, fewer threads per query = longer tails (item 19 already flagged
> this). Check by an A/B with `-DduckDbThreads=4` on the same build (perf test with fake S3 latency, 8/32 clients); if
> confirmed, make the default per store (S3/remote stores keep >= 4) or otherwise restore the tail without losing the
> local-disk gains; else bisect between 7562a5b and d22cdac. Take this before continuing item 22 (finish the current
> step of 22 first if you are in the middle of it).
> *Done 2026-10-28 (4bb6441, package run 40635098385 green, pre-release `server-1.0.0-4bb6441569be`; see Log):* confirmed by a local A/B (server over fake S3 + real s3 plugin, 4 cores,
> `SmallQueryThroughputPerfTest -Dperf.store=s3`): 2 threads lengthen c1/c8 p95 by 1.13-1.7x. Default is now
> max(4, cores / 4) (at most 8) when `dataDir` is `s3://`/`s3a://`/`s3n://`/`lakefs://`; local/HDFS unchanged.
> **Screening:** measure the pre-release against `7562a5bdb7b9` (expect S3 c8/c32 p95 back to ~1.0x, local disk
> unchanged); if c32 q/s on S3 drops noticeably, see Open question "S3 thread default".

> **Maintainer, 2026-10-28 22:30 - keep the core lane busy; do not wait for measurements.** Screening/stand numbers arrive
> on their own; meanwhile take code items in this order: (1) **item 21 continued** - upstream native still does ~70 q/s vs
> our ~53 on 2 CPUs (screening `track-94ad61021d93`): next cuts from your item-21 breakdown (e.g. a one-round-trip path for
> small results, plan/prepare cache keyed by SQL text + catalog snapshot, Hazelcast work off the single-node path, fewer
> allocations per batch); goal >= upstream q/s at 8 and 32 clients with c1 p50 not worse. (2) **item 22 continued** -
> 4-8 stream scans: goal >= 750 MB/s on 2 CPUs (now ~1.18x of 7562a5b at 8 streams). (3) **item 19b step 2** adaptive
> threads per query (opt-in). (4) item 16 decision from the existing numbers. Each with a `package=true` pre-release.

> **Maintainer, 2026-10-28: stage 3 - take these in this order before every other open item** (the presentation build is
> fixed at `server-1.0.0-7562a5bdb7b9`; nothing below may change its package, only new pre-releases). Basis: the
> four-server comparison in the public CI screening repo (identical containers: 2 CPUs, 6 GB, `-Xmx3g`, direct 2g; reports
> `COMPARE-SPEED/LOAD/ROBUSTNESS-2026-10-27.md`): ours wins TPC-H, memory, cancel, soak and cluster, upstream native wins
> three places. Every item: code + tests + CI green + `package=true` pre-release with a Log line saying what changed and the
> local numbers; the screening repo then measures each such pre-release against `7562a5bdb7b9` on the same runner.
> Item 18 step 2 is unblocked (answers in Open questions).
> 1. item 20 (new) - **response compression for slow links.** Upstream: ZSTD 264 MB/s vs ours 104 MB/s at 10 ms / 1 Gbit
>    (S11 in `UPSTREAM-SYNC.md`; we have no compression). Arrow IPC body compression (LZ4_FRAME and ZSTD, level setting),
>    off by default; enabled per connection when the client asks (call header and Flight SQL session option, e.g.
>    `arrow-compression: zstd`) or for all streams by a server setting. Rules: never for clients that did not ask (the stock
>    JDBC driver lacks the codec - add `arrow-compression` to our patched driver and say so in START-NOTES); skip
>    compression for small batches / when the ratio is poor; compress in parallel per batch without breaking the
>    outbound budget (0f). Tests: same answers with and without compression for Java Flight, ADBC (C++/Go) and pyarrow;
>    a CI perf test with `tc netem` delay 10 ms + rate 1 Gbit (MB/s, CPU, both codecs) - goal >= upstream's 264 MB/s.
>    *Step 1 done 2026-10-27 (6ca7352 + 85aaf46, pre-release `server-1.0.0-85aaf4604d9a`, see Log):* opt-in ZSTD per call (`arrow-compression: zstd|none` header on
>    DoGet) or server default `flightCompression` (none), `flightCompressionLevel` (1), `flightCompressionMinBytes`
>    (64 KiB, smaller batches plain), a stream gives up after 4 batches that shrink < 10 %; metrics
>    `arrowflight_flight_compress*`. Found: Arrow Java's LZ4_FRAME codec (commons-compress) cannot compress 8 MB in
>    2 min, so `lz4` is refused for now; Java Flight clients decode ZSTD once `arrow-compression` + `zstd-jni` are on
>    their class path (ServiceLoader), no driver code needed for that. ZSTD level 1 single-thread ~250-285 MB/s
>    compress, ~450-600 MB/s decompress (8 MiB of small longs, ratio 2.9). *Step (b) done 2026-10-27 (d22cdac, see
>    Log):* `arrow-compression: lz4` / `flightCompression=lz4` via our own `Lz4FrameCodec` on lz4-java (JNI, from the libs
>    set; lz4 refused if missing), ~510 MB/s vs ZSTD ~330 MB/s single thread (`StreamCompressionPerfTest`, perf tag).
>    *Step (c) done 2026-10-27 (ba4139a, see Log):* Flight SQL session option `arrow-compression` via
>    SetSessionOptions (node-local sessions, cookie `arrow_flight_session_id`, unknown cookies ignored,
>    `flightSessionIdleSeconds` 3600 / `flightSessionMax` 10000); header > session > `flightCompression`.
>    **Next steps of 20:** ~~(b) LZ4 via lz4-java~~; ~~(c)
>    Flight SQL session option `arrow-compression`~~; ~~(d) START-NOTES text + the patched JDBC driver change
>    described for the coordinator~~ (done 2026-10-28, see Log; the driver itself lives in a client repo);
>    ~~(e) pyarrow / ADBC C++/Go answer checks in CI~~ (done 2026-10-28, `CompressionClientsIntegrationTest`; ADBC C++
>    is not a separate Python package - pyarrow Flight is the C++ client); ~~(f) parallel per-buffer compression within
>    a batch and the `tc netem` 10 ms / 1 Gbit CI perf test~~ (done 2026-10-28, cadd1ac, pre-release
>    `server-1.0.0-cadd1aca9c23`; `flightCompressionThreads`, workflow `compression-perf.yml`: zstd 429 MB/s vs plain 90
>    at 10 ms / 1 Gbit on synthetic lineitem, ratio 4.9 - see Log). **Item 20 done**; open follow-up: lz4 reads
>    by Java clients are bound by commons-compress's decoder (~180 MB/s), pyarrow/ADBC are not.
> 2. item 21 (new) - **small single-table queries under load.** On 2 CPUs upstream native reaches ~84 queries/s vs ours
>    ~50 (both CPU-bound from 8 clients); c1 latency is already ours. So this is CPU per query, not waiting: profile one
>    query end to end under 8-32 clients (async-profiler or JFR in a 2-CPU container), list where the CPU goes
>    (GetFlightInfo + DoGet round trips, planning / views snapshot, statement cache, auth per call, metrics, logging,
>    Hazelcast in single-node mode, thread hand-offs, Arrow allocation, DuckDB threads vs cores) and cut the top items;
>    compare with upstream's path for the same query. Goal: >= upstream's queries/s at 8 and 32 clients on 2 CPUs with
>    identical answers; c1 p50 must not get worse.
>    *Step 1 done 2026-10-28 (881b7c6, package run 40618153054 green, pre-release `server-1.0.0-881b7c62b373`; see Log):* new perf test `SmallQueryThroughputPerfTest` (1/8/32 clients, point lookup +
>    filtered count as stock clients run them: prepare, GetFlightInfo, DoGet, close; q/s, p50/p95, process CPU per
>    query and per thread group; run it under `taskset -c 0,1`). Where the CPU goes (local, 2 CPUs, c32, 4 local
>    Parquet files, ~11 ms CPU per query incl. the in-process client): DoGet handler ~5.7 ms = DuckDB prepare of the
>    new text 0.8 + execute/Arrow export 3.4 + fetch 0.45 + Flight putNext 0.27 + execution-path event 0.29 + ticket
>    decode/metrics/qid 0.25; GetFlightInfo 0.67 (0.64 of it Hazelcast registry + heartbeat reads, local dirs only);
>    prepare (cached describe) 0.16; close 0.03; gRPC/netty for the 4 calls ~2; DuckDB's second thread ~1.4 (see
>    (b)); client ~0.9. Cut: the Hazelcast reads on a one-member cluster, the Jackson tree of the event, a
>    `Mac.getInstance` per ticket, `String.format` per qid: c8 159 -> 188 q/s, c32 222 -> 250 (same session, A/B
>    twice). JaCoCo's agent costs ~25 % q/s here: measure with `-Djacoco.skip=true -DargLine=-Dx=1`.
>    **Next steps of 21:** (b) `duckDbThreads=1` gives another ~9 % at c8 on 2 CPUs (c1 unchanged locally) but the
>    lockdown (`lock_configuration`) forbids changing threads per query under load - Open question; (c) the storage
>    the screening repo uses for the load test (local vs HDFS/S3): over HDFS every query also opens files through
>    hadoopfs (not profiled here) - measure on the stand / screening which part is storage; ~~(d) the remaining
>    DoGet prepare and the 4 gRPC calls per query~~ (measured 2026-10-28, see Log: the server's own Java work is now
>    ~0.2 ms of ~6 ms CPU per query at c32; the rest is DuckDB's own prepare + execute + Arrow export (~4 ms, same as
>    plain DuckDB JDBC on one connection) and the describe of each new text (~0.5 ms); plain statements are 10-15 %
>    more q/s than prepare/execute/close. No server cut left on this path short of the Open question (b) or fewer
>    describes - see Open question "Item 21 (d)"). **Next for 21:** wait for the screening's numbers on
>    `server-1.0.0-881b7c62b373` (with and without `-DduckDbThreads=1`) before more work here; meanwhile item 22.
> 3. item 22 (new) - **one scan in 4-8 parallel streams.** Upstream 807-823 MB/s vs ours 463-502 MB/s on 2 CPUs (at about
>    twice our memory; one stream is a tie). Find why (per-stream DuckDB work, batch size, copies between DuckDB's Arrow
>    output and the Flight stream, the per-stream budget of 0f, endpoint split of S6) and close the gap. Goal >= 750 MB/s
>    with peak RSS at most +25 % over today's.
>    *Step 1 done 2026-10-28 (see Log):* new perf test `ParallelScanPerfTest` (one partial-mode scan in N endpoints -
>    complete mode is always one stream - or N concurrent complete-mode scans; `-Dperf.server=host:port` measures a
>    separately started server). Findings on 2 cores: the server's CPU per GB grows with the number of parallel
>    streams because every endpoint runs in its own DuckDB with `duckDbThreads` (2) threads; plain DuckDB JDBC shows
>    the same (4 instances x 2 threads ~830 MB/s at 2.1-2.6 s CPU/GB, x 1 thread ~1270 MB/s at 1.5). Cut, opt-in:
>    `duckDbThreadsUnderLoad` (second, low-thread connection per worker for DoGets that start while running DoGets x
>    `duckDbThreads` > cores). Measured with a pyarrow client on other cores, 16-file 6 M-row lineitem-like table:
>    4 endpoints 712 -> 792 MB/s, 8 endpoints 579 -> 611, 1 stream unchanged. Zero-copy Flight writes
>    (`arrow.flight.enable_zero_copy_write`, not used by us) were inconclusive here (+11 % once, worse once).
>    **Next steps of 22:** (b) the screening repo measures the pre-release with `-DduckDbThreadsUnderLoad=1` and
>    `endpointsPerServer` 4/8 (partial mode); ~~(c) 8 endpoints stay ~20 % below 4 on 2 cores: try capping the DoGets
>    that produce batches at once to the cores (opt-in) and look at per-stream budget waits~~ (done 2026-10-28, db66d64, opt-in
>    `duckDbProducingStreams`, in pre-release `server-1.0.0-9506620bb170`, see Log; screening: measure with value 2 = cores / duckDbThreads at 4/8 endpoints, alone
>    and with `duckDbThreadsUnderLoad=1`); ~~(d) zero-copy writes
>    for the DuckDB stream path only (its buffers are fresh per batch)~~ (done 2026-10-28, 78a2720, opt-in
>    `flightZeroCopyWrites`, in pre-release `server-1.0.0-7f308a6cdb4d`, see Log; local A/B: 8 endpoints +1-15 % MB/s,
>    -10-14 % CPU/GB, 1/4 streams within noise; screening: measure `-DflightZeroCopyWrites=true` at 1/4/8 endpoints,
>    alone and with `duckDbProducingStreams=2`). **Item 22 has no open code step**; next is the screening's numbers.
>    Caveat (see the maintainer's S3 tail bug above): fewer threads per query lengthen round-trip-bound S3 reads, so
>    leave `duckDbThreadsUnderLoad` off on S3 nodes until that bug is understood; it targets CPU-bound local/HDFS scans.
> 3b. item 17 (e) (new, from the CI screening's cache run 40569584228, `COMPARE-CACHE-2026-10-27.md`) - **S3 read cache
>    memory and large scans.** With `arrowflight.s3.cacheMegabytes=1024` (`-Xmx3g`, direct 2g, 6 GB container) peak RSS
>    was ~5.7 GB vs ~2.3 GB off (2.45x; ~300 MB from the container limit, no OOM kill yet) - find where the extra ~3.4 GB
>    lives (heap LRU vs copies per request vs DuckDB buffers holding the same data) and bound the whole thing to the
>    configured size; and TPC-H SF10 is ~8 % slower with the cache on (does not fit, the cache churns) - bypass the
>    cache for large sequential reads (size-aware admission). Wins to keep: repeated small queries p95 8-20x faster,
>    SF1 0.32-0.47x. Take it right after item 20.
>    *Done 2026-10-28 (99113c9, package run 40645727394 green, pre-release `server-1.0.0-99113c98c1c3`; see Log):* the extra memory was mostly the cache itself: each 1 MiB block was one
>    array, a humongous G1 object taking a whole 2 MiB region (256 MiB of blocks = 512 MiB of heap at `-Xmx3g`), so
>    1 GiB of cache used ~2 GiB and the heap filled to `-Xmx`; plus each miss GET (up to 64 MiB) buffered by the SDK and
>    copied twice. Now 256 KiB pieces + streamed misses (heap = configured size). Size-aware admission does not work
>    for DuckDB (scans read column chunks of 2 KiB-3 MiB, a `SELECT *` point lookup a whole 4.5 MiB row group), so
>    instead opt-in `arrowflight.s3.cacheAdmission=reuse` (scan-resistant: kept on the second miss within the cache's
>    reach). **Screening:** the cache run again (`cacheMegabytes=1024`) on the pre-release: peak RSS (expect ~1 GB
>    less), SF10 with `always` and with `reuse` vs off, repeated small queries p95 with both. See Open question
>    "Read cache admission default".
> 3c. item 19b (new, maintainer 2026-10-28) - **DuckDB threads and memory by host size (cores AND memory).** Today:
>    threads = cores / 4 clamped 2..8 (S3/lakeFS >= 4), `duckDbMemoryLimit` unset (DuckDB default = 80 % of RAM *per
>    instance*, and we run one instance per worker connection -> no real bound). Production hosts are large (many cores,
>    a lot of RAM): the cap of 8 leaves them idle, and the memory side is uncontrolled. Make the defaults depend on both:
>    read the limits the JVM really has (cgroup/container cores and `memory.max`, else physical RAM); DuckDB budget =
>    memory - `-Xmx` - direct memory - headroom (~15 %); per-connection `memory_limit` = budget / expected concurrent
>    connections (worker pool / `maxConnections`), at least 1 GB; threads per query = min(cores-based value,
>    memory_limit / ~1 GB per thread (DuckDB's guidance for joins/aggregates)), with the upper clamp raised for big hosts
>    (e.g. 16, maybe 32 - measure). Explicit settings always win; one startup line with the inputs and the chosen values.
>    Step 2 (opt-in): adaptive threads per query - a heavy query gets more threads when the node is idle (`SET threads`
>    per connection), fewer under concurrency. Tests: formula across host profiles (2c/6g container, 4c/16g, 16c/64g,
>    64c/512g, small `memory.max` with many cores), no OOM in the existing 6 GB container tests; perf in CI on 4 vCPU
>    (no regression vs today's defaults) - large hosts are measured on the maintainer's stand later.
>    *Step 1 done 2026-10-28 (a9e43ca, package run 40673683569 green, pre-release `server-1.0.0-a9e43ca5e296`; see Log):* `DuckDbSizing` + `HostLimits` (cgroup v2/v1 `memory.max`, JVM total
>    memory, `-Xmx`, direct limit); `memory_limit` per connection = budget / `duckDbMemoryConnections` (default cores /
>    threads, at least 2), >= 1 GiB, <= 80 % of memory; threads cores / 4 in 2..16 (S3 >= 4), <= 1 per GiB, >= 2;
>    `duckDbMemoryLimit=none` = old behaviour; startup line `duckdb=sizing`. **Next of 19b:** screening on the
>    pre-release (2c/6g gets 1 GiB per connection - TPC-H SF10 and the load test vs `7562a5bdb7b9`, see Open question
>    "Item 19b expected connections"); stand: large hosts, clamp 16 vs 32; ~~step 2 (adaptive threads, opt-in)~~
>    (done 2026-10-28, fd9f62f, see Log: opt-in `duckDbThreadsWhenIdle` = more threads for a DoGet that starts alone on
>    the node; "fewer under concurrency" is the existing `duckDbThreadsUnderLoad`). **Stand (large hosts):** a lone
>    TPC-H SF10 query on 16+ cores with `duckDbThreadsWhenIdle=<cores>` vs default, and the load test with both on.
>    *Fix 2026-10-28 (9efe670, stand p19b):* thread floor 4 (cores / 4 in 4..16), per-GiB cap only above 4; the memory
>    split keeps its divisor (cores / (cores / 4 in 2..16)).
> 4. ~~item 18 step 2~~ - **moved to lane 3** (`docs/dev/LANE3-STATUS.md`, branch `lane/distributed`, 2026-10-28). Was: distributed complete mode (answers below; opt-in `distributedComplete`, default false).
> 5. ~~item 17 (a) - local-disk tier of the S3 read cache: opt-in (`arrowflight.s3.cacheDiskPath` + size cap, default off;
>    whether production has a local disk is unknown, so it must stay optional).~~ *Done 2026-10-28 (ea55a4f, in pre-release `server-1.0.0-177dfe3e5716`, see Log):*
>    `cacheDiskPath` + `cacheDiskMegabytes` (s3 and lakefs), write-behind block files, CRC-32C checked, LRU in bytes.
>    **Stand / screening:** S3 SF10 JDBC + repeated small queries with `cacheMegabytes=256`, `cacheDiskPath` on a local
>    disk, `cacheDiskMegabytes=16384` vs memory-only 1024: timings, RSS, `diskHits=` / `diskDropped=` in `s3=cacheClosed`.
> 6. then the stand rounds of items 15, 16, 19 and the other open items as before.

> **Stage 2, approved by the maintainer 2026-10-26 - take these in this order before the other open items** (basis: `docs/dev/TUNING.md`
> and the speed comparison in `docs/dev/UPSTREAM-SYNC.md`; each one: code + tests + CI green + a `package=true` pre-release when it
> changes runtime behaviour, so the coordinator can measure it on the stand):
> 1. ~~item 14 - DuckDB 1.5.6~~ - code done 2026-10-27 (see item 14 and Log); stand rerun pending.
> 2. item 15 - per-query overhead (target <= 3 ms over plain DuckDB for a point query) - steps 1, 2, 2 (a), (b) and 3 done 2026-10-27 (see item 15); stand measure pending, then close.
> 3. ~~item 0g - S3 connections: reuse/pool connections so 32 clients x 8 threads do not exhaust local ports (TIME_WAIT).~~ - code done
>    2026-10-27 (6acc9cc, pre-release `server-1.0.0-6acc9cce0b26`; `arrowflight.s3.connectionCaching`, default true = DuckDB 1.5 `httpfs_connection_caching`; see item 0g and Log); stand rerun pending.
> 4. item 17 (new) - S3 read path: a memory-bounded DuckDB external file cache (unbounded it was ~40x faster on S3 but OOM-killed
>    the server), parallel range reads / prefetch, metadata (footer, listing) cache. Goal: close the 2-3x gap to HDFS.
>    *2026-10-27 findings (cloud session, DuckDB 1.5.6 + fake S3):* (i) footers: DuckDB 1.5.6 already keeps a file's HEAD + footer
>    per connection: a repeated 4-file query makes 12 instead of 20 requests with `parquet_metadata_cache` off, and turning it on
>    changes nothing (20 -> 12 both ways) - no footer-cache setting needed; listing cache not looked at yet. (ii) memory bound:
>    1.5.6 has no size setting for the external file cache (only `enable_external_file_cache` / `validate_external_file_cache`);
>    it lives in each connection's buffer pool, so the only bound is `duckDbMemoryLimit` x live connections (one DuckDB per
>    worker thread). A real bound needs either a shared DuckDB database (UPSTREAM-SYNC S5, rejected for view isolation) or a
>    node-wide cache of our own below DuckDB - design question, next session. (iii) bug found and fixed (7562a5b): the
>    per-connection metadata makes the first query after an S3 object is rewritten fail with httpfs's ETag-changed error on
>    every connection that read it before; now retried once before the first batch (pre-release `server-1.0.0-7562a5bdb7b9`, see Log).
>    *2026-10-27 (second cloud session):* (iv) listing: already cached on the Java side (snapshot per data directory,
>    `listingTtlSeconds`, background refresh) and DuckDB gets explicit file URIs, so it never lists - nothing to add. (v) range
>    reads (`S3ReadPathPerfTest`, fake S3 with 10 ms per request, 8 files x 4 row groups x 100k rows): with the plugin's
>    defaults a repeated query sends no HEAD and no footer read, one GET per row group (all its columns together), a
>    pruned point query 2 GETs (~70 ms vs ~48 ms at 0 ms latency, i.e. ~2 round trips); more threads read row groups side
>    by side (scan 1346 / 503 / 448 ms at 1 / 4 / 8 threads). `prefetch_all_parquet_files` changes nothing on S3;
>    `metadataCache=false` costs a HEAD + footer GETs per file and query (point 71 -> 179 ms at 8 threads). So no new
>    read setting; the pattern is now pinned in CI (`S3IntegrationTest.repeatedQueriesReadOnlyTheRowGroupsTheyNeed`).
>    What is left of item 17 is the bounded data cache: Open questions, "S3 read cache with a memory bound".
>    *2026-10-27 (third cloud session): option A step 1 done (84fec77, pre-release `server-1.0.0-84fec772ec50`, see Log):* opt-in `arrowflight.s3.cacheMegabytes`
>    (default 0 = off) + `cacheCheckSeconds` (default 10): node-wide heap LRU of 1 MiB blocks behind a 127.0.0.1
>    endpoint, SigV4-checked random key per store, misses with `If-Match`. **Stand:** S3 SF10 JDBC and the 32-client
>    small-query step with `arrowflight.s3.cacheMegabytes=1024` (and `-Xmx` >= 2.5g) vs off: timings, RSS, the
>    `s3=cacheClosed` hit/miss line; expect repeated rounds near the HDFS numbers and no OOM. **Next steps of 17:**
>    ~~(a) local-disk tier with a size cap~~ (done 2026-10-28, ea55a4f, opt-in, see Log);
>    ~~(b) lakeFS stores through the cache~~ (done 2026-10-27, 752e272: opt-in `arrowflight.lakefs.cacheMegabytes` /
>    `cacheCheckSeconds`, own cache per plugin class loader, metrics `arrowflight_lakefs_cache_*`, pre-release
>    `server-1.0.0-752e272b3ab6`; see Log); ~~(c) single-flight for concurrent misses of one block~~ (done 2026-10-27,
>    8625e28, pre-release `server-1.0.0-8625e28f08cd`: one S3 read per missed block for all concurrent requests,
>    `joined=` in the `s3=cacheClosed` line); ~~(d) metrics~~ (done 2026-10-27, b28fedf, pre-release `server-1.0.0-b28fedf9edbf`:
>    `arrowflight_s3_cache_*` on `/metrics` through the new SPI 1.3 `StoreContext.metrics()`, see Log). **Stand
>    (optional):** one round of the S3 SF10 JDBC step at `duckDbThreads=8` vs 4 to see how much of the HDFS gap is
>    round trips per thread (feeds item 19: a lower thread default may cost more on S3 than on HDFS).
> 5. item 18 (new) - distributed complete mode: in a cluster the planning node computes a complete answer alone; let each node
>    compute its part (partial aggregates / filtered scans) in parallel and merge on the planning node, for queries where this is
>    provably correct; fall back to today's path otherwise. Opt-in setting first; must keep the complete-mode wire contract.
>    *Step 1 done 2026-10-27 (895dc20, no runtime change):* `CompleteAnswerMerge` - which query shapes merge provably
>    (single table; plain columns, count/sum/min/max, GROUP BY of selected columns, DISTINCT, ORDER BY over output
>    columns, LIMIT except on aggregates) and the DuckDB merge SQL over the union of endpoint answers; checked against
>    DuckDB over all rows incl. an empty endpoint, names and types (`CompleteAnswerMergeTest`). **Step 2 is blocked on
>    Open questions "Item 18: node-to-node calls"** (how the planning node calls its peers, under whose identity).
>    Then: step 2 = node-to-node DoGet of the partial endpoints + merge in a DuckDB of the planning node behind an
>    opt-in setting (`distributedComplete`, default false), fallback to today's path on any refusal or peer error
>    before the first batch; step 3 = cluster integration test (3 nodes, answers = single-node answers).
> 6. item 16 - single-stream scan opt-ins as defaults for plain scans (only if answers unchanged). *Local check done 2026-10-27
>    (see item 16): no default change - `false` is slower, 64 MiB within noise; the stand round decides.*
> 7. ~~item 19 (new) - size-aware `duckDbThreads` default (cores/4, clamped 2..8); default change approved as part of stage 2.~~
>    Code done 2026-10-27 (04e5580, package run 40536373045 green, pre-release `server-1.0.0-04e558056068`; see Log): unset or 0 = cores / 4 between 2 and 8, explicit values
>    unchanged, startup line `duckdb=threads perConnection= availableProcessors=`. **Stand:** check the startup line on the
>    stand hosts (cores the JVM sees in the container) and rerun small JDBC/ADBC c1/c8/c32 + SF10 JDBC (HDFS and S3) with
>    the default vs `-DduckDbThreads=4`; together with the item 15 round. If S3 heavy queries lose much at 2 threads on small
>    hosts, a per-store default is the next question.

0g. ~~**Bug (stand, 2026-10-26, build 57d252b1437f): S3 + `duckDbThreads=8`, 32 concurrent small queries fail with
    ephemeral-port exhaustion.**~~ — **cause found, workaround + startup WARN done 2026-10-26 (9f7eb4f, see Log); the
    server-side fix needs a decision (Open questions, "S3 connection churn").** **Server-side fix done 2026-10-27 (6acc9cc, package run 40470316519 green, pre-release
    `server-1.0.0-6acc9cce0b26`; see Log):** DuckDB 1.5.6's httpfs has `httpfs_connection_caching` (default off; churn measured unchanged on 1.5.6: 40 + 31
    connections for two queries, 8 files x 4 row groups, 8 threads); the s3 plugin now turns it on (new setting
    `arrowflight.s3.connectionCaching`, default true): 8 + 0 connections, later queries 0, a cut connection is reopened.
    **Stand:** rerun the 32-client small-query step at 8 threads on the pre-release *without* `tcp_tw_reuse=1` (expect 0
    failures, TIME_WAIT toward S3 near 0, startup line `s3=open ... connectionCaching=true`, no `s3=ephemeralPorts` WARN)
    and the SF10 JDBC S3 round (timings vs 45fc0a9/e46f28d; expect equal or faster). DuckDB 1.4.5 httpfs keeps HTTP
    clients per open file handle only, and at 2+ threads a thread opens a file anew for each row group it takes, so
    about every range read opens a new connection (`http_keep_alive`, the curl client and the prefetch settings change
    nothing; measured 78 connections / 96 requests per query at 4-16 threads vs 32 at 1 thread, 16 files x 4 row
    groups). Only `duckDbExternalFileCache=true` (DuckDB default, ours off) stops it for repeated reads: a repeated
    query on the same connection opens 0 connections. **Stand:** rerun the 32-client small-query step at 8 threads
    with `--sysctl net.ipv4.tcp_tw_reuse=1` (expect 0 failures, check the startup WARN is gone), and once with
    `duckDbExternalFileCache=true` (count TIME_WAIT as before; note RSS). Original report: Every round (3/3): 32-64 ADBC and 0-1 JDBC queries of the 32-client small-query step fail
    "IO Error: Could not establish connection error for HTTP GET to '<s3 endpoint>/.../data_N.parquet'" (123-192 server
    log lines per round). Measured in the server's network namespace every 5 s: sockets in TIME_WAIT toward the S3
    endpoint reach 28231 = the whole `ip_local_port_range` (32768-60999) during that step; 4 threads peak at 20-22k
    (70-78 %, no failures yet), the SF10 JDBC step 18-24k at both. TIME_WAIT lasts 60 s, so the ~6 s retry of 0e cannot
    bridge it. Fix: reuse HTTP connections (httpfs keep-alive across queries / one connection cache for all DuckDB
    instances of a node, check `http_keep_alive` and whether each worker's instance opens its own), or fewer, larger
    range reads; test: count new connections per query at 8 threads x 32 clients against a local S3 (rclone/MinIO) and
    assert it stays bounded. Document the deployment workaround (`net.ipv4.tcp_tw_reuse=1`, wider port range).
    (MinIO's `MINIO_API_REQUESTS_MAX` not checked; the failures are TCP connect errors, not 503 throttling.)

0h. **Recheck (stand): 3-node heavy4 slower on 57d252b1437f?** 4 concurrent heavy JDBC queries (q01 q09 q18 q21, SF10,
    complete mode, 4 threads): slowest 5.2 s (rounds 4.9-5.3) vs 4.4 s (3.7-4.7) for 4eaab052deaf in the same session;
    single node unchanged (2.6 vs 2.6 s), cluster SF10 JDBC not slower (13.7 vs 15.3 s). Possibly the outbound budget
    (per-stream share = budget / open streams) or the planner change; rerun with more rounds before acting.

0f. ~~**Bug (stand, 2026-10-26): 3-node cluster, 8 concurrent partial-mode full scans -> streams CANCELLED and a node
    dies.**~~ — done 2026-10-26 (dd31117, see Log): all streams of a node share one outbound budget
    (`flightOutboundBudgetBytes`, default a quarter of the direct memory limit; each stream budget / open streams,
    at least 1 MiB). **Stand:** rerun the 8-client x 3-endpoint lineitem SF10 rounds (1 and 8 DuckDB threads) on
    a pre-release with this commit; check the startup line `Flight outbound budget: ...` and that no endpoint read
    fails and no node dies; note the timings against the single-server run. If a node still dies without a Java
    error, it is native memory (DuckDB `duckDbMemoryLimit` x live connections, see Open questions), not Flight.
    Original report: 3 servers (HDFS, `-Xmx2g`, `duckDbMemoryLimit=1GB`, 4 GB containers, server 4eaab052deaf), 8 clients each
    reading all 3 endpoints of lineitem SF10: 7-10 endpoint reads per round fail "CANCELLED: Server sendMessage()
    failed" in every round (1 and 8 DuckDB threads); with 8 threads the node serving the coordinator role was gone
    after the scan in 2 of 2 valid rounds (container removed, no exit info). A single server with the same clients has
    0 errors. Same mechanism as item 13b (256 MiB backpressure per stream exhausts direct memory): bound the per-stream
    and total outbound buffer (13b default 16 MiB and/or a global budget), fail a stream instead of the process, and
    add a test with many concurrent streams under a small `MaxDirectMemorySize`.
    **Correction (2026-10-26, stand):** the "node dies" part was a benchmark-harness artifact, not a server crash: the
    harness memory guard removes the server container when host MemAvailable falls below 4.5 GB, and this scan needs
    ~10-11 GB of host memory (3 servers + client); both removals fell inside the scan step. The CANCELLED errors were
    real. **Stand result on 57d252b1437f:** 8 clients x 3 endpoints, 0 failed endpoint reads in 6/6 rounds (4 and 8
    threads), all 480 M rows, no node lost, max node RSS 4.1 GB, startup line `Flight outbound budget: 536870912 bytes
    shared by all streams (per stream at most 268435456, at least 1048576)`; control 4eaab052deaf in the same session:
    5/7/10 failed reads per round (direct-memory OOM -> CANCELLED). Fixed. Timings in `docs/dev/TUNING.md`.

0e. ~~**Bug (stand, 2026-10-26): S3 + `duckDbThreads=8` -> every complete-mode query fails**~~ "Could not
    establish connection" — done 2026-10-26 (c71ee2e, see Log): DuckDB now retries S3 requests (connect errors
    included) for ~6 s instead of its own ~0.5 s; settings `arrowflight.s3.httpRetries` / `httpRetryWaitMs` /
    `httpRetryBackoff`. **Stand:** rerun SF10 JDBC on MinIO at `duckDbThreads=8` (and 4 as control, timings). If
    it still fails, the endpoint refuses for longer than 6 s: count client sockets in TIME_WAIT during the round
    (`ss -tan state time-wait | wc -l` vs `net.ipv4.ip_local_port_range`) and MinIO's
    `MINIO_API_REQUESTS_MAX` — httpfs opens a new connection for most range GETs at 8+ threads (measured: 144
    connections / 352 requests at 8 threads, 322 at 32, 32 at 1), so ephemeral-port exhaustion is the likely
    limit; then raise `httpRetries` or report the numbers. Also recheck the S3-outage time (3-node stand (a)):
    a read now retries ~6 s before failing.
    **Stand result on 57d252b1437f (2026-10-26):** SF10 JDBC on MinIO at 8 threads 22/22 in 3/3 rounds (24.2 s; 4 threads
    33.4 s), all raw scans complete -> heavy path fixed. The 32-client small-query step still fails at 8 threads:
    ephemeral-port exhaustion, measured -> new item 0g.

0d. ~~**Intermittent `GetFlightInfo` TIMED_OUT with a cluster member down (StarRocks connector CI)**~~ — done
    2026-10-26 (8e755fb, see Log): the planner's node-load read (and, on shared storage, the inventory read) was a
    Hazelcast whole-map query (`entrySet`), which waits ~10 s after a member left whose address stops answering
    (container gone); now key lookups of the current members. A stopped server leaves Hazelcast gracefully
    (Hazelcast's terminating shutdown hook off, graceful wait max 5 s). Left: a *crashed* node (kill -9, host gone)
    still blocks every map operation on its partitions until Hazelcast drops the member (10 s, see Open questions).
    StarRocks repo: rerun marten m4 check 6 on the next pre-release before moving its default off 20b887839edf.

0c. ~~**Maintainer decision 2026-10-26: make `duckDbThreads` default 4**~~ — done 2026-10-26 (60e888b, CI run
    40245198713 green, pre-release `server-1.0.0-60e888bc26ec`). Items 13b-d stay as proposals (behaviour/default
    changes there still need approval).

0b. ~~**Bug: `duckDbStatementCacheSize=0` breaks parameterized prepared statements**~~ — done 2026-10-25 (see Log):
    `streamComplete` streams its bound statement directly; the parse check in `streamSql` used to evict and close it
    from a one-statement cache (0 is clamped to 1). Stand: rerun the perf screening's cache-size-0 case (JDBC + ADBC).

> **Maintainer, 2026-10-25:** the production Hadoop cluster has HA NameNode with ZooKeeper (automatic failover),
> Kerberos and Ranger. Versions (2026-10-25): Hadoop 3.1.3 (vendor build) on the cluster side; HA with 2 NameNodes,
> automatic failover, ConfiguredFailoverProxyProvider, RPC 8020; ZooKeeper 3.9.2 ensemble, SASL/Kerberos; Ranger 2.4.0
> (HDFS authorizer on the NameNodes; plugins also for Hive). Our client libs must work against a Hadoop 3.1 cluster
> (RPC/SASL compatibility of the newer hadoop-client), and Ranger checks must target the 2.4 REST API. Consequences: item 11 (ZooKeeper and lz4-java missing from the new
> runtime libs set) is now required — put the ZooKeeper client (and its Curator/Jute deps as Hadoop needs them) and
> lz4-java back into the libs set, with a startup/link check that the HA/ZK and Kerberos code paths load. Keep items
> 7 (Kerberos for HMS and HDFS), 8 (Ranger subquery bypass) and 9 (per-user identity) — they are needed for production.

0a. ~~**hadoopfs timeout defaults 3 s / 10 s**~~ — done 2026-10-24 (732e238, CI run 40140133221 green).

> **Stand, 3-node S3 cluster on int7 (2026-10-25)** (unnumbered; sessions keep taking the numbered items, see Open questions): all correct (JDBC 22/22, Trino 22/22, B2, B5, mid-stream kill, ticket matrix, MinIO outage + recovery without restart). Server follow-ups, none give wrong results: (a) during an S3 outage each read needs 20–40 s to fail (DuckDB httpfs retries + DNS); make S3 retry/timeout settings explicit and shorter when the endpoint is unreachable; (b) ~~plans that are never read keep their node load reservation up to 10 min~~ — partly fixed 2026-10-26 (9168a44, see Log): `CancelFlightInfo` / `CancelQuery` release a plan's reservations at once; clients that abandon a plan without cancelling still hold them 10 min (shorter TTL = default change, Open questions). Original: skewing later plans (one node got no endpoints ~5 min) — release reservations on ticket expiry/cancel or shorten the TTL; (c) ~~after B5 the first plan on S3 waits 9–10 s while dead members are dropped (HDFS cluster: ~1 s) — find why~~ — cause found 2026-10-27 (9934d3b, see Log): a crashed node whose *address* vanished (container removed) holds every map operation on its partitions until `hazelcast.max.no.heartbeat.seconds` (10 s); one whose port still refuses is dropped in ~0.35 s — so the two stands most likely differed in how the nodes died. Same mechanism as the 0d crash Open question (faster detection = maintainer's call); `-D` override documented in START-NOTES. **Stand:** note how B5 kills the nodes on both stands (process vs container/host); optionally rerun B5 on S3 with `-Dhazelcast.max.no.heartbeat.seconds=5`.

> **Stand regression, single node, build 4eaab052deaf with the new runtime libs set (2026-10-25)** (unnumbered): the
> release jar + `flight-server-libs` (60 jars, Hadoop 3.5.0) instead of the old 335-jar libs dir, against int7 on the
> same node, alternating runs. HDFS (HA nameservice, hadoopfs 51b7220-afc3): startup log shows timeouts 3000/10000,
> extension versions and `storageSetupVerified` (lockdown check); TPC-H SF10 JDBC 22/22 `9e2af63e8711` x3, Trino 455
> + plugin v10 22/22 `05bc0dc2713a` x3, jdbc.sh 27/2, safety 82/82. S3 plugin on MinIO (read-only user): JDBC 22/22
> x2, 27/2 + 82/82. HMS catalog (hdfs + s3 roots, libthrift 0.24.0): reference queries 76 OK / 2 expected / 11
> time travel in complete and partial mode (= int7). Iceberg local warehouse: 9/9 answers = int7, refusals intact.
> Timings equal within noise (HDFS JDBC warm 24.6 vs 24.7 s, Trino 43.9 vs 43.7 s, S3 JDBC 33.0 vs 32.9 s); JVM
> RSS ~50 MB lower idle, same peak (2.1–2.5 GB after Trino). No missing class on any tested path; a Kerberos keytab
> login (no KDC) reaches JAAS. Follow-ups: items 10 and 11.

> **Stand, secure HA HDFS (production profile), build 4eaab052deaf (2026-10-25)** (unnumbered): on-demand stand with an
> MIT Kerberos KDC, ZooKeeper 3.9.2 x3 (SASL client auth required + quorum SASL), Hadoop 3.1.3 with 2 NameNodes + ZKFC
> (automatic failover, ConfiguredFailoverProxyProvider, RPC 8020), 3 JournalNodes, 3 DataNodes (Kerberos, block access
> tokens, HTTPS_ONLY), Ranger Admin 2.4.0 (PostgreSQL) with the Ranger HDFS plugin on both NameNodes (policy download via
> SPNEGO; test policies: one user reads `tpch` only, another nothing, the server principal reads all). Data SF1 tpch +
> types. Results:
> - **Works:** Kerberos Flight authentication (`FLIGHT_KERBEROS_*` + TLS) with a real KDC: keytab login, SPNEGO
>   `Negotiate` handshake from a Java client, bearer session; the Java Hadoop client of the new libs set (3.5.0) against
>   the 3.1.3 cluster: Kerberos RPC, HA nameservice and failover — no ZooKeeper class is needed on this client path
>   (item 11 is still right for the elector/ZK code paths, not hit here). Queries with hadoopfs (libhdfs3, Kerberos from
>   a ticket cache) read correctly once DataNodes do not require SASL on data transfer (secure DataNodes on privileged
>   ports). Ranger: disabling the server principal's policy makes queries fail with HDFS `Permission denied` within the
>   10 s policy poll; enabling it again restores them.
> - **(a) hadoopfs cannot read from DataNodes with SASL data transfer protection** (`dfs.data.transfer.protection`, the
>   usual Hadoop 3 secure setup without privileged ports) nor with `dfs.encrypt.data.transfer=true`: NameNode RPC works,
>   every block read fails (`Read 8 bytes failed from <dn>:9866`; DataNode: `InvalidMagicNumberException`, the client never
>   sends the SASL handshake). The Java client reads fine. Production must use privileged-port DataNodes, or the
>   extension needs the SASL data-transfer handshake (libhdfs3) — check which one production uses.
> - ~~**(b) hadoopfs needs a ticket cache:**~~ done 2026-10-26 (2b2cac8, see Log): optional `hdfsKerberosPrincipal` +
>   `hdfsKerberosKeytab` make the server write and renew the MIT ticket cache the extension reads (default: LIBHDFS3_CONF's
>   `hadoop.security.kerberos.ticket.cache.path`, else `KRB5CCNAME`, else `/tmp/krb5cc_<uid>`). **Stand:** on the secure
>   HA stand drop the external `kinit`, set both settings (server keytab), check the startup line
>   `hdfs=kerberosTicketCache ... ticketEnd=...` and no cache WARN, run queries; with a short `ticket_lifetime` (e.g.
>   10 min) check that queries keep working past the first lifetime (a renewal line every ~8 min). Original: with only the server keytab (no `KRB5CCNAME` cache) the Java side starts and
>   lists, every query fails `IO Error: Unable to connect to HDFS: hdfs://<ns>`. The server does not hand its keytab
>   login to the extension; today an external `kinit -kt` (+ renewal before expiry) is required. Options: the server
>   writes and renews a private ccache from its keytab, or the extension takes keytab + principal settings.
> - ~~**(c) Short-circuit is forced on:**~~ fixed 2026-10-26 (see Log): the deployment's value is kept, otherwise on only with a `dfs.domain.socket.path`. Original: `HadoopDataStoreProvider.open` sets `dfs.client.read.shortcircuit=true`; a client
>   config without `dfs.domain.socket.path` (no local DataNode) stops the server at start (`The short-circuit local reads
>   feature is enabled but dfs.domain.socket.path is not set`). Keep the deployment's value / make it a setting.
> - **(d) NameNode host crash with libhdfs3 defaults:** partly done 2026-10-26 (8e0307f, see Log): the hadoopfs
>   extension (afc3) has no RPC timeout setting, so the server cannot set them in DuckDB; it now WARNs at startup on HDFS
>   for each of the four `rpc.client.*` timeouts LIBHDFS3_CONF leaves unset or over 60 s, and the values below are
>   documented (`arrowflight.properties`, START-NOTES). **Stand:** put them into the stand's/production LIBHDFS3_CONF.
>   Making the server supply them needs extension settings (`hdfs_rpc_*`, hadoopfs repo) — ask if wanted. Original: killing the active NameNode *process* costs the running query
>   ~10 s (60/60 OK). Killing the whole active NameNode *host* (its address disappears; new active after ~27 s): one
>   query hung 170 s and failed (`Failed to invoke RPC call "getFileInfo" on server <old active>:8020 ... Connection reset
>   by peer`), later queries OK. With `rpc.client.connect.timeout=5000`, `rpc.client.read.timeout=15000`,
>   `rpc.client.write.timeout=15000`, `rpc.client.timeout=20000`, `rpc.client.connect.retry=2` in the libhdfs3 conf: 80/80
>   OK, the slowest query 32.6 s (= the failover itself). Consider shipping these as defaults next to the DataNode
>   timeouts (item 0a) or documenting them.
> - ~~**(e)**~~ fixed 2026-10-26 (see Log): stack frames are dropped from every client error text. Original: HDFS errors reach the Flight client with the NameNode's Java stack trace in the message (e.g. the
>   `Permission denied` above, `FSPermissionChecker...`); trim to the first line.
> - As expected without item 9, every authenticated user reads what the server principal may read (Ranger sees only
>   the server principal). Kerberized HMS on this stand: prepared (principal + keytab, auth_to_local, proxyuser), not run.

0. **Upstream is active again (checked 2026-10-23)** — upstream `main` is still dbacd8d (our base), but new work sits on
   public upstream branches (read-only for us: `git fetch https://github.com/VadimSurpin/arrow-flight-server.git
   '+refs/heads/*:refs/remotes/upstream/*'`; never push there). Review per feature and port what beats ours (rule:
   try ours and theirs, keep the more effective variant per feature, our features stay; record the decision per item
   in docs/dev/UPSTREAM-SYNC.md with numbers where possible):
   - `feat/starrocks-native-integration` (86 commits, 10-16..10-23): server — stream DuckDB results instead of
     materializing (compare with ours), bounded DuckDB read-ahead + unordered scan output, apply client runtime
     IN-filters and bloom filters to plain scans, table-statistics Flight action, optional parallel ZSTD batch
     compression + wire-format matrix, narrow DECIMAL wire form (`decimalWire=narrow`), DuckDB spill to a temp dir,
     S3 via s3a + httpfs (compare with our S3 plugin), join key reduction / parse cache limit, exit on failed start;
     plus their StarRocks fork patches (runtime filters to the Flight server, aggregate predicate pushdown) — note
     anything our StarRocks/Trino connectors should adopt in Open questions (the coordinator forwards it).
   - `feat/trino-flight-connector` (11 commits, 10-16..10-22): their own Trino connector (`flight-trino`: Kerberos
     auth, statistics, routing/partitioning, types) — compare with our plugin (separate repo); server-side bits only here.
   - `perf/pushdown-statistics` (3 commits, August): check if anything is still useful.
   - Dependabot PRs upstream: hazelcast 5.7.0, netty 4.2.19, grpc 1.85.0 — fold into 1b.
   - **Progress:** triage of every server-side feature is in `docs/dev/UPSTREAM-SYNC.md` (rows S1..S12, T1..T2).
     S1 (exit on failed start) ported, S2 ours already, S5 (shared DuckDB database) rejected as-is (breaks our
     per-connection catalog views), S3 (spill size cap) ported as per-connection `duckDbMaxTempSize`, S4
     ported as opt-in `duckDbPreserveInsertionOrder` / `duckDbStreamingBufferSize` (order audit clean; stand
     measurement pending, see Open questions), S6 ported as opt-in `endpointsPerServer` (fixed upstream's
     split of single-node queries; stand measurement pending), S9 ported as action
     `arrowflight.table_statistics` (no NDV yet), T1 rejected in favour of S9, S10 rejected as-is
     (upstream drops the file lockdown for S3; speed comparison moved to the stand, Open questions),
     T2 ported (client pool identity covers every setting; routing headers on every call), perf
     branch: P1 (Q6/Q17) ours already, P3 ported (Spark post-pushdown statistics, filter part
     opt-in), P4 cost gate deferred to the stand, S11 (batch compression) deferred (Open
     questions), S12 ported to all three query-text caches together with item 6. The remaining
     item 0 work is only S7/S8 (wait on Open questions) and the stand measurements: 1b is next.
1b. **Dependency security upgrades** (audit 2026-10-23: `docs/dev/DEPENDENCY-AUDIT.md`; native/C++ items there are
   unverified). **Done 2026-10-24 (server module):** Hazelcast 5.7.0, Netty 4.2.19 under `netty-bom`, `netty-all`
   replaced (README notes the private Hazelcast port; `DependencyFloorTest`); `flight-spark` imports the same
   `netty-bom`. **Done 2026-10-24 (client group):** `flight-client-core` / `flight-starrocks` on the same BOM
   (Netty 4.2.19, tcnative 2.0.84, no `netty-all` from the provided Hadoop deps; `NettyFloorTest`).
   **Done 2026-10-24 (Jackson):** one Jackson 2.21.8 (`jackson-bom`) in server, client-core, spark and
   starrocks; the Spark connector's shaded jar relocates Jackson (Spark 3.5 keeps its 2.15); Iceberg plugin
   2.22.3; floor tests in every module. Stand: run one Spark read with the new shaded connector jar.
   Dev CI runs again (2026-10-24 22:49, repo public). **Done 2026-10-25 (Parquet read path):** server Parquet
   1.13.1 → 1.18.1, bringing aircompressor 2.0.3 and snappy-java 1.1.10.8 (`DependencyFloorTest.parquetCodecs*`).
   **Done 2026-10-25 (Hive plugin):** libthrift 0.9.3 → 0.24.0 for the metastore client (1 GiB response cap);
   the test metastore (Hive 3.1.3 server, broken on libthrift 0.14+) runs on 0.9.3 in its own class loader.
   Stand: one Hive reference run (framed and unframed if the HMS has both) with the new plugin dir.
   **Done 2026-10-25 (runtime libs set):** the build copies the Hadoop client libraries the shaded jar leaves to
   the deployment (`provided`) to `flight-server/target/libs` (58 jars, 42 MB, Hadoop 3.5.0, mediated against
   the jar's own libraries); the `package` dispatch publishes it as `flight-server-libs-<version>-<sha>.tar.zst`;
   `LibsSetTest` checks floors and starts the server on jar classpath + libs (Parquet read) and without libs
   (must fail). START-NOTES / README now start with `java -cp '<jar>:libs/*'` (`java -jar` never worked without
   Hadoop on the class path). Stand: replace the old libs dir with the new asset (first `package` dispatch
   pending, see Log) and run one HDFS + one S3 query. Follow-up (optional, after that stand run): prune what only
   Hadoop daemons use from the set — MapReduce/YARN client, Jetty 9.4.58 (EOL line), Jersey 2/hk2, leveldbjni,
   commons-daemon, jline, `hadoop-hdfs` (server) — the server's own code uses hadoop-common + hdfs-client only;
   the API jars overlapping the shaded jar (javax.servlet 4.0.0 vs jakarta.servlet-api 4.0.4, JAX-RS 1.1 in
   Ranger's `jersey-bundle` vs jakarta.ws.rs-api 2.1.6, javax.annotation/inject) would go with them.
   Item 1b is done apart from the stand checks named above.
2. **Iceberg equality deletes are correct but slow** — **complete mode done 2026-10-25** (5466f86): snapshots with
   equality deletes are read by `CatalogSources.deletesAppliedSource` (read_parquet by field ID, ANTI JOINs on the
   equality columns with IS NOT DISTINCT FROM, position deletes on normalised path + `file_row_number`; delete
   assignment = the plugin's `FileScanTask.deletes()`, i.e. Iceberg's sequence-number rules); deletion vectors,
   columns without field ID, equality columns the schema dropped, a column named `file_row_number` keep the old
   iceberg_scan read. **Partial mode done 2026-10-25** (see Log): `completeReason` accepts deletes that
   `CatalogSources.deletesApplicable` covers; the executing node re-derives each ticket file's delete files from
   the pinned snapshot (tickets unchanged) and reads them with `deletesAppliedSource`. **Remaining:** (ii) stand: time the int7 eq-delete
   queries (was 25–90 s) on the new source and check answers vs the old ones, in complete mode (JDBC) and partial
   mode (Trino, StarRocks: these tables now get one endpoint per node instead of one complete endpoint); ~~(iii) many data files with position
   deletes become one UNION ALL branch each — measure planning time on a table after a large MERGE.~~ Done
   2026-10-27 (4e77c6b, package run 40544759257 green, pre-release `server-1.0.0-4e77c6bda59a`, see Log): files sharing their delete files are one branch; 500 files with one shared
   position delete file: prepare ~750 -> ~20 ms, run ~1200 -> ~50 ms. **Stand (with (ii)):** a table after a
   large MERGE (position deletes over many files): planning + query time and answers vs the previous build. Original
   text: Iceberg equality deletes are correct but slow (full scan of all columns per query, 25–90 s; DuckDB 1.4.5's
   iceberg extension refuses or mis-applies eq deletes when filters/projections are pushed). Make them fast and
   correct: e.g. read data files + equality delete files yourself (anti-join on the equality columns with sequence-
   number rules), in complete and partial mode (partial: per-file anti-join on the node). Prove with a test table
   with eq + position deletes vs an independent answer.
3. ~~**Iceberg time travel**~~ — done 2026-10-25 (see Log): `FOR VERSION AS OF <id | 'tag'>` / `FOR TIMESTAMP AS OF
   TIMESTAMP '<time> <zone>'` after an iceberg-hadoop / iceberg-rest table name, complete mode (rewritten to a read of
   the snapshot's own files, deletes and columns); partial mode answers such queries completely. Follow-ups (optional):
   ~~(a) partial mode in parts~~ - done 2026-10-27 (b7be8d1, package run 40582448194 green, pre-release
   `server-1.0.0-b7be8d166498`, see Log): a partial query of one table with one clause
   and nothing else (no join, subquery, set operation) is planned in parts on the travelled snapshot; ~~(b) Iceberg tables registered in a Hive
   Metastore (`TableFormat` needs an `asOf`)~~ (done 2026-10-28, SPI 1.5 `TableFormat.asOf`, see Log); ~~(c) snapshots that cannot be read file by file (deletion vectors,
   renamed nested fields) are refused~~ (done 2026-10-28, 411e804, in pre-release `server-1.0.0-2cfa23eb62f9`, see Log: read by `iceberg_scan(...,
   snapshot_from_id => )`, which on DuckDB 1.5.6 uses the snapshot's schema; refused only when DuckDB's columns differ
   from the snapshot's declared ones). **Stand:** one travelled snapshot of a table with deletion vectors (v3, if the
   stand's writers produce them) vs Spark's answer. For the coordinator: Trino plugin / StarRocks connector may forward the clause.
4. ~~**Second S3 store**~~ — done 2026-10-25 (see Log): named stores `arrowflight.s3.stores=<name>,...`, each
   with `arrowflight.s3.<name>.root` and its own endpoint/region/URL style/credentials, own DuckDB secret
   `flight_s3_<name>`. Stand: one Iceberg or Hive catalog in a second MinIO bucket with another (read-only)
   user next to the S3 data directory; check both secrets in the startup log and one query per store.
5. ~~**Config precedence bug (upstream)**~~ — done 2026-10-25 (see Log): every `-D` name > environment >
   `etc/plugins/<ns>.properties` > `arrowflight.properties` > default, server and plugin settings alike.
   Stand: check that no leftover environment variable (e.g. `FLIGHT_RESULT_MODE`, `DUCKDB_*`) now changes
   behaviour that the bundled file used to hide.
6. ~~**Prepared-statement cache bound**~~ — done 2026-10-24 with UPSTREAM-SYNC S12: the statement cache already was
   an LRU of `duckDbStatementCacheSize` (default 64) per worker thread; now texts over 256 Ki chars stay out of it
   (and out of the parse and schema caches), evictions and skips are Prometheus counters.
7. ~~**Kerberos for the HMS client**~~ — done 2026-10-25 (see Log): `arrowflight.hive.authentication=kerberos`,
   SASL/GSSAPI as Hive's client (`metastorePrincipal` with `_HOST`, `saslQop` default
   `auth-conf,auth-int,auth`), login from `clientPrincipal` + `clientKeytab` or the ticket cache
   (`KRB5CCNAME` / `clientTicketCache`), renewed at 80 % of the TGT lifetime; framed + Kerberos refused.
   Tested with an in-process Kerby KDC and a SASL front for the test metastore (no kerberized real HMS in CI).
   Stand: the prepared kerberized HMS (principal, keytab, auth_to_local, proxyuser) — reference queries with
   `authentication=kerberos`, once from the keytab and once from a `kinit` cache, with the metastore's
   `hadoop.rpc.protection` = authentication and = privacy. HDFS Kerberos with hadoopfs: see the secure HA
   stand findings (a) above (data-transfer SASL) — still open, not numbered; (b) (ticket cache) done 2026-10-26.
8. **Ranger hardening** (security): details are kept outside this public repo; ask the maintainer before starting. (The maintainer wants Ranger/
   Kerberos finished before production; implement here, the stand check comes later.)
9. **Per-user identity** (needed for Ranger/Kerberos): JWT/bearer pass-through of the end user to Ranger checks;
   design in Open questions first. **Design written 2026-10-26** (Open questions, "Item 9"): phase A trusted-proxy
   header, phase B JWT provider, phase C storage-level identity; waits on the maintainer's answers.
10. ~~**Local catalog roots under an HDFS `fs.defaultFS`**~~ — done 2026-10-25 (see Log): a catalog location
   without a scheme (`/wh`, `file:/wh`, `file:///wh` all normalise to `/wh`) now opens a local store
   (`file:/wh`) instead of resolving against `fs.defaultFS`; the data directory as a plain path still follows
   `fs.defaultFS`. Stand: the int7 case (`-Darrowflight.iceberg-hadoop.warehouse=file:///wh` with the HDFS
   client config on the class path) must start and answer.
11. ~~**Runtime libs set gaps**~~ — done 2026-10-25 (see Log): the set now carries ZooKeeper 3.8.6 (+ jute,
   audience-annotations) and lz4-java (`at.yawk.lz4` 1.10.4), Curator is 5.2.0 in jar and set alike (the jar had
   curator-framework 2.13.0 with curator-client 5.2.0). `LibsSetTest` links every class of hadoop-common,
   hadoop-hdfs-client and hadoop-auth on the thin class path. Stand: replace the libs dir with release `server-1.0.0-c3b276054cc3`'s
   `flight-server-libs` asset and run one HDFS query against the HA nameservice; a real Kerberos login (KDC)
   is still item 7. Note: lz4-java 1.12.1 exists (we follow Hadoop 3.5.0's 1.10.4).

12. ~~**`s3a://` / `s3n://` catalog roots are not matched by the open S3 stores**~~ — done 2026-10-26 (514782d,
   see Log): `CatalogLocations.normalize` (server) and `Locations.normalize` (iceberg plugin) map s3a/s3n to
   s3, so store lookup (`holder`, both `locate`s), the roots checks, `read_parquet` lists and the position
   delete `file_path` match treat the three forms as one. Stand: one Iceberg table written by Spark through S3A
   (`s3a://` paths in its metadata) under an `s3://` or `s3a://` warehouse, complete and partial mode.

13. **Tuning defaults from the perf campaign** (stand + CI screening 2026-10-25/26, `docs/dev/TUNING.md`):
    (a) ~~`duckDbThreads` default 2 -> 4~~ done as item 0c (plain 4) (SF10 JDBC 1.8x, SF1 1.3x faster; single point lookup +14 %, 32-client p95
    unchanged), or a size-aware default (cores / 4, between 2 and 8); (b) `flightBackpressureThresholdBytes` default
    256 MiB -> 16 MiB (no measured loss; with `endpointsPerServer` 4/8 the 256 MiB per stream exhausts
    `MaxDirectMemorySize` and streams are CANCELLED); (c) ~~`duckDbExternalFileCache=true` has no memory bound and gets the
    server OOM-killed on SF10 (HDFS and S3) - bound it or document it as unsafe~~ (done 2026-10-28, 3042102, see Log:
    DuckDB 1.5.6 keeps the cache within each connection's `memory_limit`, so since 19b's default it is bounded per
    connection (node: limit x live connections); startup line `duckdb=externalFileCache`, WARN
    `duckdb=externalFileCacheUnbounded` with `duckDbMemoryLimit=none`; README documents it; default stays off. The
    node-wide bound for S3/lakeFS is item 17's read cache); (d) ~~ADBC Go driver connects rarely fail
    with "failed to update autocommit: rpc error: code = Unimplemented"~~ — fixed 2026-10-26 (4939fb7, see Log):
    the call is BeginTransaction (ADBC sends it only when it decoded SqlInfo TRANSACTION as supported); concurrent
    DoGet streams corrupted each other's empty buffers through Arrow's shared empty buffer (`EmptyListOffsets`).
    **Stand:** rerun the 8-client ADBC connect rounds (45 runs); expect no autocommit error and no
    `beginTransaction refused` WARN line in the server log; if one appears, note the ADBC version. Connector side (forward to the Trino
    connector repo): Trino q18 fails "Java heap space" (6 GB heap) as soon as `endpointsPerServer` > 1.

14. ~~**DuckDB 1.5.6**~~ — **code done 2026-10-27 (a442867 + 27f583a, CI run 40441645851 green, pre-release `server-1.0.0-27f583adccd7`); stand rerun
    done 2026-10-27 (short check, single node HDFS: small JDBC/ADBC c1/c8/c32, JDBC TPC-H SF1/SF10, 3 rounds vs 57d252b and
    upstream native; all checksums match; numbers in the Log). Not covered by this check: Trino, S3, Hive/Iceberg reference
    queries, 3 nodes.** **Coordinator (done 2026-10-27: both files are in `ci-assets`, sha256 in its SHA256SUMS; build distributed to the client repos):** put `duckdb-extensions-v1.5.6.tar.zst` (httpfs, iceberg, avro, hadoopfs `51b7220-afc3` /
    v1.5.6 under `v1.5.6/linux_amd64/`) and `hadoopfs-v1.5.6-afc3.duckdb_extension` into `ci-assets` (Dev CI uses it
    when present, else the official httpfs/iceberg/avro builds); the stand needs the v1.5.6 extension set, the
    startup check refuses the 1.4.5 files. **Stand:** P4 small + TPC-H SF1/SF10 (checksums 9e2af63e8711 / 05bc0dc2713a)
    + Trino, HDFS and S3, Hive/Iceberg reference queries; check the startup lockdown passes with hdfs:// and s3:// roots
    (1.5.6 stores a URI root whose scheme no loaded extension registers as a local path — fails closed at startup).
    Note for 0g/17: httpfs 1.5.6 uses a curl client by default and has `httpfs_connection_caching` (default false):
    measure connections per query with it on before building a pool of our own.
    Original: (upstream speed comparison 2026-10-27, `docs/dev/UPSTREAM-SYNC.md` "Speed comparison 2026-10-26"): both
    upstream builds bundle DuckDB 1.5.6; on the same HDFS files, without a Flight server, 1.5.6 answers a point lookup in
    5.6 ms vs 10.6 ms and a filtered count in 9.6 vs 15.8 ms (1.4.5). The hadoopfs extension from our patched sources builds and
    loads for v1.5.6 (footer `51b7220-afc3` / `v1.5.6`). Upgrade `duckdb_jdbc` to 1.5.6.0 and the CI extension set (httpfs, iceberg,
    avro, hadoopfs), run the full suite, then the stand rerun (P4 small + TPC-H SF1/SF10 + Trino). Expected: 5-6 ms less per
    small query (25-45 % of the small-query p50).
15. **Per-query overhead** (same comparison): a point query costs ~15-20 ms over Flight vs ~6-11 ms in plain DuckDB; upstream
    (even its fork-point `main`, which keeps per-connection instances like ours) answers point/count queries 35-50 % faster
    and TPC-H SF1 via Trino 17 % faster. Profile GetFlightInfo -> first batch for a point query (per-connection catalog view
    refresh, inventory planning, load accounting) and cut what is not needed for a single-node plain query; target <= +3 ms
    over raw DuckDB. Regression test with a latency budget is not reliable in CI: measure on the stand.
    *Stand 2026-10-27 (p156, DuckDB 1.5.6 build 27f583adccd7 vs 57d252b1437f):* single-client latency improved ~17 %, but
    under concurrency it got worse: JDBC c8 p95 +18 %, c32 p50 +13-15 % (JDBC and ADBC), consistent over 3 rounds. Check whether
    1.5.6 needs different per-connection settings (threads / scheduler / connection reuse) and include c8/c32 in the target.
    **Step 1 done 2026-10-27 (775100b, CI run 40453394028 green, pre-release `server-1.0.0-775100bf923f`; see Log).** Local profile (`PointQueryOverheadPerfTest`, tag `perf`, 4 cores,
    client in the same JVM, 400k rows / 4 files, varying literal per query): plain DuckDB point 1.7 ms; over Flight in
    complete mode (default) 14.0 ms = prepare 3.6 + GetFlightInfo 3.7 + DoGet 5.0 + close 1.2. Cause: three DuckDB
    describes and four parse checks of the same text per query (prepare: result + parameter schema; GetFlightInfo:
    result again). Now one description per (text, views snapshot) serves both, the describe connection keeps its
    parse-check statement, close does not wait for Hazelcast: 10.2 ms (prepare 1.9, info 2.2, DoGet 4.7, close 1.0),
    c8 p50 31 -> 22 ms, c32 94 -> 58 ms. Server-side work left per query: describe ~0.4 ms, DoGet parse check +
    prepare + run ~1.8 ms (raw 1.5-1.7), Hazelcast ~0.05 ms; the rest is the 4 gRPC calls stock clients make (~1 ms
    each here). **Stand:** rerun small JDBC/ADBC c1/c8/c32 on the pre-release of this commit vs 27f583a.
    **Step 2 (part) done 2026-10-27 (80ccac9, CI run 40459493162 green, pre-release `server-1.0.0-80ccac9d9de6`; see Log):**
    the parse check of a text runs once per node (shared by describe and DoGet), DEBUG-only log arguments are no
    longer built, the execution-path event reuses its regex/digest. Local (same test, noisy 4-core box, before ->
    after in the same session): complete point total p50 12.8-13.3 -> 11.9 ms, DoGet 6.1-6.4 -> 5.4 ms, c32
    74-82 -> 71 ms. What is left in a complete-mode DoGet (JFR): DuckDB prepare + execute of the new text
    (~= plain DuckDB), Arrow C import + Flight schema/batch messages, the INFO `executionPath=` line ((b)
    below). Local-profile artifacts that do *not* happen on the stand: GetFlightInfo's `allServerLoads` Hazelcast
    `entrySet` (only for non-shared storage, i.e. local dirs; ~60 % of the local info phase) and the partial-mode
    `sampleFile` listing forking `ls` for local-FS permissions. **Stand:** small JDBC/ADBC c1/c8/c32 on this
    pre-release vs 775100b and 27f583a. c8/c32 vs 1.4.5 not explained yet: at c32 with `duckDbThreads=4` every
    worker's DuckDB has 4 threads (128 on 4 cores locally); worth one stand round at `duckDbThreads=2` (c32) to see
    whether the 1.5.6 regression is scheduler oversubscription (feeds item 19).
    **Step 2 (a) done 2026-10-27 (01cdd57, CI run 40462267120 green; pre-release `server-1.0.0-45fc0a9e6e02` with (b)):** partial-mode describes bind against a sample file remembered
    per table for 30 s (re-listed once when a describe cannot read it). Local, before -> after in one session:
    partial point prepare p50 28.5 -> 3.4 ms, total 38.4 -> 12.5 ms, c8 97 -> 28 ms, c32 73 -> 74 ms; complete
    mode unchanged (point 15.8 / 16.4 ms on this noisier box). **Stand:** partial-mode small queries (JDBC/ADBC
    with `x-flight-result-mode: partial`) c1/c8 and complete c1/c8/c32 on `server-1.0.0-45fc0a9e6e02` vs 80ccac9.
    **Step 2 candidates left:** (b) the INFO line
    `executionPath=...` per DoGet goes through slf4j-simple to stderr synchronously (contention at c32): consider
    DEBUG or an async logger (log change -> ask); **Maintainer 2026-10-27: approved - make it asynchronous, keep the line at INFO.** (c) the DoGet parse check repeats the describe's (needed: DoGet can
    reach a node that did not describe).
    **(b) done 2026-10-27 (45fc0a9, package run 40463504538 green, pre-release `server-1.0.0-45fc0a9e6e02`):** the `executionPath=` INFO line is written by a background thread
    (approved above). Local, same session: complete point p50 15.8 -> 15.1 ms, c32 90 -> 78 ms; partial c8 28 -> 24 ms.
    **Proposal to close item 15** after the stand round: the server-side work per query is now ~= plain DuckDB
    + ~1 ms; what remains is the 4 gRPC calls stock clients make (client side, not ours to cut).
    **Step 3 done 2026-10-27 (e46f28d, package run 40467177476 green, pre-release `server-1.0.0-e46f28dfdf09`; see Log):** a complete-mode prepare of a query
    without `?` parameters returns the plain SQL handle, so GetFlightInfo skips the synchronous Hazelcast `get`
    of parameter values and close skips the delete (both remote on a multi-node cluster). Server-side TIMING of
    a complete point DoGet (local, c1): `execution.total` 3.6 ms, of which DuckDB prepare + run to the first batch
    2.5 ms (plain DuckDB over the same files 1.9 ms); client-side DoGet 4.9 ms (the rest = the gRPC call).
    **c8/c32 check (local, same session, `perf.threads` 4 / 2 / 1):** complete c32 p50 64-69 / 57 / 51 ms, c8
    22-23 / 22 / 20 ms; c1 point unchanged, c1 count 9.2 / 11.3 / 13.2 ms. So the concurrency loss is mostly DuckDB
    thread oversubscription (32 sessions x 4 threads on 4 cores), the item 19 question, not per-query overhead.
    **Stand:** small JDBC/ADBC c1/c8/c32 on `server-1.0.0-e46f28dfdf09` vs 45fc0a9, plus one c32 round at
    `duckDbThreads=2`; then close item 15 (remaining time is client gRPC round trips and DuckDB itself) and take
    item 19 earlier if the c32 round confirms oversubscription.
16. **Single-stream scan throughput** (same comparison): upstream native (defaults `preserve_insertion_order=false`,
    `streaming_buffer_size=64MiB`, DuckDB 1.5.6) streams one full `lineitem` scan at 1510 MB/s with 8 threads vs ~950 MB/s
    for ours at any thread count (4 and 8 concurrent scans: equal). Measure our opt-ins `duckDbPreserveInsertionOrder=false`
    + `duckDbStreamingBufferSize=64MiB` on the stand (scan x1, Trino splits) before and after item 14; if answers are unchanged
    and x1 improves, propose them as defaults for plain scans without ORDER BY (default change -> Open questions first).
    **Local check 2026-10-27 (DuckDB 1.5.6, new `ScanThroughputPerfTest`, numbers in `docs/dev/TUNING.md`):** answers equal
    in all four variants, but `duckDbPreserveInsertionOrder=false` is *slower* (4 threads 334-349 vs 348-388 MB/s, 8 threads
    234-246 vs 346-367; same 92 Flight batches, so inside DuckDB) and a 64 MiB buffer +5-15 % (4/16 MiB: noise). So no
    default change from this box (client in the same JVM on 4 cores, CPU-bound). **Stand (decides):** scan x1 (JDBC/ADBC
    full lineitem SF10, complete mode) at the default threads with `-DduckDbStreamingBufferSize=64MiB` vs unset and
    `-DduckDbPreserveInsertionOrder=false` vs unset, 3 rounds; also RSS. Adopt the 64 MiB buffer as default only if x1 gains
    clearly (> 15 %) without RSS growth at 8 concurrent scans; `false` only if it is faster there too.

## Open questions

- **Item 19b expected connections (2026-10-28, core lane).** The default `memory_limit` divides the DuckDB budget by
  `duckDbMemoryConnections` = cores / threads (at least 2), i.e. the queries that fill the cores - not by the worker
  pool (32-64 threads), which would leave a single heavy query 1 GiB on most hosts. More concurrent heavy queries can
  still overcommit the budget (each is capped, the sum is not; still far below the old 80 % of RAM per instance).
  On the 2c/6g screening container (`-Xmx3g`, direct 2g) the budget is ~0.1 GiB, so every connection gets the 1 GiB
  floor (was 4.8 GiB); TUNING.md's stand round saw no speed change at `duckDbMemoryLimit=1GB` on SF10, but the
  screening should confirm. Options if it regresses: a floor of 2 GiB, or divide by the worker pool only above some
  host size. Maintainer's call on the divisor; `duckDbMemoryLimit=none` restores the old behaviour per deployment.

- **Quota tails (2026-10-28, core lane, for the screening/coordinator).** With `--cpus=2` the c8/c32 p95/p99 of small
  queries are dominated by CFS throttling (a query that crosses a throttled period waits ~45 ms), in every build; ratios
  of p95 between builds then mostly measure the stall, not the server. Proposal: the screening adds one pinned run
  (`--cpuset-cpus`) or reports p50 + q/s as the primary small-query numbers under quota. No server setting can remove
  the stall short of using less CPU per query. Maintainer's call; nothing changes in the server.

- **Read cache admission default (item 17 (e), 2026-10-28).** `cacheAdmission=reuse` (opt-in) keeps a block only
  on its second miss while it is among the last N not-kept names (N = cache blocks): scans larger than the cache stop
  churning it, but a repeated query hits from its third run instead of its second. If the screening shows SF10
  back near "off" with `reuse` and the small-query p95 win intact, should `reuse` become the default (an existing
  opt-in feature's behaviour, hence the question)?

- **S3 thread default (2026-10-28, cloud session).** The fix keeps >= 4 DuckDB threads on S3/lakeFS data
  directories (maintainer's suggestion). Decided by the data directory's scheme only: catalogs whose tables are on
  S3 behind a local/HDFS data directory (Iceberg REST/Hive on S3) keep cores / 4 - set `duckDbThreads=4` there, or
  should the default look at the catalogs' stores too? Local in-process run (CPU-saturated by the fake S3 and the
  clients) showed c32 q/s -18 % at 4 vs 2 threads while c1/c8 p95 improved; the screening's numbers decide whether
  `duckDbThreadsUnderLoad` should become the S3 answer for c32.

- **Item 21 (d): what is left per small query is DuckDB (2026-10-28).** Measured locally (2 CPUs, the perf test's
  point lookup / filtered count over 4 local Parquet files): plain DuckDB JDBC on one connection, new text each
  time, costs ~3.5-4 ms CPU per query with Arrow export (prepare ~0.5 of it); ours costs ~4.5 ms in the Flight
  executor threads (that DuckDB work + ~0.5 ms describing the new text over the empty views) + ~0.4 ms gRPC/netty.
  JFR: no Java hot spot left in our code (sampled frames outside DuckDB are scattered, < 5 % in total). Not
  helping: `parquet_metadata_cache` for local files, Arrow batch size, profiling/progress settings. Remaining
  levers, each needing a decision: (1) `duckDbThreads=1` on small-query nodes (question (b) above; at c1 the
  second DuckDB thread doubles the CPU per query: ~15 vs ~6 ms); (2) clients sending plain statements
  (GetFlightInfo + DoGet) instead of prepare/execute/close: +10-15 % q/s at c8/c32, a client-side change
  (patched JDBC driver / ADBC usage), not a server one; (3) skipping the describe run for texts whose schema
  DuckDB's prepare already gives (scalar types only) - saves ~0.3 ms (~5 %) but the Arrow schema then comes from
  our own type mapping instead of DuckDB's export, a wire-visible risk; recommendation: not worth it. If the
  screening still shows a gap after (1), the gap is DuckDB per-query cost on its runner, not our server path.

- **Item 21 (b): DuckDB threads under load (2026-10-28).** On 2 CPUs with 8-32 small queries at once, `duckDbThreads=1`
  instead of the default 2 saves ~1.4 ms CPU per query (+9 % q/s at c8, local; heavy single queries lose their second
  thread). Changing the thread count per query when more queries run than cores is not possible today: every
  session sets `lock_configuration = true` after its setup, and `SET threads` is refused after that. Options: (1)
  leave it, document `duckDbThreads=1` for small-query nodes; (2) a 1-core default floor for <= 2 cores (default
  change); (3) open a second, 1-thread connection per worker for use under load (memory: one more DuckDB per
  worker). Recommendation: (1) now, and let the screening repo measure `-DduckDbThreads=1` on the pre-release.
  *2026-10-28:* option (3) built as an opt-in for item 22 (`duckDbThreadsUnderLoad`, default 0 = off; no default
  changed). The question left is only whether to turn it on by default (e.g. 1 on nodes with <= 4 cores) once the
  screening / stand numbers are in.

- **Maintainer 2026-10-28, answers for item 18:** (1) identity: option (b) - an internal call signed with the ticket
  secret carrying the user name, accepted only from live cluster members' addresses and only when
  `distributedComplete=true` (default false); document that the ticket secret then allows impersonation and must be
  protected like a keytab. (2) yes, reuse the server's TLS settings and trust store for node-to-node calls. (3) yes:
  peer failure before the first batch -> fall back to the single-node path; after the first batch -> fail the query.
- **Maintainer 2026-10-28, item 17 follow-ups:** (1) production local disk is unknown - build the disk tier as an
  opt-in (path + size cap). (2) keep `cacheCheckSeconds=10` as the default and document it.


- **Item 18: node-to-node calls for distributed complete mode (2026-10-27).** The merge is ready (step 1); what is
  left needs a server to call its peers, which it never does today (only clients call nodes). The planning node
  would plan the query in parts as in partial mode, read each part with a Flight DoGet from its node (its own parts
  locally), merge in DuckDB and stream the complete answer. Questions: (1) identity on the peer: with Ranger
  authorization every peer must check the *client's* user. Options: (a) forward the client's bearer token / basic
  credentials as received (no Kerberos: a service ticket cannot be forwarded without delegation); (b) a cluster-
  internal call signed with the existing ticket secret, carrying the user name, which the peer trusts and authorizes
  as that user (works for every auth provider; makes the ticket secret a credential for any user - a leak of it
  then means full impersonation, today it means forged tickets); (c) only for `AllowAllAuthorization` setups.
  Proposal: (b), off unless `distributedComplete=true`, peers accept it only from live cluster members' addresses.
  (2) TLS between nodes: reuse the server's own TLS settings and trust store for the internal client - ok?
  (3) a peer failure mid-stream after the first batch fails the query (as today for a lost node), before the first
  batch the planning node falls back to the single-node path - ok?

- **S3 read cache follow-ups (item 17, 2026-10-27).** Step 1 is on the heap (bounded at half of `-Xmx`). (1) Is a
  local disk with a size cap available on the production hosts for a second, larger tier (the original proposal)?
  (2) Default `cacheCheckSeconds=10`: a file rewritten in place under the same key may be answered in its old version
  (never mixed) for up to that long while the cache is on; lake tables (Iceberg, lakeFS, Hive) never rewrite keys,
  plain S3 directories might. Keep 10, or 0 (a HEAD per GET: saves bandwidth, not round trips)?

- **Maintainer 2026-10-27: option (A) chosen** for the S3 read cache below (node-wide byte-bounded range cache in the s3 plugin behind a 127.0.0.1-only loopback endpoint with a per-process token; note the security review in the Log). Do it as the next part of item 17.
- **S3 read cache with a memory bound (item 17, 2026-10-27).** The stand measured DuckDB's external file cache ~40x
  faster for repeated S3 reads but it OOM-killed the server: DuckDB 1.5.6 has no size setting for it, it lives in each
  connection's buffer pool (one DuckDB per worker thread), so the only bound is `duckDbMemoryLimit` x live connections.
  The read pattern itself is already lean (item 17 (v)), so the remaining gain is caching data. Options:
  (A) a node-wide range cache in the s3 plugin: DuckDB reads `s3://` through a loopback HTTP endpoint of the plugin
  that answers GETs from a byte-bounded LRU (memory, or local disk with a size cap) keyed by bucket/key/ETag/range and
  forwards misses to S3 with the plugin's own signed client; one cache for all DuckDB instances, bound in bytes,
  invalidated by the listing's ETag/mtime. Costs: a hop and a copy per miss, credentials stay in Java (DuckDB would
  send unsigned requests to the loopback port, so the port needs a per-process token header and must bind
  127.0.0.1 only - security review), more code (~1-2 sessions). (B) the community `cache_httpfs` extension (in-memory /
  on-disk block cache with a size cap below httpfs): needs a build for 1.5.6 in `ci-assets`, a community
  extension under our startup lockdown, and its cache is per DuckDB instance unless on disk. (C) keep it off and
  document `duckDbExternalFileCache=true` only with a `duckDbMemoryLimit` x `maxConnections` that fits the host.
  Proposal: (A) with a local-disk cache, opt-in setting first (`arrowflight.s3.cacheBytes`, default 0 = off). Which
  one, and is a local disk for it available on the production hosts?

- ~~**S3 connection churn (item 0g, 2026-10-26).**~~ Resolved 2026-10-27 without a new mechanism: DuckDB 1.5.6 httpfs has
  `httpfs_connection_caching`; on by default through `arrowflight.s3.connectionCaching` (stage 2 item 0g approval), off
  restores the old behaviour. Original text: DuckDB's httpfs opens a new TCP connection for about every S3
  range read at 2+ threads (per-file-handle client cache, a new handle per row group and thread); each closed one
  holds a local port 60 s. Today: startup WARN + documented host workaround (`tcp_tw_reuse=1`, wider port range).
  Options needing your call: (1) turn `duckDbExternalFileCache` on by default for S3 data stores only (repeated
  reads need no connection; costs up to `duckDbMemoryLimit` of cache per live connection — the reason it is off);
  (2) an in-process HTTP forward proxy with pooled keep-alive connections for DuckDB's S3 reads (`http_proxy`,
  plain-HTTP endpoints only; HTTPS would tunnel 1:1 and gain nothing; touches the lockdown); (3) a pooled client
  in httpfs itself (upstream DuckDB, or our own build like hadoopfs). Recommendation: decide after the stand rerun
  with `tcp_tw_reuse=1`; if that is clean, keep (0) and document it as a deployment requirement.

- **(2026-10-26, item 0d follow-up) Faster detection of a crashed member?** With kill -9 / host gone, every
  Hazelcast map operation touching the dead member's partitions (heartbeat puts, getAll, ticket reservations)
  waits until the member is dropped: `hazelcast.max.no.heartbeat.seconds=10` (our default). Measured in one JVM
  with an iptables DROP: first plan 10.4 s, then ~10 ms. Side effect seen: a survivor's own heartbeat put can block
  as long, so other nodes briefly drop that live node as stale (back within 3 s; no wrong answers). Options: lower
  it to 5 s (risk: members dropped on GC pauses / busy runners), or plan from a local cluster snapshot refreshed by
  the heartbeat thread. Both change existing behaviour — maintainer's call. (2026-10-27: this is also 3-node S3 finding (c); only a
  vanished address waits — a refused port is detected in < 1 s. A deployment can already set
  `-Dhazelcast.max.no.heartbeat.seconds` on every node, now documented in START-NOTES.)

- **Item 9 (per-user identity): design proposal 2026-10-26 — please answer the questions at the end.** Today the
  embedded Ranger gate (`FLIGHT_AUTHORIZATION_PROVIDER=ranger`, service type `arrowflight`, schema/table policies)
  already decides per Flight peer identity on every call that names data (GetFlightInfo, DoGet re-check of the
  ticket's query, catalog listings, `arrowflight.table_statistics`). The gap: when the Flight client is a shared
  service (Trino coordinator + workers, StarRocks BEs, a gateway) the peer identity is that service's principal,
  so every end user gets the service's rights; and storage (HDFS, HMS) is always read as the server principal, so
  the cluster's own Ranger HDFS/Hive policies see only the server. Proposal in three phases, each opt-in and off
  by default (no change for existing deployments):
  - **A. Trusted-proxy impersonation (server only).** New settings `FLIGHT_IMPERSONATION_PROXIES=<principal or
    short name>,...` and optional `FLIGHT_IMPERSONATION_USERS` / `FLIGHT_IMPERSONATION_GROUPS` (allow lists, same
    idea as Hadoop's `hadoop.proxyuser.<svc>.users/groups`). A call may carry `x-flight-effective-user: <name>`;
    when the authenticated peer is a listed proxy and the target user/group is allowed, the effective user becomes
    the call's identity — implemented as a wrapper around the configured `CallHeaderAuthenticator`, so every
    existing `peerIdentity()` check uses it without further changes. The header from a non-proxy, or for a user
    outside the allow list, is rejected (`PERMISSION_DENIED`), never silently ignored. Ranger audit records the
    effective user and names the proxy in the request data; the server log line too. Partial mode: DoGet already
    re-checks with the caller's identity, so the connector's workers must send the same header on each endpoint
    call (connector work, for the coordinator); tickets stay unchanged (alternative: bind the effective user into
    the signed ticket — ticket format change, not proposed). Requires an identity-establishing provider (Kerberos
    or B); the static bearer token (identity `token`) cannot be a proxy, `FlightSecurity` already enforces that.
  - **B. JWT authentication provider** `FLIGHT_AUTH_PROVIDER=jwt` (TLS required): verifies signed JWTs (JWKS URL
    or public key file, required issuer and audience, clock skew limit, configurable user claim — `sub` or
    `preferred_username`); identity = that claim. Fits clients where users log in through OAuth2/OIDC and the
    client forwards the user's token (Trino with OAuth2 can pass it through); then no impersonation is needed.
    Library: nimbus-jose-jwt (already in the dependency tree through hadoop-auth) as a direct dependency at a
    current version. Session tokens of the Kerberos provider are unaffected.
  - **C. Storage-level identity (bigger, separate decision).** Read HDFS and query HMS as the effective user via
    Hadoop proxy users: the Java side (listing, footers, statistics) under `UserGroupInformation.createProxyUser
    (user, login).doAs`, the HMS client with `set_ugi` / SASL proxy; needs `hadoop.proxyuser.<server>.hosts/users`
    on the cluster. The hard part is DuckDB's hadoopfs reads: one DuckDB instance per worker thread with one
    libhdfs3 connection cache, so per-user reads need per-user connections (or an effective-user setting per
    query) — whether libhdfs3 supports a proxy user over Kerberos is unverified; plus every server cache (listing,
    footer, statistics, external file cache) must be keyed by user or filled only after the user's own check.
    Only worth it if the cluster's Ranger HDFS/Hive policies must be the single source of truth.
  Questions: (1) Which clients reach production and how do their users authenticate to them (Trino with
  Kerberos/LDAP/OAuth2, StarRocks, BI tools)? That picks A, B or both. (2) Is the embedded `arrowflight` Ranger
  service (schema/table policies on the server) enough, or must the cluster's HDFS/Hive Ranger policies decide per
  user (phase C)? (3) Header name `x-flight-effective-user` and reject-on-misuse OK? Proposal: implement A first
  (one session, plus connector header forwarding), B if (1) shows OAuth2/OIDC, C only on explicit request.

- Production versions (maintainer, 2026-10-25) vs our client side, not yet checked: the libs set carries the
  ZooKeeper 3.8.6 client (Hadoop 3.5.0's; talks to a 3.9.2 ensemble, SASL client code is the same line) and
  hadoop-client 3.5.0 against a Hadoop 3.1.3 cluster (HDFS RPC and SASL are wire-compatible across 3.x, but
  e.g. the 3.5 client's defaults — `dfs.client.*` timeouts, observer reads off — should be confirmed on the
  stand); the embedded Ranger evaluator is ranger-plugins-common 2.8.0 against a Ranger 2.4.0 admin (policy
  download REST). Should the libs set follow the cluster (ZooKeeper 3.9.x, or Hadoop 3.3.x client), or stay on
  Hadoop 3.5.0 unless the stand shows a problem? Proposal: stay, and test HA failover + Kerberos on the stand
  with release `server-1.0.0-c3b276054cc3` first.

- **Item 3 (Iceberg time travel): ANSWERED 2026-10-25 — yes, implement the proposed `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF` clause.** (proposal 2026-10-25:) Recommended: the
  SQL clause Trino and Spark use, after a catalog table name: `FROM ns.t FOR VERSION AS OF
  8213491011553364004` (snapshot ID) and `FOR TIMESTAMP AS OF TIMESTAMP '2026-10-01 00:00:00 UTC'`
  (latest snapshot committed at or before; a time zone is required, no server-local time). The
  server's parser takes the clause out before DuckDB and pins that snapshot for the query exactly as
  it pins the current one; tickets already carry the snapshot ID, so no ticket or header change, and
  partial mode works the same. An unknown snapshot ID or a time before the first snapshot is an
  error, never the current snapshot; data directory and Hive tables refuse the clause. Alternatives:
  (a) a `t@<snapshot>` name form (no grammar for clients, but clashes with quoting and Trino's Iceberg
  connector dropped it), (b) a session header such as `x-flight-iceberg-snapshot` (new header, one
  table per query). For the coordinator: the Trino plugin / StarRocks connector would have to
  forward the clause (or their own time-travel syntax translated to it). OK to implement the clause?

- Upstream's `.github/workflows/ci.yml` now runs only on manual dispatch (it ran on every push and Dependabot PR,
  failed, and doubled the minutes; dev-ci.yml covers the same checks). Revert the `on:` block if you want it back.
- Dependabot PRs (#4..#7 on this repo) no longer get CI. Close them or fold the bumps into item 1b?
- `duckDbMaxTempSize` (new, UPSTREAM-SYNC S3) is unset by default = DuckDB's cap of 90 % of free disk for every
  connection. Upstream defaults to 20GiB per server; ours is per connection. Set it on the stand (spill disk /
  expected concurrent connections), or want a default?
- `duckDbMemoryLimit` / `duckDbThreads` apply per DuckDB connection (one in-memory instance per worker thread), so a
  node can use limit × live connections (upstream hit an OOM kill this way and switched to one shared database,
  f5f11b3). We keep per-connection instances because catalog views are per connection; options: (a) document it and
  size the limit as node budget / expected concurrent connections, (b) per-query view names so a shared database becomes safe (bigger
  change). Which one?
- UPSTREAM-SYNC S4 on the stand: measure TPC-H SF10 (JDBC and Trino) with `DUCKDB_PRESERVE_INSERTION_ORDER=false`
  and `DUCKDB_STREAMING_BUFFER_SIZE=64MiB` (each alone and both) against the defaults. If they win, should they
  become the defaults? (The order audit found nothing relying on row order without ORDER BY; clients that read
  a plain `SELECT` in file order would see a different order.)
- GitHub Pages: the Actions list shows a `pages build and deployment` run on branch `gh-pages`
  (2026-10-24 18:50 UTC, success); no `gh-pages` branch exists now. CLAUDE.md wants no website: please
  check Settings → Pages is off (sessions cannot change repo settings).
- UPSTREAM-SYNC S6 on the stand: measure TPC-H SF10 partial mode (Trino, StarRocks) with
  `FLIGHT_ENDPOINTS_PER_SERVER=2` and `4` against 1. DuckDB already uses `duckDbThreads` per
  endpoint, and each extra endpoint takes another DuckDB connection (memory limit per connection), so
  a win is expected only where one client stream is the bottleneck. Keep default 1 unless it wins?
- For the coordinator (client repos): the server now answers `arrowflight.table_statistics`
  (README "Table statistics action"; upstream's StarRocks fork patch and `TableStats` parse it
  unchanged). Our Trino plugin / StarRocks connector could feed row counts and column bounds to their
  optimizers from it; `ndv` is always null so far. Want the background NDV estimate (upstream runs
  `approx_count_distinct` per column once per file set; costs a full column scan per table)?
- UPSTREAM-SYNC S10 on the stand (MinIO): compare listing + footer time of a 1000+ object data
  directory and SF10 lineitem through our S3 plugin vs upstream's s3a reader (9f66812) to find any
  win worth porting into our plugin (page size, footer read size). Cloud sessions have no MinIO.
- UPSTREAM-SYNC P3 on the stand (Spark, TPC-H SF10 with `FlightSessionCatalog`): run Q2/Q3/Q5/Q7
  with the read option `statistics.filterSelectivity=true` vs default and check the plans switch to
  broadcast joins without driver/executor memory trouble. If it wins, make it the default? (It is a
  heuristic, no column statistics; P4 cost gate decided after this.)
- UPSTREAM-SYNC S11 (Flight batch compression, LZ4/ZSTD IPC buffers) deferred: upstream's own benchmark was
  negative (LZ4) or mixed (ZSTD), it helps only on slow links, and every client must then decode compressed buffers.
  Is there a slow link (WAN client, cross-site StarRocks) that wants it? If so it becomes an opt-in setting.
- **(2026-10-26, 3-node S3 finding (b)) Reservation TTL of unread endpoints.** `CancelFlightInfo` now releases a
  plan's node-load reservations at once (additive). A client that drops a plan without cancelling still holds
  them 10 min (tickets carry no expiry). Options: (1) connectors (Trino plugin, StarRocks connector) send
  `CancelFlightInfo` when a query is abandoned / finished early (LIMIT) — for the coordinator; (2) a shorter
  reservation TTL (e.g. 2 min, or a new setting with the 10 min default kept); (3) both. Proposal: (1) plus an
  opt-in setting. Which? *2026-10-28 (core lane): the opt-in part is done (additive, default unchanged):
  `flightLoadReservationSeconds` (default 600, see Log); (1) stays with the coordinator, and a shorter default is
  still your call.*
- The stand findings (a)–(c) in Next steps (S3 outage read latency, unread plans keeping node load
  reservations up to 10 min, slow first plan after B5 on S3) are not numbered, so sessions keep going down the
  numbered list (1b next). Want one of them taken before 1b's remaining groups? (b) skews endpoint placement
  for ~5 min; none gives wrong results.
- Hazelcast phone-home is on: a started node tries to reach `phonehome.hazelcast.com` (version, cluster size;
  seen 2026-10-25 from the cloud sandbox, where the proxy refused it). Nothing in the config sets
  `hazelcast.phone.home.enabled=false`. Turn it off by default (no client-visible change)? Until then the stand
  can pass `-Dhazelcast.phone.home.enabled=false` or `HZ_PHONE_HOME_ENABLED=false`.
- Item 5 (config precedence, done 2026-10-25) changes which value wins when a setting is set twice:
  environment variables now beat `arrowflight.properties` (both the bundled one and one an operator puts on
  the class path) and `etc/plugins/<ns>.properties`. Before upgrading the stand/production, check the
  service environment for leftover variables that used to be ignored (most visible: `FLIGHT_RESULT_MODE`,
  `FLIGHT_ENDPOINT_BACKUP_LOCATIONS`, `FLIGHT_METRICS_ENABLED`, `DUCKDB_LOCKDOWN`). No wire change by itself.
- Upstream wire extensions (UPSTREAM-SYNC S7 `decimalWire=narrow`, S8 runtime IN/bloom filters in the ticket): want
  them (opt-in) for our StarRocks/Trino connectors? Both change what clients see/send; for the coordinator: upstream's
  StarRocks fork pushes runtime filters and partial aggregates (SUM cast to DOUBLE because integer SUM streams as
  DECIMAL(38,0)) to the server.

## Log

- 2026-10-23 (maintainer's local coordinator): repo created from local integration-3 (e9ef57b); heavy upstream
  workflows disabled; CI assets uploaded.
- 2026-10-23 (cloud session): item 1 CI done. `dev-ci.yml` added; first run green on c4ca0bf (run 39990624546: build
  5 min, server 642 tests / 2 skipped, client-core 149, spark 162, starrocks 7). Fixes needed for green:
  `SparkJoinIntegrationTest` moved from flight-server (no `flight` data source on its classpath, it always failed)
  to flight-spark; Hive tests need a UTF-8 JVM locale (set in the workflow). Package dispatch run 39991672183 did
  not reach the package job: `HiveCatalogTest.anOutageIsAnErrorNotAnEmptyAnswer` failed once (`schemas()` after the
  metastore proxy closed did not throw); 10 local runs pass. The test proxy had a race (a connection accepted while
  `close()` ran stayed open), fixed in HmsFixture (96b878a); root cause not confirmed: if that test fails again,
  root-cause it before other work. Package dispatch run 39993691816 on 96b878a green (build 5 min + package 3 min):
  pre-release `server-1.0.0-96b878a93be8` (linux jar 133 MB, plugins tar 40 MB, START-NOTES.md, SHA256SUMS).
- 2026-10-23 (cloud session): item 0 started. Triage of upstream's server-side work written to
  `docs/dev/UPSTREAM-SYNC.md` (S1..S12, T1..T2, decisions per row). Ported S1: `main` exits with status 1 when the
  start fails before serving or throws an `Error` (regression tests in `HadoopArrowFlightServerUnitTest`); CI run
  40005871418 on 379f72f green. S5 (shared DuckDB database) rejected as-is, question added to Open questions.
- 2026-10-23 (cloud session): item 0, UPSTREAM-SYNC S3 ported: setting `duckDbMaxTempSize` /
  `DUCKDB_MAX_TEMP_SIZE` sets `max_temp_directory_size` on every DuckDB connection (validated as a size, refused as a
  plugin setting, default unset). Tests in `DuckDbAdapterTest`, `ConfigAdapterTest`, `DuckDbSetupValidatorTest`.
  CI run 40022303101 on 2dae6ce green (~8 min).
- 2026-10-23 (cloud session): item 0, UPSTREAM-SYNC S4 ported as opt-in settings `duckDbPreserveInsertionOrder`
  (default true, strict true/false) and `duckDbStreamingBufferSize` (default unset), per connection, refused as
  plugin settings; tests in `ConfigAdapterTest`, `DuckDbAdapterTest`, `DuckDbSetupValidatorTest`. Order audit:
  CI runs 40036505609 / 40037380505 on branch `claude/s4-order-audit` (both settings forced on, audit only, not
  merged) failed only the 3 tests that assert the defaults / golden SET lines (server 649, client-core 149, spark
  162, starrocks 7, plugins green); EXPLAIN on DuckDB 1.4.5 shows the equality-delete LIMIT barrier still keeps
  filters above the scan. CI run 40037403277 on e984ff5 green.
- 2026-10-24 (cloud session): item 0, UPSTREAM-SYNC S6 ported as opt-in `endpointsPerServer` /
  `FLIGHT_ENDPOINTS_PER_SERVER` (default 1; below 1 refused): partial results split each node's files
  into up to N endpoints of similar size, each with its own backups and load accounting; queries
  with a single-node feature (OFFSET, HAVING, DISTINCT aggregates) and joins never split (upstream
  split them on a one-node cluster: wrong answers). Tests in `QueryPlannerTest`, `ConfigAdapterTest`.
  Commit 930e6db. Dev CI run 40046662476 could not start (2 attempts, no runner, see Open questions);
  full local `clean verify checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services`
  green instead (server 649 / 9 skipped — Iceberg ITs need the ci-assets extensions — client-core 149,
  spark 162, starrocks 7, plugins green).
- 2026-10-24 (cloud session): Dev CI still cannot start (dispatch run 40055199702 on 7d4cebe: failed
  in 2 s, no runner). Item 0, UPSTREAM-SYNC S9 ported as Flight action `arrowflight.table_statistics`
  (`TableStatisticsService`: footer row count, bounds, nulls, sizes for data-directory tables through
  the DataStore; catalog snapshot rows/size, upper bound with delete files; refused on a multi-node
  local-disk cluster; no NDV yet); T1 rejected in favour of it. Tests `TableStatisticsServiceTest`
  (vs DuckDB over the same files), `FlightSqlProducerTest.tableStatistics*`. Commit 89c9b95. Full local
  `clean verify checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services` green (server
  660 / 9 skipped, client-core 149, spark 162, starrocks 7, plugins green).
- 2026-10-24 (cloud session): Dev CI still cannot start (dispatch run 40063344609 on 6bbc09b: 5 s,
  no runner). Item 0: UPSTREAM-SYNC S10 rejected as-is (upstream turns the file lockdown off for S3
  data dirs; speed comparison needs MinIO, moved to the stand), T2 ported: `flight-client-core` pool
  identity now hashes every client setting (a `crtVerify=false` client was reused for a verifying
  configuration, likewise trust store / routing / retries / timeout / allocation limit), schema and
  routing headers go with every call and the Kerberos refresh. Tests `ClientHeadersTest` (in-process
  Flight server), `ClientRetryTest.connectionIdentity*` (fail on the old code). Full local `clean
  verify checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services` green (server 660 / 9
  skipped, client-core 154, spark 162, starrocks 7, plugins green).
- 2026-10-24 (cloud session): Dev CI still cannot start (dispatch run 40071167062 on fd6fb30: 5 s, no
  runner). Item 0, perf branch rows P1..P4 in UPSTREAM-SYNC: Q6/Q17 aggregate fixes (88c0a16) ours
  already, 49a5a5c benchmark-only, 482d24b ported to `flight-spark` (pushed global aggregate reports
  1 row; filter selectivity scaling behind read option `statistics.filterSelectivity`, default off;
  NOT of an unknown predicate keeps every row, upstream estimated 0), 7cf1544 deferred to the stand.
  Tests `ScanSelectivityTest`, `TableTest.estimates*`. Full local `clean verify checkstyle:check
  spotbugs:check -DexcludedGroups=perf,real-services` green on d264273 (server
  660 / 9 skipped, client-core 154, spark 173, starrocks 7, plugins green).
- 2026-10-24 (cloud session): Dev CI still cannot start (dispatch run 40079012105 on ce71eec and push
  run 40080400054 on d8237a1: 4 s each, no runner). Item 0: S11 (batch compression) deferred (Open
  questions); S12 ported and widened, closing Next step 6: query texts over 256 Ki chars stay out of
  the parse cache, the result-schema cache and the per-thread statement cache (which was already an
  LRU of `duckDbStatementCacheSize`); one uncached statement slot per thread; counters
  `arrowflight_query_cache_oversize_total` / `arrowflight_query_cache_evictions_total{cache}`. Tests
  in `ParquetQueryParserUnitTest`, `DuckDbAdapterTest`, `ExecutionServiceTest`, `MetricsServiceTest`.
  Commit d8237a1. Full local `clean verify checkstyle:check spotbugs:check
  -DexcludedGroups=perf,real-services` green (server 665 / 9 skipped, client-core 154, spark 173,
  starrocks 7, plugins green). Note: `spotbugs-exclude.xml` still matches the old package
  `net.surpin` (stale, harmless).
- 2026-10-24 (cloud session): Dev CI still cannot start (dispatch run 40086887735 on fb6e840: 3 s, no
  runner). Item 1b, first group (server): Hazelcast 5.6.0 → 5.7.0, Netty 4.2.9 → 4.2.19 under
  `netty-bom`, `netty-all` replaced by codec-http2 + handler-proxy + epoll (x86-64/ARM64); netty-all
  excluded from the provided Hadoop and test Spark deps, Spark's kqueue natives kept out of the jar
  (the BOM manages native modules to runtime scope). Shaded jar checked: no Redis/HAProxy/SMTP/STOMP/
  MQTT/XML/HTTP3/OCSP/DNS/kqueue/io_uring classes. `flight-spark` needed the same BOM (its tests failed
  with `NoSuchMethodError` in the epoll transport on mixed 4.1/4.2 modules). README: Hazelcast port
  must stay private. Test `DependencyFloorTest` (one Netty version >= 4.2.17, unused codecs absent,
  Hazelcast >= 5.7.0). Full local `clean verify checkstyle:check spotbugs:check
  -DexcludedGroups=perf,real-services` green (server 669 / 9 skipped, client-core 154, spark 173,
  starrocks 7, plugins green). Commit 69d50f8; its push run 40088691573 also failed in 3 s, no runner.
  Stand: run the 3-node kill tests once with this jar and
  restart all nodes together (no rolling upgrade 5.6 → 5.7 in Hazelcast open source).
- 2026-10-24 (cloud session): Dev CI still cannot start (dispatch run 40094973520 on 1445f68: 3 s, no
  runner). Item 1b, client group: `flight-client-core` and `flight-starrocks` import `netty-bom` 4.2.19
  (they ran Arrow's 4.2.9 mixed with grpc-netty's 4.1.130 HTTP/2/http/socks/proxy/unix-common;
  tcnative 2.0.74 → 2.0.84); client-core's provided hadoop-client/hadoop-hdfs exclude `netty-all`
  (under the BOM its natives and codecs would become runtime deps of the connectors). Runtime
  classpath diff vs main: only the Netty versions change (+ codec-compression, − the 4.1 codec jar).
  `NettyFloorTest` in both modules (fails on the old poms). Full local `clean verify checkstyle:check
  spotbugs:check -DexcludedGroups=perf,real-services` green (server 669 / 9 skipped, client-core 156,
  spark 173, starrocks 9, plugins green). Commit d59b974; push run 40095535435 failed in 3 s, no runner.
- 2026-10-24 (cloud session): Dev CI still cannot start (dispatch run 40112719429 on e6ff83b: 3 s, no
  runner). Item 1b, Jackson group (commit fbb2aca): 2.15.3 (mixed with Arrow's 2.21.0 jsr310, Ranger's 2.17.2 JAX-RS)
  → one 2.21.8 under `jackson-bom` in flight-server, client-core, spark, starrocks (2.21 = Arrow 19's line;
  the BOM also lifts the Scala module of the test Spark, so the Spark tests run on it). The Spark connector's
  shaded jar relocates `com.fasterxml.jackson` to `pro.surpin.data.arrowflight.shaded.jackson` (checked: no
  class in the jar references the unrelocated package; service files relocated). Iceberg plugin bundles
  2.22.3 (Iceberg 1.12 brought 2.22.2). Floor tests `DependencyFloorTest.jackson*` and `JacksonFloorTest`
  (client-core, spark, starrocks, iceberg; the StarRocks and Iceberg ones fail on the old poms).
  `PluginLoadingTest` now expects the server's 2.21.8. Full local `clean verify checkstyle:check
  spotbugs:check -DexcludedGroups=perf,real-services` green (server 670 / 9 skipped, client-core 158,
  spark 175, starrocks 11, plugins green).
- 2026-10-24 (cloud session): Dev CI runs again (the Actions history restarted with the public repo;
  the earlier "cannot start" question is closed). Dispatch run 40139808951 on f354d38 was cancelled by
  the newer push run. Item 0a: hadoopfs DataNode timeout defaults 5000 / 15000 → 3000 / 10000
  (`AppConfig`, `arrowflight.properties`, startup warning hint, CHANGELOG; golden setup SQL and
  `ConfigAdapterTest` updated, their override cases moved to 4000 so they still differ from the default).
  Commit 732e238: push run 40140133221 green (build 6 min) — first CI run covering 89c9b95..fbb2aca too.
  Full local `clean verify checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services` green
  (server 670 / 9 skipped, client-core 158, spark 175, starrocks 11, plugins green). Note: `./mvnw` got
  HTTP 429 from Maven Central for the Maven 3.10.0 download; `/opt/maven` 3.9.11 works.
- 2026-10-25 (cloud session): item 1b, Parquet read path group (commit 6314b72): server Parquet 1.13.1 →
  1.18.1 (`parquet.version`), which brings aircompressor 0.21 → 2.0.3 and snappy-java 1.1.8.3 → 1.1.10.8
  (zstd-jni 1.5.0-1 → 1.5.7-15); the server only reads footers/statistics with it, data pages stay DuckDB's.
  Spark keeps its provided Parquet 1.13.1; plugins unaffected (Iceberg already on aircompressor 2.0.3).
  `DependencyFloorTest.parquetCodecsAreAtTheFloor` (fails on the old pom). Full local `clean verify
  checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services` green (server 671 / 9 skipped,
  client-core 158, spark 175, starrocks 11, plugins green); push run 40148323187 green (6 min). Next: libthrift.
- 2026-10-25 (cloud session): item 1b, Hive plugin group (commit cf060fe): libthrift 0.9.3 → 0.24.0 for the
  metastore client (`TSocket` with a `TConfiguration` capping responses at 1 GiB like Hive 4 — libthrift's
  default 100 MB would refuse large partition lists; frames 16 MB as before; the read budget resets on every
  flush, so pooled connections do not run into it); `TFramedTransport` moved to `transport.layered`. Hive 3.1.3's
  generated classes and libfb303 0.9.3 run unchanged on it. The Hive 3.1.3 metastore server fails on libthrift
  0.14+ (`TFramedTransport$Factory` gone), so the test metastore now runs on 0.9.3 in a child-first class loader
  over target/fixture-libs in the plugin's tests too (`IsolatedHms`; the server's `HiveTestTables` already did):
  the tests are a new client against an old server. New tests in `HiveCatalogTest`: framed transport through a
  framing proxy (same snapshot as unframed), libthrift floor >= 0.24 and the 1 GiB cap, fixture on 0.9.3. Full
  local `clean verify checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services` green (server 671 / 9
  skipped, client-core 158, spark 175, starrocks 11, hive 11, plugins green); push run 40156159318 green.
  Next: the runtime libs set (1b's last group).
- 2026-10-25 (cloud session): item 1b, runtime libs set (commit 4eaab05). Finding: the shaded jar never carried
  Hadoop (`provided`, "comes from the deployment"), so `java -jar` (README Quick Start, START-NOTES) failed with
  `NoClassDefFoundError: org/apache/hadoop/conf/Configuration`; the stand ran on its stale libs dir. The build now
  copies the provided closure (mediated against the jar's libraries: 58 jars, 42 MB, Hadoop 3.5.0, Jetty 9.4.58,
  no ZooKeeper / Netty 3 / log4j / BouncyCastle / second Netty-Jackson-protobuf) to `flight-server/target/libs`;
  the `package` dispatch publishes `flight-server-libs-<version>-<sha>.tar.zst` (`libs/` + DEPENDENCIES.txt +
  SHA256SUMS). New `LibsSetTest` (floors, one version per jar, child-JVM start on server classpath + libs
  reading Parquet, and a start without libs that must fail). README / START-NOTES start with
  `java -cp '<jar>:libs/*'`; START-NOTES' layout also fixed (`../plugins` beside the jar). Found on the way: the
  bundled properties shadow `-Darrowflight.*` too (added to item 5), Hazelcast phone-home is on (Open questions).
  Full local `clean verify checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services` green (server 675
  / 9 skipped, client-core 158, spark 175, starrocks 11, hive 11, plugins green); push run 40163635118 cancelled by
  dispatch run 40163642740 (`package=true`) on the same commit: green, pre-release `server-1.0.0-4eaab052deaf`
  with the libs asset (38.9 MB). Next: item 2 (Iceberg equality deletes), unless the maintainer picks a stand
  finding (a)–(c) first.
- 2026-10-25 (cloud session): item 2, complete mode (commit 5466f86). Snapshots with equality deletes no longer go
  through iceberg_scan with every column read: `CatalogSources.deletesAppliedSource` reads the data files by field
  ID and applies the plan's delete files itself (ANTI JOIN per set of equality columns, IS NOT DISTINCT FROM; position
  deletes on `file_row_number` and the delete file's path normalised in SQL like `CatalogLocations.normalize`); data
  files with the same deletes share one read_parquet, a file with position deletes is its own UNION ALL branch.
  EXPLAIN on DuckDB 1.4.5: plain hash anti joins, the data scan projects only the used + equality columns and takes
  the query's filters. Deletion vectors, columns without field ID, dropped equality columns, a `file_row_number`
  column keep the old read. New `CatalogSourcesDeletesTest` (3000 rows, renamed column, pos deletes with `file:` /
  `file://` paths and rows of a file they do not apply to, equality deletes on 1 and 2 columns with nulls; answers
  computed in Java; fails with `=` instead of IS NOT DISTINCT FROM); `CatalogViewsIcebergSourceTest` updated. Local
  server tests green (681 / 9 skipped); push run 40170480666 green (incl. the Iceberg ITs on Iceberg-Java tables).
  Next: item 2 partial mode (see Next steps 2 (i)), then the stand timing (ii).
- 2026-10-25 (cloud session): item 2 (i), partial mode (commit f518695). Iceberg snapshots with position/equality
  deletes that `CatalogSources.deletesApplicable` covers (new; same checks `deletesAppliedSource` makes, they hold
  for every subset of files) are no longer answered completely in partial mode: `completeReason` lets them through,
  the plan spreads their files like any snapshot's (tickets unchanged: snapshot ID + location), and the executing
  node, which re-plans the pinned snapshot anyway, reads its ticket files with their delete files through
  `deletesAppliedSource` (`ExecutionService.catalogFilesSource`). Plans with delete files report records `-1`
  (recorded rows overcount). Deletion vectors, columns without field ID, dropped equality columns stay complete.
  Client-visible: such tables now give one endpoint per node in partial mode instead of one complete endpoint
  (same as every other catalog table; no ticket or header change). Tests: `CatalogSourcesDeletesTest` (each file
  alone with its own deletes adds up to the independent answer), `CatalogTablesTest` (reasons; plan in parts with
  unknown row count; DV still complete), `IcebergCatalogIntegrationTest` (Iceberg-Java table, hadoop + REST: located
  partial endpoint, a ticket per data file returns exactly that file's surviving rows, eq delete older than the
  second file leaves it alone; the iceberg_scan-workaround queries now also run partially). Local server verify:
  692 tests, only `LakeFsPluginTest` erred for the unbuilt lakeFS plugin in a single-module run (green after building
  it); Checkstyle/SpotBugs clean. CI: dispatch run 40178274322 green (the push had a [skip ci] head). Next: item 2 (ii)/(iii) are stand work; next code item
  is 3 (time travel; propose the form in Open questions first).
- 2026-10-25 (maintainer's local agent): single-node stand regression of release `server-1.0.0-4eaab052deaf`
  with the new libs set vs int7 — all checks pass, timings and memory equal (note under Next steps); new items 10
  (file:// catalog roots under an HDFS default FS, also in int7) and 11 (zookeeper / lz4-java absent from the set).
- 2026-10-25 (cloud session): item 3 needs the maintainer first: proposal for the time-travel form (SQL
  clause `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF`) added to Open questions, no code. Item 4 done (commit
  a4d0e63): named S3 stores `arrowflight.s3.stores` / `arrowflight.s3.<name>.root` with their own endpoint,
  region, URL style and credentials (`FLIGHT_S3_<NAME>_ACCESS_KEY`, ...; never the main ones; CA file, TLS
  check, metadata cache and listing settings default to the main store's, since DuckDB holds them per
  connection), own DuckDB secret `flight_s3_<name>` scoped to the store root; `StoreRegistry` now refuses a
  second store only for the same provider *and* configuration name (SPI `DataStore.configName()`); a broken
  store list stops the start. Tests: plugin `S3NamedStoresTest` (names, longest root, settings view) and
  server `S3NamedStoresTest` (real s3 + iceberg plugins: Iceberg warehouse in a second bucket next to an S3
  data directory → two scoped secrets, locate per store; refusal names the remedy; missing credentials;
  secret as property refused unprinted). Found on the way: `s3a://` catalog roots never match S3 stores
  (new item 12). Full local `clean verify checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services`
  green (server 687 / 9 skipped, client-core 158, spark 175, starrocks 11, hive 11, plugins green); push
  run 40186896468 green. Next: item 3 once the form is confirmed, else item 5 (config precedence).
- 2026-10-25 (cloud session): item 3 still waits on the maintainer (Open questions). Item 5 done (commit 1da3703):
  configuration precedence is now every `-D` name (short key and `arrowflight.*` alias) > environment >
  `etc/plugins/<ns>.properties` > `arrowflight.properties` > default. Before, the bundled file hid environment
  variables (`FLIGHT_RESULT_MODE`, `FLIGHT_ENDPOINT_BACKUP_LOCATIONS`, `FLIGHT_METRICS_ENABLED`, `DUCKDB_*`,
  `FLIGHT_S3_REGION` via `s3Region`, ...) and the file's short key beat a `-Darrowflight.*` alias. `ConfigSource`
  (`lookup`, new `firstSystemProperty` / `firstEnvironment` / `firstFileProperty`) carries the order;
  `ConfigAdapter` reads through it (`getConfig(ConfigSource)`), `PropertiesPluginConfig` puts the plugin file
  between environment and bundled file; SPI `PluginConfig` Javadoc, README, properties comments updated. Tests:
  `ConfigAdapterTest` (env beats bundled file, every -D beats env/file, blanks fall through),
  `PropertiesPluginConfigTest` and `DuckDbExtensionsTest` rewritten to the new order, `LibsSetTest` now starts
  the child server with `-Darrowflight.metrics.enabled=false`. Upgrade note in CHANGELOG and Open questions.
  Full local `clean verify checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services` green (server
  691 / 9 skipped, client-core 158, spark 175, starrocks 11, hive 11, plugins green); push run 40195045583
  green. Next: item 3 once the form is confirmed, else item 7 (Kerberos for the HMS client; 8/9 need the
  maintainer first) or items 10–12.
- 2026-10-25 (cloud session): item 11 done (commit c3b2760), taken before item 7 per the maintainer's note.
  Cause: Spark's test dependencies (zookeeper 3.6.3, org.lz4 1.8.0, curator 2.13.0) are nearer in the tree
  than Hadoop's, so ZooKeeper and lz4-java became test-only and left the libs set, and the shaded jar got
  curator-framework 2.13.0 next to curator-client 5.2.0. Fix: zookeeper 3.8.6 and at.yawk.lz4:lz4-java 1.10.4
  declared `provided` (Hadoop 3.5.0's versions, hadoop-project exclusions: no Netty, logback, slf4j, jline),
  Curator managed at 5.2.0, org.lz4 excluded from spark-core. Set: 62 jars. `LibsSetTest`: floors, one
  Curator version, link check of all hadoop-common / hdfs-client / hadoop-auth classes (left out: jsch for
  SFTP and ssh fencing, the protobuf-2.5 `ProtobufRpcEngine`), HA/Kerberos/ZK classes by name, `Lz4Codec`
  round trip; proven to fail with ZooKeeper and lz4-java removed from the set. Full local `clean verify
  checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services` green (server 694 / 9 skipped,
  client-core 158, spark 175); push run 40204556893 green; package dispatch run 40205062825 green: pre-release `server-1.0.0-c3b276054cc3` (jar, plugins, `flight-server-libs-1.0.0-c3b276054cc3.tar.zst`) for the stand. Next: item 3 once the form is confirmed, else
  item 7 (Kerberos for the HMS client) or items 10, 12.
- 2026-10-25 (maintainer's local agent, stand): secure HA HDFS stand (KDC, ZooKeeper 3.9.2 SASL, Hadoop 3.1.3 HA + ZKFC,
  Ranger 2.4.0 HDFS plugin) and first smoke of 4eaab052deaf on it — see the note "Stand, secure HA HDFS" under Next steps
  (hadoopfs: no SASL data transfer, needs a ticket cache; forced short-circuit; libhdfs3 RPC timeouts for NameNode host loss).
- 2026-10-25 (cloud session): item 3 done (commit f652985), as approved: Iceberg time travel with `FOR VERSION AS
  OF <snapshot id | 'tag/branch'>` and `FOR TIMESTAMP AS OF TIMESTAMP '<time> <zone>'` (zone required; latest
  snapshot-log entry at or before). `TimeTravelClause` finds the clause (skips literals, quoted identifiers,
  comments, dollar quotes) and `CatalogViews.timeTravel` replaces each reference in complete mode with a read of
  that snapshot's own files, deletes and columns (never `iceberg_scan`, which reads the metadata file's current
  snapshot; snapshots not readable file by file are refused); the table name stays the alias unless the client
  gave one. Partial mode answers such queries completely (no ticket or header change). Errors: unknown/expired
  snapshot, unknown ref, time before the first commit, missing zone → INVALID_ARGUMENT; missing table →
  NOT_FOUND; data-directory and Hive tables refuse. Access checks, `readsOnlyCatalogTables` see the SQL with the
  clauses removed. SPI 1.2: `AsOf` + `Catalog.asOf` (default refused), iceberg plugin `spi=1.2` (needs a server
  of this build). Tests: `TimeTravelClauseTest`, `FlightAuthorizationTimeTravelTest`, plugin
  `IcebergTimeTravelTest` (id, tag, branch, instants, schema of the old snapshot, metadata cached before a
  commit), `IcebergCatalogIntegrationTest.timeTravelReadsTheFirstSnapshotWithItsColumns` (hadoop + REST,
  complete and partial, joins old vs current, deletes snapshot, refusals; runs in CI only: the ci-assets
  download is refused from the cloud sandbox). Local verify: server 699 tests, the last failure fixed and the
  touched tests rerun; push run 40214412156 green. Next: item 7 (Kerberos for the HMS client), or 10 / 12;
  item 3 follow-ups (a)-(c) are optional.
- 2026-10-25 (cloud): **item 0b done** (cd389ff). Root cause: `streamComplete` bound the parameter values to the
  cached statement, then called `streamSql`, whose parse check took `PARSE_CHECK_SQL` from the per-thread LRU; with
  `duckDbStatementCacheSize` 0 or 1 (clamped to 1) that closed the bound statement and the query text was prepared
  again without values → "Parameter count mismatch". Now `streamSql` = parse check + `streamPrepared`, and
  `streamComplete` calls `streamPrepared` with its bound statement (also saves a second parse check per
  complete query). Regression test `DuckDbAdapterTest.streamCompleteBindsParametersWithAnyStatementCacheSize`
  (sizes 0, 1, 2, 64; parameterized, plain, parameterized again, wrong value count) failed before the fix with the
  reported error. Local: DuckDbAdapterTest 87/87, checkstyle + spotbugs clean; push run 40222858195 green.
  Next: item 7 (Kerberos for the HMS client), or 10 / 12.
- 2026-10-25 (cloud): **item 7 done** (d326dd6): Kerberos (SASL/GSSAPI) for the Hive Metastore client, new
  `KerberosLogin` (JAAS Krb5LoginModule, keytab or ticket cache, renewal) and settings `metastorePrincipal`,
  `clientPrincipal`, `clientKeytab`, `clientTicketCache`, `saslQop` (additive; `authentication=none` unchanged).
  `KerberosMetastoreTest`: Kerby 2.0.3 KDC in the JVM (test scope), a `TSaslServerTransport` front as
  `hive/localhost` forwarding to the real Hive 3.1.3 test metastore; keytab login at auth-conf, ticket cache at
  auth / auth-int (catalog answers = plain metastore), unknown service principal, no common protection layer,
  missing cache, wrong keytab principal, plain client against SASL. Local: hive module 16/16 + checkstyle +
  spotbugs clean; push run 40232501858 green. Next: item 10 or 12 (8 needs the maintainer first, 9 design first).
- 2026-10-25 (cloud): **item 10 done** (67d809c): `StoreRegistry.CatalogStores.locate` opens a catalog location
  without a scheme as `file:<path>` (normalised `/wh`, `file:/wh`, `file:///wh`), so a local warehouse no longer
  resolves against a non-local `fs.defaultFS`; the data directory as a plain path is unchanged (follows
  `fs.defaultFS`). Regression test `CatalogRegistryTest.localWarehouseUnderANonLocalDefaultFileSystem` (viewfs
  default FS standing in for HDFS; four warehouse spellings; table files located in a local store) failed before the
  fix with the stand's error. Local: CatalogRegistryTest + StoreRegistryTest, checkstyle + spotbugs clean (local
  Maven: the wrapper's 3.10.0 download fails, `/opt/maven/bin/mvn` works); push run 40241864311 green.
  Next: item 12 (s3a/s3n catalog roots); 8 needs the maintainer first, 9 design first.
- 2026-10-26 (local perf campaign): `docs/dev/TUNING.md` (stand single node, HDFS, build 4eaab052deaf; CI screening on
  4eaab052deaf and c3b276054cc3); new item 13 (default changes). Storage HDFS vs S3 and 3-node runs with the best
  settings not measured yet (stand not available for valid measurements).
- 2026-10-26 (cloud session): item 0c done (60e888b): `duckDbThreads` default 4 in code (was 1) and in the bundled
  `arrowflight.properties` (was 2); unit test `ConfigAdapterTest.duckDbThreadsDefaultsToFourWithoutAnySetting`;
  CHANGELOG, TUNING.md, START-NOTES, user guides. Package dispatch run 40245198713 green (build + package; the push
  run 40245189332 was cancelled in its favour): pre-release `server-1.0.0-60e888bc26ec`. Stand: rerun with the new
  default where it ran 2. Next: item 1b (dependency security upgrades).
- 2026-10-26 (cloud session): item 12 done (514782d): `s3a://` / `s3n://` catalog locations are matched as
  `s3://` — `CatalogLocations.normalize`, the SQL `normalizedLocation` for position delete paths,
  `StoreRegistry.locate` and the iceberg plugin's `Locations.normalize` (an s3a warehouse used to stop the
  start; s3a files under an s3 root were hidden). Regression tests in `CatalogTablesTest`,
  `CatalogSourcesDeletesTest`, `S3NamedStoresTest`, `PluginPartsTest`. CI run 40251162808 green. Next
  numbered items: 8 (ask the maintainer first), 9 (design in Open questions first), 13 (proposals awaiting
  approval); stand checks listed per item.
- 2026-10-26 (cloud session): numbered items left are stand checks, 8 (ask first), 9 (design first) and 13
  (proposals), so: item 9 design written to Open questions (phase A trusted-proxy header
  `x-flight-effective-user`, B JWT provider, C storage-level identity; questions for the maintainer). Fixed
  secure-HA stand finding (c) (6b20c3a): `HadoopDataStoreProvider` keeps the deployment's
  `dfs.client.read.shortcircuit`, otherwise turns it on only with a `dfs.domain.socket.path` (was forced on and stopped
  the start without a socket); `HadoopDataStoreProviderTest`. CI run 40260783151 green. Stand: int7 HDFS with local
  DataNodes must still read short-circuit (socket path set). Next: answers to item 9 / 13, or the other unnumbered
  stand findings (secure HA (d)/(e), 3-node S3 (a)-(c)).
- 2026-10-26 (cloud session): item 0d done (8e755fb). Reproduced in one JVM (3 Hazelcast members, iptables DROP on
  the leaving member's port to mimic a gone container): after a member left, even gracefully, `entrySet` on the
  survivor took 10.3 s while key ops took ms; `ClusterService.allServerLoads` (every plan) used it. Now
  `getAll(memberUris)` (entrySet only when members carry no URI), and `fileLocations(true)` reads members'
  inventories by key on shared storage (local disks keep every inventory so a lost node's files fail the plan).
  Measured after the fix: 11 ms. Hazelcast's shutdown hook (TERMINATE = looks like a crash) disabled; the server's
  hook leaves gracefully, `hazelcast.graceful.shutdown.max.wait` 5 s (was 600 s: a graceful leave against an
  unreachable peer hung 363 s). Tests: `ClusterServiceTest` (key reads, inventories kept on local disks),
  `ClusterMemberLossIntegrationTest.survivorPlansAtOnceAfterAMemberLeft`, `HazelcastAdapterTest`. Crash case
  (10 s) → Open questions. CI run 40272329184 green. Next: answers to the Open questions (0d crash case, 9, 13) or the unnumbered stand findings.
- 2026-10-26 (cloud session): item 0e done (c71ee2e + golden 8b4ffe7). Measured with DuckDB 1.4.5 + the ci-assets
  httpfs against a local S3 stand-in (16 Parquet files, 352 range requests per query): httpfs opened 32 TCP
  connections at 1 thread, 144 at 8, 322 at 32 (keep-alive reuse collapses with threads), and against a
  refusing port its default retries (3 from 100 ms, backoff 4) gave up after 0.5 s; with 6 retries from 200 ms,
  backoff 2 a query rode out a 3 s refusal. The s3 plugin now sets `http_retries` / `http_retry_wait_ms` /
  `http_retry_backoff` (new settings `httpRetries` 6, `httpRetryWaitMs` 200, `httpRetryBackoff` 2, ~6 s in
  all; inherited by named stores; validator allowlist extended). Tests: `S3SettingsTest`, `S3DataStoreTest`,
  `S3NamedStoresTest`, `S3PluginTest`, golden `duckdb-setup.txt`, and
  `S3IntegrationTest.connectRefusalsAreRetried` (forwarder refuses connections 1.5 s during an 8-thread read;
  retries 0 fails as control). CI run 40282571105 red (golden file not updated), 40283144913 on 8b4ffe7 green.
  Root cause on the stand not confirmed (likely client ephemeral ports / endpoint limits from the connection
  churn) — stand check listed in item 0e. Next: stand rerun of 0e; answers to the Open questions (0d crash case,
  9, 13) or the unnumbered stand findings.
- 2026-10-26 (cloud session): item 0f done. Reproduced the stand error in a test JVM: a Flight server with 256 MiB
  direct memory streaming 1 MiB batches to 16 clients that read late fails every stream with "CANCELLED: Server
  sendMessage() failed with Error" (`OutOfMemoryError: Cannot reserve ... direct buffer memory` in gRPC's
  MessageFramer; peak direct 2.1 GB at `-XX:MaxDirectMemorySize=2g`, i.e. each stream filled its 256 MiB
  threshold). Some streams of such a server never finish (no error, no data) — likely what clients saw as hangs.
  Fix: new gRPC interceptor `OutboundBudget` (installed via the Flight builder's `grpc.builderConsumer`) caps
  each streaming call's onReady threshold at budget / open streams (min 1 MiB, max the requested
  `flightBackpressureThresholdBytes`, rebalanced when streams open/close; gRPC reads the threshold on every
  readiness check). New setting `flightOutboundBudgetBytes` (default 0 = MaxDirectMemorySize / 4, else heap /
  4); per-stream threshold default unchanged (13b stays a proposal). Measured: same 16 streams, budget 16 MiB,
  peak direct ~130 MB of 256 MiB, all rows; budget 8-32 MiB at 2 GB: 0.37-0.42 GB peak, all rows.
  Tests: `OutboundBudgetTest` (units + forked-JVM regression, also asserts the unbounded control fails),
  `ConfigAdapterTest`, `HadoopArrowFlightServerUnitTest`. CI run 40297852948 on dd31117 green.
  Next: stand rerun of 0f and 0e; answers to the Open questions (0d crash case, 9, 13).
- 2026-10-26 (cloud session): no numbered item open without a maintainer answer or a stand run (0d-0f, 1b, 2,
  item 0 stand measurements; 8, 9, 13 wait on Open questions), so took the secure HA stand finding (e). Hadoop
  IPC sends the NameNode exception stringified with its stack trace, which reached Flight clients as the error
  description. New gRPC interceptor `StackTraceTrimmer` (installed next to `OutboundBudget` in `grpcOptions`)
  drops Java frame lines (`at a.B.c(B.java:1)`, module/`app//` forms) and `... n more` from every closing
  status; message and `Caused by:` lines stay, multi-line DuckDB errors (Binder `LINE 1:` carets) are
  unchanged; `failureDescription` and the "Cannot read the table's files" text trim before the 1024-char cut.
  Server logs unchanged. Tests `StackTraceTrimmerTest` (incl. an in-process Flight server/client),
  `HadoopArrowFlightServerUnitTest`. Commit 5e573c1, CI run 40310036063 green. Next: stand reruns of 0e/0f; answers to the Open
  questions (0d crash case, 9, 13); other unnumbered findings (secure HA (b) ticket cache from the keytab,
  (d) libhdfs3 RPC timeouts as defaults = maintainer's call, 3-node S3 (b) reservation release, (c)).
- 2026-10-26 (cloud session): 3-node S3 stand finding (b) partly fixed (9168a44): Flight `CancelFlightInfo` (and Flight
  SQL `CancelQuery`, which Arrow maps to it) was `UNIMPLEMENTED`; now it resolves each endpoint's signed ticket and
  releases its node-load reservation at once (idempotent through the reservation key; running streams continue; a
  later read of the ticket releases nothing twice), answers `CANCELLED`, `INVALID_ARGUMENT` for unsigned tickets.
  Tests `FlightSqlProducerTest` (action wiring, tracked/untracked, forged and non-SQL tickets),
  `EndpointFailoverIntegrationTest.cancelFlightInfoReleasesTheLoadOfAnUnreadPlanAtOnce` (Flight client, real
  Hazelcast). README "Cancelling a plan". Local: those tests + checkstyle + spotbugs clean; CI run 40325762742 green.
  Clients that never cancel still hold reservations 10 min → Open questions (connectors should cancel; TTL).
  Next: stand reruns of 0e/0f; answers to the Open questions (0d crash case, 9, 13, reservation TTL); other
  unnumbered findings (secure HA (b) ticket cache from the keytab, (d) libhdfs3 RPC timeouts, 3-node S3 (c)).
- 2026-10-26 (cloud session): item 13(d) (ADBC "failed to update autocommit: ... Unimplemented"). ADBC's Go driver
  sends BeginTransaction only when its connect-time SqlInfo read decoded FLIGHT_SQL_SERVER_TRANSACTION as
  TRANSACTION/SAVEPOINT, so the client had read a wrong answer. Root cause found in Arrow Java (19.0.0, also arrow-java
  main): `ListVector`/`LargeListVector.getFieldBuffers` set `writerIndex(4/8)` on the offset buffer even when empty,
  and an unallocated vector's buffer is the allocation manager's process-wide empty `ArrowBuf`; every other empty
  vector sets it to 0, so concurrent streams serialize empty buffers with the wrong length (a CANCELLED stream with
  `IndexOutOfBoundsException` in `ArrowMessage.asInputStream`, or a header/body mismatch the client decodes shifted).
  Every SqlInfo answer has empty list/map union children. Fix: `EmptyListOffsets` wraps every DoGet listener
  (`FlightSqlProducer.getStream`) and allocates own offset buffers for empty list vectors before each batch;
  BeginTransaction stays UNIMPLEMENTED with a description + WARN line (to match on the stand). Repro:
  `FlightSqlProducerTest.concurrentClientsAllReadNoTransactionSupport` (16 clients x 25 SqlInfo reads over Flight)
  failed 3/3 without the guard, passes 3/3 with it; `EmptyListOffsetsTest`. Not reproduced against a locally started
  server with ADBC Go 1.8.0 / Python ADBC 1.13.0 (~13 000 concurrent connects, insecure and TLS+bearer, CPU
  load) — the stand rerun (13d in Next steps) decides. CI run 40348114333 on 4939fb7 green. Next: stand reruns of 0e/0f/13d; answers
  to the Open questions (0d crash case, 9, 13, reservation TTL); other unnumbered findings (secure HA (b) ticket cache
  from the keytab, (d) libhdfs3 RPC timeouts, 3-node S3 (c)).
- 2026-10-26 (cloud session): secure HA stand finding (d) (libhdfs3 NameNode RPC timeouts). The hadoopfs
  extension (v1.4.5-afc3, ci-assets) has settings only for DataNode connect/read and dead-node TTL, none for
  `rpc.client.*`, so the server cannot ship them as DuckDB defaults. Added (additive, log only):
  `HadoopArrowFlightServer.longHdfsRpcTimeouts` + startup WARN on HDFS naming each of
  `rpc.client.{connect.timeout,read.timeout,write.timeout,timeout}` LIBHDFS3_CONF leaves unset or > 60 s with the
  stand's value; values documented in `arrowflight.properties` and START-NOTES. Test
  `HadoopArrowFlightServerTest.reportsLongHdfsRpcTimeouts`. CI run 40362074150 on 8e0307f green. Note: the extension's
  `hdfs_dead_node_ttl_ms` is not exposed as a server setting yet (optional follow-up). Next: secure HA (b) (ticket
  cache from the keytab), 3-node S3 (c), and the stand reruns of 0e/0f/13d.
- 2026-10-26 (local perf campaign, final phase on 57d252b1437f, 3 rounds, HDFS threads 1/4/8, S3 4/8, 3 nodes 4/8 + two
  4eaab052deaf controls): `docs/dev/TUNING.md` final comparison (default 4 threads = balanced on HDFS, S3 and 3 nodes; same
  speed as 4eaab at 4). 0f confirmed fixed (0 failed reads in 6/6 rounds) and corrected (the node death was the harness
  memory guard); 0e heavy path fixed (SF10 JDBC 22/22 at 8 threads on S3); new 0g (S3 ephemeral-port exhaustion at 8 threads
  x 32 clients), 0h (3-node heavy4 +18 %, recheck); ADBC autocommit race still seen once on 57d252b1437f (before 4939fb7).
- 2026-10-26 (cloud session): item 0g (S3 ephemeral-port exhaustion). Cause measured with DuckDB 1.4.5 + the
  ci-assets httpfs against a counting HTTP server/forwarder: httpfs caches HTTP clients per open file handle only
  and at 2+ threads every row group read gets a new handle, so ~1 connection per range read (16 files x 4 row
  groups: 78 conns/96 requests per query at 4-16 threads, 32 at 1 thread; `http_keep_alive`, curl client,
  prefetch settings no help; metadata caches only save footers/HEADs). `duckDbExternalFileCache=true` makes a
  repeated query open 0 connections. Added (additive, log only): S3 plugin WARNs once at startup when
  `net.ipv4.tcp_tw_reuse` != 1 with port count and reads/s (`S3DataStoreProvider.ephemeralPortWarning`,
  `S3DataStoreProviderTest`); workaround in `arrowflight.properties` and START-NOTES; regression test
  `S3IntegrationTest.connectionsPerQueryAreBoundedAndCachedReadsOpenNone` (8 files x 4 row groups at 8 threads:
  39/31 connections for the first/repeated query, with the cache 40/0). Server-side fix -> Open questions
  ("S3 connection churn"). CI run 40399232182 on 9f7eb4f green. Next: 0h (stand recheck), stand rerun of 0g
  with `tcp_tw_reuse=1`, then secure HA (b) (ticket cache from the keytab), 3-node S3 (c).
- 2026-10-26 (cloud session): secure HA stand finding (b) (hadoopfs needs a ticket cache). libhdfs3 (afc3) reads
  only a ticket cache (`hadoop.security.kerberos.ticket.cache.path` in LIBHDFS3_CONF, else `KRB5CCNAME`, else the
  default). Added (additive, off unless set): `hdfsKerberosPrincipal` + `hdfsKerberosKeytab` (`HDFS_KERBEROS_*`,
  optional `hdfsKerberosTicketCache`) — `HdfsTicketCache` logs in from the keytab (JAAS), writes the TGT as a MIT
  file cache v4 (owner-only, atomic replace) before the data store opens, and logs in again at 80 % of the
  lifetime (retry every minute on failure; a failed first login stops the start); WARN when the cache is not the
  one the extension reads. Test `HdfsTicketCacheTest` (in-process Kerby KDC; the cache is read by the JDK, Kerby
  and MIT `klist`/`kvno`, which also gets a service ticket with it — verified locally; Dev CI now installs
  krb5-user so that case runs there too). CI run 40415521254 on 2b2cac8 green. Next: 0h (stand recheck), stand
  reruns (0g with `tcp_tw_reuse=1`, secure HA (b) without `kinit`), then 3-node S3 (c) (first plan after B5 waits
  9-10 s), optional: expose the extension's `hdfs_dead_node_ttl_ms` as a server setting.
- 2026-10-27 (cloud session): 3-node S3 stand finding (c) (first plan after B5 waits 9-10 s on S3, ~1 s on HDFS)
  explained, no code change. Measured in one JVM (doomed member on 127.0.0.2): crash with its port refusing ->
  member dropped and first plan's cluster ops done in ~0.35 s; crash with the address made silent (iptables DROP)
  -> 9.96 s = `hazelcast.max.no.heartbeat.seconds`. S3 and HDFS stores are both `shared` and plan through the same
  key lookups since 0d, so the storage is not the difference; how the nodes died is. Tests in
  `ClusterMemberLossIntegrationTest` (refused port < 5 s; vanished address with the timeout at 3 s via `-D` takes
  2-8 s — needs iptables as root or `sudo -n`, skipped otherwise); START-NOTES documents the window and the `-D`
  override. Decision on faster detection stays in Open questions (0d follow-up). CI run 40428800760 on 9934d3b
  green (server 764 tests / 2 skipped = the usual two; the iptables case runs on the runner via sudo -n).
  Next: 0h (stand recheck), stand reruns (0g with `tcp_tw_reuse=1`, secure HA (b) without `kinit`, B5 kill mode),
  optional: expose the extension's `hdfs_dead_node_ttl_ms` as a server setting; 3-node S3 (a) (S3 outage read
  latency) partly covered by 0e's retry settings.
- 2026-10-27 (maintainer's local agent): speed comparison upstream `main` dbacd8d / `feat/starrocks-native-integration`
  07631d7 vs ours 57d252b on the same single-node stand (3 rounds, same container/JVM/data/clients): results in
  `docs/dev/UPSTREAM-SYNC.md` "Speed comparison 2026-10-26". Upstream wrong answers (LIMIT ignored, decimal scale lost in q06,
  no AVG / prepared statements / parameters) disqualify it on JDBC TPC-H, heavy and small aggregates; upstream wins point/count
  latency, TPC-H SF1 via Trino and single-stream scan. Next steps 14-16 added.
- 2026-10-27 (cloud session): item 14, DuckDB 1.5.6 (`duckdb_jdbc` 1.5.6.0 in server, iceberg and hive modules;
  a442867, 27f583a). Behaviour differences found and handled: 1.5.6 shows a `file:///d/` root in allowed_directories
  as `/d/` (lockdown check's comparable form now drops `file:`; Iceberg/Hive ITs failed on it in CI run 40440676383),
  and stores a URI root whose scheme no loaded extension registers as a local path below the working directory
  (the check refuses it: fails closed; the server loads httpfs/hadoopfs before the lockdown, and the S3 ITs with
  httpfs pass); connect errors now read "Could not connect to server" (test wording). New test
  `LockdownCheckTest.acceptsFileUriRoots`. Dev CI takes `duckdb-extensions-v1.5.6.tar.zst` from ci-assets when
  present, else downloads httpfs/iceberg/avro v1.5.6 from extensions.duckdb.org (hadoopfs IT stays skipped). CI run
  40441645851 on 27f583a green; package run 40442729111 green: pre-release `server-1.0.0-27f583adccd7`. Next: coordinator adds
  the v1.5.6 extension set (with hadoopfs) to ci-assets and reruns the stand (item 14 Stand); then item 15.
- 2026-10-27 (maintainer's local agent): item 14 stand rerun (short): `server-1.0.0-27f583adccd7` (DuckDB 1.5.6, v1.5.6
  extension set, hadoopfs 51b7220-afc3 loads) vs 57d252b (1.4.5) vs upstream native 07631d7 (duckDbThreads 8), single-node
  HDFS stand, same tpch-only files and schema-qualified queries, 3 interleaved rounds, none discarded. TPC-H JDBC 22/22 PASS on
  27f583a and 57d252b (SF10 9e2af63e8711, SF1 de713f9eea2f). 27f583a vs 57d252b, default 4 threads: small JDBC p50/p95 c1
  18.1/33.4 vs 21.7/38.6 ms, c8 26.3/82.1 vs 27.0/69.6, c32 86.1/328 vs 76.0/354; ADBC c1 13.9/28.7 vs 16.5/34.6, c8 23.3/70.6
  vs 23.7/67.4, c32 77.7/298 vs 67.5/302; TPC-H SF1 1.66 vs 1.82 s, SF10 11.42 vs 11.96 s. c1 p50 of the classes upstream
  answers correctly (point_orders / point_lineitem / count_filter): ADBC 14.1/16.2/28.3 vs 19.6/24.5/33.9, upstream
  11.5/14.7/18.7 ms; JDBC 18.9/23.9/32.7 vs 24.5/30.5/37.3, upstream 22.0/22.8/18.9 ms. DuckDB 1.5.6 closes ~59 % of the
  small-query gap to upstream (sum of these classes, both clients): point lookups nearly closed, count_filter 25-37 %
  (rest = item 15). Upstream again ran out of memory under c8/c32 and returned wrong LIMIT / TPC-H answers.
- 2026-10-27 (cloud session): item 15 step 1 (775100b). Profiled a point query phase by phase with the new
  `PointQueryOverheadPerfTest` (tag `perf`, not in CI): complete mode did three DuckDB describes and four parse checks
  of the same text per JDBC/ADBC query. `DuckDbAdapter` now keeps one description (result + parameter schema) per
  (text, views snapshot identity), bounded to 1024 texts (metric label `describe`); the describe connection keeps its
  parse-check statement; `forgetParameters` uses `deleteAsync`. Local: point p50 14.0 -> 10.2 ms, c8 31 -> 22, c32 94
  -> 58 ms (plain DuckDB 1.7 ms); no wire, setting or answer change. Tests: `DuckDbAdapterTest.describeCompleteIsReused
  UntilTheViewsChange`, `ClusterServiceTest.forgetParametersDoesNotWait`. CI run 40453394028 green; package run
  40454083694 green: pre-release `server-1.0.0-775100bf923f`. Next: stand rerun of the small-query rounds on it
  (item 15 Stand); step 2 candidates listed in item 15 (partial-mode sample-file listing first).
- 2026-10-27 (cloud session): item 15 step 2 (part, 80ccac9). JFR of `PointQueryOverheadPerfTest`: every DoGet
  repeated the single-query parse check (json_serialize_sql) the describe had already done, built DEBUG-only
  arguments (`LogUtil.elapsedNanos` -> `String.format`) with DEBUG off, and compiled two regexes + looked up
  SHA-256 for the execution-path event. Now: `DuckDbAdapter` remembers accepted texts with their function/table
  names (1024 texts, metric label `parse_check`; refused names checked on every use, also ones plugins add
  later), debug arguments guarded, regex/digest reused (same event output). Local: complete point p50 ~13.1 ->
  11.9 ms (DoGet 6.2 -> 5.4), c32 ~78 -> 71 ms; partial unchanged apart from DoGet. No wire, setting, log-line or
  answer change. Tests: `DuckDbAdapterTest.parseCheckIsRememberedAndRefusedNamesAreCheckedAgain`,
  `ExecutionPathRecorderTest.digestIsStableAcrossRepeatedCalls`. CI run 40459493162 green; package run
  40460140236: green, pre-release `server-1.0.0-80ccac9d9de6`. Next: stand rerun (item 15 Stand) on this pre-release; then the remaining step 2
  candidates (a)-(b) or close item 15 on the stand numbers.
- 2026-10-27 (cloud session): item 15 step 2 (a) + (b). (a) 01cdd57: partial-mode describes bind against a sample
  file remembered per table for 30 s (one re-list when a describe cannot read it) instead of listing the table on
  every new query text. (b) 45fc0a9, approved by the maintainer in 10b6163: the per-DoGet `executionPath=` INFO line
  and evidence file are written by one background thread (same level/text, caller writes itself when 8192 are
  pending, drained at shutdown). Local `PointQueryOverheadPerfTest` (same session, noisy 4-core box): partial point
  p50 38.4 -> 12.5 -> 11.1 ms (prepare 28.5 -> 3.1), partial c8 97 -> 28 -> 24 ms; complete point 15.8 -> 15.1 ms,
  complete c32 90 -> 78 ms; plain DuckDB point 2.0 ms. No wire, setting, answer or log-text change. Tests:
  `ParquetAdapterTest.sampleFileIsRememberedUntilForgotten`, `ExecutionServiceTest.describeRetriesWithAFreshSampleFileAfterAReadError`
  / `describeDoesNotRetryAQueryError`, `ExecutionPathRecorderTest.eventsRecordedConcurrentlyAreAllWritten`. CI run
  40462267120 green (01cdd57); package run 40463504538 green on 45fc0a9: pre-release `server-1.0.0-45fc0a9e6e02`.
  Next: stand rerun (item 15 Stand) on this pre-release, then close item 15 and go on with stage 2 item 0g.
- 2026-10-27 (cloud session): item 15 step 3 (e46f28d). Server TIMING log of `PointQueryOverheadPerfTest`
  (complete point, c1): DoGet `execution.total` 3.6 ms (DuckDB to first batch 2.5 ms; plain DuckDB 1.9 ms),
  client-side DoGet 4.9 ms. Every complete-mode prepare got a statement id, so GetFlightInfo did a synchronous
  Hazelcast `get` for parameter values and close a delete even without parameters; now only a query with `?`
  parameters gets the id handle (others: the SQL text, as in partial mode). Local: close p50 1.15 -> 1.05 ms,
  info 2.4 -> 2.2 ms (one node; on a cluster the get is a network round trip). Only visible difference: a client
  that sends values to a query without parameters and keeps its first handle now runs it without them instead of
  getting a count-mismatch error (CHANGELOG). Test `FlightSqlProducerTest.onlyAQueryWithParametersHasAStatementId`.
  c8/c32: c32 p50 64-69 ms at 4 DuckDB threads, 57 at 2, 51 at 1 (local 4 cores) -> oversubscription (item 19).
  Package run 40467177476 (push run 40467155113 cancelled in its favour): build + package green, pre-release `server-1.0.0-e46f28dfdf09`. Next: stand rerun (item 15 Stand),
  then close item 15.
- 2026-10-27 (cloud session): item 15 waits for the stand (code complete; two sessions have passed since the priority
  note), so stage 2 item 0g. Measured on DuckDB 1.5.6 (`S3IntegrationTest`, counting forwarder, 8 files x 4 row
  groups, 8 threads): still 40 + 31 connections for two queries (80 requests). httpfs 1.5 has
  `httpfs_connection_caching` (default off); on: 8 + 0, every later query 0, a connection the endpoint cut is
  reopened and the answer stays right. 6acc9cc: new s3 setting `arrowflight.s3.connectionCaching` (default true,
  named stores and lakeFS inherit it; false = old behaviour), allowed by the setup validator; the `tcp_tw_reuse`
  startup WARN only with false; `s3=open` line names it. Tests: `S3IntegrationTest.connectionsPerQueryAreBoundedAnd
  CachedReadsOpenNone` (off / file cache / on + cut), `S3SettingsTest.connectionCachingFromTheConfiguration`,
  `S3DataStoreTest.duckDbConnectionCachingCanBeOff`, `S3DataStoreProviderTest.silentWhenConnectionsAreKept`, goldens.
  Local full `clean verify checkstyle:check spotbugs:check -DexcludedGroups=perf,real-services` green (server 774 / 3
  skipped). Package run 40470316519 (push run 40470310907 cancelled in its favour): build + package green,
  pre-release `server-1.0.0-6acc9cce0b26`. Next: stand rerun of 0g (see item 0g Stand) and item 15 Stand on this
  pre-release (it carries item 15 step 3 too); then item 17.
- 2026-10-27 (cloud session): item 15 still waits for the stand, so stage 2 item 17 (S3 read path). Findings (item 17
  in the stage 2 list): DuckDB 1.5.6 already caches HEAD + footer per connection (`parquet_metadata_cache` adds nothing:
  20 -> 12 requests for a repeated 4-file query either way), and has no size cap for the external file cache. Bug
  found on the way: after an S3 object is rewritten, the first query of each connection that had read it fails
  "ETag on reading file ... was initially ..." (with and without `connectionCaching`; the connection's next query reads
  the new file). 7562a5b: such a refusal before the listener started runs the query once more (INFO
  `duckdb=changedFileRetry`), after the first batch it still fails; no wire or setting change. Tests:
  `S3IntegrationTest.aQueryAfterAFileWasRewrittenReadsTheNewFile` (fails without the fix),
  `DuckDbAdapterTest.recognisesARewrittenRemoteFile`. Package run 40472888557 (push run 40472882410 cancelled in its
  favour): build + package green, pre-release `server-1.0.0-7562a5bdb7b9`. **Stand:** rewrite one S3 data file while the 32-client small-query step runs; expect no ETag
  errors at the clients and `duckdb=changedFileRetry` lines in the server log. Next: item 15 stand + close; item 17
  memory-bound design (Open questions candidate), listing cache, parallel range reads.
- 2026-10-27 (cloud session): item 15 still waits for the stand; item 17 continued (findings (iv)-(v) in the stage 2
  list). Listing: already cached in Java, DuckDB never lists. Range reads measured with a latency per request in the
  fake S3 (`FakeS3.latency`, new perf probe `S3ReadPathPerfTest`, 10 ms): with the plugin defaults a repeated query
  sends no HEAD and no footer read and one GET per row group (all columns), a pruned point query 2 GETs;
  `prefetch_all_parquet_files` changes nothing; `metadataCache=false` costs a HEAD + footer per file and query. No new
  setting; the pattern is pinned in CI by `S3IntegrationTest.repeatedQueriesReadOnlyTheRowGroupsTheyNeed` (cfe406b,
  test-only, no pre-release needed). CI run 40475972551 green (server 777 / 2 skipped). New Open question "S3 read cache
  with a memory bound" (options A loopback range cache in the plugin / B `cache_httpfs` / C document only). Next: item
  15 stand + close; item 17 waits for that decision, so item 18 (distributed complete mode) is the next stage 2 item.
- 2026-10-27 (cloud session): item 15 still waits for the stand (code complete, two sessions passed since the
  priority note); item 17 waits for the cache decision, so stage 2 item 18 (distributed complete mode), step 1
  (895dc20): `CompleteAnswerMerge` (which shapes merge provably + DuckDB merge SQL) with `CompleteAnswerMergeTest`
  (17 shapes merged = DuckDB over all rows, names and types; refusals for LIMIT on aggregates, unselected GROUP BY
  columns, ORDER BY over inputs, HAVING/OFFSET/DISTINCT aggregates/avg/joins). Not wired in: no runtime change, no
  pre-release. CI run 40477705088 green. New Open question "Item 18: node-to-node calls" (peer identity under
  Ranger, inter-node TLS, failure rule) - step 2 waits for it. Next: item 15 stand + close; then, while 17 and 18
  wait for decisions, item 16 (single-stream scan opt-ins; stand measurement) or item 19.
- 2026-10-27 (cloud session): item 15 still waits for the stand (code complete), so stage 2 item 17, option A step 1
  (84fec77, chosen by the maintainer in ab48098): opt-in node-wide S3 read cache. `arrowflight.s3.cacheMegabytes`
  (default 0 = off; JVM heap, refused above half of it) starts one 127.0.0.1-only HTTP endpoint in the s3 plugin;
  DuckDB's secret then points at it with a random per-store key (checked as SigV4, `S3SigV4`; the S3 credentials stay
  in Java), a key reads only its own data directory, listings are refused (501). GETs are answered from an LRU of
  1 MiB blocks keyed by bucket/key/ETag (`S3BlockCache`); misses read up to 64 blocks per GET with `If-Match`, so
  one answer never mixes object versions (412 -> new version, DuckDB refuses the change and the server's
  changedFileRetry runs the query again); HEADs always reach S3; versions are trusted `cacheCheckSeconds` (default
  10). Main and named stores; lakeFS not yet. Tests: `S3CacheEndpointTest` (SDK client signing in front, fake S3
  behind: hits, capacity, rewrites, refusals, ranges), `S3IntegrationTest.theReadCacheServesEveryConnectionOfTheNode`
  (DuckDB 1.5.6 httpfs through the cache: a second DuckDB instance reads 0 GETs from S3, answers right). FakeS3:
  content ETags and If-Match. Local: s3 plugin verify + checkstyle + spotbugs green, `S3IntegrationTest` 10/10 with httpfs 1.5.6. Package run
  40480315297 green (push run 40480303237 cancelled in its favour): pre-release `server-1.0.0-84fec772ec50`. Next: stand round
  (item 17 Stand), then 17 (a)-(d); Open question "S3 read cache follow-ups".
- 2026-10-27 (cloud session): item 15 still waits for the stand (code complete), 18 for its node-to-node decision,
  so item 17 (c) (8625e28): single flight in the S3 read cache. Concurrent GETs that miss the same 1 MiB block read
  it from S3 once; the others wait for that read (`S3CacheEndpoint.blocks`/`readOwn`, a per-block future map) and
  take its bytes or its failure; a request reads its own claimed blocks before it waits, so two requests never wait
  for each other; a refused If-Match fails every waiter, each looks the version up and the new version is read once
  again. `joined=` added to the `s3=cacheClosed` line. Tests (`S3CacheEndpointTest`, fake S3 with 300 ms latency,
  4 readers): one GET for 2 blocks (4 before the change), 2 GETs across a rewrite (8 before). Local s3 module 54/54 +
  checkstyle + spotbugs green. Package run 40491054544 green: pre-release `server-1.0.0-8625e28f08cd`. **Stand** (with
  the item 17 round): note `joined=` at the 32-client step with `cacheMegabytes=1024`. Next: item 15 stand + close;
  17 (b) lakeFS through the cache or (d) metrics; then item 16 / 19.
- 2026-10-27 (cloud session): item 15 still waits for the stand (code complete; the priority note's two sessions have
  passed), 18 for its node-to-node decision, so item 17 (d) (b28fedf): S3 read cache metrics. New SPI 1.3
  (additive): `StoreContext.metrics()` returns `PluginMetrics` (default: publishes nothing); a plugin registers a
  `MetricSource` of unlabelled `arrowflight_*` `MetricSample`s (counter names end in `_total`), read on every scrape of
  `/metrics`; a name the server or another source already shows is left out, a throwing source is skipped. The shared
  cache endpoint registers at start and unregisters with its last route: `arrowflight_s3_cache_capacity_bytes`,
  `_used_bytes`, `_blocks`, `_hits_total`, `_misses_total`, `_evictions_total`, `_joined_total`, `_s3_reads_total`,
  `_s3_read_bytes_total`, `_s3_lookups_total`. s3 plugin descriptor `spi=1.3`. Tests:
  `MetricsServiceTest.exposesPluginMetricsWhileRegistered`, `S3CacheEndpointTest.metricsCountBlocksAndRequestsToS3` /
  `theSharedEndpointPublishesItsMetricsWhileItRuns`, `S3IntegrationTest.theReadCacheServesEveryConnectionOfTheNode`
  (hits on the endpoint), `SpiTest`. Local: spi + s3 module + server metrics/plugin/S3 tests, checkstyle, spotbugs
  green (S3IntegrationTest needs the httpfs extension: CI only). Package run 40507196974 (push run 40507189613
  superseded): build + package green (server 783 / 2 skipped), pre-release
  `server-1.0.0-b28fedf9edbf`. **Stand** (with the item 17 round): scrape `/metrics` during the
  32-client step with `cacheMegabytes=1024`, compare hits/misses/`s3_reads_total` with the `s3=cacheClosed` line.
  Next: item 15 stand + close; 17 (b) lakeFS through the cache; then item 16 / 19.
- 2026-10-27 (cloud session): item 15 still waits for the stand (code complete), 18 for its node-to-node decision,
  so item 17 (b) (752e272): lakeFS through the read cache. New public `S3ReadCache` in the s3 plugin (opens a route
  from a namespace's `cacheMegabytes` / `cacheCheckSeconds`, contributes the cached secret); the s3 provider uses it,
  the lakefs provider opens a route for `s3://<repository>` (the DuckDB secret's scope; pinned commits are immutable
  keys). Plugins load in their own class loaders, so the lakefs cache is a separate instance: its log lines and
  metrics carry `lakefs` (`lakefs=cacheOpen`, `arrowflight_lakefs_cache_*`; the same names would be dropped as
  duplicates by `/metrics`); with both on, the two sizes together should stay below half of `-Xmx` (each is checked
  alone; documented in START-NOTES). Fix in both caches: a miss GET whose answer carries another ETag than the
  looked-up version is refused like a 412 (a gateway that ignores If-Match could otherwise mix versions within
  `cacheCheckSeconds`; `S3CacheEndpointTest.aStoreThatIgnoresIfMatchIsNeverMixedEither` fails without it). lakefs
  descriptor `spi=1.3`. Tests: `S3CacheEndpointTest` (+2), `LakeFsIntegrationTest.duckDbReadsThePinnedCommitThroughTheReadCache`
  (second connection 0 GETs to the gateway, moved branch read at its new commit, lakefs metrics; needs httpfs: CI).
  Local: s3 58 / lakefs 15 + checkstyle + spotbugs green, server LakeFs/S3/plugin/metrics tests 25 green. Package run
  40525341020 (push run 40525325992 cancelled in its favour): build + package green (server 784 / 2 skipped, the new
  lakeFS IT ran), pre-release `server-1.0.0-752e272b3ab6`. **Stand** (optional, with the item 17 round):
  lakeFS SF1 per commit with `arrowflight.lakefs.cacheMegabytes=1024` vs off. Next: item 15 stand + close; then item 16 / 19
  (17 (a) waits for the local-disk question).
- 2026-10-27 (cloud session): item 15 still waits for the stand (code complete), 16 is a stand measurement (S4 open
  question), 17 (a) waits for the local-disk question, 18 for node-to-node calls, so item 19 (04e5580): size-aware
  `duckDbThreads` default (approved in stage 2). Unset or 0 = `availableProcessors / 4`, clamped to 2..8 (container
  CPU limits count: Java 21 reads the cgroup quota); explicit values unchanged, negative refused at startup; bundled
  `arrowflight.properties` leaves it empty; new INFO line `duckdb=threads perConnection= availableProcessors=`. Basis:
  item 15's local c32 run (4 cores: p50 64-69 ms at 4 threads, 57 at 2) and TUNING's 4 as balanced on the stand's
  host size. Tests: `ConfigAdapterTest` (formula 1/4/11/12/16/32/128 cores, unset and 0 = formula, negative refused,
  bundled defaults). Local: ConfigAdapterTest, DuckDbAdapterTest, GoldenDuckDbSetupTest + checkstyle + spotbugs green.
  Package run 40536373045 (push run 40536363011 cancelled in its favour): build + package green, pre-release `server-1.0.0-04e558056068`. **Stand:** see stage 2 item 19.
  Next: item 15 stand + close; item 16 / 19 stand rounds; 17 (a) and 18 wait for answers.
- 2026-10-27 (cloud session): item 15 still waits for the stand (code complete; the priority note's two sessions
  have passed), 16/0h are stand measurements, 17 (a) and 18 wait for answers, so item 2 (iii) (4e77c6b): position
  deletes over many data files. `CatalogSources.deletesAppliedSource` wrote one UNION ALL branch per data file with
  position deletes, each re-reading all its position delete files; after a MERGE (one delete file over many data
  files) that is O(files) branches and reads. Now data files with the same delete files are one `read_parquet`
  (`file_row_number = true`), anti-joined to the position deletes read once and mapped to the list index by a
  `VALUES (normalised location, i)` join on DuckDB 1.5's `file_index` virtual column; a single file (the partial-mode
  ticket read) and tables with a column named `file_index` keep the per-file SQL. Local (DuckDB 1.5.6, 500 files x
  2000 rows, one shared position delete file, `count/sum WHERE v = 3`): prepare 700-800 -> 17-21 ms, run 1200-1240 ->
  49-53 ms, same answer. Test: `CatalogSourcesDeletesTest.manyFilesSharingTheirDeletesReadInOneBranch` (40 files,
  `file:` and bare paths, deletes naming a file outside the plan, an equality delete, a subset, the `file_index`
  fallback; ids vs Java-computed). Local: CatalogSources/Catalog/Iceberg tests 44 green + checkstyle + spotbugs.
  Package run 40544759257 (push run 40544741781 cancelled in its favour): build + package green, pre-release
  `server-1.0.0-4e77c6bda59a`. **Stand:** item 2 Stand.
  Next: item 15 stand + close; stand rounds of 16 / 19 / 2; 17 (a) and 18 wait for answers.
- 2026-10-27 (cloud session): item 15 still waits for the stand, so stage-2 item 16 (single-stream scan opt-ins as
  defaults). New `ScanThroughputPerfTest` (tag `perf`, 166a093; numbers in `docs/dev/TUNING.md`): one client, full scan
  over Flight SQL, complete mode, DuckDB 1.5.6, 6 M rows / 8 files, 4 cores, variants in both orders. Answers equal in
  every variant; `duckDbPreserveInsertionOrder=false` is slower (8 threads 234-246 vs 346-367 MB/s, 4 threads -5..-10 %,
  same 92 batches, so inside DuckDB); `duckDbStreamingBufferSize=64MiB` +5-15 % (4/16 MiB noise). No default change;
  item 16 stays open for the stand round (see item 16). CI run 40565455791 green (no runtime change, no pre-release). Local Maven: the wrapper download got HTTP 429,
  system `mvn` 3.9.11 works.
  Next: item 15 stand + close; stand rounds of 16 / 19 / 2; 17 (a) and 18 wait for answers.
- 2026-10-27 (cloud session): item 15 still waits for the stand (code complete), 16/19/2/0h are stand rounds, 17 (a) and
  18 wait for answers, so optional item 3 (a) (b7be8d1): Iceberg time travel in partial mode in parts. A partial query
  that reads one catalog table with one `FOR VERSION/TIMESTAMP AS OF` clause and nothing else (no join, subquery, CTE,
  set operation: exactly one SELECT) is planned like a current-snapshot query: tickets carry the query without its
  clause and file entries of the travelled snapshot (its plan ID pins that snapshot's schema; executors already
  accepted any snapshot ID, so ticket format and older executors are unaffected); the partial describe binds against
  that snapshot (`ExecutionService.travelledSchema`), so the stream schema equals the FlightInfo schema (it did not
  before for an unaliased `count(*)`: complete describe says `count_star()`). Anything else, refused points and
  snapshots not readable file by file keep the complete answer and its errors. Test:
  `IcebergCatalogIntegrationTest.timeTravelInPartialModeIsReadInParts` (hadoop + REST: located endpoints, answers and
  columns = complete, deletes of the old snapshot applied file by file, joins/subquery/UNION/two clauses complete).
  Local: Iceberg IT + QueryPlanner/FlightSqlProducer/ExecutionService/TimeTravel tests (145) + checkstyle + spotbugs
  green (extensions from `ci-assets`; unpack the .tar.zst with python3.13 `zstandard`, no `zstd` binary here).
  Package run 40582448194 (push run 40582437183 cancelled in its favour): build + package green, pre-release
  `server-1.0.0-b7be8d166498`. **Stand (optional):** one Trino/StarRocks read of an Iceberg table `FOR VERSION AS OF`
  in partial mode (expect one endpoint per node, same answer as complete mode).
  Next: item 15 stand + close; stand rounds of 16 / 19 / 2; 17 (a) and 18 wait for answers; optional 3 (b)/(c).
- 2026-10-27 (cloud session): stage 3 item 20 step 1 - opt-in ZSTD compression of result streams (6ca7352). New
  middleware `StreamCompression` reads `arrow-compression` per call (none/zstd; unknown values and `lz4` are
  INVALID_ARGUMENT, never ignored) or the server default `flightCompression=none`; `CompressingStreamListener` puts
  a codec-aware unloader into Flight's stream listener after it started (Flight Java has no codec hook; the
  protected `unloader` field is set by reflection, plain batches if that ever fails). Batches below
  `flightCompressionMinBytes` (64 KiB) go plain; after 4 compressed batches shrinking less than 10 % the stream goes
  plain. Arrow's codec already sends a buffer raw when it does not shrink. New dependency `arrow-compression`
  (zstd-jni excluded, parquet's 1.5.7-15 kept; commons-compress 1.28.0 now compile scope). SpotBugs needed a
  REC_CATCH_EXCEPTION exclusion on `HadoopArrowFlightServer.start` (flagged once the middleware joined the builder
  chain; the catch-all log + exit is deliberate). Measured: commons-compress LZ4_FRAME is unusable (one 8 MiB buffer
  did not finish in 2 min; the first IT run took 554 s because of it) - lz4 deferred to step (b). Tests:
  `StreamCompressionTest` (codec names, header vs default, settings, large/small batches, IPC round trip, give-up)
  and `StreamCompressionIntegrationTest` (120k rows of mixed types incl. list/struct/decimal/NULLs: zstd answers =
  plain in partial and complete mode, compressed counter moves; no header / `none` stays plain; small answers plain;
  `gzip` refused). Local: those + ConfigAdapter/MetricsService/HadoopArrowFlightServer/FlightSqlProducer tests (98)
  + checkstyle + spotbugs green. CI: package run 40587941135 failed (`LibsSetTest`: arrow-compression's
  commons-compress pulled commons-io out of the provided libs set into the jar); fixed in 85aaf46 by excluding
  commons-compress (only Arrow's LZ4 codec needs it); package run 40589347178 green, pre-release
  `server-1.0.0-85aaf4604d9a`. No local throughput numbers yet
  (no netem here): **Stand / screening:** with the pre-release, a pyarrow or ADBC client sending
  `arrow-compression: zstd` at 10 ms / 1 Gbit vs plain (MB/s, server CPU); level 1 vs 3.
  Next: item 20 (b)-(f), then item 21.
- 2026-10-27 (cloud session): item 20 step (b) done (d22cdac): LZ4 stream compression. `arrow-compression: lz4`
  (`lz4_frame`, `lz4-frame`) and `flightCompression=lz4` were refused; new `Lz4FrameCodec` (Arrow LZ4_FRAME) writes
  standard LZ4 frames (independent 4 MiB blocks, no checksums) with lz4-java's JNI compressor straight between
  direct Arrow buffers; lz4-java comes from the runtime libs set (still `provided`, LibsSetTest unchanged), `lz4` is
  refused if it is missing. Frames checked both ways against commons-compress (what Java Flight clients decode
  with; it is already on the server's compile class path) and in `StreamCompressionIntegrationTest` (partial +
  complete). Local (4 cores, opt-in `StreamCompressionPerfTest`, 8 MiB of random longs 0..999, one thread): LZ4
  ~510 MB/s compress (ratio 2.6) vs ZSTD level 1 ~330 MB/s (ratio 5.0); a first version through
  `LZ4FrameOutputStream` + byte arrays did ~320 MB/s. Push run 40596424406 cancelled in favour of package run
  40596456091 (green: build 8 min + package 2 min), pre-release `server-1.0.0-d22cdaccb4e1`. **Screening:** lz4 vs
  zstd vs plain at 10 ms / 1 Gbit with a pyarrow/ADBC client (MB/s, server CPU). Next: item 20 (c)-(f), then item 21.
- 2026-10-27 (cloud session): item 20 step (c) done (ba4139a): Flight SQL session option `arrow-compression`.
  SetSessionOptions / GetSessionOptions / CloseSession were UNIMPLEMENTED (additive change; ListActions now lists
  them). New `FlightSessions` (own store, not Arrow's `ServerSessionMiddleware`: that one fails every call with
  NOT_FOUND when the cookie is unknown - after a restart, or a cookie the JDBC driver forwards to another cluster
  node - and never expires sessions). Ours: cookie `arrow_flight_session_id`, unknown/expired/other-identity
  cookies ignored (call runs with header or server default; compression is never a correctness option),
  sessions bound to `peerIdentity()`, `flightSessionIdleSeconds` (3600) and `flightSessionMax` (10000, LRU)
  per node. Only option `arrow-compression` (none/zstd/lz4, empty = unset); others INVALID_NAME, bad values
  INVALID_VALUE. Precedence: header > session > `flightCompression`. Sessions are node-local: a DoGet on another
  node gets the header/default codec (documented in README). Tests: `FlightSessionsTest` (cookie parsing,
  identity, expiry, LRU cap, close), `StreamCompressionTest` (precedence, settings),
  `StreamCompressionIntegrationTest.sessionOptionCompressesTheSessionsReads` (Java FlightSqlClient with cookie
  middleware: zstd answers = plain in both modes, header `none` wins, `gzip` INVALID_VALUE keeps zstd, `catalog`
  INVALID_NAME, CloseSession -> plain, stale cookie does not fail). Push run 40603753406 superseded by package run
  40603766088 (green), pre-release `server-1.0.0-ba4139a2eb93`. Next: item 20 (d)-(f), then item 21.
- 2026-10-27 (plugins lane, FYI core lane): CI run 40587941135 (head 6ca7352) is red on one test only,
  `LibsSetTest.setHoldsOneVersionPerLibraryAtTheFloors` ("commons-io missing"): with 6ca7352's `arrow-compression`
  dependency the server's provided libs set (`dependency:list -DincludeScope=provided`) loses commons-io 2.16.1 (62 -> 61
  entries, checked locally with the server pom of 600eb08 vs now, same hive pom). Not fixed by the plugins lane (core
  scope). Also seen once: `S3CacheEndpointTest.aChangedObjectFailsEveryWaiterAndIsReadAgainOnce` 3 GETs instead of 2
  (run 40586746160, timing; passes locally 3/3).
- 2026-10-27 (plugins lane, FYI core lane): core commit 08966aa - plugin descriptor `share` is now a list of `hadoop`
  and `arrow`; `share=arrow` makes `PluginClassLoader` take `org.apache.arrow.*` from the server (and refuses plugin
  jars that carry Arrow classes). Additive, existing descriptors unchanged; the hive plugin uses `share=hadoop,arrow`
  for its ORC reader (P1). The S3CacheEndpointTest timing note above did not recur in run 40589347178.
- 2026-10-27 (plugins lane, FYI core lane): 9c6267c - s3 read cache (`S3CacheEndpoint.blocks`) claims a missing
  span's blocks under one lock (suspected cause of the `S3CacheEndpointTest` GET-count flake; not reproduced
  locally). Run 40594636655 green.
- 2026-10-27 (plugins lane, FYI core lane): SPI 1.4 (6884223, additive: `ReadSpec.ArrowStream`, `ArrowScan`,
  `Catalog.exportArrowScan`) and core commit e3e22bf - new `catalog/ArrowStreamScans` (per-reference DuckDB Arrow
  streams), `SpiCatalog.exportArrowScan` delegation, `CatalogSources.filesSource` refuses `ArrowStream` snapshots.
  Not wired into execution yet; next plugins-lane step touches `ExecutionService` (partial path), `QueryPlanner`
  (`shipValues`) and the complete-mode reference rewrite next to `CatalogViews.timeTravel` - small, own commits.
- 2026-10-28 (plugins lane, FYI core lane): core commit (P1 step 2 (b) part 2 (1)) - partial mode reads catalog
  tables whose snapshot is `ReadSpec.ArrowStream`: `ExecutionService.readCatalogFiles` -> `streamArrowScan`
  (`ArrowStreamScans` on the thread's DuckDB connection, closed in `finally`), new `DuckDbAdapter.streamSqlOnce`
  (prepared per run, not cached, parse check not remembered, no changed-file retry), new
  `catalog/ArrowScanRequests` (columns + predicate hints), `shipsPartitionValues` used by `QueryPlanner` and
  `ticketFiles`, describe via `CatalogViews.declaredColumnsSource`. Parquet/Iceberg paths unchanged (only
  `partialSql` extracted). No plugin returns ArrowStream yet.
- 2026-10-28 (plugins lane, FYI core lane): core commit 53c4d71 (P1 step 2 (b) part 2 (2)) - complete mode reads
  catalog tables whose snapshot is `ReadSpec.ArrowStream`: no view for them (marker in `CatalogViews.refresh`); new
  `CatalogViews.arrowStreams(sql, scans)` rewrites their schema-qualified FROM references (new
  `services/TableReferences` on `TimeTravelClause`'s tokenizer, now package-visible) after `timeTravel`; new
  `DuckDbAdapter.streamCompleteOnce` (views applied, uncached prepare, `bindParameters` extracted from
  `streamComplete`); `ExecutionService.readComplete` / `completeSchema` / `completeParameterSchema` call it.
  Without such references the SQL and the path are unchanged (`ArrowStreamScans.close` no-op without streams).
- 2026-10-28 (cloud session): item 20 steps (d) and (e) done (23e36ae, no runtime change, no pre-release).
  (e) `CompressionClientsIntegrationTest` (integration tag): an in-process server, a mixed-type table (120k rows:
  bigint, int, nullable text, decimal, double, date, timestamp, bool, list, struct) read by
  `src/test/resources/python/compression_clients.py` through pyarrow Flight (C++; Flight SQL command encoded by
  hand) and the ADBC Flight SQL driver (Go; headers as `adbc.flight.sql.rpc.call_header.*`), each with no header,
  `none`, `zstd`, `lz4`, in partial and complete mode: answers equal the client's plain answer, and per client the
  server's compressed-batch counter rises (so both really asked and decoded). dev-ci.yml installs pyarrow 26.0.0 +
  adbc-driver-flightsql/-manager 1.13.0 in a venv and passes `-Dpython.it.executable`; without it the test is
  skipped. (d) START-NOTES: compression opt-in bullet + what our patched JDBC driver needs (bundle
  arrow-compression + zstd-jni (+ commons-compress) relocated like its Arrow classes with the codec factory
  service file merged; the driver already sends unknown URL properties as headers, so `?arrow-compression=zstd`
  is enough) - for the coordinator, the driver repo is not touched here. Push run 40609399809 green (server 831
  tests, 2 skipped as before = the new test ran). Next: item 20 (f), then item 21.
- 2026-10-28 (cloud session): item 20 step (f) done (cadd1ac + b492228), item 20 closed. The buffers of a large
  compressed batch are compressed side by side by the stream's thread plus up to `flightCompressionThreads` - 1
  helper threads shared by all streams (`StreamCompression.ParallelWork`, daemon pool, idle threads end after 30 s;
  only buffers >= 128 KiB earn a helper; largest first; the stream waits for all, so no extra batch is held and the
  outbound budget (0f) is unchanged; on a codec error every buffer is released and the error is thrown). Default
  0 = one thread per core, at most 8; 1 = serial as before; batches are byte for byte the serial ones. Only clients
  that ask for compression are affected (no wire or default change for others). Tests: `StreamCompressionTest`
  (parallel batch == serial batch for zstd and lz4 and decodes; failure releases all buffers; rejected helpers;
  settings), integration tests run the parallel path by default. New perf test `CompressionThroughputPerfTest`
  (one complete-mode stream, 3 M lineitem-like rows, best of 3; Arrow MB/s, process CPU, ratio) and manual
  workflow `compression-perf.yml` (unthrottled, then `tc netem delay 10ms rate 1gbit` on lo = 20 ms RTT). Run
  40613018399 on 4 cores, throttled: plain 90 MB/s, zstd 429 MB/s (serial and parallel equal: link-bound, ~88 MB/s
  on the wire), lz4 178; unthrottled: plain 640, zstd 473 serial -> 645 parallel, lz4 187 (Java client's
  commons-compress lz4 decoder is the bottleneck). Caveat: the synthetic data compresses 4.9x; real TPC-H lineitem
  will be lower, so the screening repo's number decides the 264 MB/s goal. Locally (4 cores, no netem) zstd 209 ->
  314 MB/s. Package run 40611555433 green (push run 40611545280 superseded), pre-release `server-1.0.0-cadd1aca9c23`;
  first perf run 40611557966 failed to fork (`surefire:test` skipped the JaCoCo argLine, fixed in b492228). Next: item 21.
- 2026-10-28 (cloud session): item 21 step 1 (881b7c6). New perf test `SmallQueryThroughputPerfTest` (1/8/32 clients,
  point lookup + filtered count via prepare/GetFlightInfo/DoGet/close; q/s, p50/p95, CPU per query by thread group).
  Profiled under `taskset -c 0,1` with JFR and per-phase thread-CPU probes (temporary, not committed; async-profiler
  was not allowed in the session). Breakdown and next steps in item 21. Cut: GetFlightInfo's registry + heartbeat
  reads on a one-member cluster (`ClusterService.alone()`, local member list), the Jackson tree of the per-DoGet
  execution-path event (hand-written, byte-equal to Jackson's text - test), `Mac.getInstance` per ticket (one per
  thread), `String.format` per binary qid (same ids - test). Local A/B twice, same session, JaCoCo off: c8 159/161 ->
  181/195 q/s, c32 218/227 -> 247/254 q/s, CPU/query c8 12.3 -> 10.4 ms. No wire, setting, answer or log-text
  change. Found: per-query DuckDB threads cannot adapt to load (`lock_configuration`) - Open question "Item 21 (b)";
  `duckDbThreads=1` is +9 % at c8 locally. Push run 40618147100 cancelled in favour of package run 40618153054
  (green), pre-release `server-1.0.0-881b7c62b373` for the screening repo (suggest also a round with
  `-DduckDbThreads=1`). Next: item 21 (c)/(d), or item 22 if the screening shows the gap closed.
- 2026-10-28 (cloud session): item 21 (d), measurement only. `SmallQueryThroughputPerfTest` got
  `-Dperf.client=statement` (plain GetFlightInfo + DoGet). Local, `taskset -c 0,1`, JaCoCo off: prepared c1/c8/c32
  118/276/317 q/s (CPU 12.4/6.7/6.0 ms per query), statement 135/316/350 (10.3/5.9/5.4). JFR (jcmd-started; the
  surefire fork never dumps on exit) + a standalone DuckDB JDBC bench: the Flight executor's ~4.5 ms per query is
  DuckDB prepare + execute + Arrow export (~4 ms, same as plain DuckDB) + the describe of the new text (~0.5 ms);
  no Java hot spot left. No server change; findings and the remaining levers in Open question "Item 21 (d)".
  Test-only change, push run 40622807155 green; no pre-release (no runtime change). Next: item 22 (one scan in 4-8 parallel streams) while the screening
  measures `server-1.0.0-881b7c62b373`.
- 2026-10-28 (cloud session): item 22 step 1 (e9f0fd3). New perf test `ParallelScanPerfTest`. Found on 2 cores:
  complete mode is always one stream (parallel endpoints need partial mode); per-GB server CPU grows with parallel
  endpoints because each runs its own DuckDB with `duckDbThreads` threads (plain DuckDB JDBC: 4 instances x 2 threads
  ~830 MB/s at 2.1-2.6 s CPU/GB, x 1 thread ~1270 MB/s at 1.5 s/GB). Added opt-in `duckDbThreadsUnderLoad`
  (default 0 = off; item 21 (b) option 3): a second, low-thread DuckDB connection per worker for DoGets that start
  while running DoGets x `duckDbThreads` > cores. Standalone server pinned to 2 cores, pyarrow client on 2 other cores,
  partial mode, 16 files / 6 M rows: 4 endpoints 712 -> 792 MB/s (server CPU -13 %), 8 endpoints 579 -> 611 (-6 %),
  1 stream unchanged. Zero-copy Flight writes inconclusive (follow-up (d)). Tests: `DuckDbAdapterTest` (switch only
  under load, back after; off when 0 or >= duckDbThreads), `ConfigAdapterTest`, `ThreadsUnderLoadIntegrationTest`
  (same answers in both modes with 4 parallel endpoints). Package run 40631441911 green on 1f600b9 (my commit + a
  plugins-lane test commit), pre-release `server-1.0.0-1f600b930eda`; push run 40630875352 and first package run
  40630892523 cancelled (concurrency group). Screening: one scan in 4/8 partial-mode endpoints
  (`endpointsPerServer`) with and without `-DduckDbThreadsUnderLoad=1`. Next: the maintainer's S3 small-query p95 bug
  (bug first), then item 22 (b)-(d).
- 2026-10-28 (cloud session): maintainer's bug first, S3 small-query p95 (4bb6441). Confirmed the suspect locally:
  `SmallQueryThroughputPerfTest` got `-Dperf.store=s3` (table on the fake S3 behind the real s3 plugin, latency per
  request `-Ds3.perf.latencyMs`); 4 cores, prepared point/count queries, 2 vs 4 DuckDB threads, twice each: at 2 ms
  c1 p95 48/47 vs 34/37 ms, c8 p95 76/79 vs 66/70; at 10 ms c1 107/107 vs 63/66, c8 122/125 vs 104/99; p50 about
  equal; c32 q/s 237-246 at 2 threads vs 184-207 at 4 (in-process fake S3 + clients saturate the CPU). Fix: with
  `duckDbThreads` unset/0 and `dataDir` on s3/s3a/s3n/lakefs the default is max(4, cores / 4), at most 8; local/HDFS
  and explicit values unchanged (`ConfigAdapterTest.duckDbThreadsDefaultKeepsFourOnObjectStores`). Also: unset
  `-Dperf.threads` in that perf test now means the server's default (was the helper's 1 thread). Package run
  40635098385 green, pre-release `server-1.0.0-4bb6441569be`; push run 40635080394 cancelled (concurrency group).
  Screening: S3 small queries c8/c32 vs `7562a5bdb7b9`, local disk unchanged. New Open question "S3 thread default".
  Next: item 22 (b)-(d) per stage 3 order (or 3b item 17 (e)).
- 2026-10-28 (plugins lane, FYI core lane): core commit d5d5466 (P1 step 2 (f)) - `ArrowScanRequests.columns` no
  longer takes a multiplication's `*` for `SELECT *` (Arrow-stream tables only, i.e. Hive ORC with
  `hive.orcTables`); stars that may select columns still read every column. Test `ArrowScanRequestsTest` 7/7.
- 2026-10-28 (cloud session): item 22 (c) (db66d64). Opt-in `duckDbProducingStreams` / `DUCKDB_PRODUCING_STREAMS`
  (default 0 = no cap): a fair semaphore (`ProducingStreamGate`) that a DoGet holds only while DuckDB executes its
  query or loads its next batch, released before the client/budget wait (`streamPrepared`, i.e. every DuckDB stream,
  both result modes); a waiting stream whose client cancels ends with CANCELLED. Found on the way: the test helper's
  `QueryPlanner` ignored `endpointsPerServer`, so in-process "endpoints" runs of `ParallelScanPerfTest` and
  `ThreadsUnderLoadIntegrationTest` were one stream (item 22 step 1's numbers came from the external-server pyarrow
  runs, so they stand); fixed, the integration tests assert the split. Local (4-core box, `taskset -c 0,1`,
  `duckDbThreads=2`, in-process client sharing the 2 cores, 6 M rows / 16 files, two rounds, median MB/s): off 4 ep
  352/346, 8 ep 310/330; value 1 326/339, 335/335; value 2 326/371, 381/397 (CPU 6.2/5.8 -> 5.0/4.9 s/GB). So value
  2 (= cores / duckDbThreads) closes the 8-vs-4 gap here; value 1 is neutral. Tests: `ProducingStreamGateTest`,
  `ProducingStreamsIntegrationTest` (same answers with cap 1 in both modes, plus one client reading 4 endpoints in
  turn on one thread), `ConfigAdapterTest`. My push run 40639577758 and package dispatch 40639585513 were cancelled
  by the plugins lane's pushes (concurrency group); their package run 40641171065 on 9506620 (contains db66d64) is
  green: pre-release `server-1.0.0-9506620bb170`. Screening: one partial-mode scan in 4/8 endpoints on 2 CPUs with
  `-DduckDbProducingStreams=2` (alone and with `-DduckDbThreadsUnderLoad=1`) vs `7562a5bdb7b9`. Next: item 22 (d)
  (zero-copy writes on the DuckDB stream path), or 3b item 17 (e).
- 2026-10-28 (maintainer, stand p45f, 3 rounds x 7, pnew method): 45fc0a9e6e02 vs 27f583adccd7 same session, HDFS t4: small c1 p50 JDBC 0.87 / ADBC 0.83, c8/c32 noise, SF10 JDBC 0.90, Trino/scan equal; S3 t8 SF10 22/22 x3 (ADBC c32 53-94 errors per round = item 0g port exhaustion on 45fc, which predates 6acc9cc connection caching; unchanged vs 27f5); 3 nodes heavy4 3.6 s vs 5.2 s (57d2) -> item 0h closed; 0 FAIL in 21 rounds (q18 Trino heap NO-RESULT on 3 nodes as before).
- 2026-10-28 (cloud session): item 17 (e), S3 read cache memory and large scans (99113c9). Cause of most of the extra
  RSS: each cached 1 MiB block was one `byte[]`, a humongous G1 object occupying a whole 2 MiB region at `-Xmx3g`
  (local check: 256 MiB of 1 MiB arrays = 512 MiB heap, in 256 KiB pieces = 256 MiB), so `cacheMegabytes=1024`
  held ~2 GiB and pushed the heap to `-Xmx`; each miss GET (up to 64 blocks) was also buffered whole by the SDK
  (`getObjectAsBytes`), copied by `asByteArray` and copied again per block. Fix: blocks in 256 KiB pieces, misses
  streamed from S3 into them (a store ignoring Range still answered right - new `FakeS3.ignoreRange`). Admission:
  measured on the fake S3 (lineitem-like, 2 x 1.2 M rows, DuckDB 1.5.6, 2/4/8 threads) that request size cannot
  tell scans from small queries - scans read column chunks of 2 KiB-3 MiB, a `SELECT *` point lookup one 4.5 MiB
  row group - so no size rule; instead opt-in `arrowflight.s3.cacheAdmission` / `arrowflight.lakefs.cacheAdmission`
  = `always` (default) | `reuse` (kept on a second miss within the last N not-kept names, N = cache blocks; metric
  `*_cache_not_admitted_total`). Not measured here: SF10 timing and real RSS (no stand). Tests:
  `S3CacheEndpointTest` (pieces, reuse admission with scans through a 4-block cache, ignore-Range),
  `S3IntegrationTest.readCacheAdmissionOnReuseKeepsWhatIsReadAgain`. Package run 40645727394 green, pre-release
  `server-1.0.0-99113c98c1c3`; push run 40645716991 cancelled (concurrency group). Screening: the cache run with
  `cacheMegabytes=1024` (RSS, SF10, repeated small queries) with `cacheAdmission` always and reuse vs off. New Open
  question "Read cache admission default". Next: item 22 (d) (zero-copy writes on a quiet runner) or item 18 step 2.
- 2026-10-28 (cloud session): item 22 (d) done (78a2720): opt-in `flightZeroCopyWrites` /
  `arrowflight.flight.zeroCopyWrites` / `FLIGHT_ZERO_COPY_WRITES` (default false, strict true/false). DuckDB result
  streams (`streamPrepared`, the only DuckDB stream path) call `setUseZeroCopy(true)` before start, so Flight wraps the
  exported batch buffers instead of copying them; checked in Arrow 19 that `ArrowMessage` retains them
  (`ArrowBufRetainingCompositeByteBuf`) and DuckDB's C-data import gives every batch new buffers, so a queued batch
  stays intact while the next is computed; the outbound budget still applies (gRPC on-ready threshold counts the
  bytes either way); a queued batch now sits in DuckDB's native memory instead of Netty direct buffers. Metadata and
  SqlInfo streams keep copying (they may reuse buffers; the global `arrow.flight.enable_zero_copy_write` stays unused).
  Tests: `ConfigAdapterTest`, `DuckDbAdapterTest.streamSqlUsesZeroCopyWritesOnlyWhenSet`,
  `ZeroCopyWritesIntegrationTest` (4 endpoints, batchSize 1024, readers pausing after the first batch, mixed types incl.
  lists/structs/decimals/nulls, partial + complete, plain + zstd: answers equal to a copying server). Local A/B
  (`ParallelScanPerfTest -Dperf.zeroCopyWrites`, `taskset -c 0,1`, in-process client on the same cores, 6 M rows,
  2 rounds x 3 runs): 1 stream 191/214 -> 193/228 MB/s, 4 endpoints 258/276 -> 250/264, 8 endpoints 243/234 ->
  245/268 at 8.05/8.34 -> 7.26/7.15 s CPU/GB - only 8 endpoints gains clearly, not a quiet runner. CI: my push run
  40651074736 and package dispatch 40651083243 were cancelled by the plugins lane's package dispatch 40651658732 on
  7f308a6 (concurrency group), which includes 78a2720 and is green (build + package): pre-release
  `server-1.0.0-7f308a6cdb4d` carries this change. Screening: `-DflightZeroCopyWrites=true` vs off at 1/4/8
  endpoints (MB/s, CPU, peak RSS). Next: item 18 step 2 (distributed complete mode).
- 2026-10-28 (plugins lane, FYI): core 6af7b87 - `FlightAuthenticationProvider.groups(identity)` (default none) and
  `RangerAuthorization.union`: Ranger's groups are the auth provider's groups plus the Hadoop group mapping (built-in
  providers add none, so unchanged); `ServerModule` got a 5-arg constructor (4-arg kept). Used by flight-auth-ldap
  group search (P2 step 2). Dev CI 40656572265 green, pre-release `server-1.0.0-e70d80bb7995`.
- 2026-10-28 (cloud session): maintainer's bug first, ADBC c8 tail (`4bb6441569be` vs `7562a5bdb7b9`). Not reproduced as
  a regression locally (4 cores: in-process `SmallQueryThroughputPerfTest` under `taskset -c 0,1`, 2 rounds x prepared
  and statement: new build better at p50/p95/p99, e.g. statement c8 p95 68.2/76.9 -> 60.5/62.6 ms; external server
  pinned to 2 cores, client on the other 2: statement c8 p95 61.9/54.0 -> 50.5/50.2, p99 78.8/71.4 -> 67.3/68.1).
  Added (4843cca, 4b47b0c, test/CI only): `-Dperf.server` / `-Dperf.dataDir`, p99 and max in that perf test; manual
  workflow `small-query-perf.yml` (baseline vs ref, each alone in a 2-CPU container by quota and pinned). Run
  40665245865 (statement client, 2 rounds): under `--cpus=2` both builds show a bimodal c8 distribution (p50 11-15 ms,
  p95 59-63 ms = CFS throttling stall), head p50 0.72-0.80x, p95 0.93-0.94x, q/s +22-29 %; pinned head p95 0.87-0.91x.
  So the screening's c8 p95 1.07 is the throttle stall's spread, not a server change; `duckDbThreads=4` changes nothing
  there. New Open question "Quota tails". Dev CI push run 40661672025 on 4843cca green; push run on 4b47b0c cancelled
  by the plugins lane, their dispatch 40665594250 on 1b3a7bb (contains it) green. No pre-release (no runtime change).
  Next: item 19b (3c, maintainer's new item) or the next stage-3 item per the order above.
- 2026-10-28 (lane 3 session): item 18 step 2 (lane 3 D1) merged into `main` (738d1e7): opt-in `distributedComplete`
  (default false) - complete answers of mergeable shapes are merged on the planning node from its partial endpoints,
  read from the nodes as the client's user (signed header `x-kestrel-internal-identity`, live members only). Core
  files touched: `FlightSqlProducer.getStreamStatement` (complete branch), `ExecutionService.readArrowParts`,
  `ClusterService.internalCallIdentity/liveServerUris`, `FlightSecurity.configure`, `HadoopArrowFlightServer`,
  `ServerComponent`, `CompleteAnswerMerge` (output names; ORDER BY qualified with `merged.`). Branch CI run
  40670031954 green. Details in `docs/dev/LANE3-STATUS.md`.
- 2026-10-28 (plugins lane, FYI): P4 step 1 - Hive text tables via the ArrowStream path, opt-in `hive.textTables`
  (c258adf, plugin only; core: one comment line in `arrowflight.properties`). Its push cancelled lane 3's dispatch
  40671772031 (738d1e7, same concurrency group); the plugins dispatch 40672085389 (package=true, includes 738d1e7)
  is green, pre-release `server-1.0.0-c258adf27dc9`.
- 2026-10-28 (cloud session): item 19b step 1 done (a9e43ca): DuckDB threads and memory by host size. New
  `HostLimits` (cores, container `memory.max` (cgroup v2, else v1) or the JVM's total memory, `-Xmx`, direct limit) and
  `DuckDbSizing`: with `duckDbMemoryLimit` unset each connection gets `memory_limit` = (memory - heap - direct - 15 %)
  / `duckDbMemoryConnections` (new, `DUCKDB_MEMORY_CONNECTIONS`, default cores / threads, at least 2), at least 1 GiB,
  at most 80 % of memory, written in MiB (DuckDB 1.5.6 accepts it, `DuckDbAdapterTest`); `duckDbMemoryLimit=none` =
  DuckDB's default as before. `duckDbThreads` default: cores / 4 in 2..16 (clamp was 8; S3/lakeFS >= 4), at most one
  per GiB of the connection limit (explicit or default), never below 2 - hosts up to 32 cores keep their thread count.
  Startup line `duckdb=sizing threads= memoryLimit= coreThreads= connections= budgetMiB= cores= memoryMiB= heapMiB=
  directMiB=`; the per-connection memoryLimit log is now debug. Profiles in `DuckDbSizingTest` (2c/6g -> 2 threads,
  1024MiB; 4c/16g -> 2, ~2.8 GiB; 16c/64g -> 4; 64c/512g -> 16, ~110 GiB; 64c/24g -> 3; unknown memory -> unset).
  Not measured locally (no perf change on <= 32 cores by threads; memory cap new everywhere). Push run 40673675988
  cancelled by the package dispatch 40673683569 (build + package green), pre-release `server-1.0.0-a9e43ca5e296`.
  Screening: TPC-H SF10 + load test on the 2c/6g container vs `7562a5bdb7b9` (1 GiB per connection now; see Open
  question "Item 19b expected connections"). Next: 19b step 2 (adaptive threads, opt-in) after the screening's
  numbers, or item 17 (a).
- 2026-10-28 (lane 3 session): D2 (a) merged into `main` (8d2006c): 3-node in-JVM test of distributed complete mode
  (`DistributedClusterIntegrationTest`, `CuttingRelay`; `TestFlightServerHelper` gains `hazelcastSeeds` and
  `sharedStorage`). Test only. CI 40681311340 green on `lane/distributed`. Details in `LANE3-STATUS.md`.
- 2026-10-28 (cloud session): 19b step 2 waits for the screening's numbers, so item 17 (a) (ea55a4f): disk tier of the
  S3 / lakeFS read cache, opt-in `arrowflight.<s3|lakefs>.cacheDiskPath` (absolute) + `cacheDiskMegabytes` (both, and
  `cacheMegabytes` > 0, else refused at start). `S3DiskCache`: blocks read from S3 also go to a write-behind queue (64
  blocks, dropped when full - a cache, never a wait) and one writer thread writes them as `<n>.blk` (tmp + atomic
  move) under `<path>/<kind>-cache/` (mode 700); LRU in bytes in an in-memory index with each file's CRC-32C; a block
  missing from memory is read from its file before S3 (in `readOwn`, after the single-flight claim, so concurrent
  misses still read once) and put back in memory; a damaged/short/deleted file is a miss (`errors`), never an answer.
  Files of an earlier run deleted at start, all at close; `<path>/<kind>-cache.lock` (tryLock) refuses a second
  server; WARN `cacheDiskSmall` when the file system has less free space than the cap. `cacheAdmission=reuse` gives
  the disk its own ghost list (cap / 1 MiB names). Metrics `arrowflight_<kind>_cache_disk_*` (10), `disk...=` in
  `cacheClosed`. Tests: `S3DiskCacheTest` (6: blocks dropped from memory come from disk with 0 GETs, damaged/deleted
  files read from S3 again, cap + LRU + oversize, lock/start-empty/foreign files kept, reuse admission, settings),
  `S3IntegrationTest.theReadCacheDiskTierServesWhatTheMemoryDropped` (DuckDB, 1 MiB memory + 64 MiB disk: repeated
  scan 0 GETs, files deleted at close; CI only - extensions.duckdb.org is blocked in the session). Not measured
  (no stand here). CI: my dispatches 40681442479 / 40681896929 / 40682559133 were cancelled by other lanes' pushes
  and dispatches (shared `dev-ci-main` concurrency group); the plugins lane's package run 40682965605 on 177dfe3
  (contains ea55a4f) green: server 907 tests / 2 skipped, pre-release `server-1.0.0-177dfe3e5716`. Stand/screening:
  see Next steps item 5. Next: 19b step 2 after the screening, or the stand rounds (items 15, 16, 19).
- 2026-10-28 (lane 3 session): D2 (b) + D2 (c) part 2 merged into `main` (ffcbabf, c7e436e): `DistributedCompletePerfTest`
  (`perf`), peer bytes / wait counters, and opt-in `distributedComplete` now fails over to an endpoint's backup
  locations before the single-node fallback. Only `DistributedComplete` and its tests changed. Details in
  `LANE3-STATUS.md`.
- 2026-10-28 (cloud session): item 19b step 2 done (fd9f62f): opt-in `duckDbThreadsWhenIdle` /
  `arrowflight.duckdb.threadsWhenIdle` / `DUCKDB_THREADS_WHEN_IDLE` (default 0 = off): each Flight worker opens on first
  use a further DuckDB connection with that many threads (capped at the cores; off when not above `duckDbThreads`); a
  DoGet that enters its query slot while no other runs on the node uses it to its end; later queries get
  `duckDbThreads`, or `duckDbThreadsUnderLoad` once the cores are taken (the "fewer under concurrency" half of step 2
  already existed). Same `memory_limit` as the other connections (README: ~1 GiB per thread, set
  `duckDbMemoryLimit` to match); startup line `duckdb=threads ... whenIdle=`, `DuckDbAdapter.queriesWhenIdle()`. Tests:
  `DuckDbAdapterTest` (alone -> 4 threads, next to it -> 2, third -> underLoad 1, cap/off rules), `ConfigAdapterTest`,
  `ThreadsUnderLoadIntegrationTest` (same answers off / under load / when idle / both, partial + complete). Not
  measured (no large host here); see item 3c for the stand. CI: push run 40691933422 cancelled by the catalogs lane's
  push; its run 40692394131 and my package dispatch 40693932375 on 207b74c failed in lane 3's
  `DistributedClusterIntegrationTest` (a different cut test each time, `mergedAnswers` 1 instead of 0): since D2 (c)
  part 2 a relay cut before the peer's schema fails over to a backup location and merges, which the two cut tests
  did not allow. Test-only fix 94ad610 (either exact outcome; merged only with `failovers() > 0`), passes locally
  3/3. **Lane 3: please check the fix matches your intent.** My push cancelled lane 3's package dispatch 40691235109;
  package run 40695407602 on 94ad610 (contains c7e436e and fd9f62f) green, pre-release `server-1.0.0-94ad61021d93`.
  Next: the screening's numbers (items 21/22/19b/17 (e)), then the stand rounds; no open code item left in stage 3.
- 2026-10-28 (cloud session): stage 3 has no open code step (all wait on the screening / stand), so backlog item 13 (c)
  (3042102): DuckDB's external file cache. Measured on DuckDB 1.5.6 (httpfs over a local HTTP range server and the
  fake S3): the cache lives in the connection's buffer pool and is evicted within `memory_limit` (200 MB limit, 330 MB
  read: 93-125 MiB resident; local files are not cached, only 256-byte header ranges). So since item 19b's default
  `memory_limit` it is bounded per connection; the node may hold that limit x live connections (an idle connection
  keeps its cache until it is closed). The stand's OOM kills were on builds without a default limit. Added: startup
  line `duckdb=externalFileCache perConnection= expectedConnections= expectedMiB= memoryMiB=` when the cache is on,
  WARN `duckdb=externalFileCacheUnbounded` with `duckDbMemoryLimit=none`; README / `arrowflight.properties` / TUNING
  text. Tests: `DuckDbSizingTest`, `S3IntegrationTest.theExternalFileCacheStaysWithinTheMemoryLimit` (64 MiB limit,
  8 x 30 MB files read twice: cache <= limit, the last file re-read without GETs; 3/3 locally). Default unchanged
  (off). Note: a 64 MiB limit OOMs a 4-thread scan of 8 such files with the cache on or off, so that is DuckDB's
  operator memory, not the cache. Dev CI push run 40702582891 green. No pre-release (log lines only).
  Tip for later sessions: S3/httpfs tests run in the session: download `ci-assets` asset
  `duckdb-extensions-v1.5.6.tar.zst` with curl from the release URL, extract it with Python `zstandard` (no zstd
  binary here), set `DUCKDB_HTTPFS_EXTENSION`, build `flight-plugin-s3` (`install -DskipTests`), then run the tests
  with `-DexcludedGroups=perf`.
  Next: the screening's numbers (items 21/22/19b/17 (e)), then the stand rounds; other code candidates: item 3 (c)
  (check `iceberg_scan(..., snapshot_from_id =>)` on 1.5.6 for the refused snapshots), item 1b libs pruning (risky
  without a stand: Ranger needs Jersey).
- 2026-10-28 (maintainer, laptop stand p19b): 19b step 1 measured vs 7562a5bdb7b9 under container limits (4c/8g, 8c/16g, 16c/24g, -Xmx3g, direct 2g; ADBC + raw Flight, HDFS SF10 + S3 pair, 3 rounds each, 0 OOM, 22/22). `duckdb=sizing` shows container cores/memory. Chosen threads 2/2/3, memory_limit 1024/2201/3942 MiB. SF10 17.95/17.94/13.43 s vs 10.71/10.73/9.92 (1.35-1.7x slower); 4 threads + 1 GiB limit 10.11 s (limit neutral, RSS unchanged 3.2-3.9 GB). t8/t16 SF10 -23/-20 % but small c8/c32 p50 1.6-3x worse, RSS +27 % at t16. -> bug-first note.
- 2026-10-28 (lane 3 session): D2 (c) part 1 merged into `main` (eb82020): `DistributedTwoProcessTlsTest`
  (`integration`, two server JVMs with TLS + token provider, distributed complete answers equal DuckDB's, merged from
  both nodes; no-token and forged internal identity refused). Core change, additive and small: optional
  `dataDirShared` / `FLIGHT_DATA_DIR_SHARED` (default false) declares a local data directory a mount every node reads
  (`HadoopDataStore` / `HadoopDataStoreProvider` / `StoreRegistry`); README + CHANGELOG. Lane 3 checked 94ad610 (the
  cut tests of `DistributedClusterIntegrationTest`): it matches the intent - a cut before the peer's schema fails
  over and merges, after it falls back, and the answer stays exact either way. Package run 40715095069 green,
  pre-release `server-1.0.0-2cfa23eb62f9`. Details in `LANE3-STATUS.md`.
- 2026-10-28 (cloud session): stage 3 still waits on the screening, so backlog item 3 (c) (411e804): Iceberg time
  travel to snapshots that cannot be read file by file (deletion vectors, renamed nested fields, columns without field
  ID, other `completeReason`s) is no longer refused: `CatalogViews.travelSource` reads them with
  `iceberg_scan('<metadata>', snapshot_from_id => <id>)` (equality deletes get the all-columns wrapper as for current
  snapshots), and `requireDeclaredColumns` refuses the snapshot unless DuckDB's DESCRIBE equals the snapshot's
  declared columns (names + `typeof(CAST(NULL AS <declared>))`, nested field names included) - never an answer with
  another schema. Checked on DuckDB 1.5.6: both snapshots of the `nested` fixture (before / after `st.b` -> `st.c`)
  read with their own field names, complete and partial mode, hadoop and REST catalogs
  (`IcebergCatalogIntegrationTest`, was an assertion that the clause is refused). Found while testing: the SPI's
  `snapshotId` is `<iceberg id>.<schema id>`, so the Iceberg id is parsed out (a decimal literal would be rounded at
  schema id >= 5); regression in `CatalogViewsIcebergSourceTest`. Not covered: deletion vectors (no v3 DV fixture;
  the current-snapshot views already rely on iceberg_scan for them) - stand check in item 3. Log line
  `views=icebergSnapshotRead`. CI: my push run 40710205300 and package dispatch 40710223962 were cancelled by other
  lanes' pushes (shared concurrency group); push run 40712744776 on 2cfa23e (contains 411e804) green, the package
  dispatch 40715095069 on 2cfa23e green, pre-release `server-1.0.0-2cfa23eb62f9`. Next: the screening's numbers
  (items 21/22/19b/17 (e)), then the stand rounds; code candidates: item 3 (b) (time travel for Iceberg tables in a
  Hive Metastore - plugin + SPI `TableFormat.asOf`, overlaps the plugins lane), item 1b libs pruning (needs a stand).
- 2026-10-28 (maintainer, CI build tracking 94ad61021d93): small S3 flag - ADBC c1 p95 1.08 / p99 1.09 vs 7562a5bdb7b9 in 3/3 rounds (p50 better); load +6-7 % at peak, +18-23 % at 128 clients; scan x8 1.18. Likely the same 19b thread count (fixed in 9efe670, floor 4); the screening re-measures the next pre-release - if the S3 c1 tail stays, treat it as a bug.
- 2026-10-28 (maintainer): the 15:41Z session fixed 19b (9efe670, Dev CI 40720127635 green, tpch-perf 40722015834: default 21.82 s vs -DduckDbThreads=4 21.38 s vs pre-fix 29.48 s) but ended without a pre-release and left LOCK held; package run 40736052279 dispatched on main by the maintainer, LOCK released.
- 2026-10-28 (lane 3 session): D3 step 1 merged into `main` (a110a22) - the item 0d crash Open question as the maintainer
  decided it (local view, `max.no.heartbeat` unchanged): the planner's cluster reads go through `ClusterView` (bounded
  1 s wait, last values read, a silent member's node left out of plans until it answers or is dropped). Core files:
  `ClusterService`, `HazelcastAdapter`, new `adapters/ClusterView`. Writes to a silent member's partitions still wait
  (D3 step 2). Details in `LANE3-STATUS.md`.
- 2026-10-28 (cloud session): stage 3 still waits on the screening, so backlog item 3 (b): Iceberg time travel for
  Iceberg tables registered in a Hive Metastore. SPI 1.5 (fa3dc0c, own commit): `TableFormat.asOf(table,
  metadataLocation, point)` (default refuses), delegated by `SpiTableFormat`. Plugins (27d9b32): `IcebergTableFormat`
  plans the snapshot with `TimeTravel.find` / `schemaOf` like the iceberg catalogs; `HiveCatalog.asOf` sends Iceberg
  tables to it (a snapshot ID is tried in the listed metadata file first, then the metastore's current one; tags,
  branches, instants always fresh) and refuses other Hive tables ("only Iceberg tables of the metastore keep snapshot
  history"); hive descriptor now `spi=1.5` (it was 1.1 while already using 1.4 methods). Test:
  `HiveCatalogIntegrationTest.icebergTablesOfTheMetastoreTravelInTime` (version and timestamp = DuckDB's
  `iceberg_scan` of the earlier metadata file, complete = partial, 2 rows fewer than now; unknown snapshot and a
  Parquet Hive table refused, both modes); ran locally with the ci-assets 1.5.6 extensions (Hive 8/8, Iceberg 12/12).
  CI: my package dispatch 40737948663 on 27d9b32 failed `PluginLoadingTest.newerSpiVersionsAreRefused` (it used
  1.5 as "newer"; now MAJOR.(MINOR+1), 929b544) and once `DistributedClusterIntegrationTest.startCluster` (Hazelcast
  port already in use - a port collision, not this change; lane 3: watch for a repeat); dispatch 40739820098 on 929b544
  was cancelled by lane 3's push; lane 3's package dispatch 40741178454 on a110a22 (contains all of the above) green,
  pre-release `server-1.0.0-a110a2279437`. Note for the coordinator: the maintainer's package dispatch 40736052279 (19b
  fix) was cancelled by a plugins-lane push (shared concurrency group); `server-1.0.0-a110a2279437` contains the 19b fix
  (9efe670) - measure that one for the 19b bug note. Next: the
  screening's numbers (items 21/22/19b/17 (e)) on that pre-release, then the stand rounds; code candidate: item 1b libs
  pruning (needs a stand).
- 2026-10-28 (plugins lane, FYI): SPI 1.6 (5ea70fe, additive): `Catalog.views(schema)` (default none) and
  `CatalogView(name, sql, sources, columns, fingerprint)`; the hive plugin (8410789, descriptor `spi=1.6`) lists
  simple Hive views under the opt-in `hive.views`. The server does not read them yet; the plugins lane's next step
  (P4 views part 3, see PLUGINS-STATUS.md) expands view references in `CatalogViews` before `timeTravel`, checks
  view sources in `requireQuery`, lists views in GetTables - small core commits, announced here.
- 2026-10-28 (cloud session, core lane): every numbered core item waits on the screening or the stand, so took the
  additive half of Open question "Reservation TTL of unread endpoints" (3-node S3 finding (b)): new setting
  `flightLoadReservationSeconds` / `FLIGHT_LOAD_RESERVATION_SECONDS` (default 600 = unchanged, below 1 refused) for the
  node-load reservations of planned endpoints, also when a backup node claims one (a29a0a4; tests in
  `ClusterServiceTest`, `ConfigAdapterTest`; README "Cancelling a plan", `arrowflight.properties`). Dev CI run
  40746317500 green. No pre-release (default unchanged). Stand (optional): 3-node S3 with
  `-DflightLoadReservationSeconds=120` - abandoned plans stop skewing placement after 2 min. Next: as before (the
  screening's numbers on `server-1.0.0-a110a2279437`, then the stand rounds; code candidate 1b libs pruning).
