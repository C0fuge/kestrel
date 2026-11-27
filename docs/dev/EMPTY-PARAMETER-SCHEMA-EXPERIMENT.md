# Empty prepared parameter schema experiment

`arrowflight.experimental.shareEmptyParameterSchema` defaults to false. It shares
only a canonical empty parameter Schema and its immutable IPC ByteString. Current
authorization and dataset/parameter resolution still run on each request. Nonempty
or metadata-bearing parameter schemas and all dataset schemas are encoded normally.
The lazy constants add no map, LRU, per-schema hashing or cache lock.

The general schema encoding cache reduced sampled allocation pressure but regressed
some tails and mixed single-client CPU/query. This narrower candidate isolates the
cost of repeatedly constructing and encoding the empty parameter schema. No speed
gain is assumed. Java 21 compilation, Checkstyle and 71 focused tests passed with
the switch off and on, including wire equality, schema changes, metadata and access.

Screen the same revision with the switch off/on, with integer reuse and admission
off: three paired rounds on short queries and two on the heavy-every-eighth mixed
workload, pinned four cores, 1/8/32 clients. Preserve complete raw artifacts and
compare per-kind tails, QPS, CPU/query, RSS/GC and sampled allocation/query before
considering a default change.

## Verified result: keep as a research prototype

The [short run](https://github.com/C0fuge/kestrel/actions/runs/46038575795) and
[mixed run](https://github.com/C0fuge/kestrel/actions/runs/46038575460) passed on
`8499948a`, with this flag as the only ON/OFF difference. The
[fast CI](https://github.com/C0fuge/kestrel/actions/runs/46038575561) passed 228 tests
without skips or failures and Checkstyle, including full test-source compilation.

Paired median ON/OFF ratios (higher QPS, lower other values favor ON):

| Workload | Clients | QPS | Process CPU/query | Allocation sample weight/query |
| --- | --- | --- | --- | --- |
| Short | 1 | 1.009 | 0.989 | 0.978 |
| Short | 8 | 0.987 | 1.012 | 0.980 |
| Short | 32 | 1.007 | 0.991 | 0.978 |
| Mixed | 1 | 0.997 | 1.012 | 0.993 |
| Mixed | 8 | 0.999 | 1.000 | 0.992 |
| Mixed | 32 | 0.974 | 1.028 | 0.998 |

Short point-query p99 worsened at eight clients in all three pairs (median 1.057).
Mixed 32-client QPS, CPU/query, aggregate p99 and join p99 worsened in both pairs.
Parameter-schema serialization disappears from allocation samples with ON, while
dataset serialization remains; the net allocation reduction does not produce a
consistent performance benefit. These observations do not establish causality, but
do not justify integrating this prototype or enabling a general default.

All 306,557 raw query answers, including warmups, passed independent synthetic
row/checksum checks. All 288,270 ranked samples succeeded and 142,733 overlapping
typed answers matched. The 230 short and 174 mixed raw manifest files and both
GitHub archive digests were verified. All 16 build/preflight/ranked wrappers exited
successfully without OOM; host/container before/after snapshots were complete.
Ranked memory PSI deltas and observed CPU steal were zero. These are scoped host/cgroup
observations, not exclusive per-query resource attribution or proof that all noise
is absent. Raw archives, paired arrays, per-kind tails, GC/RSS and statistical JFR
allocation analysis are retained; native-method residency is not CPU percentage.
