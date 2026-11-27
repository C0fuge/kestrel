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
