# Flight SQL implementation review — 2026-11-27

This review compared protocol and lifecycle implementation choices, then implemented
and tested two concrete corrections. It does not claim a throughput improvement.
The baseline was commit `6dc55e1b` (the latest local main, including server `06d6468b`).
No existing server features, result-mode defaults, ticket formats or authorization
providers were replaced.

## Implementations examined

| Implementation, pinned revision | Relevant observation | Decision |
| --- | --- | --- |
| [GizmoSQL](https://github.com/gizmodata/gizmosql/blob/04012f3e707cd4f50e950c445ff744de6171c513/src/duckdb/duckdb_server.cpp), `04012f3e707c` | DuckDB prepared statements coordinate binding and execution; unresolved parameter-dependent schemas need special handling; query binding accepts one parameter row. | Keep our signed stateless handles and legacy cluster bindings, but ensure schema RPCs resolve the same bindings as planning. Refuse unsupported row batches before serializing them. |
| [Apache Arrow Java stateless example](https://github.com/apache/arrow-java/blob/1f461ae6f0c78e014d553ff1c93cba729c1c903f/flight/flight-sql/src/test/java/org/apache/arrow/flight/sql/example/FlightSqlStatelessExample.java), `1f461ae6f0c7` | Bound values travel in the replacement handle and are applied during execution. | Retain our existing Arrow IPC/HMAC encoding rather than copying Java object serialization or changing wire compatibility. Share binding resolution between metadata and query planning. |
| [Apache Ballista scheduler adapter](https://github.com/apache/datafusion-ballista/blob/a67ca065642fee85207377463b3f20e4d9f1a090/ballista/scheduler/src/flight_sql.rs), `a67ca065642f` | The Flight SQL backend is separate from scheduling; job status streaming has a bounded buffer and an explicit session lifecycle. | Keep our existing service boundaries, cancellation and bounded endpoint scans. This is a useful design reference, not evidence that replacing our distributed engine would be faster. |

The [Flight SQL specification](https://arrow.apache.org/docs/format/FlightSql.html)
was used to check the relation between schema metadata, prepared statements, binding
and query execution. These references informed the investigation; no external source
code was copied into the patch. Local research artifacts preserve the downloaded
sources and their pinned revision identifiers.

## Corrections

### Bound prepared statement metadata

Previously `getFlightInfoPreparedStatement` resolved parameters from either a signed
handle or the cluster's original-handle binding. `getSchemaPreparedStatement` only
read SQL and described it without values. For `SELECT ? AS v`, numeric bindings could
therefore describe the text fallback instead of the numeric column actually returned.

Both paths now use `preparedParameters`. Embedded values take precedence; original
statement IDs still use cluster-held values; unbound/plain SQL keeps null bindings.
Authorization is still checked before returning schema metadata. Partial-mode metadata
continues to use its existing path.

The new end-to-end regression sends a signed handle containing a BIGINT value through
GetSchema, GetFlightInfo and DoGet, and verifies identical schemas and the value 42.

### Reject unsupported binding rows before serialization

Previously DoPut counted rows, serialized every nonempty batch, drained the upload,
and only then rejected more than one row. It could allocate an IPC copy of an input
that the endpoint could never accept.

The existing one-row limit is now checked inside the read loop before serialization
or cluster-state mutation. The supported input set and INVALID_ARGUMENT status are
unchanged. This avoids unnecessary work on invalid uploads; it does not claim a measured
latency or memory percentage for valid queries.

## Verification

- Before the production fix, the four new producer regressions produced one failure
  and two errors; the unbound compatibility case already passed.
- After the fix, 58 focused producer/handle tests passed.
- Eight complete-answer integration tests passed through a real Flight server/client
  and DuckDB, including the numeric parameter schema regression.
- Repository-wide compile, Checkstyle and SpotBugs passed for all 15 reactor modules.
- The full default test reactor passed: 1,831 tests reported, zero failures, zero errors,
  six skipped. This includes 1,198 server tests (five skipped). The eight complete-answer
  integration tests above were run separately because the default lifecycle excludes
  the integration tag.

The full reactor also exposed an existing test-order dependency: `HiddenAttachmentsTest`
registered its synthetic attachment in the process-wide registry and never removed it.
Subsequent golden setup tests then recorded extra hiding macros. Test teardown now removes
only that synthetic entry; the production registry remains add-only and the golden file
is unchanged. Running the hidden-attachment test before the golden test passes.

The test environment resolves its container hostname to loopback for Hive's embedded
metastore. Plugin packaging is performed before the test reactor because plugin fixture
tests require the generated plugin directories and JARs.

The checks run with Java 21 in a dedicated container. Raw build logs and test reports
are retained in the private research workspace. No production/three-node cluster was
used, no release package was replaced, and no QPS/scan benchmark was run for this
correctness-focused change.

## Follow-up candidates

- Compare bounded statement scheduling and query cancellation under mixed small/heavy
  load before adopting further admission-control changes.
- Compare native Arrow batch transfer and copy/backpressure behavior under the same
  DuckDB version, workload and resource budget; preserve raw latency, throughput,
  CPU/RSS/heap/direct-memory/GC, bytes/batches, startup and error data.
- Changes to distributed planning or failover require their cluster-specific checks;
  a design similarity or a README performance claim alone is insufficient.
