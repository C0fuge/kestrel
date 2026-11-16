# StarRocks access to this Flight SQL server

The Spark connector in `flight-spark` implements Spark DataSource V2. Its Java
classes cannot be loaded into StarRocks to make an external catalog. StarRocks'
[external catalog types](https://docs.starrocks.io/docs/data_source/catalog/catalog_overview/)
currently do not include Arrow Flight SQL. StarRocks'
[Arrow Flight SQL support](https://docs.starrocks.io/docs/unloading/arrow_flight/)
serves StarRocks query results to clients; it does not read another Flight SQL
server. A [native ADBC external catalog request](https://github.com/StarRocks/starrocks/issues/69868)
is open.

## Native connector proposal

Initial Java and C++ connector components now live in
[`flight-starrocks`](../../flight-starrocks/README.md). They implement metadata
access and streaming batch reads; the StarRocks source integration described
below remains necessary.

An inbound Flight SQL catalog needs changes in StarRocks itself. The existing
[JDBC connector](https://github.com/StarRocks/starrocks/blob/main/fe/fe-core/src/main/java/com/starrocks/connector/jdbc/JDBCConnector.java)
is a useful model for catalog registration and metadata discovery, while the
[connector manager](https://github.com/StarRocks/starrocks/blob/main/fe/fe-core/src/main/java/com/starrocks/connector/ConnectorMgr.java)
registers supported types. The implementation would need:

1. A new catalog type with connection, TLS, and authentication properties. The
   FE would use Flight SQL metadata APIs to enumerate schemas and tables and
   map Arrow types to StarRocks types.
2. A scan path that plans remote SQL and Flight endpoints in the FE, streams
   Arrow batches from this server in the BE/CN, and converts them to StarRocks
   chunks. Start with full table scans; add projection, filter, and limit
   pushdown after correctness tests.
3. Authentication propagation from each StarRocks scan to this server. Ranger
   rules on this server remain authoritative for each Flight request. Avoid a
   shared privileged identity that would bypass per-user policies.
4. StarRocks integration tests for schema discovery, data types, distributed
   reads, failure/retry behavior, TLS, and denied access.

The helper JAR in this repository does not register a catalog in StarRocks;
that requires StarRocks source changes and deployment. A prototype could also try the
[Apache Arrow Flight SQL JDBC driver](https://arrow.apache.org/docs/14.0/java/flight_sql_jdbc_driver.html)
with a [StarRocks JDBC catalog](https://docs.starrocks.io/docs/data_source/catalog/jdbc_catalog/).
That route is unverified: StarRocks documents specific JDBC backends and
schema resolvers, so driver loading alone does not establish metadata or scan
compatibility. It would also use row-oriented JDBC conversion. A simpler route
for Parquet already registered in Hive or Iceberg is to use a corresponding
StarRocks external catalog. Another route is to read through Spark and load
results into StarRocks when a direct catalog is not required.

## Join runtime filters with the ticket

A client may append the keys of a join's build side to a statement ticket (item 51): protobuf field 15 of the
ticket's `Any` (length-delimited), next to the signed statement handle, which stays untouched. The block is
little-endian:

- `"ARF1"` (`0x41524631`): filter count (int32, at most 16), then per filter the output column name (int32 length,
  1-256 UTF-8 bytes) and its keys (int32 count, int64 each; at most 2 000 000 keys per block).
- `"ARF2"` (`0x41524632`): the same IN-filters, then StarRocks split-block bloom filters (count; per filter the
  column name, the partition count, per partition log2 of its bucket count as int32, the directory length as uint64
  and the directory; then the witness count and per witness a key as int64 and the client's answer as uint8). The
  server checks their layout and skips them for now: only the IN-filters are applied.

The server applies the filters to a plain scan in partial mode (no join, aggregate, ORDER BY, LIMIT, DISTINCT or
expression in the select list), on integer output columns that are plain table columns (an alias is followed; names
compare case-insensitively): up to 4096 keys as `col BETWEEN min AND max AND col IN (...)`, more keys as the range
only. The condition is ANDed to the scan's WHERE clause, so DuckDB prunes row groups and files by their statistics.
Filters on other columns or types, and every other query (complete mode included), leave the answer as it is; a
filter only removes rows the client's join drops, so the join's answer is the same with or without the block. Keys
the column type cannot hold are dropped; a filter without keys returns no row. A malformed block, or more than one
block, is refused with `INVALID_ARGUMENT`. Authorization is unchanged: the ticket's query is checked as before.
Tickets without the block are served as before.
