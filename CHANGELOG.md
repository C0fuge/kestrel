# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Fixed
- DuckDB connections of ended gRPC threads were never closed: memory grew with every new
  executor thread. They are now closed, results are streamed instead of buffered, the DuckDB
  external file cache is off by default and the prepared-statement cache is bounded.
- All DuckDB connections spilled into the same `.tmp` directory; each now has its own.
- The schema in `FlightInfo` now always equals the schema of the stream (column order, `SUM`
  types, join column names): it is taken from DuckDB for the same SQL.
- `MIN`/`MAX` from Parquet footers returned NULL for dates and failed for decimals; the footer
  path is now used only for integer and date columns.
- `GetTables` returned table schemas that stock drivers could not read.
- A client cancel now interrupts the DuckDB query; before, it ran to its end.
- On shared storage a dead node no longer makes its files unavailable: they go to live nodes.
  Nodes beat every 3 s and are left out after 10 s (was 15 s / 45 s).
- A DuckDB statement closed by a failed run is prepared again instead of failing every later
  run of the same SQL with "Statement was closed".

### Added
- `ORDER BY`, `LIMIT` and `OFFSET` (see README for multi-node semantics).
- Prepared statements without parameters, empty key metadata, metadata endpoints without a
  fixed `0.0.0.0` location: the stock Arrow Flight SQL JDBC driver works.
- Optional access token (`authToken` / `FLIGHT_AUTH_TOKEN`).
- Complete-answer mode, the default (`resultMode`): one endpoint with the complete answer of any
  single read-only DuckDB query over views of all tables; clients ask for the old per-node parts
  with the header `x-flight-result-mode: partial` (the Trino and Spark connectors do).
- DuckDB lockdown (`duckDbLockdown`): only the data and spill directories are readable, no
  extension installs or loads by itself, settings are locked.
- Unsigned integer columns shown as the next wider signed type in complete mode
  (`widenUnsignedTypes`).
- `?` parameters of prepared statements in complete mode: the server keeps nothing and answers
  the values with a new statement handle carrying the SQL and the values; DuckDB binds them.
- Settings `duckDbStreamResults`, `duckDbExternalFileCache`, `duckDbStatementCacheSize`,
  `duckDbTempDirectory`.
- Apache-2.0 LICENSE
- `CONTRIBUTING.md`, `SECURITY.md`, `CHANGELOG.md`
- Issue and PR templates
- `dependabot.yml` for automated dependency updates
- Release workflow triggered on `v*` tags
- `CODEOWNERS` for automatic review routing
- Package renamed from `net.surpin` to `pro.surpin`

### Changed
- GitHub Actions pinned to full commit SHAs (supply-chain hardening)
- `groupId` updated from `com.example` to `pro.surpin.arrowflight`
- SonarCloud project key and organization updated to `VadimSurpin`
- README badges and clone URLs updated to `VadimSurpin/arrow-flight-server`
- `commons-cli` updated from 1.5.0 to 1.9.0

### Removed
- `.gitlab-ci.yml` (GitHub Actions is the canonical CI)

---

## [1.0.0] — 2026-07-28

Initial public release. Forked from [nsu-fit/ArrowFlight](https://github.com/nsu-fit/ArrowFlight).

### Added
- Arrow Flight SQL server running as a sidecar on each Hadoop DataNode or Ozone storage node
- Parquet footer fast-path for `COUNT(*)`, `MIN`, `MAX` with zero column I/O
- DuckDB in-process aggregation engine receiving Arrow C streams from Parquet scanner
- Hazelcast-based cluster formation and node registry (zero manual configuration)
- Size-aware file scheduler: assigns Parquet files to nodes by total bytes, TTL-cached
- Prometheus metrics endpoint with per-query execution-path tracking
- Spark DataSource V2 connector with column projection, predicate pushdown, and aggregate pushdown
- Columnar read path (`FlightArrowColumnVector`) — no row conversion in Spark
- `FlightSessionCatalog` for Spark catalog integration
- `TypeConversionHelper` with explicit Arrow↔Spark type mapping (fixes INT_8 ClassCastException)
- Dagger 2 dependency injection replacing manual wiring
- Multi-stage Docker image (`maven:3.9.9` build + `eclipse-temurin:21-jre-jammy` runtime)
- `docker-compose.yml` orchestrating 8 services: 3 Flight nodes, Spark cluster, Grafana, Prometheus
- TPC-H and TPC-DS benchmark suite (`benchmarks/benchbase-spark/`)
- GitHub Actions CI: build, lint (Checkstyle + SpotBugs), integration tests, JaCoCo coverage
- Architecture Decision Records for distribution strategy, benchmark suite, DI choice, BenchBase bridge
- Architecture documentation: query execution flow, Parquet storage model, cluster service

### Changed
- All Russian-language comments translated to English
- Package restructured into `adapters/`, `services/`, `model/`, `metrics/` sub-packages
- Substrait filter pushdown replaced by DuckDB JDBC execution
- `HadoopFlightSqlService` God class decomposed into `FlightSqlProducer`, `ExecutionService`, `QueryPlanner`, `ClusterService`
- Java version bumped from 17 to 21
- Checkstyle and SpotBugs now fail the build on violations
- JaCoCo coverage reporting with SonarCloud integration

[Unreleased]: https://github.com/VadimSurpin/arrow-flight-server/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/VadimSurpin/arrow-flight-server/releases/tag/v1.0.0
