# Plugins and integrations lane - status

Second lane of development (see `CLAUDE.md`, "Second lane"). Basis: the maintainer's research of 2026-10-28 (summary
below). Production target: on-prem Hadoop 3.1.3 (HA, Kerberos), Hive Metastore, Ranger 2.4; BI and Spark users.

## Next steps (one per session, in this order)

> **Maintainer, 2026-10-28 15:00 - native first.** Integrations must talk Arrow Flight / Flight SQL natively (Arrow batches
> end to end, parallel endpoints, pushdown), not through a JDBC/ODBC layer, wherever the consumer allows it - as done for
> StarRocks (native catalog instead of the JDBC bridge), Trino and Spark. JDBC/ODBC stay supported for tools that only
> speak them. For P5 this means ClickHouse's native `arrowFlight` path, not its JDBC bridge.

P1. **ORC tables in the Hive catalog.** *(Done 2026-10-28; opt-in `hive.orcTables`.)* Today the Hive plugin reads only Parquet and Iceberg tables and skips ORC; most
    production Hive tables are ORC. DuckDB has no usable ORC reader (the community `orc` extension reads local files
    only, no pushdown, no 1.5.6 build). Build a Java reader: Apache `orc-core` (with `hive-storage-api`) reading from HDFS
    (Kerberos, HA nameservice) and S3 through the existing storage setup, converting stripes to Arrow and handing them to
    DuckDB as an Arrow stream / registered scan (projection, predicate pushdown via ORC search arguments, stripe
    statistics, partition columns from the Hive partition spec). Opt-in per catalog first; refuse clearly (no silent
    skip) for ORC ACID tables until P6. Tests: ORC files written in the test (all Hive types incl. decimal, timestamp,
    date, char/varchar, binary, list/map/struct), answers equal to the same data in Parquet; partitioned table; HDFS
    mini-cluster or the existing CI HDFS container. Check both partial and complete result modes. Perf: SF1 lineitem in
    ORC vs Parquet (note it in the Log).
    *Step 1 done 2026-10-27:* `OrcArrowReader` (hive plugin) - one ORC file -> Arrow batches (an `ArrowReader`, so
    `Data.exportArrayStream` can hand it to DuckDB), read through the store's `ReadableFile` via `ReadableFileSystem`;
    tests `OrcArrowReaderTest` (all types + nulls + nested, by name / by position, projection, SARG skipping).
    Dependencies: orc-core/orc-format `shaded-protobuf` (the test metastore needs protobuf 2.5), hive-storage-api 2.8.1
    (now compile; the test metastore runs on it), Arrow and Hadoop `provided`.
    *Step 2 (a) done 2026-10-27:* descriptor `share` is a list of `hadoop`/`arrow`; `share=arrow` takes
    `org.apache.arrow.*` from the server and refuses plugin jars carrying Arrow classes (core commit 08966aa, test
    `PluginSharingTest`); hive descriptor `share=hadoop,arrow` (df0328d) - `HiveCatalogIntegrationTest` passes with
    the server's Hadoop and Arrow (locally with a UTF-8 locale: its partition path has non-ASCII characters).
    *Step 2 (b) part 1 done 2026-10-27:* SPI 1.4 (6884223): `ReadSpec.ArrowStream(format)`, `ArrowScan(columns,
    predicates)` (columns non-empty - DuckDB refuses a stream without columns, "must have at least one column";
    predicates are hints, the server keeps every filter above the stream), `Catalog.exportArrowScan(snapshot, files,
    scan, streamAddress, Object allocator)` (default refuses; partition columns come as UTF-8 text, the server
    casts). Core e3e22bf: `ArrowStreamScans` (per query: one `ArrowArrayStream` + `registerArrowStream` view per table
    reference, `DROP VIEW` + release on close; test `ArrowStreamScansTest` 5/5: self-join over two streams, partition
    text -> INTEGER, unread and failed streams released with 0 bytes leaked), `SpiCatalog` delegation,
    `CatalogSources.filesSource` refuses ArrowStream snapshots (so complete-mode views skip such tables with a log
    line, partial queries fail clearly). Checked by experiment on duckdb_jdbc 1.5.6.0: `registerArrowStream` =
    `CREATE OR REPLACE TEMP VIEW` over a one-shot stream - a second read (second statement, second reference,
    even a re-prepare) fails "This stream has been released"; a view over it can be read once. duckdb_jdbc 1.5.6
    also has Java table functions (`DuckDBTableFunction`), considered and rejected: projection pushdown only, no
    filter pushdown, per-value JNI writes, no LIST/MAP/STRUCT/BLOB.
    *Step 2 (b) part 2 (1) done 2026-10-28:* partial mode reads ArrowStream snapshots (core commit, STATUS.md Log):
    `ExecutionService.streamArrowScan` (one `ArrowStreamScans` per endpoint call, closed in `finally`),
    `DuckDbAdapter.streamSqlOnce` (prepared per run, never cached, parse check not remembered, no changed-file
    retry), `catalog/ArrowScanRequests` (columns = declared columns named anywhere in the endpoint SQL, all for a
    star, the first for none; predicates = simple conditions of the WHERE's top-level AND with literals of the
    column's exact declared type - BIGINT/INTEGER/SMALLINT/TINYINT -> Long, DECIMAL -> BigDecimal, DOUBLE ->
    Double, VARCHAR, BOOLEAN, DATE, TIMESTAMP; FLOAT and others never), `ExecutionService.shipsPartitionValues`
    (planner + `ticketFiles`; partition pruning applies), describe -> `CatalogViews.declaredColumnsSource`. Tests
    `ArrowScanRequestsTest` 6/6, `ArrowStreamExecutionTest` 5/5 (whole snapshot, same text twice, count(*), group
    by partition, planner tickets -> endpoint read, outside-root refusal, describe reads nothing, failing catalog
    leaves no view/buffer).
    *Step 2 (b) part 2 (2) done 2026-10-28:* complete mode reads ArrowStream snapshots (core 53c4d71, STATUS.md
    Log): such tables get no view (marker in `CatalogViews.refresh`); `CatalogViews.arrowStreams(sql, scans)`
    (after `timeTravel`) replaces each schema-qualified reference in a FROM position (`services/TableReferences`
    on `TimeTravelClause`'s tokens: after FROM, JOIN or a comma of a FROM list; three-part names, table functions,
    literals, comments, column qualifiers left alone) by its own stream source; columns from the whole client SQL
    (all for `*`, `COLUMNS(...)`, or a whole-row use of the alias/name); predicates only when the SQL is one
    SELECT of that table alone (`singleSelect` + `ParquetQueryParser`); describe -> `declaredColumnsSource`;
    `DuckDbAdapter.streamCompleteOnce` (views applied, uncached prepare, parameters bound). Known limits: an
    unqualified name (`FROM orders`) or a reference after an ON condition's comma is not replaced and fails as a
    missing table (never an empty answer). Tests `ArrowStreamCompleteTest` 7/7, `TableReferencesTest` 4/4.
    *Step 2 (c) done 2026-10-28 (2e4d2d9):* `hive.orcTables` (default false; ORC tables were and stay skipped
    without it) -> ORC tables (`HiveTable.Kind.ORC`, serde/input format simple name containing `orc`) are shown
    with `ReadSpec.ArrowStream("orc")`; their schema IDs hash with a `hive-orc|` prefix (Parquet IDs unchanged);
    `pinnedColumns` keeps the read spec. `HiveCatalog.exportArrowScan` -> `OrcTableReader` (files one after the
    other, each opened only when the previous is done, through `Stores.locate` + `DataStore.open`, refusing files
    outside `hive.roots`; partition columns as UTF-8 text; context class loader set in `loadNextBatch` /
    `closeReadSource`; errors name the file). Hints -> `OrcSearchArguments` (integers -> LONG, DOUBLE, DECIMAL,
    STRING/VARCHAR, BOOLEAN, DATE via `java.sql.Date.valueOf`; CHAR, FLOAT, TIMESTAMP, nested, NULL literals and
    NaN left out). Found on the way: ORC evaluates a SARG leaf only on columns it reads, so a hint on a column
    outside the projection skipped nothing; `OrcArrowReader` now also reads the SARG's columns. Transactional ORC
    tables stay skipped with the log reason `transactional (ACID) ORC table: not read yet` (like ACID Parquet
    tables; a client sees no such table, never wrong rows). Tests: `HiveOrcTableTest` 6/6 (metastore; DuckDB reads
    the exported stream = the same 6000 rows computed in DuckDB SQL, all 12 types incl. decimal/date/timestamp/
    char/list/struct/map with NULLs, JVM zone America/Los_Angeles; partitions; partition-only scan; row-group
    skipping; broken / outside file), `OrcSearchArgumentsTest` 2/2.
    *Step 2 (d) done 2026-10-28 (86af8f9):* `HiveOrcIntegrationTest` (flight-server) runs the server with the
    packaged plugin (`plugins/hive`, `share=hadoop,arrow`) and `hive.orcTables=true`: ORC tables `hvorc.flat`
    (2 files) and `hvorc.items` (partitioned by region, a file per compression kind ZLIB/ZSTD/SNAPPY/LZ4/NONE, a
    NULL partition) next to Parquet tables of the same rows; whole-table fingerprints = DuckDB computing the rows;
    8 queries (star, count, group by partition, nested columns, filters on BIGINT/VARCHAR/DECIMAL/DATE, IS NULL,
    empty result) equal to Parquet in both modes (schema and rows); partition pruning (1 endpoint); joins ORC x
    Parquet, ORC x ORC, ORC self-join (complete). zstd-jni risk checked: the server's own `Zstd` (server class
    path) works before and after the plugin decodes ZSTD ORC files - no conflict. ORC files are written by
    `OrcTestFiles` (hive test fixture, shared with `HiveOrcTableTest`) in the metastore's class loader; orc-core,
    orc-format, orc-shims, aircompressor, threeten-extra, zstd-jni added to the hive module's `fixture-libs`.
    Pre-release `server-1.0.0-3fbf5b28b186` (run 40627574196, package=true).
    *Step 2 (e) measured 2026-10-28:* `HiveOrcPerfTest` (perf tag, flight-server; manual workflow
    `hive-orc-perf.yml`, inputs rows/files): lineitem-like table (16 columns, `OrcTestFiles.LINEITEM` SQL = the
    Java writer `writeLineitem`; DuckDB `tpch` not needed) as Parquet (DuckDB default snappy) and ORC zlib/zstd,
    4 files each, through the server with the packaged plugin; complete answers of ORC = Parquet (checked every
    run). SF1 (6M rows) on a 4-core runner, run 40631384599 (1f600b9), best of 3, seconds (complete / partial):
    sizes Parquet 136 MB, ORC zlib 69 MB, zstd 59 MB; count(*) 0.01 / 0.01 vs ORC 0.28-0.40 / 0.24-0.27; Q1-style
    0.15 / 0.17 vs 6.6-7.0; Q6-style 0.04 / 0.05 vs 6.6-6.9; `l_orderkey < 50000` (sorted key, SARG skips row
    groups) 0.01 vs 0.06-0.07; `SELECT *` 1.69 / 1.61 vs 7.7-8.1. So ORC works and pushdown helps, but full
    scans are ~45x slower than Parquet for aggregations (Parquet: DuckDB's parallel reader with its own
    pushdown/statistics; ORC: one single-threaded Java stream per table reference, per-value copy into Arrow).
    First fix ca47448: `USE_DECIMAL64` batches (decimals <= 18 digits decoded into longs) - Q1 at 1M rows locally
    2.0 -> 1.4 s, but at SF1 (run 40633767991) only ~5 %: Q1 6.26-6.58, Q6 6.20-6.47, `SELECT *`
    7.3-7.6 s, count(*) 0.37-0.51 - decimals are not the main cost.
    *Step 2 (f) done 2026-10-28:* profiled (JFR via `jcmd` on the surefire fork; the hive-module reader alone
    via an in-test `jdk.jfr.Recording`). Findings and fixes: (1) **the main cost** - `ArrowScanRequests` took any
    `*` for `SELECT *`, so Q1/Q6 (`l_extendedprice * l_discount`) streamed all 16 columns; a star between two
    values is now a product (core d5d5466, regression test `aProductReadsOnlyItsColumns`; stars that may select
    columns - `t.*`, `DISTINCT ON (a) *`, `* EXCLUDE`, next to a comment - still read all); (2) bulk copy of flat
    columns into Arrow (dfad426, `OrcArrowReader.copyFlat`; the per-value `setSafe` was ~60 % of the reader's
    time); (3) files of a scan decoded up to `min(4, cores)` at once, 2 batches ahead each, handed over in file
    order (cc1230c, `OrcTableReader.FileRead`, pool `hive-orc-read-*`; plugin only, no core `ArrowStreamScans`
    change needed). Also: the perf test's JVM ran with the jacoco agent and `-ea` (~40 % slower Java path, not
    Parquet's) - `hive-orc-perf.yml` now runs without them (9506620). SF1 on a 4-core runner, run 40641173349,
    best of 3, seconds (complete / partial), Parquet vs ORC zlib-zstd: count(*) 0.01 vs 0.08-0.27; Q1-style 0.12
    vs 0.72-0.84 (was 6.6); Q6-style 0.03-0.04 vs 0.36-0.45 (was 6.6); `orderkey < 50000` 0.01 vs 0.02; `SELECT *`
    1.19-1.25 vs 1.51-1.69 (was 7.7). Batch size (4) not tried.
    *Step 2 (g) done 2026-10-28 (77a8db9, 3a78772):* a file larger than a quarter of `ORC_RANGE_BYTES` (32 MiB,
    so 8 MiB) is planned from its footer (`OrcArrowReader.stripes`, on the stream's thread, once per file) into
    groups of whole stripes of at least `min(32 MiB, size / readAhead)`; each group is a read-ahead unit
    (`Reader.Options.range`, ORC reads the stripes that start in it), handed over in file and stripe order. The
    first range starts at 0 and the last is open-ended, so every stripe is read once whatever the footer or listed
    size say; an unreadable footer leaves the file one range, whose read reports the failure naming the file.
    Tests `OrcTableReaderTest` 6/6 (new: one file of >= 8 stripes, ranges per group, listed size right and short,
    read-ahead 1/3/8; a large broken file). SF1 one file per table, run 40643095396: count(*) 0.07-0.17, Q1-style
    0.73-0.83, Q6-style 0.37-0.46, `orderkey < 50000` 0.02, `SELECT *` 1.51-1.77 s (Parquet 0.01 / 0.24 / 0.08 /
    0.01 / 1.23-1.30) = the 4-file numbers of step 2 (f). Batch size: 4096 / 8192 / 16384 rows tried locally
    (2M rows, 4 files, 4 cores) - differences within run-to-run noise (Q1 0.41-0.53 / 0.45-0.68 / 0.40-0.60 s),
    kept 4096. The rest of the gap to Parquet (~3-10x on aggregations) is one Arrow stream per table reference
    consumed by DuckDB (no parallel scan of a stream) plus ORC decoding; closing it would need several streams per
    table reference (a union of streams, core `ArrowStreamScans`) - not planned; revisit if production queries
    need it. **P1 closed.**
P2. **Login for generic clients: LDAP/AD and JWT (OIDC/Knox) auth providers.** JDBC/ODBC/ADBC and BI tools can send only
    user/password or a bearer token; with Ranger on, today they need Kerberos. Add auth providers (plugin modules behind
    the existing auth SPI): LDAP bind (ldaps, StartTLS, user DN template or search, group lookup), JWT verification
    (JWKS URL with cache, issuer/audience, user and groups claims). The resulting identity must flow to Ranger checks
    exactly like a Kerberos principal. Tests: OpenLDAP container and a local JWKS in CI; negative cases (bad password,
    expired token, wrong audience). Document in START-NOTES. Wire behaviour: new providers only, existing ones unchanged.
    *Step 1 done 2026-10-28 (LDAP login):* module `flight-auth-ldap` (after `flight-server`, which it takes as
    `provided` and imports as BOM for gRPC/Netty versions; runs on the server class path, `auth/ldap/*`, not in the
    plugin loader - the auth SPI is `FlightAuthenticationProviderFactory` via ServiceLoader). `FLIGHT_AUTH_PROVIDER=
    ldap`: Basic user:password -> UnboundID LDAP SDK 7.0.3 simple bind (DN template, or search with optional service
    account + `FLIGHT_LDAP_USER_ATTRIBUTE`), ldaps / StartTLS with host name verification, clear text only with
    `FLIGHT_LDAP_ALLOW_PLAINTEXT`, servers tried in order; reply carries a signed session bearer (`LdapSessionToken`,
    HMAC like the Kerberos sessions, own `ldap1` version tag, `FLIGHT_LDAP_SESSION_SECRET` shared by nodes,
    `FLIGHT_LDAP_SESSION_MINUTES` default 10); outage -> UNAVAILABLE, bad credentials -> UNAUTHENTICATED; login
    names restricted to `[\p{L}\p{N}._@-]{1,256}`, empty passwords refused. Identity = login name (or attribute) ->
    Ranger via `createRemoteUser(name).getShortUserName()` (= the name) and Hadoop group mapping. Tests (25, unit,
    in-process UnboundID `InMemoryDirectoryServer` with keytool-made certs instead of an OpenLDAP container):
    `LdapDirectoryTest` 11 (template/StartTLS, search/LDAPS, ambiguous, AD-style filter, anonymous search, bad
    service account, failover + unreachable, untrusted cert, wrong host name with a positive control by IP, server
    without StartTLS, plaintext opt-in), `LdapFlightLoginTest` 3 (real TLS FlightServer via `FlightSecurity.
    fromSettings`: `authenticateBasicToken` -> bearer -> peer identity; bad/empty password, forged bearer, no
    credential; TLS required, no mixing with `FLIGHT_BEARER_TOKEN`), `LdapSettingsTest` 5, `LdapSessionTokenTest` 3,
    `LdapFlightAuthenticatorTest` 3. Docs: `docs/security/providers.md` (settings table), START-NOTES; release asset
    `flight-auth-<v>-<sha>.tar.zst` (dev-ci `package`).
    *Step 2 done 2026-10-28 (groups into Ranger):* core 6af7b87 (own commit, STATUS.md Log line):
    `FlightAuthenticationProvider.groups(identity)` (default `Set.of()`); `RangerAuthorization.union(provider,
    mapping)` = provider groups + Hadoop group mapping (decided: union; a throwing provider makes the check deny,
    `RangerAuthorization` already fails closed on runtime exceptions); wired via `SecurityProviders.authorization(env,
    groups)` and a new 5-arg `ServerModule` constructor fed by `flightSecurity.authentication()::groups` (4-arg kept).
    Plugin e70d80b: opt-in `FLIGHT_LDAP_GROUP_SEARCH_BASE`, `_FILTER` (default `(|(member={1})(uniqueMember={1})
    (memberUid={0}))`, `{0}` identity, `{1}` user DN, both escaped; filter validated at start),
    `FLIGHT_LDAP_GROUP_NAME_ATTRIBUTE` (default `cn`); `FLIGHT_LDAP_BIND_DN` now also allowed in template mode when a
    group search is set. Groups read at the login (service account if set, else as the user), cached per identity
    for the session lifetime (`LdapGroupCache`, max 10000 entries); a bearer on a node without the entry triggers one
    lookup (identity -> DN: template, or search by filter / `FLIGHT_LDAP_USER_ATTRIBUTE`; service account or
    anonymous) in the authenticator, failure -> UNAVAILABLE "LDAP group lookup failed"; > 1000 groups, no single
    entry, missing base -> failure, never fewer groups. Tests: module 35 (`LdapDirectoryTest` 15 incl. groupOfNames /
    groupOfUniqueNames / posixGroup (test schema extended with RFC 2307 posixGroup), custom filter + attribute,
    failures; `LdapGroupCacheTest` 3; `LdapFlightAuthenticatorTest` 4 incl. other-node lookup and outage;
    `LdapSettingsTest` 6; `LdapFlightLoginTest` 4 incl. groups through `FlightSecurity` + `union`), core
    `RangerAuthorizationTest` 6 (union, provider failure denies, LDAP name `alice` = Kerberos `alice@EXAMPLE.COM`
    decision). Docs: `docs/security/providers.md`, START-NOTES, CHANGELOG.
    *Step 3 done 2026-10-28 (JWT, 1b3a7bb):* module `flight-auth-jwt` (same layout as `flight-auth-ldap`: server
    `provided` + BOM import, `auth/jwt/*` on the server class path, dev-ci `package` adds `auth/jwt/` to the
    `flight-auth-...tar.zst` asset). `FLIGHT_AUTH_PROVIDER=jwt`: every call's `Bearer` JWT is verified with
    nimbus-jose-jwt 10.10 (only runtime jar; its deps are optional) - `JwtVerifier`: key set from
    `FLIGHT_JWT_JWKS_URL` (https only, `JWKSourceBuilder` cache `FLIGHT_JWT_JWKS_CACHE_MINUTES` default 15,
    re-fetch on unknown `kid` rate limited by `FLIGHT_JWT_JWKS_REFRESH_LIMIT_SECONDS` default 30, own trust store,
    512 KiB limit) or `FLIGHT_JWT_JWKS_FILE` (public parts only); `typ` absent/`JWT`/`at+jwt`; algorithms RS/PS/ES
    256-512 only (`FLIGHT_JWT_ALGORITHMS` subset; HS*/none/EdDSA refused at start); required `iss` (exact
    `FLIGHT_JWT_ISSUER`), `aud` (one of `FLIGHT_JWT_AUDIENCE`), `exp`, user claim; skew
    `FLIGHT_JWT_CLOCK_SKEW_SECONDS` default 60. Identity = `FLIGHT_JWT_USER_CLAIM` (default `sub`; same character
    rule as LDAP; a numeric `sub` is read as text by nimbus, other non-string claims refused). Groups =
    `FLIGHT_JWT_GROUPS_CLAIM` (dotted path, array or one string, > 1000 or other shapes refuse the token) stored per
    identity until that token's expiry (`JwtGroups`, max 10000) -> `groups(identity)`; unseen/expired identity
    throws -> Ranger denies. Known limit (documented): two tokens of one user with different groups at the same
    moment may see each other's groups (never another user's). Opt-in `FLIGHT_JWT_BASIC_PASSWORD`: Basic password =
    token, reply carries it as bearer. Bad token UNAUTHENTICATED, key fetch failure UNAVAILABLE. Tests 16 (unit):
    `JwtVerifierTest` 6 (RS256/ES256/at+jwt, claim choice, audiences; groups shapes/nesting/limits; expiry with
    and beyond skew, nbf, iss, aud, missing exp, bad identities; plain/HS256 with a kid/tampered/unknown kid/wrong
    key under a known kid/`typ` JOSE/garbage/algorithm subset; HTTPS JWKS with trust store, cache = 1 fetch, key
    rotation -> 1 more fetch, old key then refused; untrusted cert and unreachable URL -> unavailable),
    `JwtSettingsTest` 3, `JwtGroupsTest` 3, `JwtFlightLoginTest` 4 (real TLS FlightServer through `FlightSecurity`:
    identity + per-token groups, `RangerAuthorization.union`, Basic password, expired/foreign/forged/missing,
    TLS requirement, no mixing with `FLIGHT_BEARER_TOKEN`). Docs: `docs/security/providers.md`, START-NOTES,
    CHANGELOG. **P2 closed** (optional OpenLDAP container test: see Open questions).
P3. **(moved 2026-10-28 to its own repository - do not change `flight-spark` here; skip this item)** Spark connector for production (`flight-spark`). (a) Kerberos on YARN executors: a Spark
    `HadoopDelegationTokenProvider` that gets a server session on the driver and ships it in the job credentials (today
    every executor does its own SPNEGO and fails without a TGT); (b) build profiles for Spark 3.3/3.4 (check what Hadoop
    3.1.3 clusters usually run) and Spark 4.x (Scala 2.13, JDK 17/21); (c) a proper Spark catalog (namespaces/tables from
    the server's metadata). Look at upstream's `feat/starrocks-native-integration` branch (runtime/bloom filters, AQE
    guard) - port what is a clear win, ticket format changes go to Open questions first. Tests: Spark local mode (existing
    175), a Spark 4 job, a KDC container + keytab job.
P4. **Hive TEXTFILE/CSV, Avro and JSON tables, then simple Hive views.** DuckDB's readers over the existing HDFS/S3
    setup; SerDe properties (field/line delimiters, null format, escape, skip.header), partition columns. Views: plain
    HiveQL views whose text DuckDB can run after a small translation; refuse the rest clearly.
    *Step 1 done 2026-10-28 (c258adf, text tables):* decided to read text in the plugin (Java) through the ORC
    path (`ReadSpec.ArrowStream("text")`, both modes, partition pruning, tickets) instead of DuckDB's `read_csv`:
    Hive's lazy semantics (unreadable field -> NULL, no quoting, escapes, `\N`, positional fields, missing ->
    NULL) cannot be expressed with `read_csv` options without core SQL changes, and no SPI/core change is needed
    this way. Opt-in `hive.textTables` (default false) shows `TextInputFormat` + `LazySimpleSerDe` tables.
    `TextFormat` (serde params, table params override, as Hive merges them): `field.delim` / `serialization.format`
    (byte number or first char; ASCII only), `escape.delim`, `serialization.escape.crlf`,
    `serialization.null.format` (default `\N`), `skip.header.line.count` (per file),
    `serialization.last.column.takes.rest`; refused (table skipped, log reason): `line.delim` other than LF,
    `serialization.encoding` other than UTF-8, `skip.footer.line.count`, `timestamp.formats`, escape = delimiter.
    The format is part of the schema ID (`hive-text|<format>|...`). `TextArrowReader`: lines end at LF/CR/CRLF
    (Hadoop's LineReader), 64 MiB line limit, `.gz` (multi-member) / `.deflate` / `.bz2` (Hadoop BZip2Codec),
    `.snappy/.lz4/.zst/.lzo/.br` refused naming the file. `TextValues`: integers with blanks/sign/fraction
    (dropped), out of range NULL; booleans true/false any case; float/double as Java; decimals HALF_UP to scale,
    NULL above precision; dates `yyyy-M-d` (rest after a blank ignored), timestamps `yyyy-M-d[ |T]H:m[:s[.f]]`
    (micros), impossible days NULL; binary Base64-decoded when all bytes are Base64; text UTF-8 with U+FFFD for
    malformed bytes, VARCHAR/CHAR cut to length, CHAR right-trimmed (as the ORC reader). `TableFilesReader` (was
    `OrcTableReader`) takes a `Format` strategy (ORC: stripe ranges + SARG; text: one range per file, no hints).
    A partition whose own serde properties give another format (or another storage format) fails the query
    naming the partition (Hive reads it with its own; never silently mis-split). Primitive columns only: nested
    columns skip the table. Tests: `TextValuesTest` 4, `TextArrowReaderTest` 6, `HiveTextTableTest` 3
    (metastore: opt-in, DuckDB reads the stream as the declared types incl. a gzip file, partitions with header
    and NULL partition, partition with its own serde refused, footer/nested skipped).
    *Step 2 (a)+(b) done 2026-10-28:* (a) dd9175e `HiveTextIntegrationTest` (server, packaged plugin, real
    metastore): text tables (Hive defaults with a gzip file; partitioned, `,` + header + `''` NULL) answer like
    the same rows in Parquet, both modes, filters, GROUP BY partition, joins text/Parquet, partition pruning (1
    endpoint). (b) acb87cb nested columns: `TextFormat` separators per level (`collection.delim`, else Hive's
    misspelt `colelction.delim`, default 2; `mapkey.delim` default 3; levels 3-7 bytes 4-8; part of the schema
    ID; refused when non-ASCII, CR/LF or equal to the escape); `TextArrowReader` splits recursively as
    `LazyArray` / `LazyMap` / `LazyStruct`: array/struct use their level's separator and children level+1, a
    map entries at its level, key/value at level+1, children level+2; empty field = empty array/map; entry
    without key separator = NULL value; NULL / unreadable keys dropped, first of equal keys kept (DuckDB
    refuses both anyway); struct fields by position, missing NULL, `last.column.takes.rest` also applies to
    nested structs (Hive passes the serde params to every struct). Skipped: unions, non-primitive map keys,
    > 7 levels (Hive's `extend.nesting.levels` not read). Tests: `TextArrowReaderTest` 7 (new nested test),
    `HiveTextTableTest` 4 (DuckDB reads list/map/struct from the stream), `HiveTextIntegrationTest` 4 with
    array/struct/map columns.
    *Step 2 (c) part 1 done 2026-10-28 (OpenCSVSerde):* `CsvLayout` (opencsv 2.3 `CSVReader` per line, the
    serde's constructor choice: 3-arg when escapeChar is the default `"`, so the escape is the backslash);
    `TextFormat.csv` (+ `csv` component, in the schema ID; header lines, footer refused; table params win);
    `HiveTable.csv(sd)`, a partition with the other serde fails the query; non-string columns skip the table
    (Hive-created tables store `string` for all columns). Tests: `TextArrowReaderTest` 8 (opencsv semantics:
    quotes, doubled quotes, escapes dropped before other chars, unclosed quote drops the field, empty line,
    U+FFFD), `HiveTextTableTest` 5, `HiveTextIntegrationTest` 5 (`openCsvSerdeTablesAnswerInBothModes`).
    *Step 2 (c) part 2 done 2026-10-28 (b17866c, JsonSerDe):* read from Hive 3.1.3's own sources (hcatalog
    `JsonSerDe`, serde2 `JsonSerDe`, `HCatSchema`, `Date`/`Timestamp`, `HiveBaseChar`, Java object inspectors):
    both serdes are the same Jackson 1.9 token walk (not the newer `HiveJsonStructReader` of Hive 4). Ported
    one to one in `JsonRows` with the same parser (`jackson-core-asl` 1.9.13, new plugin dependency, no deps) so
    token quirks match: first token must be `{`, keys lower-cased and matched (hcatalog: against lower-cased
    names; serde2: against declared names), unknown keys skipped with their children, later key wins,
    hcatalog `_col<n>` keys (`_col01` fails, out of range fails; serde2 fails on them); ints via Jackson
    (`1.9` -> 1, tinyint 200 -> -56, quoted number / bool / object fail), boolean = `Boolean.valueOf(getText)`,
    string = `getText` of any token (an object gives `{` and its keys then count for the enclosing object -
    reproduced), binary UTF-8 bytes, date/timestamp Hive's LENIENT formatters copied (`2020-2-30` ->
    03-01, a date-only timestamp FAILS in Hive 3.1.3 - reproduced), decimal `HiveDecimal.create` +
    `enforcePrecisionScale` (storage-api), varchar/char cut by code points, char stripped; map keys via
    `Integer.valueOf` & co. Hive fails the query on any serde exception, so the plugin fails the read
    naming file and line (also for unprojected columns - the serde is eager). Arrow-only failures: NULL map
    key (unreadable decimal key), duplicate keys after conversion (binary keys, sub-microsecond timestamps),
    dates/timestamps out of range. `HiveTable.json(sd)` by full serde class (OpenX's `JsonSerDe` stays
    skipped), `TextFormat.json` (+ `json` component in schema ID; header lines; footer and any
    `timestamp.formats` skip the table), partitions with the other JSON serde fail the query. Tests:
    `TextArrowReaderTest` 11 (+3: every type and quirk, 23 failing lines, formats/types),
    `HiveTextTableTest` 6 (+1 metastore: hcatalog + serde2 + gzip + NULL partition, OpenX/formats skipped,
    partition with other serde), `HiveTextIntegrationTest` 6 (+1: DuckDB `to_json` rows with `S`/`_col0`/
    unknown keys = Parquet in both modes, fingerprint, join on struct/map).
    *Step 2 (c) part 3 done 2026-10-28 (deb0a99, Avro tables):* opt-in `hive.avroTables` (default false) shows
    `AvroSerDe` + `AvroContainerInputFormat` tables, read in the plugin (`ReadSpec.ArrowStream("avro")`, both modes,
    pruning) - DuckDB's `avro` extension not used: Hive's semantics (schema resolution per file, decimal scale from
    the file's schema, writer time zone) need the Java reader, and no SPI/core change is needed this way. Ported
    from Hive 3.1.3's sources (`AvroSerDe`, `AvroSerdeUtils.determineSchemaOrThrowException`, `SchemaToTypeInfo`,
    `TypeInfoToSchema`, `AvroDeserializer`, `AvroGenericRecordReader`). `AvroFormat`: reader schema =
    `avro.schema.literal` (table params over serde params, `none` = unset) else the columns' schema
    (`TypeInfoToSchema`: `baseRecord`, fields `[null, T]` default null, inner records `record_<n>`, tinyint/
    smallint -> int); its Hive types (`SchemaToTypeInfo`: `[null, T]` -> T, enum -> string, fixed -> binary,
    bytes+decimal -> decimal(p,s), int+date, long+timestamp-millis, string+char/varchar(maxLength), records
    structs, maps `map<string,..>`) must equal the metastore columns (position, name any case, type), else the
    table is skipped with a log reason (also: `avro.schema.url`, other unions, null type, recursive records -
    a record reused outside itself is fine, bad decimal/maxLength props). Schema ID `hive-avro|<schema>|...`.
    `AvroArrowReader` (Avro 1.12.0 `DataFileReader` + `GenericDatumReader.setExpected(table schema)` over the
    store's `ReadableFile` through a 1 MiB window): fields by position; strings/enums text; bytes/fixed binary;
    decimal = unscaled bytes at the scale of the file's own field schema (Hive's `fileSchema`), then
    `enforcePrecisionScale` to the column (NULL when too wide); char/varchar cut to the file schema's maxLength
    then the column's (char stripped); date days; timestamp millis UTC -> local time of the file's
    `writer.time.zone` (Hive 3.1.3 writes it; a file without it fails on its first non-NULL timestamp, see Open
    questions); a decimal/char/varchar field missing in the file schema fails (Hive does); map key text, HashMap
    order as Hive. An empty (0-byte) file has no rows. Found on the way: Avro's `DataFileStream.hasNext` returns
    false on any EOF, so a file cut inside a block was read as shorter - the reader checks
    `previousSync() == length` at the end and fails otherwise. A partition of another storage format, or (unless a
    table property names the schema) whose serde literal / columns give another schema (`Schema.equals`, docs
    ignored), fails the query naming the partition. Codecs null/deflate/snappy (snappy-java 1.1.10.7)/bzip2/
    zstandard. Dependencies: avro 1.12.0 (jackson excluded), Jackson 2.17.2 now compile (was test-scoped 2.12.0
    for the test metastore; it runs on 2.17.2), snappy-java. Tests: `AvroFormatTest` 4, `AvroArrowReaderTest` 5
    (every type + NULLs, projections incl. empty, resolution: int->long, dropped/added/defaulted fields, decimal
    at the file's scale 3 -> (10,2) rounding and NULL above precision, varchar at the file's maxLength, required
    field without default, decimal without scale; writer time zones UTC/Tokyo/none/bad; 5 codecs x 5000 rows in
    many blocks; empty, not-Avro, cut by 20/10/2 bytes), `HiveAvroTableTest` 3 (metastore: opt-in, literal and
    column tables, tinyint/url/stale skipped, DuckDB reads all 17 types, partitions with NULL partition,
    partition-only scan, partition with another schema refused).
    End to end (cdc0889, test only): `HiveAvroIntegrationTest` (server, packaged plugin, real metastore): `hvavro.flat`
    (schema as table literal; deflate + uncompressed files) and `hvavro.items` (columns' schema, partitioned by
    region with a NULL partition, zstandard) answer like the same rows in Parquet in both modes (schema and rows;
    star, count, GROUP BY partition, nested columns, filters), fingerprints = DuckDB computing the rows, partition
    pruning (1 endpoint), joins Avro x Parquet / Avro x Avro / self-join. Files written by `AvroTestFiles` (hive
    test fixture) in the metastore class loader; avro, commons-compress, commons-io added to `fixture-libs`.
    *Step 2 (c) part 4 done 2026-10-28 (8d95ce8, `avro.schema.url`):* the schema file is read through the server's
    stores when it lies under `hive.roots` (4 MiB at most), kept per location and read again when its size or
    modification time changes; HTTP URLs and files elsewhere keep the table skipped with a log reason; a literal still
    wins, as in Hive. Tests in `AvroFormatTest`, `HiveAvroTableTest`.
    *Step 2 (c) views, part 1 done 2026-10-28 (translator, not wired):* `HiveViewSql.translate(viewExpandedText,
    declared columns, database -> server schema)` turns a simple view into DuckDB SQL
    `SELECT CAST("_v0" AS <declared>) AS "<col>", ... FROM (<translated>) AS "_view"("_v0", ...)` plus the list of
    (database, table) it reads. Whitelist only: backtick identifiers -> double-quoted; tables only as `db`.`t` right
    after FROM / JOIN / a comma of a FROM list (anything else there - string literal, table function, VALUES, unknown
    or hidden database, unqualified name - refused: DuckDB would read files or table functions named in FROM), or a
    parenthesised subquery starting with SELECT; keywords SELECT DISTINCT ALL FROM WHERE GROUP BY HAVING AS AND OR NOT
    IS NULL TRUE FALSE IN BETWEEN CASE WHEN THEN ELSE END JOIN INNER LEFT RIGHT FULL OUTER CROSS ON UNION LIKE;
    operators `= <> != == < > <= >= + - ||`, `*` only in `count(*)`; LIKE only with a literal pattern, written with
    `ESCAPE '\'` (Hive's escape); functions count sum min max coalesce nvl lower/lcase upper/ucase abs year month
    day/dayofmonth; string literals '...' and "..." with escapes `\' \" \\ \n \t \r \% \_`; integer literals only.
    Refused (reason in the message): casts (Hive truncates double -> int, DuckDB rounds; text formats of doubles
    differ), `/ % div` (result types differ), `*` (decimal scale rules differ), decimal/suffixed literals (literal
    types differ), `[]` (0- vs 1-based), ORDER BY / LIMIT (NULL order differs), WITH, LATERAL VIEW, windows, other
    functions and keywords, adjacent string literals, `;`, comments left open. Tests: `HiveViewSqlTest` 3 (DuckDB runs
    the translations: filter + LIKE escape, grouping with declared types, LEFT JOIN + UNION ALL + FROM subquery + IN /
    BETWEEN, CASE / coalesce / nvl / `||` / `==` / comma join; 30+ refusals; FROM-position safety; quoting of
    identifiers with quotes and backticks).
    *Step 2 (c) views, part 2 done 2026-10-28 (5ea70fe SPI, 8410789 hive):* SPI 1.6 (own commit, additive):
    `Catalog.views(schema)` default `List.of()`, record `CatalogView(name, sql, sources, columns, fingerprint)`
    (sources/columns non-empty, copied). Hive (descriptor `spi=1.6`): opt-in `hive.views` (default false); with it
    `VIRTUAL_VIEW` tables are collected in `tablesOf` (same metastore pass, same names TTL) instead of being logged
    as skipped; `MATERIALIZED_VIEW` stays skipped (its stored rows may differ from its text). `views(schema)`
    translates each with `HiveViewSql` (schema function = prefix + database when that database is shown) and shows
    it only when every source is in `tables(schema)` or is itself a shown view (followed recursively, at most 16
    deep, a view on the current path refused = cycles); declared columns -> `ColumnSpec(name, HiveTypes.duckDb,
    nullable, no field ID)` (unsupported types skip); fingerprint = SHA-256 of `hive-view|<sql>` (first 16 bytes).
    Skips are logged once (`hive=viewSkipped view= reason=`). The server does not call `views` yet: no client-visible
    change. Tests: `HiveViewCatalogTest` 3 (metastore: off -> none; prefix `h_` -> sources/columns/fingerprints,
    the SQL runs in DuckDB over `h_hvw.people` and over the inner view; cross-database view, cast, ORC source
    without `orcTables` (shown with it), missing table, view over a skipped view, two-view cycle, union column type,
    database outside `hive.databases`).
    *Step 2 (c) views, parts 3 + 4 done 2026-10-28 (core 74cc484, 4ca22c3; test c62e0bb):* `SpiCatalog.views`
    delegates; `CatalogTables.views` drops views with invalid names or a source in another catalog.
    `CatalogViews.catalogViews(sql)` -> `Expanded(sql, sources)`: a schema-qualified view in a FROM position
    (`TableReferences`) becomes `(<view sql>) AS "<name>"` (alias kept), repeated for views over views (at most 16
    rounds, then refused); `FOR VERSION/TIMESTAMP AS OF` on a view refused; SQL without a reference in a catalog
    schema returns at once without listing views. The view map is its own 60 s cache read from the catalogs only
    (no DuckDB work, so partial queries cannot fail on a view refresh): a view named like a table of its schema is
    left out (table wins), as is one whose SQL names (in a FROM position) a table it does not declare as a source
    (keeps the Ranger check honest); a failing catalog keeps its views. Expansion runs before `timeTravel` in the
    three complete calls of `ExecutionService`. `FlightSqlProducer.requireQuery(principal, query)` (all 7 call
    sites): `authorization.requireQuery` as before plus `canReadTable` on every source of every expanded view
    (invoker rights, see Open questions); the ticket path runs the very text it checked (no second map read) and
    skips the distributed merge for views. Partial requests: `completeAnswerReason` = "the query reads a catalog
    view" -> one complete endpoint (same path as other queries endpoints cannot answer in parts). GetTables: views
    as `VIEW` when no type or `VIEW` is asked, only if the caller may read the view and its sources, schema from
    the complete describe; GetTableTypes adds `VIEW` when a catalog shows views. Tests: `CatalogViewQueryTest` 5
    (views over an Arrow-stream table: joins/subqueries/comma lists/view over view, sources listed, idempotent,
    describe, table wins, undeclared source hidden, time travel/cycle refused), `FlightSqlProducerTest` +4 (source
    denied -> UNAUTHORIZED before describe, time travel -> INVALID_ARGUMENT, ticket runs the checked text, partial ->
    complete plan, listing with type filters and denied source), `MetadataUtilsTest` +1, `HiveViewIntegrationTest` 4
    (packaged plugin, real metastore: views over ORC/Parquet/text, ORC x text join, view over view, both modes,
    declared types, GetTables/GetTableTypes, time travel refused). Not done: a Ranger end-to-end test of a denied
    view source (unit-tested in the producer only).
    *Step 2 (d) done 2026-10-28 (4690319, 5152e7f, 8216228, 80aa61a):* (1) large Avro and uncompressed text files
    are split like ORC files (inside the stream, not into endpoints - one Arrow stream per table reference stays):
    a file over 8 MiB is planned into equal byte ranges of `min(32 MiB, size / readAhead)`, decoded up to 4 at once
    and handed over in order; an Avro range reads the blocks whose preceding sync marker starts in it
    (`DataFileReader.sync` + `pastSync`, Hadoop's rule), a text range the lines that start in it (a range after
    the first starts a byte early and drops its first, partial line; `\n`, `\r`, `\r\n` ends). Compressed text
    (by suffix) and tables with `skip.header.line.count` stay one range. Tests: every split point and even range
    sizes give the whole file's rows once and in order (`TextArrowReaderTest`, `AvroArrowReaderTest`, incl. a cut
    file noticed by the last range), `SplitFilesReaderTest` through `TableFilesReader` (read-ahead 1/3/8, range
    sizes, open counts, gz and header files one range). (2) `HiveOrcPerfTest` / `hive-orc-perf.yml` also write the
    lineitem-like table as text (\001, DuckDB `COPY`) and deflate Avro (Java rows) and check their complete
    answers equal Parquet's. (3) Profile-driven speedups: Avro decodes only projected top-level fields (others
    skipped through the resolving decoder, so unresolvable files still fail; float/double and nested-record
    fields still read - a skip cannot widen promotions or follow a nested writer order; the new projection test
    caught a first version that also skipped fields inside nested records); text fast paths for plain decimals
    <= 18 digits (unscaled long), `yyyy-mm-dd` dates, all-ASCII strings (equivalence tests against the general
    parsers, 20k inputs each). SF1, 4 files per table, 4-core runner, best of 3, seconds (complete mode),
    Parquet / ORC zstd / text / Avro: run 40774271172 (before (3)): count(*) 0.01 / 0.22 / 2.80 / 4.59; Q1-style
    0.15 / 0.88 / 8.43 / 10.03; Q6-style 0.04 / 0.45 / 6.79 / 8.40; `orderkey < 50000` 0.01 / 0.02 / 3.68 / 6.11;
    `SELECT *` 1.55 / 1.95 / 14.82 / 12.22. Run 40776750625 (80aa61a, same runner speed - Parquet equal):
    count(*) 0.01 / 0.19 / 2.70 / 3.44; Q1 0.15 / 0.85 / 4.30 / 9.25; Q6 0.04 / 0.44 / 3.56 / 7.07; orderkey
    0.01 / 0.02 / 2.95 / 5.01; `SELECT *` 1.58 / 2.05 / 7.42 / 12.71 (partial mode within 5 % of complete).
    Sizes: Parquet 136 MB, text 798 MB, Avro deflate 201 MB. So text and Avro are 25-60x Parquet on aggregations
    (no pushdown or statistics, every row parsed): fine for occasional queries, not for heavy BI - converting hot
    tables to Parquet/ORC is the answer. (Run 40775865607 on 8216228 had a faster runner - not comparable.)
    *Optional, only if production needs it:* Avro is now dominated by deflate and Avro's generic decoding (profile
    it like text: JFR through an in-test `jdk.jfr.Recording`, the surefire `argLine` is not passed to the fork);
    text: the rest is line splitting and Arrow `setSafe`. A Ranger end-to-end test of a denied view source is
    still open (unit-tested in the producer only). **P4 closed.**
P5. **Plain Flight / command descriptors (ClickHouse `arrowFlight`, other generic Flight clients).** Accept a SQL text
    in a CMD `FlightDescriptor` (not wrapped in a Flight SQL command) for read queries, same auth and Ranger checks;
    test with a ClickHouse container (`arrowFlight` table function) and DuckDB's `adbc` path. This touches core request
    handling: keep it small and coordinate via `STATUS.md` Log; wire change -> Open questions first, opt-in setting.
    *Step 1 done 2026-10-28 (core 5e9ac98, test a802c41):* opt-in `plainFlightCommands` (`FLIGHT_PLAIN_COMMANDS`,
    default false = such descriptors refused as before; additive, see Open questions). ClickHouse's client read from
    its sources (`StorageArrowFlight`, `ArrowFlightSource`, `ArrowFlightConnection`, master 2026-10): GetSchema, then
    GetFlightInfo and DoGet on every endpoint over its one connection (locations ignored); descriptor
    `Path({dataset})` by default, `Command("SELECT * FROM " + dataset)` with `arrow_flight_request_descriptor_type =
    'command'`; 4 arguments / named collection -> `AuthenticateBasicToken` and the returned bearer on every call.
    Server: `PlainFlightCommands.query(descriptor)` - a command whose bytes parse as a protobuf `Any` with a Flight
    SQL type URL stays Flight SQL (null); otherwise strict UTF-8 SQL text (blank / malformed -> INVALID_ARGUMENT); a
    path = 1-3 plain identifiers (`[A-Za-z_][A-Za-z0-9_]{0,127}`, dotted or as elements) -> `SELECT * FROM a.b`
    (anything else INVALID_ARGUMENT, so a path cannot inject SQL or name a table function). `FlightSqlProducer`
    overrides `getFlightInfo` / `getSchema`: plain query -> `requireQuery` (Ranger + view sources, as Flight SQL
    statements) -> `flightInfoForQuery(..., complete)` with complete unless the client sends
    `x-flight-result-mode: partial` (a generic client concatenates endpoints, so partial answers of an aggregate
    would be wrong rows); tickets are the usual statement tickets (DoGet unchanged). Tests: `PlainFlightCommandsTest`
    3, `FlightSqlProducerTest` +2 (off -> refused without a Ranger call; Ranger denies text and path in both calls
    before describing; complete by default on a `partial` server, parts on request, Flight SQL keeps the default),
    `ConfigAdapterTest` +1, `PlainFlightCommandsIntegrationTest` 5 (Java FlightClient: aggregate -> 1 row on a
    partial-default server, path dotted/elements/SQL = whole table, GetSchema = FlightInfo schema, parts on request,
    refusals, Flight SQL unchanged, off), `GenericFlightClientsIntegrationTest` 2 (pyarrow plain Flight: path,
    SQL text, aggregate; ClickHouse 26.8.36.3 `clickhouse local` in Docker: path and command mode with a WHERE in
    the dataset; answers = DuckDB over the same files, incl. NULLs, DECIMAL, DATE). Dev CI pulls the ClickHouse
    image (failed pull skips that test). Docs: README, `arrowflight.properties`, START-NOTES, CHANGELOG.
    *Step 2 done 2026-10-28 (test only):* `ClickHouseLdapLoginTest` (flight-auth-ldap, integration, needs
    `-Dclickhouse.it.image`): a TLS Flight server through `FlightSecurity` with `FLIGHT_AUTH_PROVIDER=ldap`
    (in-memory directory, keytool certificate with SAN localhost) and a producer that answers with the caller's
    identity; ClickHouse 26.8 `clickhouse local` with a named collection (`use_basic_authentication`, `enable_ssl`,
    `ssl_ca` = the mounted PEM) reads `alice  42`: GetSchema, GetFlightInfo (path `[sales.orders]`) and DoGet all
    arrive as alice on the session bearer from the Basic handshake; a wrong password fails in ClickHouse ("Failed to
    authenticate") before any call reaches the producer. No code change was needed.
    *Next step (3):* DuckDB as a client: (a) the ADBC path is Flight SQL already (ADBC Flight SQL driver is checked by
    `CompressionClientsIntegrationTest`); (b) DuckDB's community `airport` extension speaks plain Flight
    (`airport_take_flight('grpc://host:port', 'SELECT ...')` / a path) - check in CI whether it reads plain
    descriptors (community-extensions.duckdb.org is not reachable from the cloud session, so add it to the Python
    step of Dev CI and a case to `GenericFlightClientsIntegrationTest`, skipped when the extension cannot be
    installed); it may need Airport-specific actions/metadata - if so, note it and do not add them. Then close
    P5. ClickHouse `INSERT` (DoPut) stays unsupported (read-only server; it fails as before).
    *Step 3 done 2026-10-29 (0de9086, test + docs only):* (b) DuckDB `airport` (community build, first DuckDB of
    2.0.0 / 1.5.6 / 1.4.5 with one; Dev CI step "DuckDB airport extension", `-Dairport.it.python`, skipped with a
    warning when none installs): `airport_take_flight` gets the schema of a path (dotted / elements) or SQL-text
    descriptor, but reads rows only through Airport's own `endpoints` DoAction (msgpack parameters), which the
    server does not implement -> the query fails with `Unrecognized request: endpoints`, never with wrong rows.
    `GenericFlightClientsIntegrationTest.duckdbAirportGetsSchemaButNeedsItsEndpointsAction` pins that; README and
    CHANGELOG say airport is unsupported (use DuckDB's ADBC Flight SQL path). Not added (Open questions). **P5
    closed.**
P6. **(in progress)** Ranger Hive-policy reuse with column masking / row filters (catalogs, federation, Delta, ACID moved to lane 4,
    `docs/dev/CATALOGS-STATUS.md`).
    *Plan (2026-10-29):* module `flight-auth-ranger-hive` (server class path `auth/ranger-hive/*`, like the auth
    modules), provider `FLIGHT_AUTHORIZATION_PROVIDER=ranger-hive` evaluating the cluster's **Hive** Ranger service
    (service type `hive`, resources database/table/column, ranger-plugins-common 2.8.0 from the server) for the
    Hive catalog's schemas (server schema = prefix + database). Fail closed everywhere: a table whose mask or row
    filter cannot be written exactly is denied, never read unmasked. Steps: (1) masks -> DuckDB SQL; (2) policy
    evaluator (`_any` for database visibility as Hive's `SHOW DATABASES`, `select` **per column**, row filter
    and mask lookups via `evalRowFilterPolicies` / `evalDataMaskPolicies`), tests with a real `RangerBasePlugin`
    fed with `ServicePolicies` + Ranger's own `ranger-servicedef-hive.json` (no admin); (3) core hooks (own
    commits): `FlightAuthorizationProviderFactory.create(env, providerGroups)` default (custom providers get no
    LDAP/JWT groups today), and a per-table restriction hook - the server must tell the provider the referenced
    columns (a table-level `select` check would let a column-level deny through: Ranger matches a request without
    column only against `*` policies) and their Hive types (CHAR(n) masks work on padded values), and must
    replace each reference by `(SELECT <masks> FROM t WHERE <filter>)` in both modes, views and tickets; (4)
    row filters: Hive expressions through the `HiveViewSql` whitelist (else deny); (5) end-to-end with the hive
    plugin. Wire/security design goes to Open questions before step 3 is merged.
    *Step 1 done 2026-10-29 (a1aa21f):* `HiveMask.parse(maskType, transformer)` + `sql(column, hiveType,
    duckDbType)`, ported from Hive 3.1.3's `GenericUDFMask` / `MaskShowFirstN` / `MaskShowLastN` / `MaskHash` /
    `BaseMaskUDF` sources: MASK_NULL -> typed NULL, MASK_NONE -> column; transformers `mask`,
    `mask_show_first_n`, `mask_show_last_n`, `mask_hash` with `{col}` and literal arguments (Hive's argument
    rules: int literal = code point, string = first char, '' = default, number 0-9 else 1, day 1-31 / month
    0-11 corrections, negative count 0); strings per char by Unicode category (`\p{Lu}`/`\p{Ll}`/`\p{Nd}`, a
    supplementary character = two "other" chars as in Java), show-first/last by position (a value with
    characters outside the BMP that is masked in part fails the query - Hive counts UTF-16 chars), CHAR(n)
    masked padded then shown trimmed; integers by digit arithmetic with Java overflow per width (the minimum
    value computed by `HiveMaskNumbers`, a port of the transformers); dates with fixed/kept parts
    (`mask_show_*_n` on a date gives 0000-01-01 like Hive); `mask_hash` SHA-256 hex of text, NULL for other
    types; other primitive types NULL (Hive's unsupported-type adapter); complex columns, CUSTOM, other
    functions and arguments Hive would refuse -> `IllegalArgumentException` (the provider will deny). Tests
    `HiveMaskTest` 6/6 (all mask types of Ranger's Hive service definition; DuckDB vs Hive's algorithms over
    Unicode strings, CHAR(8), tinyint..bigint incl. MIN/MAX and random values, dates incl. impossible days).
    Not wired: no runtime change.
    *Step 2 done 2026-10-29 (f455b87):* `HivePolicies(RangerBasePlugin, groups)` - read from Ranger 2.8.0's
    `RangerHiveAuthorizer` sources: `canSeeDatabase` / `canSeeTable` = `_any` access on the database / table
    resource (as Hive's SHOW DATABASES / SHOW TABLES filters; a column-level policy inside makes them visible),
    `readTable(principal, HiveTable(database, table, owner), referencedColumns)` -> `TableAccess(allowed,
    rowFilter, masks, reason)`: `select` per referenced column (lower case, as Hive), table-level `select` only
    when no column is referenced (count(*) - needs a policy on column `*`; a column-level deny is not seen by
    it, hence per-column checks); row filter = `evalRowFilterPolicies` on the table (Hive's raw expression,
    translated in step 4); masks = `evalDataMaskPolicies` per referenced column -> `HiveMask` (service
    definition transformer; MASK_NULL; CUSTOM value parsed as a mask function, null value = NULL; any other
    custom expression or a `{colType}` transformer denies the table). `{OWNER}` policies through the table owner
    (`setOwnerUser`), Ranger roles from `getRolesFromUserAndGroups`, user = UGI short name (as the core
    `RangerAuthorization`). Every failure (no identity, group lookup error, policies not loaded, evaluation
    error) denies. `hadoop-common` added as `provided`. Tests `HivePoliciesTest` 5/5 with a real
    `RangerBasePlugin` fed with `ServicePolicies` + `ranger-servicedef-hive.json` (column-level allow, table
    `*`, group policy, column deny, `{OWNER}`, row filter, mask types incl. CUSTOM and MASK_NONE, fail closed).
    Not wired: no runtime change. *Next step (3):* core hooks - write the design (provider factory with
    provider groups; per-table restriction hook giving the provider referenced columns + Hive types; reference
    replacement `(SELECT <masks> FROM t WHERE <filter>)` in both modes, views and tickets) into Open questions
    first, then the `FlightAuthorizationProviderFactory.create(env, providerGroups)` default (own core commit,
    STATUS.md Log line).

## Open questions

- (P2, optional) JWT: only one issuer per server and keys as JWKS (URL or file); Knox without a JWKS endpoint would
  need a PEM public key setting, and several issuers (e.g. Keycloak + Entra) a list - add if production needs them.
  An OpenLDAP container test in CI (`real-services`) for the LDAP provider - only if wanted (UnboundID in-memory
  directory covers the protocol paths today).

- (P4, OpenCSVSerde) tables whose metastore columns are typed (`int`, ...) are skipped: Hive reads every column of
  the serde as string, so the plugin would have to announce strings for declared ints (Trino refuses such tables,
  Hive-created ones store `string`). Read them as strings instead, or cast like Spark does? Skipped until decided.

- (P4, JsonSerDe) an empty line in a JSON table fails the query: Hive 3.1.3's serde fails on it at a file's
  start and otherwise repeats the previous row (it re-parses the reused line buffer - a Hive bug). Skip empty
  lines instead (no row), if production files have them?

- (P4, Avro) timestamps of Avro files without `writer.time.zone` metadata (written by Hive before 3.1 or by other
  tools) fail the query: Hive 3.1.3 shows them in the time zone of the HiveServer2 that reads them (or UTC with
  `hive.avro.timestamp.skip.conversion=true`), which the plugin cannot know. Add a setting (e.g.
  `hive.avroTimestampZone`, default unset = fail) if production has such files?

- (P4, views) Ranger semantics for Hive views: Hive (and Ranger's Hive plugin) let a user who may select from a view
  read it without rights on the tables it reads (definer rights). The server (since part 3, 74cc484) requires the
  view **and** every table and view it reads, for queries and for GetTables (invoker rights - stricter, never more
  access than today). Switch to Hive's semantics
  (view only) if production relies on views to hide tables?

- (P5, decided as additive - tell if not) `plainFlightCommands` accepts descriptors that today are refused
  (INVALID_ARGUMENT), only when the setting is on; Flight SQL commands are recognised by their protobuf type URL and
  unchanged. Plain answers default to complete regardless of `resultMode`, since generic clients concatenate
  endpoints. Should a path also accept quoted / non-ASCII table names (today plain identifiers only)?

- (P5, optional) DuckDB's `airport` extension needs Airport-specific DoActions (`endpoints`, msgpack-encoded,
  maybe more for catalogs) to read rows; not added per P5 (DuckDB users have the ADBC Flight SQL path). Add an
  opt-in Airport compatibility layer if DuckDB users want `airport_take_flight` / `ATTACH ... (TYPE AIRPORT)`?

- (maintainer, pending) share of ORC / Parquet / text / ACID tables in production; Spark versions and YARN keytab use;
  Ranger masking/row filters in use; AD/LDAP or Knox for BI logins; BI tools in use; Delta/Hudi/Paimon/Ozone; plan for
  DuckDB 2.0 (released 2026-10-21; community extensions move to it first).

## Log

- 2026-10-28 (maintainer): lane created; P1-P6 from the integration research.
- 2026-10-27 (plugins session): P1 step 1 - ORC file reader to Arrow in the hive plugin (`OrcArrowReader`,
  `ReadableFileSystem`, 5 unit tests); no catalog/runtime change, no core or SPI change. CI: run 40586746160 (9438f59)
  failed in the s3 plugin on a timing test (`S3CacheEndpointTest.aChangedObjectFailsEveryWaiterAndIsReadAgainOnce`, 3
  GETs instead of 2; passes locally 3/3; its re-run was cancelled by the core lane's push); run 40587941135 (6ca7352, on
  top of 9438f59): all plugin modules green incl. hive (OrcArrowReaderTest 5/5), server red only on `LibsSetTest`
  (commons-io dropped from the libs set by the core lane's `arrow-compression` dependency in 6ca7352, checked locally;
  noted in STATUS.md Log). Next session: confirm a green run once the core lane fixes LibsSetTest, then step 2.
- 2026-10-27 (plugins session): P1 step 1 confirmed green (run 40589347178 on 85aaf46). P1 step 2 (a): `share=arrow`
  in plugin descriptors (core commit 08966aa, additive; noted in STATUS.md Log) and `share=hadoop,arrow` for the hive
  plugin (df0328d); tests `PluginSharingTest` 3/3, `PluginLoadingTest` 6/6, `HiveCatalogIntegrationTest` 6/6 (+1
  iceberg skip) locally. CI: run 40593759006 (df0328d) red only on
  `S3CacheEndpointTest.aChangedObjectFailsEveryWaiterAndIsReadAgainOnce` (3 GETs instead of 2, second time after
  40586746160); fix 9c6267c - the s3 read cache claims a missing span's blocks under one lock (requests could split
  a span into two GETs; the failure's stats fit that) + stress test `concurrentMissesOfOneSpanAreReadWithOneGet`.
  Not reproduced locally (even pinned to one CPU), so the cause is suspected, not proven: if the count failure
  recurs, look again (next suspect: a reader arriving between the 412 and the version being forgotten). Run
  40594636655 (9c6267c, all modules) green. No package release: no runtime behaviour visible to
  clients changes (ORC still skipped). Step 2 (b) findings and proposal in Next steps.
- 2026-10-27 (plugins session): P1 step 2 (b) part 1 - SPI 1.4 (6884223: `ReadSpec.ArrowStream`, `ArrowScan`,
  `Catalog.exportArrowScan`) and core e3e22bf (`ArrowStreamScans`, `SpiCatalog` delegation, read_parquet refusal
  of ArrowStream snapshots; FYI line in STATUS.md Log). Local: `SpiTest` 7/7, `ArrowStreamScansTest` 5/5,
  `PluginLoadingTest` 6/6 (its SPI-version expectation now follows `SpiVersion.current()`), Checkstyle + SpotBugs
  clean. CI: run 40603510805 (e3e22bf) cancelled by the core lane's push; run 40603766088 (ba4139a, on top of
  e3e22bf) green, all modules. No package release: no client-visible change.
- 2026-10-28 (plugins session): P1 step 2 (b) part 2 (1) - partial mode wired for ArrowStream snapshots (core
  3f81556: `ArrowScanRequests`, `DuckDbAdapter.streamSqlOnce`, `ExecutionService.streamArrowScan` /
  `shipsPartitionValues`; SPI doc-only beb7349; FYI line in STATUS.md Log). Local: `ArrowScanRequestsTest` 6/6,
  `ArrowStreamExecutionTest` 5/5, `DuckDbAdapterTest` 90, `QueryPlannerTest` 58, `ExecutionServiceTest` 43,
  `HiveCatalogIntegrationTest` 7 (+1 skip), Checkstyle + SpotBugs clean. CI: run 40608463690 (31f16f5) green.
  No package release: no plugin serves ArrowStream yet, nothing client-visible changes. Next: part 2 (2), complete
  mode (see Next steps).
- 2026-10-28 (plugins session): P1 step 2 (b) part 2 (2) - complete mode over ArrowStream snapshots (core 53c4d71:
  `CatalogViews.arrowStreams`, `services/TableReferences`, `DuckDbAdapter.streamCompleteOnce`; FYI line in
  STATUS.md Log; CHANGELOG 9db19b0). Local: `ArrowStreamCompleteTest` 7/7, `TableReferencesTest` 4/4,
  `ArrowStreamExecutionTest` 5/5, `ArrowScanRequestsTest` 6/6, `DuckDbAdapterTest` 90 (+2 skip),
  `ExecutionServiceTest` 43, `QueryPlannerTest` 58, `FlightSqlProducerTest` 27, CatalogViews tests, Checkstyle +
  SpotBugs clean. CI: run 40616190366 (9db19b0, dispatched: the push's head commit was `[skip ci]`) green. No
  package release: no plugin serves ArrowStream yet, nothing client-visible changes. Next: step 2 (c), hive
  `exportArrowScan` + `hive.orcTables`.
- 2026-10-28 (plugins session): P1 step 2 (c) - hive `exportArrowScan` over ORC and `hive.orcTables` opt-in
  (2e4d2d9, plugin only; no SPI or core code change, a comment line in `arrowflight.properties`); fix: SARG columns
  are now read by `OrcArrowReader` (hints on unprojected columns were ignored by ORC). Local: hive module 29 tests
  (`HiveOrcTableTest` 6, `OrcSearchArgumentsTest` 2, `OrcArrowReaderTest` 5, `HiveCatalogTest` 11, Kerberos 5),
  Checkstyle + SpotBugs clean. CI: run 40621078865 (2e4d2d9) green, all modules. No package release yet: planned after the
  end-to-end tests of step 2 (d) (default behaviour unchanged).
- 2026-10-28 (plugins session): P1 step 2 (d) - ORC Hive tables end to end through the server (86af8f9, tests and
  test fixtures only: `HiveOrcIntegrationTest` 5/5, `OrcTestFiles`; hive `fixture-libs` gets orc-core & co.; no
  core, SPI or plugin main code change). Local: `HiveOrcIntegrationTest` 5/5, `HiveCatalogIntegrationTest` 7 (+1
  skip, UTF-8 locale), hive module 29/29 + Checkstyle + SpotBugs. CI: run 40627574196 (3fbf5b2, dispatched with
  package=true; the push's head was `[skip ci]`) green; pre-release `server-1.0.0-3fbf5b28b186` carries the
  `hive.orcTables` opt-in (default off). zstd-jni of server and plugin coexist. Next: P1 perf note (step 2 (e)).
- 2026-10-28 (plugins session): P1 step 2 (e) - perf note. `HiveOrcPerfTest` + workflow `hive-orc-perf.yml`
  (1f600b9, test only; CI run 40631441911 green; SF1 perf run 40631384599: ORC ~45x slower than Parquet on
  Q1/Q6-style aggregations, 4.7x on `SELECT *`, numbers in P1). First speed-up ca47448 (hive plugin:
  `USE_DECIMAL64` row batches, `serialize64` for fitting `HiveDecimalWritable`s; new test
  `readsDecimalsOfEveryWidthAndWidenedScales`; hive module 30/30 + Checkstyle + SpotBugs locally). CI: run
  40633748155 (ca47448) green, SF1 perf re-run 40633767991 green (~5 % gain at SF1, numbers in P1).
  No pre-release: `hive.orcTables` is opt-in and answers are unchanged; one is planned when P1 closes.

- 2026-10-28 (plugins session): P1 step 2 (f) - ORC scan speed. Profiled: Q1/Q6 streamed all 16 columns because
  `ArrowScanRequests` took a multiplication's `*` for `SELECT *` (core fix d5d5466, own commit, FYI line in
  STATUS.md Log); then bulk copy of flat columns into Arrow (dfad426) and per-file read-ahead in `OrcTableReader`
  (cc1230c, up to min(4, cores) files at once, file order kept); `hive-orc-perf.yml` without jacoco/`-ea`
  (9506620). Local: hive module 36/36 (new `OrcTableReaderTest` 4, `OrcArrowReaderTest` 8), `ArrowScanRequestsTest`
  7, ArrowStream* tests, `HiveOrcIntegrationTest` 5/5, `HiveCatalogIntegrationTest` 7 (+1 skip), Checkstyle +
  SpotBugs clean. CI: Dev CI run 40641171065 (9506620, package=true) green, pre-release `server-1.0.0-9506620bb170`
  (ORC opt-in default off; answers unchanged); push run 40640386236 (dfad426) cancelled by the newer push. SF1 perf
  runs 40640401475 (dfad426, still with jacoco: Q1 1.5, Q6 0.8, `SELECT *` 4.0 s) and 40641173349 (9506620:
  Q1 0.72-0.84 vs Parquet 0.12, Q6 0.36-0.45 vs 0.03, `SELECT *` 1.5-1.7 vs 1.2; before 6.6 / 6.6 / 7.7).
  Next: step 2 (g) stripe-range read-ahead for single-file tables, batch size, then close P1.
- 2026-10-28 (plugins session): P1 step 2 (g) - large ORC files read as stripe-group ranges ahead (77a8db9,
  3a78772; hive plugin only, no core or SPI change; CHANGELOG). Local: hive module 38/38 (`OrcTableReaderTest`
  6), Checkstyle + SpotBugs clean; local perf 2M rows (batch size 4096/8192/16384: no difference beyond noise,
  4096 kept). CI: Dev CI run 40643089615 (77a8db9) green; Dev CI run 40644229233 (36b2177, package=true) green,
  pre-release `server-1.0.0-36b2177d51b4` (ORC opt-in default off; answers unchanged); SF1 one-file perf run
  40643095396 (77a8db9) = 4-file speed. **P1 closed**; next session starts P2 (LDAP/JWT auth providers).
- 2026-10-28 (plugins session): P2 step 1 - LDAP / AD login provider, new module `flight-auth-ldap` (857af1d; no
  SPI or core code change; root `pom.xml` module line, dev-ci `package` assembles `flight-auth-<v>-<sha>.tar.zst`;
  CHANGELOG, `docs/security/providers.md`, START-NOTES). Local: module 25/25 (`LdapDirectoryTest` 11,
  `LdapFlightLoginTest` 3, `LdapSettingsTest` 5, `LdapSessionTokenTest` 3, `LdapFlightAuthenticatorTest` 3),
  Checkstyle + SpotBugs clean. CI: Dev CI run 40651658732 (7f308a6, package=true) green, pre-release
  `server-1.0.0-7f308a6cdb4d` with the `flight-auth` asset (opt-in provider; existing auth unchanged). That dispatch
  cancelled the core lane's run 40651083243 (78a2720, same concurrency group); 40651658732 includes 78a2720 and is
  green. Next: step 2 (provider groups into Ranger, small core hook), then step 3 (JWT).
- 2026-10-28 (plugins session): P2 step 2 - provider groups into Ranger. Core 6af7b87 (own commit, FYI line in
  STATUS.md Log: `FlightAuthenticationProvider.groups(identity)`, default none; Ranger groups = provider groups +
  Hadoop group mapping, a failing provider denies) and plugin e70d80b (opt-in `FLIGHT_LDAP_GROUP_SEARCH_BASE` /
  `_FILTER` / `FLIGHT_LDAP_GROUP_NAME_ATTRIBUTE`, `LdapGroupCache`; CHANGELOG, providers.md, START-NOTES). Local:
  flight-auth-ldap 35/35, `RangerAuthorizationTest` 6, `SecurityProvidersTest` 4, `FlightSecurityTest` 3,
  Checkstyle + SpotBugs clean (server and module). CI: Dev CI run 40656572265 (e70d80b, package=true) green,
  pre-release `server-1.0.0-e70d80bb7995` (group search opt-in; existing auth unchanged); push run 40656558867
  cancelled by that dispatch. Next: step 3 (JWT provider).
- 2026-10-28 (plugins session): P2 step 3 - OIDC / Knox JWT provider, new module `flight-auth-jwt` (1b3a7bb; no
  SPI or core change; root `pom.xml` module line, dev-ci `package` loops over `ldap jwt` for the `flight-auth`
  asset; CHANGELOG, `docs/security/providers.md`, START-NOTES). Local: module 16/16 (`JwtVerifierTest` 6,
  `JwtFlightLoginTest` 4, `JwtSettingsTest` 3, `JwtGroupsTest` 3), Checkstyle + SpotBugs clean. CI: Dev CI run
  40665594250 (1b3a7bb, package=true) green (build + package), pre-release `server-1.0.0-1b3a7bbad4bc` (opt-in
  provider; existing auth unchanged); push run 40665576701 cancelled by that dispatch (it also superseded the core
  lane's push run 40665228585 for 4b47b0c, which 40665594250 includes). **P2 closed**; next session starts P3
  (Spark connector: Kerberos delegation tokens on YARN executors first).
- 2026-10-28 (plugins session): P3 skipped (moved to its own repository). P4 step 1 - Hive text tables
  (`LazySimpleSerDe`) read by the hive plugin, opt-in `hive.textTables` (c258adf; plugin only, no SPI or core code
  change, a comment line in `arrowflight.properties`; CHANGELOG). Local: hive module 51 tests (new `TextValuesTest` 4,
  `TextArrowReaderTest` 6, `HiveTextTableTest` 3; `OrcTableReaderTest` 6 on the renamed `TableFilesReader`; UTF-8
  locale), Checkstyle + SpotBugs clean. CI: Dev CI run 40672085389 (c258adf, package=true; it also covers lane 3's
  738d1e7, whose dispatch 40671772031 my push cancelled - same concurrency group) green (build + package), pre-release `server-1.0.0-c258adf27dc9` (text tables opt-in, default off; existing behaviour unchanged). Next: P4 step 2 (a) end-to-end server test, then (b)-(d).
- 2026-10-28 (plugins session): P4 step 2 (a) dd9175e `HiveTextIntegrationTest` (text tables end to end through
  the server vs Parquet, both modes, pruning, joins; test only); (b) acb87cb nested text columns (array/map/struct
  at Hive's level separators, `collection.delim`/`colelction.delim`/`mapkey.delim`; unions, nested map keys, > 7
  levels skipped; text schema IDs change); (c part 1) cdfbe8d OpenCSVSerde tables via opencsv 2.3 (new plugin
  dependency, Hive 3.1.3's version; string columns only, typed ones skipped - Open question). Plugin only, no SPI or
  core code change (comment lines in `arrowflight.properties`); CHANGELOG. Local: hive module 55/55 + Checkstyle +
  SpotBugs, `HiveTextIntegrationTest` 5/5, `HiveOrcIntegrationTest` 5/5, `HiveCatalogIntegrationTest` 7 (+1 skip).
  CI: Dev CI run 40682965605 (177dfe3, package=true) green (build + package), pre-release `server-1.0.0-177dfe3e5716`
  (all behind the `hive.textTables` opt-in, default off). My push runs 40681698511 / 40682819503 and dispatch
  40682828658 were cancelled by me so as not to cancel other lanes' runs in the shared `dev-ci-main` concurrency
  group (lane runs 40681442479, 40682559133 were cancelled anyway; 40682965605 covers their commits). Next: P4 step 2
  (c) JsonSerDe, then Avro, views; (d) perf.
- 2026-10-28 (plugins session): P4 step 2 (c) part 2 - Hive JsonSerDe tables (hcatalog + serde2) under
  `hive.textTables` (b17866c; plugin only + tests, comment lines in `arrowflight.properties`; new plugin dependency
  `jackson-core-asl` 1.9.13 = Hive's parser; CHANGELOG). Local: hive module 59/59 + Checkstyle + SpotBugs,
  `HiveTextIntegrationTest` 6/6. CI: Dev CI run 40689263636 (f73de96, package=true; push head was `[skip ci]`)
  green (build + package), pre-release `server-1.0.0-881bbf73e946` (JSON tables behind the `hive.textTables` opt-in,
  default off). Next: P4 step 2 (c) Avro, then views; (d) perf.
- 2026-10-28 (plugins session): P4 step 2 (c) part 3 - Hive Avro tables under the new opt-in `hive.avroTables`
  (default false; deb0a99 plugin + unit/metastore tests, cdc0889 server end-to-end test; plugin only, no SPI or core
  code change, comment lines in `arrowflight.properties`; new plugin dependencies Avro 1.12.0 + snappy-java, Jackson
  2.17.2 now compile; CHANGELOG). No local build (Maven Central rate-limited the session: HTTP 429). CI: Dev CI run
  40712744776 (2cfa23e, push from the catalogs lane, contains both commits) green (build; all modules SUCCESS). My
  push runs 40710493366 / 40711359997 were superseded in the shared `dev-ci-main` group (and 40710493366 cancelled
  the catalogs lane's dispatch 40710223962 for 411e804 - covered by the green run above). Pre-release
  `server-1.0.0-2cfa23eb62f9` (catalogs lane's dispatch 40715095069, package=true, green; contains both commits;
  Avro tables behind the `hive.avroTables` opt-in, default off). Next: P4 `avro.schema.url`, views, perf.
- 2026-10-28 (maintainer): plugins session 16:18Z ended after 8d95ce8 (avro.schema.url, CI 40725913763 green) without updating this file or releasing LOCK-PLUGINS; lock released by the maintainer. Next session: add the Log line for 8d95ce8 and continue P4.
- 2026-10-28 (plugins session, recorded by the next session): P4 step 2 (c) part 4 - `avro.schema.url` schema files
  read through the stores under `hive.roots` (8d95ce8; plugin only, CHANGELOG). CI: run 40725913763 green.
- 2026-10-28 (plugins session): P4 step 2 (c) views part 1 - `HiveViewSql` translator of simple Hive views to DuckDB
  SQL, whitelist only, not wired into the catalog yet (5011e8d; plugin only, no SPI or core change, no runtime
  behaviour change, so no CHANGELOG line or pre-release). Local (Maven through the GCS mirror of Central, since
  `repo.maven.apache.org` answered 429): `HiveViewSqlTest` 3/3, Checkstyle + SpotBugs clean. CI: Dev CI run
  40737753002 cancelled by a later push; Dev CI run 40737948663 (27d9b32, contains 5011e8d): SPI, all plugin
  modules incl. hive (`HiveViewSqlTest`) SUCCESS; server red on two failures not from this change -
  `PluginLoadingTest.newerSpiVersionsAreRefused` (fa3dc0c bumps the SPI to 1.5, the test's "newer" version is
  presumably 1.5 now; that lane's lock is held, left to it) and `DistributedClusterIntegrationTest.startCluster`
  (Hazelcast port already in use, runner flake). Open question added: Ranger semantics for views (plan: view and its source tables). Next: views part 2
  (SPI 1.6 `Catalog.views` + `hive.views` opt-in), see Next steps.
- 2026-10-28 (plugins session): P4 step 2 (c) views part 2 - SPI 1.6 `Catalog.views` / `CatalogView` (5ea70fe, own
  commit, additive; FYI line in STATUS.md Log) and hive `hive.views` opt-in listing translatable views over shown
  tables (8410789; descriptor `spi=1.6`; comment lines in `arrowflight.properties`; CHANGELOG). No client-visible
  change (the server does not read catalog views yet). Local (GCS mirror of Central): `SpiTest` 9/9, hive module
  79/79 (new `HiveViewCatalogTest` 3; UTF-8 locale), Checkstyle + SpotBugs clean. CI: Dev CI run 40744052381
  (8410789) green (build, all modules). No pre-release (no runtime change). Next: views part 3 (core), detailed in Next steps.
- 2026-10-28 (plugins session): P4 step 2 (c) views parts 3 + 4 - queries of catalog views (core commits 74cc484
  expansion + source access check + partial -> complete, 4ca22c3 GetTables `VIEW` / GetTableTypes; FYI line in
  STATUS.md Log) and `HiveViewIntegrationTest` (c62e0bb, with docs in `arrowflight.properties` and CHANGELOG). Only
  servers with `hive.views=true` see a change. Local (GCS mirror of Central): `CatalogViewQueryTest` 5/5,
  `FlightSqlProducerTest` 31/31, `MetadataUtilsTest`, `HiveViewIntegrationTest` 4/4, Hive Text/Orc/Avro/Catalog
  integration tests green (Catalog needs `LC_ALL=C.UTF-8`), Checkstyle + SpotBugs clean. CI: push runs 40764016829,
  40764736076, 40765524515 cancelled by later pushes; Dev CI run 40766473788 (9044653, contains all three commits)
  green incl. package -> pre-release `server-1.0.0-9044653333cd` (dispatched by another lane; carries this change).
  Next: P4 (d) perf note + splitting large Avro/text files, see Next steps.
- 2026-10-28 (plugins session): P4 step 2 (d) - large Avro/uncompressed text files read as parallel byte ranges
  (4690319), text/Avro tables in the Hive perf test (5152e7f), Avro projection skip (8216228), text fast paths
  (80aa61a); plugin only (plus the perf test/workflow), no SPI or core change; CHANGELOG. Local (GCS mirror of
  Central): hive module 88/88, Checkstyle + SpotBugs clean; `HiveOrcPerfTest` at 400k rows (answers equal Parquet).
  CI: Dev CI push runs 40773505710, 40774236756, 40775370618 cancelled by later pushes; Dev CI run 40776549317
  (80aa61a, contains all four commits) green; perf runs 40774271172, 40775865607, 40776750625 (numbers under P4);
  package run 40778373787 (package=true, 80aa61a) green -> pre-release `server-1.0.0-80aa61a15557`. P4 closed;
  next session: P5.
- 2026-10-28 (plugins session): P5 steps 1 + 2 - plain Flight descriptors (SQL text command, table path) behind the
  new opt-in `plainFlightCommands` (core 5e9ac98, own commit, STATUS.md Log line; Open question noted as additive),
  pyarrow + ClickHouse 26.8 `arrowFlight` end-to-end tests and a ClickHouse image step in Dev CI (a802c41),
  ClickHouse TLS + LDAP Basic login test (7c88ccf, test only). Local (GCS mirror of Central; Docker started in the
  session, image via mirror.gcr.io): `PlainFlightCommandsTest` 3/3, `PlainFlightCommandsIntegrationTest` 5/5,
  `FlightSqlProducerTest` 33/33, `ConfigAdapterTest` 48/48, `GenericFlightClientsIntegrationTest` 2/2,
  flight-auth-ldap 36/36 incl. `ClickHouseLdapLoginTest`; Checkstyle + SpotBugs clean. CI: push runs 40788411531,
  40788854220, 40789242301 cancelled by later pushes; Dev CI run 40789295791 (881bbf7, dispatched by the core lane,
  contains all three commits) green incl. the ClickHouse image step and package -> pre-release
  `server-1.0.0-881bbf73e946`. Next: P5 step 3 (DuckDB `airport` in CI), then close P5.
- 2026-10-29 (plugins session): P5 step 3 - DuckDB `airport` extension against plain descriptors (0de9086, test,
  Dev CI step, README/CHANGELOG; no code change): schema works, reading needs Airport's own `endpoints` action, which
  is not added (Open questions); the test pins the clear refusal. Probe runs on temporary branch
  `probe/airport-plugins` (40792710735 found the `endpoints` action, 40793004644 green); the session could not
  delete that branch (proxy 403) - maintainer: please delete it. Local (GCS mirror of Central): flight-server
  test-compile + Checkstyle clean (the test skips without the extension; airport hosts are blocked in the cloud
  session). CI: Dev CI push run 40793321181 (0de9086) cancelled by a later push; Dev CI run 40794638660 (f082b73,
  contains 0de9086) green incl. the airport step. No pre-release (no runtime change). **P5 closed**; next: P6.
- 2026-10-29 (plugins session): P6 started - plan written under P6 (provider `ranger-hive` on the cluster's Hive
  Ranger service, fail closed; core hooks for referenced columns / Hive types / table replacement still to design in
  Open questions before merging). Step 1: new module `flight-auth-ranger-hive` with `HiveMask` (Ranger Hive mask
  types -> DuckDB expressions equal to Hive 3.1.3's mask UDFs) and `HiveMaskNumbers` (a1aa21f; nothing registered,
  no runtime change, so no CHANGELOG line or pre-release). Local (GCS mirror of Central): `HiveMaskTest` 6/6,
  Checkstyle + SpotBugs clean. CI: Dev CI push runs 40801711288, 40802647736, 40804099018 cancelled by other lanes'
  pushes; Dev CI run 40805065855 (6558c3d, contains a1aa21f) green (build). Next: P6 step 2 (policy evaluator).
- 2026-10-29 (plugins session): P6 step 2 - `HivePolicies` (Ranger Hive service evaluator: visibility by `_any`,
  select per column, row filter, masks via `HiveMask`, fail closed; f455b87, module `flight-auth-ranger-hive` only,
  nothing registered, so no CHANGELOG line or pre-release). Local (Maven Central): `HivePoliciesTest` 5/5,
  `HiveMaskTest` 6/6, Checkstyle + SpotBugs clean. CI: Dev CI run 40812949194 (f455b87) - running. Next: P6 step 3
  (core hook design into Open questions, then the provider-groups factory default).
