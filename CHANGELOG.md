# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Settings since 7562a5bdb7b9 (summary of the entries below)

Every setting added, or whose default changed, since build `7562a5bdb7b9` (2026-10-27), with its default as of this
build. Unless a row says otherwise a new setting's default keeps the earlier behaviour. Server settings take the `-D`
name, the `arrowflight.*` property or (where listed) the environment variable, in that order of precedence; plugin
settings are `arrowflight.<plugin>.<key>` properties; the auth providers read environment variables only.

**Defaults that changed**

| Setting | Default now | Before |
|---|---|---|
| `duckDbThreads` (`arrowflight.duckdb.threads`) | unset/0 = cores / 4 between 4 and 16 per connection, above 4 at most one per GiB of the connection's `memory_limit` (startup lines `duckdb=threads`, `duckdb=sizing`) | fixed 4 |
| `duckDbMemoryLimit` (`arrowflight.duckdb.memoryLimit`) | unset = per connection (memory - `-Xmx` - direct memory - 15 %) / `duckDbMemoryConnections`, at least 1 GiB, at most 80 % of memory; `none` = DuckDB's own; a size (e.g. `4GB`) wins | DuckDB's 80 % of RAM per instance |
| `shutdownDrainSeconds` (`arrowflight.server.shutdownDrainSeconds`, `FLIGHT_SHUTDOWN_DRAIN_SECONDS`) | 20: a stopping node leaves the pool, refuses new calls `UNAVAILABLE`, lets running answers finish (`docker stop -t 25`) | did not exist; stop cut running answers (`INTERNAL`); `0` restores that |
| `duckDbHdfsShortcircuit` / `duckDbHdfsDomainSocketPath` (`HDFS_SHORTCIRCUIT` / `HDFS_DOMAIN_SOCKET_PATH`) | unset = `dfs.client.read.shortcircuit` / `dfs.domain.socket.path` from the `LIBHDFS3_CONF` file, else from the server's Hadoop configuration (a socket path alone turns short-circuit on); startup line `hdfs=shortCircuit` | unset = hadoopfs's own defaults (off, no socket), so DuckDB read every HDFS block over TCP whatever `LIBHDFS3_CONF` said |
| `duckDbPreserveInsertionOrder` under `resourceProfile=speed` only | `false` (rows without ORDER BY in any order) | `true` (balanced unchanged) |
| `hdfsLocalCopyMinFreeBytes` (`arrowflight.hdfs.localCopyMinFreeBytes`, `HDFS_LOCAL_COPY_MIN_FREE_BYTES`) | unset = min(1 GiB, 10 % of the copies' disk); a size wins, 0 = the copies only have to fit (line `hdfs=localCopyCache ... minFreeBytes=... reserve=default\|set diskBytes=...`, WARN when a set reserve is not below the disk size) | 1 GiB on any disk (item 30) |

**New server settings (opt-in or unchanged behaviour by default)**

| Setting | Default | Meaning |
|---|---|---|
| `resourceProfile` (`arrowflight.resourceProfile`, `FLIGHT_RESOURCE_PROFILE`) | `balanced` (nothing changes) | `speed` supplies faster defaults at a memory/CPU cost (see "Resource profile speed"); `-D` and environment settings win |
| `duckDbMemoryConnections` (`arrowflight.duckdb.memoryConnections`, `DUCKDB_MEMORY_CONNECTIONS`) | 0 = cores / (cores / 4 between 2 and 16), at least 2 | connections the default `memory_limit` is split between |
| `duckDbThreadsWhenIdle` (`arrowflight.duckdb.threadsWhenIdle`, `DUCKDB_THREADS_WHEN_IDLE`) | 0 = off | threads of a query that starts alone on the node |
| `duckDbThreadsUnderLoad` (`arrowflight.duckdb.threadsUnderLoad`, `DUCKDB_THREADS_UNDER_LOAD`) | 0 = off | threads of a query that starts while the node's cores are taken |
| `duckDbProducingStreams` (`arrowflight.duckdb.producingStreams`, `DUCKDB_PRODUCING_STREAMS`) | 0 = no cap | DoGet streams computing inside DuckDB at once |
| `duckDbSharedDatabase` (`arrowflight.duckdb.sharedDatabase`, `DUCKDB_SHARED_DATABASE`) | false | one DuckDB per node for every worker connection |
| `duckDbSharedMaxThreads` (`arrowflight.duckdb.sharedMaxThreads`, `DUCKDB_SHARED_MAX_THREADS`) | 8 (0 = no cap) | shared DuckDB threads = min(cores, cap) unless `duckDbThreads` is set |
| `duckDbSharedBufferedConnections` (`arrowflight.duckdb.sharedBufferedConnections`, `DUCKDB_SHARED_BUFFERED_CONNECTIONS`) | 64 | shared DuckDB connections that carry `duckDbStreamingBufferSize` (only with both set) |
| `flightCompression` (`arrowflight.flight.compression`) | `none` | codec (`zstd`/`lz4`) of DoGet calls without the `arrow-compression` header or session option; keep `none` while stock JDBC drivers read |
| `flightCompressionLevel` (`arrowflight.flight.compressionLevel`) | 1 | zstd level 1..22 |
| `flightCompressionMinBytes` (`arrowflight.flight.compressionMinBytes`) | 65536 | smaller batches go plain |
| `flightCompressionThreads` (`arrowflight.flight.compressionThreads`) | 0 = one per core, at most 8 | compress a batch's columns side by side; 1 = serial |
| `flightZeroCopyWrites` (`arrowflight.flight.zeroCopyWrites`, `FLIGHT_ZERO_COPY_WRITES`) | false | gRPC sends DuckDB's exported buffers without a copy |
| `flightSessionIdleSeconds` (`arrowflight.flight.sessionIdleSeconds`) | 3600 | Flight SQL session (SetSessionOptions) idle expiry |
| `flightSessionMax` (`arrowflight.flight.sessionMax`) | 10000 | sessions per node |
| `plainFlightCommands` (`arrowflight.plainFlightCommands`, `FLIGHT_PLAIN_COMMANDS`) | false | plain Arrow Flight clients (SQL command / `schema.table` path descriptors, e.g. ClickHouse) |
| `flightLoadReservationSeconds` (`arrowflight.cluster.loadReservationSeconds`, `FLIGHT_LOAD_RESERVATION_SECONDS`) | 600 | how long an unread planned endpoint counts as node load |
| `distributedComplete` (`arrowflight.cluster.distributedComplete`, `FLIGHT_DISTRIBUTED_COMPLETE`) | false | the planning node merges complete answers from the nodes' partial endpoints |
| `distributedCompleteTrustedCertificates` (`arrowflight.cluster.distributedCompleteTrustedCertificates`, `FLIGHT_DISTRIBUTED_COMPLETE_TRUSTED_CERTS`) | empty = the node's own certificate | peer TLS trust of distributed complete mode |
| `distributedCompletePeerStreams` (`arrowflight.cluster.distributedCompletePeerStreams`, `FLIGHT_DISTRIBUTED_COMPLETE_PEER_STREAMS`) | 0 = one per 128 MiB of direct memory, at least 2 | endpoint streams the distributed complete merges of a node hold open at once; a merge waits for room (item 35) |
| `distributedCompleteAdmissionWaitMillis` (`arrowflight.cluster.distributedCompleteAdmissionWaitMillis`, `FLIGHT_DISTRIBUTED_COMPLETE_ADMISSION_WAIT_MILLIS`) | 300000 | longest wait of a merge for room, then `RESOURCE_EXHAUSTED` |
| `dataDirShared` (`arrowflight.storage.dataDirShared`, `FLIGHT_DATA_DIR_SHARED`) | false | a local data directory is a mount every node reads (NFS) |
| `hdfsLocalCopyMaxBytes` (`arrowflight.hdfs.localCopyMaxBytes`, `HDFS_LOCAL_COPY_MAX_BYTES`) | `32GiB` (0 = off) | only on HDFS with SASL DataNodes (`hdfs=shortCircuitOnly`, where such queries failed before): bound of the node-local copies of files the node does not hold every block of; least recently used unused copies are evicted |
| `hdfsLocalCopyThreads` (`arrowflight.hdfs.localCopyThreads`, `HDFS_LOCAL_COPY_THREADS`) | 4 | files a node copies at the same time |
| `hdfsLocalCopyRoomWaitMillis` (`arrowflight.hdfs.localCopyRoomWaitMillis`, `HDFS_LOCAL_COPY_ROOM_WAIT_MILLIS`) | 0 = refuse at once | how long a query whose node-local copies do not fit waits for other running queries (e.g. the sibling tickets of the same query) to release theirs before it is refused |
| `hdfsLocalCopyDir` (`arrowflight.hdfs.localCopyDir`, `HDFS_LOCAL_COPY_DIR`) | unset = below the DuckDB temp root | absolute directory (another disk) for the node-local copies; the server copies into its own `duckdb-<pid>-*` subdirectory there, allowed to DuckDB, deleted when it stops |

**New plugin settings (all off by default)**

| Setting | Default | Meaning |
|---|---|---|
| `arrowflight.s3.cacheMegabytes` / `arrowflight.lakefs.cacheMegabytes` | 0 = off | node-wide read cache in the JVM heap (at most half of `-Xmx`) |
| `arrowflight.s3.cacheCheckSeconds` / `arrowflight.lakefs.cacheCheckSeconds` | 10 | seconds an object's version is trusted before it is checked again |
| `arrowflight.s3.cacheAdmission` / `arrowflight.lakefs.cacheAdmission` | `always` | `reuse` = keep a missed block only when it is read again soon |
| `arrowflight.s3.cacheDiskPath` / `arrowflight.lakefs.cacheDiskPath` | empty = memory only | local directory of the disk tier (with `cacheDiskMegabytes`) |
| `arrowflight.s3.cacheDiskMegabytes` / `arrowflight.lakefs.cacheDiskMegabytes` | 0 | size of the disk tier |
| `arrowflight.hive.orcTables` | false | show and read ORC tables (ACID ORC stays skipped) |
| `arrowflight.hive.textTables` | false | show and read text, OpenCSVSerde and JsonSerDe tables |
| `arrowflight.hive.avroTables` | false | show and read Avro tables |
| `arrowflight.hive.insertOnlyTables` | false | show insert-only (MM) transactional tables (Hive 3) |
| `arrowflight.hive.views` | false | list simple Hive views the plugin can translate |
| `arrowflight.iceberg-rest.vendedCredentials` | false | ask the REST catalog for temporary per-table storage credentials |
| `catalog=jdbc` + `arrowflight.jdbc.sources` (new plugin `plugins/jdbc`) | unset | PostgreSQL / MySQL tables read-only through DuckDB's scanners; per source `<s>.type`, `.host`, `.port`, `.database`, `.user`, `.tables` (`schema.table`, only these are shown), `.schema`, `.sslMode`; password only from `FLIGHT_JDBC_<S>_PASSWORD` or `<s>.passwordFile` |

**New auth providers (new modules in the `flight-auth-...tar.zst` asset; nothing changes unless selected)**

| Setting | Default |
|---|---|
| `FLIGHT_AUTH_PROVIDER=ldap`: `FLIGHT_LDAP_URL`, `FLIGHT_LDAP_SESSION_SECRET` | required |
| `FLIGHT_LDAP_USER_DN_TEMPLATE` or `FLIGHT_LDAP_USER_SEARCH_BASE` | exactly one required |
| `FLIGHT_LDAP_USER_SEARCH_FILTER` / `FLIGHT_LDAP_USER_ATTRIBUTE` | `(uid={0})` / unset = the login name |
| `FLIGHT_LDAP_BIND_DN`, `FLIGHT_LDAP_BIND_PASSWORD` (or `_FILE`) | unset |
| `FLIGHT_LDAP_START_TLS` / `FLIGHT_LDAP_ALLOW_PLAINTEXT` | false / false |
| `FLIGHT_LDAP_TRUSTSTORE`, `_TYPE`, `_PASSWORD` | unset = JVM trust / `PKCS12` / unset |
| `FLIGHT_LDAP_CONNECT_TIMEOUT_MS` / `FLIGHT_LDAP_RESPONSE_TIMEOUT_MS` / `FLIGHT_LDAP_SESSION_MINUTES` | 5000 / 10000 / 10 |
| `FLIGHT_LDAP_GROUP_SEARCH_BASE` / `_FILTER` / `FLIGHT_LDAP_GROUP_NAME_ATTRIBUTE` | unset = no directory groups / `(\|(member={1})(uniqueMember={1})(memberUid={0}))` / `cn` |
| `FLIGHT_AUTH_PROVIDER=jwt`: `FLIGHT_JWT_JWKS_URL` or `FLIGHT_JWT_JWKS_FILE` | exactly one required |
| `FLIGHT_JWT_ISSUER`, `FLIGHT_JWT_AUDIENCE` | required |
| `FLIGHT_JWT_USER_CLAIM` / `FLIGHT_JWT_GROUPS_CLAIM` | `sub` / unset = no token groups |
| `FLIGHT_JWT_ALGORITHMS` | RS256/384/512, PS256/384/512, ES256/384/512 |
| `FLIGHT_JWT_CLOCK_SKEW_SECONDS` / `FLIGHT_JWT_JWKS_CACHE_MINUTES` / `FLIGHT_JWT_JWKS_REFRESH_LIMIT_SECONDS` | 60 / 15 / 30 |
| `FLIGHT_JWT_TRUSTSTORE`, `_TYPE`, `_PASSWORD` | unset = JVM trust / `PKCS12` / unset |
| `FLIGHT_JWT_CONNECT_TIMEOUT_MS` / `FLIGHT_JWT_READ_TIMEOUT_MS` | 5000 / 10000 |
| `FLIGHT_JWT_BASIC_PASSWORD` | false (true = the token may come as a Basic password) |
| `FLIGHT_AUTHORIZATION_PROVIDER=ranger-hive`: `RANGER_HIVE_SERVICE_NAME`, `RANGER_ADMIN_URL` | required |
| `RANGER_HIVE_SCHEMA_PREFIX` | empty (set it to the hive catalog's `schemaPrefix`) |

Client side (no server setting): the DoGet call header / session option `arrow-compression: zstd|lz4` asks for a
compressed stream; Java clients need the `arrow-flight-codec-lz4` asset for fast LZ4 decoding.

### Changed - log noise on node stop, pool queueing and copy-answered tables (STATUS item 36 (4))

- A query that meets a thread pool already stopped by a node stop (`RejectedExecutionException` under the execution,
  the description or the schema of a query) logs one WARN line (`execution=failed ... result=stopping`,
  `query=failed` / `schema=failed reason='The server is stopping; retry the query'`) instead of an ERROR with a stack,
  and the client gets `UNAVAILABLE` (as during the drain) instead of `INTERNAL`. A stream that starts while the node
  stops runs without its client-cancel check instead of failing.
- `pool=queueGrowth` is logged at most once per 10 s per node, with `peakQueued=` and `suppressed=` (lines held back).
  Fixed: a task refused by the stopped I/O pool stayed counted as active.
- `views=unreadableTable` is INFO, not WARN, when the node reads node-local copies (`hdfsLocalCopyDir`): such tables
  are answered (`queries read node-local copies of its files`).

### Fixed - Kerberos keytab start on a host whose krb5 default ticket cache is a keyring (STATUS item 36 (2))

- With `hdfsKerberosPrincipal` + `hdfsKerberosKeytab` set and neither `KRB5CCNAME` nor
  `hadoop.security.kerberos.ticket.cache.path` (LIBHDFS3_CONF) naming the ticket cache, the hadoopfs extension reads
  libkrb5's default, `default_ccache_name` of `KRB5_CONFIG` or `/etc/krb5.conf` (RHEL-like hosts ship
  `KEYRING:persistent:%{uid}` or `KCM:`), while the server wrote `/tmp/krb5cc_<uid>`: the extension found no ticket,
  every catalog view failed and every query answered `Catalog Error: Table with name ... does not exist`. The server
  now reads `default_ccache_name` (following `include` / `includedir`, expanding `%{uid}`, `%{euid}`, `%{USERID}`,
  `%{TEMP}`): a `FILE:` default is where the keytab login is written; any other cache type fails the start with the
  cause and the fix (`set KRB5CCNAME=FILE:<path> for the server process, or hadoop.security.kerberos.ticket.cache.path
  in the LIBHDFS3_CONF file`). Setups with `KRB5CCNAME` or the LIBHDFS3_CONF path set are unchanged.
- `views=createFailed` now logs DuckDB's error (e.g. `IO Error: Unable to connect to HDFS ...`) instead of the JDBC
  driver's `Attempting to execute an unsuccessful or closed pending query result` line in front of it.

### Changed - refusal and failure log lines stay on one line (STATUS item 36 (4), part)

- `query=refused` (reason and SQL) and `execution=failed` (error) lines write line breaks as `\n`, so one record is
  one line even for multi-line SQL or DuckDB messages. Client statuses and messages are unchanged.

### Added - CI: distributed complete on small copy disks (STATUS item 36 (1))

- Service hdfs, sasl-cluster speed cell: last stage restarts the three servers as on the maintainer's stand
  (`resourceProfile=speed`, `distributedComplete=true`, `hdfsLocalCopyDir` on a small tmpfs each) and runs eight
  complete queries over `chunks.lineitem` twice (`tools/ci/service-hdfs-dcsmalldisk-check.py`): every answer exact,
  and after each query no server holds a deleted copy file open (`/proc/1/fd`).

### Fixed - distributed complete on a small copy disk: endpoints of one node waited for each other's copy room (STATUS item 36 (1), part 1)

- With local copies on (SASL HDFS, `hdfsLocalCopyMaxBytes` > 0) and several endpoints per node (`endpointsPerServer`,
  4 under `resourceProfile=speed`), a merge opened every endpoint stream at once but reads the answers one after the
  other: endpoints not yet read copied their files and held the copy room while they waited, and a sibling endpoint
  of the same node waited for that room until it was refused (`No room ... The query needs all its copies at once`)
  although the query fits in batches. Such a merge now opens one endpoint per node at a time; the node's next
  endpoint opens as soon as its earlier answer has been read, so nodes still compute in parallel (log line
  `distributedComplete=oneStreamPerNode`). Aggregates without GROUP BY / DISTINCT (one-row answers) and clusters
  without local copies are unchanged.
- A copy-room refusal and its `hdfs=localCopyNoSpace` line now also count the copies deleted while this server still
  holds them open (`deletedOpenFiles=`, `deletedOpenBytes=`; Linux `/proc/self/fd`), whose disk room only comes back
  when their reader closes them.

### Fixed - access denials reported as INTERNAL with an ERROR stack (STATUS item 36 (3), (5))

- A denial (UNAUTHORIZED, PERMISSION_DENIED on the wire) raised under another failure while a query is described
  (`Error getting Arrow schema`, `Error describing query`) or executed was reported as INTERNAL with an ERROR and a
  long stack. It now reaches the client as the denial and logs one WARN line: `query=denied reason='...' query='...'`
  (SQL on one line), or `execution=failed ... result=denied error='...'`. Denials that already reached the client as
  UNAUTHORIZED are unchanged. Tests: `FlightSqlProducerTest.deniedSchemaIsUnauthorizedWithOneWarnLine`,
  `deniedExecutionIsUnauthorizedWithOneWarnLine`, `denialIsFoundAmongTheCauses`.
- START-NOTES: recommended Java Hadoop client settings for HDFS HA NameNode host crashes
  (`ipc.client.connect.timeout=5000`, `ipc.client.connect.max.retries.on.timeouts=2`,
  `dfs.client.failover.sleep.max.millis=2000`; slowest query 67-71 s -> 25.5 s on the stand).

### Fixed - CI: item 35 step compiled DuckDB from source (no wheel for `duckdb==1.4.1`)

- The Dev CI step "Distributed complete under concurrency (item 35)" generates its Parquet data with `duckdb==1.5.6`
  (wheels only, as the other workflows) and has its own 10-minute timeout. With 1.4.1 pip built DuckDB from source
  (12 min on the push run, over the 30-minute job budget on the `package=true` run 42233371052, which was cancelled).
  CI only; the server is unchanged.

### Fixed - concurrent distributed complete answers failed on the Arrow memory limit (STATUS item 35)

- With `distributedComplete=true`, concurrent full scans no longer fail with `INVALID_ARGUMENT: Could not load
  buffers ... Unable to allocate buffer ... due to memory limit`. Every merge opened all of its endpoint streams at
  once, and each open stream holds batches in direct memory. Merges now take room for their endpoint streams in one
  node-wide budget before they open any (`PeerStreamAdmission`). The budget is `distributedCompletePeerStreams`,
  by default one stream per 128 MiB of direct memory (16 at 2 GiB: four merges of four endpoints). A merge that
  finds too little room waits, in arrival order, for running merges to end (`distributedComplete=admissionWait`,
  then `distributedComplete=admitted waitedMs=`). After `distributedCompleteAdmissionWaitMillis` (default 5 min)
  it is refused with `RESOURCE_EXHAUSTED ... retry later`. A merge of more endpoints than the budget runs alone.
  Startup line: `distributedComplete peer streams: N (...)`.
- An Arrow memory shortage anywhere in a query's failure is now reported as `RESOURCE_EXHAUSTED`, never as
  `INVALID_ARGUMENT` (Arrow's stream reader wraps its memory limit in an IllegalArgumentException). It is logged
  as one WARN line `execution=failed ... result=resourceExhausted` without a stack.
- Reproduced on one node with the stand's settings (local Parquet, 6M rows, `-Xmx2g -XX:MaxDirectMemorySize=2g`,
  speed + distributedComplete, 4 cores): 16 concurrent full scans + 32 small queries failed 7 of 48 before the change.
  After it, 48/48 and 56/56 (24 scans) were exact, with 0 failed executions and 0 fallbacks.
- Tests: `PeerStreamAdmissionTest` (7), `DistributedCompleteIntegrationTest`
  `concurrentMergesWaitForRoomAndStayExact` / `mergeWithoutRoomIsRefusedAsResourceExhausted`, `FlightSqlProducerTest`
  `arrowMemoryLimitIsResourceExhausted` / `memoryShortageIsFoundAmongTheCauses`. Dev CI step "Distributed complete
  under concurrency (item 35)": one node as on the stand with TPC-H SF1 lineitem, 16 full scans + 32 small queries at
  once, every answer exact, no failed execution, all 48 merged (`tools/ci/distributed-concurrency-check.py`).

### Changed - log hygiene for expected refusals (STATUS item 34)

- Partial-mode queries refused while planned (SQL the partial path cannot read, e.g. several tables; features
  endpoints cannot answer, e.g. an aggregate over DISTINCT values; complete answers on local-disk clusters) log one
  WARN line `qid=<id> node=<n> query=refused mode=partial reason='...' query='...'` without a stack (the multi-table
  case was `ERROR Error getting Arrow schema for query` with a `RuntimeException: Invalid SQL query` stack), and,
  with local copies on, an `hdfs=reads files=0 shortCircuit=0 localCopy=0 copied=0 refused=partialPlan` line under
  the same qid. Each refused planning call gets its own qid.
- A stream refused for lack of copy room (`RESOURCE_EXHAUSTED`) logs one WARN line `execution=failed ... result=noRoom
  error='No room ...'` instead of an ERROR with the `NoRoomException` stack. Client statuses are unchanged.
- Tests: `FlightSqlProducerTest.partialRefusalsLogOneWarnLineWithQueryIdAndReads`,
  `copyDiskRefusalLogsOneWarnLine`, `ExecutionServiceTest.refusedQueryLogsItsReadsLine`; the Service hdfs small-disk
  stage fails when a refused join logs a `NoRoomException` stack.

### Fixed - tests: flaky `EndpointFailoverIntegrationTest` (load handover checked before it landed)

- `ticketPlannedForAnotherNodeGivesTheSameAnswerAndMovesItsLoad` read the planned node's load right after the stream
  ended, but the server moves that load in the background and joins the move only after the last batch is sent; the
  test now waits for the handover as it already did for the release (failed Dev CI 41644812393 and 42099508904, the
  latter blocking the item 33 `package=true`). Test only, no server change.

### Added - `hdfsLocalCopyRoomWaitMillis`: wait for copy room held by other queries (STATUS item 33, speed cells)

- With `endpointsPerServer > 1` on a small copy disk, the tickets of one partial query run in parallel on a node and
  each pins its copies; when their sum exceeds the room, the later tickets were refused `RESOURCE_EXHAUSTED` at once
  although the room comes back as soon as the earlier tickets end (egret `part-lineitem-sp` / `part-group-sp`).
  New setting `hdfsLocalCopyRoomWaitMillis` (`arrowflight.hdfs.localCopyRoomWaitMillis`,
  `HDFS_LOCAL_COPY_ROOM_WAIT_MILLIS`): a query whose new copies do not fit while other running queries hold copies
  waits up to that long for a release, then plans again (log `hdfs=localCopyRoomWait` when it starts waiting,
  `hdfs=localCopyRoomWaited ms=` when it fits). While it waits it holds no copies, so two waiting queries never hold
  each other's room. When no other query holds copies it is refused at once as before; after the wait the refusal
  says `after waiting N ms`. Default 0: refused at once, unchanged (making 30000 the default is an Open question).
  The startup line `hdfs=localCopyCache` shows `roomWaitMillis=`.
- A waiting query re-checks the disk at least every second, and waiting queries no longer wake each other in a loop
  (first CI runs: the loop held the cache's lock so the ticket holding the room could not finish until the others
  timed out; regression test `LocalCopyCacheTest.ticketsOfOneQueryTakeTheRoomInTurnsWhileCopying`). CI small disk with
  `endpointsPerServer=4 -DhdfsLocalCopyRoomWaitMillis=60000`: egret's partial `part-lineitem-sp` / `part-group-sp` three
  times each, exact, 18 waits, 0 refusals.

### Changed - SASL HDFS clusters: every copy refusal of a complete answer says why it was not batched (STATUS item 33, regression)

- A complete-mode answer refused for lack of copy room (RESOURCE_EXHAUSTED) now always logs
  `hdfs=completeInCopyBatches skipped tables=[...] localCopy=N copyBytes=B reason='...'` (WARN) with the exact reason
  it was not read in batches of copies: the query joins tables or reads more than one table (named), it is not one
  query of one table (parser message), it uses a feature the batches cannot merge (named, e.g. ORDER BY without an
  aggregate, DISTINCT or LIMIT), the column types differ, the statement has parameter values, or its copies make one
  batch only / fitted the copy room when it was planned (sizes given). Before, several of these cases logged nothing,
  and the line came before the refusal even when the query was then answered. Log only; answers unchanged.

### Changed - SASL HDFS clusters: copy refusals tell the query's own copies from other queries' (STATUS item 33, regression)

- A copy refusal (RESOURCE_EXHAUSTED, `hdfs=localCopyNoSpace`) no longer reports the copies the refused query itself
  reuses as "copies in use by running queries": its message names the bytes of the copies this query reuses and of
  the copies other running queries hold apart, and the log line reads `reusedBytes=... otherInUseBytes=...`
  (was `inUseBytes=...`, which counted every cached copy). Copies already cached are pinned by the query that reuses
  them before room is made (item 33 (C)), so a query that needs all its copies at once was refused naming its own
  reused copies as "in use" on a small copy disk; no copy stays pinned after its query ends (unit-tested over 20
  sequential batched queries on a stand-sized disk).
- Every released lease logs `hdfs=localCopyReleased files=N inUseBytes=X cachedBytes=Y` (0 in-use bytes when no
  query runs), and a complete-mode query whose copies do not fit at once but cannot be read in batches of copies logs
  `hdfs=completeInCopyBatches skipped ... reason='...'` (WARN) before it is answered (or refused) with every copy at once.

### Fixed - SASL HDFS clusters: partial-mode queries with a subquery failed INTERNAL (STATUS item 33 (E))

- On HDFS read short-circuit only (SASL DataNodes), a query asked for in parts that holds a nested query (TPC-H q04's
  `EXISTS`, `IN (SELECT ...)`, a subquery in FROM such as q07/q13's, a CTE, a set operation) now gets the complete answer (log `query=answeredCompletely
  mode=partial reason='the query has a subquery ...'`), which plans node-local copies for every table it reads, or
  is refused INVALID_ARGUMENT when the node cannot read them. Before, it was planned in parts over the outer table's
  files and each endpoint read the subquery's table whole, failing as INTERNAL on the blocks its DataNode lacks.
  Other storage keeps answering such queries in parts (and refusing FROM subqueries other than a column list over
  one table, as before).
- A complete answer refused for lack of copy-disk room (RESOURCE_EXHAUSTED) logs its `hdfs=reads ... copied=0
  refused=noRoom` line too, so every query on a short-circuit-only node has one.

### Fixed - SASL HDFS clusters, small copy disk: disk filled up, `count(*)` and `GROUP BY 1` refused; subqueries kept off distributed complete (STATUS item 33, part 1)

- Node-local copies: making room for a query's new copies could evict a copy the same query reuses, which was then
  copied again without being counted, so the copy disk filled up (`IOException: Cannot copy ... No space left on
  device`). Copies a query reuses are now pinned before anything is evicted, and every copy is started only with its
  bytes counted against the disk. Aggregates, DISTINCT and LIMIT read in batches now take files already cached first.
- Complete mode: a one-table query whose copies do not fit is read in batches also when the partial path names its
  columns otherwise (an unaliased `count(*)` is `count_star()` in the complete answer); the answer keeps the complete
  answer's names (log `hdfs=completeInCopyBatches ... renamed=true`). Before, such queries were refused
  RESOURCE_EXHAUSTED.
- `GROUP BY 1` (ordinals naming a column of the select list) groups like the column it names, in both modes, so it is
  read in batches of copies like `GROUP BY <column>`.
- The parser's "Can only select from ONE table" for multi-table queries is logged at DEBUG, not ERROR with a stack trace.
- `distributedComplete=true`: a query with a subquery (TPC-H q04's `EXISTS`, `IN (SELECT ...)`, a subquery of the
  same table) keeps the single-node complete path, which plans locality and node-local copies for every table it
  reads. Before, each endpoint read the subquery's table whole without them (on SASL HDFS over TCP:
  `HdfsNetworkException`), and the failover node lacked the schema views.

### Changed - node-local copy reserve relative on small disks; `hdfs=reads` on every complete answer (STATUS item 32)

- `hdfsLocalCopyMinFreeBytes` unset now keeps min(1 GiB, 10 % of the copy disk's size) free instead of 1 GiB, so a
  small copy disk (e.g. a 64 MiB tmpfs) takes copies with the defaults; a set value is kept as it is. The
  `hdfs=localCopyCache` line (first copy) adds `reserve=default|set diskBytes=...` and is a WARN when the reserve is
  not below the disk's size (every copy would be refused).
- On short-circuit-only stores every complete answer logs an `hdfs=reads` line: also one that reads no data-store
  table (`files=0 ... tables=none|unknown`) and, on the merging node of `distributedComplete=true`, one with
  `tables=endpoints=N` (the endpoint nodes log their own reads). Copy-disk refusals stay `RESOURCE_EXHAUSTED` on the wire
  (ADBC prints `INTERNAL: ... (ResourceExhausted; ...)`).
- CI (service hdfs): the small-disk stage runs with the default reserve and checks the cache line; every sasl cell and
  stage checks that each successful query (executionPath `success=true`) has an `hdfs=reads` line with its qid.

### Changed - SASL HDFS clusters: `hdfs=reads` for every complete answer (STATUS item 31, acceptance)

- A complete answer on a short-circuit-only node whose tables are all held whole now logs `hdfs=reads files=N
  shortCircuit=N localCopy=0` too (before, only queries that copied logged the line), and `files=` / `shortCircuit=`
  of complete answers that copy count every table the query reads, not only the copied ones.
- The copy-disk refusal is `RESOURCE_EXHAUSTED` on the wire (since part 1). ADBC clients print it as `INTERNAL:
  ... (ResourceExhausted; DoGet ...)` because ADBC has no resource-exhausted status; the gRPC status is the one in
  parentheses. The CI small-disk stage now requires `ResourceExhausted` there.

### Fixed - distributed complete answers ran out of direct memory (STATUS item 31, part 3)

- With `distributedComplete=true` the node merging a complete answer opens every endpoint stream at once (so all
  peers compute in parallel) but reads them one after another; every stream not yet read held up to five batches in
  direct memory (Flight's read-ahead), so a full scan with 12 endpoints (`speed`: 4 per node, 3 nodes) failed with
  `OutOfMemoryError: Cannot reserve 16777216 bytes of direct buffer memory` on a 512 MiB direct limit. A stream now
  receives only its schema until the merge gets to it, then reads ahead as before; peers wait in their own bounded
  outbound queues. Answers, wire format and settings are unchanged.

### Fixed - SASL HDFS clusters: complete one-table queries read their copies in batches (STATUS item 31, part 2)

- In `complete` mode a query of one table (scan, aggregate, GROUP BY, DISTINCT, LIMIT; plain or qualified names)
  whose local copies do not fit the copy cache or the copy disk at once is answered like a partial-mode ticket over
  all the table's files: the copies go in batches that fit and the batches' answers are concatenated or merged
  (log `hdfs=completeInCopyBatches`, then `hdfs=copyBatches`). Before, such queries failed with "No room on the disk
  of the node-local copy cache". Joins and shapes the batches cannot merge keep the single-copy path (refused with
  `RESOURCE_EXHAUSTED` when the copies do not fit).

### Fixed - SASL HDFS clusters: plain table names and silent 0-row joins (STATUS item 31, part 1)

- **Wrong answers:** the view of a table a node holds no file of whole returned typed NULL columns behind an error
  filter; DuckDB folded a join on such columns to an empty result without evaluating the error, so a comma join with
  that table answered 0 rows instead of failing (`SELECT count(*) FROM orders_sp o, customer c WHERE ...` gave 0).
  Every column of that view is now the error itself, so any query reading it fails with the files it lacks.
- Plain table names (`FROM lineitem`, as clients with a default schema write them) only worked for tables a node held
  whole: planning, the readable-here check, local copies and join views looked for the name without its schema, so a
  complete answer was refused ("this node lacks blocks of ...") or DuckDB read a non-local file ("Connection reset").
  On HDFS read short-circuit only, every plain name that DuckDB resolves (the one schema with a table of that name)
  now gets its schema before the query is planned (`node=... query=qualifiedPlainNames` at debug); names in several
  schemas, unknown names and CTE names stay as written. Other storage is unchanged.
- A query refused because the copy disk has no room (`hdfs=localCopyNoSpace`) now fails `RESOURCE_EXHAUSTED`, not
  `INTERNAL`; `hdfs=localCopyOverLimit` is logged once per query instead of once per copy batch.
- CI: service-hdfs sasl-cluster uploads schema `plain` (every tpch table as `<name>_p`, lineitem/orders with 1 MiB
  blocks and replication 1) and runs the 22 TPC-H queries with plain names complete on every node, a plain comma
  join, and count/sum in both modes, answers checked against the expected ones.

### Fixed - evicted node-local copies kept their disk space (STATUS item 30, part 2b)

- A query over node-local copies (HDFS with SASL DataNodes) left its DuckDB prepared statement in the per-session
  statement cache, and an open DuckDB statement keeps the Parquet files it read open. A copy evicted or deleted after
  the query therefore kept its disk space until the cache dropped the statement: on a copy disk smaller than a query's
  input the second batch of copies was refused (`hdfs=localCopyNoSpace`) although the cache held nothing, and every
  later query needing a copy failed the same way. SQL that reads the copy directory is now prepared for its run only
  and closed after it (other statements are cached as before). Found by the new service-hdfs small-disk stage
  (sasl-cluster balanced: one server whose `hdfsLocalCopyDir` is a tmpfs smaller than `chunks.lineitem`).

### Added - `hdfsLocalCopyMinFreeBytes`: node-local copies never fill their disk (STATUS item 30, part 2b)

- Before a query's new node-local copies (HDFS with SASL DataNodes) are started, the node checks the free space of
  the copies' disk (`hdfsLocalCopyDir`, else the DuckDB temp root): the copies must leave `hdfsLocalCopyMinFreeBytes`
  (default 1 GiB) free there, counting copies still being made at their full length. Unused copies are evicted for
  room first, also below `hdfsLocalCopyMaxBytes`; if that is not enough the query fails at once with a message naming
  the files, the bytes needed and free, and the bytes held by running queries' copies (log `hdfs=localCopyNoSpace`),
  and nothing is copied. Before, such a query copied until the disk was full ("No space left on device"), which could
  also break DuckDB spills sharing that disk. Scans and mergeable aggregates read in batches of copies now size the
  batches by the cache limit or the disk's room, whichever is smaller, so they still finish on a small disk. Startup
  line `hdfs=localCopyCache ... minFreeBytes=`.

### Added - `hdfsLocalCopyDir`: node-local copies on a disk of their own (STATUS item 30, part 2b)

- New setting `hdfsLocalCopyDir` (`arrowflight.hdfs.localCopyDir`, `HDFS_LOCAL_COPY_DIR`, absolute path): the
  node-local copies of HDFS files (SASL DataNodes) go into a `duckdb-<pid>-*` subdirectory there instead of
  `localcopy` below the DuckDB temp root, so they can use a disk other than DuckDB's spill. The locked-down DuckDB
  may read that subdirectory (and the startup lockdown check expects it); it is deleted when the server stops, and
  one left by a dead process is removed at the next start. Unset (default) or `hdfsLocalCopyMaxBytes=0`: unchanged.

### Documentation - SASL HDFS clusters: known limits of node-local copies (STATUS item 30 done)

- Item 30 (node-local copies of HDFS files a SASL cluster's node does not hold every block of) is complete with two
  known limits, written into START-NOTES: a join, a `complete` answer that cannot be merged and a scan with ORDER BY
  and no LIMIT need all their copies at once. They run while those copies fit the copies' free disk
  (`hdfsLocalCopyMinFreeBytes` kept free); beyond it they are refused before copying, naming the files
  (`hdfs=localCopyNoSpace`). The disk is never filled. Scans and mergeable aggregates are processed in batches.

### Changed - SASL HDFS clusters: HAVING and DISTINCT aggregates merged from batches of copies (STATUS item 30, part 2b)

- On a single-server cluster (where partial mode accepts these shapes), a query of one table whose files to copy
  exceed the cache / disk room is now answered per batch of copies and merged on the node also when it is an
  aggregate with HAVING (the condition may use plain count/sum/min/max, selected GROUP BY columns, literals, AND / OR /
  NOT, comparisons and `+ - *`; each batch answers the query without HAVING plus the aggregates the condition reads,
  the condition is applied to the merged groups) or uses count / sum over DISTINCT values of one plain column (each
  batch also groups by that column; part answers then hold one row per group and distinct value, in memory). Log
  line `hdfs=copyBatches ... merge=rewrite`. Other HAVING forms (IS NULL, functions, avg, DISTINCT inside HAVING),
  DISTINCT over two columns or expressions keep holding their copies at once, as before.

### Changed - SASL HDFS clusters: LIMIT aggregates, DISTINCT and LIMIT scans merged from batches of copies (STATUS item 30, part 2b)

- Like the aggregates below, a partial-mode query of one table whose files to copy exceed `hdfsLocalCopyMaxBytes` is
  now answered per batch of copies that fit and merged on the node when it is: an aggregate with LIMIT (each batch
  answers it without ORDER BY and LIMIT, so every group keeps its full values; ORDER BY of output columns and LIMIT are
  applied to the merged answer), or a scan with DISTINCT and/or LIMIT (optionally ORDER BY of output columns; each
  batch keeps its own first rows, DISTINCT / ORDER BY / LIMIT applied again). Same answer as computed at once. Log line
  `hdfs=copyBatches ... merge=rows` for the scans. Still holding their copies at once: a scan with ORDER BY and no
  LIMIT (its part answers would be the whole input in memory), HAVING, OFFSET, aggregates over DISTINCT values, avg or
  other expressions, joins and complete answers.

### Changed - SASL HDFS clusters: a footer-answered `count(*)` copies nothing (STATUS item 30, part 2b)

- A `SELECT count(*)` over all rows of a table (no filter, GROUP BY, ORDER BY, LIMIT or DISTINCT) is answered from the
  Parquet footers, which the Java Hadoop client reads under SASL too; files the node lacks blocks of are no longer
  copied to the local-copy cache for it. Log line `hdfs=reads files=... shortCircuit=... localCopy=0 footerOnly=<files
  not held whole>`. Same answer; other queries unchanged.

### Changed - SASL HDFS clusters: aggregates larger than the local-copy cache merged from batches (STATUS item 30, part 2b)

- A partial-mode aggregate of one table whose answer distributed complete mode can merge (counts, sums, minima,
  maxima, grouped by selected columns, optional ORDER BY of output columns; no LIMIT, HAVING, DISTINCT aggregate or
  expression) and whose files to copy exceed `hdfsLocalCopyMaxBytes` used to copy them all at once. It now answers
  per batch of files whose copies fit the limit, releasing each batch's copies before the next, keeps the small part
  answers in memory and merges them into the ticket's answer (same rows and column types as computed at once). Log
  lines `hdfs=copyBatches ... merge=aggregate` and `hdfs=copyBatches merged parts=... partBytes=...`; if the merge
  cannot be started (`merge=fallback`) the old path answers. Other aggregates, joins and complete answers over more
  than the limit still hold their copies at once.

### Changed - SASL HDFS clusters: scans larger than the local-copy cache read in batches (STATUS item 30, part 2b)

- A partial-mode plain scan of one table (no aggregate, ORDER BY, LIMIT, OFFSET, DISTINCT or window function) whose
  files to copy exceed `hdfsLocalCopyMaxBytes` used to copy them all at once (over the limit, deleted after the
  query): the disk could fill. It now reads the ticket's files in batches whose copies fit the limit (a file larger
  than the limit alone), releasing each batch's copies before copying the next, and sends all rows as one stream
  (same rows; files the node holds whole go with the first batch). Log line `hdfs=copyBatches batches=... files=...
  maxBytes=...` plus one `hdfs=reads` line per batch. Aggregates, joins and complete answers over more than the
  limit still hold their copies at once (next step).

### Added - SASL HDFS clusters: complete answers over tables the node lacks blocks of (STATUS item 30, part 2)

- A complete answer computed on the node that receives the query (non-mergeable shapes such as TPC-H joins, or
  `distributedComplete=false`) was refused with "this node lacks blocks of ..." when a table it reads has files the
  node does not hold whole. With local copies on (`hdfsLocalCopyMaxBytes` > 0) the node now copies those files to its
  local cache and reads each schema-qualified reference to such a table as `read_parquet` over the copies and the
  files it holds whole (same column types as the table's view); log line `hdfs=completeFromCopies` plus the per-query
  `hdfs=reads` line. A reference the rewrite cannot replace is still refused naming the files; a table named without
  its schema keeps the earlier check. With local copies off nothing changes.

### Added - SASL HDFS clusters: node-local copies of files no node holds whole (STATUS item 30, part 1)

- On HDFS whose DataNodes require SASL data-transfer protection (DuckDB reads short-circuit only), a partial-mode
  endpoint or join that needs a file this node does not hold every block of now copies it through the Java Hadoop
  client (SASL works) into a bounded cache below the DuckDB temp root (`<duckDbTempDirectory>/.../localcopy`) and
  DuckDB reads the copy with full Parquet pushdown; files held whole keep the direct short-circuit path. Before,
  such files were refused ("no live Flight node runs on a DataNode that holds every block of ...").
- The planner treats such files as readable by any live node: files held whole still go to their holders, the others
  to the least loaded node; a join runs on the node with the most local input and copies the rest.
- Copies are keyed by path, length and modification time (a rewritten file is copied again), reused across queries,
  made in parallel (`hdfsLocalCopyThreads`, one copy per file however many queries wait for it) and evicted least
  recently used beyond `hdfsLocalCopyMaxBytes` (default 32 GiB; 0 = off, item 29 behaviour). Copies a running query
  uses are never evicted; when they exceed the limit (a query needing more than the cache, or one file larger than
  it) the extra copies are deleted as soon as the query ends. Copies are deleted when the server stops.
- Logs: `hdfs=localCopy file=... bytes=... ms=... MBps=...` per copy and `hdfs=reads files=N shortCircuit=N
  localCopy=N copied=N reused=N copiedBytes=N copyMs=N copyMBps=N` per query on a node that copies.
- `complete` with `distributedComplete=true`: a mergeable query planned as one endpoint whose files the receiving node
  lacks blocks of is read from that endpoint's node (which copies) instead of being refused. Joins are described on a
  readable sample or a Java footer stub (they got a guessed all-text schema on nodes lacking the first file).
- Not yet (part 2): a `complete` answer computed on a node lacking blocks (non-mergeable shapes, or
  `distributedComplete=false`) is still refused naming the files; the disk the copies take for one query is not yet
  bounded below its input size.

### Fixed - SASL HDFS clusters: files go only to nodes holding every block (STATUS item 29, part 2)

- When the DataNodes require SASL data-transfer protection (`dfs.data.transfer.protection`, startup warning
  `hdfs=shortCircuitOnly`), DuckDB on a node can read only files with every block on its own DataNode. A file's
  hosts and each node's published inventory now name only such whole-file holders (before: any node holding one
  block), so queries in parts and `distributedComplete` send each file only to a node that can read it, and backup
  locations are only such nodes (before: any live node).
- A file no live node holds whole, a server-side join no single node can read, or a `complete` answer computed on a
  node missing blocks of the query's files now fails at once naming the files (at most ten) and the remedy
  (replication / a node on that DataNode / partial results / `distributedComplete=true`), instead of
  "Connection reset" from the hadoopfs extension after a few seconds. Clusters without SASL protection are unchanged.
  Reading such files anyway (a node-local copy) is item 30.
- Views no longer make DuckDB read a remote footer on such clusters: DuckDB binds a table's view on its first file,
  so a node lacking that file's blocks failed **every** query (found by the new CI cell). The view is now bound on
  a file the node holds whole; a table the node holds no file of gets a typed view that fails with the reason when
  read (queries in parts still read it on the nodes holding its files). A view DuckDB still cannot create is logged
  (`views=createFailed`) and skipped alone instead of failing every other table's view.
- A query in parts no longer announces a guessed result schema on such clusters (e.g. `sum` of a decimal as double
  while the endpoints send decimal: ADBC "endpoint returned inconsistent schema"): the schema is described on a
  sample file this node holds whole, else on its footer read through the Java client.
- CI: `service-hdfs.yml` cell `sasl-cluster` (3 DataNodes, 3 servers, Kerberos + SASL; schemas with replication
  3 / 2 and a file no DataNode holds whole).

### Fixed - SASL HDFS clusters: table schemas through the Java client, one bad table no longer refuses all (STATUS item 29, part 1)

- On HDFS the complete-answer views described every table with DuckDB (`DESCRIBE SELECT * FROM read_parquet(...)` of
  its first file through hadoopfs). With DataNodes that require SASL data-transfer protection hadoopfs can read only
  blocks with a local replica, so one table whose first footer was not on the node failed the whole setup and every
  query (even `select 1`) with `Connection reset by peer`. The footer is now read through the Java Hadoop client
  (which speaks SASL) and DuckDB describes a local copy of it without row groups (below the DuckDB temp root, removed
  at once; same columns, types and hive partition columns). Other stores are unchanged.
- A table whose first file cannot be described (any store) is left out of the views with a `views=skipTable ...
  reason=describe` warning instead of failing every query; a query naming it gets that reason, not "does not exist".
- The `hdfs=shortCircuitOnly` startup warning now says that schemas go through the Java client and that DuckDB can read
  only files with every block on the node's DataNode. Planner routing of such files to nodes holding all their blocks
  is the next step of item 29.

### Changed - `speed` drops DuckDB's insertion order (STATUS item 27)

- `resourceProfile=speed` now also offers `duckDbPreserveInsertionOrder=false` (upstream's default): parallel scan
  threads hand on their batches as they finish, so rows of a query without ORDER BY come in any order (ORDER BY
  answers unchanged). One full `lineitem` read on S3 (SF5, ADBC, 4 CPUs): 910 -> 1118 MB/s, 4 concurrent reads +5 %,
  same peak memory, same answers. An explicit `-DduckDbPreserveInsertionOrder=true` (or
  `DUCKDB_PRESERVE_INSERTION_ORDER=true`) wins; `balanced` is unchanged.
- New manual workflow `scan-perf.yml` + `tools/ci/scan-adbc.py`: full-table read x1 / xN / Parquet export over a local
  S3 endpoint for this ref, option variants and an upstream commit on the same runner, answers checked.

### Documentation - libhadoop and row order under `speed` (STATUS item 28)

- START-NOTES: libhadoop is not bundled (data reads go through hadoopfs, which needs none); how to point the server JVM
  at an existing one (`-Djava.library.path`) so its Java footer reads go short-circuit, and that without it those few
  reads go over TCP (SASL on a protected cluster works).
- START-NOTES / profile table: under `resourceProfile=speed` rows of a query without ORDER BY come in any order;
  ORDER BY or `-DduckDbPreserveInsertionOrder=true` (`DUCKDB_PRESERVE_INSERTION_ORDER=true`) restores the file order.

### Fixed - hadoopfs reads HDFS short-circuit (STATUS item 26)

- The hadoopfs extension hands libhdfs3 its own `hdfs_shortcircuit` (default false) and `hdfs_domain_socket_path`
  (default empty), which override `dfs.client.read.shortcircuit` / `dfs.domain.socket.path` in `LIBHDFS3_CONF`; both
  are needed. The server set them only from explicit `duckDbHdfsShortcircuit` / `duckDbHdfsDomainSocketPath`, so every
  block went over TCP (DataNode `BlocksRead` = blocks read). Unset, they now come from `LIBHDFS3_CONF`, then from the
  Hadoop configuration (`HdfsShortCircuit`); explicit settings still win. A block without a local replica, or a node
  without the socket, still falls back to TCP.
- When the cluster sets `dfs.data.transfer.protection` (SASL data transfer, which libhdfs3 cannot speak) and the node
  has short-circuit off or no domain socket, the server stops at start with the reason instead of failing every query;
  with short-circuit on it logs `hdfs=shortCircuitOnly` (blocks without a replica on this node cannot be read).
- `service-hdfs.yml`: new cell topology `ha-krb-sasl` (DataNode with `dfs.data.transfer.protection=authentication`; a
  plain TCP read is refused, a SASL one passes, the server answers TPC-H 22/22 short-circuit only), and every cell with
  short-circuit configured fails when the server reads a block over TCP or makes no short-circuit fd request.

### Added

- **CI: service matrix, HDFS part, and the pre-release gate** (core lane, item 25): workflow `service-hdfs.yml`
  (input `server` = a pre-release id, else a build of the commit): one server node on a real HDFS (apache/hadoop
  3.4.1, short-circuit configured) in three topologies - `plain` (one NameNode), `ha` (nameservice `ns1`, nn1
  standby / nn2 active, so the client must fail over) and `ha-krb` (HA + Kerberos with an MIT KDC container; the
  server logs in itself with `hdfsKerberosPrincipal` / `hdfsKerberosKeytab`, no kinit) - each in `balanced` and
  `speed`; hadoopfs from `-Darrowflight.duckdb.extensionDir` only; TPC-H SF1 22/22 checked against in-process
  DuckDB, metadata and a prepared query (`tools/ci/service-hdfs-cell.sh`). Dev CI `package=true` now publishes the
  pre-release only when the latest run of every `service-*.yml` on the commit is green: job `service-gate`
  dispatches missing or cancelled runs on the branch and waits for them (`tools/ci/service-gate.sh`).
- CI: service matrix, cluster part (lane 3, STATUS item 25): workflow `service-cluster.yml` (dispatch with input
  `server` = a pre-release id, else a build of the commit): a 3-node cluster on HDFS (NameNode + 3 DataNodes, each
  server next to its DataNode with short-circuit reads) and on S3 (MinIO), cells balanced / speed / speed +
  `distributedComplete=true` / a node killed under load; TPC-H SF1 22/22 checked against in-process DuckDB, Flight SQL
  metadata and a prepared query; job summary table cell -> pass/fail + first error.
  The HDFS cells load hadoopfs from `-Darrowflight.duckdb.extensionDir` only (as a real cluster runs; catches the
  2026-11-02 extensionDir bug) and note each DataNode's TCP block reads from JMX (lane 3, D13).
  A third store `hdfs-ha` (lane 3, D13 b): the same cluster on an HA nameservice `hdfs://ns1` (nn1 standby / nn2
  active, so every client fails over); its loss cell also fails the NameNodes over nn2 -> nn1 under load.
- **CI: service matrix, security part** (`service-security.yml`, item 25 / plugins lane): the server against real
  security services, in `balanced` and `speed`, with Flight TLS - OpenLDAP login (search mode, service account,
  group search) and Keycloak JWT login (JWKS over HTTPS, audience and groups claims); each cell checks refused
  missing/wrong credentials, GetSqlInfo, GetTables, a table schema, TPC-H SF1 22/22 equal to DuckDB's in-process
  answers and a prepared query with bound parameters (`tools/ci/service-check.py`, ADBC Flight SQL). Runs on
  dispatch (input `server` = a pre-release tag, default this commit) and on pushes touching its files. Cell
  `hive`: a real Hive 3.1.3 (HiveServer2, then a standalone metastore on the same Derby database) with TPC-H SF1 as
  external tables in Parquet and text (files by DuckDB) and ORC and Avro (files written by Hive's own INSERT
  OVERWRITE), plus a view joining all four formats; the server reads them through the packaged hive plugin and the
  same checks run against each format's schema, plus the view's answer (`tools/ci/service-hive-data.py`,
  `service-hive.sh`). Cell `ranger`: Apache Ranger admin 2.7.0 (PostgreSQL) with policies in its Hive service for
  the OpenLDAP group `analysts` - select on two of the Hive cell's databases (a third is refused), a
  `MASK_SHOW_LAST_4` mask on `customer.c_phone`, a row filter on `orders`; the server with `ranger-hive` answers the
  22 queries, the prepared query and direct mask / filter / refusal probes as DuckDB computes them under the same
  mask and filter (`tools/ci/service-ranger.sh`, `service-ranger-data.py`, `service-check.py --extra`).
- CI: service matrix, stores part (lane 4, STATUS item 25): workflow `service-stores.yml` (dispatch with input `server` =
  a pre-release tag, default = build of the commit; on push to its files). Each cell starts the real service as
  containers (MinIO with two s3 stores, an Iceberg Hadoop catalog on MinIO, Lakekeeper (Iceberg REST) on MinIO with
  vended STS credentials only, Apache Ozone's S3 gateway, lakeFS on a branch and on a pinned commit, PostgreSQL and
  MySQL through the jdbc plugin), the server with `balanced` and `-DresourceProfile=speed`, and checks
  TPC-H SF1 22/22 answers against in-process DuckDB, Flight SQL metadata and a prepared query
  (`tools/ci/service-tpch.py`, `tools/ci/service-stores-cell.sh`, `tools/ci/service-iceberg.py`); the job summary
  is the service x profile table.

- **Resource profile `speed`** (item 23): `resourceProfile` / `FLIGHT_RESOURCE_PROFILE`, default `balanced` (nothing
  changes). `speed` supplies defaults for the settings measured as faster: shared DuckDB with all cores
  (`duckDbSharedDatabase=true`, `duckDbSharedMaxThreads=0`, `duckDbThreads`=cores, at least `balanced`'s 4 per query), node-wide `duckDbMemoryLimit` of
  what `balanced` gives its expected connections together (per-connection limit x connections, at most 80 % of memory;
  item 23b - it was 75 % of memory minus heap and direct memory), `flightZeroCopyWrites=true`, `endpointsPerServer=4`,
  `flightBackpressureThresholdBytes`=16 MiB; the s3/lakeFS read caches stay as configured (with them on, `speed` was
  1.33x slower on S3, TPC-H SF10). `-D` and environment settings win; startup line `resourceProfile profile=... (explicit)`. Unknown profile names stop the server.
- SPI 1.8: `ColumnSpec.sourceType` - the column type as the catalog declares it (a Hive type such as `char(10)`),
  null when unknown; the four-argument constructor stays. Plugins built against 1.8 need a 1.8 server.
- `ranger-hive` applies Ranger Hive row filters whose expression is in a typed subset (`HiveRowFilter`: columns,
  string/integer literals, comparisons of one kind, IS NULL, IN, BETWEEN, LIKE, AND/OR/NOT, lower/upper,
  `current_user()`); other filters still refuse the table. (P6 step 3 D.)
- `ranger-hive` applies Ranger Hive column masks: a masked column is read as Hive 3.1.3's mask written in DuckDB
  SQL (same type), in the table and in views over it, both result modes (answered complete). Tables and databases
  are visible with any access (as Hive's SHOW TABLES / SHOW DATABASES; listings no longer audited); a query table
  needs `select` on all its columns. Masks use the Hive type the hive catalog declares (SPI 1.8 `sourceType`;
  CHAR(n) masked padded); still refused: non-NULL masks on nested columns, and show-last masks on text columns of
  tables without a declared Hive type. (P6 step 3 C.)
- Authorization SPI: row filters and column masks. `FlightAuthorization.restrictsTables()` (default false) and
  `restrictTable(principal, schema, table, columns)` -> `TableRestriction` (allowed, row filter SQL, mask SQL per
  column; default = `canReadTable`); `columns` = every column as `ColumnSpec` (DuckDB type, catalog type). For a provider that restricts, each reference to a restricted table (after
  view expansion) is read as `(SELECT * REPLACE (CAST((<mask>) AS <type>) AS c, ...) FROM s.t WHERE (<filter>))`;
  such queries are answered complete by one node (also when asked in parts, one endpoint), the restriction is
  derived again at DoGet for the redeeming user (a partial ticket of an unrestricted user is refused), table
  statistics of a restricted table are refused, and anything the rewrite cannot reach (a reference the finder
  misses, time travel on a restricted table, an unknown mask column, a failing provider) is UNAUTHORIZED. No
  change for existing providers. (P6 step 3 A/B; test `TableRestrictionIntegrationTest`, `QueryRestrictionsTest`.)
- Iceberg REST vended credentials (lane 4, C1 step 2b; opt-in `arrowflight.iceberg-rest.vendedCredentials`, default
  false): loadTable asks for temporary S3 credentials (`X-Iceberg-Access-Delegation: vended-credentials`) and DuckDB
  reads the table's files with them. SPI 1.7 `TableSnapshot.credentials` (`StorageCredential`: prefix, type,
  options, expiry); the server installs each as a temporary DuckDB secret scoped to the directory of the snapshot's
  files below the prefix, only below the catalog's roots, refreshed when the catalog returns new ones (the plugin
  reloads a table 5 minutes before expiry) and dropped when it stops; values never logged. Catalogs CI entry
  `lakekeeper-s3-vended`.
- New plugin `flight-plugin-jdbc` (`plugins/jdbc`, `catalog=jdbc`; lane 4, C2): tables of PostgreSQL and MySQL
  databases, read-only through DuckDB's `postgres_scanner` / `mysql_scanner` v1.5.6 (projection and filter pushdown
  by the scanners). `arrowflight.jdbc.sources=<s>,...`; per source `<s>.type` (postgres|mysql), `.host`, `.port`,
  `.database`, `.user`, `.tables` (`schema.table`, comma separated: only these become server tables, named in lower
  case, in schema `.schema`, default `<s>`), `.sslMode` (mysql); the password only from `FLIGHT_JDBC_<S>_PASSWORD`
  or `<s>.passwordFile`, redacted everywhere. Each query of such a table is answered complete by one node (partial
  requests get one endpoint). Client queries cannot call the scanners' functions (`postgres_query`, `postgres_scan`,
  `mysql_query`, ...: the plugin's list plus the SPI 1.9 check), name the attachments, ATTACH or DETACH. Tests:
  `JdbcCatalogTest`, `ScannerFunctionsTest`, `JdbcFederationIntegrationTest` (Catalogs CI job `jdbc-scanners`,
  PostgreSQL 16.4 and MySQL 8.4 with type matrices: answers equal DuckDB's own scan, partial = complete).
- SPI 1.10: attached remote databases, for the coming jdbc federation plugin (lane 4, C2). `DuckDbSetup
  .attachDatabase(name, type, options)` - `postgres` / `mysql`, only after the type's scanner extension was loaded
  with `loadExtensionRefusingFunctions`; the server writes a temporary secret (allowlisted options HOST, PORT,
  DATABASE, USER, PASSWORD, mysql also SSL_MODE / SSL_CA; sensitive values redacted) and `ATTACH '' AS name (...,
  READ_ONLY)` before the lockdown, and refuses the attachment's name in client SQL (as catalog or schema
  qualifier). `ReadSpec.AttachedTable(database, schema, table)` - a catalog table read through the attachment
  (declared columns, or all), no files, answered complete by one node; table statistics report it as unknown.
  No change for existing plugins.
- SPI 1.9: `DuckDbSetup.loadExtensionRefusingFunctions(name)` - loads an extension and refuses in client queries
  every function and table function it adds (the server compares `duckdb_functions()` before and after the load, so
  a later build of the extension cannot add an unrefused one; names that existed before stay allowed; startup line
  `duckdb=extensionFunctionsRefused`). For plugins whose extensions do their own I/O (the coming jdbc federation
  plugin). No change for existing plugins. (Lane 4, Q4 (b); tests `DuckDbSetupValidatorTest`, `DuckDbAdapterTest`.)
- Iceberg REST vended credentials, manifests (lane 4, C1 step 2b (ii)): with `vendedCredentials` the plugin also
  reads a table's manifest lists and manifests with the vended S3 credentials (its own SigV4-signed HEAD / ranged GET
  on the JDK HTTP client, no AWS SDK; endpoint, region, path style and session token from the vended config), for
  files below a credential's prefix and below the catalog's roots; everything else still goes through the server's
  stores. So a deployment whose server keys cannot read the warehouse can plan the table. No change without the
  setting. Tests `VendedManifestReadsTest` (signatures checked with the s3 plugin's verifier, server stores refusing
  every read) and the `lakekeeper-s3-vended` conformance entry (plugin store refuses every read).
- `duckDbSharedMaxThreads` (`DUCKDB_SHARED_MAX_THREADS`, `arrowflight.duckdb.sharedMaxThreads`, default 8, 0 = no
  cap): with the opt-in `duckDbSharedDatabase` and `duckDbThreads` unset, the node's DuckDB gets min(cores, cap)
  threads instead of all cores (stand p22s at 16 cores: c1/c8 small-query p50 and mixed p99 were worse with 16
  threads). An explicit `duckDbThreads` still wins; the default path (no shared DuckDB) is unchanged.
- Distributed complete mode (opt-in `distributedComplete`) merges plain `avg` items (lane 3, D10): the parts are
  planned with `sum` and `count` in place of each average and the merge divides as DuckDB's `avg` does (DECIMAL
  and integer averages bit-identical to the single-node answer while the unscaled sum is below 2^53). TPC-H q01
  is now computed by the cluster; `avg(DISTINCT ...)`, `FILTER`/`OVER` and `ORDER BY avg(...)` keep the
  single-node path.
- Manual workflow `distributed-join.yml` (lane 3, D9) and `NodeJoinUnderLoadIntegrationTest`: a node joins (and
  then leaves) a cluster under query load; no wrong answers, no failures, the new node is planned on. The load
  client (`RollingRestartLoadTest`) takes `-Dload.joinServers` / `-Dload.joinFile` and prints the first time an
  endpoint is planned on each node.
- `ranger-hive` authorization provider: table decisions are audited to Ranger (one `select` per table; denials by
  column restrictions with their reason); security-zone policies covered by tests.
- Optional authorization provider `ranger-hive` (module `flight-auth-ranger-hive`, `auth/ranger-hive/` in the
  `flight-auth-...tar.zst` asset; `FLIGHT_AUTHORIZATION_PROVIDER=ranger-hive`, `RANGER_HIVE_SERVICE_NAME`,
  `RANGER_ADMIN_URL`, `RANGER_HIVE_SCHEMA_PREFIX`): the Hive catalog's schemas and tables checked against the
  cluster's Hive Ranger service (database visibility as `SHOW DATABASES`, select on all columns); a table with a mask,
  row filter or column deny for the caller, or with policies the server cannot evaluate exactly (`{OWNER}`,
  conditions, column patterns, restrictive tag policies), is refused - masks and row filters are not applied yet (P6).
- `ranger-hive`: end-to-end test in a real Flight SQL server (`RangerHiveFlightSqlTest`: masked, row-filtered and
  column-denied tables refused in queries, joins and listings in both result modes; unrestricted tables answered).
- `ranger-hive`: end-to-end test over the packaged hive plugin and a metastore (`RangerHiveCatalogTest`: ORC,
  Parquet and text tables and Hive views; a column deny, mask or row filter refuses the table and every view over it,
  a view granted without its source is refused (invoker rights), listings show only readable tables, both modes).
- Module `flight-codec-lz4` (release asset `arrow-flight-codec-lz4-<version>.jar`): the server's lz4-java
  LZ4_FRAME codec as an Arrow codec factory (service provider) for Java Flight clients, which otherwise decode LZ4
  streams with commons-compress at a few MB/s to ~180 MB/s; with this jar and lz4-java ahead of
  `arrow-compression` on the class path ~2 GB/s per thread. Decoding now writes frames of independent blocks (the
  server's, commons-compress's) block by block into the output buffer; other frames go through lz4-java's stream as
  before. The server's compressed bytes are unchanged.
- Optional setting `shutdownDrainSeconds` (`FLIGHT_SHUTDOWN_DRAIN_SECONDS`, default 0 = unchanged): a stopped node
  leaves the cluster's pool, refuses new calls with `UNAVAILABLE` and lets its running answers finish (up to that many
  seconds) before it closes; without it a graceful stop cuts running answers with `INTERNAL` (found by D8).
- Manual workflow `distributed-restart.yml` (lane 3, D8) and `RollingRestartLoadTest`: three server containers
  restarted one by one (graceful, then kill -9) under a mixed query load; fails on a wrong answer or a failure with a
  code that is not retryable, reports errors and recovery times per node.
- Manual workflow `distributed-perf.yml` (lane 3, D7): three server containers of the ref (one cluster, shared
  TPC-H lineitem directory, `--cpus`/`--memory` per node) measured with `distributedComplete` off vs on over seven
  lineitem shapes that merge; fails when the answers differ or an "on" answer was not merged, reports times and rows/s.
- Custom authorization providers (`FlightAuthorizationProviderFactory`) can override
  `create(environment, providerGroups)` to get the groups the authentication provider (LDAP, JWT) knows for a caller,
  as the built-in `ranger` provider does; the default calls `create(environment)`, so existing providers are unchanged.
- Manual workflow `tpch-perf.yml` takes `store=s3`: the TPC-H Parquet files served by a local S3 endpoint and read
  through the s3 plugin, to compare builds on S3; `s3latency` (e.g. `10ms`) adds a round trip to that endpoint only.
- `tpch-perf.yml` input `clients=N`: every pass on N concurrent Java clients (each starting at another query, answers
  checked per client, failed queries reported per query); every configuration reports the container's peak memory.
- `tpch-perf.yml` input `client=adbc` runs the 22 queries through the ADBC Flight SQL driver (Go, via Python;
  `tools/ci/tpch-adbc.py`) instead of the Java Flight SQL client, as the CI screening measures.
- Plain Flight descriptors for generic Flight clients (ClickHouse `arrowFlight`, plain Flight libraries), with the
  new optional setting `plainFlightCommands` (`FLIGHT_PLAIN_COMMANDS`, default `false`: refused as before):
  `GetFlightInfo` / `GetSchema` with a command descriptor holding UTF-8 SQL text, or a path descriptor naming a table
  (`schema.table`, plain identifiers -> `SELECT * FROM schema.table`), then `DoGet` on the tickets. Flight SQL
  commands keep their handling; same authentication and Ranger checks as Flight SQL queries; the answer is complete
  unless the client sends `x-flight-result-mode: partial` (P5). Checked in CI with pyarrow, ClickHouse 26.8 and
  DuckDB's `airport` extension; the latter is not supported (it needs its own `endpoints` action and fails clearly).
- With `duckDbSharedDatabase`, `duckDbStreamingBufferSize` now applies instead of being ignored: new setting
  `duckDbSharedBufferedConnections` (`DUCKDB_SHARED_BUFFERED_CONNECTIONS`, default 64; 0 = ignored as before)
  connections get the buffer before the lockdown and are lent to worker connections, given back without temporary
  views and catalog when a worker thread ends; workers beyond them use DuckDB's default buffer (one WARN). Without
  `duckDbSharedDatabase` or `duckDbStreamingBufferSize` nothing changes (item 22 shared DuckDB step (c)).
- Hive plugin: insert-only (MM) transactional tables with the new optional setting `hive.insertOnlyTables=true`
  (default `false`: skipped as before; Hive 3 metastores). The plugin takes one snapshot of committed writes per plan
  (`get_open_txns` + `get_valid_write_ids`) and reads in every table/partition directory what Hive reads at that
  snapshot: the newest usable `base_N`, the committed `delta_x_y[_stmt]` above it, original files only without a
  base; aborted and open writes and `_tmp` / hidden directories are never read. Layouts that could give wrong rows
  (`delete_delta_*`, a compacted base newer than the snapshot, multi-write deltas with aborted ids, overlapping
  deltas) fail the query with a message instead of being read in part. Full ACID tables stay skipped. Covered
  through the server for Parquet, ORC and text (with `hive.textTables=true`, gzip deltas included) tables, and
  against tables written by a real Hive 4.0.1 (inserts, failed inserts, INSERT OVERWRITE, major and minor
  compaction; Catalogs CI job `hive-writer`).
- Hive plugin: large Avro data files and uncompressed text files (text, CSV, JSON tables without header lines) of a
  scan are read as byte ranges decoded in parallel (Avro from the next sync marker, text from the next line start, as
  Hadoop splits them; files over 8 MiB, like ORC files since P1), instead of one thread per file. Same rows in the
  same order; compressed text files and tables with `skip.header.line.count` are still read whole. Avro records
  decode only the requested columns (others are skipped through Avro's resolver, so a file that does not resolve
  still fails as in Hive). Text tables parse plain decimals (up to 18 digits), `yyyy-mm-dd` dates and ASCII strings
  without intermediate objects (about 2.5x faster on a Q1-style column set, same values) (P4 (d)).
- With `duckDbSharedDatabase`, queries are described in one node-wide describe DuckDB (reads no files, 1 thread,
  256 MB, as before) with a catalog per worker connection, instead of one small describe DuckDB per worker; replaced
  as a whole when invalidated (item 22 shared DuckDB step (a)).
- Setting `duckDbSharedDatabase` (`DUCKDB_SHARED_DATABASE`, default false = unchanged): every worker connection
  connects to one DuckDB of the node (one thread pool and one `memory_limit`; storage setup, secrets and lockdown run
  once and are checked at startup); each connection keeps the catalog views in an in-memory catalog of its own, and
  client SQL naming such a `flight_session_<n>` catalog or schema is refused. With it, `duckDbThreads` /
  `duckDbMemoryLimit` size the node (unset = cores / whole DuckDB budget); `duckDbThreadsUnderLoad`,
  `duckDbThreadsWhenIdle` and `duckDbStreamingBufferSize` are ignored. A DuckDB invalidated by a FATAL error is
  replaced as a whole. Manual workflow `tpch-perf.yml` takes `extra` server options, measured and answer-checked
  against the default (item 22 step 1).
- Queries of catalog views (Hive views with the opt-in `hive.views`; nothing changes without it): a view named with
  its schema (`FROM h_db.my_view`, also in joins, subqueries and other views) reads its translated query over the
  tables; such queries are always answered completely, also when parts are asked for (one endpoint). Access needs the
  view and every table and view it reads (invoker rights, stricter than Hive's definer rights - see Open questions in
  `docs/dev/PLUGINS-STATUS.md`). GetTables lists views as `VIEW` (only those the caller may read with their sources),
  GetTableTypes adds `VIEW` when a catalog shows views. Refused: a view read `FOR VERSION/TIMESTAMP AS OF`, views
  nested deeper than 16; a view named like a table of its schema is not shown (the table wins); an unqualified view
  name is a missing table (P4 views part 3).
- Setting `flightLoadReservationSeconds` (`FLIGHT_LOAD_RESERVATION_SECONDS`, default 600 = unchanged): how long a
  planned endpoint's bytes count as load on its node when the client neither reads it to the end nor sends
  `CancelFlightInfo`; a shorter value frees nodes sooner from plans clients abandon (3-node S3 stand finding (b),
  README "Cancelling a plan"). Values below 1 stop the start.
- SPI 1.6: `Catalog.views(schema)` (default none) and `CatalogView(name, sql, sources, columns, fingerprint)`. Hive
  plugin (descriptor now SPI 1.6): opt-in `hive.views` (default false) lists simple virtual views whose expanded text
  `HiveViewSql` translates and whose every source is a table (or a listed view) the catalog shows; casts, untranslated
  constructs, sources not shown (another format without its opt-in, a database outside `hive.databases`), cycles and
  materialized views keep a view skipped with a log reason. The server does not read catalog views yet, so nothing
  changes for clients (P4 views part 2).
- Iceberg time travel for Iceberg tables registered in a Hive Metastore (item 3 (b)): `FOR VERSION AS OF` /
  `FOR TIMESTAMP AS OF` after such a table name reads the snapshot through the iceberg plugin, as for the iceberg
  catalogs (both result modes); other Hive tables still refuse the clause. SPI 1.5: `TableFormat.asOf(table,
  metadataLocation, point)` (default refuses); the hive plugin descriptor now names SPI 1.5.
- Hive plugin: Avro tables whose schema is given by `avro.schema.url` (under `hive.avroTables`; such tables were
  skipped). The schema file is read through the server's stores only when it lies under `hive.roots` (an HTTP URL or
  a file elsewhere keeps the table skipped with a log reason), at most 4 MiB, kept per location and read again when
  its size or modification time changes; a literal still wins over a URL, as in Hive. A partition's own URL is read
  the same way; a changed schema file makes queries planned before it fail clearly (P4).
- Iceberg time travel to snapshots whose files cannot be read one by one (deletion vectors, renamed nested fields;
  item 3 (c)): such a snapshot is now read by DuckDB's `iceberg_scan(..., snapshot_from_id => <id>)` instead of
  being refused, and only when DuckDB reads it with the snapshot's own columns (names and types, nested field names
  included) - otherwise refused as before. DuckDB 1.5.6 reads both snapshots of a table before and after a nested
  rename with their own field names (`IcebergCatalogIntegrationTest`, complete and partial mode). Log line
  `views=icebergSnapshotRead`.
- Hive plugin: Avro tables (`AvroSerDe` + `AvroContainerInputFormat`), opt-in `hive.avroTables` (default false; such
  tables were and stay skipped without it), read by the plugin as Hive 3.1.3 reads them and handed over as Arrow
  streams (both result modes, partition pruning). Reader schema from `avro.schema.literal` (table properties over
  serde properties) or made from the columns as Hive's `TypeInfoToSchema`; the table is shown only when that schema
  gives the metastore's columns (`avro.schema.url`, unions other than with null, Avro types without a Hive column
  type: skipped with a log reason). Each file is resolved from its own schema to the table's (promotions, defaults);
  decimals at the file's scale, char/varchar cut to the file's maxLength, timestamps in the file's
  `writer.time.zone` (a file without it fails on its first timestamp: Hive shows them in its server's zone); a
  partition with another schema, a file cut short and records Hive fails on fail the query naming the file. Codecs
  null/deflate/snappy/bzip2/zstandard. New plugin dependencies: Avro 1.12.0, snappy-java, Jackson 2.17 (P4).
- `dataDirShared` (`FLIGHT_DATA_DIR_SHARED`, default false): declares a local data directory a mount every node reads
  (NFS), so a cluster plans complete answers and spreads files over all nodes as on HDFS. Distributed complete mode
  tested between two server processes with TLS and the token provider (`DistributedTwoProcessTlsTest`: answers equal
  DuckDB's, merged from 4 endpoints on 2 nodes; a call without the token and a forged internal identity are refused)
  (item 18, D2 (c) part 1).
- Startup line `duckdb=externalFileCache perConnection= expectedConnections= expectedMiB= memoryMiB=` when
  `duckDbExternalFileCache=true`, and a WARN `duckdb=externalFileCacheUnbounded` when it is on without a per-connection
  `memory_limit` (`duckDbMemoryLimit=none`). Measured on DuckDB 1.5.6: the cache stays within each connection's
  `memory_limit` (evicted like any buffer), so the node bound is that limit times the live connections; pinned in
  `S3IntegrationTest.theExternalFileCacheStaysWithinTheMemoryLimit` (item 13 (c)). Default unchanged (off).
- Distributed complete mode (opt-in `distributedComplete`): an endpoint answer that fails before its schema (peer's
  Flight service down, connection refused) is read from the endpoint's backup locations in order, as clients fail
  over, before the whole query falls back to the single-node path; `DistributedComplete.failovers()` and a
  `distributedComplete=failover` log line (item 18, D2 (c) part 2). A failure after the schema still falls back.
- Distributed complete mode: `DistributedCompletePerfTest` (`perf`, item 18 step 3, D2 (b)) - three nodes, Q1/Q6-like
  aggregates, wide DISTINCT, ORDER BY LIMIT, wide GROUP BY and wide rows with `distributedComplete` off vs on (wall
  time, peer MB, peer wait); `DistributedComplete.peerBytes()` / `peerWaitNanos()` and `peerBytes=` / `peerWaitMs=` in
  its `merged` log line.
- `duckDbThreadsWhenIdle` (item 19b step 2), opt-in: `arrowflight.duckdb.threadsWhenIdle` / `DUCKDB_THREADS_WHEN_IDLE`
  (default 0 = off). Each Flight worker gets, on first use, a further DuckDB connection with that many threads (at
  most the cores); a DoGet that starts while no other runs on the node uses it to its end. Values not above
  `duckDbThreads` are off. Startup line `duckdb=threads ... whenIdle=`.
- S3 / lakeFS read cache disk tier (item 17 (a)), opt-in: `arrowflight.s3.cacheDiskPath` (absolute directory, default
  empty = memory only) + `arrowflight.s3.cacheDiskMegabytes` (size cap; both needed, and `cacheMegabytes` > 0); same
  keys under `arrowflight.lakefs.`. Blocks read from S3 are also written (one background writer, queue of 64 blocks,
  dropped when full) to files under `<path>/<plugin>-cache/`, least recently used deleted beyond the cap; a block the
  memory tier no longer holds is read from its file before S3 and kept in memory again. Each file's CRC-32C is checked
  on read (a damaged, short or deleted file is a miss read from S3, never an answer); files of an earlier run are
  deleted at start and all at close; a lock file `<path>/<plugin>-cache.lock` refuses a second server on the same
  directory; directory mode 700. `cacheAdmission=reuse` applies to each tier with its own reach. Metrics
  `arrowflight_s3_cache_disk_*` (capacity/used bytes, blocks, hits, misses, writes, evictions, dropped, errors,
  not_admitted), `disk...=` counts in the `s3=cacheClosed` line, `diskPath=` / `diskCapacityBytes=` in `s3=cacheOpen`.
- Hive JSON tables (P4 step 2c) under `hive.textTables`: `org.apache.hive.hcatalog.data.JsonSerDe` and
  `org.apache.hadoop.hive.serde2.JsonSerDe` (TextInputFormat; OpenX's serde stays skipped), one JSON object per line
  parsed exactly as Hive 3.1.3's serde does, with the same parser (Jackson 1.9 `jackson-core-asl` 1.9.13, new plugin
  dependency): keys matched ignoring case, unknown keys skipped, a later key wins, HCatalog's `_col<n>` keys; numbers
  through Jackson's accessors (fractions cut for integer columns, tinyint up to 255 wraps), booleans true only for
  `true`, strings from any scalar, binary as the text's UTF-8 bytes, Hive's lenient `Date.valueOf` /
  `Timestamp.valueOf`, decimals rounded half up (unreadable or too wide NULL), varchar/char cut; arrays, maps (keys
  read as their type) and structs, nested values also where Hive's token walk goes astray. A line Hive's serde fails
  on (not an object, a quoted number for a number column, an unreadable date or timestamp, an empty line) fails the
  query naming file and line, never a silent NULL; so do rows Arrow cannot hold (NULL or duplicate map keys).
  Header lines and `.gz`/`.deflate`/`.bz2` as for other text tables; `timestamp.formats` and footer lines skip the
  table; a partition with the other JSON serde fails the query.
- Hive `OpenCSVSerde` tables (P4 step 2c) under `hive.textTables`: lines parsed with opencsv 2.3 (new plugin
  dependency, the version Hive 3.1.3 uses) exactly as Hive's serde does: `separatorChar` / `quoteChar` /
  `escapeChar` (default `,` `"` and, for the default escape `"`, opencsv's backslash), header lines, missing
  fields NULL, empty line all NULL, no NULL text. Every column is a string, as Hive reads them; tables that
  declare other column types stay skipped with a log reason, as do footer lines.
- Hive text tables with nested columns (P4 step 2): arrays, maps and structs of `LazySimpleSerDe` tables
  (`hive.textTables`) are read as Hive reads them: split at `collection.delim` (or the misspelt
  `colelction.delim`, default byte 2), `mapkey.delim` (default 3) and bytes 4-8 for deeper levels, up to Hive's 7
  nesting levels; an empty field is an empty array or map, a map entry without a key separator has a NULL value,
  entries with a NULL or unreadable key are dropped and of equal keys the first is kept; struct fields by
  position. Tables with unions, non-primitive map keys or deeper nesting stay skipped with a log reason. New
  end-to-end test `HiveTextIntegrationTest` (text vs Parquet of the same rows through the server, both result
  modes, partition pruning, joins). Text tables' schema IDs change (the separators are part of them).
- Hive text tables (P4 step 1), opt-in `hive.textTables` (default false; text tables were and stay skipped
  without it): `STORED AS TEXTFILE` tables of `LazySimpleSerDe` are read by the hive plugin and handed to DuckDB
  as Arrow streams (the ORC path, both result modes), as Hive reads them: `field.delim` / `serialization.format`,
  `escape.delim` (+ `serialization.escape.crlf`), `serialization.null.format`, `skip.header.line.count`,
  `serialization.last.column.takes.rest`, rows ending at LF/CR/CRLF, missing fields NULL, fields that do not read
  as their type NULL; `.gz`, `.deflate`, `.bz2` files. Primitive columns only for now; tables with nested
  columns, footer lines, `timestamp.formats`, another encoding or line delimiter, and other text serdes (CSV,
  JSON, Regex) stay skipped with a log reason; a partition with its own serde properties fails the query.
- Hudi on S3 probe (C4 (b)): `DuckDbPaimonHudiProbe` reads every Hudi fixture on S3 through `read_hudi`'s
  `hudi_option` (`;`-separated object_store keys such as `aws_endpoint_url`, `aws_access_key_id`), and records that
  the same options reach S3 locations outside `allowed_directories`. Test only.
- Paimon / Hudi lockdown probe (C4 (b)): `tools/ci/DuckDbPaimonHudiProbe.java` (Catalogs CI job `paimon-hudi`)
  loads DuckDB's `paimon` and `hudi` v1.5.6 extensions into a connection locked down as the server's query
  connections and records answers (local, S3 on moto, HDFS) and what gets past the lockdown (tables outside
  `allowed_directories`, other hosts, metadata services, attach). The extensions are kept as a workflow artifact.
  Test only.
- Paimon / Hudi light-reader probe (C4 (b)): `catalogs-ci.yml` job `paimon-hudi` writes Apache Paimon 1.2.0 and
  Apache Hudi 0.15.0 / 1.0.2 tables with Spark 3.5.3 (`tools/ci/paimon-hudi-fixtures.py`: append and primary-key
  tables, upserts, deletes, deletion vectors, full compaction, INSERT OVERWRITE, schema evolution; copy-on-write,
  merge-on-read with and without compaction, a commit left inflight) and `tools/ci/paimon-hudi-probe.py` answers
  them from the formats' own metadata plus DuckDB `read_parquet` (no Paimon / Hudi library), compared with
  Spark's answers; a wrong answer fails the job, unsupported layouts are refused. Also records whether DuckDB
  paimon / hudi extensions exist for v1.5.6. Test only.
- Unity Catalog OSS as an Iceberg REST catalog (C1 step 4): `catalogs-ci.yml` entry `unity-catalog`
  (`unitycatalog/unitycatalog:v0.7.0`, managed tables on a local storage root) runs both REST conformance tests;
  answers equal the Hadoop and Hive metastore references. Unity Catalog refuses Iceberg `time` columns (new
  optional `ICEBERG_REST_IT_OMIT_COLUMNS` leaves them out of both sides) and its sample table
  `default.marksheet_uniform` answers 500 (the server test shows namespace `ice` only,
  `ICEBERG_REST_IT_NAMESPACES`). Test only.
- Apache Ozone as an S3 backend (C3): `catalogs-ci.yml` entry `iceberg-rest-fixture-ozone-s3` runs both REST
  conformance tests with the warehouse on Ozone 2.0.0's S3 gateway (`tools/ci/ozone-s3.sh`: SCM, OM, one datanode,
  S3 gateway; non-secure, replication ONE) instead of moto: Iceberg S3FileIO writer, the s3 plugin and DuckDB httpfs
  read it; answers equal the Hadoop reference. Test only.
- DuckDB delta extension probe (C3 step 1): `catalogs-ci.yml` job `delta` writes Delta tables with Spark 3.5.3 +
  delta-spark 3.3.2 (`tools/ci/delta-fixtures.py`: partitions, deletion vectors, column mapping by name and id,
  checkpoints, versions, schema evolution, types; Spark's answers in `expected.tsv`) and
  `tools/ci/DuckDbDeltaProbe.java` reads them with `delta` v1.5.6 under the server's lockdown, locally and on S3
  (moto), and records what is read correctly and whether paths outside `allowed_directories` are refused. Test only.
- Delta on HDFS probe (C3): the `delta` job also puts the fixtures on a single-node HDFS (`tools/ci/hdfs-single.sh`,
  `apache/hadoop:3.4.1`) and probes them with our hadoopfs build loaded (`PROBE_HDFS_ROOT`; the probe runs in an
  Ubuntu 20.04 JDK container, the hadoopfs build links libssl 1.1). Result: hadoopfs reads the Parquet files, but
  `delta` v1.5.6 does not read `hdfs://` tables (delta-kernel: `Unable to recognise URL`). Test only.
- DuckDB postgres / mysql scanner probe (C2 step 1): `catalogs-ci.yml` job `jdbc-scanners`
  (`tools/ci/DuckDbScannerProbe.java`) loads `postgres_scanner` / `mysql_scanner` v1.5.6 by path, attaches
  PostgreSQL 16 / MySQL 8.4 read-only with a temporary secret, applies the server's lockdown and records which
  reads, pushdowns and client statements get through (results in docs/dev/CATALOGS-STATUS.md). Test only.
- Iceberg REST catalog tables registered in a Hive metastore (C1 step 3c): on local warehouses
  `RestCatalogServerConformanceTest` registers the REST catalog's metadata files in the in-process metastore (schema
  `hms_ice`, hive plugin -> iceberg plugin); answers equal the REST catalog's, partial equal complete. Test only.
- Iceberg REST catalogs through the server on S3 warehouses (C1 step 3b): `RestCatalogServerConformanceTest` also
  runs for polaris-s3, iceberg-rest-fixture-s3 and lakekeeper-s3, the server reading the tables through the s3 plugin
  (static keys); the iceberg module's `fixture-libs` carry Iceberg's S3FileIO and the AWS SDK. Test only.
- Iceberg REST catalogs through the server (C1 step 3, local warehouses): `RestCatalogServerConformanceTest` runs the
  server with the iceberg plugin's iceberg-rest catalog on Polaris / the Iceberg REST fixture next to an iceberg-hadoop
  reference with the same tables; complete answers equal the reference, partial equal complete. Test only
  (`catalogs-ci.yml`; `IcebergFixtures.writeRest`).
- Iceberg REST catalog conformance on S3 (C1 step 2a): `catalogs-ci.yml` also runs `RestCatalogConformanceTest`
  with the warehouse in an S3 bucket (moto's S3 server; MinIO's images no longer pull anonymously) for Apache
  Polaris 1.2.0 (S3 storage without STS), the Iceberg REST fixture 1.10.0 (S3FileIO) and Lakekeeper 0.13.8
  (PostgreSQL backend, S3 warehouse); the plugin reads metadata through the s3 plugin's store and DuckDB reads the
  rows over httpfs. Static keys only (vended credentials are step 2b). Test only, no runtime change.
- Iceberg REST catalog conformance (C1 step 1): new workflow `catalogs-ci.yml` starts Apache Polaris 1.2.0 (OAuth2
  client credentials, prefix from `/v1/config`) and the Apache Iceberg REST fixture 1.10.0 as containers and runs
  `RestCatalogConformanceTest`: the fixture tables are written through each catalog with Iceberg Java's RESTCatalog
  and must look to the `iceberg-rest` catalog exactly as the same tables in a Hadoop-layout warehouse (names,
  columns, files, row and delete counts, partial-read problems, DuckDB `iceberg_scan` rows). Test only, no runtime
  change.
- Distributed complete mode, endpoint answers cut after the client's first batch (lane 3 D5):
  `DistributedCompleteCutAfterFirstBatchTest` passes the endpoint answers through a throttled TCP relay, uses
  1024-row batches so that DuckDB sends the first batch before the endpoint answers are read, and resets the relay
  once the client has it: the client gets an error (INTERNAL, naming the peer's UNAVAILABLE), never a silently
  partial answer, and the node neither merges nor falls back. `CuttingRelay` gained a rate limit and `cutNow()`.
  Test only, no runtime change.
- Distributed complete mode, 3-node test (item 18 step 3, lane 3 D2): `DistributedClusterIntegrationTest` runs
  three complete-mode nodes on one Hazelcast cluster in one JVM over a shared data directory (6 files, two
  endpoints per node): merged answers equal single-node answers for 9 shapes (incl. wide GROUP BY and a wide
  unordered projection), a peer stream reset mid-way (TCP relay) and a peer whose Flight service is down fall back
  to the exact single-node answer (or fail; never a partial answer), and a node that left is not planned. Test only,
  no runtime change.
- Distributed complete mode (item 18 step 2, lane 3), opt-in `distributedComplete` / `FLIGHT_DISTRIBUTED_COMPLETE`
  (default false, unchanged): the planning node of a complete answer reads the query's partial-mode endpoints from the
  nodes (Flight DoGet as the client's user, header `x-kestrel-internal-identity` signed with the ticket secret,
  accepted from live members' addresses only) and merges them in DuckDB (`CompleteAnswerMerge`, columns renamed and
  checked against the single-node answer's schema). Refusal, unmergeable shapes, one-part plans and peer failures
  before the first batch keep the single-node path. Peer TLS trust: `distributedCompleteTrustedCertificates`, default
  the node's own certificate. With the setting on, the ticket secret allows impersonation (see README).
- OIDC / Knox JWT bearer tokens (P2 step 3), new optional module `flight-auth-jwt`, `FLIGHT_AUTH_PROVIDER=jwt`
  (existing providers unchanged; release asset `flight-auth-...` now also holds `auth/jwt/`): every call's bearer JWT
  is verified with nimbus-jose-jwt 10.10 against the issuer's JWKS (`FLIGHT_JWT_JWKS_URL`, HTTPS only, cached and
  re-fetched on an unknown key id, rate limited; or `FLIGHT_JWT_JWKS_FILE`), `FLIGHT_JWT_ISSUER`,
  `FLIGHT_JWT_AUDIENCE`, expiry / not-before with `FLIGHT_JWT_CLOCK_SKEW_SECONDS` (default 60); asymmetric
  algorithms only (no HS*, no `none`). Identity from `FLIGHT_JWT_USER_CLAIM` (default `sub`), optional Ranger groups
  from `FLIGHT_JWT_GROUPS_CLAIM` (dotted path, e.g. `realm_access.roles`), opt-in `FLIGHT_JWT_BASIC_PASSWORD` for
  tools with only user/password fields. Bad tokens `UNAUTHENTICATED`, unreachable keys `UNAVAILABLE`. No core or SPI
  change.
- Small-query perf tooling (maintainer's ADBC c8 tail bug): `SmallQueryThroughputPerfTest` prints p99 and max and
  takes `-Dperf.server=host:port` / `-Dperf.dataDir` to measure a separately started server; new manual workflow
  `small-query-perf.yml` runs a baseline build and the current ref, each alone in a container with 2 CPUs by quota
  (`--cpus=2`, like the CI screening) and pinned (`--cpuset-cpus`), client on the other cores. No runtime change.
- Zero-copy Flight writes for DuckDB result streams (item 22 (d)), opt-in: `flightZeroCopyWrites` /
  `arrowflight.flight.zeroCopyWrites` / `FLIGHT_ZERO_COPY_WRITES` (default false, unchanged; only `true`/`false`).
  gRPC sends the buffers DuckDB exported for a batch instead of copying each batch first; DuckDB exports every batch
  into new buffers and Flight keeps them until written, so a batch stays intact while DuckDB computes the next ones.
  Other streams (metadata, SqlInfo) always copy. Local A/B on 2 cores (`ParallelScanPerfTest`, in-process client on
  the same cores, 6 M-row table, two rounds each): 8 endpoints 243/234 -> 245/268 MB/s at 8.05/8.34 -> 7.26/7.15 s
  CPU per GB; 1 and 4 streams within noise. `ParallelScanPerfTest` takes `-Dperf.zeroCopyWrites`.
- LDAP groups for Ranger (P2 step 2), opt-in `FLIGHT_LDAP_GROUP_SEARCH_BASE` (+ `FLIGHT_LDAP_GROUP_SEARCH_FILTER`,
  default `(|(member={1})(uniqueMember={1})(memberUid={0}))`, and `FLIGHT_LDAP_GROUP_NAME_ATTRIBUTE`, default `cn`):
  the user's directory groups, read at the login and kept for the session lifetime (another node reads them once with
  the service account, `FLIGHT_LDAP_BIND_DN` now also allowed in template mode), are added to the Hadoop group
  mapping for Ranger checks. Unreadable groups fail the call `UNAVAILABLE` / deny, never fewer groups. Core: new
  `FlightAuthenticationProvider.groups(identity)` (default none, so the built-in providers are unchanged).
- LDAP / Active Directory login (P2 step 1), new optional module `flight-auth-ldap`, `FLIGHT_AUTH_PROVIDER=ldap`
  (existing providers unchanged): a Basic user name + password (what JDBC/ODBC/ADBC and BI tools send) is checked with
  a simple bind - DN template (`FLIGHT_LDAP_USER_DN_TEMPLATE`, e.g. an AD UPN `{0}@corp.example`) or search with a
  service account (`FLIGHT_LDAP_USER_SEARCH_BASE` / `_FILTER` / `FLIGHT_LDAP_BIND_DN`, identity optionally from
  `FLIGHT_LDAP_USER_ATTRIBUTE`); ldaps or StartTLS with host name checks (clear text only with
  `FLIGHT_LDAP_ALLOW_PLAINTEXT=true`), several servers in order; the answer carries a signed session bearer token
  (`FLIGHT_LDAP_SESSION_SECRET`, shared by all nodes, `FLIGHT_LDAP_SESSION_MINUTES` default 10) so later calls skip
  the directory. Empty passwords, names outside `[letters digits . _ - @]` and names matching several entries are
  refused; a directory outage is `UNAVAILABLE`, bad credentials `UNAUTHENTICATED`. Requires Flight TLS. The identity
  reaches Ranger as a Kerberos principal's short name does. Release asset `flight-auth-<v>-<sha>.tar.zst`
  (`auth/ldap/`).
- S3 / lakeFS read cache admission (item 17 (e)), opt-in: `arrowflight.s3.cacheAdmission` /
  `arrowflight.lakefs.cacheAdmission` = `always` (default, unchanged) or `reuse`: a missed block is kept only when it
  is read again while it is among the last N blocks not kept (N = the cache's blocks), so a scan larger than the cache
  passes through and leaves what small queries read again; repeated queries hit from their third run on. Metric
  `arrowflight_<kind>_cache_not_admitted_total`. Measured on the fake S3 why a size rule would not do: DuckDB 1.5.6
  reads a scan per column chunk (2 KiB - 3 MiB GETs on a lineitem-like table, 122880-row groups) while a
  `SELECT *` point lookup reads its whole row group (4.5 MiB) in one GET.
- Cap on the streams inside DuckDB at once (item 22 (c)), opt-in: `duckDbProducingStreams` /
  `arrowflight.duckdb.producingStreams` / `DUCKDB_PRODUCING_STREAMS` (default 0 = no cap, unchanged). A DoGet holds
  one of N turns (handed out in arrival order) only while DuckDB runs its query or computes its next batch, never while
  it waits for the client or the outbound budget, so endpoints a client reads in turn cannot block each other; a client
  that goes away while its stream waits ends the wait with CANCELLED. On 2 cores (`duckDbThreads=2`, in-process
  client on the same cores, 6 M-row table, two rounds) one partial-mode scan in 8 endpoints went 310/330 -> 381/397
  MB/s at value 2 (CPU 6.2/5.8 -> 5.0/4.9 s/GB), 4 endpoints 352/346 -> 326/371; value 1 was neutral.
  `ParallelScanPerfTest` takes `-Dperf.producingStreams` and prints the waits.
- `HiveOrcPerfTest` (perf tag) and the manual workflow `hive-orc-perf.yml` (P1 step 2 (e)): a lineitem-like Hive
  table (SF1 size by default) as Parquet and as ORC (zlib, zstd) through the server with the hive plugin, best of 3
  per query and result mode; complete answers of the ORC tables must equal the Parquet table's. Test only.
- `SmallQueryThroughputPerfTest` (perf tag) can read its table from the fake S3 through the real s3 plugin
  (`-Dperf.store=s3`, `-Ds3.perf.latencyMs`, needs `-Ds3.it.httpfs`); `-Dperf.threads` unset now means the server's
  own default instead of the test helper's 1 thread. Test only.
- DuckDB threads under load (item 22, also item 21 (b) option 3), opt-in: `duckDbThreadsUnderLoad` /
  `arrowflight.duckdb.threadsUnderLoad` / `DUCKDB_THREADS_UNDER_LOAD` (default 0 = off, unchanged). Each Flight
  worker gets a second DuckDB connection with that many threads, opened on first use; a DoGet that starts while the
  running DoGets times `duckDbThreads` exceed the cores runs on it (same setup, views and answers), a query alone keeps
  all threads. On 2 cores (`duckDbThreads=2`, value 1, pyarrow client on other cores) one partial-mode scan in 4
  endpoints went 712 -> 792 MB/s and in 8 endpoints 579 -> 611 MB/s, server CPU -13 % / -6 %. New perf test
  `ParallelScanPerfTest` (one scan in N partial-mode endpoints, or N concurrent complete-mode scans; optionally
  against a separately started server).
- `SmallQueryThroughputPerfTest` (perf tag) can run plain statements (`-Dperf.client=statement`: GetFlightInfo +
  DoGet) besides the stock prepared pattern, to compare the two client patterns (item 21 (d)). Test only.
- ORC tables in the Hive catalog, opt-in (P1 step 2 (c)): with `hive.orcTables=true`
  (`arrowflight.hive.orcTables`, default false: ORC tables stay skipped as before) the hive plugin shows ORC tables
  and reads their files itself (orc-core through the server's stores), one Arrow stream per table reference with only
  the columns the query names; partition values come from the metastore. The query's simple filters on integer,
  DOUBLE, DECIMAL, STRING/VARCHAR, BOOLEAN and DATE columns skip ORC stripes and row groups (the server still applies
  every filter). Transactional (ACID) ORC tables stay skipped with a log line. End-to-end server tests follow.
- End-to-end tests of ORC Hive tables through the server with the packaged hive plugin (P1 step 2 (d),
  `HiveOrcIntegrationTest`): both result modes answer ORC tables (ZLIB, ZSTD, SNAPPY, LZ4 and uncompressed files)
  exactly like the same rows in Parquet, incl. partition pruning, filters, group by a partition column and joins of
  ORC with Parquet and with ORC; the plugin's zstd-jni works next to the server's. Test only.
- Parallel stream compression (item 20, step (f)): the buffers of a large compressed batch are compressed side by
  side by the stream's thread and helper threads shared by all streams, largest first; the batch is sent once all
  are done (byte for byte the serial batch, no extra batches held). New setting `flightCompressionThreads` /
  `arrowflight.flight.compressionThreads` (default 0 = one per core, at most 8; 1 = serial as before). Only streams
  of clients that ask for compression are affected. New manual workflow `compression-perf.yml` and perf test
  `CompressionThroughputPerfTest`: one stream over loopback slowed by `tc netem` (10 ms, 1 Gbit), plain / zstd / lz4,
  serial vs parallel (Arrow MB/s, CPU, ratio).
- Compression answer checks for non-Java clients (item 20, step (e)): `CompressionClientsIntegrationTest` reads
  a mixed-type table through pyarrow Flight (C++) and the ADBC Flight SQL driver (Go), plain, `none`, `zstd` and
  `lz4`, in both result modes, and requires identical answers and server-side compressed batches per client
  (CI installs pyarrow 26.0.0 / adbc-driver-flightsql 1.13.0; skipped without `-Dpython.it.executable`).
  START-NOTES now describe the compression opt-in and the change our patched JDBC driver needs to ask for it
  (step (d)). No runtime change.
- Complete answers over catalog tables read as Arrow streams (P1 step 2 (b) part 2 (2); no plugin produces such
  tables yet): each reference of such a table named with its schema (`FROM hive.orders o`, joins, FROM lists,
  subqueries) reads a stream of its own with the columns the query names (all for `*`, `COLUMNS(...)` or a
  whole-row use of the alias); the WHERE clause's simple conditions go along as hints only when the query is one
  SELECT of that table alone. The statement is prepared for the run only, with its parameters bound
  (`DuckDbAdapter.streamCompleteOnce`); describing reads no data. These tables have no complete-mode view, so a
  reference without the schema fails as a missing table (never an empty answer). Other tables are unchanged.
- Partial answers over catalog tables read as Arrow streams (P1 step 2 (b) part 2 (1); no plugin produces such
  tables yet): the planner ships their files with partition values like Hive Parquet files, every endpoint asks
  the catalog for one stream with the columns the query names (all for `*`, one for `count(*)`) and the simple
  conditions of the WHERE clause's top-level AND as hints (`ArrowScanRequests`), runs the query once over it
  without the statement cache (`DuckDbAdapter.streamSqlOnce`) and drops the stream afterwards; describing such a
  query reads no data (declared columns).
- Flight SQL sessions with the option `arrow-compression` (item 20, step (c)): SetSessionOptions /
  GetSessionOptions / CloseSession (were UNIMPLEMENTED) keep options per `arrow_flight_session_id` cookie on the
  node; `arrow-compression` (`none`/`zstd`/`lz4`) is the codec of the session's DoGet calls without the header
  (header first, then the session, then `flightCompression`). Unknown cookies are ignored rather than failing the
  call (unlike Arrow's own session middleware); sessions are bound to the caller's identity, expire after
  `flightSessionIdleSeconds` (3600) idle and are capped at `flightSessionMax` (10000) per node. ListActions now
  lists the three session actions.
- SPI 1.4 (P1 step 2 (b), additive, no runtime change yet): a catalog may hand table data to the server as Arrow
  streams - `ReadSpec.ArrowStream`, `ArrowScan` (columns + filter hints) and `Catalog.exportArrowScan` (default
  refuses). Server side `ArrowStreamScans` registers one DuckDB Arrow stream per table reference and drops it after
  the query (DuckDB's registered streams are read once); read_parquet sources refuse such snapshots. Plugins built
  against SPI <= 1.3 load unchanged.
- LZ4 compression of result streams (item 20, step (b)): `arrow-compression: lz4` (also `lz4_frame`) and
  `flightCompression=lz4` send Arrow LZ4_FRAME batches, compressed by our own codec on lz4-java (JNI) from the
  runtime libs set straight between Arrow buffers (~510 MB/s single thread vs ZSTD level 1 ~330 MB/s, at half
  ZSTD's ratio on small longs). Was refused before; refused still when lz4-java is not on the class path. Java
  Flight clients decode it with `arrow-compression` + `commons-compress`.
- Opt-in Zstandard compression of result streams (item 20, step 1): a client that sends `arrow-compression: zstd`
  gets Arrow IPC batches with ZSTD body compression (level `flightCompressionLevel`, default 1); batches under
  `flightCompressionMinBytes` (64 KiB) and streams whose batches do not shrink go plain; `flightCompression`
  (default `none`) sets the codec for calls without the header. Clients that do not ask get plain batches as
  before. Unknown values and `lz4` (Arrow Java's LZ4 codec is far too slow) are refused with INVALID_ARGUMENT.
  Metrics `arrowflight_flight_compress*`. New dependency `arrow-compression` (zstd-jni kept at parquet's version).
- Plugin descriptors: `share` takes a comma separated list of `hadoop` and `arrow` (was empty or `hadoop`);
  with `arrow` the plugin's class loader takes `org.apache.arrow.*` from the server (one Arrow memory and C data
  library per JVM, so a plugin can hand Arrow streams to the server) and a jar of the plugin that carries Arrow
  classes is refused. The load line logs `shareArrow=`. Existing descriptors are unchanged.
- Hive plugin (P1 step 2 (a)): descriptor `share=hadoop,arrow` - the plugin now runs on the server's Hadoop and
  Arrow classes (needed by the ORC reader); the metastore client is unchanged (Hive classes stay the plugin's).
- Hive plugin, ORC step 1 (P1, no runtime change yet): `OrcArrowReader` reads one ORC file of a Hive table as Arrow
  batches through the server's stores (orc-core 2.1.3 over a one-file Hadoop file system wrapping the store's
  `ReadableFile`): columns by name (case-insensitive) or by position for Hive 1.x `_colN` files, missing columns
  NULL, ORC schema evolution for changed types, projection, stripe/row-group skipping by search argument,
  timestamps as wall clock, CHAR without trailing spaces; all Hive types incl. decimal, date, timestamp, binary,
  list/map/struct. ORC tables are still skipped by the catalog; the plugin directory gains orc-core and its libraries.
- Iceberg time travel in partial mode (item 3 (a)): a query that reads one iceberg-hadoop / iceberg-rest table
  `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF` and nothing else is planned in parts like a query of the current
  snapshot (endpoints with locations; each ticket names files of the earlier snapshot, read with that snapshot's
  columns and deletes) instead of one complete endpoint. Joins, subqueries, set operations, several clauses and
  snapshots that cannot be read file by file still get the complete answer. Ticket format unchanged.
- Perf test `ScanThroughputPerfTest` (tag `perf`, item 16): one client streams a full table scan over Flight SQL
  (complete mode) per variant of `duckDbPreserveInsertionOrder` / `duckDbStreamingBufferSize` and prints MB/s and
  batches (`-Dperf.variants=true:,true:64MiB`, `-Dperf.threads`, `-Dperf.rows`, `-Dperf.runs`). Defaults unchanged.
- lakeFS read cache (item 17 (b); opt-in): `arrowflight.lakefs.cacheMegabytes` (default 0 = off) and
  `arrowflight.lakefs.cacheCheckSeconds` (default 10) let DuckDB read the lakeFS S3 gateway through a node-wide,
  byte-bounded cache like the s3 plugin's (`arrowflight.s3.cacheMegabytes`), scoped to the repository; separate
  from the s3 cache (log `lakefs=cacheOpen` / `lakefs=cacheClosed`, metrics `arrowflight_lakefs_cache_*`). The lakefs
  plugin descriptor now says `spi=1.3`. New public s3 plugin class `S3ReadCache` for plugins built on it.

- S3 read cache metrics (item 17 (d)): while `arrowflight.s3.cacheMegabytes` > 0 the Prometheus endpoint shows
  `arrowflight_s3_cache_capacity_bytes`, `_used_bytes`, `_blocks` (gauges) and `arrowflight_s3_cache_hits_total`,
  `_misses_total`, `_evictions_total`, `_joined_total`, `_s3_reads_total`, `_s3_read_bytes_total`,
  `_s3_lookups_total` (counters; the last three count the GETs, bytes and HEADs the cache sent to S3). New SPI 1.3:
  `StoreContext.metrics()` (`PluginMetrics`, `MetricSource`, `MetricSample`) lets a plugin publish unlabelled
  `arrowflight_*` samples on the server's endpoint; a name the server or another source already shows is left out.
  The s3 plugin descriptor now says `spi=1.3` (it needs a server of this build).
- S3 read cache, single flight (item 17 (c)): concurrent requests that miss the same 1 MiB block read it from S3
  once; the later ones wait for the first read and take its bytes (or its failure: a refused `If-Match` makes each of
  them look the version up again and read the new one, again once for all). The `s3=cacheClosed` line counts these
  blocks as `joined=`. Only active with `arrowflight.s3.cacheMegabytes` > 0.
- S3 read cache (item 17, option A; opt-in): `arrowflight.s3.cacheMegabytes` (default 0 = off) starts one
  node-wide, byte-bounded cache (JVM heap, at most half of it) that every DuckDB connection reads `s3://` through: a
  127.0.0.1-only HTTP endpoint of the s3 plugin answers ranged GETs from 1 MiB blocks (least recently used dropped
  first) and reads misses from S3 with the store's own client and `If-Match` on the object's ETag, so an answer never
  mixes two versions of an object. DuckDB's secret then holds a random per-store key the endpoint checks as an AWS
  SigV4 signature (the S3 credentials stay in Java); a key reads only its own data directory; listings are not
  served. HEADs always reach S3; an object's version is trusted for `arrowflight.s3.cacheCheckSeconds` (default 10)
  between checks, so a file rewritten in place under the same key may be read in its old version for that long.
  Applies to the main store and named stores (`arrowflight.s3.stores`), not yet to lakeFS. Startup line
  `s3=cacheOpen`, shutdown line `s3=cacheClosed` with hits/misses/evictions.
- Distributed complete mode, step 1 (item 18): `CompleteAnswerMerge` decides whether a query's
  complete answer can be rebuilt from the answers its partial-mode endpoints give for their own
  files and renders the DuckDB merge (rows concatenated; counts and sums summed, min/max taken
  again per selected group; DISTINCT, ORDER BY over output columns and LIMIT applied again).
  Refused, so answered as today: joins, OFFSET, HAVING, DISTINCT aggregates, avg and other
  expressions, LIMIT on an aggregate, GROUP BY columns that are not selected, ORDER BY over
  anything but an output column. Checked against DuckDB's own answer over all rows
  (`CompleteAnswerMergeTest`). Not wired into query execution yet: no runtime change.
- Tests (S3 read path, item 17): `S3IntegrationTest.repeatedQueriesReadOnlyTheRowGroupsTheyNeed`
  pins the request pattern of the s3 plugin's defaults on DuckDB 1.5.6 (a repeated query: no HEAD,
  no footer read, one GET per row group it reads; a pruned point query at most 2 GETs; with
  `metadataCache=false` every query looks up and reads each footer again), so a DuckDB or setting
  change that adds round trips fails CI. `S3ReadPathPerfTest` (tag `perf`) prints time and
  requests per query for thread counts and read settings against the fake S3 with a latency per
  request (`FakeS3.latency`). No runtime change.
- S3: setting `arrowflight.s3.connectionCaching` (default true; named stores and lakeFS inherit it)
  turns on DuckDB 1.5's `httpfs_connection_caching`: DuckDB keeps its S3 connections and reads later
  ranges and queries over them. Without it httpfs opened a connection for about every range read
  at 2+ threads (8 files x 4 row groups at 8 threads: 40 then 31 connections for two queries; now
  8 then 0), and the closed ones ran a host out of local ports under 32 concurrent clients (item
  0g). The startup `tcp_tw_reuse` WARN now appears only with `connectionCaching=false`; the
  `s3=open` line names the setting. Answers unchanged.
- HDFS with Kerberos: optional settings `hdfsKerberosPrincipal` + `hdfsKerberosKeytab`
  (`HDFS_KERBEROS_*`, optional `hdfsKerberosTicketCache`) make the server log in from the keytab,
  write the ticket-granting ticket as a MIT file ticket cache for the hadoopfs extension (libhdfs3
  reads only a ticket cache) and replace it at 80 % of its lifetime: no external `kinit` + renewal
  (secure HA stand finding (b)). The cache defaults to the one the extension reads; unset settings
  change nothing. Tested with an in-process KDC; MIT `klist` / `kvno` read the cache in CI.
- S3: a startup WARN when `net.ipv4.tcp_tw_reuse` is not 1, with the port range and the reads per
  second it allows: DuckDB's httpfs opens a new connection for most range reads at 4+ threads and
  each closed one holds its port 60 s, so 32 concurrent small queries at 8 threads ran the stand
  out of local ports (item 0g). Workaround documented in `arrowflight.properties` and START-NOTES
  (`tcp_tw_reuse=1`, wider port range, `duckDbExternalFileCache=true` for repeated reads); no
  setting changes.
- HDFS: a startup WARN names every libhdfs3 NameNode RPC timeout (`rpc.client.connect.timeout`,
  `read`, `write`, `timeout`) the `LIBHDFS3_CONF` file leaves unset or over a minute; libhdfs3's
  defaults let a query hang ~170 s after the active NameNode host vanished (secure HA stand finding
  (d)). Recommended values in `arrowflight.properties` and START-NOTES; no setting changes.
- Flight `CancelFlightInfo` (and Flight SQL `CancelQuery`): releases the node-load reservations of
  a plan's endpoints at once instead of after their 10-minute expiry, so plans a client abandons no
  longer steer later endpoints away from a node (3-node S3 stand finding (b)); answers `CANCELLED`,
  running streams continue (README "Cancelling a plan"). Was `UNIMPLEMENTED`.
- Hive plugin: Kerberos metastores (`arrowflight.hive.authentication=kerberos`): SASL/GSSAPI
  transport as Hive's own client (`metastorePrincipal` with `_HOST`, `saslQop`), login from
  `clientPrincipal` + `clientKeytab` or from the ticket cache (`KRB5CCNAME` / `clientTicketCache`),
  renewed before the ticket runs out. Tested against an in-process KDC (Apache Kerby, test scope only).
- Iceberg time travel: `FROM ns.t FOR VERSION AS OF <snapshot id | 'tag'>` and `FOR TIMESTAMP AS OF
  TIMESTAMP '<time> <zone>'` read an earlier snapshot of an iceberg-hadoop / iceberg-rest table with
  its own files, deletes and columns (README "Iceberg time travel"); errors for points without a
  snapshot, refused for other tables; partial mode answers such queries completely. SPI 1.2 adds
  `Catalog.asOf` (default: refused); the iceberg plugin now needs SPI 1.2.
- Several S3 stores per server: `arrowflight.s3.stores=<name>,...` with `arrowflight.s3.<name>.root`
  and that store's own endpoint, region, URL style and credentials (`FLIGHT_S3_<NAME>_ACCESS_KEY`,
  `..._SECRET_KEY`, `..._SESSION_TOKEN` or `arrowflight.s3.<name>.credentialsFile`) let catalogs read
  roots outside the S3 data directory (another bucket, endpoint or account). Each store gets its own
  DuckDB secret (`flight_s3_<name>`) scoped to its root. A root that needs a second store of the
  main S3 configuration is refused as before; a named store never borrows the main credentials. SPI: `DataStore`
  gains `configName()` (default empty).
- Runtime libs set: the build copies the Hadoop client libraries the shaded server jar leaves to the
  deployment (Hadoop is `provided`) to `flight-server/target/libs`, mediated against the jar's own
  libraries (58 jars, Hadoop 3.5.0; none of the stale stand set's ZooKeeper 3.6, Netty 3, Jetty
  9.4.51, BouncyCastle 1.79, log4j 2.20 or second Netty/Jackson/protobuf). The manual `package`
  release of `dev-ci.yml` publishes it as `flight-server-libs-<version>-<sha>.tar.zst` (`libs/` with
  `DEPENDENCIES.txt` and `SHA256SUMS`); start with `java -cp '<jar>:libs/*' <main class>`.
  `LibsSetTest` checks the floors and starts the server on exactly this set.
- Spark connector: a scan with a pushed global aggregation (no GROUP BY) reports one row to
  Spark's optimizer instead of the raw table size. Read option `statistics.filterSelectivity`
  (default `false`): `true` scales the reported rows and bytes by the estimated selectivity of the
  pushed filters (equality 0.1, range 1/3, IN 0.1 per value up to 0.5, AND/OR/NOT combined), so
  Spark can broadcast selectively filtered tables in joins. Heuristic: an underestimate can make
  Spark broadcast a large table (idea from upstream 482d24b; negating an unknown predicate keeps
  every row here instead of none).
- Setting `duckDbMaxTempSize` (`arrowflight.duckdb.maxTempSize`, env `DUCKDB_MAX_TEMP_SIZE`):
  cap of the DuckDB spill directory, e.g. `20GiB`. It holds per DuckDB connection (each is an
  instance of its own), so a node may spill up to the cap times its live connections. Unset =
  DuckDB's own cap (90 % of the free disk space, per connection). Plugins may not set
  `max_temp_directory_size` (idea from upstream 2e31e1f, whose cap is per server).
- Settings `duckDbPreserveInsertionOrder` (`arrowflight.duckdb.preserveInsertionOrder`, env
  `DUCKDB_PRESERVE_INSERTION_ORDER`, default `true`; only `true`/`false` accepted) and
  `duckDbStreamingBufferSize` (`arrowflight.duckdb.streamingBufferSize`, env
  `DUCKDB_STREAMING_BUFFER_SIZE`, e.g. `64MiB`; unset = DuckDB's own): `false` lets DuckDB return
  the rows of a query without ORDER BY in any order (parallel scan threads do not wait for each
  other), the buffer lets a streamed result run ahead of a slow Flight consumer. Both are opt-in
  until measured on the stand; plugins may not set them (upstream cb45f1c turns both on).
- Setting `endpointsPerServer` (`arrowflight.cluster.endpointsPerServer`, env
  `FLIGHT_ENDPOINTS_PER_SERVER`, default 1): partial results split one node's files into up to that
  many endpoints of similar size (each with its own backup locations and load accounting), so a
  client can read a node over several parallel streams. Queries that need every row in one place
  (OFFSET, HAVING, DISTINCT aggregates) and joins are never split; values below 1 are refused
  (idea from upstream 1d3f331, which also split those single-node queries on a one-node cluster).
- Flight action `arrowflight.table_statistics` (JSON `{schema, table, columns}`): row count,
  per-column bounds, null counts and sizes from the Parquet footers of a data-directory table
  (cached per file set), or the row count and size of a catalog table's current snapshot (an upper
  bound, `rowsExact: false`, with delete files); no ticket, no node load; access checked like
  `SELECT * FROM` the table; listed by `ListActions`. Same name and request as upstream da6a357,
  answer extended by `rowsExact`, `bytes`, `files`; unlike upstream it refuses a multi-node cluster
  on local disks (a node sees only its own files) and estimates no distinct counts yet.

### Security
- Authorization (every policy provider): queries with subqueries, CTEs (`WITH`, `WITH RECURSIVE`) and set
  operations (`UNION`, `EXCEPT`, `INTERSECT`) run again under a provider. Their tables are named from DuckDB's own
  parse (`json_serialize_sql`, in a small DuckDB of its own that reads nothing; CTE names count only where they are
  in scope) and each one is checked; a caller without access gets UNAUTHORIZED. Table functions
  (`read_parquet(...)`, `range(...)`), `DESCRIBE`/`SHOW` sources and catalog-qualified names inside such queries are
  refused with INVALID_ARGUMENT; names without a schema that are no CTE are refused as before. Partial mode still
  does not run nested queries.
- Authorization (every policy provider: `ranger`, `ranger-hive`, custom): a table read only inside a subquery
  (`WHERE id IN (SELECT ... FROM s.t)`, `EXISTS (...)`, a scalar subquery in the select list) or named quoted in a
  join (`JOIN "s"."t"`) was not checked, and its rows reached callers without access (complete mode; scalar
  subqueries in both modes). With an authorization provider, queries with a subquery (other than the plain
  `FROM (SELECT cols FROM t [WHERE ...]) alias` wrapper) are now refused with INVALID_ARGUMENT, and every table named
  after FROM / JOIN / a comma, quoted or after a comment, is checked. Without a provider nothing changes.
- Server: Hazelcast 5.6.0 → 5.7.0 (critical/high advisories on the member port) and Netty 4.2.9 → 4.2.19
  (HTTP/2 and handler advisories) under one `netty-bom`. `netty-all` is replaced by the modules the gRPC server
  uses (HTTP/2, proxy, epoll for Linux x86-64/ARM64), which drops about 25 advisories in unused codecs (Redis,
  HAProxy, SMTP, XML, HTTP/3, OCSP, DNS …); macOS builds now use Netty's NIO transport (no kqueue). README: keep
  the Hazelcast port private. Upgrade every node of a cluster together (Hazelcast open source has no rolling
  upgrade between minor versions). `DependencyFloorTest` guards both floors. The Spark connector imports the same
  `netty-bom` (its shaded jar's Netty 4.2.9 → 4.2.19; its tests mixed 4.1 and 4.2 modules).
- Client core and StarRocks connector: one Netty 4.2.19 under `netty-bom` instead of Arrow's 4.2.9 mixed with
  grpc-netty's 4.1.130 (HTTP/2 codec, http, socks, handler-proxy, unix-common were 4.1.130), tcnative
  2.0.74 → 2.0.84. The provided Hadoop dependencies no longer bring `netty-all`, so no codec or native
  transport leaks into the connectors' runtime classpath. `NettyFloorTest` in both modules.
- Jackson 2.15.3 (databind/core, mixed with Arrow's 2.21.0 jsr310 and Ranger's 2.17.2 JAX-RS) → one 2.21.8 under
  `jackson-bom` in the server, client core, Spark and StarRocks connectors (databind DoS and
  PolymorphicTypeValidator advisories). The Spark connector's shaded jar relocates Jackson to
  `pro.surpin.data.arrowflight.shaded.jackson`: Spark 3.5 keeps its own 2.15, the connector runs on 2.21.8 (before,
  Spark's 2.15 shadowed the bundled copy). `DependencyFloorTest` / `JacksonFloorTest` guard one version >= 2.21.7.
  The Iceberg plugin (own class loader) bundles Jackson 2.22.3 instead of Iceberg 1.12's 2.22.2.
- Server: Parquet 1.13.1 → 1.18.1 (footer and statistics reads), which lifts aircompressor 0.21 → 2.0.3
  (CVE-2024-36114, CVE-2025-67721: crash / memory leak on crafted LZ4/Snappy/Zstd input) and snappy-java
  1.1.8.3 → 1.1.10.8 (CVE-2023-43642, CVE-2023-34455 and others: DoS on crafted chunks). Data pages are still
  read by DuckDB. `DependencyFloorTest` guards Parquet >= 1.15, aircompressor >= 2.0.3, snappy-java >= 1.1.10.4.
- Hive plugin: libthrift 0.9.3 → 0.24.0 for the metastore client (CVE-2026-43869, CVE-2026-43871,
  CVE-2020-13949, CVE-2019-0205, CVE-2018-1320: parser DoS from a hostile metastore, TLS host checks). Hive
  3.1.3's generated metastore classes and libfb303 0.9.3 run unchanged on it. Responses are capped at 1 GiB
  (Hive 4's default; libthrift's own 100 MB would refuse large partition lists, 0.9.3 had no cap); frames stay
  at 16 MB. The test metastore (Hive 3.1.3 server, which fails on libthrift 0.14+) now runs on 0.9.3 in its own
  class loader, so the tests cover a new client against an old server; a framed-transport test was added.

### Changed
- `resourceProfile=speed` (item 24) turns DuckDB's external file cache on (`duckDbExternalFileCache=true`) while the
  shared DuckDB is on; an explicit `duckDbExternalFileCache` / `DUCKDB_EXTERNAL_FILE_CACHE` wins, `balanced` and
  `speed` with `duckDbSharedDatabase=false` keep it off. All sessions share one buffer pool, so the cache stays within
  the node-wide `memory_limit` (startup line `duckdb=externalFileCache nodeWide=<limit> bound=sharedMemoryLimit`); the
  private DuckDB a query moves to after running out of the shared memory never caches. S3, 2 CPUs / 6 GB: small
  queries c1 214 vs 43 q/s (p95 7.5 vs 38.6 ms), c32 392 vs 239 q/s (small-query-perf 41453983213, 0 errors);
  TPC-H SF10 on S3 283.6 -> 91.2 s, same answers, peak
  container memory 3317 vs 1640 MiB of 6 GiB (tpch-perf 41453978726); local Parquet unchanged (40.0 -> 37.6 s, peak 1717 vs 1690 MiB; DuckDB caches only
  files read through an extension; tpch-perf 41458186484).
- `shutdownDrainSeconds` defaults to 20 (was 0; maintainer, lane 3 Q2): a node stopped with SIGTERM leaves the pool,
  refuses new calls with `UNAVAILABLE` and lets running answers finish for up to 20 s instead of cutting them with
  `INTERNAL`. `docker stop` needs `-t 25` or more (its default is 10 s); `0` restores the immediate stop.
- `distributedComplete` (opt-in): a query whose merged answer fails after the client's first batch because a peer's
  part stream ended with `UNAVAILABLE` or a transport error now fails with `UNAVAILABLE` ("peer lost during
  distributed complete answer; retry") instead of `INTERNAL`, so clients and drivers that retry on `UNAVAILABLE`
  re-run it; other failures stay `INTERNAL` (lane 3 D6, maintainer's answer to Q1).
- Cluster: a node that crashed with its address (host down, container removed) no longer holds every plan on the other
  nodes until Hazelcast drops it (10 s). The planner reads node loads, heartbeats and inventories through a local cluster
  view: the first plan waits at most 1 s for the silent member, later plans not at all; its node is left out of plans
  (not removed from the pool) until any operation on its partitions answers or it leaves the cluster, and its keys take
  the last values read (refreshed at every heartbeat). `hazelcast.max.no.heartbeat.seconds` stays 10 (item D3).
- Cluster: writes no longer wait for such a node either (item D3 step 2). A write whose key lives on a member that stopped
  answering (its owner, or the synchronous backup the write waits for up to 5 s) runs in the background: load changes
  (counted in this node's plans at once), endpoint load reservations and their releases (in order per key), handles,
  prepared-statement parameters, pool removals and the heartbeat (one pending at a time). The heartbeat thread
  refreshes the cluster view first and skips repairs it cannot do now; a live node whose heartbeat entry sits on the
  silent member is no longer dropped as stale. Measured with an iptables DROP: 40 load-tracked tickets with their
  releases and heartbeats in 65-88 ms (before: the first write on the silent member's partitions waited until
  Hazelcast dropped it, up to 10 s; a write whose backup alone was silent waited the 5 s backup timeout).
- Cluster: the remaining waits for such a node (item D3 step 2b). A backup node moving an endpoint's load reservation
  whose entry sits on the silent member no longer holds the end of the stream (the move and the release queue in the
  background, in order); publishing this node's file inventory and the heartbeat's repairs of the inventory and ticket
  secret (maps kept on every member, whose puts wait for each one) wait in the background or for the next beat, and
  this node's own pending inventory counts as published; a node that stops while its registry entry sits on the
  silent member leaves without deregistering (the others drop it when its member leaves).
- Cluster: plans on local disks (each node its own data directory) no longer wait for such a node either (item D3
  step 3). They need every published inventory, so that files only a lost node held fail the plan instead of leaving
  the answer; the full read of the inventory map waited for the silent member. Each node now keeps a local copy of the
  inventories (full reads plus the map's entry events) and, while a member does not answer, reads the inventories on
  members that answer by key and takes the others from that copy; a member's inventory it never saw makes it read the
  whole map as before. Measured with an iptables DROP: local-disk planning reads well under 500 ms (before: until
  Hazelcast dropped the member, 10 s).
- DuckDB threads and memory by host size (item 19b): with `duckDbMemoryLimit` unset every DuckDB connection now gets
  `memory_limit` = the node's DuckDB budget (container `memory.max` or physical memory, minus `-Xmx`, direct memory
  and 15 % headroom) / the connections expected at once (`duckDbMemoryConnections`, default cores / threads, at
  least 2), at least 1 GiB and at most 80 % of the memory (was unset = DuckDB's 80 % of RAM *per instance*);
  `duckDbMemoryLimit=none` keeps DuckDB's default, an explicit size wins. The `duckDbThreads` default is cores / 4
  between 2 and 16 (was 8; S3/lakeFS at least 4 as before), capped at one thread per GiB of the connection's limit
  but never below 2. Hosts up to 32 cores keep their thread count. Startup line `duckdb=sizing` shows the inputs
  and the chosen values.
- Hive ORC tables (`hive.orcTables`): a file larger than 8 MiB is read as several ranges of whole stripes (planned
  from its footer; at least a quarter of the file's size per range when it is large enough, at most about 32 MiB),
  each a read-ahead unit next to the other files' (P1 step 2 (g)), so a table of one large file is decoded in
  parallel too; batches still reach DuckDB in file and stripe order, and the first and last ranges are open, so every
  stripe is read once whatever the footer or listed size say. Answers unchanged; SF1 lineitem ORC in one file on a
  4-core runner: Q1-style 0.73-0.83 s, Q6-style 0.37-0.46 s, as fast as the same table in 4 files.
- Hive ORC tables (`hive.orcTables`): batches of integer, date, double, decimal (<= 18 digits), string and binary
  columns are copied into Arrow in bulk instead of value by value (P1 step 2 (f)). Answers unchanged; local 2M-row
  lineitem ORC: `SELECT *` 2.3 -> 1.9 s, Q1-style 0.8 -> 0.7 s.
- Hive ORC tables (`hive.orcTables`): the files of a table scan are decoded up to 4 at once (at most the core count)
  on the plugin's `hive-orc-read-*` threads, each at most 2 batches ahead; batches still reach DuckDB in file order,
  closing the stream stops the decoding and frees what was read ahead (P1 step 2 (f)). Answers unchanged; local
  2M-row lineitem ORC in 4 files: Q6-style 0.45 -> 0.26 s, `SELECT *` 1.9 -> 1.26 s (Parquet 0.03 / 0.85 s).
- Hive plugin, ORC tables: decimals of up to 18 digits are decoded by ORC straight into longs (`USE_DECIMAL64` row
  batches) and copied to Arrow without a `BigDecimal` per value; Q1-style queries over ORC 5-30 % faster
  (SF1 on a CI runner ~5 %, 1M rows locally ~30 %; P1 step 2 (e)). Answers unchanged (new test `OrcArrowReaderTest.readsDecimalsOfEveryWidthAndWidenedScales`).
- Less CPU per small query (item 21, step 1): GetFlightInfo of a complete answer on a node alone in its cluster no
  longer reads the server registry and heartbeat maps (it checked the node count on every query over local
  directories); the per-DoGet `executionPath=` event is written as text instead of through a Jackson tree (same
  text); endpoint tickets reuse one HMAC instance per thread; binary ticket query ids are formatted without
  `String.format` (same ids). Local, 2 CPUs, point lookups + filtered counts over 4 local Parquet files, client in
  the same JVM (`SmallQueryThroughputPerfTest`): c8 ~159 -> ~188 queries/s, c32 ~222 -> ~250, CPU per query
  ~12.3 -> ~10.4 ms at c8. No wire, setting, answer or log-text change.
- Iceberg position deletes (item 2 (iii)): data files that share their delete files (as after a MERGE) are now read
  in one `read_parquet` whose position delete files are read once and matched by DuckDB's `file_index` and the row
  number, instead of one UNION ALL branch per data file that re-read every delete file. Local, 500 files x 2000 rows
  with one shared position delete file: prepare ~750 -> ~20 ms, run ~1200 -> ~50 ms, same answer. A single file, and
  any table with a column named `file_index`, keep the per-file form.
- `duckDbThreads` default is size-aware (item 19, approved in stage 2): unset or 0 = a quarter of the cores the JVM
  sees, between 2 and 8 (4 cores: 2, 16: 4, 32+: 8; was a fixed 4). Every DuckDB connection (one per busy worker
  thread) runs that many threads, so 32 concurrent sessions at 4 threads on 4 cores oversubscribed the CPU (local
  c32 point p50 64-69 ms at 4 threads vs 57 at 2). An explicit value is used as before; a negative one is refused
  at startup. The bundled `arrowflight.properties` leaves it empty. New startup line `duckdb=threads perConnection=
  availableProcessors=`.
- Complete-answer mode: less work per query (item 15). The result and parameter schema of a query
  are described once per views snapshot and shared by the client's prepare and its GetFlightInfo
  (before: three DuckDB describes and four parse checks per JDBC/ADBC query); the describe
  connection keeps its parse-check statement; closing a prepared statement no longer waits for the
  shared parameter cache. Local measurement (`PointQueryOverheadPerfTest`, tag `perf`): point
  query p50 14.0 -> 10.2 ms, 8 clients 31 -> 22 ms, 32 clients 94 -> 58 ms. New metric label
  `describe` on the cache counters. Answers, wire format and settings unchanged.
- Less work per DoGet (item 15 step 2): a query text is parsed for the single-query check once per
  node (the describe at prepare and the DoGet shared none of it before; the refused function names
  are still checked on every use, also names a plugin adds later); DEBUG-only log arguments
  (durations formatted with `String.format`) are no longer built when DEBUG is off; the per-DoGet
  execution-path event reuses its regular expressions and SHA-256 digest (same output). Local
  measurement: complete-mode point query DoGet p50 6.2 -> 5.4 ms, total 13.1 -> 11.9 ms. New metric
  label `parse_check` on the cache counters. Answers, wire format, log lines and settings unchanged.
- Partial-result mode: a new query text no longer lists the table's files to describe it (item 15
  step 2 (a)). The file a table's queries are bound against is remembered per table for 30 s (the
  metadata TTL); a describe that cannot read it (table rewritten) lists the table again once.
  Local measurement: partial point query prepare p50 28.5 -> 3.4 ms, total 38.4 -> 12.5 ms,
  8 clients 97 -> 28 ms. Answers, wire format and settings unchanged.
- The per-DoGet INFO line `executionPath=...` (and the optional `BENCHMARK_EXECUTION_PATH_LOG`
  evidence file) is written by one background thread instead of the DoGet thread (item 15 (b),
  approved by the maintainer); same level, text and order per query; when 8192 events are pending
  the DoGet writes its own; pending events are written at shutdown. Local measurement: complete-mode
  32 clients p50 90 -> 78 ms, partial 8 clients 28 -> 24 ms.
- Complete mode: a prepared query without `?` parameters gets the plain handle (its SQL text, as in
  partial mode) instead of one with a statement id, so its GetFlightInfo no longer reads the
  cluster map for parameter values and its close no longer deletes from it (item 15 step 3). A
  query with parameters keeps the id handle. Answers and settings unchanged; the only visible
  difference is for a client that sends values to a query without parameters and keeps using the
  first handle: the query now runs without them instead of failing with a count mismatch.
  Local: close p50 1.15 -> 1.05 ms, GetFlightInfo 2.4 -> 2.2 ms (one node; a remote partition
  owner saves a network round trip per query).

### Fixed
- `ranger-hive` refused every table when the Hive service has a tag service with a restrictive tag-based policy - which
  is always the case, since Ranger creates the deny policy `EXPIRES_ON` in every tag service. It now reads the tags
  (Ranger's tag enricher, downloaded from Ranger admin) and refuses only tables whose database, table or a column
  carries a tag of a restrictive tag policy (conditions such as the expiry date are not evaluated: tagged = refused);
  with tags not loaded yet it still refuses every table. Service matrix cell `ranger` keeps the default policy and
  checks an expired `EXPIRES_ON` tag on one table (refused) next to an untagged one (read).
- Table checks under an authorization provider (Ranger, a plugin provider) refused queries DuckDB runs: comma joins
  (`FROM a, b WHERE ...`), a FROM subquery with a join, an aggregate or GROUP BY, and `?` parameters of prepared
  statements (`INVALID_ARGUMENT`; TPC-H q3, q5, q7-q10, q12-q14, q19, q22). Their tables are now named by DuckDB's own
  parse and each one is checked; table functions, catalog-qualified names and several statements stay refused.
  Without a provider nothing changes.
- HDFS under the lockdown (refused queries in `balanced`, shared DuckDB not starting in `speed`/dc): DuckDB 1.5.6 stores
  a URI root in `allowed_directories` through the file system that claims its scheme; without the hadoopfs extension
  loaded an `hdfs://ns/...` root became `<working dir>/hdfs:/ns/...`, so every HDFS read was refused. A `balanced` server
  now runs the lockdown check at startup too (it skipped it when no extension or setup statement was configured), and
  the check names the missing extension (`DuckDB has no file system for hdfs:// ... set duckDbHdfsExtension`) instead
  of failing every query. An `hdfs://` data dir now finds `hadoopfs` in `arrowflight.duckdb.extensionDir`
  (`<dir>/v<version>/<platform>/hadoopfs.duckdb_extension`, as the s3 plugin finds httpfs) when `duckDbHdfsExtension`
  is not set; a directory without it stops the start with a message naming both settings. Refused queries report DuckDB's own error (`IO Error: ...`, `Permission Error: ...`) in place
  of the JDBC driver's `Attempting to execute an unsuccessful or closed pending query result`, and the first refusal on a
  node is logged with its stack (`query=refused ... first=true`). Dev CI now runs the hadoopfs lockdown tests with
  the real v1.5.6 extension (balanced and the shared DuckDB).
- `resourceProfile=speed` (item 23b follow-up): `duckDbThreads` is all cores but at least what `balanced` gives one
  query (4), so on 2-3 core nodes the shared DuckDB gets 4 threads instead of one per core. With 2 threads on 2
  cores S3 point lookups had 1.6x balanced's c1 p95 (53 vs 33 ms, 39 vs 49 q/s) and c8 p95 106 vs 62-85 ms; with 4:
  c1 = balanced, c8 p95 83 ms, c32 unchanged (~330-345 q/s vs balanced ~205-235). 1 thread was worse (c1 p95 ~100
  ms). Run 41083790232.
- `duckDbSharedDatabase` / `resourceProfile=speed` (item 23b, root cause): the sessions of the shared DuckDB now keep
  their cached prepared statements within one node-wide budget, an eighth of its memory limit at 1 MiB each (256 for
  2 GiB; log `cachedStatementBudget=`); past it a session evicts its own least recently used statement (at least one
  kept). Each session kept up to `duckDbStatementCacheSize` (64) statements, and a bound S3 scan holds httpfs's 1 MiB
  read buffer, so 32 sessions of point lookups filled the 2 GiB limit (`duckdb_memory()` ALLOCATOR 2042 MiB) and every
  later query failed. A shared DuckDB that runs out of memory now logs `duckdb=sharedOutOfMemory` with its memory by
  tag (at most every 10 s). DuckDB-per-connection mode (`balanced`) is unchanged.
- `duckDbSharedDatabase` / `resourceProfile=speed` (item 23b): a query that runs out of the shared DuckDB's memory
  (`Out of Memory Error`) before anything was sent now runs once more on a private DuckDB of the thread, sized as one
  connection of the DuckDB-per-connection mode (log `duckdb=outOfMemoryRetry`); before, 32 concurrent S3 small queries
  under `speed` (2 CPUs / 6 GB, `-Xmx3g`, direct 2g: 1 GiB node-wide) failed in every round while `balanced` answered.
  DuckDB result streams now send the schema with the first batch instead of before running the query (same messages).
- JWT login (`FLIGHT_AUTH_PROVIDER=jwt`) with `FLIGHT_JWT_AUDIENCE` set failed at start with a NullPointerException:
  the accepted audiences were an immutable `Set.copyOf`, and the nimbus claims verifier on the server class path
  (10.0.1, older than the module's 10.10) asks `contains(null)`. The settings now hold null-tolerant copies.
- Partial mode: a join naming a table with double quotes (`... JOIN "sales"."orders" t ...`) failed with `Catalog
  Error: ... schema "sales" does not exist` unless the worker's DuckDB connection happened to hold the catalog views
  from an earlier complete query (flaky `RangerHiveFlightSqlTest` in CI, "second server in one JVM" in the plugins
  lane's note): the join parser now names and rewrites quoted tables like bare ones. An error, never wrong rows.
- Node load leak with concurrent identical queries (found by D9): endpoint tickets are deterministic, so clients
  planning the same query at the same time got the same ticket, and its load reservation was overwritten - one
  release then found nothing and the node kept the endpoint's bytes as load for good, so later plans skewed away
  from it (in the in-JVM test a node got no endpoints at all). A reservation key now holds one reservation per
  planned endpoint (`LoadReservations` when there are several; atomic `putIfAbsent`/`replace`, lifetime kept); a
  claim by a backup node moves one of them. Ticket format unchanged.
- With an authorization provider on (`FLIGHT_AUTHORIZATION_PROVIDER`), a query whose tables the check cannot
  name (e.g. `UNION`, a statement that does not parse) failed with a message-less `INTERNAL`; it is now refused with
  `INVALID_ARGUMENT` and the parser's reason, as without authorization. Still refused, only the status changes.
- Hive plugin, `hive.insertOnlyTables=true`: an insert-only table whose layout is refused (e.g. a `delete_delta_*`
  directory) no longer makes the server treat the whole metastore as unreadable. Before, the refusal was reported like
  a metastore outage, so the catalog views refresh left out every table listed after it (all schemas of that
  metastore); now only that table is left out (WARN `views=skipTable`), complete queries of it answer NOT_FOUND and
  partial ones INVALID_ARGUMENT with the reason. Server: a table-level error of a catalog while choosing the result
  mode no longer escapes as a message-less INTERNAL error; planning reports it (C4 step 3).
- Tests: the cluster and distributed tests (and `TestFlightServerHelper`) take their Hazelcast and Flight ports from
  `TestPorts`, below the system's ephemeral port range, instead of `ServerSocket(0)`: an ephemeral port could be
  taken by an outgoing connection between the probe and the bind ("Address already in use" on CI). Test-only.
- `duckDbThreadsUnderLoad`: a query alone on the node now keeps its `duckDbThreads`, as documented, also when
  `duckDbThreads` exceeds the cores (the default floor of 4 threads on 1-3 core nodes); before, every query on such
  a node ran on the low-thread connection. `SmallQueryThroughputPerfTest` takes `-Dperf.threadsUnderLoad`.
- DuckDB thread default no longer drops below 4 (laptop stand p19b, 2026-10-28): since item 19b 4c/8g, 8c/16g and
  16c/24g containers got 2/2/3 threads per connection (cut by the one-thread-per-GiB cap) and TPC-H SF10 ran
  1.35-1.7x slower than with 4. Default now cores / 4 between 4 and 16, the per-GiB cap applies only above 4; the
  default per-connection `memory_limit` keeps its values (1024/2201/3942 MiB on those profiles). New manual workflow
  `tpch-perf.yml` (SF10 in a 4c/8g container, default vs `-DduckDbThreads=4`, fails when the default is > 10 % slower
  or answers differ) with the perf-tag client `TpchPerfTest`.
- S3 / lakeFS read cache took about twice its size in heap (item 17 (e), CI screening `COMPARE-CACHE-2026-10-27`:
  RSS ~5.7 GB with `cacheMegabytes=1024` vs ~2.3 GB off): each 1 MiB block was one array, a humongous object for G1
  that takes whole regions (256 MiB of blocks used 512 MiB of heap at `-Xmx3g`); a miss of up to 64 MiB was also
  buffered by the SDK, copied, and copied again per block. Blocks are now held in 256 KiB pieces (256 MiB of blocks =
  256 MiB of heap) and misses are streamed from S3 into them, no whole-GET buffer. Answers unchanged.
- Tests: `TestFlightServerHelper` ignored `endpointsPerServer`, so in-process partial-mode scans were always one
  endpoint (`ParallelScanPerfTest` endpoint runs and `ThreadsUnderLoadIntegrationTest` measured one stream); the
  integration tests now assert the split. Test only.
- Arrow-stream tables (Hive ORC, `hive.orcTables`): a multiplication in the query (`sum(a * b)`) counted as a
  `SELECT *`, so the catalog streamed every column of the table (P1 step 2 (f)). `ArrowScanRequests` now takes a
  star between two values for a product; a star that may select columns (`SELECT *`, `t.*`, `DISTINCT ON (a) *`,
  `* EXCLUDE`, a star next to a comment) still reads all. Answers unchanged; local 2M-row lineitem ORC: Q6-style
  1.8 -> 0.5 s, Q1-style 2.0 -> 0.8 s.
- S3 small queries: longer tail since the size-aware `duckDbThreads` default (item 19; 4 vCPU -> 2 threads). With
  `duckDbThreads` unset or 0 and `dataDir` on `s3://`, `s3a://`, `s3n://` or `lakefs://` the default is now at least
  4 (the value before item 19; larger hosts keep cores / 4, at most 8); local and HDFS data directories are unchanged,
  an explicit value still wins. Local A/B, 4 cores, fake S3 at 2 ms per request, prepared point/count queries:
  2 -> 4 threads c1 p95 48 -> 35 ms, c8 p95 77 -> 67 ms (10 ms per request: c1 107 -> 64, c8 123 -> 101), c32 q/s
  -18 % on this CPU-saturated in-process run.
- S3 read cache: concurrent requests for the same missing span now claim all its blocks under one lock, so the
  span is read with one GET; before, two requests could each claim part of it and read it in two GETs (answers
  were right). Suspected cause of the `S3CacheEndpointTest` count failures in CI runs 40586746160 and 40593759006.
- S3 read cache: a miss read whose answer carries another ETag than the version looked up is refused like a failed
  `If-Match`, so a store or gateway that ignores `If-Match` cannot mix two versions of an object in one answer.
- S3 (DuckDB 1.5.6): after an object was rewritten, the first query of every server connection
  that had read it before failed "HTTP Error: ETag on reading file ... was initially ... this likely
  means the remote file has changed" (DuckDB keeps a file's metadata per connection; the next query
  reads the new file). A query refused this way before anything was sent now runs once more and
  returns the new file's rows (INFO `duckdb=changedFileRetry`); a refusal after the first batch
  still fails the stream. Test `S3IntegrationTest.aQueryAfterAFileWasRewrittenReadsTheNewFile`.
- Cluster: why the first plan after a kill test waited 9-10 s on one stand and ~1 s on another
  (3-node S3 stand finding (c)): a crashed node whose address vanished with it holds every map
  operation on its partitions until Hazelcast drops it (`hazelcast.max.no.heartbeat.seconds`,
  default 10, `-D` overrides it); one whose port still refuses is dropped at once. Documented in
  START-NOTES; `ClusterMemberLossIntegrationTest` covers both cases and the override (the
  vanished-address case uses iptables as root or via `sudo -n`, skipped otherwise). No code change.
- Concurrent DoGet streams could corrupt each other's batches: Arrow Java's `ListVector` /
  `LargeListVector` set the writer index of the process-wide empty buffer to 4 (8) when an empty
  list is written to IPC, so another stream serialized its empty buffers with the wrong length
  (a stream CANCELLED with `IndexOutOfBoundsException`, or, when the length changes between the
  batch header and the body, a body the client decodes shifted).
  Every SqlInfo answer has empty list/map children: the likely cause of ADBC connects that read
  "transactions supported" and failed `failed to update autocommit: ... Unimplemented` (TUNING.md,
  item 13d). Every batch a ticket sends now gives its empty list vectors own offset buffers first
  (`EmptyListOffsets`). BeginTransaction stays `UNIMPLEMENTED`, now with a description and a WARN
  log line.
- Error texts sent to clients no longer carry Java stack frames: HDFS NameNode errors (e.g.
  `Permission denied`, through DuckDB or the Java Hadoop client) used to arrive with the NameNode's whole
  stack trace; a gRPC interceptor now drops `at ...(...)` and `... n more` lines from every error
  status (the message and `Caused by:` lines stay; the server log keeps the full text).
- Many concurrent streams no longer exhaust direct memory: every stream could queue up to
  `flightBackpressureThresholdBytes` (256 MiB) for a slow client, so 8 clients scanning a 3-node
  cluster got streams CANCELLED ("Server sendMessage() failed with Error", direct memory out) and a
  node killed. All open streams of a node now share one outbound budget, new setting
  `flightOutboundBudgetBytes` (default 0 = a quarter of the direct memory limit): each stream queues
  at most budget / open streams (at least 1 MiB) and then waits for its client. Regression test runs
  16 slow-read streams against a server JVM with 256 MiB direct memory.
- S3: DuckDB retries failed S3 requests, connect errors included, for about 6 s instead of
  DuckDB's own ~0.5 s (3 retries from 100 ms, backoff 4), which a busy endpoint refusing
  connections outlasted at `duckDbThreads=8` (every complete-mode TPC-H query failed on a MinIO
  store with "Could not establish connection"). New settings `arrowflight.s3.httpRetries` (default
  6, 0-20), `httpRetryWaitMs` (200, first wait) and `httpRetryBackoff` (2, 1-10) set
  `http_retries`, `http_retry_wait_ms` and `http_retry_backoff`; named stores inherit them (they
  are per DuckDB connection, so differing values are refused at start).
- Cluster: planning no longer waits ~10 s after a node stopped. The node loads (every plan) and, on
  shared storage, the node inventories are read by key for the current Hazelcast members instead of
  with a whole-map query, which blocks while the cluster settles after a member left whose address
  no longer answers (GetFlightInfo timed out on a survivor). A stopped server now leaves the
  Hazelcast cluster gracefully (Hazelcast's terminating shutdown hook is off; graceful wait at most
  5 s). A crashed node still costs up to the 10 s member heartbeat timeout.
- HDFS short-circuit reads are no longer forced on: the deployment's `dfs.client.read.shortcircuit`
  (hdfs-site.xml or `-D`) is kept, and without it short-circuit is on only when
  `dfs.domain.socket.path` is set. A client configuration without a domain socket (no local DataNode)
  used to stop the server at start.
- An `s3a://` or `s3n://` catalog root (`arrowflight.iceberg-hadoop.warehouse=s3a://b/wh`) stopped the
  server at start ("The store opened for s3a://b/wh does not hold it (roots [s3://b/wh/])"), and Iceberg
  tables whose metadata names `s3a://` / `s3n://` files under an `s3://` root were hidden. Catalog
  locations now treat the three S3 scheme forms as one (store lookup, roots check, `read_parquet` file
  lists and the data file paths inside position delete files); DuckDB already accepted all three forms.
- A local catalog root (`arrowflight.iceberg-hadoop.warehouse=/wh`, `file:/wh` or `file:///wh`, local
  Iceberg/Hive data file paths) stopped the server at start when the Hadoop configuration names a
  non-local `fs.defaultFS` (`hdfs://<ns>`): "The store opened for /wh does not hold it". Catalog
  locations without a scheme are local files (as DuckDB reads them) and now open a local store; a
  data directory given as a plain path still follows `fs.defaultFS`.
- `duckDbStatementCacheSize=0` (or 1) broke every parameterized prepared statement with "Invalid
  Input Error: Parameter count mismatch" (JDBC, ADBC): the parse check before streaming took a
  statement from the per-thread cache and closed the one the parameter values were bound to. The
  bound statement now streams directly; 0 or 1 keep only the statement in use.
- Runtime libs set: ZooKeeper (3.8.6 with zookeeper-jute) and lz4-java (`at.yawk.lz4` 1.10.4, Hadoop's
  coordinates) were missing, because Spark's test dependencies (ZooKeeper 3.6.3, `org.lz4` 1.8.0) took them
  out of the `provided` scope; HA NameNode failover with ZooKeeper, the ZooKeeper delegation-token and
  signer-secret managers, `ZKCuratorManager`, `SecurityUtil` reflection and Hadoop's `Lz4Codec` failed with
  `NoClassDefFoundError`. The same cause gave the shaded jar curator-framework 2.13.0 next to curator-client
  5.2.0 (and curator-recipes 2.13.0 in the set); Curator is now 5.2.0 throughout. `LibsSetTest` now loads
  every class of hadoop-common, hadoop-hdfs-client and hadoop-auth on the thin class path and resolves their
  member types (only jsch for SFTP / ssh fencing and the legacy protobuf-2.5 `ProtobufRpcEngine` are left
  out), checks the HA/Kerberos/ZooKeeper classes by name and round-trips data through `Lz4Codec`.
- Configuration precedence: environment variables (`FLIGHT_RESULT_MODE`, `FLIGHT_ENDPOINT_BACKUP_LOCATIONS`,
  `FLIGHT_METRICS_ENABLED`, `DUCKDB_*`, `HDFS_*`, `FLIGHT_S3_*`, ...) were ignored whenever the bundled
  `arrowflight.properties` set the same setting, and a `-Darrowflight.*` alias lost to the file's short key
  (`metricsEnabled=true` beat `-Darrowflight.metrics.enabled=false`). The order is now the documented one,
  for server and plugin settings alike: every `-D` name > environment > `etc/plugins/<ns>.properties` >
  `arrowflight.properties` > default. Upgrade note: an environment variable that was silently shadowed
  before now takes effect (for example a leftover `FLIGHT_RESULT_MODE=partial`), and a plugin environment
  variable now beats the same key in `etc/plugins/<ns>.properties`.
- Iceberg equality deletes are applied by the server with read_parquet (complete answers):
  snapshots with equality deletes were read through DuckDB's iceberg_scan with all their columns
  and every filter kept above the scan (~25–90 s per query on the stand), because DuckDB 1.4.5's
  iceberg_scan fails or mis-applies equality deletes under projections and pushed filters. Data
  files are now read by field ID and anti-joined with their equality delete files (null matches
  null) and position delete files (file path and row number), as the catalog's plan assigns them
  (sequence numbers), so queries read only the columns they use and filters reach the scans.
  Snapshots with deletion vectors, columns without field IDs or an equality column the schema no
  longer has keep the old read. Partial answers of such snapshots are still answered completely.
- README Quick Start and the release START-NOTES started the server with `java -jar` on the shaded jar,
  which fails with `NoClassDefFoundError: org/apache/hadoop/conf/Configuration` (Hadoop is not in the
  jar); START-NOTES also put the jar where its default `../plugins` did not point at the unpacked plugins.
- Query texts longer than 256 Ki characters (a join key list a connector sends as an IN list can
  be megabytes, and each one differs) are no longer kept in the parse cache, the result-schema
  cache or the per-thread prepared-statement cache: each cache could pin up to its entry limit
  of them (1024 parses, 1024 schemas, `duckDbStatementCacheSize` statements per worker thread) and
  evicted the short, repeating queries. A long text keeps one uncached statement slot per
  thread. New Prometheus counters `arrowflight_query_cache_oversize_total{cache}` and
  `arrowflight_query_cache_evictions_total{cache}` (`parse`, `schema`, `statement`). Idea from
  upstream f998bf7 (parse cache only).
- `flight-client-core`: pooled clients were keyed only by host, port, user and secret, so a
  configuration that asks for certificate verification could get a client built without it (or
  with another trust store, schema/routing headers, retries, timeouts or allocation limit). The pool
  key now covers every setting (hashed). Schema and routing headers (`SCHEMA`, `ROUTING_TAG`,
  `ROUTING_QUEUE`) went only with the handshake; they now go with every call and with the Kerberos
  session refresh (from upstream `feat/trino-flight-connector`, UPSTREAM-SYNC T2).
- A server whose start failed before it began serving (storage that cannot be opened, a
  cluster join timeout) or with an `Error` stayed alive without ever becoming ready: Hazelcast
  and gRPC threads are not daemons. `main` now logs the failure and exits with status 1, so a
  supervisor restarts it (ported from upstream 7b277f7).
- HDFS files whose path has escaped characters (Hive partitions such as `l_shipmode=REG AIR`,
  `tag=50%25`) failed with "Unable to get file info": DuckDB was handed the URI-escaped path,
  and the hadoopfs extension reads paths literally. Paths now reach DuckDB decoded.
- Iceberg tables with equality deletes failed in DuckDB 1.4.5 ("Equality deletes need the
  relevant columns to be selected", or an INTERNAL error that invalidated the connection on most
  other projections, also on queries of one non-key column when the table has position deletes
  and renamed columns). Views of such snapshots now read every column
  (`hash(<all columns>) IS NOT NULL`) below a `LIMIT` no table reaches, so the query's own
  filters stay above the scan: a filter pushed into the scan (also through a `MATERIALIZED` CTE)
  applied the deletes wrongly. Correct, but every query reads the whole snapshot.
- Iceberg tables written by Spark / Iceberg HadoopFileIO name their files `file:/...` and
  `s3a://...`; DuckDB's lockdown refused them ("file system operations are disabled"). Catalog
  roots now also allow their other scheme forms (`file:` for plain paths, `s3`/`s3a`/`s3n`; S3
  secrets are scoped to all three).
- Iceberg REST transport failures were reported as "null"; they name the URI and the exception,
  and a catalog outage during planning or describe is UNAVAILABLE. A REST table whose metadata
  lies outside the roots is hidden (WARN once, NOT_FOUND). iceberg-hadoop lists only
  directories in the Hadoop layout. `fs.file.impl` / `fs.hdfs.impl` are set unless configured
  (merged Hadoop class paths lost the registrations: "No FileSystem for scheme file").
- lakeFS: a partial-mode explicit JOIN re-listed the tables at execution and could mix two
  commits in one answer; it now reads the ticket's files. All tables of a view refresh or plan
  are listed in one read scope (one branch resolution), and complete-mode views refresh as soon
  as the branch head moves (were up to 60 s stale).
- lakeFS error texts: missing credentials, HTTP 401/403, unreachable or unknown hosts, unknown
  branches/tags/commits and repositories are named; a partial plan with storage down is
  UNAVAILABLE "Cannot read the table's files: ..."; complete-mode failures keep the store's
  context instead of a bare root cause.
- lakeFS tags and short commit ids are resolved once to the full commit id and read through it
  (the S3 gateway served tags 2-3x slower).
- A Hive table with a partition outside `arrowflight.hive.roots` was hidden from queries but
  still listed by GetTables until a query had planned it; the table list now checks partition
  locations too.
- A Hive Metastore that hangs (no answer) blocked queries for minutes: every view refresh asked
  the metastore once per schema and table, and every planning call waited for the timeouts again.
  A failing catalog is now asked once per view refresh, the metastore client fails at once for
  5 s after the metastore could not be reached and drops its idle connections after a broken one.
  The schema of a query on a table whose catalog is down is UNAVAILABLE with the catalog's error,
  not NOT_FOUND "Could not find Arrow schema for query".
- With a table catalog configured (Iceberg, Hive) and a table in the data directory, the first
  refresh of the table inventory after its 30 s TTL called itself until the stack overflowed;
  every later call failed with "Application error processing RPC".
- Wrong answers and crashes of large `GROUP BY` queries (TPC-H q04 at SF10: duplicate groups,
  SIGSEGV in `TupleDataCollection::Combine`, INTERNAL errors) with 2 DuckDB threads: DuckDB
  1.4.1-1.4.3 skipped repartitioning in the hash aggregate's combine (duckdb#20357, fixed in
  1.4.4). DuckDB is now 1.4.5.
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
- Partial results: a select item other than a column or a plain `count`/`sum`/`min`/`max`
  (`CAST(sum(x) AS DECIMAL(38,2))`, `avg(x)`, `sum(x) / count(*)`, `x + 1`) failed with
  "Binder Error: Referenced column ... not found", and `FILTER (WHERE ...)` or `OVER (...)` on
  an aggregate was silently dropped (wrong results). Such queries now get the complete answer
  from one endpoint (logged as `query=answeredCompletely`); a cluster that keeps its files on
  local disks refuses them with the reason.
- Partial results streamed unsigned integers unwidened (uint64) while `GetTables` and complete
  answers said `decimal128(20,0)`; with `widenUnsignedTypes` (default) both modes now stream the
  same wider signed types (uint8/16/32/64 as int16/int32/int64/decimal128(20,0)).
- A DuckDB statement closed by a failed run is prepared again instead of failing every later
  run of the same SQL with "Statement was closed".
- After a DuckDB error that invalidates its database (FATAL or INTERNAL, e.g. a hash-aggregate
  failure under memory pressure) the thread's DuckDB is closed and replaced; before, every
  later query on that thread failed with "database has been invalidated" until a restart.
- Releasing the load of an endpoint no longer puts a node that was removed as stale back into
  the registry (where, without heartbeats, it counted as a node that had just started).

### Added
- Development CI `dev-ci.yml`: on pushes to `main` and on demand, one full build with every test
  except `perf`/`real-services` (Iceberg and fake-S3 integration tests use the DuckDB 1.4.5
  extensions of the private `ci-assets` release), Checkstyle and SpotBugs; a manual `package` job
  publishes a linux-only jar, the plugins dir, START-NOTES and SHA256SUMS as a pre-release
  `server-<version>-<sha>`. Upstream's `ci.yml` is manual-only in this fork.
- `SparkJoinIntegrationTest` moved from `flight-server` (where the `flight` Spark source is not on
  the classpath, so it always failed) to `flight-spark`, reading the server's `test_db`.
- Endpoint failover for partial results: every endpoint lists up to `endpointBackupLocations`
  (default 2, 0 = off) other live nodes after its planned node, those that can read all its
  files (HDFS/S3: any node; local disks: replica holders), best first (local bytes, load,
  rendezvous hash). Tickets are unchanged and run on any node; a node running an endpoint
  planned elsewhere logs `execution=failover` and takes over its load accounting. The planning
  node lists itself as a backup only for clients that send `x-flight-location-failover: 1`
  (older Trino plugins read every endpoint listing it from it). Clients may send keepalive
  pings every 5 s. Complete answers are unchanged.
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
- Storage plugins: module `flight-spi` (plugin API: stores, typed DuckDB setup, namespaced
  settings), plugins loaded from `<arrowflight.plugins.dir>/<name>/` (`FLIGHT_PLUGINS_DIR`,
  default `../plugins` beside the jar) in their own class loader, only when the data directory
  needs them; `SHA256SUMS` checked (`arrowflight.plugins.requireChecksums`). Plugins describe
  their DuckDB setup; the server writes and checks the SQL and reads the lockdown back at
  startup (settings, allowed directories, secrets) before the node joins the cluster.
- lakeFS data directories (`lakefs://<repository>/<ref>/<prefix>`, plugin `lakefs`): read
  through the lakeFS S3 gateway; a branch is pinned to its head commit per listing, so all
  endpoints of a query read the same commit. Settings `arrowflight.lakefs.*` /
  `FLIGHT_LAKEFS_*`.
- Table catalogs besides the data directory (`catalog=directory,iceberg-hadoop,iceberg-rest`),
  from plugins; their schemas appear next to the data directory's (a name the data directory
  has wins). Plugin `iceberg`: Iceberg tables of a warehouse directory in the Hadoop layout
  (`iceberg-hadoop`) or of an Iceberg REST catalog such as Gravitino or Polaris (`iceberg-rest`,
  bearer token or OAuth2 client credential from `FLIGHT_ICEBERG_REST_*` only). Complete answers
  read the snapshot with DuckDB's `iceberg` extension (position and equality deletes, renamed
  columns); partial results spread the snapshot's data files over the nodes, read by Parquet
  field ID, with the snapshot pinned in the tickets; tables with delete files, nested column
  changes or no data files, and joins of catalog tables, get the complete answer instead.
  Needs the DuckDB `iceberg` and `avro` extensions (`duckDbIcebergExtension`,
  `duckDbAvroExtension` or `arrowflight.duckdb.extensionDir`).
- Hive Metastore catalog (`catalog=hive`, plugin `hive`, Thrift client of Hive 3.1.3 without
  Hadoop): databases are schemas, Parquet tables are tables (other formats, views and ACID tables
  are skipped with a log line); partitions, their locations and their values come from the
  metastore (never from directory names; `__HIVE_DEFAULT_PARTITION__` is NULL), columns are read
  by name with the metastore's types, partition columns are typed constants. Every table and
  partition location must lie under `arrowflight.hive.roots`, else the table is hidden. Partial
  results ship each file's partition values in the ticket (partitions read once, at planning) and
  leave out partitions that simple predicates on partition columns rule out. Iceberg tables
  registered in the metastore are read through the iceberg plugin when it is installed.
  Metadata is cached (`namesTtlSeconds`, `metadataTtlSeconds`); a metastore that cannot be
  reached is an error (GetTables: UNAVAILABLE), never an empty schema.
- Plugin SPI 1.1 (additive): `DataStore.files(dir, recursive)`, `Catalog.pinnedColumns`, table
  formats other plugins' catalogs use (`TableFormat`, descriptor `provides.format`).
- Apache-2.0 LICENSE
- `CONTRIBUTING.md`, `SECURITY.md`, `CHANGELOG.md`
- Issue and PR templates
- `dependabot.yml` for automated dependency updates
- Release workflow triggered on `v*` tags
- `CODEOWNERS` for automatic review routing
- Package renamed from `net.surpin` to `pro.surpin`

### Changed
- DuckDB 1.5.6 (was 1.4.5; `duckdb_jdbc` 1.5.6.0): plain DuckDB answers a point lookup ~2x faster
  on the stand files. Deployments need the httpfs, iceberg, avro and hadoopfs extension files of
  v1.5.6 (the startup check refuses other versions); Dev CI takes them from `ci-assets` or, until a
  v1.5.6 set is there, the official httpfs/iceberg/avro builds.
- `duckDbThreads` defaults to 4 (was 2 in the bundled `arrowflight.properties`, 1 in code): SF10 1.8x and SF1
  1.3x faster on the stand for +2.6 ms on a point lookup (docs/dev/TUNING.md). Set 1 where compute shares the node.
- Partial results of Iceberg tables with delete files: a snapshot with position and/or equality
  deletes is no longer answered completely by one node. Its data files are spread over the nodes
  like other snapshots' (tickets unchanged: snapshot ID and file location), and the executing node
  takes each file's delete files from the pinned snapshot's plan and applies them with read_parquet
  anti-joins (as complete answers do). Such plans report an unknown row count (`-1`) instead of the
  files' recorded rows. Deletion vectors, columns without field IDs and equality columns the schema
  dropped still get the complete answer.
- Metadata endpoints (catalogs, schemas, tables, SqlInfo, type info) carry no location by
  default: clients read them over their own connection (stock JDBC metadata 1.4-1.6x faster in
  plaintext, 3.2x with TLS). `metadataEndpointLocation=advertised`
  (`FLIGHT_METADATA_ENDPOINT_LOCATION`) restores the advertised server URI.
- DuckDB is pinned to 1.4.5.0 (was 1.4.1.0): the hadoopfs and httpfs extension files must be
  the v1.4.5 builds (`extensions/v1.4.5/linux_amd64/`). The server needs JDK 21 (Netty 4.2 + Arrow
  fail on JDK 24+).
- HDFS: the server sets the hadoopfs extension's DataNode timeouts, `duckDbHdfsConnectTimeoutMs`
  (default 3000) and `duckDbHdfsReadTimeoutMs` (default 10000), as `hdfs_connect_timeout_ms` /
  `hdfs_read_timeout_ms`; they win over `LIBHDFS3_CONF`, `0` leaves them to the extension. An
  extension built without these settings keeps its own timeouts (one warning). The defaults were
  5000 / 15000 in earlier builds of this line: with the hadoopfs dead-DataNode list only the first
  read that hits a lost DataNode waits, and 3 s / 10 s cut that wait from ~15-22 s to ~4-11 s.
- S3 moved into the plugin `s3` (`plugins/s3`, built by `tools/make-plugin-dir.sh`): the AWS
  SDK is no longer in the server jar, and an `s3://` data directory without the plugin is a
  startup error naming it. All S3 settings keep their names (also `arrowflight.s3.*`,
  `FLIGHT_S3_*`, `etc/plugins/s3.properties`). `arrowflight.duckdb.extensionDir` may replace
  the per-extension file settings (`<dir>/v1.4.5/linux_amd64/<name>.duckdb_extension`).
- The DuckDB lockdown is read back at startup also for HDFS (hadoopfs) data directories.
- Calcite and with it Arrow Gandiva are gone from the server jar (318 MB -> 166 MB): the DDL
  logged at startup names its types itself, with the same names. `-Plinux-only` leaves out the
  DuckDB, zstd-jni and netty-tcnative native libraries of other platforms than Linux
  x86-64/ARM64 (113 MB).
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
