# Arrow Flight server @VERSION@ (@SHA@)

Development build of the maintainer's fork, commit @COMMIT@. Built by `dev-ci.yml` (job `package`) after the full
test suite passed on the same commit. Linux x86-64 / ARM64 only (`-Plinux-only`).

## Assets

- `arrow-flight-server-@VERSION@-@SHA@-linux.jar`: the shaded server jar.
- `arrow-flight-codec-lz4-@VERSION@.jar`: for Java Flight *clients* only (the server jar already contains it): the
  LZ4 codec that decodes `arrow-compression: lz4` streams fast; needs lz4-java, goes before `arrow-compression`.
- `flight-plugins-@VERSION@-@SHA@.tar.zst`: `plugins/{s3,lakefs,iceberg,hive,jdbc}/` (jars + a `SHA256SUMS` per plugin).
- `flight-server-libs-@VERSION@-@SHA@.tar.zst`: `libs/`, the Hadoop client libraries the shaded jar leaves to the
  deployment (Hadoop is `provided`; the server does not start without them), mediated against the jar's own
  libraries so none repeats one of it; `libs/DEPENDENCIES.txt` lists the coordinates, `libs/SHA256SUMS` the jars.
  It replaces any older libs directory: do not mix it with other Hadoop, Netty, Jackson or BouncyCastle jars.
- `flight-auth-@VERSION@-@SHA@.tar.zst`: `auth/ldap/` - the optional LDAP / Active Directory login provider
  (`FLIGHT_AUTH_PROVIDER=ldap`, its jar + the UnboundID LDAP SDK + a `SHA256SUMS`) and `auth/jwt/` - the optional
  OIDC / Knox JWT provider (`FLIGHT_AUTH_PROVIDER=jwt`, its jar + nimbus-jose-jwt + a `SHA256SUMS`) and
  `auth/ranger-hive/` - the optional policy provider on the cluster's Hive Ranger service
  (`FLIGHT_AUTHORIZATION_PROVIDER=ranger-hive`, its jar only); each on the class path only when used.
- `SHA256SUMS`: checksums of the files above.

## Start

Java 21 (Netty 4.2 + Arrow fail on JDK 24+).

```bash
sha256sum -c SHA256SUMS
mkdir -p /opt/flight/server
tar --zstd -xf flight-plugins-@VERSION@-@SHA@.tar.zst -C /opt/flight
tar --zstd -xf flight-server-libs-@VERSION@-@SHA@.tar.zst -C /opt/flight
(cd /opt/flight/libs && sha256sum -c SHA256SUMS)
cp arrow-flight-server-@VERSION@-@SHA@-linux.jar /opt/flight/server/
# plugins are found as ../plugins next to the jar (here /opt/flight/plugins), or with FLIGHT_PLUGINS_DIR /
# -Darrowflight.plugins.dir; the jar comes first on the class path
FLIGHT_TLS_CERT=... FLIGHT_TLS_KEY=... FLIGHT_BEARER_TOKEN=... \
  java -cp "/opt/flight/server/arrow-flight-server-@VERSION@-@SHA@-linux.jar:/opt/flight/libs/*" \
    pro.surpin.data.arrowflight.server.HadoopArrowFlightServer \
    --data-dir <dir or s3://bucket/prefix> --localhost <this host> --port 32010 --hosts <host1,host2,...>
```

- Stopping a node (SIGTERM): since 2026-10-29 it drains for up to 20 s by default (`shutdownDrainSeconds` /
  `FLIGHT_SHUTDOWN_DRAIN_SECONDS`, was 0): it leaves the cluster's pool, answers new queries `UNAVAILABLE 'The server is
  stopping; retry the query'` (since item 37) and
  lets its running answers finish. `docker stop` waits only 10 s before SIGKILL: use `docker stop -t 25` (or more,
  or `--stop-timeout 25` on the container); systemd's default `TimeoutStopSec` (90 s) and Kubernetes' grace period
  (30 s) are enough. `0` restores the immediate stop (running answers fail with `INTERNAL`).
  Log of a stop: `shutdownDrain=started runningCalls=<n>`, then one `shutdownDrain=done after <ms> ms ...
  refusedQueries=<n> runningCalls=0` line. Queries refused while the node stops are only counted there
  (`refusedQueries=`): there is no per-query line for them. A per-query WARN `result=stopping` appears only for a query
  that met an already stopped pool (rare). When calls still run at the end, the line is a WARN `shutdownDrain=timeout
  ... runningCalls=<n> methods={DoGet=<n>}: these calls are cut`. On the stopping node a distributed-complete merge that
  falls back logs INFO `distributedComplete=fallback stopping=true` (the other nodes refuse its internal calls once it
  left the pool; expected).
- **hadoopfs build `51b7220-afc6`** (item 41; `ci-assets` `hadoopfs-v1.5.6-afc6.duckdb_extension`, sha256
  `836299dc639b2fd0f829723e3c7bfe28a403a04be7244482bde88191d8b40948`) is the CI / release default (was afc4; afc5 adds
  a listing cache, afc6 SASL data-transfer protection on the read path). On DataNodes with `dfs.data.transfer.protection`
  the new setting `hdfsSaslDataTransfer` (`arrowflight.hdfs.saslDataTransfer`, `HDFS_SASL_DATA_TRANSFER`, default
  `auto`) lets DuckDB read non-local blocks directly: no short-circuit-only planning, no node-local copies
  (`hdfsLocalCopyMaxBytes` unused). It needs `dfs.data.transfer.protection` (as on the DataNodes) and
  `hadoop.security.authentication=kerberos` in the `LIBHDFS3_CONF` file. Check the start line
  `hdfs=saslDataTransfer mode=auto direct=true extensionVersion=51b7220-afc6`; `direct=false reason=...` says why not
  (older build, setting missing). `-DhdfsSaslDataTransfer=off` restores the short-circuit-only path with copies; `on`
  skips the version check (another build that speaks SASL) and stops the start when `LIBHDFS3_CONF` lacks the settings.
  Not in afc6 yet: AES cipher negotiation (3DES wrap with `privacy`, slower), `dfs.encrypt.data.transfer`.
- **hadoopfs build `51b7220-afc4`** (DuckDB v1.5.6; `ci-assets` `hadoopfs-v1.5.6-afc4.duckdb_extension`, sha256
  `64a97a6b08e1f725c980d46f25d664a1c3edde8a73b624ac31bdc75f9095942c`) is the CI / release default (was afc3): file-status
  + located-blocks cache, lazy open from cached blocks, real modification times. New setting `hdfsMetadataCacheTtlMs`
  (`arrowflight.hdfs.metadataCacheTtlMs`, `HDFS_METADATA_CACHE_TTL_MS`): DuckDB `hdfs_metadata_cache_ttl_ms` on every
  connection, default 30000 with an `hdfs://` data dir, else 0 = off; start line `hdfs=metadataCache ttlMs=30000`
  (afc3 and older: one WARN `hdfs=metadataCacheUnsupported`, no cache). A file rewritten in place may be read with its
  old status for up to the TTL. On `hdfs://` `duckDbExternalFileCache` is now on by default in `balanced` too
  (startup line `resourceProfile profile=balanced duckDbExternalFileCache=true`); each connection's cache stays within
  its `memory_limit`, the node may hold that limit times its live connections - watch RSS under many clients, opt out
  with `-DduckDbExternalFileCache=false`.
- DuckDB extensions (httpfs, iceberg, avro, hadoopfs) of DuckDB 1.5.6 (was 1.4.5) must be installed as on the stand; the server
  checks the extension versions at startup. The jdbc plugin (`catalog=jdbc`) also needs `postgres_scanner` and/or
  `mysql_scanner` v1.5.6 in the extension directory; every function they add is refused in client queries, and each
  query of a jdbc table is answered complete by one node.
- DuckDB runs a quarter of the cores (between 4 and 16) threads per connection by default (`duckDbThreads`, was a
  fixed 4; the startup lines `duckdb=threads perConnection=` and `duckdb=sizing` show the value), above 4 at most one
  per GiB of the connection's memory limit. Since 2026-10-28 (laptop stand p19b) the floor is 4 everywhere: builds
  a9e43ca..2d93081 chose 2-3 threads on 4-16 core containers and ran TPC-H SF10 1.35-1.7x slower. Explicit values win;
  `-DduckDbThreads=1` / `FLIGHT_DUCKDB_THREADS=1` where compute workers share the node.
- Since item 19b every DuckDB connection gets a `memory_limit` by default (was DuckDB's 80 % of RAM per instance):
  (memory of the container or machine - `-Xmx` - direct memory - 15 %) / `duckDbMemoryConnections` (default cores /
  (cores / 4 between 2 and 16), at least 2), at least 1 GiB. A 2-CPU / 6 GB container with `-Xmx3g` and 2g direct gets 1 GiB per
  connection. `-DduckDbMemoryLimit=none` restores the old behaviour, an explicit size (e.g. `4GB`) wins.
- HDFS: put the NameNode RPC timeouts into the `LIBHDFS3_CONF` file (`rpc.client.connect.timeout=5000`,
  `rpc.client.read.timeout=15000`, `rpc.client.write.timeout=15000`, `rpc.client.timeout=20000`,
  `rpc.client.connect.retry=2`); libhdfs3's defaults let a query hang for minutes after a NameNode host crash. The
  server logs a WARN at startup while one of them is unset or over a minute.
- HDFS HA, the server's own Java Hadoop client (footers, listings, metadata): set in the `hdfs-site.xml` /
  `core-site.xml` on the server's class path `ipc.client.connect.timeout=5000`,
  `ipc.client.connect.max.retries.on.timeouts=2` and `dfs.client.failover.sleep.max.millis=2000`. With Hadoop's
  defaults a NameNode host crash stalls queries for 67-71 s (`getFileInfo` waits ~20 s per attempt on the dead
  NameNode); with these the slowest query on the maintainer's stand took 25.5 s (item 36 (5)).
- LDAP / Active Directory logins for JDBC/ODBC/ADBC and BI tools (user name + password, no Kerberos on the client):
  unpack `flight-auth-...tar.zst` into `/opt/flight`, add `:/opt/flight/auth/ldap/*` to `-cp`, and set
  `FLIGHT_AUTH_PROVIDER=ldap`, TLS (`FLIGHT_TLS_CERT` / `FLIGHT_TLS_KEY`), `FLIGHT_LDAP_URL=ldaps://dc1.corp:636
  ldaps://dc2.corp:636`, `FLIGHT_LDAP_SESSION_SECRET` (base64, >= 32 bytes, the same on every node) and either
  `FLIGHT_LDAP_USER_DN_TEMPLATE={0}@corp.example` (AD UPN bind) or `FLIGHT_LDAP_USER_SEARCH_BASE` +
  `FLIGHT_LDAP_USER_SEARCH_FILTER=(sAMAccountName={0})` + `FLIGHT_LDAP_BIND_DN` / `FLIGHT_LDAP_BIND_PASSWORD_FILE`.
  The login name is the Flight identity Ranger checks (groups: Hadoop's group mapping, as for Kerberos users; with
  `FLIGHT_LDAP_GROUP_SEARCH_BASE` also the user's directory groups - on a cluster set `FLIGHT_LDAP_BIND_DN` for it).
  All settings: `docs/security/providers.md` in the repository.
- OIDC / Knox JWT bearer tokens (Keycloak, Azure AD / Entra, Okta, Knox; the Flight SQL JDBC driver's `token=`,
  ADBC's `authorization` header): add `:/opt/flight/auth/jwt/*` to `-cp` and set `FLIGHT_AUTH_PROVIDER=jwt`, TLS,
  `FLIGHT_JWT_JWKS_URL=https://idp.corp/realms/data/protocol/openid-connect/certs` (or `FLIGHT_JWT_JWKS_FILE`),
  `FLIGHT_JWT_ISSUER` (exact `iss`), `FLIGHT_JWT_AUDIENCE`; optional `FLIGHT_JWT_USER_CLAIM` (default `sub`, e.g.
  `preferred_username` / `upn`) and `FLIGHT_JWT_GROUPS_CLAIM` (e.g. `groups`, `realm_access.roles`) for Ranger
  groups. No shared secret or session state between nodes: every call carries the token. Tools that can send only
  a user name and password: `FLIGHT_JWT_BASIC_PASSWORD=true` and the token as password.
- Hive Ranger policies instead of an `arrowflight` Ranger service (Hive catalog only): add
  `:/opt/flight/auth/ranger-hive/*` to `-cp`, set `FLIGHT_AUTHORIZATION_PROVIDER=ranger-hive`,
  `RANGER_HIVE_SERVICE_NAME` (the Hive service in Ranger admin), `RANGER_ADMIN_URL`, `RANGER_HIVE_SCHEMA_PREFIX` (the
  hive catalog's `schemaPrefix`), and not `RANGER_SERVICE_NAME`. Column masks and row filters are applied (queries
  of restricted tables are answered complete by one node); a column deny on any column, or a row filter outside the
  translated subset (see `docs/security/providers.md`), refuses the table; other catalogs' schemas are refused. Needs
  the hive plugin of the same release (SPI 1.8, Hive column types for CHAR(n) masks).
- Kerberos HDFS without an external `kinit`: set `hdfsKerberosPrincipal` + `hdfsKerberosKeytab`
  (`HDFS_KERBEROS_PRINCIPAL` / `HDFS_KERBEROS_KEYTAB`); the server writes and renews the ticket cache the hadoopfs
  extension reads (startup line `hdfs=kerberosTicketCache ... path=...`). Optional `hdfsKerberosTicketCache`; a WARN
  says when it is not the cache the extension reads (`KRB5CCNAME` / `hadoop.security.kerberos.ticket.cache.path`).
  Without either, the extension reads krb5's `default_ccache_name` (`KRB5_CONFIG` / `/etc/krb5.conf`): a `FILE:`
  default is used; a keyring or KCM default (RHEL-like hosts) fails the start - set `KRB5CCNAME=FILE:/path` then.
  Without `hdfsKerberos*` (an external `kinit`), a WARN `hdfs=noTicketCache` at start says the cache the extension reads
  is missing; for a keyring or KCM cache the server asks `klist -s <cache>` and warns (`hdfs=noTicketCache
  cache=KEYRING:... checked=true|false`) when it holds no valid ticket or klist is not installed.
- HDFS short-circuit and the native Hadoop library (libhadoop): all data reads go through the hadoopfs extension
  (libhdfs3), which reads short-circuit without libhadoop. Only the server's own Java HDFS client (Parquet footers and
  metadata, about one block per file) needs libhadoop for short-circuit; without it those few reads go over TCP, and on
  a cluster with `dfs.data.transfer.protection` they speak SASL (they work). The release does not bundle libhadoop; to
  use an existing one, point the server JVM at its directory: `-Djava.library.path=/opt/hadoop/lib/native` (also
  `LD_LIBRARY_PATH=/opt/hadoop/lib/native` for the libraries it loads), with `dfs.client.read.shortcircuit=true` and
  `dfs.domain.socket.path` in the `hdfs-site.xml` on the server's class path and the DataNode socket visible on the node.
  Take libhadoop from a Hadoop 3.x install for the same CPU and glibc (CI runs the 3.4.1 one with the server's 3.5.0
  jars). Without it Hadoop logs `NativeCodeLoader: Unable to load native-hadoop library` once at start; that WARN is
  harmless.
- HDFS with SASL DataNodes (`dfs.data.transfer.protection`, startup line `hdfs=shortCircuitOnly`): DuckDB reads only
  files whose every block is on the node's own DataNode. A partial endpoint or join that needs another file copies it
  whole through the Java Hadoop client into `localcopy` below the DuckDB temp directory (`duckDbTempDirectory`) and
  reads the copy (log `hdfs=localCopy`, per query `hdfs=reads ... shortCircuit=N localCopy=N`). **Disk cost:** up to
  `hdfsLocalCopyMaxBytes` (default 32 GiB) of unused copies per node, kept for reuse and evicted least recently used,
  plus whatever the running queries need at once (a query's copies are never evicted while it runs; beyond the limit
  they are deleted when it ends). A plain scan of one table (no aggregate, ORDER BY, LIMIT, DISTINCT) whose copies
  do not fit the limit is read in batches that fit, each batch's copies deleted before the next is copied (log
  `hdfs=copyBatches`). So are, with each batch's answer merged on the node, aggregates of count/sum/min/max per
  selected group (also with ORDER BY / LIMIT, with HAVING, and count/sum over DISTINCT values of one column) and
  scans with DISTINCT and/or LIMIT (log `merge=aggregate` / `merge=rows` / `merge=rewrite`), in both result
  modes: a `complete` query of one table with such a shape is read the same way over all the table's files (log
  `hdfs=completeInCopyBatches`). **Known limits:** a
  join, a `complete` answer that cannot be merged (window functions, avg routed to complete mode, most TPC-H joins)
  and a scan with ORDER BY and no LIMIT need all their copies at once; they run while those copies fit the free disk
  (below), beyond it they are refused naming the files. Put the temp directory on a disk with that much room, or give the copies a disk of their own with
  `hdfsLocalCopyDir=<absolute path>` (the server copies into a `duckdb-<pid>-*` subdirectory there, startup line
  `hdfs=localCopyCache dir=...` at the first copy). A query's new copies must leave `hdfsLocalCopyMinFreeBytes`
  (default min(1 GiB, 10 % of the disk's size), the cache line shows `minFreeBytes=... reserve=default|set
  diskBytes=...`) free on that disk (unused copies are evicted for room first, batches shrink to the room left);
  otherwise it fails at once naming the files and the free bytes (`hdfs=localCopyNoSpace`, status
  `RESOURCE_EXHAUSTED`; ADBC clients print it as `INTERNAL: ... (ResourceExhausted; ...)`) instead of filling the
  disk. A set `hdfsLocalCopyMinFreeBytes` not below the disk's size refuses every copy (the cache line is then a WARN).
  With `endpointsPerServer > 1` the tickets of one query copy on a node at once; on a small copy disk set
  `hdfsLocalCopyRoomWaitMillis=<ms>` (default 0 = refuse at once) so that a ticket whose copies do not fit while
  other running queries hold copies waits for them (`hdfs=localCopyRoomWait` / `hdfs=localCopyRoomWaited ms=`)
  instead of being refused (CI small disk, 4 endpoints: egret's `part-lineitem-sp` / `part-group-sp` exact with
  `hdfsLocalCopyRoomWaitMillis=60000`, no refusal).
  `hdfs=reads` is logged for every query, also with `localCopy=0` and, for answers reading no data-store table or
  merged from peers (`distributedComplete=true`), with `files=0 ... tables=none|unknown|endpoints=N`. `hdfsLocalCopyMaxBytes=0`
  turns copies off (such files are then refused). Copies go over the network at the DataNodes' speed and are deleted
  when the server stops.
- S3: DuckDB keeps its S3 connections and reads later ranges and queries over them
  (`arrowflight.s3.connectionCaching`, default true; startup line `s3=open ... connectionCaching=true`). Only with
  `connectionCaching=false` set `net.ipv4.tcp_tw_reuse=1` in the server's network namespace (container: `--sysctl
  net.ipv4.tcp_tw_reuse=1`) and widen `net.ipv4.ip_local_port_range` (e.g. `1024 65535`): httpfs then opens a new
  connection for most S3 range reads at 4+ threads and each closed one holds its local port 60 s, so many concurrent
  queries run out of ports ("Could not establish connection"); the server WARNs at startup in that case.
- S3 read cache (opt-in, `arrowflight.s3.cacheMegabytes`, default 0 = off): one cache per node for every DuckDB
  connection, in the JVM heap (at most half of `-Xmx`; the start fails above that). DuckDB then reads `s3://` through
  a loopback endpoint of the s3 plugin (127.0.0.1, random port, random per-store key checked as an S3 signature;
  credentials stay in Java), whole 1 MiB blocks, misses read from S3 with `If-Match` on the object's ETag. Startup
  line `s3=cacheOpen ... capacityBytes=...`, at shutdown `s3=cacheClosed ... hits=... misses=...`. A file rewritten
  in place under the same key can be answered in its old version for up to `arrowflight.s3.cacheCheckSeconds`
  (default 10); never a mix of versions. Globs on `s3://` paths (listings) are refused while it is on.
  While it runs, the Prometheus endpoint (`/metrics`) shows `arrowflight_s3_cache_*`: capacity/used bytes and
  blocks (gauges), `hits_total`, `misses_total`, `evictions_total`, `joined_total` (blocks taken from another
  request's read) and the requests it sent to S3 (`s3_reads_total`, `s3_read_bytes_total`, `s3_lookups_total` = HEADs).
  The heap it takes is its size: blocks are held in 256 KiB pieces (a 1 MiB array is a humongous G1 object and took
  2 MiB of heap, so `cacheMegabytes=1024` used ~2 GiB before 2026-10-28) and misses are streamed from S3 into them, no
  whole-GET buffer. `arrowflight.s3.cacheAdmission` (default `always` = unchanged): `reuse` keeps a missed block only
  when it is read again while it is among the last blocks not kept (as many names as the cache has blocks), so a scan
  of more data than the cache (TPC-H SF10 lineitem in a 1 GiB cache) passes through without pushing out what small
  queries read again; a repeated query is then served from the cache from its third run on. Counter
  `not_admitted_total`, `notAdmitted=` in the `s3=cacheClosed` line, `admitOnReuse=` in `s3=cacheOpen`.
- Read cache disk tier (opt-in, s3 and lakefs, 2026-10-28): `arrowflight.s3.cacheDiskPath=<absolute dir>` +
  `arrowflight.s3.cacheDiskMegabytes=<MiB>` (both, with `cacheMegabytes` > 0; `arrowflight.lakefs.` the same) put a
  second, larger tier of the cache on a local disk: blocks read from S3 are also written there in the background
  and answered from their files once memory dropped them. Use a local disk (not NFS) with that much free space; a
  `s3=cacheDiskSmall` WARN at start says when it has less. The directory is the server's own: its block files are
  deleted at start and at stop (other files are left alone), a second server on the same directory fails to start
  (lock `s3-cache.lock`); s3 and lakefs may share one path. The block files hold table data in the clear (their
  directory `<path>/s3-cache` is mode 700) - put the directory where only the server user can read. Every read is checked against a
  CRC-32C kept in memory, so a damaged file is read from S3 again (`arrowflight_s3_cache_disk_errors_total`).
  Metrics `arrowflight_s3_cache_disk_*`; `diskHits=` etc. in the `s3=cacheClosed` line.
- lakeFS read cache (opt-in, `arrowflight.lakefs.cacheMegabytes` / `arrowflight.lakefs.cacheCheckSeconds` /
  `arrowflight.lakefs.cacheAdmission`, same
  meaning and defaults): DuckDB reads the lakeFS S3 gateway through the same kind of loopback cache, for the whole
  repository (pinned commits never change, so their blocks stay valid). It is a cache of its own, separate from the
  s3 plugin's: with both on, keep the two sizes together below half of `-Xmx`. Log lines `lakefs=cacheOpen` /
  `lakefs=cacheClosed`, metrics `arrowflight_lakefs_cache_*`. A miss read whose answer carries another ETag than the
  one looked up is refused like a failed `If-Match` (for gateways that ignore `If-Match`; both caches).
- Result stream compression (opt-in, item 20): off unless a client asks. A client sends the call header
  `arrow-compression: zstd` or `lz4` on its DoGet calls, or sets the Flight SQL session option `arrow-compression`
  (SetSessionOptions; the session lives on the node that created it); `flightCompression=zstd|lz4` makes it the
  default for calls without either, which breaks every client without the codec - leave it at `none` while stock
  JDBC drivers read from the node. pyarrow and ADBC (C++/Go) decode both codecs as they are (checked in CI by
  `CompressionClientsIntegrationTest`); Java Flight clients need `arrow-compression` + `zstd-jni` (zstd) or,
  for lz4, the release asset `arrow-flight-codec-lz4-@VERSION@.jar` + lz4-java placed before `arrow-compression` on
  their class path (~2 GB/s decode; `commons-compress` also works but decodes at a few MB/s to ~180 MB/s). `lz4` needs lz4-java from `libs/` on the server. Counters
  `arrowflight_flight_compress*` on `/metrics`; level `flightCompressionLevel` (1), batches under
  `flightCompressionMinBytes` (64 KiB) go plain; `flightCompressionThreads` (0 = one per core, at most 8; 1 = off)
  compresses the columns of a large batch side by side.
- JDBC: the stock Arrow Flight SQL JDBC driver cannot decode compressed batches, so it must not ask (and the server
  default must stay `none` for it). Our patched driver can, once it carries the codecs: (1) bundle
  `org.apache.arrow:arrow-compression` (same Arrow version as the driver) and `com.github.luben:zstd-jni` (and
  for lz4 `arrow-flight-codec-lz4` + lz4-java, ahead of `arrow-compression` so its factory entry comes first) in
  the driver jar, relocated the same way as its Arrow classes, with
  the `META-INF/services/...CompressionCodec$Factory` entry merged (ServicesResourceTransformer) so the driver's
  `FlightStream` finds the codec factory; (2) the driver already sends URL properties it does not know as call
  headers, so `jdbc:arrow-flight-sql://host:32010/?arrow-compression=zstd` asks for zstd on every call (the server
  only reads it on DoGet); no SetSessionOptions call is needed. Without (1) such a URL fails on the first
  compressed batch instead of answering wrong. Check: same answers with and without the property on a query of a
  few MB, and `arrowflight_flight_compressed_batches_total` rising.
- Generic Flight clients (opt-in `plainFlightCommands=true` / `FLIGHT_PLAIN_COMMANDS=true`, P5): clients that speak
  plain Arrow Flight, not Flight SQL, may send a command descriptor with SQL text or a path descriptor naming a table
  (`schema.table`). ClickHouse: `SELECT ... FROM arrowFlight('flight-host:32010', 'sales.orders')` (path, its
  default; user and password - 4 arguments, or a named collection, which also has `enable_ssl` / `ssl_ca` for TLS -
  go as a Basic handshake whose bearer ClickHouse then sends, as the LDAP provider's Basic login expects; checked
  with ClickHouse over TLS in CI, `ClickHouseLdapLoginTest`), or `... arrowFlight('flight-host:32010', 'sales.orders WHERE day >= ''2026-01-01''') SETTINGS
  arrow_flight_request_descriptor_type = 'command'` - ClickHouse then sends `SELECT * FROM <second argument>`, so
  the filter runs on the server. ClickHouse reads every endpoint over its one connection; the answer is complete
  (one endpoint) unless the client sends `x-flight-result-mode: partial`. Checked with ClickHouse 26.8 and pyarrow in
  CI (`GenericFlightClientsIntegrationTest`).
- A cluster needs the Hazelcast member port reachable between the nodes only (keep it private).
- A node that crashes with its address (container removed, host down) stays a cluster member until Hazelcast drops
  it: `hazelcast.max.no.heartbeat.seconds`, default 10 (`-Dhazelcast.max.no.heartbeat.seconds=<s>` on every node;
  lower values also drop nodes that only pause, e.g. in GC). Plans no longer wait for it: the first plan after the
  crash waits up to 1 s for its partitions, then plans leave that node out until it answers again or is dropped
  (log `did not answer within 1000 ms`). Writes to its partitions (load accounting, tickets with load tracking,
  heartbeats) still wait. A stopped server leaves at once; a crashed one whose address still refuses connections is
  dropped within a second.
- Without TLS settings the server refuses to start unless `FLIGHT_ALLOW_INSECURE=true` (isolated test setups only).
- Changes since the previous build: `CHANGELOG.md` at @COMMIT@.

## Resource profile `speed` (item 23)

`-DresourceProfile=speed` (or `FLIGHT_RESOURCE_PROFILE=speed`) turns on, as defaults, what was measured as faster;
`balanced` (default) changes nothing, except `duckDbExternalFileCache=true` on an `hdfs://` data dir (Q11, item 39;
also under `speed` without the shared DuckDB). A setting given as `-D` or in the environment (or in a plugin's
`<ns>.properties`) wins over the profile; the profile wins over the bundled `arrowflight.properties`. Startup line
`resourceProfile profile=speed <key>=<value> ...` (operator values marked `(explicit)`).

| Setting | `speed` value | Cost |
|---|---|---|
| `duckDbSharedDatabase` | true | one DuckDB per node: node-wide thread pool and memory limit |
| `duckDbSharedMaxThreads` | 0 (no cap) | |
| `duckDbThreads` | all cores, at least what `balanced` gives one query (4) (not set when `duckDbSharedMaxThreads` is set) | every core busy on one heavy query; small-query tails may grow |
| `duckDbMemoryLimit` | node-wide: what `balanced` gives its expected connections together (per-connection limit x connections), at least 1 GiB, at most 80 % of memory (only while the shared DuckDB is on) | e.g. 32 GB / `-Xmx8g` / direct 4g: 15564 MiB (balanced: 4 x 3891 MiB); 6 GB / `-Xmx3g` / direct 2g on S3: 2 GiB (balanced: 2 x 1 GiB) |
| `duckDbExternalFileCache` | true (only while the shared DuckDB is on; on an `hdfs://` data dir always, as in `balanced`) | DuckDB keeps S3/HDFS file data in memory, within the node-wide `memory_limit` (evicted like any buffer): peak RSS about 2x balanced (S3 TPC-H SF10 on 2 CPUs / 6 GB: 3.3 vs 1.6 GiB); S3 small queries up to 5x faster, TPC-H SF10 on S3 3.1x faster (283.6 -> 91.2 s) |
| `duckDbPreserveInsertionOrder` | false (item 27) | rows of a query without ORDER BY come in any order (as upstream's default; ORDER BY answers are unchanged): one full `lineitem` read on S3 910 -> 1118 MB/s (+23 %), 4 concurrent reads +5 %, same peak memory; `-DduckDbPreserveInsertionOrder=true` (or `DUCKDB_PRESERVE_INSERTION_ORDER=true`) restores the file order |
| `flightZeroCopyWrites` | true | |
| `endpointsPerServer` | 4 | partial-mode clients get 4 streams per node |
| `flightBackpressureThresholdBytes` | 16 MiB (bundled default 256 MiB) | needed with several endpoints per node (direct memory) |

**Row order under `speed`:** a query without ORDER BY returns its rows in any order (they may differ from run to
run and from `balanced`, which keeps the file order); counts, aggregates and ORDER BY answers are unchanged. Clients
that rely on the file order should add ORDER BY, or start the server with `-DduckDbPreserveInsertionOrder=true` (or
`DUCKDB_PRESERVE_INSERTION_ORDER=true`), which keeps the rest of `speed` (a full scan on S3 then reads about 20 % slower: 1118 -> 910 MB/s in item 27).

A query that runs out of the shared DuckDB's memory before anything was sent runs once more on a private DuckDB of
its thread, sized as one `balanced` connection (log `duckdb=outOfMemoryRetry`); that DuckDB stays open for the thread's
next refusal, so a node under memory pressure holds at most what `balanced` would.

`speed` leaves the s3/lakeFS read caches (`arrowflight.<ns>.cacheMegabytes`) as configured (off by default): with
them on, TPC-H SF10 on S3 was 1.33x slower (one pass reads each file once, the cache only adds a hop and heap);
without them `speed` is as fast as `balanced` there. Turn a cache on yourself for workloads that re-read the same files.

**Sizing `speed` for S3 (item 40(a)):** DuckDB's external file cache lives inside the node-wide `memory_limit`, and
`speed` derives that limit from memory - `-Xmx` - `MaxDirectMemorySize` - 15 %. With a large heap and direct reservation
the limit is small, the cache cannot hold the data a workload re-reads, and the warm passes read S3 again. Measured
(`tpch-perf.yml`, S3 TPC-H SF10, 4 CPUs / 8 GB container, ADBC; runs 42748064489, 42751636105): `-Xmx3g`, direct 2g
(limit 2048 MiB) 75.8 s; same JVM with `-DduckDbMemoryLimit=5120MiB` 19.4 s; `-Xmx1g -XX:MaxDirectMemorySize=1g`
(limit 4914 MiB, peak RSS 4.1 of 8 GB) 15.9 s - plain in-process DuckDB 1.5.6 (the GizmoSQL-like reference) 16.9 s,
same answers. On an S3 node under `speed`, give the JVM only what it needs (the Java side holds batches in flight,
not table data: 1 GiB heap and 1 GiB direct served TPC-H SF10 above), or set `duckDbMemoryLimit` yourself; check the
start line `duckdb=sizing ... memoryLimit=` and keep the sum of heap, direct memory and the DuckDB limit below the
container's limit. The defaults are unchanged until the maintainer decides Q-40a (docs/dev/PLUGINS-STATUS.md).

## Settings since 7562a5bdb7b9

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
| `defaultSchema` (`arrowflight.defaultSchema`, `FLIGHT_DEFAULT_SCHEMA`) | unset | schema a table name without a schema reads when several schemas have that table; unset = such a name is refused `INVALID_ARGUMENT ... exists in schemas ...` (item 39 (a)); a client's session option or call header `schema` (JDBC URL `?schema=tpch`) goes before it |
| `distributedComplete` (`arrowflight.cluster.distributedComplete`, `FLIGHT_DISTRIBUTED_COMPLETE`) | false | the planning node merges complete answers from the nodes' partial endpoints |
| `distributedCompleteTrustedCertificates` (`arrowflight.cluster.distributedCompleteTrustedCertificates`, `FLIGHT_DISTRIBUTED_COMPLETE_TRUSTED_CERTS`) | empty = the node's own certificate | peer TLS trust of distributed complete mode |
| `distributedCompletePeerStreams` (`arrowflight.cluster.distributedCompletePeerStreams`, `FLIGHT_DISTRIBUTED_COMPLETE_PEER_STREAMS`) | 0 = one per 128 MiB of the smaller of direct memory and the Arrow allocator limit (`arrowAllocatorLimitBytes`, default 2 GiB), at least 2; one-row merges are not counted | endpoint streams the distributed complete merges of a node hold open at once; a merge waits for room (items 35, 39 (d)) |
| `distributedCompleteAdmissionWaitMillis` (`arrowflight.cluster.distributedCompleteAdmissionWaitMillis`, `FLIGHT_DISTRIBUTED_COMPLETE_ADMISSION_WAIT_MILLIS`) | 300000 | longest wait of a merge for room, then `RESOURCE_EXHAUSTED` |
| `dataDirShared` (`arrowflight.storage.dataDirShared`, `FLIGHT_DATA_DIR_SHARED`) | false | a local data directory is a mount every node reads (NFS) |

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
