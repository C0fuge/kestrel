# Arrow Flight server @VERSION@ (@SHA@)

Development build of the maintainer's fork, commit @COMMIT@. Built by `dev-ci.yml` (job `package`) after the full
test suite passed on the same commit. Linux x86-64 / ARM64 only (`-Plinux-only`).

## Assets

- `arrow-flight-server-@VERSION@-@SHA@-linux.jar`: the shaded server jar.
- `flight-plugins-@VERSION@-@SHA@.tar.zst`: `plugins/{s3,lakefs,iceberg,hive}/` (jars + a `SHA256SUMS` per plugin).
- `flight-server-libs-@VERSION@-@SHA@.tar.zst`: `libs/`, the Hadoop client libraries the shaded jar leaves to the
  deployment (Hadoop is `provided`; the server does not start without them), mediated against the jar's own
  libraries so none repeats one of it; `libs/DEPENDENCIES.txt` lists the coordinates, `libs/SHA256SUMS` the jars.
  It replaces any older libs directory: do not mix it with other Hadoop, Netty, Jackson or BouncyCastle jars.
- `flight-auth-@VERSION@-@SHA@.tar.zst`: `auth/ldap/` - the optional LDAP / Active Directory login provider
  (`FLIGHT_AUTH_PROVIDER=ldap`, its jar + the UnboundID LDAP SDK + a `SHA256SUMS`) and `auth/jwt/` - the optional
  OIDC / Knox JWT provider (`FLIGHT_AUTH_PROVIDER=jwt`, its jar + nimbus-jose-jwt + a `SHA256SUMS`); each on the
  class path only when used.
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

- DuckDB extensions (httpfs, iceberg, avro, hadoopfs) of DuckDB 1.5.6 (was 1.4.5) must be installed as on the stand; the server
  checks the extension versions at startup.
- DuckDB runs a quarter of the cores (between 2 and 8) threads per connection by default (`duckDbThreads`, was a fixed
  4; the startup line `duckdb=threads perConnection=` shows the value), at least 4 when `dataDir` is an `s3://` /
  `s3a://` / `s3n://` / `lakefs://` URI (since 2026-10-28); set `-DduckDbThreads=4` to keep the old value,
  `-DduckDbThreads=1` / `FLIGHT_DUCKDB_THREADS=1` where compute workers share the node.
- HDFS: put the NameNode RPC timeouts into the `LIBHDFS3_CONF` file (`rpc.client.connect.timeout=5000`,
  `rpc.client.read.timeout=15000`, `rpc.client.write.timeout=15000`, `rpc.client.timeout=20000`,
  `rpc.client.connect.retry=2`); libhdfs3's defaults let a query hang for minutes after a NameNode host crash. The
  server logs a WARN at startup while one of them is unset or over a minute.
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
- Kerberos HDFS without an external `kinit`: set `hdfsKerberosPrincipal` + `hdfsKerberosKeytab`
  (`HDFS_KERBEROS_PRINCIPAL` / `HDFS_KERBEROS_KEYTAB`); the server writes and renews the ticket cache the hadoopfs
  extension reads (startup line `hdfs=kerberosTicketCache ... path=...`). Optional `hdfsKerberosTicketCache`; a WARN
  says when it is not the cache the extension reads (`KRB5CCNAME` / `hadoop.security.kerberos.ticket.cache.path`).
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
  `CompressionClientsIntegrationTest`); Java Flight clients need `arrow-compression` + `zstd-jni` (zstd) or
  `commons-compress` (lz4) on their class path. `lz4` needs lz4-java from `libs/` on the server. Counters
  `arrowflight_flight_compress*` on `/metrics`; level `flightCompressionLevel` (1), batches under
  `flightCompressionMinBytes` (64 KiB) go plain; `flightCompressionThreads` (0 = one per core, at most 8; 1 = off)
  compresses the columns of a large batch side by side.
- JDBC: the stock Arrow Flight SQL JDBC driver cannot decode compressed batches, so it must not ask (and the server
  default must stay `none` for it). Our patched driver can, once it carries the codecs: (1) bundle
  `org.apache.arrow:arrow-compression` (same Arrow version as the driver) and `com.github.luben:zstd-jni` (and
  `org.apache.commons:commons-compress` for lz4) in the driver jar, relocated the same way as its Arrow classes, with
  the `META-INF/services/...CompressionCodec$Factory` entry merged (ServicesResourceTransformer) so the driver's
  `FlightStream` finds the codec factory; (2) the driver already sends URL properties it does not know as call
  headers, so `jdbc:arrow-flight-sql://host:32010/?arrow-compression=zstd` asks for zstd on every call (the server
  only reads it on DoGet); no SetSessionOptions call is needed. Without (1) such a URL fails on the first
  compressed batch instead of answering wrong. Check: same answers with and without the property on a query of a
  few MB, and `arrowflight_flight_compressed_batches_total` rising.
- A cluster needs the Hazelcast member port reachable between the nodes only (keep it private).
- A node that crashes with its address (container removed, host down) holds every plan on the other nodes until
  Hazelcast drops it: `hazelcast.max.no.heartbeat.seconds`, default 10 (`-Dhazelcast.max.no.heartbeat.seconds=<s>`
  on every node; lower values also drop nodes that only pause, e.g. in GC). A stopped server leaves at once; a crashed
  one whose address still refuses connections is dropped within a second.
- Without TLS settings the server refuses to start unless `FLIGHT_ALLOW_INSECURE=true` (isolated test setups only).
- Changes since the previous build: `CHANGELOG.md` at @COMMIT@.
