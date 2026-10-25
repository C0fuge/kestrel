# Dependency audit (third-party packages)

Date: 2026-10-23. Read-only audit; no code changed, nothing pushed.

Method
- Server (afs-dev-cloud): `./mvnw dependency:list` (runtime scope, resolved for spi/plugins/server/client-core; flight-spark and flight-starrocks parsed from poms because the reactor could not resolve siblings without a build) plus the poms for AWS SDK / Iceberg / Hadoop / Spark.
- Libs dir (afc-offline/bench/tools/agent-fo-bench/libs, 335 jars): coordinates inferred from jar names (group guessed from the artifact name; ~20 odd ones mapped by hand).
- Trino plugin: poms plus the jar list inside `dist-out/trino-arrow-flight-plugin.tar.xz` (the built artifact, which is older than the pom for Jackson).
- StarRocks: `starrocks-src` (tag 4.1.4 + 2) `fe/pom.xml` and `thirdparty/vars.sh`.
- hadoopfs: `sources.env` / `scripts/build-deps.sh` of C0fuge/plover.
- Vulnerabilities: OSV `querybatch` (Maven ecosystem) by package + version, details from `/v1/vulns/<id>`. Latest versions: repo1.maven.org `maven-metadata.xml` (stable only), PyPI JSON.
- Severity is the GHSA database severity (CRIT/HIGH/MOD/LOW). OSV counts below are "number of advisories matching this exact version". The "fix >=" column is the lowest version of the same major.minor line that clears all listed advisories (approximate; check the line you stay on).
- Caveat: OSV does not see shaded/bundled jars (hadoop-client-runtime, jersey-bundle, calcite/avatica shaded parts, grpc-netty-shaded) or native C/C++ libs. Native versions (StarRocks BE, hadoopfs) are from my own knowledge of the upstream CVE history, NOT OSV-verified; treat as "to verify".
- "Reachable?" is a quick judgement from the code/architecture, not a proof.

Dates in CVE ids are as published in OSV (2026-dated ids are recent).

---

## 1. Server: afs-dev-cloud (pom-resolved versions)

Effective versions: Arrow 19.0.0, gRPC 1.84.2 (server; client-core/spark pull 1.79.0 via Arrow), Netty 4.2.9.Final in flight-server (4.1.130.Final transitive in client-core/spark), Jackson 2.15.3 (pinned; jsr310 2.21.0, jaxrs 2.17.2 mixed), protobuf 4.34.1, Guava 33.4.8 (32.1.3 pinned in client-core/spark), Hazelcast 5.6.0, DuckDB JDBC 1.4.5.0, AWS SDK 2.34.1, Iceberg 1.12.0, Hadoop 3.5.0 (provided), Parquet 1.13.1, Hive metastore 3.1.3 + libthrift 0.9.3, BouncyCastle 1.85, Spark 3.5.9 (provided/test), jOOQ 3.21.6.

No OSV advisories at all for: Arrow 19.0.0, gRPC 1.84.2/1.79.0, protobuf 4.34.1/4.33.4, Guava 33.4.8/32.1.3, DuckDB 1.4.5.0, AWS SDK 2.34.1, Iceberg 1.12.0, Hadoop 3.5.0, Spark 3.5.9, BouncyCastle 1.85, jOOQ, SLF4J 2.0.17, commons-lang3 3.20.0, commons-codec 1.21.0, commons-cli, snakeyaml 2.1, json-smart 2.4.10, Ranger 2.8.0, hive-standalone-metastore 3.1.3.

| Package | Version | Severity | CVE / GHSA (main) | Fixed in | Reachable in our use? | Action |
|---|---|---|---|---|---|---|
| com.hazelcast:hazelcast | 5.6.0 | CRITICAL 1, HIGH 1 | CVE-2026-107726 (GHSA-6v25-8wq6-xq4j, member memory access by low-privileged client); CVE-2026-107725 (GHSA-w294-6q5q-53p8, IMap Predicates authz bypass) | 5.7.0 | Yes, if the Hazelcast port (HazelcastAdapter, member mode, fixed port) is reachable by untrusted peers. We use embedded members, so any process that can speak the member protocol is a "client". IMap predicates only if we use them. | Upgrade to 5.7.0 (latest). Meanwhile keep the cluster port on a private network / enable member TLS + auth. |
| com.fasterxml.jackson.core:jackson-databind | 2.15.3 (server, client-core, spark) | HIGH 5, MOD 7 | CVE-2026-91777, CVE-2026-91776, CVE-2026-68497 (DoS); CVE-2026-54512/54513 (PolymorphicTypeValidator bypass); CVE-2026-54514/77310 (DNS/SSRF on InetSocketAddress) | 2.18.11 / 2.21.7 / 2.22.3 | Partly. Used for config/JSON parsing; polymorphic-typing bypasses matter only if default typing is enabled on untrusted JSON (check). DoS ones apply to any untrusted JSON (Flight SQL command/ticket JSON if any). | Bump to 2.18.11 (LTS line) or 2.22.3; align jackson-core/annotations/yaml/jsr310 on one version (currently 2.15.3 / 2.17.2 / 2.21.0 mixed). Java 21 allows 2.22.x. |
| com.fasterxml.jackson.core:jackson-core | 2.15.3 | HIGH 2, MOD 1 | CVE-2026-89425, CVE-2026-68494 (async parser maxNumberLength bypass), CVE-2026-18401 | 2.18.11 / 2.21.7 / 2.22.3 | Async parser only; low. Others: untrusted JSON DoS. | Same bump as databind. |
| io.netty:netty-handler | 4.2.9 (4.1.130 transitive) | CRITICAL 1, HIGH 3, MOD 1 | CVE-2026-75595 (SNI routing bypass via fragmented ClientHello); CVE-2026-50010 (wrapped plain TrustManager silently disables hostname verification); CVE-2026-45416, CVE-2026-44249 | 4.2.17 / 4.1.137 | SNI: no SniHandler in our code or gRPC, so not reachable. CVE-2026-50010 may matter for client-side TLS with a custom X509TrustManager (flight-client-core/spark) - check. | Bump Netty (see next row). |
| io.netty:netty-codec-http2 | 4.2.9 / 4.1.130 | HIGH 3, MOD 4 | CVE-2026-33871 (CONTINUATION flood), CVE-2026-56819 (decompress ref leak), CVE-2026-50560 (reset attack variant), CVE-2026-47244 (MAX_CONCURRENT_STREAMS not enforced), CVE-2026-42587 | 4.2.16 / 4.1.136 | Yes. unshaded grpc-netty runs the Flight gRPC port on this codec; reachable by any client that can connect (gRPC adds some limits). | Bump Netty to the latest 4.2.x (4.2.19 on Central) and 4.1.x for client-core/spark; use `netty-bom` so all modules share one version. |
| io.netty:netty-codec-http | 4.2.9 / 4.1.130 | HIGH 6, MOD 10 | CVE-2026-33870 (chunked-ext smuggling), CVE-2026-42584, CVE-2026-55831 (SPDY), CVE-2026-56745 ... | 4.2.17 / 4.1.137 | Mostly no (HTTP/1 server/SPDY/CORS/WebSocket not used by a gRPC server). Pulled in by netty-all and by grpc-netty proxy support. | Bump with the rest. |
| io.netty: codec / codec-compression | 4.2.9 / 4.1.130 | HIGH 2 | CVE-2026-59901 (Bzip2 loop), CVE-2026-42583 (Lz4FrameDecoder resource exhaustion) | 4.2.16 / 4.1.136 | No (we do not decode Lz4Frame/Bzip2 from the network). | Bump. |
| io.netty: haproxy, redis, stomp, mqtt, smtp, xml, sctp, http3, quic, ssl-ocsp, resolver-dns, epoll/kqueue | 4.2.9 | HIGH (about 25 advisories in total) | e.g. CVE-2026-44250/50011 (redis), CVE-2026-44893/48059 (haproxy), CVE-2026-56817 (xml), CVE-2026-56820/21/22 (OCSP), CVE-2026-47691 (DNS), CVE-2026-42577 (epoll RST DoS) | 4.2.13 to 4.2.17 | Mostly no: the code is on the classpath only because flight-server depends on `netty-all` (a bundle of every codec). epoll transport: reachable on Linux if gRPC uses epoll (default) - CVE-2026-42577 DoS by RST. | Replace `netty-all` by the needed modules (buffer, codec-http2, handler, transport-native-epoll, tcnative) via netty-bom: removes about 25 unreachable advisories and ~15 jars. |
| io.airlift:aircompressor | 0.21 (parquet-hadoop) | HIGH 2 | CVE-2024-36114 (JVM crash / memory leak on crafted LZ4/Snappy/Zstd), CVE-2025-67721 | 0.27 / 2.0.3 | Yes if ParquetAdapter reads untrusted Parquet files with those codecs. | Exclude/override to aircompressor 2.0.3 (check parquet 1.13.1 compatibility) or upgrade parquet to 1.15+ / 1.18.1. |
| org.apache.thrift:libthrift | 0.9.3 (hive plugin) | HIGH 5, MOD 1 | CVE-2026-43869 (host mismatch accepted in TLS), CVE-2026-43871, CVE-2020-13949, CVE-2019-0205, CVE-2018-1320 | 0.23.0 / 0.24.0 | Only when talking to a malicious/compromised HMS (parser DoS), or to an HMS over TLS (CVE-2026-43869 hostname validation). Hive 3.1.3 generated code needs a compatibility test with a newer runtime. | Override to libthrift >= 0.16 (0.24.0 is what StarRocks FE uses) and run the HMS plugin tests; long-term Hive metastore client 4.x (4.2.1). |
| org.xerial.snappy:snappy-java | 1.1.8.3 | HIGH 2, MOD 2 | CVE-2023-43642, CVE-2023-34455 (DoS on crafted chunk), CVE-2023-34453/54 | 1.1.10.4 | Yes if Parquet/Snappy data is read from untrusted sources. | Bump to 1.1.10.12 (latest). |
| org.apache.commons:commons-compress | 1.23.0 | MOD 3 | CVE-2024-26308, CVE-2024-25710, CVE-2023-42503 | 1.26.0 | Unlikely (archives not parsed). | Bump to 1.28.0. |
| org.apache.commons:commons-configuration2 | 2.8.0 | MOD 3 | CVE-2026-45205, CVE-2024-29133/29131 (StackOverflow) | 2.15.0 | Hadoop config only; low. | Bump to 2.15.1. |
| com.nimbusds:nimbus-jose-jwt | 10.0.1 | MOD 1 | CVE-2025-53864 (deep nesting JSON DoS) | 10.0.2 | Yes if JWT auth is used in the server (via Ranger/hadoop-auth transitive path - verify). | Bump to 10.10. |
| com.google.guava:guava | 32.1.3 (client-core, spark) | none | - | - | - | Optional: align on 33.x. |

Outdated but without a known vulnerability: commons-lang3 3.20.0 (3.21.0), protobuf 4.34.1 (4.36.2), gRPC 1.84.2 (1.85.1), AWS SDK 2.34.1 (2.57.3, about 23 minor releases behind), Parquet 1.13.1 (1.18.1; 1.13.1 is 2023, and parquet-avro 1.13.1 has CRITICAL CVE-2025-30065 but we do not use parquet-avro), Hive metastore 3.1.3 (4.2.1), slf4j-simple 2.0.20, dagger 2.50 (2.60.1), mockito 5.20 (5.24), JUnit 5.14.4 (6.1.3 exists), Hadoop 3.5.0 (current), Iceberg 1.12.0 (current), jooq 3.21.6 (3.21.9).

---

## 2. Server runtime libs set (bench stand): afc-offline/bench/tools/agent-fo-bench/libs (335 jars)

This set is clearly an older snapshot than the poms (Arrow 18.0.0, Netty 4.1.114, gRPC 1.65.0, DuckDB 1.4.1.0, BouncyCastle 1.79, Jackson 2.15.3) and it carries a Spark/Hadoop 3.3.x/ZooKeeper/Jetty 9.4/log4j 2.20 tree. Both `reload4j` and `log4j-core 2.20.0` are present, plus `netty-all` AND the split netty jars (duplicate classes), plus `netty-3.10.6`.

| Package | Version | Severity | CVE / GHSA (main) | Fixed in | Reachable? | Action |
|---|---|---|---|---|---|---|
| org.bouncycastle:bcprov-jdk18on / bcpkix | 1.79 | CRITICAL 2, HIGH 1, MOD 1 (bcpkix MOD 1) | CVE-2025-14813 (GOST 28147 CTR keystream reuse), CVE-2026-8763 (name-constraints bypass), CVE-2026-13506 | 1.85 | Yes for TLS cert-path/name-constraint validation if BC provider is used for cert handling (server uses BC for test certs / PEM). | Replace with 1.85 (pom already uses 1.85 - rebuild the libs set). |
| io.netty:netty (3.x) | 3.10.6.Final | CRITICAL 1, HIGH 3, MOD 5 | CVE-2019-20444, CVE-2021-37136/37137, CVE-2019-16869 | none (EOL line) | Only if code uses Netty 3 (ZooKeeper client/Hadoop). Not the Flight path. | Drop the jar (zookeeper 3.6.3 client can use Netty 3 only via optional transport; remove ZooKeeper if unused). |
| org.apache.zookeeper:zookeeper | 3.6.3 | CRITICAL 1, MOD 1 | CVE-2023-44981 (SASL quorum auth bypass) | 3.8.4 | No (quorum auth is server-side; we only carry the client via Hadoop/Curator). | Drop or bump to 3.9.x with Curator 5.x. |
| io.netty 4.1.114 (handler, codec-http2, codec-http, codec, redis, ...) | 4.1.114.Final | CRITICAL 1, HIGH about 25, MOD about 20 | same set as section 1 plus CVE-2025-55163 (HTTP/2 MadeYouReset), CVE-2025-24970 (SslHandler native crash), CVE-2025-59419 (SMTP), CVE-2025-25193 (netty-common) | 4.1.137 | HTTP/2 codec: yes (Flight gRPC port). Others no. | Rebuild libs set from the current pom (Netty 4.2.9 -> bump 4.2.17+). |
| com.fasterxml.jackson.core 2.15.3 | 2.15.3 | HIGH 7 | see section 1 | 2.18.11+ | same as section 1 | Bump. |
| com.hazelcast:hazelcast | 5.6.0 | CRITICAL 1, HIGH 1 | see section 1 | 5.7.0 | see section 1 | Bump. |
| org.lz4:lz4-java | 1.8.0 | HIGH 3, MOD 4, LOW 1 | CVE-2025-66566, CVE-2025-12183, CVE-2026-106451 | 1.8.1 | Yes if Spark/Arrow/Parquet LZ4 decompress untrusted data (Arrow IPC LZ4 frames are decompressed by arrow-compression, not by this jar; Spark shuffle uses it). Low on Flight server path. | Bump to 1.8.1 (org.lz4 coordinates; the old net.jpountz name is relocated). |
| org.eclipse.jetty 9.4.51 (server, http, security, xml) | 9.4.51.v20230217 | HIGH 3, MOD 4, LOW 2 | CVE-2024-13009 (gzip buffer leak), CVE-2026-10050 (security), CVE-2026-2332 (http) | 9.4.58 fixes some; 9.4 line is EOL, newer fixes only in 12.x | Only Hadoop/Spark web UIs; not on the Flight path unless the Hadoop HttpServer is started. | Drop from the libs set if unused; otherwise move to Jetty 12 (Hadoop 3.5 needs 9.4 -> check). |
| org.apache.logging.log4j:log4j-core / api / 1.2-api | 2.20.0 | MOD 5 | (none critical; Log4Shell fixed since 2.17) | 2.25.4 / 2.25.5 | Low. | Bump to 2.26.1 (latest) or remove log4j (reload4j + slf4j-reload4j 1.7.36 already present: pick one logging backend). |
| org.apache.httpcomponents.core5: httpcore5 5.3.5, httpcore5-h2 5.3.4; client5 5.5 | 5.3.x | HIGH 2, MOD 1 | CVE-2026-54399, CVE-2026-54428, (client5 MOD) | 5.4.3 / 5.6.3 | Only if the AWS SDK uses the client5 (our S3 plugin uses `apache-client` = httpclient 4.5.13, not client5). | Bump to 5.4.4 / 5.6.5 or drop. |
| commons-beanutils | 1.9.4 | HIGH 1 | CVE-2025-48734 | 1.11.0 | Hadoop config only. | Bump 1.11.0. |
| commons-io | 2.8.0 | HIGH 1 | CVE-2024-47554 (XmlStreamReader DoS) | 2.14.0 | Low. | Bump 2.22.0. |
| dnsjava | 2.1.7 | HIGH 1 | CVE-2024-25638 and others | 3.6.0 | Kerberos DNS lookups only. | Bump to 3.6.5. |
| io.airlift:aircompressor 0.21, snappy-java 1.1.8.3, nimbus-jose-jwt 9.8.1 (HIGH CVE-2023-52428), commons-compress 1.23.0, commons-configuration2 2.8.0, hadoop-common 3.3.6 (LOW) | | see section 1 | | | | Bump as in section 1. |
| com.google.protobuf:protobuf-java-util | 3.25.4 | HIGH 1 (protobuf-java 3.25.4 core, CVE-2024-7254) | CVE-2024-7254 | 3.25.5 | Parser DoS on deeply nested groups; the core in the set is 4.34.1, only util is 3.25.4 (mixed). | Align on one protobuf version. |
| Arrow 18.0.0, gRPC 1.65.0, DuckDB 1.4.1.0, AWS SDK 2.34.1, Guava 33.3.1, Hadoop 3.3.6 | | no OSV advisories | | | | Outdated against the poms (Arrow 19.0.0, gRPC 1.84.2, DuckDB 1.4.5.0). Rebuild the libs set from the current pom. |

Shaded jars OSV cannot see: `hadoop-client-runtime-3.3.4` / `hadoop-client-api-3.3.4` (bundles old Jackson, Woodstox, protobuf 2.5/3.x, commons) and `jersey-bundle` style jars - treat as having the same Jackson/Woodstox CVEs as upstream 3.3.4; prefer Hadoop 3.4.x/3.5.0 (as in the pom).

---

## 3. Trino plugin: trino-arrow-flight-connector (profiles Trino 455/477/483)

Plugin-bundled libs (from the built tarball; pom pins Arrow 18.1.0, gRPC 1.65.1, Netty 4.1.114, Guava 33.4.0, Guice 7.0.0, SLF4J 2.0.16). Note the packaged artifact still has Netty 4.1.100 for http2/http/socks/handler-proxy/unix-common and gRPC 1.65.0 despite the pom pins - rebuild before shipping. The pom also imports jackson-bom 2.22.1 (fresh build would give 2.22.1, not the 2.18.0 seen in the tar).

| Package | Version | Severity | CVE / GHSA (main) | Fixed in | Reachable? | Action |
|---|---|---|---|---|---|---|
| io.netty:netty-codec-http2 | 4.1.100 (in tar), 4.1.114 pinned | HIGH 4, MOD 4 | CVE-2025-55163 (MadeYouReset), CVE-2026-33871 (CONTINUATION flood), CVE-2026-56819, CVE-2026-42587 | 4.1.136 | Client side of Flight gRPC (Trino worker connects to our server); malicious/compromised Flight server could send crafted frames. Lower than server side. | Bump `dep.netty.version` to 4.1.137; make the pin apply to http2/http/socks/proxy/unix-common (they are 4.1.100 now). The gRPC 1.65.x pin keeps Netty on 4.1.x - test gRPC 1.7x+ with Netty 4.1.137. |
| io.netty:netty-handler / codec / common | 4.1.114 | CRITICAL 1, HIGH 4 (handler); HIGH 2 (codec); MOD 2 (common) | CVE-2026-75595 (SNI, not reachable), CVE-2025-24970 (SslHandler crash with native SSL engine, tcnative is used), CVE-2026-50010 (hostname verification when TrustManager is wrapped), CVE-2026-59901/42583 | 4.1.137 | CVE-2025-24970 and CVE-2026-50010 relevant to TLS to the Flight server. | Bump Netty (above). tcnative 2.0.66 -> match Netty (2.0.7x). |
| com.fasterxml.jackson.core:jackson-databind/core | 2.18.0 (tar); 2.22.1 (pom bom) | HIGH 5 + MOD 8 (2.18.0); HIGH 3 + MOD 2 and core HIGH 2 (2.22.1) | CVE-2026-91777, CVE-2026-68497, CVE-2026-91776; core CVE-2026-89425, CVE-2026-89407 | 2.22.3 / 2.18.11 | Inside Trino plugin classloader: Airlift json for config; low. | Override jackson-bom to 2.22.3 (or 2.18.11). |
| com.google.protobuf:protobuf-java / -util | 3.25.4 | HIGH 1 | CVE-2024-7254 | 3.25.5 | Parser DoS on crafted nested groups from a remote Flight server. | Bump to 3.25.8 (3.25 line) with Arrow flight-core 18.1.0. |
| io.opentelemetry:opentelemetry-api | 1.37.0 (provided at 1.41.0 in pom) | MOD 1 | | 1.62.0 | `provided` (Trino ships its own). | Ignore. |
| org.apache.logging.log4j:log4j-api / to-slf4j | 2.23.1 | MOD 1 | | 2.25.5 | Low. | Bump with Arrow/BOM. |
| Arrow 18.1.0, gRPC 1.65.0/1.65.1, Guava 33.4.0, Guice 7.0.0, Airlift 252, gson 2.10.1, commons-codec 1.17.1, hibernate-validator 8.0.1, byte-buddy 1.14.17 | | no OSV advisories | | | | Outdated: Arrow 18.1.0 (19.0.0 is latest), gRPC 1.65 (1.85.1), Airlift 252, Trino SPI 455 profile (latest 484; 477/483 profiles exist). |

Trino itself (455) is provided by the host and is not audited here (Trino 455 is about 29 releases behind 484; run the 483 profile in production).

---

## 4. StarRocks connector: starrocks-flight (patches against StarRocks 4.1.4)

The repo only carries patches; the dependencies below are those of the StarRocks tree used (tag 4.1.4 +2 in `~/Projects/starrocks-src`) and ship with the connector.

FE (Java 17) from `fe/pom.xml`:

| Package | Version | Severity | CVE / GHSA | Fixed in | Reachable? | Action |
|---|---|---|---|---|---|---|
| org.bouncycastle:bcprov-jdk18on | 1.84 | CRITICAL 1, HIGH 1 | CVE-2026-8763, CVE-2026-13506 | 1.85 | FE uses BC for TLS/crypto helpers; reachable if cert-name constraints are validated with BC. | Bump to 1.85 (StarRocks upstream). |
| com.fasterxml.jackson (databind) | 2.21.4 | HIGH 3, MOD 6 | CVE-2026-91777, CVE-2026-68497, CVE-2026-91776 | 2.21.7 | FE REST/JSON; moderate. | Bump to 2.21.7 upstream. |
| org.apache.kafka:kafka-clients | 3.9.1 | HIGH 1, MOD 1 | CVE-2026-35554 | 3.9.2 | Only for routine-load from Kafka; not our Flight path. | Bump to 3.9.2. |
| org.apache.logging.log4j | 2.19.0 | MOD 3-4 | | 2.25.4 | Low. | Bump. |
| org.eclipse.jetty | 9.4.58.v20250814 | MOD 1 | | 12.0.35 (9.4 EOL) | FE HTTP port. | Upstream decision. |
| io.netty (BOM) | 4.1.136.Final | CRITICAL 1 (netty-handler CVE-2026-75595, SNI, not reachable), MOD 1 (codec-http) | | 4.1.137 | Only a few codecs used; FE Flight client uses gRPC 1.63.0. | Bump to 4.1.137. |
| Arrow Java 18.0.0 (+ gRPC 1.63.0, protobuf 3.25.5, thrift 0.24.0, parquet 1.16.0, avro 1.12.0, hadoop 3.4.3, iceberg 1.10.0) | | none in OSV for Maven artifacts (note: grpc-netty-shaded 1.63.0 itself carries CVE-2025-55163, HIGH, fixed in 1.75.0; the FE uses the unshaded grpc-netty from Arrow so it depends on the Netty 4.1.136 above, which is fixed) | | | | Keep; Arrow 19 / gRPC 1.7x+ only if we port. |

BE (C++ thirdparty, `thirdparty/vars.sh`). NOT OSV-verified (from upstream CVE knowledge); please verify with a scanner on the image:

| Library | Version in 4.1.4 | Notable issues (upstream) | Fixed in | Reachable? |
|---|---|---|---|---|
| OpenSSL | 1.1.1m | EOL since 2023-09; CVE-2022-0778 (BN_mod_sqrt loop, fixed 1.1.1n), CVE-2022-2097, CVE-2023-0286 (X.400 type confusion, HIGH), CVE-2023-0215/0464/0465/0466, CVE-2023-2650, later 1.1.1 fixes (1.1.1w) | 1.1.1w (last public); real fix = OpenSSL 3.x | Yes: gRPC/Arrow Flight TLS (`grpc+tls`) and S3/HTTPS use it. |
| gRPC (C++) | 1.43.0 (Dec 2021) | HTTP/2 hpack/frame DoS fixes in 2023 (e.g. CVE-2023-32731, CVE-2023-33953, CVE-2023-4785), MadeYouReset-class fixes in later releases | 1.53 to 1.7x | Yes: Arrow Flight client in the BE (our FlightDataSource) talks gRPC to our server; a hostile server can send crafted frames. |
| protobuf (C++) | 3.14.0 | CVE-2021-22570 (nullptr), CVE-2022-1941 (parser DoS on crafted messages), CVE-2022-3171 | 3.15+ / 3.20.2+ / 3.21.x | Yes for Arrow Flight/brpc message parsing of data from the server. |
| zlib | 1.2.11 | CVE-2018-25032, CVE-2022-37434 (heap overflow in inflateGetHeader, critical 9.8) | 1.2.13 (1.3.1 current) | Used by Arrow compression and curl; gzip streams only if enabled. |
| curl | 8.4.0 | CVE-2023-46218, CVE-2023-46219, CVE-2024-2398, CVE-2024-6197, CVE-2024-7264 and later | 8.9+ (current 8.1x) | S3/HTTP access; not Flight path. |
| libxml2 | 2.11.7 | CVE-2024-56171, CVE-2025-24928, CVE-2025-27113 and 2025 series | 2.12.10 / 2.13.x | Azure/S3 XML responses; not Flight path. |
| krb5 | 1.19.4 | CVE-2023-39975, CVE-2024-37370/37371 (GSS), fixed in 1.21.3 | 1.21.3 | HDFS Kerberos only. |
| c-ares | 1.19.1 | CVE-2024-25629 | 1.27.0 | DNS in gRPC; low. |
| boost 1.80, brpc 1.9.0, rocksdb 6.22.1, abseil 20220623, cyrus-sasl 2.1.28, Arrow C++ 19.0.1 (Arrow Flight), thrift 0.24.0, zstd 1.5.7, lz4 1.10.0, ICU 76.1, aws-sdk-cpp 1.11.267 | | Old (2022-2023) but no high-impact remotely reachable CVE I am aware of for our path; Arrow C++ 19.0.1 and thrift 0.24.0 are current. | | |

Recommended for StarRocks: these versions are upstream's; for our connector the practical mitigations are (a) use Flight over a trusted network or mTLS terminated by a proxy, (b) raise OpenSSL/gRPC upstream in the next StarRocks rebase (the 4.1.x thirdparty pins are old; StarRocks main has newer ones).

---

## 5. hadoopfs extension: C0fuge/plover (build scripts: `sources.env`, `scripts/build-deps.sh`)

Static builds into the extension (not OSV-checkable; known upstream status):

| Component | Version | Notes | Action |
|---|---|---|---|
| libhdfs3 (ClickHouse fork) | commit 94a9ead (+ our patches) | Native HDFS RPC client (protobuf over TCP, SASL/Kerberos via gsasl). No CVE list of its own; it parses responses from the NameNode/DataNode, so only matters against hostile HDFS. | Keep; track ClickHouse/libhdfs3 master. |
| protobuf C++ | 3.21.12 (last 3.21.x, Jan 2023) | CVE-2022-1941 and 2022-3171 fixed before 3.21.12; the 2024-2025 protobuf CVEs (CVE-2024-7254, CVE-2025-4565) are Java/Python. Hdfs3 generated code is proto2 from 2.5 - needs the 3.x line, so do not jump to 4.x/5.x without regenerating. | OK; consider 3.25.x/4.x only with a regen test. |
| libgsasl | 1.10.0 (2018) | Old; 2.2.x is current (2.2.2). Known advisories on gsasl are in SCRAM/GSSAPI server-side or in `gsasl_*_decode` handling (CVE-2022-2469 is for libgsasl before 2.0.0 - verify exact applicability). We use only the GSSAPI client mechanism against Hadoop. | Verify CVE-2022-2469 applicability; test with libgsasl 2.2.x. |
| Runtime OS libs (dynamic): libxml2, MIT krb5, libuuid, OpenSSL | from the build image/host (dnf `libxml2-devel krb5-devel libuuid-devel openssl-devel`) | Patch level equals the host OS; the extension does not bundle them. | Ship on a patched base image (e.g. Rocky/Alma 9 current). |
| DuckDB | v1.4.5 | No OSV (C++ core); the DuckDB JDBC 1.4.5.0 has no advisories. DuckDB 2.0.0 is released (JDBC 2.0.0.0). | Plan an upgrade to 2.0.x. |
| vincent-chang/duckdb-hdfs | commit 51b7220 (v1.4.1) | Upstream base is stale; our patches layered on top. | Keep. |

---

## 6. Clients repo: flight-clients-compat

- No lockfiles. `.github/workflows/e2e.yml` runs `pip install -q adbc-driver-flightsql pyarrow [pandas polars duckdb]` unpinned, so CI always uses the latest releases (currently adbc-driver-flightsql 1.12.0, pyarrow 26.0.0, pandas 3.1.0, polars 2.0.0, duckdb 2.0.0). No known-vulnerability exposure from age, but builds are not reproducible and a compromised release would be picked up automatically.
- `actions/checkout@v4`, `actions/upload-artifact@v4` pinned by tag, not SHA.
- Action: pin versions (or a constraints file) and optionally actions by SHA.

---

## Top risks (ordered)

1. Hazelcast 5.6.0 (CRITICAL CVE-2026-107726 + HIGH CVE-2026-107725) in the server: embedded member port is the attack surface. Upgrade to 5.7.0 and restrict/secure the cluster port. Easy fix, reachable.
2. Netty: 4.2.9 (server) / 4.1.130 (client-core, spark) / 4.1.100-114 (Trino plugin, libs set) - tens of advisories, the HTTP/2 ones (CONTINUATION flood CVE-2026-33871, reset attack CVE-2026-50560, stream-limit CVE-2026-47244, CVE-2026-56819) are on the Flight gRPC path. Fix: 4.2.17+ / 4.1.137, one `netty-bom`, and replace `netty-all` by only the needed modules.
3. Jackson 2.15.3 pinned everywhere in the server (HIGH 7 advisories incl. polymorphic validation bypass and DoS) and 2.18.0 in the Trino tar: move to 2.18.11 or 2.22.3 and align module versions.
4. **Done 2026-10-25** (server: Parquet 1.18.1 → aircompressor 2.0.3, snappy-java 1.1.10.8). Parquet read path: aircompressor 0.21 (CVE-2024-36114, CVE-2025-67721), snappy-java 1.1.8.3 (CVE-2023-43642/34455) - reachable when reading untrusted Parquet. Bump to aircompressor 2.0.3 / snappy 1.1.10.12, or Parquet to 1.15+.
5. **Done 2026-10-25** (Hive plugin client on libthrift 0.24.0; test metastore stays 0.9.3 in its own class loader). Hive plugin libthrift 0.9.3 (CVE-2026-43869 TLS host mismatch, CVE-2020-13949, CVE-2019-0205): override to 0.24.0 (needs a test) - HMS is usually trusted, so medium.
6. Libs set on the stand is stale vs the poms (Arrow 18, gRPC 1.65, Netty 4.1.114, BC 1.79 CRITICAL, ZooKeeper 3.6.3 CRITICAL, Netty 3.10.6, Jetty 9.4.51, lz4-java 1.8.0, log4j 2.20, DuckDB 1.4.1): rebuild it from the current poms; prune unused Hadoop/Spark/ZooKeeper/Jetty/Netty-3 jars.
7. StarRocks BE native stack: OpenSSL 1.1.1m (EOL), gRPC 1.43, protobuf 3.14, zlib 1.2.11 (unverified by OSV, from upstream CVE history): mitigate by network isolation/mTLS, fix on StarRocks rebase.
8. Trino plugin tar: Netty 4.1.100 modules and gRPC 1.65.0 in the shipped tar differ from pom pins (4.1.114 / 1.65.1) - rebuild; plus protobuf 3.25.4 (CVE-2024-7254).

## Seriously outdated core libraries (even without CVEs)

| Library | Used | Latest stable (Central, 2026-10-23) |
|---|---|---|
| Arrow Java | 19.0.0 (server), 18.1.0 (Trino), 18.0.0 (libs, StarRocks FE) | 19.0.0 |
| gRPC Java | 1.84.2 (server), 1.79.0 (client-core), 1.65.x (Trino, libs), 1.63.0 (StarRocks FE) | 1.85.1 |
| Netty | 4.2.9 / 4.1.130 / 4.1.114 / 4.1.100 | 4.2.19 (4.1.137+ for the 4.1 line) |
| Jackson | 2.15.3 (server), 2.18.0 (Trino tar) | 2.22.4 |
| AWS SDK v2 | 2.34.1 | 2.57.3 |
| Hive metastore / libthrift | 3.1.3 / 0.9.3 | 4.2.1 / 0.25.0 |
| Parquet | 1.13.1 | 1.18.1 |
| Hadoop | 3.5.0 (pom, current); 3.3.6/3.3.4 in libs set | 3.5.0 |
| Spark | 3.5.9 (2.12, latest 3.5 line) | 4.2.0 (Scala 2.13) |
| DuckDB JDBC | 1.4.5.0 (pom); 1.4.1.0 (libs) | 2.0.0.0 |
| Hazelcast | 5.6.0 | 5.7.0 |
| Guava | 33.4.8 (32.1.3 in client-core/spark; 33.4.0 Trino) | 33.7.2 |
| protobuf-java | 4.34.1 (server); 3.25.4 (Trino, libs util) | 4.36.2 |
| Log4j / SLF4J | log4j 2.20.0 (libs), 2.19.0 (StarRocks FE), slf4j-simple 2.0.20 | log4j 2.26.1, slf4j 2.0.20 (slf4j-api 2.0.17 is the one in poms) |
| BouncyCastle | 1.85 (pom), 1.79 (libs), 1.84 (StarRocks FE) | 1.86 |
| Trino SPI | 455 / 477 / 483 | 484 |
| Iceberg | 1.12.0 | 1.12.0 (current) |
| Jetty | 9.4.51 (libs) | 12.1.14 (9.4 is EOL) |
| ZooKeeper | 3.6.3 (libs) | 3.9.6 |
| OpenSSL / gRPC C++ / protobuf C++ (StarRocks BE) | 1.1.1m / 1.43.0 / 3.14.0 | 3.x / 1.7x / 5.x-6.x |
