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

- DuckDB extensions (httpfs, iceberg, avro, hadoopfs) of DuckDB 1.4.5 must be installed as on the stand; the server
  checks the extension versions at startup.
- DuckDB runs 4 threads per connection by default (`duckDbThreads`, was 2); set `-DduckDbThreads=1` /
  `FLIGHT_DUCKDB_THREADS=1` where compute workers share the node.
- A cluster needs the Hazelcast member port reachable between the nodes only (keep it private).
- Without TLS settings the server refuses to start unless `FLIGHT_ALLOW_INSECURE=true` (isolated test setups only).
- Changes since the previous build: `CHANGELOG.md` at @COMMIT@.
