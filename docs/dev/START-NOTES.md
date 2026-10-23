# Arrow Flight server @VERSION@ (@SHA@)

Development build of the maintainer's fork, commit @COMMIT@. Built by `dev-ci.yml` (job `package`) after the full
test suite passed on the same commit. Linux x86-64 / ARM64 only (`-Plinux-only`).

## Assets

- `arrow-flight-server-@VERSION@-@SHA@-linux.jar`: the shaded server jar.
- `flight-plugins-@VERSION@-@SHA@.tar.zst`: `plugins/{s3,lakefs,iceberg,hive}/` (jars + a `SHA256SUMS` per plugin).
- `SHA256SUMS`: checksums of the files above.

## Start

Java 21 (Netty 4.2 + Arrow fail on JDK 24+).

```bash
sha256sum -c SHA256SUMS
mkdir -p /opt/flight && tar --zstd -xf flight-plugins-@VERSION@-@SHA@.tar.zst -C /opt/flight
cp arrow-flight-server-@VERSION@-@SHA@-linux.jar /opt/flight/
# plugins are found as ../plugins next to the jar, or with FLIGHT_PLUGINS_DIR / -Darrowflight.plugins.dir
FLIGHT_TLS_CERT=... FLIGHT_TLS_KEY=... FLIGHT_BEARER_TOKEN=... \
  java -jar /opt/flight/arrow-flight-server-@VERSION@-@SHA@-linux.jar \
    --data-dir <dir or s3://bucket/prefix> --localhost <this host> --port 32010 --hosts <host1,host2,...>
```

- DuckDB extensions (httpfs, iceberg, avro, hadoopfs) of DuckDB 1.4.5 must be installed as on the stand; the server
  checks the extension versions at startup.
- A cluster needs the Hazelcast member port reachable between the nodes only (keep it private).
- Without TLS settings the server refuses to start unless `FLIGHT_ALLOW_INSECURE=true` (isolated test setups only).
- Changes since the previous build: `CHANGELOG.md` at @COMMIT@.
