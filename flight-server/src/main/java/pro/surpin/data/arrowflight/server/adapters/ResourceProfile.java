package pro.surpin.data.arrowflight.server.adapters;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import pro.surpin.data.arrowflight.server.plugin.ConfigSource;

/**
 * Resource profile of {@code resourceProfile} ({@code FLIGHT_RESOURCE_PROFILE}): {@code balanced}
 * (default) changes nothing except {@code duckDbExternalFileCache=true} on an {@code hdfs://} data
 * dir; {@code speed} trades memory and CPU for speed by supplying defaults
 * for the settings measured as faster. A setting given as a system property or in the
 * environment always wins over the profile; the profile wins over {@code arrowflight.properties}.
 *
 * @param name     profile name
 * @param defaults values the profile supplies, by property name
 * @param explicit settings the profile would supply but the operator set, with their values
 */
public record ResourceProfile(String name, Map<String, String> defaults, Map<String, String> explicit) {

    /** Default profile: today's behaviour (external file cache on for an HDFS data dir). */
    public static final String BALANCED = "balanced";

    /** Profile that trades memory and CPU for speed. */
    public static final String SPEED = "speed";

    /** Backpressure threshold per stream of {@code speed}: 16 MiB, needed with several endpoints per node. */
    static final int SPEED_BACKPRESSURE_BYTES = 16 << 20;

    /** Endpoints per node and partial query of {@code speed}. */
    static final int SPEED_ENDPOINTS_PER_SERVER = 4;

    /** Least heap speed reserves for the JVM when it sizes DuckDB's memory limit (Q-40a). */
    static final long SPEED_MIN_HEAP_RESERVE = 1L << 30;

    /** Fixed part of the direct memory speed reserves for the JVM (Q-40a). */
    static final long SPEED_DIRECT_RESERVE_BASE = 1L << 30;

    /** Share of the direct memory limit speed reserves on top of the fixed part (Q-40a). */
    static final int SPEED_DIRECT_RESERVE_PERCENT = 25;

    /**
     * Creates a profile with unmodifiable copies of its maps, in insertion order.
     *
     * @param name     profile name
     * @param defaults values the profile supplies, by property name
     * @param explicit settings the operator set instead, with their values
     */
    public ResourceProfile {
        defaults = Collections.unmodifiableMap(new LinkedHashMap<>(defaults));
        explicit = Collections.unmodifiableMap(new LinkedHashMap<>(explicit));
    }

    /**
     * Resolves the profile from a source without profile defaults.
     *
     * @param raw  configuration source
     * @param host host limits, read only for {@code speed}
     * @return profile
     * @throws IllegalArgumentException for an unknown profile name
     */
    public static ResourceProfile resolve(ConfigSource raw, Supplier<HostLimits> host) {
        String name = raw.lookup(List.of("resourceProfile", "arrowflight.resourceProfile"),
                List.of("FLIGHT_RESOURCE_PROFILE")).orElse(BALANCED).toLowerCase(Locale.ROOT);
        if (BALANCED.equals(name)) {
            if (!hdfsDataDir(raw)) {
                return new ResourceProfile(BALANCED, Map.of(), Map.of());
            }
            Builder b = new Builder(raw);
            offerHdfsFileCache(b);
            return new ResourceProfile(BALANCED, b.defaults, b.explicit);
        }
        if (!SPEED.equals(name)) {
            throw new IllegalArgumentException("resourceProfile must be balanced or speed: " + name);
        }
        return speed(raw, host.get());
    }

    /**
     * Builds the {@code speed} profile for a host.
     *
     * @param raw  configuration source without profile defaults
     * @param host host limits
     * @return profile
     */
    static ResourceProfile speed(ConfigSource raw, HostLimits host) {
        Builder b = new Builder(raw);
        boolean shared = !"false".equalsIgnoreCase(b.offer("true", "duckDbSharedDatabase",
                "arrowflight.duckdb.sharedDatabase", "DUCKDB_SHARED_DATABASE"));
        boolean capSet = b.offer("0", "duckDbSharedMaxThreads", "arrowflight.duckdb.sharedMaxThreads",
                "DUCKDB_SHARED_MAX_THREADS") != null;
        DuckDbSizing balanced = ConfigAdapter.duckDbSizing(raw, host);
        if (!capSet) {
            // An explicit thread cap of the shared DuckDB is the operator's thread choice: keep it.
            b.offer(Integer.toString(speedThreads(host, balanced)), "duckDbThreads", "arrowflight.duckdb.threads",
                    null);
        }
        if (shared && host.memoryBytes() > 0) {
            // Node-wide only: as a per-connection limit this would let every connection take it.
            // Sized with the heap and direct memory the JVM really uses, not their limits (Q-40a): with
            // -Xmx3g / direct 2g in 8 GB the limit held no S3 file cache, warm TPC-H SF10 75.8 vs 15.9 s.
            HostLimits reserved = speedReservation(host);
            long mib = speedMemoryBytes(reserved, ConfigAdapter.duckDbSizing(raw, reserved)) >> 20;
            b.offer(mib + "MiB", "duckDbMemoryLimit", "arrowflight.duckdb.memoryLimit", null);
        }
        if (shared || hdfsDataDir(raw)) {
            // One buffer pool: the cache stays within the node-wide memory limit. With a DuckDB per
            // connection only on HDFS (offerHdfsFileCache): there every connection keeps its own,
            // each within its connection's memory limit. S3 TPC-H SF10 262.5 -> 42.7 s, c32 p95
            // 18.0 -> 0.58 s, peak RSS x1.6-2.1 (egret fc-s3-f06b5834e76e, run 41437523179).
            offerHdfsFileCache(b);
        }
        // Parallel scan threads hand their batches on as they finish instead of in file order (rows of a
        // query without ORDER BY come in any order, as upstream's default): one full lineitem read on S3
        // 910 -> 1118 MB/s, same answers, same peak memory (item 27, scan-perf runs 41585815577, 41587292385).
        b.offer("false", "duckDbPreserveInsertionOrder", "arrowflight.duckdb.preserveInsertionOrder",
                "DUCKDB_PRESERVE_INSERTION_ORDER");
        b.offer("true", "flightZeroCopyWrites", "arrowflight.flight.zeroCopyWrites", "FLIGHT_ZERO_COPY_WRITES");
        b.offer(Integer.toString(SPEED_ENDPOINTS_PER_SERVER), "endpointsPerServer",
                "arrowflight.cluster.endpointsPerServer", "FLIGHT_ENDPOINTS_PER_SERVER");
        b.offer(Integer.toString(SPEED_BACKPRESSURE_BYTES), "flightBackpressureThresholdBytes",
                "arrowflight.flight.backpressureThresholdBytes", null);
        // No S3 read caches: one pass reads each file once, so a cache only adds the loopback hop and heap
        // (TPC-H SF10 on S3 1.33x slower with them, as fast as balanced without; runs 40939989610, 40945132304).
        return new ResourceProfile(SPEED, b.defaults, b.explicit);
    }

    /**
     * Tells whether the data dir is on HDFS ({@code hdfs://}), where every profile keeps DuckDB's
     * external file cache by default.
     *
     * @param raw configuration source without profile defaults
     * @return true for an {@code hdfs://} data dir
     */
    static boolean hdfsDataDir(ConfigSource raw) {
        return ConfigAdapter.isHdfs(raw.lookup(List.of("dataDir"), List.of()).orElse(null));
    }

    /**
     * Supplies {@code duckDbExternalFileCache=true} for an HDFS data dir (Q11, item 39): without it
     * every per-thread DuckDB's libhdfs3 asked the DataNode for the short-circuit fds again,
     * ~5-6 times per point query; with it 0-0.2 and +10..15 % q/s (HDFS point cell, run
     * 42689843600). The hadoopfs build 51b7220-afc4 reports real modification times, so a
     * rewritten file is read again instead of served from the cache. With a DuckDB per connection
     * each connection's cache stays within that connection's memory limit.
     *
     * @param b profile builder
     */
    private static void offerHdfsFileCache(Builder b) {
        b.offer("true", "duckDbExternalFileCache", "arrowflight.duckdb.externalFileCache",
                "DUCKDB_EXTERNAL_FILE_CACHE");
    }

    /**
     * Sizes the DuckDB threads of {@code speed}: all cores, but never fewer than balanced gives
     * one query (its floor of 4). With 2 threads on 2 cores a point lookup on S3 waited on more of
     * its round trips one after another: c1 39 vs 49 q/s, c1 p95 53 vs 33 ms; with 4 threads c1
     * equals balanced and c8 p95 drops from 106 to 83 ms, c32 unchanged (run 41083790232).
     *
     * @param host     host limits
     * @param balanced sizing of the DuckDB-per-connection mode without a profile
     * @return threads
     */
    static int speedThreads(HostLimits host, DuckDbSizing balanced) {
        return Math.max(Math.max(1, host.cores()), balanced.threads());
    }

    /**
     * Returns the host limits with the JVM's reservation of {@code speed} in place of its heap and
     * direct memory limits: heap = min(-Xmx, max(1 GiB, heap in use x 2)), direct = min(direct
     * limit, 1 GiB + 25 % of it). The memory stays the container's (cgroup) or machine's.
     *
     * @param host host limits
     * @return host limits with the reserved heap and direct memory
     */
    static HostLimits speedReservation(HostLimits host) {
        long heap = Math.min(host.maxHeapBytes(), Math.max(SPEED_MIN_HEAP_RESERVE, host.usedHeapBytes() * 2));
        long direct = Math.min(host.maxDirectBytes(),
                SPEED_DIRECT_RESERVE_BASE + host.maxDirectBytes() / 100 * SPEED_DIRECT_RESERVE_PERCENT);
        return new HostLimits(host.cores(), host.memoryBytes(), heap, direct, host.usedHeapBytes());
    }

    /**
     * Sizes the node-wide memory limit of the shared DuckDB under {@code speed}: what the
     * DuckDB-per-connection mode gives the connections it expects at once, at least 1 GiB and at
     * most DuckDB's own default share of 80 % of the memory. The earlier rule (75 % of the memory
     * minus heap and direct) was always smaller and left 1 GiB for the whole node with a large
     * heap, where 32 concurrent S3 queries ran out of it (item 23b).
     *
     * @param host     host limits with known memory
     * @param balanced sizing of the DuckDB-per-connection mode without a profile
     * @return memory limit in bytes
     */
    static long speedMemoryBytes(HostLimits host, DuckDbSizing balanced) {
        Long perConnection = balanced.memoryLimit() == null ? null : DuckDbSizing.parseBytes(balanced.memoryLimit());
        long bytes = perConnection == null ? balanced.budgetBytes() : perConnection * balanced.connections();
        bytes = Math.min(bytes, host.memoryBytes() / 100 * DuckDbSizing.MAX_CONNECTION_PERCENT);
        return Math.max(DuckDbSizing.MIN_CONNECTION_BYTES, bytes);
    }

    /**
     * Returns a source that reads this profile's defaults.
     *
     * @param raw configuration source without profile defaults
     * @return source
     */
    public ConfigSource applyTo(ConfigSource raw) {
        return defaults.isEmpty() ? raw : raw.withProfileDefaults(defaults);
    }

    /**
     * Describes the profile and the effective values of its settings for the startup log.
     *
     * @return one line of key=value pairs
     */
    public String describe() {
        StringBuilder line = new StringBuilder("profile=").append(name);
        defaults.forEach((key, value) -> line.append(' ').append(key).append('=').append(value));
        explicit.forEach((key, value) -> line.append(' ').append(key).append('=').append(value)
                .append("(explicit)"));
        return line.toString();
    }

    /** Collects the values a profile supplies, skipping those the operator set. */
    private static final class Builder {

        private final ConfigSource raw;
        private final Map<String, String> defaults = new LinkedHashMap<>();
        private final Map<String, String> explicit = new LinkedHashMap<>();

        /**
         * Creates a builder.
         *
         * @param raw configuration source without profile defaults
         */
        Builder(ConfigSource raw) {
            this.raw = raw;
        }

        /**
         * Supplies a value unless the operator set the setting as a system property or in the environment.
         *
         * @param value   profile value
         * @param key     primary property name, the name the profile value is stored under
         * @param alias   second property name
         * @param envName environment variable, or null
         * @return the operator's value, or null when the profile value is used
         */
        String offer(String value, String key, String alias, String envName) {
            Optional<String> set = raw.firstSystemProperty(List.of(key, alias))
                    .or(() -> envName == null ? Optional.empty() : raw.firstEnvironment(List.of(envName)));
            if (set.isPresent()) {
                explicit.put(key, set.get());
                return set.get();
            }
            defaults.put(key, value);
            return null;
        }
    }
}
