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
 * (default) changes nothing; {@code speed} trades memory and CPU for speed by supplying defaults
 * for the settings measured as faster. A setting given as a system property or in the
 * environment always wins over the profile; the profile wins over {@code arrowflight.properties}.
 *
 * @param name     profile name
 * @param defaults values the profile supplies, by property name
 * @param explicit settings the profile would supply but the operator set, with their values
 */
public record ResourceProfile(String name, Map<String, String> defaults, Map<String, String> explicit) {

    /** Default profile: today's behaviour. */
    public static final String BALANCED = "balanced";

    /** Profile that trades memory and CPU for speed. */
    public static final String SPEED = "speed";

    /** Backpressure threshold per stream of {@code speed}: 16 MiB, needed with several endpoints per node. */
    static final int SPEED_BACKPRESSURE_BYTES = 16 << 20;

    /** Endpoints per node and partial query of {@code speed}. */
    static final int SPEED_ENDPOINTS_PER_SERVER = 4;

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
            return new ResourceProfile(BALANCED, Map.of(), Map.of());
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
            long mib = speedMemoryBytes(host, balanced) >> 20;
            b.offer(mib + "MiB", "duckDbMemoryLimit", "arrowflight.duckdb.memoryLimit", null);
        }
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
