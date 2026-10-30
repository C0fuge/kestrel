package pro.surpin.data.arrowflight.server.adapters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;
import pro.surpin.data.arrowflight.server.plugin.PropertiesPluginConfig;

/** Tests the resource profiles of {@code resourceProfile} (item 23). */
@Tag("unit")
class ResourceProfileTest {

    private static final long GIB = 1L << 30;

    /** 4 cores, 6 GB, heap 1.5 GB, direct 1 GB. */
    private static final HostLimits SMALL = new HostLimits(4, 6 * GIB, 3 * GIB / 2, GIB);

    /** 16 cores, 32 GB, heap 8 GB, direct 4 GB. */
    private static final HostLimits MEDIUM = new HostLimits(16, 32 * GIB, 8 * GIB, 4 * GIB);

    /** 64 cores, 256 GB, heap 32 GB, direct 16 GB. */
    private static final HostLimits LARGE = new HostLimits(64, 256 * GIB, 32 * GIB, 16 * GIB);

    /**
     * Builds a source without a profile.
     *
     * @param system system properties
     * @param env    environment
     * @param file   properties file entries
     * @return source
     */
    private static ConfigSource raw(Map<String, String> system, Map<String, String> env, Map<String, String> file) {
        Properties properties = new Properties();
        properties.putAll(file);
        return new ConfigSource(system::get, properties, env, null);
    }

    /**
     * Resolves a source with its profile for a host.
     *
     * @param raw  source without a profile
     * @param host host limits
     * @return source with the profile defaults
     */
    private static ConfigSource effective(ConfigSource raw, HostLimits host) {
        return ResourceProfile.resolve(raw, () -> host).applyTo(raw);
    }

    /** Without the setting nothing changes and the host is not even read. */
    @Test
    void balancedIsTheDefaultAndChangesNothing() {
        ConfigSource raw = raw(Map.of(), Map.of(), Map.of("duckDbSharedDatabase", "false"));
        ResourceProfile profile = ResourceProfile.resolve(raw, () -> {
            throw new AssertionError("host read for balanced");
        });
        assertEquals(ResourceProfile.BALANCED, profile.name());
        assertTrue(profile.defaults().isEmpty());
        assertSame(raw, profile.applyTo(raw));
        assertEquals("profile=balanced", profile.describe());
        AppConfig config = ConfigAdapter.getConfig(effective(raw, MEDIUM), MEDIUM);
        assertEquals(AppConfig.DEFAULT_ENDPOINTS_PER_SERVER, config.endpointsPerServer());
        assertNull(ConfigAdapter.duckDbShared(effective(raw, MEDIUM), MEDIUM));
        assertFalse(ConfigAdapter.flightZeroCopyWrites(effective(raw, MEDIUM)));
    }

    /** A typo is refused instead of running balanced silently; the name ignores case. */
    @Test
    void unknownProfileIsRefusedAndCaseIsIgnored() {
        ConfigSource typo = raw(Map.of("resourceProfile", "sped"), Map.of(), Map.of());
        assertThrows(IllegalArgumentException.class, () -> ResourceProfile.resolve(typo, () -> MEDIUM));
        ConfigSource upper = raw(Map.of(), Map.of("FLIGHT_RESOURCE_PROFILE", "SPEED"), Map.of());
        assertEquals(ResourceProfile.SPEED, ResourceProfile.resolve(upper, () -> MEDIUM).name());
    }

    /** speed on 4 / 16 / 64 cores: shared DuckDB with all cores and the memory balanced gives its connections. */
    @Test
    void speedSizesTheSharedDuckDbByHost() {
        assertShared(SMALL, 4, "2662MiB");
        assertShared(MEDIUM, 16, "15564MiB");
        assertShared(LARGE, 64, "173668MiB");
    }

    /**
     * Checks the shared DuckDB and the Flight settings of speed on a host, and that the read caches stay off.
     *
     * @param host     host limits
     * @param threads  expected node threads
     * @param memory   expected node memory limit
     */
    private static void assertShared(HostLimits host, int threads, String memory) {
        // The bundled file's defaults lose to the profile.
        ConfigSource source = effective(raw(Map.of("resourceProfile", "speed"), Map.of(),
                Map.of("duckDbSharedDatabase", "false", "flightBackpressureThresholdBytes", "268435456")), host);
        DuckDbSizing.Shared shared = ConfigAdapter.duckDbShared(source, host);
        assertEquals(threads, shared.threads(), "threads " + host);
        assertEquals(memory, shared.memoryLimit(), "memory " + host);
        assertTrue(ConfigAdapter.flightZeroCopyWrites(source));
        AppConfig config = ConfigAdapter.getConfig(source, host);
        assertEquals(4, config.endpointsPerServer());
        assertEquals(16 << 20, config.flightBackpressureThresholdBytes());
        assertEquals(threads, config.duckDbThreads());
        for (String ns : List.of("s3", "lakefs")) {
            assertEquals(0, new PropertiesPluginConfig(ns, source, List.of()).getInt("cacheMegabytes", 0),
                    ns + " cache " + host);
        }
    }

    /** Settings given as system properties or in the environment win over the profile. */
    @Test
    void explicitSettingsWin() {
        ConfigSource raw = raw(
                Map.of("resourceProfile", "speed", "duckDbThreads", "6", "arrowflight.cluster.endpointsPerServer", "2"),
                Map.of("FLIGHT_ZERO_COPY_WRITES", "false", "FLIGHT_S3_CACHE_MEGABYTES", "100"), Map.of());
        ConfigSource source = effective(raw, MEDIUM);
        assertEquals(6, ConfigAdapter.duckDbShared(source, MEDIUM).threads());
        assertFalse(ConfigAdapter.flightZeroCopyWrites(source));
        assertEquals(2, ConfigAdapter.getConfig(source, MEDIUM).endpointsPerServer());
        assertEquals(100, new PropertiesPluginConfig("s3", source, List.of()).getInt("cacheMegabytes", 0));
        assertEquals(0, new PropertiesPluginConfig("lakefs", source, List.of()).getInt("cacheMegabytes", 0));
        String line = ResourceProfile.resolve(raw, () -> MEDIUM).describe();
        assertTrue(line.startsWith("profile=speed duckDbSharedDatabase=true"), line);
        assertTrue(line.contains("duckDbThreads=6(explicit)"), line);
        assertTrue(line.contains("flightZeroCopyWrites=false(explicit)"), line);
    }

    /** An explicit thread cap of the shared DuckDB keeps applying: speed then sets no thread count. */
    @Test
    void explicitSharedThreadCapKeepsItsThreads() {
        ConfigSource source = effective(raw(Map.of("resourceProfile", "speed", "duckDbSharedMaxThreads", "12"),
                Map.of(), Map.of()), LARGE);
        assertEquals(12, ConfigAdapter.duckDbShared(source, LARGE).threads());
    }

    /** With the shared DuckDB turned off the node-wide memory limit is not used per connection. */
    @Test
    void sharedOffKeepsThePerConnectionMemoryLimit() {
        ConfigSource raw = raw(Map.of("resourceProfile", "speed"), Map.of("DUCKDB_SHARED_DATABASE", "false"),
                Map.of());
        ConfigSource source = effective(raw, MEDIUM);
        assertNull(ConfigAdapter.duckDbShared(source, MEDIUM));
        DuckDbSizing perConnection = ConfigAdapter.duckDbSizing(source, MEDIUM);
        DuckDbSizing balanced = ConfigAdapter.duckDbSizing(raw(Map.of(), Map.of(), Map.of()), MEDIUM);
        assertEquals(balanced.memoryLimit(), perConnection.memoryLimit());
        assertEquals(16, perConnection.threads());
    }

    /**
     * The egret profile of item 23b (2 cores, 6 GB, -Xmx3g, direct 2g, data on S3): the shared DuckDB gets what DuckDB-per-connection gives its two expected
     * connections (2 x 1 GiB) instead of 1 GiB for the whole node, and a query that still runs
     * out of memory runs again on a private DuckDB sized as balanced.
     */
    @Test
    void speedNeverGivesTheSharedDuckDbLessThanBalancedGivesItsConnections() {
        HostLimits egret = new HostLimits(2, 6 * GIB, 3 * GIB, 2 * GIB);
        Map<String, String> s3 = Map.of("dataDir", "s3://bucket/tpch");
        ConfigSource raw = raw(Map.of("resourceProfile", "speed"), Map.of(), s3);
        DuckDbSizing balanced = ConfigAdapter.duckDbSizing(raw(Map.of(), Map.of(), s3), egret);
        assertEquals("1024MiB", balanced.memoryLimit());
        assertEquals(2, balanced.connections());
        DuckDbSizing.Shared shared = ConfigAdapter.duckDbShared(effective(raw, egret), egret)
                .withFallback(ConfigAdapter.duckDbSizing(raw, egret));
        assertEquals("2048MiB", shared.memoryLimit());
        assertEquals(4, shared.threads());
        assertEquals(balanced.threads(), shared.fallbackThreads());
        assertEquals("1024MiB", shared.fallbackMemoryLimit());
        for (HostLimits host : List.of(egret, SMALL, MEDIUM, LARGE)) {
            DuckDbSizing perConnection = ConfigAdapter.duckDbSizing(raw(Map.of(), Map.of(), Map.of()), host);
            long node = DuckDbSizing.parseBytes(ConfigAdapter.duckDbShared(
                    effective(raw(Map.of("resourceProfile", "speed"), Map.of(), Map.of()), host), host).memoryLimit());
            assertEquals(Math.min(DuckDbSizing.parseBytes(perConnection.memoryLimit()) * perConnection.connections(),
                    host.memoryBytes() / 100 * DuckDbSizing.MAX_CONNECTION_PERCENT) >> 20, node >> 20, host.toString());
        }
    }

    /**
     * On fewer than 4 cores speed keeps balanced's floor of 4 threads instead of one per core: with
     * 2 threads point lookups on S3 had 1.5x balanced's c1 / c8 p95 (item 23b follow-up).
     */
    @Test
    void speedNeverGivesFewerThreadsThanBalanced() {
        for (int cores : List.of(1, 2, 3, 4, 6, 16, 64)) {
            HostLimits host = new HostLimits(cores, 6 * GIB, 3 * GIB, 2 * GIB);
            for (Map<String, String> file : List.of(Map.<String, String>of(), Map.of("dataDir", "s3://bucket/t"))) {
                ConfigSource raw = raw(Map.of("resourceProfile", "speed"), Map.of(), file);
                int balanced = ConfigAdapter.duckDbSizing(raw(Map.of(), Map.of(), file), host).threads();
                int speed = ConfigAdapter.duckDbShared(effective(raw, host), host).threads();
                assertEquals(Math.max(cores, balanced), speed, cores + " cores " + file);
                assertTrue(speed >= 4, cores + " cores " + file);
            }
        }
        ConfigSource explicit = effective(raw(Map.of("resourceProfile", "speed", "duckDbThreads", "2"), Map.of(),
                Map.of()), new HostLimits(2, 6 * GIB, 3 * GIB, 2 * GIB));
        assertEquals(2, ConfigAdapter.duckDbShared(explicit, new HostLimits(2, 6 * GIB, 3 * GIB, 2 * GIB)).threads());
    }

    /** A small host gets the minimum node memory limit and no read caches. */
    @Test
    void smallHostGetsTheMinimumMemoryLimit() {
        HostLimits tiny = new HostLimits(2, 2 * GIB, 256L << 20, 256L << 20);
        ConfigSource source = effective(raw(Map.of("resourceProfile", "speed"), Map.of(), Map.of()), tiny);
        assertEquals(0, new PropertiesPluginConfig("s3", source, List.of()).getInt("cacheMegabytes", 0));
        // Balanced gives its two connections 1 GiB each; capped at 80 % of the 2 GiB.
        assertEquals("1638MiB", ConfigAdapter.duckDbShared(source, tiny).memoryLimit());
    }
}
