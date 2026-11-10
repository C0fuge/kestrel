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
        assertFalse(config.duckDbExternalFileCache());
        assertTrue(config.duckDbPreserveInsertionOrder());
    }

    /**
     * Q11 (item 39): on an hdfs:// data dir balanced keeps DuckDB's external file cache, over the
     * bundled file's false; the operator's value wins, and other data dirs stay unchanged.
     */
    @Test
    void balancedKeepsTheExternalFileCacheOnHdfs() {
        Map<String, String> file = Map.of("duckDbExternalFileCache", "false", "duckDbSharedDatabase", "false");
        ConfigSource hdfs = raw(Map.of("dataDir", "hdfs://nn:8020/data"), Map.of(), file);
        ResourceProfile profile = ResourceProfile.resolve(hdfs, () -> {
            throw new AssertionError("host read for balanced");
        });
        assertEquals("profile=balanced duckDbExternalFileCache=true", profile.describe());
        assertTrue(ConfigAdapter.getConfig(effective(hdfs, MEDIUM), MEDIUM).duckDbExternalFileCache());
        assertNull(ConfigAdapter.duckDbShared(effective(hdfs, MEDIUM), MEDIUM));

        ConfigSource off = raw(Map.of("dataDir", "hdfs://nn:8020/data"),
                Map.of("DUCKDB_EXTERNAL_FILE_CACHE", "false"), file);
        assertEquals("profile=balanced duckDbExternalFileCache=false(explicit)",
                ResourceProfile.resolve(off, () -> MEDIUM).describe());
        assertFalse(ConfigAdapter.getConfig(effective(off, MEDIUM), MEDIUM).duckDbExternalFileCache());

        ConfigSource local = raw(Map.of("dataDir", "/data/parquet"), Map.of(), file);
        assertEquals("profile=balanced", ResourceProfile.resolve(local, () -> MEDIUM).describe());
        assertFalse(ConfigAdapter.getConfig(effective(local, MEDIUM), MEDIUM).duckDbExternalFileCache());

        ConfigSource speedPerConnection = raw(Map.of("resourceProfile", "speed", "dataDir", "hdfs://nn/d",
                "duckDbSharedDatabase", "false"), Map.of(), file);
        assertTrue(ConfigAdapter.getConfig(effective(speedPerConnection, MEDIUM), MEDIUM)
                .duckDbExternalFileCache());
    }

    /** A typo is refused instead of running balanced silently; the name ignores case. */
    @Test
    void unknownProfileIsRefusedAndCaseIsIgnored() {
        ConfigSource typo = raw(Map.of("resourceProfile", "sped"), Map.of(), Map.of());
        assertThrows(IllegalArgumentException.class, () -> ResourceProfile.resolve(typo, () -> MEDIUM));
        ConfigSource upper = raw(Map.of(), Map.of("FLIGHT_RESOURCE_PROFILE", "SPEED"), Map.of());
        assertEquals(ResourceProfile.SPEED, ResourceProfile.resolve(upper, () -> MEDIUM).name());
    }

    /**
     * speed on 4 / 16 / 64 cores: shared DuckDB with all cores and the memory balanced gives its
     * connections when only the JVM's reservation (Q-40a) is taken off: SMALL 1 GiB heap + 1 GiB direct,
     * MEDIUM 1 GiB + 2 GiB, LARGE capped at 80 % of the memory.
     */
    @Test
    void speedSizesTheSharedDuckDbByHost() {
        assertShared(SMALL, 4, "3174MiB");
        assertShared(MEDIUM, 16, "24780MiB");
        assertShared(LARGE, 64, "209715MiB");
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
        assertTrue(config.duckDbExternalFileCache(), "external file cache " + host);
        assertFalse(config.duckDbPreserveInsertionOrder(), "insertion order " + host);
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
     * The egret profile of item 23b (2 cores, 6 GB, -Xmx3g, direct 2g, data on S3): the shared DuckDB gets
     * at least what DuckDB-per-connection gives its two expected connections (2 x 1 GiB) instead of 1 GiB
     * for the whole node (now 2 x 1331 MiB with the Q-40a reservation), and a query that still runs out
     * of memory runs again on a private DuckDB sized as balanced.
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
        assertEquals("2662MiB", shared.memoryLimit());
        assertEquals(4, shared.threads());
        assertEquals(balanced.threads(), shared.fallbackThreads());
        assertEquals("1024MiB", shared.fallbackMemoryLimit());
        for (HostLimits host : List.of(egret, SMALL, MEDIUM, LARGE)) {
            DuckDbSizing perConnection = ConfigAdapter.duckDbSizing(raw(Map.of(), Map.of(), Map.of()), host);
            long node = DuckDbSizing.parseBytes(ConfigAdapter.duckDbShared(
                    effective(raw(Map.of("resourceProfile", "speed"), Map.of(), Map.of()), host), host).memoryLimit());
            long cap = host.memoryBytes() / 100 * DuckDbSizing.MAX_CONNECTION_PERCENT;
            assertTrue(node >= Math.min(DuckDbSizing.parseBytes(perConnection.memoryLimit())
                    * perConnection.connections(), cap), host.toString());
            assertTrue(node <= cap, host.toString());
        }
    }

    /**
     * Q-40a: speed reserves for the JVM what it can use, not its limits - heap min(-Xmx, max(1 GiB, heap in
     * use x 2)), direct min(limit, 1 GiB + 25 %) - so DuckDB's limit holds the S3 file cache. tpch-perf's
     * container (4 cores, 8 GB, -Xmx3g, direct 2g): 2048 MiB before, 4402 MiB now (5120 MiB measured warm
     * TPC-H SF10 on S3 19.4 s instead of 75.8 s).
     */
    @Test
    void speedReservesWhatTheJvmUsesNotItsLimits() {
        HostLimits harness = new HostLimits(4, 8 * GIB, 3 * GIB, 2 * GIB);
        HostLimits reserved = ResourceProfile.speedReservation(harness);
        assertEquals(GIB, reserved.maxHeapBytes());
        assertEquals(GIB + 2 * GIB / 100 * 25, reserved.maxDirectBytes());
        assertEquals(harness.memoryBytes(), reserved.memoryBytes());
        assertEquals(harness.cores(), reserved.cores());
        ConfigSource raw = raw(Map.of("resourceProfile", "speed"), Map.of(), Map.of("dataDir", "s3://bucket/t"));
        assertEquals("4402MiB", ConfigAdapter.duckDbShared(effective(raw, harness), harness).memoryLimit());

        // A heap in use counts twice, up to -Xmx; with it all in use speed reserves the whole heap.
        HostLimits busy = new HostLimits(4, 8 * GIB, 3 * GIB, 2 * GIB, 3 * GIB / 4);
        assertEquals(3 * GIB / 2, ResourceProfile.speedReservation(busy).maxHeapBytes());
        assertEquals("3890MiB", ConfigAdapter.duckDbShared(effective(raw, busy), busy).memoryLimit());
        HostLimits full = new HostLimits(4, 8 * GIB, 3 * GIB, 2 * GIB, 2 * GIB);
        assertEquals(3 * GIB, ResourceProfile.speedReservation(full).maxHeapBytes());

        // Small limits are reserved whole: never more than the JVM may take.
        HostLimits small = new HostLimits(2, 2 * GIB, 256L << 20, 256L << 20, 200L << 20);
        assertEquals(256L << 20, ResourceProfile.speedReservation(small).maxHeapBytes());
        assertEquals(256L << 20, ResourceProfile.speedReservation(small).maxDirectBytes());

        // balanced and an explicit limit are unchanged.
        DuckDbSizing balanced = ConfigAdapter.duckDbSizing(raw(Map.of(), Map.of(), Map.of()), harness);
        assertEquals(ConfigAdapter.duckDbSizing(raw(Map.of(), Map.of(), Map.of()), reserved).connections(),
                balanced.connections());
        assertEquals("1024MiB", balanced.memoryLimit());
        ConfigSource explicit = raw(Map.of("resourceProfile", "speed", "duckDbMemoryLimit", "3GiB"), Map.of(),
                Map.of());
        assertEquals("3GiB", ConfigAdapter.duckDbShared(effective(explicit, harness), harness).memoryLimit());
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
        // Balanced gives its two connections 1 GiB each, 80 % of the 2 GiB would be 1638 MiB: with heap and
        // direct memory above the 2 GiB, so the budget (2 GiB - 512 MiB - 15 %) caps it (item 42).
        assertEquals("1228MiB", ConfigAdapter.duckDbShared(source, tiny).memoryLimit());
    }

    /**
     * speed turns DuckDB's external file cache on only with the shared DuckDB, whose one memory
     * limit bounds it, and an explicit setting wins (item 24).
     */
    @Test
    void speedCachesFileDataOnlyInTheSharedDuckDb() {
        ConfigSource on = effective(raw(Map.of("resourceProfile", "speed"), Map.of(), Map.of()), MEDIUM);
        assertTrue(ConfigAdapter.getConfig(on, MEDIUM).duckDbExternalFileCache());
        assertTrue(ResourceProfile.resolve(raw(Map.of("resourceProfile", "speed"), Map.of(), Map.of()),
                () -> MEDIUM).describe().contains(" duckDbExternalFileCache=true"));

        ConfigSource perConnection = effective(raw(Map.of("resourceProfile", "speed"),
                Map.of("DUCKDB_SHARED_DATABASE", "false"), Map.of()), MEDIUM);
        assertFalse(ConfigAdapter.getConfig(perConnection, MEDIUM).duckDbExternalFileCache());

        ConfigSource raw = raw(Map.of("resourceProfile", "speed"), Map.of("DUCKDB_EXTERNAL_FILE_CACHE", "false"),
                Map.of());
        assertFalse(ConfigAdapter.getConfig(effective(raw, MEDIUM), MEDIUM).duckDbExternalFileCache());
        assertTrue(ResourceProfile.resolve(raw, () -> MEDIUM).describe()
                .contains("duckDbExternalFileCache=false(explicit)"));
        ConfigSource property = effective(raw(Map.of("resourceProfile", "speed",
                "arrowflight.duckdb.externalFileCache", "false"), Map.of(), Map.of()), MEDIUM);
        assertFalse(ConfigAdapter.getConfig(property, MEDIUM).duckDbExternalFileCache());
    }

    /**
     * speed lets parallel scan threads hand on batches out of file order (item 27); a setting given
     * as a system property or in the environment keeps the order, and balanced keeps it.
     */
    @Test
    void speedDropsTheInsertionOrderUnlessSet() {
        ConfigSource speed = raw(Map.of("resourceProfile", "speed"), Map.of(), Map.of());
        assertFalse(ConfigAdapter.getConfig(effective(speed, MEDIUM), MEDIUM).duckDbPreserveInsertionOrder());
        assertTrue(ResourceProfile.resolve(speed, () -> MEDIUM).describe()
                .contains(" duckDbPreserveInsertionOrder=false"));

        ConfigSource env = raw(Map.of("resourceProfile", "speed"), Map.of("DUCKDB_PRESERVE_INSERTION_ORDER", "true"),
                Map.of());
        assertTrue(ConfigAdapter.getConfig(effective(env, MEDIUM), MEDIUM).duckDbPreserveInsertionOrder());
        assertTrue(ResourceProfile.resolve(env, () -> MEDIUM).describe()
                .contains("duckDbPreserveInsertionOrder=true(explicit)"));
        ConfigSource property = raw(Map.of("resourceProfile", "speed", "duckDbPreserveInsertionOrder", "true"),
                Map.of(), Map.of());
        assertTrue(ConfigAdapter.getConfig(effective(property, MEDIUM), MEDIUM).duckDbPreserveInsertionOrder());

        ConfigSource balanced = raw(Map.of(), Map.of(), Map.of());
        assertTrue(ConfigAdapter.getConfig(effective(balanced, MEDIUM), MEDIUM).duckDbPreserveInsertionOrder());
    }

    /**
     * Item 42: in a 3 GiB container with -Xmx768m and direct 512m on 16 cores speed gave DuckDB 2457 MiB
     * (4 connections x the 1 GiB floor, capped only at 80 % of the memory) with a budget of 1331 MiB, and
     * the server was OOM-killed; the limit now never exceeds the budget, for small budgets too.
     */
    @Test
    void speedNeverSizesDuckDbAboveTheBudgetInSmallContainers() {
        long mib = 1L << 20;
        HostLimits stand = new HostLimits(16, 3 * GIB, 768 * mib, 512 * mib, 100 * mib);
        ConfigSource raw = raw(Map.of("resourceProfile", "speed"), Map.of(), Map.of());
        DuckDbSizing reserved = ConfigAdapter.duckDbSizing(raw, ResourceProfile.speedReservation(stand));
        assertEquals(1331, reserved.budgetBytes() >> 20);
        assertEquals("1331MiB", ConfigAdapter.duckDbShared(effective(raw, stand), stand).memoryLimit());

        for (long memory : List.of(1 * GIB, 3 * GIB / 2, 2 * GIB, 5 * GIB / 2, 3 * GIB)) {
            for (long heap : List.of(256 * mib, 512 * mib, 768 * mib, GIB)) {
                for (long direct : List.of(256 * mib, 512 * mib, GIB)) {
                    for (int cores : List.of(2, 4, 16, 64)) {
                        HostLimits host = new HostLimits(cores, memory, heap, direct, heap / 4);
                        long budget = ConfigAdapter.duckDbSizing(raw, ResourceProfile.speedReservation(host))
                                .budgetBytes();
                        long limit = DuckDbSizing.parseBytes(
                                ConfigAdapter.duckDbShared(effective(raw, host), host).memoryLimit());
                        String at = host.toString();
                        assertTrue(limit <= Math.max(DuckDbSizing.MIN_BUDGET_BYTES, budget), at);
                        assertTrue(limit >= DuckDbSizing.MIN_BUDGET_BYTES, at);
                        if (budget >= DuckDbSizing.MIN_BUDGET_BYTES) {
                            assertTrue(limit + heap + direct <= memory, at);
                        }
                    }
                }
            }
        }

        // Large hosts keep their limits: the budget is far above them there.
        HostLimits harness = new HostLimits(4, 8 * GIB, 3 * GIB, 2 * GIB);
        ConfigSource s3 = raw(Map.of("resourceProfile", "speed"), Map.of(), Map.of("dataDir", "s3://bucket/t"));
        assertEquals("4402MiB", ConfigAdapter.duckDbShared(effective(s3, harness), harness).memoryLimit());
    }
}
