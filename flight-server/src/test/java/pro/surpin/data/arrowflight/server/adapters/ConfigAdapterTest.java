package pro.surpin.data.arrowflight.server.adapters;

import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/** Tests system-property and environment configuration mapping. */
@Tag("unit")
class ConfigAdapterTest {

    private static final Set<String> CLEARED = new HashSet<>();

    @AfterEach
    void clearSystemProps() {
        for (String key : CLEARED) {
            System.clearProperty(key);
        }
        CLEARED.clear();
    }

    private static void setProp(String key, String value) {
        System.setProperty(key, value);
        CLEARED.add(key);
    }

    @Test
    void getConfigDefaults() {
        AppConfig cfg = ConfigAdapter.getConfig();

        assertEquals(6, cfg.numServers());
        assertEquals(65536, cfg.batchSize());
        assertEquals(1048576, cfg.ioFileBufferSize());
        // ioParallelism defaults to the formula (properties ship ioParallelism=0),
        // so the value is core-count dependent: clamped to [minThreads=8, 64].
        assertTrue(cfg.ioParallelism() >= 8 && cfg.ioParallelism() <= 64,
                "ioParallelism should be within the formula range, got " + cfg.ioParallelism());
        assertEquals(DuckDbSizing.compute(HostLimits.detect(), false, 0, null, 0).threads(),
                cfg.duckDbThreads());
        assertTrue(cfg.duckDbStreamResults());
        assertFalse(cfg.duckDbExternalFileCache());
        assertEquals(64, cfg.duckDbStatementCacheSize());
        assertEquals("/data/parquet", cfg.dataDir());
        assertNull(cfg.localDataDir());
        assertEquals(32010, cfg.port());
        assertEquals(5701, cfg.hazelcastPort());
        assertEquals(60, cfg.hazelcastClusterJoinTimeoutSec());
        assertEquals(3, cfg.clientMaxRetries());
        assertEquals(1000, cfg.clientRetryBackoffMs());
        assertEquals(0, cfg.clientConnectTimeoutMs());
        assertFalse(cfg.duckDbAllowUnsignedExtensions());
        assertTrue(cfg.metricsEnabled());
        assertEquals(2_147_483_648L, cfg.arrowAllocatorLimitBytes());
        assertEquals(67_108_864, cfg.grpcMaxInboundMessageSize());
        assertEquals(268435456, cfg.flightBackpressureThresholdBytes());
        assertEquals(300000, cfg.flightListenerReadyTimeoutMillis());
    }

    @Test
    void getConfigNumServersViaSysProp() {
        setProp("numServers", "10");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(10, cfg.numServers());
    }

    @Test
    void getConfigNumServersDefaults() {
        // No explicit property set, should use arrowflight.properties.
        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(6, cfg.numServers());
    }

    @Test
    void getConfigBatchSizeViaSysProp() {
        setProp("batchSize", "8192");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(8192, cfg.batchSize());
    }

    @Test
    void getConfigSysPropTakesPrecedenceOverPropsFile() {
        // The system property must override arrowflight.properties.
        // System property must override it.
        setProp("batchSize", "100");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(100, cfg.batchSize());
    }

    @Test
    void getConfigIoParallelismExplicit() {
        setProp("ioParallelism", "8");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(8, cfg.ioParallelism());
    }

    @Test
    void getConfigIoParallelismClamped() {
        setProp("ioParallelism", "999");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(64, cfg.ioParallelism());
    }

    /** The code default matches the bundled file: 4 threads without any setting (TUNING.md). */
    @Test
    void duckDbThreadsDefaultsToAQuarterOfTheCoresWithoutAnySetting() throws IOException {
        int expected = DuckDbSizing.compute(HostLimits.detect(), false, 0, null, 0).threads();
        AppConfig cfg = ConfigAdapter.getConfig(
                new ConfigSource(key -> null, new Properties(), Map.of(), null));
        assertEquals(expected, cfg.duckDbThreads());
        assertEquals(expected, ConfigAdapter.getConfig(source(Map.of("duckDbThreads", "0"),
                Map.of())).duckDbThreads());
    }

    @Test
    void duckDbThreadsUnderLoadIsOffByDefaultAndRefusesNegativeValues() throws IOException {
        assertEquals(0, ConfigAdapter.duckDbThreadsUnderLoad(source(Map.of(), Map.of())));
        assertEquals(1, ConfigAdapter.duckDbThreadsUnderLoad(
                source(Map.of("duckDbThreadsUnderLoad", "1"), Map.of())));
        assertEquals(1, ConfigAdapter.duckDbThreadsUnderLoad(
                source(Map.of("arrowflight.duckdb.threadsUnderLoad", "1"), Map.of())));
        assertEquals(2, ConfigAdapter.duckDbThreadsUnderLoad(
                source(Map.of(), Map.of("DUCKDB_THREADS_UNDER_LOAD", "2"))));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ConfigAdapter.duckDbThreadsUnderLoad(
                        source(Map.of("duckDbThreadsUnderLoad", "-1"), Map.of())));
        assertTrue(e.getMessage().contains("duckDbThreadsUnderLoad"), e.getMessage());
    }

    @Test
    void duckDbThreadsWhenIdleIsOffByDefaultAndRefusesNegativeValues() throws IOException {
        assertEquals(0, ConfigAdapter.duckDbThreadsWhenIdle(source(Map.of(), Map.of())));
        assertEquals(8, ConfigAdapter.duckDbThreadsWhenIdle(
                source(Map.of("duckDbThreadsWhenIdle", "8"), Map.of())));
        assertEquals(8, ConfigAdapter.duckDbThreadsWhenIdle(
                source(Map.of("arrowflight.duckdb.threadsWhenIdle", "8"), Map.of())));
        assertEquals(16, ConfigAdapter.duckDbThreadsWhenIdle(
                source(Map.of(), Map.of("DUCKDB_THREADS_WHEN_IDLE", "16"))));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ConfigAdapter.duckDbThreadsWhenIdle(
                        source(Map.of("duckDbThreadsWhenIdle", "-1"), Map.of())));
        assertTrue(e.getMessage().contains("duckDbThreadsWhenIdle"), e.getMessage());
    }

    @Test
    void duckDbProducingStreamsIsOffByDefaultAndRefusesNegativeValues() throws IOException {
        assertEquals(0, ConfigAdapter.duckDbProducingStreams(source(Map.of(), Map.of())));
        assertEquals(2, ConfigAdapter.duckDbProducingStreams(
                source(Map.of("duckDbProducingStreams", "2"), Map.of())));
        assertEquals(1, ConfigAdapter.duckDbProducingStreams(
                source(Map.of("arrowflight.duckdb.producingStreams", "1"), Map.of())));
        assertEquals(3, ConfigAdapter.duckDbProducingStreams(
                source(Map.of(), Map.of("DUCKDB_PRODUCING_STREAMS", "3"))));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ConfigAdapter.duckDbProducingStreams(
                        source(Map.of("duckDbProducingStreams", "-1"), Map.of())));
        assertTrue(e.getMessage().contains("duckDbProducingStreams"), e.getMessage());
    }

    @Test
    void flightZeroCopyWritesIsOffByDefaultAndRefusesOtherValues() throws IOException {
        assertFalse(ConfigAdapter.flightZeroCopyWrites(source(Map.of(), Map.of())));
        assertTrue(ConfigAdapter.flightZeroCopyWrites(
                source(Map.of("flightZeroCopyWrites", "true"), Map.of())));
        assertTrue(ConfigAdapter.flightZeroCopyWrites(
                source(Map.of("arrowflight.flight.zeroCopyWrites", "TRUE"), Map.of())));
        assertTrue(ConfigAdapter.flightZeroCopyWrites(
                source(Map.of(), Map.of("FLIGHT_ZERO_COPY_WRITES", "true"))));
        assertFalse(ConfigAdapter.flightZeroCopyWrites(
                source(Map.of("flightZeroCopyWrites", "false"), Map.of())));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ConfigAdapter.flightZeroCopyWrites(
                        source(Map.of("flightZeroCopyWrites", "yes"), Map.of())));
        assertTrue(e.getMessage().contains("flightZeroCopyWrites"), e.getMessage());
    }

    @Test
    void distributedCompleteIsOffByDefaultAndRefusesOtherValues() throws IOException {
        assertFalse(ConfigAdapter.distributedComplete(source(Map.of(), Map.of())));
        assertTrue(ConfigAdapter.distributedComplete(
                source(Map.of("distributedComplete", "true"), Map.of())));
        assertTrue(ConfigAdapter.distributedComplete(
                source(Map.of(), Map.of("FLIGHT_DISTRIBUTED_COMPLETE", "TRUE"))));
        assertThrows(IllegalArgumentException.class, () -> ConfigAdapter.distributedComplete(
                source(Map.of("distributedComplete", "on"), Map.of())));
        assertEquals(null, ConfigAdapter.distributedCompleteTrustedCertificates(source(Map.of(), Map.of())));
        assertEquals("/etc/flight/ca.pem", ConfigAdapter.distributedCompleteTrustedCertificates(
                source(Map.of(), Map.of("FLIGHT_DISTRIBUTED_COMPLETE_TRUSTED_CERTS", " /etc/flight/ca.pem "))));
    }

    /**
     * Regression for the S3 small-query tail (2026-10-28): on object stores the default keeps at
     * least 4 threads, local and HDFS data directories keep the size-aware default, an explicit
     * value wins everywhere.
     */
    @Test
    void duckDbThreadsDefaultKeepsFourOnObjectStores() throws IOException {
        assertEquals(4, ConfigAdapter.defaultDuckDbThreads(4, "s3://lake/data"));
        assertEquals(4, ConfigAdapter.defaultDuckDbThreads(1, "S3A://lake/data"));
        assertEquals(4, ConfigAdapter.defaultDuckDbThreads(8, "s3n://lake/data"));
        assertEquals(4, ConfigAdapter.defaultDuckDbThreads(4, "lakefs://repo/main/data"));
        assertEquals(6, ConfigAdapter.defaultDuckDbThreads(24, "s3://lake/data"));
        assertEquals(16, ConfigAdapter.defaultDuckDbThreads(64, "s3://lake/data"));
        assertEquals(2, ConfigAdapter.defaultDuckDbThreads(4, "/data/parquet"));
        assertEquals(2, ConfigAdapter.defaultDuckDbThreads(4, "hdfs://nn:8020/data"));
        assertEquals(2, ConfigAdapter.defaultDuckDbThreads(4, "file:///data/s3://x"));
        assertEquals(2, ConfigAdapter.defaultDuckDbThreads(4, null));
        HostLimits host = new HostLimits(8, 64L << 30, 8L << 30, 8L << 30);
        assertEquals(4, ConfigAdapter.getConfig(
                source(Map.of("dataDir", "s3://lake/data"), Map.of()), host).duckDbThreads());
        assertEquals(2, ConfigAdapter.getConfig(
                source(Map.of("dataDir", "hdfs://nn:8020/data"), Map.of()), host).duckDbThreads());
        assertEquals(2, ConfigAdapter.getConfig(source(Map.of("dataDir", "s3://lake/data",
                "duckDbThreads", "2"), Map.of())).duckDbThreads());
    }

    @Test
    void duckDbThreadsDefaultIsAQuarterOfTheCoresBetweenTwoAndSixteen() {
        assertEquals(2, ConfigAdapter.defaultDuckDbThreads(1));
        assertEquals(2, ConfigAdapter.defaultDuckDbThreads(4));
        assertEquals(2, ConfigAdapter.defaultDuckDbThreads(11));
        assertEquals(3, ConfigAdapter.defaultDuckDbThreads(12));
        assertEquals(4, ConfigAdapter.defaultDuckDbThreads(16));
        assertEquals(8, ConfigAdapter.defaultDuckDbThreads(32));
        assertEquals(16, ConfigAdapter.defaultDuckDbThreads(64));
        assertEquals(16, ConfigAdapter.defaultDuckDbThreads(128));
    }

    /** Item 19b: memory limit by host size, explicit values win, none keeps DuckDB's default. */
    @Test
    void duckDbMemoryLimitDefaultsToTheHostBudgetPerConnection() throws IOException {
        HostLimits host = new HostLimits(16, 64L << 30, 8L << 30, 4L << 30);
        AppConfig auto = ConfigAdapter.getConfig(source(Map.of(), Map.of()), host);
        // 64 - 8 - 4 - 15 % of 64 = ~42.4 GiB over 16 / 4 = 4 connections
        assertEquals("10854MiB", auto.duckDbMemoryLimit());
        assertEquals(4, auto.duckDbThreads());
        assertEquals("8GB", ConfigAdapter.getConfig(
                source(Map.of("duckDbMemoryLimit", "8GB"), Map.of()), host).duckDbMemoryLimit());
        assertNull(ConfigAdapter.getConfig(
                source(Map.of("duckDbMemoryLimit", "none"), Map.of()), host).duckDbMemoryLimit());
        assertEquals("2713MiB", ConfigAdapter.getConfig(
                source(Map.of("duckDbMemoryConnections", "16"), Map.of()), host).duckDbMemoryLimit());
        assertThrows(IllegalArgumentException.class, () -> ConfigAdapter.getConfig(
                source(Map.of("duckDbMemoryConnections", "-1"), Map.of()), host));
    }

    @Test
    void negativeDuckDbThreadsIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> ConfigAdapter.getConfig(
                source(Map.of("duckDbThreads", "-1"), Map.of())));
    }

    @Test
    void getConfigDuckDbThreads() {
        setProp("duckDbThreads", "6");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(6, cfg.duckDbThreads());
    }

    /** Verifies DuckDB HDFS extension settings map from system properties. */
    @Test
    void getConfigDuckDbHdfsExtension() {
        setProp("arrowflight.duckdb.hdfs.extension", "/opt/duckdb-hdfs/hadoopfs.duckdb_extension");
        setProp("arrowflight.duckdb.allowUnsignedExtensions", "true");

        AppConfig cfg = ConfigAdapter.getConfig();

        assertEquals("/opt/duckdb-hdfs/hadoopfs.duckdb_extension",
                cfg.duckDbHdfsExtension());
        assertTrue(cfg.duckDbAllowUnsignedExtensions());
    }

    /** Verifies the HDFS DataNode timeouts: 3 s / 10 s by default, 0 allowed, negative refused. */
    @Test
    void getConfigHdfsTimeouts() {
        AppConfig defaults = ConfigAdapter.getConfig();
        assertEquals(AppConfig.DEFAULT_HDFS_CONNECT_TIMEOUT_MS, defaults.duckDbHdfsConnectTimeoutMs());
        assertEquals(AppConfig.DEFAULT_HDFS_READ_TIMEOUT_MS, defaults.duckDbHdfsReadTimeoutMs());
        assertEquals(3000, defaults.duckDbHdfsConnectTimeoutMs());
        assertEquals(10000, defaults.duckDbHdfsReadTimeoutMs());

        setProp("duckDbHdfsConnectTimeoutMs", "4000");
        setProp("arrowflight.duckdb.hdfs.readTimeoutMs", "0");
        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(4000, cfg.duckDbHdfsConnectTimeoutMs());
        assertEquals(0, cfg.duckDbHdfsReadTimeoutMs());

        setProp("duckDbHdfsReadTimeoutMs", "-1");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                ConfigAdapter::getConfig);
        assertTrue(e.getMessage().contains("duckDbHdfsReadTimeoutMs"), e.getMessage());
    }

    /** Verifies endpoints per node: 1 by default, read from both keys, below 1 refused. */
    @Test
    void getConfigEndpointsPerServer() {
        assertEquals(1, ConfigAdapter.getConfig().endpointsPerServer());

        setProp("arrowflight.cluster.endpointsPerServer", "4");
        assertEquals(4, ConfigAdapter.getConfig().endpointsPerServer());
        setProp("endpointsPerServer", "2");
        assertEquals(2, ConfigAdapter.getConfig().endpointsPerServer());
        assertEquals(3, ConfigAdapter.getConfig().withEndpointsPerServer(3).endpointsPerServer());
        assertEquals(2, ConfigAdapter.getConfig().withDuckDbMaxTempSize("1GB").endpointsPerServer());

        setProp("endpointsPerServer", "0");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                ConfigAdapter::getConfig);
        assertTrue(e.getMessage().contains("endpointsPerServer"), e.getMessage());
    }

    /** Verifies the spill size cap: unset by default, read from both keys, garbage refused. */
    @Test
    void getConfigDuckDbMaxTempSize() {
        assertNull(ConfigAdapter.getConfig().duckDbMaxTempSize());

        setProp("arrowflight.duckdb.maxTempSize", " 20GiB ");
        assertEquals("20GiB", ConfigAdapter.getConfig().duckDbMaxTempSize());
        setProp("duckDbMaxTempSize", "500 MB");
        assertEquals("500 MB", ConfigAdapter.getConfig().duckDbMaxTempSize());
        assertNull(ConfigAdapter.getConfig().withDuckDbMaxTempSize(" ").duckDbMaxTempSize());

        setProp("duckDbMaxTempSize", "20GiB'; SET threads = 1; --");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                ConfigAdapter::getConfig);
        assertTrue(e.getMessage().contains("duckDbMaxTempSize"), e.getMessage());
    }

    /** Verifies scan order and read-ahead: DuckDB's own by default, read from both keys, garbage refused. */
    @Test
    void getConfigDuckDbScanOrder() {
        assertTrue(ConfigAdapter.getConfig().duckDbPreserveInsertionOrder());
        assertNull(ConfigAdapter.getConfig().duckDbStreamingBufferSize());

        setProp("arrowflight.duckdb.preserveInsertionOrder", "FALSE");
        setProp("arrowflight.duckdb.streamingBufferSize", " 64MiB ");
        assertFalse(ConfigAdapter.getConfig().duckDbPreserveInsertionOrder());
        assertEquals("64MiB", ConfigAdapter.getConfig().duckDbStreamingBufferSize());
        setProp("duckDbPreserveInsertionOrder", "true");
        setProp("duckDbStreamingBufferSize", "8MB");
        assertTrue(ConfigAdapter.getConfig().duckDbPreserveInsertionOrder());
        assertEquals("8MB", ConfigAdapter.getConfig().duckDbStreamingBufferSize());
        assertNull(ConfigAdapter.getConfig().withDuckDbScanOrder(true, " ")
                .duckDbStreamingBufferSize());

        setProp("duckDbPreserveInsertionOrder", "flase");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                ConfigAdapter::getConfig);
        assertTrue(e.getMessage().contains("duckDbPreserveInsertionOrder"), e.getMessage());
        setProp("duckDbPreserveInsertionOrder", "true");
        setProp("duckDbStreamingBufferSize", "64MiB'; SET threads = 1; --");
        e = assertThrows(IllegalArgumentException.class, ConfigAdapter::getConfig);
        assertTrue(e.getMessage().contains("duckDbStreamingBufferSize"), e.getMessage());
    }

    @Test
    void getConfigHazelcastJoinTimeout() {
        setProp("hazelcastClusterJoinTimeoutSec", "30");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(30, cfg.hazelcastClusterJoinTimeoutSec());
    }

    @Test
    void getConfigHazelcastJoinTimeoutViaAlias() {
        setProp("arrowflight.hazelcast.clusterJoinTimeoutSec", "120");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(120, cfg.hazelcastClusterJoinTimeoutSec());
    }

    @Test
    void getConfigClientSettings() {
        setProp("client.maxRetries", "5");
        setProp("client.retryBackoffMs", "2000");
        setProp("client.connectTimeoutMs", "60000");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(5, cfg.clientMaxRetries());
        assertEquals(2000, cfg.clientRetryBackoffMs());
        assertEquals(60000, cfg.clientConnectTimeoutMs());
    }

    @Test
    void getConfigDataDir() {
        setProp("dataDir", "/custom/data/path");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals("/custom/data/path", cfg.dataDir());
    }

    @Test
    void getConfigLocalDataDir() {
        setProp("localDataDir", "/staging");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals("/staging", cfg.localDataDir());
    }

    @Test
    void getConfigPort() {
        setProp("port", "32020");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(32020, cfg.port());
    }

    @Test
    void getConfigHazelcastPort() {
        setProp("hazelcastPort", "5702");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(5702, cfg.hazelcastPort());
    }

    @Test
    void getConfigGrpcMaxInbound() {
        setProp("grpcMaxInboundMessageSize", "1048576");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(1048576, cfg.grpcMaxInboundMessageSize());
    }

    /** Verifies the Arrow allocator budget can be configured. */
    @Test
    void getConfigArrowAllocatorLimit() {
        setProp("arrowAllocatorLimitBytes", "1073741824");

        assertEquals(1_073_741_824L, ConfigAdapter.getConfig().arrowAllocatorLimitBytes());
    }

    /** Verifies invalid memory budgets fail at startup. */
    @Test
    void getConfigRejectsNonPositiveMemoryBudgets() {
        setProp("arrowAllocatorLimitBytes", "0");
        assertThrows(IllegalArgumentException.class, ConfigAdapter::getConfig);
        clearSystemProps();
        setProp("grpcMaxInboundMessageSize", "0");
        assertThrows(IllegalArgumentException.class, ConfigAdapter::getConfig);
    }

    /** Verifies the Flight outbound backpressure budget is configurable. */
    @Test
    void getConfigFlightBackpressureThreshold() {
        setProp("flightBackpressureThresholdBytes", "33554432");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(33554432, cfg.flightBackpressureThresholdBytes());
    }

    /** Verifies a non-positive Flight outbound backpressure budget is rejected. */
    @Test
    void getConfigRejectsNonPositiveFlightBackpressureThreshold() {
        setProp("flightBackpressureThresholdBytes", "0");

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, ConfigAdapter::getConfig);
        assertTrue(error.getMessage().contains("flightBackpressureThresholdBytes"));
    }

    /** Verifies the outbound budget of all Flight streams defaults to derived and is settable. */
    @Test
    void getConfigFlightOutboundBudget() {
        assertEquals(0L, ConfigAdapter.getConfig().flightOutboundBudgetBytes());

        setProp("flightOutboundBudgetBytes", "134217728");
        assertEquals(134217728L, ConfigAdapter.getConfig().flightOutboundBudgetBytes());
        assertEquals(134217728L, ConfigAdapter.getConfig().withEndpointsPerServer(2)
                .flightOutboundBudgetBytes());
    }

    /** Verifies a negative outbound budget is rejected. */
    @Test
    void getConfigRejectsNegativeFlightOutboundBudget() {
        setProp("flightOutboundBudgetBytes", "-1");

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, ConfigAdapter::getConfig);
        assertTrue(error.getMessage().contains("flightOutboundBudgetBytes"));
    }

    /** Verifies benchmark metrics can be disabled with a system property. */
    @Test
    void getConfigMetricsDisabled() {
        setProp("metricsEnabled", "false");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertFalse(cfg.metricsEnabled());
    }

    @Test
    void getConfigFlightListenerReady() {
        setProp("flightListenerReadyTimeoutMs", "30000");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(30000, cfg.flightListenerReadyTimeoutMillis());
    }

    /** Verifies invalid readiness timeout configuration is rejected at startup. */
    @Test
    void getConfigRejectsNonPositiveFlightListenerReadyTimeout() {
        setProp("flightListenerReadyTimeoutMs", "0");

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, ConfigAdapter::getConfig);
        assertTrue(error.getMessage().contains("flightListenerReadyTimeoutMs"));
    }

    @Test
    void getConfigIoFileBuffer() {
        setProp("ioFileBufferSize", "65536");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(65536, cfg.ioFileBufferSize());
    }

    @Test
    void getConfigInvalidIntThrows() {
        setProp("batchSize", "not-a-number");

        assertThrows(IllegalArgumentException.class, ConfigAdapter::getConfig);
    }

    /** Regression: values in the bundled arrowflight.properties hid environment variables. */
    @Test
    void theEnvironmentBeatsTheBundledFile() throws IOException {
        Map<String, String> env = new HashMap<>();
        env.put("FLIGHT_RESULT_MODE", "partial");
        env.put("FLIGHT_ENDPOINT_BACKUP_LOCATIONS", "0");
        env.put("FLIGHT_METRICS_ENABLED", "false");
        env.put("DUCKDB_LOCKDOWN", "false");
        env.put("DUCKDB_STREAM_RESULTS", "false");
        AppConfig cfg = ConfigAdapter.getConfig(source(Map.of(), env));
        assertEquals("partial", cfg.resultMode());
        assertEquals(0, cfg.endpointBackupLocations());
        assertFalse(cfg.metricsEnabled());
        assertFalse(cfg.duckDbLockdown());
        assertFalse(cfg.duckDbStreamResults());

        AppConfig bundled = ConfigAdapter.getConfig(source(Map.of(), Map.of()));
        assertEquals("complete", bundled.resultMode());
        assertEquals(2, bundled.endpointBackupLocations());
        assertTrue(bundled.metricsEnabled());
        assertTrue(bundled.duckDbLockdown());
    }

    /** Regression: the file's short key beat the documented -Darrowflight.* alias. */
    @Test
    void everySystemPropertyBeatsTheEnvironmentAndTheFile() throws IOException {
        Map<String, String> env = Map.of("FLIGHT_METRICS_ENABLED", "true",
                "FLIGHT_RESULT_MODE", "partial");
        AppConfig cfg = ConfigAdapter.getConfig(source(Map.of(
                "arrowflight.metrics.enabled", "false",
                "arrowflight.resultMode", "complete",
                "arrowflight.cluster.numServers", "9",
                "arrowflight.duckdb.threads", "5"), env));
        assertFalse(cfg.metricsEnabled());
        assertEquals("complete", cfg.resultMode());
        assertEquals(9, cfg.numServers());
        assertEquals(5, cfg.duckDbThreads());

        AppConfig primary = ConfigAdapter.getConfig(source(Map.of(
                "numServers", "4", "arrowflight.cluster.numServers", "9"), Map.of()));
        assertEquals(4, primary.numServers());
    }

    /** A blank system property or variable counts as unset and lets the next source through. */
    @Test
    void blankValuesFallThrough() throws IOException {
        AppConfig cfg = ConfigAdapter.getConfig(source(Map.of("resultMode", " "),
                Map.of("FLIGHT_RESULT_MODE", "partial", "FLIGHT_ENDPOINT_BACKUP_LOCATIONS", "")));
        assertEquals("partial", cfg.resultMode());
        assertEquals(2, cfg.endpointBackupLocations());
    }

    private static ConfigSource source(Map<String, String> system, Map<String, String> env)
            throws IOException {
        Properties bundled = new Properties();
        try (InputStream input = ConfigAdapterTest.class.getClassLoader()
                .getResourceAsStream("arrowflight.properties")) {
            bundled.load(input);
        }
        Map<String, String> sys = Map.copyOf(system);
        return new ConfigSource(sys::get, bundled, env, null);
    }
}
