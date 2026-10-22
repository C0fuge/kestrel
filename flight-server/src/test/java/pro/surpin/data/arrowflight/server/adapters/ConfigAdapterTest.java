package pro.surpin.data.arrowflight.server.adapters;

import pro.surpin.data.arrowflight.server.model.AppConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
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
        assertEquals(2, cfg.duckDbThreads());
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

    @Test
    void getConfigDuckDbThreads() {
        setProp("duckDbThreads", "4");

        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(4, cfg.duckDbThreads());
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

    /** Verifies the HDFS DataNode timeouts: 5 s / 15 s by default, 0 allowed, negative refused. */
    @Test
    void getConfigHdfsTimeouts() {
        AppConfig defaults = ConfigAdapter.getConfig();
        assertEquals(AppConfig.DEFAULT_HDFS_CONNECT_TIMEOUT_MS, defaults.duckDbHdfsConnectTimeoutMs());
        assertEquals(AppConfig.DEFAULT_HDFS_READ_TIMEOUT_MS, defaults.duckDbHdfsReadTimeoutMs());
        assertEquals(5000, defaults.duckDbHdfsConnectTimeoutMs());
        assertEquals(15000, defaults.duckDbHdfsReadTimeoutMs());

        setProp("duckDbHdfsConnectTimeoutMs", "3000");
        setProp("arrowflight.duckdb.hdfs.readTimeoutMs", "0");
        AppConfig cfg = ConfigAdapter.getConfig();
        assertEquals(3000, cfg.duckDbHdfsConnectTimeoutMs());
        assertEquals(0, cfg.duckDbHdfsReadTimeoutMs());

        setProp("duckDbHdfsReadTimeoutMs", "-1");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                ConfigAdapter::getConfig);
        assertTrue(e.getMessage().contains("duckDbHdfsReadTimeoutMs"), e.getMessage());
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

    @Test
    void s3SettingsDefaults() {
        pro.surpin.data.arrowflight.server.storage.S3Settings s3 = ConfigAdapter.getConfig().s3();

        assertNull(s3.endpoint());
        assertEquals("us-east-1", s3.region());
        assertEquals("path", s3.urlStyle());
        assertTrue(s3.verifyTls());
        assertEquals(30, s3.listingTtlSeconds());
        assertEquals(ConfigAdapter.getConfig().ioParallelism(), s3.maxConnections());
        assertTrue(s3.metadataCache());
        assertFalse(s3.hasCredentials());
    }

    @Test
    void s3SettingsFromSystemPropertiesAndEnvironment() {
        setProp("s3Endpoint", "http://minio:9000");
        setProp("arrowflight.s3.region", "eu-west-3");
        setProp("s3UrlStyle", "VHOST");
        setProp("s3ListingTtlSeconds", "5");
        setProp("s3MaxConnections", "7");
        setProp("duckDbHttpfsExtension", "/ext/httpfs.duckdb_extension");
        setProp("duckDbS3MetadataCache", "false");

        pro.surpin.data.arrowflight.server.storage.S3Settings s3 = ConfigAdapter.s3Settings(
                new java.util.Properties(), 16, true,
                java.util.Map.of("FLIGHT_S3_ACCESS_KEY", "AK", "FLIGHT_S3_SECRET_KEY", "SK"));

        assertEquals("http://minio:9000", s3.endpoint());
        assertEquals("eu-west-3", s3.region());
        assertEquals("vhost", s3.urlStyle());
        assertEquals(5, s3.listingTtlSeconds());
        assertEquals(7, s3.maxConnections());
        assertEquals("/ext/httpfs.duckdb_extension", s3.httpfsExtension());
        assertFalse(s3.metadataCache());
        assertEquals("AK", s3.accessKey());
        assertEquals("SK", s3.secretKey());
    }

    @Test
    void s3CredentialsAreReadOnlyForAnS3DataDirectory() {
        pro.surpin.data.arrowflight.server.storage.S3Settings s3 = ConfigAdapter.s3Settings(
                new java.util.Properties(), 16, false,
                java.util.Map.of("FLIGHT_S3_ACCESS_KEY", "AK", "FLIGHT_S3_SECRET_KEY", "SK"));

        assertFalse(s3.hasCredentials());
        assertEquals(16, s3.maxConnections());
    }

    @Test
    void s3SecretAsSystemPropertyIsAStartupError() {
        setProp("s3SecretKey", "do-not-pass-me-here");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                ConfigAdapter::getConfig);

        assertTrue(e.getMessage().contains("s3SecretKey"), e.getMessage());
        assertFalse(e.getMessage().contains("do-not-pass-me-here"), e.getMessage());
    }

    @Test
    void s3SecretInPropertiesFileIsAStartupError() {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("arrowflight.s3.secretKey", "x");

        assertThrows(IllegalArgumentException.class,
                () -> ConfigAdapter.s3Settings(props, 16, true, java.util.Map.of()));
    }
}
