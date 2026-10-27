package pro.surpin.data.arrowflight.server.adapters;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;

/**
 * Loads configuration from system properties, environment variables and arrowflight.properties,
 * in that order of precedence (see {@link ConfigSource}). Produces an immutable AppConfig record.
 */
public class ConfigAdapter {

    /**
     * Utility class, no instantiation.
     */
    private ConfigAdapter() {
    }

    /**
     * Loads and returns the complete server configuration.
     *
     * @return resolved AppConfig
     */
    public static AppConfig getConfig() {
        return getConfig(ConfigSource.system());
    }

    /**
     * Resolves the server configuration from a source.
     *
     * @param props configuration source
     * @return resolved AppConfig
     */
    public static AppConfig getConfig(ConfigSource props) {
        int ioParallelism = computeIoParallelism(props);
        int numServers = getInt("numServers", "arrowflight.cluster.numServers", 3, props);
        int batchSize = getInt("batchSize", "arrowflight.duckdb.batchSize", 65536, props);
        int ioFileBufferSize = getInt("ioFileBufferSize", null, 131072, props);
        int duckDbWarmConnections = getInt("duckDbWarmConnections",
                "arrowflight.duckdb.warmConnections", Math.min(8, ioParallelism), props);
        int duckDbGroups = getInt("duckDbGroups",
                "arrowflight.duckdb.groups", Math.min(8, ioParallelism), props);
        int duckDbThreads = getInt("duckDbThreads", "arrowflight.duckdb.threads", 0, props);
        if (duckDbThreads < 0) {
            throw new IllegalArgumentException("duckDbThreads must not be negative: " + duckDbThreads);
        }
        if (duckDbThreads == 0) {
            duckDbThreads = defaultDuckDbThreads(Runtime.getRuntime().availableProcessors());
        }
        String duckDbHdfsExtension = getStringWithEnv("duckDbHdfsExtension",
                "arrowflight.duckdb.hdfs.extension", "DUCKDB_HDFS_EXTENSION", null, props);
        boolean duckDbAllowUnsignedExtensions = getBooleanWithEnv("duckDbAllowUnsignedExtensions",
                "arrowflight.duckdb.allowUnsignedExtensions", "DUCKDB_ALLOW_UNSIGNED_EXTENSIONS",
                false, props);
        String duckDbHdfsDefaultNamenode = getStringWithEnv("duckDbHdfsDefaultNamenode",
                "arrowflight.duckdb.hdfs.defaultNamenode", "HDFS_DEFAULT_NAMENODE", null, props);
        String duckDbHdfsHaNamenodes = getStringWithEnv("duckDbHdfsHaNamenodes",
                "arrowflight.duckdb.hdfs.haNamenodes", "HDFS_HA_NAMENODES", null, props);
        String duckDbHdfsShortcircuit = getStringWithEnv("duckDbHdfsShortcircuit",
                "arrowflight.duckdb.hdfs.shortcircuit", "HDFS_SHORTCIRCUIT", null, props);
        String duckDbHdfsDomainSocketPath = getStringWithEnv("duckDbHdfsDomainSocketPath",
                "arrowflight.duckdb.hdfs.domainSocketPath", "HDFS_DOMAIN_SOCKET_PATH", null, props);
        String duckDbMemoryLimit = getString("duckDbMemoryLimit",
                "arrowflight.duckdb.memoryLimit", null, props);
        boolean metricsEnabled = getBooleanWithEnv("metricsEnabled",
                "arrowflight.metrics.enabled", "FLIGHT_METRICS_ENABLED", false, props);
        long arrowAllocatorLimitBytes = getLong("arrowAllocatorLimitBytes",
                "arrowflight.arrow.allocatorLimitBytes", 2_147_483_648L, props);
        if (arrowAllocatorLimitBytes <= 0) {
            throw new IllegalArgumentException("arrowAllocatorLimitBytes must be positive: "
                    + arrowAllocatorLimitBytes);
        }
        int grpcMaxInboundMessageSize = getInt("grpcMaxInboundMessageSize",
                "arrowflight.grpc.maxInboundMessageSize", 67_108_864, props);
        if (grpcMaxInboundMessageSize <= 0) {
            throw new IllegalArgumentException("grpcMaxInboundMessageSize must be positive: "
                    + grpcMaxInboundMessageSize);
        }
        int flightBackpressureThresholdBytes = getInt("flightBackpressureThresholdBytes",
                "arrowflight.flight.backpressureThresholdBytes", 67_108_864, props);
        if (flightBackpressureThresholdBytes <= 0) {
            throw new IllegalArgumentException(
                    "flightBackpressureThresholdBytes must be positive: "
                            + flightBackpressureThresholdBytes);
        }
        // Queued outbound bytes all Flight streams share; 0 = a quarter of the direct memory limit.
        long flightOutboundBudgetBytes = getLong("flightOutboundBudgetBytes",
                "arrowflight.flight.outboundBudgetBytes", 0L, props);
        if (flightOutboundBudgetBytes < 0) {
            throw new IllegalArgumentException(
                    "flightOutboundBudgetBytes must not be negative: " + flightOutboundBudgetBytes);
        }
        long flightListenerReadyTimeoutMillis = getLong("flightListenerReadyTimeoutMs",
                "arrowflight.flight.listenerReadyTimeoutMs", 300_000L, props);
        if (flightListenerReadyTimeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "flightListenerReadyTimeoutMs must be positive: "
                            + flightListenerReadyTimeoutMillis);
        }
        String dataDir = getString("dataDir", null, "/data/parquet", props);
        String localDataDir = getStringWithEnv("localDataDir", "arrowflight.localDataDir",
                "FLIGHT_LOCAL_DATA_DIR", null, props);
        int port = getInt("port", null, 32010, props);
        int hazelcastPort = getInt("hazelcastPort", null, 5701, props);
        int hazelcastClusterJoinTimeoutSec = getInt("hazelcastClusterJoinTimeoutSec",
                "arrowflight.hazelcast.clusterJoinTimeoutSec", 60, props);
        int clientMaxRetries = getInt("client.maxRetries", null, 3, props);
        int clientRetryBackoffMs = getInt("client.retryBackoffMs", null, 1000, props);
        int clientConnectTimeoutMs = getInt("client.connectTimeoutMs", null, 0, props);
        boolean duckDbStreamResults = getBooleanWithEnv("duckDbStreamResults",
                "arrowflight.duckdb.streamResults", "DUCKDB_STREAM_RESULTS", true, props);
        boolean duckDbExternalFileCache = getBooleanWithEnv("duckDbExternalFileCache",
                "arrowflight.duckdb.externalFileCache", "DUCKDB_EXTERNAL_FILE_CACHE",
                false, props);
        int duckDbStatementCacheSize = getInt("duckDbStatementCacheSize",
                "arrowflight.duckdb.statementCacheSize", 64, props);
        String duckDbTempDirectory = getStringWithEnv("duckDbTempDirectory",
                "arrowflight.duckdb.tempDirectory", "DUCKDB_TEMP_DIRECTORY", null, props);
        String duckDbMaxTempSize = getStringWithEnv("duckDbMaxTempSize",
                "arrowflight.duckdb.maxTempSize", "DUCKDB_MAX_TEMP_SIZE", null, props);
        String preserveOrder = getStringWithEnv("duckDbPreserveInsertionOrder",
                "arrowflight.duckdb.preserveInsertionOrder", "DUCKDB_PRESERVE_INSERTION_ORDER",
                "true", props);
        // A typo must not silently turn the order guarantee off: only true or false.
        if (!"true".equalsIgnoreCase(preserveOrder) && !"false".equalsIgnoreCase(preserveOrder)) {
            throw new IllegalArgumentException(
                    "duckDbPreserveInsertionOrder must be true or false: " + preserveOrder);
        }
        String duckDbStreamingBufferSize = getStringWithEnv("duckDbStreamingBufferSize",
                "arrowflight.duckdb.streamingBufferSize", "DUCKDB_STREAMING_BUFFER_SIZE", null,
                props);
        String authToken = getStringWithEnv("authToken",
                "arrowflight.auth.token", "FLIGHT_AUTH_TOKEN", null, props);
        String resultMode = getStringWithEnv("resultMode", "arrowflight.resultMode",
                "FLIGHT_RESULT_MODE", "complete", props);
        boolean widenUnsignedTypes = getBooleanWithEnv("widenUnsignedTypes",
                "arrowflight.widenUnsignedTypes", "FLIGHT_WIDEN_UNSIGNED_TYPES", true, props);
        boolean duckDbLockdown = getBooleanWithEnv("duckDbLockdown",
                "arrowflight.duckdb.lockdown", "DUCKDB_LOCKDOWN", true, props);
        int endpointBackupLocations = getIntWithEnv("endpointBackupLocations",
                "arrowflight.cluster.endpointBackupLocations", "FLIGHT_ENDPOINT_BACKUP_LOCATIONS",
                AppConfig.DEFAULT_ENDPOINT_BACKUP_LOCATIONS, props);
        if (endpointBackupLocations < 0) {
            throw new IllegalArgumentException(
                    "endpointBackupLocations must not be negative: " + endpointBackupLocations);
        }
        int endpointsPerServer = getIntWithEnv("endpointsPerServer",
                "arrowflight.cluster.endpointsPerServer", "FLIGHT_ENDPOINTS_PER_SERVER",
                AppConfig.DEFAULT_ENDPOINTS_PER_SERVER, props);
        String metadataEndpointLocation = getStringWithEnv("metadataEndpointLocation",
                "arrowflight.metadataEndpointLocation", "FLIGHT_METADATA_ENDPOINT_LOCATION",
                AppConfig.METADATA_LOCATION_NONE, props);

        // DataNode timeouts of the hadoopfs extension; 0 = not set by the server (the extension
        // takes LIBHDFS3_CONF's value, else its own default).
        int duckDbHdfsConnectTimeoutMs = getIntWithEnv("duckDbHdfsConnectTimeoutMs",
                "arrowflight.duckdb.hdfs.connectTimeoutMs", "HDFS_CONNECT_TIMEOUT_MS",
                AppConfig.DEFAULT_HDFS_CONNECT_TIMEOUT_MS, props);
        int duckDbHdfsReadTimeoutMs = getIntWithEnv("duckDbHdfsReadTimeoutMs",
                "arrowflight.duckdb.hdfs.readTimeoutMs", "HDFS_READ_TIMEOUT_MS",
                AppConfig.DEFAULT_HDFS_READ_TIMEOUT_MS, props);

        return new AppConfig(
                numServers, batchSize, ioParallelism, ioFileBufferSize,
                duckDbWarmConnections, duckDbGroups, duckDbThreads,
                duckDbHdfsExtension, duckDbAllowUnsignedExtensions,
                duckDbHdfsDefaultNamenode, duckDbHdfsHaNamenodes,
                duckDbHdfsShortcircuit, duckDbHdfsDomainSocketPath, duckDbMemoryLimit,
                metricsEnabled, arrowAllocatorLimitBytes,
                grpcMaxInboundMessageSize, flightBackpressureThresholdBytes,
                flightListenerReadyTimeoutMillis,
                dataDir, localDataDir, port, hazelcastPort, hazelcastClusterJoinTimeoutSec,
                clientMaxRetries, clientRetryBackoffMs, clientConnectTimeoutMs,
                duckDbStreamResults, duckDbExternalFileCache, duckDbStatementCacheSize,
                duckDbTempDirectory, authToken, resultMode, widenUnsignedTypes, duckDbLockdown,
                endpointBackupLocations, metadataEndpointLocation,
                duckDbHdfsConnectTimeoutMs, duckDbHdfsReadTimeoutMs, duckDbMaxTempSize,
                Boolean.parseBoolean(preserveOrder), duckDbStreamingBufferSize,
                endpointsPerServer, flightOutboundBudgetBytes);
    }

    /**
     * Returns the DuckDB threads per connection used when duckDbThreads is unset or 0: a quarter
     * of the cores, between 2 and 8.
     *
     * @param availableCores processors available to the JVM
     * @return threads per DuckDB connection
     */
    static int defaultDuckDbThreads(int availableCores) {
        return Math.min(8, Math.max(2, availableCores / 4));
    }

    /**
     * Computes I/O thread pool parallelism from config, cores, and constraints.
     *
     * @param props configuration source
     * @return thread count (clamped to 64)
     */
    private static int computeIoParallelism(ConfigSource props) {
        Integer explicit = getOptionalInt("ioParallelism", "arrowflight.io.parallelism", props);
        if (explicit != null && explicit > 0) {
            return Math.min(explicit, 64);
        }
        int availableCores = Runtime.getRuntime().availableProcessors();
        int maxCores = getInt("ioParallelismMaxCores", "arrowflight.io.maxCores", 8, props);
        int effectiveCores = maxCores > 0 ? Math.min(availableCores, maxCores) : availableCores;
        int multiplier = getInt("ioParallelismMultiplier", "arrowflight.io.parallelismMultiplier", 8, props);
        int minThreads = getInt("ioParallelismMinThreads", "arrowflight.io.minParallelism", 32, props);
        return Math.min(64, Math.max(minThreads, effectiveCores * multiplier));
    }

    /**
     * Reads a string config: the key and its alias as system properties, then the key and its
     * alias in the properties file.
     *
     * @param key      primary config key
     * @param sysAlias secondary property key, or null
     * @param fallback default value
     * @param props    configuration source
     * @return resolved value
     */
    private static String getString(String key, String sysAlias, String fallback, ConfigSource props) {
        return props.lookup(names(key, sysAlias), List.of()).orElse(fallback);
    }

    /**
     * Reads a string config: the key and its alias as system properties, then the environment
     * variable, then the key and its alias in the properties file.
     *
     * @param key      primary config key
     * @param sysAlias secondary property key
     * @param envName  environment variable name
     * @param fallback default value
     * @param props    configuration source
     * @return resolved value
     */
    private static String getStringWithEnv(String key, String sysAlias, String envName,
            String fallback, ConfigSource props) {
        return props.lookup(names(key, sysAlias), List.of(envName)).orElse(fallback);
    }

    /**
     * Lists a key and its alias.
     *
     * @param key      primary config key
     * @param sysAlias secondary property key, or null
     * @return names in lookup order
     */
    private static List<String> names(String key, String sysAlias) {
        List<String> names = new ArrayList<>(2);
        names.add(key);
        if (sysAlias != null) {
            names.add(sysAlias);
        }
        return names;
    }

    /**
     * Reads an integer config with fallback.
     *
     * @param key      primary config key
     * @param sysAlias secondary property key
     * @param fallback default value
     * @param props    configuration source
     * @return resolved value
     */
    private static int getInt(String key, String sysAlias, int fallback, ConfigSource props) {
        String raw = getString(key, sysAlias, String.valueOf(fallback), props);
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer for " + key + ": " + raw, e);
        }
    }

    /**
     * Reads an integer config with environment variable.
     *
     * @param key      primary config key
     * @param sysAlias secondary property key
     * @param envName  environment variable name
     * @param fallback default value
     * @param props    configuration source
     * @return resolved value
     */
    private static int getIntWithEnv(String key, String sysAlias, String envName,
            int fallback, ConfigSource props) {
        String raw = getStringWithEnv(key, sysAlias, envName, null, props);
        if (raw == null) {
            return fallback;
        }
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer for " + key + ": " + raw, e);
        }
    }

    /**
     * Reads an optional integer config, returns null if not set.
     *
     * @param key      primary config key
     * @param sysAlias secondary property key
     * @param props    configuration source
     * @return integer value or null
     */
    private static Integer getOptionalInt(String key, String sysAlias, ConfigSource props) {
        Optional<String> value = props.lookup(names(key, sysAlias), List.of());
        if (value.isEmpty()) {
            return null;
        }
        try {
            return Integer.parseInt(value.get());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer for " + key + ": " + value.get(), e);
        }
    }

    /**
     * Reads a boolean config with environment variable.
     *
     * @param key      primary config key
     * @param sysAlias secondary property key
     * @param envName  environment variable name
     * @param fallback default value
     * @param props    configuration source
     * @return resolved value
     */
    private static boolean getBooleanWithEnv(String key, String sysAlias, String envName,
            boolean fallback, ConfigSource props) {
        String value = getStringWithEnv(key, sysAlias, envName, null, props);
        return value == null ? fallback : Boolean.parseBoolean(value);
    }

    /**
     * Reads a long config with fallback.
     *
     * @param key      primary config key
     * @param sysAlias secondary property key
     * @param fallback default value
     * @param props    configuration source
     * @return resolved value
     */
    private static long getLong(String key, String sysAlias, long fallback, ConfigSource props) {
        String raw = getString(key, sysAlias, String.valueOf(fallback), props);
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid long for " + key + ": " + raw, e);
        }
    }
}
