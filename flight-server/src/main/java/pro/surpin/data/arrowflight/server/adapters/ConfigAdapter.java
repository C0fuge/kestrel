package pro.surpin.data.arrowflight.server.adapters;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;

/**
 * Loads configuration from system properties, environment variables and arrowflight.properties,
 * in that order of precedence (see {@link ConfigSource}). Produces an immutable AppConfig record.
 */
public class ConfigAdapter {

    /** Schemes of data directories on object stores read over HTTP. */
    private static final Set<String> OBJECT_STORE_SCHEMES = Set.of("s3", "s3a", "s3n", "lakefs");

    /** Fewest default DuckDB threads per connection on such stores (the default before item 19). */
    private static final int REMOTE_MIN_DUCKDB_THREADS = 4;

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
        return getConfig(props, HostLimits.detect());
    }

    /**
     * Resolves the server configuration from a source for a host of the given size.
     *
     * @param props configuration source
     * @param host  cores and memory of the host
     * @return resolved AppConfig
     */
    public static AppConfig getConfig(ConfigSource props, HostLimits host) {
        int ioParallelism = computeIoParallelism(props);
        int numServers = getInt("numServers", "arrowflight.cluster.numServers", 3, props);
        int batchSize = getInt("batchSize", "arrowflight.duckdb.batchSize", 65536, props);
        int ioFileBufferSize = getInt("ioFileBufferSize", null, 131072, props);
        int duckDbWarmConnections = getInt("duckDbWarmConnections",
                "arrowflight.duckdb.warmConnections", Math.min(8, ioParallelism), props);
        int duckDbGroups = getInt("duckDbGroups",
                "arrowflight.duckdb.groups", Math.min(8, ioParallelism), props);
        String dataDir = getString("dataDir", null, "/data/parquet", props);
        DuckDbSizing sizing = duckDbSizing(props, host);
        int duckDbThreads = sizing.threads();
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
        String duckDbMemoryLimit = sizing.memoryLimit();
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
     * Loads the stream compression settings.
     *
     * @return resolved settings
     */
    public static StreamCompression.Settings streamCompression() {
        return streamCompression(ConfigSource.system());
    }

    /**
     * Resolves the stream compression settings: {@code flightCompression} (none, zstd or lz4; codec
     * for calls without the {@code arrow-compression} header), {@code flightCompressionLevel}
     * (Zstandard level), {@code flightCompressionMinBytes} (smaller batches go plain) and
     * {@code flightCompressionThreads} (threads per batch, 0 = one per core up to 8).
     *
     * @param props configuration source
     * @return resolved settings
     * @throws IllegalArgumentException for an unknown codec or a value out of range
     */
    public static StreamCompression.Settings streamCompression(ConfigSource props) {
        StreamCompression.Settings defaults = StreamCompression.Settings.DEFAULT;
        String codec = getString("flightCompression", "arrowflight.flight.compression", "none", props);
        int level = getInt("flightCompressionLevel", "arrowflight.flight.compressionLevel",
                defaults.zstdLevel(), props);
        long minBytes = getLong("flightCompressionMinBytes", "arrowflight.flight.compressionMinBytes",
                defaults.minBatchBytes(), props);
        int threads = getInt("flightCompressionThreads", "arrowflight.flight.compressionThreads",
                defaults.threads(), props);
        return new StreamCompression.Settings(StreamCompression.parse(codec), level, minBytes, threads);
    }

    /**
     * Returns duckDbThreadsUnderLoad from the system configuration.
     *
     * @return DuckDB threads per query while the node is loaded, 0 = off
     */
    public static int duckDbThreadsUnderLoad() {
        return duckDbThreadsUnderLoad(ConfigSource.system());
    }

    /**
     * Resolves {@code duckDbThreadsUnderLoad} ({@code DUCKDB_THREADS_UNDER_LOAD}): DuckDB threads
     * of a second connection per worker that queries use while the running queries times
     * duckDbThreads exceed the cores; default 0 = off.
     *
     * @param props configuration source
     * @return threads, 0 = off
     * @throws IllegalArgumentException for a negative value
     */
    public static int duckDbThreadsUnderLoad(ConfigSource props) {
        int threads = getIntWithEnv("duckDbThreadsUnderLoad", "arrowflight.duckdb.threadsUnderLoad",
                "DUCKDB_THREADS_UNDER_LOAD", 0, props);
        if (threads < 0) {
            throw new IllegalArgumentException("duckDbThreadsUnderLoad must not be negative: " + threads);
        }
        return threads;
    }

    /**
     * Returns duckDbThreadsWhenIdle from the system configuration.
     *
     * @return DuckDB threads of a query that starts alone on the node, 0 = off
     */
    public static int duckDbThreadsWhenIdle() {
        return duckDbThreadsWhenIdle(ConfigSource.system());
    }

    /**
     * Resolves {@code duckDbThreadsWhenIdle} ({@code DUCKDB_THREADS_WHEN_IDLE}): DuckDB threads
     * of a further connection per worker that a query uses when it starts while no other query
     * runs on the node; default 0 = off.
     *
     * @param props configuration source
     * @return threads, 0 = off
     * @throws IllegalArgumentException for a negative value
     */
    public static int duckDbThreadsWhenIdle(ConfigSource props) {
        int threads = getIntWithEnv("duckDbThreadsWhenIdle", "arrowflight.duckdb.threadsWhenIdle",
                "DUCKDB_THREADS_WHEN_IDLE", 0, props);
        if (threads < 0) {
            throw new IllegalArgumentException("duckDbThreadsWhenIdle must not be negative: " + threads);
        }
        return threads;
    }

    /**
     * Returns duckDbProducingStreams from the system configuration.
     *
     * @return streams that may produce batches in DuckDB at once, 0 = no cap
     */
    public static int duckDbProducingStreams() {
        return duckDbProducingStreams(ConfigSource.system());
    }

    /**
     * Resolves {@code duckDbProducingStreams} ({@code DUCKDB_PRODUCING_STREAMS}): how many Flight
     * streams of a node may be inside DuckDB running their query or computing their next batch
     * at once; the others wait their turn. Default 0 = no cap.
     *
     * @param props configuration source
     * @return streams, 0 = no cap
     * @throws IllegalArgumentException for a negative value
     */
    public static int duckDbProducingStreams(ConfigSource props) {
        int streams = getIntWithEnv("duckDbProducingStreams", "arrowflight.duckdb.producingStreams",
                "DUCKDB_PRODUCING_STREAMS", 0, props);
        if (streams < 0) {
            throw new IllegalArgumentException("duckDbProducingStreams must not be negative: " + streams);
        }
        return streams;
    }

    /**
     * Returns distributedComplete from the system configuration.
     *
     * @return whether complete answers may be merged from endpoint answers read from the nodes
     */
    public static boolean distributedComplete() {
        return distributedComplete(ConfigSource.system());
    }

    /**
     * Resolves {@code distributedComplete} ({@code FLIGHT_DISTRIBUTED_COMPLETE}): whether the
     * planning node of a complete answer reads the query's partial-mode endpoints from the
     * cluster's nodes, as the client's user, and merges them, for query shapes whose merge is
     * exact; default false. When on, nodes accept calls signed with the cluster's ticket secret
     * from live members' addresses, so the secret then allows impersonating any user.
     *
     * @param props configuration source
     * @return true when on
     * @throws IllegalArgumentException for a value other than true or false
     */
    public static boolean distributedComplete(ConfigSource props) {
        String value = getStringWithEnv("distributedComplete", "arrowflight.cluster.distributedComplete",
                "FLIGHT_DISTRIBUTED_COMPLETE", "false", props).trim();
        if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
            throw new IllegalArgumentException("distributedComplete must be true or false: " + value);
        }
        return Boolean.parseBoolean(value);
    }

    /**
     * Resolves {@code distributedCompleteTrustedCertificates}
     * ({@code FLIGHT_DISTRIBUTED_COMPLETE_TRUSTED_CERTS}): the PEM file of certificates a node
     * trusts when it reads endpoint answers from its peers over TLS; empty means the node's own
     * certificate file ({@code FLIGHT_TLS_CERT}), which fits nodes sharing one certificate or a
     * chain that includes the issuing CA.
     *
     * @param props configuration source
     * @return path, or null when not set
     */
    public static String distributedCompleteTrustedCertificates(ConfigSource props) {
        String value = getStringWithEnv("distributedCompleteTrustedCertificates",
                "arrowflight.cluster.distributedCompleteTrustedCertificates",
                "FLIGHT_DISTRIBUTED_COMPLETE_TRUSTED_CERTS", "", props).trim();
        return value.isEmpty() ? null : value;
    }

    /**
     * Returns flightLoadReservationSeconds from the system configuration.
     *
     * @return lifetime of an endpoint's node-load reservation in seconds
     */
    public static long flightLoadReservationSeconds() {
        return flightLoadReservationSeconds(ConfigSource.system());
    }

    /**
     * Resolves {@code flightLoadReservationSeconds} ({@code FLIGHT_LOAD_RESERVATION_SECONDS}): how
     * long a planned endpoint's bytes count as load on its node when the client neither reads it
     * to the end nor cancels the plan; default 600.
     *
     * @param props configuration source
     * @return seconds
     * @throws IllegalArgumentException for a value below 1
     */
    public static long flightLoadReservationSeconds(ConfigSource props) {
        int seconds = getIntWithEnv("flightLoadReservationSeconds",
                "arrowflight.cluster.loadReservationSeconds", "FLIGHT_LOAD_RESERVATION_SECONDS",
                600, props);
        if (seconds < 1) {
            throw new IllegalArgumentException(
                    "flightLoadReservationSeconds must be at least 1: " + seconds);
        }
        return seconds;
    }

    /**
     * Returns flightZeroCopyWrites from the system configuration.
     *
     * @return whether DuckDB result streams hand their batches to gRPC without a copy
     */
    public static boolean flightZeroCopyWrites() {
        return flightZeroCopyWrites(ConfigSource.system());
    }

    /**
     * Resolves {@code flightZeroCopyWrites} ({@code FLIGHT_ZERO_COPY_WRITES}): whether streams of
     * DuckDB results let gRPC send the batch buffers without copying them first; default false.
     *
     * @param props configuration source
     * @return true when on
     * @throws IllegalArgumentException for a value other than true or false
     */
    public static boolean flightZeroCopyWrites(ConfigSource props) {
        String value = getStringWithEnv("flightZeroCopyWrites", "arrowflight.flight.zeroCopyWrites",
                "FLIGHT_ZERO_COPY_WRITES", "false", props).trim();
        if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
            throw new IllegalArgumentException("flightZeroCopyWrites must be true or false: " + value);
        }
        return Boolean.parseBoolean(value);
    }

    /**
     * Creates the Flight SQL session store from the system configuration.
     *
     * @return session store
     */
    public static FlightSessions flightSessions() {
        return flightSessions(ConfigSource.system());
    }

    /**
     * Creates the Flight SQL session store: {@code flightSessionIdleSeconds} (default 3600, a
     * session unused this long is dropped) and {@code flightSessionMax} (default 10000 sessions
     * per node, the least recently used dropped first).
     *
     * @param props configuration source
     * @return session store
     */
    public static FlightSessions flightSessions(ConfigSource props) {
        long idle = getLong("flightSessionIdleSeconds", "arrowflight.flight.sessionIdleSeconds", 3600L, props);
        int max = getInt("flightSessionMax", "arrowflight.flight.sessionMax", 10_000, props);
        return new FlightSessions(idle, max);
    }

    /**
     * Resolves the DuckDB threads and memory limit per connection: {@code duckDbThreads} (0 or
     * unset = by cores and memory), {@code duckDbMemoryLimit} (unset = the host's DuckDB budget
     * split between the expected connections, {@code none} = DuckDB's default) and
     * {@code duckDbMemoryConnections} ({@code DUCKDB_MEMORY_CONNECTIONS}, 0 or unset = cores /
     * threads, at least 2).
     *
     * @param props configuration source
     * @param host  cores and memory of the host
     * @return sizing
     * @throws IllegalArgumentException for a negative thread or connection count
     */
    public static DuckDbSizing duckDbSizing(ConfigSource props, HostLimits host) {
        int threads = getInt("duckDbThreads", "arrowflight.duckdb.threads", 0, props);
        if (threads < 0) {
            throw new IllegalArgumentException("duckDbThreads must not be negative: " + threads);
        }
        int connections = getIntWithEnv("duckDbMemoryConnections",
                "arrowflight.duckdb.memoryConnections", "DUCKDB_MEMORY_CONNECTIONS", 0, props);
        if (connections < 0) {
            throw new IllegalArgumentException(
                    "duckDbMemoryConnections must not be negative: " + connections);
        }
        String memoryLimit = getString("duckDbMemoryLimit", "arrowflight.duckdb.memoryLimit", null, props);
        String dataDir = getString("dataDir", null, "/data/parquet", props);
        return DuckDbSizing.compute(host, isObjectStore(dataDir), threads, memoryLimit, connections);
    }

    /**
     * Returns the DuckDB threads per connection the cores alone give: a quarter of the cores,
     * between 4 and 16.
     *
     * @param availableCores processors available to the JVM
     * @return threads per DuckDB connection
     */
    static int defaultDuckDbThreads(int availableCores) {
        return Math.min(DuckDbSizing.MAX_THREADS, Math.max(DuckDbSizing.MIN_THREADS, availableCores / 4));
    }

    /**
     * Returns the default DuckDB threads per connection for a data directory: on object stores
     * read over HTTP (s3, s3a, s3n, lakefs), whose reads wait on round trips, at least 4; elsewhere
     * the size-aware default.
     *
     * @param availableCores processors available to the JVM
     * @param dataDir data directory URI or path
     * @return threads per DuckDB connection
     */
    static int defaultDuckDbThreads(int availableCores, String dataDir) {
        int threads = defaultDuckDbThreads(availableCores);
        return isObjectStore(dataDir) ? Math.max(REMOTE_MIN_DUCKDB_THREADS, threads) : threads;
    }

    /**
     * Tells whether a data directory lives on an object store read over HTTP.
     *
     * @param dataDir data directory URI or path, may be null
     * @return true for s3, s3a, s3n and lakefs URIs
     */
    static boolean isObjectStore(String dataDir) {
        if (dataDir == null) {
            return false;
        }
        int colon = dataDir.indexOf("://");
        if (colon <= 0) {
            return false;
        }
        return OBJECT_STORE_SCHEMES.contains(dataDir.substring(0, colon).toLowerCase(Locale.ROOT));
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
