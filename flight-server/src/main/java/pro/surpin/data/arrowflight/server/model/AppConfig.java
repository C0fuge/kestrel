package pro.surpin.data.arrowflight.server.model;

/**
 * Server configuration loaded at startup.
 * Values are resolved from arrowflight.properties, system properties, and environment variables.
 */
public record AppConfig(
    int numServers,
    int batchSize,
    int ioParallelism,
    int ioFileBufferSize,
    int duckDbWarmConnections,
    int duckDbGroups,
    int duckDbThreads,
    String duckDbHdfsExtension,
    boolean duckDbAllowUnsignedExtensions,
    String duckDbHdfsDefaultNamenode,
    String duckDbHdfsHaNamenodes,
    String duckDbHdfsShortcircuit,
    String duckDbHdfsDomainSocketPath,
    String duckDbMemoryLimit,
    boolean metricsEnabled,
    long arrowAllocatorLimitBytes,
    int grpcMaxInboundMessageSize,
    int flightBackpressureThresholdBytes,
    long flightListenerReadyTimeoutMillis,
    String dataDir,
    String localDataDir,
    int port,
    int hazelcastPort,
    int hazelcastClusterJoinTimeoutSec,
    int clientMaxRetries,
    int clientRetryBackoffMs,
    int clientConnectTimeoutMs,
    boolean duckDbStreamResults,
    boolean duckDbExternalFileCache,
    int duckDbStatementCacheSize,
    String duckDbTempDirectory,
    String authToken,
    String resultMode,
    boolean widenUnsignedTypes,
    boolean duckDbLockdown
) {

    /**
     * Configuration with upstream's fields only; the settings this fork adds take their defaults
     * (streamed results, no external file cache, 64 cached statements, partial results, unsigned
     * widening and the DuckDB lockdown on).
     */
    @SuppressWarnings({"java:S107", "checkstyle:ParameterNumber"})
    public AppConfig(int numServers, int batchSize, int ioParallelism, int ioFileBufferSize,
            int duckDbWarmConnections, int duckDbGroups, int duckDbThreads,
            String duckDbHdfsExtension, boolean duckDbAllowUnsignedExtensions,
            String duckDbHdfsDefaultNamenode, String duckDbHdfsHaNamenodes,
            String duckDbHdfsShortcircuit, String duckDbHdfsDomainSocketPath,
            String duckDbMemoryLimit, boolean metricsEnabled, long arrowAllocatorLimitBytes,
            int grpcMaxInboundMessageSize, int flightBackpressureThresholdBytes,
            long flightListenerReadyTimeoutMillis, String dataDir, String localDataDir, int port,
            int hazelcastPort, int hazelcastClusterJoinTimeoutSec, int clientMaxRetries,
            int clientRetryBackoffMs, int clientConnectTimeoutMs) {
        this(numServers, batchSize, ioParallelism, ioFileBufferSize, duckDbWarmConnections,
                duckDbGroups, duckDbThreads, duckDbHdfsExtension, duckDbAllowUnsignedExtensions,
                duckDbHdfsDefaultNamenode, duckDbHdfsHaNamenodes, duckDbHdfsShortcircuit,
                duckDbHdfsDomainSocketPath, duckDbMemoryLimit, metricsEnabled,
                arrowAllocatorLimitBytes, grpcMaxInboundMessageSize,
                flightBackpressureThresholdBytes, flightListenerReadyTimeoutMillis, dataDir,
                localDataDir, port, hazelcastPort, hazelcastClusterJoinTimeoutSec,
                clientMaxRetries, clientRetryBackoffMs, clientConnectTimeoutMs,
                true, false, 64, null, null, "partial", true, true);
    }
}
