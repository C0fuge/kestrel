package pro.surpin.data.arrowflight.server.model;

import pro.surpin.data.arrowflight.server.storage.S3Settings;

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
    boolean duckDbLockdown,
    S3Settings s3,
    int endpointBackupLocations,
    String metadataEndpointLocation
) {

    /** Backup locations per partial-result endpoint unless configured otherwise. */
    public static final int DEFAULT_ENDPOINT_BACKUP_LOCATIONS = 2;
    /** Metadata endpoints carry no location: clients read them over their own connection. */
    public static final String METADATA_LOCATION_NONE = "none";
    /** Metadata endpoints carry this server's advertised URI (upstream's behaviour). */
    public static final String METADATA_LOCATION_ADVERTISED = "advertised";

    /**
     * Fills in defaults that cannot be null.
     */
    public AppConfig {
        if (s3 == null) {
            s3 = S3Settings.NONE;
        }
        if (endpointBackupLocations < 0) {
            throw new IllegalArgumentException(
                    "endpointBackupLocations must not be negative: " + endpointBackupLocations);
        }
        if (metadataEndpointLocation == null || metadataEndpointLocation.isBlank()) {
            metadataEndpointLocation = METADATA_LOCATION_NONE;
        }
        metadataEndpointLocation = metadataEndpointLocation.trim().toLowerCase(java.util.Locale.ROOT);
        if (!METADATA_LOCATION_NONE.equals(metadataEndpointLocation)
                && !METADATA_LOCATION_ADVERTISED.equals(metadataEndpointLocation)) {
            throw new IllegalArgumentException("metadataEndpointLocation must be "
                    + METADATA_LOCATION_NONE + " or " + METADATA_LOCATION_ADVERTISED + ": "
                    + metadataEndpointLocation);
        }
    }

    /**
     * Configuration without the metadata endpoint location setting (no location).
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
            int clientRetryBackoffMs, int clientConnectTimeoutMs, boolean duckDbStreamResults,
            boolean duckDbExternalFileCache, int duckDbStatementCacheSize,
            String duckDbTempDirectory, String authToken, String resultMode,
            boolean widenUnsignedTypes, boolean duckDbLockdown, S3Settings s3,
            int endpointBackupLocations) {
        this(numServers, batchSize, ioParallelism, ioFileBufferSize, duckDbWarmConnections,
                duckDbGroups, duckDbThreads, duckDbHdfsExtension, duckDbAllowUnsignedExtensions,
                duckDbHdfsDefaultNamenode, duckDbHdfsHaNamenodes, duckDbHdfsShortcircuit,
                duckDbHdfsDomainSocketPath, duckDbMemoryLimit, metricsEnabled,
                arrowAllocatorLimitBytes, grpcMaxInboundMessageSize,
                flightBackpressureThresholdBytes, flightListenerReadyTimeoutMillis, dataDir,
                localDataDir, port, hazelcastPort, hazelcastClusterJoinTimeoutSec,
                clientMaxRetries, clientRetryBackoffMs, clientConnectTimeoutMs,
                duckDbStreamResults, duckDbExternalFileCache, duckDbStatementCacheSize,
                duckDbTempDirectory, authToken, resultMode, widenUnsignedTypes, duckDbLockdown, s3,
                endpointBackupLocations, METADATA_LOCATION_NONE);
    }

    /**
     * Returns this configuration with another metadata endpoint location setting.
     *
     * @param location {@link #METADATA_LOCATION_NONE} or {@link #METADATA_LOCATION_ADVERTISED}
     * @return changed copy
     */
    public AppConfig withMetadataEndpointLocation(String location) {
        return new AppConfig(numServers, batchSize, ioParallelism, ioFileBufferSize,
                duckDbWarmConnections, duckDbGroups, duckDbThreads, duckDbHdfsExtension,
                duckDbAllowUnsignedExtensions, duckDbHdfsDefaultNamenode, duckDbHdfsHaNamenodes,
                duckDbHdfsShortcircuit, duckDbHdfsDomainSocketPath, duckDbMemoryLimit,
                metricsEnabled, arrowAllocatorLimitBytes, grpcMaxInboundMessageSize,
                flightBackpressureThresholdBytes, flightListenerReadyTimeoutMillis, dataDir,
                localDataDir, port, hazelcastPort, hazelcastClusterJoinTimeoutSec,
                clientMaxRetries, clientRetryBackoffMs, clientConnectTimeoutMs,
                duckDbStreamResults, duckDbExternalFileCache, duckDbStatementCacheSize,
                duckDbTempDirectory, authToken, resultMode, widenUnsignedTypes, duckDbLockdown, s3,
                endpointBackupLocations, location);
    }

    /**
     * Tells whether metadata endpoints carry this server's advertised URI.
     *
     * @return true for {@link #METADATA_LOCATION_ADVERTISED}
     */
    public boolean advertiseMetadataLocation() {
        return METADATA_LOCATION_ADVERTISED.equals(metadataEndpointLocation);
    }

    /**
     * Configuration whose data directory is not on S3.
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
            int clientRetryBackoffMs, int clientConnectTimeoutMs, boolean duckDbStreamResults,
            boolean duckDbExternalFileCache, int duckDbStatementCacheSize,
            String duckDbTempDirectory, String authToken, String resultMode,
            boolean widenUnsignedTypes, boolean duckDbLockdown) {
        this(numServers, batchSize, ioParallelism, ioFileBufferSize, duckDbWarmConnections,
                duckDbGroups, duckDbThreads, duckDbHdfsExtension, duckDbAllowUnsignedExtensions,
                duckDbHdfsDefaultNamenode, duckDbHdfsHaNamenodes, duckDbHdfsShortcircuit,
                duckDbHdfsDomainSocketPath, duckDbMemoryLimit, metricsEnabled,
                arrowAllocatorLimitBytes, grpcMaxInboundMessageSize,
                flightBackpressureThresholdBytes, flightListenerReadyTimeoutMillis, dataDir,
                localDataDir, port, hazelcastPort, hazelcastClusterJoinTimeoutSec,
                clientMaxRetries, clientRetryBackoffMs, clientConnectTimeoutMs,
                duckDbStreamResults, duckDbExternalFileCache, duckDbStatementCacheSize,
                duckDbTempDirectory, authToken, resultMode, widenUnsignedTypes, duckDbLockdown,
                S3Settings.NONE, DEFAULT_ENDPOINT_BACKUP_LOCATIONS);
    }

    /**
     * Configuration in the fork's pre-merge shape (no Arrow allocator limit: upstream's 2 GiB).
     */
    @SuppressWarnings({"java:S107", "checkstyle:ParameterNumber"})
    public AppConfig(int numServers, int batchSize, int ioParallelism, int ioFileBufferSize,
            int duckDbWarmConnections, int duckDbGroups, int duckDbThreads,
            String duckDbHdfsExtension, boolean duckDbAllowUnsignedExtensions,
            String duckDbHdfsDefaultNamenode, String duckDbHdfsHaNamenodes,
            String duckDbHdfsShortcircuit, String duckDbHdfsDomainSocketPath,
            String duckDbMemoryLimit, boolean metricsEnabled,
            int grpcMaxInboundMessageSize, int flightBackpressureThresholdBytes,
            long flightListenerReadyTimeoutMillis, String dataDir, String localDataDir, int port,
            int hazelcastPort, int hazelcastClusterJoinTimeoutSec, int clientMaxRetries,
            int clientRetryBackoffMs, int clientConnectTimeoutMs, boolean duckDbStreamResults,
            boolean duckDbExternalFileCache, int duckDbStatementCacheSize,
            String duckDbTempDirectory, String authToken, String resultMode,
            boolean widenUnsignedTypes, boolean duckDbLockdown) {
        this(numServers, batchSize, ioParallelism, ioFileBufferSize, duckDbWarmConnections,
                duckDbGroups, duckDbThreads, duckDbHdfsExtension, duckDbAllowUnsignedExtensions,
                duckDbHdfsDefaultNamenode, duckDbHdfsHaNamenodes, duckDbHdfsShortcircuit,
                duckDbHdfsDomainSocketPath, duckDbMemoryLimit, metricsEnabled,
                2_147_483_648L, grpcMaxInboundMessageSize,
                flightBackpressureThresholdBytes, flightListenerReadyTimeoutMillis, dataDir,
                localDataDir, port, hazelcastPort, hazelcastClusterJoinTimeoutSec,
                clientMaxRetries, clientRetryBackoffMs, clientConnectTimeoutMs,
                duckDbStreamResults, duckDbExternalFileCache, duckDbStatementCacheSize,
                duckDbTempDirectory, authToken, resultMode, widenUnsignedTypes, duckDbLockdown,
                S3Settings.NONE, DEFAULT_ENDPOINT_BACKUP_LOCATIONS);
    }

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
                true, false, 64, null, null, "partial", true, true, S3Settings.NONE, DEFAULT_ENDPOINT_BACKUP_LOCATIONS);
    }

    /**
     * Configuration in the fork's pre-merge shape with S3 settings (pre-merge shape with S3;
     * Arrow allocator limit: upstream's 2 GiB).
     */
    @SuppressWarnings({"java:S107", "checkstyle:ParameterNumber"})
    public AppConfig(int numServers, int batchSize, int ioParallelism, int ioFileBufferSize,
            int duckDbWarmConnections, int duckDbGroups, int duckDbThreads,
            String duckDbHdfsExtension, boolean duckDbAllowUnsignedExtensions,
            String duckDbHdfsDefaultNamenode, String duckDbHdfsHaNamenodes,
            String duckDbHdfsShortcircuit, String duckDbHdfsDomainSocketPath,
            String duckDbMemoryLimit, boolean metricsEnabled,
            int grpcMaxInboundMessageSize, int flightBackpressureThresholdBytes,
            long flightListenerReadyTimeoutMillis, String dataDir, String localDataDir, int port,
            int hazelcastPort, int hazelcastClusterJoinTimeoutSec, int clientMaxRetries,
            int clientRetryBackoffMs, int clientConnectTimeoutMs, boolean duckDbStreamResults,
            boolean duckDbExternalFileCache, int duckDbStatementCacheSize,
            String duckDbTempDirectory, String authToken, String resultMode,
            boolean widenUnsignedTypes, boolean duckDbLockdown, S3Settings s3) {
        this(numServers, batchSize, ioParallelism, ioFileBufferSize, duckDbWarmConnections,
                duckDbGroups, duckDbThreads, duckDbHdfsExtension, duckDbAllowUnsignedExtensions,
                duckDbHdfsDefaultNamenode, duckDbHdfsHaNamenodes, duckDbHdfsShortcircuit,
                duckDbHdfsDomainSocketPath, duckDbMemoryLimit, metricsEnabled,
                2_147_483_648L, grpcMaxInboundMessageSize,
                flightBackpressureThresholdBytes, flightListenerReadyTimeoutMillis, dataDir,
                localDataDir, port, hazelcastPort, hazelcastClusterJoinTimeoutSec,
                clientMaxRetries, clientRetryBackoffMs, clientConnectTimeoutMs,
                duckDbStreamResults, duckDbExternalFileCache, duckDbStatementCacheSize,
                duckDbTempDirectory, authToken, resultMode, widenUnsignedTypes, duckDbLockdown, s3,
                DEFAULT_ENDPOINT_BACKUP_LOCATIONS);
    }

    /**
     * Configuration without the endpoint backup setting (default backups).
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
            int clientRetryBackoffMs, int clientConnectTimeoutMs, boolean duckDbStreamResults,
            boolean duckDbExternalFileCache, int duckDbStatementCacheSize,
            String duckDbTempDirectory, String authToken, String resultMode,
            boolean widenUnsignedTypes, boolean duckDbLockdown, S3Settings s3) {
        this(numServers, batchSize, ioParallelism, ioFileBufferSize, duckDbWarmConnections,
                duckDbGroups, duckDbThreads, duckDbHdfsExtension, duckDbAllowUnsignedExtensions,
                duckDbHdfsDefaultNamenode, duckDbHdfsHaNamenodes, duckDbHdfsShortcircuit,
                duckDbHdfsDomainSocketPath, duckDbMemoryLimit, metricsEnabled,
                arrowAllocatorLimitBytes, grpcMaxInboundMessageSize,
                flightBackpressureThresholdBytes, flightListenerReadyTimeoutMillis, dataDir,
                localDataDir, port, hazelcastPort, hazelcastClusterJoinTimeoutSec,
                clientMaxRetries, clientRetryBackoffMs, clientConnectTimeoutMs,
                duckDbStreamResults, duckDbExternalFileCache, duckDbStatementCacheSize,
                duckDbTempDirectory, authToken, resultMode, widenUnsignedTypes, duckDbLockdown, s3,
                DEFAULT_ENDPOINT_BACKUP_LOCATIONS);
    }
}
