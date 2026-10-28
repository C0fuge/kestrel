package pro.surpin.data.arrowflight.server;

import dagger.Module;
import dagger.Provides;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.FlightSqlProducer;
import pro.surpin.data.arrowflight.server.adapters.HazelcastAdapter;
import pro.surpin.data.arrowflight.server.adapters.HostLimits;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.adapters.ProducingStreamGate;
import pro.surpin.data.arrowflight.server.catalog.CatalogTables;
import pro.surpin.data.arrowflight.server.duckdb.DuckDbExtensions;
import pro.surpin.data.arrowflight.server.duckdb.DuckDbSetupPlan;
import pro.surpin.data.arrowflight.server.duckdb.DuckDbSetupValidator;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;
import pro.surpin.data.arrowflight.server.services.ClusterService;
import pro.surpin.data.arrowflight.server.services.ExecutionService;
import pro.surpin.data.arrowflight.server.services.MetadataService;
import pro.surpin.data.arrowflight.server.services.QueryPlanner;
import pro.surpin.data.arrowflight.server.storage.DataStore;
import pro.surpin.data.arrowflight.server.storage.StoreRegistry;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;

/**
 * Dagger module that wires all server components as singletons.
 * Runtime parameters (hosts, Hadoop config) are passed via constructor.
 */
@Module
public final class ServerModule {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerModule.class);

    private final String[] hazelcastHosts;
    private final String serverUri;
    private final String localStorageHost;
    private final Configuration hadoopConfig;
    private final Function<String, Set<String>> providerGroups;

    /**
     * Creates module with runtime parameters.
     *
     * @param hazelcastHosts cluster seed hosts
     * @param serverUri      this server's URI
     * @param localStorageHost HDFS DataNode host colocated with this server
     * @param hadoopConfig   Hadoop configuration
     */
    public ServerModule(String[] hazelcastHosts, String serverUri,
            String localStorageHost, Configuration hadoopConfig) {
        this(hazelcastHosts, serverUri, localStorageHost, hadoopConfig, identity -> Set.of());
    }

    /**
     * Creates module with runtime parameters and the authentication provider's groups.
     *
     * @param hazelcastHosts cluster seed hosts
     * @param serverUri      this server's URI
     * @param localStorageHost HDFS DataNode host colocated with this server
     * @param hadoopConfig   Hadoop configuration
     * @param providerGroups groups the authentication provider knows for an identity
     */
    public ServerModule(String[] hazelcastHosts, String serverUri,
            String localStorageHost, Configuration hadoopConfig,
            Function<String, Set<String>> providerGroups) {
        this.hazelcastHosts = hazelcastHosts;
        this.serverUri = serverUri;
        this.localStorageHost = localStorageHost;
        this.hadoopConfig = hadoopConfig;
        this.providerGroups = providerGroups;
    }

    /**
     * Provide AppConfig singleton
     * @return application configuration
     */
    @Provides
    @Singleton
    AppConfig config() {
        System.setProperty(
            org.apache.arrow.memory.DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
            org.apache.arrow.memory.DefaultAllocationManagerOption.AllocationManagerType.Netty.name());
        return ConfigAdapter.getConfig();
    }

    /**
     * Provide HazelcastAdapter singleton
     * @param config application configuration
     * @return Hazelcast adapter
     */
    @Provides
    @Singleton
    HazelcastAdapter hazelcast(AppConfig config) {
        return new HazelcastAdapter(config, serverUri, hazelcastHosts);
    }

    /**
     * Provide ClusterService singleton
     * @param hazelcast Hazelcast adapter
     * @param config application configuration
     * @return cluster service
     */
    @Provides
    @Singleton
    ClusterService cluster(HazelcastAdapter hazelcast, AppConfig config) {
        // Registered by the server once it listens.
        return new ClusterService(hazelcast, config, serverUri, false);
    }

    /**
     * Provide I/O thread pool singleton
     * @param config application configuration
     * @return executor service
     */
    @Provides
    @Singleton
    ExecutorService ioPool(AppConfig config) {
        int parallelism = config.ioParallelism();
        LOGGER.info("node={} pool=create threads={}", LogUtil.node(), parallelism);
        return new java.util.concurrent.AbstractExecutorService() {
            private final ExecutorService delegate = Executors.newFixedThreadPool(parallelism, r -> {
                Thread t = new Thread(r, "parquet-io");
                t.setDaemon(true);
                return t;
            });
            private final java.util.concurrent.atomic.AtomicInteger activeTasks = new java.util.concurrent.atomic.AtomicInteger(0);
            private final java.util.concurrent.atomic.AtomicLong submittedTasks = new java.util.concurrent.atomic.AtomicLong(0);

            @Override
            public void execute(Runnable command) {
                long submitTime = System.nanoTime();
                long taskId = submittedTasks.incrementAndGet();
                int active = activeTasks.incrementAndGet();
                if (active > parallelism) {
                    LOGGER.warn("node={} pool=queueGrowth active={} max={} queued={}",
                            LogUtil.node(), active, parallelism, active - parallelism);
                }
                delegate.execute(() -> {
                    long queueDelay = System.nanoTime() - submitTime;
                    if (queueDelay > 1_000_000_000L) {
                        LOGGER.warn("qid={} node={} pool=starvation taskId={} queueDelay={} active={}",
                                LogUtil.qid(), LogUtil.node(), taskId,
                                LogUtil.elapsedNanos(submitTime), activeTasks.get());
                    }
                    try {
                        command.run();
                    } finally {
                        activeTasks.decrementAndGet();
                    }
                });
            }

            @Override
            public void shutdown() {
                delegate.shutdown();
            }
            @Override
            public List<Runnable> shutdownNow() {
                return delegate.shutdownNow();
            }
            @Override
            public boolean isShutdown() {
                return delegate.isShutdown();
            }
            @Override
            public boolean isTerminated() {
                return delegate.isTerminated();
            }
            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
                return delegate.awaitTermination(timeout, unit);
            }
        };
    }

    /**
     * Provide StoreRegistry singleton: the stores and the plugins they need, chosen by the
     * scheme of the data directory
     * @param config application configuration
     * @return store registry
     */
    @Provides
    @Singleton
    StoreRegistry storeRegistry(AppConfig config) {
        return StoreRegistry.open(config, hadoopConfig, localStorageHost);
    }

    /**
     * Provide DataStore singleton, the store of the data directory
     * @param registry store registry
     * @return store of the data directory
     */
    @Provides
    @Singleton
    DataStore dataStore(StoreRegistry registry) {
        return registry.dataDirectory();
    }

    /**
     * Provide ParquetAdapter singleton
     * @param config application configuration
     * @param store store of the data directory
     * @param catalogTables tables of the other catalogs
     * @return parquet adapter
     */
    @Provides
    @Singleton
    ParquetAdapter parquetAdapter(AppConfig config, DataStore store, CatalogTables catalogTables) {
        return new ParquetAdapter(config, store, localStorageHost, catalogTables);
    }

    /**
     * Provides the tables of the catalogs besides the data directory ({@code catalog=}).
     *
     * @param registry store registry, which opened the catalogs
     * @return catalog tables, empty without catalogs
     */
    @Provides
    @Singleton
    CatalogTables catalogTables(StoreRegistry registry) {
        return registry.catalogTables();
    }

    /**
     * Provide DuckDbAdapter singleton
     * @param config application configuration
     * @param ioPool I/O thread pool
     * @param registry stores, whose checked DuckDB setup every connection runs
     * @return DuckDB adapter
     */
    @Provides
    @Singleton
    DuckDbAdapter duckDb(AppConfig config, ExecutorService ioPool, StoreRegistry registry) {
        LOGGER.info("node={} duckdb=sizing {}", LogUtil.node(),
                ConfigAdapter.duckDbSizing(ConfigSource.system(), HostLimits.detect()).describe());
        return new DuckDbAdapter(config, ioPool, registry.duckDbSetup(),
                ConfigAdapter.duckDbThreadsUnderLoad(), Runtime.getRuntime().availableProcessors(),
                new ProducingStreamGate(ConfigAdapter.duckDbProducingStreams()),
                ConfigAdapter.flightZeroCopyWrites());
    }

    /**
     * Store of the data directory with the pre-plugin signature (not a provider; for tests that
     * build stores and adapters one by one). The store's plugin stays loaded until the JVM ends.
     *
     * @param config application configuration
     * @return store of the data directory
     */
    DataStore dataStore(AppConfig config) {
        return storeRegistry(config).dataDirectory();
    }

    /**
     * DuckDB adapter for one store with the pre-plugin signature (not a provider; for tests): the
     * store's DuckDB setup is checked and written as the server does it.
     *
     * @param config application configuration
     * @param ioPool I/O thread pool
     * @param store store of the data directory
     * @return DuckDB adapter
     */
    DuckDbAdapter duckDb(AppConfig config, ExecutorService ioPool, DataStore store) {
        DuckDbSetupPlan plan = new DuckDbSetupValidator(
                new DuckDbExtensions(ConfigSource.system()), new SecretRedactor())
                .plan(DuckDbSetupValidator.Contribution.ofStore(store.kind(), store),
                        List.of(), List.of());
        return new DuckDbAdapter(config, ioPool, plan);
    }

    /**
     * Provide MetadataService singleton
     * @param parquetAdapter parquet adapter
     * @return metadata service
     */
    @Provides
    @Singleton
    MetadataService metadata(ParquetAdapter parquetAdapter) {
        return new MetadataService(parquetAdapter);
    }

    /**
     * Provide QueryPlanner singleton
     * @param parquetAdapter parquet adapter
     * @param clusterService cluster service
     * @param config application configuration
     * @return query planner
     */
    @Provides
    @Singleton
    QueryPlanner queryPlanner(ParquetAdapter parquetAdapter, ClusterService clusterService,
            AppConfig config) {
        return new QueryPlanner(parquetAdapter, clusterService, config.endpointBackupLocations(),
                config.endpointsPerServer());
    }

    /**
     * Provide BufferAllocator singleton
     * @param config server configuration
     * @return buffer allocator
     */
    @Provides
    @Singleton
    BufferAllocator allocator(AppConfig config) {
        return new RootAllocator(config.arrowAllocatorLimitBytes());
    }

    /**
     * Provide FlightSqlProducer singleton
     * @param allocator buffer allocator
     * @param metadataService metadata service
     * @param queryPlanner query planner
     * @param executionService execution service
     * @param clusterService cluster service
     * @param authorization authorization evaluator
     * @param config application configuration
     * @return Flight SQL producer
     */
    @Provides
    @Singleton
    @SuppressWarnings("java:S107") // Dependencies of the producer, wired in one place.
    FlightSqlProducer producer(BufferAllocator allocator,
            MetadataService metadataService, QueryPlanner queryPlanner,
            ExecutionService executionService, ClusterService clusterService,
            FlightAuthorization authorization, AppConfig config) {
        Location location = new Location(URI.create(serverUri));
        return new FlightSqlProducer(location, allocator, metadataService,
                queryPlanner, executionService, clusterService, authorization,
                config.resultMode(), config.advertiseMetadataLocation());
    }

    /**
     * Creates the optional Ranger authorization evaluator.
     *
     * @return authorization evaluator
     */
    @Provides
    @Singleton
    FlightAuthorization authorization() {
        return SecurityProviders.authorization(System.getenv(), providerGroups);
    }

    /**
     * Producer with upstream's signature (upstream signature, used by its tests); the result mode
     * comes from the loaded configuration.
     *
     * @param allocator buffer allocator
     * @param metadataService metadata service
     * @param queryPlanner query planner
     * @param executionService execution service
     * @param clusterService cluster service
     * @param authorization authorization evaluator
     * @return Flight SQL producer
     */
    FlightSqlProducer producer(BufferAllocator allocator,
            MetadataService metadataService, QueryPlanner queryPlanner,
            ExecutionService executionService, ClusterService clusterService,
            FlightAuthorization authorization) {
        return producer(allocator, metadataService, queryPlanner, executionService,
                clusterService, authorization,
                pro.surpin.data.arrowflight.server.adapters.ConfigAdapter.getConfig());
    }

    /**
     * Provide ExecutionService singleton
     * @param parquetAdapter parquet adapter
     * @param duckDbAdapter DuckDB adapter
     * @param metadataService metadata service
     * @param appConfig application configuration
     * @param ioPool I/O thread pool
     * @return execution service
     */
    @Provides
    @Singleton
    ExecutionService execution(ParquetAdapter parquetAdapter, DuckDbAdapter duckDbAdapter,
            MetadataService metadataService,
            AppConfig appConfig, ExecutorService ioPool) {
        return new ExecutionService(parquetAdapter, duckDbAdapter,
                metadataService, appConfig, ioPool);
    }
}
