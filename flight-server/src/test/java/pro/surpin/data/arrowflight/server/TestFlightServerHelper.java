package pro.surpin.data.arrowflight.server;

import pro.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.FlightSessions;
import pro.surpin.data.arrowflight.server.adapters.FlightSqlProducer;
import pro.surpin.data.arrowflight.server.adapters.HazelcastAdapter;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.adapters.ResultModeMiddleware;
import pro.surpin.data.arrowflight.server.adapters.ProducingStreamGate;
import pro.surpin.data.arrowflight.server.adapters.StreamCompression;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.services.ClusterService;
import pro.surpin.data.arrowflight.server.services.ExecutionService;
import pro.surpin.data.arrowflight.server.services.MetadataService;
import pro.surpin.data.arrowflight.server.services.ParquetQueryParser;
import pro.surpin.data.arrowflight.server.services.QueryPlanner;
import pro.surpin.data.arrowflight.server.storage.HadoopDataStore;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;

import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Executors;

import static org.apache.arrow.memory.DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME;

public class TestFlightServerHelper implements AutoCloseable {

    public final BufferAllocator allocator;
    public final HazelcastAdapter hazelcastAdapter;
    public final ParquetAdapter parquetAdapter;
    public final DuckDbAdapter duckDbAdapter;
    public final MetadataService metadataService;
    public final ClusterService clusterService;
    public final QueryPlanner queryPlanner;
    public final ExecutionService executionService;
    public final FlightSqlProducer flightSqlProducer;
    public final FlightServer server;
    public final FlightClient flightClient;
    public final FlightSqlClient sqlClient;
    public final Location location;
    /** Registry the server was opened with, or null for the plain local data directory. */
    public final pro.surpin.data.arrowflight.server.storage.StoreRegistry registry;

    private final String[] savedProps;

    private TestFlightServerHelper(Builder b) throws Exception {
        savedProps = saveProps();

        System.setProperty(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
                DefaultAllocationManagerOption.AllocationManagerType.Netty.name());
        allocator = b.allocator != null ? b.allocator : new RootAllocator(Long.MAX_VALUE);

        int hazelcastPort = b.hazelcastPort != 0 ? b.hazelcastPort : findFreePort();

        String defaultDataDir = Paths.get("src/test/resources/test_db").toAbsolutePath().toString();
        String dataDir = b.dataDir != null ? b.dataDir : defaultDataDir;

        System.setProperty("dataDir", dataDir);
        System.setProperty("hazelcastPort", String.valueOf(hazelcastPort));
        System.setProperty("duckDbWarmConnections", "1");
        System.setProperty("duckDbGroups", "1");
        System.setProperty("duckDbThreads", String.valueOf(b.threads));
        System.setProperty("ioParallelism", "2");
        AppConfig appConfig = ConfigAdapter.getConfig();

        if (b.registry != null) {
            // Stores, catalogs and the checked DuckDB setup as the server opens them.
            registry = b.registry.apply(appConfig);
            parquetAdapter = new ParquetAdapter(appConfig, registry.dataDirectory(), "localhost",
                    registry.catalogTables());
            duckDbAdapter = new DuckDbAdapter(appConfig, Executors.newCachedThreadPool(),
                    registry.duckDbSetup());
        } else {
            registry = null;
            // Local filesystem adapter
            LocalFileSystem localFs = new LocalFileSystem();
            localFs.initialize(URI.create("file:///"), new Configuration());

            // Adapters
            pro.surpin.data.arrowflight.server.storage.DataStore store =
                    new HadoopDataStore(localFs, appConfig.dataDir(), "localhost");
            parquetAdapter = new ParquetAdapter(appConfig, b.sharedStorage ? shared(store) : store);
            duckDbAdapter = new DuckDbAdapter(appConfig, Executors.newCachedThreadPool(),
                    pro.surpin.data.arrowflight.server.duckdb.DuckDbSetupPlan.of(List.of(), List.of()),
                    b.threadsUnderLoad, b.threadsWhenIdle, b.cores,
                    new ProducingStreamGate(b.producingStreams),
                    b.zeroCopyWrites, b.sharedDatabase);
        }

        // Services
        metadataService = new MetadataService(parquetAdapter);

        if (b.hazelcastSeeds != null) {
            // A member of a cluster tells the others its Flight URI: the test picks both ports.
            if (b.flightPort == 0) {
                throw new IllegalArgumentException("hazelcastSeeds needs an explicit flightPort");
            }
            location = Location.forGrpcInsecure("localhost", b.flightPort);
            hazelcastAdapter = new HazelcastAdapter(appConfig, location.getUri().toString(),
                    b.hazelcastSeeds);
        } else {
            hazelcastAdapter = new HazelcastAdapter(appConfig);
            // Pick the Flight port only after Hazelcast owns its port. Otherwise two
            // consecutive ephemeral-port probes can return the same released port.
            int flightPort = b.flightPort != 0 ? b.flightPort : findFreePort();
            location = Location.forGrpcInsecure("localhost", flightPort);
        }
        clusterService = new ClusterService(hazelcastAdapter, appConfig,
                location.getUri().toString());
        // endpointsPerServer as the server reads it (tests set it as a system property).
        queryPlanner = new QueryPlanner(parquetAdapter, clusterService,
                QueryPlanner.DEFAULT_BACKUP_LOCATIONS, appConfig.endpointsPerServer());

        executionService = new ExecutionService(parquetAdapter, duckDbAdapter,
                metadataService, appConfig,
                Executors.newCachedThreadPool());

        flightSqlProducer = new FlightSqlProducer(location, allocator,
                metadataService, queryPlanner, executionService, clusterService,
                b.resultMode);

        // As HadoopArrowFlightServer does: the client's x-flight-result-mode header decides.
        server = FlightServer.builder(allocator, location, flightSqlProducer)
                .middleware(ResultModeMiddleware.KEY, new ResultModeMiddleware.Factory())
                .middleware(StreamCompression.KEY, new StreamCompression.Factory(b.compression))
                .middleware(FlightSessions.KEY, new FlightSessions(3600, 1000).factory())
                .build();
        server.start();

        flightClient = FlightClient.builder(allocator, location).build();
        sqlClient = new FlightSqlClient(flightClient);
    }

    public FlightSqlClient sqlClient() {
        return sqlClient;
    }

    public FlightClient flightClient() {
        return flightClient;
    }

    @Override
    public void close() throws Exception {
        Exception first = null;
        try { if (sqlClient != null) sqlClient.close(); }
        catch (Exception e) { if (first == null) first = e; }
        try { if (flightClient != null) flightClient.close(); }
        catch (Exception e) { if (first == null) first = e; }
        try { if (server != null) server.shutdown(); }
        catch (Exception e) { if (first == null) first = e; }
        try { if (flightSqlProducer != null) flightSqlProducer.close(); }
        catch (Exception e) { if (first == null) first = e; }
        try { if (clusterService != null) clusterService.close(); }
        catch (Exception e) { if (first == null) first = e; }
        try { if (hazelcastAdapter != null) hazelcastAdapter.close(); }
        catch (Exception e) { if (first == null) first = e; }
        try { if (duckDbAdapter != null && registry != null) duckDbAdapter.close(); }
        catch (Exception e) { if (first == null) first = e; }
        try { if (registry != null) registry.close(); }
        catch (Exception e) { if (first == null) first = e; }
        try { if (allocator != null) allocator.close(); }
        catch (Exception e) { if (first == null) first = e; }
        restoreProps(savedProps);
        if (first != null) throw first;
    }

    /**
     * Presents a local directory as storage every node reads (as HDFS or S3), for clusters of
     * nodes in one JVM that share it.
     *
     * @param store local store
     * @return the same store, shared
     */
    private static pro.surpin.data.arrowflight.server.storage.DataStore shared(
            pro.surpin.data.arrowflight.server.storage.DataStore store) {
        return (pro.surpin.data.arrowflight.server.storage.DataStore) java.lang.reflect.Proxy.newProxyInstance(
                TestFlightServerHelper.class.getClassLoader(),
                new Class<?>[] {pro.surpin.data.arrowflight.server.storage.DataStore.class},
                (proxy, method, args) -> {
                    if ("shared".equals(method.getName()) && method.getParameterCount() == 0) {
                        return true;
                    }
                    try {
                        return method.invoke(store, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static int findFreePort() throws Exception {
        try (ServerSocket ss = new ServerSocket(0)) {
            return ss.getLocalPort();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private BufferAllocator allocator;
        private String dataDir;
        private String resultMode = "partial";
        private int hazelcastPort;
        private int flightPort;
        private String[] hazelcastSeeds;
        private boolean sharedStorage;
        private int threads = 1;
        private int threadsUnderLoad;
        private int threadsWhenIdle;
        private int cores = Runtime.getRuntime().availableProcessors();
        private int producingStreams;
        private boolean zeroCopyWrites;
        private pro.surpin.data.arrowflight.server.adapters.DuckDbSizing.Shared sharedDatabase;
        private StreamCompression.Settings compression = StreamCompression.Settings.DEFAULT;
        private java.util.function.Function<AppConfig,
                pro.surpin.data.arrowflight.server.storage.StoreRegistry> registry;

        public Builder registry(java.util.function.Function<AppConfig,
                pro.surpin.data.arrowflight.server.storage.StoreRegistry> r) { this.registry = r; return this; }
        public Builder allocator(BufferAllocator a) { this.allocator = a; return this; }
        public Builder dataDir(String d) { this.dataDir = d; return this; }
        public Builder resultMode(String m) { this.resultMode = m; return this; }
        public Builder hazelcastPort(int p) { this.hazelcastPort = p; return this; }
        public Builder flightPort(int p) { this.flightPort = p; return this; }
        /** Joins the Hazelcast members at these host:port seeds; needs hazelcastPort and flightPort. */
        /** Treats the local data directory as storage every node reads (nodes share it). */
        public Builder sharedStorage() { this.sharedStorage = true; return this; }
        public Builder hazelcastSeeds(String... s) { this.hazelcastSeeds = s.clone(); return this; }
        public Builder threads(int t) { this.threads = t; return this; }
        public Builder threadsUnderLoad(int t, int c) { this.threadsUnderLoad = t; this.cores = c; return this; }
        public Builder threadsWhenIdle(int t) { this.threadsWhenIdle = t; return this; }
        public Builder producingStreams(int p) { this.producingStreams = p; return this; }
        public Builder zeroCopyWrites(boolean z) { this.zeroCopyWrites = z; return this; }
        /** Runs every session on one DuckDB of the node (duckDbSharedDatabase) with this sizing. */
        public Builder sharedDatabase(pro.surpin.data.arrowflight.server.adapters.DuckDbSizing.Shared s) {
            this.sharedDatabase = s; return this; }
        public Builder compression(StreamCompression.Settings c) { this.compression = c; return this; }

        public TestFlightServerHelper start() throws Exception {
            return new TestFlightServerHelper(this);
        }
    }

    private static String[] saveProps() {
        return new String[] {
                System.getProperty(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME),
                System.getProperty("dataDir"),
                System.getProperty("hazelcastPort"),
                System.getProperty("duckDbWarmConnections"),
                System.getProperty("duckDbGroups"),
                System.getProperty("duckDbThreads"),
                System.getProperty("ioParallelism")
        };
    }

    private static void restoreProps(String[] saved) {
        String[] keys = {
                ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
                "dataDir", "hazelcastPort",
                "duckDbWarmConnections", "duckDbGroups", "duckDbThreads",
                "ioParallelism"
        };
        for (int i = 0; i < keys.length; i++) {
            if (saved[i] != null) {
                System.setProperty(keys[i], saved[i]);
            } else {
                System.clearProperty(keys[i]);
            }
        }
    }
}
