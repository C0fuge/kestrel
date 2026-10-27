package pro.surpin.data.arrowflight.server;

import com.hazelcast.cluster.MembershipAdapter;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.core.HazelcastInstance;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntSupplier;

import pro.surpin.data.arrowflight.server.adapters.ResultModeMiddleware;
import pro.surpin.data.arrowflight.server.adapters.StreamCompression;
import pro.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import pro.surpin.data.arrowflight.server.adapters.OutboundBudget;
import pro.surpin.data.arrowflight.server.adapters.StackTraceTrimmer;
import pro.surpin.data.arrowflight.server.metrics.MetricsService;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;
import pro.surpin.data.arrowflight.server.storage.DataStore;

/**
 * Main entry point for the Hadoop Arrow Flight SQL server.
 * Uses Dagger for dependency injection and manages the Flight server lifecycle.
 */
public class HadoopArrowFlightServer {

    /** Shortest interval between client keepalive pings the server accepts. */
    static final long KEEPALIVE_PERMIT_SECONDS = 5;
    private static final String DEFAULT_LOG_LEVEL_PROPERTY = "org.slf4j.simpleLogger.defaultLogLevel";

    static {
        initLogLevel();
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopArrowFlightServer.class);

    /**
     * Initializes SLF4J log level from properties, system properties, or env var.
     */
    private static void initLogLevel() {
        Properties props = new Properties();
        try (InputStream input = HadoopArrowFlightServer.class.getClassLoader()
                .getResourceAsStream("arrowflight.properties")) {
            if (input != null) {
                props.load(input);
            }
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
        for (String key : new String[]{"logLevel", "arrowflight.log.level"}) {
            String v = System.getProperty(key);
            if (v != null && !v.isBlank()) {
                System.setProperty(DEFAULT_LOG_LEVEL_PROPERTY, v.trim());
                return;
            }
        }
        String env = System.getenv("LOGGING_LEVEL");
        if (env != null && !env.isBlank()) {
            System.setProperty(DEFAULT_LOG_LEVEL_PROPERTY, env.trim());
            return;
        }
        for (String key : new String[]{"logLevel", "arrowflight.log.level"}) {
            String v = props.getProperty(key);
            if (v != null && !v.isBlank()) {
                System.setProperty(DEFAULT_LOG_LEVEL_PROPERTY, v.trim());
                return;
            }
        }
    }

    private FlightServer server;
    private ServerComponent component;
    private MetricsService metricsService;
    private FlightSecurity flightSecurity;
    private HdfsTicketCache hdfsTicketCache;

    /**
     * Reads the stream compression settings and logs them.
     *
     * @return middleware factory choosing each call's codec
     * @throws IllegalArgumentException for an unknown codec or a value out of range
     */
    private static StreamCompression.Factory streamCompression() {
        StreamCompression.Settings settings = ConfigAdapter.streamCompression();
        LOGGER.info("Flight stream compression: default {} (clients choose with the {} header),"
                + " zstd level {}, batches below {} bytes plain", settings.defaultCodec(),
                StreamCompression.HEADER, settings.zstdLevel(), settings.minBatchBytes());
        return new StreamCompression.Factory(settings);
    }

    /**
     * Starts the server with command-line arguments.
     *
     * @param args CLI arguments including data, Flight, cluster, and metrics settings
     */
    public void start(String... args) {
        AppConfig config = ConfigAdapter.getConfig();
        String dataDirectory = getArgValue(args, "--data-dir", config.dataDir());
        int port = Integer.parseInt(
                getArgValue(args, "--port", String.valueOf(config.port())));
        String hosts = getArgValue(args, "--hosts", "0.0.0.0");
        String localhost = getArgValue(args, "--localhost", "localhost");
        String storageHost = getArgValue(args, "--storage-host", localhost);
        int hazelcastPort = Integer.parseInt(
                getArgValue(args, "--hazelcast-port", String.valueOf(config.hazelcastPort())));
        int metricsPort = Integer.parseInt(getArgValue(args, "--metrics-port", "9404"));
        boolean metricsEnabled = Boolean.parseBoolean(getArgValue(args, "--metrics-enabled",
                String.valueOf(config.metricsEnabled())));
        flightSecurity = FlightSecurity.fromEnvironment();

        // Generate default host list from numServers if --hosts not explicitly set
        String hostsRaw = getArgValue(args, "--hosts", null);
        if (hostsRaw == null && config.numServers() > 1) {
            String[] defaultHosts = new String[config.numServers()];
            for (int i = 0; i < defaultHosts.length; i++) {
                defaultHosts[i] = "flight-server-" + (i + 1);
            }
            hosts = String.join(",", defaultHosts);
            LOGGER.info("Generated --hosts from numServers={}: {}", config.numServers(), hosts);
        }

        // Push CLI overrides into system properties so ConfigAdapter picks them up
        System.setProperty("dataDir", dataDirectory);
        System.setProperty("port", String.valueOf(port));
        System.setProperty("hazelcastPort", String.valueOf(hazelcastPort));

        LOGGER.info("Starting Hadoop Arrow Flight SQL server...");
        LOGGER.info("Data Directory: {}", dataDirectory);
        LOGGER.info("Hosts: {}", hosts);
        LOGGER.info("Local storage host: {}", storageHost);
        LOGGER.info("Port: {}", port);

        Configuration hadoopConfig = new Configuration();
        try {
            flightSecurity.initializeAuthentication(hadoopConfig);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to initialize Flight authentication", e);
        }
        // Before the data store opens: the hadoopfs extension reads only a ticket cache.
        hdfsTicketCache = HdfsTicketCache.start(ConfigSource.system(),
                libhdfs3Properties(System.getenv("LIBHDFS3_CONF"))).orElse(null);
        String[] hazelcastHosts = hosts.split(",");

        // Create Dagger component with runtime parameters
        component = DaggerServerComponent.builder()
                .serverModule(new ServerModule(hazelcastHosts,
                        flightSecurity.location(localhost, port).getUri().toString(),
                        storageHost, hadoopConfig))
                .build();

        // Opens the data directory before joining the cluster: storage this build cannot read
        // fails here, with its reason, instead of after every node has joined.
        DataStore store = component.dataStore();
        LOGGER.info("Storage: kind={} shared={} locality={}",
                store.kind(), store.shared(), store.hasLocality());
        if ("hdfs".equals(store.kind())) {
            LOGGER.info("HDFS DataNode timeouts set in DuckDB: connect={} read={} (0 = left to the"
                    + " hadoopfs extension / LIBHDFS3_CONF)", config.duckDbHdfsConnectTimeoutMs(),
                    config.duckDbHdfsReadTimeoutMs());
            // Only a timeout the server leaves to the extension comes from LIBHDFS3_CONF.
            for (String problem : longHdfsTimeouts(System.getenv("LIBHDFS3_CONF"),
                    config.duckDbHdfsConnectTimeoutMs() <= 0, config.duckDbHdfsReadTimeoutMs() <= 0)) {
                LOGGER.warn("{}: DuckDB reads from a DataNode that vanished wait that long, on every node."
                        + " Set duckDbHdfsConnectTimeoutMs / duckDbHdfsReadTimeoutMs, or a few seconds"
                        + " in the LIBHDFS3_CONF file (e.g. 3000 / 10000).", problem);
            }
            java.util.List<String> rpc = longHdfsRpcTimeouts(System.getenv("LIBHDFS3_CONF"));
            if (!rpc.isEmpty()) {
                LOGGER.warn("{}: a query whose NameNode host vanished (crash, failover) can hang that long."
                        + " Set them in the LIBHDFS3_CONF file, with rpc.client.connect.retry=2.",
                        String.join("; ", rpc));
            }
        }
        String nameEncoding = System.getProperty("sun.jnu.encoding", "");
        if (!"UTF-8".equalsIgnoreCase(nameEncoding)) {
            // Java reads file names in the locale's encoding: other names are skipped silently.
            LOGGER.warn("File names are read as {}, not UTF-8: files whose names are not ASCII are"
                    + " skipped. Start the server with a UTF-8 locale (LANG=C.UTF-8).", nameEncoding);
        }

        // Wait for cluster nodes (single-node skip)
        if (hazelcastHosts.length > 1) {
            waitForCluster(hazelcastHosts, config.hazelcastClusterJoinTimeoutSec());
        }

        OutboundBudget outboundBudget = outboundBudget(config);
        try {
            // Increase HTTP/2 initial flow control window from default 64KB to 1MB.
            // Allows server to send larger Arrow batches without waiting for WINDOW_UPDATE.
            System.setProperty("grpc.netty.server.flowControlWindow", "1048576");

            Location location = flightSecurity.location(localhost, port);
            server = flightSecurity.configure(FlightServer.builder(
                    component.allocator(),
                    location,
                    component.producer())
                    .maxInboundMessageSize(config.grpcMaxInboundMessageSize())
                    .backpressureThreshold(config.flightBackpressureThresholdBytes())
                    .middleware(ResultModeMiddleware.KEY, new ResultModeMiddleware.Factory())
                    .middleware(StreamCompression.KEY, streamCompression())
                    .transportHint("grpc.builderConsumer", grpcOptions(outboundBudget)))
                    .build();
            LOGGER.info("Default result mode: {}; metadata endpoint location: {}; Flight security: {}",
                    config.resultMode(), config.metadataEndpointLocation(), flightSecurity);
            LOGGER.info("Flight outbound budget: {} bytes shared by all streams (per stream at most {},"
                    + " at least {})", outboundBudget.budgetBytes(),
                    config.flightBackpressureThresholdBytes(), OutboundBudget.MIN_STREAM_BYTES);
            server.start();
            // Only now may other nodes send clients here.
            component.clusterService().register();

            LOGGER.info("Server Arrow Flight SQL started on {}", location);
            if (metricsEnabled) {
                metricsService = new MetricsService(metricsPort);
                metricsService.start();
                LOGGER.info("Prometheus metrics started on 0.0.0.0:{}", metricsPort);
            } else {
                LOGGER.info("Prometheus metrics are disabled");
            }

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Shutting down server...");
                stop();
            }));

            server.awaitTermination();
        } catch (Exception e) {
            LOGGER.error("Error starting server", e);
            System.exit(1);
        }
    }

    /**
     * Stops the server and releases resources.
     */
    public void stop() {
        if (metricsService != null) {
            metricsService.close();
            metricsService = null;
            LOGGER.info("Prometheus metrics stopped");
        }
        if (server != null) {
            server.shutdown();
            LOGGER.info("Flight SQL server stopped");
        }
        if (component != null) {
            try {
                component.authorization().close();
            } catch (Exception e) {
                LOGGER.error("Error closing authorization provider", e);
            }
            try {
                component.duckDb().close();
            } catch (Exception e) {
                LOGGER.error("Error closing DuckDB adapter", e);
            }
            try {
                component.storeRegistry().close();
            } catch (Exception e) {
                LOGGER.error("Error closing stores and plugins", e);
            }
            try {
                component.clusterService().close();
            } catch (Exception e) {
                LOGGER.error("Error closing cluster service", e);
            }
            try {
                component.allocator().close();
            } catch (Exception e) {
                LOGGER.error("Error closing allocator", e);
            }
        }
        if (hdfsTicketCache != null) {
            hdfsTicketCache.close();
            hdfsTicketCache = null;
        }
        if (flightSecurity != null) {
            try {
                flightSecurity.close();
            } catch (Exception e) {
                LOGGER.error("Error closing authentication provider", e);
            }
            flightSecurity = null;
        }
    }

    /**
     * Combines the gRPC Netty builder settings of the server: keepalive permit, outbound budget
     * and the removal of stack frames from error texts sent to clients.
     *
     * @param outboundBudget budget shared by all Flight streams
     * @return gRPC Netty server builder customizer
     */
    static java.util.function.Consumer<io.grpc.netty.NettyServerBuilder> grpcOptions(
            OutboundBudget outboundBudget) {
        return keepAlivePermit().andThen(builder -> builder.intercept(outboundBudget)
                .intercept(new StackTraceTrimmer()));
    }

    /**
     * Creates the outbound byte budget all Flight streams of this server share.
     *
     * @param config server configuration
     * @return the configured budget, else one derived from the direct memory limit
     */
    static OutboundBudget outboundBudget(AppConfig config) {
        long configured = config.flightOutboundBudgetBytes();
        return new OutboundBudget(configured > 0 ? configured : OutboundBudget.defaultBudgetBytes());
    }

    /**
     * Lets clients send HTTP/2 keepalive pings as often as every
     * {@value #KEEPALIVE_PERMIT_SECONDS} seconds, also on idle connections. Clients use them to
     * notice a node that vanished without closing its connections (host or address gone) within
     * seconds, and read from a backup location instead; gRPC's default (one ping per 5 minutes)
     * closes the connection of such a client, and every stream on it, after a few pings.
     *
     * @return gRPC Netty server builder customizer
     */
    static java.util.function.Consumer<io.grpc.netty.NettyServerBuilder> keepAlivePermit() {
        return builder -> builder
                .permitKeepAliveTime(KEEPALIVE_PERMIT_SECONDS, TimeUnit.SECONDS)
                .permitKeepAliveWithoutCalls(true);
    }

    /**
     * Waits for all Hazelcast cluster nodes to connect.
     * @param hosts expected host addresses
     * @param timeoutSec max wait time in seconds
     * @throws IllegalStateException if timeout reached or interrupted
     */
    private void waitForCluster(String[] hosts, int timeoutSec) {
        HazelcastInstance hazelcast = component.clusterService().getHazelcastInstance();
        long t = System.nanoTime();
        ReentrantLock membershipLock = new ReentrantLock();
        Condition membershipChanged = membershipLock.newCondition();

        LOGGER.info("Waiting up to {}s for {} nodes to connect (hosts: {})",
                timeoutSec, hosts.length, Arrays.toString(hosts));

        java.util.UUID listenerId = hazelcast.getCluster().addMembershipListener(
                new MembershipAdapter() {
                    @Override
                    public void memberAdded(MembershipEvent e) {
                        membershipLock.lock();
                        try {
                            membershipChanged.signalAll();
                        } finally {
                            membershipLock.unlock();
                        }
                    }
                });

        try {
            boolean joined = awaitMemberCount(
                    () -> hazelcast.getCluster().getMembers().size(),
                    membershipLock, membershipChanged, hosts.length,
                    TimeUnit.SECONDS.toMillis(timeoutSec));
            if (!joined) {
                long elapsed = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - t);
                int connected = hazelcast.getCluster().getMembers().size();
                String msg = String.format(
                        "Cluster join timeout after %ds: only %d of %d nodes connected. "
                                + "Expected hosts: %s.",
                        elapsed, connected, hosts.length, Arrays.toString(hosts));
                LOGGER.error(msg);
                throw new IllegalStateException(msg);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for cluster join", e);
        } finally {
            hazelcast.getCluster().removeMembershipListener(listenerId);
        }

        LogUtil.logTiming(t, "cluster.waitForMembers", "expected=" + hosts.length);
        long totalSec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - t);
        LOGGER.info("All {} nodes connected in {}s. Initializing...", hosts.length, totalSec);
    }

    /**
     * Waits until a changing member count reaches the expected value.
     *
     * @param memberCount current member-count supplier
     * @param lock lock guarding membership-change signals
     * @param membershipChanged condition signalled after membership changes
     * @param expected expected member count
     * @param timeoutMillis maximum wait time in milliseconds
     * @return true when expected count is reached, false on timeout
     * @throws InterruptedException if waiting is interrupted
     * @throws IllegalArgumentException if expected count or timeout is not positive
     */
    static boolean awaitMemberCount(IntSupplier memberCount, ReentrantLock lock,
            Condition membershipChanged, int expected, long timeoutMillis)
            throws InterruptedException {
        if (expected <= 0 || timeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "Expected member count and timeout must be positive");
        }

        long remainingNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        lock.lockInterruptibly();
        try {
            while (memberCount.getAsInt() < expected) {
                if (remainingNanos <= 0) {
                    return false;
                }
                remainingNanos = membershipChanged.awaitNanos(remainingNanos);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Gets the value of a named CLI argument.
     * @param args CLI arguments array
     * @param key argument name
     * @param defaultValue default if not found
     * @return argument value or default
     */
    private String getArgValue(String[] args, String key, String defaultValue) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals(key) && i + 1 < args.length) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }

    /**
     * Main entry point.
     *
     * @param args CLI arguments
     * @throws Exception on startup failure
     */
    public static void main(String... args) throws Exception {
        HadoopArrowFlightServer server = new HadoopArrowFlightServer();
        if (!startOrReport(() -> server.start(args))) {
            System.exit(1);
        }
    }

    /**
     * Runs the server start and logs a failure instead of throwing it. Hazelcast and gRPC threads
     * are not daemons, so a start that fails before it serves (storage that cannot be opened, a
     * cluster join timeout) would otherwise leave a process that never becomes ready.
     *
     * @param start server start
     * @return true when the start returned normally, false when it failed
     */
    static boolean startOrReport(Runnable start) {
        try {
            start.run();
            return true;
        } catch (RuntimeException | Error e) {
            LOGGER.error("The server failed to start", e);
            return false;
        }
    }

    /** Longest libhdfs3 connect / read timeout, in ms, that keeps a vanished DataNode from stalling reads. */
    private static final long HDFS_TIMEOUT_LIMIT_MILLIS = 60_000;

    /**
     * Finds libhdfs3 timeouts that let DuckDB's HDFS reads wait minutes for a DataNode that has gone
     * away: the library defaults to a 600 s connect and a 3600 s read timeout.
     *
     * @param confPath the LIBHDFS3_CONF file, or null
     * @return one line per timeout that is unset or longer than a minute
     */
    static java.util.List<String> longHdfsTimeouts(String confPath) {
        return longHdfsTimeouts(confPath, true, true);
    }

    /**
     * Finds libhdfs3 timeouts that let DuckDB's HDFS reads wait minutes, among those that come
     * from the LIBHDFS3_CONF file: a timeout the server sets in DuckDB wins over the file.
     *
     * @param confPath the LIBHDFS3_CONF file, or null
     * @param connect whether the connect timeout comes from the file
     * @param read whether the read timeout comes from the file
     * @return one line per checked timeout that is unset or longer than a minute
     */
    static java.util.List<String> longHdfsTimeouts(String confPath, boolean connect, boolean read) {
        java.util.List<String> problems = new java.util.ArrayList<>();
        if (!connect && !read) {
            return problems;
        }
        java.util.Map<String, String> props = libhdfs3Properties(confPath);
        java.util.List<String> keys = new java.util.ArrayList<>();
        if (connect) {
            keys.add("input.connect.timeout");
        }
        if (read) {
            keys.add("input.read.timeout");
        }
        for (String key : keys) {
            String value = props.get(key);
            long millis;
            try {
                millis = value == null ? -1 : Long.parseLong(value);
            } catch (NumberFormatException e) {
                millis = -1;
            }
            if (millis <= 0 || millis > HDFS_TIMEOUT_LIMIT_MILLIS) {
                problems.add("libhdfs3 " + key + " is "
                        + (value == null ? "not set (library default: minutes)" : value + " ms"));
            }
        }
        return problems;
    }

    /** libhdfs3 NameNode RPC timeouts and the values tested on the secure HA stand (host crash, failover). */
    private static final java.util.Map<String, Long> HDFS_RPC_TIMEOUTS = java.util.Map.of(
            "rpc.client.connect.timeout", 5_000L,
            "rpc.client.read.timeout", 15_000L,
            "rpc.client.write.timeout", 15_000L,
            "rpc.client.timeout", 20_000L);

    /**
     * Finds libhdfs3 NameNode RPC timeouts that let a query wait minutes when the active NameNode
     * host disappears: the library defaults to a 600 s connect and 3600 s read, write and call
     * timeouts, and the hadoopfs extension has no setting for them.
     *
     * @param confPath the LIBHDFS3_CONF file, or null
     * @return one line per RPC timeout that is unset or longer than a minute, in name order
     */
    static java.util.List<String> longHdfsRpcTimeouts(String confPath) {
        java.util.Map<String, String> props = libhdfs3Properties(confPath);
        java.util.List<String> problems = new java.util.ArrayList<>();
        for (String key : new java.util.TreeSet<>(HDFS_RPC_TIMEOUTS.keySet())) {
            String value = props.get(key);
            long millis;
            try {
                millis = value == null ? -1 : Long.parseLong(value);
            } catch (NumberFormatException e) {
                millis = -1;
            }
            if (millis <= 0 || millis > HDFS_TIMEOUT_LIMIT_MILLIS) {
                problems.add("libhdfs3 " + key + " is "
                        + (value == null ? "not set (library default: minutes)" : value + " ms")
                        + ", e.g. " + HDFS_RPC_TIMEOUTS.get(key));
            }
        }
        return problems;
    }

    /**
     * Reads the name/value properties of a libhdfs3 configuration file.
     *
     * @param confPath the LIBHDFS3_CONF file, or null
     * @return the properties; empty when the path is unset or the file cannot be read
     */
    private static java.util.Map<String, String> libhdfs3Properties(String confPath) {
        java.util.Map<String, String> props = new java.util.HashMap<>();
        if (confPath != null && !confPath.isBlank()) {
            try {
                javax.xml.parsers.DocumentBuilderFactory factory =
                        javax.xml.parsers.DocumentBuilderFactory.newInstance();
                factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
                org.w3c.dom.NodeList list = factory.newDocumentBuilder()
                        .parse(new java.io.File(confPath)).getElementsByTagName("property");
                for (int i = 0; i < list.getLength(); i++) {
                    org.w3c.dom.Element property = (org.w3c.dom.Element) list.item(i);
                    org.w3c.dom.NodeList names = property.getElementsByTagName("name");
                    org.w3c.dom.NodeList values = property.getElementsByTagName("value");
                    if (names.getLength() > 0 && values.getLength() > 0) {
                        props.put(names.item(0).getTextContent().trim(),
                                values.item(0).getTextContent().trim());
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("Cannot read LIBHDFS3_CONF {}: {}", confPath, e.getMessage());
            }
        }
        return props;
    }
}
