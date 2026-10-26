package pro.surpin.data.arrowflight.server.adapters;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cluster.Member;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryExpiredListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import pro.surpin.data.arrowflight.server.model.AppConfig;

/**
 * Wraps HazelcastInstance lifecycle and distributed map access.
 */
public final class HazelcastAdapter implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastAdapter.class);

    private static final String SERVER_REGISTRY_MAP = "server-registry";
    private static final String STATEMENT_CACHE_MAP = "statement-cache";
    private static final String SERVER_HEARTBEAT_MAP = "server-heartbeats";
    private static final String SERVER_FILES_MAP = "server-files";
    private static final String CLUSTER_SECRETS_MAP = "cluster-secrets";
    /** Member attribute with the Flight URI of the server that runs the member. */
    static final String SERVER_URI_ATTRIBUTE = "arrowflight.uri";
    /**
     * Copies of the small maps every plan depends on (file inventories, the ticket secret): as
     * many as Hazelcast allows, so that they survive the loss of all members but one. With the
     * default single copy, two members lost at once take every entry with them whose primary
     * and backup they held (Hazelcast: "Promotion commit failed"), and the survivor planned
     * with part of the inventories (a partial table) or signed tickets the others refused.
     */
    static final int ALL_MEMBERS_BACKUP_COUNT = MapConfig.MAX_BACKUP_COUNT;

    private final HazelcastInstance instance;
    private final IMap<String, Long> serverRegistry;
    private final IMap<String, Serializable> statementCache;
    private final IMap<String, Long> serverHeartbeats;
    private final IMap<String, Map<String, Long>> serverFiles;
    private final IMap<String, Serializable> clusterSecrets;

    /**
     * Creates a new Hazelcast instance using TCP/IP join on the given hosts.
     *
     * @param appConfig server configuration
     * @param hosts cluster seed hosts
     */
    public HazelcastAdapter(AppConfig appConfig, String... hosts) {
        this(appConfig, null, hosts);
    }

    /**
     * Creates a new Hazelcast instance using TCP/IP join on the given hosts, telling the other
     * members which Flight server it belongs to.
     *
     * @param appConfig server configuration
     * @param serverUri Flight URI of this server, or null when the member does not say
     * @param hosts cluster seed hosts
     */
    public HazelcastAdapter(AppConfig appConfig, String serverUri, String... hosts) {
        Config config = new Config();
        if (serverUri != null) {
            // Cluster membership is the one view of live nodes that a lost map partition cannot
            // falsify: planners leave out every server whose member has left.
            config.getMemberAttributeConfig().setAttribute(SERVER_URI_ATTRIBUTE, serverUri);
        }
        config.addMapConfig(new MapConfig(SERVER_FILES_MAP)
                .setBackupCount(ALL_MEMBERS_BACKUP_COUNT));
        config.addMapConfig(new MapConfig(CLUSTER_SECRETS_MAP)
                .setBackupCount(ALL_MEMBERS_BACKUP_COUNT));
        // Hazelcast otherwise resolves the classes of map values through the thread's context
        // class loader, which may belong to a plugin when a member is created or deserializes.
        config.setClassLoader(HazelcastAdapter.class.getClassLoader());
        // A member whose host vanished (machine down, container gone) sends no reset: by default
        // Hazelcast waits up to 60 s for it, and every map operation on its partitions waits too.
        // Same 10 s as the server's own heartbeat; -D system properties still override.
        setDefault(config, "hazelcast.heartbeat.interval.seconds", "1");
        setDefault(config, "hazelcast.max.no.heartbeat.seconds", "10");
        // Hazelcast's own JVM shutdown hook terminates the member (no goodbye), so a stopped
        // server (SIGTERM, docker stop) looked crashed: map operations on the other nodes waited
        // for it until the heartbeat timeout. The server's shutdown hook leaves the cluster
        // gracefully instead (close), and waits for that at most a few seconds.
        setDefault(config, "hazelcast.shutdownhook.enabled", "false");
        setDefault(config, "hazelcast.graceful.shutdown.max.wait", "5");
        NetworkConfig network = config.getNetworkConfig();
        network.setPort(appConfig.hazelcastPort());
        network.setPortAutoIncrement(false);
        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        for (String host : hosts) {
            tcpIpConfig.addMember(host);
        }
        tcpIpConfig.setEnabled(true);

        this.instance = Hazelcast.newHazelcastInstance(config);
        this.serverRegistry = instance.getMap(SERVER_REGISTRY_MAP);
        this.statementCache = instance.getMap(STATEMENT_CACHE_MAP);
        this.serverHeartbeats = instance.getMap(SERVER_HEARTBEAT_MAP);
        this.serverFiles = instance.getMap(SERVER_FILES_MAP);
        this.clusterSecrets = instance.getMap(CLUSTER_SECRETS_MAP);
    }

    /**
     * Sets a Hazelcast property unless the JVM was started with it.
     *
     * @param config Hazelcast configuration
     * @param name property name
     * @param value default value
     */
    private static void setDefault(Config config, String name, String value) {
        config.setProperty(name, System.getProperty(name, value));
    }

    /**
     * Returns the distributed server registry (URI → load).
     */
    public IMap<String, Long> serverRegistry() {
        return serverRegistry;
    }

    /**
     * Returns the distributed statement cache (handle → state).
     */
    public IMap<String, Serializable> statementCache() {
        return statementCache;
    }

    /**
     * Returns the distributed heartbeat map (URI → timestamp).
     */
    public IMap<String, Long> serverHeartbeats() {
        return serverHeartbeats;
    }

    /**
     * Returns the distributed local-file inventory (server URI → relative path/size).
     */
    public IMap<String, Map<String, Long>> serverFiles() {
        return serverFiles;
    }

    /**
     * Returns the distributed map of cluster-wide secrets (the endpoint ticket key), kept on
     * every member.
     */
    public IMap<String, Serializable> clusterSecrets() {
        return clusterSecrets;
    }

    /**
     * Lists the Flight URIs of the servers whose Hazelcast members are in the cluster now.
     *
     * @return URIs of current members, or null when a member does not tell its URI (a member
     *         created without one, as in some tests), so membership cannot decide
     */
    public Set<String> memberServerUris() {
        Set<String> uris = new LinkedHashSet<>();
        for (Member member : instance.getCluster().getMembers()) {
            String uri = member.getAttribute(SERVER_URI_ATTRIBUTE);
            if (uri == null) {
                return null;
            }
            uris.add(uri);
        }
        return uris;
    }

    /**
     * Registers an entry-expired listener on the statement cache.
     *
     * @param listener the listener to attach
     */
    public void onStatementExpired(EntryExpiredListener<String, Serializable> listener) {
        statementCache.addLocalEntryListener(listener);
    }

    /**
     * Returns the underlying HazelcastInstance for advanced operations.
     *
     * @return Hazelcast instance
     */
    public HazelcastInstance instance() {
        return instance;
    }

    @Override
    public void close() {
        instance.shutdown();
        LOGGER.info("Hazelcast instance shut down");
    }
}
