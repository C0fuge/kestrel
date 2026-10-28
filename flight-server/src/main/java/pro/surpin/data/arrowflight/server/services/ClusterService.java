package pro.surpin.data.arrowflight.server.services;

import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryExpiredListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.HazelcastInstance;

import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.server.adapters.ClusterView;
import pro.surpin.data.arrowflight.server.adapters.HazelcastAdapter;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.model.FileAssignment;
import pro.surpin.data.arrowflight.server.model.HandleState;

/**
 * Manages server registration, heartbeats, load tracking, and statement cache lifecycle.
 * Coordinates with other cluster members via Hazelcast distributed maps.
 */
public final class ClusterService implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterService.class);

    // A node that stops beating is left out of plans after HEARTBEAT_TIMEOUT_SEC; until then
    // clients may be sent to it and get "connection refused".
    private static final long HEARTBEAT_INTERVAL_SEC = 3;
    private static final String PARAMETERS_PREFIX = "parameters:";
    /** Parameter values of a prepared statement are kept this long after they were sent. */
    private static final long PARAMETERS_TTL_MINUTES = 30;
    private static final long HEARTBEAT_TIMEOUT_SEC = 10;
    private static final long HANDLE_TTL_NANOS = TimeUnit.MINUTES.toNanos(10);
    private static final int TICKET_SECRET_BYTES = 32;
    private static final String TICKET_SECRET_KEY = "__arrowflight-ticket-secret-v1";
    private static final String TICKET_LOAD_PREFIX = "__arrowflight-ticket-load-v1:";
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private final HazelcastAdapter hazelcast;
    private final AppConfig appConfig;
    private final String serverUri;
    private final ScheduledExecutorService heartbeatExecutor;
    // Moves load accounting of endpoints run here for another node, off the query's path: while
    // the planned node is still a cluster member that does not answer, map calls wait for it.
    private final ExecutorService loadClaimExecutor;
    private boolean registered;
    private final EndpointTicketCodec endpointTicketCodec;
    private volatile byte[] currentSecret;
    private final InternalCallIdentity internalCallIdentity =
            new InternalCallIdentity(() -> currentSecret);
    private final Map<String, LocalHandle> localHandles = new ConcurrentHashMap<>();
    /** This node's published inventory, published again when the cluster lost it. */
    private volatile Map<String, Long> localInventory;

    /**
     * Associates locally cached state with its expiration deadline.
     *
     * @param state handle state
     * @param expiresAtNanos monotonic expiration deadline
     */
    private record LocalHandle(HandleState state, long expiresAtNanos) {
    }

    /**
     * Creates a ClusterService for the given server URI.
     * Registers the server in the cluster and starts periodic heartbeat.
     *
     * @param hazelcast  Hazelcast adapter
     * @param appConfig  server configuration
     * @param serverUri  this server's URI string
     */
    public ClusterService(HazelcastAdapter hazelcast, AppConfig appConfig, String serverUri) {
        this(hazelcast, appConfig, serverUri, true);
    }

    /**
     * Creates a ClusterService for the given server URI, registering it now or later.
     *
     * @param hazelcast  Hazelcast adapter
     * @param appConfig  server configuration
     * @param serverUri  this server's URI string
     * @param registerNow whether to register at once; otherwise {@link #register()} does it once
     *        the server listens, so that no node sends clients to a port that is not open yet
     */
    public ClusterService(HazelcastAdapter hazelcast, AppConfig appConfig, String serverUri,
            boolean registerNow) {
        this.hazelcast = hazelcast;
        this.appConfig = appConfig;
        this.serverUri = serverUri;
        byte[] secret = ticketSecret(hazelcast);
        this.currentSecret = secret.clone();
        this.endpointTicketCodec = new EndpointTicketCodec(secret);
        pro.surpin.data.arrowflight.server.adapters.QueryParameters.useSigningKey(secret);
        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "flight-heartbeat");
            t.setDaemon(true);
            return t;
        });
        this.loadClaimExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "flight-load-claim");
            t.setDaemon(true);
            return t;
        });
        if (registerNow) {
            register();
        }

        hazelcast.onStatementExpired((EntryExpiredListener<String, Serializable>) event -> {
            Serializable value = event.getOldValue();
            if (value instanceof HandleState state && state.serverUri() != null
                    && state.loadTracked()) {
                hazelcast.serverRegistry().compute(state.serverUri(), (k, v) -> {
                    if (v == null) {
                        return null;
                    }
                    long updated = v - state.bytes();
                    return updated <= 0 ? 0L : updated;
                });
            }
        });
    }

    /**
     * Puts this server in the cluster's pool and starts its heartbeat; later calls do nothing.
     */
    public synchronized void register() {
        if (registered) {
            return;
        }
        registered = true;
        hazelcast.serverRegistry().put(serverUri, 0L);
        hazelcast.serverHeartbeats().put(serverUri, System.currentTimeMillis());
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                hazelcast.serverRegistry().putIfAbsent(serverUri, 0L);
                hazelcast.serverHeartbeats().put(serverUri, System.currentTimeMillis());
                removeExpiredLocalEntries();
            } catch (Exception e) {
                LOGGER.warn("Failed to update heartbeat for {}: {}", serverUri, e.getMessage());
            }
            try {
                refreshView();
            } catch (Exception e) {
                LOGGER.warn("Failed to refresh the cluster view of {}: {}", serverUri, e.getMessage());
            }
            try {
                repairClusterState();
            } catch (Exception e) {
                LOGGER.warn("Failed to check the cluster state of {}: {}", serverUri, e.getMessage());
            }
        }, HEARTBEAT_INTERVAL_SEC, HEARTBEAT_INTERVAL_SEC, TimeUnit.SECONDS);
    }

    /**
     * Filters out servers that haven't sent heartbeat within timeout.
     *
     * @param serverUris set of URIs to check
     * @return set of live server URIs
     */
    public Set<String> filterLiveServers(Set<String> serverUris) {
        long t = LogUtil.mark();
        long now = System.currentTimeMillis();
        long deadline = now - HEARTBEAT_TIMEOUT_SEC * 1000;

        Set<String> members = memberServerUris();
        Map<String, Long> heartbeats = readKeys(hazelcast.serverHeartbeats(), serverUris);
        Set<String> unresponsive = hazelcast.view() == null ? Set.of() : hazelcast.view().unresponsiveServers();
        Set<String> live = new LinkedHashSet<>();
        for (String uri : serverUris) {
            Long lastHb = heartbeats.get(uri);
            if (unresponsive.contains(uri) && !uri.equals(serverUri)) {
                // Its member stopped answering (crashed, host gone): plans would wait for it until
                // Hazelcast drops it. Left out, not removed: it is back once it answers.
                LOGGER.debug("Server {} does not answer, leaving it out of the plan", uri);
            } else if (members != null && !members.contains(uri)) {
                // Its Hazelcast member left: the node is gone, whatever the maps still say (a
                // lost partition can drop its heartbeat entry, which reads as "just registered").
                LOGGER.warn("Server {} is no longer a cluster member, removing from pool", uri);
                hazelcast.serverRegistry().remove(uri);
                hazelcast.serverHeartbeats().remove(uri);
            } else if (lastHb == null) {
                // Registration and the first heartbeat are separate distributed-map writes.
                // Keep a newly registered node eligible during that short window.
                live.add(uri);
            } else if (lastHb >= deadline) {
                live.add(uri);
            } else {
                LOGGER.warn("Server {} is stale (last heartbeat {}ms ago), removing from pool",
                        uri, now - lastHb);
                hazelcast.serverRegistry().remove(uri);
                hazelcast.serverHeartbeats().remove(uri);
            }
        }
        LogUtil.logTiming(t, "hazelcast.filterLiveServers", "total=" + serverUris.size() + " live=" + live.size());
        return live;
    }

    /**
     * Returns all registered server URIs and their current loads.
     *
     * @return map of URI to load (bytes)
     */
    public Map<String, Long> allServerLoads() {
        long t = LogUtil.mark();
        Map<String, Long> result = new LinkedHashMap<>();
        Set<String> members = memberServerUris();
        if (members != null) {
            // Only the servers of current members can be planned. Key lookups, unlike a query of
            // the whole map (entrySet), do not wait ~10 s after a member left while the cluster
            // settles its partitions: every first plan after a node stopped used to.
            Map<String, Long> loads = readKeys(hazelcast.serverRegistry(), members);
            for (String uri : members) {
                Long load = loads.get(uri);
                if (load != null) {
                    result.put(uri, load);
                }
            }
        } else {
            for (Map.Entry<String, Long> entry : hazelcast.serverRegistry().entrySet()) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        LogUtil.logTiming(t, "hazelcast.allServerLoads", "servers=" + result.size());
        return result;
    }

    /**
     * Tells whether this node is the only member of its cluster, from the local member list:
     * then no other node is live, without reading the distributed maps.
     *
     * @return true for a one-node cluster; false when membership cannot be read
     */
    public boolean alone() {
        try {
            return hazelcast.memberCount() == 1;
        } catch (RuntimeException e) {
            LOGGER.warn("Cluster membership unavailable: {}", e.toString());
            return false;
        }
    }

    /**
     * Returns load for a specific set of server URIs.
     *
     * @param serverUris URIs to query
     * @return map of URI to load
     */
    public Map<String, Long> getLoads(Set<String> serverUris) {
        return hazelcast.serverRegistry().getAll(serverUris);
    }

    /**
     * Publishes the files stored on this node. Every path is relative to the node's
     * configured data directory.
     *
     * @param files relative file path to byte size
     */
    public void registerLocalFiles(Map<String, Long> files) {
        localInventory = new LinkedHashMap<>(files);
        hazelcast.serverFiles().put(this.serverUri, new LinkedHashMap<>(files));
        LOGGER.info("Registered {} local Parquet file(s) for {}", files.size(), this.serverUri);
    }

    /**
     * Merges all published node inventories into planner file assignments.
     * Duplicate relative paths are treated as replicas and retain every owner.
     *
     * @return relative path to size and owning servers
     */
    public Map<String, FileAssignment> fileLocations() {
        return fileLocations(false);
    }

    /**
     * Merges published node inventories into planner file assignments, from every node that
     * ever published one or from the current members only. Duplicate relative paths are treated
     * as replicas and retain every owner.
     *
     * @param membersOnly whether to read only the inventories of current members, which a plan
     *        on shared storage needs for locality alone (the store's listing names the files); a
     *        plan on local disks needs every inventory, so that files only a lost node held fail
     *        the plan instead of leaving the answer
     * @return relative path to size and owning servers
     */
    public Map<String, FileAssignment> fileLocations(boolean membersOnly) {
        long t = LogUtil.mark();
        Map<String, Map<String, Long>> inventories = new LinkedHashMap<>();
        Set<String> members = membersOnly ? memberServerUris() : null;
        if (members != null && hazelcast.view() != null) {
            // Plans leave out the servers whose members stopped answering, and their files
            // are in the store's listing.
            members = new LinkedHashSet<>(members);
            members.removeAll(hazelcast.view().unresponsiveServers());
        }
        if (members != null) {
            // Key lookups do not wait for the cluster to settle after a member left, as
            // entrySet does (see allServerLoads).
            Map<String, Map<String, Long>> found = readKeys(hazelcast.serverFiles(), members);
            for (String uri : members) {
                Map<String, Long> inventory = found.get(uri);
                if (inventory == null && uri.equals(serverUri)) {
                    // This node's own entry may live on a member that stopped answering.
                    inventory = localInventory;
                }
                if (inventory != null) {
                    inventories.put(uri, inventory);
                }
            }
        } else {
            for (Map.Entry<String, Map<String, Long>> entry : hazelcast.serverFiles().entrySet()) {
                inventories.put(entry.getKey(), entry.getValue());
            }
        }
        Map<String, FileAssignment> result = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, Long>> server : inventories.entrySet()) {
            String serverAddress = server.getKey();
            for (Map.Entry<String, Long> file : server.getValue().entrySet()) {
                result.compute(file.getKey(), (path, current) -> {
                    Set<String> owners = new LinkedHashSet<>();
                    long size = file.getValue();
                    if (current != null) {
                        if (current.size() != size) {
                            throw new IllegalStateException(
                                    "Conflicting sizes for replicated file " + path);
                        }
                        owners.addAll(current.hosts());
                    }
                    owners.add(serverAddress);
                    return new FileAssignment(size, owners);
                });
            }
        }
        LogUtil.logTiming(t, "hazelcast.fileLocations", "files=" + result.size());
        return result;
    }

    /**
     * Checks whether a server published its local inventory, including an empty one.
     */
    public boolean hasFileInventory(String serverUri) {
        if (hazelcast.view() != null && hazelcast.view().ownerUnresponsive(serverUri)) {
            // The entry lives on a member that stopped answering. Every node publishes its
            // inventory before it registers, and the inventory gives locality only on the
            // shared storage such plans read: waiting for the member would hold the plan.
            return true;
        }
        return hazelcast.serverFiles().containsKey(serverUri);
    }

    /**
     * Reads keys of a cluster map through this node's cluster view: a member that stopped
     * answering holds the read for the view's read wait at most once, and its keys take the
     * last values read.
     *
     * @param map distributed map
     * @param keys keys to read
     * @param <V> value type
     * @return values found, by key
     */
    private <V> Map<String, V> readKeys(IMap<String, V> map, Set<String> keys) {
        ClusterView view = hazelcast.view();
        if (view == null) {
            return map.getAll(keys);
        }
        return view.getAll(map, keys, hazelcast.members());
    }

    /**
     * Reads the loads and heartbeats of the current members at every heartbeat, so that the
     * cluster view knows recent values and finds a member that stopped answering before a plan
     * waits for it.
     */
    void refreshView() {
        Set<String> members = memberServerUris();
        if (hazelcast.view() == null || members == null) {
            return;
        }
        readKeys(hazelcast.serverRegistry(), members);
        readKeys(hazelcast.serverHeartbeats(), members);
    }

    /**
     * Adds delta bytes to server's tracked load.
     *
     * @param uri   server URI
     * @param delta bytes to add (positive or negative)
     */
    public void addLoad(String uri, long delta) {
        long t = LogUtil.mark();
        hazelcast.serverRegistry().compute(uri, (k, v) -> {
            if (v == null) {
                return delta;
            }
            long updated = v + delta;
            return updated <= 0 ? 0L : updated;
        });
        LogUtil.logTiming(t, "hazelcast.addLoad", "uri=" + uri + " delta=" + delta);
    }

    /**
     * Stores a query handle in the distributed cache with 10-minute TTL.
     *
     * @param handle handle string
     * @param state  handle state
     */
    public void storeHandle(String handle, HandleState state) {
        long t = LogUtil.mark();
        localHandles.put(handle, new LocalHandle(
                state, System.nanoTime() + HANDLE_TTL_NANOS));
        hazelcast.statementCache().put(handle, state, 10, TimeUnit.MINUTES);
        LogUtil.logTiming(t, "hazelcast.storeHandle");
    }

    /**
     * Keeps the parameter values a client sent for a prepared statement, for clients that keep
     * using the statement's first handle; every node can read them.
     *
     * @param statementId id in the statement's handle
     * @param parameters one-row Arrow IPC stream with the values
     */
    public void rememberParameters(String statementId, byte[] parameters) {
        hazelcast.statementCache().put(PARAMETERS_PREFIX + statementId, parameters,
                PARAMETERS_TTL_MINUTES, TimeUnit.MINUTES);
    }

    /**
     * Returns the parameter values last sent for a prepared statement.
     *
     * @param statementId id in the statement's handle
     * @return one-row Arrow IPC stream with the values, or null
     */
    public byte[] recallParameters(String statementId) {
        Serializable value = hazelcast.statementCache().get(PARAMETERS_PREFIX + statementId);
        return value instanceof byte[] bytes ? bytes : null;
    }

    /**
     * Drops the parameter values of a closed prepared statement. The client does not wait for
     * it: nothing reads a closed statement's values, and values left behind expire.
     *
     * @param statementId id in the statement's handle
     */
    public void forgetParameters(String statementId) {
        hazelcast.statementCache().deleteAsync(PARAMETERS_PREFIX + statementId);
    }

    /**
     * Retrieves a handle state from the distributed cache.
     *
     * @param handle handle string
     * @return handle state, or null if not found
     */
    public HandleState getHandle(String handle) {
        long t = LogUtil.mark();
        LocalHandle local = localHandles.get(handle);
        if (local != null) {
            if (local.expiresAtNanos() > System.nanoTime()) {
                LogUtil.logTiming(t, "local.getHandle", "found=true");
                return local.state();
            }
            localHandles.remove(handle, local);
        }
        HandleState state = (HandleState) hazelcast.statementCache().get(handle);
        if (state != null) {
            localHandles.put(handle, new LocalHandle(
                    state, System.nanoTime() + HANDLE_TTL_NANOS));
        }
        LogUtil.logTiming(t, "hazelcast.getHandle", "found=" + (state != null));
        return state;
    }

    /**
     * Creates a signed self-contained handle for a server-directed Flight endpoint.
     *
     * @param state endpoint execution state
     * @return authenticated handle bytes
     */
    public byte[] createEndpointHandle(HandleState state) {
        byte[] handle = endpointTicketCodec.encode(state);
        if (state.loadTracked()) {
            hazelcast.statementCache().put(
                    loadReservationKey(handle), state, 10, TimeUnit.MINUTES);
        }
        return handle;
    }

    /**
     * Resolves a signed endpoint handle or a legacy distributed UUID handle.
     *
     * @param handle ticket statement-handle bytes
     * @return endpoint state, or null for an unknown legacy handle
     */
    public HandleState resolveEndpointHandle(byte[] handle) {
        if (endpointTicketCodec.isEncoded(handle)) {
            return endpointTicketCodec.decode(handle);
        }
        return getHandle(new String(handle, StandardCharsets.UTF_8));
    }

    /**
     * Releases distributed load once for a completed signed endpoint ticket, from the node that
     * holds the reservation: the planned node, or the backup node that ran the endpoint instead.
     *
     * @param handle ticket statement-handle bytes
     * @param state completed endpoint state
     */
    public void releaseEndpointLoad(byte[] handle, HandleState state) {
        if (!state.loadTracked() || state.serverUri() == null || state.bytes() == 0L) {
            return;
        }
        Serializable reservation = hazelcast.statementCache()
                .remove(loadReservationKey(handle));
        if (reservation instanceof HandleState reserved) {
            subtractLoad(reserved.serverUri(), reserved.bytes());
        }
    }

    /**
     * Moves the load reservation of an endpoint planned for another node to this node, which
     * runs it instead (the client could not reach the planned node). The planned node's load
     * drops by the endpoint's bytes and this node's load grows by them, so that
     * {@link #releaseEndpointLoad} later releases them here. Does nothing for endpoints planned
     * here, without tracked load, or whose reservation is already gone.
     *
     * @param handle ticket statement-handle bytes
     * @param state endpoint state
     * @return true when the endpoint was planned for another node
     */
    public boolean claimEndpointLoad(byte[] handle, HandleState state) {
        if (!plannedElsewhere(state)) {
            return false;
        }
        if (!state.loadTracked() || state.bytes() == 0L) {
            return true;
        }
        String key = loadReservationKey(handle);
        Serializable reservation = hazelcast.statementCache().remove(key);
        if (reservation instanceof HandleState reserved && !serverUri.equals(reserved.serverUri())) {
            subtractLoad(reserved.serverUri(), reserved.bytes());
            addLoad(serverUri, reserved.bytes());
            hazelcast.statementCache().put(key, new HandleState(reserved.query(),
                    reserved.filePaths(), serverUri, reserved.bytes(), true, false,
                    reserved.parameters()), 10, TimeUnit.MINUTES);
        } else if (reservation != null) {
            hazelcast.statementCache().put(key, reservation, 10, TimeUnit.MINUTES);
        }
        return true;
    }

    /**
     * Tells whether an endpoint was planned for another node: this node runs it as a backup
     * location.
     *
     * @param state endpoint state
     * @return true for a partial-result endpoint whose planned node is not this one
     */
    public boolean plannedElsewhere(HandleState state) {
        return !state.complete() && state.serverUri() != null && !state.serverUri().equals(serverUri);
    }

    /**
     * Runs {@link #claimEndpointLoad} in the background, so that the stream starts at once even
     * while the cluster still waits for the vanished planned node. Wait for the result before
     * {@link #releaseEndpointLoad}, so the release finds the moved reservation.
     *
     * @param handle ticket statement-handle bytes
     * @param state endpoint state
     * @return completion of the claim; it never completes exceptionally
     */
    public CompletableFuture<Void> claimEndpointLoadLater(byte[] handle, HandleState state) {
        byte[] copy = handle.clone();
        return CompletableFuture.runAsync(() -> {
            try {
                claimEndpointLoad(copy, state);
            } catch (RuntimeException e) {
                LOGGER.warn("Could not move the load of an endpoint planned for {}: {}",
                        state.serverUri(), e.toString());
            }
        }, loadClaimExecutor);
    }

    /**
     * Lowers a node's tracked load, never below zero. A node no longer in the registry (stale
     * and removed) is not added back: a registry entry without heartbeats counts as a node that
     * just started, and the planner would send clients to it.
     *
     * @param uri server URI
     * @param bytes bytes to subtract
     */
    private void subtractLoad(String uri, long bytes) {
        long t = LogUtil.mark();
        hazelcast.serverRegistry().compute(uri, (k, v) -> {
            if (v == null) {
                return null;
            }
            long updated = v - bytes;
            return updated <= 0 ? 0L : updated;
        });
        LogUtil.logTiming(t, "hazelcast.subtractLoad", "uri=" + uri + " bytes=" + bytes);
    }

    /**
     * Removes a handle from the distributed cache.
     *
     * @param handle handle string
     */
    public void removeHandle(String handle) {
        localHandles.remove(handle);
        hazelcast.statementCache().remove(handle);
    }

    /**
     * Lists the Flight URIs of the current cluster members.
     *
     * @return URIs, or null when membership cannot tell (a member without its URI)
     */
    private Set<String> memberServerUris() {
        try {
            Set<String> uris = hazelcast.memberServerUris();
            // This node is always a member: no URI at all means none was told.
            return uris == null || uris.isEmpty() ? null : uris;
        } catch (RuntimeException e) {
            LOGGER.warn("Cluster membership unavailable: {}", e.toString());
            return null;
        }
    }

    /**
     * Repairs what the cluster lost of this node's state, at every heartbeat: Hazelcast keeps
     * map entries on a limited number of members, and members lost together take entries
     * with them. Publishes this node's inventory again when it is missing, and makes this node
     * sign tickets with the cluster's secret (putting its own back when the cluster has none),
     * so that every node accepts the tickets of every other.
     */
    void repairClusterState() {
        Map<String, Long> inventory = localInventory;
        if (inventory != null && !hazelcast.serverFiles().containsKey(serverUri)) {
            hazelcast.serverFiles().put(serverUri, new LinkedHashMap<>(inventory));
            LOGGER.warn("The cluster had lost the file inventory of {}; published it again ({} files)",
                    serverUri, inventory.size());
        }
        Serializable shared = hazelcast.clusterSecrets().get(TICKET_SECRET_KEY);
        if (shared == null) {
            byte[] own = currentSecret;
            shared = hazelcast.clusterSecrets().putIfAbsent(TICKET_SECRET_KEY, own);
            if (shared == null) {
                LOGGER.warn("The cluster had lost the endpoint ticket secret; {} put its own back",
                        serverUri);
                return;
            }
        }
        if (shared instanceof byte[] secret && secret.length >= TICKET_SECRET_BYTES
                && endpointTicketCodec.useSecret(secret)) {
            currentSecret = secret.clone();
            pro.surpin.data.arrowflight.server.adapters.QueryParameters.useSigningKey(secret);
            LOGGER.warn("{} signed endpoint tickets with a secret the cluster does not share;"
                    + " now signs with the cluster's (tickets signed before still verify here)",
                    serverUri);
        }
    }

    /**
     * Obtains the cluster-shared secret used to authenticate endpoint tickets.
     *
     * @param hazelcast Hazelcast adapter
     * @return cluster-shared secret bytes
     */
    private static byte[] ticketSecret(HazelcastAdapter hazelcast) {
        byte[] candidate = new byte[TICKET_SECRET_BYTES];
        SECURE_RANDOM.nextBytes(candidate);
        Serializable existing = hazelcast.clusterSecrets()
                .putIfAbsent(TICKET_SECRET_KEY, candidate);
        if (existing == null) {
            return candidate;
        }
        if (existing instanceof byte[] secret && secret.length >= TICKET_SECRET_BYTES) {
            return secret.clone();
        }
        throw new IllegalStateException("Invalid cluster Flight ticket secret");
    }

    /**
     * Creates a compact stable key for a distributed load reservation.
     *
     * @param handle ticket statement-handle bytes
     * @return base64-encoded SHA-256 digest
     */
    private static String loadReservationKey(byte[] handle) {
        try {
            return TICKET_LOAD_PREFIX + Base64.getEncoder().encodeToString(
                    MessageDigest.getInstance("SHA-256").digest(handle));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    /**
     * Removes expired local handle and idempotency entries.
     */
    private void removeExpiredLocalEntries() {
        long now = System.nanoTime();
        localHandles.entrySet().removeIf(
                entry -> entry.getValue().expiresAtNanos() <= now);
    }

    /**
     * Returns this server's URI.
     *
     * @return server URI string
     */
    public String serverUri() {
        return serverUri;
    }

    /**
     * Returns the signer of node-to-node caller identities, keyed by the cluster's current
     * ticket secret.
     *
     * @return identity signer
     */
    public InternalCallIdentity internalCallIdentity() {
        return internalCallIdentity;
    }

    /**
     * Returns the URIs of the live servers of the cluster, this one included.
     *
     * @return live server URIs
     */
    public Set<String> liveServerUris() {
        return filterLiveServers(allServerLoads().keySet());
    }

    /**
     * Returns the underlying Hazelcast instance for cluster management.
     *
     * @return Hazelcast instance
     */
    public HazelcastInstance getHazelcastInstance() {
        return hazelcast.instance();
    }

    @Override
    public void close() {
        try {
            hazelcast.serverRegistry().remove(serverUri);
            hazelcast.serverHeartbeats().remove(serverUri);
        } catch (Exception e) {
            LOGGER.warn("Failed to deregister server from cluster: {}", e.getMessage());
        }
        heartbeatExecutor.shutdownNow();
        loadClaimExecutor.shutdownNow();
        localHandles.clear();
        try {
            hazelcast.close();
        } catch (Exception e) {
            LOGGER.error("Error closing Hazelcast adapter", e);
        }
    }
}
