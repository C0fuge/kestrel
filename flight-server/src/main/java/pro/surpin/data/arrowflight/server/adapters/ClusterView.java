package pro.surpin.data.arrowflight.server.adapters;

import com.hazelcast.cluster.Member;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * This node's view of the cluster maps the planner reads: key reads that wait a bounded time,
 * the last value read of every key, and the members whose partitions stopped answering.
 *
 * <p>A member that crashed without a goodbye and whose address vanished (host down, container
 * removed) holds every operation on its partitions until Hazelcast drops it
 * (hazelcast.max.no.heartbeat.seconds, 10 s). Reads through this view wait for such a member at
 * most once, for the read wait: then the member is unresponsive, its keys are answered from the
 * last values read, and the planner leaves its server out until any operation on its partitions
 * answers again or the member leaves the cluster.
 */
public final class ClusterView {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterView.class);

    /** Default time a planning read waits for a member before it counts as unresponsive. */
    public static final long DEFAULT_READ_WAIT_MILLIS = 1000;
    private static final String PROBE_PREFIX = "__arrowflight-probe-";
    private static final int MAX_PROBE_CANDIDATES = 100_000;
    /** Stands for a read that did not answer in time. */
    private static final Object NO_ANSWER = new Object();

    private final PartitionService partitions;
    private final Function<Member, String> serverUriOf;
    private final Member localMember;
    private final long readWaitNanos;
    private final Map<String, Map<String, Object>> lastValues = new ConcurrentHashMap<>();
    private final Set<String> unresponsive = ConcurrentHashMap.newKeySet();
    private final Set<UUID> unresponsiveMembers = ConcurrentHashMap.newKeySet();
    private final IntFunction<List<UUID>> replicasOf;
    private final Map<String, String> probeKeys = new ConcurrentHashMap<>();

    /**
     * Creates a view over the partitions of one Hazelcast member.
     *
     * @param partitions partition service of the local member
     * @param localMember the local member, never counted unresponsive
     * @param serverUriOf Flight URI of a member's server, or null when the member does not tell
     * @param readWaitMillis time a read waits for a member before the member counts unresponsive
     */
    public ClusterView(PartitionService partitions, Member localMember,
            Function<Member, String> serverUriOf, long readWaitMillis) {
        this(partitions, localMember, serverUriOf, readWaitMillis, null);
    }

    /**
     * Creates a view over the partitions of one Hazelcast member that also knows the backup
     * replicas of every partition, which a write waits for as it waits for the owner.
     *
     * @param partitions partition service of the local member
     * @param localMember the local member, never counted unresponsive
     * @param serverUriOf Flight URI of a member's server, or null when the member does not tell
     * @param readWaitMillis time a read waits for a member before the member counts unresponsive
     * @param replicasOf members holding a partition, by partition id: owner first, then the
     *        backups in order; null or an empty list when unknown (the owner alone counts)
     */
    public ClusterView(PartitionService partitions, Member localMember,
            Function<Member, String> serverUriOf, long readWaitMillis,
            IntFunction<List<UUID>> replicasOf) {
        this.replicasOf = replicasOf;
        this.partitions = partitions;
        this.localMember = localMember;
        this.serverUriOf = serverUriOf;
        this.readWaitNanos = TimeUnit.MILLISECONDS.toNanos(readWaitMillis);
    }

    /**
     * Reads keys of a map, waiting at most the read wait for members that answer and not at all
     * for unresponsive ones. Keys that did not answer in time take their last value read; their
     * owners become unresponsive. The read also probes one partition of every other member, so a
     * dead member is found even when none of the keys lives on its partitions.
     *
     * @param map distributed map
     * @param keys keys to read
     * @param members current members
     * @param <V> value type
     * @return values found, by key; keys without a value are absent
     */
    public <V> Map<String, V> getAll(IMap<String, V> map, Set<String> keys, Collection<Member> members) {
        forgetMembersThatLeft(members);
        Map<String, Object> last = lastValues.computeIfAbsent(map.getName(), name -> new ConcurrentHashMap<>());
        Map<String, CompletableFuture<V>> reads = new LinkedHashMap<>();
        Map<String, Member> owners = new LinkedHashMap<>();
        for (String key : keys) {
            Member owner = owner(key);
            owners.put(key, owner);
            reads.put(key, read(map, key, owner, last));
        }
        Map<Member, CompletableFuture<V>> probes = new LinkedHashMap<>();
        for (Member member : members) {
            if (!member.equals(localMember)) {
                String probe = probeKey(member);
                if (probe != null) {
                    probes.put(member, read(map, probe, member, null));
                }
            }
        }
        long deadline = System.nanoTime() + readWaitNanos;
        Set<Member> late = new HashSet<>();
        for (Map.Entry<Member, CompletableFuture<V>> probe : probes.entrySet()) {
            if (!await(probe.getValue(), probe.getKey(), deadline)) {
                late.add(probe.getKey());
            }
        }
        Map<String, V> result = new LinkedHashMap<>();
        for (Map.Entry<String, CompletableFuture<V>> read : reads.entrySet()) {
            String key = read.getKey();
            Member owner = owners.get(key);
            Object value = late.contains(owner) ? valueIfDone(read.getValue())
                    : awaitValue(read.getValue(), owner, deadline);
            if (value == NO_ANSWER) {
                late.add(owner);
                value = last.get(key);
            }
            if (value != null) {
                @SuppressWarnings("unchecked")
                V found = (V) value;
                result.put(key, found);
            }
        }
        for (Member member : late) {
            markUnresponsive(member);
        }
        return result;
    }

    /**
     * Tells whether the member that owns a key's partition stopped answering.
     *
     * @param key map key
     * @return true when an operation on the key would wait for an unresponsive member
     */
    public boolean ownerUnresponsive(String key) {
        Member owner = owner(key);
        String uri = owner == null ? null : serverUriOf.apply(owner);
        return uri != null && unresponsive.contains(uri);
    }

    /**
     * Tells whether a write of a key would wait for a member that stopped answering: its owner,
     * or one of the first backups a map with that many synchronous backups waits for.
     *
     * @param key map key
     * @param backupCount synchronous backups of the map
     * @return true when the owner or one of those backups is unresponsive
     */
    public boolean replicaUnresponsive(String key, int backupCount) {
        if (unresponsive.isEmpty() && unresponsiveMembers.isEmpty()) {
            return false;
        }
        if (ownerUnresponsive(key)) {
            return true;
        }
        if (replicasOf == null || backupCount <= 0) {
            return false;
        }
        List<UUID> replicas;
        try {
            Partition partition = partitions.getPartition(key);
            replicas = partition == null ? null : replicasOf.apply(partition.getPartitionId());
        } catch (RuntimeException e) {
            return false;
        }
        if (replicas == null) {
            return false;
        }
        for (int i = 1; i <= backupCount && i < replicas.size(); i++) {
            UUID replica = replicas.get(i);
            if (replica != null && unresponsiveMembers.contains(replica)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the Flight URIs of the servers whose members stopped answering.
     *
     * @return URIs, empty when every member answers
     */
    public Set<String> unresponsiveServers() {
        return Set.copyOf(unresponsive);
    }

    /**
     * Starts a read and remembers its value when it answers; an answer, even late, shows that
     * the owner answers again.
     */
    private <V> CompletableFuture<V> read(IMap<String, V> map, String key, Member owner,
            Map<String, Object> last) {
        CompletableFuture<V> future;
        try {
            future = map.getAsync(key).toCompletableFuture();
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        future.whenComplete((value, error) -> {
            if (error == null) {
                if (last != null) {
                    if (value == null) {
                        last.remove(key);
                    } else {
                        last.put(key, value);
                    }
                }
                markResponsive(owner);
            }
        });
        return future;
    }

    /**
     * Waits for a read until the deadline; an unresponsive owner is not waited for.
     *
     * @return the read's value (null for no value), or NO_ANSWER when it did not answer
     */
    private Object awaitValue(CompletableFuture<?> future, Member owner, long deadline) {
        return await(future, owner, deadline) ? valueIfDone(future) : NO_ANSWER;
    }

    /**
     * Waits for a read until the deadline, not at all for an unresponsive owner.
     *
     * @param future the read
     * @param owner owner of the key's partition, or null when unknown
     * @param deadline System.nanoTime() after which the read counts as late
     * @return true when the read answered
     */
    private boolean await(CompletableFuture<?> future, Member owner, long deadline) {
        if (future.isDone() || owner == null || owner.equals(localMember)) {
            return waitWithoutBound(future);
        }
        if (isUnresponsive(owner)) {
            return false;
        }
        long remaining = deadline - System.nanoTime();
        try {
            future.get(Math.max(remaining, 0L), TimeUnit.NANOSECONDS);
            return true;
        } catch (TimeoutException e) {
            return false;
        } catch (ExecutionException e) {
            throw unwrap(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while reading the cluster maps", e);
        }
    }

    /**
     * Waits for a read that does not depend on another member (done, local, or owner unknown
     * during a migration), as a plain map read would.
     */
    private static boolean waitWithoutBound(CompletableFuture<?> future) {
        try {
            future.get();
            return true;
        } catch (ExecutionException e) {
            throw unwrap(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while reading the cluster maps", e);
        }
    }

    /**
     * Returns the value of a read that answered.
     *
     * @return the value (null for no value), or NO_ANSWER while the read waits
     */
    private static Object valueIfDone(CompletableFuture<?> future) {
        if (!future.isDone()) {
            return NO_ANSWER;
        }
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw unwrap(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while reading the cluster maps", e);
        }
    }

    /**
     * Returns the failure of a read as the exception the map would have thrown.
     *
     * @param e failed read
     * @return the cause, unchecked
     */
    private static RuntimeException unwrap(ExecutionException e) {
        return e.getCause() instanceof RuntimeException runtime ? runtime
                : new IllegalStateException(e.getCause());
    }

    /**
     * Finds the member that owns a key's partition, from the local partition table.
     *
     * @param key map key
     * @return the owner, or null while unknown
     */
    private Member owner(String key) {
        try {
            Partition partition = partitions.getPartition(key);
            return partition == null ? null : partition.getOwner();
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * Finds a key whose partition the member owns, to probe the member with a read.
     *
     * @return the key, or null when the member owns no partition (yet)
     */
    private String probeKey(Member member) {
        String uuid = member.getUuid().toString();
        String known = probeKeys.get(uuid);
        if (known != null && member.equals(owner(known))) {
            return known;
        }
        for (int i = 0; i < MAX_PROBE_CANDIDATES; i++) {
            String candidate = PROBE_PREFIX + i;
            if (member.equals(owner(candidate))) {
                probeKeys.put(uuid, candidate);
                return candidate;
            }
        }
        probeKeys.remove(uuid);
        return null;
    }

    /**
     * Tells whether a member stopped answering.
     *
     * @param member cluster member
     * @return true when it is unresponsive
     */
    private boolean isUnresponsive(Member member) {
        String uri = serverUriOf.apply(member);
        return uri != null && unresponsive.contains(uri);
    }

    /**
     * Records that a member did not answer in time; the local member never counts.
     *
     * @param member cluster member, or null
     */
    private void markUnresponsive(Member member) {
        String uri = member == null || member.equals(localMember) ? null : serverUriOf.apply(member);
        if (uri != null) {
            unresponsiveMembers.add(member.getUuid());
        }
        if (uri != null && unresponsive.add(uri)) {
            LOGGER.warn("Cluster member of {} did not answer within {} ms; plans leave it out"
                    + " until it answers or leaves the cluster", uri,
                    TimeUnit.NANOSECONDS.toMillis(readWaitNanos));
        }
    }

    /**
     * Records that an operation on a member's partitions answered.
     *
     * @param member cluster member, or null
     */
    private void markResponsive(Member member) {
        String uri = member == null ? null : serverUriOf.apply(member);
        if (member != null) {
            unresponsiveMembers.remove(member.getUuid());
        }
        if (uri != null && unresponsive.remove(uri)) {
            LOGGER.warn("Cluster member of {} answers again", uri);
        }
    }

    /**
     * Forgets unresponsive members that left the cluster.
     *
     * @param members current members
     */
    private void forgetMembersThatLeft(Collection<Member> members) {
        Set<String> current = new HashSet<>();
        Set<UUID> currentMembers = new HashSet<>();
        for (Member member : members) {
            currentMembers.add(member.getUuid());
            String uri = serverUriOf.apply(member);
            if (uri != null) {
                current.add(uri);
            }
        }
        unresponsive.retainAll(current);
        unresponsiveMembers.retainAll(currentMembers);
    }
}
