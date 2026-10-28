package pro.surpin.data.arrowflight.server.adapters;

import com.hazelcast.cluster.Member;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Item D3: planning reads through the cluster view wait for a member that stopped answering at
 * most once, take the last values read for its keys and leave it out until it answers.
 */
@Tag("unit")
class ClusterViewTest {

    private static final long WAIT_MILLIS = 200;

    private final Member local = member("grpc+tcp://n1:32010");
    private final Member other = member("grpc+tcp://n2:32010");
    private final Member silent = member("grpc+tcp://n3:32010");
    private final Map<String, Long> values = new HashMap<>();
    private final List<CompletableFuture<Long>> pending = new ArrayList<>();
    private final Map<String, Member> owners = new HashMap<>();
    private boolean silentAnswers = true;
    private IMap<String, Long> map;
    private ClusterView view;

    private static Member member(String uri) {
        Member member = mock(Member.class);
        when(member.getUuid()).thenReturn(UUID.randomUUID());
        when(member.getAttribute(HazelcastAdapter.SERVER_URI_ATTRIBUTE)).thenReturn(uri);
        when(member.toString()).thenReturn(uri);
        return member;
    }

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        PartitionService partitions = mock(PartitionService.class);
        when(partitions.getPartition(anyString())).thenAnswer(call -> {
            String key = call.getArgument(0);
            Partition partition = mock(Partition.class);
            when(partition.getOwner()).thenReturn(owners.getOrDefault(key, ownerByHash(key)));
            return partition;
        });
        map = mock(IMap.class);
        when(map.getName()).thenReturn("server-registry");
        when(map.getAsync(anyString())).thenAnswer(call -> {
            String key = call.getArgument(0);
            Member owner = owners.getOrDefault(key, ownerByHash(key));
            if (owner == silent && !silentAnswers) {
                CompletableFuture<Long> future = new CompletableFuture<>();
                pending.add(future);
                return future;
            }
            return CompletableFuture.completedFuture(values.get(key));
        });
        view = new ClusterView(partitions, local,
                m -> m.getAttribute(HazelcastAdapter.SERVER_URI_ATTRIBUTE), WAIT_MILLIS);
        values.put("a", 1L);
        values.put("b", 2L);
        values.put("c", 3L);
        owners.put("a", local);
        owners.put("b", other);
        owners.put("c", silent);
    }

    /** Spreads probe keys over the three members, as partitions do. */
    private Member ownerByHash(String key) {
        return List.of(local, other, silent).get(Math.floorMod(key.hashCode(), 3));
    }

    private Map<String, Long> read(Set<String> keys) {
        return view.getAll(map, keys, List.of(local, other, silent));
    }

    @Test
    void readsEveryKeyWhileAllMembersAnswer() {
        assertEquals(Map.of("a", 1L, "b", 2L, "c", 3L), read(Set.of("a", "b", "c")));
        assertTrue(view.unresponsiveServers().isEmpty());
    }

    @Test
    void silentMemberHoldsOneReadForTheWaitThenItsKeysTakeTheLastValues() {
        read(Set.of("a", "b", "c"));
        values.put("c", 30L);
        silentAnswers = false;

        long started = System.nanoTime();
        Map<String, Long> first = read(Set.of("a", "b", "c"));
        long firstMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        assertEquals(Map.of("a", 1L, "b", 2L, "c", 3L), first, "c's last value read");
        assertTrue(firstMillis >= WAIT_MILLIS - 20, "waited " + firstMillis + " ms");
        assertTrue(firstMillis < 5 * WAIT_MILLIS, "waited " + firstMillis + " ms");
        assertEquals(Set.of("grpc+tcp://n3:32010"), view.unresponsiveServers());
        assertTrue(view.ownerUnresponsive("c"));
        assertFalse(view.ownerUnresponsive("b"));

        started = System.nanoTime();
        assertEquals(Map.of("a", 1L, "b", 2L, "c", 3L), read(Set.of("a", "b", "c")));
        long laterMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        assertTrue(laterMillis < WAIT_MILLIS / 2, "a later read waited " + laterMillis + " ms");
    }

    @Test
    void keyNeverReadOnASilentMemberIsAbsent() {
        silentAnswers = false;
        assertEquals(Map.of("a", 1L, "b", 2L), read(Set.of("a", "b", "c")));
    }

    @Test
    void probeFindsASilentMemberThatOwnsNoneOfTheKeys() {
        silentAnswers = false;
        assertEquals(Map.of("a", 1L, "b", 2L), read(Set.of("a", "b")));
        assertEquals(Set.of("grpc+tcp://n3:32010"), view.unresponsiveServers());
    }

    @Test
    void memberIsBackOnceAReadOfItsPartitionsAnswers() {
        silentAnswers = false;
        read(Set.of("a", "c"));
        assertEquals(Set.of("grpc+tcp://n3:32010"), view.unresponsiveServers());
        for (CompletableFuture<Long> future : pending) {
            future.complete(null);
        }
        assertTrue(view.unresponsiveServers().isEmpty());
    }

    @Test
    void memberThatLeftIsForgotten() {
        silentAnswers = false;
        read(Set.of("a"));
        assertEquals(Set.of("grpc+tcp://n3:32010"), view.unresponsiveServers());
        owners.put("c", other);
        view.getAll(map, Set.of("a", "b"), List.of(local, other));
        assertTrue(view.unresponsiveServers().isEmpty());
    }
}
