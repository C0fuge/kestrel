package pro.surpin.data.arrowflight.server.services;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import pro.surpin.data.arrowflight.server.adapters.HazelcastAdapter;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.model.FileAssignment;
import pro.surpin.data.arrowflight.server.model.HandleState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ClusterServiceTest {

    @Mock
    private HazelcastAdapter hazelcast;

    @Mock
    private IMap<String, Long> serverRegistry;

    @Mock
    private IMap<String, Long> serverHeartbeats;

    @Mock
    private IMap<String, Serializable> statementCache;

    @Mock
    private IMap<String, Map<String, Long>> serverFiles;

    private static final AppConfig APP_CONFIG = new AppConfig(
            3, 4096, 1, 131072, 1, 1, 1,
            null, false, null, null,
            "true", "/var/lib/hadoop-hdfs/socket/dn_socket", null,
            false, 2_147_483_648L, 1048576, 67108864, 60000L, "/data/parquet", null, 32010, 5701, 60,
            3, 1000, 30000, true, false, 64, null, null, "complete", true, true);

    /**
     * Creates a ClusterService with the required constructor mocks.
     */
    private ClusterService createService(String serverUri) {
        when(hazelcast.serverRegistry()).thenReturn(serverRegistry);
        when(hazelcast.serverHeartbeats()).thenReturn(serverHeartbeats);
        when(hazelcast.statementCache()).thenReturn(statementCache);
        when(hazelcast.serverFiles()).thenReturn(serverFiles);
        when(hazelcast.clusterSecrets()).thenReturn(statementCache);
        return new ClusterService(hazelcast, APP_CONFIG, serverUri);
    }

    /** Verifies a node whose Hazelcast member left is not live, even without a heartbeat entry. */
    @Test
    void filterLiveServersDropsServerWhoseMemberLeft() {
        String s1 = "grpc://n1:32010";
        String s2 = "grpc://n2:32010";
        when(hazelcast.memberServerUris()).thenReturn(Set.of(s1));
        // s2's heartbeat entry was lost with a partition: alone it reads as "just registered".
        when(serverHeartbeats.getAll(Set.of(s1, s2))).thenReturn(Map.of(s1, System.currentTimeMillis()));

        ClusterService cs = createService(s1);
        assertEquals(Set.of(s1), cs.filterLiveServers(Set.of(s1, s2)));
        verify(serverRegistry).remove(s2);
        verify(serverHeartbeats).remove(s2);
        cs.close();
    }

    /** Verifies membership does not decide when a member does not tell its URI. */
    @Test
    void filterLiveServersKeepsHeartbeatRuleWithoutMemberUris() {
        String s1 = "grpc://n1:32010";
        when(hazelcast.memberServerUris()).thenReturn(null);
        when(serverHeartbeats.getAll(Set.of(s1))).thenReturn(Map.of());

        ClusterService cs = createService(s1);
        assertEquals(Set.of(s1), cs.filterLiveServers(Set.of(s1)));
        cs.close();
    }

    /** Verifies a node is alone only when it is its cluster's single member. */
    @Test
    void aloneOnlyWithOneMember() {
        ClusterService cs = createService("grpc://n1:32010");
        when(hazelcast.memberCount()).thenReturn(1);
        assertTrue(cs.alone());
        when(hazelcast.memberCount()).thenReturn(2);
        assertFalse(cs.alone());
        when(hazelcast.memberCount()).thenThrow(new IllegalStateException("not active"));
        assertFalse(cs.alone());
        cs.close();
    }

    /** Verifies a node publishes its inventory again when the cluster lost it. */
    @Test
    void repairClusterStatePublishesLostInventoryAgain() {
        ClusterService cs = createService("s1");
        cs.registerLocalFiles(Map.of("s/t/a.parquet", 10L));
        when(serverFiles.containsKey("s1")).thenReturn(false);

        cs.repairClusterState();

        verify(serverFiles, times(2)).put(eq("s1"), eq(Map.of("s/t/a.parquet", 10L)));
        cs.close();
    }

    /** Verifies a node switches to the cluster's ticket secret and still accepts its old tickets. */
    @Test
    void repairClusterStateAdoptsTheClusterSecret() {
        AtomicReference<Serializable> stored = new AtomicReference<>();
        when(statementCache.putIfAbsent(anyString(), any())).thenAnswer(invocation -> {
            stored.compareAndSet(null, invocation.getArgument(1));
            return stored.get() == invocation.getArgument(1) ? null : stored.get();
        });
        ClusterService survivor = createService("s1");
        HandleState state = HandleState.forServerFiles(
                "SELECT * FROM s.t", new String[]{"s/t/f.parquet"}, "s1", 1L, false);
        byte[] old = survivor.createEndpointHandle(state);
        // The entry was lost and a restarted node put a secret of its own.
        stored.set(null);
        ClusterService restarted = createService("s2");
        when(statementCache.get(anyString())).thenAnswer(invocation -> stored.get());

        survivor.repairClusterState();

        assertEquals(state, restarted.resolveEndpointHandle(survivor.createEndpointHandle(state)));
        assertEquals(state, survivor.resolveEndpointHandle(restarted.createEndpointHandle(state)));
        assertEquals(state, survivor.resolveEndpointHandle(old));
        survivor.close();
        restarted.close();
    }

    // ── filterLiveServers ─────────────────────────────────────────────────

    @Test
    void filterLiveServersKeepsLiveServer() {
        String s1 = "grpc://127.0.0.1:32010";
        long recentHb = System.currentTimeMillis() - 1_000;
        when(serverHeartbeats.getAll(Set.of(s1))).thenReturn(Map.of(s1, recentHb));

        ClusterService cs = createService(s1);
        Set<String> live = cs.filterLiveServers(Set.of(s1));
        assertEquals(Set.of(s1), live);
        cs.close();
    }

    @Test
    void filterLiveServersRemovesExpiredServer() {
        String s1 = "grpc://127.0.0.1:32010";
        long expiredHb = System.currentTimeMillis() - 120_000;
        when(serverHeartbeats.getAll(Set.of(s1))).thenReturn(Map.of(s1, expiredHb));

        ClusterService cs = createService(s1);
        Set<String> live = cs.filterLiveServers(Set.of(s1));
        assertTrue(live.isEmpty(), "Expired server must be removed");
        cs.close();
    }

    @Test
    void filterLiveServersKeepsNewlyRegisteredNoHeartbeat() {
        String s1 = "grpc://127.0.0.1:32010";
        when(serverHeartbeats.getAll(Set.of(s1))).thenReturn(Map.of());

        ClusterService cs = createService(s1);
        Set<String> live = cs.filterLiveServers(Set.of(s1));
        assertEquals(Set.of(s1), live,
                "Newly registered server without heartbeat must be kept");
        cs.close();
    }

    // ── allServerLoads / getLoads ─────────────────────────────────────────

    @Test
    void allServerLoadsReturnsRegistryContents() {
        when(serverRegistry.entrySet())
                .thenReturn(Set.of(Map.entry("s1", 100L), Map.entry("s2", 200L)));

        ClusterService cs = createService("s1");
        Map<String, Long> loads = cs.allServerLoads();
        assertEquals(2, loads.size());
        assertEquals(100L, loads.get("s1"));
        cs.close();
    }

    /**
     * Verifies loads of current members are read by key, not by a query of the whole map, which
     * waits ~10 s after a member left.
     */
    @Test
    void allServerLoadsReadsTheLoadsOfCurrentMembersByKey() {
        when(hazelcast.memberServerUris()).thenReturn(Set.of("s1", "s2"));
        when(serverRegistry.getAll(Set.of("s1", "s2"))).thenReturn(Map.of("s1", 100L));

        ClusterService cs = createService("s1");
        assertEquals(Map.of("s1", 100L), cs.allServerLoads(),
                "a member that has not registered yet is not in the pool");
        verify(serverRegistry, never()).entrySet();
        cs.close();
    }

    // ── addLoad ───────────────────────────────────────────────────────────

    @Test
    void addLoadDelegatesToCompute() {
        ClusterService cs = createService("s1");
        cs.addLoad("s2", 50);
        verify(serverRegistry).compute(eq("s2"), any());
        cs.close();
    }

    // ── handle lifecycle ──────────────────────────────────────────────────

    @Test
    void storeAndGetHandle() {
        HandleState state = HandleState.forQuery("SELECT * FROM t");

        ClusterService cs = createService("s1");
        cs.storeHandle("h1", state);

        HandleState retrieved = cs.getHandle("h1");
        assertEquals("SELECT * FROM t", retrieved.query());
        cs.close();
    }

    /** Verifies endpoint state round-trips without a distributed map lookup. */
    @Test
    void signedEndpointHandleRoundTripsLocally() {
        HandleState state = HandleState.forServerFiles(
                "SELECT * FROM s.t", new String[]{"s/t/f.parquet"},
                "s1", 100L, false);
        ClusterService cs = createService("s1");

        byte[] handle = cs.createEndpointHandle(state);
        HandleState decoded = cs.resolveEndpointHandle(handle);

        assertEquals(state.query(), decoded.query());
        assertArrayEquals(state.filePaths(), decoded.filePaths());
        assertEquals(state.bytes(), decoded.bytes());
        verify(statementCache, never()).get(anyString());
        cs.close();
    }

    /** Verifies modified self-contained tickets fail authentication. */
    @Test
    void signedEndpointHandleRejectsTampering() {
        HandleState state = HandleState.forServerFiles(
                "SELECT * FROM s.t", new String[]{"s/t/f.parquet"},
                "s1", 100L, false);
        ClusterService cs = createService("s1");
        byte[] handle = cs.createEndpointHandle(state);
        handle[handle.length - 1] ^= 1;

        assertThrows(IllegalArgumentException.class,
                () -> cs.resolveEndpointHandle(handle));
        cs.close();
    }

    /** Verifies every node uses the same Hazelcast-backed ticket signing secret. */
    @Test
    void signedEndpointHandleRoundTripsAcrossClusterServices() {
        AtomicReference<Serializable> sharedSecret = new AtomicReference<>();
        when(statementCache.putIfAbsent(anyString(), any())).thenAnswer(invocation -> {
            Serializable candidate = invocation.getArgument(1);
            Serializable existing = sharedSecret.get();
            if (existing == null && sharedSecret.compareAndSet(null, candidate)) {
                return null;
            }
            return sharedSecret.get();
        });
        ClusterService first = createService("s1");
        ClusterService second = createService("s2");
        HandleState state = HandleState.forServerFiles(
                "SELECT * FROM s.t", new String[]{"s/t/f.parquet"},
                "s2", 100L, false);

        byte[] handle = first.createEndpointHandle(state);
        HandleState decoded = second.resolveEndpointHandle(handle);

        assertEquals(state.query(), decoded.query());
        assertArrayEquals(state.filePaths(), decoded.filePaths());
        first.close();
        second.close();
    }

    /** Verifies load release is skipped for uniquely owned shards. */
    @Test
    void releaseEndpointLoadSkipsUntrackedShard() {
        HandleState state = HandleState.forServerFiles(
                "SELECT * FROM s.t", new String[]{"s/t/f.parquet"},
                "s1", 100L, false);
        ClusterService cs = createService("s1");
        byte[] handle = cs.createEndpointHandle(state);

        cs.releaseEndpointLoad(handle, state);

        verify(serverRegistry, never()).compute(eq("s1"), any());
        cs.close();
    }

    /** Verifies retries release tracked endpoint load only once. */
    @Test
    void releaseEndpointLoadIsIdempotent() {
        HandleState state = HandleState.forServerFiles(
                "SELECT * FROM s.t", new String[]{"s/t/f.parquet"},
                "s1", 100L, true);
        ClusterService cs = createService("s1");
        byte[] handle = cs.createEndpointHandle(state);
        when(statementCache.remove(anyString())).thenReturn(state, null);

        cs.releaseEndpointLoad(handle, state);
        cs.releaseEndpointLoad(handle, state);

        verify(serverRegistry, times(1)).compute(eq("s1"), any());
        cs.close();
    }

    /** Backs the registry mock's compute with a real map, as Hazelcast applies it. */
    private Map<String, Long> registryBackedByMap() {
        Map<String, Long> backing = new java.util.concurrent.ConcurrentHashMap<>();
        when(serverRegistry.compute(anyString(), any())).thenAnswer(invocation -> {
            java.util.function.BiFunction<String, Long, Long> function = invocation.getArgument(1);
            return backing.compute(invocation.getArgument(0), function);
        });
        return backing;
    }

    /** Verifies a stale node removed from the registry is not added back by a late release. */
    @Test
    void releaseEndpointLoadDoesNotBringBackARemovedNode() {
        Map<String, Long> registry = registryBackedByMap();
        HandleState state = HandleState.forServerFiles(
                "SELECT * FROM s.t", new String[]{"s/t/f.parquet"}, "gone", 100L, true);
        ClusterService cs = createService("s1");
        byte[] handle = cs.createEndpointHandle(state);
        when(statementCache.remove(anyString())).thenReturn(state);

        cs.releaseEndpointLoad(handle, state);

        assertFalse(registry.containsKey("gone"),
                "a registry entry without heartbeats would count as a live node");
        cs.close();
    }

    /** Verifies a backup node takes over the load reservation of the endpoint it runs. */
    @Test
    void claimEndpointLoadMovesTheReservationToTheBackupNode() {
        Map<String, Long> registry = registryBackedByMap();
        registry.put("primary", 150L);
        registry.put("backup", 20L);
        HandleState state = HandleState.forServerFiles(
                "SELECT * FROM s.t", new String[]{"s/t/f.parquet"}, "primary", 100L, true);
        ClusterService cs = createService("backup");
        byte[] handle = cs.createEndpointHandle(state);
        when(statementCache.remove(anyString())).thenReturn(state);

        assertTrue(cs.claimEndpointLoad(handle, state));

        assertEquals(50L, registry.get("primary"));
        assertEquals(120L, registry.get("backup"));
        org.mockito.ArgumentCaptor<Serializable> moved =
                org.mockito.ArgumentCaptor.forClass(Serializable.class);
        verify(statementCache, atLeastOnce()).put(startsWith("__arrowflight-ticket-load-v1:"),
                moved.capture(), eq(10L), eq(java.util.concurrent.TimeUnit.MINUTES));
        HandleState reservation = (HandleState) moved.getValue();
        assertEquals("backup", reservation.serverUri());
        assertEquals(100L, reservation.bytes());

        // The release at the end of the stream now lowers the backup node's load.
        when(statementCache.remove(anyString())).thenReturn(reservation, (Serializable) null);
        cs.releaseEndpointLoad(handle, state);
        cs.releaseEndpointLoad(handle, state);
        assertEquals(50L, registry.get("primary"));
        assertEquals(20L, registry.get("backup"));
        cs.close();
    }

    /** Verifies the background claim moves the reservation before the release that waits for it. */
    @Test
    void backgroundClaimCompletesBeforeTheRelease() {
        Map<String, Long> registry = registryBackedByMap();
        registry.put("primary", 100L);
        registry.put("backup", 0L);
        HandleState state = HandleState.forServerFiles(
                "SELECT * FROM s.t", new String[]{"s/t/f.parquet"}, "primary", 100L, true);
        ClusterService cs = createService("backup");
        byte[] handle = cs.createEndpointHandle(state);
        when(statementCache.remove(anyString())).thenReturn(state);

        assertTrue(cs.plannedElsewhere(state));
        cs.claimEndpointLoadLater(handle, state).join();

        assertEquals(0L, registry.get("primary"));
        assertEquals(100L, registry.get("backup"));
        cs.close();
    }

    /** Verifies a failing background claim is logged, not thrown at the release that waits for it. */
    @Test
    void failingBackgroundClaimDoesNotFailTheStream() {
        HandleState state = HandleState.forServerFiles(
                "SELECT * FROM s.t", new String[]{"s/t/f.parquet"}, "primary", 100L, true);
        ClusterService cs = createService("backup");
        byte[] handle = cs.createEndpointHandle(state);
        when(statementCache.remove(anyString())).thenThrow(new IllegalStateException("member gone"));

        cs.claimEndpointLoadLater(handle, state).join();
        cs.close();
    }

    /** Verifies the planned node itself, complete answers and released reservations claim nothing. */
    @Test
    void claimEndpointLoadIgnoresOwnCompleteAndReleasedEndpoints() {
        Map<String, Long> registry = registryBackedByMap();
        registry.put("primary", 100L);
        registry.put("backup", 0L);
        HandleState state = HandleState.forServerFiles(
                "SELECT * FROM s.t", new String[]{"s/t/f.parquet"}, "primary", 100L, true);
        ClusterService planned = createService("primary");
        byte[] handle = planned.createEndpointHandle(state);
        assertFalse(planned.claimEndpointLoad(handle, state));
        assertFalse(planned.claimEndpointLoad(handle, HandleState.forCompleteAnswer("SELECT 1")));
        verify(statementCache, never()).remove(anyString());

        ClusterService backup = createService("backup");
        when(statementCache.remove(anyString())).thenReturn(null);
        assertTrue(backup.claimEndpointLoad(handle, state));
        assertEquals(100L, registry.get("primary"));
        assertEquals(0L, registry.get("backup"));

        HandleState untracked = HandleState.forServerFiles(
                "SELECT * FROM s.t", new String[]{"s/t/f.parquet"}, "primary", 100L, false);
        assertTrue(backup.claimEndpointLoad(handle, untracked));
        verify(statementCache, times(1)).remove(anyString());
        planned.close();
        backup.close();
    }

    /** Verifies a node other than the planned one can verify and run a ticket. */
    @Test
    void backupNodeResolvesTicketPlannedForAnotherNode() {
        AtomicReference<Serializable> sharedSecret = new AtomicReference<>();
        when(statementCache.putIfAbsent(anyString(), any())).thenAnswer(invocation -> {
            sharedSecret.compareAndSet(null, invocation.getArgument(1));
            return sharedSecret.get() == invocation.getArgument(1) ? null : sharedSecret.get();
        });
        ClusterService planner = createService("n1");
        ClusterService backup = createService("n3");
        HandleState state = HandleState.forServerFiles(
                "SELECT * FROM s.t", new String[]{"s/t/f.parquet"}, "n2", 100L, true);

        HandleState decoded = backup.resolveEndpointHandle(planner.createEndpointHandle(state));

        assertEquals(state, decoded);
        planner.close();
        backup.close();
    }

    @Test
    void getHandleReturnsNullWhenNotStored() {
        when(statementCache.get("nonexistent")).thenReturn(null);

        ClusterService cs = createService("s1");
        assertNull(cs.getHandle("nonexistent"));
        cs.close();
    }

    @Test
    void removeHandleDeletesFromCache() {
        ClusterService cs = createService("s1");
        cs.removeHandle("h1");
        verify(statementCache).remove("h1");
        cs.close();
    }

    // ── fileLocations ─────────────────────────────────────────────────────

    @Test
    void fileLocationsMergesMultipleServers() {
        when(serverFiles.entrySet()).thenReturn(Set.of(
                Map.entry("s1", Map.of("f1.parquet", 100L)),
                Map.entry("s2", Map.of("f1.parquet", 100L, "f2.parquet", 200L))));

        ClusterService cs = createService("s1");
        Map<String, FileAssignment> result = cs.fileLocations();

        assertEquals(2, result.size());
        assertEquals(100L, result.get("f1.parquet").size());
        assertEquals(2, result.get("f1.parquet").hosts().size());
        assertEquals(1, result.get("f2.parquet").hosts().size());
        cs.close();
    }

    /** Verifies a plan on shared storage reads the inventories of current members by key. */
    @Test
    void fileLocationsOfMembersReadsTheirInventoriesByKey() {
        when(hazelcast.memberServerUris()).thenReturn(Set.of("s1", "s2"));
        when(serverFiles.getAll(Set.of("s1", "s2"))).thenReturn(Map.of(
                "s2", Map.of("f1.parquet", 100L)));

        ClusterService cs = createService("s1");
        Map<String, FileAssignment> result = cs.fileLocations(true);
        assertEquals(Set.of("s2"), result.get("f1.parquet").hosts());
        verify(serverFiles, never()).entrySet();
        cs.close();
    }

    /**
     * Verifies a plan on local disks keeps the inventories of nodes that left, so that their
     * files fail the plan instead of leaving the answer.
     */
    @Test
    void fileLocationsOfEveryNodeKeepsTheInventoriesOfNodesThatLeft() {
        when(hazelcast.memberServerUris()).thenReturn(Set.of("s1"));
        when(serverFiles.entrySet()).thenReturn(Set.of(
                Map.entry("s1", Map.of("f1.parquet", 100L)),
                Map.entry("s2", Map.of("f2.parquet", 200L))));

        ClusterService cs = createService("s1");
        assertEquals(Set.of("f1.parquet", "f2.parquet"), cs.fileLocations(false).keySet());
        verify(serverFiles, never()).getAll(any());
        cs.close();
    }

    @Test
    void fileLocationsConflictingSizesThrows() {
        when(serverFiles.entrySet()).thenReturn(Set.of(
                Map.entry("s1", Map.of("f1.parquet", 100L)),
                Map.entry("s2", Map.of("f1.parquet", 200L))));

        ClusterService cs = createService("s1");
        assertThrows(IllegalStateException.class, cs::fileLocations);
        cs.close();
    }

    @Test
    void fileLocationsEmptyInventory() {
        when(serverFiles.entrySet()).thenReturn(Set.of());

        ClusterService cs = createService("s1");
        Map<String, FileAssignment> result = cs.fileLocations();
        assertTrue(result.isEmpty());
        cs.close();
    }

    // ── hasFileInventory ──────────────────────────────────────────────────

    @Test
    void hasFileInventoryReturnsTrue() {
        when(serverFiles.containsKey("s1")).thenReturn(true);

        ClusterService cs = createService("s1");
        assertTrue(cs.hasFileInventory("s1"));
        cs.close();
    }

    @Test
    void hasFileInventoryReturnsFalse() {
        when(serverFiles.containsKey("other")).thenReturn(false);

        ClusterService cs = createService("s1");
        assertFalse(cs.hasFileInventory("other"));
        cs.close();
    }

    // ── registerLocalFiles ────────────────────────────────────────────────

    @Test
    void registerLocalFilesPublishesInventory() {
        Map<String, Long> files = new LinkedHashMap<>();
        files.put("dir/f1.parquet", 100L);

        ClusterService cs = createService("s1");
        cs.registerLocalFiles(files);
        verify(serverFiles).put(eq("s1"), anyMap());
        cs.close();
    }

    // ── serverUri ─────────────────────────────────────────────────────────

    @Test
    void serverUriReturnsConstructorArg() {
        ClusterService cs = createService("grpc://127.0.0.1:32010");
        assertEquals("grpc://127.0.0.1:32010", cs.serverUri());
        cs.close();
    }

    // ── close deregistration ──────────────────────────────────────────────

    @Test
    void closeRemovesFromRegistryAndHeartbeats() {
        ClusterService cs = createService("s1");
        cs.close();
        verify(serverRegistry).remove("s1");
        verify(serverHeartbeats).remove("s1");
        verify(hazelcast).close();
    }

    /** Verifies a complete-answer ticket keeps its mode and carries no file list. */
    @Test
    void completeAnswerHandleRoundTrips() {
        HandleState state = HandleState.forCompleteAnswer("SELECT avg(x) FROM s.t");
        ClusterService cs = createService("s1");

        HandleState decoded = cs.resolveEndpointHandle(cs.createEndpointHandle(state));

        assertTrue(decoded.complete());
        assertNull(decoded.filePaths());
        assertEquals(state.query(), decoded.query());
        assertEquals(state, decoded);
        cs.close();
    }

    /** Verifies a complete-answer ticket carries its parameter values. */
    @Test
    void completeAnswerHandleKeepsParameterValues() {
        HandleState state = HandleState.forCompleteAnswer("SELECT ?", new byte[] {7, 8, 9});
        ClusterService cs = createService("s1");

        HandleState decoded = cs.resolveEndpointHandle(cs.createEndpointHandle(state));

        assertArrayEquals(new byte[] {7, 8, 9}, decoded.parameters());
        assertEquals(state, decoded);
        cs.close();
    }

    /** Verifies a node joins the pool only when registered, and only once. */
    @Test
    void nodeJoinsThePoolWhenRegistered() {
        when(hazelcast.serverRegistry()).thenReturn(serverRegistry);
        when(hazelcast.serverHeartbeats()).thenReturn(serverHeartbeats);
        when(hazelcast.statementCache()).thenReturn(statementCache);
        when(hazelcast.clusterSecrets()).thenReturn(statementCache);
        ClusterService cs = new ClusterService(hazelcast, APP_CONFIG, "grpc://late:32010", false);
        verify(serverRegistry, never()).put(eq("grpc://late:32010"), anyLong());

        cs.register();
        cs.register();

        verify(serverRegistry, times(1)).put("grpc://late:32010", 0L);
        cs.close();
    }

    /** Verifies parameter values are kept in the shared cache under the statement id. */
    @Test
    void parameterValuesAreSharedByStatementId() {
        ClusterService cs = createService("s1");
        byte[] values = {4, 2};
        when(statementCache.get("parameters:abc")).thenReturn(values);

        cs.rememberParameters("abc", values);

        verify(statementCache).put(eq("parameters:abc"), eq(values), anyLong(), any());
        assertArrayEquals(values, cs.recallParameters("abc"));
        assertNull(cs.recallParameters("other"));
        cs.close();
    }

    /** Verifies closing a statement drops its values without waiting for the shared cache. */
    @Test
    void forgetParametersDoesNotWait() {
        ClusterService cs = createService("s1");

        cs.forgetParameters("abc");

        verify(statementCache).deleteAsync("parameters:abc");
        verify(statementCache, never()).remove("parameters:abc");
        cs.close();
    }
}
