package pro.surpin.data.arrowflight.server.services;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.adapters.HazelcastAdapter;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.model.FileAssignment;
import pro.surpin.data.arrowflight.server.model.HandleState;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Three real Hazelcast members in one JVM; two of them die at once (terminate = no goodbye, as
 * kill -9). Stand finding B5: with Hazelcast's single backup the survivor lost map entries whose
 * primary and backup the two dead members held ("Promotion commit failed"): node inventories
 * (the survivor planned half of lineitem and answered without an error), heartbeats (dead
 * nodes stayed in plans) and the endpoint ticket secret (restarted nodes made a new one, and
 * every ticket planned on the survivor was refused on them and the other way round).
 */
@Tag("integration")
class ClusterMemberLossIntegrationTest {

    private final List<ClusterService> services = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (ClusterService service : services) {
            try {
                service.close();
            } catch (RuntimeException e) {
                // a terminated member
            }
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static AppConfig config(int hazelcastPort) {
        return new AppConfig(
                1, 100, 1, 131072, 0, 1, 1,
                null, false, null, null,
                null, null, null, false, 1048576, 67108864, 60000L, null, null,
                32010, hazelcastPort, 60, 3, 1000, 30000, true, false, 64, null, null,
                "complete", true, true);
    }

    private ClusterService start(int port, String[] seeds, String uri, Map<String, Long> files) {
        HazelcastAdapter adapter = new HazelcastAdapter(config(port), uri, seeds);
        ClusterService service = new ClusterService(adapter, config(port), uri);
        service.registerLocalFiles(files);
        services.add(service);
        return service;
    }

    private static Map<String, Long> files(String node, int count) {
        Map<String, Long> files = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            files.put("tpch/lineitem/" + node + "-" + i + ".parquet", 1000L + i);
        }
        return files;
    }

    private static void awaitMembers(ClusterService service, int members) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (service.getHazelcastInstance().getCluster().getMembers().size() != members) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("cluster did not reach " + members + " members");
            }
            Thread.sleep(50);
        }
    }

    @Test
    void survivorOfTwoLostMembersKeepsEveryInventoryDropsTheDeadAndSharesTicketsWithRestartedNodes()
            throws Exception {
        int[] ports = {freePort(), freePort(), freePort()};
        String[] seeds = new String[ports.length];
        String[] uris = new String[ports.length];
        for (int i = 0; i < ports.length; i++) {
            seeds[i] = "127.0.0.1:" + ports[i];
            uris[i] = "grpc+tcp://n" + (i + 1) + ":32010";
        }
        // Enough files per node that the inventories land in several partitions.
        ClusterService survivor = start(ports[0], seeds, uris[0], files("n1", 30));
        ClusterService second = start(ports[1], seeds, uris[1], files("n2", 25));
        ClusterService third = start(ports[2], seeds, uris[2], files("n3", 33));
        awaitMembers(survivor, 3);
        // In one JVM the two members rarely die before the survivor re-replicates, so the loss
        // itself is not reliably reproduced here (bench/tools/agent-int4-stand/b5loop.sh does on
        // containers); the maps every plan needs must be kept on every member.
        for (String map : List.of("server-files", "cluster-secrets")) {
            assertEquals(6, survivor.getHazelcastInstance().getConfig().getMapConfig(map)
                    .getBackupCount(), map);
        }
        Map<String, FileAssignment> before = survivor.fileLocations();
        assertEquals(88, before.size());
        HandleState state = HandleState.forServerFiles("SELECT * FROM tpch.lineitem",
                new String[]{"tpch/lineitem/n1-0.parquet"}, uris[0], 1000L, false);
        byte[] plannedBeforeLoss = survivor.createEndpointHandle(state);

        terminateTogether(second, third);
        awaitMembers(survivor, 1);

        assertEquals(before.keySet(), survivor.fileLocations().keySet(),
                "every node's inventory survives the loss of two members");
        assertEquals(Set.of(uris[0]), survivor.filterLiveServers(Set.of(uris)),
                "nodes whose members left are not live, whatever their heartbeat entries say");

        // The two nodes come back (new members, same URIs); tickets work in both directions.
        ClusterService secondAgain = start(ports[1], seeds, uris[1], files("n2", 25));
        ClusterService thirdAgain = start(ports[2], seeds, uris[2], files("n3", 33));
        awaitMembers(survivor, 3);
        assertEquals(Set.of(uris[0], uris[1], uris[2]),
                survivor.filterLiveServers(Set.of(uris)));
        for (ClusterService restarted : List.of(secondAgain, thirdAgain)) {
            assertArrayEquals(state.filePaths(),
                    restarted.resolveEndpointHandle(plannedBeforeLoss).filePaths());
            HandleState there = HandleState.forServerFiles("SELECT * FROM tpch.lineitem",
                    new String[]{"tpch/lineitem/n2-0.parquet"}, restarted.serverUri(), 1000L, false);
            assertArrayEquals(there.filePaths(),
                    survivor.resolveEndpointHandle(restarted.createEndpointHandle(there)).filePaths());
        }
        assertTrue(survivor.fileLocations().size() >= 88);
    }

    @Test
    void lostInventoryAndSecretAreRepairedByTheNodesThatStillHaveThem() throws Exception {
        int[] ports = {freePort(), freePort()};
        String[] seeds = {"127.0.0.1:" + ports[0], "127.0.0.1:" + ports[1]};
        ClusterService first = start(ports[0], seeds, "grpc+tcp://a:32010", files("a", 3));
        ClusterService other = start(ports[1], seeds, "grpc+tcp://b:32010", files("b", 2));
        awaitMembers(first, 2);
        HandleState state = HandleState.forServerFiles("SELECT 1",
                new String[]{"tpch/lineitem/a-0.parquet"}, "grpc+tcp://a:32010", 1L, false);
        byte[] oldTicket = first.createEndpointHandle(state);

        // As if a partition loss took the entries: first's inventory, and the secret, which
        // a newly joining node then replaced by one of its own.
        HazelcastAdapter hazelcast = adapterOf(first);
        hazelcast.serverFiles().remove("grpc+tcp://a:32010");
        byte[] foreign = new byte[32];
        java.util.Arrays.fill(foreign, (byte) 7);
        hazelcast.clusterSecrets().put("__arrowflight-ticket-secret-v1", foreign);

        first.repairClusterState();
        other.repairClusterState();

        assertTrue(hazelcast.serverFiles().containsKey("grpc+tcp://a:32010"));
        assertEquals(5, first.fileLocations().size());
        byte[] newTicket = first.createEndpointHandle(state);
        assertArrayEquals(state.filePaths(), other.resolveEndpointHandle(newTicket).filePaths());
        assertArrayEquals(state.filePaths(), first.resolveEndpointHandle(oldTicket).filePaths(),
                "tickets signed before the switch still verify");
    }

    /**
     * Terminates members at the same moment, as kill -9 of two containers: none of them hands
     * its partitions over first.
     *
     * @param doomed services whose members die
     * @throws Exception when interrupted
     */
    private static void terminateTogether(ClusterService... doomed) throws Exception {
        java.util.concurrent.CountDownLatch go = new java.util.concurrent.CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (ClusterService service : doomed) {
            Thread thread = new Thread(() -> {
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                service.getHazelcastInstance().getLifecycleService().terminate();
            });
            thread.start();
            threads.add(thread);
        }
        go.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
    }

    private static HazelcastAdapter adapterOf(ClusterService service) throws Exception {
        java.lang.reflect.Field field = ClusterService.class.getDeclaredField("hazelcast");
        field.setAccessible(true);
        return (HazelcastAdapter) field.get(service);
    }
}
