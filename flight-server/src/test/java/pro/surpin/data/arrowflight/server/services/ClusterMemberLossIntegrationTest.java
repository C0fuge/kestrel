package pro.surpin.data.arrowflight.server.services;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.TestPorts;
import pro.surpin.data.arrowflight.server.adapters.HazelcastAdapter;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.model.FileAssignment;
import pro.surpin.data.arrowflight.server.model.HandleState;

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

    private static final String MAX_NO_HEARTBEAT = "hazelcast.max.no.heartbeat.seconds";
    private static final String LOCAL_ADDRESS = "hazelcast.local.localAddress";

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
        return TestPorts.free();
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
     * Item 0d (StarRocks connector CI, a node stopped before a full scan): after a member left,
     * every query of a whole map (entrySet) waited ~10 s while the cluster settled, and the
     * planner ran one per plan for the node loads, so GetFlightInfo on a survivor took 10 s and
     * more and timed out on a busy runner. The planner's reads are key lookups of the members
     * now. In one JVM the left member's port refuses at once and the wait does not show; it does
     * when its address stops answering (container gone; reproduced with an iptables DROP:
     * allServerLoads 10.3 s before, 11 ms after).
     */
    @Test
    void survivorPlansAtOnceAfterAMemberLeft() throws Exception {
        int[] ports = {freePort(), freePort(), freePort()};
        String[] seeds = new String[ports.length];
        String[] uris = new String[ports.length];
        for (int i = 0; i < ports.length; i++) {
            seeds[i] = "127.0.0.1:" + ports[i];
            uris[i] = "grpc+tcp://n" + (i + 1) + ":32010";
        }
        ClusterService survivor = start(ports[0], seeds, uris[0], files("n1", 10));
        ClusterService stopped = start(ports[1], seeds, uris[1], files("n2", 10));
        start(ports[2], seeds, uris[2], files("n3", 10));
        awaitMembers(survivor, 3);
        assertEquals(Set.of(uris), survivor.filterLiveServers(survivor.allServerLoads().keySet()));
        // Partitions move to the new members first: then the leaving one owns its share.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!survivor.getHazelcastInstance().getPartitionService().isClusterSafe()) {
            assertTrue(System.nanoTime() < deadline, "the cluster did not settle");
            Thread.sleep(50);
        }

        stopped.close();
        awaitMembers(survivor, 2);

        long started = System.nanoTime();
        Set<String> live = survivor.filterLiveServers(survivor.allServerLoads().keySet());
        Map<String, FileAssignment> local = survivor.fileLocations(true);
        long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        assertTrue(millis < 5000, "planning reads took " + millis + " ms");
        assertEquals(Set.of(uris[0], uris[2]), live);
        assertEquals(20, local.size(), "the inventories of the members that are left");
    }

    /**
     * 3-node S3 stand finding (c): after kill tests the first plan waited 9-10 s on one stand and
     * about 1 s on another. A crashed member whose address still answers (process gone, port
     * refuses) is dropped as soon as a connection to it fails; one whose address went away with
     * it (container removed, host down) answers nothing, and every map operation on its
     * partitions waits until the failure detector gives up on it.
     */
    @Test
    void crashedMemberWhosePortRefusesIsDroppedAtOnce() throws Exception {
        List<ClusterService> cluster = crashCluster(null);
        long millis = firstPlanMillisAfterCrash(cluster);
        assertTrue(millis < 5000, "first plan took " + millis + " ms");
    }

    /**
     * The other half of the finding above, item D3: a crashed member whose address vanished used
     * to hold every plan for hazelcast.max.no.heartbeat.seconds (10 s). The planner's reads go
     * through the cluster view now: the first plan waits for the silent member once, for the
     * read wait (1 s), and leaves it out; later plans do not wait at all. The doomed member
     * listens on 127.0.0.2 and an iptables DROP makes that address silent, which needs root or
     * password-less sudo (skipped otherwise; GitHub runners have it).
     */
    @Test
    void crashedMemberWhoseAddressVanishedHoldsOnlyTheFirstPlanForTheReadWait() throws Exception {
        String iptables = iptablesCommand();
        Assumptions.assumeTrue(iptables != null, "needs iptables as root or via sudo -n");
        List<ClusterService> cluster = crashCluster(null);
        ClusterService survivor = cluster.get(0);
        String rules = "%1$s %2$s INPUT -s 127.0.0.2 -j DROP && %1$s %2$s INPUT -d 127.0.0.2 -j DROP";
        long first;
        long later;
        try {
            assertTrue(run(rules.formatted(iptables, "-I")), "the DROP rules were not added");
            first = firstPlanMillisAfterCrash(cluster);
            long started = System.nanoTime();
            for (int i = 0; i < 5; i++) {
                assertEquals(Set.of("grpc+tcp://n1:32010", "grpc+tcp://n2:32010"),
                        survivor.filterLiveServers(survivor.allServerLoads().keySet()));
                // n2's inventory is missing when it lives on n3's partitions and was never read:
                // on shared storage the listing still names its files (locality only is lost).
                Map<String, FileAssignment> located = survivor.fileLocations(true);
                assertTrue(located.keySet().stream().noneMatch(path -> path.contains("/n3-")),
                        "the crashed member's inventory: " + located.keySet());
                assertTrue(located.keySet().stream().filter(path -> path.contains("/n1-")).count() == 10);
                assertTrue(survivor.hasFileInventory("grpc+tcp://n2:32010"));
            }
            later = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) / 5;
            // Still within Hazelcast's 10 s: the member has not been dropped, only left out.
            assertEquals(3, survivor.getHazelcastInstance().getCluster().getMembers().size(),
                    "the crashed member was dropped already; the test measured nothing");
        } finally {
            run(rules.formatted(iptables, "-D"));
        }
        assertTrue(first < 3000, "first plan took " + first + " ms");
        assertTrue(later < 500, "later plans took " + later + " ms each");
    }

    /**
     * Item D3 step 3: a plan on local disks needs every inventory, including the silent
     * member's, so that its files fail the plan instead of leaving the answer. That read waited
     * for the silent member (a full read of the map) until Hazelcast dropped it; now it takes
     * the inventories on silent partitions from the node's local copy and does not wait.
     */
    @Test
    void crashedMemberWhoseAddressVanishedDoesNotHoldLocalDiskPlans() throws Exception {
        String iptables = iptablesCommand();
        Assumptions.assumeTrue(iptables != null, "needs iptables as root or via sudo -n");
        List<ClusterService> cluster = crashCluster(null);
        ClusterService survivor = cluster.get(0);
        Map<String, FileAssignment> before = survivor.fileLocations(false);
        String rules = "%1$s %2$s INPUT -s 127.0.0.2 -j DROP && %1$s %2$s INPUT -d 127.0.0.2 -j DROP";
        long millis;
        try {
            assertTrue(run(rules.formatted(iptables, "-I")), "the DROP rules were not added");
            firstPlanMillisAfterCrash(cluster);
            long started = System.nanoTime();
            for (int i = 0; i < 5; i++) {
                Map<String, FileAssignment> located = survivor.fileLocations(false);
                assertEquals(before.keySet(), located.keySet());
                assertTrue(located.keySet().stream().anyMatch(path -> path.contains("/n3-")),
                        "the crashed member's inventory is gone: " + located.keySet());
            }
            millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) / 5;
            assertEquals(3, survivor.getHazelcastInstance().getCluster().getMembers().size(),
                    "the crashed member was dropped already; the test measured nothing");
        } finally {
            run(rules.formatted(iptables, "-D"));
        }
        assertTrue(millis < 500, "local-disk plans took " + millis + " ms each");
    }

    /**
     * Item D3 step 2: after the first plan found the silent member, the writes of a partial-mode
     * plan with load tracking (load added per server, a reservation per ticket, its release) and
     * the heartbeat do not wait for it either; they land once Hazelcast dropped the member, and
     * every load goes back to zero.
     */
    @Test
    void crashedMemberWhoseAddressVanishedHoldsNoWritesOfPlansOrHeartbeats() throws Exception {
        String iptables = iptablesCommand();
        Assumptions.assumeTrue(iptables != null, "needs iptables as root or via sudo -n");
        List<ClusterService> cluster = crashCluster(null);
        ClusterService survivor = cluster.get(0);
        String[] servers = {"grpc+tcp://n1:32010", "grpc+tcp://n2:32010"};
        String rules = "%1$s %2$s INPUT -s 127.0.0.2 -j DROP && %1$s %2$s INPUT -d 127.0.0.2 -j DROP";
        long millis;
        try {
            assertTrue(run(rules.formatted(iptables, "-I")), "the DROP rules were not added");
            firstPlanMillisAfterCrash(cluster);
            long started = System.nanoTime();
            for (int i = 0; i < 40; i++) {
                String server = servers[i % 2];
                HandleState state = HandleState.forServerFiles("SELECT " + i,
                        new String[]{"tpch/lineitem/f" + i + ".parquet"}, server, 100L, true);
                byte[] handle = survivor.createEndpointHandle(state);
                survivor.addLoad(server, 100L);
                survivor.releaseEndpointLoad(handle, state);
                survivor.beat();
            }
            survivor.repairClusterState();
            millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertEquals(3, survivor.getHazelcastInstance().getCluster().getMembers().size(),
                    "the crashed member was dropped already; the test measured nothing");
        } finally {
            run(rules.formatted(iptables, "-D"));
        }
        assertTrue(millis < 1000, "40 tickets took " + millis + " ms");
        assertTrue(survivor.deferredWrites() > 0, "no write went to the crashed member's partitions");
        awaitMembers(survivor, 2);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        Map<String, Long> loads = survivor.getLoads(Set.of(servers));
        while (!loads.equals(Map.of(servers[0], 0L, servers[1], 0L))) {
            assertTrue(System.nanoTime() < deadline, "loads after the deferred writes: " + loads);
            Thread.sleep(100);
            loads = survivor.getLoads(Set.of(servers));
        }
    }

    /**
     * Starts two members on 127.0.0.1 and a third, the one to crash, on 127.0.0.2.
     *
     * @param maxNoHeartbeatSeconds hazelcast.max.no.heartbeat.seconds for every member, or null
     *        for the server's default
     * @return the members, the one to crash last
     * @throws Exception when the cluster does not form
     */
    private List<ClusterService> crashCluster(String maxNoHeartbeatSeconds) throws Exception {
        int[] ports = {freePort(), freePort(), freePort()};
        String[] hosts = {"127.0.0.1", "127.0.0.1", "127.0.0.2"};
        String[] seeds = new String[ports.length];
        for (int i = 0; i < ports.length; i++) {
            seeds[i] = hosts[i] + ":" + ports[i];
        }
        List<ClusterService> cluster = new ArrayList<>();
        try {
            if (maxNoHeartbeatSeconds != null) {
                System.setProperty(MAX_NO_HEARTBEAT, maxNoHeartbeatSeconds);
            }
            for (int i = 0; i < ports.length; i++) {
                if (i == ports.length - 1) {
                    System.setProperty(LOCAL_ADDRESS, hosts[i]);
                }
                cluster.add(start(ports[i], seeds, "grpc+tcp://n" + (i + 1) + ":32010",
                        files("n" + (i + 1), 10)));
            }
        } finally {
            System.clearProperty(LOCAL_ADDRESS);
            System.clearProperty(MAX_NO_HEARTBEAT);
        }
        awaitMembers(cluster.get(0), ports.length);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!cluster.get(0).getHazelcastInstance().getPartitionService().isClusterSafe()) {
            assertTrue(System.nanoTime() < deadline, "the cluster did not settle");
            Thread.sleep(50);
        }
        return cluster;
    }

    /**
     * Crashes the last member (no goodbye) and times the planner's cluster reads and ticket
     * reservations on the first one.
     *
     * @param cluster members from crashCluster
     * @return milliseconds the first plan's cluster operations took
     * @throws Exception when interrupted
     */
    private static long firstPlanMillisAfterCrash(List<ClusterService> cluster) throws Exception {
        ClusterService survivor = cluster.get(0);
        // As every heartbeat does (3 s): the view knows the members' loads before the crash. A
        // member's entry on the crashed member's partitions that was never read is not planned.
        survivor.refreshView();
        cluster.get(cluster.size() - 1).getHazelcastInstance().getLifecycleService().terminate();
        long started = System.nanoTime();
        Set<String> live = survivor.filterLiveServers(survivor.allServerLoads().keySet());
        // Handles land on every partition, the crashed member's too.
        for (int i = 0; i < 20; i++) {
            survivor.createEndpointHandle(HandleState.forCompleteAnswer("SELECT " + i, null));
        }
        long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        assertEquals(Set.of("grpc+tcp://n1:32010", "grpc+tcp://n2:32010"), live);
        return millis;
    }

    /**
     * Finds how this process can change iptables rules.
     *
     * @return the command prefix, or null when it cannot
     * @throws Exception when interrupted
     */
    private static String iptablesCommand() throws Exception {
        if (!System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT).contains("linux")) {
            return null;
        }
        for (String command : List.of("iptables", "sudo -n iptables")) {
            if (run(command + " -n -L INPUT")) {
                return command;
            }
        }
        return null;
    }

    /**
     * Runs a shell command line and discards its output.
     *
     * @param commandLine command line for sh -c
     * @return whether it exited with status 0
     * @throws Exception when interrupted
     */
    private static boolean run(String commandLine) throws Exception {
        try {
            Process process = new ProcessBuilder("sh", "-c", commandLine)
                    .redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .start();
            return process.waitFor(30, TimeUnit.SECONDS) && process.exitValue() == 0;
        } catch (java.io.IOException e) {
            return false;
        }
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
