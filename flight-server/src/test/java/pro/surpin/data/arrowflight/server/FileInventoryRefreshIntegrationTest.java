package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightEndpoint;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.server.model.FileAssignment;
import pro.surpin.data.arrowflight.server.model.HandleState;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Item 49 (d): two nodes, each with its own local data directory. A file written on the second
 * node after both started reaches the first node's plans once the second node lists its files
 * again ({@code fileInventoryRefreshSeconds}); before, a node published its inventory only at
 * start and plans on local disks silently left the file out of the answer.
 */
@Tag("integration")
class FileInventoryRefreshIntegrationTest {

    private static final int ROWS_FIRST = 300;
    private static final int ROWS_LATER = 120;
    private static final String NEW_FILE = "s/t/part-1-later.parquet";
    private static final List<Path> DIRS = new ArrayList<>();
    private static final List<TestFlightServerHelper> NODE = new ArrayList<>();

    /**
     * Writes one file of the table on the first node, an empty table directory on the second,
     * and starts both nodes in one cluster.
     *
     * @throws Exception on failure
     */
    @BeforeAll
    static void startCluster() throws Exception {
        for (int i = 0; i < 2; i++) {
            Path dir = Files.createTempDirectory("inventory-refresh-" + i);
            Files.createDirectories(dir.resolve("s").resolve("t"));
            DIRS.add(dir);
        }
        write(DIRS.get(0).resolve("s/t/part-0.parquet"), 0, ROWS_FIRST);
        int[] hazelcastPorts = {freePort(), freePort()};
        String[] seeds = {"127.0.0.1:" + hazelcastPorts[0], "127.0.0.1:" + hazelcastPorts[1]};
        for (int i = 0; i < 2; i++) {
            TestFlightServerHelper node = TestFlightServerHelper.builder().dataDir(DIRS.get(i).toString())
                    .hazelcastPort(hazelcastPorts[i]).flightPort(freePort()).hazelcastSeeds(seeds).start();
            NODE.add(node);
            node.clusterService.refreshInventory(node.parquetAdapter::localFileInventory, 1);
        }
        await(locations -> locations.containsKey("s/t/part-0.parquet"), "the first node's file");
    }

    /**
     * Stops the nodes and removes their directories.
     *
     * @throws Exception on failure
     */
    @AfterAll
    static void stopCluster() throws Exception {
        for (int i = NODE.size() - 1; i >= 0; i--) {
            NODE.get(i).close();
        }
        for (Path dir : DIRS) {
            try (var files = Files.walk(dir)) {
                files.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    /**
     * A file written on the second node after the start is in the first node's plans.
     *
     * @throws Exception on failure
     */
    @Test
    void fileWrittenAfterStartIsPlanned() throws Exception {
        assertFalse(NODE.get(0).clusterService.fileLocations(false).containsKey(NEW_FILE));
        // Written aside and moved in, as a writer should, so no listing sees half a file.
        Path aside = DIRS.get(1).resolve("part-1-later.tmp");
        write(aside, ROWS_FIRST, ROWS_LATER);
        Files.move(aside, DIRS.get(1).resolve(NEW_FILE));
        String second = NODE.get(1).location.getUri().toString();
        await(locations -> locations.containsKey(NEW_FILE) && locations.get(NEW_FILE).hosts().contains(second),
                "the file written later on the second node");

        // The plan is checked, not run: both nodes share one host, and the planner tells nodes
        // apart by host, so on local disks it may send a file to the other node of the host.
        TestFlightServerHelper first = NODE.get(0);
        Set<String> planned = new TreeSet<>();
        for (FlightEndpoint endpoint : first.queryPlanner.plan("SELECT v FROM s.t").endpoints()) {
            byte[] handle = com.google.protobuf.Any.parseFrom(endpoint.getTicket().getBytes())
                    .unpack(org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery.class)
                    .getStatementHandle().toByteArray();
            HandleState state = first.clusterService.resolveEndpointHandle(handle);
            planned.addAll(Arrays.asList(state.filePaths()));
            // Planning reserved the endpoint's load; nothing reads it.
            first.clusterService.releaseEndpointLoad(handle, state);
        }
        assertEquals(new TreeSet<>(Set.of("s/t/part-0.parquet", NEW_FILE)), planned,
                "the plan names the file written later");
    }

    /**
     * Writes rows {@code v = first .. first + rows - 1} to a Parquet file.
     *
     * @param file target file
     * @param first first value
     * @param rows row count
     * @throws Exception on failure
     */
    private static void write(Path file, int first, int rows) throws Exception {
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = duck.createStatement()) {
            statement.execute("COPY (SELECT (" + first + " + i)::BIGINT AS v FROM range(" + rows + ") r(i))"
                    + " TO '" + file + "' (FORMAT PARQUET)");
        }
    }

    /**
     * Waits until the first node's view of the published inventories passes a check.
     *
     * @param check check of relative path to assignment
     * @param what what is awaited, for the failure
     * @throws Exception on failure or timeout
     */
    private static void await(Predicate<Map<String, FileAssignment>> check, String what) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            if (check.test(NODE.get(0).clusterService.fileLocations(false))) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("not published within 30 s: " + what + "; published "
                + NODE.get(0).clusterService.fileLocations(false).keySet());
    }

    /**
     * Finds a free local port.
     *
     * @return port
     * @throws Exception on failure
     */
    private static int freePort() throws Exception {
        return TestPorts.free();
    }
}
