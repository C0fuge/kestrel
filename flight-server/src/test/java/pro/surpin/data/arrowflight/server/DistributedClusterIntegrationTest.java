package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import pro.surpin.data.arrowflight.server.services.DistributedComplete;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Distributed complete mode on a cluster of three nodes in one JVM (item 18 step 3, D2): the
 * nodes share one data directory, so every node can also answer alone; the first node's merged
 * answers must equal its single-node answers, with peers healthy, a peer's stream cut in the
 * middle, a peer whose Flight service is down while its member is live (read from backup
 * locations), every location down, and a node gone.
 */
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DistributedClusterIntegrationTest {

    private static final int NODES = 3;
    private static final int FILES = 6;
    private static final int ROWS = 20_000;
    private static Path dataDir;
    private static final List<TestFlightServerHelper> NODE = new ArrayList<>();
    private static BufferAllocator peerAllocator;
    private static DistributedComplete distributed;

    /** Queries whose shape merges; ordered ones compare row by row, the others as row sets. */
    private static final List<String> MERGED = List.of(
            "SELECT count(*) FROM s.t",
            "SELECT count(*) AS n, sum(v) AS total, min(d) AS lo, max(name) AS hi FROM s.t",
            "SELECT k, count(*) AS n, sum(v) AS total, min(v) AS lo, max(d) AS hi FROM s.t"
                    + " GROUP BY k ORDER BY k",
            "SELECT DISTINCT name FROM s.t ORDER BY name",
            "SELECT v, name FROM s.t WHERE v % 7 = 0 ORDER BY v DESC LIMIT 5",
            "SELECT v, k FROM s.t WHERE v < 400 ORDER BY 1",
            "SELECT k, name FROM s.t WHERE v % 1000 = 3");

    /** Large endpoint answers merged only after every peer was read. */
    private static final String WIDE_GROUPS =
            "SELECT v, count(*) AS n, max(name) AS hi FROM s.t GROUP BY v ORDER BY v";

    /** Large endpoint answers concatenated as they arrive. */
    private static final String WIDE_ROWS = "SELECT v, d, name FROM s.t WHERE k < 8";

    /**
     * Writes a table of six files and starts three complete-mode nodes on one cluster, two
     * endpoints per node and query.
     *
     * @throws Exception on failure
     */
    @BeforeAll
    static void startCluster() throws Exception {
        dataDir = Files.createTempDirectory("distributed-cluster");
        Path table = Files.createDirectories(dataDir.resolve("s").resolve("t"));
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = duck.createStatement()) {
            for (int f = 0; f < FILES; f++) {
                // v is unique over the table; file 5 has no row with k = 9.
                statement.execute("COPY (SELECT (i % " + (f == 5 ? 9 : 10) + ")::INTEGER AS k,"
                        + " (i * " + FILES + " + " + f + ")::BIGINT AS v, (i / 4.0)::DOUBLE AS d,"
                        + " 'n' || (i % 13) AS name FROM range(" + ROWS + ") r(i))"
                        + " TO '" + table.resolve("part-" + f + ".parquet") + "' (FORMAT PARQUET)");
            }
        }
        int[] hazelcastPorts = new int[NODES];
        int[] flightPorts = new int[NODES];
        String[] seeds = new String[NODES];
        for (int i = 0; i < NODES; i++) {
            hazelcastPorts[i] = freePort();
            flightPorts[i] = freePort();
            seeds[i] = "127.0.0.1:" + hazelcastPorts[i];
        }
        System.setProperty("endpointsPerServer", "2");
        try {
            for (int i = 0; i < NODES; i++) {
                NODE.add(TestFlightServerHelper.builder().dataDir(dataDir.toString())
                        .resultMode("complete").sharedStorage().hazelcastPort(hazelcastPorts[i])
                        .flightPort(flightPorts[i]).hazelcastSeeds(seeds).start());
            }
        } finally {
            System.clearProperty("endpointsPerServer");
        }
        awaitPlanSpanning(NODES);
        peerAllocator = NODE.get(0).allocator.newChildAllocator("peers", 0, Long.MAX_VALUE);
    }

    /**
     * Stops the nodes that are left and removes the table.
     *
     * @throws Exception on failure
     */
    @AfterAll
    static void stopCluster() throws Exception {
        try {
            useDistributed(null);
            if (peerAllocator != null) {
                peerAllocator.close();
            }
            for (int i = NODE.size() - 1; i >= 0; i--) {
                try {
                    NODE.get(i).close();
                } catch (Exception e) {
                    // a node a test stopped
                }
            }
        } finally {
            try (var files = Files.walk(dataDir)) {
                files.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    /**
     * Turns the distributed path off after each test.
     *
     * @throws Exception on failure
     */
    @AfterEach
    void off() throws Exception {
        useDistributed(null);
    }

    /**
     * Verifies the first node's merged answers, read from all three nodes, equal its single-node
     * answers.
     *
     * @throws Exception on failure
     */
    @Test
    @Order(1)
    void mergedAnswersFromThreeNodesEqualSingleNodeAnswers() throws Exception {
        for (String query : withWide()) {
            String alone = answer(query);
            useDistributed(DistributedComplete.clientFactory(peerAllocator, null));
            assertEquals(alone, answer(query), query);
            assertEquals(1, distributed.mergedAnswers(), "merged from endpoint answers: " + query);
            assertEquals(0, distributed.fallbacks(), query);
            assertTrue(distributed.peerBytes() > 0, "peer bytes counted: " + query);
            useDistributed(null);
        }
    }

    /**
     * Verifies a peer stream reset before the first batch reached the client gives the exact
     * answer: the merge of groups reads every endpoint answer first, so a reset after the peer's
     * schema falls back to the single-node answer, and one before it fails over to a backup
     * location and merges (which of the two depends on where the relay's byte count lands).
     *
     * @throws Exception on failure
     */
    @Test
    @Order(2)
    void peerStreamCutBeforeTheFirstBatchFallsBackToSingleNodeAnswer() throws Exception {
        String alone = answer(WIDE_GROUPS);
        try (CuttingRelay relay = new CuttingRelay(port(NODE.get(2)), 32 * 1024)) {
            useDistributed(through(NODE.get(2), relay));
            assertEquals(alone, answer(WIDE_GROUPS));
            assertTrue(relay.wasCut(), "the peer's stream was not cut");
            assertCutAnswered(1);
        }
    }

    /**
     * Verifies a peer stream reset in the middle of a concatenated answer never yields a silently
     * partial answer: the client gets either the single-node answer (the reset came before the
     * first batch) or an error.
     *
     * @throws Exception on failure
     */
    @Test
    @Order(3)
    void peerStreamCutDuringTheAnswerFailsOrFallsBackNeverPartial() throws Exception {
        String alone = answer(WIDE_ROWS);
        try (CuttingRelay relay = new CuttingRelay(port(NODE.get(2)), 32 * 1024)) {
            useDistributed(through(NODE.get(2), relay));
            String outcome;
            try {
                outcome = answer(WIDE_ROWS);
            } catch (FlightRuntimeException e) {
                outcome = null;
            }
            assertTrue(relay.wasCut(), "the peer's stream was not cut");
            if (outcome != null) {
                assertEquals(alone, outcome);
            }
            assertCutAnswered(outcome != null ? 1 : 0);
        }
        // The node still answers afterwards, merged.
        useDistributed(DistributedComplete.clientFactory(peerAllocator, null));
        assertEquals(alone, answer(WIDE_ROWS));
        assertEquals(1, distributed.mergedAnswers());
    }

    /**
     * Verifies the endpoint answers of a peer whose member is live but whose Flight service is gone
     * are read from the endpoints' backup locations, and the merged answer stays exact.
     *
     * @throws Exception on failure
     */
    @Test
    @Order(4)
    void peerWithoutFlightServiceIsReadFromBackupLocations() throws Exception {
        int closedPort = freePort();
        Function<Location, FlightClient> healthy = DistributedComplete.clientFactory(peerAllocator, null);
        for (String query : List.of(MERGED.get(2), WIDE_ROWS)) {
            String alone = answer(query);
            useDistributed(location -> healthy.apply(port(location) == port(NODE.get(1))
                    ? Location.forGrpcInsecure("localhost", closedPort) : location));
            assertEquals(alone, answer(query), query);
            assertEquals(1, distributed.mergedAnswers(), query);
            assertEquals(0, distributed.fallbacks(), query);
            assertTrue(distributed.failovers() > 0, "the down peer's endpoints failed over: " + query);
            useDistributed(null);
        }
    }

    /**
     * Verifies an endpoint whose locations all refuse the call falls back to the single-node
     * answer.
     *
     * @throws Exception on failure
     */
    @Test
    @Order(5)
    void endpointDownAtAllLocationsFallsBackToSingleNodeAnswer() throws Exception {
        int closedPort = freePort();
        Function<Location, FlightClient> healthy = DistributedComplete.clientFactory(peerAllocator, null);
        for (String query : List.of(MERGED.get(2), WIDE_ROWS)) {
            String alone = answer(query);
            useDistributed(location -> healthy.apply(Location.forGrpcInsecure("localhost", closedPort)));
            assertEquals(alone, answer(query), query);
            assertEquals(0, distributed.mergedAnswers(), query);
            assertEquals(1, distributed.fallbacks(), query);
            useDistributed(null);
        }
    }

    /**
     * Verifies that after a node left the cluster, plans leave it out and the merged answers of
     * the two nodes left equal the single-node answers.
     *
     * @throws Exception on failure
     */
    @Test
    @Order(6)
    void nodeThatLeftIsNotPlannedAndAnswersStayExact() throws Exception {
        TestFlightServerHelper gone = NODE.remove(2);
        String goneUri = gone.location.getUri().toString();
        gone.close();
        awaitPlanSpanning(NODES - 1);
        for (String query : withWide()) {
            String alone = answer(query);
            useDistributed(DistributedComplete.clientFactory(peerAllocator, null));
            assertEquals(alone, answer(query), query);
            assertEquals(1, distributed.mergedAnswers(), query);
            assertEquals(0, distributed.fallbacks(), query);
            useDistributed(null);
        }
        assertTrue(!planNodes().contains(goneUri), "the node that left is still planned");
    }

    /**
     * Lists the merged queries and the two wide ones.
     *
     * @return queries
     */
    private static List<String> withWide() {
        List<String> queries = new ArrayList<>(MERGED);
        queries.add(WIDE_GROUPS);
        queries.add(WIDE_ROWS);
        return queries;
    }

    /**
     * Waits until the first node's partial plan of the table has endpoints on the given number of
     * nodes (members joined and inventories published).
     *
     * @param nodes expected nodes
     * @throws Exception when it does not happen within a minute
     */
    private static void awaitPlanSpanning(int nodes) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        Set<String> planned = Set.of();
        while (System.nanoTime() < deadline) {
            planned = planNodes();
            if (planned.size() == nodes) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("the plan spans " + planned + ", not " + nodes + " nodes");
    }

    /**
     * Plans the table on the first node in parts and lists the nodes of the endpoints.
     *
     * @return URIs of the endpoints' first locations
     * @throws Exception on failure
     */
    private static Set<String> planNodes() throws Exception {
        Set<String> nodes = new HashSet<>();
        List<FlightEndpoint> endpoints = NODE.get(0).queryPlanner.plan("SELECT * FROM s.t").endpoints();
        for (FlightEndpoint endpoint : endpoints) {
            nodes.add(endpoint.getLocations().get(0).getUri().toString());
        }
        for (FlightEndpoint endpoint : endpoints) {
            // Planning reserved the endpoints' loads; nothing reads them.
            byte[] handle = com.google.protobuf.Any.parseFrom(endpoint.getTicket().getBytes())
                    .unpack(org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery.class)
                    .getStatementHandle().toByteArray();
            NODE.get(0).clusterService.releaseEndpointLoad(handle,
                    NODE.get(0).clusterService.resolveEndpointHandle(handle));
        }
        return nodes;
    }

    /**
     * Opens peer clients that reach one node through a relay and the others directly.
     *
     * @param node node behind the relay
     * @param relay relay to that node
     * @return client factory
     */
    private static Function<Location, FlightClient> through(TestFlightServerHelper node, CuttingRelay relay) {
        Function<Location, FlightClient> direct = DistributedComplete.clientFactory(peerAllocator, null);
        return location -> direct.apply(port(location) == port(node)
                ? Location.forGrpcInsecure("localhost", relay.port()) : location);
    }

    /**
     * Checks how a query whose peer stream was cut was answered: merged only after a failover to
     * a backup location, else by the single-node fallback.
     *
     * @param answered queries that got an answer (merged or fallen back)
     */
    private static void assertCutAnswered(int answered) {
        assertEquals(answered, distributed.mergedAnswers() + distributed.fallbacks(),
                "merged " + distributed.mergedAnswers() + ", fell back " + distributed.fallbacks());
        if (distributed.mergedAnswers() > 0) {
            assertTrue(distributed.failovers() > 0, "merged after a cut without a failover");
        }
    }

    /**
     * Installs a distributed path on the first node, closing the previous one.
     *
     * @param factory peer client factory, or null to turn the path off
     * @throws Exception on failure
     */
    private static void useDistributed(Function<Location, FlightClient> factory) throws Exception {
        TestFlightServerHelper first = NODE.get(0);
        first.flightSqlProducer.useDistributedComplete(null);
        if (distributed != null) {
            distributed.close();
            distributed = null;
        }
        if (factory != null) {
            distributed = new DistributedComplete(first.queryPlanner, first.clusterService,
                    first.executionService, peerAllocator, factory);
            first.flightSqlProducer.useDistributedComplete(distributed);
        }
    }

    /**
     * Reads the first node's complete answer of a query as text: the schema, then the rows, sorted
     * unless the query orders them.
     *
     * @param query client SQL
     * @return schema and rows
     * @throws Exception on failure
     */
    private static String answer(String query) throws Exception {
        FlightInfo info = NODE.get(0).sqlClient().execute(query);
        assertEquals(1, info.getEndpoints().size());
        StringBuilder rows = new StringBuilder();
        String schema;
        try (FlightStream stream = NODE.get(0).sqlClient().getStream(info.getEndpoints().get(0).getTicket())) {
            schema = stream.getSchema().toString();
            while (stream.next()) {
                rows.append(stream.getRoot().contentToTSVString().lines().skip(1)
                        .reduce(new StringBuilder(), (b, l) -> b.append(l).append('\n'), StringBuilder::append));
            }
        }
        String[] lines = rows.toString().split("\n");
        if (!query.contains("ORDER BY")) {
            Arrays.sort(lines);
        }
        return schema + "\n" + lines.length + " rows\n" + String.join("\n", lines);
    }

    /**
     * Returns the Flight port of a node.
     *
     * @param node node
     * @return port
     */
    private static int port(TestFlightServerHelper node) {
        return node.location.getUri().getPort();
    }

    /**
     * Returns the port of a location.
     *
     * @param location location
     * @return port
     */
    private static int port(Location location) {
        return location.getUri().getPort();
    }

    /**
     * Finds a free local port.
     *
     * @return port
     * @throws Exception on failure
     */
    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
