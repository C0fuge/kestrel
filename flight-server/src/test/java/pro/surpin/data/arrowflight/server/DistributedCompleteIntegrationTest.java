package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.server.services.DistributedComplete;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests distributed complete mode (item 18 step 2) end to end on one node whose plans have four
 * endpoints: the node reads its own endpoints over Flight, merges them, and answers exactly as it
 * does alone; a peer that cannot be reached before the first batch falls back to that answer.
 */
@Tag("integration")
class DistributedCompleteIntegrationTest {

    private static final int FILES = 4;
    private static Path dataDir;
    private static TestFlightServerHelper helper;
    private static BufferAllocator peerAllocator;
    private static DistributedComplete distributed;

    /**
     * Writes a table of four files and starts a complete-mode server with four endpoints per node.
     *
     * @throws Exception on failure
     */
    @BeforeAll
    static void startServer() throws Exception {
        dataDir = Files.createTempDirectory("distributed-complete");
        Path table = Files.createDirectories(dataDir.resolve("s").resolve("t"));
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = duck.createStatement()) {
            for (int f = 0; f < FILES; f++) {
                // File 3 has no row with k = 9; v is unique over the table.
                statement.execute("COPY (SELECT (i % " + (f == 3 ? 9 : 10) + ")::INTEGER AS k,"
                        + " (i * " + FILES + " + " + f + ")::BIGINT AS v, (i / 4.0)::DOUBLE AS d,"
                        + " 'n' || (i % 13) AS name FROM range(1000) r(i))"
                        + " TO '" + table.resolve("part-" + f + ".parquet") + "' (FORMAT PARQUET)");
            }
        }
        System.setProperty("endpointsPerServer", String.valueOf(FILES));
        helper = TestFlightServerHelper.builder().dataDir(dataDir.toString()).resultMode("complete")
                .start();
        peerAllocator = helper.allocator.newChildAllocator("peers", 0, Long.MAX_VALUE);
    }

    /**
     * Stops the server and removes the table.
     *
     * @throws Exception on failure
     */
    @AfterAll
    static void stopServer() throws Exception {
        try {
            useDistributed(null);
            peerAllocator.close();
            helper.close();
        } finally {
            System.clearProperty("endpointsPerServer");
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

    /** Queries whose shape merges. */
    private static final List<String> MERGED = List.of(
            "SELECT count(*) FROM s.t",
            "SELECT count(*) AS n, sum(v) AS total, min(d) AS lo, max(name) AS hi FROM s.t",
            "SELECT k, count(*) AS n, sum(v) AS total, min(v) AS lo, max(d) AS hi FROM s.t"
                    + " GROUP BY k ORDER BY k",
            "SELECT DISTINCT name FROM s.t ORDER BY name",
            "SELECT v, name FROM s.t WHERE v % 7 = 0 ORDER BY v DESC LIMIT 5",
            "SELECT v, k FROM s.t WHERE v < 40 ORDER BY 1",
            "SELECT k, count(*) AS n FROM s.t WHERE k = 9 GROUP BY k ORDER BY k");

    /**
     * Verifies merged answers equal the answers the node computes alone.
     *
     * @throws Exception on failure
     */
    @Test
    void mergedAnswerEqualsSingleNodeAnswer() throws Exception {
        for (String query : MERGED) {
            String alone = answer(query);
            useDistributed(DistributedComplete.clientFactory(peerAllocator, null));
            String merged = answer(query);
            assertEquals(alone, merged, query);
            assertEquals(1, distributed.mergedAnswers(), "merged from endpoint answers: " + query);
            assertEquals(0, distributed.fallbacks(), query);
            useDistributed(null);
        }
    }

    /**
     * Verifies a query whose shape does not merge keeps the single-node path untouched.
     *
     * @throws Exception on failure
     */
    @Test
    void refusedShapeKeepsSingleNodePath() throws Exception {
        for (String query : List.of("SELECT k, count(*) AS n FROM s.t GROUP BY k ORDER BY k LIMIT 3",
                "SELECT avg(v) AS a FROM s.t")) {
            String alone = answer(query);
            useDistributed(DistributedComplete.clientFactory(peerAllocator, null));
            assertEquals(alone, answer(query), query);
            assertEquals(0, distributed.mergedAnswers(), query);
            assertEquals(0, distributed.fallbacks(), query);
            useDistributed(null);
        }
    }

    /**
     * Verifies an unreachable peer before the first batch falls back to the single-node answer.
     *
     * @throws Exception on failure
     */
    @Test
    void unreachablePeerFallsBackToSingleNodeAnswer() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        Location dead = Location.forGrpcInsecure("localhost", closedPort);
        Function<Location, FlightClient> factory = DistributedComplete.clientFactory(peerAllocator, null);
        for (String query : List.of(
                "SELECT k, count(*) AS n, sum(v) AS total FROM s.t GROUP BY k ORDER BY k",
                "SELECT v FROM s.t WHERE v < 40 ORDER BY v")) {
            String alone = answer(query);
            useDistributed(location -> factory.apply(dead));
            assertEquals(alone, answer(query), query);
            assertEquals(0, distributed.mergedAnswers(), query);
            assertEquals(1, distributed.fallbacks(), query);
            useDistributed(null);
        }
    }

    /**
     * Installs a distributed path on the producer, closing the previous one.
     *
     * @param factory peer client factory, or null to turn the path off
     * @throws Exception on failure
     */
    private static void useDistributed(Function<Location, FlightClient> factory) throws Exception {
        helper.flightSqlProducer.useDistributedComplete(null);
        if (distributed != null) {
            distributed.close();
            distributed = null;
        }
        if (factory != null) {
            distributed = new DistributedComplete(helper.queryPlanner, helper.clusterService,
                    helper.executionService, peerAllocator, factory);
            helper.flightSqlProducer.useDistributedComplete(distributed);
        }
    }

    /**
     * Reads the complete answer of a query as text, with its schema.
     *
     * @param query client SQL
     * @return schema and rows
     * @throws Exception on failure
     */
    private static String answer(String query) throws Exception {
        FlightInfo info = helper.sqlClient().execute(query);
        assertEquals(1, info.getEndpoints().size());
        StringBuilder text = new StringBuilder();
        try (FlightStream stream = helper.sqlClient().getStream(info.getEndpoints().get(0).getTicket())) {
            text.append(stream.getSchema()).append('\n');
            while (stream.next()) {
                text.append(stream.getRoot().contentToTSVString());
            }
        }
        return text.toString();
    }
}
