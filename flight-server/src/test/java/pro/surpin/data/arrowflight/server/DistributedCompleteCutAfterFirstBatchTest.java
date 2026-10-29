package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.server.services.DistributedComplete;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Forces the after-first-batch failure path of distributed complete mode (lane 3, D5): the
 * endpoint answers reach the merging node through a relay that passes them slowly, the node's
 * batches are small so that DuckDB sends the client its first batch long before the endpoint
 * answers are read, and the relay resets its connections as soon as that first batch arrived. The
 * client must get an error, never the rows it already has as if they were the whole answer, and
 * the node must not fall back (the client's stream has started).
 */
@Tag("integration")
class DistributedCompleteCutAfterFirstBatchTest {

    private static final int FILES = 4;
    private static final int ROWS = 100_000;
    private static final String QUERY = "SELECT v, d, name FROM s.t WHERE k < 8";
    /** Rate of the relayed endpoint answers: their ~7 MB take several seconds to pass. */
    private static final long BYTES_PER_SECOND = 1024 * 1024;
    private static Path dataDir;
    private static TestFlightServerHelper helper;
    private static BufferAllocator peerAllocator;

    /**
     * Writes a table of four files and starts a complete-mode server with four endpoints per node
     * and batches of 1024 rows.
     *
     * @throws Exception on failure
     */
    @BeforeAll
    static void startServer() throws Exception {
        dataDir = Files.createTempDirectory("distributed-cut");
        Path table = Files.createDirectories(dataDir.resolve("s").resolve("t"));
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = duck.createStatement()) {
            for (int f = 0; f < FILES; f++) {
                statement.execute("COPY (SELECT (i % 10)::INTEGER AS k,"
                        + " (i * " + FILES + " + " + f + ")::BIGINT AS v, (i / 4.0)::DOUBLE AS d,"
                        + " 'n' || (i % 13) AS name FROM range(" + ROWS + ") r(i))"
                        + " TO '" + table.resolve("part-" + f + ".parquet") + "' (FORMAT PARQUET)");
            }
        }
        System.setProperty("endpointsPerServer", String.valueOf(FILES));
        System.setProperty("batchSize", "1024");
        try {
            helper = TestFlightServerHelper.builder().dataDir(dataDir.toString()).resultMode("complete")
                    .start();
        } finally {
            System.clearProperty("endpointsPerServer");
            System.clearProperty("batchSize");
        }
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
            helper.flightSqlProducer.useDistributedComplete(null);
            peerAllocator.close();
            helper.close();
        } finally {
            try (var files = Files.walk(dataDir)) {
                files.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    /**
     * Verifies a reset of the endpoint answers after the client's first batch fails the query:
     * the client sees an error after some rows, the node counts neither a merge nor a fallback,
     * and the error does not blame the client's query.
     *
     * @throws Exception on failure
     */
    @Test
    void endpointAnswersCutAfterTheFirstBatchFailTheQuery() throws Exception {
        long total = rowCount();
        try (CuttingRelay relay = new CuttingRelay(helper.location.getUri().getPort(), Long.MAX_VALUE,
                        BYTES_PER_SECOND);
                DistributedComplete distributed = install(relay)) {
            FlightInfo info = helper.sqlClient().execute(QUERY);
            long rows = 0;
            FlightRuntimeException failure = null;
            long relayedAtCut = -1;
            try (FlightStream stream = helper.sqlClient().getStream(info.getEndpoints().get(0).getTicket())) {
                while (stream.next()) {
                    rows += stream.getRoot().getRowCount();
                    if (relayedAtCut < 0) {
                        relayedAtCut = relay.relayed();
                        relay.cutNow();
                    }
                }
            } catch (FlightRuntimeException e) {
                failure = e;
            }
            assertTrue(relayedAtCut >= 0, "the client got no batch before the end (" + rows + " rows)");
            assertNotNull(failure, "the cut answer ended without an error after " + rows + " of " + total
                    + " rows (relayed " + relayedAtCut + " bytes before the cut)");
            assertTrue(rows > 0 && rows < total, "rows before the error: " + rows + " of " + total);
            assertNotEquals(FlightStatusCode.INVALID_ARGUMENT, failure.status().code(),
                    "a lost endpoint answer is not the client's fault: " + failure.getMessage());
            assertEquals(0, distributed.mergedAnswers(), "merged");
            assertEquals(0, distributed.fallbacks(), "fell back after the first batch");
        } finally {
            helper.flightSqlProducer.useDistributedComplete(null);
        }
        // The node still answers the query in full afterwards, merged from healthy endpoints.
        try (DistributedComplete distributed = new DistributedComplete(helper.queryPlanner,
                helper.clusterService, helper.executionService, peerAllocator,
                DistributedComplete.clientFactory(peerAllocator, null))) {
            helper.flightSqlProducer.useDistributedComplete(distributed);
            assertEquals(total, rowCount());
            assertEquals(1, distributed.mergedAnswers());
        } finally {
            helper.flightSqlProducer.useDistributedComplete(null);
        }
    }

    /**
     * Installs a distributed path that reads every endpoint answer through a relay.
     *
     * @param relay relay to this node
     * @return the installed path
     */
    private static DistributedComplete install(CuttingRelay relay) {
        Function<Location, FlightClient> direct = DistributedComplete.clientFactory(peerAllocator, null);
        DistributedComplete distributed = new DistributedComplete(helper.queryPlanner,
                helper.clusterService, helper.executionService, peerAllocator,
                location -> direct.apply(Location.forGrpcInsecure("localhost", relay.port())));
        helper.flightSqlProducer.useDistributedComplete(distributed);
        return distributed;
    }

    /**
     * Counts the rows of the query's complete answer on the current path.
     *
     * @return rows
     * @throws Exception on failure
     */
    private static long rowCount() throws Exception {
        FlightInfo info = helper.sqlClient().execute(QUERY);
        assertEquals(1, info.getEndpoints().size());
        long rows = 0;
        try (FlightStream stream = helper.sqlClient().getStream(info.getEndpoints().get(0).getTicket())) {
            while (stream.next()) {
                rows += stream.getRoot().getRowCount();
            }
        }
        return rows;
    }
}
