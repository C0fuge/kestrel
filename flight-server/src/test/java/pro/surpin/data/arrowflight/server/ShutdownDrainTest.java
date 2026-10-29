package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The drain of a stopping server (shutdownDrainSeconds, lane 3 D8): found by the rolling-restart
 * run, a server stopped with SIGTERM cut its running answers with {@code INTERNAL} (RST_STREAM),
 * which clients do not retry. With the drain, the server leaves the pool, refuses new calls with
 * {@code UNAVAILABLE} and lets a running answer finish.
 */
@Tag("integration")
class ShutdownDrainTest {

    private static final int FILES = 4;
    private static final int ROWS = 100_000;
    private static final String QUERY = "SELECT v, d, name FROM s.t";
    private Path dataDir;
    private TestFlightServerHelper helper;

    /**
     * Writes a table of four files and starts a complete-mode server with small batches.
     *
     * @throws Exception on failure
     */
    @BeforeEach
    void startServer() throws Exception {
        dataDir = Files.createTempDirectory("shutdown-drain");
        Path table = Files.createDirectories(dataDir.resolve("s").resolve("t"));
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = duck.createStatement()) {
            for (int f = 0; f < FILES; f++) {
                statement.execute("COPY (SELECT (i * " + FILES + " + " + f + ")::BIGINT AS v, (i / 4.0)::DOUBLE AS d,"
                        + " 'n' || (i % 13) AS name FROM range(" + ROWS + ") r(i))"
                        + " TO '" + table.resolve("part-" + f + ".parquet") + "' (FORMAT PARQUET)");
            }
        }
        System.setProperty("batchSize", "1024");
        try {
            helper = TestFlightServerHelper.builder().dataDir(dataDir.toString()).resultMode("complete").start();
        } finally {
            System.clearProperty("batchSize");
        }
    }

    /**
     * Stops the server and removes the table.
     *
     * @throws Exception on failure
     */
    @AfterEach
    void stopServer() throws Exception {
        try {
            helper.close();
        } finally {
            try (var files = Files.walk(dataDir)) {
                files.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    /**
     * Verifies the drain waits for an answer the client is reading, leaves the pool and refuses
     * new calls with {@code UNAVAILABLE}, and the answer arrives in full.
     *
     * @throws Exception on failure
     */
    @Test
    void drainLetsARunningAnswerFinishAndRefusesNewCalls() throws Exception {
        String self = helper.location.getUri().toString();
        assertTrue(helper.clusterService.liveServerUris().contains(self), "registered before the drain");
        FlightInfo info = helper.sqlClient().execute(QUERY);
        long rows = 0;
        CompletableFuture<Boolean> drained;
        try (FlightStream stream = helper.sqlClient().getStream(info.getEndpoints().get(0).getTicket())) {
            assertTrue(stream.next(), "first batch");
            rows += stream.getRoot().getRowCount();
            drained = CompletableFuture.supplyAsync(
                    () -> HadoopArrowFlightServer.drain(helper.server, helper.clusterService, 30));
            Thread.sleep(1000);
            assertFalse(drained.isDone(), "the drain ended while an answer was still being read");
            assertFalse(helper.clusterService.liveServerUris().contains(self), "still in the pool while draining");
            FlightRuntimeException refused = assertThrows(FlightRuntimeException.class,
                    () -> helper.sqlClient().execute(QUERY));
            assertEquals(FlightStatusCode.UNAVAILABLE, refused.status().code(), refused.getMessage());
            while (stream.next()) {
                rows += stream.getRoot().getRowCount();
            }
        }
        assertEquals((long) FILES * ROWS, rows, "the running answer arrived in full");
        assertTrue(drained.get(10, TimeUnit.SECONDS), "drained in time");
    }

    /**
     * Verifies a drain without running calls returns at once.
     */
    @Test
    void drainWithoutRunningCallsEndsAtOnce() {
        long t0 = System.nanoTime();
        assertTrue(HadoopArrowFlightServer.drain(helper.server, helper.clusterService, 30));
        assertTrue(System.nanoTime() - t0 < TimeUnit.SECONDS.toNanos(10), "drain took too long");
        assertFalse(helper.clusterService.liveServerUris().contains(helper.location.getUri().toString()));
    }
}
