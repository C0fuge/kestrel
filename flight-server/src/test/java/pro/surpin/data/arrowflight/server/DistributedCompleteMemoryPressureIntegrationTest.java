package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.server.adapters.DuckDbSizing;
import pro.surpin.data.arrowflight.server.services.DistributedComplete;
import pro.surpin.data.arrowflight.server.services.PeerStreamAdmission;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests item 56: concurrent full scans under distributed complete mode on a shared DuckDB with a
 * small memory limit answer completely. A merge reads its endpoint answers one after the other;
 * before item 56 it opened all of them at once, so every endpoint not read yet held a running
 * DuckDB scan, and the shared DuckDB ran out of memory (on this test's 48 MiB about 140 runs
 * again on a private DuckDB and two merges falling back per run; on the stand an endpoint
 * stream failed). Now a node keeps two endpoint streams of a plain scan open at a time.
 */
@Tag("integration")
class DistributedCompleteMemoryPressureIntegrationTest {

    private static final int FILES = 8;
    private static final int ROWS_PER_FILE = 150_000;
    private static final int CLIENTS = 6;
    private static final int ROUNDS = 3;

    /**
     * Runs rounds of concurrent full scans under dc and checks every answer is complete.
     *
     * @param dir temporary data directory
     * @throws Exception on failure
     */
    @Test
    void concurrentFullScansAnswerCompletelyUnderMemoryPressure(@TempDir Path dir) throws Exception {
        Path table = Files.createDirectories(dir.resolve("s").resolve("t"));
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = duck.createStatement()) {
            for (int f = 0; f < FILES; f++) {
                statement.execute(String.format(Locale.ROOT,
                        "COPY (SELECT i AS id, (i %% 97)::INTEGER AS k, md5(i::VARCHAR) AS h1,"
                                + " md5((i + 1)::VARCHAR) || md5((i + 2)::VARCHAR) AS h2, (i / 7.0)::DOUBLE AS d"
                                + " FROM range(%d, %d) r(i)) TO '%s' (FORMAT PARQUET, ROW_GROUP_SIZE 30000)",
                        (long) f * ROWS_PER_FILE, (long) (f + 1) * ROWS_PER_FILE,
                        table.resolve("part-" + f + ".parquet")));
            }
        }
        System.setProperty("endpointsPerServer", String.valueOf(FILES));
        ExecutorService pool = Executors.newFixedThreadPool(CLIENTS);
        try (TestFlightServerHelper helper = TestFlightServerHelper.builder().dataDir(dir.toString())
                .resultMode("complete").threads(4).sharedDatabase(new DuckDbSizing.Shared(4, "48MiB")).start();
                BufferAllocator peers = helper.allocator.newChildAllocator("peers", 0, Long.MAX_VALUE)) {
            Function<Location, FlightClient> factory = DistributedComplete.clientFactory(peers, null);
            try (DistributedComplete distributed = new DistributedComplete(helper.queryPlanner,
                    helper.clusterService, helper.executionService, peers, factory,
                    new PeerStreamAdmission(64, PeerStreamAdmission.DEFAULT_WAIT_MILLIS))) {
                helper.flightSqlProducer.useDistributedComplete(distributed);
                String query = "SELECT * FROM s.t";
                List<String> failures = new ArrayList<>();
                for (int round = 0; round < ROUNDS; round++) {
                    List<Future<Long>> scans = new ArrayList<>();
                    for (int c = 0; c < CLIENTS; c++) {
                        scans.add(pool.submit(() -> rows(helper, query)));
                    }
                    for (Future<Long> scan : scans) {
                        try {
                            assertEquals((long) FILES * ROWS_PER_FILE, scan.get());
                        } catch (java.util.concurrent.ExecutionException e) {
                            failures.add(String.valueOf(e.getCause()));
                        }
                    }
                }
                helper.flightSqlProducer.useDistributedComplete(null);
                assertTrue(failures.isEmpty(), "failed scans: " + failures);
                assertTrue(distributed.mergedAnswers() > 0, "merged answers");
                // Every merged scan kept at most two of its eight endpoint streams open at a time.
                assertEquals(distributed.mergedAnswers(), distributed.scansOpenedInTurn(), "scans opened in turn");
            }
        } finally {
            System.clearProperty("endpointsPerServer");
            pool.shutdownNow();
        }
    }

    /**
     * Reads a query's complete answer and counts its rows, slowly enough that the endpoint
     * answers not being read yet wait.
     *
     * @param helper server
     * @param query client SQL
     * @return rows
     * @throws Exception on failure
     */
    private static long rows(TestFlightServerHelper helper, String query) throws Exception {
        FlightInfo info = helper.sqlClient().execute(query);
        long rows = 0;
        try (FlightStream stream = helper.sqlClient().getStream(info.getEndpoints().get(0).getTicket())) {
            while (stream.next()) {
                rows += stream.getRoot().getRowCount();
                Thread.sleep(1);
            }
        }
        return rows;
    }
}
