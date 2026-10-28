package pro.surpin.data.arrowflight.server;

import java.nio.charset.StandardCharsets;
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
import java.util.concurrent.TimeUnit;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks {@code duckDbProducingStreams}: with one stream allowed inside DuckDB at a time, endpoints
 * read in parallel give the same answers as without the cap in both result modes, and a client that
 * reads all endpoints of a scan in turn on one thread is not blocked by streams waiting for it.
 */
@Tag("integration")
class ProducingStreamsIntegrationTest {

    private static final long ROWS = 200_000;
    private static final int FILES = 4;
    private static final List<String> QUERIES = List.of(
            "SELECT * FROM s.items",
            "SELECT id, label FROM s.items WHERE id % 7 = 3 ORDER BY id DESC LIMIT 1000",
            "SELECT id % 10 AS k, count(*) AS n, sum(id) AS total FROM s.items GROUP BY 1 ORDER BY 1");

    /**
     * Runs the queries with endpoints read in parallel, with and without the cap, and one scan
     * whose endpoints are read in turn.
     *
     * @param dir temporary data directory
     * @throws Exception on failure
     */
    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void cappedStreamsGiveTheSameAnswers(@TempDir Path dir) throws Exception {
        Path table = dir.resolve("s").resolve("items");
        Files.createDirectories(table);
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = duck.createStatement()) {
            for (int f = 0; f < FILES; f++) {
                st.execute(String.format(Locale.ROOT,
                        "COPY (SELECT i AS id, 'label ' || (i %% 977) AS label FROM range(%d, %d) t(i))"
                                + " TO '%s' (FORMAT parquet, ROW_GROUP_SIZE 20000)",
                        f * ROWS / FILES, (f + 1) * ROWS / FILES, table.resolve("part-" + f + ".parquet")));
            }
        }
        System.setProperty("endpointsPerServer", String.valueOf(FILES));
        // Small batches: every endpoint sends many, so streams take turns many times.
        System.setProperty("batchSize", "2048");
        try {
            for (String mode : List.of("partial", "complete")) {
                List<String> plain = answers(dir, mode, 0);
                List<String> capped = answers(dir, mode, 1);
                assertEquals(plain, capped, mode);
            }
        } finally {
            System.clearProperty("endpointsPerServer");
            System.clearProperty("batchSize");
        }
    }

    /**
     * Starts a server and returns every query's answer, then reads one scan's endpoints in turn.
     *
     * @param dir data directory
     * @param mode result mode
     * @param producingStreams duckDbProducingStreams, 0 = no cap
     * @return one digest per query, then the digest of the scan read in turn
     * @throws Exception on failure
     */
    private static List<String> answers(Path dir, String mode, int producingStreams) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(FILES);
        try (TestFlightServerHelper helper = TestFlightServerHelper.builder()
                .dataDir(dir.toString()).resultMode(mode).threads(2)
                .producingStreams(producingStreams).start()) {
            FlightSqlClient client = helper.sqlClient();
            FlightCallHeaders headers = new FlightCallHeaders();
            headers.insert("x-flight-result-mode", mode);
            CallOption header = new HeaderCallOption(headers);
            List<String> out = new ArrayList<>();
            for (String sql : QUERIES) {
                FlightInfo info = client.execute(sql, header);
                if ("partial".equals(mode) && sql.equals(QUERIES.get(0))) {
                    assertEquals(FILES, info.getEndpoints().size(), "endpoints of the scan");
                }
                List<Future<List<String>>> parts = new ArrayList<>();
                for (FlightEndpoint endpoint : info.getEndpoints()) {
                    parts.add(pool.submit(() -> read(client, endpoint, header)));
                }
                List<String> rows = new ArrayList<>();
                for (Future<List<String>> part : parts) {
                    rows.addAll(part.get());
                }
                if (!sql.contains("ORDER BY")) {
                    rows.sort(null);
                }
                out.add(sql + " -> " + rows.size() + " rows, hash " + rows.hashCode());
            }
            out.add("in turn -> " + readInTurn(client, client.execute(QUERIES.get(0), header), header));
            assertTrue(out.get(0).contains(" " + ROWS + " rows"), out.get(0));
            assertEquals(producingStreams, helper.duckDbAdapter.producingGate().permits());
            return out;
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Opens every endpoint's stream and reads one batch of each in turn on this thread.
     *
     * @param client Flight SQL client
     * @param info the query's endpoints
     * @param header result-mode header
     * @return row count and hash of the sorted rows
     * @throws Exception on failure
     */
    private static String readInTurn(FlightSqlClient client, FlightInfo info, CallOption header)
            throws Exception {
        List<FlightStream> streams = new ArrayList<>();
        List<String> rows = new ArrayList<>();
        try {
            for (FlightEndpoint endpoint : info.getEndpoints()) {
                streams.add(client.getStream(endpoint.getTicket(), header));
            }
            List<FlightStream> open = new ArrayList<>(streams);
            while (!open.isEmpty()) {
                for (var it = open.iterator(); it.hasNext();) {
                    FlightStream stream = it.next();
                    if (stream.next()) {
                        addRows(stream.getRoot(), rows);
                    } else {
                        it.remove();
                    }
                }
            }
        } finally {
            for (FlightStream stream : streams) {
                stream.close();
            }
        }
        rows.sort(null);
        return rows.size() + " rows, hash " + rows.hashCode();
    }

    /**
     * Reads one endpoint as text rows.
     *
     * @param client Flight SQL client
     * @param endpoint endpoint
     * @param header result-mode header
     * @return rows in arrival order
     * @throws Exception on failure
     */
    private static List<String> read(FlightSqlClient client, FlightEndpoint endpoint, CallOption header)
            throws Exception {
        List<String> rows = new ArrayList<>();
        try (FlightStream stream = client.getStream(endpoint.getTicket(), header)) {
            while (stream.next()) {
                addRows(stream.getRoot(), rows);
            }
        }
        return rows;
    }

    /**
     * Appends a batch's rows as text.
     *
     * @param root batch
     * @param rows destination
     */
    private static void addRows(VectorSchemaRoot root, List<String> rows) {
        for (int r = 0; r < root.getRowCount(); r++) {
            StringBuilder row = new StringBuilder();
            for (var vector : root.getFieldVectors()) {
                Object value = vector instanceof BigIntVector b ? Long.valueOf(b.get(r))
                        : vector instanceof VarCharVector v ? new String(v.get(r), StandardCharsets.UTF_8)
                        : vector.getObject(r);
                row.append(value).append('|');
            }
            rows.add(row.toString());
        }
    }
}
