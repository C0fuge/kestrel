package pro.surpin.data.arrowflight.server;

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
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks that queries moved to the low-thread DuckDB connection by {@code duckDbThreadsUnderLoad}
 * return the same answers as without it: a scan split into several endpoints read at once, an
 * ordered query and an aggregate, in both result modes.
 */
@Tag("integration")
class ThreadsUnderLoadIntegrationTest {

    private static final long ROWS = 200_000;
    private static final int FILES = 4;
    private static final List<String> QUERIES = List.of(
            "SELECT * FROM s.items",
            "SELECT id, label FROM s.items WHERE id % 7 = 3 ORDER BY id DESC LIMIT 1000",
            "SELECT id % 10 AS k, count(*) AS n, sum(id) AS total FROM s.items GROUP BY 1 ORDER BY 1");

    /**
     * Runs the queries with endpoints read in parallel, with and without the setting.
     *
     * @param dir temporary data directory
     * @throws Exception on failure
     */
    @Test
    void queriesUnderLoadGiveTheSameAnswers(@TempDir Path dir) throws Exception {
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
        try {
            for (String mode : List.of("partial", "complete")) {
                List<String> plain = answers(dir, mode, 0);
                List<String> underLoad = answers(dir, mode, 1);
                assertEquals(plain, underLoad, mode);
            }
        } finally {
            System.clearProperty("endpointsPerServer");
        }
    }

    /**
     * Starts a server with two DuckDB threads on one core and returns every query's answer.
     *
     * @param dir data directory
     * @param mode result mode
     * @param threadsUnderLoad duckDbThreadsUnderLoad, 0 = off
     * @return one digest per query
     * @throws Exception on failure
     */
    private static List<String> answers(Path dir, String mode, int threadsUnderLoad) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(FILES);
        try (TestFlightServerHelper helper = TestFlightServerHelper.builder()
                .dataDir(dir.toString()).resultMode(mode).threads(2)
                .threadsUnderLoad(threadsUnderLoad, 1).start()) {
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
            assertEquals(threadsUnderLoad > 0, helper.duckDbAdapter.queriesUnderLoad() > 0,
                    "queries under load");
            assertTrue(out.get(0).contains(" " + ROWS + " rows"), out.get(0));
            return out;
        } finally {
            pool.shutdownNow();
        }
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
                VectorSchemaRoot root = stream.getRoot();
                for (int r = 0; r < root.getRowCount(); r++) {
                    StringBuilder row = new StringBuilder();
                    for (var vector : root.getFieldVectors()) {
                        Object value = vector instanceof BigIntVector b ? Long.valueOf(b.get(r))
                                : vector instanceof VarCharVector v ? new String(v.get(r),
                                        java.nio.charset.StandardCharsets.UTF_8)
                                : vector.getObject(r);
                        row.append(value).append('|');
                    }
                    rows.add(row.toString());
                }
            }
        }
        return rows;
    }
}
