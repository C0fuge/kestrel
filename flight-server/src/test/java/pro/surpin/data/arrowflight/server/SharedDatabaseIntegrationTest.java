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
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.DuckDbSizing;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks that {@code duckDbSharedDatabase} (every session a connection to one DuckDB of the node)
 * returns the same answers as a DuckDB per session: a scan split into endpoints read at once, an
 * ordered query, an aggregate and a join, in both result modes; and that a client cannot name a
 * session's catalog.
 */
@Tag("integration")
class SharedDatabaseIntegrationTest {

    private static final long ROWS = 200_000;
    private static final int FILES = 4;
    private static final List<String> QUERIES = List.of(
            "SELECT * FROM s.items",
            "SELECT id, label FROM s.items WHERE id % 7 = 3 ORDER BY id DESC LIMIT 1000",
            "SELECT id % 10 AS k, count(*) AS n, sum(id) AS total FROM s.items GROUP BY 1 ORDER BY 1",
            "SELECT a.id, b.label FROM s.items a JOIN s.items b ON a.id = b.id WHERE a.id < 5000");

    /**
     * Runs the queries with endpoints read in parallel, with and without the setting.
     *
     * @param dir temporary data directory
     * @throws Exception on failure
     */
    @Test
    void sharedDatabaseGivesTheSameAnswers(@TempDir Path dir) throws Exception {
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
                assertEquals(answers(dir, mode, null), answers(dir, mode, new DuckDbSizing.Shared(2, "1GiB")),
                        mode);
            }
        } finally {
            System.clearProperty("endpointsPerServer");
        }
    }

    /**
     * Starts a server and returns every query's answer.
     *
     * @param dir data directory
     * @param mode result mode
     * @param shared node-wide sizing of the shared DuckDB, or null for a DuckDB per session
     * @return one digest per query
     * @throws Exception on failure
     */
    private static List<String> answers(Path dir, String mode, DuckDbSizing.Shared shared) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(FILES);
        try (TestFlightServerHelper helper = TestFlightServerHelper.builder()
                .dataDir(dir.toString()).resultMode(mode).threads(2).sharedDatabase(shared).start()) {
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
            assertTrue(out.get(0).contains(" " + ROWS + " rows"), out.get(0));
            if (shared != null) {
                if ("complete".equals(mode)) {
                    String other = DuckDbAdapter.SESSION_CATALOG_PREFIX + "1";
                    FlightRuntimeException e = assertThrows(FlightRuntimeException.class, () -> {
                        FlightInfo info = client.execute("SELECT * FROM " + other + ".s.items", header);
                        read(client, info.getEndpoints().get(0), header);
                    });
                    assertTrue(String.valueOf(e.getMessage()).contains("not allowed"), e.getMessage());
                }
            }
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
                        row.append(vector.getObject(r)).append('|');
                    }
                    rows.add(row.toString());
                }
            }
        }
        return rows;
    }
}
