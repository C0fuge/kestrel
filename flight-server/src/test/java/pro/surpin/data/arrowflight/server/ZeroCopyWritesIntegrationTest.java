package pro.surpin.data.arrowflight.server;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks {@code flightZeroCopyWrites}: gRPC sends DuckDB's batch buffers without copying them, so
 * a batch must stay intact while DuckDB computes the next ones. Readers that take their time keep
 * many batches queued on the server; the answers must equal those of a server that copies, for
 * mixed column types, both result modes, plain and compressed streams.
 */
@Tag("integration")
class ZeroCopyWritesIntegrationTest {

    private static final long ROWS = 120_000;
    private static final int FILES = 4;
    private static final List<String> QUERIES = List.of(
            "SELECT * FROM z.wide",
            "SELECT name, count(*) AS n, sum(amount) AS s FROM z.wide GROUP BY name ORDER BY name",
            "SELECT id, pair, rec FROM z.wide WHERE id % 3 = 1 ORDER BY id DESC LIMIT 5000");

    /**
     * Runs the queries on a copying server and on a zero-copy server and compares the answers.
     *
     * @param dir temporary data directory
     * @throws Exception on failure
     */
    @Test
    @Timeout(value = 4, unit = TimeUnit.MINUTES)
    void zeroCopyStreamsGiveTheSameAnswers(@TempDir Path dir) throws Exception {
        Path table = Files.createDirectories(dir.resolve("z").resolve("wide"));
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = duck.createStatement()) {
            for (int f = 0; f < FILES; f++) {
                st.execute("COPY (SELECT r::BIGINT AS id, (r % 97)::INTEGER AS k,"
                        + " CASE WHEN r % 11 = 0 THEN NULL ELSE 'name-' || (r % 1000) END AS name,"
                        + " (r * 1.25)::DECIMAL(18,2) AS amount, r / 7.0 AS ratio,"
                        + " DATE '2020-01-01' + (r % 3000)::INTEGER AS day,"
                        + " [r % 5, r % 7] AS pair, {'a': r % 13, 'b': 'x' || (r % 17)} AS rec"
                        + " FROM range(" + f * ROWS / FILES + ", " + (f + 1) * ROWS / FILES + ") t(r))"
                        + " TO '" + table.resolve("part-" + f + ".parquet") + "' (FORMAT parquet)");
            }
        }
        System.setProperty("endpointsPerServer", String.valueOf(FILES));
        // Small batches: every stream sends many, so many sit in gRPC while DuckDB runs on.
        System.setProperty("batchSize", "1024");
        try {
            for (String mode : List.of("partial", "complete")) {
                for (String codec : List.of("none", "zstd")) {
                    List<String> copied = answers(dir, mode, codec, false);
                    List<String> zeroCopy = answers(dir, mode, codec, true);
                    assertEquals(copied, zeroCopy, mode + " " + codec);
                }
            }
        } finally {
            System.clearProperty("endpointsPerServer");
            System.clearProperty("batchSize");
        }
    }

    /**
     * Starts a server and returns every query's answer, its endpoints read in parallel by slow
     * readers.
     *
     * @param dir data directory
     * @param mode result mode
     * @param codec arrow-compression header value
     * @param zeroCopy flightZeroCopyWrites
     * @return one line per query: row count and hash of the rows
     * @throws Exception on failure
     */
    private static List<String> answers(Path dir, String mode, String codec, boolean zeroCopy)
            throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(FILES);
        try (TestFlightServerHelper helper = TestFlightServerHelper.builder()
                .dataDir(dir.toString()).resultMode(mode).threads(2)
                .zeroCopyWrites(zeroCopy).start()) {
            FlightSqlClient client = helper.sqlClient();
            FlightCallHeaders headers = new FlightCallHeaders();
            headers.insert("x-flight-result-mode", mode);
            headers.insert("arrow-compression", codec);
            CallOption header = new HeaderCallOption(headers);
            List<String> out = new ArrayList<>();
            for (String sql : QUERIES) {
                FlightInfo info = client.execute(sql, header);
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
            return out;
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Reads one endpoint as text rows, pausing after the first batch so the server queues more.
     *
     * @param client Flight SQL client
     * @param endpoint endpoint
     * @param header call headers
     * @return rows in arrival order
     * @throws Exception on failure
     */
    private static List<String> read(FlightSqlClient client, FlightEndpoint endpoint, CallOption header)
            throws Exception {
        List<String> rows = new ArrayList<>();
        try (FlightStream stream = client.getStream(endpoint.getTicket(), header)) {
            boolean first = true;
            while (stream.next()) {
                if (first) {
                    Thread.sleep(300);
                    first = false;
                }
                VectorSchemaRoot root = stream.getRoot();
                for (int r = 0; r < root.getRowCount(); r++) {
                    StringBuilder row = new StringBuilder();
                    for (FieldVector vector : root.getFieldVectors()) {
                        row.append(vector.getObject(r)).append('|');
                    }
                    rows.add(row.toString());
                }
            }
        }
        return rows;
    }
}
