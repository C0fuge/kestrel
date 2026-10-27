package pro.surpin.data.arrowflight.server;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Measures one client streaming a full table scan over Flight SQL (complete mode) with DuckDB's
 * default scan order and read-ahead versus the opt-ins {@code duckDbPreserveInsertionOrder=false}
 * and {@code duckDbStreamingBufferSize=64MiB} (STATUS item 16). Prints MB/s per variant; asserts
 * only that every variant returns the same rows. {@code -Dperf.variants=true:,true:16MiB} picks
 * other variants, {@code -Dperf.threads}, {@code -Dperf.rows} and {@code -Dperf.runs} the rest.
 */
@Tag("perf")
class ScanThroughputPerfTest {

    private static final long ROWS = Long.getLong("perf.rows", 6_000_000L);
    private static final int FILES = 8;
    private static final int RUNS = Integer.getInteger("perf.runs", 5);
    /** Variants as preserveInsertionOrder:streamingBufferSize, empty size = DuckDB's default. */
    private static final String VARIANTS = "true:,false:,true:64MiB,false:64MiB";

    /**
     * Prints the scan throughput of each variant.
     *
     * @param dir temporary data directory
     * @throws Exception on failure
     */
    @Test
    void printScanThroughput(@TempDir Path dir) throws Exception {
        Path table = dir.resolve("s").resolve("lineitem");
        Files.createDirectories(table);
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = duck.createStatement()) {
            for (int f = 0; f < FILES; f++) {
                st.execute(String.format(Locale.ROOT,
                        "COPY (SELECT i AS l_orderkey, i %% 200000 AS l_partkey, i %% 7 + 1 AS l_linenumber,"
                                + " (i * 13 %% 5000) / 100.0 AS l_quantity, (i * 7 %% 100000) / 10.0"
                                + " AS l_extendedprice, DATE '1992-01-01' + CAST(i %% 2500 AS INTEGER)"
                                + " AS l_shipdate, 'comment ' || (i %% 9973) AS l_comment"
                                + " FROM range(%d, %d) t(i)) TO '%s' (FORMAT parquet, ROW_GROUP_SIZE 122880)",
                        f * ROWS / FILES, (f + 1) * ROWS / FILES, table.resolve("part-" + f + ".parquet")));
            }
        }
        String threads = System.getProperty("perf.threads", "4");
        long[] expected = null;
        for (String spec : System.getProperty("perf.variants", VARIANTS).split(",")) {
            List<String> variant = List.of(spec.split(":", -1));
            System.setProperty("duckDbPreserveInsertionOrder", variant.get(0));
            System.setProperty("duckDbStreamingBufferSize", variant.get(1));
            try (TestFlightServerHelper helper = TestFlightServerHelper.builder()
                    .dataDir(dir.toString()).resultMode("complete")
                    .threads(Integer.parseInt(threads)).start()) {
                FlightSqlClient client = helper.sqlClient();
                FlightCallHeaders headers = new FlightCallHeaders();
                headers.insert("x-flight-result-mode", "complete");
                CallOption header = new HeaderCallOption(headers);
                long[] mbps = new long[RUNS];
                long batches = 0;
                for (int i = -1; i < RUNS; i++) {
                    long t0 = System.nanoTime();
                    long[] got = scan(client, header);
                    double seconds = (System.nanoTime() - t0) / 1e9;
                    if (expected == null) {
                        expected = got;
                    }
                    assertEquals(expected[0], got[0], "rows " + variant);
                    assertEquals(expected[1], got[1], "sum of l_orderkey " + variant);
                    if (i >= 0) {
                        mbps[i] = Math.round(got[2] / seconds / 1e6);
                        batches = got[3];
                    }
                }
                Arrays.sort(mbps);
                System.out.printf(Locale.ROOT, "threads %s preserveInsertionOrder=%-5s"
                                + " streamingBufferSize=%-7s scan MB/s median %d (min %d, max %d), %d batches%n",
                        threads, variant.get(0), variant.get(1).isEmpty() ? "default" : variant.get(1),
                        mbps[RUNS / 2], mbps[0], mbps[RUNS - 1], batches);
            } finally {
                System.clearProperty("duckDbPreserveInsertionOrder");
                System.clearProperty("duckDbStreamingBufferSize");
            }
        }
    }

    /**
     * Streams the whole table once.
     *
     * @param client Flight SQL client
     * @param header result-mode header
     * @return rows, sum of l_orderkey, Arrow buffer bytes received, batches
     * @throws Exception on failure
     */
    private static long[] scan(FlightSqlClient client, CallOption header) throws Exception {
        long rows = 0;
        long sum = 0;
        long bytes = 0;
        long batches = 0;
        FlightInfo info = client.execute("SELECT * FROM s.lineitem", header);
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(endpoint.getTicket(), header)) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    rows += root.getRowCount();
                    batches++;
                    BigIntVector keys = (BigIntVector) root.getVector("l_orderkey");
                    for (int r = 0; r < root.getRowCount(); r++) {
                        sum += keys.get(r);
                    }
                    for (FieldVector vector : root.getFieldVectors()) {
                        bytes += vector.getBufferSize();
                    }
                }
            }
        }
        return new long[] {rows, sum, bytes, batches};
    }
}
