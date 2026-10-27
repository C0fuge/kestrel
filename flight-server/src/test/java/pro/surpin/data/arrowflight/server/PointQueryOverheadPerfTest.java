package pro.surpin.data.arrowflight.server;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.IntFunction;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Measures what a small query costs over Flight SQL compared with plain DuckDB on the same
 * files, phase by phase, as stock JDBC/ADBC clients run it (prepare, GetFlightInfo, DoGet,
 * close). Prints the numbers; asserts only the answers (latency budgets are not reliable in CI).
 */
@Tag("perf")
class PointQueryOverheadPerfTest {

    private static final int ROWS = 400_000;
    private static final int FILES = 4;
    private static final int WARMUP = 200;
    private static final int RUNS = 600;

    /**
     * Prints per-phase latencies of point and filtered-count queries in both result modes.
     *
     * @param dir temporary data directory
     * @throws Exception on failure
     */
    @Test
    void printOverhead(@TempDir Path dir) throws Exception {
        Path table = dir.resolve("s").resolve("orders");
        Files.createDirectories(table);
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = duck.createStatement()) {
            for (int f = 0; f < FILES; f++) {
                st.execute(String.format(Locale.ROOT,
                        "COPY (SELECT i AS o_orderkey, i %% 1000 AS o_custkey, (i * 7 %% 100000) / 10.0"
                                + " AS o_totalprice, 'comment ' || i AS o_comment"
                                + " FROM range(%d, %d) t(i)) TO '%s' (FORMAT parquet, ROW_GROUP_SIZE 25000)",
                        f * ROWS / FILES, (f + 1) * ROWS / FILES, table.resolve("part-" + f + ".parquet")));
            }
            String files = "read_parquet('" + table + "/*.parquet')";
            System.out.println("== plain DuckDB (one connection, threads=4)");
            st.execute("SET threads=4");
            report("duckdb point", i -> {
                try (ResultSet rs = st.executeQuery(
                        "SELECT * FROM " + files + " WHERE o_orderkey = " + key(i))) {
                    rs.next();
                    return rs.getLong(1);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            report("duckdb count", i -> {
                try (ResultSet rs = st.executeQuery(
                        "SELECT count(*) FROM " + files + " WHERE o_totalprice > " + price(i))) {
                    rs.next();
                    return rs.getLong(1);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
        }
        String threads = System.getProperty("perf.threads", "4");
        for (String mode : List.of("complete", "partial")) {
            try (TestFlightServerHelper helper = TestFlightServerHelper.builder()
                    .dataDir(dir.toString()).resultMode(mode).threads(Integer.parseInt(threads)).start()) {
                System.out.println("== Flight SQL, mode " + mode + ", threads " + threads);
                FlightSqlClient client = helper.sqlClient();
                CallOption header = new HeaderCallOption(headers(mode));
                flight("point", client, header, i -> "SELECT * FROM s.orders WHERE o_orderkey = " + key(i));
                flight("count", client, header, i -> "SELECT count(*) FROM s.orders WHERE o_totalprice > "
                        + price(i));
                concurrent(client, header, 8);
                concurrent(client, header, 32);
            }
        }
    }

    /**
     * Returns the order key of a run.
     *
     * @param i run number, negative during warm-up
     * @return existing order key
     */
    private static long key(int i) {
        return Math.floorMod(i * 7919L, (long) ROWS);
    }

    /**
     * Returns the price bound of a run.
     *
     * @param i run number, negative during warm-up
     * @return SQL literal
     */
    private static String price(int i) {
        return String.format(Locale.ROOT, "%.1f", Math.floorMod(i, 9000) + 500.0);
    }

    /**
     * Builds the headers that ask for a result mode.
     *
     * @param mode result mode
     * @return call headers
     */
    private static FlightCallHeaders headers(String mode) {
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert("x-flight-result-mode", mode);
        return headers;
    }

    /**
     * Runs a query shape over Flight phase by phase and prints the percentiles of each phase.
     *
     * @param name label
     * @param client Flight SQL client
     * @param header result-mode header
     * @param sql query text of a run
     * @throws Exception on failure
     */
    private static void flight(String name, FlightSqlClient client, CallOption header,
            IntFunction<String> sql) throws Exception {
        long[][] phases = new long[4][RUNS];
        for (int i = -WARMUP; i < RUNS; i++) {
            long t0 = System.nanoTime();
            FlightSqlClient.PreparedStatement prepared = client.prepare(sql.apply(i), header);
            long t1 = System.nanoTime();
            FlightInfo info = prepared.execute(header);
            long t2 = System.nanoTime();
            long rows = 0;
            for (FlightEndpoint endpoint : info.getEndpoints()) {
                try (FlightStream stream = client.getStream(endpoint.getTicket(), header)) {
                    while (stream.next()) {
                        rows += stream.getRoot().getRowCount();
                    }
                }
            }
            long t3 = System.nanoTime();
            prepared.close(header);
            long t4 = System.nanoTime();
            assertEquals(1, rows, sql.apply(i));
            if (i >= 0) {
                phases[0][i] = t1 - t0;
                phases[1][i] = t2 - t1;
                phases[2][i] = t3 - t2;
                phases[3][i] = t4 - t3;
            }
        }
        long[] total = new long[RUNS];
        for (int i = 0; i < RUNS; i++) {
            total[i] = phases[0][i] + phases[1][i] + phases[2][i] + phases[3][i];
        }
        System.out.printf(Locale.ROOT, "%-14s total %s | prepare %s | info %s | doget %s | close %s%n",
                name, pct(total), pct(phases[0]), pct(phases[1]), pct(phases[2]), pct(phases[3]));
    }

    /**
     * Runs point queries from several clients at once and prints the latency percentiles.
     *
     * @param client Flight SQL client
     * @param header result-mode header
     * @param clients concurrent clients
     * @throws Exception on failure
     */
    private static void concurrent(FlightSqlClient client, CallOption header, int clients)
            throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        try {
            int perClient = Math.max(50, RUNS * 2 / clients);
            List<Future<long[]>> futures = new ArrayList<>();
            for (int c = 0; c < clients; c++) {
                int base = c * 100_003;
                futures.add(pool.submit(() -> {
                    long[] times = new long[perClient];
                    for (int i = -20; i < perClient; i++) {
                        long t0 = System.nanoTime();
                        String sql = "SELECT * FROM s.orders WHERE o_orderkey = " + key(base + i);
                        try (FlightSqlClient.PreparedStatement prepared = client.prepare(sql, header)) {
                            FlightInfo info = prepared.execute(header);
                            for (FlightEndpoint endpoint : info.getEndpoints()) {
                                try (FlightStream stream = client.getStream(endpoint.getTicket(), header)) {
                                    while (stream.next()) {
                                        stream.getRoot().getRowCount();
                                    }
                                }
                            }
                        }
                        if (i >= 0) {
                            times[i] = System.nanoTime() - t0;
                        }
                    }
                    return times;
                }));
            }
            List<Long> all = new ArrayList<>();
            for (Future<long[]> f : futures) {
                for (long t : f.get()) {
                    all.add(t);
                }
            }
            System.out.printf(Locale.ROOT, "point c%-6d total %s%n", clients,
                    pct(all.stream().mapToLong(Long::longValue).toArray()));
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Times a query shape run in plain DuckDB and prints the percentiles.
     *
     * @param name label
     * @param run one run
     */
    private static void report(String name, IntFunction<Long> run) {
        long[] times = new long[RUNS];
        for (int i = -WARMUP; i < RUNS; i++) {
            long t0 = System.nanoTime();
            run.apply(i);
            if (i >= 0) {
                times[i] = System.nanoTime() - t0;
            }
        }
        System.out.printf(Locale.ROOT, "%-14s total %s%n", name, pct(times));
    }

    /**
     * Formats the median and 95th percentile of durations.
     *
     * @param nanos durations in nanoseconds
     * @return text in milliseconds
     */
    private static String pct(long[] nanos) {
        long[] sorted = nanos.clone();
        Arrays.sort(sorted);
        return String.format(Locale.ROOT, "p50 %6.2f p95 %6.2f ms",
                sorted[sorted.length / 2] / 1e6, sorted[(int) (sorted.length * 0.95)] / 1e6);
    }
}
