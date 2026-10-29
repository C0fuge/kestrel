package pro.surpin.data.arrowflight.server;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Times the TPC-H queries against a server started separately (item 19b's thread floor, the
 * maintainer's laptop stand p19b): {@code -Dperf.server=host:port} names the server,
 * {@code -Dperf.queries} a directory of {@code q01.sql} .. {@code q22.sql} on its tables. Runs
 * one unmeasured pass, then {@code -Dperf.rounds} (default 2) measured passes, and prints one
 * line per query and pass ({@code q01 r1 1234.5 ms rows 4 sum <checksum>}) plus one
 * {@code total r<n> <seconds> s} line per pass. The checksum is order-independent with
 * floating-point values rounded, so runs with other thread counts can be compared. Asserts the
 * row count of every answer: the one in {@code qNN.rows} next to the query when present, else
 * at least one row. {@code -Dperf.clients=N} (default 1) runs each pass on N concurrent clients,
 * each starting at another query, and tags their lines {@code c0} .. {@code c<N-1>}; a pass's
 * total is then its wall-clock time. Failed queries and wrong row counts are printed per query
 * and fail the test at the end.
 */
@Tag("perf")
class TpchPerfTest {

    /**
     * Runs the passes and prints the times.
     *
     * @throws Exception on failure
     */
    @Test
    void printTimes() throws Exception {
        String external = System.getProperty("perf.server");
        String queriesDir = System.getProperty("perf.queries");
        Assumptions.assumeTrue(external != null && queriesDir != null,
                "needs -Dperf.server=host:port and -Dperf.queries=<dir>");
        List<Path> files;
        try (Stream<Path> list = Files.list(Path.of(queriesDir))) {
            files = list.filter(p -> p.getFileName().toString().endsWith(".sql")).sorted().toList();
        }
        assertTrue(!files.isEmpty(), "no .sql files in " + queriesDir);
        List<String> sqls = new ArrayList<>();
        List<Long> expectedRows = new ArrayList<>();
        for (Path f : files) {
            sqls.add(Files.readString(f, StandardCharsets.UTF_8).trim().replaceAll(";\\s*$", ""));
            Path rows = f.resolveSibling(f.getFileName().toString().replace(".sql", ".rows"));
            expectedRows.add(Files.exists(rows) ? Long.parseLong(Files.readString(rows).trim()) : -1L);
        }
        int rounds = Integer.parseInt(System.getProperty("perf.rounds", "2"));
        int clients = Integer.parseInt(System.getProperty("perf.clients", "1"));
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert("x-flight-result-mode", System.getProperty("perf.mode", "complete"));
        CallOption header = new HeaderCallOption(headers);
        String[] hostPort = external.split(":");
        Location location = Location.forGrpcInsecure(hostPort[0], Integer.parseInt(hostPort[1]));
        List<String> names = files.stream().map(f -> f.getFileName().toString().replace(".sql", "")).toList();
        AtomicInteger failures = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        try (BufferAllocator allocator = new RootAllocator()) {
            List<FlightClient> flights = new ArrayList<>();
            try {
                for (int c = 0; c < clients; c++) {
                    flights.add(FlightClient.builder(allocator, location).build());
                }
                for (int r = 0; r <= rounds; r++) {
                    int pass = r;
                    long t0 = System.nanoTime();
                    long[] sums = new long[clients];
                    List<Future<?>> runs = new ArrayList<>();
                    for (int c = 0; c < clients; c++) {
                        int client = c;
                        runs.add(pool.submit(() -> {
                            sums[client] = runPass(new FlightSqlClient(flights.get(client)), header, sqls, names,
                                    expectedRows, pass, client, clients, failures);
                            return null;
                        }));
                    }
                    for (Future<?> run : runs) {
                        run.get();
                    }
                    long nanos = clients == 1 ? sums[0] : System.nanoTime() - t0;
                    System.out.printf(Locale.ROOT, "total r%d %.2f s%s%n", r, nanos / 1e9,
                            r == 0 ? " (warm-up)" : "");
                }
            } finally {
                pool.shutdownNow();
                for (FlightClient flight : flights) {
                    flight.close();
                }
            }
        }
        assertEquals(0, failures.get(), "queries failed or answered wrong row counts");
    }

    /**
     * Runs every query once on one client, from query {@code client * queries / clients} on so
     * concurrent clients run different queries at a time, and prints one line per query; a
     * failed query or a wrong row count is printed and counted instead of ending the pass.
     *
     * @param client      Flight SQL client
     * @param header      result-mode header
     * @param sqls        query texts
     * @param names       query names
     * @param expected    expected row counts, -1 = at least one row
     * @param pass        pass number, 0 = warm-up
     * @param index       index of the client
     * @param clients     number of concurrent clients
     * @param failures    counter of failed queries
     * @return the summed query time in nanoseconds
     */
    private static long runPass(FlightSqlClient client, CallOption header, List<String> sqls, List<String> names,
            List<Long> expected, int pass, int index, int clients, AtomicInteger failures) {
        String tag = clients == 1 ? "" : " c" + index;
        long total = 0;
        for (int k = 0; k < sqls.size(); k++) {
            int i = (k + index * sqls.size() / clients) % sqls.size();
            String name = names.get(i);
            long t0 = System.nanoTime();
            try {
                long[] answer = read(client, client.execute(sqls.get(i), header), header);
                long nanos = System.nanoTime() - t0;
                total += nanos;
                long want = expected.get(i);
                if (want >= 0 ? answer[0] != want : answer[0] <= 0) {
                    failures.incrementAndGet();
                    System.out.printf(Locale.ROOT, "%s r%d wrong rows %d expected %d%s%n", name, pass, answer[0],
                            want, tag);
                }
                System.out.printf(Locale.ROOT, "%s r%d %9.1f ms rows %d sum %016x%s%n",
                        name, pass, nanos / 1e6, answer[0], answer[1], tag);
            } catch (Exception e) {
                failures.incrementAndGet();
                String message = String.valueOf(e.getMessage()).lines().findFirst().orElse("");
                System.out.printf(Locale.ROOT, "%s r%d error %.1f ms %s: %s%s%n", name, pass,
                        (System.nanoTime() - t0) / 1e6, e.getClass().getSimpleName(), message, tag);
            }
        }
        return total;
    }

    /**
     * Reads every endpoint of a query's answer.
     *
     * @param client Flight SQL client
     * @param info the query's flight info
     * @param header result-mode header
     * @return rows read and the order-independent checksum of the values
     * @throws Exception on failure
     */
    private static long[] read(FlightSqlClient client, FlightInfo info, CallOption header) throws Exception {
        long rows = 0;
        long sum = 0;
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(endpoint.getTicket(), header)) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    for (int row = 0; row < root.getRowCount(); row++) {
                        StringBuilder text = new StringBuilder();
                        for (FieldVector vector : root.getFieldVectors()) {
                            text.append(normalized(vector.getObject(row))).append('|');
                        }
                        sum += mix(text.toString().hashCode());
                    }
                    rows += root.getRowCount();
                }
            }
        }
        return new long[] {rows, sum};
    }

    /**
     * Renders a value with floating-point and decimal values rounded to 2 decimals, so sums
     * added up in another order compare equal.
     *
     * @param value cell value, may be null
     * @return text
     */
    private static String normalized(Object value) {
        if (value instanceof Double || value instanceof Float) {
            return String.format(Locale.ROOT, "%.2f", ((Number) value).doubleValue());
        }
        if (value instanceof BigDecimal d) {
            return String.format(Locale.ROOT, "%.2f", d);
        }
        return String.valueOf(value);
    }

    /**
     * Spreads a row hash over 64 bits before it is added up.
     *
     * @param hash row hash
     * @return mixed value
     */
    private static long mix(int hash) {
        long z = hash * 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        return z ^ (z >>> 31);
    }
}
