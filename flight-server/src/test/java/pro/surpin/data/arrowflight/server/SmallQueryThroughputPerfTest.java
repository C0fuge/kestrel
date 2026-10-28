package pro.surpin.data.arrowflight.server;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

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
 * Measures small single-table queries under load (item 21): queries per second, latency and the
 * CPU spent per query from 1, 8 and 32 clients running point lookups and filtered counts the way
 * stock JDBC/ADBC clients do (prepare, GetFlightInfo, DoGet, close), or as plain statements
 * ({@code -Dperf.client=statement}: GetFlightInfo, DoGet). Also prints the CPU time per
 * query by thread group, so the server's share can be told from the in-process clients'. Run it
 * pinned to few cores (for example {@code taskset -c 0,1}) to see the CPU-bound case. Prints the
 * numbers; asserts only the answers.
 */
@Tag("perf")
class SmallQueryThroughputPerfTest {

    private static final int ROWS = 400_000;
    private static final int FILES = 4;

    /**
     * Prints throughput, latency and CPU per query for 1, 8 and 32 clients.
     *
     * @param dir temporary data directory
     * @throws Exception on failure
     */
    @Test
    void printThroughput(@TempDir Path dir) throws Exception {
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
        }
        int threads = Integer.parseInt(System.getProperty("perf.threads", "0"));
        int seconds = Integer.parseInt(System.getProperty("perf.seconds", "15"));
        String mode = System.getProperty("perf.mode", "complete");
        TestFlightServerHelper.Builder builder = TestFlightServerHelper.builder()
                .dataDir(dir.toString()).resultMode(mode);
        if (threads > 0) {
            builder.threads(threads);
        }
        try (TestFlightServerHelper helper = builder.start()) {
            FlightSqlClient client = helper.sqlClient();
            FlightCallHeaders headers = new FlightCallHeaders();
            headers.insert("x-flight-result-mode", mode);
            CallOption header = new HeaderCallOption(headers);
            boolean statements = "statement".equals(System.getProperty("perf.client", "prepared"));
            System.out.printf(Locale.ROOT,
                    "== mode %s, client %s, duckDbThreads %s, %d cores, %d s per step%n",
                    mode, statements ? "statement" : "prepared", threads > 0 ? threads : "default",
                    Runtime.getRuntime().availableProcessors(), seconds);
            run(client, header, statements, 8, Math.max(3, seconds / 3), false);
            for (String clients : System.getProperty("perf.clients", "1,8,32").split(",")) {
                run(client, header, statements, Integer.parseInt(clients.strip()), seconds, true);
            }
        }
    }

    /**
     * Runs point lookups and filtered counts alternately from several clients for a while.
     *
     * @param client Flight SQL client
     * @param header result-mode header
     * @param statements whether to run plain statements (GetFlightInfo, DoGet) instead of
     *                   prepared ones (prepare, GetFlightInfo, DoGet, close)
     * @param clients concurrent clients
     * @param seconds duration
     * @param print whether to print the step (false for warm-up)
     * @throws Exception on failure
     */
    private static void run(FlightSqlClient client, CallOption header, boolean statements,
            int clients, int seconds, boolean print) throws Exception {
        AtomicInteger names = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(clients,
                r -> new Thread(r, "perf-client-" + names.incrementAndGet()));
        ThreadMXBean mx = ManagementFactory.getThreadMXBean();
        Map<String, Long> cpuBefore = cpuByGroup(mx);
        long cpu0 = processCpu();
        long end = System.nanoTime() + seconds * 1_000_000_000L;
        long wall0 = System.nanoTime();
        try {
            List<Future<long[]>> futures = new ArrayList<>();
            for (int c = 0; c < clients; c++) {
                int base = c * 100_003;
                futures.add(pool.submit(() -> {
                    long[] times = new long[1 << 16];
                    int n = 0;
                    for (int i = 0; System.nanoTime() < end && n < times.length; i++) {
                        long t0 = System.nanoTime();
                        long k = Math.floorMod((base + i) * 7919L, (long) ROWS);
                        String sql = (i & 1) == 0
                                ? "SELECT * FROM s.orders WHERE o_orderkey = " + k
                                : "SELECT count(*) FROM s.orders WHERE o_orderkey < " + k;
                        long rows;
                        if (statements) {
                            rows = read(client, client.execute(sql, header), header);
                        } else {
                            try (FlightSqlClient.PreparedStatement prepared = client.prepare(sql, header)) {
                                rows = read(client, prepared.execute(header), header);
                            }
                        }
                        assertEquals(1, rows, sql);
                        times[n++] = System.nanoTime() - t0;
                    }
                    return Arrays.copyOf(times, n);
                }));
            }
            List<long[]> all = new ArrayList<>();
            for (Future<long[]> f : futures) {
                all.add(f.get());
            }
            double wall = (System.nanoTime() - wall0) / 1e9;
            long cpu = processCpu() - cpu0;
            if (!print) {
                return;
            }
            long[] times = all.stream().flatMapToLong(Arrays::stream).toArray();
            Arrays.sort(times);
            int queries = times.length;
            System.out.printf(Locale.ROOT,
                    "c%-3d %7.1f q/s  p50 %6.2f p95 %6.2f ms  process cpu %5.2f ms/query (%.2f cores)%n",
                    clients, queries / wall, times[queries / 2] / 1e6, times[(int) (queries * 0.95)] / 1e6,
                    cpu / 1e6 / queries, cpu / 1e9 / wall);
            Map<String, Long> cpuAfter = cpuByGroup(mx);
            StringBuilder line = new StringBuilder("     cpu ms/query by thread group:");
            Map<String, Long> delta = new TreeMap<>();
            for (Map.Entry<String, Long> e : cpuAfter.entrySet()) {
                delta.put(e.getKey(), e.getValue() - cpuBefore.getOrDefault(e.getKey(), 0L));
            }
            delta.entrySet().stream().filter(e -> e.getValue() > 0)
                    .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                    .forEach(e -> line.append(String.format(Locale.ROOT, " %s %.2f", e.getKey(),
                            e.getValue() / 1e6 / queries)));
            System.out.println(line);
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Reads every endpoint of a query's answer.
     *
     * @param client Flight SQL client
     * @param info the query's flight info
     * @param header result-mode header
     * @return rows read
     * @throws Exception on failure
     */
    private static long read(FlightSqlClient client, FlightInfo info, CallOption header) throws Exception {
        long rows = 0;
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(endpoint.getTicket(), header)) {
                while (stream.next()) {
                    rows += stream.getRoot().getRowCount();
                }
            }
        }
        return rows;
    }

    /**
     * Returns the CPU time of live threads grouped by thread name without trailing digits.
     *
     * @param mx thread bean
     * @return nanoseconds per group
     */
    private static Map<String, Long> cpuByGroup(ThreadMXBean mx) {
        Map<String, Long> out = new HashMap<>();
        for (ThreadInfo info : mx.getThreadInfo(mx.getAllThreadIds())) {
            if (info == null) {
                continue;
            }
            long t = mx.getThreadCpuTime(info.getThreadId());
            if (t > 0) {
                String group = info.getThreadName().replaceAll("[-#_ ]?\\d+", "").replaceAll("\\s+", "");
                out.merge(group, t, Long::sum);
            }
        }
        return out;
    }

    /**
     * Returns the CPU time of this process.
     *
     * @return nanoseconds
     */
    private static long processCpu() {
        return ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean())
                .getProcessCpuTime();
    }
}
