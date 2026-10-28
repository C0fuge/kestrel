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
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.plugin.s3.FakeS3;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;
import pro.surpin.data.arrowflight.server.plugin.PluginSettings;
import pro.surpin.data.arrowflight.server.storage.StoreRegistry;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Measures small single-table queries under load (item 21): queries per second, latency and the
 * CPU spent per query from 1, 8 and 32 clients running point lookups and filtered counts the way
 * stock JDBC/ADBC clients do (prepare, GetFlightInfo, DoGet, close), or as plain statements
 * ({@code -Dperf.client=statement}: GetFlightInfo, DoGet). Also prints the CPU time per
 * query by thread group, so the server's share can be told from the in-process clients'. Run it
 * pinned to few cores (for example {@code taskset -c 0,1}) to see the CPU-bound case. With
 * {@code -Dperf.store=s3} the table lives on the fake S3 behind the real s3 plugin, every object
 * request delayed by {@code -Ds3.perf.latencyMs} (default 2); needs {@code -Ds3.it.httpfs}. Prints
 * the numbers; asserts only the answers. {@code -Dperf.server=host:port} measures a server started
 * separately (pinned to its own cores, so the in-process clients do not share its CPUs) on the
 * table under {@code -Dperf.dataDir}, which the test writes when it is missing.
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
    void printThroughput(@TempDir Path tmp) throws Exception {
        String external = System.getProperty("perf.server");
        Path dir = System.getProperty("perf.dataDir") != null ? Path.of(System.getProperty("perf.dataDir")) : tmp;
        Path table = dir.resolve("s").resolve("orders");
        if (!Files.isDirectory(table)) {
            writeTable(table);
        }
        if (external != null) {
            String[] hostPort = external.split(":");
            try (BufferAllocator allocator = new RootAllocator();
                    FlightClient flight = FlightClient.builder(allocator,
                            Location.forGrpcInsecure(hostPort[0], Integer.parseInt(hostPort[1]))).build()) {
                measureAll(new FlightSqlClient(flight), System.getProperty("perf.mode", "complete"),
                        "external " + external);
            }
            return;
        }
        int threads = Integer.parseInt(System.getProperty("perf.threads", "0"));
        String mode = System.getProperty("perf.mode", "complete");
        boolean s3 = "s3".equals(System.getProperty("perf.store", "local"));
        // 0 = the server's own default for the store (not the helper's 1 thread).
        TestFlightServerHelper.Builder builder = TestFlightServerHelper.builder()
                .dataDir(dir.toString()).resultMode(mode).threads(threads);
        try (FakeS3 fake = s3 ? onFakeS3(builder, dir) : null;
                TestFlightServerHelper helper = builder.start()) {
            measureAll(helper.sqlClient(), mode, (s3 ? "s3 (latency " + Long.getLong("s3.perf.latencyMs", 2)
                    + " ms)" : "local") + ", duckDbThreads " + (threads > 0 ? threads : "default"));
        }
    }

    /**
     * Writes the orders-like table.
     *
     * @param table table directory
     * @throws Exception on failure
     */
    private static void writeTable(Path table) throws Exception {
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
    }

    /**
     * Warms up, then prints one step per client count.
     *
     * @param client Flight SQL client of the measured server
     * @param mode result mode
     * @param server description of the server for the header line
     * @throws Exception on failure
     */
    private static void measureAll(FlightSqlClient client, String mode, String server) throws Exception {
        int seconds = Integer.parseInt(System.getProperty("perf.seconds", "15"));
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert("x-flight-result-mode", mode);
        CallOption header = new HeaderCallOption(headers);
        boolean statements = "statement".equals(System.getProperty("perf.client", "prepared"));
        System.out.printf(Locale.ROOT, "== server %s, mode %s, client %s, %d cores, %d s per step%n",
                server, mode, statements ? "statement" : "prepared", Runtime.getRuntime().availableProcessors(),
                seconds);
        run(client, header, statements, 8, Math.max(3, seconds / 3), false);
        for (String clients : System.getProperty("perf.clients", "1,8,32").split(",")) {
            run(client, header, statements, Integer.parseInt(clients.strip()), seconds, true);
        }
    }

    /**
     * Moves the written table to a fake S3 and points the server at it through the real s3 plugin.
     *
     * @param builder server builder, given the S3 data directory and the plugin registry
     * @param dir temporary directory holding the written table
     * @return the running fake S3
     * @throws Exception when the httpfs extension is missing or the plugin cannot be copied
     */
    private static FakeS3 onFakeS3(TestFlightServerHelper.Builder builder, Path dir) throws Exception {
        String httpfs = System.getProperty("s3.it.httpfs", System.getenv("DUCKDB_HTTPFS_EXTENSION"));
        Assumptions.assumeTrue(httpfs != null && Files.isReadable(Path.of(httpfs)),
                "httpfs extension not configured (-Ds3.it.httpfs or DUCKDB_HTTPFS_EXTENSION)");
        Path plugin = Files.createDirectories(dir.resolve("plugins").resolve("s3"));
        try (var jars = Files.list(Path.of("../flight-plugin-s3/target/plugin/s3"))) {
            for (Path jar : jars.toList()) {
                Files.copy(jar, plugin.resolve(jar.getFileName().toString()));
            }
        }
        FakeS3 s3 = FakeS3.start();
        for (int f = 0; f < FILES; f++) {
            s3.put("lake", "perf/s/orders/part-" + f + ".parquet",
                    Files.readAllBytes(dir.resolve("s").resolve("orders").resolve("part-" + f + ".parquet")));
        }
        s3.latency(Long.getLong("s3.perf.latencyMs", 2));
        Map<String, String> sys = Map.of("s3Endpoint", s3.endpoint(), "s3UrlStyle", "path",
                "duckDbHttpfsExtension", httpfs);
        Map<String, String> env = Map.of("FLIGHT_S3_ACCESS_KEY", "AKIAPERF", "FLIGHT_S3_SECRET_KEY", "perf-secret");
        ConfigSource source = new ConfigSource(sys::get, new Properties(), env, null);
        PluginSettings settings = new PluginSettings(dir.resolve("plugins"), null, "directory", false);
        builder.dataDir("s3://lake/perf").registry(config -> StoreRegistry.open(config, new Configuration(),
                "localhost", settings, source, List.of()));
        return s3;
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
                    "c%-3d %7.1f q/s  p50 %6.2f p95 %6.2f p99 %6.2f max %7.2f ms"
                            + "  process cpu %5.2f ms/query (%.2f cores)%n",
                    clients, queries / wall, times[queries / 2] / 1e6, times[(int) (queries * 0.95)] / 1e6,
                    times[(int) (queries * 0.99)] / 1e6, times[queries - 1] / 1e6,
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
