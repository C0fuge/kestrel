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
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.server.adapters.ProducingStreamGate;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Measures full-table scans read in several parallel streams (item 22): either one partial-mode
 * query whose answer is split into N endpoints ({@code endpointsPerServer=N},
 * {@code -Dperf.shape=endpoints}, the default) or N complete-mode clients each scanning the whole table at once ({@code -Dperf.shape=clients}).
 * Every stream has its own Flight client connection. Prints the aggregate Arrow MB/s, the process
 * CPU per GB and the CPU by thread group (DuckDB's own threads are native and show only in the
 * process total); asserts only the answers. Run it pinned to few cores (for example
 * {@code taskset -c 0,1}) to see the CPU-bound case. {@code -Dperf.streams=1,4,8},
 * {@code -Dperf.rows}, {@code -Dperf.runs}, {@code -Dperf.threads} (duckDbThreads) and
 * {@code -Dperf.threadsUnderLoad} (duckDbThreadsUnderLoad) and {@code -Dperf.producingStreams}
 * (duckDbProducingStreams) pick the rest.
 * {@code -Dperf.server=host:port} measures a server started separately (pinned to its own cores, so
 * the in-process clients do not share its CPUs) on the table under {@code -Dperf.dataDir}, which the
 * test writes when it is missing.
 */
@Tag("perf")
class ParallelScanPerfTest {

    private static final long ROWS = Long.getLong("perf.rows", 6_000_000L);
    private static final int FILES = 16;
    private static final int RUNS = Integer.getInteger("perf.runs", 3);

    /**
     * Prints the aggregate scan throughput for each stream count.
     *
     * @param tmp temporary data directory, unless {@code -Dperf.dataDir} names one
     * @throws Exception on failure
     */
    @Test
    void printParallelScanThroughput(@TempDir Path tmp) throws Exception {
        String external = System.getProperty("perf.server");
        Path dir = System.getProperty("perf.dataDir") != null
                ? Path.of(System.getProperty("perf.dataDir")) : tmp;
        Path table = dir.resolve("s").resolve("lineitem");
        if (!Files.isDirectory(table)) {
            writeTable(table);
        }
        boolean clients = "clients".equals(System.getProperty("perf.shape", "endpoints"));
        int threads = Integer.getInteger("perf.threads", 0);
        for (String spec : System.getProperty("perf.streams", "1,4,8").split(",")) {
            int streams = Integer.parseInt(spec.strip());
            if (external != null) {
                // The external server's own endpointsPerServer decides the endpoints.
                String[] hostPort = external.split(":");
                measure(Location.forGrpcInsecure(hostPort[0], Integer.parseInt(hostPort[1])), streams,
                        clients, "external");
                continue;
            }
            System.setProperty("endpointsPerServer", String.valueOf(clients ? 1 : streams));
            // duckDbThreads 0 = the server's default for the cores it sees.
            try (TestFlightServerHelper helper = TestFlightServerHelper.builder()
                    .dataDir(dir.toString()).resultMode(clients ? "complete" : "partial").threads(threads)
                    .threadsUnderLoad(Integer.getInteger("perf.threadsUnderLoad", 0),
                            Runtime.getRuntime().availableProcessors())
                    .producingStreams(Integer.getInteger("perf.producingStreams", 0)).start()) {
                measure(helper.location, streams, clients,
                        "duckDbThreads " + (threads > 0 ? threads : "default"));
                ProducingStreamGate gate = helper.duckDbAdapter.producingGate();
                if (gate.permits() > 0) {
                    System.out.printf(Locale.ROOT, "  producingStreams %d: %d waits, %.0f ms waited%n",
                            gate.permits(), gate.waits(), gate.waitNanos() / 1e6);
                }
            } finally {
                System.clearProperty("endpointsPerServer");
            }
        }
    }

    /**
     * Writes the lineitem-like table.
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
                        "COPY (SELECT i AS l_orderkey, i %% 200000 AS l_partkey, i %% 7 + 1 AS l_linenumber,"
                                + " (i * 13 %% 5000) / 100.0 AS l_quantity, (i * 7 %% 100000) / 10.0"
                                + " AS l_extendedprice, DATE '1992-01-01' + CAST(i %% 2500 AS INTEGER)"
                                + " AS l_shipdate, 'comment ' || (i %% 9973) AS l_comment"
                                + " FROM range(%d, %d) t(i)) TO '%s' (FORMAT parquet, ROW_GROUP_SIZE 122880)",
                        f * ROWS / FILES, (f + 1) * ROWS / FILES, table.resolve("part-" + f + ".parquet")));
            }
        }
    }

    /**
     * Measures and prints one stream count against one server.
     *
     * @param location server location
     * @param streams parallel streams
     * @param clients whether every stream runs its own query
     * @param label server description for the output
     * @throws Exception on failure
     */
    private static void measure(Location location, int streams, boolean clients, String label)
            throws Exception {
        long expectedSum = (ROWS - 1) * ROWS / 2;
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            List<FlightSqlClient> sql = new ArrayList<>();
            for (int s = 0; s < streams; s++) {
                sql.add(new FlightSqlClient(FlightClient.builder(allocator, location).build()));
            }
            FlightCallHeaders headers = new FlightCallHeaders();
            // Only partial mode splits a node's files into endpoints; complete mode is one stream.
            headers.insert("x-flight-result-mode", clients ? "complete" : "partial");
            CallOption header = new HeaderCallOption(headers);
            AtomicInteger names = new AtomicInteger();
            ExecutorService pool = Executors.newFixedThreadPool(streams,
                    r -> new Thread(r, "perf-client-" + names.incrementAndGet()));
            ThreadMXBean mx = ManagementFactory.getThreadMXBean();
            try {
                long[] mbps = new long[RUNS];
                double cpuPerGb = 0;
                Map<String, Long> groups = new TreeMap<>();
                long bytes = 0;
                long endpoints = 0;
                for (int i = -1; i < RUNS; i++) {
                    Map<String, Long> cpuBefore = cpuByGroup(mx);
                    long cpu0 = processCpu();
                    long t0 = System.nanoTime();
                    long[] got = clients ? scanByClients(sql, header, pool)
                            : scanByEndpoints(sql, header, pool);
                    double seconds = (System.nanoTime() - t0) / 1e9;
                    long cpu = processCpu() - cpu0;
                    assertEquals((clients ? streams : 1) * ROWS, got[0], "rows");
                    assertEquals((clients ? streams : 1) * expectedSum, got[1], "sum of l_orderkey");
                    if (i >= 0) {
                        endpoints = got[4];
                        mbps[i] = Math.round(got[2] / seconds / 1e6);
                        cpuPerGb += cpu / 1e9 / (got[2] / 1e9) / RUNS;
                        bytes += got[2];
                        Map<String, Long> cpuAfter = cpuByGroup(mx);
                        for (Map.Entry<String, Long> e : cpuAfter.entrySet()) {
                            long d = e.getValue() - cpuBefore.getOrDefault(e.getKey(), 0L);
                            if (d > 0) {
                                groups.merge(e.getKey(), d, Long::sum);
                            }
                        }
                    }
                }
                Arrays.sort(mbps);
                System.out.printf(Locale.ROOT, "%s %d streams (%d endpoints), %s, %d cores:"
                                + " MB/s median %d (min %d, max %d), process cpu %.2f s/GB%n",
                        clients ? "clients" : "endpoints", streams, endpoints, label,
                        Runtime.getRuntime().availableProcessors(),
                        mbps[RUNS / 2], mbps[0], mbps[RUNS - 1], cpuPerGb);
                StringBuilder line = new StringBuilder("     cpu s/GB by thread group:");
                long total = bytes;
                groups.entrySet().stream().sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                        .limit(6)
                        .forEach(e -> line.append(String.format(Locale.ROOT, " %s %.2f", e.getKey(),
                                e.getValue() / (double) total)));
                System.out.println(line);
            } finally {
                pool.shutdownNow();
                for (FlightSqlClient c : sql) {
                    c.close();
                }
            }
        }
    }

    /**
     * Runs one query and reads its endpoints in parallel, one client each (round robin).
     *
     * @param sql one client per stream
     * @param header result-mode header
     * @param pool reader threads
     * @return rows, sum of l_orderkey, Arrow buffer bytes, batches, endpoints
     * @throws Exception on failure
     */
    private static long[] scanByEndpoints(List<FlightSqlClient> sql, CallOption header,
            ExecutorService pool) throws Exception {
        FlightInfo info = sql.get(0).execute("SELECT * FROM s.lineitem", header);
        List<Future<long[]>> parts = new ArrayList<>();
        int n = 0;
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            FlightSqlClient client = sql.get(n++ % sql.size());
            parts.add(pool.submit(() -> read(client, endpoint, header)));
        }
        return sum(parts);
    }

    /**
     * Runs the whole scan once from every client at the same time.
     *
     * @param sql one client per stream
     * @param header result-mode header
     * @param pool reader threads
     * @return rows, sum of l_orderkey, Arrow buffer bytes, batches, endpoints over all clients
     * @throws Exception on failure
     */
    private static long[] scanByClients(List<FlightSqlClient> sql, CallOption header,
            ExecutorService pool) throws Exception {
        List<Future<long[]>> parts = new ArrayList<>();
        for (FlightSqlClient client : sql) {
            parts.add(pool.submit(() -> {
                long[] total = new long[5];
                FlightInfo info = client.execute("SELECT * FROM s.lineitem", header);
                for (FlightEndpoint endpoint : info.getEndpoints()) {
                    long[] got = read(client, endpoint, header);
                    for (int k = 0; k < total.length; k++) {
                        total[k] += got[k];
                    }
                }
                return total;
            }));
        }
        return sum(parts);
    }

    /**
     * Adds up the results of several readers.
     *
     * @param parts reader results
     * @return element-wise sums
     * @throws Exception when a reader failed
     */
    private static long[] sum(List<Future<long[]>> parts) throws Exception {
        long[] total = new long[5];
        for (Future<long[]> part : parts) {
            long[] got = part.get();
            for (int k = 0; k < total.length; k++) {
                total[k] += got[k];
            }
        }
        return total;
    }

    /**
     * Reads one endpoint.
     *
     * @param client Flight SQL client
     * @param endpoint endpoint to read
     * @param header result-mode header
     * @return rows, sum of l_orderkey, Arrow buffer bytes, batches, 1 endpoint
     * @throws Exception on failure
     */
    private static long[] read(FlightSqlClient client, FlightEndpoint endpoint, CallOption header)
            throws Exception {
        long rows = 0;
        long sum = 0;
        long bytes = 0;
        long batches = 0;
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
        return new long[] {rows, sum, bytes, batches, 1};
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
