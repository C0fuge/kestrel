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
import java.util.concurrent.TimeUnit;
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
import pro.surpin.data.arrowflight.server.adapters.DuckDbSizing;
import pro.surpin.data.arrowflight.server.adapters.ProducingStreamGate;
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
 * {@code -Dperf.threads} and {@code -Dperf.threadsUnderLoad} set duckDbThreads and
 * duckDbThreadsUnderLoad of the in-process server (default: the server's defaults);
 * {@code -Dperf.shared=threads:memoryLimit} runs it on one shared DuckDB (duckDbSharedDatabase).
 * {@code -Dperf.producingStreams} caps active producers and records gate wait deltas.
 * {@code -Dperf.heavyEvery=N} replaces every Nth query with a bounded customer-key self join,
 * phased by client (default 0 keeps the original point/count mix). Raw query kinds are
 * 0 for point lookups, 1 for filtered counts and 2 for the heavier join count.
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
            try (PerfRunArtifacts artifacts = PerfRunArtifacts.open();
                    BufferAllocator allocator = new RootAllocator();
                    FlightClient flight = FlightClient.builder(allocator,
                            Location.forGrpcInsecure(hostPort[0], Integer.parseInt(hostPort[1]))).build()) {
                if (artifacts != null) {
                    artifacts.stage("external-client-ready");
                }
                measureAll(new FlightSqlClient(flight), System.getProperty("perf.mode", "complete"),
                        "external " + external, null, null);
            }
            return;
        }
        int threads = Integer.parseInt(System.getProperty("perf.threads", "0"));
        int underLoad = Integer.parseInt(System.getProperty("perf.threadsUnderLoad", "0"));
        String mode = System.getProperty("perf.mode", "complete");
        boolean s3 = "s3".equals(System.getProperty("perf.store", "local"));
        // 0 = the server's own default for the store (not the helper's 1 thread).
        TestFlightServerHelper.Builder builder = TestFlightServerHelper.builder()
                .dataDir(dir.toString()).resultMode(mode).threads(threads)
                .threadsUnderLoad(underLoad, Runtime.getRuntime().availableProcessors())
                .producingStreams(Integer.getInteger("perf.producingStreams", 0));
        String shared = System.getProperty("perf.shared");
        if (shared != null) {
            // threads:memoryLimit of one node-wide DuckDB, as duckDbSharedDatabase=true.
            String[] parts = shared.split(":", 2);
            builder.sharedDatabase(new DuckDbSizing.Shared(Integer.parseInt(parts[0]), parts[1]));
        }
        try (PerfRunArtifacts artifacts = PerfRunArtifacts.open();
                FakeS3 fake = s3 ? onFakeS3(builder, dir) : null;
                TestFlightServerHelper helper = startServer(builder, artifacts)) {
            if (artifacts != null) {
                artifacts.stage("server-ready");
            }
            measureAll(helper.sqlClient(), mode, (s3 ? "s3 (latency " + Long.getLong("s3.perf.latencyMs", 2)
                    + " ms)" : "local") + ", duckDbThreads " + (threads > 0 ? threads : "default")
                    + (underLoad > 0 ? ", duckDbThreadsUnderLoad " + underLoad : ""),
                    helper.duckDbAdapter.producingGate(), joinGate(helper.duckDbAdapter));
        }
    }

    /**
     * Starts the server after recording its initialization boundary.
     *
     * @param builder server configuration
     * @param artifacts optional raw measurement collector
     * @return running server helper
     * @throws Exception when recording or startup fails
     */
    private static TestFlightServerHelper startServer(TestFlightServerHelper.Builder builder,
            PerfRunArtifacts artifacts) throws Exception {
        if (artifacts != null) {
            artifacts.stage("server-start");
        }
        return builder.start();
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
     * @param gate producer gate, or null for an external server
     * @param joinGate optional experimental join producer gate
     * @throws Exception on failure
     */
    private static void measureAll(FlightSqlClient client, String mode, String server,
            ProducingStreamGate gate, ProducingStreamGate joinGate) throws Exception {
        int seconds = Integer.parseInt(System.getProperty("perf.seconds", "15"));
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert("x-flight-result-mode", mode);
        CallOption header = new HeaderCallOption(headers);
        boolean statements = "statement".equals(System.getProperty("perf.client", "prepared"));
        System.out.printf(Locale.ROOT, "== server %s, mode %s, client %s, %d cores, %d s per step%n",
                server, mode, statements ? "statement" : "prepared", Runtime.getRuntime().availableProcessors(),
                seconds);
        run(client, header, statements, 8, Math.max(3, seconds / 3), false, gate, joinGate);
        for (String clients : System.getProperty("perf.clients", "1,8,32").split(",")) {
            run(client, header, statements, Integer.parseInt(clients.strip()), seconds, true, gate, joinGate);
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
     * @param gate producer gate, or null for an external server
     * @throws Exception on failure
     */
    static void run(FlightSqlClient client, CallOption header, boolean statements,
            int clients, int seconds, boolean print, ProducingStreamGate gate) throws Exception {
        run(client, header, statements, clients, seconds, print, gate, null);
    }

    /**
     * Finds optional join-gate telemetry while keeping this harness compatible with the baseline.
     *
     * @param adapter server's DuckDB adapter
     * @return experimental gate, or null when the baseline does not expose one
     * @throws ReflectiveOperationException when an existing getter fails
     */
    private static ProducingStreamGate joinGate(Object adapter) throws ReflectiveOperationException {
        try {
            return (ProducingStreamGate) adapter.getClass().getMethod("joinProducingGate").invoke(adapter);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    /**
     * Measures one concurrency step with optional experimental admission telemetry.
     *
     * @param client Flight SQL client
     * @param header result-mode header
     * @param statements whether to use statements rather than prepares
     * @param clients concurrent clients
     * @param seconds duration
     * @param print whether to print the measured step
     * @param gate global producer gate, or null
     * @param joinGate experimental join gate, or null
     * @throws Exception on failure
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    private static void run(FlightSqlClient client, CallOption header, boolean statements,
            int clients, int seconds, boolean print, ProducingStreamGate gate,
            ProducingStreamGate joinGate) throws Exception {
        int heavyEvery = Integer.getInteger("perf.heavyEvery", 0);
        if (heavyEvery < 0) {
            throw new IllegalArgumentException("perf.heavyEvery must not be negative");
        }
        AtomicInteger names = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(clients,
                r -> new Thread(r, "perf-client-" + names.incrementAndGet()));
        ThreadMXBean mx = ManagementFactory.getThreadMXBean();
        Map<String, Long> cpuBefore = cpuByGroup(mx);
        long cpu0 = processCpu();
        long waits0 = gate == null ? 0 : gate.waits();
        long waitNanos0 = gate == null ? 0 : gate.waitNanos();
        long joinWaits0 = joinGate == null ? 0 : joinGate.waits();
        long joinWaitNanos0 = joinGate == null ? 0 : joinGate.waitNanos();
        long end = System.nanoTime() + seconds * 1_000_000_000L;
        long wall0 = System.nanoTime();
        List<StringBuffer> samples = new ArrayList<>();
        Throwable failure = null;
        try {
            List<Future<long[]>> futures = new ArrayList<>();
            for (int c = 0; c < clients; c++) {
                int base = c * 100_003;
                int thread = c;
                StringBuffer raw = new StringBuffer();
                samples.add(raw);
                futures.add(pool.submit(() -> {
                    long[] times = new long[1 << 16];
                    int n = 0;
                    for (int i = 0; System.nanoTime() < end && n < times.length; i++) {
                        long t0 = System.nanoTime();
                        long k = Math.floorMod((base + i) * 7919L, (long) ROWS);
                        QueryCase query = queryFor(i, thread, k, heavyEvery);
                        String sql = query.sql();
                        QueryRead result;
                        try {
                            if (statements) {
                                result = read(client, client.execute(sql, header), header);
                            } else {
                                try (FlightSqlClient.PreparedStatement prepared = client.prepare(sql, header)) {
                                    result = read(client, prepared.execute(header), header);
                                }
                            }
                        } catch (Exception e) {
                            String error = e.getClass().getSimpleName();
                            appendSample(raw, thread, i, query.kind(), k, t0 - wall0,
                                    System.nanoTime() - t0, null, error.isEmpty() ? "Exception" : error);
                            throw e;
                        }
                        long elapsed = System.nanoTime() - t0;
                        times[n++] = elapsed;
                        String error = result.rows() == 1 && result.first() == query.expectedFirst()
                                ? "" : "answer_mismatch";
                        appendSample(raw, thread, i, query.kind(), k, t0 - wall0, elapsed, result, error);
                        assertEquals(1, result.rows(), sql);
                        assertEquals(query.expectedFirst(), result.first(), sql);
                    }
                    return Arrays.copyOf(times, n);
                }));
            }
            List<long[]> all = awaitClients(futures, end + TimeUnit.SECONDS.toNanos(30));
            double wall = (System.nanoTime() - wall0) / 1e9;
            long cpu = processCpu() - cpu0;
            if (gate != null) {
                String sample = "permits=" + gate.permits() + "\nwaits=" + (gate.waits() - waits0)
                        + "\nwait_ns=" + (gate.waitNanos() - waitNanos0) + "\n";
                if (PerfRunArtifacts.enabled()) {
                    String label = print ? "c" + clients : "warmup-c" + clients;
                    Files.writeString(Path.of(System.getProperty("perf.output"), label + "-gate.txt"), sample);
                }
                if (print) {
                    System.out.printf(Locale.ROOT, "gate c%d %s%n", clients,
                            sample.strip().replace('\n', ' '));
                }
            }
            if (joinGate != null) {
                String sample = "permits=" + joinGate.permits() + "\nwaits=" + (joinGate.waits() - joinWaits0)
                        + "\nwait_ns=" + (joinGate.waitNanos() - joinWaitNanos0) + "\n";
                if (PerfRunArtifacts.enabled()) {
                    String label = print ? "c" + clients : "warmup-c" + clients;
                    Files.writeString(Path.of(System.getProperty("perf.output"), label + "-join-gate.txt"), sample);
                }
                if (print) {
                    System.out.printf(Locale.ROOT, "join_gate c%d %s%n", clients,
                            sample.strip().replace('\n', ' '));
                }
            }
            if (PerfRunArtifacts.enabled()) {
                String label = print ? "c" + clients : "warmup-c" + clients;
                Files.writeString(Path.of(System.getProperty("perf.output"), label + "-wall.txt"),
                        "wall_start_ns=" + wall0 + "\nwall_seconds=" + wall + "\nprocess_cpu_ns=" + cpu + "\n");
            }
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
        } catch (Exception | Error e) {
            failure = e;
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw e;
        } finally {
            finishClients(pool, samples, print ? "c" + clients : "warmup-c" + clients, failure);
        }
    }

    /**
     * Collects answers against one global deadline, including the final in-flight queries.
     *
     * @param futures submitted client tasks
     * @param deadline absolute monotonic deadline
     * @return completed latency arrays
     * @throws Exception when a client fails, times out or the caller is interrupted
     */
    static List<long[]> awaitClients(List<Future<long[]>> futures, long deadline) throws Exception {
        List<long[]> all = new ArrayList<>();
        for (Future<long[]> future : futures) {
            all.add(future.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS));
        }
        return all;
    }

    /**
     * Stops client tasks and preserves complete sample rows even when shutdown fails.
     *
     * @param pool client executor
     * @param samples synchronized per-client sample buffers
     * @param label workload step
     * @param failure original failure, or null
     * @throws Exception when cleanup fails without an original failure
     */
    static void finishClients(ExecutorService pool, List<StringBuffer> samples,
            String label, Throwable failure) throws Exception {
        pool.shutdownNow();
        try {
            if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Performance clients did not stop");
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (failure == null) {
                throw e;
            }
            failure.addSuppressed(e);
        } finally {
            if (PerfRunArtifacts.enabled()) {
                PerfRunArtifacts.writeSamples(label, samples);
            }
        }
    }

    /**
     * Appends one complete success or failure sample when raw output is enabled.
     *
     * @param raw synchronized client buffer
     * @param thread client index
     * @param sequence query index
     * @param kind query kind
     * @param key generated key
     * @param start start relative to this step
     * @param elapsed query latency
     * @param result consumed answer, or null after an execution failure
     * @param error empty on success, otherwise a safe error type
     */
    private static void appendSample(StringBuffer raw, int thread, int sequence, int kind, long key,
            long start, long elapsed, QueryRead result, String error) {
        if (PerfRunArtifacts.enabled()) {
            synchronized (raw) {
                raw.append(thread).append(',').append(sequence).append(',').append(kind)
                        .append(',').append(key).append(',').append(start).append(',').append(elapsed)
                        .append(',').append(result == null ? -1 : result.rows())
                        .append(',').append(result == null ? 0 : result.checksum())
                        .append(',').append(result == null ? -1 : result.bytes())
                        .append(',').append(result == null ? -1 : result.batches())
                        .append(',').append(error.isEmpty() ? 1 : 0).append(',').append(error).append('\n');
            }
        }
    }

    /**
     * Selects one deterministic query, rotating the heavy-query phase by client.
     *
     * @param sequence query sequence within the client
     * @param thread client index
     * @param key existing order key
     * @param heavyEvery heavy-query interval, or 0 to disable
     * @return query kind, SQL and exact first value
     */
    static QueryCase queryFor(int sequence, int thread, long key, int heavyEvery) {
        long slot = (long) sequence + thread;
        if (heavyEvery > 0 && Math.floorMod(slot, heavyEvery) == heavyEvery - 1) {
            long bound = Math.min(key, 5000);
            return new QueryCase(2, "SELECT count(*) FROM s.orders a JOIN s.orders b"
                    + " ON a.o_custkey = b.o_custkey WHERE a.o_orderkey < " + bound,
                    bound * (ROWS / 1000L));
        }
        long smallSlot = heavyEvery == 0 ? sequence : slot - slot / heavyEvery;
        return (smallSlot & 1) == 0
                ? new QueryCase(0, "SELECT * FROM s.orders WHERE o_orderkey = " + key, key)
                : new QueryCase(1, "SELECT count(*) FROM s.orders WHERE o_orderkey < " + key, key);
    }

    /**
     * One checked workload query.
     *
     * @param kind 0 for point, 1 for count, 2 for heavy join
     * @param sql query text
     * @param expectedFirst exact first numeric result
     */
    record QueryCase(int kind, String sql, long expectedFirst) {
    }

    /**
     * Reads every endpoint of a query's answer.
     *
     * @param client Flight SQL client
     * @param info the query's flight info
     * @param header result-mode header
     * @return rows, first value, checksum and transferred Arrow buffers
     * @throws Exception on failure
     */
    private static QueryRead read(FlightSqlClient client, FlightInfo info, CallOption header) throws Exception {
        long rows = 0;
        long first = 0;
        long checksum = 1;
        long bytes = 0;
        long batches = 0;
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(endpoint.getTicket(), header)) {
                while (stream.next()) {
                    org.apache.arrow.vector.VectorSchemaRoot root = stream.getRoot();
                    if (rows == 0 && root.getRowCount() > 0) {
                        first = ((Number) root.getVector(0).getObject(0)).longValue();
                    }
                    for (org.apache.arrow.vector.FieldVector vector : root.getFieldVectors()) {
                        bytes += vector.getBufferSize();
                    }
                    for (int row = 0; row < root.getRowCount(); row++) {
                        for (org.apache.arrow.vector.FieldVector vector : root.getFieldVectors()) {
                            Object value = vector.getObject(row);
                            checksum = 31 * checksum + (value == null ? 0 : value.toString().hashCode());
                        }
                    }
                    rows += root.getRowCount();
                    batches++;
                }
            }
        }
        return new QueryRead(rows, first, checksum, bytes, batches);
    }

    /**
     * One completely consumed answer.
     *
     * @param rows returned rows
     * @param first first numeric value
     * @param checksum ordered hash of all values
     * @param bytes bytes of Arrow buffers
     * @param batches batches received
     */
    private record QueryRead(long rows, long first, long checksum, long bytes, long batches) {
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
