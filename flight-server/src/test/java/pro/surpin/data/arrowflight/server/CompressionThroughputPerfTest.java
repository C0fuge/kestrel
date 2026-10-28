package pro.surpin.data.arrowflight.server;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.adapters.ResultModeMiddleware;
import pro.surpin.data.arrowflight.server.adapters.StreamCompression;
import pro.surpin.data.arrowflight.server.metrics.CompressionMetrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Measures one result stream over the loopback link with and without compression: Arrow MB/s the
 * client receives, process CPU seconds (server and client share the JVM) and the compression
 * ratio, for plain, zstd and lz4 with one compression thread and with the automatic count. Meant
 * for a link slowed by {@code tc netem} (workflow {@code compression-perf.yml}: delay 10 ms, rate
 * 1 Gbit); opt-in: perf tag. Rows: {@code -Dcompression.perf.rows} (default 3 000 000); the table
 * also goes to {@code -Dcompression.perf.report} when set.
 */
@Tag("perf")
class CompressionThroughputPerfTest {

    private static final String TABLE = "perf.lineitem";
    private static final int ROUNDS = 3;

    @TempDir
    static Path data;

    /**
     * Writes a lineitem-like table and prints the best of three reads per variant.
     *
     * @throws Exception on write, startup or query failure
     */
    @Test
    void streamThroughput() throws Exception {
        long rows = Long.getLong("compression.perf.rows", 3_000_000L);
        writeTable(rows);
        List<String> report = new ArrayList<>();
        report.add("| variant | threads | MB/s (Arrow) | seconds | CPU s | ratio |");
        report.add("|---|---|---|---|---|---|");
        for (int threads : new int[]{1, 0}) {
            StreamCompression.Settings settings = new StreamCompression.Settings(
                    StreamCompression.Codec.NONE, 1, 64 * 1024L, threads);
            int resolved = settings.threadsFor(Runtime.getRuntime().availableProcessors());
            try (TestFlightServerHelper helper = TestFlightServerHelper.builder().dataDir(data.toString())
                    .resultMode("complete").threads(2).compression(settings).start()) {
                for (String codec : threads == 1 ? List.of("none", "zstd", "lz4") : List.of("zstd", "lz4")) {
                    Result best = null;
                    for (int round = 0; round < ROUNDS; round++) {
                        Result result = read(helper, codec);
                        assertEquals(rows, result.rows(), codec);
                        if (best == null || result.seconds() < best.seconds()) {
                            best = result;
                        }
                    }
                    report.add(String.format("| %s | %d | %.0f | %.2f | %.2f | %.2f |", codec, resolved,
                            best.bytes() / 1e6 / best.seconds(), best.seconds(), best.cpuSeconds(), best.ratio()));
                }
            }
        }
        String table = String.join("\n", report);
        System.out.println(table);
        String target = System.getProperty("compression.perf.report");
        if (target != null && !target.isBlank()) {
            Files.writeString(Path.of(target), table + "\n", StandardCharsets.UTF_8);
        }
    }

    /**
     * Writes two Parquet files of TPC-H lineitem-like columns.
     *
     * @param rows total rows
     * @throws Exception on write failure
     */
    private static void writeTable(long rows) throws Exception {
        Path dir = Files.createDirectories(data.resolve("perf/lineitem"));
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            for (int part = 0; part < 2; part++) {
                s.execute("COPY (SELECT r // 4 AS l_orderkey, (hash(r) % 200000)::BIGINT AS l_partkey,"
                        + " (hash(r * 7) % 10000)::BIGINT AS l_suppkey, (r % 7 + 1)::INTEGER AS l_linenumber,"
                        + " (hash(r * 3) % 50 + 1)::DECIMAL(15,2) AS l_quantity,"
                        + " (hash(r * 5) % 10000000 / 100.0)::DECIMAL(15,2) AS l_extendedprice,"
                        + " ((r % 11) / 100.0)::DECIMAL(15,2) AS l_discount,"
                        + " ((r % 9) / 100.0)::DECIMAL(15,2) AS l_tax,"
                        + " ['A','N','R'][r % 3 + 1] AS l_returnflag, ['O','F'][r % 2 + 1] AS l_linestatus,"
                        + " DATE '1992-01-01' + (hash(r * 11) % 2500)::INTEGER AS l_shipdate,"
                        + " DATE '1992-01-01' + (hash(r * 13) % 2500)::INTEGER AS l_commitdate,"
                        + " DATE '1992-01-01' + (hash(r * 17) % 2500)::INTEGER AS l_receiptdate,"
                        + " ['DELIVER IN PERSON','COLLECT COD','NONE','TAKE BACK RETURN'][r % 4 + 1] AS l_shipinstruct,"
                        + " ['AIR','MAIL','SHIP','TRUCK','RAIL','FOB','REG AIR'][r % 7 + 1] AS l_shipmode,"
                        + " 'carefully ' || ['final','regular','express','pending'][(hash(r) % 4)::BIGINT + 1]"
                        + " || ' deposits ' || (hash(r * 19) % 100000)::VARCHAR AS l_comment"
                        + " FROM range(" + part * rows / 2 + ", " + (part + 1) * rows / 2 + ") t(r))"
                        + " TO '" + dir.resolve("part-" + part + ".parquet") + "' (FORMAT parquet)");
            }
        }
    }

    /**
     * Reads the whole table in one stream.
     *
     * @param helper server and clients
     * @param codec compression header value
     * @return measurement
     */
    private static Result read(TestFlightServerHelper helper, String codec) {
        FlightCallHeaders planHeaders = new FlightCallHeaders();
        planHeaders.insert(ResultModeMiddleware.HEADER, "complete");
        FlightInfo info = helper.sqlClient().execute("SELECT * FROM " + TABLE, new HeaderCallOption(planHeaders));
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert(StreamCompression.HEADER, codec);
        CallOption option = new HeaderCallOption(headers);
        long inBefore = CompressionMetrics.inputBytes();
        long outBefore = CompressionMetrics.outputBytes();
        long cpuBefore = processCpuNanos();
        long start = System.nanoTime();
        long rows = 0;
        long bytes = 0;
        try (FlightStream stream = helper.flightClient().getStream(info.getEndpoints().get(0).getTicket(), option)) {
            while (stream.next()) {
                VectorSchemaRoot root = stream.getRoot();
                rows += root.getRowCount();
                for (FieldVector vector : root.getFieldVectors()) {
                    bytes += vector.getBufferSize();
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        double seconds = (System.nanoTime() - start) / 1e9;
        double cpu = (processCpuNanos() - cpuBefore) / 1e9;
        long in = CompressionMetrics.inputBytes() - inBefore;
        long out = CompressionMetrics.outputBytes() - outBefore;
        return new Result(rows, bytes, seconds, cpu, out == 0 ? 1.0 : (double) in / out);
    }

    /**
     * Reads the CPU time of this process.
     *
     * @return nanoseconds, or 0 when the JVM does not tell
     */
    private static long processCpuNanos() {
        if (ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.OperatingSystemMXBean os) {
            return os.getProcessCpuTime();
        }
        return 0;
    }

    /**
     * One read.
     *
     * @param rows rows received
     * @param bytes Arrow buffer bytes received
     * @param seconds wall time
     * @param cpuSeconds process CPU time
     * @param ratio compression input over output bytes, 1 when plain
     */
    private record Result(long rows, long bytes, double seconds, double cpuSeconds, double ratio) {
    }
}
