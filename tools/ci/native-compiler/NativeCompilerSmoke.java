package pro.surpin.data.arrowflight.server;

import java.io.BufferedWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpServer;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBResultSet;

/** Checks one isolated native candidate with exact rows and the actual mapped JNI library hash. */
public final class NativeCompilerSmoke {
    private static final String TYPED = "SELECT 42::BIGINT, 170141183460469231731687303715884105727::HUGEINT,"
            + " 1.015::DECIMAL(8,3), DATE '1994-01-01', 'é🙂', NULL::INTEGER";
    private final BufferedWriter raw;
    private boolean failed;

    /**
     * Uses the append-only raw query log.
     *
     * @param raw raw sample writer
     */
    private NativeCompilerSmoke(BufferedWriter raw) {
        this.raw = raw;
    }

    /**
     * Runs a correctness-only smoke in one fresh JVM and fresh fixture directory.
     *
     * @param args output directory, expected native SHA-256, common corpus path and compiler
     * @throws Exception when any smoke or artifact operation fails
     */
    public static void main(String[] args) throws Exception {
        Path output = Path.of(args[0]);
        try (BufferedWriter raw = Files.newBufferedWriter(output.resolve("queries.csv"),
                java.nio.file.StandardOpenOption.CREATE_NEW)) {
            raw.write("query,start_epoch_ms,latency_ns,rows,checksum,arrow_bytes,batches,ok,error_type,checksum_version\n");
            raw.flush();
            NativeCompilerSmoke smoke = new NativeCompilerSmoke(raw);
            try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
                smoke.check("loaded_native", () -> loadedNative(output, args[1]));
                smoke.check("source_version", () -> {
                    TpchChecksum checksum = new TpchChecksum();
                    try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery(
                            "SELECT library_version, source_id FROM pragma_version()")) {
                        require(rs.next() && "v1.5.6".equals(rs.getString(1))
                                && rs.getString(2).startsWith("069cc9f9b5"), "Wrong native version/source ID");
                        checksum.addRow(rs.getString(1), rs.getString(2));
                    }
                    return new Metrics(1, checksum.value(), -1, -1);
                });
                long typedExpected = typedExpected();
                smoke.check("jdbc_types", () -> jdbc(connection, TYPED, 1, typedExpected));
                smoke.check("arrow_types", () -> arrow(connection, TYPED, 1, typedExpected));
                Path parquet = Path.of(args[2]).toAbsolutePath();
                if ("gcc".equals(args[3])) {
                    require(!Files.exists(parquet), "Corpus must be fresh and written by GCC only");
                    try (Statement st = connection.createStatement()) {
                        st.execute("COPY (SELECT i, i::DECIMAL(12,2), DATE '1994-01-01' + i::INTEGER,"
                                + " 'row ' || i FROM range(128) t(i)) TO '" + parquet
                                + "' (FORMAT PARQUET, COMPRESSION SNAPPY)");
                    }
                }
                require(Files.isRegularFile(parquet), "Missing common GCC-written corpus");
                TpchChecksum expected = new TpchChecksum();
                for (long i = 0; i < 128; i++) {
                    expected.addRow(i, BigDecimal.valueOf(i), LocalDate.of(1994, 1, 1).plusDays(i), "row " + i);
                }
                String sql = "SELECT * FROM read_parquet('" + parquet + "')";
                smoke.check("snappy_jdbc", () -> jdbc(connection, sql, 128, expected.value()));
                smoke.check("snappy_arrow", () -> arrow(connection, sql, 128, expected.value()));
                smoke.check("cancel_and_reuse", () -> cancelAndReuse(connection, output));
                smoke.check("native_extensions", () -> extensions(connection, parquet, expected.value(), output));
            } catch (Throwable error) {
                smoke.failed = true;
                smoke.record("startup_or_fixture", System.currentTimeMillis(), 0, null, error);
                error.printStackTrace();
            }
            if (smoke.failed) {
                throw new AssertionError("Native candidate smoke failed; raw attempts retained");
            }
        }
    }

    /**
     * Checks the actual mapped shared object rather than assuming the intended JAR was loaded.
     *
     * @param output evidence directory
     * @param expected expected library SHA-256
     * @return identity check result
     * @throws Exception when identity differs or cannot be proved
     */
    private static Metrics loadedNative(Path output, String expected) throws Exception {
        List<String> paths = Files.readAllLines(Path.of("/proc/self/maps")).stream()
                .filter(line -> line.contains("/libduckdb_java"))
                .map(line -> line.substring(line.indexOf('/')).strip()).distinct().toList();
        require(paths.size() == 1, "Expected one mapped DuckDB JNI library");
        Path mapped = Path.of(paths.getFirst());
        String actual = sha256(mapped);
        Files.writeString(output.resolve("loaded-native.json"), "{\"sha256\":\"" + actual
                + "\",\"expected_sha256\":\"" + expected + "\",\"mapped_basename\":\""
                + mapped.getFileName() + "\",\"pid\":" + ProcessHandle.current().pid() + "}\n",
                java.nio.file.StandardOpenOption.CREATE_NEW);
        require(actual.equals(expected), "Actual mapped native SHA differs");
        List<String> runtimes = Files.readAllLines(Path.of("/proc/self/maps")).stream()
                .filter(line -> line.contains("/libstdc++.so"))
                .map(line -> line.substring(line.indexOf('/')).strip()).distinct().toList();
        require(runtimes.size() == 1, "Expected one mapped libstdc++ runtime");
        Path runtime = Path.of(runtimes.getFirst());
        Files.writeString(output.resolve("libstdcxx-runtime.json"), "{\"sha256\":\"" + sha256(runtime)
                + "\",\"mapped_basename\":\"" + runtime.getFileName() + "\"}\n",
                java.nio.file.StandardOpenOption.CREATE_NEW);
        return new Metrics(-1, 0, -1, -1);
    }

    /**
     * Hashes an actually mapped file without buffering it whole.
     *
     * @param path mapped file
     * @return SHA-256 hex
     * @throws Exception when the file cannot be read
     */
    private static String sha256(Path path) throws Exception {
        try (var stream = Files.newInputStream(path)) {
            MessageDigest hash = MessageDigest.getInstance("SHA-256");
            byte[] block = new byte[65536];
            int count;
            while ((count = stream.read(block)) != -1) {
                hash.update(block, 0, count);
            }
            return HexFormat.of().formatHex(hash.digest());
        }
    }

    /**
     * Computes a type-sensitive independent expected answer.
     *
     * @return canonical expected checksum
     */
    private static long typedExpected() {
        TpchChecksum checksum = new TpchChecksum();
        checksum.addRow(42L, new BigInteger("170141183460469231731687303715884105727"),
                new BigDecimal("1.015"), LocalDate.of(1994, 1, 1), "é🙂", null);
        return checksum.value();
    }

    /**
     * Reads and checks a complete JDBC answer.
     *
     * @param connection candidate connection
     * @param sql fixture SQL
     * @param expectedRows exact row count
     * @param expectedChecksum exact value checksum
     * @return consumed answer measurements
     * @throws Exception when execution or validation fails
     */
    private static Metrics jdbc(Connection connection, String sql, long expectedRows, long expectedChecksum)
            throws Exception {
        long rows = 0;
        TpchChecksum checksum = new TpchChecksum();
        try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                Object[] values = new Object[rs.getMetaData().getColumnCount()];
                for (int column = 0; column < values.length; column++) {
                    Object value = rs.getObject(column + 1);
                    values[column] = value instanceof java.sql.Date day ? day.toLocalDate() : value;
                }
                checksum.addRow(values);
                rows++;
            }
        }
        require(rows == expectedRows && checksum.value() == expectedChecksum, "JDBC answer mismatch");
        return new Metrics(rows, checksum.value(), -1, -1);
    }

    /**
     * Reads delayed Arrow batches and checks their logical values.
     *
     * @param connection candidate connection
     * @param sql fixture SQL
     * @param expectedRows exact row count
     * @param expectedChecksum exact value checksum
     * @return consumed Arrow buffer measurements
     * @throws Exception when execution, lifetime or validation fails
     */
    private static Metrics arrow(Connection connection, String sql, long expectedRows, long expectedChecksum)
            throws Exception {
        long rows = 0;
        long bytes = 0;
        long batches = 0;
        TpchChecksum checksum = new TpchChecksum();
        try (RootAllocator allocator = new RootAllocator(); Statement st = connection.createStatement();
                DuckDBResultSet result = (DuckDBResultSet) st.executeQuery(sql);
                ArrowReader reader = (ArrowReader) result.arrowExportStream(allocator, 31)) {
            while (reader.loadNextBatch()) {
                var root = reader.getVectorSchemaRoot();
                Thread.sleep(2);
                for (var vector : root.getFieldVectors()) {
                    bytes += vector.getBufferSize();
                }
                for (int row = 0; row < root.getRowCount(); row++) {
                    Object[] values = new Object[root.getFieldVectors().size()];
                    for (int column = 0; column < values.length; column++) {
                        values[column] = TpchArrowValues.cell(root.getVector(column), row);
                    }
                    checksum.addRow(values);
                }
                rows += root.getRowCount();
                batches++;
            }
        }
        require(rows == expectedRows && checksum.value() == expectedChecksum && batches > 0, "Arrow answer mismatch");
        return new Metrics(rows, checksum.value(), bytes, batches);
    }

    /**
     * Cancels native execution and reuses the same connection with a fresh statement.
     *
     * @param connection candidate connection
     * @param output evidence directory
     * @return reuse check result
     * @throws Exception when cancellation or reuse fails
     */
    private static Metrics cancelAndReuse(Connection connection, Path output) throws Exception {
        var pool = Executors.newSingleThreadExecutor(task -> {
            Thread thread = new Thread(task, "native-smoke-cancel");
            thread.setDaemon(true);
            return thread;
        });
        try (Statement st = connection.createStatement()) {
            CountDownLatch started = new CountDownLatch(1);
            var future = pool.submit(() -> {
                long epoch = System.currentTimeMillis();
                long begin = System.nanoTime();
                started.countDown();
                try (ResultSet ignored = st.executeQuery("SELECT sum(a.i*b.i) FROM range(1000000000) a(i),"
                        + " range(1000000000) b(i)")) {
                    return false;
                } catch (java.sql.SQLException expected) {
                    boolean interrupted = expected.getMessage().toLowerCase(Locale.ROOT).contains("interrupt");
                    Files.writeString(output.resolve("cancellation-attempt.json"), "{\"start_epoch_ms\":" + epoch
                            + ",\"elapsed_ns\":" + (System.nanoTime() - begin) + ",\"rows\":-1,\"arrow_bytes\":-1,"
                            + "\"batches\":-1,\"checksum\":\"0000000000000000\",\"interrupted\":" + interrupted
                            + ",\"error_type\":\"" + expected.getClass().getSimpleName() + "\"}\n",
                            java.nio.file.StandardOpenOption.CREATE_NEW);
                    return interrupted;
                }
            });
            require(started.await(5, TimeUnit.SECONDS), "Cancellation worker did not start");
            for (int i = 0; i < 200 && !future.isDone(); i++) {
                try {
                    st.cancel();
                } catch (java.sql.SQLException stopped) {
                    if (!st.isClosed()) {
                        throw stopped;
                    }
                    break;
                }
                Thread.sleep(5);
            }
            require(future.get(5, TimeUnit.SECONDS), "Query was not interrupted");
            try (Statement reuse = connection.createStatement(); ResultSet result = reuse.executeQuery("SELECT 42")) {
                require(result.next() && result.getInt(1) == 42, "Connection cannot be reused after cancellation");
            }
        } finally {
            pool.shutdownNow();
            require(pool.awaitTermination(5, TimeUnit.SECONDS), "Cancellation worker did not stop");
        }
        TpchChecksum checksum = new TpchChecksum();
        checksum.addRow(42);
        return new Metrics(1, checksum.value(), -1, -1);
    }

    /**
     * Loads signed stock extensions and reads the Snappy fixture through httpfs.
     *
     * @param connection candidate connection
     * @param parquet generated fixture
     * @param expected exact checksum
     * @param output evidence directory
     * @return extension query measurements
     * @throws Exception when the stock extensions cannot load or execute correctly
     */
    private static Metrics extensions(Connection connection, Path parquet, long expected, Path output) throws Exception {
        try (Statement st = connection.createStatement()) {
            st.execute("SET extension_directory='" + output.resolve("extensions").toAbsolutePath() + "'");
            st.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;");
            try (ResultSet result = st.executeQuery("SELECT count(*) FROM duckdb_extensions()"
                    + " WHERE extension_name IN ('httpfs','iceberg') AND loaded")) {
                require(result.next() && result.getInt(1) == 2, "Stock extensions did not load");
            }
        }
        byte[] fixture = Files.readAllBytes(parquet);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/fixture.parquet", exchange -> {
            exchange.getResponseHeaders().set("Content-Length", Integer.toString(fixture.length));
            if ("HEAD".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(200, -1);
            } else {
                exchange.sendResponseHeaders(200, fixture.length);
                exchange.getResponseBody().write(fixture);
            }
            exchange.close();
        });
        server.start();
        try {
            String sql = "SELECT * FROM read_parquet('http://127.0.0.1:" + server.getAddress().getPort()
                    + "/fixture.parquet')";
            return arrow(connection, sql, 128, expected);
        } finally {
            server.stop(0);
        }
    }

    /**
     * Retains one attempt before continuing to the next independent smoke.
     *
     * @param name smoke name
     * @param action smoke action
     * @throws Exception when raw evidence cannot be saved
     */
    private void check(String name, Check action) throws Exception {
        long epoch = System.currentTimeMillis();
        long started = System.nanoTime();
        try {
            Metrics metrics = action.run();
            record(name, epoch, System.nanoTime() - started, metrics, null);
        } catch (Throwable error) {
            failed = true;
            record(name, epoch, System.nanoTime() - started, null, error);
            error.printStackTrace();
        }
    }

    /**
     * Flushes success or honest unknown failure metrics.
     *
     * @param name smoke name
     * @param epoch start epoch milliseconds
     * @param elapsed elapsed nanoseconds
     * @param result complete result or null
     * @param error failure or null
     * @throws Exception when writing fails
     */
    private void record(String name, long epoch, long elapsed, Metrics result, Throwable error) throws Exception {
        raw.write(String.format(Locale.ROOT, "%s,%d,%d,%d,%016x,%d,%d,%d,%s,%s%n", name, epoch, elapsed,
                result == null ? -1 : result.rows(), result == null ? 0 : result.checksum(),
                result == null ? -1 : result.arrowBytes(), result == null ? -1 : result.batches(),
                error == null ? 1 : 0, error == null ? "" : error.getClass().getSimpleName(), TpchChecksum.VERSION));
        raw.flush();
    }

    /**
     * Requires a correctness invariant.
     *
     * @param condition invariant
     * @param message failure detail
     */
    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    /**
     * Describes one complete smoke result.
     *
     * @param rows result rows
     * @param checksum canonical checksum
     * @param arrowBytes Arrow buffers or -1 when unavailable
     * @param batches Arrow batches or -1 when unavailable
     */
    private record Metrics(long rows, long checksum, long arrowBytes, long batches) {
    }

    /** Runs one independently recorded correctness check. */
    @FunctionalInterface
    private interface Check {
        /**
         * Executes the check.
         *
         * @return complete measurements
         * @throws Exception when the check fails
         */
        Metrics run() throws Exception;
    }
}
