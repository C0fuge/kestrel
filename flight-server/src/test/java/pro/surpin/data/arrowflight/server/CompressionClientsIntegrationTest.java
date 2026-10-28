package pro.surpin.data.arrowflight.server;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.metrics.CompressionMetrics;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests that non-Java clients asking for arrow-compression - pyarrow Flight (C++) and the ADBC
 * Flight SQL driver (Go) - decode zstd and lz4 batches to the same answer as a plain read, in
 * both result modes. Needs {@code -Dpython.it.executable} pointing at a Python with pyarrow and
 * adbc-driver-flightsql; skipped without it.
 */
@Tag("integration")
class CompressionClientsIntegrationTest {

    private static final String TABLE = "pyc.wide";
    private static final int ROWS = 120_000;
    private static final String PYTHON = System.getProperty("python.it.executable", "");

    @TempDir
    static Path data;

    private static TestFlightServerHelper helper;

    /**
     * Writes two files of mixed column types and starts a server that compresses only on request.
     *
     * @throws Exception on write or startup failure
     */
    @BeforeAll
    static void startServer() throws Exception {
        assumeTrue(!PYTHON.isBlank(), "needs -Dpython.it.executable with pyarrow and adbc-driver-flightsql");
        Path dir = Files.createDirectories(data.resolve("pyc/wide"));
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            for (int part = 0; part < 2; part++) {
                s.execute("COPY (SELECT r::BIGINT AS id, (r % 97)::INTEGER AS k,"
                        + " CASE WHEN r % 11 = 0 THEN NULL ELSE 'name-' || (r % 1000) END AS name,"
                        + " (r * 1.25)::DECIMAL(18,2) AS amount, r / 7.0 AS ratio,"
                        + " DATE '2020-01-01' + (r % 3000)::INTEGER AS day,"
                        + " TIMESTAMP '2020-01-01' + INTERVAL (r) SECOND AS ts,"
                        + " r % 3 = 0 AS flag, [r % 5, r % 7] AS pair,"
                        + " {'a': r % 13, 'b': 'x' || (r % 17)} AS rec"
                        + " FROM range(" + part * ROWS / 2 + ", " + (part + 1) * ROWS / 2 + ") t(r))"
                        + " TO '" + dir.resolve("part-" + part + ".parquet") + "' (FORMAT parquet)");
            }
        }
        helper = TestFlightServerHelper.builder().dataDir(data.toString()).resultMode("partial").start();
    }

    /**
     * Stops the server.
     *
     * @throws Exception on shutdown failure
     */
    @AfterAll
    static void stopServer() throws Exception {
        if (helper != null) {
            helper.close();
        }
    }

    /**
     * Runs the Python client script: every client, mode and codec must give the plain answer, and
     * the compressed reads must really have been compressed by the server.
     *
     * @throws Exception on script or query failure
     */
    @Test
    void pyarrowAndAdbcDecodeCompressedAnswers() throws Exception {
        Path script = data.resolve("compression_clients.py");
        try (InputStream in = getClass().getResourceAsStream("/python/compression_clients.py")) {
            Files.copy(in, script, StandardCopyOption.REPLACE_EXISTING);
        }
        for (String client : List.of("pyarrow", "adbc-go")) {
            long before = CompressionMetrics.compressedBatches();
            String out = run(List.of(PYTHON, "-I", script.toString(), "localhost",
                    String.valueOf(helper.location.getUri().getPort()),
                    "SELECT * FROM " + TABLE, String.valueOf(ROWS), client));
            List<String> lines = out.lines().filter(l -> l.startsWith(client + " ")).toList();
            assertEquals(2 * 4, lines.size(), out);
            for (String line : lines) {
                assertTrue(line.endsWith("rows=" + ROWS + " same"), out);
            }
            // two modes x two codecs, each read over the size threshold: the client really asked
            assertTrue(CompressionMetrics.compressedBatches() - before >= 2 * 2, client + "\n" + out);
        }
    }

    /**
     * Runs a process, failing the test on a non-zero exit or a timeout.
     *
     * @param command command and arguments
     * @return combined standard output and error
     * @throws IOException on start failure
     * @throws InterruptedException when interrupted
     */
    private static String run(List<String> command) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        byte[] output;
        try (InputStream in = process.getInputStream()) {
            output = in.readAllBytes();
        }
        boolean done = process.waitFor(5, TimeUnit.MINUTES);
        String text = new String(output, StandardCharsets.UTF_8);
        if (!done) {
            process.destroyForcibly();
        }
        assertTrue(done, "python client timed out:\n" + text);
        assertEquals(0, process.exitValue(), text);
        return text;
    }
}
