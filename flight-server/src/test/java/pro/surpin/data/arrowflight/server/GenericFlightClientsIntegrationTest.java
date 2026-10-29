package pro.surpin.data.arrowflight.server;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests plain Flight descriptors (plainFlightCommands) with non-Java generic Flight clients: pyarrow
 * Flight (needs {@code -Dpython.it.executable}) and ClickHouse's {@code arrowFlight} table function
 * run as {@code clickhouse local} in Docker (needs {@code -Dclickhouse.it.image}) and DuckDB's
 * community {@code airport} extension (needs {@code -Dairport.it.python}: a Python with the duckdb
 * package and the extension installed); each is skipped without its property.
 */
@Tag("integration")
class GenericFlightClientsIntegrationTest {

    private static final String PYTHON = System.getProperty("python.it.executable", "");
    private static final String CLICKHOUSE = System.getProperty("clickhouse.it.image", "");
    private static final String AIRPORT = System.getProperty("airport.it.python", "");
    private static final int ROWS = 50_000;
    private static final String KEY_FILTER = "k = 3";

    @TempDir
    static Path data;

    private static TestFlightServerHelper helper;
    /** DuckDB's answers over the same files: count, sum(id), NULL names, sum(amount) in cents, max(day), rows and sum with k = 3. */
    private static List<String> expected;

    /**
     * Writes two files and starts a server with plain descriptors on, whose Flight SQL default is parts.
     *
     * @throws Exception on write or startup failure
     */
    @BeforeAll
    static void startServer() throws Exception {
        Path dir = Files.createDirectories(data.resolve("gfc/items"));
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            for (int part = 0; part < 2; part++) {
                s.execute("COPY (SELECT r::BIGINT AS id, (r % 7)::INTEGER AS k,"
                        + " CASE WHEN r % 11 = 0 THEN NULL ELSE 'name-' || (r % 1000) END AS name,"
                        + " (r * 1.25)::DECIMAL(18,2) AS amount,"
                        + " DATE '2020-01-01' + (r % 3000)::INTEGER AS day"
                        + " FROM range(" + part * ROWS / 2 + ", " + (part + 1) * ROWS / 2 + ") t(r))"
                        + " TO '" + dir.resolve("part-" + part + ".parquet") + "' (FORMAT parquet)");
            }
            String files = "read_parquet('" + dir + "/*.parquet')";
            try (ResultSet rs = s.executeQuery("SELECT count(*), sum(id), count(*) - count(name),"
                    + " (sum(amount) * 100)::BIGINT, max(day)::VARCHAR,"
                    + " (SELECT count(*) FROM " + files + " WHERE " + KEY_FILTER + "),"
                    + " (SELECT sum(id) FROM " + files + " WHERE " + KEY_FILTER + ") FROM " + files)) {
                rs.next();
                expected = new ArrayList<>();
                for (int i = 1; i <= 7; i++) {
                    expected.add(rs.getString(i));
                }
            }
        }
        helper = TestFlightServerHelper.builder().dataDir(data.toString()).resultMode("partial").start();
        helper.flightSqlProducer.acceptPlainCommands(true);
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
     * Reads a table path, a SQL text and an aggregate through plain pyarrow Flight: whole answers,
     * one complete endpoint, GetSchema equal to the stream's schema.
     *
     * @throws Exception on script or query failure
     */
    @Test
    void pyarrowReadsPlainDescriptors() throws Exception {
        assumeTrue(!PYTHON.isBlank(), "needs -Dpython.it.executable with pyarrow");
        Path script = data.resolve("plain_flight_client.py");
        try (InputStream in = getClass().getResourceAsStream("/python/plain_flight_client.py")) {
            Files.copy(in, script, StandardCopyOption.REPLACE_EXISTING);
        }
        String port = String.valueOf(helper.location.getUri().getPort());
        String whole = "rows=" + expected.get(0) + " endpoints=1 sum=" + expected.get(1) + " same_schema=True";
        assertEquals(whole, run(List.of(PYTHON, "-I", script.toString(), "localhost", port, "path",
                "gfc.items")).strip());
        assertEquals(whole, run(List.of(PYTHON, "-I", script.toString(), "localhost", port, "path",
                "gfc/items")).strip());
        assertEquals("rows=" + expected.get(5) + " endpoints=1 sum=" + expected.get(6) + " same_schema=True",
                run(List.of(PYTHON, "-I", script.toString(), "localhost", port, "command",
                        "SELECT id FROM gfc.items WHERE " + KEY_FILTER)).strip());
        assertEquals("rows=1 endpoints=1 sum=" + expected.get(0) + " same_schema=True",
                run(List.of(PYTHON, "-I", script.toString(), "localhost", port, "command",
                        "SELECT count(*) AS n FROM gfc.items")).strip());
    }

    /**
     * Reads the table with ClickHouse's arrowFlight table function, by path (its default) and by
     * SQL command; ClickHouse computes its aggregates over every row the server sends.
     *
     * @throws Exception on Docker or query failure
     */
    @Test
    void clickhouseArrowFlightReadsTheTable() throws Exception {
        assumeTrue(!CLICKHOUSE.isBlank(), "needs -Dclickhouse.it.image and Docker");
        String address = "localhost:" + helper.location.getUri().getPort();
        String aggregates = "SELECT count(), sum(id), countIf(isNull(name)), toInt64(sum(amount) * 100),"
                + " toString(max(day))";
        String path = run(clickhouse(aggregates + " FROM arrowFlight('" + address + "', 'gfc.items')"
                + " FORMAT TSV"));
        assertEquals(String.join("\t", expected.subList(0, 5)), lastLine(path), path);
        String command = run(clickhouse("SELECT count(), sum(id) FROM arrowFlight('" + address
                + "', 'gfc.items WHERE " + KEY_FILTER + "')"
                + " SETTINGS arrow_flight_request_descriptor_type = 'command' FORMAT TSV"));
        assertEquals(expected.get(5) + "\t" + expected.get(6), lastLine(command), command);
    }

    /**
     * Reads a table path (dotted and as elements), a SQL text and an aggregate with DuckDB's
     * airport extension (airport_take_flight): whole answers over every endpoint.
     *
     * @throws Exception on script or query failure
     */
    @Test
    void duckdbAirportReadsPlainDescriptors() throws Exception {
        assumeTrue(!AIRPORT.isBlank(), "needs -Dairport.it.python with duckdb and the airport extension");
        Path script = data.resolve("airport_client.py");
        try (InputStream in = getClass().getResourceAsStream("/python/airport_client.py")) {
            Files.copy(in, script, StandardCopyOption.REPLACE_EXISTING);
        }
        String port = String.valueOf(helper.location.getUri().getPort());
        String whole = "rows=" + expected.get(0) + " sum=" + expected.get(1);
        assertEquals(whole, lastLine(run(List.of(AIRPORT, "-I", script.toString(), "localhost", port, "path",
                "gfc.items"))));
        assertEquals(whole, lastLine(run(List.of(AIRPORT, "-I", script.toString(), "localhost", port, "path",
                "gfc/items"))));
        assertEquals("rows=" + expected.get(5) + " sum=" + expected.get(6),
                lastLine(run(List.of(AIRPORT, "-I", script.toString(), "localhost", port, "command",
                        "SELECT id FROM gfc.items WHERE " + KEY_FILTER))));
        assertEquals("rows=1 sum=" + expected.get(0),
                lastLine(run(List.of(AIRPORT, "-I", script.toString(), "localhost", port, "command",
                        "SELECT count(*) AS n FROM gfc.items"))));
    }

    /**
     * Builds the Docker command running one query with clickhouse local on the host network.
     *
     * @param query ClickHouse SQL
     * @return command and arguments
     */
    private static List<String> clickhouse(String query) {
        return List.of("docker", "run", "--rm", "--network", "host", CLICKHOUSE,
                "clickhouse", "local", "--query", query);
    }

    /**
     * Returns the last non-empty line of an output.
     *
     * @param text process output
     * @return last line
     */
    private static String lastLine(String text) {
        List<String> lines = text.lines().filter(l -> !l.isBlank()).toList();
        return lines.isEmpty() ? "" : lines.get(lines.size() - 1).strip();
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
        boolean done = process.waitFor(10, TimeUnit.MINUTES);
        String text = new String(output, StandardCharsets.UTF_8);
        if (!done) {
            process.destroyForcibly();
        }
        assertTrue(done, "client timed out:\n" + text);
        assertEquals(0, process.exitValue(), String.join(" ", command) + "\n" + text);
        return text;
    }
}
