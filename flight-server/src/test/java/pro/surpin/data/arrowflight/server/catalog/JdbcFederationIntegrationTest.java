package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.server.TestFlightServerHelper;
import pro.surpin.data.arrowflight.server.adapters.ResultModeMiddleware;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;
import pro.surpin.data.arrowflight.server.plugin.PluginSettings;
import pro.surpin.data.arrowflight.server.storage.StoreRegistry;

/**
 * C2: PostgreSQL and MySQL tables through the jdbc plugin (catalog=jdbc) and DuckDB's scanner
 * extensions, against real databases. Answers through the server equal DuckDB's own scan of the
 * same remote tables, in complete and partial mode; the scanners' functions and the attachments
 * cannot be named by clients; only configured tables are shown; the password appears nowhere.
 * Skipped unless {@code JDBC_IT_EXTENSION_DIR} (with {@code postgres_scanner.duckdb_extension}
 * and {@code mysql_scanner.duckdb_extension} for DuckDB 1.5.6) and the {@code PROBE_PG_*} /
 * {@code PROBE_MYSQL_*} variables of the Catalogs CI job {@code jdbc-scanners} are set (tables
 * {@code t} and {@code types} in both databases).
 */
@Tag("jdbc-federation")
class JdbcFederationIntegrationTest {

    private static final Path PLUGIN = Path.of("../flight-plugin-jdbc/target/plugin/jdbc");
    private static final String VERSION = "v1.5.6";
    private static final String PLATFORM = "linux_amd64";

    @TempDir
    static Path dir;
    static TestFlightServerHelper helper;
    static Path extensions;
    /** TPC-H tables. */
    private static final List<String> TPCH = List.of("customer", "lineitem", "nation", "orders", "part",
            "partsupp", "region", "supplier");
    /** DuckDB holding the generated TPC-H data (the reference), or null without TPC-H. */
    static Connection tpch;

    @BeforeAll
    static void start() throws Exception {
        String source = env("JDBC_IT_EXTENSION_DIR").orElse(null);
        Assumptions.assumeTrue(source != null && env("PROBE_PG_HOST").isPresent()
                && env("PROBE_MYSQL_HOST").isPresent(),
                "no databases (JDBC_IT_EXTENSION_DIR, PROBE_PG_*, PROBE_MYSQL_*)");
        Assumptions.assumeTrue(Files.isDirectory(PLUGIN), "flight-plugin-jdbc is not built");
        extensions = Files.createDirectories(dir.resolve("ext").resolve(VERSION).resolve(PLATFORM));
        for (String name : List.of("postgres_scanner", "mysql_scanner")) {
            Files.copy(Path.of(source, name + ".duckdb_extension"),
                    extensions.resolve(name + ".duckdb_extension"));
        }
        String pgTables = "public.t,public.types";
        Optional<String> scale = env("JDBC_IT_TPCH_SF");
        if (scale.isPresent()) {
            Files.copy(Path.of(source, "tpch.duckdb_extension"), extensions.resolve("tpch.duckdb_extension"));
            loadTpch(scale.get());
            pgTables += "," + String.join(",", TPCH.stream().map(t -> "public." + t).toList());
        }
        Path plugins = dir.resolve("plugins");
        Path target = Files.createDirectories(plugins.resolve("jdbc"));
        try (Stream<Path> jars = Files.list(PLUGIN)) {
            for (Path jar : jars.toList()) {
                Files.copy(jar, target.resolve(jar.getFileName().toString()));
            }
        }
        Map<String, String> sys = new HashMap<>();
        sys.put("arrowflight.duckdb.extensionDir", dir.resolve("ext").toString());
        sys.put("arrowflight.jdbc.sources", "pg,my");
        sys.put("arrowflight.jdbc.pg.type", "postgres");
        sys.put("arrowflight.jdbc.pg.host", env("PROBE_PG_HOST").orElseThrow());
        sys.put("arrowflight.jdbc.pg.port", env("PROBE_PG_PORT").orElse("5432"));
        sys.put("arrowflight.jdbc.pg.database", env("PROBE_PG_DATABASE").orElseThrow());
        sys.put("arrowflight.jdbc.pg.user", env("PROBE_PG_USER").orElseThrow());
        sys.put("arrowflight.jdbc.pg.tables", pgTables);
        sys.put("arrowflight.jdbc.my.type", "mysql");
        sys.put("arrowflight.jdbc.my.host", env("PROBE_MYSQL_HOST").orElseThrow());
        sys.put("arrowflight.jdbc.my.port", env("PROBE_MYSQL_PORT").orElse("3306"));
        sys.put("arrowflight.jdbc.my.database", env("PROBE_MYSQL_DATABASE").orElseThrow());
        sys.put("arrowflight.jdbc.my.user", env("PROBE_MYSQL_USER").orElseThrow());
        sys.put("arrowflight.jdbc.my.tables", env("PROBE_MYSQL_DATABASE").orElseThrow() + ".t,"
                + env("PROBE_MYSQL_DATABASE").orElseThrow() + ".types");
        Map<String, String> envs = new HashMap<>();
        envs.put("FLIGHT_JDBC_PG_PASSWORD", env("PROBE_PG_PASSWORD").orElseThrow());
        envs.put("FLIGHT_JDBC_MY_PASSWORD", env("PROBE_MYSQL_PASSWORD").orElseThrow());
        Files.createDirectories(dir.resolve("data"));
        ConfigSource config = new ConfigSource(sys::get, new Properties(), envs, null);
        PluginSettings settings = new PluginSettings(plugins, null, "jdbc", false);
        helper = TestFlightServerHelper.builder().dataDir(dir.resolve("data").toString())
                .resultMode("complete")
                .registry(c -> StoreRegistry.open(c, new Configuration(), "localhost",
                        settings, config, List.of()))
                .start();
    }

    @AfterAll
    static void stop() throws Exception {
        if (helper != null) {
            helper.close();
        }
        if (tpch != null) {
            tpch.close();
        }
    }

    /**
     * Generates TPC-H data in an in-memory DuckDB (kept as the reference) and copies every table
     * into PostgreSQL through a writable attachment of the scanner.
     *
     * @param scale scale factor, for example 0.1
     * @throws Exception on failure
     */
    private static void loadTpch(String scale) throws Exception {
        Properties props = new Properties();
        props.setProperty("allow_unsigned_extensions", "true");
        tpch = DriverManager.getConnection("jdbc:duckdb:", props);
        try (Statement s = tpch.createStatement()) {
            s.execute("LOAD '" + extensions.resolve("tpch.duckdb_extension") + "'");
            s.execute("CALL dbgen(sf = " + Double.parseDouble(scale) + ")");
            s.execute("LOAD '" + extensions.resolve("postgres_scanner.duckdb_extension") + "'");
            s.execute("CREATE TEMPORARY SECRET w_secret (TYPE postgres, HOST '" + env("PROBE_PG_HOST").orElseThrow()
                    + "', PORT " + env("PROBE_PG_PORT").orElseThrow() + ", DATABASE '"
                    + env("PROBE_PG_DATABASE").orElseThrow() + "', USER '" + env("PROBE_PG_USER").orElseThrow()
                    + "', PASSWORD '" + env("PROBE_PG_PASSWORD").orElseThrow() + "')");
            s.execute("ATTACH '' AS w (TYPE postgres, SECRET w_secret)");
            for (String table : TPCH) {
                s.execute("DROP TABLE IF EXISTS w.public." + table);
                s.execute("CREATE TABLE w.public." + table + " AS SELECT * FROM memory.main." + table);
            }
            s.execute("DETACH w");
        }
    }

    private static Optional<String> env(String name) {
        return Optional.ofNullable(System.getenv(name)).filter(v -> !v.isBlank());
    }

    @Test
    void onlyConfiguredTablesAreShown() throws Exception {
        List<String> schemas = column(helper.sqlClient().getSchemas(null, null), "db_schema_name");
        assertTrue(schemas.containsAll(List.of("pg", "my")), schemas.toString());
        assertFalse(schemas.stream().anyMatch(s -> s.startsWith("jdbc_attached")), schemas.toString());
        for (String schema : List.of("pg", "my")) {
            assertEquals(List.of("t", "types"), column(helper.sqlClient().getTables(null, schema, null,
                    null, false), "table_name").stream().sorted().toList(), schema);
        }
    }

    @Test
    void answersEqualDuckDbsOwnScan() throws Exception {
        for (String[] table : new String[][] {{"pg", "postgres", "public"},
                {"my", "mysql", env("PROBE_MYSQL_DATABASE").orElseThrow()}}) {
            for (String name : List.of("t", "types")) {
                // Both sides as DuckDB's text of each value, so that types compare alike.
                String text = "SELECT COLUMNS(*)::VARCHAR FROM ";
                List<String> expected = direct(table[1], text + "src." + table[2] + "." + name);
                assertEquals(expected, answer(text + table[0] + "." + name, "complete"), name);
                String sql = "SELECT * FROM " + table[0] + "." + name;
                assertEquals(answer(sql, "complete"), answer(sql, "partial"), "partial = complete: " + sql);
                FlightInfo info = info(sql, "partial");
                assertEquals(1, info.getEndpoints().size(), "one node answers: " + sql);
            }
            // Filters and projections are pushed down; the rows are the same either way.
            assertEquals(List.of("2|b"), answer("SELECT id, name FROM " + table[0] + ".t WHERE id = 2",
                    "partial"));
            assertEquals(List.of("3"), answer("SELECT count(*) FROM " + table[0] + ".t", "complete"));
        }
        assertEquals(List.of("1|a|1|a", "2|b|2|b", "3|c|3|c"), answer(
                "SELECT p.id, p.name, m.id, m.name FROM pg.t p JOIN my.t m ON p.id = m.id ORDER BY p.id",
                "complete"));
    }

    /**
     * TPC-H through the server (the 22 queries, unqualified table names: only the pg source has
     * the TPC-H tables) equals DuckDB over the generated data. Numbers compare with a relative
     * tolerance: averages of doubles depend on the summation order.
     */
    @Test
    void tpchAnswersEqualDuckDbOverTheSameData() throws Exception {
        Assumptions.assumeTrue(tpch != null, "no TPC-H data (JDBC_IT_TPCH_SF)");
        Map<Integer, String> queries = new java.util.TreeMap<>();
        try (Statement s = tpch.createStatement();
                ResultSet rs = s.executeQuery("SELECT query_nr, query FROM tpch_queries()")) {
            while (rs.next()) {
                queries.put(rs.getInt(1), rs.getString(2));
            }
        }
        assertEquals(22, queries.size());
        for (Map.Entry<Integer, String> query : queries.entrySet()) {
            List<String> expected = new ArrayList<>();
            try (Statement s = tpch.createStatement(); ResultSet rs = s.executeQuery(query.getValue())) {
                int columns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    List<String> values = new ArrayList<>();
                    for (int i = 1; i <= columns; i++) {
                        values.add(normalize(rs.getObject(i)));
                    }
                    expected.add(String.join("|", values));
                }
            }
            List<String> actual = new ArrayList<>();
            for (FlightEndpoint endpoint : info(query.getValue(), "complete").getEndpoints()) {
                try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                    while (stream.next()) {
                        actual.addAll(rows(stream.getRoot()));
                    }
                }
            }
            assertFalse(expected.isEmpty(), "Q" + query.getKey() + " answers rows");
            assertClose(expected, actual, "Q" + query.getKey());
        }
    }

    /**
     * Compares rows value by value, numbers with a relative tolerance of 1e-9.
     *
     * @param expected expected rows
     * @param actual actual rows
     * @param what query, for messages
     */
    private static void assertClose(List<String> expected, List<String> actual, String what) {
        assertEquals(expected.size(), actual.size(), what + " rows: " + actual);
        for (int r = 0; r < expected.size(); r++) {
            String[] e = expected.get(r).split("\\|", -1);
            String[] a = actual.get(r).split("\\|", -1);
            assertEquals(e.length, a.length, what + " row " + r);
            for (int c = 0; c < e.length; c++) {
                if (e[c].equals(a[c])) {
                    continue;
                }
                try {
                    double x = Double.parseDouble(e[c]);
                    double y = Double.parseDouble(a[c]);
                    assertTrue(Math.abs(x - y) <= 1e-9 * Math.max(1, Math.abs(x)),
                            what + " row " + r + " column " + c + ": " + e[c] + " vs " + a[c]);
                } catch (NumberFormatException ex) {
                    assertEquals(e[c], a[c], what + " row " + r + " column " + c);
                }
            }
        }
    }

    @Test
    void clientsCannotReachTheRemoteDatabasesOtherwise() {
        String password = env("PROBE_PG_PASSWORD").orElseThrow();
        for (String sql : List.of(
                "SELECT * FROM postgres_query('jdbc_attached_pg', 'SELECT 1')",
                "SELECT * FROM postgres_scan('host=127.0.0.1 dbname=probe', 'public', 't')",
                "SELECT * FROM postgres_execute('jdbc_attached_pg', 'SELECT 1')",
                "SELECT * FROM mysql_query('jdbc_attached_my', 'SELECT 1')",
                "SELECT * FROM jdbc_attached_pg.public.t",
                "SELECT * FROM jdbc_attached_my.t",
                "SELECT * FROM duckdb_secrets()",
                "ATTACH 'host=127.0.0.1' AS x (TYPE postgres)",
                "DETACH jdbc_attached_pg")) {
            for (String mode : List.of("complete", "partial")) {
                FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                        () -> answer(sql, mode), sql);
                assertFalse(String.valueOf(e.getMessage()).contains(password), sql);
            }
        }
    }

    /**
     * Reads a remote table with DuckDB's scanner directly (the reference).
     *
     * @param type postgres or mysql
     * @param sql query over the attachment {@code src}
     * @return rows as text, sorted
     * @throws Exception on failure
     */
    private static List<String> direct(String type, String sql) throws Exception {
        String prefix = "postgres".equals(type) ? "PROBE_PG_" : "PROBE_MYSQL_";
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("LOAD '" + extensions.resolve(type + "_scanner.duckdb_extension") + "'");
            s.execute("CREATE TEMPORARY SECRET src_secret (TYPE " + type + ", HOST '"
                    + env(prefix + "HOST").orElseThrow() + "', PORT " + env(prefix + "PORT").orElseThrow()
                    + ", DATABASE '" + env(prefix + "DATABASE").orElseThrow() + "', USER '"
                    + env(prefix + "USER").orElseThrow() + "', PASSWORD '"
                    + env(prefix + "PASSWORD").orElseThrow() + "')");
            s.execute("ATTACH '' AS src (TYPE " + type + ", SECRET src_secret, READ_ONLY)");
            List<String> rows = new ArrayList<>();
            try (ResultSet rs = s.executeQuery(sql)) {
                ResultSetMetaData meta = rs.getMetaData();
                while (rs.next()) {
                    List<String> values = new ArrayList<>();
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        values.add(normalize(rs.getObject(i)));
                    }
                    rows.add(String.join("|", values));
                }
            }
            rows.sort(null);
            return rows;
        }
    }

    /**
     * Writes a value the way both sides can compare: text of numbers, dates and strings; bytes
     * as a list.
     *
     * @param value JDBC or Arrow value
     * @return text
     */
    private static String normalize(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof byte[] bytes) {
            return Arrays.toString(bytes);
        }
        if (value instanceof java.sql.Date date) {
            return date.toLocalDate().toString();
        }
        if (value instanceof java.sql.Blob blob) {
            try {
                return Arrays.toString(blob.getBytes(1, (int) blob.length()));
            } catch (java.sql.SQLException e) {
                throw new IllegalStateException(e);
            }
        }
        return String.valueOf(value);
    }

    private static List<String> answer(String sql, String mode) throws Exception {
        FlightInfo info = info(sql, mode);
        List<String> rows = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    rows.addAll(rows(stream.getRoot()));
                }
            }
        }
        if (!sql.contains("ORDER BY")) {
            rows.sort(null);
        }
        return rows;
    }

    private static List<String> rows(VectorSchemaRoot root) {
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < root.getRowCount(); i++) {
            List<String> values = new ArrayList<>();
            for (FieldVector vector : root.getFieldVectors()) {
                Object value = vector.isNull(i) ? null : vector.getObject(i);
                if (value != null && vector instanceof org.apache.arrow.vector.DateDayVector) {
                    value = java.time.LocalDate.ofEpochDay((Integer) value);
                }
                values.add(normalize(value));
            }
            rows.add(String.join("|", values));
        }
        return rows;
    }

    private static FlightInfo info(String sql, String mode) {
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert(ResultModeMiddleware.HEADER, mode);
        return helper.sqlClient().execute(sql, new HeaderCallOption(headers));
    }

    private static List<String> column(FlightInfo info, String name) throws Exception {
        List<String> values = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VarCharVector vector = (VarCharVector) stream.getRoot().getVector(name);
                    for (int i = 0; i < stream.getRoot().getRowCount(); i++) {
                        values.add(new String(vector.get(i), java.nio.charset.StandardCharsets.UTF_8));
                    }
                }
            }
        }
        return values;
    }
}
