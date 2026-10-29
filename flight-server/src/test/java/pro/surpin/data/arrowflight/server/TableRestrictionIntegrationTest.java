package pro.surpin.data.arrowflight.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;

/**
 * Runs a provider that restricts tables in a real Flight SQL server: a user with a row filter and
 * column masks on a table gets exactly the filtered, masked rows (computed by DuckDB over the same
 * files) in plain queries, count(*), joins, self-joins, subqueries and prepared statements, in
 * partial mode (answered complete, one endpoint) and complete mode, with the unrestricted schema;
 * the unrestricted user's complete answers stay whole; a partial ticket planned for another user is not
 * redeemed unrestricted.
 */
@Tag("integration")
class TableRestrictionIntegrationTest {

    private static final String PASSWORD = "test-password";
    private static final int FILES = 2;
    private static final int ROWS = 1_000;
    /** Alice's view of s.people: EU rows, name masked to its first letter, salary hidden. */
    private static final TableRestriction ALICE = new TableRestriction(true, "region = 'eu'",
            Map.of("name", "left(name, 1) || '***'", "salary", "NULL"));
    /** Columns the server passed per table, for the check that it passes them all. */
    private static final Map<String, List<ColumnSpec>> SEEN = new ConcurrentHashMap<>();

    /** Restricts s.people for alice; bob reads everything. */
    private static final FlightAuthorization PROVIDER = new FlightAuthorization() {
        @Override
        public boolean canSeeSchema(String principal, String schema) {
            return "s".equals(schema);
        }

        @Override
        public boolean canReadTable(String principal, String schema, String table) {
            return canSeeSchema(principal, schema);
        }

        @Override
        public boolean restrictsTables() {
            return true;
        }

        @Override
        public TableRestriction restrictTable(String principal, String schema, String table,
                List<ColumnSpec> columns) {
            SEEN.put(table, columns);
            return "alice".equals(principal) && "people".equals(table) ? ALICE
                    : FlightAuthorization.super.restrictTable(principal, schema, table, columns);
        }
    };

    @Test
    void restrictedUserGetsFilteredMaskedRows(@TempDir Path dir) throws Exception {
        write(dir);
        System.setProperty("endpointsPerServer", String.valueOf(FILES));
        try (TestFlightServerHelper helper = TestFlightServerHelper.builder()
                .dataDir(dir.toString()).resultMode("partial").threads(2)
                .authorization(PROVIDER).password(PASSWORD).start();
                Connection duck = DriverManager.getConnection("jdbc:duckdb:")) {
            FlightSqlClient client = helper.sqlClient();
            String people = "(SELECT * FROM read_parquet('" + dir.resolve("s/people/*.parquet") + "'))";
            String masked = "(SELECT id, left(name, 1) || '***' AS name, region,"
                    + " NULL::DECIMAL(10, 2) AS salary, dept FROM " + people + " WHERE region = 'eu')";
            String depts = "(SELECT * FROM read_parquet('" + dir.resolve("s/depts/*.parquet") + "'))";
            Map<String, String> queries = Map.of(
                    "SELECT * FROM s.people", "SELECT * FROM %1$s",
                    "SELECT count(*) FROM s.people", "SELECT count(*) FROM %1$s",
                    "SELECT p.name, d.title FROM s.people p JOIN s.depts d ON p.dept = d.dept",
                    "SELECT p.name, d.title FROM %1$s p JOIN %2$s d ON p.dept = d.dept",
                    "SELECT a.id, b.name FROM \"s\".\"people\" a JOIN s.people b ON a.id = b.id + 1",
                    "SELECT a.id, b.name FROM %1$s a JOIN %1$s b ON a.id = b.id + 1",
                    "SELECT title FROM s.depts WHERE dept IN (SELECT dept FROM s.people WHERE salary > 0)",
                    "SELECT title FROM %2$s WHERE dept IN (SELECT dept FROM %1$s WHERE salary > 0)",
                    "SELECT region, sum(salary) AS s, max(name) AS n FROM s.people GROUP BY region",
                    "SELECT region, sum(salary) AS s, max(name) AS n FROM %1$s GROUP BY region");
            for (String mode : List.of("partial", "complete")) {
                CallOption[] alice = login(helper, "alice", mode);
                CallOption[] bob = login(helper, "bob", mode);
                for (Map.Entry<String, String> query : queries.entrySet()) {
                    String label = mode + ": " + query.getKey();
                    assertEquals(expected(duck, String.format(Locale.ROOT, query.getValue(), masked, depts)),
                            rows(client, query.getKey(), alice, true), "alice " + label);
                    if (mode.equals("complete")) {
                        // In parts, bob's endpoints answer for their own files.
                        assertEquals(expected(duck, String.format(Locale.ROOT, query.getValue(), people, depts)),
                                rows(client, query.getKey(), bob, false), "bob " + label);
                    }
                }
                FlightInfo masks = client.execute("SELECT * FROM s.people", alice);
                assertEquals(client.execute("SELECT * FROM s.people", bob).getSchema(), masks.getSchema(),
                        mode + ": masked columns keep their types");
                assertEquals(1, masks.getEndpoints().size(), mode + ": answered complete");
                FlightSqlClient.PreparedStatement prepared =
                        client.prepare("SELECT * FROM s.people WHERE id < 100", alice);
                try {
                    List<String> got = read(client, prepared.execute(alice), alice);
                    assertEquals(expected(duck, "SELECT * FROM " + masked + " WHERE id < 100"), sorted(got),
                            mode + ": prepared statement");
                } finally {
                    prepared.close(alice);
                }
            }
            assertEquals(List.of("id INTEGER", "name VARCHAR", "region VARCHAR", "salary DECIMAL(10,2)",
                    "dept BIGINT"), SEEN.get("people").stream().map(c -> c.name() + " " + c.duckDbType()).toList(),
                    "every column passed");
            assertEquals(null, SEEN.get("people").get(0).sourceType(), "no catalog type for a directory table");

            // A ticket planned in parts for bob is refused for alice, never read unrestricted.
            FlightInfo bobs = client.execute("SELECT * FROM s.people", login(helper, "bob", "partial"));
            assertEquals(FILES, bobs.getEndpoints().size());
            CallOption[] alice = login(helper, "alice", "partial");
            FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                    () -> read(client, bobs, alice));
            assertEquals(FlightStatusCode.UNAUTHORIZED, e.status().code());
            assertFalse(String.valueOf(e.getMessage()).contains("person"), e.getMessage());
        } finally {
            System.clearProperty("endpointsPerServer");
        }
    }

    /**
     * Writes s.people (in {@link #FILES} files) and s.depts.
     *
     * @param dir data directory
     * @throws Exception on failure
     */
    private static void write(Path dir) throws Exception {
        Files.createDirectories(dir.resolve("s/people"));
        Files.createDirectories(dir.resolve("s/depts"));
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = duck.createStatement()) {
            for (int f = 0; f < FILES; f++) {
                st.execute(String.format(Locale.ROOT, "COPY (SELECT i::INTEGER AS id, 'person ' || i AS name,"
                        + " CASE i %% 3 WHEN 0 THEN 'eu' WHEN 1 THEN 'us' ELSE NULL END AS region,"
                        + " (i * 10.5)::DECIMAL(10, 2) AS salary, i %% 5 AS dept FROM range(%d, %d) t(i))"
                        + " TO '%s' (FORMAT parquet)", f * ROWS / FILES, (f + 1) * ROWS / FILES,
                        dir.resolve("s/people/part-" + f + ".parquet")));
            }
            st.execute("COPY (SELECT i AS dept, 'dept ' || i AS title FROM range(5) t(i)) TO '"
                    + dir.resolve("s/depts/part-0.parquet") + "' (FORMAT parquet)");
        }
    }

    /**
     * Computes the expected rows in DuckDB.
     *
     * @param duck DuckDB connection
     * @param sql query over the files
     * @return sorted rows as text
     * @throws Exception on failure
     */
    private static List<String> expected(Connection duck, String sql) throws Exception {
        List<String> rows = new ArrayList<>();
        try (Statement st = duck.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int c = 1; c <= meta.getColumnCount(); c++) {
                    Object value = rs.getObject(c);
                    row.append(value instanceof Number n && !(value instanceof java.math.BigDecimal)
                            ? Long.toString(n.longValue()) : String.valueOf(value)).append('|');
                }
                rows.add(row.toString());
            }
        }
        return sorted(rows);
    }

    /**
     * Logs a user in.
     *
     * @param helper running server
     * @param user user name
     * @param mode result mode
     * @return bearer token and result-mode header
     */
    private static CallOption[] login(TestFlightServerHelper helper, String user, String mode) {
        CallOption token = helper.flightClient().authenticateBasicToken(user, PASSWORD)
                .orElseThrow(() -> new AssertionError("no token for " + user));
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert("x-flight-result-mode", mode);
        return new CallOption[] {token, new HeaderCallOption(headers)};
    }

    /**
     * Plans and reads a query.
     *
     * @param client Flight SQL client
     * @param sql query
     * @param option caller's options
     * @param oneEndpoint whether the answer must come from one endpoint
     * @return sorted rows as text
     * @throws Exception on failure
     */
    private static List<String> rows(FlightSqlClient client, String sql, CallOption[] option,
            boolean oneEndpoint) throws Exception {
        FlightInfo info = client.execute(sql, option);
        if (oneEndpoint) {
            assertEquals(1, info.getEndpoints().size(), sql);
        }
        return sorted(read(client, info, option));
    }

    /**
     * Reads every endpoint of a plan.
     *
     * @param client Flight SQL client
     * @param info plan
     * @param option caller's options
     * @return rows in arrival order
     * @throws Exception on failure
     */
    private static List<String> read(FlightSqlClient client, FlightInfo info, CallOption[] option)
            throws Exception {
        List<String> rows = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(endpoint.getTicket(), option)) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    for (int r = 0; r < root.getRowCount(); r++) {
                        StringBuilder row = new StringBuilder();
                        for (var vector : root.getFieldVectors()) {
                            Object value = vector.getObject(r);
                            row.append(value instanceof Number n && !(value instanceof java.math.BigDecimal)
                                    ? Long.toString(n.longValue()) : String.valueOf(value)).append('|');
                        }
                        rows.add(row.toString());
                    }
                }
            }
        }
        return rows;
    }

    /**
     * Sorts rows.
     *
     * @param rows rows
     * @return sorted copy
     */
    private static List<String> sorted(List<String> rows) {
        List<String> copy = new ArrayList<>(rows);
        copy.sort(null);
        return copy;
    }
}
