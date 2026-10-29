package pro.surpin.data.arrowflight.server;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
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
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.DuckDbSizing;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Checks per-user isolation with {@code duckDbSharedDatabase}: while one user keeps reading a
 * table on the node's shared DuckDB (its views live in that user's session catalogs), another
 * user without the grant cannot read the table by name, through another session's catalog, in a
 * join, union or CTE, through a file function or through DuckDB's metadata functions, and both
 * users' answers stay correct; the same with a DuckDB per session.
 */
@Tag("integration")
class SharedDatabaseAuthorizationIntegrationTest {

    private static final String PASSWORD = "test-password";
    private static final long ROWS = 40_000;
    private static final int FILES = 2;
    private static final int ROUNDS = 12;
    /** Tables each user may read; every user sees schema s. */
    private static final Map<String, Set<String>> GRANTS = Map.of(
            "alice", Set.of("items", "secret"),
            "bob", Set.of("items"));

    /**
     * Runs both users at once against a shared DuckDB and a DuckDB per session, in both modes.
     *
     * @param dir temporary data directory
     * @throws Exception on failure
     */
    @Test
    void userWithoutGrantCannotReachAnotherUsersTable(@TempDir Path dir) throws Exception {
        write(dir, "items", 0);
        write(dir, "secret", 1_000_000);
        System.setProperty("endpointsPerServer", String.valueOf(FILES));
        try {
            for (String mode : List.of("partial", "complete")) {
                run(dir, mode, new DuckDbSizing.Shared(2, "1GiB"));
                run(dir, mode, null);
            }
        } finally {
            System.clearProperty("endpointsPerServer");
        }
    }

    /**
     * Writes a table of {@link #ROWS} ids starting at an offset.
     *
     * @param dir data directory
     * @param table table name in schema s
     * @param offset first id
     * @throws Exception on failure
     */
    private static void write(Path dir, String table, long offset) throws Exception {
        Path path = dir.resolve("s").resolve(table);
        Files.createDirectories(path);
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = duck.createStatement()) {
            for (int f = 0; f < FILES; f++) {
                st.execute(String.format(Locale.ROOT,
                        "COPY (SELECT i AS id, '%s ' || i AS label FROM range(%d, %d) t(i))"
                                + " TO '%s' (FORMAT parquet)", table,
                        offset + f * ROWS / FILES, offset + (f + 1) * ROWS / FILES,
                        path.resolve("part-" + f + ".parquet")));
            }
        }
    }

    /**
     * Starts a server with the grants and lets alice read while bob probes.
     *
     * @param dir data directory
     * @param mode result mode
     * @param shared sizing of the shared DuckDB, or null for a DuckDB per session
     * @throws Exception on failure
     */
    private static void run(Path dir, String mode, DuckDbSizing.Shared shared) throws Exception {
        String label = mode + (shared == null ? " per session" : " shared");
        FlightAuthorization grants = new FlightAuthorization() {
            @Override
            public boolean canSeeSchema(String principal, String schema) {
                return GRANTS.containsKey(principal) && "s".equals(schema);
            }

            @Override
            public boolean canReadTable(String principal, String schema, String table) {
                return canSeeSchema(principal, schema) && GRANTS.get(principal).contains(table);
            }
        };
        ExecutorService pool = Executors.newFixedThreadPool(2 * FILES + 2);
        try (TestFlightServerHelper helper = TestFlightServerHelper.builder()
                .dataDir(dir.toString()).resultMode(mode).threads(2).sharedDatabase(shared)
                .authorization(grants).password(PASSWORD).start()) {
            FlightSqlClient client = helper.sqlClient();
            CallOption[] alice = login(helper, "alice", mode);
            CallOption[] bob = login(helper, "bob", mode);
            String secretFile = dir.resolve("s").resolve("secret").resolve("part-0.parquet").toString();
            List<String> probes = new ArrayList<>(List.of(
                    "SELECT * FROM s.secret",
                    "SELECT count(*) FROM s.secret",
                    "SELECT a.id, b.label FROM s.items a JOIN s.secret b ON a.id = b.id",
                    "SELECT * FROM s.items UNION ALL SELECT * FROM s.secret",
                    "WITH x AS (SELECT * FROM s.secret) SELECT * FROM x",
                    "SELECT * FROM read_parquet('" + secretFile + "')",
                    "SELECT * FROM '" + secretFile + "'",
                    "SELECT * FROM duckdb_views()",
                    "SELECT * FROM duckdb_databases()"));
            for (int n = 0; n < 2 * FILES + 2; n++) {
                probes.add("SELECT * FROM " + DuckDbAdapter.SESSION_CATALOG_PREFIX + n + ".s.secret");
            }

            AtomicBoolean done = new AtomicBoolean();
            Future<Integer> reader = pool.submit(() -> {
                int reads = 0;
                try {
                    while (!done.get() || reads < ROUNDS) {
                        List<String> rows = rows(client, "SELECT * FROM s.secret", alice, pool);
                        assertEquals(ROWS, rows.size(), label + ": alice's secret rows");
                        assertTrue(rows.stream().allMatch(r -> r.contains("secret ")), label);
                        reads++;
                    }
                } finally {
                    done.set(true);
                }
                return reads;
            });
            for (int round = 0; round < ROUNDS && !done.get(); round++) {
                for (String sql : probes) {
                    try {
                        List<String> rows = rows(client, sql, bob, pool);
                        fail(label + ": bob read " + rows.size() + " rows of: " + sql);
                    } catch (FlightRuntimeException e) {
                        FlightStatusCode code = e.status().code();
                        assertTrue(code == FlightStatusCode.UNAUTHORIZED
                                        || code == FlightStatusCode.INVALID_ARGUMENT,
                                label + ": " + sql + " -> " + code + " " + e.getMessage());
                        assertFalse(String.valueOf(e.getMessage()).contains("secret 1"),
                                label + ": error text carries secret rows: " + e.getMessage());
                    }
                }
                List<String> own = rows(client, "SELECT * FROM s.items", bob, pool);
                assertEquals(ROWS, own.size(), label + ": bob's own rows");
                assertTrue(own.stream().noneMatch(r -> r.contains("secret")), label);
            }
            done.set(true);
            assertTrue(reader.get() >= ROUNDS, label + ": alice's reads");
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Logs a user in and returns the call options for its calls in a result mode.
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
     * Plans a query and reads all its endpoints in parallel.
     *
     * @param client Flight SQL client
     * @param sql query
     * @param option caller's token and mode header
     * @param pool threads for the endpoints
     * @return rows as text
     * @throws Exception on failure
     */
    private static List<String> rows(FlightSqlClient client, String sql, CallOption[] option,
            ExecutorService pool) throws Exception {
        FlightInfo info = client.execute(sql, option);
        List<Future<List<String>>> parts = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            parts.add(pool.submit(() -> read(client, endpoint, option)));
        }
        List<String> rows = new ArrayList<>();
        for (Future<List<String>> part : parts) {
            try {
                rows.addAll(part.get());
            } catch (java.util.concurrent.ExecutionException e) {
                if (e.getCause() instanceof FlightRuntimeException flight) {
                    throw flight;
                }
                throw e;
            }
        }
        return rows;
    }

    /**
     * Reads one endpoint as text rows.
     *
     * @param client Flight SQL client
     * @param endpoint endpoint
     * @param option caller's token and mode header
     * @return rows in arrival order
     * @throws Exception on failure
     */
    private static List<String> read(FlightSqlClient client, FlightEndpoint endpoint, CallOption[] option)
            throws Exception {
        List<String> rows = new ArrayList<>();
        try (FlightStream stream = client.getStream(endpoint.getTicket(), option)) {
            while (stream.next()) {
                VectorSchemaRoot root = stream.getRoot();
                for (int r = 0; r < root.getRowCount(); r++) {
                    StringBuilder row = new StringBuilder();
                    for (var vector : root.getFieldVectors()) {
                        row.append(vector.getObject(r)).append('|');
                    }
                    rows.add(row.toString());
                }
            }
        }
        return rows;
    }
}
