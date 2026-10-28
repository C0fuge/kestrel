package pro.surpin.data.arrowflight.server.adapters;

import pro.surpin.data.arrowflight.server.duckdb.DuckDbSetupPlan;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.services.CatalogViews;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests of duckDbSharedDatabase: every session is a connection to one DuckDB of the node. */
@Tag("unit")
class DuckDbSharedDatabaseTest {

    /** Node-wide sizing of the tests. */
    private static final DuckDbSizing.Shared SHARED = new DuckDbSizing.Shared(3, "512MiB");

    /**
     * Verifies sessions of different threads connect to one DuckDB that was set up and locked
     * down once: node-wide threads, memory limit and setup settings, a catalog of their own, no
     * file outside the data directory and no configuration change.
     */
    @Test
    void sessionsShareOneLockedDownDatabase(@TempDir Path dir) throws Exception {
        Path data = Files.createDirectories(dir.resolve("data"));
        Path outside = Files.createDirectories(dir.resolve("outside"));
        try (Connection free = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = free.createStatement()) {
            s.execute("COPY (SELECT 42 AS x) TO '" + data.resolve("t.parquet") + "' (FORMAT parquet)");
            s.execute("COPY (SELECT 7 AS x) TO '" + outside.resolve("t.parquet") + "' (FORMAT parquet)");
        }
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try (DuckDbAdapter adapter = sharedAdapter(List.of(data.toString()),
                List.of("SET binary_as_string = true"))) {
            CyclicBarrier both = new CyclicBarrier(2);
            List<Future<String>> catalogs = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                catalogs.add(workers.submit(() -> {
                    Connection conn = adapter.connection();
                    assertEquals("3", single(conn, "SELECT current_setting('threads')"));
                    assertEquals("512.0 MiB", single(conn, "SELECT current_setting('memory_limit')"));
                    assertEquals("true", single(conn, "SELECT current_setting('binary_as_string')"));
                    assertEquals("42", single(conn, "SELECT x FROM read_parquet('"
                            + data.resolve("t.parquet") + "')"));
                    assertThrows(java.sql.SQLException.class, () -> single(conn,
                            "SELECT x FROM read_parquet('" + outside.resolve("t.parquet") + "')"));
                    assertThrows(java.sql.SQLException.class,
                            () -> single(conn, "SELECT * FROM read_text('/etc/hostname')"));
                    try (Statement s = conn.createStatement()) {
                        assertThrows(java.sql.SQLException.class,
                                () -> s.execute("SET enable_external_access = true"));
                        assertThrows(java.sql.SQLException.class, () -> s.execute("SET threads = 1"));
                    }
                    String catalog = single(conn, "SELECT current_database()");
                    both.await(30, TimeUnit.SECONDS);
                    return catalog + "|" + single(conn,
                            "SELECT string_agg(database_name, ',' ORDER BY database_name) FROM duckdb_databases()");
                }));
            }
            String[] a = catalogs.get(0).get(60, TimeUnit.SECONDS).split("\\|");
            String[] b = catalogs.get(1).get(60, TimeUnit.SECONDS).split("\\|");
            assertTrue(a[0].startsWith(DuckDbAdapter.SESSION_CATALOG_PREFIX), a[0]);
            assertNotEquals(a[0], b[0]);
            // One DuckDB: each session sees the other's catalog.
            assertTrue(List.of(a[1].split(",")).contains(b[0]), a[1]);
            assertEquals(2, adapter.openSessions());
            assertNotNull(adapter.sharedDatabase());
        } finally {
            workers.shutdownNow();
        }
    }

    /**
     * Verifies concurrent sessions keep their own catalog views, also at different view versions
     * with the same names, and that a query may not name another session's catalog.
     */
    @Test
    void catalogViewsAreIsolatedBetweenConcurrentSessions() throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = sharedAdapter(List.of(), List.of())) {
            CyclicBarrier round = new CyclicBarrier(2);
            List<Future<String>> results = new ArrayList<>();
            for (CatalogViews.Snapshot views : List.of(views(1L, 10), views(2L, 100))) {
                results.add(workers.submit(() -> {
                    StringBuilder answers = new StringBuilder();
                    for (int i = 0; i < 20; i++) {
                        round.await(30, TimeUnit.SECONDS);
                        answers.append(answer(adapter, allocator, "SELECT sum(price) FROM s.t", views))
                                .append(answer(adapter, allocator, "SELECT max(price) FROM t", views));
                    }
                    return answers + "|" + single(adapter.connection(), "SELECT current_database()");
                }));
            }
            String[] first = results.get(0).get(60, TimeUnit.SECONDS).split("\\|");
            String[] second = results.get(1).get(60, TimeUnit.SECONDS).split("\\|");
            assertEquals("60.030.0".repeat(20), first[0]);
            assertEquals("600.0300.0".repeat(20), second[0]);

            for (String sql : List.of("SELECT * FROM " + second[1] + ".s.t",
                    "SELECT * FROM \"" + second[1].toUpperCase(java.util.Locale.ROOT) + "\".main.t",
                    "SELECT * FROM " + second[1] + ".t",
                    "SELECT * FROM s.t JOIN " + first[1] + ".s.t u USING (id)")) {
                IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                        () -> answer(adapter, allocator, sql, views(1L, 10)), sql);
                assertTrue(e.getMessage().contains("is not allowed"), e.getMessage());
                assertThrows(IllegalArgumentException.class,
                        () -> adapter.describeComplete(allocator, sql, views(1L, 10)), sql);
            }
        } finally {
            workers.shutdownNow();
        }
    }

    /**
     * Verifies that after the shared DuckDB is replaced (as when DuckDB invalidates it) a thread
     * gets a session on the new one, with its views set up again.
     */
    @Test
    void replacedDatabaseGivesSessionsANewConnection() throws Exception {
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = sharedAdapter(List.of(), List.of())) {
            CatalogViews.Snapshot views = views(1L, 10);
            assertEquals("60.0", answer(adapter, allocator, "SELECT sum(price) FROM s.t", views));
            Connection before = adapter.connection();
            Connection root = adapter.sharedDatabase();
            adapter.replaceSharedRoot(root);
            assertNotSame(root, adapter.sharedDatabase());
            assertTrue(root.isClosed());
            assertEquals("60.0", answer(adapter, allocator, "SELECT sum(price) FROM s.t", views));
            assertNotSame(before, adapter.connection());
            assertTrue(before.isClosed());
            assertEquals(1, adapter.openSessions());
            // A second report for the old one changes nothing.
            Connection current = adapter.sharedDatabase();
            adapter.replaceSharedRoot(root);
            assertSame(current, adapter.sharedDatabase());
        }
    }

    /** Verifies the session of an ended thread is closed and its catalog detached. */
    @Test
    void endedSessionDetachesItsCatalog() throws Exception {
        try (DuckDbAdapter adapter = sharedAdapter(List.of(), List.of())) {
            ExecutorService worker = Executors.newSingleThreadExecutor();
            String catalog = worker.submit(() -> single(adapter.connection(), "SELECT current_database()"))
                    .get(30, TimeUnit.SECONDS);
            worker.shutdown();
            assertTrue(worker.awaitTermination(30, TimeUnit.SECONDS));
            Connection own = adapter.connection();
            assertTrue(single(own, "SELECT string_agg(database_name, ',') FROM duckdb_databases()")
                    .contains(catalog));
            adapter.closeSessionsOfEndedThreads();
            assertFalse(single(own, "SELECT string_agg(database_name, ',') FROM duckdb_databases()")
                    .contains(catalog));
            assertEquals(1, adapter.openSessions());
        }
    }

    /** Verifies the setting is off by default: no shared DuckDB and session catalogs not refused. */
    @Test
    void offByDefault() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(config(), ioPool)) {
            assertNull(adapter.sharedDatabase());
            // A missing catalog, as before: DuckDB's error, not the shared mode's refusal.
            assertThrows(java.sql.SQLException.class, () -> answer(adapter, allocator,
                    "SELECT * FROM " + DuckDbAdapter.SESSION_CATALOG_PREFIX + "1.s.t", views(1L, 10)));
        } finally {
            ioPool.shutdownNow();
        }
    }

    /** Verifies the parse check keeps session catalog and schema qualifiers among the names. */
    @Test
    void parseCheckKeepsSessionQualifiers() {
        assertEquals(List.of("t", "flight_session_7"), DuckDbAdapter.requireAllowedFunctions(
                "{\"schema_name\":\"s\",\"table_name\":\"t\",\"catalog_name\":\"Flight_Session_7\"}"));
        assertEquals(List.of("t", "flight_session_2"), DuckDbAdapter.requireAllowedFunctions(
                "{\"schema_name\":\"flight_session_2\",\"table_name\":\"t\",\"catalog_name\":\"\"}"));
    }

    /**
     * Builds an adapter on a shared DuckDB.
     *
     * @param roots      readable roots; empty disables the lockdown
     * @param statements storage setup statements
     * @return adapter
     */
    private static DuckDbAdapter sharedAdapter(List<String> roots, List<String> statements) {
        return new DuckDbAdapter(config(), Executors.newSingleThreadExecutor(),
                DuckDbSetupPlan.of(roots, statements), 0, 0, 2, ProducingStreamGate.OFF, false, SHARED);
    }

    /**
     * Builds a complete-mode configuration.
     *
     * @return configuration
     */
    private static AppConfig config() {
        return new AppConfig(
                3, 100, 1, 131072, 0, 1, 1,
                null, false, null, null,
                null, null, null, false, 1048576, 67108864, 60000L, null, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 64, null, null,
                "complete", true, true);
    }

    /**
     * Builds view definitions s.t and main.t over ids 1, 2, 3 with prices 1, 2, 3 times a factor.
     *
     * @param version views version
     * @param factor  price of id 1
     * @return view definitions
     */
    private static CatalogViews.Snapshot views(long version, int factor) {
        String values = "(VALUES (1, " + factor + ".0), (2, " + 2 * factor + ".0), (3, " + 3 * factor
                + ".0)) v(id, price)";
        return new CatalogViews.Snapshot(version,
                List.of("CREATE SCHEMA IF NOT EXISTS s",
                        "CREATE OR REPLACE VIEW s.t AS SELECT * FROM " + values,
                        "CREATE OR REPLACE VIEW main.t AS SELECT * FROM s.t"),
                List.of("CREATE SCHEMA IF NOT EXISTS s",
                        "CREATE OR REPLACE VIEW s.t AS SELECT CAST(NULL AS INTEGER) AS id,"
                                + " CAST(NULL AS DECIMAL(6,1)) AS price WHERE false",
                        "CREATE OR REPLACE VIEW main.t AS SELECT * FROM s.t"),
                List.of("DROP VIEW IF EXISTS main.t", "DROP SCHEMA IF EXISTS s CASCADE"));
    }

    /**
     * Runs a complete-answer query and returns the first value.
     *
     * @param adapter   adapter
     * @param allocator Arrow allocator
     * @param sql       client SQL
     * @param views     view definitions
     * @return first value of the answer
     * @throws Exception on refusal or query failure
     */
    private static String answer(DuckDbAdapter adapter, RootAllocator allocator, String sql,
            CatalogViews.Snapshot views) throws Exception {
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);
        AtomicReference<VectorSchemaRoot> rootRef = new AtomicReference<>();
        AtomicReference<String> value = new AtomicReference<>();
        doAnswer(invocation -> {
            rootRef.set(invocation.getArgument(0));
            return null;
        }).when(listener).start(any(VectorSchemaRoot.class));
        when(listener.isReady()).thenReturn(true);
        doAnswer(invocation -> {
            if (value.get() == null) {
                value.set(String.valueOf(rootRef.get().getVector(0).getObject(0)));
            }
            return null;
        }).when(listener).putNext();
        adapter.streamComplete(allocator, sql, views, listener, true);
        return value.get();
    }

    /**
     * Runs a query on a connection and returns the first value.
     *
     * @param conn connection
     * @param sql  query
     * @return first value as text
     * @throws java.sql.SQLException on query failure
     */
    private static String single(Connection conn, String sql) throws java.sql.SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getString(1);
        }
    }
}
