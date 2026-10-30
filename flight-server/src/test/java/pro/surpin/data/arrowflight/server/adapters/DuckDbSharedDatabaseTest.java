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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
            awaitEnded(worker, List.of(worker.submit(Thread::currentThread).get(30, TimeUnit.SECONDS)));
            Connection own = adapter.connection();
            assertTrue(single(own, "SELECT string_agg(database_name, ',') FROM duckdb_databases()")
                    .contains(catalog));
            adapter.closeSessionsOfEndedThreads();
            assertFalse(single(own, "SELECT string_agg(database_name, ',') FROM duckdb_databases()")
                    .contains(catalog));
            assertEquals(1, adapter.openSessions());
        }
    }

    /**
     * Verifies sessions describe in catalogs of their own in one describe DuckDB of the node:
     * concurrent describes over different view versions keep their own column types, and the
     * describe DuckDB reads no files, also inside the data directory.
     */
    @Test
    void sessionsDescribeInOneDescribeDatabase(@TempDir Path dir) throws Exception {
        Path data = Files.createDirectories(dir.resolve("data"));
        try (Connection free = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = free.createStatement()) {
            s.execute("COPY (SELECT 42 AS x) TO '" + data.resolve("t.parquet") + "' (FORMAT parquet)");
        }
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = sharedAdapter(List.of(data.toString()), List.of())) {
            Connection describeRoot = adapter.describeDatabase();
            assertNotNull(describeRoot);
            CyclicBarrier round = new CyclicBarrier(2);
            List<Future<String>> types = new ArrayList<>();
            for (CatalogViews.Snapshot views : List.of(views(1L, 10), textViews(2L))) {
                types.add(workers.submit(() -> {
                    StringBuilder seen = new StringBuilder();
                    for (int i = 0; i < 10; i++) {
                        round.await(30, TimeUnit.SECONDS);
                        // A new text each round, so that every round describes in DuckDB.
                        seen.append(adapter.describeComplete(allocator,
                                "SELECT price FROM s.t WHERE id > " + i, views)
                                .getFields().get(0).getType()).append(';');
                        assertEquals(1, adapter.describeParameters(
                                "SELECT price FROM s.t WHERE id = ? + " + i, views).getFields().size());
                    }
                    round.await(30, TimeUnit.SECONDS);
                    // Both sessions' describe catalogs are attached while both threads live.
                    long attached;
                    try (Statement s = describeRoot.createStatement(); ResultSet rs = s.executeQuery(
                            "SELECT count(*) FROM duckdb_databases() WHERE database_name LIKE '"
                                    + DuckDbAdapter.SESSION_CATALOG_PREFIX + "%'")) {
                        assertTrue(rs.next());
                        attached = rs.getLong(1);
                    }
                    round.await(30, TimeUnit.SECONDS);
                    return seen + "|" + attached;
                }));
            }
            String[] first = types.get(0).get(60, TimeUnit.SECONDS).split("\\|");
            String[] second = types.get(1).get(60, TimeUnit.SECONDS).split("\\|");
            assertEquals("Decimal(6, 1, 128);".repeat(10), first[0]);
            assertEquals("Utf8;".repeat(10), second[0]);
            assertEquals("2", first[1]);
            assertEquals("2", second[1]);
            assertSame(describeRoot, adapter.describeDatabase());

            // The query connection reads the file; the describe DuckDB does not.
            String direct = "SELECT x FROM read_parquet('" + data.resolve("t.parquet") + "')";
            assertEquals("42", single(adapter.connection(), direct));
            assertThrows(java.sql.SQLException.class,
                    () -> adapter.describeComplete(allocator, direct, views(1L, 10)));
            try (Statement s = describeRoot.createStatement()) {
                assertThrows(java.sql.SQLException.class,
                        () -> s.execute("SET enable_external_access = true"));
            }
        } finally {
            workers.shutdownNow();
        }
    }

    /**
     * Verifies that after the describe DuckDB is replaced (as when DuckDB invalidates it) a
     * session describes on the new one with its views set up again, and that an ended thread's
     * describe catalog is detached.
     */
    @Test
    void replacedDescribeDatabaseAndEndedThreads() throws Exception {
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = sharedAdapter(List.of(), List.of())) {
            CatalogViews.Snapshot views = views(1L, 10);
            assertEquals(1, adapter.describeComplete(allocator, "SELECT price FROM s.t", views)
                    .getFields().size());
            Connection root = adapter.describeDatabase();
            adapter.replaceDescribeRoot(root);
            assertNotSame(root, adapter.describeDatabase());
            assertTrue(root.isClosed());
            // A new text, so the description cache does not answer it.
            assertEquals(2, adapter.describeComplete(allocator, "SELECT id, price FROM s.t", views)
                    .getFields().size());
            Connection current = adapter.describeDatabase();
            adapter.replaceDescribeRoot(root);
            assertSame(current, adapter.describeDatabase());

            ExecutorService worker = Executors.newSingleThreadExecutor();
            worker.submit(() -> adapter.describeComplete(allocator, "SELECT id FROM s.t", views))
                    .get(30, TimeUnit.SECONDS);
            awaitEnded(worker, List.of(worker.submit(Thread::currentThread).get(30, TimeUnit.SECONDS)));
            String catalogs = "SELECT count(*) FROM duckdb_databases() WHERE database_name LIKE '"
                    + DuckDbAdapter.SESSION_CATALOG_PREFIX + "%'";
            assertEquals("2", single(current, catalogs));
            adapter.closeSessionsOfEndedThreads();
            assertEquals("1", single(current, catalogs));
        }
    }

    /** Verifies the setting is off by default: no shared DuckDB and session catalogs not refused. */
    @Test
    void offByDefault() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(config(), ioPool)) {
            assertNull(adapter.sharedDatabase());
            assertNull(adapter.describeDatabase());
            // A missing catalog, as before: DuckDB's error, not the shared mode's refusal.
            assertThrows(java.sql.SQLException.class, () -> answer(adapter, allocator,
                    "SELECT * FROM " + DuckDbAdapter.SESSION_CATALOG_PREFIX + "1.s.t", views(1L, 10)));
        } finally {
            ioPool.shutdownNow();
        }
    }

    /**
     * Verifies duckDbStreamingBufferSize reaches sessions of the shared DuckDB through the
     * connections opened before the lockdown: a session gets one while there are idle ones, a
     * further session DuckDB's default, and a connection comes back without the temporary views
     * and the catalog of its last session when that session's thread ends.
     */
    @Test
    void streamingBufferSizeReachesSessionsThroughBufferedConnections(@TempDir Path dir)
            throws Exception {
        Path data = Files.createDirectories(dir.resolve("data"));
        AppConfig config = config().withDuckDbScanOrder(config().duckDbPreserveInsertionOrder(), "8MB");
        try (DuckDbAdapter adapter = new DuckDbAdapter(config, Executors.newSingleThreadExecutor(),
                DuckDbSetupPlan.of(List.of(data.toString()), List.of()), 0, 0, 2,
                ProducingStreamGate.OFF, false, SHARED.withBufferedConnections(2))) {
            BufferedConnections pool = adapter.bufferedConnections();
            assertNotNull(pool);
            assertSame(adapter.sharedDatabase(), pool.root());
            assertEquals(2, pool.idle());

            CyclicBarrier all = new CyclicBarrier(3);
            ExecutorService workers = Executors.newFixedThreadPool(3);
            List<Future<String>> sessions = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                sessions.add(workers.submit(() -> {
                    Connection conn = adapter.connection();
                    String buffer = single(conn, "SELECT current_setting('streaming_buffer_size')");
                    try (Statement s = conn.createStatement()) {
                        assertThrows(java.sql.SQLException.class,
                                () -> s.execute("SET streaming_buffer_size = '1MB'"));
                    }
                    try (Statement s = conn.createStatement()) {
                        s.execute("CREATE OR REPLACE TEMP VIEW left_behind AS SELECT 1 AS x");
                    }
                    all.await(30, TimeUnit.SECONDS);
                    return buffer + "|" + single(conn, "SELECT current_database()") + "|"
                            + Thread.currentThread().getName();
                }));
            }
            List<String> buffers = new ArrayList<>();
            List<String> catalogs = new ArrayList<>();
            List<String> threadNames = new ArrayList<>();
            for (Future<String> session : sessions) {
                String[] parts = session.get(60, TimeUnit.SECONDS).split("\\|");
                buffers.add(parts[0]);
                catalogs.add(parts[1]);
                threadNames.add(parts[2]);
            }
            buffers.sort(null);
            assertEquals(List.of("7.6 MiB", "7.6 MiB", "976.5 KiB"), buffers);
            assertEquals(0, pool.idle());
            assertEquals(1, pool.missed());

            List<Thread> threads = Thread.getAllStackTraces().keySet().stream()
                    .filter(t -> threadNames.contains(t.getName())).toList();
            awaitEnded(workers, threads);
            adapter.closeSessionsOfEndedThreads();
            assertEquals(2, pool.idle());

            ExecutorService next = Executors.newSingleThreadExecutor();
            try {
                String reused = next.submit(() -> {
                    Connection conn = adapter.connection();
                    return single(conn, "SELECT current_setting('streaming_buffer_size')") + "|"
                            + single(conn, "SELECT count(*) FROM duckdb_views() WHERE view_name = 'left_behind'")
                            + "|" + single(conn, "SELECT string_agg(database_name, ',') FROM duckdb_databases()");
                }).get(30, TimeUnit.SECONDS);
                String[] parts = reused.split("\\|");
                assertEquals("7.6 MiB", parts[0]);
                assertEquals("0", parts[1]);
                for (String catalog : catalogs) {
                    assertFalse(List.of(parts[2].split(",")).contains(catalog), parts[2]);
                }
                assertEquals(1, pool.idle());
            } finally {
                next.shutdownNow();
            }
        }
    }

    /**
     * Verifies a replaced shared DuckDB gets buffered connections of its own and the old ones are
     * closed, also the ones lent out when their sessions end.
     */
    @Test
    void replacedDatabaseGetsNewBufferedConnections() throws Exception {
        AppConfig config = config().withDuckDbScanOrder(config().duckDbPreserveInsertionOrder(), "8MB");
        try (DuckDbAdapter adapter = new DuckDbAdapter(config, Executors.newSingleThreadExecutor(),
                DuckDbSetupPlan.of(List.of(), List.of()), 0, 0, 2, ProducingStreamGate.OFF, false,
                SHARED.withBufferedConnections(2))) {
            BufferedConnections before = adapter.bufferedConnections();
            Connection lent = adapter.connection();
            assertEquals(1, before.idle());
            adapter.replaceSharedRoot(adapter.sharedDatabase());
            BufferedConnections after = adapter.bufferedConnections();
            assertNotSame(before, after);
            assertSame(adapter.sharedDatabase(), after.root());
            assertEquals(0, before.idle());
            assertEquals(2, after.idle());
            Connection fresh = adapter.connection();
            assertNotSame(lent, fresh);
            assertTrue(lent.isClosed());
            assertEquals("7.6 MiB", single(fresh, "SELECT current_setting('streaming_buffer_size')"));
            assertEquals(1, after.idle());
        }
    }

    /**
     * Verifies no buffered connections are opened without duckDbStreamingBufferSize or with
     * duckDbSharedBufferedConnections 0: sessions keep DuckDB's default.
     */
    @Test
    void noBufferedConnectionsWithoutTheBufferSize() throws Exception {
        try (DuckDbAdapter adapter = sharedAdapter(List.of(), List.of())) {
            assertNull(adapter.bufferedConnections());
            assertEquals("976.5 KiB",
                    single(adapter.connection(), "SELECT current_setting('streaming_buffer_size')"));
        }
        AppConfig config = config().withDuckDbScanOrder(config().duckDbPreserveInsertionOrder(), "8MB");
        try (DuckDbAdapter adapter = new DuckDbAdapter(config, Executors.newSingleThreadExecutor(),
                DuckDbSetupPlan.of(List.of(), List.of()), 0, 0, 2, ProducingStreamGate.OFF, false,
                SHARED.withBufferedConnections(0))) {
            assertNull(adapter.bufferedConnections());
            assertEquals("976.5 KiB",
                    single(adapter.connection(), "SELECT current_setting('streaming_buffer_size')"));
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
     * Shuts a pool down and waits until its threads have ended: awaitTermination may return
     * while a worker thread is still alive, and the session reaper only closes sessions of
     * threads that are no longer alive.
     *
     * @param pool    pool to shut down
     * @param threads the pool's threads
     * @throws InterruptedException when interrupted while waiting
     */
    private static void awaitEnded(ExecutorService pool, List<Thread> threads) throws InterruptedException {
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        for (Thread thread : threads) {
            thread.join(30_000);
            assertFalse(thread.isAlive(), thread.getName());
        }
    }

    /**
     * Verifies a query that runs out of memory in the shared DuckDB before sending anything runs
     * again on a private DuckDB of the per-connection size and answers once (item 23b: 32 S3
     * queries in a 1 GiB shared DuckDB failed where DuckDB-per-connection answered), and that a
     * query too large for the private DuckDB too fails with DuckDB's error.
     */
    @Test
    void outOfMemoryInTheSharedDatabaseRunsAgainOnAPrivateOne() throws Exception {
        // list() cannot spill: 4M BIGINTs need about 32 MiB.
        String sql = "SELECT len(list(x)) FROM range(4000000) t(x)";
        DuckDbSizing.Shared tight = new DuckDbSizing.Shared(1, "8MiB", 0, 1, "256MiB");
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(config(), Executors.newSingleThreadExecutor(),
                        DuckDbSetupPlan.of(List.of(), List.of()), 0, 0, 2, ProducingStreamGate.OFF, false, tight)) {
            FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);
            assertEquals("4000000", answer(adapter, allocator, sql, views(1L, 10), listener));
            verify(listener, times(1)).start(any(VectorSchemaRoot.class));
            assertEquals(1, adapter.outOfMemoryRetries());
            // The thread keeps its private DuckDB for the next refusal, beside its shared session.
            assertEquals(2, adapter.openSessions());
            assertEquals("4000000", answer(adapter, allocator, sql, views(1L, 10)));
            assertEquals(2, adapter.outOfMemoryRetries());
            assertEquals(2, adapter.openSessions());
            assertEquals("30.0", answer(adapter, allocator, "SELECT max(price) FROM t", views(1L, 10)));
            assertEquals(2, adapter.outOfMemoryRetries());

            java.sql.SQLException e = assertThrows(java.sql.SQLException.class, () -> answer(adapter, allocator,
                    "SELECT len(list(x)) FROM range(100000000) t(x)", views(1L, 10)));
            assertTrue(DuckDbAdapter.outOfMemory(e), e.getMessage());
            assertEquals(3, adapter.outOfMemoryRetries());
        }
    }

    /**
     * Verifies the sessions of a shared DuckDB keep their prepared statements within one node-wide
     * budget (an eighth of the memory limit at 1 MiB each): 32 sessions of 64 statements each
     * filled a 2 GiB limit with S3 point lookups (item 23b).
     */
    @Test
    void sessionsKeepTheirStatementsWithinTheNodeBudget() throws Exception {
        DuckDbSizing.Shared sized = new DuckDbSizing.Shared(1, "16MiB");
        int sessions = 4;
        ExecutorService pool = Executors.newFixedThreadPool(sessions);
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(config(), Executors.newSingleThreadExecutor(),
                        DuckDbSetupPlan.of(List.of(), List.of()), 0, 0, 2, ProducingStreamGate.OFF, false, sized)) {
            List<Future<?>> runs = new ArrayList<>();
            for (int s = 0; s < sessions; s++) {
                runs.add(pool.submit(() -> {
                    for (int k = 0; k < 10; k++) {
                        assertEquals(Integer.toString(k), answer(adapter, allocator,
                                "SELECT " + k + " AS v", views(1L, 10)));
                    }
                    return null;
                }));
            }
            for (Future<?> run : runs) {
                run.get();
            }
            // Budget 2 (16 MiB / 8 / 1 MiB); each session may keep one beyond it.
            assertTrue(adapter.sharedStatements() <= 2 + sessions,
                    "cached statements: " + adapter.sharedStatements());
            assertTrue(adapter.sharedStatements() >= 1);
        } finally {
            pool.shutdownNow();
        }
    }

    /** Without the shared DuckDB a query that runs out of memory fails as before. */
    @Test
    void outOfMemoryWithoutTheSharedDatabaseIsNotRunAgain() throws Exception {
        AppConfig tight = new AppConfig(
                3, 100, 1, 131072, 0, 1, 1,
                null, false, null, null,
                null, null, "8MiB", false, 1048576, 67108864, 60000L, null, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 64, null, null,
                "complete", true, true);
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(tight, Executors.newSingleThreadExecutor(),
                        DuckDbSetupPlan.of(List.of(), List.of()), 0, 0, 2, ProducingStreamGate.OFF, false, null)) {
            java.sql.SQLException e = assertThrows(java.sql.SQLException.class, () -> answer(adapter, allocator,
                    "SELECT len(list(x)) FROM range(4000000) t(x)", views(1L, 10)));
            assertTrue(DuckDbAdapter.outOfMemory(e), e.getMessage());
            assertEquals(0, adapter.outOfMemoryRetries());
        }
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
     * Builds view definitions s.t over ids and prices as text.
     *
     * @param version views version
     * @return view definitions
     */
    private static CatalogViews.Snapshot textViews(long version) {
        return new CatalogViews.Snapshot(version,
                List.of("CREATE SCHEMA IF NOT EXISTS s",
                        "CREATE OR REPLACE VIEW s.t AS SELECT * FROM (VALUES (1, 'a')) v(id, price)"),
                List.of("CREATE SCHEMA IF NOT EXISTS s",
                        "CREATE OR REPLACE VIEW s.t AS SELECT CAST(NULL AS INTEGER) AS id,"
                                + " CAST(NULL AS VARCHAR) AS price WHERE false"),
                List.of("DROP SCHEMA IF EXISTS s CASCADE"));
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
        return answer(adapter, allocator, sql, views, mock(FlightProducer.ServerStreamListener.class));
    }

    /**
     * Runs a complete-answer query on a listener and returns the first value.
     *
     * @param adapter   adapter
     * @param allocator Arrow allocator
     * @param sql       client SQL
     * @param views     view definitions
     * @param listener  mocked Flight listener
     * @return first value of the answer
     * @throws Exception on refusal or query failure
     */
    private static String answer(DuckDbAdapter adapter, RootAllocator allocator, String sql,
            CatalogViews.Snapshot views, FlightProducer.ServerStreamListener listener) throws Exception {
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
