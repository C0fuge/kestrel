package pro.surpin.data.arrowflight.server.adapters;

import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.services.ParquetQueryParser;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Tests DuckDB SQL helpers, connection setup, and Flight backpressure handling. */
@Tag("unit")
class DuckDbAdapterTest {

    /** Verifies least recently used statements close when the per-thread cache fills. */
    @Test
    void statementCacheEvictsAndClosesLeastRecentlyUsedStatement() throws Exception {
        DuckDbAdapter.StatementCache cache = new DuckDbAdapter.StatementCache(2);
        PreparedStatement first = mock(PreparedStatement.class);
        PreparedStatement second = mock(PreparedStatement.class);
        PreparedStatement third = mock(PreparedStatement.class);

        cache.put("first", first);
        cache.put("second", second);
        cache.get("first");
        cache.put("third", third);

        assertEquals(2, cache.size());
        assertFalse(cache.containsKey("second"));
        verify(second).close();
        verify(first, never()).close();
        verify(third, never()).close();
    }

    @Test
    void sqlStringLiteralNormal() {
        assertEquals("'hello'", DuckDbAdapter.sqlStringLiteral("hello"));
    }

    @Test
    void sqlStringLiteralEscapesSingleQuote() {
        assertEquals("'it''s'", DuckDbAdapter.sqlStringLiteral("it's"));
    }

    @Test
    void sqlStringLiteralEmpty() {
        assertEquals("''", DuckDbAdapter.sqlStringLiteral(""));
    }

    @Test
    void quoteIdentifierNormal() {
        assertEquals("\"table_name\"", DuckDbAdapter.quoteIdentifier("table_name"));
    }

    @Test
    void quoteIdentifierEscapesDoubleQuote() {
        assertEquals("\"col\"\"name\"", DuckDbAdapter.quoteIdentifier("col\"name"));
    }

    @Test
    void readParquetFromClauseSingleFile() {
        String result = DuckDbAdapter.readParquetFromClause(List.of("/data/file.parquet"));
        assertEquals("read_parquet(['/data/file.parquet'])", result);
    }

    @Test
    void readParquetFromClauseMultipleFiles() {
        String result = DuckDbAdapter.readParquetFromClause(
                List.of("/data/a.parquet", "/data/b.parquet"));
        assertEquals("read_parquet(['/data/a.parquet', '/data/b.parquet'])", result);
    }

    @Test
    void readParquetFromClauseEmpty() {
        String result = DuckDbAdapter.readParquetFromClause(List.of());
        assertEquals("read_parquet([])", result);
    }

    /** Verifies readiness changes cannot be lost before handler registration. */
    @Test
    void listenerReadinessChangeDoesNotWaitForTimeout() throws Exception {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        AtomicBoolean ready = new AtomicBoolean();
        when(listener.isReady()).thenAnswer(invocation -> ready.get());
        doAnswer(invocation -> {
            ready.set(true);
            return null;
        }).when(listener).setOnReadyHandler(any(Runnable.class));

        assertTimeoutPreemptively(Duration.ofSeconds(1),
                () -> assertDoesNotThrow(
                        () -> DuckDbAdapter.awaitListenerReady(listener, 200)));
        verify(listener).setOnReadyHandler(any(Runnable.class));
    }

    /** Verifies polling observes readiness when Arrow never invokes its callback. */
    @Test
    void listenerReadinessWithoutCallbackIsPolled() throws Exception {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        AtomicBoolean ready = new AtomicBoolean();
        CountDownLatch handlerRegistered = new CountDownLatch(1);
        when(listener.isReady()).thenAnswer(invocation -> ready.get());
        doAnswer(invocation -> {
            handlerRegistered.countDown();
            return null;
        }).when(listener).setOnReadyHandler(any(Runnable.class));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Void> result = executor.submit(() -> {
                DuckDbAdapter.awaitListenerReady(listener, 500);
                return null;
            });
            assertTrue(handlerRegistered.await(1, TimeUnit.SECONDS));
            ready.set(true);
            assertNull(result.get(1, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    /** Verifies a readiness signal received before waiting remains observable. */
    @Test
    void listenerReadinessSignalBeforeWaitIsRetained() {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        AtomicBoolean ready = new AtomicBoolean();
        AtomicInteger readinessChecks = new AtomicInteger();
        when(listener.isReady()).thenAnswer(invocation ->
                readinessChecks.getAndIncrement() > 0 && ready.get());
        doAnswer(invocation -> {
            ready.set(true);
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(listener).setOnReadyHandler(any(Runnable.class));

        assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
                assertDoesNotThrow(
                        () -> DuckDbAdapter.awaitListenerReady(listener, 200)));
    }

    /** Verifies a spurious wake-up cannot release a non-ready listener. */
    @Test
    void listenerSpuriousWakeupKeepsWaiting() throws Exception {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        AtomicBoolean ready = new AtomicBoolean();
        AtomicReference<Runnable> readyHandler = new AtomicReference<>();
        CountDownLatch handlerRegistered = new CountDownLatch(1);
        when(listener.isReady()).thenAnswer(invocation -> ready.get());
        doAnswer(invocation -> {
            readyHandler.set(invocation.getArgument(0));
            handlerRegistered.countDown();
            return null;
        }).when(listener).setOnReadyHandler(any(Runnable.class));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Void> result = executor.submit(() -> {
                DuckDbAdapter.awaitListenerReady(listener, 500);
                return null;
            });
            assertTrue(handlerRegistered.await(1, TimeUnit.SECONDS));
            readyHandler.get().run();
            assertThrows(TimeoutException.class,
                    () -> result.get(50, TimeUnit.MILLISECONDS));

            ready.set(true);
            readyHandler.get().run();
            assertNull(result.get(1, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    /** Verifies cancellation releases a signalled waiter without sending data. */
    @Test
    void listenerCancellationStopsWaiting() throws Exception {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicReference<Runnable> cancelHandler = new AtomicReference<>();
        CountDownLatch cancelHandlerRegistered = new CountDownLatch(1);
        when(listener.isCancelled()).thenAnswer(invocation -> cancelled.get());
        doAnswer(invocation -> {
            cancelHandler.set(invocation.getArgument(0));
            cancelHandlerRegistered.countDown();
            return null;
        }).when(listener).setOnCancelHandler(any(Runnable.class));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Void> result = executor.submit(() -> {
                DuckDbAdapter.awaitListenerReady(listener, 500);
                return null;
            });
            assertTrue(cancelHandlerRegistered.await(1, TimeUnit.SECONDS));
            cancelled.set(true);
            cancelHandler.get().run();
            java.util.concurrent.ExecutionException exception = assertThrows(
                    java.util.concurrent.ExecutionException.class,
                    () -> result.get(1, TimeUnit.SECONDS));
            FlightRuntimeException cause = assertInstanceOf(
                    FlightRuntimeException.class, exception.getCause());
            assertEquals(FlightStatusCode.CANCELLED, cause.status().code());
        } finally {
            executor.shutdownNow();
        }
    }

    /** Verifies an absent readiness signal is bounded by configured timeout. */
    @Test
    void listenerWithoutSignalTimesOut() {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);

        FlightRuntimeException exception = assertTimeoutPreemptively(
                Duration.ofSeconds(1), () -> assertThrows(FlightRuntimeException.class,
                        () -> DuckDbAdapter.awaitListenerReady(listener, 25)));
        assertEquals(FlightStatusCode.TIMED_OUT, exception.status().code());
    }

    /** Verifies non-positive readiness timeouts fail before waiting. */
    @Test
    void listenerRejectsNonPositiveTimeout() {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);

        assertThrows(IllegalArgumentException.class,
                () -> DuckDbAdapter.awaitListenerReady(listener, 0));
    }

    @Test
    void ignoresHdfsOptionsWhenExtensionIsNotConfigured() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        AppConfig config = new AppConfig(
                3, 4096, 1, 131072, 1, 1, 1,
                null, false, null, null,
                "true", "/var/lib/hadoop-hdfs/socket/dn_socket", null,
                false, 2_147_483_648L, 1048576, 67108864, 60000L, "/data/parquet", null,
                32010, 5701, 60,
                3, 1000, 30000, true, false, 64, null, null, "complete", true, true);

        try {
            DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool);
            try (Connection connection = adapter.connection()) {
                assertFalse(connection.isClosed());
            }
        } finally {
            ioPool.shutdownNow();
        }
    }

    /** Verifies DuckDB results are streamed through the Flight listener. */
    @Test
    void streamSqlDeliversDuckDbRowsToFlight() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        AppConfig config = new AppConfig(
                3, 2, 1, 131072, 1, 1, 1,
                null, false, null, null,
                null, null, null, false, 2_147_483_648L, 1048576, 67108864, 60000L, null, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 64, null, null, "complete", true, true);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        AtomicReference<VectorSchemaRoot> rootRef = new AtomicReference<>();
        AtomicInteger rows = new AtomicInteger();
        doAnswer(invocation -> {
            rootRef.set(invocation.getArgument(0));
            return null;
        }).when(listener).start(any(VectorSchemaRoot.class));
        when(listener.isReady()).thenReturn(true);
        doAnswer(invocation -> {
            rows.addAndGet(rootRef.get().getRowCount());
            return null;
        }).when(listener).putNext();

        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool)) {
            adapter.streamSql(allocator,
                    "SELECT range AS id FROM range(0, 5)", listener, true);
        } finally {
            ioPool.shutdownNow();
        }

        assertEquals(5, rows.get());
        verify(listener).start(any(VectorSchemaRoot.class));
    }

    /** Verifies streaming reuses DuckDB batches without scheduling background buffering. */
    @Test
    void streamSqlDoesNotScheduleBatchBuffering() throws Exception {
        ExecutorService ioPool = mock(ExecutorService.class);
        AppConfig config = new AppConfig(
                3, 2, 1, 131072, 0, 1, 1,
                null, false, null, null,
                null, null, null, false, 2_147_483_648L, 1048576, 67108864, 60000L, null, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 64, null, null, "complete", true, true);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        when(listener.isReady()).thenReturn(true);

        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool)) {
            adapter.streamSql(allocator,
                    "SELECT range AS id FROM range(0, 5)", listener, true);
        }

        verify(listener).start(any(VectorSchemaRoot.class));
        verify(listener, org.mockito.Mockito.times(3)).putNext();
        verifyNoInteractions(ioPool);
    }

    /** Verifies the connection of an ended thread is closed instead of being kept forever. */
    @Test
    void closesConnectionOfEndedThread() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        AppConfig config = new AppConfig(
                3, 2, 1, 131072, 0, 1, 1,
                null, false, null, null,
                null, null, null, false, 1048576, 67108864, 60000L, null, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 64, null, null, "complete", true, true);
        AtomicReference<Connection> used = new AtomicReference<>();

        try (DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool)) {
            Thread worker = new Thread(() -> used.set(adapter.connection()));
            worker.start();
            worker.join();
            Connection own = adapter.connection();
            assertEquals(2, adapter.openSessions());

            adapter.closeSessionsOfEndedThreads();

            assertEquals(1, adapter.openSessions());
            assertTrue(used.get().isClosed());
            assertFalse(own.isClosed());
        } finally {
            ioPool.shutdownNow();
        }
    }

    /** Verifies connections spill to directories of their own, removed with the adapter. */
    @Test
    void connectionsUseSeparateTempDirectories(@org.junit.jupiter.api.io.TempDir java.nio.file.Path base)
            throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        AppConfig config = new AppConfig(
                3, 2, 1, 131072, 0, 1, 1,
                null, false, null, null,
                null, null, null, false, 1048576, 67108864, 60000L, null, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 64, base.toString(), null, "complete", true, true);
        AtomicReference<String> other = new AtomicReference<>();
        String own;

        try (DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool)) {
            Thread worker = new Thread(() -> other.set(tempDirectory(adapter.connection())));
            worker.start();
            worker.join();
            own = tempDirectory(adapter.connection());

            assertNotEquals(own, other.get());
            assertTrue(java.nio.file.Path.of(own).startsWith(base));
            assertTrue(java.nio.file.Path.of(other.get()).startsWith(base));
        } finally {
            ioPool.shutdownNow();
        }

        try (java.util.stream.Stream<java.nio.file.Path> left = java.nio.file.Files.list(base)) {
            assertEquals(0, left.count());
        }
    }

    /**
     * Reads the spill directory of a DuckDB connection.
     *
     * @param connection DuckDB connection
     * @return value of temp_directory
     */
    private static String tempDirectory(Connection connection) {
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT current_setting('temp_directory')")) {
            rs.next();
            return rs.getString(1);
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Verifies statements evicted from a full cache are prepared again on reuse. */
    @Test
    void streamSqlWorksBeyondStatementCacheSize() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        AppConfig config = new AppConfig(
                3, 100, 1, 131072, 0, 1, 1,
                null, false, null, null,
                null, null, null, false, 1048576, 67108864, 60000L, null, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 2, null, null, "complete", true, true);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        AtomicReference<VectorSchemaRoot> rootRef = new AtomicReference<>();
        AtomicInteger rows = new AtomicInteger();
        doAnswer(invocation -> {
            rootRef.set(invocation.getArgument(0));
            return null;
        }).when(listener).start(any(VectorSchemaRoot.class));
        when(listener.isReady()).thenReturn(true);
        doAnswer(invocation -> {
            rows.addAndGet(rootRef.get().getRowCount());
            return null;
        }).when(listener).putNext();

        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool)) {
            for (int pass = 0; pass < 2; pass++) {
                for (int n = 1; n <= 4; n++) {
                    adapter.streamSql(allocator,
                            "SELECT range AS id FROM range(0, " + n + ")", listener, true);
                }
            }
        } finally {
            ioPool.shutdownNow();
        }

        assertEquals(2 * (1 + 2 + 3 + 4), rows.get());
    }

    /**
     * Verifies the statement cache (upstream's LRU inside our per-thread session) prepares a
     * statement again after DuckDB closed it: a run-time error closes the cached statement, and
     * reusing it would fail every later run of that SQL with "Statement was closed".
     */
    @Test
    void streamSqlFailingAtRunTimeCanBeRunAgain() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        AppConfig config = new AppConfig(
                3, 100, 1, 131072, 0, 1, 1,
                null, false, null, null,
                null, null, null, false, 1048576, 67108864, 60000L, null, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 2, null, null, "partial", true, true);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        when(listener.isReady()).thenReturn(true);
        String failing = "SELECT CAST('x' || range AS INTEGER) AS v FROM range(0, 3)";
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool)) {
            for (int run = 0; run < 3; run++) {
                Exception e = assertThrows(Exception.class,
                        () -> adapter.streamSql(allocator, failing, listener, true));
                assertTrue(String.valueOf(e.getMessage()).contains("Conversion Error"),
                        "run " + run + ": " + e.getMessage());
            }
            adapter.streamSql(allocator, "SELECT range AS id FROM range(0, 2)", listener, true);
        } finally {
            ioPool.shutdownNow();
        }
    }

    /** Verifies stream completion is signalled immediately after the final batch. */
    @Test
    void streamSqlDoesNotAddPollingDelayAfterFinalBatch() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        AppConfig config = new AppConfig(
                3, 2, 1, 131072, 1, 1, 1,
                null, false, null, null,
                null, null, null, false, 2_147_483_648L, 1048576, 67108864, 60000L, null, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 64, null, null, "complete", true, true);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        CountDownLatch finalBatchSent = new CountDownLatch(3);
        when(listener.isReady()).thenReturn(true);
        doAnswer(invocation -> {
            finalBatchSent.countDown();
            return null;
        }).when(listener).putNext();

        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool)) {
            Future<Void> result = caller.submit(() -> {
                adapter.streamSql(allocator,
                        "SELECT range AS id FROM range(0, 5)", listener, true);
                return null;
            });
            assertTrue(finalBatchSent.await(1, TimeUnit.SECONDS));
            assertNull(result.get(75, TimeUnit.MILLISECONDS));
        } finally {
            caller.shutdownNow();
            ioPool.shutdownNow();
        }
    }

    /** Verifies a stalled Flight consumer aborts the DuckDB producer with timeout status. */
    @Test
    void streamSqlPropagatesListenerTimeout() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        AppConfig config = new AppConfig(
                3, 2, 1, 131072, 1, 1, 1,
                null, false, null, null,
                null, null, null, false, 2_147_483_648L, 1048576, 67108864, 25L, null, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 64, null, null, "complete", true, true);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);

        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool)) {
            FlightRuntimeException exception = assertTimeoutPreemptively(
                    Duration.ofSeconds(1), () -> assertThrows(FlightRuntimeException.class,
                            () -> adapter.streamSql(allocator,
                                    "SELECT range AS id FROM range(0, 5)",
                                    listener, true)));
            assertEquals(FlightStatusCode.TIMED_OUT, exception.status().code());
        } finally {
            ioPool.shutdownNow();
        }
    }

    // ── buildDuckSql / buildDuckSqlWithFilter ─────────────────────────────

    @Test
    void buildDuckSqlWithColumnSelect() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT id, name FROM s.t WHERE id > 0");
        String sql = DuckDbAdapter.buildDuckSql(pq, "my_from");
        assertTrue(sql.contains("SELECT \"id\", \"name\" FROM my_from"));
        assertTrue(sql.contains("\"id\" > 0"));
    }

    @Test
    void buildDuckSqlCastsDecimalSumToSparkPrecision() {
        // DuckDB widens decimal sums to precision 38; the pushed SUM must be cast
        // back to Decimal(min(38, p + 10), s) so the streamed batch type matches
        // the FlightInfo schema the strict columnar reader validates against.
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT sum(cast(l_quantity as decimal(15,2))) FROM s.t");
        String sql = DuckDbAdapter.buildDuckSql(pq, "t0");
        assertTrue(sql.contains("as decimal(25,2))"),
                "SUM of decimal(15,2) must be cast to decimal(25,2). Got: " + sql);
        assertTrue(sql.matches(".*cast\\(sum\\(.*\\) as decimal\\(25,2\\)\\).*"),
                "Got: " + sql);
    }

    @Test
    void buildDuckSqlWithCountStar() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT count(*) FROM s.t");
        String sql = DuckDbAdapter.buildDuckSql(pq, "t0");
        assertTrue(sql.contains("count(*)"));
        assertTrue(sql.contains("FROM t0"), "Got: " + sql);
    }

    @Test
    void buildDuckSqlWithGroupBy() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT bool_col, count(*) FROM s.t GROUP BY bool_col");
        String sql = DuckDbAdapter.buildDuckSql(pq, "t0");
        assertTrue(sql.contains("GROUP BY \"bool_col\""), "Got: " + sql);
    }

    @Test
    void buildDuckSqlWithFilter() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT id FROM s.t WHERE id > 10");
        String sql = DuckDbAdapter.buildDuckSql(pq, "t0");
        assertTrue(sql.contains("WHERE"), "Got: " + sql);
    }

    /** Verifies the TPC-H Q4 column comparison reaches DuckDB unchanged. */
    @Test
    void buildDuckSqlWithQ4ColumnComparison() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT l_orderkey FROM tpch.lineitem "
                        + "WHERE l_commitdate < l_receiptdate");

        String sql = DuckDbAdapter.buildDuckSql(pq, "lineitem_files");

        assertTrue(sql.contains("\"l_commitdate\" < \"l_receiptdate\""),
                "Got: " + sql);
    }

    @Test
    void buildDuckSqlWithFilterAppliedSkipsWhere() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT id FROM s.t WHERE id > 10");
        String sql = DuckDbAdapter.buildDuckSqlWithFilter(pq, "t0", true);
        assertFalse(sql.contains("WHERE"), "Got: " + sql);
    }

    @Test
    void buildDuckSqlWithSumMinMax() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT sum(amount), min(id), max(id) FROM s.t");
        String sql = DuckDbAdapter.buildDuckSql(pq, "t0");
        assertTrue(sql.contains("sum(\"amount\")"), "Got: " + sql);
        assertTrue(sql.contains("min(\"id\")"), "Got: " + sql);
        assertTrue(sql.contains("max(\"id\")"), "Got: " + sql);
    }

    @Test
    void buildDuckSqlDoesNotQuoteDecimalSumAsOneIdentifier() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT sum(cast(l_extendedprice * (1 - l_discount) "
                        + "as decimal(32,4))) FROM tpch.lineitem");
        String sql = DuckDbAdapter.buildDuckSql(pq, "t0");

        assertTrue(sql.contains("sum(cast("), "Got: " + sql);
        assertTrue(sql.contains("\"l_extendedprice\""), "Got: " + sql);
        assertTrue(sql.contains("\"l_discount\""), "Got: " + sql);
        assertFalse(sql.contains("sum(\"cast("), "Got: " + sql);
    }

    @Test
    void buildDuckSqlPreservesAggregateAliases() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT region, sum(revenue) AS total, count(*) AS rows "
                        + "FROM s.t GROUP BY region");

        String sql = DuckDbAdapter.buildDuckSql(pq, "read_parquet('part.parquet')");

        assertTrue(sql.contains("sum(\"revenue\") AS \"total\""), "Got: " + sql);
        assertTrue(sql.contains("count(*) AS \"rows\""), "Got: " + sql);
    }

    @Test
    void decimalSumExpressionExecutesInDuckDb() throws Exception {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT sum(cast(l_extendedprice * (1 - l_discount) "
                        + "as decimal(32,4))) FROM tpch.lineitem");
        String sql = DuckDbAdapter.buildDuckSql(pq, "q1_input");

        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE q1_input (l_extendedprice DECIMAL(15,2), "
                    + "l_discount DECIMAL(15,2))");
            statement.execute("INSERT INTO q1_input VALUES (100.00, 0.10)");
            try (ResultSet result = statement.executeQuery(sql)) {
                assertTrue(result.next());
                assertEquals(0, new java.math.BigDecimal("90.0000")
                        .compareTo(result.getBigDecimal(1)));
                assertEquals(38, result.getMetaData().getPrecision(1));
                assertEquals(4, result.getMetaData().getScale(1));
            }
        }
    }

    @Test
    void buildDuckSqlWithCountColumn() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT count(id) FROM s.t");
        String sql = DuckDbAdapter.buildDuckSql(pq, "t0");
        assertTrue(sql.contains("count(\"id\")"), "Got: " + sql);
    }

    // ── buildGroupedDuckSql ───────────────────────────────────────────────

    @Test
    void buildGroupedDuckSqlSingleFile() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT count(*) FROM s.t");
        String sql = DuckDbAdapter.buildGroupedDuckSql(pq, 1, false);
        assertTrue(sql.contains("FROM \"t0\""), "Got: " + sql);
        assertFalse(sql.contains("UNION ALL"), "Got: " + sql);
    }

    @Test
    void buildGroupedDuckSqlMultipleFiles() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT count(*) FROM s.t");
        String sql = DuckDbAdapter.buildGroupedDuckSql(pq, 3, false);
        assertTrue(sql.contains("UNION ALL"), "Got: " + sql);
        assertTrue(sql.contains("\"t0\""), "Got: " + sql);
        assertTrue(sql.contains("\"t1\""), "Got: " + sql);
        assertTrue(sql.contains("\"t2\""), "Got: " + sql);
        assertTrue(sql.startsWith("SELECT count(*) AS \"count(*)\" FROM ("), "Got: " + sql);
    }

    // ── buildSelectSql ────────────────────────────────────────────────────

    @Test
    void buildSelectSqlSelectStar() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT * FROM s.t");
        String sql = DuckDbAdapter.buildSelectSql(pq,
                DuckDbAdapter.readParquetFromClause(List.of("/data/f.parquet")));
        assertTrue(sql.contains("SELECT *"), "Got: " + sql);
        assertTrue(sql.contains("read_parquet"), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithColumns() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT id, name FROM s.t");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("\"id\", \"name\""), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithFilter() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT * FROM s.t WHERE id = 1");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("WHERE"), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithAggregation() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT count(*) FROM s.t WHERE id > 0");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("count(*)"), "Got: " + sql);
        assertTrue(sql.contains("WHERE"), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithCountColumn() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT count(id) FROM s.t");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("count(\"id\")"), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithSum() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT sum(amount) FROM s.t");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("sum(\"amount\")"), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithMin() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT min(id) FROM s.t");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("min(\"id\")"), "Got: " + sql);
    }

    @Test
    void buildSelectSqlWithMax() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT max(id) FROM s.t");
        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");
        assertTrue(sql.contains("max(\"id\")"), "Got: " + sql);
    }

    // ── appendSelectExpr ──────────────────────────────────────────────────

    @Test
    void appendSelectExprCountStar() {
        StringBuilder sb = new StringBuilder();
        ParquetQueryParser.SelectExpr expr = parseSingleExpr("SELECT count(*) FROM s.t");
        DuckDbAdapter.appendSelectExpr(sb, expr);
        assertTrue(sb.toString().startsWith("count(*)"),
                "Got: " + sb);
    }

    @Test
    void appendSelectExprCountColumn() {
        StringBuilder sb = new StringBuilder();
        ParquetQueryParser.SelectExpr expr = parseSingleExpr("SELECT count(id) FROM s.t");
        DuckDbAdapter.appendSelectExpr(sb, expr);
        assertTrue(sb.toString().contains("count(\"id\")"), "Got: " + sb);
    }

    @Test
    void appendSelectExprSum() {
        StringBuilder sb = new StringBuilder();
        ParquetQueryParser.SelectExpr expr = parseSingleExpr("SELECT sum(amount) FROM s.t");
        DuckDbAdapter.appendSelectExpr(sb, expr);
        assertTrue(sb.toString().contains("sum(\"amount\")"), "Got: " + sb);
    }

    @Test
    void appendSelectExprMin() {
        StringBuilder sb = new StringBuilder();
        ParquetQueryParser.SelectExpr expr = parseSingleExpr("SELECT min(price) FROM s.t");
        DuckDbAdapter.appendSelectExpr(sb, expr);
        assertTrue(sb.toString().contains("min(\"price\")"), "Got: " + sb);
    }

    @Test
    void appendSelectExprMax() {
        StringBuilder sb = new StringBuilder();
        ParquetQueryParser.SelectExpr expr = parseSingleExpr("SELECT max(score) FROM s.t");
        DuckDbAdapter.appendSelectExpr(sb, expr);
        assertTrue(sb.toString().contains("max(\"score\")"), "Got: " + sb);
    }

    @Test
    void appendSelectExprColumn() {
        StringBuilder sb = new StringBuilder();
        ParquetQueryParser.SelectExpr expr = parseSingleExpr("SELECT col_name FROM s.t");
        DuckDbAdapter.appendSelectExpr(sb, expr);
        assertTrue(sb.toString().contains("\"col_name\""), "Got: " + sb);
    }

    private static ParquetQueryParser.SelectExpr parseSingleExpr(String sql) {
        return ParquetQueryParser.parse(sql).selectExprs.get(0);
    }

    // ── ORDER BY / LIMIT, select order, result schema ─────────────────────

    /** Verifies scans carry ORDER BY and LIMIT to DuckDB. */
    @Test
    void buildSelectSqlAppendsOrderAndLimit() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT id FROM s.t WHERE id > 10 ORDER BY id DESC LIMIT 3");

        String sql = DuckDbAdapter.buildSelectSql(pq, "t0");

        assertTrue(sql.toLowerCase().contains(" order by \"id\" desc"), "Got: " + sql);
        assertTrue(sql.endsWith(" LIMIT 3"), "Got: " + sql);
    }

    /** Verifies aggregates return columns in the selected order, with their aliases. */
    @Test
    void aggregateKeepsSelectOrderAndAliases() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT count(*) AS c, flag AS f FROM s.t GROUP BY flag ORDER BY c DESC LIMIT 2");

        String sql = DuckDbAdapter.buildDuckSql(pq, "t0");

        assertTrue(sql.startsWith("SELECT count(*) AS \"c\", \"flag\" AS \"f\" FROM t0"),
                "Got: " + sql);
        assertTrue(sql.endsWith(" LIMIT 2"), "Got: " + sql);
    }

    /** Verifies grouping columns the client did not select still come first for merging. */
    @Test
    void aggregatePutsUnselectedGroupingColumnsFirst() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT count(*) AS c FROM s.t GROUP BY flag");

        String sql = DuckDbAdapter.buildDuckSql(pq, "t0");

        assertTrue(sql.startsWith("SELECT \"flag\", count(*) AS \"c\" FROM t0"), "Got: " + sql);
    }

    /** Verifies the described schema is the schema of the streamed batches. */
    @Test
    void describeSqlReportsStreamSchema() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        AppConfig config = new AppConfig(
                3, 2, 1, 131072, 0, 1, 1,
                null, false, null, null,
                null, null, null, false, 1048576, 67108864, 60000L, null, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 64, null, null, "complete", true, true);
        String sql = "SELECT range AS id, 'x' AS name, sum(range) AS total FROM range(0, 5) GROUP BY ALL";
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        AtomicReference<org.apache.arrow.vector.types.pojo.Schema> streamed = new AtomicReference<>();
        doAnswer(invocation -> {
            streamed.set(((VectorSchemaRoot) invocation.getArgument(0)).getSchema());
            return null;
        }).when(listener).start(any(VectorSchemaRoot.class));
        when(listener.isReady()).thenReturn(true);

        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool)) {
            org.apache.arrow.vector.types.pojo.Schema described = adapter.describeSql(allocator, sql);
            adapter.streamSql(allocator, sql, listener, true);

            assertEquals(List.of("id", "name", "total"),
                    described.getFields().stream().map(f -> f.getName()).toList());
            assertEquals(streamed.get(), described);
        } finally {
            ioPool.shutdownNow();
        }
    }

    /** Verifies DISTINCT, HAVING and DISTINCT aggregates are rendered for DuckDB. */
    @Test
    void rendersDistinctAndHaving() {
        String distinct = DuckDbAdapter.buildSelectSql(
                ParquetQueryParser.parse("SELECT DISTINCT a FROM s.t WHERE a > 1"), "t0");
        String having = DuckDbAdapter.buildDuckSql(ParquetQueryParser.parse(
                "SELECT a, count(*) AS c FROM s.t GROUP BY a HAVING count(*) > 1 ORDER BY c"), "t0");
        String countDistinct = DuckDbAdapter.buildDuckSql(
                ParquetQueryParser.parse("SELECT count(DISTINCT a) AS d FROM s.t"), "t0");

        assertTrue(distinct.startsWith("SELECT DISTINCT \"a\""), "Got: " + distinct);
        assertTrue(having.toLowerCase().contains(" having count(*) > 1 order by "), "Got: " + having);
        assertTrue(countDistinct.startsWith("SELECT count(DISTINCT \"a\") AS \"d\""),
                "Got: " + countDistinct);
    }

    // ── complete-answer mode ──────────────────────────────────────────────

    /** Verifies the first word of a statement is found behind comments and parentheses. */
    @Test
    void firstKeywordSkipsCommentsAndParentheses() {
        assertEquals("select", DuckDbAdapter.firstKeyword("  -- note\n /* x */ ((SELECT 1))"));
        assertEquals("with", DuckDbAdapter.firstKeyword("WITH t AS (SELECT 1) SELECT * FROM t"));
        assertEquals("drop", DuckDbAdapter.firstKeyword("/* hidden */ DROP VIEW s.t"));
        assertEquals("", DuckDbAdapter.firstKeyword("   "));
    }

    /** Verifies the schema of a complete answer, including two columns with the same name. */
    @Test
    void describeCompleteKeepsDuplicateColumnNames() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(completeConfig(), ioPool)) {
            org.apache.arrow.vector.types.pojo.Schema schema = adapter.describeComplete(allocator,
                    "SELECT a.id, b.id, avg(a.price) AS p FROM s.t a JOIN s.t b ON a.id = b.id GROUP BY 1, 2",
                    views());

            assertEquals(List.of("id", "id", "p"),
                    schema.getFields().stream().map(f -> f.getName()).toList());
        } finally {
            ioPool.shutdownNow();
        }
    }

    /** Verifies only statements that read are run in complete-answer mode. */
    @Test
    void completeAnswerRefusesStatementsThatAreNotQueries() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(completeConfig(), ioPool)) {
            for (String sql : new String[] {"DROP VIEW s.t", "CREATE TABLE x AS SELECT 1",
                    "INSERT INTO s.t VALUES (1, 2.0)", "SET threads = 1", "PRAGMA version",
                    "COPY (SELECT 1) TO 'x.csv'", "ATTACH 'x.db' AS x"}) {
                assertThrows(Exception.class, () -> adapter.describeComplete(allocator, sql, views()), sql);
            }
            assertNotNull(adapter.describeComplete(allocator, "VALUES (1)", views()));
        } finally {
            ioPool.shutdownNow();
        }
    }

    /** Verifies queries may not switch on DuckDB's log, read it or secrets, or run SQL text. */
    @Test
    void completeAnswerRefusesLoggingSecretsAndDynamicSql() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(completeConfig(), ioPool)) {
            for (String sql : new String[] {"SELECT * FROM enable_logging('HTTP')",
                    "SELECT * FROM ENABLE_LOGGING(level := 'trace')", "SELECT * FROM disable_logging()",
                    "SELECT * FROM truncate_duckdb_logs()", "SELECT * FROM duckdb_logs",
                    "SELECT * FROM system.main.duckdb_logs", "SELECT * FROM duckdb_log_contexts",
                    "SELECT * FROM duckdb_secrets()", "SELECT * FROM which_secret('s3://a/b', 's3')",
                    "SELECT * FROM query('SELECT 1')", "SELECT * FROM query_table('s.t')",
                    "SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT 1'))",
                    "SELECT id FROM s.t WHERE id IN (SELECT 1 FROM enable_logging())",
                    "WITH l AS (SELECT * FROM duckdb_logs) SELECT count(*) FROM l"}) {
                IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                        () -> adapter.describeComplete(allocator, sql, views()), sql);
                assertTrue(e.getMessage().contains("is not allowed"), sql + ": " + e.getMessage());
            }
            // Names inside text are not calls.
            assertNotNull(adapter.describeComplete(allocator,
                    "SELECT 'enable_logging' AS f, 'x\",\"function_name\":\"query' AS g", views()));
        } finally {
            ioPool.shutdownNow();
        }
    }

    /** Verifies a complete answer is computed over the views and streamed. */
    @Test
    void streamCompleteRunsClientSqlOverViews() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        AtomicReference<VectorSchemaRoot> rootRef = new AtomicReference<>();
        AtomicReference<String> values = new AtomicReference<>("");
        doAnswer(invocation -> {
            rootRef.set(invocation.getArgument(0));
            return null;
        }).when(listener).start(any(VectorSchemaRoot.class));
        when(listener.isReady()).thenReturn(true);
        doAnswer(invocation -> {
            values.set(values.get() + rootRef.get().contentToTSVString());
            return null;
        }).when(listener).putNext();

        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(completeConfig(), ioPool)) {
            adapter.streamComplete(allocator,
                    "SELECT count(*) AS n, avg(price) AS p FROM s.t WHERE id IN (SELECT id FROM s.t WHERE id > 1)",
                    views(), listener, true);
        } finally {
            ioPool.shutdownNow();
        }

        assertTrue(values.get().contains("2\t25.0"), values.get());
    }

    /** Verifies a locked-down connection reads its data directory and nothing else. */
    @Test
    void lockdownAllowsOnlyTheDataDirectory(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        java.nio.file.Path data = java.nio.file.Files.createDirectories(dir.resolve("data"));
        java.nio.file.Path outside = java.nio.file.Files.createDirectories(dir.resolve("outside"));
        try (Connection free = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = free.createStatement()) {
            s.execute("COPY (SELECT 42 AS x) TO '" + data.resolve("t.parquet") + "' (FORMAT parquet)");
            s.execute("COPY (SELECT 7 AS x) TO '" + outside.resolve("t.parquet") + "' (FORMAT parquet)");
        }
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try {
            DuckDbAdapter adapter = new DuckDbAdapter(completeConfig(), ioPool, List.of(data.toString()));
            Connection conn = adapter.connection();
            try (Statement s = conn.createStatement();
                    ResultSet rs = s.executeQuery("SELECT x FROM read_parquet('" + data.resolve("t.parquet") + "')")) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
            }
            for (String sql : new String[] {
                    "SELECT x FROM read_parquet('" + outside.resolve("t.parquet") + "')",
                    "SELECT * FROM read_text('/etc/hostname')"}) {
                try (Statement s = conn.createStatement()) {
                    assertThrows(java.sql.SQLException.class, () -> s.executeQuery(sql), sql);
                }
            }
            try (Statement s = conn.createStatement()) {
                assertThrows(java.sql.SQLException.class, () -> s.execute("SET enable_external_access = true"));
            }
            adapter.close();
        } finally {
            ioPool.shutdownNow();
        }
    }

    /** Verifies the store's setup statements run in every connection before the lockdown. */
    @Test
    void storageSetupRunsBeforeLockdown(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (DuckDbAdapter adapter = new DuckDbAdapter(completeConfig(), ioPool,
                List.of(dir.toString()), List.of("SET VARIABLE storage_marker = 42"))) {
            Connection conn = adapter.connection();
            try (Statement s = conn.createStatement();
                    ResultSet rs = s.executeQuery("SELECT getvariable('storage_marker')")) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
            }
            try (Statement s = conn.createStatement()) {
                assertThrows(java.sql.SQLException.class, () -> s.execute("SET enable_external_access = true"));
            }
        } finally {
            ioPool.shutdownNow();
        }
    }

    /**
     * Verifies a failing setup statement stops the adapter at construction and is not quoted, in
     * the exception or in the log, as it may hold storage credentials.
     */
    @Test
    void storageSetupFailureDoesNotQuoteTheStatement() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        java.io.PrintStream err = System.err;
        java.io.ByteArrayOutputStream log = new java.io.ByteArrayOutputStream();
        try {
            System.setErr(new java.io.PrintStream(log, true, java.nio.charset.StandardCharsets.UTF_8));
            for (String broken : List.of(
                    "CREATE SECRET broken (TYPE nosuchtype, SECRET 'hunter2')",
                    "CREATE SECRET broken (TYPE s3 KEY_ID 'AKIA' SECRET 'hunter2' syntax error")) {
                IllegalStateException e = assertThrows(IllegalStateException.class,
                        () -> new DuckDbAdapter(completeConfig(), ioPool, List.of(),
                                List.of("SELECT 1", broken)));
                StringBuilder messages = new StringBuilder();
                for (Throwable t = e; t != null; t = t.getCause()) {
                    messages.append(t.getMessage()).append('\n');
                }
                assertTrue(messages.toString().contains("Storage setup statement 2 of 2 failed"),
                        messages.toString());
                assertFalse(messages.toString().contains("hunter2"), messages.toString());
            }
        } finally {
            System.setErr(err);
            ioPool.shutdownNow();
        }
        assertFalse(log.toString(java.nio.charset.StandardCharsets.UTF_8).contains("hunter2"));
    }

    /** Verifies a setup error loses every string literal and the configured S3 secret. */
    @Test
    void setupErrorsAreSanitized() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        AppConfig base = completeConfig();
        AppConfig config = new AppConfig(base.numServers(), base.batchSize(), base.ioParallelism(),
                base.ioFileBufferSize(), base.duckDbWarmConnections(), base.duckDbGroups(),
                base.duckDbThreads(), null, false, null, null, null, null, null, false,
                base.grpcMaxInboundMessageSize(), base.flightBackpressureThresholdBytes(),
                base.flightListenerReadyTimeoutMillis(), null, null, 32010, 5701, 60, 3, 1000,
                30000, true, false, 64, null, null, "complete", true, true,
                new pro.surpin.data.arrowflight.server.storage.S3Settings("http://h:1", null, null,
                        null, true, "AKIA", "plainSecret", null, 30, 1, null, true));
        try (DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool)) {
            assertEquals("LINE 1: SECRET '***' near plain *** and '***'",
                    adapter.sanitizeSetupError("LINE 1: SECRET 'x''y' near plain plainSecret and 'cut off"));
            assertEquals("", adapter.sanitizeSetupError(null));
        } finally {
            ioPool.shutdownNow();
        }
    }

    /** Verifies DuckDB neither reads nor writes persistent secrets. */
    @Test
    void persistentSecretsAreDisabled() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (DuckDbAdapter adapter = new DuckDbAdapter(completeConfig(), ioPool)) {
            try (Statement s = adapter.connection().createStatement();
                    ResultSet rs = s.executeQuery("SELECT current_setting('allow_persistent_secrets')")) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1));
            }
        } finally {
            ioPool.shutdownNow();
        }
    }

    /**
     * Builds a configuration for complete-answer tests.
     *
     * @return server configuration
     */
    private static AppConfig completeConfig() {
        return new AppConfig(
                3, 100, 1, 131072, 0, 1, 1,
                null, false, null, null,
                null, null, null, false, 1048576, 67108864, 60000L, null, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 64, null, null, "complete", true, true);
    }

    /**
     * Builds view definitions over an in-memory table: ids 1, 2, 3 with prices 10, 20, 30.
     *
     * @return view definitions
     */
    private static pro.surpin.data.arrowflight.server.services.CatalogViews.Snapshot views() {
        return new pro.surpin.data.arrowflight.server.services.CatalogViews.Snapshot(1L,
                List.of("CREATE SCHEMA IF NOT EXISTS s",
                        "CREATE OR REPLACE VIEW s.t AS SELECT * FROM (VALUES (1, 10.0), (2, 20.0), (3, 30.0)) v(id, price)",
                        "CREATE OR REPLACE VIEW main.t AS SELECT * FROM s.t"),
                List.of("CREATE SCHEMA IF NOT EXISTS s",
                        "CREATE OR REPLACE VIEW s.t AS SELECT CAST(NULL AS INTEGER) AS id, CAST(NULL AS DECIMAL(3,1)) AS price WHERE false",
                        "CREATE OR REPLACE VIEW main.t AS SELECT * FROM s.t"),
                List.of("DROP VIEW IF EXISTS main.t", "DROP SCHEMA IF EXISTS s CASCADE"));
    }

    /** Verifies a text with several statements is refused before any of them runs. */
    @Test
    void completeAnswerRunsNothingFromATextWithSeveralStatements() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(completeConfig(), ioPool)) {
            for (String sql : new String[] {
                    "SELECT 1; DROP SCHEMA s CASCADE; SELECT 2",
                    "CREATE OR REPLACE VIEW s.t AS SELECT 'x' AS id; SELECT 1",
                    "SELECT $$'$$; DROP SCHEMA s CASCADE; SELECT '$$'",
                    "SELECT 1; SELECT 2"}) {
                assertThrows(IllegalArgumentException.class,
                        () -> adapter.describeComplete(allocator, sql, views()), sql);
                assertThrows(IllegalArgumentException.class,
                        () -> adapter.streamComplete(allocator, sql, views(),
                                mock(FlightProducer.ServerStreamListener.class), true), sql);
            }
            assertEquals(List.of("id", "price"), adapter.describeComplete(allocator,
                    "SELECT * FROM s.t;", views()).getFields().stream().map(f -> f.getName()).toList());
            assertEquals("3", completeAnswer(adapter, allocator, "SELECT count(*) FROM s.t WHERE id > 0"));
        } finally {
            ioPool.shutdownNow();
        }
    }

    /** Verifies a table can be named without its schema when no other schema has that name. */
    @Test
    void completeAnswerFindsTablesWithoutSchema() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(completeConfig(), ioPool)) {
            assertEquals("60.0", completeAnswer(adapter, allocator, "SELECT sum(price) FROM t"));
        } finally {
            ioPool.shutdownNow();
        }
    }

    /** Verifies a query needing an extension that is not loaded fails at once instead of downloading it. */
    @Test
    void describeDoesNotInstallExtensions() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(completeConfig(), ioPool)) {
            long start = System.nanoTime();
            assertThrows(java.sql.SQLException.class,
                    () -> adapter.describeComplete(allocator, "SELECT * FROM tpch_queries()", views()));
            assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5));
        } finally {
            ioPool.shutdownNow();
        }
    }

    /** Verifies a client's cancel interrupts DuckDB while it computes, without the cancel callback. */
    @Test
    void clientCancelInterruptsTheQuery() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        AtomicBoolean cancelled = new AtomicBoolean();
        when(listener.isCancelled()).thenAnswer(invocation -> cancelled.get());
        java.util.concurrent.ScheduledExecutorService client = Executors.newSingleThreadScheduledExecutor();
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(completeConfig(), ioPool)) {
            // As in gRPC: the flag is set at once, the callback waits for the busy call thread.
            client.schedule(() -> cancelled.set(true), 500, TimeUnit.MILLISECONDS);
            long start = System.nanoTime();
            FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                    () -> adapter.streamSql(allocator,
                            "SELECT sum(i % 7) FROM range(200000000000) r(i)", listener, true));
            assertEquals(FlightStatusCode.CANCELLED, e.status().code());
            assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10));
            assertEquals("1", completeAnswer(adapter, allocator, "SELECT 1"));
        } finally {
            client.shutdownNow();
            ioPool.shutdownNow();
        }
    }

    /**
     * Runs a complete-answer query and returns its first value.
     *
     * @param adapter DuckDB adapter
     * @param allocator Arrow allocator
     * @param sql client SQL
     * @return first value as text
     * @throws Exception on failure
     */
    private static String completeAnswer(DuckDbAdapter adapter, RootAllocator allocator, String sql)
            throws Exception {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
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
        adapter.streamComplete(allocator, sql, views(), listener, true);
        return value.get();
    }

    /** Verifies a query that failed while running fails the same way when run again. */
    @Test
    void queryFailingAtRunTimeCanBeRunAgain() throws Exception {
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(completeConfig(), ioPool)) {
            String sql = "SELECT max(error('late ' || id)) FROM s.t";
            for (int run = 0; run < 2; run++) {
                Exception e = assertThrows(Exception.class, () -> completeAnswer(adapter, allocator, sql));
                assertTrue(String.valueOf(e.getMessage()).contains("late"), e.getMessage());
            }
        } finally {
            ioPool.shutdownNow();
        }
    }
}
