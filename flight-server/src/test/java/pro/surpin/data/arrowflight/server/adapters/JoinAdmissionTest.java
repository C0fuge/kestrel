package pro.surpin.data.arrowflight.server.adapters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.DriverManager;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.server.duckdb.DuckDbSetupPlan;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.services.CatalogViews;
import pro.surpin.data.arrowflight.server.services.QueryCacheLimits;

/** Tests structural join admission and its existing cancellable producer-pass lifecycle. */
@Tag("unit")
class JoinAdmissionTest {
    private static final String PROPERTY = "arrowflight.experimental.joinProducingStreams";
    private static final String JOIN = "SELECT a.i FROM range(3) a(i) JOIN range(3) b(j) ON a.i = b.j";
    private static final CatalogViews.Snapshot EMPTY = new CatalogViews.Snapshot(1L,
            List.of(), List.of(), List.of());

    /** Checks real DuckDB node types and excludes fake markers in client text. */
    @Test
    void classifiesDuckDbJoinsWithoutLookingForSqlKeywords() throws Exception {
        String check = "SELECT coalesce((j->>'error')::BOOLEAN, true), j->>'error_type',"
                + " j->>'error_message', coalesce(json_array_length(j->'statements'), 0), j::VARCHAR"
                + " FROM (SELECT json_serialize_sql(?::VARCHAR) AS j)";
        try (var connection = DriverManager.getConnection("jdbc:duckdb:");
                var statement = connection.prepareStatement(check)) {
            for (String sql : List.of(
                    "SELECT * FROM s.a JOIN s.b USING (id)",
                    "SELECT * FROM s.a, s.b",
                    "SELECT * FROM s.a CROSS JOIN s.b",
                    "SELECT * FROM s.a LEFT JOIN s.a b ON true",
                    "SELECT * FROM s.a NATURAL JOIN s.b",
                    "SELECT * FROM s.a SEMI JOIN s.b USING (id)",
                    "SELECT * FROM s.a ANTI JOIN s.b USING (id)",
                    "SELECT * FROM s.a POSITIONAL JOIN s.b",
                    "SELECT * FROM s.a ASOF JOIN s.b ON a.id >= b.id",
                    "SELECT * FROM (SELECT * FROM s.a JOIN s.b USING (id)) q",
                    "WITH q AS (SELECT * FROM s.a JOIN s.b USING (id)) SELECT * FROM q")) {
                assertEquals(DuckDbAdapter.Admission.JOIN,
                        DuckDbAdapter.checkSingleQuery(statement, sql).admission(), sql);
            }
            for (String sql : List.of(
                    "SELECT * FROM s.a WHERE id = 1",
                    "SELECT count(*) FROM s.a",
                    "SELECT '\"type\":\"JOIN\"' FROM s.a",
                    "SELECT $$\"type\":\"JOIN\"$$ FROM s.a",
                    "SELECT E'\\\' JOIN s.b' FROM s.a",
                    "SELECT 1 /* outer /* inner */ JOIN s.b */ FROM s.a",
                    "SELECT 1 AS \"JOIN\" FROM s.a -- JOIN s.b")) {
                assertEquals(DuckDbAdapter.Admission.NON_JOIN,
                        DuckDbAdapter.checkSingleQuery(statement, sql).admission(), sql);
            }
            assertEquals(DuckDbAdapter.Admission.UNKNOWN,
                    DuckDbAdapter.checkSingleQuery(statement, "SELECT 1 UNION ALL SELECT 2").admission());
        }
        assertEquals(DuckDbAdapter.Admission.UNKNOWN, DuckDbAdapter.admission(null));
    }

    /** Requires an explicit nonnegative integer while leaving the experiment off by default. */
    @Test
    void validatesTheExperimentalProperty() {
        String previous = System.getProperty(PROPERTY);
        try {
            System.clearProperty(PROPERTY);
            assertEquals(0, DuckDbAdapter.joinProducingStreams());
            for (String value : List.of("-1", "garbage", "", "2147483648")) {
                System.setProperty(PROPERTY, value);
                IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                        DuckDbAdapter::joinProducingStreams);
                assertTrue(error.getMessage().contains(PROPERTY));
            }
        } finally {
            restore(previous);
        }
    }

    /** Keeps the existing global gate authoritative and gates unknowns conservatively. */
    @Test
    void preservesGlobalAdmissionAndSelectsTheExperimentalGate() throws Exception {
        ProducingStreamGate global = new ProducingStreamGate(2);
        try (Harness fixture = new Harness(global)) {
            for (DuckDbAdapter.Admission admission : DuckDbAdapter.Admission.values()) {
                assertSame(global, fixture.adapter.producingGate(admission));
            }
        }
        try (Harness fixture = new Harness(ProducingStreamGate.OFF)) {
            assertSame(ProducingStreamGate.OFF,
                    fixture.adapter.producingGate(DuckDbAdapter.Admission.NON_JOIN));
            assertSame(fixture.adapter.joinProducingGate(),
                    fixture.adapter.producingGate(DuckDbAdapter.Admission.JOIN));
            assertSame(fixture.adapter.joinProducingGate(),
                    fixture.adapter.producingGate(DuckDbAdapter.Admission.UNKNOWN));
        }
    }

    /** Carries admission through cache hits, uncached large SQL, and both one-shot paths. */
    @Test
    void joinsWaitOnEveryStreamingPathWhileNonJoinsBypass() throws Exception {
        try (Harness fixture = new Harness(ProducingStreamGate.OFF)) {
            String large = JOIN + " /*" + "x".repeat(QueryCacheLimits.MAX_CACHED_QUERY_CHARS) + "*/";
            for (int path = 0; path < 6; path++) {
                int selected = path;
                String sql = path == 2 ? large : path >= 3 ? JOIN + " /* path " + path + " */" : JOIN;
                long waits = fixture.adapter.joinProducingGate().waits();
                Future<?> join;
                try (var holder = fixture.adapter.joinProducingGate().pass(() -> false)) {
                    holder.enter();
                    join = fixture.pool.submit(() -> {
                        fixture.stream(selected, sql, ready());
                        return null;
                    });
                    awaitWait(fixture.adapter.joinProducingGate(), waits);
                    Future<?> point = fixture.pool.submit(() -> {
                        fixture.stream(0, "SELECT 1", ready());
                        return null;
                    });
                    point.get(5, TimeUnit.SECONDS);
                    assertFalse(join.isDone());
                }
                join.get(5, TimeUnit.SECONDS);
                if (path == 3 || path == 5) {
                    assertFalse(fixture.adapter.parseCheckRemembered(sql));
                }
            }
            assertTrue(fixture.adapter.parseCheckRemembered(JOIN));
            assertFalse(fixture.adapter.parseCheckRemembered(large));
        }
    }

    /** Keeps non-join pruning rewrites out of the join queue without a second parse check. */
    @Test
    void pruningRetainsTheAcceptedAdmissionClass() throws Exception {
        try (Harness fixture = new Harness(ProducingStreamGate.OFF)) {
            CatalogViews.Snapshot views = new CatalogViews.Snapshot(1L,
                    List.of("CREATE SCHEMA s", "CREATE VIEW s.t AS SELECT 1 AS id"),
                    List.of("CREATE SCHEMA s", "CREATE VIEW s.t AS SELECT 1 AS id WHERE false"),
                    List.of("DROP SCHEMA s CASCADE"));
            String original = "SELECT id FROM s.t";
            fixture.adapter.describeComplete(fixture.allocator, original, views);
            int at = original.indexOf("s.t");
            String replacement = "(SELECT 1 AS id) AS t";
            String rewritten = original.substring(0, at) + replacement;
            assertTrue(fixture.adapter.acceptRewrite(original, at, at + 3, rewritten, replacement));
            try (var holder = fixture.adapter.joinProducingGate().pass(() -> false)) {
                holder.enter();
                fixture.pool.submit(() -> {
                    fixture.stream(0, rewritten, ready());
                    return null;
                }).get(5, TimeUnit.SECONDS);
            }
            assertEquals(0, fixture.adapter.joinProducingGate().waits());
            String join = "SELECT a.id FROM s.t a JOIN s.t b ON a.id = b.id";
            fixture.adapter.describeComplete(fixture.allocator, join, views);
            int joinAt = join.indexOf("s.t");
            String joined = join.substring(0, joinAt) + "(SELECT 1 AS id)" + join.substring(joinAt + 3);
            assertTrue(fixture.adapter.acceptRewrite(join, joinAt, joinAt + 3, joined, "(SELECT 1 AS id)"));
            Future<?> waiting;
            try (var holder = fixture.adapter.joinProducingGate().pass(() -> false)) {
                holder.enter();
                waiting = fixture.pool.submit(() -> {
                    fixture.adapter.streamComplete(fixture.allocator, joined, views, ready(), true);
                    return null;
                });
                awaitWait(fixture.adapter.joinProducingGate(), 0);
            }
            waiting.get(5, TimeUnit.SECONDS);
        }
    }

    /** Cancels queued work and releases permits after both execution and handoff failures. */
    @Test
    void cancellationAndFailuresDoNotLeakPermits() throws Exception {
        try (Harness fixture = new Harness(ProducingStreamGate.OFF)) {
            ServerStreamListener cancelled = ready();
            AtomicBoolean gone = new AtomicBoolean();
            when(cancelled.isCancelled()).thenAnswer(invocation -> gone.get());
            try (var holder = fixture.adapter.joinProducingGate().pass(() -> false)) {
                holder.enter();
                Future<?> waiting = fixture.pool.submit(() -> {
                    fixture.stream(0, JOIN, cancelled);
                    return null;
                });
                awaitWait(fixture.adapter.joinProducingGate(), 0);
                gone.set(true);
                ExecutionException error = assertThrows(ExecutionException.class,
                        () -> waiting.get(5, TimeUnit.SECONDS));
                assertEquals(FlightStatusCode.CANCELLED,
                        ((FlightRuntimeException) error.getCause()).status().code());
            }
            assertThrows(Exception.class, () -> fixture.stream(0,
                    "SELECT error('expected') FROM range(1) a JOIN range(1) b ON true", ready()));
            ServerStreamListener failed = ready();
            doAnswer(invocation -> { throw new IllegalStateException("expected handoff failure"); })
                    .when(failed).putNext();
            assertThrows(Exception.class, () -> fixture.stream(0, JOIN, failed));
            ArrowReader reader = mock(ArrowReader.class);
            when(reader.loadNextBatch()).thenThrow(new IOException("expected batch failure"));
            var load = DuckDbAdapter.class.getDeclaredMethod("loadNextBatch", ArrowReader.class,
                    ProducingStreamGate.Pass.class);
            load.setAccessible(true);
            try (var pass = fixture.adapter.joinProducingGate().pass(() -> false)) {
                InvocationTargetException error = assertThrows(InvocationTargetException.class,
                        () -> load.invoke(null, reader, pass));
                assertTrue(error.getCause() instanceof IOException);
                // Keep the pass open: loadNextBatch itself must have released the permit.
                fixture.pool.submit(() -> {
                    fixture.stream(0, JOIN, ready());
                    return null;
                }).get(5, TimeUnit.SECONDS);
            }
            fixture.pool.submit(() -> {
                fixture.stream(0, JOIN, ready());
                return null;
            }).get(5, TimeUnit.SECONDS);
        }
    }

    /** Gives the permit back before listener readiness and outbound handoff can block. */
    @Test
    void slowReadersDoNotBlockOtherJoinProducers() throws Exception {
        try (Harness fixture = new Harness(ProducingStreamGate.OFF)) {
            ServerStreamListener slow = ready();
            CountDownLatch readiness = new CountDownLatch(1);
            CountDownLatch send = new CountDownLatch(1);
            CountDownLatch sending = new CountDownLatch(1);
            AtomicBoolean canRead = new AtomicBoolean();
            when(slow.isReady()).thenAnswer(invocation -> {
                readiness.countDown();
                return canRead.get();
            });
            doAnswer(invocation -> {
                sending.countDown();
                assertTrue(send.await(5, TimeUnit.SECONDS));
                return null;
            }).when(slow).putNext();
            Future<?> first = fixture.pool.submit(() -> {
                fixture.stream(0, JOIN, slow);
                return null;
            });
            try {
                assertTrue(readiness.await(5, TimeUnit.SECONDS));
                fixture.pool.submit(() -> {
                    fixture.stream(0, JOIN, ready());
                    return null;
                }).get(5, TimeUnit.SECONDS);
                canRead.set(true);
                assertTrue(sending.await(5, TimeUnit.SECONDS));
                fixture.pool.submit(() -> {
                    fixture.stream(0, JOIN, ready());
                    return null;
                }).get(5, TimeUnit.SECONDS);
            } finally {
                canRead.set(true);
                send.countDown();
            }
            first.get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Waits until a producer has entered the gate's cancellation-aware wait.
     *
     * @param gate gate under test
     * @param before previous wait count
     * @throws InterruptedException when interrupted
     */
    private static void awaitWait(ProducingStreamGate gate, long before) throws InterruptedException {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (gate.waits() == before && System.nanoTime() < end) {
            Thread.sleep(5);
        }
        assertTrue(gate.waits() > before, "producer did not reach admission");
    }

    /**
     * Makes a listener that accepts every batch.
     *
     * @return ready listener
     */
    private static ServerStreamListener ready() {
        ServerStreamListener listener = mock(ServerStreamListener.class);
        when(listener.isReady()).thenReturn(true);
        return listener;
    }

    /**
     * Restores the test's experimental property.
     *
     * @param previous previous property value, or null
     */
    private static void restore(String previous) {
        if (previous == null) {
            System.clearProperty(PROPERTY);
        } else {
            System.setProperty(PROPERTY, previous);
        }
    }

    /** Owns the small in-memory DuckDB sessions used by each test. */
    private static final class Harness implements AutoCloseable {
        private final ExecutorService pool = Executors.newCachedThreadPool();
        private final RootAllocator allocator = new RootAllocator();
        private final DuckDbAdapter adapter;

        /**
         * Creates a node with an experimental join cap of one.
         *
         * @param global existing global producer gate
         */
        private Harness(ProducingStreamGate global) {
            AppConfig config = new AppConfig(3, 100, 1, 131072, 0, 1, 1,
                    null, false, null, null, null, null, null, false,
                    1048576, 67108864, 60000L, null, null,
                    32010, 5701, 60, 3, 1000, 30000, true, false, 2, null, null,
                    "complete", true, true);
            String previous = System.getProperty(PROPERTY);
            try {
                System.setProperty(PROPERTY, "1");
                adapter = new DuckDbAdapter(config, pool, DuckDbSetupPlan.of(List.of(), List.of()),
                        0, 1, global);
            } finally {
                restore(previous);
            }
        }

        /**
         * Executes one of the normal or one-shot streaming entry points.
         *
         * @param path entry point index
         * @param sql statement to stream
         * @param listener stream listener
         * @throws Exception on stream failure
         */
        private void stream(int path, String sql, ServerStreamListener listener) throws Exception {
            switch (path) {
                case 3 -> adapter.streamSqlOnce(allocator, sql, listener, true);
                case 4 -> adapter.streamComplete(allocator, sql, List.of(), EMPTY, listener, true);
                case 5 -> adapter.streamCompleteOnce(allocator, sql, List.of(), EMPTY, listener, true);
                default -> adapter.streamSql(allocator, sql, listener, true);
            }
        }

        @Override
        public void close() throws Exception {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
            adapter.close();
            allocator.close();
        }
    }
}
