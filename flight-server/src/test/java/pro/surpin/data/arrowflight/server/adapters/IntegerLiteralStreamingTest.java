package pro.surpin.data.arrowflight.server.adapters;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.metrics.CacheMetrics;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.services.CatalogViews;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Checks opt-in literal statement lifetime while Flight readers pause, cancel and resume. */
@Tag("unit")
class IntegerLiteralStreamingTest {
    private static final String EXPERIMENT = "arrowflight.experimental.integerLiteralParameters";
    private static final CatalogViews.Snapshot VIEWS = new CatalogViews.Snapshot(1, List.of(), List.of(), List.of());
    private String previous;

    /** Enables the experiment for this test alone. */
    @BeforeEach
    void enableExperiment() {
        previous = System.setProperty(EXPERIMENT, "true");
    }

    /** Restores the caller's experiment setting after all streams have stopped. */
    @AfterEach
    void restoreExperiment() {
        if (previous == null) {
            System.clearProperty(EXPERIMENT);
        } else {
            System.setProperty(EXPERIMENT, previous);
        }
    }

    /**
     * Keeps a partial-mode batch stable during backpressure and binds the next literal correctly.
     *
     * @throws Exception when streaming or cleanup fails
     */
    @Test
    void partialSlowReaderCanResumeAndRebind() throws Exception {
        checkSlowReader(false);
    }

    /**
     * Keeps a complete-mode batch stable during backpressure and binds the next literal correctly.
     *
     * @throws Exception when streaming or cleanup fails
     */
    @Test
    void completeSlowReaderCanResumeAndRebind() throws Exception {
        checkSlowReader(true);
    }

    /**
     * Reuses a partial-mode worker after cancellation with a different comparison value.
     *
     * @throws Exception when streaming or cleanup fails
     */
    @Test
    void partialCancelledStreamCanRebindOnTheSameSession() throws Exception {
        checkCancelThenReuse(false);
    }

    /**
     * Reuses a complete-mode worker after cancellation with a different comparison value.
     *
     * @throws Exception when streaming or cleanup fails
     */
    @Test
    void completeCancelledStreamCanRebindOnTheSameSession() throws Exception {
        checkCancelThenReuse(true);
    }

    /**
     * Disarms an old partial-mode cancel callback before its statement serves another stream.
     *
     * @throws Exception when streaming or cleanup fails
     */
    @Test
    void partialOldCancelCannotInterruptTheReusedStatement() throws Exception {
        checkOldCancelAfterReuse(false);
    }

    /**
     * Disarms an old complete-mode cancel callback before its statement serves another stream.
     *
     * @throws Exception when streaming or cleanup fails
     */
    @Test
    void completeOldCancelCannotInterruptTheReusedStatement() throws Exception {
        checkOldCancelAfterReuse(true);
    }

    /**
     * Exercises backpressure and changed values with one- and two-entry statement caches.
     *
     * @param complete whether to stream the complete result path
     * @throws Exception when streaming or cleanup fails
     */
    private static void checkSlowReader(boolean complete) throws Exception {
        for (int cacheSize : new int[] {1, 2}) {
            try (Harness harness = new Harness(cacheSize)) {
                Capture paused = harness.capture(false);
                Future<?> first = harness.stream(complete, 5, paused);
                paused.awaitStart();
                List<Long> original = paused.currentBatch();
                assertEquals(List.of(0L, 1L), original);
                assertThrows(TimeoutException.class, () -> first.get(50, TimeUnit.MILLISECONDS));
                assertEquals(original, paused.currentBatch(), "a paused Arrow batch must not be overwritten");
                assertTrue(paused.values.isEmpty(), "no batch may reach a non-ready reader");
                paused.resume();
                first.get(5, TimeUnit.SECONDS);
                assertEquals(List.of(0L, 1L, 2L, 3L, 4L), paused.values);

                Capture next = harness.capture(true);
                harness.stream(complete, 3, next).get(5, TimeUnit.SECONDS);
                assertEquals(List.of(0L, 1L, 2L), next.values);
            }
        }
    }

    /**
     * Cancels a backpressured stream and uses the same worker session for another literal.
     *
     * @param complete whether to stream the complete result path
     * @throws Exception when streaming or cleanup fails
     */
    private static void checkCancelThenReuse(boolean complete) throws Exception {
        for (int cacheSize : new int[] {1, 2}) {
            try (Harness harness = new Harness(cacheSize)) {
                Connection original = harness.worker.submit(harness.adapter::connection).get(5, TimeUnit.SECONDS);
                Capture cancelled = harness.capture(false);
                Future<?> first = harness.stream(complete, 7, cancelled);
                cancelled.awaitStart();
                cancelled.cancel();
                ExecutionException failure = assertThrows(ExecutionException.class,
                        () -> first.get(5, TimeUnit.SECONDS));
                FlightRuntimeException cause = assertInstanceOf(FlightRuntimeException.class, failure.getCause());
                assertEquals(FlightStatusCode.CANCELLED, cause.status().code());
                assertTrue(cancelled.values.isEmpty());

                Capture next = harness.capture(true);
                harness.stream(complete, 3, next).get(5, TimeUnit.SECONDS);
                assertEquals(List.of(0L, 1L, 2L), next.values);
                assertSame(original, harness.worker.submit(harness.adapter::connection).get(5, TimeUnit.SECONDS),
                        "recovery must work on the same session, not by opening another connection");
            }
        }
    }

    /**
     * Invokes the previous stream's cancellation while its cached shape computes a new query.
     *
     * @param complete whether to stream the complete result path
     * @throws Exception when streaming or cleanup fails
     */
    private static void checkOldCancelAfterReuse(boolean complete) throws Exception {
        try (Harness harness = new Harness(2)) {
            String sql = "SELECT sum((a.i * b.j) % 97) FROM range(0, 100000) a(i)"
                    + " CROSS JOIN range(0, 100000) b(j) WHERE a.i < ";
            long evictions = CacheMetrics.evictionCount(CacheMetrics.Cache.STATEMENT);
            Capture previous = harness.capture(true);
            harness.stream(complete, sql + 1, previous).get(5, TimeUnit.SECONDS);
            assertEquals(List.of(0L), previous.values);
            Capture next = harness.capture(true);
            // Keep native execution active only until the controlled cancellation below.
            Future<?> second = harness.stream(complete, sql + 99999, next);
            harness.awaitExecutingQuery();
            previous.cancel();
            assertThrows(TimeoutException.class, () -> second.get(100, TimeUnit.MILLISECONDS));
            assertFalse(next.cancelled.get());
            next.cancel();
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> second.get(5, TimeUnit.SECONDS));
            assertEquals(FlightStatusCode.CANCELLED,
                    assertInstanceOf(FlightRuntimeException.class, failure.getCause()).status().code());
            assertTrue(next.values.isEmpty());

            Capture recovered = harness.capture(true);
            harness.stream(complete, sql + 2, recovered).get(5, TimeUnit.SECONDS);
            // 1030 full cycles of residues 0..96, followed by 0..89.
            assertEquals(List.of(4_799_685L), recovered.values);
            assertEquals(evictions, CacheMetrics.evictionCount(CacheMetrics.Cache.STATEMENT),
                    "the parse check and one reused shape fit in two entries");
        }
    }

    /** Holds a small adapter and one persistent query worker for statement reuse checks. */
    private static final class Harness implements AutoCloseable {
        private final ExecutorService io = Executors.newSingleThreadExecutor();
        private final AtomicReference<Thread> queryThread = new AtomicReference<>();
        private final ExecutorService worker = Executors.newSingleThreadExecutor(task -> {
            Thread thread = new Thread(task, "literal-stream-test");
            queryThread.set(thread);
            return thread;
        });
        private final RootAllocator allocator = new RootAllocator();
        private final DuckDbAdapter adapter;
        private final List<Capture> captures = new ArrayList<>();

        /**
         * Opens a tiny-batch adapter with a chosen statement cache bound.
         *
         * @param cacheSize prepared statements kept per session
         */
        private Harness(int cacheSize) {
            AppConfig config = new AppConfig(
                    3, 2, 1, 131072, 0, 1, 1,
                    null, false, null, null,
                    null, null, "64MiB", false, 1048576, 67108864, 5000L, null, null,
                    32010, 5701, 60, 3, 1000, 30000, true, false, cacheSize, null, null,
                    "complete", true, true);
            adapter = new DuckDbAdapter(config, io);
        }

        /**
         * Creates a listener whose lifetime is bounded by this harness.
         *
         * @param ready initial reader readiness
         * @return capture of streamed values and lifecycle callbacks
         */
        private Capture capture(boolean ready) {
            Capture capture = new Capture(ready);
            captures.add(capture);
            return capture;
        }

        /**
         * Runs an eligible query on the persistent worker.
         *
         * @param complete whether to use complete-mode streaming
         * @param bound exclusive comparison bound
         * @param capture controlled Flight reader
         * @return submitted stream task
         */
        private Future<?> stream(boolean complete, int bound, Capture capture) {
            String sql = "SELECT i FROM range(0, 10) t(i) WHERE i < " + bound + " ORDER BY i";
            return stream(complete, sql, capture);
        }

        /**
         * Runs checked SQL on the persistent worker.
         *
         * @param complete whether to use complete-mode streaming
         * @param sql eligible query SQL
         * @param capture controlled Flight reader
         * @return submitted stream task
         */
        private Future<?> stream(boolean complete, String sql, Capture capture) {
            assertNotNull(DuckDbAdapter.literalParameters(sql), "the experimental path must apply");
            return worker.submit(() -> {
                if (complete) {
                    adapter.streamComplete(allocator, sql, VIEWS, capture.listener, true);
                } else {
                    adapter.streamSql(allocator, sql, capture.listener, true);
                }
                return null;
            });
        }

        /**
         * Waits until the stream's query is computing in DuckDB rather than parsing its SQL.
         *
         * @throws InterruptedException when the test is interrupted
         */
        private void awaitExecutingQuery() throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                StackTraceElement[] stack = queryThread.get().getStackTrace();
                boolean query = java.util.Arrays.stream(stack).anyMatch(frame ->
                        frame.getClassName().equals(DuckDbAdapter.class.getName())
                                && frame.getMethodName().equals("executeQuery"));
                boolean nativeCall = java.util.Arrays.stream(stack).anyMatch(frame ->
                        frame.getClassName().equals("org.duckdb.DuckDBNative") && frame.isNativeMethod());
                if (query && nativeCall) {
                    return;
                }
                Thread.sleep(1);
            }
            fail("query did not reach native execution");
        }

        /**
         * Stops outstanding streams before releasing their native and Arrow resources.
         *
         * @throws Exception when worker termination or resource cleanup fails
         */
        @Override
        public void close() throws Exception {
            captures.forEach(Capture::stop);
            worker.shutdownNow();
            try {
                assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS), "query worker did not stop");
            } finally {
                adapter.close();
                allocator.close();
                io.shutdownNow();
            }
        }
    }

    /** Captures synchronous batches while exposing explicit readiness and cancellation controls. */
    private static final class Capture {
        private final FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);
        private final AtomicBoolean ready;
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final AtomicReference<VectorSchemaRoot> root = new AtomicReference<>();
        private final AtomicReference<Runnable> readyHandler = new AtomicReference<>();
        private final AtomicReference<Runnable> cancelHandler = new AtomicReference<>();
        private final CountDownLatch started = new CountDownLatch(1);
        private final List<Long> values = new ArrayList<>();

        /**
         * Registers a listener that copies values only during synchronous batch delivery.
         *
         * @param initiallyReady initial reader readiness
         */
        private Capture(boolean initiallyReady) {
            ready = new AtomicBoolean(initiallyReady);
            when(listener.isReady()).thenAnswer(invocation -> ready.get());
            when(listener.isCancelled()).thenAnswer(invocation -> cancelled.get());
            doAnswer(invocation -> {
                readyHandler.set(invocation.getArgument(0));
                return null;
            }).when(listener).setOnReadyHandler(any(Runnable.class));
            doAnswer(invocation -> {
                cancelHandler.set(invocation.getArgument(0));
                return null;
            }).when(listener).setOnCancelHandler(any(Runnable.class));
            doAnswer(invocation -> {
                root.set(invocation.getArgument(0));
                started.countDown();
                return null;
            }).when(listener).start(any(VectorSchemaRoot.class));
            doAnswer(invocation -> {
                values.addAll(currentBatch());
                return null;
            }).when(listener).putNext();
        }

        /**
         * Waits for the first batch to reach the readiness boundary.
         *
         * @throws InterruptedException when the test is interrupted
         */
        private void awaitStart() throws InterruptedException {
            assertTrue(started.await(5, TimeUnit.SECONDS), "stream did not reach its first batch");
        }

        /**
         * Copies values from the current stable batch.
         *
         * @return current batch values
         */
        private List<Long> currentBatch() {
            VectorSchemaRoot batch = root.get();
            List<Long> copy = new ArrayList<>();
            for (int row = 0; row < batch.getRowCount(); row++) {
                copy.add(((Number) batch.getVector(0).getObject(row)).longValue());
            }
            return copy;
        }

        /** Marks the reader ready and wakes its current stream when a callback is registered. */
        private void resume() {
            ready.set(true);
            Runnable callback = readyHandler.get();
            if (callback != null) {
                callback.run();
            }
        }

        /** Stops any active native query or readiness wait during fixture cleanup. */
        private void stop() {
            cancelled.set(true);
            resume();
            Runnable callback = cancelHandler.get();
            if (callback != null) {
                callback.run();
            }
        }

        /** Delivers the listener's cancellation flag and callback to its current or finished stream. */
        private void cancel() {
            cancelled.set(true);
            assertNotNull(cancelHandler.get(), "stream did not register its cancel callback");
            cancelHandler.get().run();
        }
    }
}
