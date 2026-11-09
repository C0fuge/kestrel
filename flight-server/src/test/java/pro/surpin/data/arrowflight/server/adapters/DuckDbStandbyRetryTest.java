package pro.surpin.data.arrowflight.server.adapters;

import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.services.CatalogViews;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests that a query refused by a standby HDFS NameNode before anything was sent runs again
 * (an HA failover: libhdfs3 gives up while both NameNodes are standby), and only then.
 */
@Tag("unit")
class DuckDbStandbyRetryTest {

    /** Hadoop's StandbyException text as the hadoopfs extension reports it. */
    private static final String STANDBY = "IO Error: Unable to get file info: hdfs://ns1/data/t/part-0.parquet:"
            + " Operation category READ is not supported in state standby";

    /**
     * Verifies the text of a standby NameNode's refusal is recognised, also as a cause, and
     * other errors are not.
     */
    @Test
    void recognisesTheStandbyRefusal() {
        assertTrue(DuckDbAdapter.standbyNameNode(new java.sql.SQLException(STANDBY)));
        assertTrue(DuckDbAdapter.standbyNameNode(new RuntimeException("wrapped",
                new java.sql.SQLException("org.apache.hadoop.ipc.StandbyException: no"))));
        assertFalse(DuckDbAdapter.standbyNameNode(new java.sql.SQLException(
                "IO Error: Unable to get file info: hdfs://ns1/x: Permission denied")));
        assertFalse(DuckDbAdapter.standbyNameNode(new java.sql.SQLException((String) null)));
    }

    /**
     * Verifies a query that a standby NameNode refused twice before sending anything runs again
     * and answers once (service matrix hdfs-ha loss cell: q07 / q08 / q21 failed INTERNAL after
     * {@code haadmin -failover}).
     */
    @Test
    void refusedBeforeSendingRunsAgainAndAnswers() throws Exception {
        String sql = "SELECT CASE WHEN nextval('standby_seq') <= 2 THEN error('" + STANDBY + "')"
                + " ELSE 42 END AS v";
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(config(), Executors.newSingleThreadExecutor())) {
            adapter.standbyRetryDelaysMillis(1, 1, 1);
            FlightProducer.ServerStreamListener listener = listener();
            AtomicReference<String> value = capture(listener);
            adapter.streamComplete(allocator, sql, views(), listener, true);
            assertEquals("42", value.get());
            verify(listener, times(1)).start(any(VectorSchemaRoot.class));
            assertEquals(2, adapter.standbyRetries());
        }
    }

    /**
     * Verifies a query the NameNodes refuse on every run fails with the refusal after the last
     * pause, and a different error is not run again.
     */
    @Test
    void refusedOnEveryRunFailsWithTheRefusal() throws Exception {
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(config(), Executors.newSingleThreadExecutor())) {
            adapter.standbyRetryDelaysMillis(1, 1, 1);
            String always = "SELECT CASE WHEN nextval('standby_seq') > 0 THEN error('" + STANDBY + "')"
                    + " ELSE 1 END AS v";
            java.sql.SQLException e = assertThrows(java.sql.SQLException.class, () -> adapter.streamComplete(
                    allocator, always, views(), listener(), true));
            assertTrue(e.getMessage().contains("in state standby"), e.getMessage());
            assertEquals(3, adapter.standbyRetries());

            assertThrows(java.sql.SQLException.class, () -> adapter.streamComplete(allocator,
                    "SELECT CASE WHEN nextval('standby_seq') > 0 THEN error('boom') ELSE 1 END AS v",
                    views(), listener(), true));
            assertEquals(3, adapter.standbyRetries());

            // The plain path (streamSql) runs again too.
            assertThrows(java.sql.SQLException.class, () -> adapter.streamSql(allocator,
                    "SELECT error('" + STANDBY + "')", listener(), true));
            assertEquals(6, adapter.standbyRetries());
        }
    }

    /**
     * Verifies a refusal raised while DuckDB computes the stream, before its first batch (the
     * Arrow reader reports it as an IOException), runs the query again too.
     */
    @Test
    void refusedInTheStreamBeforeTheFirstBatchRunsAgain() throws Exception {
        // The aggregate reads every row before its one batch; the second run passes.
        String sql = "SELECT sum(CASE WHEN x = 3000000 AND nextval('standby_seq') <= 1 THEN error('" + STANDBY
                + "') ELSE x END) AS v FROM range(4000000) t(x)";
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(config(), Executors.newSingleThreadExecutor())) {
            adapter.standbyRetryDelaysMillis(1, 1, 1);
            FlightProducer.ServerStreamListener listener = listener();
            AtomicReference<String> value = capture(listener);
            adapter.streamComplete(allocator, sql, views(), listener, true);
            assertEquals("7999998000000", value.get());
            verify(listener, times(1)).start(any(VectorSchemaRoot.class));
            assertEquals(1, adapter.standbyRetries());
        }
    }

    /**
     * Verifies a query refused after a batch reached the client is never run again: the client
     * would get rows twice.
     */
    @Test
    void refusedAfterSendingIsNotRunAgain() throws Exception {
        String sql = "SELECT CASE WHEN x = 3000000 THEN error('" + STANDBY + "') ELSE x END AS v"
                + " FROM range(4000000) t(x)";
        try (RootAllocator allocator = new RootAllocator();
                DuckDbAdapter adapter = new DuckDbAdapter(config(), Executors.newSingleThreadExecutor())) {
            adapter.standbyRetryDelaysMillis(1, 1, 1);
            FlightProducer.ServerStreamListener listener = listener();
            AtomicInteger batches = new AtomicInteger();
            doAnswer(invocation -> {
                batches.incrementAndGet();
                return null;
            }).when(listener).putNext();
            Exception e = assertThrows(Exception.class, () -> adapter.streamComplete(
                    allocator, sql, views(), listener, true));
            assertTrue(DuckDbAdapter.standbyNameNode(e), String.valueOf(e.getMessage()));
            assertTrue(batches.get() > 0, "no batch was sent before the error");
            verify(listener, times(1)).start(any(VectorSchemaRoot.class));
            assertEquals(0, adapter.standbyRetries());
        }
    }

    /**
     * Builds a ready Flight listener mock.
     *
     * @return listener
     */
    private static FlightProducer.ServerStreamListener listener() {
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);
        when(listener.isReady()).thenReturn(true);
        return listener;
    }

    /**
     * Makes a listener remember the first value of the first batch.
     *
     * @param listener listener mock
     * @return the value, set on the first batch
     */
    private static AtomicReference<String> capture(FlightProducer.ServerStreamListener listener) {
        AtomicReference<VectorSchemaRoot> rootRef = new AtomicReference<>();
        AtomicReference<String> value = new AtomicReference<>();
        doAnswer(invocation -> {
            rootRef.set(invocation.getArgument(0));
            return null;
        }).when(listener).start(any(VectorSchemaRoot.class));
        doAnswer(invocation -> {
            if (value.get() == null) {
                value.set(String.valueOf(rootRef.get().getVector(0).getObject(0)));
            }
            return null;
        }).when(listener).putNext();
        return value;
    }

    /**
     * Builds catalog statements that create the sequence the queries count their runs with, on
     * one DuckDB thread so a stream sends its batches in order.
     *
     * @return view definitions
     */
    private static CatalogViews.Snapshot views() {
        List<String> statements = List.of("SET threads = 1", "CREATE SEQUENCE IF NOT EXISTS standby_seq");
        return new CatalogViews.Snapshot(1L, statements, statements, List.of("DROP SEQUENCE IF EXISTS standby_seq"));
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
}
