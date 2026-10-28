package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.model.HandleState;
import pro.surpin.data.arrowflight.server.services.ClusterService;
import pro.surpin.data.arrowflight.server.services.ExecutionService;
import pro.surpin.data.arrowflight.server.services.ParquetQueryParser;
import pro.surpin.data.arrowflight.server.services.QueryPlanner;
import pro.surpin.data.arrowflight.spi.catalog.ArrowScan;

/**
 * Partial answers over a catalog table read as Arrow streams (P1 step 2 (b) part 2 (1)): the
 * planner ships the files with their partition values, every endpoint asks the catalog for the
 * columns the query names with the WHERE clause's simple conditions as hints, DuckDB answers over
 * a stream of its own each time (the same text twice on one thread works: nothing is cached), the
 * stream is dropped afterwards, and the answers are those of the data.
 */
@Tag("unit")
class ArrowStreamExecutionTest {

    private ArrowStreamScansTest.StreamingCatalog catalog;
    private CatalogTables tables;
    private ParquetAdapter parquet;
    private ExecutorService ioPool;
    private DuckDbAdapter duck;
    private ExecutionService execution;
    private BufferAllocator allocator;

    @BeforeEach
    void open() throws Exception {
        catalog = new ArrowStreamScansTest.StreamingCatalog();
        tables = new CatalogTables(List.of(new CatalogTables.Entry(
                new SpiCatalog(catalog, null, "fake"), List.of("file:/data"), true)));
        tables.schemas(Set.of());
        parquet = mock(ParquetAdapter.class);
        when(parquet.catalogTables()).thenReturn(tables);
        when(parquet.isCatalogSchema("s")).thenReturn(true);
        when(parquet.localFileInventory()).thenReturn(Map.of());
        ioPool = Executors.newCachedThreadPool();
        duck = new DuckDbAdapter(config(), ioPool);
        execution = new ExecutionService(parquet, duck, null, config(), ioPool);
        allocator = new RootAllocator();
    }

    @AfterEach
    void close() throws Exception {
        duck.close();
        ioPool.shutdownNow();
        allocator.close();
    }

    @Test
    void wholeSnapshotIsAnsweredOverItsStreamWithColumnsAndHints() throws Exception {
        String sql = "SELECT id, label FROM s.t WHERE p = 1 AND id >= 1 ORDER BY id";
        assertEquals(List.of("1|l1", "2|l2"), read(sql, null));
        ArrowScan scan = catalog.scans.get(0);
        assertEquals(List.of("id", "p", "label"), scan.columns());
        assertEquals(List.of(new ArrowScan.Predicate("p", ArrowScan.Op.EQ, List.of(1L)),
                new ArrowScan.Predicate("id", ArrowScan.Op.GE, List.of(1L))), scan.predicates());
        // The same text again on this thread reads a new stream (no cached statement).
        assertEquals(List.of("1|l1", "2|l2"), read(sql, null));
        assertEquals(2, catalog.scans.size());
        assertEquals(0, streamViews());
    }

    @Test
    void aggregatesAndCountStarReadOneColumnAtLeast() throws Exception {
        assertEquals(List.of("6"), read("SELECT count(*) FROM s.t", null));
        assertEquals(List.of("id"), catalog.scans.get(0).columns());
        assertEquals(List.of("1|3", "null|3"), read("SELECT p, count(*) AS n FROM s.t GROUP BY p"
                + " ORDER BY p", null));
        assertEquals(List.of("p"), catalog.scans.get(1).columns());
        assertEquals(List.of("3"), read("SELECT sum(id) FROM s.t WHERE p IS NULL", null));
        assertEquals(0, streamViews());
    }

    @Test
    void ticketsShipTheFilesWithTheirPartitionValues() throws Exception {
        ClusterService cluster = mock(ClusterService.class);
        String self = "grpc://127.0.0.1:32010";
        when(cluster.allServerLoads()).thenReturn(Map.of(self, 0L));
        when(cluster.filterLiveServers(anySet())).thenReturn(Set.of(self));
        when(cluster.hasFileInventory(any())).thenReturn(true);
        when(cluster.serverUri()).thenReturn(self);
        ArgumentCaptor<HandleState> states = ArgumentCaptor.forClass(HandleState.class);
        when(cluster.createEndpointHandle(states.capture())).thenReturn(new byte[] {1});
        new QueryPlanner(parquet, cluster).plan("SELECT id, p FROM s.t WHERE p = 1");
        List<String> entries = new ArrayList<>();
        for (HandleState state : states.getAllValues()) {
            entries.addAll(List.of(state.filePaths()));
        }
        // The partition pruner left the p IS NULL file out.
        assertEquals(1, entries.size(), entries.toString());
        CatalogFileKey key = CatalogFileKey.decode(entries.get(0));
        assertTrue(key.carriesPartitionValues());
        assertEquals("1", key.partitionValues().get("p"));
        assertEquals(List.of("0|1", "1|1", "2|1"), read("SELECT id, p FROM s.t WHERE p = 1"
                + " ORDER BY id", entries.toArray(new String[0])));
        // A file outside the catalog's roots is refused, not read.
        String outside = new CatalogFileKey("1", "file:/etc/x.orc", Map.of("p", "1")).encode();
        assertThrows(IllegalArgumentException.class,
                () -> read("SELECT id FROM s.t", new String[] {outside}));
        assertEquals(0, streamViews());
        assertEquals(0, allocator.getAllocatedMemory());
    }

    @Test
    void describingReadsNoStream() throws Exception {
        Schema schema = execution.travelledSchema(allocator,
                ParquetQueryParser.parse("SELECT id, p FROM s.t"),
                tables.current("s", "t").orElseThrow());
        assertEquals("id", schema.getFields().get(0).getName());
        assertEquals("p", schema.getFields().get(1).getName());
        assertEquals(new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true),
                schema.getFields().get(1).getType());
        assertTrue(catalog.scans.isEmpty());
    }

    @Test
    void aFailingCatalogFailsTheQueryAndLeavesNothingBehind() throws Exception {
        catalog.fail = true;
        Exception e = assertThrows(Exception.class, () -> read("SELECT id FROM s.t", null));
        assertTrue(String.valueOf(e.getMessage()).contains("cannot open"), e.toString());
        assertEquals(0, streamViews());
        assertEquals(0, allocator.getAllocatedMemory());
        catalog.fail = false;
        assertEquals(List.of("6"), read("SELECT count(id) FROM s.t", null));
    }

    private List<String> read(String sql, String[] ticket) throws Exception {
        RowsListener listener = new RowsListener();
        execution.readParquet(allocator, sql, ticket, listener, true);
        return listener.rows;
    }

    private long streamViews() throws Exception {
        try (Statement statement = duck.connection().createStatement();
                ResultSet rs = statement.executeQuery("SELECT count(*) FROM duckdb_views()"
                        + " WHERE view_name LIKE '" + ArrowStreamScans.NAME_PREFIX + "%'")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    static AppConfig config() {
        return new AppConfig(
                3, 100, 1, 131072, 0, 1, 1,
                null, false, null, null,
                null, null, null, false, 1048576, 67108864, 60000L, null, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 64, null, null, "complete", true, true);
    }

    /** Collects the rows an execution streams, columns separated by |. */
    static final class RowsListener implements FlightProducer.ServerStreamListener {
        final List<String> rows = new ArrayList<>();
        private VectorSchemaRoot root;

        @Override
        public void start(VectorSchemaRoot r, DictionaryProvider p, IpcOption o) {
            this.root = r;
        }

        @Override
        public void putNext() {
            collect();
        }

        @Override
        public void putNext(ArrowBuf metadata) {
            collect();
        }

        private void collect() {
            for (int row = 0; row < root.getRowCount(); row++) {
                StringBuilder text = new StringBuilder();
                for (int column = 0; column < root.getFieldVectors().size(); column++) {
                    text.append(column > 0 ? "|" : "")
                            .append(root.getFieldVectors().get(column).getObject(row));
                }
                rows.add(text.toString());
            }
        }

        @Override
        public void putMetadata(ArrowBuf metadata) {
            // Metadata frames carry no rows.
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void setOnReadyHandler(Runnable handler) {
            // Always ready.
        }

        @Override
        public void setOnCancelHandler(Runnable handler) {
            // Never cancelled.
        }

        @Override
        public void error(Throwable ex) {
            throw new IllegalStateException(ex);
        }

        @Override
        public void completed() {
            // The rows are read after the call returns.
        }
    }
}
