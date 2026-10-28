package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.adapters.QueryParameters;
import pro.surpin.data.arrowflight.server.services.ExecutionService;
import pro.surpin.data.arrowflight.spi.catalog.ArrowScan;

/**
 * Complete answers over a catalog table read as Arrow streams (P1 step 2 (b) part 2 (2)): every
 * reference of the table in the client's SQL reads a stream of its own with the columns the query
 * names, a query of that table alone hands its WHERE clause's simple conditions over as hints,
 * describing reads no stream, parameters are bound on the statement prepared for the run, the
 * streams are dropped afterwards, and a reference the server cannot replace fails instead of
 * reading nothing.
 */
@Tag("unit")
class ArrowStreamCompleteTest {

    private ArrowStreamScansTest.StreamingCatalog catalog;
    private ExecutorService ioPool;
    private DuckDbAdapter duck;
    private ExecutionService execution;
    private BufferAllocator allocator;

    @BeforeEach
    void open() throws Exception {
        catalog = new ArrowStreamScansTest.StreamingCatalog();
        CatalogTables tables = new CatalogTables(List.of(new CatalogTables.Entry(
                new SpiCatalog(catalog, null, "fake"), List.of("file:/data"), true)));
        tables.schemas(Set.of());
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        when(parquet.catalogTables()).thenReturn(tables);
        when(parquet.isCatalogSchema("s")).thenReturn(true);
        when(parquet.getSchemas(null)).thenReturn(Map.of("s", ""));
        when(parquet.getTables("s", null)).thenReturn(Map.of("t", ""));
        when(parquet.localFileInventory()).thenReturn(Map.of());
        ioPool = Executors.newCachedThreadPool();
        duck = new DuckDbAdapter(ArrowStreamExecutionTest.config(), ioPool);
        execution = new ExecutionService(parquet, duck, null, ArrowStreamExecutionTest.config(),
                ioPool);
        allocator = new RootAllocator();
    }

    @AfterEach
    void close() throws Exception {
        duck.close();
        ioPool.shutdownNow();
        allocator.close();
    }

    @Test
    void aQueryOfTheTableAloneReadsItsColumnsWithHints() throws Exception {
        String sql = "SELECT id, label FROM s.t WHERE p = 1 AND id >= 1 ORDER BY id";
        assertEquals(List.of("1|l1", "2|l2"), read(sql, null));
        ArrowScan scan = catalog.scans.get(0);
        assertEquals(List.of("id", "p", "label"), scan.columns());
        assertEquals(List.of(new ArrowScan.Predicate("p", ArrowScan.Op.EQ, List.of(1L)),
                new ArrowScan.Predicate("id", ArrowScan.Op.GE, List.of(1L))), scan.predicates());
        // The same text again reads a new stream: nothing is cached.
        assertEquals(List.of("1|l1", "2|l2"), read(sql, null));
        assertEquals(List.of("6"), read("SELECT count(*) FROM \"s\".\"t\" AS x", null));
        assertEquals(List.of("id"), catalog.scans.get(2).columns());
        assertEquals(0, streamViews());
        assertEquals(0, allocator.getAllocatedMemory());
    }

    @Test
    void everyReferenceReadsItsOwnStreamWithoutHints() throws Exception {
        String sql = "SELECT a.id, b.label FROM s.t a JOIN s.t AS b ON a.id = b.id AND a.p = b.p"
                + " WHERE a.p = 1 AND b.id < 2 ORDER BY a.id";
        assertEquals(List.of("0|l0", "1|l1"), read(sql, null));
        assertEquals(2, catalog.scans.size());
        for (ArrowScan scan : catalog.scans) {
            assertEquals(List.of("id", "p", "label"), scan.columns());
            // A filter of a join may not hold for each reference's rows.
            assertEquals(List.of(), scan.predicates());
        }
        // A subquery and a union: one stream per reference, still no hints.
        assertEquals(List.of("4"), read("SELECT count(*) FROM s.t WHERE id IN"
                + " (SELECT id FROM s.t WHERE p = 1 AND label = 'l1') OR p = 1", null));
        assertEquals(List.of(), catalog.scans.get(2).predicates());
        assertEquals(List.of("12"), read("SELECT count(*) FROM (SELECT id FROM s.t WHERE p = 1"
                + " UNION ALL SELECT id FROM s.t, (VALUES (1), (2)) v(k) WHERE k = 1"
                + " UNION ALL SELECT id FROM s.t WHERE p IS NULL)", null));
        assertEquals(7, catalog.scans.size());
        assertTrue(catalog.scans.stream().allMatch(scan -> scan.predicates().isEmpty()));
        assertEquals(0, streamViews());
    }

    @Test
    void parametersAreBoundOnTheStatementOfTheRun() throws Exception {
        byte[] values = parameters(1L);
        assertEquals(List.of("1|l1"), read("SELECT id, label FROM s.t WHERE id = ? AND p = 1",
                values));
        assertEquals(List.of("1|l1"), read("SELECT id, label FROM s.t WHERE id = ? AND p = 1",
                values));
        // A missing value is refused before any row is read.
        assertThrows(IllegalArgumentException.class,
                () -> read("SELECT id FROM s.t WHERE id = ? AND p = ?", values));
        assertEquals(0, streamViews());
        assertEquals(0, allocator.getAllocatedMemory());
    }

    @Test
    void theWholeRowReadsEveryColumn() throws Exception {
        assertEquals(6, read("SELECT t FROM s.t", null).size());
        assertEquals(List.of("id", "p", "label"), catalog.scans.get(0).columns());
        assertEquals(6, read("SELECT columns('^l.*') FROM s.t", null).size());
        assertEquals(List.of("id", "p", "label"), catalog.scans.get(1).columns());
    }

    @Test
    void describingReadsNoStream() throws Exception {
        Schema schema = execution.completeSchema(allocator,
                "SELECT a.id, b.p FROM s.t a JOIN s.t b ON a.id = b.id WHERE b.id = ?");
        assertEquals("id", schema.getFields().get(0).getName());
        assertEquals(new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true),
                schema.getFields().get(1).getType());
        assertEquals(1, execution.completeParameterSchema(
                "SELECT id FROM s.t WHERE id = ?").getFields().size());
        assertTrue(catalog.scans.isEmpty());
    }

    @Test
    void aReferenceWithoutItsSchemaFailsInsteadOfReadingNothing() {
        // Stream tables have no view: an unqualified name is a missing table, not an empty one.
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> read("SELECT count(*) FROM t", null));
        assertTrue(e.getMessage().contains("Catalog Error"), e.getMessage());
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

    private List<String> read(String sql, byte[] parameters) throws Exception {
        ArrowStreamExecutionTest.RowsListener listener = new ArrowStreamExecutionTest.RowsListener();
        execution.readComplete(allocator, sql, parameters, listener, true);
        return listener.rows;
    }

    private byte[] parameters(long value) throws Exception {
        try (BigIntVector vector = new BigIntVector("p0", allocator)) {
            vector.allocateNew(1);
            vector.set(0, value);
            vector.setValueCount(1);
            try (VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
                root.setRowCount(1);
                return QueryParameters.serialize(root);
            }
        }
    }

    private long streamViews() throws Exception {
        try (Statement statement = duck.connection().createStatement();
                ResultSet rs = statement.executeQuery("SELECT count(*) FROM duckdb_views()"
                        + " WHERE view_name LIKE '" + ArrowStreamScans.NAME_PREFIX + "%'")) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
