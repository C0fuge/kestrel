package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.services.CatalogViews;
import pro.surpin.data.arrowflight.server.services.ExecutionService;
import pro.surpin.data.arrowflight.spi.catalog.ArrowScan;
import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.CatalogView;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;

/**
 * Complete answers over views of a catalog (P4 step 2 (c) views, part 3): a view named with its
 * schema reads its query, also inside joins, subqueries and other views, the tables it reads are
 * listed for the access check, a view named like a table or naming an undeclared table is not
 * shown, and views read at an earlier snapshot or nested without end are refused.
 */
@Tag("unit")
class CatalogViewQueryTest {

    private static final String ONE = "SELECT CAST(\"_v0\" AS BIGINT) AS \"id\","
            + " CAST(\"_v1\" AS VARCHAR) AS \"label\" FROM (SELECT \"id\", \"label\""
            + " FROM \"s\".\"t\" WHERE \"p\" = 1) AS \"_view\"(\"_v0\", \"_v1\")";

    private ViewCatalog catalog;
    private ExecutorService ioPool;
    private DuckDbAdapter duck;
    private ExecutionService execution;
    private BufferAllocator allocator;

    @BeforeEach
    void open() throws Exception {
        catalog = new ViewCatalog();
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
    void aViewReadsItsQueryWhereverItIsNamed() throws Exception {
        assertEquals(List.of("0|l0", "1|l1", "2|l2"),
                read("SELECT id, label FROM s.v_one ORDER BY id"));
        assertEquals(List.of("1"), read("SELECT x.id FROM \"s\".\"V_ONE\" AS x WHERE x.label = 'l1'"));
        // Joined with its own table, in a subquery and in a comma list.
        assertEquals(List.of("6"), read("SELECT count(*) FROM s.t JOIN s.v_one v ON t.id = v.id"));
        assertEquals(List.of("6"), read("SELECT count(*) FROM s.t WHERE id IN"
                + " (SELECT id FROM s.v_one)"));
        assertEquals(List.of("9"), read("SELECT count(*) FROM s.v_one a, s.v_one b"));
        // A view over a view.
        assertEquals(List.of("3"), read("SELECT n FROM s.v_two"));
        // The stream under the view read only the columns the query names.
        assertEquals(List.of("id", "p", "label"), catalog.streaming.scans.get(0).columns());
        assertEquals(0, allocator.getAllocatedMemory());
    }

    @Test
    void theSourcesOfEveryViewAreListed() throws Exception {
        CatalogViews.Expanded none = execution.catalogViews("SELECT count(*) FROM s.t");
        assertEquals("SELECT count(*) FROM s.t", none.sql());
        assertEquals(List.of(), none.sources());
        CatalogViews.Expanded two = execution.catalogViews(
                "SELECT n FROM s.v_two UNION ALL SELECT id FROM s.v_one");
        assertEquals(List.of(new TableName("s", "v_one"), new TableName("s", "t")),
                two.sources());
        assertTrue(two.sql().contains("FROM \"s\".\"t\""), two.sql());
        // Only the aliases keep the view names.
        assertTrue(!two.sql().contains("s.v_") && !two.sql().contains("\"s\".\"v_"), two.sql());
        // Expanding the expanded text changes nothing.
        CatalogViews.Expanded again = execution.catalogViews(two.sql());
        assertEquals(two.sql(), again.sql());
        assertEquals(List.of(), again.sources());
        // A name without its schema is no view reference.
        assertEquals(List.of(), execution.catalogViews("SELECT * FROM v_one").sources());
    }

    @Test
    void describingAViewGivesItsDeclaredColumns() {
        Schema schema = execution.completeSchema(allocator, "SELECT * FROM s.v_one");
        assertEquals(List.of("id", "label"), schema.getFields().stream()
                .map(org.apache.arrow.vector.types.pojo.Field::getName).toList());
        assertEquals(new ArrowType.Int(64, true), schema.getFields().get(0).getType());
        assertTrue(catalog.streaming.scans.isEmpty());
    }

    @Test
    void theTableWinsAndUndeclaredSourcesHideTheView() throws Exception {
        // A view named like the table is left out: the table answers.
        assertEquals(List.of("6"), read("SELECT count(*) FROM s.t"));
        assertEquals(List.of(), execution.catalogViews("SELECT * FROM s.t").sources());
        // A view whose query reads a table it does not declare is not shown at all.
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> read("SELECT * FROM s.v_hidden"));
        assertTrue(e.getMessage().contains("Catalog Error"), e.getMessage());
    }

    @Test
    void viewsReadAtAnEarlierSnapshotOrWithoutEndAreRefused() {
        IllegalArgumentException travel = assertThrows(IllegalArgumentException.class,
                () -> execution.catalogViews("SELECT * FROM s.v_one FOR VERSION AS OF 1"));
        assertTrue(travel.getMessage().contains("not supported for the view s.v_one"),
                travel.getMessage());
        IllegalArgumentException loop = assertThrows(IllegalArgumentException.class,
                () -> execution.catalogViews("SELECT * FROM s.v_loop"));
        assertTrue(loop.getMessage().contains("nested deeper than"), loop.getMessage());
    }

    private List<String> read(String sql) throws Exception {
        ArrowStreamExecutionTest.RowsListener listener = new ArrowStreamExecutionTest.RowsListener();
        execution.readComplete(allocator, sql, null, listener, true);
        return listener.rows;
    }

    /** The streaming catalog of {@link ArrowStreamScansTest} with views over its table. */
    static final class ViewCatalog implements Catalog {
        final ArrowStreamScansTest.StreamingCatalog streaming =
                new ArrowStreamScansTest.StreamingCatalog();

        CatalogView one() {
            return new CatalogView("v_one", ONE, List.of(new TableName("s", "t")),
                    List.of(new ColumnSpec("id", "BIGINT", true, null),
                            new ColumnSpec("label", "VARCHAR", true, null)), "1");
        }

        CatalogView hidden() {
            return new CatalogView("v_hidden", "SELECT \"id\" FROM \"s\".\"t\"",
                    List.of(new TableName("s", "v_one")),
                    List.of(new ColumnSpec("id", "BIGINT", true, null)), "3");
        }

        @Override
        public List<String> schemas() {
            return streaming.schemas();
        }

        @Override
        public List<String> tables(String schema) {
            return streaming.tables(schema);
        }

        @Override
        public List<CatalogView> views(String schema) {
            List<CatalogView> views = new ArrayList<>();
            views.add(one());
            views.add(new CatalogView("v_two", "SELECT count(*) AS \"n\" FROM \"s\".\"v_one\"",
                    List.of(new TableName("s", "v_one")),
                    List.of(new ColumnSpec("n", "BIGINT", true, null)), "2"));
            views.add(hidden());
            views.add(new CatalogView("t", "SELECT 1 AS \"x\" FROM \"s\".\"t\"",
                    List.of(new TableName("s", "t")),
                    List.of(new ColumnSpec("x", "INTEGER", true, null)), "4"));
            views.add(new CatalogView("v_loop", "SELECT * FROM \"s\".\"v_loop\"",
                    List.of(new TableName("s", "v_loop")),
                    List.of(new ColumnSpec("x", "INTEGER", true, null)), "5"));
            return views;
        }

        @Override
        public Optional<TableSnapshot> current(TableName table) {
            return streaming.current(table);
        }

        @Override
        public List<String> readableRoots() {
            return streaming.readableRoots();
        }

        @Override
        public void exportArrowScan(TableSnapshot snapshot, List<TableFile> files, ArrowScan scan,
                long streamAddress, Object allocator) throws IOException {
            streaming.exportArrowScan(snapshot, files, scan, streamAddress, allocator);
        }
    }
}
