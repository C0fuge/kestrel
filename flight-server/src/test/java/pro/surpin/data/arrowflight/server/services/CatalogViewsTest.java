package pro.surpin.data.arrowflight.server.services;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.spi.storage.DataFile;
import pro.surpin.data.arrowflight.server.storage.DataStore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests the views the complete-answer mode runs client SQL over. */
@Tag("unit")
class CatalogViewsTest {

    /** Verifies unsigned integers widen to a signed type that holds every value. */
    @Test
    void widensUnsignedIntegers() {
        assertEquals("SMALLINT", CatalogViews.widerSignedType("UTINYINT"));
        assertEquals("INTEGER", CatalogViews.widerSignedType("USMALLINT"));
        assertEquals("BIGINT", CatalogViews.widerSignedType("UINTEGER"));
        assertEquals("DECIMAL(20,0)", CatalogViews.widerSignedType("ubigint"));
        assertNull(CatalogViews.widerSignedType("BIGINT"));
        assertNull(CatalogViews.widerSignedType("DECIMAL(18,2)"));
    }

    /** Verifies a table name used by one schema only also gets a view without its schema. */
    @Test
    void tableNamesUsedByOneSchemaWorkWithoutTheSchema(@TempDir java.nio.file.Path dir) throws Exception {
        java.nio.file.Path file = dir.resolve("f.parquet");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT 1::UTINYINT AS x) TO '" + file + "' (FORMAT parquet)");
        }
        DataFile status = new DataFile("f.parquet", file.toUri().toString(),
                java.nio.file.Files.size(file), 0, null);
        DataStore store = mock(DataStore.class);
        when(store.duckDbUri(any(DataFile.class))).thenReturn(file.toString());
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        when(parquet.dataStore()).thenReturn(store);
        when(parquet.getSchemas(null)).thenReturn(Map.of("a", "/a", "b", "/b"));
        when(parquet.getTables(eq("a"), any())).thenReturn(Map.of("only", "/a/only", "Both", "/a/Both"));
        when(parquet.getTables(eq("b"), any())).thenReturn(Map.of("both", "/b/both"));
        when(parquet.tableFiles(any(), any())).thenReturn(List.of(status));
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (DuckDbAdapter duck = new DuckDbAdapter(config(), ioPool)) {
            CatalogViews.Snapshot views = new CatalogViews(parquet, duck, true).current();

            assertTrue(views.dataStatements().contains("CREATE OR REPLACE VIEW main.\"only\" AS SELECT * FROM \"a\".\"only\""),
                    views.dataStatements().toString());
            assertTrue(views.dataStatements().stream().noneMatch(sql -> sql.toLowerCase().contains("main.\"both\"")));
            assertTrue(views.emptyStatements().contains("CREATE OR REPLACE VIEW main.\"only\" AS SELECT * FROM \"a\".\"only\""));
            assertEquals("DROP VIEW IF EXISTS main.\"only\"", views.dropStatements().get(0));
            assertTrue(views.dropStatements().contains("DROP SCHEMA IF EXISTS \"b\" CASCADE"));
            assertTrue(views.dataStatements().stream().anyMatch(sql -> sql.contains("CAST(\"x\" AS SMALLINT)")));
        } finally {
            ioPool.shutdownNow();
        }
    }

    /**
     * Verifies a new data version of the store (a lakeFS branch moved to another commit) rebuilds
     * the views at once, not after the minute, and that each rebuild lists in one read scope.
     */
    @Test
    void newDataVersionRefreshesTheViewsAtOnceInOneReadScope(@TempDir java.nio.file.Path dir)
            throws Exception {
        java.nio.file.Path one = dir.resolve("c1.parquet");
        java.nio.file.Path two = dir.resolve("c2.parquet");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT 1 AS x) TO '" + one + "' (FORMAT parquet)");
            s.execute("COPY (SELECT 2 AS x) TO '" + two + "' (FORMAT parquet)");
        }
        AtomicReference<String> version = new AtomicReference<>("c1");
        AtomicInteger opened = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();
        DataStore store = mock(DataStore.class);
        when(store.dataVersion()).thenAnswer(inv -> version.get());
        when(store.readScope()).thenAnswer(inv -> {
            opened.incrementAndGet();
            return (DataStore.ReadScope) closed::incrementAndGet;
        });
        when(store.duckDbUri(any(DataFile.class))).thenAnswer(inv -> java.nio.file.Path.of(
                java.net.URI.create(inv.<DataFile>getArgument(0).location())).toString());
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        when(parquet.dataStore()).thenReturn(store);
        when(parquet.getSchemas(null)).thenReturn(Map.of("a", "/a"));
        when(parquet.getTables(eq("a"), any())).thenReturn(Map.of("t", "/a/t"));
        when(parquet.tableFiles(any(), any())).thenAnswer(inv -> {
            java.nio.file.Path file = "c1".equals(version.get()) ? one : two;
            return List.of(new DataFile("@" + version.get() + "/a/t/f.parquet",
                    file.toUri().toString(), java.nio.file.Files.size(file), 0, null));
        });
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (DuckDbAdapter duck = new DuckDbAdapter(config(), ioPool)) {
            CatalogViews views = new CatalogViews(parquet, duck, true);
            CatalogViews.Snapshot first = views.current();
            assertSame(first, views.current());
            assertEquals(1, opened.get());
            assertTrue(first.dataStatements().toString().contains(one.toString()));

            version.set("c2");
            CatalogViews.Snapshot second = views.current();
            assertNotEquals(first.version(), second.version());
            assertTrue(second.dataStatements().toString().contains(two.toString()),
                    second.dataStatements().toString());
            assertEquals(2, opened.get());
            assertEquals(2, closed.get());
        } finally {
            ioPool.shutdownNow();
        }
    }

    /**
     * Verifies that on HDFS the views are described from footers read through the store's Java
     * client (DuckDB itself could not read the hdfs:// file here), and that a table whose first
     * file cannot be read is left out with its reason while every other table keeps its view.
     */
    @Test
    void hdfsTablesAreDescribedFromJavaFootersAndOneBadTableDoesNotRefuseTheOthers(
            @TempDir java.nio.file.Path dir) throws Exception {
        java.nio.file.Path good = dir.resolve("good.parquet");
        java.nio.file.Path bad = dir.resolve("bad.parquet");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT 1::UTINYINT AS x, 2.5::DECIMAL(9,2) AS d) TO '" + good
                    + "' (FORMAT parquet)");
        }
        java.nio.file.Files.writeString(bad, "not parquet");
        DataFile goodFile = new DataFile("tpch/good/k=7/f.parquet", "hdfs://ns/data/tpch/good/k=7/f.parquet",
                java.nio.file.Files.size(good), 0, null);
        DataFile badFile = new DataFile("tpch/bad/f.parquet", "hdfs://ns/data/tpch/bad/f.parquet",
                java.nio.file.Files.size(bad), 0, null);
        DataStore store = mock(DataStore.class);
        when(store.kind()).thenReturn("hdfs");
        when(store.duckDbUri(any(DataFile.class))).thenAnswer(inv -> inv.<DataFile>getArgument(0).location());
        when(store.inputFile(goodFile)).thenReturn(new org.apache.parquet.io.LocalInputFile(good));
        when(store.inputFile(badFile)).thenReturn(new org.apache.parquet.io.LocalInputFile(bad));
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        when(parquet.dataStore()).thenReturn(store);
        when(parquet.getSchemas(null)).thenReturn(Map.of("tpch", "/tpch"));
        when(parquet.getTables(eq("tpch"), any())).thenReturn(Map.of("good", "/tpch/good", "bad", "/tpch/bad"));
        when(parquet.tableFiles("tpch", "good")).thenReturn(List.of(goodFile));
        when(parquet.tableFiles("tpch", "bad")).thenReturn(List.of(badFile));
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (DuckDbAdapter duck = new DuckDbAdapter(config(), ioPool)) {
            CatalogViews catalogViews = new CatalogViews(parquet, duck, true);
            CatalogViews.Snapshot views = catalogViews.current();

            String data = views.dataStatements().toString();
            assertTrue(data.contains("CREATE OR REPLACE VIEW \"tpch\".\"good\" AS SELECT CAST(\"x\" AS SMALLINT)"
                    + " AS \"x\", \"d\", \"k\" FROM read_parquet(['hdfs://ns/data/tpch/good/k=7/f.parquet'])"), data);
            assertTrue(views.emptyStatements().toString().contains("CAST(NULL AS DECIMAL(9,2)) AS \"d\""),
                    views.emptyStatements().toString());
            assertTrue(data.contains("main.\"good\""), data);
            assertTrue(!data.contains("\"bad\""), data);
            String reason = catalogViews.unavailableTable("SELECT * FROM tpch.bad",
                    "Catalog Error: Table with name bad does not exist!");
            assertTrue(reason != null && reason.contains("tpch.bad") && reason.contains("hdfs://ns/data/tpch/bad/f.parquet"),
                    String.valueOf(reason));
            assertNull(catalogViews.unavailableTable("SELECT * FROM tpch.other",
                    "Catalog Error: Table with name other does not exist!"));
            try (java.util.stream.Stream<java.nio.file.Path> left = java.nio.file.Files.list(
                    java.nio.file.Files.createDirectories(duck.footerStubRoot()))) {
                assertEquals(0, left.count(), "footer stubs are removed after the describe");
            }
        } finally {
            ioPool.shutdownNow();
        }
    }

    /**
     * Verifies that with whole-file reads (SASL DataNodes) a view is bound on a file this node
     * reads, and a table this node holds no file of whole gets a typed view that fails with the
     * reason when read, instead of DuckDB failing every query on a remote footer (item 29).
     */
    @Test
    void wholeFileReadsBindViewsOnReadableFilesOnly(@TempDir java.nio.file.Path dir) throws Exception {
        java.nio.file.Path parquetFile = dir.resolve("f.parquet");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT 1::INTEGER AS x) TO '" + parquetFile + "' (FORMAT parquet)");
        }
        long size = java.nio.file.Files.size(parquetFile);
        DataFile remote = new DataFile("s/mixed/a.parquet", "hdfs://ns/data/s/mixed/a.parquet", size, 0,
                java.util.Set.of("dn2"));
        DataFile local = new DataFile("s/mixed/b.parquet", "hdfs://ns/data/s/mixed/b.parquet", size, 0,
                java.util.Set.of("dn1"));
        DataFile far = new DataFile("s/far/c.parquet", "hdfs://ns/data/s/far/c.parquet", size, 0,
                java.util.Set.of("dn3"));
        DataStore store = mock(DataStore.class);
        when(store.kind()).thenReturn("hdfs");
        when(store.wholeFileReadsOnly()).thenReturn(true);
        when(store.readableHere(any(DataFile.class))).thenAnswer(inv -> inv.<DataFile>getArgument(0) == local);
        when(store.duckDbUri(any(DataFile.class))).thenAnswer(inv -> inv.<DataFile>getArgument(0).location());
        when(store.inputFile(any(DataFile.class))).thenReturn(new org.apache.parquet.io.LocalInputFile(parquetFile));
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        when(parquet.dataStore()).thenReturn(store);
        when(parquet.getSchemas(null)).thenReturn(Map.of("s", "/s"));
        when(parquet.getTables(eq("s"), any())).thenReturn(Map.of("mixed", "/s/mixed", "far", "/s/far"));
        when(parquet.tableFiles("s", "mixed")).thenReturn(List.of(remote, local));
        when(parquet.tableFiles("s", "far")).thenReturn(List.of(far));
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (DuckDbAdapter duck = new DuckDbAdapter(config(), ioPool)) {
            CatalogViews.Snapshot views = new CatalogViews(parquet, duck, true).current();

            String data = views.dataStatements().toString();
            assertTrue(data.contains("read_parquet(['hdfs://ns/data/s/mixed/b.parquet', "
                    + "'hdfs://ns/data/s/mixed/a.parquet'])"), data);
            String farView = views.dataStatements().stream()
                    .filter(v -> v.startsWith("CREATE OR REPLACE VIEW \"s\".\"far\"")).findFirst().orElseThrow();
            assertTrue(!farView.contains("read_parquet"), farView);
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                    Statement s = conn.createStatement()) {
                s.execute("CREATE SCHEMA s");
                s.execute(farView);
                java.sql.SQLException e = org.junit.jupiter.api.Assertions.assertThrows(
                        java.sql.SQLException.class, () -> s.executeQuery("SELECT count(*) FROM s.far").next());
                assertTrue(e.getMessage().contains("SASL") && e.getMessage().contains("s/far/c.parquet"),
                        e.getMessage());
                try (Statement d = conn.createStatement();
                        java.sql.ResultSet columns = d.executeQuery("DESCRIBE s.far")) {
                    assertTrue(columns.next());
                    assertEquals("x", columns.getString("column_name"));
                    assertEquals("INTEGER", columns.getString("column_type"));
                }
            }
        } finally {
            ioPool.shutdownNow();
        }
    }

    /**
     * Builds a server configuration for the test.
     *
     * @return configuration
     */
    private static AppConfig config() {
        return new AppConfig(
                3, 100, 1, 131072, 0, 1, 1,
                null, false, null, null,
                null, null, null, false, 1048576, 67108864, 60000L, null, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 64, null, null, "complete", true, true);
    }

    /**
     * Item 31: the view of a table this node cannot read fails every query that reads it. With
     * typed NULL columns DuckDB folded a join on them to an empty result and answered 0 rows.
     */
    @Test
    void unreadableTableViewFailsJoinsInsteadOfAnsweringNoRows() throws Exception {
        String view = CatalogViews.unreadableView("\"spread\".\"orders_sp\"",
                List.of("\"o_custkey\"", "\"o_comment\""), List.of("BIGINT", "VARCHAR"),
                "UNREADABLE-HERE");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = conn.createStatement()) {
            st.execute("CREATE SCHEMA spread");
            st.execute(view);
            st.execute("CREATE TABLE customer AS SELECT range AS c_custkey, 'x' AS n FROM range(100)");
            try (java.sql.ResultSet rs = st.executeQuery("DESCRIBE SELECT * FROM spread.orders_sp")) {
                assertTrue(rs.next());
                assertEquals("BIGINT", rs.getString("column_type"));
                assertTrue(rs.next());
                assertEquals("VARCHAR", rs.getString("column_type"));
            }
            for (String query : List.of(
                    "SELECT count(*) FROM spread.orders_sp",
                    "SELECT count(o_comment) FROM spread.orders_sp",
                    "SELECT count(*) FROM spread.orders_sp o, customer c WHERE o.o_custkey = c.c_custkey",
                    "SELECT count(*) FROM spread.orders_sp JOIN customer ON o_custkey = c_custkey"
                            + " WHERE c_custkey > 5",
                    "SELECT count(*) FROM customer LEFT JOIN spread.orders_sp ON o_custkey = c_custkey",
                    "SELECT count(*) FROM customer WHERE EXISTS (SELECT 1 FROM spread.orders_sp"
                            + " WHERE o_comment = n)",
                    "SELECT count(*) FROM customer WHERE c_custkey NOT IN (SELECT o_custkey FROM"
                            + " spread.orders_sp)",
                    "SELECT count(*) FROM (SELECT o_comment FROM spread.orders_sp) t, customer"
                            + " WHERE t.o_comment = customer.n")) {
                try (Statement each = conn.createStatement()) {
                    java.sql.SQLException e = org.junit.jupiter.api.Assertions.assertThrows(
                            java.sql.SQLException.class, () -> each.executeQuery(query).next(), query);
                    assertTrue(e.getMessage().contains("UNREADABLE-HERE"), query + ": " + e.getMessage());
                }
            }
        }
    }

    /**
     * Item 36 (4): with node-local copies a table no file of which this node holds whole is
     * still answered, so its log line says so instead of announcing failing complete answers.
     */
    @Test
    void unreadableTableNoteNamesCopiesWhenTheyAnswer() {
        assertTrue(CatalogViews.unreadableTableNote(true).contains("local copies"));
        assertTrue(!CatalogViews.unreadableTableNote(true).contains("fails"));
        assertTrue(CatalogViews.unreadableTableNote(false).contains("a complete answer here fails"));
    }
}
