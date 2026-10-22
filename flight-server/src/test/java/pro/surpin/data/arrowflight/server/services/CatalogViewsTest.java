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
}
