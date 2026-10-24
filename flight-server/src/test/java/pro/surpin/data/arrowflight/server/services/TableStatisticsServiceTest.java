package pro.surpin.data.arrowflight.server.services;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.catalog.CatalogTables;
import pro.surpin.data.arrowflight.server.storage.DataStore;
import pro.surpin.data.arrowflight.spi.catalog.DeleteFile;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.storage.DataFile;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests the table statistics of the statistics action against DuckDB's own answers. */
@Tag("unit")
class TableStatisticsServiceTest {

    private static final String SELECT = "SELECT range::INTEGER AS id,"
            + " (range * 1.25)::DECIMAL(10,2) AS price,"
            + " (DATE '2024-01-01' + range::INTEGER) AS day,"
            + " CASE WHEN range % 3 = 0 THEN NULL ELSE range * 0.5 END AS score,"
            + " 'n' || range AS name";

    @TempDir
    Path dir;

    /** Reports rows, bounds and null counts that match DuckDB over the same files. */
    @Test
    void directoryTableMatchesDuckDb() throws Exception {
        List<DataFile> files = writeFiles();
        TableStatisticsService service = new TableStatisticsService(
                directoryAdapter(files, true), mock(ClusterService.class));

        TableStatisticsService.TableStatistics stats = service.statistics("s", "t", null);

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement();
                ResultSet r = s.executeQuery("SELECT count(*), min(id), max(id),"
                        + " min(price)::DOUBLE, max(price)::DOUBLE,"
                        + " (min(day) - DATE '1970-01-01'), (max(day) - DATE '1970-01-01'),"
                        + " min(score), max(score), count(*) - count(score)"
                        + " FROM read_parquet('" + dir + "/*.parquet')")) {
            r.next();
            assertEquals(r.getLong(1), stats.rows());
            assertTrue(stats.rowsExact());
            assertEquals(2, stats.files());
            assertEquals(files.get(0).size() + files.get(1).size(), stats.bytes());
            assertBounds(stats, "id", r.getDouble(2), r.getDouble(3));
            assertBounds(stats, "price", r.getDouble(4), r.getDouble(5));
            assertBounds(stats, "day", r.getDouble(6), r.getDouble(7));
            assertBounds(stats, "score", r.getDouble(8), r.getDouble(9));
            assertEquals(r.getLong(10), stats.columns().get("score").nulls());
        }
        TableStatisticsService.ColumnStatistics name = stats.columns().get("name");
        assertNull(name.min());
        assertNull(name.max());
        assertEquals(0, name.nulls());
        assertTrue(name.bytes() > 0);
        assertNull(name.ndv());
        assertEquals(Set.of("id", "price", "day", "score", "name"), stats.columns().keySet());
    }

    /** Reports only the requested columns, matched ignoring case, and skips unknown ones. */
    @Test
    void reportsRequestedColumnsOnly() throws Exception {
        TableStatisticsService service = new TableStatisticsService(
                directoryAdapter(writeFiles(), true), mock(ClusterService.class));

        TableStatisticsService.TableStatistics stats =
                service.statistics("s", "t", List.of("ID", "missing", "name"));

        assertEquals(List.of("id", "name"), List.copyOf(stats.columns().keySet()));
    }

    /** Reads the footers once per file set and again when the files change. */
    @Test
    void cachesFootersPerFileSet() throws Exception {
        List<DataFile> files = writeFiles();
        ParquetAdapter adapter = directoryAdapter(files, true);
        TableStatisticsService service = new TableStatisticsService(adapter, mock(ClusterService.class));

        long all = service.statistics("s", "t", null).rows();
        when(adapter.tableFiles("s", "t")).thenReturn(files.subList(0, 1));
        long first = service.statistics("s", "t", null).rows();

        assertEquals(100, all);
        assertEquals(40, first);
        assertEquals(1, service.cachedTables());
    }

    /** Refuses statistics a node could only give for its own files of a local-disk cluster. */
    @Test
    void refusesLocalDisksOfSeveralNodes() throws Exception {
        ClusterService cluster = mock(ClusterService.class);
        when(cluster.allServerLoads()).thenReturn(Map.of("a", 0L, "b", 0L));
        when(cluster.filterLiveServers(anySet())).thenReturn(Set.of("a", "b"));
        TableStatisticsService service = new TableStatisticsService(
                directoryAdapter(writeFiles(), false), cluster);

        assertThrows(IllegalStateException.class, () -> service.statistics("s", "t", null));

        when(cluster.filterLiveServers(anySet())).thenReturn(Set.of("a"));
        assertEquals(100, service.statistics("s", "t", null).rows());
    }

    /** Tells a missing table and invalid names apart from read failures. */
    @Test
    void missingTableAndInvalidNames() throws Exception {
        ParquetAdapter adapter = directoryAdapter(List.of(), true);
        TableStatisticsService service = new TableStatisticsService(adapter, mock(ClusterService.class));

        assertThrows(FileNotFoundException.class, () -> service.statistics("s", "t", null));
        assertThrows(IllegalArgumentException.class, () -> service.statistics("s", "../t", null));
        assertThrows(IllegalArgumentException.class, () -> service.statistics("", "t", null));
    }

    /** Marks the row count of a catalog snapshot with delete files as an upper bound. */
    @Test
    void catalogTableWithDeletesIsAnUpperBound() throws Exception {
        TableFile plain = new TableFile("s3://w/a.parquet", 100, 0, Set.of(), Map.of(), 10, List.of());
        TableFile deleted = new TableFile("s3://w/b.parquet", 50, 0, Set.of(), Map.of(), 5,
                List.of(new DeleteFile("s3://w/d.parquet", 9, DeleteFile.Kind.POSITION, List.of())));

        TableStatisticsService.TableStatistics exact = catalogService(List.of(plain))
                .statistics("ice", "t", null);
        TableStatisticsService.TableStatistics bound = catalogService(List.of(plain, deleted))
                .statistics("ice", "t", null);
        TableFile unknown = new TableFile("s3://w/c.parquet", 7, 0, Set.of(), Map.of(), -1, List.of());
        TableStatisticsService.TableStatistics none = catalogService(List.of(plain, unknown))
                .statistics("ice", "t", null);

        assertEquals(10, exact.rows());
        assertTrue(exact.rowsExact());
        assertEquals(15, bound.rows());
        assertFalse(bound.rowsExact());
        assertEquals(150, bound.bytes());
        assertEquals(2, bound.files());
        assertEquals(-1, none.rows());
        assertFalse(none.rowsExact());
    }

    /** Converts unsigned integers, timestamps and decimals; types without a number get none. */
    @Test
    void numberConversions() {
        PrimitiveType uint64 = Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.intType(64, false)).named("u");
        PrimitiveType uint32 = Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.intType(32, false)).named("u");
        PrimitiveType micros = Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                .named("ts");
        PrimitiveType time = Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named("t");
        PrimitiveType decimal = Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.decimalType(2, 18)).named("d");
        PrimitiveType dbl = Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("x");

        assertEquals(1.8446744073709552E19, TableStatisticsService.number(-1L, uint64));
        assertEquals(4.294967295E9, TableStatisticsService.number(-1, uint32));
        assertEquals(1.5, TableStatisticsService.number(1_500_000L, micros));
        assertNull(TableStatisticsService.number(1000, time));
        assertEquals(-12.34, TableStatisticsService.number(-1234L, decimal));
        assertNull(TableStatisticsService.number(Double.NaN, dbl));
        assertEquals(2.5, TableStatisticsService.number(2.5, dbl));
    }

    /** Parses requests leniently and refuses a body that is not JSON. */
    @Test
    void requestParsing() {
        TableStatisticsService.Request request = TableStatisticsService.parseRequest(
                "{\"schema\":\"s\",\"table\":\"t\",\"columns\":[\"a\"],\"extra\":1}".getBytes());
        assertEquals("s", request.schema());
        assertEquals(List.of("a"), request.columns());
        assertThrows(IllegalArgumentException.class,
                () -> TableStatisticsService.parseRequest("not json".getBytes()));
    }

    private static void assertBounds(TableStatisticsService.TableStatistics stats, String column,
            double min, double max) {
        assertEquals(min, stats.columns().get(column).min(), 1e-9, column + " min");
        assertEquals(max, stats.columns().get(column).max(), 1e-9, column + " max");
    }

    private List<DataFile> writeFiles() throws Exception {
        Path a = dir.resolve("part-0.parquet");
        Path b = dir.resolve("part-1.parquet");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (" + SELECT + " FROM range(0, 40)) TO '" + a + "' (FORMAT parquet)");
            s.execute("COPY (" + SELECT + " FROM range(40, 100)) TO '" + b
                    + "' (FORMAT parquet, ROW_GROUP_SIZE 16)");
        }
        return List.of(dataFile(a), dataFile(b));
    }

    private static DataFile dataFile(Path file) throws IOException {
        return new DataFile("s/t/" + file.getFileName(), file.toUri().toString(), Files.size(file),
                Files.getLastModifiedTime(file).toMillis(), Set.of());
    }

    private static ParquetAdapter directoryAdapter(List<DataFile> files, boolean shared)
            throws IOException {
        DataStore store = mock(DataStore.class);
        when(store.readScope()).thenReturn(DataStore.ReadScope.NONE);
        when(store.inputFile(any(DataFile.class))).thenAnswer(call -> {
            DataFile file = call.getArgument(0);
            return HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(file.location()),
                    new Configuration());
        });
        ParquetAdapter adapter = mock(ParquetAdapter.class);
        when(adapter.dataStore()).thenReturn(store);
        when(adapter.sharedStorage()).thenReturn(shared);
        when(adapter.tableFiles("s", "t")).thenReturn(files);
        return adapter;
    }

    private static TableStatisticsService catalogService(List<TableFile> files) throws IOException {
        TableSnapshot snapshot = new TableSnapshot(new TableName("ice", "t"), "1", "f", List.of(),
                List.of(), files, new ReadSpec.ParquetFiles(false));
        CatalogTables catalogTables = mock(CatalogTables.class);
        when(catalogTables.current("ice", "t")).thenReturn(Optional.of(
                new CatalogTables.Resolved(mock(CatalogTables.Entry.class), snapshot)));
        ParquetAdapter adapter = mock(ParquetAdapter.class);
        when(adapter.isCatalogSchema("ice")).thenReturn(true);
        when(adapter.catalogTables()).thenReturn(catalogTables);
        return new TableStatisticsService(adapter, mock(ClusterService.class));
    }
}
