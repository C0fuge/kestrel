package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.model.HandleState;
import pro.surpin.data.arrowflight.server.model.QueryPlan;
import pro.surpin.data.arrowflight.server.services.ClusterService;
import pro.surpin.data.arrowflight.server.services.QueryPlanner;
import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;

/**
 * Tests the core side of catalog tables read as Parquet files (Hive): ticket keys with partition
 * values, the DuckDB sources with typed partition constants, partition pruning, and partial plans
 * that ship partition values.
 */
class ParquetCatalogTablesTest {

    private static final String ROOT = "hdfs://ns/wh";
    private static final String S1 = "grpc://127.0.0.1:32010";
    private static final String S2 = "grpc://127.0.0.1:32011";

    @TempDir
    Path dir;

    /** An in-memory catalog of Parquet tables. */
    private static final class FakeCatalog implements Catalog {
        final Map<String, List<String>> tables = new HashMap<>();
        final Map<TableName, TableSnapshot> snapshots = new HashMap<>();

        @Override
        public List<String> schemas() {
            return new ArrayList<>(new TreeSet<>(tables.keySet()));
        }

        @Override
        public List<String> tables(String schema) {
            return tables.getOrDefault(schema, List.of());
        }

        @Override
        public Optional<TableSnapshot> current(TableName table) {
            return Optional.ofNullable(snapshots.get(table));
        }

        @Override
        public List<String> readableRoots() {
            return List.of(ROOT);
        }
    }

    private static Map<String, String> values(String region, String day) {
        Map<String, String> result = new LinkedHashMap<>();
        result.put("region", region);
        result.put("day", day);
        return result;
    }

    private static TableFile file(String location, Map<String, String> values) {
        return new TableFile(location, 100, 0L, null, values, -1L, null);
    }

    private static TableSnapshot parted(List<TableFile> files) {
        return new TableSnapshot(new TableName("hv", "t"), "habc", "habc/x",
                List.of(new ColumnSpec("id", "INTEGER", true, null),
                        new ColumnSpec("name", "VARCHAR", true, null),
                        new ColumnSpec("region", "VARCHAR", true, null),
                        new ColumnSpec("day", "DATE", true, null)),
                List.of("region", "day"), files, new ReadSpec.ParquetFiles(false));
    }

    private static List<TableFile> fourFiles() {
        return List.of(file(ROOT + "/t/region=AIR/day=2024-01-01/a", values("AIR", "2024-01-01")),
                file(ROOT + "/t/region=AIR/day=2024-01-02/b", values("AIR", "2024-01-02")),
                file(ROOT + "/t/mail/c", values("MAIL", "2024-01-01")),
                file(ROOT + "/t/null/d", values(null, "2024-01-03")));
    }

    @Test
    void ticketKeysCarryPartitionValues() {
        Map<String, String> odd = new LinkedHashMap<>();
        odd.put("k&=@", "a b/c:d=%'é&x@y");
        odd.put("n", null);
        odd.put("e", "");
        CatalogFileKey key = new CatalogFileKey("h1@2", ROOT + "/t/k=a b%2F/f@1", odd);
        String encoded = key.encode();
        CatalogFileKey decoded = CatalogFileKey.decode(encoded);
        assertEquals(key, decoded);
        assertTrue(decoded.carriesPartitionValues());
        assertNull(decoded.partitionValues().get("n"));
        assertTrue(decoded.partitionValues().containsKey("n"));
        assertEquals("", decoded.partitionValues().get("e"));
        assertEquals(List.of("k&=@", "n", "e"), new ArrayList<>(decoded.partitionValues().keySet()));
        CatalogFileKey none = new CatalogFileKey("h1", ROOT + "/t/f", Map.of());
        assertEquals(none, CatalogFileKey.decode(none.encode()));
        assertTrue(CatalogFileKey.decode(none.encode()).carriesPartitionValues());
        CatalogFileKey iceberg = new CatalogFileKey("7.1", ROOT + "/t/f");
        assertFalse(CatalogFileKey.decode(iceberg.encode()).carriesPartitionValues());
        assertThrows(IllegalArgumentException.class, () -> CatalogFileKey.decode("@snapshot:1@?a=1"));
        assertThrows(IllegalArgumentException.class, () -> CatalogFileKey.decode("@snapshot:1@?a&a@/x"));
    }

    @Test
    void sourcesReadByNameWithTypedPartitionConstants() throws Exception {
        Path a = dir.resolve("a.parquet");
        Path b = dir.resolve("b");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT 1 AS ID, 'x' AS name) TO '" + a + "' (FORMAT parquet)");
            s.execute("COPY (SELECT 2::BIGINT AS id) TO '" + b + "' (FORMAT parquet)");
            TableSnapshot snapshot = parted(List.of(file(a.toString(), values("AIR", "2024-01-01")),
                    file(b.toString(), values(null, "bad date"))));
            String source = CatalogSources.filesSource(snapshot, snapshot.files());
            assertTrue(source.contains("UNION ALL"), source);
            List<String> rows = new ArrayList<>();
            try (ResultSet r = s.executeQuery("SELECT * FROM " + source + " ORDER BY id")) {
                while (r.next()) {
                    rows.add(r.getInt(1) + "|" + r.getString(2) + "|" + r.getString(3) + "|"
                            + r.getString(4));
                }
            }
            // Columns by name (ID = id), missing name is NULL, NULL and bad partition values NULL.
            assertEquals(List.of("1|x|AIR|2024-01-01", "2|null|null|null"), rows);
            try (ResultSet r = s.executeQuery("DESCRIBE SELECT * FROM " + source)) {
                List<String> types = new ArrayList<>();
                while (r.next()) {
                    types.add(r.getString("column_name") + " " + r.getString("column_type"));
                }
                assertEquals(List.of("id INTEGER", "name VARCHAR", "region VARCHAR", "day DATE"), types);
            }
            TableSnapshot flat = new TableSnapshot(new TableName("hv", "f"), "h1", "h1/x",
                    List.of(new ColumnSpec("id", "BIGINT", true, null)), List.of(),
                    List.of(file(a.toString(), Map.of())), new ReadSpec.ParquetFiles(false));
            String flatSource = CatalogSources.filesSource(flat, flat.files());
            assertFalse(flatSource.contains("UNION"), flatSource);
            try (ResultSet r = s.executeQuery("SELECT sum(id) FROM " + flatSource)) {
                r.next();
                assertEquals(1L, r.getLong(1));
            }
        }
        assertEquals("'a''b'", CatalogSources.typedConstant("a'b", "VARCHAR"));
        assertEquals("TRY_CAST('5' AS INTEGER)", CatalogSources.typedConstant("5", "INTEGER"));
        assertEquals("CAST(NULL AS DATE)", CatalogSources.typedConstant(null, "DATE"));
    }

    @Test
    void partitionsAreRuledOutBySimplePredicates() {
        TableSnapshot snapshot = parted(fourFiles());
        assertEquals(List.of("a", "b", "c", "d"), names(PartitionPruner.prune(snapshot, null)));
        assertEquals(List.of("a", "b"), names(PartitionPruner.prune(snapshot, "\"region\" = 'AIR'")));
        assertEquals(List.of("a", "b"), names(PartitionPruner.prune(snapshot,
                "\"REGION\" = 'AIR' and \"id\" > 3")));
        assertEquals(List.of("a", "c"), names(PartitionPruner.prune(snapshot,
                "\"day\" = DATE '2024-01-01'")));
        assertEquals(List.of("b", "d"), names(PartitionPruner.prune(snapshot,
                "\"day\" between '2024-01-02' and '2024-01-05'")));
        assertEquals(List.of("c", "d"), names(PartitionPruner.prune(snapshot,
                "\"region\" in ('MAIL', 'SHIP') or \"region\" is null")));
        assertEquals(List.of("c"), names(PartitionPruner.prune(snapshot,
                "not (\"region\" = 'AIR')")));
        assertEquals(List.of("d"), names(PartitionPruner.prune(snapshot, "\"region\" is null")));
        assertEquals(List.of("a", "b", "c"), names(PartitionPruner.prune(snapshot,
                "\"region\" is not null")));
        assertEquals(List.of(), names(PartitionPruner.prune(snapshot, "\"region\" = 'TRUCK'")));
        // Unknown: data columns, functions, OR with a data column, text vs number.
        assertEquals(4, PartitionPruner.prune(snapshot, "\"id\" = 1").size());
        assertEquals(4, PartitionPruner.prune(snapshot, "lower(\"region\") = 'air'").size());
        assertEquals(4, PartitionPruner.prune(snapshot, "\"region\" = 'AIR' or \"id\" = 1").size());
        // Text vs number: unknown, except that a NULL value never compares.
        assertEquals(List.of("a", "b", "c"), names(PartitionPruner.prune(snapshot, "\"region\" = 5")));
        assertEquals(4, PartitionPruner.prune(snapshot, "not a parsable condition ((").size());
        assertEquals(PartitionPruner.Truth.NULL, PartitionPruner.and(PartitionPruner.Truth.TRUE,
                PartitionPruner.Truth.NULL));
        assertTrue(PartitionPruner.or(PartitionPruner.Truth.NULL, PartitionPruner.Truth.MAYBE)
                .maySelect());
        assertFalse(PartitionPruner.and(PartitionPruner.Truth.NULL, PartitionPruner.Truth.MAYBE)
                .maySelect());
        assertTrue(PartitionPruner.not(PartitionPruner.and(PartitionPruner.Truth.NULL,
                PartitionPruner.Truth.MAYBE)).maySelect());
        assertEquals(-1, PartitionPruner.compareAs("INTEGER", "9", "10"));
        assertEquals(1, PartitionPruner.compareAs("VARCHAR", "9", "10"));
        assertNull(PartitionPruner.compareAs("INTEGER", "1.5", "1"));
        assertNull(PartitionPruner.compareAs("TIMESTAMP", "a", "b"));
    }

    @Test
    void partialPlansShipPartitionValuesOfThePartitionsLeft() throws Exception {
        FakeCatalog catalog = new FakeCatalog();
        catalog.tables.put("hv", List.of("t", "e"));
        catalog.snapshots.put(new TableName("hv", "t"), parted(fourFiles()));
        catalog.snapshots.put(new TableName("hv", "e"), parted(List.of()));
        CatalogTables tables = new CatalogTables(List.of(new CatalogTables.Entry(
                new SpiCatalog(catalog, null, "fake"), List.of(ROOT), true)));
        tables.schemas(Set.of());
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        ClusterService cluster = mock(ClusterService.class);
        when(parquet.localFileInventory()).thenReturn(Map.of());
        when(parquet.catalogTables()).thenReturn(tables);
        when(parquet.isCatalogSchema("hv")).thenReturn(true);
        when(cluster.allServerLoads()).thenReturn(Map.of(S1, 0L, S2, 0L));
        when(cluster.filterLiveServers(anySet())).thenReturn(Set.of(S1, S2));
        when(cluster.hasFileInventory(any())).thenReturn(true);
        when(cluster.serverUri()).thenReturn(S1);
        ArgumentCaptor<HandleState> states = ArgumentCaptor.forClass(HandleState.class);
        when(cluster.createEndpointHandle(states.capture())).thenReturn(new byte[] {1});
        QueryPlanner planner = new QueryPlanner(parquet, cluster);

        assertNull(planner.catalogCompleteReason("SELECT id FROM hv.t"));
        planner.plan("SELECT id, region FROM hv.t WHERE region = 'AIR' AND id > 0");
        List<CatalogFileKey> keys = keys(states.getAllValues());
        assertEquals(2, keys.size());
        for (CatalogFileKey key : keys) {
            assertEquals("habc", key.snapshotId());
            assertEquals("AIR", key.partitionValues().get("region"));
            assertTrue(key.location().contains("region=AIR"));
        }
        int seen = states.getAllValues().size();
        planner.plan("SELECT id FROM hv.t WHERE region = 'NONE'");
        assertEquals(1, keys(states.getAllValues().subList(seen, states.getAllValues().size()))
                .size(), "one file stays");
        seen = states.getAllValues().size();
        QueryPlan all = planner.plan("SELECT count(*) FROM hv.t");
        assertEquals(400L, all.totalBytes());
        List<CatalogFileKey> allKeys = keys(states.getAllValues().subList(seen,
                states.getAllValues().size()));
        assertEquals(4, allKeys.size());
        assertTrue(allKeys.stream().anyMatch(k -> k.partitionValues().get("region") == null));
        assertTrue(planner.catalogCompleteReason("SELECT id FROM hv.e").contains("no data files"));
    }

    @Test
    void completeReasonsOfParquetTables() {
        CatalogTables.Entry entry = new CatalogTables.Entry(new SpiCatalog(new FakeCatalog(), null,
                "fake"), List.of(ROOT), true);
        assertNull(CatalogTables.completeReason(new CatalogTables.Resolved(entry, parted(fourFiles()))));
        TableSnapshot undeclared = new TableSnapshot(new TableName("hv", "u"), "h", "h",
                List.of(new ColumnSpec("id", "INTEGER", true, null)), List.of("region"), fourFiles(),
                new ReadSpec.ParquetFiles(false));
        assertTrue(CatalogTables.completeReason(new CatalogTables.Resolved(entry, undeclared))
                .contains("no declared type"));
    }

    private static List<CatalogFileKey> keys(List<HandleState> states) {
        List<CatalogFileKey> result = new ArrayList<>();
        for (HandleState state : states) {
            for (String entry : state.filePaths()) {
                result.add(CatalogFileKey.decode(entry));
            }
        }
        return result;
    }

    private static List<String> names(List<TableFile> files) {
        List<String> result = new ArrayList<>();
        for (TableFile file : files) {
            result.add(file.location().substring(file.location().lastIndexOf('/') + 1));
        }
        return result;
    }

    @Test
    void ticketFilesMustLieUnderTheRootsAndNameThePartitionColumns() throws Exception {
        java.lang.reflect.Method method = pro.surpin.data.arrowflight.server.services.ExecutionService.class
                .getDeclaredMethod("ticketFiles", CatalogTables.Resolved.class, List.class);
        method.setAccessible(true);
        CatalogTables.Entry entry = new CatalogTables.Entry(new SpiCatalog(new FakeCatalog(), null,
                "fake"), List.of(ROOT), true);
        CatalogTables.Resolved resolved = new CatalogTables.Resolved(entry, parted(List.of()));
        @SuppressWarnings("unchecked")
        List<TableFile> ok = (List<TableFile>) method.invoke(null, resolved, List.of(
                new CatalogFileKey("habc", ROOT + "/t/x", values("AIR", "2024-01-01"))));
        assertEquals("AIR", ok.get(0).partitionValues().get("region"));
        for (CatalogFileKey bad : List.of(
                new CatalogFileKey("habc", "/etc/passwd", values("AIR", "2024-01-01")),
                new CatalogFileKey("habc", ROOT + "/../etc/x", values("AIR", "2024-01-01")),
                new CatalogFileKey("habc", ROOT + "/t/x", Map.of("region", "AIR")))) {
            java.lang.reflect.InvocationTargetException e = assertThrows(
                    java.lang.reflect.InvocationTargetException.class,
                    () -> method.invoke(null, resolved, List.of(bad)));
            assertTrue(e.getCause() instanceof IllegalArgumentException, e.getCause().toString());
        }
        assertTrue(Files.isDirectory(dir));
    }
}
