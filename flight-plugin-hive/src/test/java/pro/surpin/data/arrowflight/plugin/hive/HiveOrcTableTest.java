package pro.surpin.data.arrowflight.plugin.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.duckdb.DuckDBConnection;

import pro.surpin.data.arrowflight.spi.catalog.ArrowScan;
import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;

/**
 * ORC tables of a real metastore (hive.orcTables): classification, snapshots read as Arrow
 * streams, the stream read by DuckDB against the same rows computed by DuckDB itself, partition
 * columns as text, stripe skipping from the filter hints, transactional ORC tables skipped, and a
 * broken file failing the stream with its location.
 */
class HiveOrcTableTest {

    @TempDir
    static Path dir;
    static IsolatedHms hms;
    static String root;
    static TimeZone zone;

    @BeforeAll
    static void start() throws Exception {
        zone = TimeZone.getDefault();
        // Not UTC: timestamps and dates must keep their wall clock.
        TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
        hms = IsolatedHms.shared();
        root = dir.resolve("lake").toString();
        Path flat = Files.createDirectories(dir.resolve("lake/flat"));
        OrcTestFiles.write(flat.resolve("000000_0"), 0, 3000);
        OrcTestFiles.write(flat.resolve("000001_0"), 3000, 6000);
        hms.createDatabase("orct");
        hms.createTable("orct", "flat", "file:" + flat, OrcTestFiles.COLUMNS, "", "orc", null);
        Path parted = Files.createDirectories(dir.resolve("lake/parted"));
        hms.createTable("orct", "parted", "file:" + parted, OrcTestFiles.COLUMNS,
                "region string;day int", "orc", null);
        String air = hms.addPartition("orct", "parted", List.of("AIR", "1"), null);
        OrcTestFiles.write(Path.of(air.substring("file:".length())).resolve("a.orc"), 0, 100);
        String nulls = hms.addPartition("orct", "parted",
                List.of("__HIVE_DEFAULT_PARTITION__", "2"), null);
        OrcTestFiles.write(Path.of(nulls.substring("file:".length())).resolve("n.orc"), 100, 150);
        hms.createTable("orct", "acid", "file:" + dir.resolve("lake/acid"), "a int", "", "orc",
                Map.of("transactional", "true", "transactional_properties", "insert_only"));
        Path broken = Files.createDirectories(dir.resolve("lake/broken"));
        Files.writeString(broken.resolve("000000_0"), "not an ORC file at all");
        hms.createTable("orct", "broken", "file:" + broken, "a int", "", "orc", null);
    }

    @AfterAll
    static void restoreZone() {
        TimeZone.setDefault(zone);
    }

    @Test
    void orcTablesAreShownOnlyWhenEnabled() throws Exception {
        try (Catalog catalog = open(Map.of())) {
            assertFalse(catalog.tables("orct").contains("flat"), catalog.tables("orct").toString());
            assertEquals(Optional.empty(), catalog.current(new TableName("orct", "flat")));
        }
        try (Catalog catalog = open(Map.of("orcTables", "true"))) {
            List<String> tables = catalog.tables("orct");
            assertTrue(tables.containsAll(List.of("flat", "parted", "broken")), tables.toString());
            assertFalse(tables.contains("acid"), "transactional ORC tables stay skipped: " + tables);
            TableSnapshot flat = catalog.current(new TableName("orct", "flat")).orElseThrow();
            assertEquals(new ReadSpec.ArrowStream("orc"), flat.read());
            assertEquals(2, flat.files().size());
            List<String> declared = new ArrayList<>();
            for (ColumnSpec column : flat.columns()) {
                declared.add(column.name() + " " + column.duckDbType());
            }
            assertEquals("i INTEGER", declared.get(0));
            assertEquals("st STRUCT(\"x\" INTEGER, \"y\" VARCHAR)", declared.get(10));
            TableSnapshot pinned = catalog.pinnedColumns(new TableName("orct", "flat"),
                    flat.snapshotId()).orElseThrow();
            assertEquals(new ReadSpec.ArrowStream("orc"), pinned.read());
            assertEquals(flat.columns(), pinned.columns());
        }
    }

    @Test
    void orcAndParquetTablesOfTheSameColumnsHaveDifferentIds() {
        org.apache.hadoop.hive.metastore.api.Table table =
                new org.apache.hadoop.hive.metastore.api.Table();
        table.setDbName("d");
        table.setTableName("t");
        List<ColumnSpec> columns = List.of(new ColumnSpec("a", "INTEGER", true, null));
        assertNotEquals(HiveTable.schemaId(table, HiveTable.Kind.PARQUET, columns, List.of()),
                HiveTable.schemaId(table, HiveTable.Kind.ORC, columns, List.of()));
    }

    @Test
    void duckDbReadsTheStreamAsTheSameRows() throws Exception {
        try (Catalog catalog = open(Map.of("orcTables", "true"));
                BufferAllocator allocator = new RootAllocator();
                Connection duckdb = DriverManager.getConnection("jdbc:duckdb:")) {
            TableSnapshot flat = catalog.current(new TableName("orct", "flat")).orElseThrow();
            List<String> all = flat.columns().stream().map(ColumnSpec::name).toList();
            try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
                catalog.exportArrowScan(flat, flat.files(), new ArrowScan(all, List.of()),
                        stream.memoryAddress(), allocator);
                duckdb.unwrap(DuckDBConnection.class).registerArrowStream("orc_flat", stream);
                // Both directions of EXCEPT ALL empty: the same multiset of rows.
                assertEquals(List.of("6000,0,0"), rows(duckdb, "WITH e AS (" + OrcTestFiles.EXPECTED
                        + " FROM range(0, 6000)), a AS (SELECT * FROM orc_flat) SELECT"
                        + " (SELECT count(*) FROM a), (SELECT count(*) FROM (FROM a EXCEPT ALL FROM e)),"
                        + " (SELECT count(*) FROM (FROM e EXCEPT ALL FROM a))"));
            }
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    @Test
    void partitionColumnsComeAsTextAndAloneCountRows() throws Exception {
        try (Catalog catalog = open(Map.of("orcTables", "true"));
                BufferAllocator allocator = new RootAllocator()) {
            TableSnapshot parted = catalog.current(new TableName("orct", "parted")).orElseThrow();
            assertEquals(List.of("region", "day"), parted.partitionColumns());
            List<String> rows = read(catalog, allocator, parted, List.of("day", "region", "i"),
                    List.of());
            assertEquals(150, rows.size());
            assertEquals("1|AIR|0", rows.get(0));
            assertEquals("2|null|149", rows.get(149));
            // Only partition columns: no ORC column is read, the rows are still counted.
            List<String> regions = read(catalog, allocator, parted, List.of("region"), List.of());
            assertEquals(150, regions.size());
            assertEquals(100, regions.stream().filter("AIR"::equals).count());
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    @Test
    void filterHintsSkipRowGroupsButNeverMatchingRows() throws Exception {
        try (Catalog catalog = open(Map.of("orcTables", "true"));
                BufferAllocator allocator = new RootAllocator()) {
            TableSnapshot flat = catalog.current(new TableName("orct", "flat")).orElseThrow();
            List<String> rows = read(catalog, allocator, flat, List.of("i"), List.of(
                    new ArrowScan.Predicate("i", ArrowScan.Op.GE, List.of(5500L))));
            // Row groups of 1000 rows: only the last one can match.
            assertEquals(1000, rows.size());
            assertEquals("5000", rows.get(0));
            rows = read(catalog, allocator, flat, List.of("i"), List.of(
                    new ArrowScan.Predicate("dt", ArrowScan.Op.EQ,
                            List.of(LocalDate.of(2020, 1, 1).plusDays(4321))),
                    new ArrowScan.Predicate("s", ArrowScan.Op.IN, List.of("s4321ü", "zzz")),
                    new ArrowScan.Predicate("dec", ArrowScan.Op.GT,
                            List.of(new BigDecimal("5000.00")))));
            assertTrue(rows.contains("4321"), "the matching row is read");
            assertEquals(1000, rows.size());
            // A predicate no row group can match reads nothing; one the reader cannot use, all.
            assertEquals(0, read(catalog, allocator, flat, List.of("i"), List.of(
                    new ArrowScan.Predicate("b", ArrowScan.Op.LT, List.of(-1L)))).size());
            assertEquals(6000, read(catalog, allocator, flat, List.of("i"), List.of(
                    new ArrowScan.Predicate("ts", ArrowScan.Op.LT,
                            List.of(LocalDateTime.of(2000, 1, 1, 0, 0))),
                    new ArrowScan.Predicate("i", ArrowScan.Op.EQ, java.util.Arrays.asList(
                            (Object) null)))).size());
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    @Test
    void aBrokenFileFailsTheStreamWithItsLocation() throws Exception {
        try (Catalog catalog = open(Map.of("orcTables", "true"));
                BufferAllocator allocator = new RootAllocator()) {
            TableSnapshot broken = catalog.current(new TableName("orct", "broken")).orElseThrow();
            IOException e = assertThrows(IOException.class, () -> read(catalog, allocator, broken,
                    List.of("a"), List.of()));
            assertTrue(e.getMessage().contains("broken/000000_0"), e.getMessage());
            TableFile outside = new TableFile(dir.resolve("elsewhere.orc").toString(), 10L, 0L,
                    null, null, -1L, null);
            e = assertThrows(IOException.class, () -> read(catalog, allocator,
                    new TableSnapshot(broken.name(), broken.snapshotId(), "x", broken.columns(),
                            List.of(), List.of(outside), broken.read()), List.of("a"), List.of()));
            assertTrue(e.getMessage().contains("outside"), e.getMessage());
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    /**
     * Reads a scan through the C stream as the server would, one text line per row.
     */
    private static List<String> read(Catalog catalog, BufferAllocator allocator,
            TableSnapshot snapshot, List<String> columns, List<ArrowScan.Predicate> predicates)
            throws IOException {
        List<String> rows = new ArrayList<>();
        try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
            catalog.exportArrowScan(snapshot, snapshot.files(), new ArrowScan(columns, predicates),
                    stream.memoryAddress(), allocator);
            try (ArrowReader reader = Data.importArrayStream(allocator, stream)) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                while (reader.loadNextBatch()) {
                    for (int r = 0; r < root.getRowCount(); r++) {
                        StringBuilder row = new StringBuilder();
                        for (int c = 0; c < root.getFieldVectors().size(); c++) {
                            row.append(c > 0 ? "|" : "").append(root.getVector(c).getObject(r));
                        }
                        rows.add(row.toString());
                    }
                }
            }
        }
        return rows;
    }

    private static List<String> rows(Connection connection, String sql) throws Exception {
        List<String> rows = new ArrayList<>();
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= columns; i++) {
                    row.append(i > 1 ? "," : "").append(rs.getObject(i));
                }
                rows.add(row.toString());
            }
        }
        return rows;
    }

    private static Catalog open(Map<String, String> overrides) throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("uris", hms.uri());
        values.put("roots", root);
        values.putAll(overrides);
        TestSupport.LocalStores stores = new TestSupport.LocalStores();
        Catalog catalog = new HiveCatalogProvider().open(new TestSupport.Context(
                new TestSupport.MapConfig("hive", values), stores, new SecretRedactor(), null));
        stores.freeze();
        return catalog;
    }
}
