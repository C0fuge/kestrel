package pro.surpin.data.arrowflight.plugin.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.zip.GZIPOutputStream;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.spi.catalog.ArrowScan;
import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;

/**
 * Text tables of a real metastore (hive.textTables): classification, serde and table properties,
 * snapshots read as Arrow streams that DuckDB reads as the declared types, partitions, and
 * partitions laid out other than their table refused.
 */
class HiveTextTableTest {

    private static final String COLUMNS = "i int;s string;d decimal(10,2);dt date;ts timestamp;"
            + "b boolean;c char(3);bin binary;l bigint;db double";

    @TempDir
    static Path dir;
    static IsolatedHms hms;
    static String root;

    @BeforeAll
    static void start() throws Exception {
        hms = IsolatedHms.shared();
        root = dir.resolve("lake").toString();
        hms.createDatabase("txt");
        Path flat = Files.createDirectories(dir.resolve("lake/flat"));
        Files.writeString(flat.resolve("000000_0"), line(1, "a", "12.345", "2024-02-29",
                "2024-01-02 03:04:05.123456", "true", "ab ", "aGk=", "9000000000", "1.5")
                + line(2, "\\N", "x", "2023-02-29", "x", "x", "\\N", "", "x", "NaN"));
        try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(
                flat.resolve("000001_0.gz")))) {
            out.write(line(3, "ü", "-0.5", "1970-01-01", "1969-12-31 23:59:59", "FALSE", "xyz",
                    "!", "-1", "-2e3").getBytes(StandardCharsets.UTF_8));
        }
        hms.createTable("txt", "flat", "file:" + flat, COLUMNS, "", "text", null);
        Path parted = Files.createDirectories(dir.resolve("lake/parted"));
        hms.createTable("txt", "parted", "file:" + parted, "id int;name string", "region string",
                "text", Map.of("serde:field.delim", ",", "serde:serialization.null.format", "",
                        "skip.header.line.count", "1"));
        String eu = hms.addPartition("txt", "parted", List.of("EU"), null);
        Files.writeString(file(eu, "a.csv"), "id,name\n1,x\n2,\n");
        String nulls = hms.addPartition("txt", "parted", List.of("__HIVE_DEFAULT_PARTITION__"), null);
        Files.writeString(file(nulls, "n.csv"), "id,name\n3,z\n");
        Path other = Files.createDirectories(dir.resolve("lake/other"));
        hms.createTable("txt", "other", "file:" + other, "id int", "p int", "text",
                Map.of("serde:field.delim", ","));
        hms.addPartition("txt", "other", List.of("1"), null);
        hms.addPartition("txt", "other", List.of("2"), null, Map.of("field.delim", "|"));
        hms.createTable("txt", "footer", "file:" + dir.resolve("lake/footer"), "a int", "", "text",
                Map.of("skip.footer.line.count", "1"));
        Path nested = Files.createDirectories(dir.resolve("lake/nested"));
        Files.writeString(nested.resolve("000000_0"), "1\u00011\u00022\u0001a\u00031\u0002b\u0003"
                + "\u00017\u0002x\u00025\u00046\n2\u0001\u0001\u0001\\N\n");
        hms.createTable("txt", "nested", "file:" + nested,
                "i int;a array<int>;m map<string,int>;st struct<x:int,y:string,z:map<int,int>>", "",
                "text", null);
        Path csv = Files.createDirectories(dir.resolve("lake/csv"));
        hms.createTable("txt", "csv", "file:" + csv, "id string;name string", "p string", "csv",
                Map.of("serde:separatorChar", ";", "skip.header.line.count", "1"));
        Files.writeString(file(hms.addPartition("txt", "csv", List.of("a"), null), "1.csv"),
                "id;name\n1;\"x;y\"\n2\n");
        hms.createTable("txt", "typedcsv", "file:" + dir.resolve("lake/typedcsv"), "id int", "",
                "csv", null);
        hms.createTable("txt", "unions", "file:" + dir.resolve("lake/unions"),
                "u uniontype<int,string>", "", "text", null);
    }

    @Test
    void textTablesAreShownOnlyWhenEnabled() throws Exception {
        try (Catalog catalog = open(Map.of())) {
            assertFalse(catalog.tables("txt").contains("flat"), catalog.tables("txt").toString());
            assertEquals(Optional.empty(), catalog.current(new TableName("txt", "flat")));
        }
        try (Catalog catalog = open(Map.of("textTables", "true"))) {
            List<String> tables = catalog.tables("txt");
            assertTrue(tables.containsAll(List.of("flat", "parted", "other")), tables.toString());
            assertFalse(tables.contains("footer"), "footer lines are not read: " + tables);
            assertTrue(tables.contains("nested"), tables.toString());
            assertFalse(tables.contains("unions"), "unions are not read: " + tables);
            assertTrue(tables.contains("csv"), tables.toString());
            assertFalse(tables.contains("typedcsv"), "OpenCSVSerde columns are strings: " + tables);
            TableSnapshot flat = catalog.current(new TableName("txt", "flat")).orElseThrow();
            assertEquals(new ReadSpec.ArrowStream("text"), flat.read());
            assertEquals(2, flat.files().size());
            TableSnapshot pinned = catalog.pinnedColumns(new TableName("txt", "flat"),
                    flat.snapshotId()).orElseThrow();
            assertEquals(new ReadSpec.ArrowStream("text"), pinned.read());
            TableSnapshot parted = catalog.current(new TableName("txt", "parted")).orElseThrow();
            assertNotEquals(flat.snapshotId(), parted.snapshotId());
        }
    }

    @Test
    void duckDbReadsTheStreamAsTheDeclaredTypes() throws Exception {
        try (Catalog catalog = open(Map.of("textTables", "true"));
                BufferAllocator allocator = new RootAllocator();
                Connection duckdb = DriverManager.getConnection("jdbc:duckdb:")) {
            TableSnapshot flat = catalog.current(new TableName("txt", "flat")).orElseThrow();
            List<String> all = List.of("i", "s", "d", "dt", "ts", "b", "c", "bin", "l", "db");
            try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
                catalog.exportArrowScan(flat, flat.files(), new ArrowScan(all, List.of()),
                        stream.memoryAddress(), allocator);
                duckdb.unwrap(DuckDBConnection.class).registerArrowStream("text_flat", stream);
                assertEquals(List.of(
                        "1,a,12.35,2024-02-29,2024-01-02 03:04:05.123456,true,ab,hi,9000000000,1.5",
                        "2,null,null,null,null,null,null,,null,NaN",
                        "3,ü,-0.50,1970-01-01,1969-12-31 23:59:59,false,xyz,!,-1,-2000.0"),
                        rows(duckdb, "SELECT i, s, d::VARCHAR, dt::VARCHAR, ts::VARCHAR, b, c,"
                                + " bin::VARCHAR, l, db FROM text_flat ORDER BY i"));
            }
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    @Test
    void duckDbReadsNestedColumnsAsListsMapsAndStructs() throws Exception {
        try (Catalog catalog = open(Map.of("textTables", "true"));
                BufferAllocator allocator = new RootAllocator();
                Connection duckdb = DriverManager.getConnection("jdbc:duckdb:")) {
            TableSnapshot nested = catalog.current(new TableName("txt", "nested")).orElseThrow();
            try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
                catalog.exportArrowScan(nested, nested.files(),
                        new ArrowScan(List.of("i", "a", "m", "st"), List.of()), stream.memoryAddress(),
                        allocator);
                duckdb.unwrap(DuckDBConnection.class).registerArrowStream("text_nested", stream);
                assertEquals(List.of(
                        "1,[1, 2],{a=1, b=NULL},{'x': 7, 'y': x, 'z': {5=6}},INTEGER[],"
                                + "MAP(VARCHAR, INTEGER)",
                        "2,[],{},null,INTEGER[],MAP(VARCHAR, INTEGER)"),
                        rows(duckdb, "SELECT i, a::VARCHAR, m::VARCHAR, st::VARCHAR, typeof(a),"
                                + " typeof(m) FROM text_nested ORDER BY i"));
            }
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    @Test
    void openCsvSerdeTablesAreReadAsHiveParsesThem() throws Exception {
        try (Catalog catalog = open(Map.of("textTables", "true"));
                BufferAllocator allocator = new RootAllocator()) {
            TableSnapshot csv = catalog.current(new TableName("txt", "csv")).orElseThrow();
            assertEquals(new ReadSpec.ArrowStream("text"), csv.read());
            assertEquals(List.of("1|x;y|a", "2|null|a"),
                    read(catalog, allocator, csv, List.of("id", "name", "p")));
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    @Test
    void partitionsHeadersAndTheirOwnLayout() throws Exception {
        try (Catalog catalog = open(Map.of("textTables", "true"));
                BufferAllocator allocator = new RootAllocator()) {
            TableSnapshot parted = catalog.current(new TableName("txt", "parted")).orElseThrow();
            assertEquals(List.of("1|x|EU", "2|null|EU", "3|z|null"),
                    read(catalog, allocator, parted, List.of("id", "name", "region")));
            assertEquals(List.of("EU", "EU", "null"),
                    read(catalog, allocator, parted, List.of("region")));
            IOException e = assertThrows(IOException.class,
                    () -> catalog.current(new TableName("txt", "other")));
            assertTrue(e.getMessage().contains("[2]") && e.getMessage().contains("serde"),
                    e.getMessage());
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    private static String line(int i, String... fields) {
        return i + "\u0001" + String.join("\u0001", fields) + "\n";
    }

    private static Path file(String location, String name) throws IOException {
        Path directory = Files.createDirectories(Path.of(location.substring("file:".length())));
        return directory.resolve(name);
    }

    /**
     * Reads a scan through the C stream as the server would, one text line per row.
     */
    private static List<String> read(Catalog catalog, BufferAllocator allocator,
            TableSnapshot snapshot, List<String> columns) throws IOException {
        List<String> rows = new ArrayList<>();
        try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
            catalog.exportArrowScan(snapshot, snapshot.files(), new ArrowScan(columns, List.of()),
                    stream.memoryAddress(), allocator);
            try (ArrowReader reader = Data.importArrayStream(allocator, stream)) {
                VectorSchemaRoot batch = reader.getVectorSchemaRoot();
                while (reader.loadNextBatch()) {
                    for (int r = 0; r < batch.getRowCount(); r++) {
                        StringBuilder row = new StringBuilder();
                        for (int c = 0; c < batch.getFieldVectors().size(); c++) {
                            row.append(c > 0 ? "|" : "").append(batch.getVector(c).getObject(r));
                        }
                        rows.add(row.toString());
                    }
                }
            }
        }
        rows.sort(null);
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
