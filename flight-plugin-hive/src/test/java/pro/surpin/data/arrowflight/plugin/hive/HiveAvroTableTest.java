package pro.surpin.data.arrowflight.plugin.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
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
 * Avro tables of a real metastore (hive.avroTables): classification by schema literal or
 * columns, snapshots read as Arrow streams that DuckDB reads as the declared types, partitions,
 * and partitions with another schema refused.
 */
class HiveAvroTableTest {

    /** Schema of the table read by {@code avro.schema.url}. */
    private static final String BY_URL = "{\"type\":\"record\",\"name\":\"u\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}";

    @TempDir
    static Path dir;
    static IsolatedHms hms;
    static String root;

    @BeforeAll
    static void start() throws Exception {
        hms = IsolatedHms.shared();
        root = dir.resolve("lake").toString();
        hms.createDatabase("avr");
        Schema all = AvroFormat.parse(AvroFormatTest.ALL_TYPES);
        Path lit = Files.createDirectories(dir.resolve("lake/lit"));
        GenericRecord full = new GenericData.Record(all);
        full.put("I", 1);
        full.put("s", "ü");
        full.put("e", new GenericData.EnumSymbol(inner(all, "e"), "B"));
        full.put("d", ByteBuffer.wrap(BigInteger.valueOf(-1235).toByteArray()));
        full.put("dt", 19782);
        full.put("ts", 0L);
        full.put("b", true);
        full.put("c", "a  b");
        full.put("v", "hello world");
        full.put("bin", ByteBuffer.wrap(new byte[] {104, 105}));
        full.put("fx", new GenericData.Fixed(inner(all, "fx"), new byte[] {111, 107}));
        full.put("l", 9_000_000_000L);
        full.put("f", 1.5f);
        full.put("db", -2e3);
        full.put("a", new GenericData.Array<>(inner(all, "a"), Arrays.asList(1, null)));
        full.put("m", Map.of("k", 2));
        GenericRecord pair = new GenericData.Record(inner(all, "st"));
        pair.put("x", 7);
        pair.put("y", "q");
        full.put("st", pair);
        write(lit.resolve("000000_0"), all, CodecFactory.snappyCodec(), full);
        write(lit.resolve("000001_0"), all, CodecFactory.deflateCodec(6),
                new GenericData.Record(all));
        hms.createTable("avr", "lit", "file:" + lit, AvroFormatTest.ALL_COLUMNS, "", "avro",
                Map.of(AvroFormat.LITERAL, AvroFormatTest.ALL_TYPES));

        Path parted = Files.createDirectories(dir.resolve("lake/parted"));
        String columns = "id int;name string;d decimal(5,2)";
        hms.createTable("avr", "parted", "file:" + parted, columns, "region string", "avro", null);
        Schema hive = AvroFormat.fromColumns(AvroFormatTest.columns(columns));
        write(file(hms.addPartition("avr", "parted", List.of("EU"), null), "a.avro"), hive,
                CodecFactory.nullCodec(), row(hive, 1, "x", 150), row(hive, 2, null, null));
        write(file(hms.addPartition("avr", "parted", List.of("__HIVE_DEFAULT_PARTITION__"), null),
                "b.avro"), hive, CodecFactory.nullCodec(), row(hive, 3, "z", -5));

        hms.createTable("avr", "tiny", "file:" + dir.resolve("lake/tiny"), "t tinyint", "", "avro",
                null);
        Files.createDirectories(dir.resolve("schemas"));
        Files.writeString(dir.resolve("schemas/a.avsc"), AvroFormat.fromColumns(
                AvroFormatTest.columns("i int")).toString());
        hms.createTable("avr", "url", "file:" + dir.resolve("lake/url"), "i int", "", "avro",
                Map.of(AvroFormat.URL, "file:" + dir.resolve("schemas/a.avsc")));
        Path schemas = Files.createDirectories(dir.resolve("lake/schemas"));
        Schema byUrl = AvroFormat.parse(BY_URL);
        Files.writeString(schemas.resolve("byurl.avsc"), BY_URL);
        Path byUrlData = Files.createDirectories(dir.resolve("lake/byurl"));
        GenericRecord one = new GenericData.Record(byUrl);
        one.put("id", 1);
        one.put("name", "a");
        write(byUrlData.resolve("000000_0"), byUrl, CodecFactory.nullCodec(), one);
        hms.createTable("avr", "byurl", "file:" + byUrlData, "id int;name string", "", "avro",
                Map.of(AvroFormat.URL, "file:" + schemas.resolve("byurl.avsc")));
        hms.createTable("avr", "nourl", "file:" + dir.resolve("lake/nourl"), "id int", "", "avro",
                Map.of("serde:" + AvroFormat.URL, "file:" + schemas.resolve("missing.avsc")));
        Path purl = Files.createDirectories(dir.resolve("lake/purl"));
        hms.createTable("avr", "purl", "file:" + purl, "id int;name string", "p int", "avro", null);
        Schema purlSchema = AvroFormat.fromColumns(AvroFormatTest.columns("id int;name string"));
        Files.writeString(schemas.resolve("purl.avsc"), purlSchema.toString());
        write(file(hms.addPartition("avr", "purl", List.of("1"), null, Map.of(AvroFormat.URL,
                "file:" + schemas.resolve("purl.avsc"))), "a.avro"), purlSchema,
                CodecFactory.nullCodec(), row2(purlSchema, 5, "e"));
        write(file(hms.addPartition("avr", "purl", List.of("2"), null), "b.avro"), purlSchema,
                CodecFactory.nullCodec(), row2(purlSchema, 6, "f"));
        hms.createTable("avr", "stale", "file:" + dir.resolve("lake/stale"), "i int;j int", "",
                "avro", Map.of("serde:" + AvroFormat.LITERAL, AvroFormatTest.ALL_TYPES));
        hms.createTable("avr", "other", "file:" + dir.resolve("lake/other"), "id int", "p int",
                "avro", null);
        hms.addPartition("avr", "other", List.of("1"), null);
        hms.addPartition("avr", "other", List.of("2"), null, Map.of(AvroFormat.LITERAL,
                "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"id\",\"type\":"
                        + "[\"null\",\"int\"],\"default\":null,\"doc\":\"renamed\"},"
                        + "{\"name\":\"extra\",\"type\":\"int\",\"default\":0}]}"));
    }

    @Test
    void avroTablesAreShownOnlyWhenEnabledAndWhenTheirSchemaIsTheColumns() throws Exception {
        try (Catalog catalog = open(Map.of())) {
            assertFalse(catalog.tables("avr").contains("lit"), catalog.tables("avr").toString());
            assertEquals(Optional.empty(), catalog.current(new TableName("avr", "lit")));
        }
        try (Catalog catalog = open(Map.of("avroTables", "true"))) {
            List<String> tables = catalog.tables("avr");
            assertTrue(tables.containsAll(List.of("lit", "parted", "other")), tables.toString());
            assertFalse(tables.contains("tiny"), "Avro has no tinyint: " + tables);
            assertFalse(tables.contains("url"), "avro.schema.url outside the roots: " + tables);
            assertFalse(tables.contains("nourl"), "missing avro.schema.url file: " + tables);
            assertTrue(tables.containsAll(List.of("byurl", "purl")), tables.toString());
            assertFalse(tables.contains("stale"), "the literal is not the columns: " + tables);
            TableSnapshot lit = catalog.current(new TableName("avr", "lit")).orElseThrow();
            assertEquals(new ReadSpec.ArrowStream("avro"), lit.read());
            assertEquals(2, lit.files().size());
            TableSnapshot pinned = catalog.pinnedColumns(new TableName("avr", "lit"),
                    lit.snapshotId()).orElseThrow();
            assertEquals(new ReadSpec.ArrowStream("avro"), pinned.read());
            TableSnapshot parted = catalog.current(new TableName("avr", "parted")).orElseThrow();
            assertNotEquals(lit.snapshotId(), parted.snapshotId());
        }
    }

    @Test
    void duckDbReadsTheStreamAsTheDeclaredTypes() throws Exception {
        try (Catalog catalog = open(Map.of("avroTables", "true"));
                BufferAllocator allocator = new RootAllocator();
                Connection duckdb = DriverManager.getConnection("jdbc:duckdb:")) {
            TableSnapshot lit = catalog.current(new TableName("avr", "lit")).orElseThrow();
            List<String> all = List.of("i", "s", "e", "d", "dt", "ts", "b", "c", "v", "bin", "fx",
                    "l", "f", "db", "a", "m", "st");
            try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
                catalog.exportArrowScan(lit, lit.files(), new ArrowScan(all, List.of()),
                        stream.memoryAddress(), allocator);
                duckdb.unwrap(DuckDBConnection.class).registerArrowStream("avro_lit", stream);
                assertEquals(List.of(
                        "1,ü,B,-12.35,2024-02-29,1970-01-01 00:00:00,true,a,hello,hi,ok,9000000000,"
                                + "1.5,-2000.0,[1, NULL],{k=2},{'x': 7, 'y': q},DECIMAL(10,2),"
                                + "MAP(VARCHAR, INTEGER)",
                        "null,null,null,null,null,null,null,null,null,null,null,null,null,null,"
                                + "null,null,null,DECIMAL(10,2),MAP(VARCHAR, INTEGER)"),
                        rows(duckdb, "SELECT i, s, e, d::VARCHAR, dt::VARCHAR, ts::VARCHAR, b, c, v,"
                                + " bin::VARCHAR, fx::VARCHAR, l, f, db, a::VARCHAR, m::VARCHAR,"
                                + " st::VARCHAR, typeof(d), typeof(m) FROM avro_lit ORDER BY i"));
            }
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    @Test
    void partitionsAndPartitionsWithAnotherSchema() throws Exception {
        try (Catalog catalog = open(Map.of("avroTables", "true"));
                BufferAllocator allocator = new RootAllocator()) {
            TableSnapshot parted = catalog.current(new TableName("avr", "parted")).orElseThrow();
            assertEquals(List.of("1|x|1.50|EU", "2|null|null|EU", "3|z|-0.05|null"),
                    read(catalog, allocator, parted, List.of("id", "name", "d", "region")));
            assertEquals(List.of("EU", "EU", "null"),
                    read(catalog, allocator, parted, List.of("region")));
            IOException e = assertThrows(IOException.class,
                    () -> catalog.current(new TableName("avr", "other")));
            assertTrue(e.getMessage().contains("[2]") && e.getMessage().contains("Avro schema"),
                    e.getMessage());
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    @Test
    void schemaUrlsUnderTheRootsAreReadAndReadAgainWhenTheyChange() throws Exception {
        try (Catalog catalog = open(Map.of("avroTables", "true", "namesTtlSeconds", "0",
                "metadataTtlSeconds", "0"));
                BufferAllocator allocator = new RootAllocator()) {
            TableName name = new TableName("avr", "byurl");
            TableSnapshot first = catalog.current(name).orElseThrow();
            assertEquals(List.of("1|a"), read(catalog, allocator, first, List.of("id", "name")));
            assertEquals(List.of("5|e|1", "6|f|2"), read(catalog, allocator,
                    catalog.current(new TableName("avr", "purl")).orElseThrow(),
                    List.of("id", "name", "p")));
            Path schema = dir.resolve("lake/schemas/byurl.avsc");
            // Another field name than the metastore's: the table is no longer shown.
            Files.writeString(schema, BY_URL.replace("\"name\":\"name\"", "\"name\":\"label\""));
            assertEquals(Optional.empty(), catalog.current(name));
            IOException e = assertThrows(IOException.class,
                    () -> catalog.exportArrowScan(first, first.files(),
                            new ArrowScan(List.of("id"), List.of()), 0L, allocator));
            assertTrue(e.getMessage().contains("changed"), e.getMessage());
            // A field doc (column comment) changes the text, not what is read: shown again.
            Files.writeString(schema, BY_URL.replace("\"type\":\"int\"",
                    "\"type\":\"int\",\"doc\":\"key\""));
            TableSnapshot again = catalog.current(name).orElseThrow();
            assertEquals(List.of("1|a"), read(catalog, allocator, again, List.of("id", "name")));
            Files.writeString(schema, BY_URL);
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    private static GenericRecord row2(Schema schema, int id, String name) {
        GenericRecord row = new GenericData.Record(schema);
        row.put("id", id);
        row.put("name", name);
        return row;
    }

    private static Schema inner(Schema record, String field) {
        return AvroFormat.unwrapped(record.getField(field).schema());
    }

    private static GenericRecord row(Schema schema, int id, String name, Integer cents) {
        GenericRecord row = new GenericData.Record(schema);
        row.put("id", id);
        row.put("name", name);
        row.put("d", cents == null ? null : ByteBuffer.wrap(BigInteger.valueOf(cents).toByteArray()));
        return row;
    }

    private static void write(Path path, Schema schema, CodecFactory codec, GenericRecord... rows)
            throws IOException {
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(
                new GenericDatumWriter<>(schema))) {
            writer.setCodec(codec);
            writer.setMeta(AvroArrowReader.WRITER_TIME_ZONE, "UTC");
            writer.create(schema, path.toFile());
            for (GenericRecord row : rows) {
                writer.append(row);
            }
        }
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
