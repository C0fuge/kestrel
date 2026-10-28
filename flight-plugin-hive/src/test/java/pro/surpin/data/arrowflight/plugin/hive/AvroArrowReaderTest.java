package pro.surpin.data.arrowflight.plugin.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.spi.storage.ReadableFile;

/**
 * One Avro data file read as Hive 3.1.3 reads it: every type with NULLs, the file's schema
 * resolved to the table's (promotions, defaults, decimals at the file's scale), the writer's time
 * zone, codecs, projections and failures.
 */
@Tag("unit")
class AvroArrowReaderTest {

    private static final List<String> ALL = List.of("i", "s", "e", "d", "dt", "ts", "b", "c", "v",
            "bin", "fx", "l", "f", "db", "a", "m", "st");

    private final BufferAllocator allocator = new RootAllocator();

    @AfterEach
    void close() {
        assertEquals(0, allocator.getAllocatedMemory());
        allocator.close();
    }

    @Test
    void everyTypeReadsAsHiveShowsIt() throws IOException {
        Schema schema = AvroFormat.parse(AvroFormatTest.ALL_TYPES);
        GenericRecord full = new GenericData.Record(schema);
        full.put("I", 1);
        full.put("s", "ü");
        full.put("e", new GenericData.EnumSymbol(field(schema, "e"), "B"));
        full.put("d", ByteBuffer.wrap(BigInteger.valueOf(-1235).toByteArray()));
        full.put("dt", 19782);
        full.put("ts", 0L);
        full.put("b", true);
        full.put("c", "a  b");
        full.put("v", "hello world");
        full.put("bin", ByteBuffer.wrap("hi".getBytes(StandardCharsets.UTF_8)));
        full.put("fx", new GenericData.Fixed(field(schema, "fx"), "ok".getBytes(StandardCharsets.UTF_8)));
        full.put("l", 9_000_000_000L);
        full.put("f", 1.5f);
        full.put("db", -2e3);
        full.put("a", new GenericData.Array<>(field(schema, "a"), Arrays.asList(1, null)));
        full.put("m", Map.of("k", 2));
        GenericRecord pair = new GenericData.Record(field(schema, "st"));
        pair.put("x", 7);
        pair.put("y", "q");
        full.put("st", pair);
        GenericRecord empty = new GenericData.Record(schema);
        byte[] data = write(schema, Map.of(AvroArrowReader.WRITER_TIME_ZONE, "America/Los_Angeles"),
                null, full, empty);
        AvroFormat format = AvroFormat.of(Map.of(), Map.of(AvroFormat.LITERAL,
                AvroFormatTest.ALL_TYPES), AvroFormatTest.columns(AvroFormatTest.ALL_COLUMNS));
        assertEquals(List.of("1|ü|B|-12.35|2024-02-29|1969-12-31T16:00|true|a|hello|hi|ok|9000000000"
                + "|1.5|-2000.0|[1, null]|{k=2}|{x=7, y=q}",
                "null|null|null|null|null|null|null|null|null|null|null|null|null|null|null|null|null"),
                read(data, format, AvroFormatTest.ALL_COLUMNS, ALL));
        assertEquals(List.of("{k=2}|1", "null|null"),
                read(data, format, AvroFormatTest.ALL_COLUMNS, List.of("m", "i")));
        assertEquals(List.of("", ""), read(data, format, AvroFormatTest.ALL_COLUMNS, List.of()));
    }

    @Test
    void theFileSchemaIsResolvedToTheTableSchema() throws IOException {
        Schema writer = AvroFormat.parse("{\"type\":\"record\",\"name\":\"w\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"gone\",\"type\":\"string\"},"
                + "{\"name\":\"d\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\","
                + "\"precision\":9,\"scale\":3}},"
                + "{\"name\":\"big\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\","
                + "\"precision\":9,\"scale\":0}},"
                + "{\"name\":\"v\",\"type\":{\"type\":\"string\",\"logicalType\":\"varchar\","
                + "\"maxLength\":2}}]}");
        String table = "{\"type\":\"record\",\"name\":\"t\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"long\"},"
                + "{\"name\":\"d\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\","
                + "\"precision\":10,\"scale\":2}},"
                + "{\"name\":\"big\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\","
                + "\"precision\":3,\"scale\":0}},"
                + "{\"name\":\"v\",\"type\":{\"type\":\"string\",\"logicalType\":\"varchar\","
                + "\"maxLength\":5}},"
                + "{\"name\":\"added\",\"type\":[\"null\",\"string\"],\"default\":null},"
                + "{\"name\":\"n\",\"type\":\"int\",\"default\":5},"
                + "{\"name\":\"dd\",\"type\":[\"null\",{\"type\":\"bytes\",\"logicalType\":\"decimal\","
                + "\"precision\":4,\"scale\":1}],\"default\":null}]}";
        String columns = "id bigint;d decimal(10,2);big decimal(3,0);v varchar(5);added string;n int;"
                + "dd decimal(4,1)";
        GenericRecord row = new GenericData.Record(writer);
        row.put("id", 1);
        row.put("gone", "x");
        row.put("d", ByteBuffer.wrap(BigInteger.valueOf(12345).toByteArray()));
        row.put("big", ByteBuffer.wrap(BigInteger.valueOf(123456).toByteArray()));
        row.put("v", "abcd");
        AvroFormat format = AvroFormat.of(Map.of(), Map.of(AvroFormat.LITERAL, table),
                AvroFormatTest.columns(columns));
        // 12.345 at the file's scale 3 rounds to 12.35; 123456 does not fit decimal(3,0): NULL;
        // varchar cut to the file's maxLength 2.
        assertEquals(List.of("1|12.35|null|ab|null|5|null"), read(write(writer, Map.of(), null, row),
                format, columns, List.of("id", "d", "big", "v", "added", "n", "dd")));

        Schema required = AvroFormat.parse("{\"type\":\"record\",\"name\":\"w\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"int\"}]}");
        GenericRecord one = new GenericData.Record(required);
        one.put("id", 1);
        String needs = "{\"type\":\"record\",\"name\":\"t\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}";
        IOException e = assertThrows(IOException.class, () -> read(write(required, Map.of(), null,
                one), AvroFormat.of(Map.of(), Map.of(AvroFormat.LITERAL, needs),
                AvroFormatTest.columns("id int;name string")), "id int;name string", List.of("id")));
        assertTrue(e.getMessage().contains("test.avro"), e.getMessage());

        // A decimal the file lacks but the table defaults is NULL; one it has without a scale fails.
        String noScale = "{\"type\":\"record\",\"name\":\"w\",\"fields\":[{\"name\":\"dd\","
                + "\"type\":{\"type\":\"bytes\"}}]}";
        Schema plain = AvroFormat.parse(noScale);
        GenericRecord bytes = new GenericData.Record(plain);
        bytes.put("dd", ByteBuffer.wrap(new byte[] {1}));
        String decimalTable = "{\"type\":\"record\",\"name\":\"t\",\"fields\":[{\"name\":\"dd\","
                + "\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":4,"
                + "\"scale\":1}}]}";
        e = assertThrows(IOException.class, () -> read(write(plain, Map.of(), null, bytes),
                AvroFormat.of(Map.of(), Map.of(AvroFormat.LITERAL, decimalTable),
                        AvroFormatTest.columns("dd decimal(4,1)")), "dd decimal(4,1)", List.of("dd")));
        assertTrue(e.getMessage().contains("Record 1 of test.avro"), e.getMessage());
    }

    @Test
    void timestampsNeedTheWriterTimeZone() throws IOException {
        String table = "{\"type\":\"record\",\"name\":\"t\",\"fields\":[{\"name\":\"ts\",\"type\":"
                + "[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}]}]}";
        Schema schema = AvroFormat.parse(table);
        AvroFormat format = AvroFormat.of(Map.of(), Map.of(AvroFormat.LITERAL, table),
                AvroFormatTest.columns("ts timestamp"));
        GenericRecord row = new GenericData.Record(schema);
        row.put("ts", 1_700_000_000_123L);
        assertEquals(List.of("2023-11-14T22:13:20.123"), read(write(schema,
                Map.of(AvroArrowReader.WRITER_TIME_ZONE, "UTC"), null, row), format, "ts timestamp",
                List.of("ts")));
        assertEquals(List.of("2023-11-15T07:13:20.123"), read(write(schema,
                Map.of(AvroArrowReader.WRITER_TIME_ZONE, "Asia/Tokyo"), null, row), format,
                "ts timestamp", List.of("ts")));
        GenericRecord none = new GenericData.Record(schema);
        assertEquals(List.of("null"), read(write(schema, Map.of(), null, none), format,
                "ts timestamp", List.of("ts")));
        IOException e = assertThrows(IOException.class, () -> read(write(schema, Map.of(), null, row),
                format, "ts timestamp", List.of("ts")));
        assertTrue(e.getMessage().contains(AvroArrowReader.WRITER_TIME_ZONE), e.getMessage());
        e = assertThrows(IOException.class, () -> read(write(schema,
                Map.of(AvroArrowReader.WRITER_TIME_ZONE, "Nowhere/Else"), null, row), format,
                "ts timestamp", List.of("ts")));
        assertTrue(e.getMessage().contains("time zone"), e.getMessage());
    }

    @Test
    void codecsAndManyBlocksReadTheSameRows() throws IOException {
        String table = "{\"type\":\"record\",\"name\":\"t\",\"fields\":[{\"name\":\"i\",\"type\":"
                + "\"int\"},{\"name\":\"s\",\"type\":[\"null\",\"string\"]}]}";
        Schema schema = AvroFormat.parse(table);
        AvroFormat format = AvroFormat.of(Map.of(), Map.of(AvroFormat.LITERAL, table),
                AvroFormatTest.columns("i int;s string"));
        GenericRecord[] rows = new GenericRecord[5000];
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < rows.length; i++) {
            rows[i] = new GenericData.Record(schema);
            rows[i].put("i", i);
            rows[i].put("s", i % 7 == 0 ? null : "row " + i);
            expected.add(i + "|" + (i % 7 == 0 ? "null" : "row " + i));
        }
        for (CodecFactory codec : List.of(CodecFactory.nullCodec(), CodecFactory.deflateCodec(6),
                CodecFactory.snappyCodec(), CodecFactory.zstandardCodec(3),
                CodecFactory.bzip2Codec())) {
            assertEquals(expected, read(write(schema, Map.of(), codec, rows), format,
                    "i int;s string", List.of("i", "s")), codec.toString());
        }
    }

    @Test
    void eachProjectionReadsTheSameValuesAsTheWholeRow() throws IOException {
        String file = "{\"type\":\"record\",\"name\":\"t\",\"fields\":["
                + "{\"name\":\"e\",\"type\":\"long\"},{\"name\":\"a\",\"type\":\"int\"},"
                + "{\"name\":\"c\",\"type\":{\"type\":\"record\",\"name\":\"c\",\"fields\":["
                + "{\"name\":\"y\",\"type\":\"string\"},{\"name\":\"x\",\"type\":\"int\"}]}},"
                + "{\"name\":\"d\",\"type\":\"bytes\"},"
                + "{\"name\":\"u\",\"type\":[\"null\",\"int\"]},"
                + "{\"name\":\"l\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}]}";
        // Fields in another order, promotions (int -> double, bytes -> string, int -> long in a
        // list), a nested record in another field order, a field the file lacks.
        String table = "{\"type\":\"record\",\"name\":\"t\",\"fields\":["
                + "{\"name\":\"a\",\"type\":\"double\"},{\"name\":\"d\",\"type\":\"string\"},"
                + "{\"name\":\"c\",\"type\":{\"type\":\"record\",\"name\":\"c\",\"fields\":["
                + "{\"name\":\"x\",\"type\":\"int\"},{\"name\":\"y\",\"type\":\"string\"}]}},"
                + "{\"name\":\"u\",\"type\":[\"null\",\"int\"]},"
                + "{\"name\":\"l\",\"type\":{\"type\":\"array\",\"items\":\"long\"}},"
                + "{\"name\":\"z\",\"type\":[\"null\",\"string\"],\"default\":null},"
                + "{\"name\":\"e\",\"type\":\"long\"}]}";
        String spec = "a double;d string;c struct<x:int,y:string>;u int;"
                + "l array<bigint>;z string;e bigint";
        Schema schema = AvroFormat.parse(file);
        Schema nested = schema.getField("c").schema();
        GenericRecord[] rows = new GenericRecord[300];
        for (int i = 0; i < rows.length; i++) {
            GenericRecord c = new GenericData.Record(nested);
            c.put("y", "y" + i);
            c.put("x", -i);
            rows[i] = new GenericData.Record(schema);
            rows[i].put("e", i * 1_000_000_000L);
            rows[i].put("a", i);
            rows[i].put("c", c);
            rows[i].put("d", ByteBuffer.wrap(("d" + i).getBytes(StandardCharsets.UTF_8)));
            rows[i].put("u", i % 3 == 0 ? null : i);
            rows[i].put("l", List.of(i, i + 1));
        }
        byte[] data = write(schema, Map.of(), null, rows);
        List<String> names = List.of("a", "d", "c", "u", "l", "z", "e");
        AvroFormat format = AvroFormat.of(Map.of(), Map.of(AvroFormat.LITERAL, table),
                AvroFormatTest.columns(spec));
        List<String> whole = read(data, format, spec, names);
        assertEquals("1.0|d1|{x=-1, y=y1}|1|[1, 2]|null|1000000000", whole.get(1));
        for (int c = 0; c < names.size(); c++) {
            List<String> one = read(data, format, spec, List.of(names.get(c)));
            for (int r = 0; r < rows.length; r++) {
                assertEquals(whole.get(r).split("\\|")[c], one.get(r), names.get(c) + " row " + r);
            }
        }
    }

    @Test
    void byteRangesReadEveryBlockOnce() throws IOException {
        String table = "{\"type\":\"record\",\"name\":\"t\",\"fields\":[{\"name\":\"i\",\"type\":"
                + "\"int\"},{\"name\":\"s\",\"type\":[\"null\",\"string\"]}]}";
        Schema schema = AvroFormat.parse(table);
        AvroFormat format = AvroFormat.of(Map.of(), Map.of(AvroFormat.LITERAL, table),
                AvroFormatTest.columns("i int;s string"));
        GenericRecord[] rows = new GenericRecord[600];
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < rows.length; i++) {
            rows[i] = new GenericData.Record(schema);
            rows[i].put("i", i);
            rows[i].put("s", i % 7 == 0 ? null : "row " + i);
            expected.add(i + "|" + (i % 7 == 0 ? "null" : "row " + i));
        }
        for (CodecFactory codec : List.of(CodecFactory.nullCodec(), CodecFactory.deflateCodec(6))) {
            byte[] data = write(schema, Map.of(), codec, rows);
            for (int split = 1; split < data.length; split++) {
                List<String> read = new ArrayList<>(read(data, format, 0, split));
                read.addAll(read(data, format, split, Long.MAX_VALUE - split));
                assertEquals(expected, read, codec + " split at " + split);
            }
            for (int step : new int[] {1, 7, 100, 1000, 2048, data.length}) {
                List<String> read = new ArrayList<>();
                long offset = 0;
                for (long start : TableFilesReader.evenStarts(data.length, step)) {
                    read.addAll(read(data, format, offset, start - offset));
                    offset = start;
                }
                read.addAll(read(data, format, offset, Long.MAX_VALUE - offset));
                assertEquals(expected, read, codec + " ranges of " + step);
            }
            // The range holding the file's end still notices a file cut short.
            byte[] cut = Arrays.copyOf(data, data.length - 10);
            read(cut, format, 0, cut.length / 2);
            IOException e = assertThrows(IOException.class, () -> read(cut, format,
                    cut.length / 2, Long.MAX_VALUE - cut.length / 2));
            assertTrue(e.getMessage().contains("cut short"), e.getMessage());
        }
    }

    @Test
    void emptyAndBrokenFiles() throws IOException {
        String table = "{\"type\":\"record\",\"name\":\"t\",\"fields\":[{\"name\":\"i\",\"type\":"
                + "\"int\"}]}";
        AvroFormat format = AvroFormat.of(Map.of(), Map.of(AvroFormat.LITERAL, table),
                AvroFormatTest.columns("i int"));
        assertEquals(List.of(), read(new byte[0], format, "i int", List.of("i")));
        IOException e = assertThrows(IOException.class,
                () -> read("1\u00012\n".getBytes(StandardCharsets.UTF_8), format, "i int",
                        List.of("i")));
        assertTrue(e.getMessage().contains("not an Avro data file"), e.getMessage());
        Schema schema = AvroFormat.parse(table);
        GenericRecord row = new GenericData.Record(schema);
        row.put("i", 1);
        byte[] whole = write(schema, Map.of(), null, row, row, row);
        // The last block is 21 bytes: a cut inside its count, its data or its sync marker fails
        // (Avro's reader alone sees the first as the end of the file).
        for (int missing : new int[] {20, 10, 2}) {
            byte[] cut = Arrays.copyOf(whole, whole.length - missing);
            e = assertThrows(IOException.class, () -> read(cut, format, "i int", List.of("i")),
                    "cut by " + missing);
            assertTrue(e.getMessage().contains("test.avro"), e.getMessage());
        }
        assertEquals(List.of("1", "1", "1"), read(whole, format, "i int", List.of("i")));
    }

    private static Schema field(Schema record, String name) {
        Schema type = record.getField(name).schema();
        return AvroFormat.unwrapped(type);
    }

    private static byte[] write(Schema schema, Map<String, String> meta, CodecFactory codec,
            GenericRecord... rows) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(
                new GenericDatumWriter<>(schema))) {
            if (codec != null) {
                writer.setCodec(codec);
            }
            writer.setSyncInterval(2048);
            meta.forEach(writer::setMeta);
            writer.create(schema, out);
            for (GenericRecord row : rows) {
                writer.append(row);
            }
        }
        return out.toByteArray();
    }

    private List<String> read(byte[] data, AvroFormat format, long offset, long length)
            throws IOException {
        return read(data, format, "i int;s string", List.of("i", "s"), offset, length);
    }

    private List<String> read(byte[] data, AvroFormat format, String columnSpec,
            List<String> projection) throws IOException {
        return read(data, format, columnSpec, projection, 0L, Long.MAX_VALUE);
    }

    private List<String> read(byte[] data, AvroFormat format, String columnSpec,
            List<String> projection, long offset, long length) throws IOException {
        List<OrcArrowReader.Column> columns = new ArrayList<>();
        for (FieldSchema column : AvroFormatTest.columns(columnSpec)) {
            columns.add(new OrcArrowReader.Column(column.getName(), column.getType()));
        }
        List<String> rows = new ArrayList<>();
        try (AvroArrowReader reader = new AvroArrowReader(allocator, file(data), "test.avro", columns,
                projection, format, 1000, offset, length)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                for (int r = 0; r < root.getRowCount(); r++) {
                    StringBuilder row = new StringBuilder();
                    for (int c = 0; c < root.getFieldVectors().size(); c++) {
                        row.append(c > 0 ? "|" : "").append(render(root.getVector(c), r));
                    }
                    rows.add(row.toString());
                }
            }
        }
        return rows;
    }

    /**
     * Renders a value without Arrow's JSON text.
     */
    private static String render(FieldVector vector, int index) {
        if (vector.isNull(index)) {
            return "null";
        }
        if (vector instanceof MapVector map) {
            StructVector entries = (StructVector) map.getDataVector();
            Map<String, String> sorted = new TreeMap<>();
            for (int i = map.getElementStartIndex(index); i < map.getElementEndIndex(index); i++) {
                sorted.put(render((FieldVector) entries.getChildByOrdinal(0), i),
                        render((FieldVector) entries.getChildByOrdinal(1), i));
            }
            StringJoiner out = new StringJoiner(", ", "{", "}");
            sorted.forEach((k, v) -> out.add(k + "=" + v));
            return out.toString();
        }
        if (vector instanceof ListVector list) {
            StringJoiner out = new StringJoiner(", ", "[", "]");
            for (int i = list.getElementStartIndex(index); i < list.getElementEndIndex(index); i++) {
                out.add(render(list.getDataVector(), i));
            }
            return out.toString();
        }
        if (vector instanceof StructVector struct) {
            StringJoiner out = new StringJoiner(", ", "{", "}");
            for (int f = 0; f < struct.size(); f++) {
                FieldVector child = (FieldVector) struct.getChildByOrdinal(f);
                out.add(child.getName() + "=" + render(child, index));
            }
            return out.toString();
        }
        Object value = vector.getObject(index);
        if (vector instanceof DateDayVector) {
            value = LocalDate.ofEpochDay((Integer) value);
        }
        return String.valueOf(value instanceof byte[] raw ? new String(raw, StandardCharsets.UTF_8)
                : value);
    }

    private static ReadableFile file(byte[] data) {
        return new ReadableFile() {
            @Override
            public long length() {
                return data.length;
            }

            @Override
            public void readFully(long position, byte[] buffer, int offset, int length)
                    throws IOException {
                if (position + length > data.length) {
                    throw new EOFException();
                }
                System.arraycopy(data, (int) position, buffer, offset, length);
            }
        };
    }
}
