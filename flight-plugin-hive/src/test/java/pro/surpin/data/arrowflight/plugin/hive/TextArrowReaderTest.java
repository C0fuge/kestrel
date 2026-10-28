package pro.surpin.data.arrowflight.plugin.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.spi.storage.ReadableFile;

/**
 * One Hive text file read as Hive's LazySimpleSerDe reads it: delimiters, escapes, NULL text,
 * header lines, missing and extra fields, line ends, compression, projections.
 */
@Tag("unit")
class TextArrowReaderTest {

    private static final List<OrcArrowReader.Column> COLUMNS = List.of(
            new OrcArrowReader.Column("i", "int"), new OrcArrowReader.Column("s", "string"),
            new OrcArrowReader.Column("d", "decimal(6,2)"), new OrcArrowReader.Column("dt", "date"),
            new OrcArrowReader.Column("ts", "timestamp"), new OrcArrowReader.Column("b", "boolean"),
            new OrcArrowReader.Column("v", "varchar(3)"), new OrcArrowReader.Column("c", "char(4)"),
            new OrcArrowReader.Column("bin", "binary"), new OrcArrowReader.Column("t", "tinyint"),
            new OrcArrowReader.Column("sm", "smallint"), new OrcArrowReader.Column("l", "bigint"),
            new OrcArrowReader.Column("f", "float"), new OrcArrowReader.Column("db", "double"));
    private static final List<String> ALL = List.of("i", "s", "d", "dt", "ts", "b", "v", "c", "bin",
            "t", "sm", "l", "f", "db");

    private final BufferAllocator allocator = new RootAllocator();

    @AfterEach
    void close() {
        assertEquals(0, allocator.getAllocatedMemory());
        allocator.close();
    }

    @Test
    void everyTypeWithHiveDefaults() throws Exception {
        String text = "1\u0001a\u00012.345\u00012024-02-29\u00012024-01-02 03:04:05.5\u0001TRUE"
                + "\u0001abcd\u0001ab  \u0001aGk=\u0001-128\u0001300\u00019000000000\u00011.5\u0001-2e10\n"
                + "\\N\u0001\\N\u0001x\u0001bad\u0001bad\u0001yes\u0001\u0001\u0001\u0001128\u0001"
                + "70000\u0001x\u0001x\u0001x\n";
        assertEquals(List.of(
                "1|a|2.35|2024-02-29|2024-01-02T03:04:05.500|true|abc|ab|hi|-128|300|9000000000|1.5"
                        + "|-2.0E10",
                "null|null|null|null|null|null||||null|null|null|null|null"),
                read(text, format(Map.of()), ALL, "f.txt"));
    }

    @Test
    void delimitersEscapesNullTextAndHeaders() throws Exception {
        Map<String, String> serde = Map.of("field.delim", ",", "escape.delim", "\\",
                "serialization.null.format", "");
        String text = "i,s\nheader two\n1,a\\,b\\\\c\n,\n2\n3,x,ignored\n";
        TextFormat format = TextFormat.of(serde, Map.of("skip.header.line.count", "2"));
        assertEquals(List.of("1|a,b\\c", "null|null", "2|null", "3|x"),
                read(text, format, List.of("i", "s"), "f.csv"));
        // An escaped delimiter in a number keeps it from reading as one.
        assertEquals(List.of("null"), read("1\\,2", format(serde), List.of("i"), "f"));
        Map<String, String> crlf = new HashMap<>(serde);
        crlf.put("serialization.escape.crlf", "true");
        assertEquals(List.of("a\nb\rc"), read("a\\nb\\rc", format(crlf), List.of("s"),
                "f", List.of(new OrcArrowReader.Column("s", "string"))));
        assertEquals(List.of("anb"), read("a\\nb", format(serde), List.of("s"),
                "f", List.of(new OrcArrowReader.Column("s", "string"))));
        // Byte numbers name delimiters, as in Hive.
        assertEquals(List.of("1|x"), read("1\tx", format(Map.of("field.delim", "9")),
                List.of("i", "s"), "f"));
        assertEquals(List.of("1|x"), read("1|x", format(Map.of("serialization.format", "|")),
                List.of("i", "s"), "f"));
    }

    @Test
    void lineEndsEmptyLinesAndTheLastColumnTakingTheRest() throws Exception {
        List<OrcArrowReader.Column> two = List.of(new OrcArrowReader.Column("a", "string"),
                new OrcArrowReader.Column("b", "string"));
        TextFormat comma = format(Map.of("field.delim", ","));
        assertEquals(List.of("1|x", "2|y", "3|z", "|null", "4|null"),
                read("1,x\r\n2,y\r3,z\n\n4", comma, List.of("a", "b"), "f", two));
        assertEquals(List.of("1|x", "|null"), read("1,x\r\r\n", comma, List.of("a", "b"), "f", two));
        assertEquals(List.of("x,y,z"), read("1,x,y,z", format(Map.of("field.delim", ",",
                "serialization.last.column.takes.rest", "true")), List.of("b"), "f", two));
        assertEquals(List.of("x"), read("1,x,y,z", comma, List.of("b"), "f", two));
    }

    @Test
    void projectionsAndRowCountsWithoutColumns() throws Exception {
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < 10_000; i++) {
            text.append(i).append('\u0001').append("s").append(i).append('\n');
        }
        try (TextArrowReader reader = reader(bytes(text.toString()), format(Map.of()), List.of(),
                "f", COLUMNS, 4096)) {
            int rows = 0;
            int batches = 0;
            while (reader.loadNextBatch()) {
                rows += reader.getVectorSchemaRoot().getRowCount();
                batches++;
            }
            assertEquals(10_000, rows);
            assertEquals(3, batches);
        }
        List<String> rows = read(text.toString(), format(Map.of()), List.of("s", "i"), "f");
        assertEquals(10_000, rows.size());
        assertEquals("s9999|9999", rows.get(9999));
        assertThrows(IllegalArgumentException.class, () -> reader(new byte[0], format(Map.of()),
                List.of("nope"), "f", COLUMNS, 10));
    }

    @Test
    void compressedFilesByTheirSuffix() throws Exception {
        String text = "1\u0001a\n2\u0001b\n";
        List<String> expected = List.of("1|a", "2|b");
        ByteArrayOutputStream gz = new ByteArrayOutputStream();
        // Two gzip members, as concatenated Hadoop outputs have.
        for (String part : new String[] {"1\u0001a\n", "2\u0001b\n"}) {
            try (OutputStream out = new GZIPOutputStream(gz)) {
                out.write(bytes(part));
            }
        }
        assertEquals(expected, read(gz.toByteArray(), "000000_0.gz"));
        ByteArrayOutputStream deflate = new ByteArrayOutputStream();
        try (OutputStream out = new DeflaterOutputStream(deflate)) {
            out.write(bytes(text));
        }
        assertEquals(expected, read(deflate.toByteArray(), "000000_0.deflate"));
        ByteArrayOutputStream bz2 = new ByteArrayOutputStream();
        BZip2Codec codec = new BZip2Codec();
        codec.setConf(new Configuration(false));
        try (OutputStream out = codec.createOutputStream(bz2)) {
            out.write(bytes(text));
        }
        assertEquals(expected, read(bz2.toByteArray(), "000000_0.bz2"));
        assertEquals(expected, read(bytes(text), "000000_0.txt"));
        IOException e = assertThrows(IOException.class, () -> read(bytes(text), "x/000000_0.snappy"));
        assertTrue(e.getMessage().contains("snappy") && e.getMessage().contains("x/000000_0"),
                e.getMessage());
        assertThrows(IOException.class, () -> read(bytes("not gzip"), "000000_0.gz"));
    }

    @Test
    void formatsAreReadFromPropertiesAndRefusedWhenNotRead() {
        TextFormat defaults = format(Map.of());
        assertEquals(1, defaults.fieldDelimiter());
        assertEquals(-1, defaults.escape());
        assertEquals("\\N", new String(defaults.nullSequence(), StandardCharsets.UTF_8));
        // Table properties win over serde properties and apply to partitions too.
        TextFormat table = TextFormat.of(Map.of("field.delim", ","), Map.of("field.delim", ";",
                "skip.header.line.count", "1", "numFiles", "3"));
        assertEquals(';', table.fieldDelimiter());
        assertEquals(table, table.forPartition(Map.of("field.delim", "|")));
        assertNotEquals(TextFormat.of(Map.of("field.delim", ","), Map.of()),
                TextFormat.of(Map.of("field.delim", ","), Map.of()).forPartition(Map.of()));
        assertNotEquals(defaults.canonical(), format(Map.of("serialization.null.format", ""))
                .canonical());
        for (Map<String, String> refused : List.of(Map.of("line.delim", "\r"),
                Map.of("serialization.encoding", "GBK"), Map.of("skip.footer.line.count", "1"),
                Map.of("timestamp.formats", "millis"), Map.of("field.delim", "\u00fe"),
                Map.of("field.delim", ",", "escape.delim", ","), Map.of("skip.header.line.count", "x"))) {
            assertThrows(IllegalArgumentException.class, () -> format(refused), refused.toString());
        }
        assertEquals(1, format(Map.of("line.delim", "\n")).fieldDelimiter());
        assertEquals(1, format(Map.of("line.delim", "10")).fieldDelimiter());
        assertEquals(List.of(":", ";"), List.of(
                String.valueOf((char) format(Map.of("colelction.delim", ":")).separator(1)),
                String.valueOf((char) format(Map.of("colelction.delim", ":",
                        "collection.delim", ";")).separator(1))));
        assertNotEquals(defaults.canonical(), format(Map.of("mapkey.delim", "=")).canonical());
        for (Map<String, String> refused : List.of(Map.of("collection.delim", "\u00fe"),
                Map.of("mapkey.delim", "\n"), Map.of("escape.delim", "\\", "mapkey.delim", "\\"))) {
            assertThrows(IllegalArgumentException.class, () -> format(refused), refused.toString());
        }
    }

    @Test
    void nestedColumnsAreSplitAtTheSeparatorsOfTheirLevel() throws Exception {
        List<OrcArrowReader.Column> columns = List.of(new OrcArrowReader.Column("a", "array<int>"),
                new OrcArrowReader.Column("m", "map<string,int>"),
                new OrcArrowReader.Column("st", "struct<x:int,y:string,z:array<string>>"),
                new OrcArrowReader.Column("aa", "array<array<int>>"),
                new OrcArrowReader.Column("ma", "map<string,array<int>>"));
        List<String> all = List.of("a", "m", "st", "aa", "ma");
        String text = String.join("\u0001", "1\u00022\u0002x\u0002",
                "k1\u00031\u0002k2\u0003\u0002k1\u00039\u0002\\N\u00035\u0002k3",
                "7\u0002hi\u0002p\u0003q", "1\u00032\u00023", "k\u00031\u00042\u0002j\u0003") + "\n"
                + "\u0001\u0001\\N\u0001\\N\u0001\n"
                + "5\n"
                + "\u0001\u0001\u0001\u0003\u0002\u0001\\N\u00031\n";
        assertEquals(List.of(
                "[1, 2, null, null]|{k1=1, k2=null, k3=null}|{x=7, y=hi, z=[p, q]}|[[1, 2], [3]]"
                        + "|{k=[1, 2], j=[]}",
                "[]|{}|null|null|{}",
                "[5]|null|null|null|null",
                "[]|{}|{x=null, y=null, z=null}|[[null, null], []]|{}"),
                read(text, format(Map.of()), all, "f", columns));
        // Own separators and escapes; struct fields beyond the text are NULL.
        TextFormat own = format(Map.of("field.delim", "|", "collection.delim", ",",
                "mapkey.delim", ":", "escape.delim", "\\"));
        List<OrcArrowReader.Column> two = List.of(new OrcArrowReader.Column("a", "array<string>"),
                new OrcArrowReader.Column("st", "struct<x:int,m:map<int,boolean>>"));
        // The map in the struct is on level 2: entries split at the map key separator ':', keys
        // from values at byte 4 (absent: NULL values); keys that are not ints are dropped.
        assertEquals(List.of("[x,y, z]|{x=1, m={2=null, 3=null}}", "[a:b]|{x=3, m=null}"),
                read("x\\,y,z|1,2:true:x:false:3\na:b|3\n", own, List.of("a", "st"), "f", two));
        for (String type : List.of("uniontype<int,string>", "map<array<int>,int>",
                "array<array<array<array<array<array<array<array<int>>>>>>>>",
                "array<array<array<array<array<array<map<int,int>>>>>>>")) {
            assertThrows(IllegalArgumentException.class, () -> TextArrowReader.checkType("c",
                    org.apache.orc.TypeDescription.fromString(type)), type);
        }
        TextArrowReader.checkType("c", org.apache.orc.TypeDescription.fromString(
                "array<array<array<array<array<array<array<int>>>>>>>"));
        TextArrowReader.checkType("c", org.apache.orc.TypeDescription.fromString(
                "array<array<array<array<array<map<int,int>>>>>>"));
    }

    private static TextFormat format(Map<String, String> serde) {
        return TextFormat.of(serde, Map.of());
    }

    private List<String> read(byte[] data, String name) throws IOException {
        return read(data, format(Map.of()), List.of("i", "s"), name, COLUMNS);
    }

    private List<String> read(String text, TextFormat format, List<String> projection, String name)
            throws IOException {
        return read(bytes(text), format, projection, name, COLUMNS);
    }

    private List<String> read(String text, TextFormat format, List<String> projection, String name,
            List<OrcArrowReader.Column> columns) throws IOException {
        return read(bytes(text), format, projection, name, columns);
    }

    private List<String> read(byte[] data, TextFormat format, List<String> projection, String name,
            List<OrcArrowReader.Column> columns) throws IOException {
        List<String> rows = new ArrayList<>();
        try (TextArrowReader reader = reader(data, format, projection, name, columns, 3)) {
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
     * Renders a value without Arrow's JSON text (the test class path has an older Jackson).
     */
    private static String render(FieldVector vector, int index) {
        if (vector.isNull(index)) {
            return "null";
        }
        if (vector instanceof MapVector map) {
            StructVector entries = (StructVector) map.getDataVector();
            StringJoiner out = new StringJoiner(", ", "{", "}");
            for (int i = map.getElementStartIndex(index); i < map.getElementEndIndex(index); i++) {
                out.add(render((FieldVector) entries.getChildByOrdinal(0), i) + "="
                        + render((FieldVector) entries.getChildByOrdinal(1), i));
            }
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

    private TextArrowReader reader(byte[] data, TextFormat format, List<String> projection,
            String name, List<OrcArrowReader.Column> columns, int batchSize) throws IOException {
        return new TextArrowReader(allocator, file(data), name, columns, projection, format,
                batchSize);
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

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
