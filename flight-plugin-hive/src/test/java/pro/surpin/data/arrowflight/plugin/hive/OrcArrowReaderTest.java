package pro.surpin.data.arrowflight.plugin.hive;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.spi.storage.ReadableFile;

/**
 * ORC files written in the test, read back as Arrow batches through a store's readable file.
 */
@Tag("unit")
class OrcArrowReaderTest {

    private static final String ALL_TYPES = "struct<b:boolean,ti:tinyint,si:smallint,i:int,"
            + "bi:bigint,f:float,d:double,dec:decimal(12,2),s:string,vc:varchar(10),c:char(5),"
            + "dt:date,ts:timestamp,bin:binary,l:array<int>,m:map<string,int>,"
            + "st:struct<x:int,y:string>>";
    private static final int ROWS = 2500;
    private static final LocalDateTime BASE = LocalDateTime.of(2020, 6, 1, 1, 30);

    @TempDir
    Path dir;

    private BufferAllocator allocator;
    private TimeZone zone;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        zone = TimeZone.getDefault();
        // Not UTC: timestamps must keep their wall clock.
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Berlin"));
    }

    @AfterEach
    void tearDown() {
        TimeZone.setDefault(zone);
        allocator.close();
    }

    @Test
    void readsEveryHiveTypeWithNulls() throws Exception {
        Path file = dir.resolve("all.orc");
        writeAllTypes(file, ALL_TYPES);
        List<OrcArrowReader.Column> columns = columns(ALL_TYPES);
        List<String> names = columns.stream().map(OrcArrowReader.Column::name).toList();
        int row = 0;
        int batches = 0;
        try (OrcArrowReader reader = reader(file, columns, names, null)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertEquals(new ArrowType.Decimal(12, 2, 128),
                    root.getSchema().findField("dec").getType());
            while (reader.loadNextBatch()) {
                batches++;
                for (int i = 0; i < root.getRowCount(); i++, row++) {
                    checkRow(root, i, row);
                }
            }
        }
        assertEquals(ROWS, row);
        assertEquals(3, batches);
    }

    @Test
    void readsProjectedColumnsByNameWhateverTheCase() throws Exception {
        Path file = dir.resolve("all.orc");
        writeAllTypes(file, ALL_TYPES);
        List<OrcArrowReader.Column> columns = new ArrayList<>();
        for (OrcArrowReader.Column column : columns(ALL_TYPES)) {
            columns.add(new OrcArrowReader.Column(column.name().toUpperCase(java.util.Locale.ROOT),
                    column.hiveType()));
        }
        columns.add(new OrcArrowReader.Column("added", "int"));
        try (OrcArrowReader reader = reader(file, columns, List.of("added", "S", "I"), null)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertEquals(List.of("added", "S", "I"), root.getSchema().getFields().stream()
                    .map(f -> f.getName()).toList());
            assertTrue(reader.loadNextBatch());
            assertNull(root.getVector(0).getObject(0));
            assertEquals("s1", root.getVector(1).getObject(1).toString());
            assertEquals(6, root.getVector(2).getObject(2));
        }
    }

    @Test
    void readsHiveOneFilesByPosition() throws Exception {
        Path file = dir.resolve("old.orc");
        TypeDescription schema = TypeDescription.fromString("struct<_col0:int,_col1:string>");
        try (Writer writer = OrcFile.createWriter(hadoopPath(file),
                OrcFile.writerOptions(new Configuration(false)).setSchema(schema))) {
            VectorizedRowBatch batch = schema.createRowBatch();
            for (int r = 0; r < 3; r++) {
                ((LongColumnVector) batch.cols[0]).vector[r] = r + 10;
                ((BytesColumnVector) batch.cols[1]).setVal(r, ("n" + r).getBytes(StandardCharsets.UTF_8));
            }
            batch.size = 3;
            writer.addRowBatch(batch);
        }
        List<OrcArrowReader.Column> columns = List.of(new OrcArrowReader.Column("id", "bigint"),
                new OrcArrowReader.Column("name", "string"));
        try (OrcArrowReader reader = reader(file, columns, List.of("name", "id"), null)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertTrue(reader.loadNextBatch());
            assertEquals(3, root.getRowCount());
            assertEquals("n2", root.getVector(0).getObject(2).toString());
            assertEquals(12L, root.getVector(1).getObject(2));
        }
    }

    @Test
    void searchArgumentSkipsRowGroupsButKeepsMatches() throws Exception {
        Path file = dir.resolve("sorted.orc");
        TypeDescription schema = TypeDescription.fromString("struct<i:int>");
        try (Writer writer = OrcFile.createWriter(hadoopPath(file),
                OrcFile.writerOptions(new Configuration(false)).setSchema(schema)
                        .rowIndexStride(1000))) {
            VectorizedRowBatch batch = schema.createRowBatch(1000);
            for (int r = 0; r < 10_000; r++) {
                ((LongColumnVector) batch.cols[0]).vector[batch.size++] = r;
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
        }
        SearchArgument sarg = SearchArgumentFactory.newBuilder().startAnd()
                .equals("i", PredicateLeaf.Type.LONG, 4500L).end().build();
        List<OrcArrowReader.Column> columns = List.of(new OrcArrowReader.Column("i", "int"));
        int rows = 0;
        boolean found = false;
        try (OrcArrowReader reader = reader(file, columns, List.of("i"), sarg)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                for (int i = 0; i < root.getRowCount(); i++) {
                    found |= Integer.valueOf(4500).equals(root.getVector(0).getObject(i));
                }
                rows += root.getRowCount();
            }
        }
        assertTrue(found);
        assertEquals(1000, rows);
    }

    @Test
    void readsDecimalsOfEveryWidthAndWidenedScales() throws Exception {
        Path file = dir.resolve("dec.orc");
        TypeDescription schema = TypeDescription.fromString(
                "struct<a:decimal(18,4),b:decimal(38,6),c:decimal(10,2),d:decimal(9,0)>");
        long[] unscaled = {0L, 1L, -1L, 999_999_999_999_999_999L, -123_456_789_012_345L};
        try (Writer writer = OrcFile.createWriter(hadoopPath(file),
                OrcFile.writerOptions(new Configuration(false)).setSchema(schema))) {
            VectorizedRowBatch batch = schema.createRowBatch();
            for (int r = 0; r < unscaled.length; r++) {
                ((DecimalColumnVector) batch.cols[0]).set(r,
                        HiveDecimal.create(BigDecimal.valueOf(unscaled[r], 4)));
                ((DecimalColumnVector) batch.cols[1]).set(r, HiveDecimal.create(
                        BigDecimal.valueOf(unscaled[r], 6).multiply(BigDecimal.TEN.pow(12))));
                ((DecimalColumnVector) batch.cols[2]).set(r,
                        HiveDecimal.create(BigDecimal.valueOf(unscaled[r] % 10_000_000_000L, 2)));
                ((DecimalColumnVector) batch.cols[3]).set(r,
                        HiveDecimal.create(BigDecimal.valueOf(unscaled[r] % 1_000_000_000L)));
            }
            batch.cols[3].noNulls = false;
            batch.cols[3].isNull[1] = true;
            batch.size = unscaled.length;
            writer.addRowBatch(batch);
        }
        // c and d widened by the metastore's declaration: decimal(10,2) -> (14,4), (9,0) -> (12,3).
        List<OrcArrowReader.Column> columns = List.of(new OrcArrowReader.Column("a", "decimal(18,4)"),
                new OrcArrowReader.Column("b", "decimal(38,6)"),
                new OrcArrowReader.Column("c", "decimal(14,4)"),
                new OrcArrowReader.Column("d", "decimal(12,3)"));
        try (OrcArrowReader reader = reader(file, columns, List.of("a", "b", "c", "d"), null)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertTrue(reader.loadNextBatch());
            assertEquals(unscaled.length, root.getRowCount());
            for (int r = 0; r < unscaled.length; r++) {
                assertEquals(BigDecimal.valueOf(unscaled[r], 4), root.getVector("a").getObject(r));
                assertEquals(BigDecimal.valueOf(unscaled[r], 6).multiply(BigDecimal.TEN.pow(12))
                        .setScale(6), root.getVector("b").getObject(r));
                assertEquals(BigDecimal.valueOf(unscaled[r] % 10_000_000_000L, 2).setScale(4),
                        root.getVector("c").getObject(r));
                Object d = root.getVector("d").getObject(r);
                if (r == 1) {
                    assertNull(d);
                } else {
                    assertEquals(BigDecimal.valueOf(unscaled[r] % 1_000_000_000L).setScale(3), d);
                }
            }
        }
    }

    @Test
    void refusesUnknownProjectedColumns() throws Exception {
        Path file = dir.resolve("all.orc");
        writeAllTypes(file, ALL_TYPES);
        IOException e = assertThrows(IOException.class,
                () -> reader(file, columns(ALL_TYPES), List.of("nope"), null));
        assertTrue(e.getMessage().contains("nope"), e.getMessage());
    }

    private OrcArrowReader reader(Path file, List<OrcArrowReader.Column> columns,
            List<String> projection, SearchArgument sarg) throws IOException {
        TestSupport.LocalStore store = new TestSupport.LocalStore(dir.toString());
        ReadableFile readable = store.open(file.getFileName().toString());
        return new OrcArrowReader(allocator, readable, file.toString(), columns, projection, sarg,
                1000);
    }

    private static List<OrcArrowReader.Column> columns(String struct) {
        TypeDescription type = TypeDescription.fromString(struct);
        List<OrcArrowReader.Column> result = new ArrayList<>();
        for (int i = 0; i < type.getChildren().size(); i++) {
            result.add(new OrcArrowReader.Column(type.getFieldNames().get(i),
                    type.getChildren().get(i).toString()));
        }
        return result;
    }

    private static org.apache.hadoop.fs.Path hadoopPath(Path file) {
        return new org.apache.hadoop.fs.Path(file.toUri());
    }

    private static boolean allNull(int r) {
        return r % 7 == 3;
    }

    private static LocalDateTime timestamp(int r) {
        return BASE.plusMinutes(r).plusNanos(123_456_000L);
    }

    private static void writeAllTypes(Path file, String struct) throws IOException {
        TypeDescription schema = TypeDescription.fromString(struct);
        try (Writer writer = OrcFile.createWriter(hadoopPath(file),
                OrcFile.writerOptions(new Configuration(false)).setSchema(schema)
                        .rowIndexStride(1000))) {
            VectorizedRowBatch batch = schema.createRowBatch(700);
            for (int r = 0; r < ROWS; r++) {
                int row = batch.size++;
                if (allNull(r)) {
                    for (ColumnVector column : batch.cols) {
                        column.noNulls = false;
                        column.isNull[row] = true;
                    }
                } else {
                    fill(batch.cols, row, r);
                }
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (batch.size > 0) {
                writer.addRowBatch(batch);
            }
        }
    }

    private static void fill(ColumnVector[] cols, int row, int r) {
        ((LongColumnVector) cols[0]).vector[row] = r % 2 == 0 ? 1 : 0;
        ((LongColumnVector) cols[1]).vector[row] = (byte) (r % 100);
        ((LongColumnVector) cols[2]).vector[row] = (short) r;
        ((LongColumnVector) cols[3]).vector[row] = r * 3;
        ((LongColumnVector) cols[4]).vector[row] = r * 1_000_000_000L;
        ((DoubleColumnVector) cols[5]).vector[row] = r / 4f;
        ((DoubleColumnVector) cols[6]).vector[row] = r / 8.0;
        ((DecimalColumnVector) cols[7]).set(row, HiveDecimal.create(BigDecimal.valueOf(r * 100L + 50, 2)));
        ((BytesColumnVector) cols[8]).setVal(row, ("s" + r + (r % 3 == 0 ? "ü" : ""))
                .getBytes(StandardCharsets.UTF_8));
        ((BytesColumnVector) cols[9]).setVal(row, ("v" + r % 1000).getBytes(StandardCharsets.UTF_8));
        ((BytesColumnVector) cols[10]).setVal(row, ("c" + r % 10).getBytes(StandardCharsets.UTF_8));
        ((LongColumnVector) cols[11]).vector[row] = 18_000 + r;
        ((TimestampColumnVector) cols[12]).set(row, Timestamp.valueOf(timestamp(r)));
        ((BytesColumnVector) cols[13]).setVal(row, binary(r));
        ListColumnVector list = (ListColumnVector) cols[14];
        int length = r % 5;
        list.offsets[row] = list.childCount;
        list.lengths[row] = length;
        list.childCount += length;
        list.child.ensureSize(list.childCount, true);
        LongColumnVector elements = (LongColumnVector) list.child;
        for (int j = 0; j < length; j++) {
            int at = (int) list.offsets[row] + j;
            if (j == 1) {
                elements.noNulls = false;
                elements.isNull[at] = true;
            } else {
                elements.isNull[at] = false;
                elements.vector[at] = r + j;
            }
        }
        MapColumnVector map = (MapColumnVector) cols[15];
        int entries = r % 3;
        map.offsets[row] = map.childCount;
        map.lengths[row] = entries;
        map.childCount += entries;
        map.keys.ensureSize(map.childCount, true);
        map.values.ensureSize(map.childCount, true);
        for (int j = 0; j < entries; j++) {
            int at = (int) map.offsets[row] + j;
            ((BytesColumnVector) map.keys).setVal(at, ("k" + j).getBytes(StandardCharsets.UTF_8));
            ((LongColumnVector) map.values).vector[at] = j * 10L;
        }
        StructColumnVector struct = (StructColumnVector) cols[16];
        if (r % 11 == 5) {
            struct.noNulls = false;
            struct.isNull[row] = true;
        } else {
            ((LongColumnVector) struct.fields[0]).vector[row] = -r;
            BytesColumnVector y = (BytesColumnVector) struct.fields[1];
            if (r % 2 == 1) {
                y.noNulls = false;
                y.isNull[row] = true;
            } else {
                y.setVal(row, ("y" + r).getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private static byte[] binary(int r) {
        byte[] result = new byte[r % 4];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) (r + i);
        }
        return result;
    }

    private static void checkRow(VectorSchemaRoot root, int i, int r) {
        String at = "row " + r;
        if (allNull(r)) {
            for (FieldVector vector : root.getFieldVectors()) {
                assertTrue(vector.isNull(i), at + " " + vector.getName());
            }
            return;
        }
        assertEquals(r % 2 == 0, root.getVector("b").getObject(i), at);
        assertEquals((byte) (r % 100), root.getVector("ti").getObject(i), at);
        assertEquals((short) r, root.getVector("si").getObject(i), at);
        assertEquals(r * 3, root.getVector("i").getObject(i), at);
        assertEquals(r * 1_000_000_000L, root.getVector("bi").getObject(i), at);
        assertEquals(r / 4f, root.getVector("f").getObject(i), at);
        assertEquals(r / 8.0, root.getVector("d").getObject(i), at);
        assertEquals(BigDecimal.valueOf(r * 100L + 50, 2),
                ((DecimalVector) root.getVector("dec")).getObject(i), at);
        assertEquals("s" + r + (r % 3 == 0 ? "ü" : ""), root.getVector("s").getObject(i).toString(), at);
        assertEquals("v" + r % 1000, root.getVector("vc").getObject(i).toString(), at);
        assertEquals("c" + r % 10, root.getVector("c").getObject(i).toString(), at);
        assertEquals(18_000 + r, root.getVector("dt").getObject(i), at);
        assertEquals(timestamp(r), ((TimeStampMicroVector) root.getVector("ts")).getObject(i), at);
        assertEquals(timestamp(r).toEpochSecond(ZoneOffset.UTC) * 1_000_000L + 123_456L,
                ((TimeStampMicroVector) root.getVector("ts")).get(i), at);
        assertArrayEquals(binary(r), (byte[]) root.getVector("bin").getObject(i), at);
        List<Integer> expectedList = new ArrayList<>();
        for (int j = 0; j < r % 5; j++) {
            expectedList.add(j == 1 ? null : r + j);
        }
        // Element by element: getObject of nested vectors needs a newer Jackson than the
        // test metastore's.
        ListVector list = (ListVector) root.getVector("l");
        List<Object> actualList = new ArrayList<>();
        for (int j = list.getElementStartIndex(i); j < list.getElementEndIndex(i); j++) {
            actualList.add(list.getDataVector().getObject(j));
        }
        assertEquals(expectedList, actualList, at);
        Map<String, Long> expectedMap = new LinkedHashMap<>();
        for (int j = 0; j < r % 3; j++) {
            expectedMap.put("k" + j, j * 10L);
        }
        Map<String, Long> map = new LinkedHashMap<>();
        MapVector mapVector = (MapVector) root.getVector("m");
        StructVector entries = (StructVector) mapVector.getDataVector();
        for (int j = mapVector.getElementStartIndex(i); j < mapVector.getElementEndIndex(i); j++) {
            map.put(entries.getChild("key").getObject(j).toString(),
                    ((Integer) entries.getChild("value").getObject(j)).longValue());
        }
        assertEquals(expectedMap, map, at);
        StructVector struct = (StructVector) root.getVector("st");
        if (r % 11 == 5) {
            assertTrue(struct.isNull(i), at);
        } else {
            assertEquals(-r, struct.getChild("x").getObject(i), at);
            Object y = struct.getChild("y").getObject(i);
            assertEquals(r % 2 == 1 ? null : "y" + r, y == null ? null : y.toString(), at);
        }
    }
}
