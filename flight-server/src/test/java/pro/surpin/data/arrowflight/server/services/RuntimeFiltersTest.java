package pro.surpin.data.arrowflight.server.services;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class RuntimeFiltersTest {

    private static final Function<String, String> PLAIN = name -> "\"" + name + "\"";

    private static Schema schema(Field... fields) {
        return new Schema(List.of(fields));
    }

    private static Field field(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), null);
    }

    @Test
    void decodesWhatEncodeWrites() {
        List<RuntimeFilters.InFilter> filters = List.of(
                new RuntimeFilters.InFilter("o_orderkey", new long[] {3, 1, 2}),
                new RuntimeFilters.InFilter("l_suppkey", new long[0]));
        RuntimeFilters decoded = RuntimeFilters.decode(RuntimeFilters.encode(filters));
        assertEquals(filters, decoded.in());
        assertEquals(0, decoded.skippedBlooms());
        assertEquals(3, decoded.keyCount());
    }

    @Test
    void readsTheInFiltersOfABloomBlockAndSkipsItsBloomFilters() {
        byte[] in = RuntimeFilters.encode(List.of(new RuntimeFilters.InFilter("k", new long[] {7})));
        byte[] name = "k".getBytes(StandardCharsets.UTF_8);
        int directory = 32 << 1;
        ByteBuffer block = ByteBuffer.allocate(in.length + 4 + 4 + name.length + 4 + 4 + 8
                + directory + 4 + 9).order(ByteOrder.LITTLE_ENDIAN);
        block.put(in).putInt(0, RuntimeFilters.MAGIC_BLOOM);
        block.putInt(1).putInt(name.length).put(name).putInt(1).putInt(1).putLong(directory);
        block.put(new byte[directory]).putInt(1).putLong(5).put((byte) 1);
        RuntimeFilters decoded = RuntimeFilters.decode(block.array());
        assertEquals(1, decoded.in().size());
        assertEquals(1, decoded.skippedBlooms());
    }

    @Test
    void refusesMalformedBlocks() {
        byte[] good = RuntimeFilters.encode(List.of(new RuntimeFilters.InFilter("k", new long[] {1, 2})));
        assertThrows(IllegalArgumentException.class, () -> RuntimeFilters.decode(new byte[0]));
        byte[] magic = good.clone();
        magic[0] = 'X';
        assertThrows(IllegalArgumentException.class, () -> RuntimeFilters.decode(magic));
        assertThrows(IllegalArgumentException.class,
                () -> RuntimeFilters.decode(Arrays.copyOf(good, good.length - 1)));
        assertThrows(IllegalArgumentException.class,
                () -> RuntimeFilters.decode(Arrays.copyOf(good, good.length + 1)));
        byte[] count = good.clone();
        ByteBuffer.wrap(count).order(ByteOrder.LITTLE_ENDIAN).putInt(4, RuntimeFilters.MAX_FILTERS + 1);
        assertThrows(IllegalArgumentException.class, () -> RuntimeFilters.decode(count));
        byte[] negative = good.clone();
        ByteBuffer.wrap(negative).order(ByteOrder.LITTLE_ENDIAN).putInt(4, -1);
        assertThrows(IllegalArgumentException.class, () -> RuntimeFilters.decode(negative));
        byte[] nameLength = good.clone();
        ByteBuffer.wrap(nameLength).order(ByteOrder.LITTLE_ENDIAN).putInt(8, 1 << 20);
        assertThrows(IllegalArgumentException.class, () -> RuntimeFilters.decode(nameLength));
        byte[] keys = good.clone();
        ByteBuffer.wrap(keys).order(ByteOrder.LITTLE_ENDIAN).putInt(13, Integer.MAX_VALUE);
        assertThrows(IllegalArgumentException.class, () -> RuntimeFilters.decode(keys));
        byte[] utf8 = good.clone();
        utf8[12] = (byte) 0xC3;
        assertThrows(IllegalArgumentException.class, () -> RuntimeFilters.decode(utf8));
    }

    @Test
    void refusesABloomFilterWhoseDirectoryDoesNotFitItsSize() {
        byte[] in = RuntimeFilters.encode(List.of());
        ByteBuffer block = ByteBuffer.allocate(in.length + 4 + 4 + 1 + 4 + 4 + 8 + 10)
                .order(ByteOrder.LITTLE_ENDIAN);
        block.put(in).putInt(0, RuntimeFilters.MAGIC_BLOOM);
        block.putInt(1).putInt(1).put((byte) 'k').putInt(1).putInt(1).putLong(10).put(new byte[10]);
        assertThrows(IllegalArgumentException.class, () -> RuntimeFilters.decode(block.array()));
    }

    @Test
    void writesRangeAndInListForAFewKeys() {
        RuntimeFilters filters = new RuntimeFilters(List.of(
                new RuntimeFilters.InFilter("K", new long[] {9, 3, 3, 5})), 0);
        String condition = filters.condition(schema(field("k", new ArrowType.Int(64, true))), PLAIN);
        assertEquals("(\"k\" BETWEEN 3 AND 9 AND \"k\" IN (3,5,9))", condition);
    }

    @Test
    void writesOneKeyAsAnEquality() {
        RuntimeFilters filters = new RuntimeFilters(List.of(
                new RuntimeFilters.InFilter("k", new long[] {4, 4})), 0);
        assertEquals("\"k\" = 4",
                filters.condition(schema(field("k", new ArrowType.Int(32, true))), PLAIN));
    }

    @Test
    void writesOnlyTheRangeForManyKeys() {
        long[] keys = new long[RuntimeFilters.MAX_IN_VALUES + 1];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = 10L + 2 * i;
        }
        RuntimeFilters filters = new RuntimeFilters(List.of(new RuntimeFilters.InFilter("k", keys)), 0);
        assertEquals("\"k\" BETWEEN 10 AND " + (10L + 2 * (keys.length - 1)),
                filters.condition(schema(field("k", new ArrowType.Int(64, true))), PLAIN));
    }

    @Test
    void dropsKeysTheColumnTypeCannotHold() {
        RuntimeFilters filters = new RuntimeFilters(List.of(
                new RuntimeFilters.InFilter("s", new long[] {-1, 40_000, 7, 200})), 0);
        assertEquals("(\"s\" BETWEEN 7 AND 200 AND \"s\" IN (7,200))",
                filters.condition(schema(field("s", new ArrowType.Int(8, false))), PLAIN));
        RuntimeFilters none = new RuntimeFilters(List.of(
                new RuntimeFilters.InFilter("s", new long[] {1L << 40})), 0);
        assertEquals("FALSE", none.condition(schema(field("s", new ArrowType.Int(32, true))), PLAIN));
    }

    @Test
    void aFilterWithoutKeysKeepsNoRow() {
        RuntimeFilters filters = new RuntimeFilters(List.of(new RuntimeFilters.InFilter("k", new long[0])), 0);
        assertEquals("FALSE", filters.condition(schema(field("k", new ArrowType.Int(64, true))), PLAIN));
    }

    @Test
    void leavesOutFiltersOnOtherColumnsAndTypes() {
        Schema schema = schema(field("name", ArrowType.Utf8.INSTANCE),
                field("price", new ArrowType.FloatingPoint(
                        org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
                field("a", new ArrowType.Int(32, true)), field("A", new ArrowType.Int(32, true)),
                field("expr", new ArrowType.Int(32, true)));
        RuntimeFilters filters = new RuntimeFilters(List.of(
                new RuntimeFilters.InFilter("name", new long[] {1}),
                new RuntimeFilters.InFilter("price", new long[] {1}),
                new RuntimeFilters.InFilter("missing", new long[] {1}),
                new RuntimeFilters.InFilter("a", new long[] {1}),
                new RuntimeFilters.InFilter("expr", new long[] {1})), 0);
        assertNull(filters.condition(schema, name -> "expr".equals(name) ? null : PLAIN.apply(name)));
    }

    @Test
    void joinsSeveralFiltersWithAnd() {
        Schema schema = schema(field("a", new ArrowType.Int(32, true)), field("b", new ArrowType.Int(16, true)));
        RuntimeFilters filters = new RuntimeFilters(List.of(
                new RuntimeFilters.InFilter("a", new long[] {1}),
                new RuntimeFilters.InFilter("b", new long[] {2, 3})), 0);
        String condition = filters.condition(schema, PLAIN);
        assertEquals("\"a\" = 1 AND (\"b\" BETWEEN 2 AND 3 AND \"b\" IN (2,3))", condition);
        assertTrue(new RuntimeFilters(List.of(), 0).isEmpty());
    }
}
