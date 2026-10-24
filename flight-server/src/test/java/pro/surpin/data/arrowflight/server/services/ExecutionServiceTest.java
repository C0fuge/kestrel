package pro.surpin.data.arrowflight.server.services;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.metrics.CacheMetrics;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ExecutionServiceTest {

    // ── minOf / maxOf ───────────────────────────────────────────────────────

    @Test
    void minOfReturnsSmaller() {
        assertEquals(5, ExecutionService.minOf(5, 10));
    }

    @Test
    void minOfReturnsFirstWhenEqual() {
        assertEquals(5, ExecutionService.minOf(5, 5));
    }

    @Test
    void minOfReturnsNonNullWhenOneIsNull() {
        assertEquals(5, ExecutionService.minOf(null, 5));
        assertEquals(5, ExecutionService.minOf(5, null));
    }

    @Test
    void maxOfReturnsLarger() {
        assertEquals(10, ExecutionService.maxOf(5, 10));
    }

    @Test
    void maxOfReturnsFirstWhenEqual() {
        assertEquals(5, ExecutionService.maxOf(5, 5));
    }

    @Test
    void maxOfReturnsNonNullWhenOneIsNull() {
        assertEquals(5, ExecutionService.maxOf(null, 5));
        assertEquals(5, ExecutionService.maxOf(5, null));
    }

    @Test
    void maxOfStringComparison() {
        assertEquals("z", ExecutionService.maxOf("a", "z"));
    }

    // ── addLongs / addDoubles ───────────────────────────────────────────────

    @Test
    void addLongsBothNonNull() {
        assertEquals(7L, ExecutionService.addLongs(3L, 4L));
    }

    @Test
    void addLongsFirstNull() {
        assertEquals(4L, ExecutionService.addLongs(null, 4L));
    }

    @Test
    void addLongsSecondNull() {
        assertEquals(3L, ExecutionService.addLongs(3L, null));
    }

    @Test
    void addLongsBothNull() {
        assertEquals(0L, ExecutionService.addLongs(null, null));
    }

    @Test
    void addDoublesBothNonNull() {
        assertEquals(7.5, ExecutionService.addDoubles(3.0, 4.5));
    }

    @Test
    void addDoublesFirstNull() {
        assertEquals(4.5, ExecutionService.addDoubles(null, 4.5));
    }

    @Test
    void addDoublesSecondNull() {
        assertEquals(3.0, ExecutionService.addDoubles(3.0, null));
    }

    @Test
    void addDoublesBothNull() {
        assertEquals(0.0, ExecutionService.addDoubles(null, null));
    }

    @Test
    void addNumbersPreservesDecimalPrecision() {
        BigDecimal first = new BigDecimal("12345678901234567890.12");
        BigDecimal second = new BigDecimal("0.34");

        assertEquals(new BigDecimal("12345678901234567890.46"),
                ExecutionService.addNumbers(first, second));
    }

    // ── partitionIntoGroups ─────────────────────────────────────────────────

    @Test
    void partitionIntoGroupsEven() {
        List<List<Integer>> groups = ExecutionService.partitionIntoGroups(
                List.of(1, 2, 3, 4, 5, 6), 3);
        assertEquals(3, groups.size());
        assertEquals(List.of(1, 4), groups.get(0));
        assertEquals(List.of(2, 5), groups.get(1));
        assertEquals(List.of(3, 6), groups.get(2));
    }

    @Test
    void partitionIntoGroupsSingleGroup() {
        List<List<Integer>> groups = ExecutionService.partitionIntoGroups(
                List.of(1, 2, 3), 1);
        assertEquals(1, groups.size());
        assertEquals(3, groups.get(0).size());
    }

    @Test
    void partitionIntoGroupsEmpty() {
        List<List<Integer>> groups = ExecutionService.partitionIntoGroups(
                List.of(), 3);
        assertEquals(3, groups.size());
        assertTrue(groups.get(0).isEmpty());
        assertTrue(groups.get(1).isEmpty());
        assertTrue(groups.get(2).isEmpty());
    }

    @Test
    void partitionIntoGroupsMoreGroupsThanItems() {
        List<List<Integer>> groups = ExecutionService.partitionIntoGroups(
                List.of(1), 3);
        assertEquals(3, groups.size());
        assertEquals(1, groups.get(0).size());
        assertTrue(groups.get(1).isEmpty());
        assertTrue(groups.get(2).isEmpty());
    }

    // ── fileName ────────────────────────────────────────────────────────────

    @Test
    void fileNameIsLastPathSegment() {
        assertEquals("part-0.parquet", ExecutionService.fileName("tpch/lineitem/part-0.parquet"));
        assertEquals("part-0.parquet", ExecutionService.fileName("part-0.parquet"));
    }

    // ── toLong ────────────────────────────────────────────────────────────

    @Test
    void toLongFromBigIntVector() {
        try (BufferAllocator alloc = new RootAllocator();
             BigIntVector v = new BigIntVector("x", alloc)) {
            v.allocateNew(1);
            v.set(0, 42L);
            assertEquals(42L, ExecutionService.toLong(v, 0));
        }
    }

    @Test
    void toLongFromIntVector() {
        try (BufferAllocator alloc = new RootAllocator();
             IntVector v = new IntVector("x", alloc)) {
            v.allocateNew(1);
            v.set(0, 7);
            assertEquals(7L, ExecutionService.toLong(v, 0));
        }
    }

    @Test
    void toLongFromSmallIntVector() {
        try (BufferAllocator alloc = new RootAllocator();
             SmallIntVector v = new SmallIntVector("x", alloc)) {
            v.allocateNew(1);
            v.set(0, (short) 3);
            assertEquals(3L, ExecutionService.toLong(v, 0));
        }
    }

    @Test
    void toLongFromTinyIntVector() {
        try (BufferAllocator alloc = new RootAllocator();
             TinyIntVector v = new TinyIntVector("x", alloc)) {
            v.allocateNew(1);
            v.set(0, (byte) 1);
            assertEquals(1L, ExecutionService.toLong(v, 0));
        }
    }

    // ── toDouble ──────────────────────────────────────────────────────────

    @Test
    void toDoubleFromFloat8Vector() {
        try (BufferAllocator alloc = new RootAllocator();
             Float8Vector v = new Float8Vector("x", alloc)) {
            v.allocateNew(1);
            v.set(0, 3.14);
            assertEquals(3.14, ExecutionService.toDouble(v, 0));
        }
    }

    @Test
    void toDoubleFromFloat4Vector() {
        try (BufferAllocator alloc = new RootAllocator();
             Float4Vector v = new Float4Vector("x", alloc)) {
            v.allocateNew(1);
            v.set(0, 2.5f);
            assertEquals(2.5f, ExecutionService.toDouble(v, 0), 0.0001);
        }
    }

    // ── setVectorValue ────────────────────────────────────────────────────

    @Test
    void setVectorValueNullSetsNull() {
        try (BufferAllocator alloc = new RootAllocator();
             BigIntVector v = new BigIntVector("x", alloc)) {
            v.allocateNew(1);
            ExecutionService.setVectorValue(v, 0, null);
            assertTrue(v.isNull(0));
        }
    }

    @Test
    void setVectorValueBigInt() {
        try (BufferAllocator alloc = new RootAllocator();
             BigIntVector v = new BigIntVector("x", alloc)) {
            v.allocateNew(1);
            ExecutionService.setVectorValue(v, 0, 42L);
            assertEquals(42L, v.get(0));
        }
    }

    @Test
    void setVectorValueInt() {
        try (BufferAllocator alloc = new RootAllocator();
             IntVector v = new IntVector("x", alloc)) {
            v.allocateNew(1);
            ExecutionService.setVectorValue(v, 0, 7);
            assertEquals(7, v.get(0));
        }
    }

    @Test
    void setVectorValueSmallInt() {
        try (BufferAllocator alloc = new RootAllocator();
             SmallIntVector v = new SmallIntVector("x", alloc)) {
            v.allocateNew(1);
            ExecutionService.setVectorValue(v, 0, (short) 3);
            assertEquals((short) 3, v.get(0));
        }
    }

    @Test
    void setVectorValueTinyInt() {
        try (BufferAllocator alloc = new RootAllocator();
             TinyIntVector v = new TinyIntVector("x", alloc)) {
            v.allocateNew(1);
            ExecutionService.setVectorValue(v, 0, (byte) 1);
            assertEquals((byte) 1, v.get(0));
        }
    }

    @Test
    void setVectorValueFloat8() {
        try (BufferAllocator alloc = new RootAllocator();
             Float8Vector v = new Float8Vector("x", alloc)) {
            v.allocateNew(1);
            ExecutionService.setVectorValue(v, 0, 3.14);
            assertEquals(3.14, v.get(0));
        }
    }

    @Test
    void setVectorValueFloat4() {
        try (BufferAllocator alloc = new RootAllocator();
             Float4Vector v = new Float4Vector("x", alloc)) {
            v.allocateNew(1);
            ExecutionService.setVectorValue(v, 0, 2.5f);
            assertEquals(2.5f, v.get(0), 0.0001);
        }
    }

    @Test
    void setVectorValueDecimal() {
        try (BufferAllocator alloc = new RootAllocator();
             DecimalVector v = new DecimalVector("x", alloc, 38, 4)) {
            v.allocateNew(1);
            BigDecimal value = new BigDecimal("12345678901234567890.1234");
            ExecutionService.setVectorValue(v, 0, value);
            assertEquals(value, v.getObject(0));
        }
    }

    @Test
    void setVectorValueBit() {
        try (BufferAllocator alloc = new RootAllocator();
             BitVector v = new BitVector("x", alloc)) {
            v.allocateNew(1);
            ExecutionService.setVectorValue(v, 0, true);
            assertEquals(1, v.get(0));
        }
    }

    @Test
    void setVectorValueVarChar() {
        try (BufferAllocator alloc = new RootAllocator();
             VarCharVector v = new VarCharVector("x", alloc)) {
            v.allocateNew(16);
            ExecutionService.setVectorValue(v, 0, "hello");
            assertEquals("hello", new String(v.get(0), StandardCharsets.UTF_8));
        }
    }

    /** Verifies footer statistics of DATE columns are written as dates, not dropped. */
    @Test
    void setVectorValueDateDay() {
        try (BufferAllocator alloc = new RootAllocator();
             org.apache.arrow.vector.DateDayVector v =
                     new org.apache.arrow.vector.DateDayVector("x", alloc)) {
            v.allocateNew(1);
            ExecutionService.setVectorValue(v, 0, 8035);
            assertEquals(8035, v.get(0));
        }
    }

    /** Verifies values are brought to the scale of the decimal vector they go into. */
    @Test
    void setVectorValueDecimalRescales() {
        try (BufferAllocator alloc = new RootAllocator();
             DecimalVector v = new DecimalVector("x", alloc, 10, 2)) {
            v.allocateNew(2);
            ExecutionService.setVectorValue(v, 0, new BigDecimal("1.5"));
            ExecutionService.setVectorValue(v, 1, 7L);
            assertEquals(new BigDecimal("1.50"), v.getObject(0));
            assertEquals(new BigDecimal("7.00"), v.getObject(1));
        }
    }

    /**
     * A join reads the files its ticket names, table by table (pinned lakeFS paths included),
     * so a table changed after planning is not mixed in; a table the ticket does not name is
     * listed instead (empty result).
     */
    @Test
    void joinTablesReadTheFilesOfTheTicket() {
        pro.surpin.data.arrowflight.server.storage.DataStore store =
                org.mockito.Mockito.mock(pro.surpin.data.arrowflight.server.storage.DataStore.class);
        org.mockito.Mockito.when(store.unversionedPath(org.mockito.ArgumentMatchers.anyString()))
                .thenAnswer(inv -> {
                    String path = inv.getArgument(0);
                    return path.startsWith("@") ? path.substring(path.indexOf('/') + 1) : path;
                });
        org.mockito.Mockito.when(store.duckDbUri(org.mockito.ArgumentMatchers.anyString()))
                .thenAnswer(inv -> "s3://lake/" + inv.<String>getArgument(0).replace("@", ""));
        String[] ticket = {"@c1/tpch/nation/data_1.parquet", "@c1/tpch/region/data_0.parquet",
            "@c1/tpch/nation_x/data_0.parquet"};
        assertEquals(List.of("s3://lake/c1/tpch/region/data_0.parquet"),
                ExecutionService.plannedTableFiles(store, "tpch.region", ticket));
        assertEquals(List.of("s3://lake/c1/tpch/nation/data_1.parquet"),
                ExecutionService.plannedTableFiles(store, "tpch.nation", ticket));
        assertTrue(ExecutionService.plannedTableFiles(store, "tpch.orders", ticket).isEmpty());
        assertTrue(ExecutionService.plannedTableFiles(store, "tpch.region", null).isEmpty());
    }

    /** Verifies a result schema is cached for a short query and looked up again for a long one. */
    @Test
    void resultSchemaCachesOnlyQueriesWithinTheLimit() throws Exception {
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        MetadataService metadata = mock(MetadataService.class);
        when(parquet.sampleFile(anyString(), anyString())).thenReturn(Optional.empty());
        Schema schema = new Schema(List.of(
                Field.nullable("a", new ArrowType.Int(64, true))));
        when(metadata.getQuerySchema(anyString())).thenReturn(schema);
        ExecutionService service = new ExecutionService(parquet, mock(DuckDbAdapter.class),
                metadata, null, null);
        String small = "SELECT a FROM s.t WHERE a IN (1, 2)";
        String huge = ParquetQueryParserUnitTest.longInListQuery();
        long skipped = CacheMetrics.oversizeCount(CacheMetrics.Cache.SCHEMA);

        try (RootAllocator allocator = new RootAllocator()) {
            assertEquals(schema, service.resultSchema(allocator, small));
            assertEquals(schema, service.resultSchema(allocator, small));
            assertEquals(schema, service.resultSchema(allocator, huge));
            assertEquals(schema, service.resultSchema(allocator, huge));
        }
        verify(metadata, times(1)).getQuerySchema(small);
        verify(metadata, times(2)).getQuerySchema(huge);
        assertEquals(skipped + 2, CacheMetrics.oversizeCount(CacheMetrics.Cache.SCHEMA));
    }
}
