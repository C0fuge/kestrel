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
import pro.surpin.data.arrowflight.spi.storage.DataFile;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
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
                ExecutionService.plannedTableFiles(store, "tpch.region", ticket, null));
        assertEquals(List.of("s3://lake/c1/tpch/nation/data_1.parquet"),
                ExecutionService.plannedTableFiles(store, "tpch.nation", ticket, null));
        assertTrue(ExecutionService.plannedTableFiles(store, "tpch.orders", ticket, null).isEmpty());
        assertTrue(ExecutionService.plannedTableFiles(store, "tpch.region", null, null).isEmpty());
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

    /** Verifies a describe that fails to read the remembered sample file lists the table again once. */
    @Test
    void describeRetriesWithAFreshSampleFileAfterAReadError() throws Exception {
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        pro.surpin.data.arrowflight.server.storage.DataStore store =
                mock(pro.surpin.data.arrowflight.server.storage.DataStore.class);
        DuckDbAdapter duckDb = mock(DuckDbAdapter.class);
        DataFile gone = new DataFile("s/t/old.parquet", "/data/s/t/old.parquet", 1L, 1L, Set.of());
        DataFile fresh = new DataFile("s/t/new.parquet", "/data/s/t/new.parquet", 1L, 2L, Set.of());
        when(parquet.sampleFile("s", "t")).thenReturn(Optional.of(gone), Optional.of(fresh));
        when(parquet.dataStore()).thenReturn(store);
        when(parquet.getTableSchema("s", "t")).thenReturn(new Schema(List.of()));
        when(store.duckDbUri(any(DataFile.class)))
                .thenAnswer(inv -> inv.<DataFile>getArgument(0).location());
        Schema schema = new Schema(List.of(
                Field.nullable("a", new ArrowType.Int(64, true))));
        when(duckDb.describeSql(any(), contains("old.parquet")))
                .thenThrow(new java.sql.SQLException("IO Error: No files found that match the pattern"));
        when(duckDb.describeSql(any(), contains("new.parquet"))).thenReturn(schema);
        ExecutionService service = new ExecutionService(parquet, duckDb,
                mock(MetadataService.class), null, null);

        try (RootAllocator allocator = new RootAllocator()) {
            assertEquals(schema, service.resultSchema(allocator, "SELECT a FROM s.t WHERE a = 1"));
        }
        verify(parquet, times(1)).forgetSampleFile("s", "t");
        verify(parquet, times(2)).sampleFile("s", "t");
    }

    /** Verifies a query DuckDB rejects is reported without listing the table again. */
    @Test
    void describeDoesNotRetryAQueryError() throws Exception {
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        pro.surpin.data.arrowflight.server.storage.DataStore store =
                mock(pro.surpin.data.arrowflight.server.storage.DataStore.class);
        DuckDbAdapter duckDb = mock(DuckDbAdapter.class);
        DataFile file = new DataFile("s/t/a.parquet", "/data/s/t/a.parquet", 1L, 1L, Set.of());
        when(parquet.sampleFile("s", "t")).thenReturn(Optional.of(file));
        when(parquet.dataStore()).thenReturn(store);
        when(parquet.getTableSchema("s", "t")).thenReturn(new Schema(List.of()));
        when(store.duckDbUri(any(DataFile.class))).thenReturn("/data/s/t/a.parquet");
        when(duckDb.describeSql(any(), anyString()))
                .thenThrow(new java.sql.SQLException("Binder Error: column b not found"));
        ExecutionService service = new ExecutionService(parquet, duckDb,
                mock(MetadataService.class), null, null);

        try (RootAllocator allocator = new RootAllocator()) {
            assertThrows(IllegalArgumentException.class,
                    () -> service.resultSchema(allocator, "SELECT b FROM s.t"));
        }
        verify(parquet, times(0)).forgetSampleFile(anyString(), anyString());
        verify(parquet, times(1)).sampleFile("s", "t");
    }

    /**
     * Verifies that on HDFS under SASL a partial query's schema is described from the sample
     * file's footer read through the Java client when this node lacks its blocks, instead of a
     * guess from table metadata (sum of a decimal announced as double, item 29).
     *
     * @param dir temporary directory
     * @throws Exception when the test file cannot be written
     */
    @Test
    void describeUsesAFooterStubForAFileThisNodeCannotRead(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        java.nio.file.Path local = dir.resolve("a.parquet");
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
                java.sql.Statement st = conn.createStatement()) {
            st.execute("COPY (SELECT 1.50::DECIMAL(15,2) AS q) TO '" + local + "' (FORMAT parquet)");
        }
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        pro.surpin.data.arrowflight.server.storage.DataStore store =
                mock(pro.surpin.data.arrowflight.server.storage.DataStore.class);
        DuckDbAdapter duckDb = mock(DuckDbAdapter.class);
        DataFile remote = new DataFile("s/t/a.parquet", "hdfs://ns/data/s/t/a.parquet",
                java.nio.file.Files.size(local), 1L, Set.of("dn2"));
        when(parquet.sampleFile("s", "t")).thenReturn(Optional.of(remote));
        when(parquet.dataStore()).thenReturn(store);
        when(store.duckDbUri(any(DataFile.class))).thenReturn("hdfs://ns/data/s/t/a.parquet");
        when(store.wholeFileReadsOnly()).thenReturn(true);
        when(store.readableHere(remote)).thenReturn(false);
        when(store.inputFile(remote)).thenReturn(new org.apache.parquet.io.LocalInputFile(local));
        java.nio.file.Path stubs = java.nio.file.Files.createDirectories(dir.resolve("stubs"));
        when(parquet.getTableSchema("s", "t")).thenReturn(new Schema(List.of()));
        when(duckDb.footerStubRoot()).thenReturn(stubs);
        Schema schema = new Schema(List.of(Field.nullable("q", new ArrowType.Decimal(38, 2, 128))));
        java.util.concurrent.atomic.AtomicReference<String> described = new java.util.concurrent.atomic.AtomicReference<>();
        when(duckDb.describeSql(any(), anyString())).thenAnswer(inv -> {
            described.set(inv.getArgument(1));
            return schema;
        });
        ExecutionService service = new ExecutionService(parquet, duckDb,
                mock(MetadataService.class), null, null);

        try (RootAllocator allocator = new RootAllocator()) {
            assertEquals(schema, service.resultSchema(allocator, "SELECT sum(q) AS q FROM s.t"));
        }
        assertTrue(described.get().contains(stubs.toString()), described.get());
        assertFalse(described.get().contains("hdfs://"), described.get());
        try (java.util.stream.Stream<java.nio.file.Path> left = java.nio.file.Files.walk(stubs)) {
            assertEquals(0, left.filter(java.nio.file.Files::isRegularFile).count(), "stub removed");
        }
    }

    /**
     * Item 30: with whole-file reads (SASL DataNodes) a ticket file this node lacks blocks of is
     * copied through the store's Java client to the node-local cache, and DuckDB reads the copy;
     * a file the node holds whole keeps its direct path.
     */
    @Test
    void ticketFilesThisNodeCannotReadAreReadFromALocalCopy(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        java.nio.file.Path source = dir.resolve("a.parquet");
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
                java.sql.Statement st = conn.createStatement()) {
            st.execute("COPY (SELECT range AS v FROM range(1000)) TO '" + source + "' (FORMAT parquet)");
        }
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        pro.surpin.data.arrowflight.server.storage.DataStore store =
                mock(pro.surpin.data.arrowflight.server.storage.DataStore.class);
        DuckDbAdapter duckDb = mock(DuckDbAdapter.class);
        long size = java.nio.file.Files.size(source);
        DataFile remote = new DataFile("s/t/k=x y/a.parquet", "hdfs://ns/data/s/t/k=x y/a.parquet",
                size, 1L, Set.of());
        DataFile held = new DataFile("s/t/k=x y/b.parquet", "hdfs://ns/data/s/t/k=x y/b.parquet",
                size, 1L, Set.of("dn1"));
        when(parquet.dataStore()).thenReturn(store);
        when(parquet.localCopies()).thenReturn(true);
        when(store.parquetFiles("s/t")).thenReturn(List.of(remote, held));
        when(store.readableHere(remote)).thenReturn(false);
        when(store.readableHere(held)).thenReturn(true);
        when(store.inputFile(remote)).thenReturn(new org.apache.parquet.io.LocalInputFile(source));
        when(store.duckDbUri(anyString())).thenAnswer(inv -> "hdfs://ns/data/" + inv.getArgument(0));
        when(duckDb.localCopyRoot()).thenReturn(dir.resolve("localcopy"));
        ExecutionService service = new ExecutionService(parquet, duckDb,
                mock(MetadataService.class), null, null);
        String[] ticket = {remote.relativePath(), held.relativePath()};

        try (pro.surpin.data.arrowflight.server.storage.LocalCopyCache.Lease lease =
                service.localCopies(ticket)) {
            String copy = ExecutionService.duckDbUri(store, remote.relativePath(), lease);
            assertTrue(copy.startsWith(dir.resolve("localcopy").toString()), copy);
            assertTrue(copy.endsWith("/s/t/k=x y/a.parquet"), "hive directories kept: " + copy);
            assertEquals("hdfs://ns/data/s/t/k=x y/b.parquet",
                    ExecutionService.duckDbUri(store, held.relativePath(), lease));
            try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
                    java.sql.Statement st = conn.createStatement();
                    java.sql.ResultSet rs = st.executeQuery("SELECT count(*), sum(v) FROM read_parquet('"
                            + copy.replace("'", "''") + "')")) {
                assertTrue(rs.next());
                assertEquals(1000, rs.getLong(1));
                assertEquals(499_500, rs.getLong(2));
            }
        }
        // Every file held whole: nothing to copy.
        assertEquals(null, service.localCopies(new String[] {held.relativePath()}));
        when(parquet.localCopies()).thenReturn(false);
        assertEquals(null, service.localCopies(ticket));
    }

    /**
     * Item 30 part 2: a complete answer on a node lacking blocks of a table (a comma join, as in
     * TPC-H) reads that table from local copies: its references are replaced by read_parquet over
     * the copies and the files held whole, and the answer equals the one over the original files.
     */
    @Test
    void completeAnswerReadsTablesThisNodeLacksBlocksOfFromLocalCopies(
            @org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
        java.nio.file.Path a = dir.resolve("a.parquet");
        java.nio.file.Path b = dir.resolve("b.parquet");
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
                java.sql.Statement st = conn.createStatement()) {
            st.execute("COPY (SELECT range AS v FROM range(1000)) TO '" + a + "' (FORMAT parquet)");
            st.execute("COPY (SELECT range AS v FROM range(1000, 2000)) TO '" + b + "' (FORMAT parquet)");
        }
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        pro.surpin.data.arrowflight.server.storage.DataStore store =
                mock(pro.surpin.data.arrowflight.server.storage.DataStore.class);
        DuckDbAdapter duckDb = mock(DuckDbAdapter.class);
        DataFile remote = new DataFile("s/t/a.parquet", "hdfs://ns/data/s/t/a.parquet",
                java.nio.file.Files.size(a), 1L, Set.of());
        DataFile held = new DataFile("s/t/b.parquet", "hdfs://ns/data/s/t/b.parquet",
                java.nio.file.Files.size(b), 1L, Set.of("dn1"));
        when(parquet.dataStore()).thenReturn(store);
        when(parquet.localCopies()).thenReturn(true);
        when(parquet.tableFiles("s", "t")).thenReturn(List.of(remote, held));
        when(parquet.tableFiles("s", "u")).thenReturn(List.of());
        when(parquet.getTableSchema("s", "t")).thenReturn(new Schema(List.of()));
        when(store.readableHere(remote)).thenReturn(false);
        when(store.readableHere(held)).thenReturn(true);
        when(store.inputFile(remote)).thenReturn(new org.apache.parquet.io.LocalInputFile(a));
        when(store.duckDbUri("s/t/b.parquet")).thenReturn(b.toString());
        when(duckDb.localCopyRoot()).thenReturn(dir.resolve("localcopy"));
        ExecutionService service = new ExecutionService(parquet, duckDb,
                mock(MetadataService.class), null, null);
        String query = "SELECT count(*) AS n, sum(x.v) AS q FROM s.t x, s.u WHERE x.v % 10 = u.k"
                + " AND x.v IN (SELECT v FROM s.t)";

        try (ExecutionService.CopiedQuery copied = service.completeFromCopies(query);
                java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
                java.sql.Statement st = conn.createStatement()) {
            assertTrue(copied.copied());
            assertFalse(copied.sql().contains("s.t"), copied.sql());
            assertTrue(copied.sql().contains(dir.resolve("localcopy").toString()), copied.sql());
            assertTrue(copied.sql().contains(b.toString()), "held file read in place: " + copied.sql());
            st.execute("CREATE SCHEMA s");
            st.execute("CREATE TABLE s.u AS SELECT 3 AS k");
            try (java.sql.ResultSet rs = st.executeQuery(copied.sql())) {
                assertTrue(rs.next());
                assertEquals(200, rs.getLong(1));
                long want = 0;
                for (long v = 3; v < 2000; v += 10) {
                    want += v;
                }
                assertEquals(want, rs.getLong(2));
            }
        }
        // Every file held whole, or copies off: the SQL is left as it is.
        when(store.readableHere(remote)).thenReturn(true);
        java.io.PrintStream err = System.err;
        java.io.ByteArrayOutputStream log = new java.io.ByteArrayOutputStream();
        System.setErr(new java.io.PrintStream(log, true, java.nio.charset.StandardCharsets.UTF_8));
        try (ExecutionService.CopiedQuery same = service.completeFromCopies(query)) {
            assertFalse(same.copied());
            assertEquals(query, same.sql());
        } finally {
            System.setErr(err);
        }
        // Still one line on how the query's files are read (item 31 acceptance: every complete
        // answer on a short-circuit-only node logs it), counting each table once.
        assertTrue(log.toString(java.nio.charset.StandardCharsets.UTF_8)
                .contains("hdfs=reads files=2 shortCircuit=2 localCopy=0"), log::toString);
        when(store.readableHere(remote)).thenReturn(false);
        when(parquet.localCopies()).thenReturn(false);
        assertEquals(query, service.completeFromCopies(query).sql());
    }

    /**
     * Item 33 (E): a complete answer whose copies are refused for lack of disk room still logs
     * its {@code hdfs=reads} line (marked {@code refused=noRoom}) before the refusal goes out.
     */
    @Test
    void copyRefusalLogsItsReads(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        java.nio.file.Path a = dir.resolve("a.parquet");
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
                java.sql.Statement st = conn.createStatement()) {
            st.execute("COPY (SELECT range AS v FROM range(100)) TO '" + a + "' (FORMAT parquet)");
        }
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        pro.surpin.data.arrowflight.server.storage.DataStore store =
                mock(pro.surpin.data.arrowflight.server.storage.DataStore.class);
        DuckDbAdapter duckDb = mock(DuckDbAdapter.class);
        DataFile remote = new DataFile("s/t/a.parquet", "hdfs://ns/data/s/t/a.parquet",
                java.nio.file.Files.size(a), 1L, Set.of());
        when(parquet.dataStore()).thenReturn(store);
        when(parquet.localCopies()).thenReturn(true);
        when(parquet.tableFiles("s", "t")).thenReturn(List.of(remote));
        when(parquet.getTableSchema("s", "t")).thenReturn(new Schema(List.of()));
        when(store.readableHere(remote)).thenReturn(false);
        when(store.inputFile(remote)).thenReturn(new org.apache.parquet.io.LocalInputFile(a));
        when(duckDb.localCopyRoot()).thenReturn(dir.resolve("localcopy"));
        ExecutionService service = new ExecutionService(parquet, duckDb,
                mock(MetadataService.class), null, null);

        java.io.PrintStream err = System.err;
        java.io.ByteArrayOutputStream log = new java.io.ByteArrayOutputStream();
        System.setErr(new java.io.PrintStream(log, true, java.nio.charset.StandardCharsets.UTF_8));
        // A reserve no disk has: every copy is refused.
        System.setProperty("hdfsLocalCopyMinFreeBytes", "1000000000000000000");
        try {
            java.io.IOException e = assertThrows(java.io.IOException.class,
                    () -> service.completeFromCopies("SELECT count(*) FROM s.t"));
            assertNotNull(pro.surpin.data.arrowflight.server.storage.LocalCopyCache.NoRoomException
                    .in(e), e::toString);
        } finally {
            System.clearProperty("hdfsLocalCopyMinFreeBytes");
            System.setErr(err);
        }
        String text = log.toString(java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(text.contains("hdfs=reads files=1 shortCircuit=0 localCopy=1 copied=0"
                + " refused=noRoom"), text);
    }

    /**
     * Item 33 regression: a complete answer refused for lack of copy room logs why it was not
     * read in batches of copies, so a refusal on a small copy disk names its cause.
     */
    @Test
    void refusedCompleteAnswerLogsWhyItIsNotBatched(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        java.nio.file.Path a = dir.resolve("a.parquet");
        java.nio.file.Path b = dir.resolve("b.parquet");
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
                java.sql.Statement st = conn.createStatement()) {
            st.execute("COPY (SELECT range AS v FROM range(100)) TO '" + a + "' (FORMAT parquet)");
            st.execute("COPY (SELECT range AS v FROM range(100)) TO '" + b + "' (FORMAT parquet)");
        }
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        pro.surpin.data.arrowflight.server.storage.DataStore store =
                mock(pro.surpin.data.arrowflight.server.storage.DataStore.class);
        DuckDbAdapter duckDb = mock(DuckDbAdapter.class);
        DataFile first = new DataFile("s/t/a.parquet", "hdfs://ns/data/s/t/a.parquet",
                java.nio.file.Files.size(a), 1L, Set.of());
        DataFile second = new DataFile("s/u/b.parquet", "hdfs://ns/data/s/u/b.parquet",
                java.nio.file.Files.size(b), 1L, Set.of());
        DataFile third = new DataFile("s/u/c.parquet", "hdfs://ns/data/s/u/c.parquet",
                java.nio.file.Files.size(b), 1L, Set.of());
        when(parquet.dataStore()).thenReturn(store);
        when(parquet.localCopies()).thenReturn(true);
        when(parquet.tableFiles("s", "t")).thenReturn(List.of(first));
        when(parquet.tableFiles("s", "u")).thenReturn(List.of(second, third));
        when(parquet.getTableSchema(anyString(), anyString())).thenReturn(new Schema(List.of()));
        when(store.readableHere(any(DataFile.class))).thenReturn(false);
        when(store.inputFile(any(DataFile.class))).thenReturn(new org.apache.parquet.io.LocalInputFile(a));
        when(duckDb.localCopyRoot()).thenReturn(dir.resolve("localcopy"));
        ExecutionService service = new ExecutionService(parquet, duckDb,
                mock(MetadataService.class), null, null);
        org.apache.arrow.flight.FlightProducer.ServerStreamListener listener =
                mock(org.apache.arrow.flight.FlightProducer.ServerStreamListener.class);

        java.io.PrintStream err = System.err;
        java.io.ByteArrayOutputStream log = new java.io.ByteArrayOutputStream();
        System.setErr(new java.io.PrintStream(log, true, java.nio.charset.StandardCharsets.UTF_8));
        // A reserve no disk has: the copy room is 1 byte and every copy is refused.
        System.setProperty("hdfsLocalCopyMinFreeBytes", "1000000000000000000");
        try (BufferAllocator allocator = new RootAllocator()) {
            ExecutionService.CopyBatches one = service.completeInCopyBatches(allocator,
                    "SELECT count(*) FROM s.t", listener, null);
            assertFalse(one.streamed());
            assertTrue(one.notBatched().contains("the 1 copies of s.t") && one.notBatched()
                    .contains("make one batch only; the copy room is 1 bytes"), one.notBatched());
            String ordered = "SELECT v FROM s.u ORDER BY v";
            ExecutionService.CopyBatches sorted = service.completeInCopyBatches(allocator, ordered,
                    listener, null);
            assertFalse(sorted.streamed());
            assertTrue(sorted.notBatched().contains("ORDER BY without"), sorted.notBatched());
            assertNotNull(pro.surpin.data.arrowflight.server.storage.LocalCopyCache.NoRoomException
                    .in(assertThrows(java.io.IOException.class,
                            () -> service.completeFromCopies(ordered, sorted.notBatched()))));
            ExecutionService.CopyBatches joined = service.completeInCopyBatches(allocator,
                    "SELECT count(*) FROM s.t JOIN s.u ON t.v = u.v", listener, null);
            assertFalse(joined.streamed());
            assertNotNull(joined.notBatched());
            // Without a reason (copies off, or a caller that has none) no line is added.
            assertThrows(java.io.IOException.class,
                    () -> service.completeFromCopies("SELECT count(*) FROM s.t", null));
        } finally {
            System.clearProperty("hdfsLocalCopyMinFreeBytes");
            System.setErr(err);
        }
        String text = log.toString(java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(text.contains("hdfs=completeInCopyBatches skipped tables=[s.u] localCopy=2 copyBytes="
                + 2 * java.nio.file.Files.size(b) + " reason='its answer cannot be merged from batches"
                + " of copies (ORDER BY without"), text);
        assertEquals(1, text.split("hdfs=completeInCopyBatches skipped", -1).length - 1, text);
        verify(listener, times(0)).start(any());
        when(parquet.localCopies()).thenReturn(false);
        try (BufferAllocator allocator = new RootAllocator()) {
            ExecutionService.CopyBatches off = service.completeInCopyBatches(allocator,
                    "SELECT count(*) FROM s.t", listener, null);
            assertFalse(off.streamed());
            assertNull(off.notBatched());
        }
    }

    /** Item 33 regression: each reason a complete query is not read in batches of copies is named. */
    @Test
    void notBatchableReasonNamesTheCause() {
        List<ParquetQueryParser.JoinTable> t = List.of(new ParquetQueryParser.JoinTable("s", "t", null));
        String scan = "SELECT a FROM s.t WHERE a > 1";
        assertNull(ExecutionService.notBatchableReason(scan, ParquetQueryParser.parse(scan), t));
        String group = "SELECT a, count(*) FROM s.t GROUP BY 1";
        assertNull(ExecutionService.notBatchableReason(group, ParquetQueryParser.parse(group), t));
        assertEquals("the query reads [s.t, s.u], not only s.t", ExecutionService.notBatchableReason(
                scan, ParquetQueryParser.parse(scan), List.of(new ParquetQueryParser.JoinTable("s", "t", null),
                        new ParquetQueryParser.JoinTable("s", "u", null))));
        assertEquals("the query reads [t], not only s.t", ExecutionService.notBatchableReason(
                scan, ParquetQueryParser.parse(scan), List.of(new ParquetQueryParser.JoinTable(null, "t", null))));
        String join = "SELECT count(*) FROM s.t x JOIN s.u y ON x.a = y.a";
        assertEquals("the query joins tables", ExecutionService.notBatchableReason(join,
                ParquetQueryParser.parse(join), List.of(new ParquetQueryParser.JoinTable("s", "t", null),
                        new ParquetQueryParser.JoinTable("s", "u", null))));
        String ordered = "SELECT a FROM s.t ORDER BY a";
        assertEquals("its answer cannot be merged from batches of copies (ORDER BY without an"
                + " aggregate, DISTINCT or LIMIT)", ExecutionService.notBatchableReason(ordered,
                        ParquetQueryParser.parse(ordered), t));
    }

    /**
     * Item 32: a complete answer that reads no table of the data store still logs one
     * {@code hdfs=reads} line on a short-circuit-only node, and none when local copies are off.
     */
    @Test
    void completeAnswerWithoutTablesLogsItsReads() throws Exception {
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        when(parquet.localCopies()).thenReturn(true);
        ExecutionService service = new ExecutionService(parquet, mock(DuckDbAdapter.class),
                mock(MetadataService.class), null, null);
        java.io.PrintStream err = System.err;
        java.io.ByteArrayOutputStream log = new java.io.ByteArrayOutputStream();
        System.setErr(new java.io.PrintStream(log, true, java.nio.charset.StandardCharsets.UTF_8));
        try (ExecutionService.CopiedQuery same = service.completeFromCopies("SELECT 1 AS one")) {
            assertFalse(same.copied());
            assertEquals("SELECT 1 AS one", same.sql());
            service.logNoFileReads("endpoints=3");
            when(parquet.localCopies()).thenReturn(false);
            service.logNoFileReads("endpoints=4");
        } finally {
            System.setErr(err);
        }
        String text = log.toString(java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(text.contains("hdfs=reads files=0 shortCircuit=0 localCopy=0 tables=none"), text);
        assertTrue(text.contains("hdfs=reads files=0 shortCircuit=0 localCopy=0 tables=endpoints=3"),
                text);
        assertFalse(text.contains("tables=endpoints=4"), text);
    }

    /**
     * Item 30 part 2b: a scan whose local copies do not fit the cache is read in batches whose
     * copies fit, each batch's copies deleted before the next is copied, and every row is sent
     * through one stream (files read in place go with the first batch).
     */
    @Test
    void scanLargerThanTheCopyCacheIsReadInBatchesThatFit(
            @org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
        java.nio.file.Path root = dir.resolve("localcopy");
        List<DataFile> remote = new java.util.ArrayList<>();
        String[] ticket = {"s/t/a.parquet", "s/t/b.parquet", "s/t/c.parquet", "s/t/d.parquet"};
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        pro.surpin.data.arrowflight.server.storage.DataStore store =
                mock(pro.surpin.data.arrowflight.server.storage.DataStore.class);
        long largest = 0;
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
                java.sql.Statement st = conn.createStatement()) {
            for (int i = 0; i < ticket.length; i++) {
                java.nio.file.Path file = dir.resolve("f" + i + ".parquet");
                st.execute("COPY (SELECT range AS v FROM range(" + i * 1000 + ", " + (i + 1) * 1000
                        + ")) TO '" + file + "' (FORMAT parquet)");
                DataFile data = new DataFile(ticket[i], "hdfs://ns/data/" + ticket[i],
                        java.nio.file.Files.size(file), 1L, i == 3 ? Set.of("dn1") : Set.of());
                when(store.readableHere(data)).thenReturn(i == 3);
                when(store.inputFile(data)).thenReturn(new org.apache.parquet.io.LocalInputFile(file));
                when(store.duckDbUri(ticket[i])).thenReturn(file.toString());
                if (i < 3) {
                    remote.add(data);
                    largest = Math.max(largest, data.size());
                }
                when(store.file(ticket[i])).thenReturn(data);
            }
        }
        List<DataFile> listed = new java.util.ArrayList<>(remote);
        when(store.parquetFiles("s/t")).thenReturn(listed);
        when(parquet.dataStore()).thenReturn(store);
        when(parquet.localCopies()).thenReturn(true);
        when(parquet.getTableSchema("s", "t")).thenReturn(new Schema(List.of()));
        DuckDbAdapter duckDb = mock(DuckDbAdapter.class);
        when(duckDb.localCopyRoot()).thenReturn(root);
        List<Long> rows = new java.util.ArrayList<>();
        List<Integer> copiesOnDisk = new java.util.ArrayList<>();
        org.mockito.Mockito.doAnswer(call -> {
            assertInstanceOf(ConcatenatingStreamListener.class, call.getArgument(2));
            try (java.util.stream.Stream<java.nio.file.Path> walk = java.nio.file.Files.walk(root);
                    java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
                    java.sql.Statement st = conn.createStatement();
                    java.sql.ResultSet rs = st.executeQuery("SELECT count(*) FROM ("
                            + call.getArgument(1, String.class) + ")")) {
                copiesOnDisk.add((int) walk.filter(p -> p.toString().endsWith(".parquet")).count());
                assertTrue(rs.next());
                rows.add(rs.getLong(1));
            }
            return null;
        }).when(duckDb).streamSql(any(), anyString(), any(), org.mockito.ArgumentMatchers.eq(true));
        ExecutionService service = new ExecutionService(parquet, duckDb,
                mock(MetadataService.class), null, null);
        org.apache.arrow.flight.FlightProducer.ServerStreamListener listener =
                mock(org.apache.arrow.flight.FlightProducer.ServerStreamListener.class);

        System.setProperty("hdfsLocalCopyMaxBytes", String.valueOf(largest));
        try (BufferAllocator allocator = new RootAllocator()) {
            service.readParquet(allocator, "SELECT v FROM s.t WHERE v >= 0", ticket, listener, true);
        } finally {
            System.clearProperty("hdfsLocalCopyMaxBytes");
        }
        // Three batches of one copy each; the held file goes with the first; all 4000 rows.
        assertEquals(List.of(2000L, 1000L, 1000L), rows);
        assertEquals(List.of(1, 1, 1), copiesOnDisk);
    }

    /**
     * Item 30 part 2b: an aggregate whose local copies do not fit the cache is answered per
     * batch of copies (one copy on disk at a time) and the part answers are merged into the
     * answer computed over all files at once.
     */
    @Test
    void aggregateLargerThanTheCopyCacheIsMergedFromBatchesThatFit(
            @org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
        java.nio.file.Path root = dir.resolve("localcopy");
        String[] ticket = {"s/t/a.parquet", "s/t/b.parquet", "s/t/c.parquet", "s/t/d.parquet"};
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        pro.surpin.data.arrowflight.server.storage.DataStore store =
                mock(pro.surpin.data.arrowflight.server.storage.DataStore.class);
        List<DataFile> remote = new java.util.ArrayList<>();
        List<String> all = new java.util.ArrayList<>();
        long largest = 0;
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
                java.sql.Statement st = conn.createStatement()) {
            for (int i = 0; i < ticket.length; i++) {
                java.nio.file.Path file = dir.resolve("f" + i + ".parquet");
                st.execute("COPY (SELECT range % 7 AS g, range AS v, CAST(range / 100 AS DECIMAL(15, 2))"
                        + " AS d FROM range(" + i * 1000 + ", " + (i + 1) * 1000 + ")) TO '" + file
                        + "' (FORMAT parquet)");
                all.add("'" + file + "'");
                DataFile data = new DataFile(ticket[i], "hdfs://ns/data/" + ticket[i],
                        java.nio.file.Files.size(file), 1L, i == 3 ? Set.of("dn1") : Set.of());
                when(store.readableHere(data)).thenReturn(i == 3);
                when(store.inputFile(data)).thenReturn(new org.apache.parquet.io.LocalInputFile(file));
                when(store.inputFile(ticket[i])).thenReturn(new org.apache.parquet.io.LocalInputFile(file));
                when(store.duckDbUri(ticket[i])).thenReturn(file.toString());
                when(store.duckDbUri(data)).thenReturn(file.toString());
                if (i < 3) {
                    remote.add(data);
                    largest = Math.max(largest, data.size());
                } else {
                    when(parquet.sampleFile("s", "t")).thenReturn(Optional.of(data));
                }
                when(store.file(ticket[i])).thenReturn(data);
            }
        }
        when(store.parquetFiles("s/t")).thenReturn(new java.util.ArrayList<>(remote));
        when(parquet.dataStore()).thenReturn(store);
        when(parquet.localCopies()).thenReturn(true);
        when(parquet.getTableSchema("s", "t")).thenReturn(new Schema(List.of()));
        java.util.concurrent.ExecutorService ioPool = java.util.concurrent.Executors.newCachedThreadPool();
        DuckDbAdapter duckDb = org.mockito.Mockito.spy(new DuckDbAdapter(new pro.surpin.data.arrowflight
                .server.model.AppConfig(3, 100, 1, 131072, 0, 1, 1, null, false, null, null, null, null,
                null, false, 1048576, 67108864, 60000L, null, null, 32010, 5701, 60, 3, 1000, 30000, true,
                false, 64, null, null, "complete", true, true), ioPool));
        org.mockito.Mockito.doReturn(root).when(duckDb).localCopyRoot();
        List<Integer> copiesOnDisk = new java.util.ArrayList<>();
        org.mockito.Mockito.doAnswer(call -> {
            assertInstanceOf(CollectedAnswers.class, call.getArgument(2));
            try (java.util.stream.Stream<java.nio.file.Path> walk = java.nio.file.Files.walk(root)) {
                copiesOnDisk.add((int) walk.filter(p -> p.toString().endsWith(".parquet")).count());
            }
            return call.callRealMethod();
        }).when(duckDb).streamSql(any(), anyString(), any(CollectedAnswers.class),
                org.mockito.ArgumentMatchers.eq(true));
        ExecutionService service = new ExecutionService(parquet, duckDb,
                mock(MetadataService.class), null, ioPool);
        String source = "read_parquet([" + String.join(", ", all) + "])";
        System.setProperty("hdfsLocalCopyMaxBytes", String.valueOf(largest));
        try (BufferAllocator allocator = new RootAllocator()) {
            for (String query : List.of(
                    "SELECT g, count(*), sum(v), min(v), max(d), sum(d) FROM s.t WHERE v >= 10"
                            + " GROUP BY g ORDER BY g",
                    "SELECT g, min(d) AS lo, count(d) FROM s.t GROUP BY g ORDER BY g DESC",
                    "SELECT sum(v) AS total, count(*) AS n, max(v) FROM s.t WHERE g = 3",
                    // Item 30: the batches answer without LIMIT; with it, near-tied counts
                    // (the second batch has 10 rows) would keep only each batch's first groups.
                    "SELECT g, count(*) AS n FROM s.t WHERE v < 1010 OR v >= 2000 GROUP BY g"
                            + " ORDER BY n DESC, g LIMIT 3",
                    "SELECT DISTINCT g FROM s.t WHERE v > 500 ORDER BY g",
                    "SELECT v, d FROM s.t WHERE g = 2 ORDER BY v DESC LIMIT 5",
                    "SELECT DISTINCT g FROM s.t ORDER BY g DESC LIMIT 3",
                    // HAVING applied to the merged groups (totals 1140858..1144286): no batch
                    // alone reaches the bound, groups 3, 4 and 5 stay below it in total.
                    "SELECT g, sum(v) AS sv, count(*) FROM s.t GROUP BY g"
                            + " HAVING sum(v) > 1142000 AND max(v) >= 10 ORDER BY g",
                    "SELECT count(DISTINCT g) AS ng, sum(DISTINCT g), count(*) FROM s.t WHERE v > 5",
                    "SELECT g, count(DISTINCT d) AS nd, sum(v) FROM s.t GROUP BY g"
                            + " HAVING count(*) > 571 ORDER BY nd DESC, g LIMIT 4")) {
                copiesOnDisk.clear();
                List<String> expected = new java.util.ArrayList<>();
                try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
                        java.sql.Statement st = conn.createStatement();
                        java.sql.ResultSet rs = st.executeQuery(query.replace("s.t", source))) {
                    int columns = rs.getMetaData().getColumnCount();
                    while (rs.next()) {
                        StringBuilder row = new StringBuilder();
                        for (int c = 1; c <= columns; c++) {
                            row.append(c > 1 ? "|" : "").append(rs.getString(c));
                        }
                        expected.add(row.toString());
                    }
                }
                TextRows listener = new TextRows();
                service.readParquet(allocator, query, ticket, listener, true);
                assertEquals(expected, listener.rows, query);
                // Three batches of one copy each; the held file goes with the first.
                assertEquals(List.of(1, 1, 1), copiesOnDisk, query);
            }
            // Merged from the part answers: no fallback that holds every copy at once.
            verify(duckDb, org.mockito.Mockito.never()).streamSql(any(), anyString(),
                    any(TextRows.class), org.mockito.ArgumentMatchers.anyBoolean());
            assertEquals(0, allocator.getAllocatedMemory());
        } finally {
            System.clearProperty("hdfsLocalCopyMaxBytes");
            duckDb.close();
            ioPool.shutdownNow();
        }
    }

    /**
     * Item 30 part 2b: a COUNT(*) over all rows is answered from the footers, read through the
     * Java client, so files this node lacks blocks of are not copied.
     */
    @Test
    void footerAnsweredCountCopiesNothing(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        java.nio.file.Path root = dir.resolve("localcopy");
        String[] ticket = {"s/t/a.parquet", "s/t/b.parquet", "s/t/c.parquet"};
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        pro.surpin.data.arrowflight.server.storage.DataStore store =
                mock(pro.surpin.data.arrowflight.server.storage.DataStore.class);
        List<DataFile> remote = new java.util.ArrayList<>();
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
                java.sql.Statement st = conn.createStatement()) {
            for (int i = 0; i < ticket.length; i++) {
                java.nio.file.Path file = dir.resolve("f" + i + ".parquet");
                st.execute("COPY (SELECT range AS v FROM range(" + i * 1000 + ", " + (i + 1) * 1000
                        + ")) TO '" + file + "' (FORMAT parquet)");
                DataFile data = new DataFile(ticket[i], "hdfs://ns/data/" + ticket[i],
                        java.nio.file.Files.size(file), 1L, Set.of());
                when(store.readableHere(data)).thenReturn(false);
                when(store.inputFile(data)).thenReturn(new org.apache.parquet.io.LocalInputFile(file));
                when(store.inputFile(ticket[i])).thenReturn(new org.apache.parquet.io.LocalInputFile(file));
                when(store.duckDbUri(ticket[i])).thenReturn(file.toString());
                when(store.duckDbUri(data)).thenReturn(file.toString());
                when(store.file(ticket[i])).thenReturn(data);
                remote.add(data);
            }
        }
        when(parquet.sampleFile("s", "t")).thenReturn(Optional.of(remote.get(0)));
        when(store.parquetFiles("s/t")).thenReturn(new java.util.ArrayList<>(remote));
        when(parquet.dataStore()).thenReturn(store);
        when(parquet.localCopies()).thenReturn(true);
        when(parquet.getTableSchema("s", "t")).thenReturn(new Schema(List.of()));
        java.util.concurrent.ExecutorService ioPool = java.util.concurrent.Executors.newCachedThreadPool();
        pro.surpin.data.arrowflight.server.model.AppConfig config = new pro.surpin.data.arrowflight
                .server.model.AppConfig(3, 100, 1, 131072, 0, 1, 1, null, false, null, null, null, null,
                null, false, 1048576, 67108864, 60000L, null, null, 32010, 5701, 60, 3, 1000, 30000, true,
                false, 64, null, null, "complete", true, true);
        DuckDbAdapter duckDb = org.mockito.Mockito.spy(new DuckDbAdapter(config, ioPool));
        org.mockito.Mockito.doReturn(root).when(duckDb).localCopyRoot();
        ExecutionService service = new ExecutionService(parquet, duckDb,
                mock(MetadataService.class), config, ioPool);
        pro.surpin.data.arrowflight.server.model.ExecutionPathTracker path =
                new pro.surpin.data.arrowflight.server.model.ExecutionPathTracker();
        try (BufferAllocator allocator = new RootAllocator()) {
            TextRows listener = new TextRows();
            service.readParquet(allocator, "SELECT count(*) FROM s.t", ticket, listener, true, path);
            assertEquals(List.of("3000"), listener.rows);
            assertEquals(0, allocator.getAllocatedMemory());
        } finally {
            duckDb.close();
            ioPool.shutdownNow();
        }
        assertEquals(pro.surpin.data.arrowflight.server.model.ExecutionPath.FOOTER_COUNT, path.path());
        assertFalse(java.nio.file.Files.exists(root), "nothing copied");
        assertTrue(ExecutionService.footerCountOnly(ParquetQueryParser.parse("SELECT count(*) AS n FROM s.t")));
        for (String query : List.of("SELECT count(*) FROM s.t WHERE v > 1", "SELECT count(v) FROM s.t",
                "SELECT v, count(*) FROM s.t GROUP BY v", "SELECT count(*), sum(v) FROM s.t",
                "SELECT count(DISTINCT v) FROM s.t", "SELECT v FROM s.t")) {
            assertFalse(ExecutionService.footerCountOnly(ParquetQueryParser.parse(query)), query);
        }
    }

    /**
     * Item 33 regression: on a copy disk smaller than a table (the stand's 64 MiB tmpfs, scaled
     * down), a sequence of one-query-at-a-time batched answers over the table's copies is never
     * refused, and no copy stays pinned once a query's batches are released.
     */
    @Test
    void sequentialBatchedQueriesOnASmallCopyDiskAreNeverRefusedAndLeaveNothingPinned(
            @org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
        long capacity = 67_108;
        long reserve = capacity / 10;
        java.nio.file.Path root = dir.resolve("c");
        List<DataFile> files = new java.util.ArrayList<>();
        String[] ticket = new String[9];
        for (int i = 0; i < ticket.length; i++) {
            ticket[i] = "spread/lineitem_sp/data_" + i + ".parquet";
            files.add(new DataFile(ticket[i], "hdfs://ns/data/" + ticket[i], i == 6 ? 25_400 : 17_300,
                    1L, Set.of()));
        }
        pro.surpin.data.arrowflight.server.storage.LocalCopyCache.FreeSpace disk = d -> {
            try (java.util.stream.Stream<java.nio.file.Path> walk = java.nio.file.Files.walk(root)) {
                return capacity - walk.filter(java.nio.file.Files::isRegularFile)
                        .mapToLong(f -> f.toFile().length()).sum();
            }
        };
        try (pro.surpin.data.arrowflight.server.storage.LocalCopyCache cache =
                new pro.surpin.data.arrowflight.server.storage.LocalCopyCache(root, 32L << 30, 2,
                        f -> new java.io.ByteArrayInputStream(new byte[(int) f.size()]), reserve, disk)) {
            for (int query = 0; query < 20; query++) {
                // Odd queries keep the ticket order (a scan), even ones take cached copies first.
                String[] order = query % 2 == 0
                        ? ExecutionService.cachedFirst(ticket, cache.present(files)) : ticket;
                List<String[]> batches = ExecutionService.copyBatches(order, files, cache.room());
                assertTrue(batches.size() > 1, "query " + query);
                for (String[] batch : batches) {
                    List<DataFile> toCopy = new java.util.ArrayList<>();
                    for (String rel : batch) {
                        files.stream().filter(f -> f.relativePath().equals(rel)).forEach(toCopy::add);
                    }
                    try (pro.surpin.data.arrowflight.server.storage.LocalCopyCache.Lease lease =
                            cache.acquire(toCopy)) {
                        assertTrue(cache.inUseBytes() > 0);
                        assertTrue(disk.usableBytes(root) >= reserve, "query " + query);
                    }
                }
                assertEquals(0, cache.inUseBytes(), "query " + query);
            }
        }
    }

    /** Item 30 part 2b: which aggregates, DISTINCT and LIMIT queries are merged from batches of copies. */
    @Test
    void onlyMergeableQueriesAreAnsweredPerBatch() {
        for (String query : List.of("SELECT a, count(*) FROM s.t GROUP BY a",
                "SELECT sum(b), min(c) FROM s.t WHERE a > 1",
                "SELECT a, max(b) FROM s.t GROUP BY a ORDER BY a",
                "SELECT a, count(*) FROM s.t GROUP BY a LIMIT 3",
                "SELECT a, sum(b) AS s FROM s.t GROUP BY a ORDER BY s DESC LIMIT 3",
                "SELECT DISTINCT a FROM s.t", "SELECT DISTINCT a FROM s.t ORDER BY a",
                "SELECT a, b FROM s.t ORDER BY b DESC LIMIT 5", "SELECT a FROM s.t LIMIT 5",
                "SELECT count(DISTINCT a) FROM s.t", "SELECT a, sum(b) FROM s.t GROUP BY a HAVING sum(b) > 1",
                "SELECT a, count(DISTINCT b), sum(DISTINCT b), min(c) FROM s.t GROUP BY a",
                "SELECT a, max(b) AS m FROM s.t GROUP BY a HAVING NOT (a = 2 OR count(*) * 2 <= 10)"
                        + " ORDER BY m LIMIT 2",
                // Item 33 regression (egret sp-group / sp-distinct shapes).
                "SELECT count(DISTINCT a), count(DISTINCT b) FROM s.t", "SELECT a, avg(b) FROM s.t GROUP BY a")) {
            assertTrue(ExecutionService.mergeable(query), query);
        }
        for (String query : List.of("SELECT a FROM s.t", "SELECT a FROM s.t ORDER BY a",
                "SELECT a, count(*) FROM s.t GROUP BY a ORDER BY count(*) LIMIT 3",
                "SELECT a FROM s.t ORDER BY a LIMIT 3 OFFSET 1",
                "SELECT count(DISTINCT a + 1) FROM s.t", "SELECT a, avg(b) FROM s.t GROUP BY a LIMIT 2",
                "SELECT a, sum(b) FROM s.t GROUP BY a HAVING avg(b) > 1",
                "SELECT a, sum(b) FROM s.t GROUP BY a HAVING count(DISTINCT b) > 1",
                "SELECT a, sum(b) FROM s.t GROUP BY a HAVING abs(sum(b)) > 1",
                "SELECT sum(b) FROM s.t GROUP BY a HAVING sum(b) > 1",
                "SELECT count(*) FROM s.t GROUP BY a", "SELECT a, avg(DISTINCT b) FROM s.t GROUP BY a",
                "SELECT x.a, count(*) FROM s.t x JOIN s.u y ON x.a = y.a GROUP BY x.a")) {
            assertFalse(ExecutionService.mergeable(query), query);
        }
    }

    /** Collects streamed rows as text, columns separated by |. */
    private static final class TextRows implements
            org.apache.arrow.flight.FlightProducer.ServerStreamListener {
        private final List<String> rows = new java.util.ArrayList<>();
        private org.apache.arrow.vector.VectorSchemaRoot root;

        @Override
        public void start(org.apache.arrow.vector.VectorSchemaRoot r,
                org.apache.arrow.vector.dictionary.DictionaryProvider p,
                org.apache.arrow.vector.ipc.message.IpcOption o) {
            root = r;
        }

        @Override
        public void putNext() {
            for (int row = 0; row < root.getRowCount(); row++) {
                StringBuilder text = new StringBuilder();
                for (int c = 0; c < root.getFieldVectors().size(); c++) {
                    Object value = root.getFieldVectors().get(c).getObject(row);
                    text.append(c > 0 ? "|" : "").append(value == null ? null
                            : value instanceof BigDecimal d ? d.toPlainString() : value.toString());
                }
                rows.add(text.toString());
            }
        }

        @Override
        public void putNext(org.apache.arrow.memory.ArrowBuf metadata) {
            putNext();
        }

        @Override
        public void putMetadata(org.apache.arrow.memory.ArrowBuf metadata) {
            // No rows.
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void setOnReadyHandler(Runnable handler) {
            // Always ready.
        }

        @Override
        public void setOnCancelHandler(Runnable handler) {
            // Never cancelled.
        }

        @Override
        public void setUseZeroCopy(boolean enabled) {
            // Not needed.
        }

        @Override
        public void error(Throwable cause) {
            throw new AssertionError(cause);
        }

        @Override
        public void completed() {
            // The caller completes.
        }
    }

    /** Item 30 part 2b: copies that fit the cache stay one batch; others are cut to fit. */
    @Test
    void copyBatchesFitTheCacheLimit() {
        String[] ticket = {"a", "b", "c", "d", "e"};
        List<DataFile> unreadable = List.of(
                new DataFile("a", "hdfs://a", 40, 1L, Set.of()),
                new DataFile("b", "hdfs://b", 40, 1L, Set.of()),
                new DataFile("d", "hdfs://d", 150, 1L, Set.of()),
                new DataFile("e", "hdfs://e", 10, 1L, Set.of()));
        assertEquals(1, ExecutionService.copyBatches(ticket, unreadable, 240).size());
        assertEquals(1, ExecutionService.copyBatches(ticket, unreadable, 0).size());
        assertEquals(1, ExecutionService.copyBatches(ticket, List.of(), 1).size());
        // Local copies off (Dev CI 41669461235: every scan failed with a NullPointerException).
        assertEquals(1, ExecutionService.copyBatches(ticket, null, 0).size());
        assertEquals(1, ExecutionService.copyBatches(ticket, null, 100).size());
        List<String[]> batches = ExecutionService.copyBatches(ticket, unreadable, 100);
        assertEquals(List.of(List.of("c", "a", "b"), List.of("d"), List.of("e")),
                batches.stream().map(List::of).toList());
    }

    /** Item 33 (C): copies already cached go into the first batch, so they are reused. */
    @Test
    void cachedCopiesGoFirst() {
        String[] ticket = {"a", "b", "c", "d"};
        assertSame(ticket, ExecutionService.cachedFirst(ticket, Set.of()));
        assertArrayEquals(new String[] {"b", "d", "a", "c"},
                ExecutionService.cachedFirst(ticket, Set.of("d", "b")));
        List<DataFile> unreadable = List.of(
                new DataFile("a", "hdfs://a", 40, 1L, Set.of()),
                new DataFile("b", "hdfs://b", 40, 1L, Set.of()),
                new DataFile("c", "hdfs://c", 40, 1L, Set.of()),
                new DataFile("d", "hdfs://d", 40, 1L, Set.of()));
        assertEquals(List.of(List.of("d", "a"), List.of("b", "c")),
                ExecutionService.copyBatches(ExecutionService.cachedFirst(ticket, Set.of("d")),
                        unreadable, 80).stream().map(List::of).toList());
    }

    /** Item 30 part 2b: only plain scans are read in batches of copies. */
    @Test
    void onlyPlainScansAreConcatenatable() {
        assertTrue(ExecutionService.concatenatable(ParquetQueryParser.parse(
                "SELECT a, b FROM s.t WHERE a > 1")));
        for (String query : List.of("SELECT count(*) FROM s.t", "SELECT a FROM s.t ORDER BY a",
                "SELECT a FROM s.t LIMIT 3", "SELECT DISTINCT a FROM s.t",
                "SELECT a, b FROM s.t GROUP BY a, b", "SELECT a FROM s.t OFFSET 2")) {
            assertFalse(ExecutionService.concatenatable(ParquetQueryParser.parse(query)), query);
        }
    }

    /**
     * Item 31 (5): complete-mode queries of one table that batches of copies answer exactly go
     * the batched path; joins, subqueries of other tables and unmergeable shapes do not.
     */
    @Test
    void completeQueriesOfOneTableAreBatchable() {
        for (String sql : List.of("SELECT a, b FROM s.t WHERE a > 1",
                "SELECT count(*) AS n FROM s.t WHERE a > 1",
                "SELECT a, count(*) AS n, sum(b) AS q FROM s.t GROUP BY a",
                "SELECT a, count(*) AS n FROM s.t GROUP BY a ORDER BY a LIMIT 3",
                "SELECT DISTINCT a FROM s.t")) {
            ParquetQueryParser parsed = ParquetQueryParser.parse(sql);
            assertTrue(ExecutionService.batchableComplete(sql, parsed,
                    List.of(new ParquetQueryParser.JoinTable("s", "t", null))), sql);
            assertFalse(ExecutionService.batchableComplete(sql, parsed,
                    List.of(new ParquetQueryParser.JoinTable("s", "t", null),
                            new ParquetQueryParser.JoinTable("s", "u", null))), sql);
            assertFalse(ExecutionService.batchableComplete(sql, parsed,
                    List.of(new ParquetQueryParser.JoinTable(null, "t", null))), sql);
        }
        String join = "SELECT count(*) FROM s.t x JOIN s.u y ON x.a = y.a";
        assertFalse(ExecutionService.batchableComplete(join, ParquetQueryParser.parse(join),
                List.of(new ParquetQueryParser.JoinTable("s", "t", null),
                        new ParquetQueryParser.JoinTable("s", "u", null))));
        String ordered = "SELECT a FROM s.t ORDER BY a";
        assertFalse(ExecutionService.batchableComplete(ordered, ParquetQueryParser.parse(ordered),
                List.of(new ParquetQueryParser.JoinTable("s", "t", null))));
    }

    /** Item 31 (5): the batched complete path is taken only when it describes the same columns. */
    @Test
    void sameColumnsIgnoresNullabilityOnly() {
        Schema a = new Schema(List.of(Field.nullable("n", new ArrowType.Int(64, true))));
        Schema b = new Schema(List.of(Field.notNullable("n", new ArrowType.Int(64, true))));
        Schema c = new Schema(List.of(Field.nullable("n", new ArrowType.Decimal(38, 0, 128))));
        Schema d = new Schema(List.of(Field.nullable("m", new ArrowType.Int(64, true))));
        assertTrue(ExecutionService.sameColumns(a, b));
        assertFalse(ExecutionService.sameColumns(a, c));
        assertFalse(ExecutionService.sameColumns(a, d));
        assertFalse(ExecutionService.sameColumns(a, new Schema(List.of())));
    }

    /** The table directory of a ticket file is its first two segments. */
    @Test
    void tableDirectoryOfATicketFile() {
        assertEquals("s/t", ExecutionService.tableDirectory("s/t/k=1/a.parquet"));
        assertEquals("s/t", ExecutionService.tableDirectory("s/t/a.parquet"));
        assertEquals("t", ExecutionService.tableDirectory("t/a.parquet"));
        assertEquals("", ExecutionService.tableDirectory("a.parquet"));
    }

    /**
     * Item 30: on SASL HDFS a join is described against each table's footer read through the
     * Java client when this node lacks blocks of the table's sample file, never through DuckDB
     * on that file (the CI cell's join answered "inconsistent schema" on two of three nodes).
     */
    @Test
    void joinDescribeUsesFooterStubsForFilesThisNodeCannotRead(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        java.nio.file.Path local = dir.resolve("a.parquet");
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
                java.sql.Statement st = conn.createStatement()) {
            st.execute("COPY (SELECT 1::BIGINT AS k, 1.50::DECIMAL(15,2) AS q) TO '" + local + "' (FORMAT parquet)");
        }
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        pro.surpin.data.arrowflight.server.storage.DataStore store =
                mock(pro.surpin.data.arrowflight.server.storage.DataStore.class);
        DuckDbAdapter duckDb = mock(DuckDbAdapter.class);
        DataFile remote = new DataFile("s/t/a.parquet", "hdfs://ns/data/s/t/a.parquet",
                java.nio.file.Files.size(local), 1L, Set.of());
        when(parquet.wholeFileReadsOnly()).thenReturn(true);
        when(parquet.sampleFile(anyString(), anyString())).thenReturn(Optional.of(remote));
        when(parquet.dataStore()).thenReturn(store);
        when(parquet.getTableSchema(anyString(), anyString())).thenReturn(new Schema(List.of()));
        when(store.duckDbUri(any(DataFile.class))).thenReturn("hdfs://ns/data/s/t/a.parquet");
        when(store.readableHere(remote)).thenReturn(false);
        when(store.inputFile(remote)).thenAnswer(inv -> new org.apache.parquet.io.LocalInputFile(local));
        java.nio.file.Path stubs = java.nio.file.Files.createDirectories(dir.resolve("stubs"));
        when(duckDb.footerStubRoot()).thenReturn(stubs);
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:")) {
            when(duckDb.connection()).thenReturn(conn);
            Schema schema = new Schema(List.of(Field.nullable("n", new ArrowType.Int(64, true))));
            java.util.concurrent.atomic.AtomicReference<String> views = new java.util.concurrent.atomic.AtomicReference<>();
            when(duckDb.describeSql(any(), anyString())).thenAnswer(inv -> {
                try (java.sql.Statement st = conn.createStatement();
                        java.sql.ResultSet rs = st.executeQuery(
                                "SELECT string_agg(sql, ' ') FROM duckdb_views() WHERE temporary")) {
                    rs.next();
                    views.set(rs.getString(1));
                }
                return schema;
            });
            ExecutionService service = new ExecutionService(parquet, duckDb,
                    mock(MetadataService.class), null, null);

            try (RootAllocator allocator = new RootAllocator()) {
                assertEquals(schema, service.resultSchema(allocator,
                        "SELECT count(*) AS n FROM s.t a JOIN s.u b ON a.k = b.k"));
            }
            assertTrue(views.get().contains(stubs.toString()), views.get());
            assertFalse(views.get().contains("hdfs://"), views.get());
        }
        try (java.util.stream.Stream<java.nio.file.Path> left = java.nio.file.Files.walk(stubs)) {
            assertEquals(0, left.filter(java.nio.file.Files::isRegularFile).count(), "stubs removed");
        }
    }
}
