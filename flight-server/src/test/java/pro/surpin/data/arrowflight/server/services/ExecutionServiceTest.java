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
        try (ExecutionService.CopiedQuery same = service.completeFromCopies(query)) {
            assertFalse(same.copied());
            assertEquals(query, same.sql());
        }
        when(store.readableHere(remote)).thenReturn(false);
        when(parquet.localCopies()).thenReturn(false);
        assertEquals(query, service.completeFromCopies(query).sql());
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
