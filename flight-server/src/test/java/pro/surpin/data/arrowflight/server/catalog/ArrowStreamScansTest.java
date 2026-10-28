package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.spi.catalog.ArrowScan;
import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;

/**
 * Per-reference Arrow streams of tables read by their catalog ({@link ArrowStreamScans}): a
 * self-join reads two streams, partition columns arrive as text and read as their declared
 * type, the requested columns reach the catalog, closing drops the views and leaks no buffer
 * (also for a stream DuckDB never read and an export that failed).
 */
class ArrowStreamScansTest {

    static final TableSnapshot SNAPSHOT = new TableSnapshot(new TableName("s", "t"), "1",
            "f", List.of(new ColumnSpec("id", "BIGINT", true, null), new ColumnSpec("p", "INTEGER", true, null),
                    new ColumnSpec("label", "VARCHAR", true, null)),
            List.of("p"), List.of(file("a", "1"), file("b", null)), new ReadSpec.ArrowStream("orc"));

    private static TableFile file(String name, String p) {
        java.util.Map<String, String> values = new java.util.HashMap<>();
        values.put("p", p);
        return new TableFile("file:/data/" + name + ".orc", 1, 0, null, values, 3, null);
    }

    /** Catalog whose files hold rows id 0..2, label "l<id>", partition value from the file. */
    static final class StreamingCatalog implements Catalog {
        final List<ArrowScan> scans = new ArrayList<>();
        boolean fail;

        @Override
        public List<String> schemas() {
            return List.of("s");
        }

        @Override
        public List<String> tables(String schema) {
            return List.of("t");
        }

        @Override
        public Optional<TableSnapshot> current(TableName table) {
            return Optional.of(SNAPSHOT);
        }

        @Override
        public List<String> readableRoots() {
            return List.of("file:/data");
        }

        @Override
        public void exportArrowScan(TableSnapshot snapshot, List<TableFile> files, ArrowScan scan,
                long streamAddress, Object allocator) throws IOException {
            if (fail) {
                throw new IOException("cannot open");
            }
            scans.add(scan);
            BufferAllocator buffers = (BufferAllocator) allocator;
            Data.exportArrayStream(buffers, new FilesReader(buffers, files, scan.columns()),
                    ArrowArrayStream.wrap(streamAddress));
        }
    }

    /** One batch per file with the requested columns. */
    static final class FilesReader extends ArrowReader {
        private final List<TableFile> files;
        private final List<String> columns;
        private int next;

        FilesReader(BufferAllocator allocator, List<TableFile> files, List<String> columns) {
            super(allocator);
            this.files = files;
            this.columns = columns;
        }

        @Override
        public boolean loadNextBatch() throws IOException {
            if (next >= files.size()) {
                return false;
            }
            TableFile file = files.get(next++);
            VectorSchemaRoot root = getVectorSchemaRoot();
            for (FieldVector vector : root.getFieldVectors()) {
                vector.allocateNew();
                for (int row = 0; row < 3; row++) {
                    String text = switch (vector.getName()) {
                        case "p" -> file.partitionValues().get("p");
                        case "label" -> "l" + row;
                        default -> null;
                    };
                    if (vector instanceof BigIntVector ids) {
                        ids.setSafe(row, row);
                    } else if (text == null) {
                        vector.setNull(row);
                    } else {
                        ((VarCharVector) vector).setSafe(row, text.getBytes(
                                java.nio.charset.StandardCharsets.UTF_8));
                    }
                }
                vector.setValueCount(3);
            }
            root.setRowCount(3);
            return true;
        }

        @Override
        public long bytesRead() {
            return 0;
        }

        @Override
        protected void closeReadSource() {
        }

        @Override
        protected Schema readSchema() {
            List<Field> fields = new ArrayList<>();
            for (String column : columns) {
                fields.add(Field.nullable(column, "id".equals(column)
                        ? new ArrowType.Int(64, true) : ArrowType.Utf8.INSTANCE));
            }
            return new Schema(fields);
        }
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

    private static long views(Connection connection) throws Exception {
        return Long.parseLong(rows(connection, "SELECT count(*) FROM duckdb_views()"
                + " WHERE view_name LIKE '" + ArrowStreamScans.NAME_PREFIX + "%'").get(0));
    }

    @Test
    void eachReferenceReadsItsOwnStream() throws Exception {
        StreamingCatalog catalog = new StreamingCatalog();
        try (BufferAllocator allocator = new RootAllocator();
                Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            try (ArrowStreamScans scans = new ArrowStreamScans(connection, allocator)) {
                ArrowScan both = new ArrowScan(List.of("id", "p"), null);
                String left = scans.source(catalog, SNAPSHOT, SNAPSHOT.files(), both);
                String right = scans.source(catalog, SNAPSHOT, SNAPSHOT.files().subList(0, 1),
                        new ArrowScan(List.of("p", "id", "label"), null));
                assertEquals(2, scans.size());
                assertEquals(List.of("0,1,0,l0", "1,1,1,l1", "2,1,2,l2"), rows(connection,
                        "SELECT a.id, a.p, b.id, b.label FROM " + left + " a JOIN " + right
                        + " b ON a.id = b.id AND a.p = b.p ORDER BY a.id"));
                assertEquals(List.of(List.of("id", "p"), List.of("p", "id", "label")),
                        catalog.scans.stream().map(ArrowScan::columns).toList());
                assertEquals(2, views(connection));
            }
            assertEquals(0, views(connection));
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    @Test
    void partitionTextReadsAsTheDeclaredTypeAndNullStaysNull() throws Exception {
        StreamingCatalog catalog = new StreamingCatalog();
        try (BufferAllocator allocator = new RootAllocator();
                Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                ArrowStreamScans scans = new ArrowStreamScans(connection, allocator)) {
            String source = scans.source(catalog, SNAPSHOT, SNAPSHOT.files(),
                    new ArrowScan(List.of("p"), null));
            assertEquals(List.of("INTEGER,1,3", "INTEGER,null,3"), rows(connection,
                    "SELECT typeof(p), p, count(*) FROM " + source + " GROUP BY p ORDER BY p"));
        }
    }

    @Test
    void unreadAndFailedStreamsAreReleased() throws Exception {
        StreamingCatalog catalog = new StreamingCatalog();
        try (BufferAllocator allocator = new RootAllocator();
                Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            try (ArrowStreamScans scans = new ArrowStreamScans(connection, allocator)) {
                scans.source(catalog, SNAPSHOT, SNAPSHOT.files(), new ArrowScan(List.of("id"), null));
                catalog.fail = true;
                assertThrows(IOException.class, () -> scans.source(catalog, SNAPSHOT,
                        SNAPSHOT.files(), new ArrowScan(List.of("id"), null)));
                assertEquals(2, scans.size());
            }
            assertEquals(0, views(connection));
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    @Test
    void onlyDeclaredColumnsOfArrowStreamSnapshotsAreRead() throws Exception {
        StreamingCatalog catalog = new StreamingCatalog();
        TableSnapshot parquet = new TableSnapshot(SNAPSHOT.name(), "1", "f", SNAPSHOT.columns(),
                null, null, new ReadSpec.ParquetFiles(false));
        try (BufferAllocator allocator = new RootAllocator();
                Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                ArrowStreamScans scans = new ArrowStreamScans(connection, allocator)) {
            assertThrows(IllegalArgumentException.class, () -> scans.source(catalog, parquet,
                    List.of(), new ArrowScan(List.of("id"), null)));
            assertThrows(IllegalArgumentException.class, () -> scans.source(catalog, SNAPSHOT,
                    SNAPSHOT.files(), new ArrowScan(List.of("nope"), null)));
            assertEquals(0, scans.size());
        }
        assertEquals("\"id\", TRY_CAST(\"p\" AS INTEGER) AS \"p\"",
                ArrowStreamScans.projection(SNAPSHOT, new ArrowScan(List.of("id", "p"), null)));
    }

    @Test
    void parquetSourcesRefuseArrowStreamSnapshots() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> CatalogSources.filesSource(SNAPSHOT, SNAPSHOT.files()));
        assertTrue(e.getMessage().contains("orc"), e.getMessage());
    }
}
