package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.DeleteFile;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;

/**
 * Iceberg deletes applied with read_parquet ({@link CatalogSources#deletesAppliedSource}): data
 * files with field IDs (one written before a rename), position deletes naming the data files
 * with {@code file:} and rows of a file they do not apply to, equality deletes on one and on two
 * columns with nulls, a file without deletes. Every answer is compared with rows computed here
 * from the same generated values, not by DuckDB.
 */
class CatalogSourcesDeletesTest {

    private static final int ROWS_PER_FILE = 1000;

    @TempDir
    static Path dir;
    static TableSnapshot snapshot;
    static List<Row> expected;

    /**
     * One generated row.
     *
     * @param id id
     * @param label label or null
     * @param v value or null
     * @param grp group or null
     */
    record Row(long id, String label, Double v, Integer grp) {
    }

    private static Row row(long id) {
        String label = id % 17 == 0 ? null : "l" + (id % 50);
        Double v = id % 11 == 0 ? null : (double) (id % 7);
        Integer grp = id % 13 == 0 ? null : (int) (id % 5);
        return new Row(id, label, v, grp);
    }

    /**
     * Writes the files and the expected rows.
     *
     * @throws Exception on DuckDB errors
     */
    @BeforeAll
    static void write() throws Exception {
        String f1 = dir.resolve("data/f1.parquet").toString();
        String f2 = dir.resolve("data/f2.parquet").toString();
        String f3 = dir.resolve("data/f3.parquet").toString();
        String p1 = dir.resolve("data/p1.parquet").toString();
        String e1 = dir.resolve("data/e1.parquet").toString();
        String e2 = dir.resolve("data/e2.parquet").toString();
        dir.resolve("data").toFile().mkdirs();
        // Position deletes of f1 (and rows of f2, which the plan does not give this file).
        List<Long> f1Positions = List.of(0L, 3L, 4L, 500L, 999L);
        List<Long> f2Positions = List.of(1L, 2L);
        // Equality deletes: label (with a null), and (grp, v) pairs (with nulls).
        List<String> e1Labels = new ArrayList<>(List.of("l7", "l8"));
        e1Labels.add(null);
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = c.createStatement()) {
            String gen = "SELECT range AS id,"
                    + " CASE WHEN range %% 17 = 0 THEN NULL ELSE 'l' || (range %% 50) END AS %s,"
                    + " CASE WHEN range %% 11 = 0 THEN NULL ELSE CAST(range %% 7 AS DOUBLE) END AS v,"
                    + " CASE WHEN range %% 13 = 0 THEN NULL ELSE CAST(range %% 5 AS INTEGER) END AS grp"
                    + " FROM range(%d, %d)";
            // f1 still has the label under its old name: read by field ID.
            s.execute("COPY (" + gen.formatted("name", 0, ROWS_PER_FILE) + ") TO '" + f1
                    + "' (FORMAT parquet, FIELD_IDS {id: 1, name: 2, v: 3, grp: 4})");
            s.execute("COPY (" + gen.formatted("label", ROWS_PER_FILE, 2 * ROWS_PER_FILE)
                    + ") TO '" + f2 + "' (FORMAT parquet, FIELD_IDS {id: 1, label: 2, v: 3, grp: 4})");
            s.execute("COPY (" + gen.formatted("label", 2 * ROWS_PER_FILE, 3 * ROWS_PER_FILE)
                    + ") TO '" + f3 + "' (FORMAT parquet, FIELD_IDS {id: 1, label: 2, v: 3, grp: 4})");
            StringBuilder positions = new StringBuilder();
            for (Long pos : f1Positions) {
                positions.append(positions.isEmpty() ? "" : ", ").append("('file:").append(f1)
                        .append("', ").append(pos).append(')');
            }
            for (Long pos : f2Positions) {
                positions.append(", ('file://").append(f2).append("', ").append(pos).append(')');
            }
            s.execute("COPY (SELECT * FROM (VALUES " + positions + ") t(file_path, pos)) TO '" + p1
                    + "' (FORMAT parquet, FIELD_IDS {file_path: 2147483546, pos: 2147483545})");
            s.execute("COPY (SELECT * FROM (VALUES ('l7'), ('l8'), (NULL)) t(lbl)) TO '" + e1
                    + "' (FORMAT parquet, FIELD_IDS {lbl: 2})");
            s.execute("COPY (SELECT * FROM (VALUES (1, 2.0), (NULL, 3.0), (2, NULL), (NULL, NULL))"
                    + " t(g, val)) TO '" + e2 + "' (FORMAT parquet, FIELD_IDS {g: 4, val: 3})");
        }
        DeleteFile pos = new DeleteFile("file:" + p1, 1, DeleteFile.Kind.POSITION, List.of());
        DeleteFile eqLabel = new DeleteFile(e1, 1, DeleteFile.Kind.EQUALITY, List.of(2));
        DeleteFile eqPair = new DeleteFile(e2, 1, DeleteFile.Kind.EQUALITY, List.of(4, 3));
        List<TableFile> files = List.of(
                file(f1, List.of(pos, eqLabel, eqPair)),
                file(f2, List.of(eqLabel)),
                file(f3, List.of()));
        snapshot = new TableSnapshot(new TableName("ice", "t"), "1.0", "f", columns(), List.of(),
                files, new ReadSpec.IcebergMetadata(dir.resolve("metadata/v3.metadata.json").toString()));
        expected = new ArrayList<>();
        for (long id = 0; id < 3 * ROWS_PER_FILE; id++) {
            Row r = row(id);
            int file = (int) (id / ROWS_PER_FILE);
            long position = id % ROWS_PER_FILE;
            boolean deleted = file == 0 && f1Positions.contains(position);
            if (file <= 1 && e1Labels.contains(r.label())) {
                deleted = true;
            }
            if (file == 0 && (Objects.equals(r.grp(), 1) && Objects.equals(r.v(), 2.0)
                    || r.grp() == null && Objects.equals(r.v(), 3.0)
                    || Objects.equals(r.grp(), 2) && r.v() == null
                    || r.grp() == null && r.v() == null)) {
                deleted = true;
            }
            if (!deleted) {
                expected.add(r);
            }
        }
    }

    private static List<ColumnSpec> columns() {
        return List.of(new ColumnSpec("id", "BIGINT", false, 1),
                new ColumnSpec("label", "VARCHAR", true, 2),
                new ColumnSpec("v", "DOUBLE", true, 3),
                new ColumnSpec("grp", "INTEGER", true, 4));
    }

    private static TableFile file(String location, List<DeleteFile> deletes) {
        return new TableFile(location, 1, 0, Set.of(), Map.of(), ROWS_PER_FILE, deletes);
    }

    private static List<Row> rows(String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                rows.add(new Row(rs.getLong("id"), rs.getString("label"),
                        (Double) rs.getObject("v"), (Integer) rs.getObject("grp")));
            }
        }
        return rows;
    }

    private static long count(String sql) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static long expectedCount(Predicate<Row> filter) {
        return expected.stream().filter(filter).count();
    }

    /** Verifies every row and column against the independently computed rows. */
    @Test
    void allRowsMatchTheIndependentAnswer() throws Exception {
        String source = CatalogSources.deletesAppliedSource(snapshot, snapshot.files());
        assertNotNull(source);
        assertFalse(source.contains("iceberg_scan"), source);
        List<Row> actual = rows("SELECT * FROM " + source + " ORDER BY id");
        assertTrue(expected.size() < 3 * ROWS_PER_FILE - 100, "the deletes remove rows");
        assertEquals(expected, actual);
    }

    /**
     * Verifies queries that read some columns only, with filters on equality columns and on
     * others, aggregates, and a self join.
     */
    @Test
    void projectionsAndFiltersMatch() throws Exception {
        String t = CatalogSources.deletesAppliedSource(snapshot, snapshot.files());
        assertEquals(expected.size(), count("SELECT count(*) FROM " + t));
        assertEquals(expectedCount(r -> r.label() != null), count("SELECT count(label) FROM " + t));
        assertEquals(expectedCount(r -> r.id() % 2 == 0),
                count("SELECT count(*) FROM " + t + " WHERE id % 2 = 0"));
        assertEquals(expectedCount(r -> "l9".equals(r.label())),
                count("SELECT count(id) FROM " + t + " WHERE label = 'l9'"));
        assertEquals(0, count("SELECT count(*) FROM " + t + " WHERE label = 'l7' AND id < 2000"));
        assertEquals(expectedCount(r -> Objects.equals(r.grp(), 1) && Objects.equals(r.v(), 2.0)),
                count("SELECT count(*) FROM " + t + " WHERE grp = 1 AND v = 2"));
        assertEquals(expectedCount(r -> r.v() != null && r.v() > 3),
                count("SELECT count(*) FROM " + t + " WHERE v > 3"));
        assertEquals(expected.stream().mapToLong(Row::id).sum(), count("SELECT sum(id) FROM " + t));
        assertEquals(expectedCount(r -> r.id() < 10), count("SELECT count(*) FROM " + t + " a JOIN "
                + t + " b USING (id) WHERE a.id < 10"));
    }

    /** Verifies a subset of the files reads those files only, with their deletes. */
    @Test
    void someFilesOnly() throws Exception {
        String t = CatalogSources.deletesAppliedSource(snapshot, snapshot.files().subList(1, 2));
        assertEquals(expectedCount(r -> r.id() >= ROWS_PER_FILE && r.id() < 2 * ROWS_PER_FILE),
                count("SELECT count(*) FROM " + t));
    }

    /** Verifies the cases the source does not cover are left to iceberg_scan. */
    @Test
    void unsupportedDeletesAreRefused() {
        TableFile f = snapshot.files().get(1);
        DeleteFile dv = new DeleteFile("/x.puffin", 1, DeleteFile.Kind.DELETION_VECTOR, List.of());
        assertNull(CatalogSources.deletesAppliedSource(with(columns(), withDeletes(f, dv)),
                List.of(withDeletes(f, dv))));
        DeleteFile dropped = new DeleteFile("/e.parquet", 1, DeleteFile.Kind.EQUALITY, List.of(9));
        assertNull(CatalogSources.deletesAppliedSource(with(columns(), withDeletes(f, dropped)),
                List.of(withDeletes(f, dropped))));
        List<ColumnSpec> noId = List.of(new ColumnSpec("id", "BIGINT", false, null));
        assertNull(CatalogSources.deletesAppliedSource(with(noId, f), List.of(f)));
        List<ColumnSpec> rowNumber = List.of(new ColumnSpec("File_Row_Number", "BIGINT", false, 1));
        assertNull(CatalogSources.deletesAppliedSource(with(rowNumber, f), List.of(f)));
    }

    /** Verifies the SQL form of the location normalisation for position delete paths. */
    @Test
    void positionDeletePathsAreNormalisedLikeTheCatalog() throws Exception {
        for (String location : List.of("file:/a/b.parquet", "FILE:///a/b.parquet", "//a/b.parquet",
                "/a/b.parquet/", " s3://bucket//k/x.parquet", "hdfs://ns/a/b.parquet", "file://host/x",
                "/")) {
            try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                    Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT "
                            + CatalogSources.normalizedLocation("'" + location + "'"))) {
                rs.next();
                assertEquals(CatalogLocations.normalize(location), rs.getString(1), location);
            }
        }
    }

    private static TableFile withDeletes(TableFile f, DeleteFile delete) {
        return new TableFile(f.location(), f.size(), 0, Set.of(), Map.of(), f.recordCount(),
                List.of(delete));
    }

    private static TableSnapshot with(List<ColumnSpec> columns, TableFile file) {
        return new TableSnapshot(snapshot.name(), "1.0", "f", columns, List.of(), List.of(file),
                snapshot.read());
    }
}
