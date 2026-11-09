package pro.surpin.data.arrowflight.server.services;

import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.storage.DataStore;
import pro.surpin.data.arrowflight.spi.storage.DataFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests file pruning by footer statistics before DuckDB (item 39 (c)). */
@Tag("unit")
class FileStatsPrunerTest {

    /** Files of table s.t: key ranges, a date column, nulls, a negative range. */
    private static final String[] FILES = {
        "SELECT range::BIGINT AS k, (DATE '2020-01-01' + range::INTEGER) AS d, range::INTEGER AS v,"
                + " 'a' || range AS s FROM range(1, 11)",                                     // k 1..10
        "SELECT range::BIGINT AS k, (DATE '2021-01-01' + range::INTEGER) AS d, NULL::INTEGER AS v,"
                + " 'b' || range AS s FROM range(11, 21)",                                    // k 11..20, v null
        "SELECT (-range)::BIGINT AS k, (DATE '2019-01-01' + range::INTEGER) AS d,"
                + " range::INTEGER AS v, 'c' AS s FROM range(11, 21)",                        // k -20..-11
        "SELECT CASE WHEN range = 25 THEN NULL ELSE range END::BIGINT AS k,"
                + " (DATE '2022-01-01' + range::INTEGER) AS d, range::INTEGER AS v, 'd' AS s"
                + " FROM range(21, 31)",                                                      // k 21..30 + null
    };

    /** Queries whose answers must not change, with how many files each may read at most. */
    private static final Object[][] QUERIES = {
        {"SELECT * FROM s.t WHERE k = 5", 1},
        {"SELECT * FROM s.t WHERE k = -15", 1},
        {"SELECT * FROM s.t WHERE k = - 15", 1},
        {"SELECT count(*) FROM s.t WHERE k = 1000", 0},
        {"SELECT count(*), sum(v) FROM s.t WHERE k = 1000", 0},
        {"SELECT * FROM s.t WHERE k BETWEEN 9 AND 12", 2},
        {"SELECT * FROM s.t x WHERE x.k BETWEEN 9 AND 12", 4},  // aliased: not parsed yet
        {"SELECT * FROM s.t WHERE k > 25", 1},
        {"SELECT * FROM s.t WHERE 25 < k", 1},
        {"SELECT * FROM s.t WHERE k <= -11", 1},
        {"SELECT * FROM s.t WHERE k IN (3, 27)", 2},
        {"SELECT * FROM s.t WHERE k = 3 OR k = 15", 2},
        {"SELECT * FROM s.t WHERE k = 3 OR v = 15", 3},
        {"SELECT * FROM s.t WHERE k = 3 AND s LIKE 'a%'", 1},
        {"SELECT * FROM s.t WHERE NOT (k = 3)", 4},
        {"SELECT * FROM s.t WHERE k <> 3", 4},
        {"SELECT * FROM s.t WHERE k IS NULL", 4},
        {"SELECT * FROM s.t WHERE k = 5.0", 1},
        {"SELECT * FROM s.t WHERE k = 5.5", 4},
        {"SELECT * FROM s.t WHERE k = '5'", 4},
        {"SELECT * FROM s.t WHERE k = CAST('5' AS BIGINT)", 4},
        {"SELECT * FROM s.t WHERE CAST(k AS VARCHAR) = '5'", 4},
        {"SELECT * FROM s.t WHERE k = NULL", 4},
        {"SELECT * FROM s.t WHERE v = 15", 2},           // v: file 1 only nulls, never pruned on
        {"SELECT * FROM s.t WHERE v = 1000", 1},
        {"SELECT * FROM s.t WHERE d = DATE '2021-01-15'", 1},
        {"SELECT * FROM s.t WHERE d = '2021-01-15'", 1},
        {"SELECT * FROM s.t WHERE d = CAST('2021-01-15' AS DATE)", 1},
        {"SELECT * FROM s.t WHERE d >= DATE '2022-01-25'", 1},
        {"SELECT * FROM s.t WHERE d < TIMESTAMP '2019-01-01 00:00:00'", 4},
        {"SELECT k, count(*) FROM s.t WHERE k > 0 GROUP BY k ORDER BY k LIMIT 3", 3},
        {"SELECT t FROM s.t WHERE k = 5", 1},
        {"SELECT t.k FROM s.t WHERE k = 5", 1},
        {"SELECT s.t.k FROM s.t WHERE k = 5", 4},          // three-part name: the view stays
        {"SELECT * FROM s.t WHERE k IN (SELECT 5)", 4},
        {"SELECT * FROM s.t a JOIN s.t b ON a.k = b.k WHERE a.k = 5", 4},
        {"WITH w AS (SELECT * FROM s.t) SELECT * FROM w WHERE k = 5", 4},
        {"SELECT * FROM s.t WHERE k = 5 UNION ALL SELECT * FROM s.t WHERE k = 6", 4},
    };

    /**
     * Verifies every query answers the same with and without pruning, and that the queries
     * pruning applies to read only the files their filter can match.
     */
    @Test
    void prunedQueriesAnswerAsTheViewDoes(@TempDir Path dir) throws Exception {
        List<DataFile> files = writeFiles(dir);
        AtomicInteger footerReads = new AtomicInteger();
        ParquetAdapter parquet = adapter(files, footerReads);
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (DuckDbAdapter duck = new DuckDbAdapter(config(), ioPool);
                Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            CatalogViews views = new CatalogViews(parquet, duck, true);
            for (String statement : views.current().dataStatements()) {
                s.execute(statement);
            }
            assertEquals("SELECT * FROM s.t WHERE k = 5", views.prunedFiles("SELECT * FROM s.t WHERE k = 5"),
                    "pruning is off until the setting turns it on");
            views.filePruning(1024);
            for (Object[] q : QUERIES) {
                String sql = (String) q[0];
                String pruned = views.prunedFiles(sql);
                int read = filesRead(pruned, dir);
                assertTrue(read <= (Integer) q[1], sql + " -> " + pruned);
                if ((Integer) q[1] == 4) {
                    assertEquals(sql, pruned, "unchanged: " + sql);
                }
                assertEquals(answer(s, sql), answer(s, pruned), sql + " -> " + pruned);
            }
            assertEquals(4, footerReads.get(), "each footer is read once");
        } finally {
            ioPool.shutdownNow();
        }
    }

    /** Verifies a table with more files than the bound is not pruned. */
    @Test
    void tablesWithTooManyFilesAreNotPruned(@TempDir Path dir) throws Exception {
        List<DataFile> files = writeFiles(dir);
        AtomicInteger footerReads = new AtomicInteger();
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (DuckDbAdapter duck = new DuckDbAdapter(config(), ioPool)) {
            CatalogViews views = new CatalogViews(adapter(files, footerReads), duck, true);
            views.current();
            views.filePruning(3);
            assertEquals("SELECT * FROM s.t WHERE k = 5", views.prunedFiles("SELECT * FROM s.t WHERE k = 5"));
            assertEquals(0, footerReads.get());
        } finally {
            ioPool.shutdownNow();
        }
    }

    /** Verifies a file rewritten in place (new length or time) has its footer read again. */
    @Test
    void aChangedFileIsReadAgain(@TempDir Path dir) throws Exception {
        List<DataFile> files = writeFiles(dir);
        DataStore store = store(new AtomicInteger());
        FileStatsPruner pruner = new FileStatsPruner(16, 16);
        assertEquals(1, pruner.keep(store, "s.t", files, "\"k\" = 5").size());
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT 5::BIGINT AS k, DATE '2020-01-01' AS d, 1 AS v, 'x' AS s)"
                    + " TO '" + dir.resolve("f1.parquet") + "' (FORMAT parquet)");
        }
        List<DataFile> changed = new ArrayList<>(files);
        DataFile old = files.get(1);
        changed.set(1, new DataFile(old.relativePath(), old.location(), old.size() + 1,
                old.modificationTime() + 1, null));
        assertEquals(2, pruner.keep(store, "s.t", changed, "\"k\" = 5").size());
        assertNull(pruner.keep(store, "s.t", files, "\"k\" = 5 OR \"k\" = 15 OR \"k\" = -15 OR \"k\" = 22"));
    }

    /** Verifies files whose columns differ from the first file's are never left out. */
    @Test
    void filesWithOtherColumnsAreKept(@TempDir Path dir) throws Exception {
        List<DataFile> files = writeFiles(dir);
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT 100::BIGINT AS k, 1 AS other) TO '" + dir.resolve("f9.parquet")
                    + "' (FORMAT parquet)");
        }
        Path extra = dir.resolve("f9.parquet");
        List<DataFile> all = new ArrayList<>(files);
        all.add(new DataFile("t/f9.parquet", extra.toString(), Files.size(extra), 9, null));
        List<DataFile> kept = new FileStatsPruner(16, 16).keep(store(new AtomicInteger()), "s.t",
                all, "\"k\" = 5");
        assertEquals(List.of(files.get(0), all.get(4)), kept);
    }

    /** Verifies the literal forms that are used and the ones that are not. */
    @Test
    void literalForms() {
        org.jooq.DSLContext dsl = org.jooq.impl.DSL.using(org.jooq.SQLDialect.DEFAULT);
        assertEquals(new java.math.BigDecimal(-7), FileStatsPruner.literal(
                dsl.parser().parseField("-7"), FileStatsPruner.Kind.INTEGER));
        assertNull(FileStatsPruner.literal(dsl.parser().parseField("1.5"), FileStatsPruner.Kind.INTEGER));
        assertNull(FileStatsPruner.literal(dsl.parser().parseField("'7'"), FileStatsPruner.Kind.INTEGER));
        assertNull(FileStatsPruner.literal(dsl.parser().parseField("1e3"), FileStatsPruner.Kind.INTEGER));
        assertEquals(java.math.BigDecimal.valueOf(java.time.LocalDate.parse("1995-03-15").toEpochDay()),
                FileStatsPruner.literal(dsl.parser().parseField("DATE '1995-03-15'"), FileStatsPruner.Kind.DATE));
        assertNull(FileStatsPruner.literal(dsl.parser().parseField("'1995-3-15'"), FileStatsPruner.Kind.DATE));
        assertNull(FileStatsPruner.literal(dsl.parser().parseField("7"), FileStatsPruner.Kind.DATE));
        assertFalse(CatalogViews.threePartName("SELECT * FROM s.t", new TableReferences.Reference(
                14, 17, "s", "t", false, false)));
        assertTrue(CatalogViews.threePartName("SELECT \"S\".t.k FROM s.t", new TableReferences.Reference(
                22, 25, "s", "t", false, false)));
    }

    private static List<DataFile> writeFiles(Path dir) throws Exception {
        List<DataFile> files = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            for (int i = 0; i < FILES.length; i++) {
                Path file = dir.resolve("f" + i + ".parquet");
                s.execute("COPY (" + FILES[i] + ") TO '" + file + "' (FORMAT parquet)");
                files.add(new DataFile("t/f" + i + ".parquet", file.toString(), Files.size(file), i, null));
            }
        }
        return files;
    }

    private static DataStore store(AtomicInteger footerReads) {
        DataStore store = mock(DataStore.class);
        when(store.duckDbUri(any(DataFile.class))).thenAnswer(inv -> ((DataFile) inv.getArgument(0)).location());
        when(store.inputFile(any(DataFile.class))).thenAnswer(inv -> {
            footerReads.incrementAndGet();
            return new LocalInputFile(Path.of(((DataFile) inv.getArgument(0)).location()));
        });
        return store;
    }

    private static ParquetAdapter adapter(List<DataFile> files, AtomicInteger footerReads) throws Exception {
        DataStore store = store(footerReads);
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        when(parquet.dataStore()).thenReturn(store);
        when(parquet.getSchemas(null)).thenReturn(Map.of("s", "/s"));
        when(parquet.getTables(eq("s"), any())).thenReturn(Map.of("t", "/s/t"));
        when(parquet.tableFiles(any(), any())).thenReturn(files);
        return parquet;
    }

    /** Counts the data files a SQL text names (all four when it reads the view). */
    private static int filesRead(String sql, Path dir) {
        if (!sql.contains("read_parquet")) {
            return sql.contains("WHERE false") ? 0 : 4;
        }
        int n = 0;
        for (int i = 0; i < FILES.length; i++) {
            if (sql.contains(dir.resolve("f" + i + ".parquet").toString())) {
                n++;
            }
        }
        return n;
    }

    /** Rows of an answer as sorted text (a multiset). */
    private static List<String> answer(Statement s, String sql) throws Exception {
        List<String> rows = new ArrayList<>();
        try (ResultSet rs = s.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    row.append(rs.getString(i)).append('|');
                }
                rows.add(row.toString());
            }
        }
        rows.sort(null);
        return rows;
    }

    private static AppConfig config() {
        return new AppConfig(
                3, 100, 1, 131072, 0, 1, 1,
                null, false, null, null,
                null, null, null, false, 1048576, 67108864, 60000L, null, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 64, null, null, "complete", true, true);
    }
}
