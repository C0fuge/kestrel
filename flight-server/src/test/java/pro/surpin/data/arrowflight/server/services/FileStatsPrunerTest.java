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
        {"SELECT * FROM s.t x WHERE x.k BETWEEN 9 AND 12", 2},
        {"SELECT * FROM s.t AS x WHERE k = 5", 1},
        {"SELECT x.k, count(*) FROM s.t x WHERE x.k > 25 GROUP BY x.k ORDER BY 1", 1},
        {"SELECT x FROM s.t x WHERE x.k = 5", 1},
        {"SELECT * FROM s.t \"X y\" WHERE \"X y\".k IN (3, 27)", 2},
        {"SELECT * FROM s.t x WHERE x.k = 5 ORDER BY x.k LIMIT 1", 1},
        {"SELECT * FROM s.t a JOIN s.t b ON a.k = b.k WHERE b.k = 5", 4},
        {"SELECT * FROM s.t x WHERE x.k IN (SELECT 5)", 4},
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
                if (!pruned.equals(sql)) {
                    rewriteIsAcceptedWithoutSecondParse(duck, views, s, sql, pruned);
                }
            }
            assertEquals(4, footerReads.get(), "each footer is read once");
        } finally {
            ioPool.shutdownNow();
        }
    }

    /**
     * Checks that DoGet's pruned text of a query whose unpruned text passed the parse check is
     * accepted without a second parse (item 40 (b)), and that DuckDB's own parse of it is one
     * statement using no name beyond the original's, {@code read_parquet} and {@code list_value}.
     */
    private static void rewriteIsAcceptedWithoutSecondParse(DuckDbAdapter duck, CatalogViews views,
            Statement s, String sql, String pruned) throws Exception {
        CatalogViews.Pruning pruning = views.pruning(sql);
        String replacement = views.prunedReplacement(pruning);
        TableReferences.Reference reference = pruning.reference();
        assertEquals(pruned, TableReferences.rewrite(sql, List.of(reference), List.of(replacement)));
        try (org.apache.arrow.memory.RootAllocator allocator = new org.apache.arrow.memory.RootAllocator()) {
            duck.describeComplete(allocator, sql, views.current());
        }
        assertTrue(duck.acceptRewrite(sql, reference.start(), reference.end(), pruned, replacement),
                sql);
        java.util.Set<String> original = serializedNames(s, sql);
        original.add("read_parquet");
        original.add("list_value");
        java.util.Set<String> rewritten = serializedNames(s, pruned);
        assertTrue(original.containsAll(rewritten), sql + ": " + rewritten + " vs " + original);
    }

    /**
     * Reads the function and table names of DuckDB's parse of one statement.
     *
     * @return lower-case names
     */
    private static java.util.Set<String> serializedNames(Statement s, String sql) throws Exception {
        String json;
        try (java.sql.PreparedStatement ps = s.getConnection().prepareStatement(
                "SELECT (j->>'error')::BOOLEAN, json_array_length(j->'statements'), j::VARCHAR"
                        + " FROM (SELECT json_serialize_sql(?::VARCHAR) AS j)")) {
            ps.setString(1, sql);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertFalse(rs.getBoolean(1), sql);
                assertEquals(1, rs.getInt(2), sql);
                json = rs.getString(3);
            }
        }
        java.util.Set<String> names = new java.util.HashSet<>();
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(
                "\"(?:function_name|table_name)\":\"((?:[^\"\\\\]|\\\\.)*)\"").matcher(json);
        while (m.find()) {
            names.add(m.group(1).toLowerCase(java.util.Locale.ROOT));
        }
        return names;
    }

    /**
     * Verifies that DoGet's pruning on a node without local copies is decided once (item 40 (b),
     * the speed c1 regression of item 39 (c)): the pruned text of a described point query is
     * accepted without a second DuckDB parse check, a text the pruning declines is marked as
     * decided too, and DoGet streams the decided text without pruning it again.
     */
    @Test
    void doGetUsesThePruningDecidedBeforeIt(@TempDir Path dir) throws Exception {
        List<DataFile> files = writeFiles(dir);
        ParquetAdapter parquet = adapter(files, new AtomicInteger());
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (DuckDbAdapter duck = new DuckDbAdapter(config(), ioPool);
                Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement();
                org.apache.arrow.memory.RootAllocator allocator = new org.apache.arrow.memory.RootAllocator()) {
            ExecutionService service = new ExecutionService(parquet, duck, mock(MetadataService.class), null,
                    null);
            // The service prunes with the default setting (fileStatsPruningMaxFiles=1024).
            for (String statement : new CatalogViews(parquet, duck, true).current().dataStatements()) {
                s.execute(statement);
            }
            String point = "SELECT * FROM s.t WHERE k = 5";
            service.completeSchema(allocator, point);
            try (ExecutionService.CopiedQuery pruned = service.completeFromCopies(point, null)) {
                assertTrue(pruned.pruned(), pruned.sql());
                assertTrue(pruned.pruningDone(), pruned.sql());
                assertFalse(pruned.sql().equals(point));
                assertTrue(duck.parseCheckRemembered(pruned.sql()), "accepted without a second parse check");
                assertEquals(1, filesRead(pruned.sql(), dir));
                assertEquals(answer(s, point), answer(s, pruned.sql()));
                org.apache.arrow.flight.FlightProducer.ServerStreamListener listener =
                        mock(org.apache.arrow.flight.FlightProducer.ServerStreamListener.class);
                when(listener.isReady()).thenReturn(true);
                service.readComplete(allocator, pruned.sql(), null, listener, true, pruned.pruningDone());
                org.mockito.Mockito.verify(listener).putNext();   // the one batch of the kept file
            }
            for (String declined : List.of("SELECT * FROM s.t WHERE k IS NULL",
                    "SELECT * FROM s.t a JOIN s.t b ON a.k = b.k WHERE a.k = 5")) {
                try (ExecutionService.CopiedQuery whole = service.completeFromCopies(declined, null)) {
                    assertFalse(whole.pruned(), declined);
                    assertTrue(whole.pruningDone(), declined);
                    assertEquals(declined, whole.sql());
                }
            }
            try (ExecutionService.CopiedQuery undecided = new ExecutionService.CopiedQuery(point, null)) {
                assertFalse(undecided.pruningDone(), "other paths leave the pruning to DoGet");
            }
        } finally {
            ioPool.shutdownNow();
        }
    }

    /**
     * Q-40b-1: with the setting unset, a local, unshared data directory is not pruned (its file
     * opens are page-cache reads) while HDFS, S3, shared mounts and plugin stores are; a given
     * setting prunes every store.
     */
    @Test
    void unsetPruningSkipsOnlyLocalUnsharedDirectories(@TempDir Path dir) throws Exception {
        List<DataFile> files = writeFiles(dir);
        String point = "SELECT * FROM s.t WHERE k = 5";
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (DuckDbAdapter duck = new DuckDbAdapter(config(), ioPool)) {
            for (String[] store : new String[][] {
                {"file", "false", "view"}, {"file", "true", "pruned"}, {"hdfs", "false", "pruned"},
                {"s3", "true", "pruned"}, {"fixture", "false", "pruned"}}) {
                ParquetAdapter parquet = adapter(files, new AtomicInteger());
                DataStore data = parquet.dataStore();
                when(data.kind()).thenReturn(store[0]);
                when(data.shared()).thenReturn(Boolean.parseBoolean(store[1]));
                String what = store[0] + " shared=" + store[1];
                CatalogViews unset = new CatalogViews(parquet, duck, true);
                unset.filePruning(1024, false);
                assertEquals("view".equals(store[2]) ? 4 : 1, filesRead(unset.prunedFiles(point), dir), what);
                CatalogViews given = new CatalogViews(parquet, duck, true);
                given.filePruning(1024, true);
                assertEquals(1, filesRead(given.prunedFiles(point), dir), "given: " + what);
            }
        } finally {
            ioPool.shutdownNow();
        }
        org.apache.hadoop.fs.FileSystem local = org.apache.hadoop.fs.FileSystem.getLocal(
                new org.apache.hadoop.conf.Configuration());
        assertTrue(CatalogViews.localStore(new pro.surpin.data.arrowflight.server.storage.HadoopDataStore(
                local, dir.toString(), null)), "a local dataDir");
        assertFalse(CatalogViews.localStore(new pro.surpin.data.arrowflight.server.storage.HadoopDataStore(
                local, dir.toString(), null, true)), "a mount every node reads (dataDirShared)");
    }

    /**
     * Verifies that point queries differing only in their key are described once (item 40 (b),
     * {@link DescribeKey}), and that a text whose literal is not left out is described itself.
     */
    @Test
    void pointQueriesShareOneDescription(@TempDir Path dir) throws Exception {
        List<DataFile> files = writeFiles(dir);
        ParquetAdapter parquet = adapter(files, new AtomicInteger());
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (DuckDbAdapter duck = new DuckDbAdapter(config(), ioPool);
                org.apache.arrow.memory.RootAllocator allocator = new org.apache.arrow.memory.RootAllocator()) {
            CatalogViews views = new CatalogViews(parquet, duck, true);
            long before = duck.describeRuns();
            org.apache.arrow.vector.types.pojo.Schema first =
                    duck.describeComplete(allocator, "SELECT k, s FROM s.t WHERE k = 5", views.current());
            for (int k = 6; k < 20; k++) {
                assertEquals(first, duck.describeComplete(allocator,
                        "SELECT k, s FROM s.t WHERE k = " + k, views.current()));
            }
            assertEquals(before + 1, duck.describeRuns(), "one describe for every key");
            org.apache.arrow.vector.types.pojo.Schema other =
                    duck.describeComplete(allocator, "SELECT k, s FROM s.t WHERE k = 5.5", views.current());
            assertEquals(before + 2, duck.describeRuns());
            assertEquals(first, other);
            assertEquals(2, duck.describeComplete(allocator, "SELECT k + 1, d FROM s.t WHERE k = 7",
                    views.current()).getFields().size());
            assertEquals(before + 3, duck.describeRuns(), "another select list is described");
        } finally {
            ioPool.shutdownNow();
        }
    }

    /** Point query shapes whose integer literals {@link DescribeKey} leaves out; %d is a value. */
    private static final String[] SHAPES = {
        "SELECT * FROM s.t WHERE k = %d",
        "SELECT * FROM s.t WHERE k = -%d",
        "SELECT * FROM s.t WHERE k = - %d",
        "SELECT * FROM s.t WHERE k > %d",
        "SELECT * FROM s.t WHERE k <= -%d",
        "SELECT * FROM s.t WHERE k >= %d AND k < %d",
        "SELECT * FROM s.t WHERE k = %d OR k = %d",
        "SELECT * FROM s.t WHERE k = %d OR v = %d",
        "SELECT * FROM s.t WHERE (k = %d) AND s LIKE 'a%%'",
        "SELECT * FROM s.t WHERE k <> %d",
        "SELECT * FROM s.t WHERE v = %d",
        "SELECT * FROM s.t WHERE k != %d AND k == %d",
        "SELECT count(*), sum(v) FROM s.t WHERE k = %d",
        "SELECT * FROM s.t AS x WHERE x.k = %d ORDER BY x.k LIMIT 1",
        "SELECT x.k, count(*) FROM s.t x WHERE x.k > %d GROUP BY x.k ORDER BY 1",
        "SELECT * FROM s.t \"X y\" WHERE \"X y\".k = %d",
        "SELECT * FROM s.t WHERE k = %d AND k IN (3, 27)",
        "SELECT * FROM s.t WHERE k BETWEEN 9 AND 12 AND k = %d",
        "SELECT * FROM s.t a JOIN s.t b ON a.k = b.k WHERE b.k = %d",
        "SELECT s.t.k FROM s.t WHERE k = %d",
        "SELECT * FROM s.t WHERE k = 3000000001 OR k = %d",
        "SELECT * FROM s.t WHERE k = %d OR k = 30000000010",
        "SELECT * FROM s.t WHERE k = %d OR s = '3000000001'",
        "SELECT * FROM s.t WHERE k = %d OR k = %d OR k = %d",
        "SELECT * FROM s.t WHERE k = 0%d",
        // Pruning shapes only (item 40 (b)): decimal, long integer and date literals.
        "SELECT * FROM s.t WHERE k = %d.0",
        "SELECT * FROM s.t WHERE k > %d.5",
        "SELECT * FROM s.t WHERE k <= -%d.5 OR k = %d.00",
        "SELECT * FROM s.t WHERE v = -%d.0",
        "SELECT count(*) FROM s.t WHERE k = %d AND v > %d.5",
        "SELECT * FROM s.t WHERE k < 1000000000%d",
        "SELECT * FROM s.t WHERE k = %d OR k > 10000000000%d",
        "SELECT * FROM s.t WHERE d = DATE '2021-01-%02d'",
        "SELECT * FROM s.t WHERE d >= date '2022-01-%02d' AND k > %d.5",
        "SELECT * FROM s.t WHERE d < DATE '2019-01-%02d' OR d = DATE '2020-01-%02d'",
        "SELECT * FROM s.t WHERE k = %d.0 OR s = '3000000001.25'",
        "SELECT * FROM s.t WHERE d = DATE '2021-01-%02d' OR s = '2999-01-01'",
        "SELECT * FROM s.t WHERE k = %d.25 OR k = 3000000002.25",
    };

    /**
     * Verifies that point queries of one shape, parsed once with stand-in literals (item 40 (b)),
     * keep exactly the files each text keeps when it is parsed itself, and answer as the view does.
     */
    @Test
    void shapeParsesKeepWhatEachTextKeeps(@TempDir Path dir) throws Exception {
        List<DataFile> files = writeFiles(dir);
        ParquetAdapter parquet = adapter(files, new AtomicInteger());
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (DuckDbAdapter duck = new DuckDbAdapter(config(), ioPool);
                Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            CatalogViews shaped = new CatalogViews(parquet, duck, true);
            CatalogViews each = new CatalogViews(parquet, duck, true);
            for (String statement : shaped.current().dataStatements()) {
                s.execute(statement);
            }
            shaped.filePruning(1024);
            each.filePruning(1024);
            each.shapeParses(false);
            int pruned = 0;
            int checked = 0;
            for (String shape : SHAPES) {
                int slots = shape.split("%(02)?d", -1).length - 1;
                for (int v = 0; v <= 32; v += 1 + checked % 3) {
                    Object[] values = new Object[slots];
                    for (int i = 0; i < slots; i++) {
                        values[i] = (v + 7 * i) % 33;
                    }
                    String sql = String.format(java.util.Locale.ROOT, shape, values);
                    List<String> kept = kept(shaped.pruning(sql));
                    assertEquals(kept(each.pruning(sql)), kept, sql);
                    checked++;
                    if (kept != null) {
                        pruned++;
                        String text = shaped.prunedFiles(sql);
                        assertEquals(answer(s, sql), answer(s, text), sql + " -> " + text);
                    }
                }
            }
            assertTrue(pruned > checked / 3, "pruned " + pruned + " of " + checked);
            assertTrue(shaped.pruningShapes() <= SHAPES.length, "one parse per shape");
            assertEquals(0, each.pruningShapes());
        } finally {
            ioPool.shutdownNow();
        }
    }

    private static List<String> kept(CatalogViews.Pruning pruning) {
        if (pruning == null) {
            return null;
        }
        List<String> paths = new ArrayList<>();
        for (DataFile file : pruning.kept()) {
            paths.add(file.relativePath());
        }
        return paths;
    }

    /**
     * Verifies that pruning with the query parser's own WHERE condition keeps the same files as
     * parsing its rendered filter again (item 40 (b): one jOOQ parse per query text).
     */
    @Test
    void parsedConditionKeepsWhatTheRenderedFilterKeeps(@TempDir Path dir) throws Exception {
        List<DataFile> files = writeFiles(dir);
        DataStore store = store(new AtomicInteger());
        FileStatsPruner pruner = new FileStatsPruner(16, 16);
        int compared = 0;
        for (Object[] q : QUERIES) {
            String sql = (String) q[0];
            ParquetQueryParser pq;
            try {
                pq = ParquetQueryParser.parse(sql);
            } catch (RuntimeException e) {
                continue;   // not a query the parser reads: never pruned
            }
            if (pq.isJoin || pq.filter == null) {
                continue;
            }
            assertTrue(pq.where != null, sql);
            assertEquals(pruner.keep(store, "s.t", files, pq.filter, null),
                    pruner.keep(store, "s.t", files, pq.filter, pq.where), sql);
            compared++;
        }
        assertTrue(compared >= 30, "compared " + compared);
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
        when(store.readableHere(any(DataFile.class))).thenReturn(true);   // a local store
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
