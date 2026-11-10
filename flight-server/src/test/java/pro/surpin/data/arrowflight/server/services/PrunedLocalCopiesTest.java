package pro.surpin.data.arrowflight.server.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.storage.DataStore;
import pro.surpin.data.arrowflight.spi.storage.DataFile;

/**
 * Tests file pruning by footer statistics on the local-copy path of SASL DataNodes (item 42 (2)):
 * a point query copies and reads only the files its filter can match.
 */
@Tag("unit")
class PrunedLocalCopiesTest {

    /** Key ranges of the files of s.t: 1..10, 11..20, 21..30, 31..40. */
    private static final int FILES = 4;

    /**
     * Verifies that a complete answer on a node lacking blocks of a table is pruned before the
     * copies are made: the kept file not held here is the only one copied, a kept file held here
     * is read in place without the node-wide readability refusal, and a filter no file can
     * match reads nothing; every answer equals the one over all files.
     */
    @Test
    void pointQueriesCopyAndReadOnlyTheFilesTheirFilterCanMatch(@TempDir Path dir) throws Exception {
        List<DataFile> files = new ArrayList<>();
        Map<String, String> uris = new HashMap<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            for (int i = 0; i < FILES; i++) {
                Path file = dir.resolve("f" + i + ".parquet");
                s.execute("COPY (SELECT range::BIGINT AS k, 'v' || range AS v FROM range(" + (i * 10 + 1)
                        + ", " + (i * 10 + 11) + ")) TO '" + file + "' (FORMAT parquet)");
                DataFile data = new DataFile("s/t/f" + i + ".parquet", file.toString(), Files.size(file), i,
                        null);
                files.add(data);
                uris.put(data.relativePath(), file.toString());
            }
        }
        DataFile notHere = files.get(0);
        DataStore store = mock(DataStore.class);
        when(store.wholeFileReadsOnly()).thenReturn(true);
        when(store.readableHere(any(DataFile.class))).thenAnswer(inv -> !notHere.equals(inv.getArgument(0)));
        when(store.duckDbUri(any(DataFile.class))).thenAnswer(inv -> ((DataFile) inv.getArgument(0)).location());
        when(store.duckDbUri(anyString())).thenAnswer(inv -> uris.get((String) inv.getArgument(0)));
        when(store.inputFile(any(DataFile.class))).thenAnswer(
                inv -> new LocalInputFile(Path.of(((DataFile) inv.getArgument(0)).location())));
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        when(parquet.dataStore()).thenReturn(store);
        when(parquet.localCopies()).thenReturn(true);
        when(parquet.getSchemas(null)).thenReturn(Map.of("s", "/s"));
        when(parquet.getTables(eq("s"), any())).thenReturn(Map.of("t", "/s/t"));
        when(parquet.tableFiles(any(), any())).thenReturn(files);
        when(parquet.getTableSchema("s", "t")).thenReturn(new Schema(List.of()));
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (DuckDbAdapter duck = new DuckDbAdapter(config(), ioPool);
                Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            List<String> all = new ArrayList<>(uris.values());
            s.execute("CREATE SCHEMA s");
            s.execute("CREATE VIEW s.t AS SELECT * FROM " + DuckDbAdapter.readParquetFromClause(all));
            ExecutionService service = new ExecutionService(parquet, duck, mock(MetadataService.class), null,
                    null);

            // Kept file not held here: only it is copied, the other three are not read.
            String inCopy = "SELECT * FROM s.t WHERE k = 5";
            try (ExecutionService.CopiedQuery copied = service.completeFromCopies(inCopy)) {
                assertTrue(copied.copied(), copied.sql());
                assertTrue(copied.sql().contains(duck.localCopyRoot().toString()), copied.sql());
                for (int i = 1; i < FILES; i++) {
                    assertFalse(copied.sql().contains(uris.get(files.get(i).relativePath())), copied.sql());
                }
                assertEquals(answer(s, inCopy), answer(s, copied.sql()));
            }

            // Kept file held here: read in place, marked pruned so no file of the table is refused.
            String held = "SELECT k, v FROM s.t WHERE k = 15";
            try (ExecutionService.CopiedQuery pruned = service.completeFromCopies(held)) {
                assertFalse(pruned.copied(), pruned.sql());
                assertTrue(pruned.pruned(), pruned.sql());
                assertTrue(pruned.sql().contains(uris.get(files.get(1).relativePath())), pruned.sql());
                assertFalse(pruned.sql().contains(notHere.location()), pruned.sql());
                assertEquals(answer(s, held), answer(s, pruned.sql()));
            }

            // No file can match: nothing is read or copied, and the answer is still a count of 0.
            String none = "SELECT count(*) FROM s.t WHERE k = 1000";
            try (ExecutionService.CopiedQuery empty = service.completeFromCopies(none)) {
                assertFalse(empty.copied(), empty.sql());
                assertTrue(empty.pruned(), empty.sql());
                assertFalse(empty.sql().contains("read_parquet"), empty.sql());
                assertEquals(answer(s, none), answer(s, empty.sql()));
            }

            // A query pruning does not apply to copies the table as before.
            String join = "SELECT count(*) FROM s.t a JOIN s.t b ON a.k = b.k WHERE a.k = 5";
            try (ExecutionService.CopiedQuery whole = service.completeFromCopies(join)) {
                assertTrue(whole.copied(), whole.sql());
                assertFalse(whole.pruned(), whole.sql());
                assertTrue(whole.sql().contains(uris.get(files.get(3).relativePath())), whole.sql());
                assertEquals(answer(s, join), answer(s, whole.sql()));
            }
        } finally {
            ioPool.shutdownNow();
        }
    }

    /**
     * Verifies that with local copies off a point query whose kept file is held here is answered
     * from that file (marked pruned, so the node-wide readability refusal does not apply), while
     * one whose kept file is not held here stays unpruned and is refused as before (Q-42 (b)).
     */
    @Test
    void withoutCopiesAPointQueryOnHeldFilesIsPrunedNotRefused(@TempDir Path dir) throws Exception {
        List<DataFile> files = new ArrayList<>();
        Map<String, String> uris = new HashMap<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            for (int i = 0; i < FILES; i++) {
                Path file = dir.resolve("f" + i + ".parquet");
                s.execute("COPY (SELECT range::BIGINT AS k, 'v' || range AS v FROM range(" + (i * 10 + 1)
                        + ", " + (i * 10 + 11) + ")) TO '" + file + "' (FORMAT parquet)");
                DataFile data = new DataFile("s/t/f" + i + ".parquet", file.toString(), Files.size(file), i,
                        null);
                files.add(data);
                uris.put(data.relativePath(), file.toString());
            }
        }
        DataFile notHere = files.get(0);
        DataStore store = mock(DataStore.class);
        when(store.wholeFileReadsOnly()).thenReturn(true);
        when(store.readableHere(any(DataFile.class))).thenAnswer(inv -> !notHere.equals(inv.getArgument(0)));
        when(store.duckDbUri(any(DataFile.class))).thenAnswer(inv -> ((DataFile) inv.getArgument(0)).location());
        when(store.duckDbUri(anyString())).thenAnswer(inv -> uris.get((String) inv.getArgument(0)));
        when(store.inputFile(any(DataFile.class))).thenAnswer(
                inv -> new LocalInputFile(Path.of(((DataFile) inv.getArgument(0)).location())));
        ParquetAdapter parquet = mock(ParquetAdapter.class);
        when(parquet.dataStore()).thenReturn(store);
        when(parquet.localCopies()).thenReturn(false);
        when(parquet.getSchemas(null)).thenReturn(Map.of("s", "/s"));
        when(parquet.getTables(eq("s"), any())).thenReturn(Map.of("t", "/s/t"));
        when(parquet.tableFiles(any(), any())).thenReturn(files);
        when(parquet.getTableSchema("s", "t")).thenReturn(new Schema(List.of()));
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (DuckDbAdapter duck = new DuckDbAdapter(config(), ioPool);
                Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA s");
            s.execute("CREATE VIEW s.t AS SELECT * FROM "
                    + DuckDbAdapter.readParquetFromClause(new ArrayList<>(uris.values())));
            ExecutionService service = new ExecutionService(parquet, duck, mock(MetadataService.class), null,
                    null);

            String held = "SELECT k, v FROM s.t WHERE k = 25";
            try (ExecutionService.CopiedQuery pruned = service.completeFromCopies(held)) {
                assertFalse(pruned.copied(), pruned.sql());
                assertTrue(pruned.pruned(), pruned.sql());
                assertTrue(pruned.sql().contains(uris.get(files.get(2).relativePath())), pruned.sql());
                assertFalse(pruned.sql().contains(notHere.location()), pruned.sql());
                assertEquals(answer(s, held), answer(s, pruned.sql()));
            }

            String none = "SELECT count(*) FROM s.t WHERE k = 1000";
            try (ExecutionService.CopiedQuery empty = service.completeFromCopies(none)) {
                assertTrue(empty.pruned(), empty.sql());
                assertEquals(answer(s, none), answer(s, empty.sql()));
            }

            // The kept file is not held here: unchanged and unpruned, so the refusal still applies.
            String missing = "SELECT * FROM s.t WHERE k = 5";
            try (ExecutionService.CopiedQuery refused = service.completeFromCopies(missing)) {
                assertFalse(refused.copied(), refused.sql());
                assertFalse(refused.pruned(), refused.sql());
                assertEquals(missing, refused.sql());
            }
        } finally {
            ioPool.shutdownNow();
        }
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
