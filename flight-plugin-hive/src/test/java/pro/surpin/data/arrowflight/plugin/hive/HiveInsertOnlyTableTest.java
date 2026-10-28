package pro.surpin.data.arrowflight.plugin.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;

/**
 * Insert-only (MM) transactional tables with hive.insertOnlyTables: write ids opened, committed and
 * aborted through the in-process Hive 3.1.3 metastore, directories written as Hive writes them;
 * the table's files give the committed rows only, at each new snapshot.
 */
class HiveInsertOnlyTableTest {

    private static final Map<String, String> MM = Map.of("transactional", "true",
            "transactional_properties", "insert_only");

    @TempDir
    static Path dir;
    static IsolatedHms hms;
    static String root;

    @BeforeAll
    static void start() throws Exception {
        hms = IsolatedHms.shared();
        root = dir.resolve("lake").toString();
        Files.createDirectories(dir.resolve("lake"));
        hms.createDatabase("mmdb");
    }

    @Test
    void committedWritesOnlyAtEachSnapshot() throws Exception {
        Path table = Files.createDirectories(dir.resolve("lake/mm"));
        hms.createTable("mmdb", "mm", "file:" + table, "id int", "", "parquet", MM);
        parquet(table.resolve("000000_0"), 0);   // original file, from before the table was transactional
        long committed = write(table, "mm", 1, true);
        write(table, "mm", 99, false);           // aborted
        long open = hms.openTxn();
        long openWrite = hms.allocateWriteId(open, "mmdb", "mm");
        parquet(table.resolve(delta(openWrite)).resolve("000000_0"), 98);
        write(table, "mm", 3, true);
        parquet(table.resolve("_tmp." + delta(openWrite + 5)).resolve("000000_0"), 97);
        assertTrue(committed > 0);
        try (Catalog catalog = open(Map.of())) {
            assertTrue(catalog.tables("mmdb").contains("mm"), catalog.tables("mmdb").toString());
            assertEquals(List.of(0, 1, 3), rows(catalog, "mm"));
            hms.commitTxn(open);
            assertEquals(List.of(0, 1, 3, 98), rows(catalog, "mm"));
            // INSERT OVERWRITE: a base replaces the originals and every older delta.
            long txn = hms.openTxn();
            long writeId = hms.allocateWriteId(txn, "mmdb", "mm");
            parquet(table.resolve(String.format("base_%07d", writeId)).resolve("000000_0"), 50);
            assertEquals(List.of(0, 1, 3, 98), rows(catalog, "mm"), "base of an open write");
            hms.commitTxn(txn);
            assertEquals(List.of(50), rows(catalog, "mm"));
            write(table, "mm", 51, true);
            assertEquals(List.of(50, 51), rows(catalog, "mm"));
        }
    }

    @Test
    void partitionsAreReadAtOneSnapshot() throws Exception {
        Path table = Files.createDirectories(dir.resolve("lake/mmp"));
        hms.createTable("mmdb", "mmp", "file:" + table, "id int", "k string", "parquet", MM);
        String a = hms.addPartition("mmdb", "mmp", List.of("a"), null);
        String b = hms.addPartition("mmdb", "mmp", List.of("b"), null);
        Path pa = Path.of(a.substring("file:".length()));
        Path pb = Path.of(b.substring("file:".length()));
        write(pa, "mmp", 1, true);
        write(pb, "mmp", 2, false);
        write(pb, "mmp", 3, true);
        try (Catalog catalog = open(Map.of())) {
            assertEquals(List.of(1, 3), rows(catalog, "mmp"));
        }
    }

    @Test
    void fullAcidLayoutIsRefusedNotReadInPart() throws Exception {
        Path table = Files.createDirectories(dir.resolve("lake/mmd"));
        hms.createTable("mmdb", "mmd", "file:" + table, "id int", "", "parquet", MM);
        write(table, "mmd", 1, true);
        Files.createDirectories(table.resolve("delete_delta_0000001_0000001_0000"));
        try (Catalog catalog = open(Map.of())) {
            IOException e = assertThrows(IOException.class,
                    () -> catalog.current(new TableName("mmdb", "mmd")));
            assertTrue(e.getMessage().contains("delete delta"), e.getMessage());
        }
    }

    @Test
    void insertOnlyTablesStaySkippedByDefault() throws Exception {
        Path table = Files.createDirectories(dir.resolve("lake/mmoff"));
        hms.createTable("mmdb", "mmoff", "file:" + table, "id int", "", "parquet", MM);
        write(table, "mmoff", 1, true);
        try (Catalog catalog = open(Map.of("insertOnlyTables", "false"))) {
            assertFalse(catalog.tables("mmdb").contains("mmoff"), catalog.tables("mmdb").toString());
        }
    }

    /**
     * Writes one row as a transaction of its own into a new delta directory.
     */
    private static long write(Path location, String table, int id, boolean commit) throws Exception {
        long txn = hms.openTxn();
        long writeId = hms.allocateWriteId(txn, "mmdb", table);
        parquet(location.resolve(delta(writeId)).resolve("000000_0"), id);
        if (commit) {
            hms.commitTxn(txn);
        } else {
            hms.abortTxn(txn);
        }
        return writeId;
    }

    private static String delta(long writeId) {
        return String.format("delta_%07d_%07d_0000", writeId, writeId);
    }

    /**
     * Reads the ids of the table's planned files with DuckDB, sorted.
     */
    private static List<Integer> rows(Catalog catalog, String table) throws Exception {
        TableSnapshot snapshot = catalog.current(new TableName("mmdb", table)).orElseThrow();
        List<String> files = new ArrayList<>();
        for (TableFile file : snapshot.files()) {
            files.add("'" + file.location().replace("'", "''") + "'");
        }
        List<Integer> result = new ArrayList<>();
        if (files.isEmpty()) {
            return result;
        }
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement();
                ResultSet rs = s.executeQuery("SELECT id FROM read_parquet([" + String.join(", ", files)
                        + "]) ORDER BY id")) {
            while (rs.next()) {
                result.add(rs.getInt(1));
            }
        }
        return result;
    }

    private static Catalog open(Map<String, String> overrides) throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("uris", hms.uri());
        values.put("roots", root);
        values.put("namesTtlSeconds", "0");
        values.put("metadataTtlSeconds", "0");
        values.put("insertOnlyTables", "true");
        values.putAll(overrides);
        TestSupport.LocalStores stores = new TestSupport.LocalStores();
        Catalog catalog = new HiveCatalogProvider().open(new TestSupport.Context(
                new TestSupport.MapConfig("hive", values), stores, new SecretRedactor(), null));
        stores.freeze();
        return catalog;
    }

    private static void parquet(Path file, int id) throws Exception {
        Files.createDirectories(file.getParent());
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT " + id + " AS id) TO '" + file.toString().replace("'", "''")
                    + "' (FORMAT parquet)");
        }
    }
}
