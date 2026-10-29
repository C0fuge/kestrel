package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
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
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.server.TestFlightServerHelper;
import pro.surpin.data.arrowflight.server.adapters.ResultModeMiddleware;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;
import pro.surpin.data.arrowflight.server.plugin.PluginSettings;
import pro.surpin.data.arrowflight.server.storage.StoreRegistry;

/**
 * Runs the server with the packaged hive plugin and hive.insertOnlyTables=true against a real
 * metastore with Hive's transaction tables: insert-only (MM) Parquet, ORC and text tables, written as
 * Hive writes them (original files, {@code delta_*} and {@code base_*} directories, write ids
 * opened, committed and aborted through the metastore), answer the committed rows only, in both
 * result modes and with partitions; a later commit becomes visible to the next partial query; a
 * full ACID layout is refused in both modes, never read in part, and its siblings stay readable.
 */
class HiveInsertOnlyIntegrationTest {

    private static final Map<String, String> MM = Map.of("transactional", "true",
            "transactional_properties", "insert_only");
    private static final String DB = "hvmm";

    @TempDir
    static Path dir;
    static HiveTestTables.Hms hms;
    static TestFlightServerHelper helper;
    static Path lake;
    static Path mmc;
    static long mmcOpen;

    @BeforeAll
    static void start() throws Exception {
        Assumptions.assumeTrue(HiveTestTables.available(), "flight-plugin-hive is not built");
        hms = HiveTestTables.hms();
        lake = Files.createDirectories(dir.resolve("lake"));
        hms.call("createDatabase", DB);

        // Parquet: originals 0..99, committed 100..199, aborted 1000..1099, an open write
        // 2000..2099 that stays open, a write in progress in a _tmp directory.
        Path mm = table("mm", "", "parquet");
        parquet(mm.resolve("000000_0"), 0, 100);
        write(mm, "mm", 100, 200, true, "parquet");
        write(mm, "mm", 1000, 1100, false, "parquet");
        long open = (Long) hms.call("openTxn");
        long openWrite = (Long) hms.call("allocateWriteId", open, DB, "mm");
        parquet(mm.resolve(delta(openWrite)).resolve("000000_0"), 2000, 2100);
        parquet(mm.resolve("_tmp." + delta(openWrite + 5)).resolve("000000_0"), 3000, 3100);

        // Partitioned: k=a committed 0..50; k=b aborted 50..100, committed 100..150.
        Path mmp = table("mmp", "k string", "parquet");
        Path pa = partition("mmp", "a");
        Path pb = partition("mmp", "b");
        write(pa, "mmp", 0, 50, true, "parquet");
        write(pb, "mmp", 50, 100, false, "parquet");
        write(pb, "mmp", 100, 150, true, "parquet");

        // ORC: committed 0..500 and 700..1000, aborted 500..700; the reference is a plain ORC
        // table with exactly the committed rows.
        String columns = HiveTestTables.orcConstant("COLUMNS");
        Path mmo = Files.createDirectories(lake.resolve("mmo"));
        hms.call("createTable", DB, "mmo", "file:" + mmo, columns, "", "orc", MM);
        write(mmo, "mmo", 0, 500, true, "orc");
        write(mmo, "mmo", 500, 700, false, "orc");
        write(mmo, "mmo", 700, 1000, true, "orc");
        Path ref = Files.createDirectories(lake.resolve("mmo_ref"));
        HiveTestTables.writeOrc(ref.resolve("000000_0"), 0, 500, "ZLIB");
        HiveTestTables.writeOrc(ref.resolve("000001_0"), 700, 1000, "ZLIB");
        hms.call("createTable", DB, "mmo_ref", "file:" + ref, columns, "", "orc", null);

        // Text (Hive's defaults, \001 and \N, a gzip file in one delta): originals 0..49,
        // committed 50..99 (gzip) and 200..249, aborted 100..199, an open write 300..349.
        Path mmt = table("mmt", "", "text");
        text(mmt.resolve("000000_0"), 0, 50, false);
        write(mmt, "mmt", 50, 100, true, "text.gz");
        write(mmt, "mmt", 100, 200, false, "text");
        write(mmt, "mmt", 200, 250, true, "text");
        long textOpen = (Long) hms.call("openTxn");
        long textWrite = (Long) hms.call("allocateWriteId", textOpen, DB, "mmt");
        text(mmt.resolve(delta(textWrite)).resolve("000000_0"), 300, 350, false);
        // A partitioned text table: k=a committed 0..20; k=b aborted 20..40, committed 40..60.
        table("mmtp", "k string", "text");
        Path ta = partition("mmtp", "a");
        Path tb = partition("mmtp", "b");
        write(ta, "mmtp", 0, 20, true, "text");
        write(tb, "mmtp", 20, 40, false, "text");
        write(tb, "mmtp", 40, 60, true, "text");

        // Committed 0..9 and an open write 10..29 that a test commits.
        mmc = table("mmc", "", "parquet");
        write(mmc, "mmc", 0, 10, true, "parquet");
        mmcOpen = (Long) hms.call("openTxn");
        long mmcWrite = (Long) hms.call("allocateWriteId", mmcOpen, DB, "mmc");
        parquet(mmc.resolve(delta(mmcWrite)).resolve("000000_0"), 10, 30);

        // A delete delta (full ACID) next to a committed write.
        Path mmd = table("mmd", "", "parquet");
        write(mmd, "mmd", 0, 10, true, "parquet");
        Files.createDirectories(mmd.resolve("delete_delta_0000001_0000001_0000"));
        // The same in one partition of a partitioned table (partitions are listed in parallel).
        Path mmdp = table("mmdp", "k string", "parquet");
        write(partition("mmdp", "a"), "mmdp", 0, 10, true, "parquet");
        Path broken = partition("mmdp", "b");
        write(broken, "mmdp", 10, 20, true, "parquet");
        Files.createDirectories(broken.resolve("delete_delta_0000002_0000002_0000"));

        Path plugins = dir.resolve("plugins");
        HiveTestTables.installPlugin(plugins);
        Map<String, String> sys = new HashMap<>();
        sys.put("arrowflight.hive.uris", hms.uri());
        sys.put("arrowflight.hive.roots", lake.toString());
        sys.put("arrowflight.hive.orcTables", "true");
        sys.put("arrowflight.hive.insertOnlyTables", "true");
        sys.put("arrowflight.hive.textTables", "true");
        sys.put("arrowflight.hive.metadataTtlSeconds", "0");
        sys.put("arrowflight.hive.namesTtlSeconds", "0");
        ConfigSource source = new ConfigSource(sys::get, new Properties(), Map.of(), null);
        PluginSettings settings = new PluginSettings(plugins, null, "directory,hive", false);
        Files.createDirectories(dir.resolve("data"));
        helper = TestFlightServerHelper.builder().dataDir(dir.resolve("data").toString())
                .resultMode("complete")
                .registry(config -> StoreRegistry.open(config, new Configuration(), "localhost",
                        settings, source, List.of()))
                .start();
    }

    @AfterAll
    static void stop() throws Exception {
        if (helper != null) {
            helper.close();
        }
    }

    @Test
    void committedRowsOnlyInBothModes() throws Exception {
        for (String mode : List.of("complete", "partial")) {
            assertEquals(List.of("200|0|199|19900"), answer("SELECT count(*), min(i), max(i),"
                    + " sum(i) FROM hvmm.mm", mode).rows(), mode);
            assertEquals(List.of("a|50|1225", "b|50|6225"), answer("SELECT k, count(*), sum(i)"
                    + " FROM hvmm.mmp GROUP BY k", mode).rows(), mode);
            assertEquals(List.of("s120"), answer("SELECT s FROM hvmm.mm WHERE i = 120", mode)
                    .rows(), mode);
            assertEquals(List.of("0"), answer("SELECT count(*) FROM hvmm.mm WHERE i >= 1000",
                    mode).rows(), mode);
        }
        String all = "SELECT * FROM hvmm.mm";
        assertEquals(answer(all, "complete").rows(), answer(all, "partial").rows());
        assertEquals(1, info("SELECT * FROM hvmm.mmp WHERE k = 'b'", "partial").getEndpoints()
                .size(), "partitions are pruned, the aborted write is not planned");
    }

    @Test
    void orcInsertOnlyTableEqualsAPlainTableWithTheCommittedRows() throws Exception {
        for (String sql : List.of("SELECT * FROM hvmm.%s",
                "SELECT count(*) AS n, sum(i) AS s FROM hvmm.%s",
                "SELECT flag, count(*) AS n, sum(dec) AS s FROM hvmm.%s GROUP BY flag",
                "SELECT i, s FROM hvmm.%s WHERE i >= 490 AND i < 710")) {
            Answer reference = answer(String.format(sql, "mmo_ref"), "complete");
            for (String mode : List.of("complete", "partial")) {
                Answer answer = answer(String.format(sql, "mmo"), mode);
                assertEquals(reference.schema(), answer.schema(), mode + ": " + sql);
                assertEquals(reference.rows(), answer.rows(), mode + ": " + sql);
            }
        }
        assertEquals(List.of("800"), answer("SELECT count(*) FROM hvmm.mmo", "partial").rows());
    }

    @Test
    void textInsertOnlyTablesAnswerTheCommittedRowsOnly() throws Exception {
        String rows = "SELECT range::INTEGER AS i, 's' || range AS s FROM range(0, 100)"
                + " UNION ALL SELECT range::INTEGER, 's' || range FROM range(200, 250)";
        List<String> expected = duckDb(rows);
        for (String mode : List.of("complete", "partial")) {
            assertEquals(expected, answer("SELECT i, s FROM hvmm.mmt", mode).rows(), mode);
            assertEquals(List.of("150|0|249|16175"), answer("SELECT count(*), min(i), max(i),"
                    + " sum(i) FROM hvmm.mmt", mode).rows(), mode);
            assertEquals(List.of("s75"), answer("SELECT s FROM hvmm.mmt WHERE i = 75", mode)
                    .rows(), mode);
            assertEquals(List.of("0"), answer("SELECT count(*) FROM hvmm.mmt WHERE i >= 100"
                    + " AND i < 200 OR i >= 300", mode).rows(), mode);
            assertEquals(List.of("a|20|190", "b|20|990"), answer("SELECT k, count(*), sum(i)"
                    + " FROM hvmm.mmtp GROUP BY k", mode).rows(), mode);
        }
        assertEquals(1, info("SELECT * FROM hvmm.mmtp WHERE k = 'b'", "partial").getEndpoints()
                .size(), "partitions are pruned, the aborted write is not planned");
        // Joined with the Parquet insert-only table: only rows committed in both (0..99).
        assertEquals(List.of("100"), answer("SELECT count(*) FROM hvmm.mmt t JOIN hvmm.mm p"
                + " ON t.i = p.i AND t.s = p.s", "complete").rows());
    }

    @Test
    void aCommitIsVisibleToTheNextPartialQuery() throws Exception {
        String sql = "SELECT count(*), sum(i) FROM hvmm.mmc";
        for (String mode : List.of("complete", "partial")) {
            assertEquals(List.of("10|45"), answer(sql, mode).rows(), mode);
        }
        FlightInfo before = info("SELECT i FROM hvmm.mmc", "partial");
        hms.call("commitTxn", mmcOpen);
        assertEquals(List.of("30|435"), answer(sql, "partial").rows());
        // Complete answers read views refreshed at most once a minute: one snapshot or the other.
        assertTrue(List.of(List.of("10|45"), List.of("30|435")).contains(answer(sql, "complete")
                .rows()));
        // Endpoints planned before the commit answer the snapshot they were planned at, or
        // fail; they never mix in the later write.
        List<String> planned;
        try {
            planned = read(before);
        } catch (FlightRuntimeException e) {
            planned = null;   // refused: the snapshot moved on
        }
        if (planned != null) {
            assertEquals(10, planned.size(), "a plan made before the commit: " + planned);
        }
        // INSERT OVERWRITE: a committed base replaces every older write.
        long overwrite = (Long) hms.call("openTxn");
        long baseId = (Long) hms.call("allocateWriteId", overwrite, DB, "mmc");
        parquet(mmc.resolve(String.format("base_%07d", baseId)).resolve("000000_0"), 500, 505);
        assertEquals(List.of("30|435"), answer(sql, "partial").rows(), "base of an open write");
        hms.call("commitTxn", overwrite);
        assertEquals(List.of("5|2510"), answer(sql, "partial").rows());
        assertTrue(List.of(List.of("10|45"), List.of("30|435"), List.of("5|2510"))
                .contains(answer(sql, "complete").rows()));
    }

    @Test
    void aFullAcidLayoutIsRefusedInBothModesAndLeavesItsSiblingsReadable() throws Exception {
        for (String table : List.of("mmd", "mmdp")) {
            // Complete answers read views: the table has none (the reason is in the server log).
            FlightRuntimeException complete = assertThrows(FlightRuntimeException.class,
                    () -> answer("SELECT count(*) FROM hvmm." + table, "complete"));
            assertEquals(FlightStatusCode.NOT_FOUND, complete.status().code(), table);
            FlightRuntimeException partial = assertThrows(FlightRuntimeException.class,
                    () -> answer("SELECT count(*) FROM hvmm." + table, "partial"));
            assertEquals(FlightStatusCode.INVALID_ARGUMENT, partial.status().code(), table);
            assertTrue(partial.getMessage().contains("delete delta"), partial.getMessage());
        }
        // Tables listed after the refused ones in the same refresh still answer.
        assertEquals(List.of("800"), answer("SELECT count(*) FROM hvmm.mmo", "complete").rows());
        assertEquals(List.of("100"), answer("SELECT count(*) FROM hvmm.mmp", "complete").rows());
    }

    // ── helpers ──────────────────────────────────────────────────────────

    /**
     * An answer: announced schema and rows as text.
     *
     * @param schema announced schema
     * @param rows rows, columns separated by |, sorted
     */
    private record Answer(Schema schema, List<String> rows) {
    }

    private static Path table(String name, String partitionColumns, String format)
            throws Exception {
        Path location = Files.createDirectories(lake.resolve(name));
        hms.call("createTable", DB, name, "file:" + location, "i int;s string", partitionColumns,
                format, MM);
        return location;
    }

    private static Path partition(String name, String value) throws Exception {
        String location = (String) hms.call("addPartition", DB, name, List.of(value), null);
        return Path.of(location.substring("file:".length()));
    }

    /**
     * Writes rows [from, to) as a transaction of its own into a new delta directory.
     */
    private static void write(Path location, String table, int from, int to, boolean commit,
            String format) throws Exception {
        long txn = (Long) hms.call("openTxn");
        long writeId = (Long) hms.call("allocateWriteId", txn, DB, table);
        Path file = Files.createDirectories(location.resolve(delta(writeId))).resolve("000000_0");
        if ("orc".equals(format)) {
            HiveTestTables.writeOrc(file, from, to, "ZLIB");
        } else if (format.startsWith("text")) {
            text(file, from, to, format.endsWith(".gz"));
        } else {
            parquet(file, from, to);
        }
        hms.call(commit ? "commitTxn" : "abortTxn", txn);
    }

    private static String delta(long writeId) {
        return String.format("delta_%07d_%07d_0000", writeId, writeId);
    }

    private static void parquet(Path file, int from, int to) throws Exception {
        Files.createDirectories(file.getParent());
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT range::INTEGER AS i, 's' || range AS s FROM range(" + from
                    + ", " + to + ")) TO '" + file.toString().replace("'", "''")
                    + "' (FORMAT parquet)");
        }
    }

    /**
     * Writes rows [from, to) as Hive's default text format: fields separated by \001, one row
     * a line; with gzip the file gets the {@code .gz} suffix Hive's codec lookup needs.
     */
    private static void text(Path file, int from, int to, boolean gzip) throws Exception {
        Files.createDirectories(file.getParent());
        StringBuilder lines = new StringBuilder();
        for (int i = from; i < to; i++) {
            lines.append(i).append('\u0001').append('s').append(i).append('\n');
        }
        byte[] bytes = lines.toString().getBytes(StandardCharsets.UTF_8);
        if (gzip) {
            try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(
                    file.resolveSibling(file.getFileName() + ".gz")))) {
                out.write(bytes);
            }
        } else {
            Files.write(file, bytes);
        }
    }

    /** Rows of a query run by DuckDB alone, as {@link #read} formats them. */
    private static List<String> duckDb(String sql) throws Exception {
        List<String> rows = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement();
                ResultSet rs = s.executeQuery(sql)) {
            int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<String> values = new ArrayList<>();
                for (int c = 1; c <= columns; c++) {
                    values.add(String.valueOf(rs.getObject(c)));
                }
                rows.add(String.join("|", values));
            }
        }
        rows.sort(null);
        return rows;
    }

    private static Answer answer(String sql, String mode) throws Exception {
        FlightInfo info = info(sql, mode);
        return new Answer(info.getSchemaOptional().orElseThrow(), read(info));
    }

    private static List<String> read(FlightInfo info) throws Exception {
        List<String> rows = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    for (int i = 0; i < root.getRowCount(); i++) {
                        List<String> values = new ArrayList<>();
                        for (FieldVector vector : root.getFieldVectors()) {
                            values.add(String.valueOf(vector.getObject(i)));
                        }
                        rows.add(String.join("|", values));
                    }
                }
            }
        }
        rows.sort(null);
        return rows;
    }

    private static FlightInfo info(String sql, String mode) {
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert(ResultModeMiddleware.HEADER, mode);
        return helper.sqlClient().execute(sql, new HeaderCallOption(headers));
    }
}
