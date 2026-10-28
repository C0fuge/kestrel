package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.luben.zstd.Zstd;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
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
 * Runs the server with the packaged hive plugin (plugins/hive, share=hadoop,arrow) and
 * hive.orcTables=true against a real metastore: ORC tables written in the test (all ORC
 * compression kinds the plugin ships codecs for, zstd included, read in the server's JVM next to
 * its own zstd-jni) are answered in both result modes exactly like the same rows in Parquet
 * tables of the same metastore, alone, filtered, grouped by a partition column and joined with
 * Parquet and with each other.
 */
class HiveOrcIntegrationTest {

    @TempDir
    static Path dir;
    static Path lake;
    static AutoCloseable proxy;
    static TestFlightServerHelper helper;
    static String expected;

    @BeforeAll
    static void start() throws Exception {
        Assumptions.assumeTrue(HiveTestTables.available(), "flight-plugin-hive is not built");
        HiveTestTables.Hms hms = HiveTestTables.hms();
        String columns = HiveTestTables.orcConstant("COLUMNS");
        expected = HiveTestTables.orcConstant("EXPECTED");
        lake = Files.createDirectories(dir.resolve("lake"));
        hms.call("createDatabase", "hvorc");
        // Unpartitioned: two files, the same rows in Parquet.
        Path flat = Files.createDirectories(lake.resolve("flat"));
        HiveTestTables.writeOrc(flat.resolve("000000_0"), 0, 1800, "ZLIB");
        HiveTestTables.writeOrc(flat.resolve("000001_0"), 1800, 3000, "ZSTD");
        hms.call("createTable", "hvorc", "flat", "file:" + flat, columns, "", "orc", null);
        Path flatPq = Files.createDirectories(lake.resolve("flat_pq"));
        parquet(flatPq.resolve("000000_0"), 0, 3000);
        hms.call("createTable", "hvorc", "flat_pq", "file:" + flatPq, columns, "", "parquet", null);
        // Partitioned by region: a file per compression kind, a NULL partition.
        Path items = Files.createDirectories(lake.resolve("items"));
        Path itemsPq = Files.createDirectories(lake.resolve("items_pq"));
        hms.call("createTable", "hvorc", "items", "file:" + items, columns, "region string", "orc",
                null);
        hms.call("createTable", "hvorc", "items_pq", "file:" + itemsPq, columns, "region string",
                "parquet", null);
        Object[][] files = {
            {"AIR", 0, 2500, "ZSTD"},
            {"MAIL", 2500, 3200, "SNAPPY"},
            {"MAIL", 3200, 4000, "NONE"},
            {"__HIVE_DEFAULT_PARTITION__", 4000, 4300, "LZ4"},
        };
        Map<String, String[]> locations = new HashMap<>();
        for (Object[] f : files) {
            String region = (String) f[0];
            if (!locations.containsKey(region)) {
                locations.put(region, new String[] {
                    (String) hms.call("addPartition", "hvorc", "items", List.of(region), null),
                    (String) hms.call("addPartition", "hvorc", "items_pq", List.of(region), null)});
            }
            String[] at = locations.get(region);
            String name = "part-" + f[1];
            HiveTestTables.writeOrc(Path.of(at[0].substring("file:".length())).resolve(name),
                    (int) f[1], (int) f[2], (String) f[3]);
            parquet(Path.of(at[1].substring("file:".length())).resolve(name), (int) f[1], (int) f[2]);
        }
        Path plugins = dir.resolve("plugins");
        HiveTestTables.installPlugin(plugins);
        proxy = hms.proxy();
        Map<String, String> sys = new HashMap<>();
        sys.put("arrowflight.hive.uris", HiveTestTables.uri(proxy));
        sys.put("arrowflight.hive.roots", lake.toString());
        sys.put("arrowflight.hive.orcTables", "true");
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
        if (proxy != null) {
            proxy.close();
        }
    }

    @Test
    void orcTablesAreListedNextToParquetOnes() throws Exception {
        List<String> tables = column(helper.sqlClient().getTables(null, "hvorc", null, null, false),
                "table_name");
        assertTrue(tables.containsAll(List.of("flat", "flat_pq", "items", "items_pq")),
                tables.toString());
    }

    @Test
    void completeAnswersEqualTheRowsComputedByDuckDb() throws Exception {
        assertEquals(direct("(" + expected + " FROM range(0, 3000))"), fingerprint("hvorc.flat"));
        assertEquals(direct("(" + expected + ", CASE WHEN range < 2500 THEN 'AIR' WHEN range < 4000"
                + " THEN 'MAIL' END AS region FROM range(0, 4300))"), fingerprint("hvorc.items"));
    }

    @Test
    void bothModesAnswerOrcTablesLikeTheSameRowsInParquet() throws Exception {
        for (String sql : List.of(
                "SELECT * FROM hvorc.%s",
                "SELECT count(*) AS n FROM hvorc.%s",
                "SELECT region, count(*) AS n, sum(dec) AS s, min(dt) AS lo, max(ts) AS hi,"
                        + " count(m) AS maps FROM hvorc.%s GROUP BY region",
                "SELECT i, s, arr, st, m, ch FROM hvorc.%s WHERE i >= 3990 AND i < 4010",
                "SELECT count(*) AS n FROM hvorc.%s WHERE region = 'MAIL' AND b < 3000000000000"
                        + " AND s LIKE 's2%%'",
                "SELECT i, flag FROM hvorc.%s WHERE dec = 1542.50",
                "SELECT count(*) AS n, sum(d) AS s FROM hvorc.%s WHERE region IS NULL",
                "SELECT region FROM hvorc.%s WHERE dt > DATE '2030-01-01'")) {
            String orc = String.format(sql, "items");
            Answer reference = answer(String.format(sql, "items_pq"), "complete");
            for (String mode : List.of("complete", "partial")) {
                Answer answer = answer(orc, mode);
                assertEquals(reference.schema(), answer.schema(), mode + ": " + orc);
                assertEquals(reference.rows(), answer.rows(), mode + ": " + orc);
            }
        }
        assertEquals(List.of("2500"), answer("SELECT count(*) FROM hvorc.items WHERE region = 'AIR'",
                "partial").rows());
        assertEquals(1, info("SELECT * FROM hvorc.items WHERE region = 'AIR'", "partial")
                .getEndpoints().size(), "partitions are pruned");
    }

    @Test
    void joinsOfOrcWithParquetAndWithOrcEqualThoseOverParquet() throws Exception {
        String join = "SELECT o.region, count(*) AS n, sum(f.dec) AS s FROM hvorc.%s o"
                + " JOIN hvorc.%s f ON o.i = f.i AND o.s = f.s GROUP BY o.region";
        Answer reference = answer(String.format(join, "items_pq", "flat_pq"), "complete");
        assertEquals(List.of("AIR|2143|3347321.25", "MAIL|428|1470892.50"), reference.rows());
        assertEquals(reference, answer(String.format(join, "items", "flat_pq"), "complete"));
        assertEquals(reference, answer(String.format(join, "items_pq", "flat"), "complete"));
        assertEquals(reference, answer(String.format(join, "items", "flat"), "complete"));
        String self = "SELECT count(*) AS n FROM hvorc.%1$s a, hvorc.%1$s b WHERE a.i = b.i + 1";
        assertEquals(answer(String.format(self, "flat_pq"), "complete"),
                answer(String.format(self, "flat"), "complete"));
    }

    @Test
    void zstdWorksInTheServerAndInThePluginOfOneJvm() throws Exception {
        byte[] data = "the server's own zstd-jni".repeat(20).getBytes(StandardCharsets.UTF_8);
        byte[] packed = Zstd.compress(data);
        // The AIR partition is ZSTD: the plugin's zstd-jni decodes it after the server's loaded.
        assertEquals(List.of("2143|3123750"), answer("SELECT count(s), sum(i) FROM hvorc.items"
                + " WHERE region = 'AIR'", "partial").rows());
        assertTrue(Arrays.equals(data, Zstd.decompress(packed, data.length)));
        assertEquals(Zstd.class.getClassLoader(), getClass().getClassLoader(),
                "the server's class path, not the plugin's");
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

    private static void parquet(Path file, int from, int to) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (" + expected + " FROM range(" + from + ", " + to + ")) TO '" + file
                    + "' (FORMAT parquet)");
        }
    }

    private static String fingerprintSql(String from) {
        return "SELECT count(*) AS n, md5(coalesce(string_agg(t::VARCHAR, '|' ORDER BY t::VARCHAR),"
                + " '')) AS h FROM " + from + " t";
    }

    private static String fingerprint(String table) throws Exception {
        return answer(fingerprintSql(table), "complete").rows().get(0);
    }

    private static String direct(String from) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement();
                ResultSet rs = s.executeQuery(fingerprintSql(from))) {
            rs.next();
            return rs.getLong(1) + "|" + rs.getString(2);
        }
    }

    private static Answer answer(String sql, String mode) throws Exception {
        FlightInfo info = info(sql, mode);
        Schema announced = info.getSchemaOptional().orElseThrow();
        List<String> rows = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                assertEquals(announced, stream.getSchema(), mode + " stream vs FlightInfo: " + sql);
                while (stream.next()) {
                    rows.addAll(rows(stream.getRoot()));
                }
            }
        }
        rows.sort(null);
        return new Answer(announced, rows);
    }

    private static List<String> rows(VectorSchemaRoot root) {
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < root.getRowCount(); i++) {
            List<String> values = new ArrayList<>();
            for (FieldVector vector : root.getFieldVectors()) {
                Object value = vector.getObject(i);
                values.add(value instanceof byte[] bytes ? Arrays.toString(bytes) : String.valueOf(value));
            }
            rows.add(String.join("|", values));
        }
        return rows;
    }

    private static FlightInfo info(String sql, String mode) {
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert(ResultModeMiddleware.HEADER, mode);
        return helper.sqlClient().execute(sql, new HeaderCallOption(headers));
    }

    private static List<String> column(FlightInfo info, String name) throws Exception {
        List<String> values = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VarCharVector vector = (VarCharVector) stream.getRoot().getVector(name);
                    for (int i = 0; i < stream.getRoot().getRowCount(); i++) {
                        values.add(new String(vector.get(i), StandardCharsets.UTF_8));
                    }
                }
            }
        }
        return values;
    }
}
