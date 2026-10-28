package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
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
 * Runs the server with the packaged hive plugin and hive.avroTables=true against a real
 * metastore: Avro tables written in the test (all of Hive's Avro column kinds incl. decimal,
 * date, timestamp, array, struct and map; one table with its schema as avro.schema.literal and
 * deflate / uncompressed files, one partitioned table with the columns' schema and zstandard
 * files) are answered in both result modes exactly like the same rows in Parquet tables of the
 * same metastore, alone, filtered, grouped by a partition column and joined with Parquet and with
 * each other.
 */
class HiveAvroIntegrationTest {

    private static final String COLUMNS = "i int;b bigint;s string;dec decimal(12,2);dt date;"
            + "d double;flag boolean;ts timestamp;arr array<int>;st struct<x:int,y:string>;"
            + "m map<string,int>";
    /** The rows computed by DuckDB: append {@code FROM range(from, to)}. */
    private static final String EXPECTED = "SELECT range::INTEGER AS i,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE range * 1000000000 END AS b,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE 's' || range || 'ü' END AS s,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE (range * 1.25)::DECIMAL(12,2) END AS dec,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE DATE '2020-01-01' + range::INTEGER END AS dt,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE range::DOUBLE / 8 END AS d,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE range % 2 = 0 END AS flag,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE TIMESTAMP '2020-06-01 01:30:00'"
            + " + INTERVAL (range) MINUTE + INTERVAL (range * 1001) MILLISECOND END AS ts,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE [range::INTEGER, range::INTEGER + 1] END AS arr,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE {'x': -range::INTEGER, 'y': 'y' || range}"
            + " END AS st,"
            + " CASE WHEN range % 7 = 3 THEN NULL ELSE MAP {'k' || range: range::INTEGER} END AS m";
    /** The schema Hive makes from {@link #COLUMNS} (every field a union with null). */
    private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"baseRecord\",\"fields\":["
            + field("i", "\"int\"") + "," + field("b", "\"long\"") + "," + field("s", "\"string\"")
            + "," + field("dec", "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":12,"
            + "\"scale\":2}") + "," + field("dt", "{\"type\":\"int\",\"logicalType\":\"date\"}") + ","
            + field("d", "\"double\"") + "," + field("flag", "\"boolean\"") + ","
            + field("ts", "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}") + ","
            + field("arr", "{\"type\":\"array\",\"items\":[\"null\",\"int\"]}") + ","
            + field("st", "{\"type\":\"record\",\"name\":\"record_0\",\"fields\":["
                    + field("x", "\"int\"") + "," + field("y", "\"string\"") + "]}") + ","
            + field("m", "{\"type\":\"map\",\"values\":[\"null\",\"int\"]}") + "]}";

    @TempDir
    static Path dir;
    static Path lake;
    static AutoCloseable proxy;
    static TestFlightServerHelper helper;

    @BeforeAll
    static void start() throws Exception {
        Assumptions.assumeTrue(HiveTestTables.available(), "flight-plugin-hive is not built");
        HiveTestTables.Hms hms = HiveTestTables.hms();
        lake = Files.createDirectories(dir.resolve("lake"));
        hms.call("createDatabase", "hvavro");
        // Unpartitioned, the schema as a table property: a deflate and an uncompressed file.
        Path flat = Files.createDirectories(lake.resolve("flat"));
        HiveTestTables.writeAvro(flat.resolve("000000_0"), SCHEMA, "deflate", rows(0, 1800));
        HiveTestTables.writeAvro(flat.resolve("000001_0"), SCHEMA, "null", rows(1800, 3000));
        hms.call("createTable", "hvavro", "flat", "file:" + flat, COLUMNS, "", "avro",
                Map.of("avro.schema.literal", SCHEMA));
        Path flatPq = Files.createDirectories(lake.resolve("flat_pq"));
        parquet(flatPq.resolve("000000_0"), 0, 3000);
        hms.call("createTable", "hvavro", "flat_pq", "file:" + flatPq, COLUMNS, "", "parquet", null);
        // Partitioned by region, the columns' schema, zstandard files.
        Path items = Files.createDirectories(lake.resolve("items"));
        Path itemsPq = Files.createDirectories(lake.resolve("items_pq"));
        hms.call("createTable", "hvavro", "items", "file:" + items, COLUMNS, "region string", "avro",
                null);
        hms.call("createTable", "hvavro", "items_pq", "file:" + itemsPq, COLUMNS, "region string",
                "parquet", null);
        Object[][] files = {
            {"AIR", 0, 2500},
            {"MAIL", 2500, 3200},
            {"MAIL", 3200, 4000},
            {"__HIVE_DEFAULT_PARTITION__", 4000, 4300},
        };
        Map<String, String[]> locations = new HashMap<>();
        for (Object[] f : files) {
            String region = (String) f[0];
            if (!locations.containsKey(region)) {
                locations.put(region, new String[] {
                    (String) hms.call("addPartition", "hvavro", "items", List.of(region), null),
                    (String) hms.call("addPartition", "hvavro", "items_pq", List.of(region), null)});
            }
            String[] at = locations.get(region);
            String name = "part-" + f[1];
            HiveTestTables.writeAvro(Files.createDirectories(Path.of(at[0].substring("file:".length())))
                    .resolve(name + ".avro"), SCHEMA, "zstandard", rows((int) f[1], (int) f[2]));
            parquet(Files.createDirectories(Path.of(at[1].substring("file:".length()))).resolve(name),
                    (int) f[1], (int) f[2]);
        }
        Path plugins = dir.resolve("plugins");
        HiveTestTables.installPlugin(plugins);
        proxy = hms.proxy();
        Map<String, String> sys = new HashMap<>();
        sys.put("arrowflight.hive.uris", HiveTestTables.uri(proxy));
        sys.put("arrowflight.hive.roots", lake.toString());
        sys.put("arrowflight.hive.avroTables", "true");
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
    void avroTablesAreListedNextToParquetOnes() throws Exception {
        List<String> tables = column(helper.sqlClient().getTables(null, "hvavro", null, null, false),
                "table_name");
        assertTrue(tables.containsAll(List.of("flat", "flat_pq", "items", "items_pq")),
                tables.toString());
    }

    @Test
    void completeAnswersEqualTheRowsComputedByDuckDb() throws Exception {
        assertEquals(direct("(" + EXPECTED + " FROM range(0, 3000))"), fingerprint("hvavro.flat"));
        assertEquals(direct("(" + EXPECTED + ", CASE WHEN range < 2500 THEN 'AIR' WHEN range < 4000"
                + " THEN 'MAIL' END AS region FROM range(0, 4300))"), fingerprint("hvavro.items"));
    }

    @Test
    void bothModesAnswerAvroTablesLikeTheSameRowsInParquet() throws Exception {
        for (String sql : List.of(
                "SELECT * FROM hvavro.%s",
                "SELECT count(*) AS n FROM hvavro.%s",
                "SELECT region, count(*) AS n, sum(dec) AS s, min(dt) AS lo, max(ts) AS hi,"
                        + " count(s) AS strings, sum(d) AS ds, count(m) AS maps, sum(st.x) AS xs,"
                        + " sum(arr[2]) AS a2 FROM hvavro.%s GROUP BY region",
                "SELECT i, s, ts, arr, st, m FROM hvavro.%s WHERE i >= 3990 AND i < 4010",
                "SELECT count(*) AS n FROM hvavro.%s WHERE region = 'MAIL' AND b < 3000000000000"
                        + " AND s LIKE 's2%%'",
                "SELECT i, flag FROM hvavro.%s WHERE dec = 1542.50",
                "SELECT count(*) AS n, sum(d) AS s FROM hvavro.%s WHERE region IS NULL",
                "SELECT region FROM hvavro.%s WHERE dt > DATE '2030-01-01'")) {
            for (String[] pair : List.of(new String[] {"items", "items_pq"},
                    new String[] {"flat", "flat_pq"})) {
                if (sql.contains("region") && pair[0].equals("flat")) {
                    continue;
                }
                String avro = String.format(sql, pair[0]);
                Answer reference = answer(String.format(sql, pair[1]), "complete");
                for (String mode : List.of("complete", "partial")) {
                    Answer answer = answer(avro, mode);
                    assertEquals(reference.schema(), answer.schema(), mode + ": " + avro);
                    assertEquals(reference.rows(), answer.rows(), mode + ": " + avro);
                }
            }
        }
        assertEquals(List.of("2500"), answer("SELECT count(*) FROM hvavro.items WHERE region = 'AIR'",
                "partial").rows());
        assertEquals(1, info("SELECT * FROM hvavro.items WHERE region = 'AIR'", "partial")
                .getEndpoints().size(), "partitions are pruned");
    }

    @Test
    void joinsOfAvroWithParquetAndWithAvroEqualThoseOverParquet() throws Exception {
        String join = "SELECT o.region, count(*) AS n, sum(f.dec) AS s FROM hvavro.%s o"
                + " JOIN hvavro.%s f ON o.i = f.i AND o.s = f.s GROUP BY o.region";
        Answer reference = answer(String.format(join, "items_pq", "flat_pq"), "complete");
        assertEquals(List.of("AIR|2143|3347321.25", "MAIL|428|1470892.50"), reference.rows());
        assertEquals(reference, answer(String.format(join, "items", "flat_pq"), "complete"));
        assertEquals(reference, answer(String.format(join, "items_pq", "flat"), "complete"));
        assertEquals(reference, answer(String.format(join, "items", "flat"), "complete"));
        String self = "SELECT count(*) AS n FROM hvavro.%1$s a, hvavro.%1$s b WHERE a.i = b.i + 1";
        assertEquals(answer(String.format(self, "flat_pq"), "complete"),
                answer(String.format(self, "flat"), "complete"));
    }

    private static String field(String name, String type) {
        return "{\"name\":\"" + name + "\",\"type\":[\"null\"," + type + "],\"default\":null}";
    }

    /**
     * Computes rows [from, to) with DuckDB as plain Java values for an Avro file: dates as days,
     * timestamps as milliseconds (UTC), decimals as BigDecimal.
     */
    private static List<Object[]> rows(int from, int to) throws Exception {
        List<Object[]> rows = new ArrayList<>();
        String sql = "SELECT i, b, s, dec, (dt - DATE '1970-01-01')::INTEGER, d, flag, epoch_ms(ts),"
                + " arr[1], arr[2], st.x, st.y, map_keys(m)[1], map_values(m)[1], b IS NULL FROM ("
                + EXPECTED + " FROM range(" + from + ", " + to + ")) ORDER BY i";
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement();
                ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                if (rs.getBoolean(15)) {
                    rows.add(new Object[] {rs.getInt(1), null, null, null, null, null, null, null,
                        null, null, null});
                    continue;
                }
                rows.add(new Object[] {rs.getInt(1), rs.getLong(2), rs.getString(3),
                    (BigDecimal) rs.getObject(4), rs.getInt(5), rs.getDouble(6), rs.getBoolean(7),
                    rs.getLong(8), Arrays.asList(rs.getInt(9), rs.getInt(10)),
                    new Object[] {rs.getInt(11), rs.getString(12)},
                    Map.of(rs.getString(13), rs.getInt(14))});
            }
        }
        return rows;
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
            s.execute("COPY (" + EXPECTED + " FROM range(" + from + ", " + to + ")) TO '" + file
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
