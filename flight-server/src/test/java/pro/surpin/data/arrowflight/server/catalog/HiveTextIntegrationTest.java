package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

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
 * Runs the server with the packaged hive plugin and hive.textTables=true against a real
 * metastore: text tables written in the test (arrays, structs and maps among the columns; Hive's
 * default layout with a gzip file, and a
 * partitioned comma-separated table with a header line and an empty null format) are answered in
 * both result modes exactly like the same rows in Parquet tables of the same metastore, alone,
 * filtered, grouped by a partition column and joined with Parquet and with each other; an
 * OpenCSVSerde table answers its strings as Hive parses them; a JsonSerDe table (DuckDB's JSON of
 * the same rows, keys in another case and order, an unknown key) answers like the Parquet one.
 */
class HiveTextIntegrationTest {

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
    private static final List<String> NAMES = List.of("i", "b", "s", "dec", "dt", "d", "flag",
            "ts", "arr", "st", "m");
    /** Each column as Hive writes it in text (nested values at the default separators 2, 3). */
    private static final List<String> TEXT = List.of("i::VARCHAR", "b::VARCHAR", "s", "dec::VARCHAR",
            "dt::VARCHAR", "d::VARCHAR", "flag::VARCHAR", "ts::VARCHAR",
            "arr[1]::VARCHAR || chr(2) || arr[2]::VARCHAR", "st.x::VARCHAR || chr(2) || st.y",
            "map_keys(m)[1] || chr(3) || map_values(m)[1]::VARCHAR");

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
        hms.call("createDatabase", "hvtxt");
        // Unpartitioned, Hive's defaults (\001, \N): a plain and a gzip file, same rows in Parquet.
        Path flat = Files.createDirectories(lake.resolve("flat"));
        Files.write(flat.resolve("000000_0"), text(0, 1800, "\u0001", "\\N", false));
        try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(
                flat.resolve("000001_0.gz")))) {
            out.write(text(1800, 3000, "\u0001", "\\N", false));
        }
        hms.call("createTable", "hvtxt", "flat", "file:" + flat, COLUMNS, "", "text", null);
        Path flatPq = Files.createDirectories(lake.resolve("flat_pq"));
        parquet(flatPq.resolve("000000_0"), 0, 3000);
        hms.call("createTable", "hvtxt", "flat_pq", "file:" + flatPq, COLUMNS, "", "parquet", null);
        // Partitioned by region, comma-separated with a header line and '' as NULL.
        Path items = Files.createDirectories(lake.resolve("items"));
        Path itemsPq = Files.createDirectories(lake.resolve("items_pq"));
        hms.call("createTable", "hvtxt", "items", "file:" + items, COLUMNS, "region string", "text",
                Map.of("serde:field.delim", ",", "serde:serialization.null.format", "",
                        "skip.header.line.count", "1"));
        hms.call("createTable", "hvtxt", "items_pq", "file:" + itemsPq, COLUMNS, "region string",
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
                    (String) hms.call("addPartition", "hvtxt", "items", List.of(region), null),
                    (String) hms.call("addPartition", "hvtxt", "items_pq", List.of(region), null)});
            }
            String[] at = locations.get(region);
            String name = "part-" + f[1];
            Files.write(Path.of(at[0].substring("file:".length())).resolve(name + ".csv"),
                    text((int) f[1], (int) f[2], ",", "", true));
            parquet(Path.of(at[1].substring("file:".length())).resolve(name), (int) f[1], (int) f[2]);
        }
        // OpenCSVSerde: every column a string, parsed by opencsv as Hive does.
        Path people = Files.createDirectories(lake.resolve("people"));
        Files.writeString(people.resolve("000000_0"),
                "id,name,note\n1,\"Doe, Jane\",\"said \"\"hi\"\"\"\n2,Bob\n");
        hms.call("createTable", "hvtxt", "people", "file:" + people, "id string;name string;note string",
                "", "csv", Map.of("skip.header.line.count", "1"));
        // HCatalog's JsonSerDe: DuckDB's JSON of the rows, a plain and a gzip file.
        Path json = Files.createDirectories(lake.resolve("json"));
        Files.write(json.resolve("000000_0"), json(0, 1000));
        try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(
                json.resolve("000001_0.gz")))) {
            out.write(json(1000, 3000));
        }
        hms.call("createTable", "hvtxt", "json", "file:" + json, COLUMNS, "", "json", null);
        Path plugins = dir.resolve("plugins");
        HiveTestTables.installPlugin(plugins);
        proxy = hms.proxy();
        Map<String, String> sys = new HashMap<>();
        sys.put("arrowflight.hive.uris", HiveTestTables.uri(proxy));
        sys.put("arrowflight.hive.roots", lake.toString());
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
        if (proxy != null) {
            proxy.close();
        }
    }

    @Test
    void textTablesAreListedNextToParquetOnes() throws Exception {
        List<String> tables = column(helper.sqlClient().getTables(null, "hvtxt", null, null, false),
                "table_name");
        assertTrue(tables.containsAll(List.of("flat", "flat_pq", "items", "items_pq")),
                tables.toString());
    }

    @Test
    void completeAnswersEqualTheRowsComputedByDuckDb() throws Exception {
        assertEquals(direct("(" + EXPECTED + " FROM range(0, 3000))"), fingerprint("hvtxt.flat"));
        assertEquals(direct("(" + EXPECTED + ", CASE WHEN range < 2500 THEN 'AIR' WHEN range < 4000"
                + " THEN 'MAIL' END AS region FROM range(0, 4300))"), fingerprint("hvtxt.items"));
    }

    @Test
    void bothModesAnswerTextTablesLikeTheSameRowsInParquet() throws Exception {
        for (String sql : List.of(
                "SELECT * FROM hvtxt.%s",
                "SELECT count(*) AS n FROM hvtxt.%s",
                "SELECT region, count(*) AS n, sum(dec) AS s, min(dt) AS lo, max(ts) AS hi,"
                        + " count(s) AS strings, sum(d) AS ds, count(m) AS maps, sum(st.x) AS xs,"
                        + " sum(arr[2]) AS a2 FROM hvtxt.%s GROUP BY region",
                "SELECT i, s, ts, arr, st, m FROM hvtxt.%s WHERE i >= 3990 AND i < 4010",
                "SELECT count(*) AS n FROM hvtxt.%s WHERE region = 'MAIL' AND b < 3000000000000"
                        + " AND s LIKE 's2%%'",
                "SELECT i, flag FROM hvtxt.%s WHERE dec = 1542.50",
                "SELECT count(*) AS n, sum(d) AS s FROM hvtxt.%s WHERE region IS NULL",
                "SELECT region FROM hvtxt.%s WHERE dt > DATE '2030-01-01'")) {
            for (String[] pair : List.of(new String[] {"items", "items_pq"},
                    new String[] {"flat", "flat_pq"})) {
                if (sql.contains("region") && pair[0].equals("flat")) {
                    continue;
                }
                String text = String.format(sql, pair[0]);
                Answer reference = answer(String.format(sql, pair[1]), "complete");
                for (String mode : List.of("complete", "partial")) {
                    Answer answer = answer(text, mode);
                    assertEquals(reference.schema(), answer.schema(), mode + ": " + text);
                    assertEquals(reference.rows(), answer.rows(), mode + ": " + text);
                }
            }
        }
        assertEquals(List.of("2500"), answer("SELECT count(*) FROM hvtxt.items WHERE region = 'AIR'",
                "partial").rows());
        assertEquals(1, info("SELECT * FROM hvtxt.items WHERE region = 'AIR'", "partial")
                .getEndpoints().size(), "partitions are pruned");
    }

    @Test
    void joinsOfTextWithParquetAndWithTextEqualThoseOverParquet() throws Exception {
        String join = "SELECT o.region, count(*) AS n, sum(f.dec) AS s FROM hvtxt.%s o"
                + " JOIN hvtxt.%s f ON o.i = f.i AND o.s = f.s GROUP BY o.region";
        Answer reference = answer(String.format(join, "items_pq", "flat_pq"), "complete");
        assertEquals(List.of("AIR|2143|3347321.25", "MAIL|428|1470892.50"), reference.rows());
        assertEquals(reference, answer(String.format(join, "items", "flat_pq"), "complete"));
        assertEquals(reference, answer(String.format(join, "items_pq", "flat"), "complete"));
        assertEquals(reference, answer(String.format(join, "items", "flat"), "complete"));
        String self = "SELECT count(*) AS n FROM hvtxt.%1$s a, hvtxt.%1$s b WHERE a.i = b.i + 1";
        assertEquals(answer(String.format(self, "flat_pq"), "complete"),
                answer(String.format(self, "flat"), "complete"));
    }

    @Test
    void jsonSerdeTablesAnswerLikeTheSameRowsInParquet() throws Exception {
        assertEquals(direct("(" + EXPECTED + " FROM range(0, 3000))"), fingerprint("hvtxt.json"));
        for (String sql : List.of(
                "SELECT * FROM hvtxt.%s",
                "SELECT count(*) AS n, sum(dec) AS s, min(dt) AS lo, max(ts) AS hi, count(s) AS c,"
                        + " sum(d) AS ds, count(m) AS maps, sum(st.x) AS xs, sum(arr[2]) AS a2"
                        + " FROM hvtxt.%s",
                "SELECT i, s, ts, arr, st, m FROM hvtxt.%s WHERE i >= 990 AND i < 1010",
                "SELECT i, flag FROM hvtxt.%s WHERE dec = 1542.50 OR s IS NULL AND i < 30")) {
            Answer reference = answer(String.format(sql, "flat_pq"), "complete");
            for (String mode : List.of("complete", "partial")) {
                Answer answer = answer(String.format(sql, "json"), mode);
                assertEquals(reference.schema(), answer.schema(), mode + ": " + sql);
                assertEquals(reference.rows(), answer.rows(), mode + ": " + sql);
            }
        }
        // Every row whose struct and map are not NULL (3000 - 429) joins its Parquet twin.
        assertEquals(List.of("2571"), answer("SELECT count(*) AS n FROM hvtxt.json a JOIN"
                + " hvtxt.flat_pq b ON a.i = b.i AND a.st = b.st AND a.m = b.m", "complete").rows());
    }

    @Test
    void openCsvSerdeTablesAnswerInBothModes() throws Exception {
        for (String mode : List.of("complete", "partial")) {
            Answer answer = answer("SELECT id, name, note FROM hvtxt.people", mode);
            assertEquals(List.of("1|Doe, Jane|said \"hi\"", "2|Bob|null"), answer.rows(), mode);
            assertEquals(List.of("1"), answer("SELECT id FROM hvtxt.people WHERE name LIKE '%Jane'",
                    mode).rows(), mode);
        }
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

    /**
     * Renders rows [from, to) as Hive text lines, the values as DuckDB prints them.
     */
    private static byte[] text(int from, int to, String delimiter, String nullFormat,
            boolean header) throws Exception {
        StringBuilder fields = new StringBuilder();
        for (String value : TEXT) {
            fields.append(fields.isEmpty() ? "" : ", ").append("coalesce(").append(value)
                    .append(", '").append(nullFormat).append("')");
        }
        StringBuilder lines = new StringBuilder(header ? String.join(delimiter, NAMES) + "\n" : "");
        String sql = "SELECT concat_ws(chr(" + (int) delimiter.charAt(0) + "), " + fields
                + ") FROM (" + EXPECTED + " FROM range(" + from + ", " + to + ")) ORDER BY i";
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement();
                ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                lines.append(rs.getString(1)).append('\n');
            }
        }
        return lines.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Renders rows [from, to) as JSON lines (DuckDB's JSON of each row with the column s as key
     * S, i as Hive's internal name _col0 and an unknown key first), as a JsonSerDe table holds
     * them.
     */
    private static byte[] json(int from, int to) throws Exception {
        StringBuilder lines = new StringBuilder();
        String sql = "SELECT to_json(t)::VARCHAR FROM (" + EXPECTED + " FROM range(" + from + ", "
                + to + ")) t ORDER BY i";
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement();
                ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                String row = rs.getString(1);
                assertTrue(row.startsWith("{\"i\":") && row.contains(",\"s\":"), row);
                lines.append("{\"extra\":[1,{\"s\":2}],\"_col0\":")
                        .append(row.substring("{\"i\":".length()).replace(",\"s\":", ",\"S\":"))
                        .append('\n');
            }
        }
        return lines.toString().getBytes(StandardCharsets.UTF_8);
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
