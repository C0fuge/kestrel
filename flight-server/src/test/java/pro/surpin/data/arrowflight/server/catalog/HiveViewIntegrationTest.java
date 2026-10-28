package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
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
 * Runs the server with the packaged hive plugin and hive.views=true against a real metastore:
 * Hive views over an ORC, a Parquet and a text table, a join of two formats and a view over a
 * view answer in both result modes like their queries over the tables, with the declared column
 * types; a request in parts gets one endpoint with the complete answer; GetTables lists the views
 * as VIEW and GetTableTypes names the type; a view read at an earlier snapshot is refused.
 */
class HiveViewIntegrationTest {

    private static final List<String> VIEWS = List.of("v_count", "v_join", "v_orc", "v_pq",
            "v_txt");

    @TempDir
    static Path dir;
    static AutoCloseable proxy;
    static TestFlightServerHelper helper;

    @BeforeAll
    static void start() throws Exception {
        Assumptions.assumeTrue(HiveTestTables.available(), "flight-plugin-hive is not built");
        HiveTestTables.Hms hms = HiveTestTables.hms();
        Path lake = Files.createDirectories(dir.resolve("lake"));
        String columns = HiveTestTables.orcConstant("COLUMNS");
        String expected = HiveTestTables.orcConstant("EXPECTED");
        hms.call("createDatabase", "hvview");
        Path orc = Files.createDirectories(lake.resolve("o"));
        HiveTestTables.writeOrc(orc.resolve("000000_0"), 0, 1000, "ZLIB");
        hms.call("createTable", "hvview", "o", "file:" + orc, columns, "", "orc", null);
        Path parquet = Files.createDirectories(lake.resolve("o_pq"));
        Path text = Files.createDirectories(lake.resolve("t"));
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (" + expected + " FROM range(0, 1000)) TO '"
                    + parquet.resolve("000000_0") + "' (FORMAT parquet)");
            StringBuilder lines = new StringBuilder();
            try (ResultSet rs = s.executeQuery("SELECT i::VARCHAR || chr(1) || coalesce(s, '\\N')"
                    + " FROM (" + expected + " FROM range(0, 1000)) ORDER BY i")) {
                while (rs.next()) {
                    lines.append(rs.getString(1)).append('\n');
                }
            }
            Files.writeString(text.resolve("000000_0"), lines, StandardCharsets.UTF_8);
        }
        hms.call("createTable", "hvview", "o_pq", "file:" + parquet, columns, "", "parquet", null);
        hms.call("createTable", "hvview", "t", "file:" + text, "i int;s string", "", "text", null);
        hms.call("createView", "hvview", "v_orc", "SELECT `o`.`i`, `o`.`s`, `o`.`dec`"
                + " FROM `hvview`.`o` WHERE `o`.`i` < 100", "i int;s string;dec decimal(12,2)");
        hms.call("createView", "hvview", "v_pq", "SELECT `o_pq`.`i`, `o_pq`.`s`, `o_pq`.`dec`"
                + " FROM `hvview`.`o_pq` WHERE `o_pq`.`i` < 100", "i int;s string;dec decimal(12,2)");
        hms.call("createView", "hvview", "v_txt", "SELECT `t`.`i`, `t`.`s` FROM `hvview`.`t`"
                + " WHERE `t`.`i` < 100", "i int;s string");
        hms.call("createView", "hvview", "v_join", "SELECT `a`.`i`, `b`.`s` FROM `hvview`.`o` `a`"
                + " JOIN `hvview`.`t` `b` ON `a`.`i` = `b`.`i` WHERE `a`.`dec` > 100",
                "i int;s string");
        hms.call("createView", "hvview", "v_count", "SELECT count(*) AS `c` FROM `hvview`.`v_orc`",
                "c bigint");
        Path plugins = dir.resolve("plugins");
        HiveTestTables.installPlugin(plugins);
        proxy = hms.proxy();
        Map<String, String> sys = new HashMap<>();
        sys.put("arrowflight.hive.uris", HiveTestTables.uri(proxy));
        sys.put("arrowflight.hive.roots", lake.toString());
        sys.put("arrowflight.hive.orcTables", "true");
        sys.put("arrowflight.hive.textTables", "true");
        sys.put("arrowflight.hive.views", "true");
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
    void viewsAnswerLikeTheirQueriesInBothModes() throws Exception {
        List<String> reference = answer("SELECT i, s, dec FROM hvview.o_pq WHERE i < 100",
                "complete").rows();
        assertEquals(100, reference.size());
        List<String> pairs = reference.stream()
                .map(row -> row.substring(0, row.lastIndexOf('|'))).sorted().toList();
        List<String> joined = answer("SELECT a.i, b.s FROM hvview.o_pq a JOIN hvview.o_pq b"
                + " ON a.i = b.i WHERE a.dec > 100", "complete").rows();
        // i 81..999 (dec = i * 1.25 > 100) less the 131 with i % 7 = 3 (dec NULL).
        assertEquals(788, joined.size());
        for (String mode : List.of("complete", "partial")) {
            assertEquals(reference, answer("SELECT * FROM hvview.v_orc", mode).rows(), mode);
            assertEquals(reference, answer("SELECT * FROM hvview.v_pq", mode).rows(), mode);
            assertEquals(pairs, answer("SELECT * FROM hvview.v_txt", mode).rows(), mode);
            assertEquals(joined, answer("SELECT i, s FROM hvview.v_join", mode).rows(), mode);
            assertEquals(List.of("100"), answer("SELECT c FROM hvview.v_count", mode).rows(), mode);
            assertEquals(List.of("s5ü"), answer("SELECT s FROM hvview.v_orc WHERE i = 5", mode)
                    .rows(), mode);
            assertEquals(List.of("100"), answer("SELECT count(*) FROM hvview.v_txt v"
                    + " JOIN hvview.o o ON v.i = o.i", mode).rows(), mode);
        }
        assertEquals(1, info("SELECT * FROM hvview.v_orc", "partial").getEndpoints().size(),
                "a view is answered completely");
    }

    @Test
    void viewsHaveTheirDeclaredColumnTypes() throws Exception {
        Schema schema = answer("SELECT * FROM hvview.v_orc", "complete").schema();
        assertEquals(List.of("i", "s", "dec"), schema.getFields().stream().map(Field::getName)
                .toList());
        assertEquals(List.of(new ArrowType.Int(32, true), ArrowType.Utf8.INSTANCE,
                new ArrowType.Decimal(12, 2, 128)), schema.getFields().stream()
                        .map(Field::getType).toList());
        assertEquals(new ArrowType.Int(64, true), answer("SELECT * FROM hvview.v_count",
                "complete").schema().getFields().get(0).getType());
    }

    @Test
    void viewsAreListedAsViews() throws Exception {
        List<String> all = column(helper.sqlClient().getTables(null, "hvview", null, null, false),
                "table_name");
        assertTrue(all.containsAll(List.of("o", "o_pq", "t")), all.toString());
        assertTrue(all.containsAll(VIEWS), all.toString());
        List<String> views = column(helper.sqlClient().getTables(null, "hvview", null,
                List.of("VIEW"), false), "table_name");
        views.sort(null);
        assertEquals(VIEWS, views);
        List<String> tables = column(helper.sqlClient().getTables(null, "hvview", null,
                List.of("TABLE"), false), "table_name");
        tables.sort(null);
        assertEquals(List.of("o", "o_pq", "t"), tables);
        assertTrue(column(helper.sqlClient().getTableTypes(), "table_type").contains("VIEW"));
    }

    @Test
    void aViewAtAnEarlierSnapshotIsRefused() {
        FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                () -> info("SELECT * FROM hvview.v_orc FOR VERSION AS OF 1", "complete"));
        assertEquals(FlightStatusCode.INVALID_ARGUMENT, e.status().code());
        assertTrue(e.getMessage().contains("view"), e.getMessage());
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

    private static Answer answer(String sql, String mode) throws Exception {
        FlightInfo info = info(sql, mode);
        Schema announced = info.getSchemaOptional().orElseThrow();
        List<String> rows = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                assertEquals(announced, stream.getSchema(), mode + " stream vs FlightInfo: " + sql);
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
        return new Answer(announced, rows);
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
