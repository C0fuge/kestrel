package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
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

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.server.TestFlightServerHelper;
import pro.surpin.data.arrowflight.server.adapters.ResultModeMiddleware;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;
import pro.surpin.data.arrowflight.server.plugin.PluginSettings;
import pro.surpin.data.arrowflight.server.storage.StoreRegistry;

/**
 * Runs the server with the hive plugin against a real metastore (Hive 3.1.3 on embedded Derby,
 * in this JVM, reached through a TCP proxy) over Parquet tables in a local lake, next to a
 * Parquet directory table, under the DuckDB lockdown: unpartitioned and partitioned tables
 * (k=v directories, a custom location, special characters, NULL, typed int/date partitions),
 * all types, hidden tables outside the roots, skipped formats. Complete answers are compared
 * with DuckDB reading the same files with {@code hive_partitioning}; partial answers with
 * complete ones; partitions are pinned per plan; a metastore outage is an error. With
 * {@code -Diceberg.it.extensionDir} and the iceberg plugin built, an Iceberg table registered in
 * the metastore is read through the iceberg plugin too.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class HiveCatalogIntegrationTest {

    private static final String SPECIAL = "a b/c:d=%'é";

    @TempDir
    static Path dir;
    static Path lake;
    static HiveTestTables.Hms hms;
    static AutoCloseable proxy;
    static TestFlightServerHelper helper;
    static String icebergMetadata;
    static String icebergWarehouse;
    static String extensionDir;

    @BeforeAll
    static void start() throws Exception {
        Assumptions.assumeTrue(HiveTestTables.available(), "flight-plugin-hive is not built");
        hms = HiveTestTables.hms();
        lake = Files.createDirectories(dir.resolve("lake"));
        hms.call("createDatabase", "hv");
        // Unpartitioned, Hive-style file names (no extension), two files.
        Path nation = Files.createDirectories(lake.resolve("nation"));
        parquet(nation.resolve("000000_0"), "SELECT range::INT AS n_nationkey, 'nation ' || range AS"
                + " n_name, (range % 5)::INT AS n_regionkey FROM range(0, 15)");
        parquet(nation.resolve("000001_0"), "SELECT range::INT AS n_nationkey, 'nation ' || range AS"
                + " n_name, (range % 5)::INT AS n_regionkey FROM range(15, 25)");
        hms.call("createTable", "hv", "nation", "file:" + nation, "n_nationkey int;n_name string;"
                + "n_regionkey int", "", "parquet", null);
        // Partitioned by l_shipmode: k=v directories, a custom location, special characters, NULL.
        Path lineitem = Files.createDirectories(lake.resolve("lineitem_p"));
        hms.call("createTable", "hv", "lineitem_p", "file:" + lineitem, "l_orderkey bigint;"
                + "l_quantity decimal(12,2);l_shipdate date", "l_shipmode string", "parquet", null);
        int seed = 0;
        for (String mode : List.of("AIR", "MAIL", "SHIP", SPECIAL, "__HIVE_DEFAULT_PARTITION__")) {
            String location = (String) hms.call("addPartition", "hv", "lineitem_p", List.of(mode), null);
            parquet(Path.of(location.substring("file:".length())).resolve("part-0.parquet"),
                    lineitemRows(seed++));
        }
        Path truck = Files.createDirectories(lake.resolve("custom/truck-files"));
        hms.call("addPartition", "hv", "lineitem_p", List.of("TRUCK"), "file:" + truck);
        parquet(truck.resolve("000000_0"), lineitemRows(seed++));
        parquet(truck.resolve("000001_0"), lineitemRows(seed));
        // Two typed partition columns.
        Path multi = Files.createDirectories(lake.resolve("multi"));
        hms.call("createTable", "hv", "multi", "file:" + multi, "v double", "yr int;dt date",
                "parquet", null);
        for (String[] p : new String[][] {{"2023", "2023-12-31"}, {"2024", "2024-01-01"},
                {"2024", "2024-02-01"}}) {
            String location = (String) hms.call("addPartition", "hv", "multi", List.of(p[0], p[1]), null);
            parquet(Path.of(location.substring("file:".length())).resolve("f.parquet"),
                    "SELECT (range * 1.5 + " + p[0] + ")::DOUBLE AS v FROM range(4)");
        }
        // All types.
        Path types = Files.createDirectories(lake.resolve("types"));
        parquet(types.resolve("f.parquet"), "SELECT i::TINYINT AS ti, i::SMALLINT AS si, i AS ii,"
                + " i::BIGINT * 1000000000 AS bi, i % 2 = 0 AS b, i / 4 AS f, i / 8 AS d,"
                + " (i * 1.25)::DECIMAL(12,2) AS dec, (i * 1.000001)::DECIMAL(38,6) AS big,"
                + " 's' || i AS s, 'v' || i AS vc, 'c' || i AS ch, DATE '2024-01-01' + i AS dt,"
                + " TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (i) HOUR AS ts, ('b' || i)::BLOB AS bin,"
                + " [i, i + 1] AS arr, MAP {'k' || i: i} AS m, {'a': i, 'b': 'z' || i} AS st"
                + " FROM (SELECT range::INT AS i FROM range(1, 6))");
        hms.call("createTable", "hv", "types", "file:" + types, "ti tinyint;si smallint;ii int;"
                + "bi bigint;b boolean;f float;d double;dec decimal(12,2);big decimal(38,6);s string;"
                + "vc varchar(10);ch char(4);dt date;ts timestamp;bin binary;arr array<int>;"
                + "m map<string,int>;st struct<a:int,b:string>", "", "parquet", null);
        // An empty partitioned table, hidden and skipped tables.
        Path empty = Files.createDirectories(lake.resolve("empty"));
        hms.call("createTable", "hv", "empty", "file:" + empty, "a int", "k string", "parquet", null);
        Path outside = Files.createDirectories(dir.resolve("outside/secret"));
        parquet(outside.resolve("s.parquet"), "SELECT 42 AS a");
        hms.call("createTable", "hv", "secret", "file:" + outside, "a int", "", "parquet", null);
        Path half = Files.createDirectories(lake.resolve("half"));
        hms.call("createTable", "hv", "half", "file:" + half, "a int", "k string", "parquet", null);
        hms.call("addPartition", "hv", "half", List.of("out"), "file:" + outside);
        hms.call("createTable", "hv", "csv", "file:" + lake.resolve("csv"), "a int", "", "text", null);
        hms.call("createView", "hv", "v", "SELECT 1");
        // Iceberg in the metastore, when the iceberg plugin and DuckDB's extensions are there.
        extensionDir = IcebergTestTables.extensionDir();
        boolean iceberg = extensionDir != null && IcebergTestTables.available();
        if (iceberg) {
            icebergWarehouse = lake.resolve("icewh").toString();
            Map<String, String> written = IcebergTestTables.write(icebergWarehouse);
            icebergMetadata = written.get("evolved");
            hms.call("registerIceberg", "hv", "ice_evolved", "file:" + icebergWarehouse
                    + "/ice/evolved", icebergMetadata);
        }
        Path data = Files.createDirectories(dir.resolve("data/dirs/t"));
        parquet(data.resolve("part-0.parquet"), "SELECT range AS id, 'r' || range AS name FROM range(3)");
        Path plugins = dir.resolve("plugins");
        HiveTestTables.installPlugin(plugins);
        if (iceberg) {
            IcebergTestTables.installPlugin(plugins);
        }
        proxy = hms.proxy();
        Map<String, String> sys = new HashMap<>();
        sys.put("arrowflight.hive.uris", HiveTestTables.uri(proxy));
        sys.put("arrowflight.hive.roots", lake.toString());
        sys.put("arrowflight.hive.metadataTtlSeconds", "0");
        sys.put("arrowflight.hive.namesTtlSeconds", "0");
        sys.put("arrowflight.hive.connectTimeoutMillis", "2000");
        sys.put("arrowflight.hive.socketTimeoutMillis", "10000");
        if (iceberg) {
            sys.put("arrowflight.duckdb.extensionDir", extensionDir);
        }
        ConfigSource source = new ConfigSource(sys::get, new Properties(), Map.of(), null);
        PluginSettings settings = new PluginSettings(plugins, null, "directory,hive", false);
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
    @Order(1)
    void databasesAndTablesAppearNextToTheDataDirectory() throws Exception {
        List<String> schemas = column(helper.sqlClient().getSchemas(null, null), "db_schema_name");
        assertTrue(schemas.containsAll(List.of("dirs", "hv", "default")), schemas.toString());
        List<String> tables = column(helper.sqlClient().getTables(null, "hv", null, null, false),
                "table_name");
        List<String> expected = new ArrayList<>(List.of("empty", "lineitem_p", "multi", "nation",
                "types"));
        if (icebergMetadata != null) {
            expected.add("ice_evolved");
        }
        assertTrue(tables.containsAll(expected), tables.toString());
        for (String hidden : List.of("secret", "csv", "v")) {
            assertFalse(tables.contains(hidden), hidden + " in " + tables);
        }
        // half: hidden once its partition outside the roots is seen.
        assertThrows(FlightRuntimeException.class, () -> answer("SELECT * FROM hv.half", "complete"));
        assertFalse(column(helper.sqlClient().getTables(null, "hv", null, null, false), "table_name")
                .contains("half"));
        List<String> fields = new ArrayList<>();
        for (Field field : tableSchema("hv", "lineitem_p").getFields()) {
            fields.add(field.getName());
        }
        assertEquals(List.of("l_orderkey", "l_quantity", "l_shipdate", "l_shipmode"), fields);
        assertEquals(18, tableSchema("hv", "types").getFields().size());
    }

    @Test
    @Order(2)
    void completeAnswersEqualDuckDbReadingTheFilesWithHivePartitioning() throws Exception {
        assertEquals(direct("read_parquet('" + lake + "/nation/*')"), fingerprint("hv.nation"));
        // DuckDB reads the k=v directories itself (it decodes Hive's escapes, but reads
        // __HIVE_DEFAULT_PARTITION__ as text, where Hive means NULL); the custom location has no
        // k=v path.
        String lineitemDirect = "(SELECT * REPLACE (nullif(l_shipmode, '__HIVE_DEFAULT_PARTITION__')"
                + " AS l_shipmode) FROM read_parquet('" + lake + "/lineitem_p/*/*',"
                + " hive_partitioning = true, hive_types = {'l_shipmode': VARCHAR})"
                + " UNION ALL SELECT *, 'TRUCK' AS l_shipmode FROM read_parquet('" + lake
                + "/custom/truck-files/*'))";
        assertEquals(direct(lineitemDirect), fingerprint("hv.lineitem_p"));
        // DuckDB orders its hive columns its own way: name them in the table's order.
        assertEquals(direct("(SELECT v, yr, dt FROM read_parquet('" + lake + "/multi/*/*/*',"
                + " hive_partitioning = true, hive_types = {'yr': INTEGER, 'dt': DATE}))"),
                fingerprint("hv.multi"));
        assertEquals(direct("read_parquet('" + lake + "/types/*')"), fingerprint("hv.types"));
        // The partition value comes from the metastore, not from the escaped directory name.
        assertEquals(List.of(SPECIAL + "|10"), answer("SELECT l_shipmode, count(*) AS n FROM"
                + " hv.lineitem_p WHERE l_shipmode LIKE 'a b%' GROUP BY l_shipmode", "complete").rows());
        assertEquals(List.of("null|10"), answer("SELECT l_shipmode, count(*) AS n FROM hv.lineitem_p"
                + " WHERE l_shipmode IS NULL GROUP BY l_shipmode", "complete").rows());
        assertEquals(List.of("20"), answer("SELECT count(*) FROM hv.lineitem_p WHERE l_shipmode ="
                + " 'TRUCK'", "complete").rows());
        assertEquals(List.of("0"), answer("SELECT count(*) FROM hv.empty", "complete").rows());
        assertEquals(List.of("0|r0|0|nation 0"), answer("SELECT d.id, d.name, n.n_nationkey,"
                + " n.n_name FROM dirs.t d JOIN hv.nation n ON n.n_nationkey = d.id WHERE d.id = 0",
                "complete").rows());
    }

    @Test
    @Order(3)
    void partialAnswersEqualCompleteOnesAndPrunePartitions() throws Exception {
        for (String sql : List.of(
                "SELECT * FROM hv.nation",
                "SELECT * FROM hv.lineitem_p",
                "SELECT l_orderkey, l_shipmode FROM hv.lineitem_p WHERE l_shipmode = 'AIR'",
                "SELECT l_shipmode, count(*) AS n, sum(l_quantity) AS q FROM hv.lineitem_p"
                        + " GROUP BY l_shipmode",
                "SELECT count(*) AS n FROM hv.lineitem_p WHERE l_shipmode IN ('MAIL', 'TRUCK')"
                        + " AND l_quantity > 3",
                "SELECT count(*) AS n FROM hv.lineitem_p WHERE l_shipmode = 'NONE'",
                "SELECT yr, dt, sum(v) AS s FROM hv.multi WHERE yr = 2024 AND dt >= DATE '2024-01-15'"
                        + " GROUP BY yr, dt",
                "SELECT * FROM hv.types",
                "SELECT count(*) AS n FROM hv.empty")) {
            Answer partial = answer(sql, "partial");
            Answer complete = answer(sql, "complete");
            assertEquals(complete.schema(), partial.schema(), sql);
            assertEquals(complete.rows(), partial.rows(), sql);
        }
        FlightInfo info = info("SELECT * FROM hv.lineitem_p WHERE l_shipmode = 'AIR'", "partial");
        assertEquals(1, info.getEndpoints().size());
        assertFalse(info.getEndpoints().get(0).getLocations().isEmpty(), "a partial endpoint");
        assertEquals(List.of("10"), answer("SELECT count(*) FROM hv.lineitem_p WHERE l_shipmode ="
                + " 'AIR'", "partial").rows());
        assertEquals(List.of("0"), answer("SELECT count(*) FROM hv.lineitem_p WHERE l_shipmode ="
                + " 'NONE'", "partial").rows());
    }

    @Test
    @Order(4)
    void hiddenTablesAndForeignFilesAreRefused() throws Exception {
        assertThrows(FlightRuntimeException.class, () -> answer("SELECT * FROM hv.secret", "complete"));
        assertThrows(FlightRuntimeException.class, () -> answer("SELECT * FROM hv.secret", "partial"));
        FlightRuntimeException lockdown = assertThrows(FlightRuntimeException.class,
                () -> answer("SELECT * FROM read_parquet('" + dir.resolve("outside/secret") + "/*')",
                        "complete"));
        assertTrue(String.valueOf(lockdown.getMessage()).contains("ermission")
                || String.valueOf(lockdown.getMessage()).contains("disabled"), lockdown.getMessage());
        String snapshotId = helper.parquetAdapter.catalogTables().current("hv", "nation")
                .orElseThrow().snapshot().snapshotId();
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);
        String outsideKey = new CatalogFileKey(snapshotId, dir.resolve("outside/secret/s.parquet")
                .toString(), Map.of()).encode();
        Exception outside = assertThrows(IllegalArgumentException.class,
                () -> helper.executionService.readParquet(helper.allocator, "SELECT * FROM hv.nation",
                        new String[] {outsideKey}, listener, true));
        assertTrue(outside.getMessage().contains("outside the roots"), outside.getMessage());
        String staleKey = new CatalogFileKey("h0000000000000000", lake + "/nation/000000_0", Map.of())
                .encode();
        Exception stale = assertThrows(Exception.class,
                () -> helper.executionService.readParquet(helper.allocator, "SELECT * FROM hv.nation",
                        new String[] {staleKey}, listener, true));
        assertTrue(stale.getMessage().contains("plan the query again"), stale.getMessage());
    }

    @Test
    @Order(5)
    void icebergTablesOfTheMetastoreGoThroughTheIcebergPlugin() throws Exception {
        Assumptions.assumeTrue(icebergMetadata != null, "needs -Diceberg.it.extensionDir and the"
                + " iceberg plugin");
        assertEquals(directIceberg(icebergMetadata), fingerprint("hv.ice_evolved"));
        String sql = "SELECT count(*) AS n, sum(amount) AS s FROM hv.ice_evolved";
        assertEquals(answer(sql, "complete").rows(), answer(sql, "partial").rows());
        FlightInfo before = info("SELECT id, label FROM hv.ice_evolved", "partial");
        int rows = read(before).size();
        String next = IcebergTestTables.appendToEvolved(icebergWarehouse, 100);
        hms.call("commitIceberg", "hv", "ice_evolved", next);
        assertEquals(rows, read(before).size(), "the plan made before the commit is pinned");
        assertEquals(rows + 2, read(info("SELECT id, label FROM hv.ice_evolved", "partial")).size());
    }

    @Test
    @Order(6)
    void partitionsArePinnedPerPlan() throws Exception {
        String sql = "SELECT l_shipmode, count(*) AS n FROM hv.lineitem_p GROUP BY l_shipmode";
        FlightInfo before = info(sql, "partial");
        List<String> rowsBefore = read(before);
        Path rail = Files.createDirectories(lake.resolve("custom/rail"));
        parquet(rail.resolve("r.parquet"), lineitemRows(99));
        hms.call("addPartition", "hv", "lineitem_p", List.of("RAIL"), "file:" + rail);
        hms.call("dropPartition", "hv", "lineitem_p", List.of("SHIP"));
        assertEquals(rowsBefore, read(before), "the plan keeps the partitions it was made with");
        List<String> after = read(info(sql, "partial"));
        assertTrue(after.contains("RAIL|10"), after.toString());
        assertFalse(after.contains("SHIP|10"), after.toString());
    }

    @Test
    @Order(100)
    void aMetastoreOutageIsAnErrorNotAnEmptyAnswer() throws Exception {
        proxy.close();
        FlightRuntimeException tables = assertThrows(FlightRuntimeException.class,
                () -> column(helper.sqlClient().getTables(null, "hv", null, null, false), "table_name"));
        assertEquals(CallStatus.UNAVAILABLE.code(), tables.status().code(), tables.getMessage());
        assertTrue(tables.getMessage().contains("Hive Metastore unavailable"), tables.getMessage());
        FlightRuntimeException partial = assertThrows(FlightRuntimeException.class,
                () -> answer("SELECT count(*) FROM hv.nation", "partial"));
        assertTrue(partial.getMessage().contains("Hive Metastore"), partial.getMessage());
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert(ResultModeMiddleware.HEADER, "partial");
        FlightRuntimeException schema = assertThrows(FlightRuntimeException.class,
                () -> helper.sqlClient().getExecuteSchema("SELECT n_name FROM hv.nation",
                        new HeaderCallOption(headers)));
        assertEquals(CallStatus.UNAVAILABLE.code(), schema.status().code(), schema.getMessage());
        assertTrue(schema.getMessage().contains("Hive Metastore"), schema.getMessage());
    }

    // ── helpers ──────────────────────────────────────────────────────────

    /**
     * An answer: announced schema, rows as text and number of endpoints.
     *
     * @param schema announced schema
     * @param rows rows, columns separated by |
     * @param endpoints endpoints read
     */
    private record Answer(Schema schema, List<String> rows, int endpoints) {
    }

    private static String lineitemRows(int seed) {
        return "SELECT (range + " + (seed * 100) + ")::BIGINT AS l_orderkey,"
                + " ((range % 7) + 0.25)::DECIMAL(12,2) AS l_quantity,"
                + " DATE '1995-01-01' + (range + " + seed + ")::INT AS l_shipdate FROM range(10)";
    }

    private static void parquet(Path file, String select) throws Exception {
        Files.createDirectories(file.getParent());
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (" + select + ") TO '" + file.toString().replace("'", "''")
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

    private static String directIceberg(String metadataLocation) throws Exception {
        Path ext = Path.of(extensionDir, "v1.5.6", "linux_amd64");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("LOAD '" + ext.resolve("avro.duckdb_extension") + "'");
            s.execute("LOAD '" + ext.resolve("iceberg.duckdb_extension") + "'");
            try (ResultSet rs = s.executeQuery(fingerprintSql("iceberg_scan('" + metadataLocation
                    + "')"))) {
                rs.next();
                return rs.getLong(1) + "|" + rs.getString(2);
            }
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
        return new Answer(announced, rows, info.getEndpoints().size());
    }

    private static List<String> read(FlightInfo info) throws Exception {
        List<String> rows = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    rows.addAll(rows(stream.getRoot()));
                }
            }
        }
        rows.sort(null);
        return rows;
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
                        values.add(new String(vector.get(i), java.nio.charset.StandardCharsets.UTF_8));
                    }
                }
            }
        }
        return values;
    }

    private static Schema tableSchema(String schema, String table) throws Exception {
        FlightInfo info = helper.sqlClient().getTables(null, schema, table, null, true);
        try (FlightStream stream = helper.flightClient().getStream(info.getEndpoints().get(0).getTicket())) {
            assertTrue(stream.next());
            VarBinaryVector schemas = (VarBinaryVector) stream.getRoot().getVector("table_schema");
            return Schema.deserializeMessage(ByteBuffer.wrap(schemas.get(0)));
        }
    }
}
