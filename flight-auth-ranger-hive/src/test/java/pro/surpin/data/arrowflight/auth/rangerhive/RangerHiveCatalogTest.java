package pro.surpin.data.arrowflight.auth.rangerhive;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemRowFilterInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerRowFilterPolicyItem;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.TestFlightServerHelper;
import pro.surpin.data.arrowflight.server.catalog.HiveTestTables;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;
import pro.surpin.data.arrowflight.server.plugin.PluginSettings;
import pro.surpin.data.arrowflight.server.storage.StoreRegistry;

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
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the {@code ranger-hive} provider in a real Flight SQL server over the packaged hive plugin and a real
 * metastore: an ORC table (Arrow stream reads), a Parquet table, a text table and Hive views over them. A column
 * denial or a row filter refuses the table and every view that reads it (invoker rights), a mask is read in the
 * table and in its views, in both result modes; a view granted without its source table is refused; listings show
 * what the caller has any access to.
 */
@Tag("integration")
class RangerHiveCatalogTest {
    private static final String PASSWORD = "test-password";
    private static final String DB = "hvranger";
    private static final List<String> ALL = List.of("o", "o_pq", "t", "v_join", "v_orc", "v_pq");
    /** Rows of each table and view for an unrestricted reader. */
    private static final Map<String, Integer> ROWS = Map.of("o", 1000, "o_pq", 1000, "t", 1000,
            "v_orc", 100, "v_pq", 100, "v_join", 788);

    @TempDir
    static Path dir;
    private static AutoCloseable proxy;
    private static TestFlightServerHelper helper;

    /**
     * Creates the metastore tables and views and starts the server with the hive plugin and in-memory policies.
     *
     * @throws Exception when the tables, the plugin or the policies cannot be prepared
     */
    @BeforeAll
    static void start() throws Exception {
        Assumptions.assumeTrue(HiveTestTables.available(), "flight-plugin-hive is not built");
        HiveTestTables.Hms hms = HiveTestTables.hms();
        Path lake = Files.createDirectories(dir.resolve("lake"));
        String columns = HiveTestTables.orcConstant("COLUMNS");
        String expected = HiveTestTables.orcConstant("EXPECTED");
        hms.call("createDatabase", DB);
        Path orc = Files.createDirectories(lake.resolve("o"));
        HiveTestTables.writeOrc(orc.resolve("000000_0"), 0, 1000, "ZLIB");
        hms.call("createTable", DB, "o", "file:" + orc, columns, "", "orc", null);
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
        hms.call("createTable", DB, "o_pq", "file:" + parquet, columns, "", "parquet", null);
        hms.call("createTable", DB, "t", "file:" + text, "i int;s string", "", "text", null);
        hms.call("createView", DB, "v_orc", "SELECT `o`.`i`, `o`.`s`, `o`.`dec` FROM `" + DB + "`.`o`"
                + " WHERE `o`.`i` < 100", "i int;s string;dec decimal(12,2)");
        hms.call("createView", DB, "v_pq", "SELECT `o_pq`.`i`, `o_pq`.`s` FROM `" + DB + "`.`o_pq`"
                + " WHERE `o_pq`.`i` < 100", "i int;s string");
        hms.call("createView", DB, "v_join", "SELECT `a`.`i`, `b`.`s` FROM `" + DB + "`.`o` `a`"
                + " JOIN `" + DB + "`.`t` `b` ON `a`.`i` = `b`.`i` WHERE `a`.`dec` > 100", "i int;s string");
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
        UserGroupInformation.setConfiguration(new Configuration());
        HiveRangerPlugin plugin = new HiveRangerPlugin("hive", "ranger-hive-catalog-test");
        plugin.setPolicies(policies());
        Map<String, Set<String>> groups = Map.of("tess", Set.of("staff"), "sam", Set.of("staff"),
                "carl", Set.of("staff", "contractors"));
        RangerHiveAuthorization authorization = new RangerHiveAuthorization(
                new HivePolicies(plugin, identity -> groups.getOrDefault(identity, Set.of())), "", () -> { });
        Files.createDirectories(dir.resolve("data"));
        helper = TestFlightServerHelper.builder().dataDir(dir.resolve("data").toString()).threads(2)
                .registry(config -> StoreRegistry.open(config, new Configuration(), "localhost",
                        settings, source, List.of()))
                .authorization(authorization).password(PASSWORD).start();
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

    /** Without restrictions every table and view of the database answers in full, in both modes. */
    @Test
    void unrestrictedReaderGetsEverything() throws Exception {
        for (String mode : List.of("partial", "complete")) {
            for (String name : ALL) {
                assertEquals(ROWS.get(name), rows("SELECT * FROM " + DB + "." + name, "tess", mode).size(),
                        mode + " " + name);
            }
            assertEquals(List.of("1000|"), rows("SELECT count(*) FROM " + DB + ".o", "tess", mode), mode);
        }
    }

    /**
     * A column denial on the ORC table (carl) and a row filter on the text table (sam) refuse those tables and the
     * views over them, also in joins; the other tables and views still answer (sam's masked ones masked).
     */
    @Test
    void restrictionsRefuseTablesAndTheirViews() throws Exception {
        Map<String, List<String>> refused = Map.of(
                "carl", List.of("o", "v_orc", "v_join"),
                "sam", List.of("t", "v_join"));
        for (String mode : List.of("partial", "complete")) {
            for (Map.Entry<String, List<String>> entry : refused.entrySet()) {
                String user = entry.getKey();
                for (String name : ALL) {
                    String table = DB + "." + name;
                    List<String> queries = List.of("SELECT * FROM " + table,
                            "SELECT count(*) FROM " + table,
                            "SELECT x.i FROM " + table + " x JOIN " + DB + ".o_pq p ON x.i = p.i",
                            "SELECT * FROM " + DB + ".t WHERE i IN (SELECT i FROM " + table + ")");
                    for (String sql : entry.getValue().contains(name) ? queries : queries.subList(0, 2)) {
                        String label = mode + " " + user + ": " + sql;
                        if (entry.getValue().contains(name)) {
                            FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                                    () -> rows(sql, user, mode), label);
                            assertEquals(FlightStatusCode.UNAUTHORIZED, e.status().code(), label);
                        } else {
                            assertFalse(rows(sql, user, mode).isEmpty(), label);
                        }
                    }
                }
            }
        }
        assertEquals(ROWS.get("v_orc"), rows("SELECT * FROM " + DB + ".v_orc", "sam", "complete").size());
    }

    /** Sam's mask on o_pq.s applies to the table and to the view over it, in both modes. */
    @Test
    void masksApplyToTablesAndViews() throws Exception {
        for (String mode : List.of("partial", "complete")) {
            List<String> table = rows("SELECT i, s FROM " + DB + ".o_pq", "sam", mode);
            assertEquals(ROWS.get("o_pq"), table.size(), mode);
            assertTrue(table.stream().allMatch(row -> row.endsWith("|null|")), mode + ": " + table.get(0));
            assertEquals(List.of("0|"), rows("SELECT count(s) FROM " + DB + ".v_pq", "sam", mode), mode);
            assertEquals(List.of(ROWS.get("v_pq") + "|"), rows("SELECT count(*) FROM " + DB + ".v_pq", "sam", mode),
                    mode);
            assertFalse(rows("SELECT count(s) FROM " + DB + ".v_pq", "tess", mode).equals(List.of("0|")), mode);
        }
    }

    /** A view granted without its source table is refused: the server checks views with invoker rights. */
    @Test
    void aViewWithoutItsSourceIsRefused() {
        for (String mode : List.of("partial", "complete")) {
            FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                    () -> rows("SELECT * FROM " + DB + ".v_pq", "vera", mode), mode);
            assertEquals(FlightStatusCode.UNAUTHORIZED, e.status().code(), mode);
        }
    }

    /**
     * Listings show the tables and views a caller has any access to (views with their sources), as Hive's SHOW
     * TABLES, and the database to anyone with a policy in it.
     */
    @Test
    void listingsShowVisibleTables() throws Exception {
        assertEquals(ALL, tables("tess"));
        assertEquals(ALL, tables("carl"));
        assertEquals(ALL, tables("sam"));
        assertEquals(List.of(), tables("vera"));
        assertEquals(List.of(), tables("mallory"));
        assertTrue(schemas("vera").contains(DB));
        assertFalse(schemas("mallory").contains(DB));
    }

    /**
     * Policies of the test Hive service: staff read the database; contractors are denied {@code o.dec}; sam has a
     * mask on {@code o_pq.s} and a row filter on {@code t}; vera may read view {@code v_pq} only.
     *
     * @return service policies
     * @throws Exception when Ranger's Hive service definition cannot be read
     */
    private static ServicePolicies policies() throws Exception {
        RangerPolicy access = RangerHiveFlightSqlTest.policy(1, RangerPolicy.POLICY_TYPE_ACCESS,
                RangerHiveFlightSqlTest.resources(DB, "*", "*"));
        access.setPolicyItems(List.of(new RangerPolicyItem(RangerHiveFlightSqlTest.select(), List.of(),
                List.of("staff"), List.of(), List.of(), false)));
        RangerPolicy deny = RangerHiveFlightSqlTest.policy(2, RangerPolicy.POLICY_TYPE_ACCESS,
                RangerHiveFlightSqlTest.resources(DB, "o", "dec"));
        deny.setDenyPolicyItems(List.of(new RangerPolicyItem(RangerHiveFlightSqlTest.select(), List.of(),
                List.of("contractors"), List.of(), List.of(), false)));
        RangerPolicy mask = RangerHiveFlightSqlTest.policy(3, RangerPolicy.POLICY_TYPE_DATAMASK,
                RangerHiveFlightSqlTest.resources(DB, "o_pq", "s"));
        mask.setDataMaskPolicyItems(List.of(new RangerDataMaskPolicyItem(RangerHiveFlightSqlTest.select(),
                new RangerPolicyItemDataMaskInfo("MASK_NULL", null, null), List.of("sam"), List.of(), List.of(),
                List.of(), false)));
        RangerPolicy filter = RangerHiveFlightSqlTest.policy(4, RangerPolicy.POLICY_TYPE_ROWFILTER,
                RangerHiveFlightSqlTest.resources(DB, "t", null));
        filter.setRowFilterPolicyItems(List.of(new RangerRowFilterPolicyItem(
                new RangerPolicyItemRowFilterInfo("i < 10"), RangerHiveFlightSqlTest.select(), List.of("sam"),
                List.of(), List.of(), List.of(), false)));
        RangerPolicy view = RangerHiveFlightSqlTest.policy(5, RangerPolicy.POLICY_TYPE_ACCESS,
                RangerHiveFlightSqlTest.resources(DB, "v_pq", "*"));
        view.setPolicyItems(List.of(new RangerPolicyItem(RangerHiveFlightSqlTest.select(), List.of("vera"),
                List.of(), List.of(), List.of(), false)));
        return RangerHiveFlightSqlTest.servicePolicies(List.of(access, deny, mask, filter, view));
    }

    /**
     * Lists the tables of the test database for a user, sorted.
     *
     * @param user user name
     * @return table and view names
     * @throws Exception on failure
     */
    private static List<String> tables(String user) throws Exception {
        CallOption[] options = login(user, "complete");
        List<String> names = column(helper.sqlClient().getTables(null, DB, null, null, false, options),
                "table_name", options);
        names.sort(null);
        return names;
    }

    /**
     * Lists the schemas a user sees.
     *
     * @param user user name
     * @return schema names
     * @throws Exception on failure
     */
    private static List<String> schemas(String user) throws Exception {
        CallOption[] options = login(user, "complete");
        return column(helper.sqlClient().getSchemas(null, null, options), "db_schema_name", options);
    }

    /**
     * Logs a user in and returns the call options for its calls in a result mode.
     *
     * @param user user name
     * @param mode result mode
     * @return bearer token and result-mode header
     */
    private static CallOption[] login(String user, String mode) {
        CallOption token = helper.flightClient().authenticateBasicToken(user, PASSWORD)
                .orElseThrow(() -> new AssertionError("no token for " + user));
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert("x-flight-result-mode", mode);
        return new CallOption[] {token, new HeaderCallOption(headers)};
    }

    /**
     * Runs a query as a user and reads all endpoints.
     *
     * @param sql query
     * @param user user name
     * @param mode result mode
     * @return rows as text
     * @throws Exception on failure
     */
    private static List<String> rows(String sql, String user, String mode) throws Exception {
        CallOption[] options = login(user, mode);
        FlightInfo info = helper.sqlClient().execute(sql, options);
        List<String> rows = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.sqlClient().getStream(endpoint.getTicket(), options)) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    for (int r = 0; r < root.getRowCount(); r++) {
                        StringBuilder row = new StringBuilder();
                        for (var vector : root.getFieldVectors()) {
                            row.append(vector.getObject(r)).append('|');
                        }
                        rows.add(row.toString());
                    }
                }
            }
        }
        return rows;
    }

    /**
     * Reads one text column of a metadata answer.
     *
     * @param info metadata flight info
     * @param name column name
     * @param options caller's token
     * @return values
     * @throws Exception on failure
     */
    private static List<String> column(FlightInfo info, String name, CallOption[] options) throws Exception {
        List<String> values = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.sqlClient().getStream(endpoint.getTicket(), options)) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    for (int r = 0; r < root.getRowCount(); r++) {
                        values.add(String.valueOf(root.getVector(name).getObject(r)));
                    }
                }
            }
        }
        return values;
    }
}
