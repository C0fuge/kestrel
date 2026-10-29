package pro.surpin.data.arrowflight.auth.rangerhive;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemRowFilterInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicy.RangerRowFilterPolicyItem;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.TestFlightServerHelper;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Runs the {@code ranger-hive} provider in a real Flight SQL server over DuckDB schemas named like Hive databases:
 * tables with a mask, a row filter or a column denial for the caller are refused in queries, joins, subqueries and
 * listings, in both result modes; unrestricted tables are answered.
 */
@Tag("integration")
class RangerHiveFlightSqlTest {
    private static final String PASSWORD = "test-password";
    private static final long ROWS = 100;

    @TempDir
    static Path dir;
    private static TestFlightServerHelper helper;

    /**
     * Writes the tables and starts the server with Ranger policies loaded in memory (no Ranger admin).
     *
     * @throws Exception when the data or the service definition cannot be prepared
     */
    @BeforeAll
    static void start() throws Exception {
        for (String table : List.of("products", "orders", "payments", "customers")) {
            write("sales", table);
        }
        write("hr", "salaries");
        UserGroupInformation.setConfiguration(new Configuration());
        HiveRangerPlugin plugin = new HiveRangerPlugin("hive", "ranger-hive-flight-test");
        plugin.setPolicies(policies());
        Map<String, Set<String>> groups = Map.of("sam", Set.of("staff"), "tess", Set.of("staff"),
                "carl", Set.of("staff", "contractors"));
        RangerHiveAuthorization authorization = new RangerHiveAuthorization(
                new HivePolicies(plugin, identity -> groups.getOrDefault(identity, Set.of())), "", () -> { });
        helper = TestFlightServerHelper.builder().dataDir(dir.toString()).threads(2)
                .authorization(authorization).password(PASSWORD).start();
    }

    @AfterAll
    static void stop() throws Exception {
        if (helper != null) {
            helper.close();
        }
    }

    /** An unrestricted table is answered for every allowed user, in both modes. */
    @Test
    void unrestrictedTablesAreAnswered() throws Exception {
        for (String mode : List.of("partial", "complete")) {
            for (String user : List.of("sam", "tess", "carl")) {
                assertEquals(ROWS, rows("SELECT * FROM sales.products", user, mode).size(), mode + " " + user);
                assertEquals(List.of(ROWS + "|"), rows("SELECT count(*) FROM sales.products", user, mode));
            }
            assertEquals(ROWS, rows("SELECT * FROM sales.orders o JOIN sales.payments p ON o.id = p.id",
                    "tess", mode).size(), mode);
        }
    }

    /**
     * A table with a mask (orders), a row filter (payments) or a column denial (customers) for the caller is
     * refused however it is referenced, also inside a subquery, a CTE or a set operation (their tables are named
     * from DuckDB's parse); the same queries pass for a user without the restriction (nested ones in complete mode).
     */
    @Test
    void restrictedTablesAreRefused() throws Exception {
        Map<String, List<String>> refused = Map.of(
                "sam", List.of("orders", "payments"),
                "carl", List.of("customers"));
        for (String mode : List.of("partial", "complete")) {
            for (Map.Entry<String, List<String>> entry : refused.entrySet()) {
                for (String table : entry.getValue()) {
                    String name = "sales." + table;
                    for (String sql : List.of(
                            "SELECT * FROM " + name,
                            "SELECT count(*) FROM " + name,
                            "SELECT id FROM " + name + " WHERE id < 3",
                            "SELECT p.id FROM sales.products p JOIN " + name + " t ON p.id = t.id",
                            "SELECT p.id FROM sales.products p JOIN \"sales\".\"" + table + "\" t ON p.id = t.id")) {
                        String label = mode + " " + entry.getKey() + ": " + sql;
                        FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                                () -> rows(sql, entry.getKey(), mode), label);
                        assertEquals(FlightStatusCode.UNAUTHORIZED, e.status().code(), label);
                        assertFalse(rows(sql, "tess", mode).isEmpty(), label);
                    }
                    for (String sql : List.of(
                            "SELECT * FROM sales.products WHERE id IN (SELECT id FROM " + name + ")",
                            "SELECT * FROM sales.products WHERE EXISTS (SELECT 1 FROM " + name + " WHERE id = 5)",
                            "SELECT (SELECT max(ssn) FROM " + name + ") AS m FROM sales.products",
                            "WITH x AS (SELECT * FROM " + name + ") SELECT count(*) FROM x",
                            "SELECT id FROM sales.products UNION ALL SELECT id FROM " + name)) {
                        String label = mode + " " + entry.getKey() + ": " + sql;
                        FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                                () -> rows(sql, entry.getKey(), mode), label);
                        assertEquals(FlightStatusCode.UNAUTHORIZED, e.status().code(), label);
                        assertFalse(String.valueOf(e.getMessage()).contains("ssn-"), label);
                        if ("complete".equals(mode)) {
                            // Partial mode does not run CTEs at all; complete mode runs them all.
                            assertDoesNotThrow(() -> rows(sql, "tess", mode), label);
                        }
                    }
                }
            }
        }
    }

    /** A database without a policy for the caller is neither readable nor visible. */
    @Test
    void databasesWithoutPoliciesAreHidden() throws Exception {
        FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                () -> rows("SELECT * FROM hr.salaries", "tess", "complete"));
        CallOption[] tess = login("tess", "complete");
        assertEquals(FlightStatusCode.UNAUTHORIZED, e.status().code());
        assertFalse(column(helper.sqlClient().getSchemas(null, null, tess), "db_schema_name", tess)
                .contains("hr"));
        assertEquals(List.of(), column(helper.sqlClient().getTables(null, "hr", null, null, false, tess),
                "table_name", tess));
    }

    /** Listings show only the tables the caller may read whole. */
    @Test
    void listingsShowOnlyReadableTables() throws Exception {
        assertEquals(List.of("customers", "products"), tables("sam"));
        assertEquals(List.of("customers", "orders", "payments", "products"), tables("tess"));
        assertEquals(List.of("orders", "payments", "products"), tables("carl"));
        assertEquals(List.of(), tables("mallory"));
    }

    /**
     * Lists the tables of schema {@code sales} for a user, sorted.
     *
     * @param user user name
     * @return table names
     * @throws Exception on failure
     */
    private static List<String> tables(String user) throws Exception {
        CallOption[] options = login(user, "complete");
        List<String> names = column(helper.sqlClient().getTables(null, "sales", null, null, false, options),
                "table_name", options);
        names.sort(null);
        return names;
    }

    /**
     * Policies of the test Hive service: staff read database {@code sales}; sam has a mask on
     * {@code orders.ssn} and a row filter on {@code payments}; contractors are denied {@code customers.email}.
     *
     * @return service policies
     * @throws Exception when Ranger's Hive service definition cannot be read
     */
    private static ServicePolicies policies() throws Exception {
        RangerPolicy access = policy(1, RangerPolicy.POLICY_TYPE_ACCESS, resources("sales", "*", "*"));
        access.setPolicyItems(List.of(new RangerPolicyItem(select(), List.of(), List.of("staff"), List.of(),
                List.of(), false)));
        RangerPolicy deny = policy(2, RangerPolicy.POLICY_TYPE_ACCESS, resources("sales", "customers", "email"));
        deny.setDenyPolicyItems(List.of(new RangerPolicyItem(select(), List.of(), List.of("contractors"),
                List.of(), List.of(), false)));
        RangerPolicy mask = policy(3, RangerPolicy.POLICY_TYPE_DATAMASK, resources("sales", "orders", "ssn"));
        mask.setDataMaskPolicyItems(List.of(new RangerDataMaskPolicyItem(select(),
                new RangerPolicyItemDataMaskInfo("MASK_NULL", null, null), List.of("sam"), List.of(), List.of(),
                List.of(), false)));
        Map<String, RangerPolicyResource> table = resources("sales", "payments", null);
        RangerPolicy filter = policy(4, RangerPolicy.POLICY_TYPE_ROWFILTER, table);
        filter.setRowFilterPolicyItems(List.of(new RangerRowFilterPolicyItem(
                new RangerPolicyItemRowFilterInfo("id < 10"), select(), List.of("sam"), List.of(), List.of(),
                List.of(), false)));
        return servicePolicies(List.of(access, deny, mask, filter));
    }

    /**
     * Wraps policies as the test Hive service's policies, with Ranger's own Hive service definition.
     *
     * @param policies policies of service {@code hivedev}
     * @return service policies
     * @throws Exception when Ranger's Hive service definition cannot be read
     */
    static ServicePolicies servicePolicies(List<RangerPolicy> policies) throws Exception {
        RangerServiceDef serviceDef;
        try (InputStream in = RangerHiveFlightSqlTest.class.getResourceAsStream(
                "/service-defs/ranger-servicedef-hive.json")) {
            serviceDef = JsonUtilsV2.jsonToObj(new String(Objects.requireNonNull(in).readAllBytes(),
                    StandardCharsets.UTF_8), RangerServiceDef.class);
        }
        ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setServiceName("hivedev");
        servicePolicies.setServiceDef(serviceDef);
        servicePolicies.setPolicyVersion(1L);
        servicePolicies.setPolicies(policies);
        return servicePolicies;
    }

    /**
     * Creates a policy of the test service.
     *
     * @param id policy id
     * @param type policy type
     * @param resources resources
     * @return the policy
     */
    static RangerPolicy policy(long id, int type, Map<String, RangerPolicyResource> resources) {
        RangerPolicy policy = new RangerPolicy();
        policy.setId(id);
        policy.setName("p" + id);
        policy.setService("hivedev");
        policy.setPolicyType(type);
        policy.setResources(resources);
        return policy;
    }

    /**
     * Builds database / table / column resources.
     *
     * @param database database value
     * @param table table value
     * @param column column value, null for a table resource
     * @return resources
     */
    static Map<String, RangerPolicyResource> resources(String database, String table, String column) {
        Map<String, RangerPolicyResource> resources = new LinkedHashMap<>();
        resources.put("database", new RangerPolicyResource(database));
        resources.put("table", new RangerPolicyResource(table));
        if (column != null) {
            resources.put("column", new RangerPolicyResource(column));
        }
        return resources;
    }

    /**
     * Builds the select access.
     *
     * @return accesses
     */
    static List<RangerPolicyItemAccess> select() {
        return List.of(new RangerPolicyItemAccess("select", true));
    }

    /**
     * Writes a Parquet table {@code schema.table} of {@link #ROWS} rows with columns id, ssn, email.
     *
     * @param schema schema directory
     * @param table table directory
     * @throws Exception on failure
     */
    private static void write(String schema, String table) throws Exception {
        Path path = dir.resolve(schema).resolve(table);
        Files.createDirectories(path);
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = duck.createStatement()) {
            st.execute(String.format(Locale.ROOT, "COPY (SELECT i AS id, 'ssn-' || i AS ssn,"
                    + " 'u' || i || '@example.org' AS email FROM range(%d) t(i)) TO '%s' (FORMAT parquet)",
                    ROWS, path.resolve("part-0.parquet")));
        }
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
        FlightSqlClient client = helper.sqlClient();
        FlightInfo info = client.execute(sql, options);
        List<String> rows = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(endpoint.getTicket(), options)) {
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
