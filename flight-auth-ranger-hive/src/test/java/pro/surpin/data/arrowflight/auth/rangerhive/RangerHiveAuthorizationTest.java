package pro.surpin.data.arrowflight.auth.rangerhive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.FlightAuthorization;
import pro.surpin.data.arrowflight.server.FlightAuthorizationProviderFactory;
import pro.surpin.data.arrowflight.server.TableRestriction;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the {@code ranger-hive} provider: schema mapping, whole-table decisions, settings, registration. */
@Tag("unit")
class RangerHiveAuthorizationTest {
    private static HivePolicies policies;

    /**
     * Loads Ranger's Hive service definition and the test policies.
     *
     * @throws Exception when the service definition cannot be read
     */
    @BeforeAll
    static void loadPolicies() throws Exception {
        UserGroupInformation.setConfiguration(new Configuration());
        RangerServiceDef serviceDef;
        try (InputStream in = RangerHiveAuthorizationTest.class.getResourceAsStream(
                "/service-defs/ranger-servicedef-hive.json")) {
            serviceDef = JsonUtilsV2.jsonToObj(new String(Objects.requireNonNull(in).readAllBytes(),
                    StandardCharsets.UTF_8), RangerServiceDef.class);
        }
        RangerPolicy access = new RangerPolicy();
        access.setId(1L);
        access.setName("p1");
        access.setService("hivedev");
        access.setResources(resources("sales", "*", "*"));
        access.setPolicyItems(List.of(new RangerPolicyItem(select(), List.of(), List.of("staff"), List.of(),
                List.of(), false)));
        RangerPolicy masks = new RangerPolicy();
        masks.setId(2L);
        masks.setName("p2");
        masks.setService("hivedev");
        masks.setPolicyType(RangerPolicy.POLICY_TYPE_DATAMASK);
        masks.setResources(resources("sales", "orders", "ssn"));
        masks.setDataMaskPolicyItems(List.of(new RangerDataMaskPolicyItem(select(),
                new RangerPolicyItemDataMaskInfo("MASK_NULL", null, null), List.of("sam"), List.of(), List.of(),
                List.of(), false)));
        RangerPolicy lastDigits = new RangerPolicy();
        lastDigits.setId(3L);
        lastDigits.setName("p3");
        lastDigits.setService("hivedev");
        lastDigits.setPolicyType(RangerPolicy.POLICY_TYPE_DATAMASK);
        lastDigits.setResources(resources("sales", "payments", "card"));
        lastDigits.setDataMaskPolicyItems(List.of(new RangerDataMaskPolicyItem(select(),
                new RangerPolicyItemDataMaskInfo("MASK_SHOW_LAST_4", null, null), List.of("sam"), List.of(),
                List.of(), List.of(), false)));
        ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setServiceName("hivedev");
        servicePolicies.setServiceDef(serviceDef);
        servicePolicies.setPolicyVersion(1L);
        servicePolicies.setPolicies(List.of(access, masks, lastDigits));
        HiveRangerPlugin plugin = new HiveRangerPlugin("hive", "ranger-hive-test");
        plugin.setPolicies(servicePolicies);
        Map<String, Set<String>> groups = Map.of("sam", Set.of("staff"), "tess", Set.of("staff"));
        policies = new HivePolicies(plugin, identity -> groups.getOrDefault(identity, Set.of()));
    }

    /** Server schemas map to Hive databases through the prefix; other schemas are refused. */
    @Test
    void mapsSchemasToDatabases() {
        RangerHiveAuthorization authorization = new RangerHiveAuthorization(policies, "hive_", () -> { });
        assertTrue(authorization.canSeeSchema("sam", "hive_sales"));
        assertFalse(authorization.canSeeSchema("sam", "sales"));
        assertFalse(authorization.canSeeSchema("sam", "hive_"));
        assertFalse(authorization.canSeeSchema("sam", null));
        assertFalse(authorization.canSeeSchema("sam", "hive_hr"));
        assertFalse(authorization.canSeeSchema("mallory", "hive_sales"));
        assertTrue(authorization.canSeeSchema("sam", "hive_SALES"));
        RangerHiveAuthorization unprefixed = new RangerHiveAuthorization(policies, "", () -> { });
        assertTrue(unprefixed.canSeeSchema("sam", "sales"));
        assertFalse(unprefixed.canSeeSchema("sam", ""));
    }

    /**
     * Tables are visible with any access; a query's table gets the caller's masks as DuckDB SQL over all its
     * columns; others' masks do not matter; what cannot be written exactly is denied.
     */
    @Test
    void restrictsTablesWithMasks() {
        RangerHiveAuthorization authorization = new RangerHiveAuthorization(policies, "hive_", () -> { });
        assertTrue(authorization.restrictsTables());
        assertTrue(authorization.canReadTable("sam", "hive_sales", "customers"));
        assertTrue(authorization.canReadTable("sam", "hive_sales", "orders"));
        assertFalse(authorization.canReadTable("mallory", "hive_sales", "orders"));
        assertFalse(authorization.canReadTable("sam", "sales", "customers"));
        assertFalse(authorization.canReadTable("sam", "hive_sales", null));
        Map<String, String> orders = new LinkedHashMap<>();
        orders.put("id", "INTEGER");
        orders.put("SSN", "VARCHAR");
        assertEquals(new TableRestriction(true, null, Map.of("SSN", "CAST(NULL AS VARCHAR)")),
                authorization.restrictTable("sam", "hive_sales", "orders", orders));
        assertEquals(TableRestriction.UNRESTRICTED,
                authorization.restrictTable("tess", "hive_sales", "orders", orders));
        assertEquals(TableRestriction.UNRESTRICTED,
                authorization.restrictTable("sam", "hive_sales", "customers", Map.of("id", "INTEGER")));
        assertEquals(TableRestriction.DENIED, authorization.restrictTable("mallory", "hive_sales", "orders", orders));
        assertEquals(TableRestriction.DENIED, authorization.restrictTable("sam", "sales", "orders", orders));
        assertEquals(TableRestriction.DENIED, authorization.restrictTable("sam", "hive_sales", "orders", Map.of()));
        // Show-last on text needs the Hive type (CHAR(n) pads); on numbers it is written.
        assertEquals(TableRestriction.DENIED,
                authorization.restrictTable("sam", "hive_sales", "payments", Map.of("card", "VARCHAR")));
        TableRestriction card = authorization.restrictTable("sam", "hive_sales", "payments",
                Map.of("card", "BIGINT"));
        assertTrue(card.restricts());
        assertTrue(card.masks().get("card").contains("\"card\""), card.masks().toString());
        // MASK_NULL nulls a list column, as Hive does; other masks refuse nested columns.
        assertEquals(new TableRestriction(true, null, Map.of("ssn", "CAST(NULL AS VARCHAR[])")),
                authorization.restrictTable("sam", "hive_sales", "orders", Map.of("ssn", "VARCHAR[]")));
        assertEquals(TableRestriction.DENIED,
                authorization.restrictTable("sam", "hive_sales", "payments", Map.of("card", "INTEGER[]")));
    }

    /** DuckDB types map to the Hive types the masks treat apart. */
    @Test
    void namesHiveTypes() {
        assertEquals("string", RangerHiveAuthorization.hiveType("VARCHAR"));
        assertEquals("int", RangerHiveAuthorization.hiveType("INTEGER"));
        assertEquals("tinyint", RangerHiveAuthorization.hiveType("tinyint"));
        assertEquals("date", RangerHiveAuthorization.hiveType("DATE"));
        assertEquals("decimal(10,2)", RangerHiveAuthorization.hiveType("DECIMAL(10,2)"));
        assertTrue(RangerHiveAuthorization.hiveType("INTEGER[]").startsWith("array<"));
        assertTrue(RangerHiveAuthorization.hiveType("STRUCT(a INTEGER)").startsWith("struct<"));
        assertTrue(RangerHiveAuthorization.hiveType("MAP(VARCHAR, INTEGER)").startsWith("map<"));
    }

    /** The provider is found by name, needs its settings, and releases the plugin on close. */
    @Test
    void factoryAndSettings() {
        FlightAuthorizationProviderFactory found = null;
        for (FlightAuthorizationProviderFactory factory : ServiceLoader.load(FlightAuthorizationProviderFactory.class)) {
            if ("ranger-hive".equals(factory.name())) {
                found = factory;
            }
        }
        assertTrue(found != null && found.requiresIdentity());
        FlightAuthorizationProviderFactory factory = found;
        IllegalArgumentException noService = assertThrows(IllegalArgumentException.class,
                () -> factory.create(Map.of("RANGER_ADMIN_URL", "http://ranger.invalid:6080")));
        assertEquals("RANGER_HIVE_SERVICE_NAME is required for the ranger-hive policy provider",
                noService.getMessage());
        assertThrows(IllegalArgumentException.class,
                () -> factory.create(Map.of("RANGER_HIVE_SERVICE_NAME", "hivedev"), identity -> Set.of()));

        assertEquals(Set.of("a", "b", "c"), RangerHiveAuthorization.union(identity -> Set.of("a", "b"),
                identity -> Set.of("b", "c")).apply("x"));
        assertThrows(IllegalStateException.class, () -> RangerHiveAuthorization.union(identity -> Set.of(),
                identity -> {
                    throw new IllegalStateException("mapping down");
                }).apply("x"));

        AtomicBoolean closed = new AtomicBoolean();
        FlightAuthorization authorization = new RangerHiveAuthorization(policies, "", () -> closed.set(true));
        authorization.close();
        assertTrue(closed.get());
    }

    /**
     * Builds database / table / column resources.
     *
     * @param database database value
     * @param table table value
     * @param column column value
     * @return resources
     */
    private static Map<String, RangerPolicyResource> resources(String database, String table, String column) {
        Map<String, RangerPolicyResource> resources = new LinkedHashMap<>();
        resources.put("database", new RangerPolicyResource(database));
        resources.put("table", new RangerPolicyResource(table));
        resources.put("column", new RangerPolicyResource(column));
        return resources;
    }

    /**
     * Builds the select access.
     *
     * @return accesses
     */
    private static List<RangerPolicyItemAccess> select() {
        return List.of(new RangerPolicyItemAccess("select", true));
    }
}
