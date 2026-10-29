package pro.surpin.data.arrowflight.auth.rangerhive;

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
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the Hive service's policies through a real Ranger plugin fed with policies, without a Ranger admin. */
@Tag("unit")
class HivePoliciesTest {
    private static final String SERVICE = "hivedev";
    private static final Map<String, Set<String>> GROUPS = Map.of(
            "alice", Set.of("analysts"), "sam", Set.of("staff"), "cora", Set.of("staff", "contractors"));
    private static final HivePolicies.HiveTable ORDERS = new HivePolicies.HiveTable("sales", "orders", "olga");

    private static RangerServiceDef serviceDef;
    private static HivePolicies policies;

    /**
     * Loads Ranger's Hive service definition and the test policies.
     *
     * @throws Exception when the service definition cannot be read
     */
    @BeforeAll
    static void loadPolicies() throws Exception {
        UserGroupInformation.setConfiguration(new Configuration());
        try (InputStream in = HivePoliciesTest.class.getResourceAsStream(
                "/service-defs/ranger-servicedef-hive.json")) {
            serviceDef = JsonUtilsV2.jsonToObj(new String(Objects.requireNonNull(in).readAllBytes(),
                    StandardCharsets.UTF_8), RangerServiceDef.class);
        }
        List<RangerPolicy> list = new ArrayList<>();
        list.add(access(1, columns("sales", "orders", "id", "amount", "region"),
                item(List.of("alice"), List.of())));
        list.add(access(2, columns("sales", "orders", "*"),
                item(List.of("bob", "carol", "dave", "erin"), List.of())));
        list.add(access(3, columns("sales", "*", "*"), item(List.of(), List.of("staff"))));
        RangerPolicy deny = access(4, columns("sales", "orders", "ssn"));
        deny.setDenyPolicyItems(List.of(item(List.of(), List.of("contractors"))));
        list.add(deny);
        list.add(access(5, columns("owned", "*", "*"), item(List.of("{OWNER}"), List.of())));

        RangerPolicy filter = policy(6, RangerPolicy.POLICY_TYPE_ROWFILTER, tables("sales", "orders"));
        filter.setRowFilterPolicyItems(List.of(new RangerRowFilterPolicyItem(
                new RangerPolicyItemRowFilterInfo("region = 'EU'"), accesses(), List.of(), List.of("analysts"),
                List.of(), List.of(), false)));
        list.add(filter);

        RangerPolicy masks = policy(7, RangerPolicy.POLICY_TYPE_DATAMASK, columns("sales", "orders", "ssn"));
        masks.setDataMaskPolicyItems(List.of(
                mask("MASK_SHOW_LAST_4", null, List.of(), List.of("staff")),
                mask("MASK_NULL", null, List.of("bob"), List.of()),
                mask("CUSTOM", "mask({col}, 'X')", List.of("carol"), List.of()),
                mask("CUSTOM", "concat({col}, '!')", List.of("dave"), List.of()),
                mask("MASK_NONE", null, List.of("erin"), List.of())));
        list.add(masks);

        ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setServiceName(SERVICE);
        servicePolicies.setServiceDef(serviceDef);
        servicePolicies.setPolicyVersion(1L);
        servicePolicies.setPolicies(list);
        RangerBasePlugin plugin = new RangerBasePlugin("hive", "hive-policies-test");
        plugin.setPolicies(servicePolicies);
        policies = new HivePolicies(plugin, identity -> GROUPS.getOrDefault(identity, Set.of()));
    }

    /** Databases and tables are visible with any access on them or inside them, as in SHOW DATABASES / TABLES. */
    @Test
    void visibilityFollowsAnyAccess() {
        assertTrue(policies.canSeeDatabase("alice", "sales"));
        assertTrue(policies.canSeeDatabase("alice", "SALES"));
        assertFalse(policies.canSeeDatabase("alice", "hr"));
        assertFalse(policies.canSeeDatabase("mallory", "sales"));
        assertTrue(policies.canSeeTable("alice", ORDERS));
        assertFalse(policies.canSeeTable("alice", new HivePolicies.HiveTable("sales", "customers", null)));
        assertTrue(policies.canSeeTable("sam", new HivePolicies.HiveTable("sales", "customers", null)));
        assertTrue(policies.canSeeTable("gina", new HivePolicies.HiveTable("owned", "t", "gina")));
        assertFalse(policies.canSeeTable("gina", new HivePolicies.HiveTable("owned", "t", "hank")));
        assertFalse(policies.canSeeTable("gina", new HivePolicies.HiveTable("owned", "t", null)));
    }

    /**
     * Select is checked per referenced column; a query reading no column needs a policy on all columns, and a
     * column-level deny is not bypassed by a table-level check.
     */
    @Test
    void selectIsCheckedPerColumn() {
        assertTrue(policies.readTable("alice", ORDERS, List.of("id", "AMOUNT")).allowed());
        HivePolicies.TableAccess ssn = policies.readTable("alice", ORDERS, List.of("id", "ssn"));
        assertFalse(ssn.allowed());
        assertEquals("no select on column ssn", ssn.reason());
        assertFalse(policies.readTable("alice", ORDERS, List.of()).allowed());
        assertTrue(policies.readTable("bob", ORDERS, List.of()).allowed());

        assertTrue(policies.readTable("cora", ORDERS, List.of("id")).allowed());
        assertTrue(policies.readTable("cora", ORDERS, List.of()).allowed());
        assertFalse(policies.readTable("cora", ORDERS, List.of("id", "ssn")).allowed());
        assertTrue(policies.readTable("sam", ORDERS, List.of("ssn")).allowed());
        assertFalse(policies.readTable("mallory", ORDERS, List.of("id")).allowed());
        assertTrue(policies.readTable("gina", new HivePolicies.HiveTable("owned", "t", "gina"),
                List.of("a")).allowed());
        assertFalse(policies.readTable("gina", new HivePolicies.HiveTable("owned", "t", "hank"),
                List.of("a")).allowed());
    }

    /** The row filter of the user's groups comes back as Hive's expression; others read every row. */
    @Test
    void rowFilterOfTheUsersGroups() {
        assertEquals("region = 'EU'", policies.readTable("alice", ORDERS, List.of("id")).rowFilter());
        assertNull(policies.readTable("bob", ORDERS, List.of("id")).rowFilter());
    }

    /**
     * Masks of the referenced columns are the service definition's transformers (or a custom mask function);
     * a custom expression that cannot be written exactly denies the table instead of reading it unmasked.
     */
    @Test
    void masksOfReferencedColumns() {
        HivePolicies.TableAccess staff = policies.readTable("sam", ORDERS, List.of("id", "ssn"));
        assertTrue(staff.allowed());
        assertEquals(Set.of("ssn"), staff.masks().keySet());
        assertEquals(HiveMask.parse("MASK_SHOW_LAST_4", transformer("MASK_SHOW_LAST_4"))
                .sql("\"ssn\"", "string", "VARCHAR"), staff.masks().get("ssn").sql("\"ssn\"", "string", "VARCHAR"));
        assertTrue(policies.readTable("sam", ORDERS, List.of("id")).masks().isEmpty());

        assertEquals("CAST(NULL AS VARCHAR)", policies.readTable("bob", ORDERS, List.of("ssn")).masks()
                .get("ssn").sql("\"ssn\"", "string", "VARCHAR"));
        assertEquals(HiveMask.parse("CUSTOM", "mask({col}, 'X')").sql("\"ssn\"", "string", "VARCHAR"),
                policies.readTable("carol", ORDERS, List.of("ssn")).masks().get("ssn")
                        .sql("\"ssn\"", "string", "VARCHAR"));
        HivePolicies.TableAccess custom = policies.readTable("dave", ORDERS, List.of("id", "ssn"));
        assertFalse(custom.allowed());
        assertTrue(policies.readTable("dave", ORDERS, List.of("id")).allowed());
        assertTrue(policies.readTable("erin", ORDERS, List.of("ssn")).masks().isEmpty());
    }

    /** Missing identities, failing group lookups, missing policies and empty names deny. */
    @Test
    void failsClosed() {
        assertFalse(policies.canSeeDatabase(null, "sales"));
        assertFalse(policies.readTable(" ", ORDERS, List.of("id")).allowed());
        assertFalse(policies.readTable("alice", new HivePolicies.HiveTable("sales", "", null), List.of()).allowed());
        assertFalse(policies.readTable("alice", ORDERS, List.of("id", "")).allowed());
        assertFalse(policies.canSeeDatabase("alice", null));

        Function<String, Set<String>> failing = identity -> {
            throw new IllegalStateException("directory down");
        };
        RangerBasePlugin loaded = new RangerBasePlugin("hive", "hive-policies-test");
        ServicePolicies all = new ServicePolicies();
        all.setServiceName(SERVICE);
        all.setServiceDef(serviceDef);
        all.setPolicyVersion(1L);
        all.setPolicies(List.of(access(1, columns("*", "*", "*"), item(List.of("alice"), List.of()))));
        loaded.setPolicies(all);
        assertTrue(new HivePolicies(loaded, identity -> Set.of()).readTable("alice", ORDERS, List.of()).allowed());
        HivePolicies noGroups = new HivePolicies(loaded, failing);
        assertFalse(noGroups.canSeeDatabase("alice", "sales"));
        assertFalse(noGroups.readTable("alice", ORDERS, List.of("id")).allowed());

        HivePolicies notLoaded = new HivePolicies(new RangerBasePlugin("hive", "hive-policies-test"),
                identity -> Set.of());
        assertFalse(notLoaded.canSeeDatabase("alice", "sales"));
        assertFalse(notLoaded.readTable("alice", ORDERS, List.of("id")).allowed());
    }

    /**
     * Reads a mask type's transformer from the service definition.
     *
     * @param maskType mask type name
     * @return the transformer
     */
    private static String transformer(String maskType) {
        for (RangerServiceDef.RangerDataMaskTypeDef type : serviceDef.getDataMaskDef().getMaskTypes()) {
            if (type.getName().equals(maskType)) {
                return type.getTransformer();
            }
        }
        throw new IllegalArgumentException(maskType);
    }

    /**
     * Builds database / table / column resources.
     *
     * @param database database value
     * @param table table value
     * @param columns column values
     * @return resources
     */
    private static Map<String, RangerPolicyResource> columns(String database, String table, String... columns) {
        Map<String, RangerPolicyResource> resources = tables(database, table);
        resources.put("column", new RangerPolicyResource(List.of(columns), false, false));
        return resources;
    }

    /**
     * Builds database / table resources.
     *
     * @param database database value
     * @param table table value
     * @return resources
     */
    private static Map<String, RangerPolicyResource> tables(String database, String table) {
        Map<String, RangerPolicyResource> resources = new LinkedHashMap<>();
        resources.put("database", new RangerPolicyResource(database));
        resources.put("table", new RangerPolicyResource(table));
        return resources;
    }

    /**
     * Builds an enabled policy of the test service.
     *
     * @param id policy id
     * @param type policy type
     * @param resources resources
     * @return the policy
     */
    private static RangerPolicy policy(long id, int type, Map<String, RangerPolicyResource> resources) {
        RangerPolicy policy = new RangerPolicy();
        policy.setId(id);
        policy.setName("p" + id);
        policy.setService(SERVICE);
        policy.setPolicyType(type);
        policy.setResources(resources);
        return policy;
    }

    /**
     * Builds an access policy.
     *
     * @param id policy id
     * @param resources resources
     * @param items allow items
     * @return the policy
     */
    private static RangerPolicy access(long id, Map<String, RangerPolicyResource> resources,
            RangerPolicyItem... items) {
        RangerPolicy policy = policy(id, RangerPolicy.POLICY_TYPE_ACCESS, resources);
        policy.setPolicyItems(List.of(items));
        return policy;
    }

    /**
     * Builds a select item.
     *
     * @param users users
     * @param groups groups
     * @return the item
     */
    private static RangerPolicyItem item(List<String> users, List<String> groups) {
        return new RangerPolicyItem(accesses(), users, groups, List.of(), List.of(), false);
    }

    /**
     * Builds a data mask item.
     *
     * @param maskType mask type
     * @param value custom value, may be null
     * @param users users
     * @param groups groups
     * @return the item
     */
    private static RangerDataMaskPolicyItem mask(String maskType, String value, List<String> users,
            List<String> groups) {
        return new RangerDataMaskPolicyItem(accesses(), new RangerPolicyItemDataMaskInfo(maskType, null, value),
                users, groups, List.of(), List.of(), false);
    }

    /**
     * Builds the select access.
     *
     * @return accesses
     */
    private static List<RangerPolicyItemAccess> accesses() {
        return List.of(new RangerPolicyItemAccess("select", true));
    }
}
