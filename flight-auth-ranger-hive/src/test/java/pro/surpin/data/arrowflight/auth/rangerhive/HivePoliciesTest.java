package pro.surpin.data.arrowflight.auth.rangerhive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemRowFilterInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicy.RangerRowFilterPolicyItem;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
        RangerBasePlugin plugin = new HiveRangerPlugin("hive", "hive-policies-test");
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
     * A table is read whole only with select on all its columns, no row filter, no mask and no column deny for
     * the user; tables without restrictive policies are unaffected by those of other tables.
     */
    @Test
    void wholeTableNeedsEveryColumnUnrestricted() {
        assertTrue(policies.readWholeTable("erin", ORDERS).allowed());
        assertEquals("no select on the table", policies.readWholeTable("alice", ORDERS).reason());
        assertEquals("masked column ssn", policies.readWholeTable("bob", ORDERS).reason());
        assertFalse(policies.readWholeTable("carol", ORDERS).allowed());
        assertFalse(policies.readWholeTable("dave", ORDERS).allowed());
        assertFalse(policies.readWholeTable("sam", ORDERS).allowed());
        assertEquals("no select on column ssn", policies.readWholeTable("cora", ORDERS).reason());
        HivePolicies.HiveTable customers = new HivePolicies.HiveTable("sales", "customers", null);
        assertTrue(policies.readWholeTable("sam", customers).allowed());
        assertTrue(policies.readWholeTable("cora", customers).allowed());
        assertTrue(policies.readWholeTable("cora", new HivePolicies.HiveTable("SALES", "Customers", null))
                .allowed());
        assertFalse(policies.readWholeTable("mallory", customers).allowed());
        assertFalse(policies.readWholeTable("gina", new HivePolicies.HiveTable("owned", "t", null)).allowed());

        HivePolicies plain = new HivePolicies(plugin(List.of(access(1, columns("*", "*", "*"),
                item(List.of("alice"), List.of())))), identity -> Set.of());
        assertFalse(plain.readWholeTable("alice", ORDERS).allowed(), "a plain RangerBasePlugin cannot list");
    }

    /** Row filters, masks on every column and policies the server cannot evaluate exactly deny the table. */
    @Test
    void wholeTableFailsClosedOnInexactPolicies() {
        RangerPolicy filter = policy(2, RangerPolicy.POLICY_TYPE_ROWFILTER, tables("sales", "orders"));
        filter.setRowFilterPolicyItems(List.of(new RangerRowFilterPolicyItem(
                new RangerPolicyItemRowFilterInfo("region = 'EU'"), accesses(), List.of("alice"), List.of(),
                List.of(), List.of(), false)));
        assertEquals("row filter on the table", whole(filter).reason());

        RangerPolicy allColumns = policy(2, RangerPolicy.POLICY_TYPE_DATAMASK, columns("sales", "*", "*"));
        allColumns.setDataMaskPolicyItems(List.of(mask("MASK_NULL", null, List.of("alice"), List.of())));
        assertEquals("masked column (any)", whole(allColumns).reason());

        RangerPolicy otherUser = policy(2, RangerPolicy.POLICY_TYPE_DATAMASK, columns("sales", "*", "ssn"));
        otherUser.setDataMaskPolicyItems(List.of(mask("MASK_NULL", null, List.of("bob"), List.of())));
        assertTrue(whole(otherUser).allowed());
        RangerPolicy otherTable = policy(2, RangerPolicy.POLICY_TYPE_DATAMASK, columns("sales", "items", "ssn"));
        otherTable.setDataMaskPolicyItems(List.of(mask("MASK_NULL", null, List.of("alice"), List.of())));
        assertTrue(whole(otherTable).allowed());
        // Hive's mask resources take no wildcards (service definition): Ranger reads "sal*" literally, so do we.
        RangerPolicy literalMask = policy(2, RangerPolicy.POLICY_TYPE_DATAMASK, columns("sal*", "ord*", "ssn"));
        literalMask.setDataMaskPolicyItems(List.of(mask("MASK_NULL", null, List.of("alice"), List.of())));
        assertTrue(whole(literalMask).allowed());
        RangerPolicy wildcardTable = access(2, columns("sal*", "ord*", "ssn"));
        wildcardTable.setDenyPolicyItems(List.of(item(List.of("alice"), List.of())));
        assertEquals("no select on column ssn", whole(wildcardTable).reason());

        RangerPolicy pattern = access(2, columns("sales", "orders", "ss*"));
        pattern.setDenyPolicyItems(List.of(item(List.of("bob"), List.of())));
        assertEquals("column pattern in policy 2", whole(pattern).reason());
        RangerPolicy patternElsewhere = access(2, columns("hr", "*", "ss*"));
        patternElsewhere.setDenyPolicyItems(List.of(item(List.of("bob"), List.of())));
        assertTrue(whole(patternElsewhere).allowed());

        Map<String, RangerPolicyResource> excludedColumns = columns("sales", "orders", "id");
        excludedColumns.get("column").setIsExcludes(true);
        RangerPolicy excludes = access(2, excludedColumns);
        excludes.setDenyPolicyItems(List.of(item(List.of("alice"), List.of())));
        assertEquals("column exclusion in policy 2", whole(excludes).reason());

        RangerPolicy owner = access(2, columns("sales", "orders", "ssn"));
        owner.setDenyPolicyItems(List.of(item(List.of("{OWNER}"), List.of())));
        assertEquals("owner restriction in policy 2", whole(owner).reason());
        RangerPolicy ownerException = access(2, columns("sales", "orders", "*"));
        ownerException.setAllowExceptions(List.of(item(List.of("{OWNER}"), List.of())));
        assertEquals("owner restriction in policy 2", whole(ownerException).reason());

        RangerPolicy conditional = access(2, columns("sales", "orders", "ssn"));
        RangerPolicyItem conditionalItem = item(List.of("bob"), List.of());
        conditionalItem.setConditions(List.of(new RangerPolicyItemCondition("ip-range", List.of("10.*"))));
        conditional.setDenyPolicyItems(List.of(conditionalItem));
        assertEquals("conditional restriction in policy 2", whole(conditional).reason());

        RangerPolicy denyOther = access(2, columns("sales", "orders", "ssn"));
        denyOther.setDenyPolicyItems(List.of(item(List.of("bob"), List.of())));
        assertTrue(whole(denyOther).allowed());
        RangerPolicy denyMe = access(2, columns("sales", "orders", "ssn"));
        denyMe.setDenyPolicyItems(List.of(item(List.of("alice"), List.of())));
        assertEquals("no select on column ssn", whole(denyMe).reason());
        RangerPolicy additional = access(2, columns("hr", "people", "ssn"));
        additional.setAdditionalResources(List.of(columns("sales", "orders", "region")));
        additional.setDenyPolicyItems(List.of(item(List.of("alice"), List.of())));
        assertEquals("no select on column region", whole(additional).reason());
    }

    /**
     * Policies of a security zone decide for the tables in the zone, also when the server reads them whole: the
     * zone's column denials and masks are listed, the default zone's policies do not reach into the zone.
     */
    @Test
    void wholeTableFollowsSecurityZones() {
        RangerPolicy defaultDeny = access(2, columns("hr", "teams", "budget"));
        defaultDeny.setDenyPolicyItems(List.of(item(List.of("alice"), List.of())));
        List<RangerPolicy> zonePolicies = new ArrayList<>();
        zonePolicies.add(access(10, columns("hr", "*", "*"), item(List.of("alice"), List.of())));
        RangerPolicy zoneDeny = access(11, columns("hr", "people", "ssn"));
        zoneDeny.setDenyPolicyItems(List.of(item(List.of("alice"), List.of())));
        zonePolicies.add(zoneDeny);
        RangerPolicy zoneMask = policy(12, RangerPolicy.POLICY_TYPE_DATAMASK, columns("hr", "staff", "salary"));
        zoneMask.setDataMaskPolicyItems(List.of(mask("MASK_NULL", null, List.of("alice"), List.of())));
        zonePolicies.add(zoneMask);
        zonePolicies.forEach(policy -> policy.setZoneName("pii"));
        ServicePolicies.SecurityZoneInfo zone = new ServicePolicies.SecurityZoneInfo();
        zone.setZoneName("pii");
        zone.setResources(List.of(new HashMap<>(Map.of("database", List.of("hr")))));
        zone.setPolicies(zonePolicies);
        ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setServiceName(SERVICE);
        servicePolicies.setServiceDef(serviceDef);
        servicePolicies.setPolicyVersion(1L);
        servicePolicies.setPolicies(List.of(access(1, columns("*", "*", "*"), item(List.of("alice"), List.of())),
                defaultDeny));
        servicePolicies.setSecurityZones(Map.of("pii", zone));
        HiveRangerPlugin plugin = new HiveRangerPlugin("hive", "hive-policies-test");
        plugin.setPolicies(servicePolicies);
        HivePolicies zoned = new HivePolicies(plugin, identity -> Set.of());

        assertTrue(zoned.readWholeTable("alice", ORDERS).allowed());
        assertEquals("no select on column ssn",
                zoned.readWholeTable("alice", new HivePolicies.HiveTable("hr", "people", null)).reason());
        assertEquals("masked column salary",
                zoned.readWholeTable("alice", new HivePolicies.HiveTable("hr", "staff", null)).reason());
        assertTrue(zoned.readWholeTable("alice", new HivePolicies.HiveTable("hr", "teams", null)).allowed());
        assertFalse(zoned.readWholeTable("bob", new HivePolicies.HiveTable("hr", "teams", null)).allowed());
    }

    /**
     * Each whole-table decision is audited once, as a select on the table without the probe columns; a denial
     * by a column restriction is audited as denied with its reason, visibility checks are not audited.
     */
    @Test
    void wholeTableDecisionsAreAudited() {
        RangerPolicy deny = access(2, columns("sales", "orders", "ssn"));
        deny.setDenyPolicyItems(List.of(item(List.of("alice"), List.of())));
        RangerBasePlugin plugin = plugin(List.of(access(1, columns("sales", "*", "*"),
                item(List.of("alice"), List.of())), deny));
        List<RangerAccessResult> audited = new ArrayList<>();
        HivePolicies audit = new HivePolicies(plugin, identity -> Set.of(), new RangerAccessResultProcessor() {
            @Override
            public void processResult(RangerAccessResult result) {
                audited.add(result);
            }

            @Override
            public void processResults(Collection<RangerAccessResult> results) {
                audited.addAll(results);
            }
        });

        assertTrue(audit.canSeeDatabase("alice", "sales"));
        assertTrue(audit.readWholeTable("alice", new HivePolicies.HiveTable("sales", "customers", null))
                .allowed());
        assertFalse(audit.readWholeTable("alice", ORDERS).allowed());
        assertFalse(audit.readWholeTable("mallory", ORDERS).allowed());
        assertEquals(3, audited.size());

        RangerAccessResult allowed = audited.get(0);
        assertTrue(allowed.getIsAllowed());
        assertEquals(1L, allowed.getPolicyId());
        assertEquals("alice", allowed.getAccessRequest().getUser());
        assertEquals("select", allowed.getAccessRequest().getAccessType());
        assertEquals("customers", allowed.getAccessRequest().getResource().getValue("table"));
        assertNull(allowed.getAccessRequest().getResource().getValue("column"));

        RangerAccessResult restricted = audited.get(1);
        assertFalse(restricted.getIsAllowed());
        assertTrue(restricted.getIsAudited());
        assertEquals(-1L, restricted.getPolicyId());
        assertEquals("no select on column ssn", restricted.getReason());
        assertEquals("orders", restricted.getAccessRequest().getResource().getValue("table"));
        assertNull(restricted.getAccessRequest().getResource().getValue("column"));

        RangerAccessResult denied = audited.get(2);
        assertFalse(denied.getIsAllowed());
        assertEquals("mallory", denied.getAccessRequest().getUser());
    }

    /**
     * Reads table sales.orders whole as alice, who may select all its columns, with one more policy.
     *
     * @param extra the extra policy
     * @return the decision
     */
    private static HivePolicies.TableAccess whole(RangerPolicy extra) {
        RangerBasePlugin plugin = plugin(List.of(access(1, columns("sales", "*", "*"),
                item(List.of("alice", "bob"), List.of())), extra));
        return new HivePolicies(plugin, identity -> Set.of()).readWholeTable("alice", ORDERS);
    }

    /**
     * Creates a listing plugin holding policies, or a plain one for a single policy.
     *
     * @param list policies
     * @return the plugin
     */
    private static RangerBasePlugin plugin(List<RangerPolicy> list) {
        ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setServiceName(SERVICE);
        servicePolicies.setServiceDef(serviceDef);
        servicePolicies.setPolicyVersion(1L);
        servicePolicies.setPolicies(list);
        RangerBasePlugin plugin = list.size() == 1 ? new RangerBasePlugin("hive", "hive-policies-test")
                : new HiveRangerPlugin("hive", "hive-policies-test");
        plugin.setPolicies(servicePolicies);
        return plugin;
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
