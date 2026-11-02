package pro.surpin.data.arrowflight.auth.rangerhive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.ServiceTags;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests restrictive tag policies of a Hive service with a tag service: only tables whose database, table or columns
 * carry such a tag are denied, as Ranger's tag enricher matches the downloaded tags.
 */
@Tag("unit")
class HiveTagPoliciesTest {
    private static final String SERVICE = "hivedev";
    private static final String TAG_SERVICE = "tagdev";
    private static final HivePolicies.HiveTable ORDERS = new HivePolicies.HiveTable("sales", "orders", null);
    private static final HivePolicies.HiveTable CUSTOMERS = new HivePolicies.HiveTable("sales", "customers", null);
    private static final HivePolicies.HiveTable OLD = new HivePolicies.HiveTable("sales", "old", null);
    private static final HivePolicies.HiveTable PAYROLL = new HivePolicies.HiveTable("hr", "payroll", null);

    private static RangerServiceDef hiveDef;
    private static RangerServiceDef tagDef;

    /**
     * Loads Ranger's Hive and tag service definitions; the tag enricher gets no retriever (tags are set here).
     *
     * @throws Exception when a service definition cannot be read
     */
    @BeforeAll
    static void loadServiceDefs() throws Exception {
        UserGroupInformation.setConfiguration(new Configuration());
        hiveDef = serviceDef("/service-defs/ranger-servicedef-hive.json");
        tagDef = serviceDef("/service-defs/ranger-servicedef-tag.json");
        tagDef.getContextEnrichers().forEach(enricher -> enricher.setEnricherOptions(Map.of()));
    }

    /**
     * Ranger's default EXPIRES_ON deny policy and a PII deny policy refuse only the tables carrying their tags (on
     * a column, the table or the database); untagged tables are read as before.
     */
    @Test
    void restrictiveTagPoliciesDenyOnlyTaggedTables() {
        HiveRangerPlugin plugin = plugin(List.of(expiresOn(100), deny(101, "PII")), true);
        HivePolicies policies = new HivePolicies(plugin, identity -> Set.of());

        assertEquals(Set.of(), plugin.tags("sales", "orders"));
        assertEquals(Set.of("PII"), plugin.tags("sales", "customers"));
        assertEquals(Set.of("EXPIRES_ON"), plugin.tags("sales", "old"));
        assertEquals(Set.of("PII"), plugin.tags("hr", "payroll"));

        assertTrue(policies.readWholeTable("alice", ORDERS).allowed());
        assertTrue(policies.readColumns("alice", ORDERS, List.of("id", "amount")).allowed());

        HivePolicies.TableAccess column = policies.readColumns("alice", CUSTOMERS, List.of("id"));
        assertFalse(column.allowed());
        assertEquals("restrictive tag policy 101 on tag pii", column.reason());
        assertFalse(policies.readWholeTable("alice", CUSTOMERS).allowed());
        HivePolicies.TableAccess expired = policies.readColumns("alice", OLD, List.of("id"));
        assertFalse(expired.allowed());
        assertEquals("restrictive tag policy 100 on tag expires_on", expired.reason());
        assertFalse(policies.readColumns("alice", PAYROLL, List.of("id")).allowed());
    }

    /** With restrictive tag policies but no tags loaded yet, every table is denied (the tags are unknown). */
    @Test
    void unknownTagsDeny() {
        HiveRangerPlugin plugin = plugin(List.of(expiresOn(100)), false);
        HivePolicies.TableAccess access = new HivePolicies(plugin, identity -> Set.of())
                .readColumns("alice", ORDERS, List.of("id"));
        assertFalse(access.allowed());
        assertEquals("restrictive tag policy 100 and the tags are unknown", access.reason());
    }

    /** A tag policy on an excluded or patterned tag may apply to any tag: tagged tables are denied, others not. */
    @Test
    void anyTagPolicyDeniesTaggedTables() {
        RangerPolicy excludes = deny(102, "PUBLIC");
        excludes.getResources().get("tag").setIsExcludes(true);
        HivePolicies policies = new HivePolicies(plugin(List.of(excludes), true), identity -> Set.of());
        assertTrue(policies.readColumns("alice", ORDERS, List.of("id")).allowed());
        HivePolicies.TableAccess tagged = policies.readColumns("alice", CUSTOMERS, List.of("id"));
        assertFalse(tagged.allowed());
        assertEquals("restrictive tag policy 102 on any tag", tagged.reason());
    }

    /** Tag policies without restrictive items change nothing, even with tags unknown. */
    @Test
    void allowOnlyTagPoliciesDoNotDeny() {
        RangerPolicy allow = tagPolicy(103, "PII");
        allow.setPolicyItems(List.of(new RangerPolicyItem(List.of(new RangerPolicyItemAccess("hive:select", true)),
                List.of("bob"), List.of(), List.of(), List.of(), false)));
        HivePolicies policies = new HivePolicies(plugin(List.of(allow), false), identity -> Set.of());
        assertTrue(policies.readColumns("alice", CUSTOMERS, List.of("id")).allowed());
    }

    /**
     * Creates a listing plugin of the Hive service: alice may select everything in databases sales and hr, the tag
     * service holds the tag policies and, when asked, tags PII on sales.customers.ssn and on database hr and
     * EXPIRES_ON (expired) on sales.old.
     *
     * @param tagPolicies tag-based policies
     * @param withTags whether the tags are loaded
     * @return the plugin
     */
    private static HiveRangerPlugin plugin(List<RangerPolicy> tagPolicies, boolean withTags) {
        ServicePolicies.TagPolicies tags = new ServicePolicies.TagPolicies();
        tags.setServiceName(TAG_SERVICE);
        tags.setServiceDef(tagDef);
        tags.setPolicyVersion(1L);
        tags.setPolicies(tagPolicies);
        ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setServiceName(SERVICE);
        servicePolicies.setServiceDef(hiveDef);
        servicePolicies.setPolicyVersion(1L);
        servicePolicies.setPolicies(List.of(
                access(1, resources("sales", "*", "*")), access(2, resources("hr", "*", "*"))));
        servicePolicies.setTagPolicies(tags);
        HiveRangerPlugin plugin = new HiveRangerPlugin("hive", "hive-tag-policies-test");
        plugin.setPolicies(servicePolicies);
        assertNotNull(plugin.getTagEnricher());
        if (withTags) {
            ServiceTags serviceTags = new ServiceTags();
            serviceTags.setServiceName(SERVICE);
            serviceTags.setTagVersion(1L);
            Map<Long, RangerTag> tagMap = new HashMap<>();
            tagMap.put(1L, tag(1, "PII", Map.of()));
            tagMap.put(2L, tag(2, "EXPIRES_ON", Map.of("expiry_date", "2020/01/01")));
            tagMap.put(3L, tag(3, "PII", Map.of()));
            serviceTags.setTags(tagMap);
            List<RangerServiceResource> serviceResources = new ArrayList<>();
            serviceResources.add(serviceResource(11, resources("sales", "customers", "ssn")));
            serviceResources.add(serviceResource(12, resources("sales", "old", null)));
            Map<String, RangerPolicyResource> database = new LinkedHashMap<>();
            database.put("database", new RangerPolicyResource("hr"));
            serviceResources.add(serviceResource(13, database));
            serviceTags.setServiceResources(serviceResources);
            serviceTags.setResourceToTagIds(Map.of(11L, List.of(1L), 12L, List.of(2L), 13L, List.of(3L)));
            plugin.getTagEnricher().setServiceTags(serviceTags);
        }
        return plugin;
    }

    /**
     * Builds Ranger's default EXPIRES_ON policy: deny for everyone when accessed after the tag's expiry date.
     *
     * @param id policy id
     * @return the policy
     */
    private static RangerPolicy expiresOn(long id) {
        RangerPolicy policy = tagPolicy(id, "EXPIRES_ON");
        policy.setDenyPolicyItems(List.of(new RangerPolicyItem(
                List.of(new RangerPolicyItemAccess("hive:select", true)), List.of(), List.of("public"), List.of(),
                List.of(new RangerPolicyItemCondition("accessed-after-expiry", List.of("yes"))), false)));
        return policy;
    }

    /**
     * Builds a tag policy denying select to everyone.
     *
     * @param id policy id
     * @param tag tag name
     * @return the policy
     */
    private static RangerPolicy deny(long id, String tag) {
        RangerPolicy policy = tagPolicy(id, tag);
        policy.setDenyPolicyItems(List.of(new RangerPolicyItem(
                List.of(new RangerPolicyItemAccess("hive:select", true)), List.of(), List.of("public"), List.of(),
                List.of(), false)));
        return policy;
    }

    /**
     * Builds an enabled tag policy without items.
     *
     * @param id policy id
     * @param tag tag name
     * @return the policy
     */
    private static RangerPolicy tagPolicy(long id, String tag) {
        RangerPolicy policy = new RangerPolicy();
        policy.setId(id);
        policy.setName("t" + id);
        policy.setService(TAG_SERVICE);
        policy.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);
        Map<String, RangerPolicyResource> resources = new LinkedHashMap<>();
        resources.put("tag", new RangerPolicyResource(tag));
        policy.setResources(resources);
        return policy;
    }

    /**
     * Builds a Hive select policy for alice.
     *
     * @param id policy id
     * @param resources resources
     * @return the policy
     */
    private static RangerPolicy access(long id, Map<String, RangerPolicyResource> resources) {
        RangerPolicy policy = new RangerPolicy();
        policy.setId(id);
        policy.setName("p" + id);
        policy.setService(SERVICE);
        policy.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);
        policy.setResources(resources);
        policy.setPolicyItems(List.of(new RangerPolicyItem(List.of(new RangerPolicyItemAccess("select", true)),
                List.of("alice"), List.of(), List.of(), List.of(), false)));
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
    private static Map<String, RangerPolicyResource> resources(String database, String table, String column) {
        Map<String, RangerPolicyResource> resources = new LinkedHashMap<>();
        resources.put("database", new RangerPolicyResource(database));
        resources.put("table", new RangerPolicyResource(table));
        if (column != null) {
            resources.put("column", new RangerPolicyResource(column));
        }
        return resources;
    }

    /**
     * Builds a tagged resource of the Hive service.
     *
     * @param id resource id
     * @param elements resource elements
     * @return the resource
     */
    private static RangerServiceResource serviceResource(long id, Map<String, RangerPolicyResource> elements) {
        RangerServiceResource resource = new RangerServiceResource(SERVICE, elements);
        resource.setId(id);
        return resource;
    }

    /**
     * Builds a tag.
     *
     * @param id tag id
     * @param type tag name
     * @param attributes tag attributes
     * @return the tag
     */
    private static RangerTag tag(long id, String type, Map<String, String> attributes) {
        RangerTag tag = new RangerTag(type, attributes);
        tag.setId(id);
        return tag;
    }

    /**
     * Reads a service definition from Ranger's jar.
     *
     * @param path resource path
     * @return the service definition
     * @throws Exception when it cannot be read
     */
    private static RangerServiceDef serviceDef(String path) throws Exception {
        try (InputStream in = HiveTagPoliciesTest.class.getResourceAsStream(path)) {
            return JsonUtilsV2.jsonToObj(new String(Objects.requireNonNull(in).readAllBytes(),
                    StandardCharsets.UTF_8), RangerServiceDef.class);
        }
    }
}
