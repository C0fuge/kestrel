package pro.surpin.data.arrowflight.auth.rangerhive;

import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.contextenricher.RangerTagEnricher;
import org.apache.ranger.plugin.contextenricher.RangerTagForEval;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A Ranger plugin of the Hive service that also lists its policies, so that restrictions on columns the server
 * cannot name (masks, column denials) are found before a whole table is read.
 */
public final class HiveRangerPlugin extends RangerBasePlugin {
    private final Set<String> zones = ConcurrentHashMap.newKeySet();
    private volatile Snapshot snapshot;

    /** Restrictive policies of one policy engine. */
    private record Snapshot(RangerPolicyEngine engine, HiveRestrictivePolicies policies) {
    }

    /**
     * Creates a plugin without settings (tests, policies set directly).
     *
     * @param serviceType Ranger service type
     * @param appId application id
     */
    public HiveRangerPlugin(String serviceType, String appId) {
        super(serviceType, appId);
    }

    /**
     * Creates a plugin from a plugin configuration.
     *
     * @param config plugin configuration
     */
    public HiveRangerPlugin(RangerPluginConfig config) {
        super(config);
    }

    @Override
    public void setPolicies(ServicePolicies policies) {
        if (policies != null && policies.getSecurityZones() != null) {
            // Zone names are kept for good: a zone's policies are listed even after an update without zones.
            zones.addAll(policies.getSecurityZones().keySet());
        }
        super.setPolicies(policies);
    }

    /**
     * Returns the restrictive policies of the current policy engine.
     *
     * @return the policies, null when no policies are loaded
     */
    HiveRestrictivePolicies restrictivePolicies() {
        RangerPolicyEngine engine = getPolicyEngine();
        if (engine == null || engine.getServiceDef() == null) {
            return null;
        }
        Snapshot current = snapshot;
        if (current == null || current.engine() != engine) {
            List<RangerPolicy> resourcePolicies = new ArrayList<>(nonNull(engine.getResourcePolicies()));
            for (String zone : zones) {
                resourcePolicies.addAll(nonNull(engine.getResourcePolicies(zone)));
            }
            current = new Snapshot(engine, HiveRestrictivePolicies.of(engine.getServiceDef(), resourcePolicies,
                    nonNull(engine.getTagPolicies())));
            snapshot = current;
        }
        return current.policies();
    }

    /**
     * Reads the names of the tags on a database, a table or one of the table's columns, as the service's tag
     * enricher (the tags downloaded from Ranger admin) matches them for an access to the table and its columns.
     *
     * @param database lower-case database name
     * @param table lower-case table name
     * @return tag names, null when the service has no tag enricher or its tags are not loaded yet
     */
    Set<String> tags(String database, String table) {
        RangerTagEnricher enricher = getTagEnricher();
        RangerPolicyEngine engine = getPolicyEngine();
        if (enricher == null || enricher.getEnrichedServiceTags() == null || engine == null) {
            return null;
        }
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setServiceDef(engine.getServiceDef());
        resource.setValue("database", database);
        resource.setValue("table", table);
        RangerAccessRequestImpl request = new RangerAccessRequestImpl(resource, RangerPolicyEngine.ANY_ACCESS,
                null, null, null);
        request.setAccessTime(new Date());
        request.setResourceMatchingScope(RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS);
        enricher.enrich(request);
        Set<String> names = new LinkedHashSet<>();
        Set<RangerTagForEval> found = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());
        if (found != null) {
            found.forEach(tag -> names.add(tag.getType()));
        }
        return names;
    }

    /**
     * Replaces a missing list by an empty one.
     *
     * @param policies policies, may be null
     * @return the policies
     */
    private static List<RangerPolicy> nonNull(List<RangerPolicy> policies) {
        return policies == null ? List.of() : policies;
    }
}
