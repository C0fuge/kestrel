package pro.surpin.data.arrowflight.auth.rangerhive;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyresourcematcher.RangerDefaultPolicyResourceMatcher;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The Hive service's policies that can restrict single columns of a table (column denials, masks) or that the
 * server cannot evaluate exactly (owner macros, item conditions, column patterns, tag policies). They decide which
 * columns must be checked before a table is read whole while the server cannot name its columns.
 */
final class HiveRestrictivePolicies {
    /** A column name no policy names: Ranger matches it only by the {@code *} column value. */
    static final String ANY_COLUMN = "\u0000any column";

    private static final String OWNER = "{OWNER}";

    private final List<Entry> entries;
    private final String globalBlock;

    /**
     * One resource set of a restrictive policy.
     *
     * @param policyId policy id, for messages
     * @param matcher matcher of the database / table / column resources
     * @param columns column values, empty when the resources name no column
     * @param columnExcludes whether the column values are an exclusion
     * @param block why a match denies the whole table, null when the columns are checked instead
     */
    private record Entry(Long policyId, RangerPolicyResourceMatcher matcher, List<String> columns,
            boolean columnExcludes, String block) {
    }

    /**
     * Probing decision for one table.
     *
     * @param columns columns to check (always including {@link #ANY_COLUMN})
     * @param denial why the table cannot be read whole, null when the columns decide
     */
    record Probe(Set<String> columns, String denial) {
    }

    /**
     * Creates the decision data.
     *
     * @param entries restrictive resource sets
     * @param globalBlock why every table is denied, null when not
     */
    private HiveRestrictivePolicies(List<Entry> entries, String globalBlock) {
        this.entries = entries;
        this.globalBlock = globalBlock;
    }

    /**
     * Collects the restrictive policies.
     *
     * @param serviceDef Hive service definition
     * @param resourcePolicies resource-based policies of all zones
     * @param tagPolicies tag-based policies
     * @return the restrictive policies
     */
    static HiveRestrictivePolicies of(RangerServiceDef serviceDef, List<RangerPolicy> resourcePolicies,
            List<RangerPolicy> tagPolicies) {
        String globalBlock = null;
        for (RangerPolicy policy : tagPolicies) {
            if (restrictive(policy)) {
                // Tags of unnamed columns are unknown here.
                globalBlock = "restrictive tag policy " + policy.getId();
                break;
            }
        }
        List<Entry> entries = new ArrayList<>();
        for (RangerPolicy policy : resourcePolicies) {
            if (!restrictive(policy)) {
                continue;
            }
            String block = inexact(policy);
            List<Map<String, RangerPolicyResource>> resourceSets = new ArrayList<>();
            resourceSets.add(policy.getResources());
            if (policy.getAdditionalResources() != null) {
                resourceSets.addAll(policy.getAdditionalResources());
            }
            for (Map<String, RangerPolicyResource> resources : resourceSets) {
                if (resources == null || !resources.containsKey("table")) {
                    continue;
                }
                RangerDefaultPolicyResourceMatcher matcher = new RangerDefaultPolicyResourceMatcher();
                matcher.setServiceDef(serviceDef);
                matcher.setPolicyResources(resources, policyType(policy));
                matcher.init();
                RangerPolicyResource column = resources.get("column");
                List<String> columns = column == null || column.getValues() == null
                        ? List.of() : List.copyOf(column.getValues());
                boolean excludes = column != null && Boolean.TRUE.equals(column.getIsExcludes());
                entries.add(new Entry(policy.getId(), matcher, columns, excludes, block));
            }
        }
        return new HiveRestrictivePolicies(List.copyOf(entries), globalBlock);
    }

    /**
     * Finds the columns of a table to check before it is read whole.
     *
     * @param database lower-case database name
     * @param table lower-case table name
     * @return the columns, or why the table cannot be read whole
     */
    Probe probe(String database, String table) {
        Set<String> columns = new LinkedHashSet<>();
        columns.add(ANY_COLUMN);
        if (globalBlock != null) {
            return new Probe(columns, globalBlock);
        }
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue("database", database);
        resource.setValue("table", table);
        for (Entry entry : entries) {
            if (!entry.matcher().isMatch(resource, RangerPolicyResourceMatcher.MatchScope.SELF_OR_DESCENDANT,
                    null)) {
                continue;
            }
            if (entry.block() != null) {
                return new Probe(columns, entry.block() + " in policy " + entry.policyId());
            }
            if (entry.columnExcludes()) {
                return new Probe(columns, "column exclusion in policy " + entry.policyId());
            }
            for (String column : entry.columns()) {
                if ("*".equals(column)) {
                    continue;
                }
                if (column.indexOf('*') >= 0 || column.indexOf('?') >= 0 || column.indexOf('{') >= 0) {
                    return new Probe(columns, "column pattern in policy " + entry.policyId());
                }
                columns.add(column.toLowerCase(Locale.ROOT));
            }
        }
        return new Probe(columns, null);
    }

    /**
     * Checks whether a policy can restrict what a user reads: deny items, allow exceptions, masks, row filters.
     *
     * @param policy the policy
     * @return whether it is restrictive
     */
    private static boolean restrictive(RangerPolicy policy) {
        return switch (policyType(policy)) {
            case RangerPolicy.POLICY_TYPE_DATAMASK -> !empty(policy.getDataMaskPolicyItems());
            case RangerPolicy.POLICY_TYPE_ROWFILTER -> !empty(policy.getRowFilterPolicyItems());
            default -> !empty(policy.getDenyPolicyItems()) || !empty(policy.getAllowExceptions());
        };
    }

    /**
     * Finds what the server cannot evaluate exactly in a policy's restrictive items: the table owner (unknown to
     * the server) and item conditions (evaluated without the client's context).
     *
     * @param policy restrictive policy
     * @return why a match denies the whole table, null when the policy's columns can be checked
     */
    private static String inexact(RangerPolicy policy) {
        List<RangerPolicyItem> items = new ArrayList<>();
        switch (policyType(policy)) {
            case RangerPolicy.POLICY_TYPE_DATAMASK -> items.addAll(nonNull(policy.getDataMaskPolicyItems()));
            case RangerPolicy.POLICY_TYPE_ROWFILTER -> items.addAll(nonNull(policy.getRowFilterPolicyItems()));
            default -> {
                items.addAll(nonNull(policy.getDenyPolicyItems()));
                items.addAll(nonNull(policy.getAllowExceptions()));
            }
        }
        for (RangerPolicyItem item : items) {
            if (item.getUsers() != null && item.getUsers().contains(OWNER)) {
                return "owner restriction";
            }
            if (!empty(item.getConditions())) {
                return "conditional restriction";
            }
        }
        return null;
    }

    /**
     * Reads a policy's type, access when unset.
     *
     * @param policy the policy
     * @return policy type
     */
    private static int policyType(RangerPolicy policy) {
        return policy.getPolicyType() == null ? RangerPolicy.POLICY_TYPE_ACCESS : policy.getPolicyType();
    }

    /**
     * Replaces a missing list by an empty one.
     *
     * @param items items, may be null
     * @param <T> item type
     * @return the items
     */
    private static <T> List<T> nonNull(List<T> items) {
        return items == null ? List.of() : items;
    }

    /**
     * Checks for a missing or empty list.
     *
     * @param items items, may be null
     * @return whether there is no item
     */
    private static boolean empty(List<?> items) {
        return items == null || items.isEmpty();
    }
}
