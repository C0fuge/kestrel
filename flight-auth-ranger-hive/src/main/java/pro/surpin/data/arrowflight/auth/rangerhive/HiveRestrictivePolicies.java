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
import java.util.function.Supplier;

/**
 * The Hive service's policies that can restrict single columns of a table (column denials, masks) or that the
 * server cannot evaluate exactly (owner macros, item conditions, column patterns, restrictive tag policies on the
 * tags of the table). They decide which columns must be checked before a table is read whole while the server cannot
 * name its columns, and which tables cannot be read at all.
 */
final class HiveRestrictivePolicies {
    /** A column name no policy names: Ranger matches it only by the {@code *} column value. */
    static final String ANY_COLUMN = "\u0000any column";

    private static final String OWNER = "{OWNER}";

    private final List<Entry> entries;
    private final List<TagEntry> tagEntries;

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
     * One restrictive tag policy.
     *
     * @param policyId policy id, for messages
     * @param tags lower-case tag names it applies to, null when it may apply to any tag (exclusions, patterns)
     */
    private record TagEntry(Long policyId, Set<String> tags) {
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
     * @param tagEntries restrictive tag policies
     */
    private HiveRestrictivePolicies(List<Entry> entries, List<TagEntry> tagEntries) {
        this.entries = entries;
        this.tagEntries = tagEntries;
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
        List<TagEntry> tagEntries = new ArrayList<>();
        for (RangerPolicy policy : tagPolicies) {
            if (restrictive(policy)) {
                tagEntries.add(new TagEntry(policy.getId(), tagNames(policy)));
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
        return new HiveRestrictivePolicies(List.copyOf(entries), List.copyOf(tagEntries));
    }

    /**
     * Finds the columns of a table to check before it is read whole.
     *
     * A restrictive tag policy denies the table when its tag is on the database, the table or one of its columns
     * (its conditions, such as the expiry of Ranger's default {@code EXPIRES_ON} policy, and the columns a tag
     * restricts are not evaluated), or when the tags cannot be read.
     *
     * @param database lower-case database name
     * @param table lower-case table name
     * @param tags reads the tag names of the database, the table and its columns, null when unknown; called
     *     only when a restrictive tag policy exists
     * @return the columns, or why the table cannot be read whole
     */
    Probe probe(String database, String table, Supplier<Set<String>> tags) {
        Set<String> columns = new LinkedHashSet<>();
        columns.add(ANY_COLUMN);
        String tagDenial = tagDenial(tags);
        if (tagDenial != null) {
            return new Probe(columns, tagDenial);
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
     * Finds a restrictive tag policy on one of the tags of a table.
     *
     * @param tags reads the tag names of the database, the table and its columns, null when unknown
     * @return why the table is denied, null when no restrictive tag policy applies
     */
    private String tagDenial(Supplier<Set<String>> tags) {
        if (tagEntries.isEmpty()) {
            return null;
        }
        Set<String> found = tags.get();
        if (found == null) {
            return "restrictive tag policy " + tagEntries.get(0).policyId() + " and the tags are unknown";
        }
        if (found.isEmpty()) {
            return null;
        }
        Set<String> names = new LinkedHashSet<>();
        found.forEach(tag -> names.add(tag.toLowerCase(Locale.ROOT)));
        for (TagEntry entry : tagEntries) {
            if (entry.tags() == null) {
                return "restrictive tag policy " + entry.policyId() + " on any tag";
            }
            for (String tag : entry.tags()) {
                if (names.contains(tag)) {
                    return "restrictive tag policy " + entry.policyId() + " on tag " + tag;
                }
            }
        }
        return null;
    }

    /**
     * Reads the tag names a tag policy applies to.
     *
     * @param policy tag policy
     * @return lower-case tag names, null when it may apply to any tag (no values, an exclusion or a pattern)
     */
    private static Set<String> tagNames(RangerPolicy policy) {
        List<Map<String, RangerPolicyResource>> resourceSets = new ArrayList<>();
        resourceSets.add(policy.getResources());
        if (policy.getAdditionalResources() != null) {
            resourceSets.addAll(policy.getAdditionalResources());
        }
        Set<String> names = new LinkedHashSet<>();
        for (Map<String, RangerPolicyResource> resources : resourceSets) {
            RangerPolicyResource tag = resources == null ? null : resources.get("tag");
            if (tag == null || empty(tag.getValues()) || Boolean.TRUE.equals(tag.getIsExcludes())) {
                return null;
            }
            for (String value : tag.getValues()) {
                if (value == null || value.indexOf('*') >= 0 || value.indexOf('?') >= 0
                        || value.indexOf('{') >= 0) {
                    return null;
                }
                names.add(value.toLowerCase(Locale.ROOT));
            }
        }
        return names;
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
