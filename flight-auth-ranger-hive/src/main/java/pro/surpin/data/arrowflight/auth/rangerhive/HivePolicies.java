package pro.surpin.data.arrowflight.auth.rangerhive;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Evaluates the policies of the cluster's Hive Ranger service (resources database / table / column) the way
 * Ranger's Hive authorizer does for HiveServer2: visibility by any access, select per column, row filters and
 * column masks. Every failure denies.
 */
public final class HivePolicies {
    private static final Logger LOGGER = LoggerFactory.getLogger(HivePolicies.class);
    private static final String SELECT = "select";
    private static final String MASK_TYPE_CUSTOM = "CUSTOM";

    private final RangerBasePlugin plugin;
    private final Function<String, Set<String>> groups;
    private final RangerAccessResultProcessor audit;

    /**
     * Creates an evaluator around a Ranger plugin of the Hive service, without audit.
     *
     * @param plugin Ranger plugin holding the Hive service's policies
     * @param groups groups of an authenticated identity
     */
    public HivePolicies(RangerBasePlugin plugin, Function<String, Set<String>> groups) {
        this(plugin, groups, null);
    }

    /**
     * Creates an evaluator that audits its whole-table decisions.
     *
     * @param plugin Ranger plugin holding the Hive service's policies
     * @param groups groups of an authenticated identity
     * @param audit receives one result per whole-table decision, null for no audit
     */
    public HivePolicies(RangerBasePlugin plugin, Function<String, Set<String>> groups,
            RangerAccessResultProcessor audit) {
        this.plugin = plugin;
        this.groups = groups;
        this.audit = audit;
    }

    /** One Hive table as the policies see it. */
    public record HiveTable(String database, String table, String owner) {
    }

    /**
     * What a user may read of one table.
     *
     * @param allowed whether the table may be read at all
     * @param rowFilter Hive expression rows must satisfy, null when every row is visible
     * @param masks masks of the requested columns that are masked, by lower-case column name
     * @param reason why the table is denied, null when allowed
     */
    public record TableAccess(boolean allowed, String rowFilter, Map<String, HiveMask> masks, String reason) {
        /**
         * Copies the masks.
         *
         * @param allowed whether the table may be read at all
         * @param rowFilter Hive expression rows must satisfy, null when every row is visible
         * @param masks masks of the requested columns that are masked
         * @param reason why the table is denied, null when allowed
         */
        public TableAccess {
            masks = Map.copyOf(masks);
        }

        /**
         * Creates a denial.
         *
         * @param reason why the table is denied
         * @return the denial
         */
        static TableAccess denied(String reason) {
            return new TableAccess(false, null, Map.of(), reason);
        }
    }

    /**
     * Checks whether a user sees a database, as Hive's SHOW DATABASES filter does (any access on the
     * database or on something inside it).
     *
     * @param principal authenticated identity
     * @param database Hive database name
     * @return whether the database is visible
     */
    public boolean canSeeDatabase(String principal, String database) {
        if (blank(database)) {
            return false;
        }
        return allowed(principal, resource(database, null, null, null), RangerPolicyEngine.ANY_ACCESS);
    }

    /**
     * Checks whether a user sees a table, as Hive's SHOW TABLES filter does (any access on the table or one
     * of its columns).
     *
     * @param principal authenticated identity
     * @param table the table
     * @return whether the table is visible
     */
    public boolean canSeeTable(String principal, HiveTable table) {
        if (invalid(table)) {
            return false;
        }
        return allowed(principal, resource(table.database(), table.table(), null, table.owner()),
                RangerPolicyEngine.ANY_ACCESS);
    }

    /**
     * Decides what a user may read of a table: select on every referenced column (on the table when no
     * column is referenced), the row filter and the masks of the referenced columns. A mask that cannot be
     * written exactly denies the table.
     *
     * @param principal authenticated identity
     * @param table the table
     * @param columns referenced column names, empty for a query reading no column (count(*))
     * @return the decision
     */
    public TableAccess readTable(String principal, HiveTable table, List<String> columns) {
        if (invalid(table)) {
            return TableAccess.denied("no table");
        }
        Set<String> names = new LinkedHashSet<>();
        for (String column : columns) {
            if (blank(column)) {
                return TableAccess.denied("empty column name");
            }
            names.add(column.toLowerCase(Locale.ROOT));
        }
        try {
            if (names.isEmpty()) {
                if (!allowed(principal, resource(table.database(), table.table(), null, table.owner()), SELECT)) {
                    return TableAccess.denied("no select on the table");
                }
            } else {
                for (String column : names) {
                    if (!allowed(principal, resource(table.database(), table.table(), column, table.owner()),
                            SELECT)) {
                        return TableAccess.denied("no select on column " + column);
                    }
                }
            }
            RangerAccessResult filter = plugin.evalRowFilterPolicies(
                    request(principal, resource(table.database(), table.table(), null, table.owner()), SELECT),
                    null);
            String rowFilter = filter != null && filter.isRowFilterEnabled() && !blank(filter.getFilterExpr())
                    ? filter.getFilterExpr() : null;
            Map<String, HiveMask> masks = new LinkedHashMap<>();
            for (String column : names) {
                RangerAccessResult mask = plugin.evalDataMaskPolicies(request(principal,
                        resource(table.database(), table.table(), column, table.owner()), SELECT), null);
                if (mask != null && mask.isMaskEnabled()) {
                    masks.put(column, mask(mask, column));
                }
            }
            return new TableAccess(true, rowFilter, masks, null);
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Ranger Hive mask of {}.{} cannot be applied, table denied: {}",
                    table.database(), table.table(), e.getMessage());
            return TableAccess.denied(e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.warn("Ranger Hive policies failed closed for {}.{}", table.database(), table.table(), e);
            return TableAccess.denied("policy evaluation failed");
        }
    }

    /**
     * Decides whether a user may read a table whole without masks or row filters, for a server that cannot name
     * the columns a query reads: select on the table (a policy on all columns), no row filter, and select without
     * a mask on every column a restrictive policy names and on columns only {@code *} policies cover. Policies the
     * server cannot evaluate exactly on the table (owner macros, conditions, column patterns, restrictive tag
     * policies) deny it. Needs a {@link HiveRangerPlugin}. The decision is audited as one select on the table.
     *
     * @param principal authenticated identity
     * @param table the table, its owner unknown
     * @return the decision, never with a row filter or masks
     */
    public TableAccess readWholeTable(String principal, HiveTable table) {
        TableAccess decision = decideWholeTable(principal, table);
        audit(principal, table, decision);
        return decision;
    }

    /**
     * Decides what a user may read of a table whose referenced columns the server names: as
     * {@link #readTable}, after refusing tables with policies that cannot be evaluated exactly without the owner
     * or a client context (see {@link #readWholeTable}). Needs a {@link HiveRangerPlugin}. The decision is
     * audited as one select on the table.
     *
     * @param principal authenticated identity
     * @param table the table, its owner unknown
     * @param columns referenced column names
     * @return the decision, with the row filter and the masks of the referenced columns
     */
    public TableAccess readColumns(String principal, HiveTable table, List<String> columns) {
        TableAccess decision = exactPolicies(table);
        if (decision == null) {
            decision = readTable(principal, table, columns);
        }
        audit(principal, table, decision);
        return decision;
    }

    /**
     * Refuses a table whose restrictive policies cannot be evaluated exactly.
     *
     * @param table the table
     * @return the denial, or null when its policies can be evaluated
     */
    private TableAccess exactPolicies(HiveTable table) {
        if (invalid(table)) {
            return TableAccess.denied("no table");
        }
        try {
            HiveRestrictivePolicies restrictive = plugin instanceof HiveRangerPlugin hive
                    ? hive.restrictivePolicies() : null;
            if (restrictive == null) {
                return TableAccess.denied("Ranger Hive policies not listed");
            }
            String denial = restrictive.probe(table.database().toLowerCase(Locale.ROOT),
                    table.table().toLowerCase(Locale.ROOT)).denial();
            return denial == null ? null : TableAccess.denied(denial);
        } catch (RuntimeException e) {
            LOGGER.warn("Ranger Hive policies failed closed for {}.{}", table.database(), table.table(), e);
            return TableAccess.denied("policy evaluation failed");
        }
    }

    /**
     * Decides whether a user may read a table whole, see {@link #readWholeTable}.
     *
     * @param principal authenticated identity
     * @param table the table, its owner unknown
     * @return the decision, never with a row filter or masks
     */
    private TableAccess decideWholeTable(String principal, HiveTable table) {
        TableAccess base = readTable(principal, table, List.of());
        if (!base.allowed()) {
            return base;
        }
        if (base.rowFilter() != null) {
            return TableAccess.denied("row filter on the table");
        }
        HiveRestrictivePolicies.Probe probe;
        try {
            HiveRestrictivePolicies restrictive = plugin instanceof HiveRangerPlugin hive
                    ? hive.restrictivePolicies() : null;
            if (restrictive == null) {
                return TableAccess.denied("Ranger Hive policies not listed");
            }
            probe = restrictive.probe(table.database().toLowerCase(Locale.ROOT),
                    table.table().toLowerCase(Locale.ROOT));
        } catch (RuntimeException e) {
            LOGGER.warn("Ranger Hive policies failed closed for {}.{}", table.database(), table.table(), e);
            return TableAccess.denied("policy evaluation failed");
        }
        if (probe.denial() != null) {
            return TableAccess.denied(probe.denial());
        }
        TableAccess columns = readTable(principal, table, List.copyOf(probe.columns()));
        if (!columns.allowed()) {
            return columns;
        }
        if (!columns.masks().isEmpty()) {
            String column = columns.masks().keySet().iterator().next();
            return TableAccess.denied("masked column " + (HiveRestrictivePolicies.ANY_COLUMN.equals(column)
                    ? "(any)" : column));
        }
        return new TableAccess(true, null, Map.of(), null);
    }

    /**
     * Audits a whole-table decision as Ranger's result of a select on the table, not of the probe requests
     * behind it (their column names are not the query's). A table denied by a restriction the table-level
     * result does not show (a column denial, a mask, a row filter, an inexact policy) is audited as denied with
     * that reason and no policy id. An audit failure does not change the decision.
     *
     * @param principal authenticated identity
     * @param table the table
     * @param decision the decision
     */
    private void audit(String principal, HiveTable table, TableAccess decision) {
        if (audit == null || invalid(table)) {
            return;
        }
        try {
            RangerAccessResult result = plugin.isAccessAllowed(
                    request(principal, resource(table.database(), table.table(), null, table.owner()), SELECT),
                    null);
            if (result == null) {
                return;
            }
            if (!decision.allowed() && result.getIsAllowed()) {
                result.setIsAllowed(false);
                result.setPolicyId(-1);
                result.setReason(decision.reason());
                result.setIsAudited(true);
            }
            audit.processResult(result);
        } catch (RuntimeException e) {
            LOGGER.warn("Ranger Hive audit failed for {}.{}", table.database(), table.table(), e);
        }
    }

    /**
     * Reads the mask of a data mask result as Hive's authorizer does.
     *
     * @param result enabled data mask result
     * @param column column name
     * @return the mask
     * @throws IllegalArgumentException when the mask cannot be written exactly
     */
    private static HiveMask mask(RangerAccessResult result, String column) {
        String maskType = result.getMaskType();
        if (MASK_TYPE_CUSTOM.equalsIgnoreCase(maskType)) {
            // Hive runs a custom value as an expression; only the mask functions are written, NULL as MASK_NULL.
            String value = result.getMaskedValue();
            if (value == null || "null".equalsIgnoreCase(value.trim())) {
                return HiveMask.parse("MASK_NULL", null);
            }
            return HiveMask.parse(maskType, value);
        }
        RangerServiceDef.RangerDataMaskTypeDef typeDef = result.getMaskTypeDef();
        String transformer = typeDef == null ? null : typeDef.getTransformer();
        if (transformer != null && transformer.contains("{colType}")) {
            throw new IllegalArgumentException("mask transformer with {colType} on column " + column);
        }
        return HiveMask.parse(maskType == null ? null : maskType.toUpperCase(Locale.ROOT), transformer);
    }

    /**
     * Evaluates one access request.
     *
     * @param principal authenticated identity
     * @param resource Hive resource
     * @param access Ranger access type
     * @return whether access is allowed
     */
    private boolean allowed(String principal, RangerAccessResourceImpl resource, String access) {
        try {
            RangerAccessResult result = plugin.isAccessAllowed(request(principal, resource, access), null);
            return result != null && result.getIsAllowed();
        } catch (RuntimeException e) {
            LOGGER.warn("Ranger Hive policies failed closed for {}", resource.getAsString(), e);
            return false;
        }
    }

    /**
     * Builds a request of the authenticated user, its groups and their Ranger roles.
     *
     * @param principal authenticated identity
     * @param resource Hive resource
     * @param access Ranger access type
     * @return the request
     * @throws IllegalStateException when the identity or the policies are missing
     */
    private RangerAccessRequestImpl request(String principal, RangerAccessResourceImpl resource, String access) {
        if (blank(principal)) {
            throw new IllegalStateException("no authenticated identity");
        }
        if (plugin.getServiceDef() == null) {
            throw new IllegalStateException("Ranger Hive policies not loaded");
        }
        String user = UserGroupInformation.createRemoteUser(principal).getShortUserName();
        Set<String> userGroups = groups.apply(principal);
        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
        request.setResource(resource);
        request.setAccessType(access);
        request.setUser(user);
        request.setUserGroups(userGroups);
        Set<String> roles = plugin.getRolesFromUserAndGroups(user, userGroups);
        request.setUserRoles(roles == null ? Set.of() : roles);
        request.setAccessTime(new Date());
        return request;
    }

    /**
     * Builds a Hive resource with lower-case names, as Hive passes them.
     *
     * @param database database name
     * @param table table name, may be null
     * @param column column name, may be null
     * @param owner table owner for owner policies, may be null
     * @return the resource
     */
    private static RangerAccessResourceImpl resource(String database, String table, String column, String owner) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue("database", database.toLowerCase(Locale.ROOT));
        if (table != null) {
            resource.setValue("table", table.toLowerCase(Locale.ROOT));
        }
        if (column != null) {
            resource.setValue("column", column);
        }
        if (!blank(owner)) {
            resource.setOwnerUser(owner);
        }
        return resource;
    }

    /**
     * Checks a table reference.
     *
     * @param table table reference
     * @return whether it lacks a database or table name
     */
    private static boolean invalid(HiveTable table) {
        return table == null || blank(table.database()) || blank(table.table());
    }

    /**
     * Checks for a missing or blank text.
     *
     * @param text text
     * @return whether it is null or blank
     */
    private static boolean blank(String text) {
        return text == null || text.isBlank();
    }
}
