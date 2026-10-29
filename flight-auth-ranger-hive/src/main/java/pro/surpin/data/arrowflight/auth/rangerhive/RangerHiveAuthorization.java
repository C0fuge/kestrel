package pro.surpin.data.arrowflight.auth.rangerhive;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import pro.surpin.data.arrowflight.server.FlightAuthorization;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

/**
 * Checks the server's Hive catalog schemas and tables against the policies of the cluster's Hive Ranger service.
 * The server cannot yet apply masks or row filters or name the columns a query reads, so a table is readable only
 * when the caller may read all of it unmasked and unfiltered; schemas outside the Hive catalog are refused.
 */
public final class RangerHiveAuthorization implements FlightAuthorization {
    private static final String SERVICE_TYPE = "hive";

    private final HivePolicies policies;
    private final String schemaPrefix;
    private final AutoCloseable onClose;

    /**
     * Creates the provider.
     *
     * @param policies Hive policy evaluator
     * @param schemaPrefix text before each Hive database name in the server's schema names
     * @param onClose releases the Ranger plugin
     */
    RangerHiveAuthorization(HivePolicies policies, String schemaPrefix, AutoCloseable onClose) {
        this.policies = policies;
        this.schemaPrefix = schemaPrefix;
        this.onClose = onClose;
    }

    /**
     * Creates the provider and its Ranger plugin from deployment settings.
     *
     * @param environment deployment settings
     * @param providerGroups groups the authentication provider knows for an identity
     * @return the provider
     * @throws IllegalArgumentException when a required setting is missing
     */
    static RangerHiveAuthorization fromSettings(Map<String, String> environment,
            Function<String, Set<String>> providerGroups) {
        String serviceName = required(environment, "RANGER_HIVE_SERVICE_NAME");
        String adminUrl = required(environment, "RANGER_ADMIN_URL");
        Properties properties = new Properties();
        String prefix = "ranger.plugin." + SERVICE_TYPE;
        properties.setProperty(prefix + ".service.name", serviceName);
        properties.setProperty(prefix + ".policy.rest.url", adminUrl);
        properties.setProperty(prefix + ".policy.pollIntervalMs", "30000");
        optional(environment, "RANGER_POLICY_REST_USERNAME", properties, prefix + ".policy.rest.client.username");
        optional(environment, "RANGER_POLICY_REST_PASSWORD", properties, prefix + ".policy.rest.client.password");
        optional(environment, "RANGER_POLICY_CACHE_DIR", properties, prefix + ".policy.cache.dir");
        HiveRangerPlugin plugin = new HiveRangerPlugin(new RangerPluginConfig(
                SERVICE_TYPE, serviceName, "arrow-flight-server", properties));
        plugin.init();
        Function<String, Set<String>> groups = union(providerGroups, principal -> Set.copyOf(
                UserGroupInformation.createRemoteUser(principal).getGroups()));
        return new RangerHiveAuthorization(new HivePolicies(plugin, groups),
                environment.getOrDefault("RANGER_HIVE_SCHEMA_PREFIX", ""), plugin::cleanup);
    }

    /**
     * Combines the authentication provider's groups with the Hadoop group mapping, as the built-in Ranger
     * provider does; a failure of either fails the lookup.
     *
     * @param providerGroups groups from the authentication provider
     * @param mappedGroups groups from the Hadoop group mapping
     * @return resolver of both groups
     */
    static Function<String, Set<String>> union(Function<String, Set<String>> providerGroups,
            Function<String, Set<String>> mappedGroups) {
        return principal -> {
            Set<String> all = new HashSet<>(providerGroups.apply(principal));
            all.addAll(mappedGroups.apply(principal));
            return Set.copyOf(all);
        };
    }

    @Override
    public boolean canSeeSchema(String principal, String schema) {
        String database = database(schema);
        return database != null && policies.canSeeDatabase(principal, database);
    }

    @Override
    public boolean canReadTable(String principal, String schema, String table) {
        String database = database(schema);
        return database != null && table != null && policies.canSeeDatabase(principal, database)
                && policies.readWholeTable(principal, new HivePolicies.HiveTable(database, table, null)).allowed();
    }

    /**
     * Maps a server schema to its Hive database.
     *
     * @param schema server schema name
     * @return the database, null when the schema is not one of the Hive catalog's
     */
    private String database(String schema) {
        if (schema == null || !schema.startsWith(schemaPrefix) || schema.length() == schemaPrefix.length()) {
            return null;
        }
        return schema.substring(schemaPrefix.length());
    }

    /**
     * Reads a required setting.
     *
     * @param environment deployment settings
     * @param key setting name
     * @return the value
     * @throws IllegalArgumentException when it is missing or blank
     */
    private static String required(Map<String, String> environment, String key) {
        String value = environment.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(key + " is required for the ranger-hive policy provider");
        }
        return value;
    }

    /**
     * Copies an optional setting into the plugin properties.
     *
     * @param environment deployment settings
     * @param key setting name
     * @param properties plugin properties
     * @param property property name
     */
    private static void optional(Map<String, String> environment, String key, Properties properties,
            String property) {
        String value = environment.get(key);
        if (value != null && !value.isBlank()) {
            properties.setProperty(property, value);
        }
    }

    @Override
    public void close() {
        try {
            onClose.close();
        } catch (Exception e) {
            throw new IllegalStateException("Ranger Hive plugin cleanup failed", e);
        }
    }
}
