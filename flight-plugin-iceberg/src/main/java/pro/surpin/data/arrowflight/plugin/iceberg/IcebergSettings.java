package pro.surpin.data.arrowflight.plugin.iceberg;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import pro.surpin.data.arrowflight.spi.config.ConfigKey;
import pro.surpin.data.arrowflight.spi.config.PluginConfig;

/**
 * Settings of one Iceberg catalog (namespace {@code iceberg-hadoop} or {@code iceberg-rest}).
 *
 * @param kind catalog kind
 * @param warehouse warehouse directory (Hadoop layout) or warehouse name sent to the REST
 *                  catalog; may be empty for REST
 * @param uri REST catalog URI, empty for the Hadoop layout
 * @param prefix REST path prefix overriding the one the catalog's config names, or empty
 * @param roots normalised roots every table location must lie under
 * @param schemaPrefix text put before each namespace name to make the server's schema name
 * @param namespaces namespaces to show, empty for all
 * @param namesTtlMillis how long lists of namespaces and tables are reused
 * @param metadataTtlMillis how long a table's current metadata file is reused
 * @param planThreads threads that read manifests while a snapshot is planned
 * @param credential OAuth2 client credential "id:secret" (REST), or empty
 * @param token bearer token (REST), or empty
 * @param oauthUri OAuth2 token endpoint, or empty for {@code <uri>/v1/oauth/tokens}
 * @param scope OAuth2 scope
 * @param requestTimeoutMillis REST request timeout
 */
record IcebergSettings(Kind kind, String warehouse, String uri, String prefix, List<String> roots,
        String schemaPrefix, Set<String> namespaces, long namesTtlMillis, long metadataTtlMillis,
        int planThreads, Optional<String> credential, Optional<String> token, String oauthUri,
        String scope, long requestTimeoutMillis) {

    /** Kind of Iceberg catalog. */
    enum Kind {
        /** A warehouse directory: namespace/table/metadata/v&lt;n&gt;.metadata.json (HadoopCatalog). */
        HADOOP("iceberg-hadoop"),
        /** An Iceberg REST catalog (Gravitino, Polaris, Nessie, ...). */
        REST("iceberg-rest");

        private final String providerName;

        Kind(String providerName) {
            this.providerName = providerName;
        }

        /**
         * Returns the provider name, also the configuration namespace.
         *
         * @return provider name
         */
        String providerName() {
            return providerName;
        }
    }

    /**
     * Declares the configuration keys of a catalog kind.
     *
     * @param kind catalog kind
     * @return keys
     */
    static List<ConfigKey> keys(Kind kind) {
        List<ConfigKey> keys = new ArrayList<>();
        keys.add(ConfigKey.of("warehouse", kind == Kind.HADOOP
                ? "warehouse directory (URI or path) holding <namespace>/<table>/metadata"
                : "warehouse name sent to the REST catalog (GET /v1/config?warehouse=)", null));
        keys.add(ConfigKey.of("roots", "comma separated roots every table location lies under"
                + (kind == Kind.HADOOP ? " (default: the warehouse)" : " (required)"), null));
        keys.add(ConfigKey.of("schemaPrefix", "text before each namespace name in the server's"
                + " schema names", ""));
        keys.add(ConfigKey.of("namespaces", "comma separated namespaces to show (default all)", null));
        keys.add(ConfigKey.of("namesTtlSeconds", "seconds lists of namespaces and tables are reused",
                "30"));
        keys.add(ConfigKey.of("metadataTtlMillis", "milliseconds a table's current metadata is"
                + " reused", "5000"));
        keys.add(ConfigKey.of("planThreads", "threads that read manifests", "4"));
        if (kind == Kind.REST) {
            keys.add(ConfigKey.of("uri", "REST catalog URI, for example http://gravitino:9001/iceberg",
                    null));
            keys.add(ConfigKey.of("prefix", "path prefix (default: what GET /v1/config names)", null));
            keys.add(ConfigKey.secret("credential", "OAuth2 client credential id:secret"));
            keys.add(ConfigKey.secret("token", "bearer token"));
            keys.add(ConfigKey.of("oauthUri", "OAuth2 token endpoint (default <uri>/v1/oauth/tokens)",
                    null));
            keys.add(ConfigKey.of("scope", "OAuth2 scope", "catalog"));
            keys.add(ConfigKey.of("requestTimeoutSeconds", "REST request timeout", "30"));
        }
        return List.copyOf(keys);
    }

    /**
     * Reads the settings of a catalog kind.
     *
     * @param kind catalog kind
     * @param config configuration of the kind's namespace
     * @return settings
     * @throws IllegalArgumentException when a required key is missing or a value is invalid
     */
    static IcebergSettings from(Kind kind, PluginConfig config) {
        String ns = kind.providerName();
        String warehouse = config.get("warehouse", "");
        String uri = config.get("uri", "");
        List<String> roots = Locations.roots(config.get("roots", ""));
        if (kind == Kind.HADOOP) {
            if (warehouse.isBlank()) {
                throw new IllegalArgumentException(ns + ".warehouse is required (arrowflight." + ns
                        + ".warehouse or " + ConfigKey.environmentName(ns, "warehouse") + ")");
            }
            if (roots.isEmpty()) {
                roots = List.of(Locations.normalize(warehouse));
            }
        } else {
            if (uri.isBlank()) {
                throw new IllegalArgumentException(ns + ".uri is required");
            }
            if (roots.isEmpty()) {
                throw new IllegalArgumentException(ns + ".roots is required: the storage roots of"
                        + " the catalog's tables, for example s3://lake/warehouse");
            }
        }
        Set<String> namespaces = new java.util.LinkedHashSet<>();
        for (String part : config.get("namespaces", "").split(",")) {
            if (!part.isBlank()) {
                namespaces.add(part.strip());
            }
        }
        return new IcebergSettings(kind, warehouse.strip(), stripSlash(uri.strip()),
                config.get("prefix", ""), List.copyOf(roots), config.get("schemaPrefix", ""),
                Set.copyOf(namespaces), 1000L * positive(config, "namesTtlSeconds", 30),
                positive(config, "metadataTtlMillis", 5000), positive(config, "planThreads", 4),
                kind == Kind.REST ? config.secret("credential") : Optional.empty(),
                kind == Kind.REST ? config.secret("token") : Optional.empty(),
                config.get("oauthUri", ""), config.get("scope", "catalog"),
                1000L * positive(config, "requestTimeoutSeconds", 30));
    }

    /**
     * Reads a non-negative integer.
     *
     * @param config configuration
     * @param key key
     * @param fallback default
     * @return value
     */
    private static int positive(PluginConfig config, String key, int fallback) {
        int value = config.getInt(key, fallback);
        if (value < 0) {
            throw new IllegalArgumentException(config.namespace() + "." + key + " must not be negative");
        }
        return value;
    }

    /**
     * Removes trailing slashes.
     *
     * @param value text
     * @return text without trailing slashes
     */
    private static String stripSlash(String value) {
        String result = value;
        while (result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    @Override
    public String toString() {
        // Credentials stay out of logs.
        return "IcebergSettings[kind=" + kind + ", warehouse=" + warehouse + ", uri=" + uri
                + ", roots=" + roots + ", schemaPrefix=" + schemaPrefix + ", namespaces=" + namespaces
                + ", credential=" + (credential.isPresent() ? "***" : "none")
                + ", token=" + (token.isPresent() ? "***" : "none") + "]";
    }
}
