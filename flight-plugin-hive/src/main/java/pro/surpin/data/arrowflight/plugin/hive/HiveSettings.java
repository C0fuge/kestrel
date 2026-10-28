package pro.surpin.data.arrowflight.plugin.hive;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import pro.surpin.data.arrowflight.spi.config.ConfigKey;
import pro.surpin.data.arrowflight.spi.config.PluginConfig;

/**
 * Settings of the Hive Metastore catalog (namespace {@code hive}: {@code arrowflight.hive.uris},
 * {@code FLIGHT_HIVE_URIS}, ...).
 *
 * @param uris metastore Thrift URIs, tried in order ({@code thrift://host:9083})
 * @param roots normalised roots every table and partition location must lie under
 * @param schemaPrefix text put before each database name to make the server's schema name
 * @param databases databases to show, empty for all
 * @param namesTtlMillis how long lists of databases and tables are reused
 * @param metadataTtlMillis how long a table's planned snapshot (table, partitions, files) is reused
 * @param connectTimeoutMillis Thrift connect timeout
 * @param socketTimeoutMillis Thrift read timeout
 * @param maxConnections metastore connections kept open
 * @param framedTransport whether the metastore uses the framed Thrift transport
 * @param authentication how the client authenticates: none or kerberos (SASL/GSSAPI)
 * @param recursiveDirectories whether files in subdirectories of a location count
 * @param partitionBatchSize partitions fetched per metastore call
 * @param listThreads threads that list partition directories
 * @param icebergTables what to do with Iceberg tables registered in the metastore
 * @param orcTables whether ORC tables are shown, read by the plugin and handed over as Arrow
 * @param textTables whether text tables (LazySimpleSerDe, OpenCSVSerde, JsonSerDe) are shown,
 *     read like ORC tables
 * @param avroTables whether Avro tables (AvroSerDe) are shown, read like ORC tables
 * @param defaultPartitionName the partition value that stands for NULL
 * @param kerberos Kerberos settings, null without Kerberos
 */
record HiveSettings(List<String> uris, List<String> roots, String schemaPrefix,
        Set<String> databases, long namesTtlMillis, long metadataTtlMillis,
        int connectTimeoutMillis, int socketTimeoutMillis, int maxConnections,
        boolean framedTransport, String authentication, boolean recursiveDirectories,
        int partitionBatchSize, int listThreads, IcebergTables icebergTables,
        boolean orcTables, boolean textTables, boolean avroTables, String defaultPartitionName,
        Kerberos kerberos) {

    /** SASL protection levels the client accepts by default: the strongest the metastore offers. */
    static final String DEFAULT_QOP = "auth-conf,auth-int,auth";

    /**
     * Kerberos settings of the metastore client.
     *
     * @param metastorePrincipal the metastore's principal, {@code hive/_HOST@REALM}
     * @param clientPrincipal the client's principal, null to take the ticket cache's
     * @param clientKeytab the client's keytab, null for the ticket cache
     * @param clientTicketCache the ticket cache, null for {@code KRB5CCNAME} or the default
     * @param qop SASL quality of protection values, in order of preference
     */
    record Kerberos(String metastorePrincipal, String clientPrincipal, String clientKeytab,
            String clientTicketCache, String qop) {
    }

    /** Provider name, also the configuration namespace. */
    static final String NAME = "hive";

    /** What happens to Iceberg tables registered in the metastore. */
    enum IcebergTables {
        /** Read through the iceberg plugin's table format when that plugin is installed. */
        AUTO,
        /** Read through the iceberg plugin; a startup error without it. */
        REQUIRE,
        /** Never shown. */
        SKIP
    }

    /**
     * Declares the configuration keys.
     *
     * @return keys
     */
    static List<ConfigKey> keys() {
        List<ConfigKey> keys = new ArrayList<>();
        keys.add(ConfigKey.of("uris", "comma separated metastore URIs, thrift://host:9083", null));
        keys.add(ConfigKey.of("roots", "comma separated roots every table and partition location"
                + " lies under (required), for example hdfs://ns/warehouse", null));
        keys.add(ConfigKey.of("schemaPrefix", "text before each database name in the server's"
                + " schema names", ""));
        keys.add(ConfigKey.of("databases", "comma separated databases to show (default all)", null));
        keys.add(ConfigKey.of("namesTtlSeconds", "seconds lists of databases and tables are reused",
                "60"));
        keys.add(ConfigKey.of("metadataTtlSeconds", "seconds a table's partitions and files are"
                + " reused", "30"));
        keys.add(ConfigKey.of("connectTimeoutMillis", "metastore connect timeout", "10000"));
        keys.add(ConfigKey.of("socketTimeoutMillis", "metastore read timeout", "60000"));
        keys.add(ConfigKey.of("maxConnections", "metastore connections kept open", "4"));
        keys.add(ConfigKey.of("framedTransport", "whether the metastore uses the framed transport",
                "false"));
        keys.add(ConfigKey.of("authentication", "none | kerberos (SASL/GSSAPI)", "none"));
        keys.add(ConfigKey.of("metastorePrincipal", "with kerberos: the metastore's principal,"
                + " _HOST stands for the host of each URI, for example hive/_HOST@EXAMPLE.COM", null));
        keys.add(ConfigKey.of("clientPrincipal", "with kerberos: the server's principal (required"
                + " with clientKeytab; default the ticket cache's)", null));
        keys.add(ConfigKey.of("clientKeytab", "with kerberos: keytab of clientPrincipal (default:"
                + " the ticket cache, KRB5CCNAME, kept fresh by kinit)", null));
        keys.add(ConfigKey.of("clientTicketCache", "with kerberos and no clientKeytab: ticket cache"
                + " file (default KRB5CCNAME or the default cache)", null));
        keys.add(ConfigKey.of("saslQop", "with kerberos: SASL quality of protection, auth |"
                + " auth-int | auth-conf, comma separated in order of preference", DEFAULT_QOP));
        keys.add(ConfigKey.of("recursiveDirectories", "whether files in subdirectories of a table"
                + " or partition location count", "false"));
        keys.add(ConfigKey.of("partitionBatchSize", "partitions fetched per metastore call", "500"));
        keys.add(ConfigKey.of("listThreads", "threads that list partition directories", "8"));
        keys.add(ConfigKey.of("icebergTables", "auto | require | skip: Iceberg tables in the"
                + " metastore read through the iceberg plugin", "auto"));
        keys.add(ConfigKey.of("orcTables", "whether ORC tables are shown (read by the plugin;"
                + " transactional ORC tables stay skipped)", "false"));
        keys.add(ConfigKey.of("textTables", "whether text tables of Hive's default serde"
                + " (LazySimpleSerDe, STORED AS TEXTFILE), OpenCSVSerde and JsonSerDe are shown"
                + " (read by the plugin)", "false"));
        keys.add(ConfigKey.of("avroTables", "whether Avro tables (AvroSerDe, STORED AS AVRO) are"
                + " shown (read by the plugin)", "false"));
        keys.add(ConfigKey.of("defaultPartitionName", "partition value that stands for NULL",
                "__HIVE_DEFAULT_PARTITION__"));
        return List.copyOf(keys);
    }

    /**
     * Reads the settings.
     *
     * @param config configuration of namespace hive
     * @return settings
     * @throws IllegalArgumentException when a required key is missing or a value is invalid
     */
    static HiveSettings from(PluginConfig config) {
        List<String> uris = new ArrayList<>();
        for (String part : config.get("uris", "").split(",")) {
            if (!part.isBlank()) {
                uris.add(checkedUri(part.strip()));
            }
        }
        if (uris.isEmpty()) {
            throw new IllegalArgumentException("hive.uris is required (arrowflight.hive.uris or "
                    + ConfigKey.environmentName(NAME, "uris") + "), for example thrift://hms:9083");
        }
        List<String> roots = HiveLocations.roots(config.get("roots", ""));
        if (roots.isEmpty()) {
            throw new IllegalArgumentException("hive.roots is required: the storage roots of the"
                    + " metastore's tables, for example hdfs://ns/warehouse (tables and partitions"
                    + " elsewhere are hidden)");
        }
        String authentication = config.get("authentication", "none").strip().toLowerCase(Locale.ROOT);
        Kerberos kerberos = switch (authentication) {
            case "none" -> null;
            case "kerberos" -> kerberos(config);
            default -> throw new IllegalArgumentException("hive.authentication must be none or"
                    + " kerberos: " + authentication);
        };
        IcebergTables iceberg;
        try {
            iceberg = IcebergTables.valueOf(config.get("icebergTables", "auto").strip()
                    .toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("hive.icebergTables must be auto, require or skip: "
                    + config.get("icebergTables", ""), e);
        }
        Set<String> databases = new LinkedHashSet<>();
        for (String part : config.get("databases", "").split(",")) {
            if (!part.isBlank()) {
                databases.add(part.strip().toLowerCase(Locale.ROOT));
            }
        }
        return new HiveSettings(List.copyOf(uris), roots, config.get("schemaPrefix", ""),
                Set.copyOf(databases), 1000L * atLeast(config, "namesTtlSeconds", 60, 0),
                1000L * atLeast(config, "metadataTtlSeconds", 30, 0),
                atLeast(config, "connectTimeoutMillis", 10000, 1),
                atLeast(config, "socketTimeoutMillis", 60000, 1),
                atLeast(config, "maxConnections", 4, 1), config.getBoolean("framedTransport", false),
                authentication, config.getBoolean("recursiveDirectories", false),
                atLeast(config, "partitionBatchSize", 500, 1), atLeast(config, "listThreads", 8, 1),
                iceberg, config.getBoolean("orcTables", false),
                config.getBoolean("textTables", false), config.getBoolean("avroTables", false),
                config.get("defaultPartitionName", "__HIVE_DEFAULT_PARTITION__"), kerberos);
    }

    /**
     * Reads the Kerberos settings.
     *
     * @param config configuration of namespace hive
     * @return settings
     * @throws IllegalArgumentException when a value is missing or invalid
     */
    private static Kerberos kerberos(PluginConfig config) {
        String metastorePrincipal = blankToNull(config.get("metastorePrincipal", ""));
        if (metastorePrincipal == null) {
            throw new IllegalArgumentException("hive.metastorePrincipal is required with"
                    + " hive.authentication=kerberos, for example hive/_HOST@EXAMPLE.COM (the"
                    + " metastore's hive.metastore.kerberos.principal)");
        }
        KerberosLogin.checkedPrincipal(metastorePrincipal);
        String clientPrincipal = blankToNull(config.get("clientPrincipal", ""));
        String clientKeytab = blankToNull(config.get("clientKeytab", ""));
        String clientTicketCache = blankToNull(config.get("clientTicketCache", ""));
        if (clientKeytab != null && clientTicketCache != null) {
            throw new IllegalArgumentException("hive.clientKeytab and hive.clientTicketCache"
                    + " exclude each other");
        }
        if (clientKeytab != null && clientPrincipal == null) {
            throw new IllegalArgumentException("hive.clientPrincipal is required with"
                    + " hive.clientKeytab");
        }
        if (config.getBoolean("framedTransport", false)) {
            throw new IllegalArgumentException("hive.framedTransport=true does not go with"
                    + " hive.authentication=kerberos: a Kerberos metastore uses the SASL transport");
        }
        List<String> qop = new ArrayList<>();
        for (String part : config.get("saslQop", DEFAULT_QOP).split(",")) {
            String value = part.strip().toLowerCase(Locale.ROOT);
            if (!value.isEmpty()) {
                if (!Set.of("auth", "auth-int", "auth-conf").contains(value)) {
                    throw new IllegalArgumentException("hive.saslQop takes auth, auth-int and"
                            + " auth-conf: " + value);
                }
                qop.add(value);
            }
        }
        if (qop.isEmpty()) {
            throw new IllegalArgumentException("hive.saslQop is empty");
        }
        return new Kerberos(metastorePrincipal, clientPrincipal, clientKeytab, clientTicketCache,
                String.join(",", qop));
    }

    /**
     * Returns a stripped value, null when blank.
     *
     * @param value value
     * @return stripped value or null
     */
    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.strip();
    }

    /**
     * Checks a metastore URI.
     *
     * @param uri {@code thrift://host:port}
     * @return the URI
     * @throws IllegalArgumentException when it is not one
     */
    private static String checkedUri(String uri) {
        URI parsed;
        try {
            parsed = URI.create(uri);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("hive.uris: bad URI " + uri, e);
        }
        if (!"thrift".equalsIgnoreCase(parsed.getScheme()) || parsed.getHost() == null) {
            throw new IllegalArgumentException("hive.uris: " + uri + " is not thrift://host:port");
        }
        return uri;
    }

    /**
     * Reads an integer with a lower bound.
     *
     * @param config configuration
     * @param key key
     * @param fallback default
     * @param min smallest value
     * @return value
     */
    private static int atLeast(PluginConfig config, String key, int fallback, int min) {
        int value = config.getInt(key, fallback);
        if (value < min) {
            throw new IllegalArgumentException("hive." + key + " must be at least " + min);
        }
        return value;
    }

    @Override
    public String toString() {
        return "HiveSettings{uris=" + uris + ", roots=" + roots + ", schemaPrefix='" + schemaPrefix
                + "', databases=" + databases + ", namesTtlMillis=" + namesTtlMillis
                + ", metadataTtlMillis=" + metadataTtlMillis + ", maxConnections=" + maxConnections
                + ", framed=" + framedTransport + ", authentication=" + authentication
                + (kerberos == null ? "" : ", metastorePrincipal=" + kerberos.metastorePrincipal()
                        + ", clientPrincipal=" + kerberos.clientPrincipal() + ", clientKeytab="
                        + kerberos.clientKeytab() + ", clientTicketCache="
                        + kerberos.clientTicketCache() + ", saslQop=" + kerberos.qop())
                + ", recursive=" + recursiveDirectories + ", icebergTables=" + icebergTables
                + ", orcTables=" + orcTables + ", textTables=" + textTables
                + ", avroTables=" + avroTables + "}";
    }
}
