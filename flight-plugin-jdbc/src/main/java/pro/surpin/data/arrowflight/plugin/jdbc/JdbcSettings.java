package pro.surpin.data.arrowflight.plugin.jdbc;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import pro.surpin.data.arrowflight.spi.config.ConfigKey;
import pro.surpin.data.arrowflight.spi.config.PluginConfig;

/**
 * Settings of the jdbc catalog ({@code arrowflight.jdbc.*}): the remote databases
 * ({@code sources}) and, per source {@code <s>}, {@code <s>.type} (postgres or mysql),
 * {@code <s>.host}, {@code <s>.port}, {@code <s>.database}, {@code <s>.user}, the password from
 * {@code FLIGHT_JDBC_<S>_PASSWORD} or the file named by {@code <s>.passwordFile} (never a
 * property), {@code <s>.tables} (the remote tables the server shows, {@code schema.table}, comma
 * separated), {@code <s>.schema} (server schema name, default {@code <s>}) and for mysql
 * {@code <s>.sslMode}. Only the listed tables are shown; no remote catalog is listed.
 *
 * @param sources configured sources, in order
 */
record JdbcSettings(List<Source> sources) {

    /** Source names, server schema names and remote identifiers. */
    private static final Pattern NAME = Pattern.compile("[a-z][a-z0-9_]{0,40}");
    /** A remote schema or table name: letters, digits, underscore, dollar, as usual unquoted. */
    private static final Pattern REMOTE = Pattern.compile("[A-Za-z_][A-Za-z0-9_$]{0,62}");
    /** A host name or address. */
    private static final Pattern HOST = Pattern.compile("[A-Za-z0-9._:\\[\\]-]{1,253}");
    /** Prefix of the attachment names; schemas may not use it. */
    static final String ATTACHMENT_PREFIX = "jdbc_attached_";

    /**
     * One remote table.
     *
     * @param schema remote schema
     * @param table remote table
     */
    record RemoteTable(String schema, String table) {
    }

    /**
     * One remote database.
     *
     * @param name source name
     * @param type postgres or mysql
     * @param host host
     * @param port port, or null for the default
     * @param database database name
     * @param user user
     * @param password password, or null
     * @param sslMode mysql ssl mode, or null
     * @param schema server schema name
     * @param tables server table name to the remote table, in order
     */
    record Source(String name, String type, String host, Integer port, String database, String user,
            String password, String sslMode, String schema, Map<String, RemoteTable> tables) {

        /**
         * Returns the attachment name of the source.
         *
         * @return attachment name
         */
        String attachment() {
            return ATTACHMENT_PREFIX + name;
        }

        /**
         * Returns the extension that reads the source.
         *
         * @return extension name
         */
        String extension() {
            return "postgres".equals(type) ? "postgres_scanner" : "mysql_scanner";
        }

        @Override
        public String toString() {
            // The password never appears.
            return "Source[" + name + ", " + type + ", " + host + (port == null ? "" : ":" + port)
                    + "/" + database + ", user=" + user + ", password=" + (password == null ? "none" : "***")
                    + ", schema=" + schema + ", tables=" + tables.keySet() + "]";
        }
    }

    /**
     * Declares the keys with fixed names.
     *
     * @return configuration keys
     */
    static List<ConfigKey> keys() {
        return List.of(ConfigKey.of("sources", "comma separated names of the remote databases;"
                + " per source <s>: <s>.type (postgres|mysql), <s>.host, <s>.port, <s>.database,"
                + " <s>.user, password from FLIGHT_JDBC_<S>_PASSWORD or <s>.passwordFile,"
                + " <s>.tables (schema.table, comma separated), <s>.schema, <s>.sslMode (mysql)", null));
    }

    /**
     * Reads the settings.
     *
     * @param config plugin configuration
     * @return settings
     * @throws IllegalArgumentException naming the key of a missing or invalid value
     */
    static JdbcSettings from(PluginConfig config) {
        String list = config.get("sources").orElseThrow(() -> new IllegalArgumentException(
                "jdbc.sources is not set: name at least one remote database"));
        List<Source> sources = new ArrayList<>();
        Set<String> schemas = new LinkedHashSet<>();
        for (String raw : list.split(",")) {
            String name = raw.strip();
            if (name.isEmpty()) {
                continue;
            }
            require(NAME.matcher(name).matches(), "jdbc.sources", name, "a lower-case identifier");
            if (sources.stream().anyMatch(s -> s.name().equals(name))) {
                throw new IllegalArgumentException("jdbc.sources names " + name + " twice");
            }
            Source source = source(config, name);
            if (!schemas.add(source.schema())) {
                throw new IllegalArgumentException("jdbc." + name + ".schema " + source.schema()
                        + " is the schema of another source");
            }
            sources.add(source);
        }
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("jdbc.sources names no remote database");
        }
        return new JdbcSettings(List.copyOf(sources));
    }

    /**
     * Reads one source.
     *
     * @param config plugin configuration
     * @param name source name
     * @return source
     */
    private static Source source(PluginConfig config, String name) {
        String type = required(config, name, "type").toLowerCase(Locale.ROOT);
        require("postgres".equals(type) || "mysql".equals(type), "jdbc." + name + ".type", type,
                "postgres or mysql");
        String host = required(config, name, "host");
        require(HOST.matcher(host).matches(), "jdbc." + name + ".host", host, "a host name or address");
        Integer port = null;
        Optional<String> portText = config.get(name + ".port");
        if (portText.isPresent() && !portText.get().isBlank()) {
            String text = portText.get().strip();
            require(text.matches("[0-9]{1,5}") && Integer.parseInt(text) > 0
                    && Integer.parseInt(text) < 65536, "jdbc." + name + ".port", text, "a port number");
            port = Integer.parseInt(text);
        }
        String database = required(config, name, "database");
        String user = required(config, name, "user");
        String password = config.secret(name + ".password").orElse(null);
        String sslMode = config.get(name + ".sslMode").map(String::strip).filter(v -> !v.isEmpty())
                .orElse(null);
        if (sslMode != null) {
            require("mysql".equals(type), "jdbc." + name + ".sslMode", sslMode, "set for mysql only");
            require(sslMode.matches("[a-z_]{1,32}"), "jdbc." + name + ".sslMode", sslMode,
                    "a mysql ssl mode such as required");
        }
        String schema = config.get(name + ".schema").map(String::strip).filter(v -> !v.isEmpty())
                .orElse(name);
        require(NAME.matcher(schema).matches() && !schema.startsWith(ATTACHMENT_PREFIX),
                "jdbc." + name + ".schema", schema, "a lower-case identifier not starting with "
                        + ATTACHMENT_PREFIX);
        Map<String, RemoteTable> tables = new LinkedHashMap<>();
        for (String raw : required(config, name, "tables").split(",")) {
            String entry = raw.strip();
            if (entry.isEmpty()) {
                continue;
            }
            String[] parts = entry.split("\\.", -1);
            require(parts.length == 2 && REMOTE.matcher(parts[0]).matches()
                    && REMOTE.matcher(parts[1]).matches(), "jdbc." + name + ".tables", entry,
                    "schema.table");
            String serverName = parts[1].toLowerCase(Locale.ROOT);
            if (tables.containsKey(serverName)) {
                throw new IllegalArgumentException("jdbc." + name + ".tables names two tables "
                        + serverName + " (server table names are the remote names in lower case)");
            }
            tables.put(serverName, new RemoteTable(parts[0], parts[1]));
        }
        require(!tables.isEmpty(), "jdbc." + name + ".tables", "", "at least one schema.table");
        return new Source(name, type, host, port, database, user, password, sslMode, schema,
                java.util.Collections.unmodifiableMap(tables));
    }

    /**
     * Reads a required key of a source.
     *
     * @param config configuration
     * @param source source name
     * @param key key below the source
     * @return trimmed value
     */
    private static String required(PluginConfig config, String source, String key) {
        String value = config.get(source + "." + key).map(String::strip).orElse("");
        if (value.isEmpty()) {
            throw new IllegalArgumentException("jdbc." + source + "." + key + " is not set");
        }
        return value;
    }

    /**
     * Refuses an invalid value.
     *
     * @param ok whether the value is valid
     * @param key full key
     * @param value value
     * @param expected what is expected
     */
    private static void require(boolean ok, String key, String value, String expected) {
        if (!ok) {
            throw new IllegalArgumentException(key + " = '" + value + "' is invalid: expected "
                    + expected);
        }
    }
}
