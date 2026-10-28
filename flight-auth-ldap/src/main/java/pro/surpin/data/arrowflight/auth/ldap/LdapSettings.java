package pro.surpin.data.arrowflight.auth.ldap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Validated settings of the LDAP authentication provider, read from {@code FLIGHT_LDAP_*}.
 *
 * @param servers directory servers, tried in order
 * @param startTls whether plain {@code ldap://} connections are upgraded with StartTLS
 * @param userDnTemplate bind name with {@code {0}} for the user name, or null in search mode
 * @param searchBase base DN of the user search, or null in template mode
 * @param searchFilter user search filter with {@code {0}} for the escaped user name
 * @param userAttribute attribute of the found entry that names the identity, or null for the login name
 * @param bindDn service account for the user search, or null for an anonymous search
 * @param bindPassword service account password, or null
 * @param trustStore trust store file for TLS, or null for the JVM default trust
 * @param trustStoreType trust store type
 * @param trustStorePassword trust store password, or null
 * @param connectTimeoutMillis connect timeout per server
 * @param responseTimeoutMillis timeout of one LDAP operation
 * @param sessionSecret HMAC key of the session tokens, shared by all nodes
 * @param sessionMillis session token lifetime
 */
record LdapSettings(List<Server> servers, boolean startTls, String userDnTemplate, String searchBase,
        String searchFilter, String userAttribute, String bindDn, String bindPassword, Path trustStore,
        String trustStoreType, String trustStorePassword, int connectTimeoutMillis,
        int responseTimeoutMillis, byte[] sessionSecret, long sessionMillis) {

    /** Default search filter: the user name as {@code uid}. */
    static final String DEFAULT_FILTER = "(uid={0})";

    /**
     * One directory server address.
     *
     * @param host host name
     * @param port port
     * @param ssl whether the connection is LDAPS
     */
    record Server(String host, int port, boolean ssl) {
    }

    /**
     * Reads and validates the provider settings.
     *
     * @param environment deployment settings
     * @return validated settings
     * @throws IllegalArgumentException when a setting is missing, malformed or insecure
     */
    static LdapSettings fromEnvironment(Map<String, String> environment) {
        List<Server> servers = servers(required(environment, "FLIGHT_LDAP_URL"));
        boolean startTls = flag(environment, "FLIGHT_LDAP_START_TLS");
        boolean plaintext = flag(environment, "FLIGHT_LDAP_ALLOW_PLAINTEXT");
        for (Server server : servers) {
            if (server.ssl() && startTls) {
                throw new IllegalArgumentException("FLIGHT_LDAP_START_TLS cannot be used with ldaps:// URLs");
            }
            if (!server.ssl() && !startTls && !plaintext) {
                throw new IllegalArgumentException("LDAP passwords would travel in clear text to "
                        + server.host() + ": use ldaps://, FLIGHT_LDAP_START_TLS=true, or (labs only) "
                        + "FLIGHT_LDAP_ALLOW_PLAINTEXT=true");
            }
        }
        String template = optional(environment, "FLIGHT_LDAP_USER_DN_TEMPLATE");
        String base = optional(environment, "FLIGHT_LDAP_USER_SEARCH_BASE");
        if ((template == null) == (base == null)) {
            throw new IllegalArgumentException("Set exactly one of FLIGHT_LDAP_USER_DN_TEMPLATE and "
                    + "FLIGHT_LDAP_USER_SEARCH_BASE");
        }
        if (template != null && !template.contains("{0}")) {
            throw new IllegalArgumentException("FLIGHT_LDAP_USER_DN_TEMPLATE must contain {0}");
        }
        String filter = optional(environment, "FLIGHT_LDAP_USER_SEARCH_FILTER");
        if (filter == null) {
            filter = DEFAULT_FILTER;
        }
        if (base != null && !filter.contains("{0}")) {
            throw new IllegalArgumentException("FLIGHT_LDAP_USER_SEARCH_FILTER must contain {0}");
        }
        String attribute = optional(environment, "FLIGHT_LDAP_USER_ATTRIBUTE");
        String bindDn = optional(environment, "FLIGHT_LDAP_BIND_DN");
        String bindPassword = secret(environment, "FLIGHT_LDAP_BIND_PASSWORD");
        if (template != null && (attribute != null || bindDn != null
                || optional(environment, "FLIGHT_LDAP_USER_SEARCH_FILTER") != null)) {
            throw new IllegalArgumentException("FLIGHT_LDAP_USER_SEARCH_FILTER, FLIGHT_LDAP_USER_ATTRIBUTE "
                    + "and FLIGHT_LDAP_BIND_DN belong to search mode (FLIGHT_LDAP_USER_SEARCH_BASE)");
        }
        if ((bindDn == null) != (bindPassword == null)) {
            throw new IllegalArgumentException("FLIGHT_LDAP_BIND_DN and its password go together");
        }
        String store = optional(environment, "FLIGHT_LDAP_TRUSTSTORE");
        String storeType = optional(environment, "FLIGHT_LDAP_TRUSTSTORE_TYPE");
        return new LdapSettings(servers, startTls, template, base, filter, attribute, bindDn, bindPassword,
                store == null ? null : Path.of(store), storeType == null ? "PKCS12" : storeType,
                secret(environment, "FLIGHT_LDAP_TRUSTSTORE_PASSWORD"),
                bounded(environment, "FLIGHT_LDAP_CONNECT_TIMEOUT_MS", 5_000, 100, 120_000),
                bounded(environment, "FLIGHT_LDAP_RESPONSE_TIMEOUT_MS", 10_000, 100, 300_000),
                sessionSecret(required(environment, "FLIGHT_LDAP_SESSION_SECRET")),
                bounded(environment, "FLIGHT_LDAP_SESSION_MINUTES", 10, 1, 1440) * 60_000L);
    }

    /**
     * Parses a space- or comma-separated list of LDAP URLs.
     *
     * @param value configured URLs
     * @return servers in order
     */
    private static List<Server> servers(String value) {
        List<Server> servers = new ArrayList<>();
        for (String url : value.trim().split("[\\s,]+")) {
            URI uri;
            try {
                uri = new URI(url);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Malformed LDAP URL: " + url, e);
            }
            String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
            if (!scheme.equals("ldap") && !scheme.equals("ldaps")) {
                throw new IllegalArgumentException("LDAP URL must start with ldap:// or ldaps://: " + url);
            }
            if (uri.getHost() == null || (uri.getPath() != null && !uri.getPath().isEmpty()
                    && !uri.getPath().equals("/")) || uri.getQuery() != null || uri.getUserInfo() != null) {
                throw new IllegalArgumentException("LDAP URL must be scheme://host[:port]: " + url);
            }
            boolean ssl = scheme.equals("ldaps");
            servers.add(new Server(uri.getHost(), uri.getPort() > 0 ? uri.getPort() : ssl ? 636 : 389, ssl));
        }
        return List.copyOf(servers);
    }

    /**
     * Decodes the shared session key.
     *
     * @param value base64 key
     * @return key bytes
     */
    private static byte[] sessionSecret(String value) {
        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(value.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("FLIGHT_LDAP_SESSION_SECRET must be base64", e);
        }
        if (decoded.length < 32) {
            throw new IllegalArgumentException("FLIGHT_LDAP_SESSION_SECRET must be at least 32 bytes");
        }
        return decoded;
    }

    /**
     * Reads a secret from {@code NAME} or from the file named by {@code NAME_FILE}.
     *
     * @param environment deployment settings
     * @param name setting name
     * @return secret, or null when neither is set
     */
    private static String secret(Map<String, String> environment, String name) {
        String value = optional(environment, name);
        String file = optional(environment, name + "_FILE");
        if (value != null && file != null) {
            throw new IllegalArgumentException("Set " + name + " or " + name + "_FILE, not both");
        }
        if (file == null) {
            return value;
        }
        try {
            return Files.readString(Path.of(file), StandardCharsets.UTF_8).strip();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + name + "_FILE", e);
        }
    }

    /**
     * Reads a required non-blank setting.
     *
     * @param environment deployment settings
     * @param name setting name
     * @return value
     */
    private static String required(Map<String, String> environment, String name) {
        String value = optional(environment, name);
        if (value == null) {
            throw new IllegalArgumentException("LDAP authentication requires " + name);
        }
        return value;
    }

    /**
     * Reads an optional setting.
     *
     * @param environment deployment settings
     * @param name setting name
     * @return trimmed value, or null when unset or blank
     */
    private static String optional(Map<String, String> environment, String name) {
        String value = environment.get(name);
        return value == null || value.isBlank() ? null : value.trim();
    }

    /**
     * Reads a boolean setting, refusing anything but true and false.
     *
     * @param environment deployment settings
     * @param name setting name
     * @return value, false when unset
     */
    private static boolean flag(Map<String, String> environment, String name) {
        String value = optional(environment, name);
        if (value == null || value.equalsIgnoreCase("false")) {
            return false;
        }
        if (value.equalsIgnoreCase("true")) {
            return true;
        }
        throw new IllegalArgumentException(name + " must be true or false");
    }

    /**
     * Reads an integer setting within bounds.
     *
     * @param environment deployment settings
     * @param name setting name
     * @param fallback value when unset
     * @param min smallest allowed value
     * @param max largest allowed value
     * @return value
     */
    private static int bounded(Map<String, String> environment, String name, int fallback, int min, int max) {
        String value = optional(environment, name);
        if (value == null) {
            return fallback;
        }
        try {
            int parsed = Integer.parseInt(value);
            if (parsed >= min && parsed <= max) {
                return parsed;
            }
        } catch (NumberFormatException e) {
            // reported below
        }
        throw new IllegalArgumentException(name + " must be an integer from " + min + " to " + max);
    }

    /**
     * Copies the session key so the record holds its own.
     */
    LdapSettings {
        servers = List.copyOf(servers);
        sessionSecret = sessionSecret.clone();
    }

    /**
     * Returns a copy of the session key.
     *
     * @return HMAC key bytes
     */
    @Override
    public byte[] sessionSecret() {
        return sessionSecret.clone();
    }

    @Override
    public String toString() {
        return "LdapSettings[servers=" + servers + ", startTls=" + startTls + ", searchMode="
                + (searchBase != null) + "]";
    }
}
