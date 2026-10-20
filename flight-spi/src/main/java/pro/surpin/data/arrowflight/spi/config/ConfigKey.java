package pro.surpin.data.arrowflight.spi.config;

import java.util.List;
import java.util.Objects;

/**
 * A configuration key of a plugin. Its canonical forms are {@code arrowflight.<ns>.<name>}
 * (system property or properties file), {@code <ns>.<name>} (properties file or
 * {@code etc/plugins/<ns>.properties}) and {@code FLIGHT_<NS>_<NAME>} (environment, the name
 * in upper snake case).
 *
 * @param name key name inside the namespace, camel case
 * @param description one line for documentation and startup logs
 * @param defaultValue default value, or null
 * @param secret whether the value is a credential: then it comes only from the environment or
 *               a {@code <name>File}, and any property form is a startup error
 * @param legacyAliases older flat names that are still read, before the canonical names, in
 *                      the order given; all upper case names are environment variables
 */
public record ConfigKey(String name, String description, String defaultValue, boolean secret,
        List<String> legacyAliases) {

    /**
     * Checks the name and copies the aliases.
     *
     * @param name key name
     * @param description description
     * @param defaultValue default or null
     * @param secret whether it is a credential
     * @param legacyAliases legacy names
     */
    public ConfigKey {
        Objects.requireNonNull(name, "name");
        if (!name.matches("[a-z][A-Za-z0-9]*(\\.[a-z][A-Za-z0-9]*)*")) {
            throw new IllegalArgumentException("Configuration key name must be camelCase: " + name);
        }
        description = description == null ? "" : description;
        legacyAliases = legacyAliases == null ? List.of() : List.copyOf(legacyAliases);
    }

    /**
     * Declares a plain key.
     *
     * @param name key name
     * @param description description
     * @param defaultValue default or null
     * @param legacyAliases legacy names
     * @return key
     */
    public static ConfigKey of(String name, String description, String defaultValue,
            String... legacyAliases) {
        return new ConfigKey(name, description, defaultValue, false, List.of(legacyAliases));
    }

    /**
     * Declares a secret key.
     *
     * @param name key name
     * @param description description
     * @param legacyAliases legacy names, which are refused as properties like the canonical ones
     * @return key
     */
    public static ConfigKey secret(String name, String description, String... legacyAliases) {
        return new ConfigKey(name, description, null, true, List.of(legacyAliases));
    }

    /**
     * Returns the environment variable of this key in a namespace.
     *
     * @param namespace namespace, for example "s3"
     * @return for example {@code FLIGHT_S3_LISTING_TTL_SECONDS}
     */
    public String environmentName(String namespace) {
        return environmentName(namespace, name);
    }

    /**
     * Returns the environment variable of a key in a namespace.
     *
     * @param namespace namespace
     * @param name key name, camel case, dots allowed
     * @return {@code FLIGHT_<NS>_<NAME>}
     */
    public static String environmentName(String namespace, String name) {
        return "FLIGHT_" + snake(namespace) + "_" + snake(name);
    }

    /**
     * Converts a camel case name to upper snake case.
     *
     * @param name name
     * @return upper snake case
     */
    private static String snake(String name) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (Character.isUpperCase(c) && i > 0 && Character.isLowerCase(name.charAt(i - 1))) {
                result.append('_');
            }
            result.append(c == '.' || c == '-' ? '_' : Character.toUpperCase(c));
        }
        return result.toString();
    }
}
