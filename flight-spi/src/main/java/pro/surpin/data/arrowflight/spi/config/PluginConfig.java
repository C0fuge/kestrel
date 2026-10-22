package pro.surpin.data.arrowflight.spi.config;

import java.util.Optional;

/**
 * Configuration of one namespace (a provider's name). Keys are looked up in this order: the
 * declared legacy aliases (system property, then properties file), {@code arrowflight.<ns>.<key>}
 * (system property, then properties file), {@code <ns>.<key>} (properties file),
 * {@code <key>} in {@code etc/plugins/<ns>.properties}, then {@code FLIGHT_<NS>_<KEY>} in the
 * environment. Blank values count as unset.
 */
public interface PluginConfig {

    /**
     * Names the namespace.
     *
     * @return namespace, for example "s3"
     */
    String namespace();

    /**
     * Reads a value.
     *
     * @param key key name
     * @return trimmed value, empty when unset
     */
    Optional<String> get(String key);

    /**
     * Reads a value with a fallback.
     *
     * @param key key name
     * @param fallback value when unset
     * @return trimmed value or the fallback
     */
    default String get(String key, String fallback) {
        return get(key).orElse(fallback);
    }

    /**
     * Reads an integer.
     *
     * @param key key name
     * @param fallback value when unset
     * @return value
     * @throws IllegalArgumentException when the value is not an integer
     */
    default int getInt(String key, int fallback) {
        Optional<String> value = get(key);
        if (value.isEmpty()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.get());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer for " + namespace() + "." + key
                    + ": " + value.get(), e);
        }
    }

    /**
     * Reads a boolean; anything but "true" (any case) is false.
     *
     * @param key key name
     * @param fallback value when unset
     * @return value
     */
    default boolean getBoolean(String key, boolean fallback) {
        return get(key).map(Boolean::parseBoolean).orElse(fallback);
    }

    /**
     * Reads a credential: from {@code FLIGHT_<NS>_<KEY>} in the environment, or from the file
     * named by {@code <key>File} (which must not be readable by every user). Any property form
     * of the key is a startup error, because system properties show in the process list.
     *
     * @param key key name
     * @return trimmed credential, empty when unset
     * @throws IllegalArgumentException when the credential is passed as a property or its file
     *         is readable by every user
     */
    Optional<String> secret(String key);
}
