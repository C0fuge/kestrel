package pro.surpin.data.arrowflight.spi.config;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Removes registered secret values from texts such as error messages. Values are registered
 * once at startup and kept for the life of the server.
 */
public final class SecretRedactor {

    /** Replaces every secret value. */
    public static final String REDACTED = "***";

    /** Secret values, longest first so that a value inside another is not replaced first. */
    private volatile List<String> secrets = List.of();

    /**
     * Registers a secret value; null and empty values are ignored.
     *
     * @param secret secret value
     */
    public synchronized void add(String secret) {
        if (secret == null || secret.isEmpty() || secrets.contains(secret)) {
            return;
        }
        List<String> sorted = new ArrayList<>(secrets);
        sorted.add(secret);
        sorted.sort(Comparator.comparingInt(String::length).reversed());
        secrets = List.copyOf(sorted);
    }

    /**
     * Replaces every registered secret value in a text.
     *
     * @param text text that may hold a secret, or null
     * @return text without the secret values, null for null
     */
    public String redact(String text) {
        String result = text;
        for (String secret : secrets) {
            if (result == null) {
                return null;
            }
            result = result.replace(secret, REDACTED);
        }
        return result;
    }

    /**
     * Tells how many values are registered.
     *
     * @return number of secret values
     */
    public int size() {
        return secrets.size();
    }

    @Override
    public String toString() {
        return "SecretRedactor[" + secrets.size() + " secrets]";
    }
}
