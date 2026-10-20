package pro.surpin.data.arrowflight.spi.duckdb;

import java.util.Objects;

/**
 * One option value of a DuckDB secret.
 *
 * @param value option value as text; boolean options take "true" or "false"
 * @param sensitive whether the value is a credential; the server then removes it from every
 *                  error and log line
 */
public record SecretValue(String value, boolean sensitive) {

    /**
     * Checks the value.
     *
     * @param value option value
     * @param sensitive whether it is a credential
     */
    public SecretValue {
        Objects.requireNonNull(value, "value");
    }

    /**
     * Creates a value that is not a credential.
     *
     * @param value option value
     * @return secret option value
     */
    public static SecretValue plain(String value) {
        return new SecretValue(value, false);
    }

    /**
     * Creates a boolean value.
     *
     * @param value option value
     * @return secret option value
     */
    public static SecretValue of(boolean value) {
        return new SecretValue(Boolean.toString(value), false);
    }

    /**
     * Creates a credential value.
     *
     * @param value credential
     * @return secret option value
     */
    public static SecretValue sensitive(String value) {
        return new SecretValue(value, true);
    }

    @Override
    public String toString() {
        return sensitive ? "SecretValue[***]" : "SecretValue[" + value + "]";
    }
}
