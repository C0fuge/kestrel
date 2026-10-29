package pro.surpin.data.arrowflight.spi.duckdb;

import java.util.Map;

/**
 * Receives what a store or catalog needs in every DuckDB connection before the server locks it
 * down. Nothing here is SQL: the server checks each item against its allowlists and writes the
 * statements itself, so a plugin cannot weaken the lockdown. Per contributor the server runs
 * the extensions first, then the settings, then the secrets, each in the order given.
 */
public interface DuckDbSetup {

    /**
     * Loads a DuckDB extension. The server resolves the name to a file it was configured with
     * ({@code duckDb<Name>Extension} or the extension directory); plugins never name files.
     *
     * @param name extension name, for example "httpfs"
     * @throws IllegalArgumentException when the name is not an extension name
     */
    void loadExtension(String name);

    /**
     * Loads a DuckDB extension as {@link #loadExtension} does and refuses in client queries every
     * function and table function the extension adds: the server compares
     * {@code duckdb_functions()} before and after the load, so a later build of the extension
     * cannot add a function that clients reach. For extensions whose functions do their own I/O
     * or run text elsewhere (database scanners, lake formats); the plugin writes the statements
     * that use them itself. Functions that existed before the load stay as they were.
     *
     * @param name extension name, for example "postgres_scanner"
     * @throws IllegalArgumentException when the name is not an extension name
     * @throws UnsupportedOperationException on a server that cannot do it (the default)
     * @since SPI 1.9
     */
    default void loadExtensionRefusingFunctions(String name) {
        throw new UnsupportedOperationException(
                "this server cannot refuse the functions of extension " + name);
    }

    /**
     * Sets a text setting.
     *
     * @param setting setting name
     * @param value value, written as a string literal
     * @throws IllegalArgumentException when the setting or value is not allowed
     */
    void set(String setting, String value);

    /**
     * Sets a boolean setting.
     *
     * @param setting setting name
     * @param value value
     * @throws IllegalArgumentException when the setting or value is not allowed
     */
    void set(String setting, boolean value);

    /**
     * Sets a numeric setting.
     *
     * @param setting setting name
     * @param value value
     * @throws IllegalArgumentException when the setting or value is not allowed
     */
    void set(String setting, long value);

    /**
     * Creates a temporary secret, scoped below one of the contributor's own roots.
     *
     * @param name secret name, an identifier unique in the server
     * @param type secret type: s3, gcs, r2 or azure
     * @param scope URI prefix the secret applies to
     * @param options options in the order they are written; sensitive values are redacted
     * @throws IllegalArgumentException when the type, an option or the scope is not allowed
     */
    void createSecret(String name, String type, String scope, Map<String, SecretValue> options);

    /**
     * Refuses more functions in client queries; the server's own list can only grow.
     *
     * @param names function or table function names
     */
    void refuseFunctions(String... names);
}
