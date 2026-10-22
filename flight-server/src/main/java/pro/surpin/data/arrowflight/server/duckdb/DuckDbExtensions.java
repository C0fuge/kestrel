package pro.surpin.data.arrowflight.server.duckdb;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.UnaryOperator;

import pro.surpin.data.arrowflight.server.plugin.ConfigSource;

/**
 * Resolves a DuckDB extension name to the file the server was configured with: first the
 * explicit key of that extension ({@code duckDb<Name>Extension},
 * {@code arrowflight.duckdb.<name>.extension}, environment {@code DUCKDB_<NAME>_EXTENSION}, as
 * {@code duckDbHttpfsExtension} always was), then
 * {@code <arrowflight.duckdb.extensionDir>/v<version>/<platform>/<name>.duckdb_extension}.
 */
public final class DuckDbExtensions implements UnaryOperator<String> {

    private final ConfigSource config;
    /** DuckDB version and platform, read once when the extension directory is used. */
    private volatile String[] versionAndPlatform;

    /**
     * Creates a resolver.
     *
     * @param config configuration source
     */
    public DuckDbExtensions(ConfigSource config) {
        this.config = config;
    }

    @Override
    public String apply(String name) {
        String camel = Character.toUpperCase(name.charAt(0)) + name.substring(1);
        String key = "duckDb" + camel + "Extension";
        Optional<String> explicit = config.lookup(
                List.of(key, "arrowflight.duckdb." + name + ".extension"),
                List.of("DUCKDB_" + name.toUpperCase(Locale.ROOT) + "_EXTENSION"));
        if (explicit.isPresent()) {
            return readable(explicit.get(), key);
        }
        Optional<String> directory = extensionDirectory();
        if (directory.isPresent()) {
            String[] vp = versionAndPlatform();
            Path file = Path.of(directory.get(), vp[0], vp[1], name + ".duckdb_extension");
            return readable(file.toString(), "arrowflight.duckdb.extensionDir");
        }
        throw new IllegalArgumentException("DuckDB extension " + name + " is needed but not"
                + " configured: set " + key + " (or DUCKDB_" + name.toUpperCase(Locale.ROOT)
                + "_EXTENSION) to its file, or arrowflight.duckdb.extensionDir to a directory with "
                + "v<version>/<platform>/" + name + ".duckdb_extension");
    }

    /**
     * Returns the configured extension directory.
     *
     * @return directory, empty when not configured
     */
    public Optional<String> extensionDirectory() {
        return config.lookup(List.of("arrowflight.duckdb.extensionDir", "duckDbExtensionDir"),
                List.of("FLIGHT_DUCKDB_EXTENSION_DIR"));
    }

    /**
     * Checks that a configured file can be read.
     *
     * @param file file as configured
     * @param key key it came from, for the error
     * @return the file
     */
    private static String readable(String file, String key) {
        Path path = Path.of(file);
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IllegalArgumentException(key + " is not a readable file: " + file);
        }
        return file;
    }

    /**
     * Reads the DuckDB version and platform from an in-memory database.
     *
     * @return version ("v1.4.1") and platform ("linux_amd64")
     */
    private String[] versionAndPlatform() {
        String[] result = versionAndPlatform;
        if (result == null) {
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                    Statement s = conn.createStatement();
                    ResultSet rs = s.executeQuery("SELECT library_version,"
                            + " (SELECT platform FROM pragma_platform()) FROM pragma_version()")) {
                rs.next();
                String version = rs.getString(1);
                result = new String[] {version.startsWith("v") ? version : "v" + version,
                    rs.getString(2)};
            } catch (SQLException e) {
                throw new IllegalStateException("Cannot read the DuckDB version and platform", e);
            }
            versionAndPlatform = result;
        }
        return result;
    }
}
