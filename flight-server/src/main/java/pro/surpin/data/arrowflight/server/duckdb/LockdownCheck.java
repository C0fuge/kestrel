package pro.surpin.data.arrowflight.server.duckdb;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reads back what a connection ended up with after its setup and lockdown, and refuses to start
 * when it differs from what the server meant to set: external access off, configuration locked,
 * no automatic extension loading or installing, exactly the expected allowed directories, and
 * exactly the declared secrets with their scopes.
 */
public final class LockdownCheck {

    /** Not instantiable. */
    private LockdownCheck() {
    }

    /**
     * Checks a locked down connection.
     *
     * @param conn connection after setup and lockdown
     * @param roots readable roots of the plan
     * @param tempRoot server's spill directory, readable by every connection
     * @param secrets declared secrets, or null to leave the secrets unchecked
     * @throws IllegalStateException naming the first difference
     * @throws SQLException when the settings cannot be read
     */
    public static void verify(Connection conn, List<String> roots, Path tempRoot,
            List<DuckDbSetupPlan.DeclaredSecret> secrets) throws SQLException {
        try (Statement s = conn.createStatement()) {
            Map<String, String> settings = new HashMap<>();
            try (ResultSet rs = s.executeQuery("SELECT name, value FROM duckdb_settings() WHERE name IN"
                    + " ('enable_external_access', 'lock_configuration',"
                    + " 'autoload_known_extensions', 'autoinstall_known_extensions')")) {
                while (rs.next()) {
                    settings.put(rs.getString(1), rs.getString(2));
                }
            }
            expect(settings, "enable_external_access", "false");
            expect(settings, "lock_configuration", "true");
            expect(settings, "autoload_known_extensions", "false");
            expect(settings, "autoinstall_known_extensions", "false");

            List<String> allowed = new ArrayList<>();
            try (ResultSet rs = s.executeQuery(
                    "SELECT unnest(current_setting('allowed_directories')::VARCHAR[])")) {
                while (rs.next()) {
                    allowed.add(rs.getString(1));
                }
            }
            Set<String> expected = new LinkedHashSet<>();
            for (String root : roots) {
                expected.add(root.endsWith("/") ? root : root + "/");
            }
            // DuckDB adds the connection's own temp_directory; it lies below the spill root.
            String spill = tempRoot.toString() + "/";
            String ownTemp = null;
            try (ResultSet rs = s.executeQuery("SELECT current_setting('temp_directory')")) {
                if (rs.next() && rs.getString(1) != null) {
                    ownTemp = rs.getString(1).endsWith("/") ? rs.getString(1) : rs.getString(1) + "/";
                }
            }
            Set<String> unexpected = new LinkedHashSet<>();
            for (String directory : allowed) {
                if (!expected.contains(directory) && !directory.startsWith(spill)
                        && !directory.equals(ownTemp)) {
                    unexpected.add(directory);
                }
            }
            if (!unexpected.isEmpty() || !new HashSet<>(allowed).containsAll(expected)
                    || !allowed.contains(spill)) {
                throw new IllegalStateException("DuckDB allowed_directories after the lockdown are "
                        + allowed + ", expected " + expected + ", the spill directory " + spill
                        + " and the connection's temp_directory");
            }

            if (secrets == null) {
                return;
            }
            Map<String, Set<String>> actualSecrets = new HashMap<>();
            try (ResultSet rs = s.executeQuery(
                    "SELECT name, unnest(scope) FROM duckdb_secrets()")) {
                while (rs.next()) {
                    actualSecrets.computeIfAbsent(rs.getString(1), k -> new HashSet<>())
                            .add(rs.getString(2));
                }
            }
            Set<String> declaredNames = new HashSet<>();
            for (DuckDbSetupPlan.DeclaredSecret secret : secrets) {
                declaredNames.add(secret.name());
                Set<String> scopes = actualSecrets.get(secret.name());
                if (scopes == null || !scopes.contains(secret.scope())) {
                    throw new IllegalStateException("DuckDB secret " + secret.name()
                            + " is missing or not scoped to " + secret.scope() + " after the setup");
                }
            }
            if (!declaredNames.containsAll(actualSecrets.keySet())) {
                Set<String> extra = new HashSet<>(actualSecrets.keySet());
                extra.removeAll(declaredNames);
                throw new IllegalStateException("DuckDB has secrets nobody declared: " + extra);
            }
        }
    }

    /**
     * Checks one setting.
     *
     * @param settings settings read back
     * @param name setting name
     * @param value expected value
     */
    private static void expect(Map<String, String> settings, String name, String value) {
        String actual = settings.get(name);
        if (!value.equalsIgnoreCase(actual)) {
            throw new IllegalStateException("DuckDB setting " + name + " is " + actual
                    + " after the lockdown, expected " + value);
        }
    }
}
