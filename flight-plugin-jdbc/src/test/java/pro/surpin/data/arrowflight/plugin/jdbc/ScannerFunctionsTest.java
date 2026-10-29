package pro.surpin.data.arrowflight.plugin.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * The functions this plugin refuses are exactly the ones the bundled scanner builds add
 * (CATALOGS-STATUS Q4 (a)): a new build that adds a function fails here, besides being refused
 * by the server's own comparison. Needs {@code JDBC_IT_EXTENSION_DIR} with
 * {@code postgres_scanner.duckdb_extension} and {@code mysql_scanner.duckdb_extension} for
 * DuckDB 1.5.6 (Catalogs CI job {@code jdbc-scanners}).
 */
class ScannerFunctionsTest {

    @Test
    void refusedListsEqualWhatTheExtensionsAdd() throws SQLException {
        String dir = System.getenv("JDBC_IT_EXTENSION_DIR");
        Assumptions.assumeTrue(dir != null && Files.isRegularFile(
                Path.of(dir, "postgres_scanner.duckdb_extension")), "no scanner extensions (JDBC_IT_EXTENSION_DIR)");
        assertEquals(new TreeSet<>(JdbcCatalog.POSTGRES_FUNCTIONS),
                added(Path.of(dir, "postgres_scanner.duckdb_extension")));
        assertEquals(new TreeSet<>(JdbcCatalog.MYSQL_FUNCTIONS),
                added(Path.of(dir, "mysql_scanner.duckdb_extension")));
    }

    private static Set<String> added(Path extension) throws SQLException {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("allow_unsigned_extensions", "true");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:", props);
                Statement s = conn.createStatement()) {
            s.execute("SET autoload_known_extensions = false");
            Set<String> before = names(s);
            s.execute("LOAD '" + extension + "'");
            Set<String> after = names(s);
            after.removeAll(before);
            return after;
        }
    }

    private static Set<String> names(Statement s) throws SQLException {
        Set<String> names = new TreeSet<>();
        try (ResultSet rs = s.executeQuery("SELECT DISTINCT lower(function_name) FROM duckdb_functions()")) {
            while (rs.next()) {
                names.add(rs.getString(1));
            }
        }
        return names;
    }

    /** Keeps the lists free of duplicates and in lower case, as the server compares them. */
    @Test
    void listsAreLowerCaseAndDistinct() {
        for (List<String> list : List.of(JdbcCatalog.POSTGRES_FUNCTIONS, JdbcCatalog.MYSQL_FUNCTIONS)) {
            assertEquals(list.size(), new TreeSet<>(list).size());
            for (String name : list) {
                assertEquals(name.toLowerCase(java.util.Locale.ROOT), name);
            }
        }
    }
}
