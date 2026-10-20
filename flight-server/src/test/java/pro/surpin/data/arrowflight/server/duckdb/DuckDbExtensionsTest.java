package pro.surpin.data.arrowflight.server.duckdb;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import pro.surpin.data.arrowflight.server.plugin.ConfigSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests how extension names resolve to configured files. */
@Tag("unit")
class DuckDbExtensionsTest {

    @Test
    void explicitKeysInTheLegacyOrder(@TempDir Path dir) throws Exception {
        Path a = Files.writeString(dir.resolve("a.duckdb_extension"), "x");
        Path b = Files.writeString(dir.resolve("b.duckdb_extension"), "x");
        Path c = Files.writeString(dir.resolve("c.duckdb_extension"), "x");
        Map<String, String> system = new HashMap<>();
        Properties file = new Properties();
        Map<String, String> env = new HashMap<>();
        DuckDbExtensions extensions = new DuckDbExtensions(
                new ConfigSource(system::get, file, Map.of(), null));
        assertThrows(IllegalArgumentException.class, () -> extensions.apply("httpfs"));

        env.put("DUCKDB_HTTPFS_EXTENSION", c.toString());
        assertEquals(c.toString(),
                new DuckDbExtensions(new ConfigSource(system::get, file, env, null)).apply("httpfs"));
        file.setProperty("arrowflight.duckdb.httpfs.extension", b.toString());
        assertEquals(b.toString(),
                new DuckDbExtensions(new ConfigSource(system::get, file, env, null)).apply("httpfs"));
        system.put("duckDbHttpfsExtension", a.toString());
        assertEquals(a.toString(),
                new DuckDbExtensions(new ConfigSource(system::get, file, env, null)).apply("httpfs"));

        system.put("duckDbHttpfsExtension", dir.resolve("missing").toString());
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new DuckDbExtensions(new ConfigSource(system::get, file, env, null))
                        .apply("httpfs"));
        assertTrue(e.getMessage().contains("duckDbHttpfsExtension is not a readable file"),
                e.getMessage());
    }

    @Test
    void extensionDirectoryByVersionAndPlatform(@TempDir Path dir) throws Exception {
        String version;
        String platform;
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement();
                ResultSet rs = s.executeQuery("SELECT library_version,"
                        + " (SELECT platform FROM pragma_platform()) FROM pragma_version()")) {
            rs.next();
            version = rs.getString(1);
            platform = rs.getString(2);
        }
        Path file = dir.resolve(version).resolve(platform).resolve("iceberg.duckdb_extension");
        Files.createDirectories(file.getParent());
        Files.writeString(file, "x");
        Map<String, String> system = Map.of("arrowflight.duckdb.extensionDir", dir.toString());
        DuckDbExtensions extensions = new DuckDbExtensions(
                new ConfigSource(system::get, new Properties(), Map.of(), null));

        assertEquals(file.toString(), extensions.apply("iceberg"));
        assertThrows(IllegalArgumentException.class, () -> extensions.apply("avro"));
    }
}
