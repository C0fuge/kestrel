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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.model.AppConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests that the startup check reads back what the lockdown set. */
@Tag("unit")
class LockdownCheckTest {

    @Test
    void acceptsTheServersLockdownAndRefusesDifferences(@TempDir Path dir) throws Exception {
        Path data = Files.createDirectories(dir.resolve("data"));
        Path temp = Files.createDirectories(dir.resolve("tmp"));
        try (Connection conn = lockedDown(data + "/", temp + "/")) {
            LockdownCheck.verify(conn, List.of(data.toString()), temp, List.of());
            IllegalStateException other = assertThrows(IllegalStateException.class,
                    () -> LockdownCheck.verify(conn, List.of(dir.resolve("other").toString()),
                            temp, List.of()));
            assertTrue(other.getMessage().contains("allowed_directories"), other.getMessage());
            IllegalStateException secret = assertThrows(IllegalStateException.class,
                    () -> LockdownCheck.verify(conn, List.of(data.toString()), temp,
                            List.of(new DuckDbSetupPlan.DeclaredSecret("flight_s3", "s3://b/"))));
            assertTrue(secret.getMessage().contains("flight_s3"), secret.getMessage());
            LockdownCheck.verify(conn, List.of(data.toString()), temp, null);
        }
        try (Connection open = DriverManager.getConnection("jdbc:duckdb:")) {
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> LockdownCheck.verify(open, List.of(data.toString()), temp, List.of()));
            assertTrue(e.getMessage().contains("enable_external_access"), e.getMessage());
        }
    }

    /**
     * DuckDB 1.4.5 shows {@code s3://b/p/} as {@code s3:/b/p/} in allowed_directories (1.4.1 as
     * set): URI roots of S3, lakeFS and HDFS stores must still pass, and another root must not.
     */
    @Test
    void acceptsUriRootsInTheFormDuckDbShowsThem(@TempDir Path dir) throws Exception {
        Path temp = Files.createDirectories(dir.resolve("tmp"));
        try (Connection conn = lockedDown("s3://lake/parquet/", "hdfs://ns/data/parquet/",
                temp + "/")) {
            LockdownCheck.verify(conn, List.of("s3://lake/parquet", "hdfs://ns/data/parquet/"),
                    temp, List.of());
            IllegalStateException other = assertThrows(IllegalStateException.class,
                    () -> LockdownCheck.verify(conn, List.of("s3://lake/other/",
                            "hdfs://ns/data/parquet/"), temp, List.of()));
            assertTrue(other.getMessage().contains("allowed_directories"), other.getMessage());
            IllegalStateException missing = assertThrows(IllegalStateException.class,
                    () -> LockdownCheck.verify(conn, List.of("s3://lake/parquet/"), temp,
                            List.of()));
            assertTrue(missing.getMessage().contains("hdfs:"), missing.getMessage());
        }
        assertEquals("s3:/lake/p/", LockdownCheck.comparable("s3://lake/p"));
        assertEquals("/tmp/a/b/", LockdownCheck.comparable("/tmp/a//b/"));
    }

    @Test
    void adapterChecksItsLockdownAtStartup(@TempDir Path dir) throws Exception {
        Path data = Files.createDirectories(dir.resolve("data"));
        DuckDbSetupPlan plan = new DuckDbSetupValidator(name -> {
            throw new IllegalArgumentException(name);
        }).plan(new DuckDbSetupValidator.Contribution("local", java.util.Set.of("file"),
                List.of(data.toString()), setup -> setup.set("parquet_metadata_cache", true)),
                List.of(), List.of());
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        AppConfig config = new AppConfig(3, 100, 1, 131072, 0, 1, 1, null, false, null, null,
                null, null, null, false, 1048576, 67108864, 60000L, null, null, 32010, 5701, 60, 3,
                1000, 30000, true, false, 64, dir.resolve("spill").toString(), null, "complete",
                true, true);
        try (DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool, plan);
                Statement s = adapter.connection().createStatement();
                ResultSet rs = s.executeQuery("SELECT current_setting('parquet_metadata_cache')")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        } finally {
            ioPool.shutdownNow();
        }
    }

    private static Connection lockedDown(String... allowed) throws Exception {
        // As the server opens its connections: with persistent secrets on, DuckDB 1.4.5 reads
        // ~/.duckdb/stored_secrets for duckdb_secrets(), which the lockdown refuses.
        java.util.Properties props = new java.util.Properties();
        props.setProperty("allow_persistent_secrets", "false");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:", props);
        try (Statement s = conn.createStatement()) {
            StringBuilder list = new StringBuilder();
            for (String directory : allowed) {
                list.append(list.isEmpty() ? "" : ", ").append('\'').append(directory).append('\'');
            }
            s.execute("SET temp_directory = '" + allowed[allowed.length - 1] + "c1'");
            s.execute("SET allowed_directories = [" + list + "]");
            s.execute("SET autoinstall_known_extensions = false");
            s.execute("SET autoload_known_extensions = false");
            s.execute("SET allow_community_extensions = false");
            s.execute("SET enable_external_access = false");
            s.execute("SET lock_configuration = true");
        }
        return conn;
    }
}
