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
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
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
