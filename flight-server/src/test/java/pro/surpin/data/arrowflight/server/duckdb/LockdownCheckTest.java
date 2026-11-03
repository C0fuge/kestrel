package pro.surpin.data.arrowflight.server.duckdb;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
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
     * DuckDB shows {@code s3://b/p/} as {@code s3:/b/p/} in allowed_directories from 1.4.4 on (1.4.1
     * as set); 1.5.6 without the extension that registers the scheme stores a URI root as a local
     * path below the working directory. The comparable form collapses repeated slashes, and a root
     * DuckDB did not keep as a URI fails the check (the server then refuses to start). That URI
     * roots pass once the extension is loaded is covered by S3IntegrationTest with httpfs.
     */
    @Test
    void refusesUriRootsDuckDbDidNotKeep(@TempDir Path dir) throws Exception {
        Path temp = Files.createDirectories(dir.resolve("tmp"));
        try (Connection conn = lockedDown("s3://lake/parquet/", "hdfs://ns/data/parquet/",
                temp + "/")) {
            IllegalStateException unregistered = assertThrows(IllegalStateException.class,
                    () -> LockdownCheck.verify(conn, List.of("s3://lake/parquet",
                            "hdfs://ns/data/parquet/"), temp, List.of()));
            assertTrue(unregistered.getMessage().contains("allowed_directories"),
                    unregistered.getMessage());
        }
        try (Connection conn = lockedDown("hdfs://ns/data/parquet/", temp + "/")) {
            IllegalStateException hdfs = assertThrows(IllegalStateException.class,
                    () -> LockdownCheck.verify(conn, List.of("hdfs://ns/data/parquet"), temp,
                            List.of()));
            assertTrue(hdfs.getMessage().contains("no file system for hdfs://")
                    && hdfs.getMessage().contains("duckDbHdfsExtension"), hdfs.getMessage());
        }
        assertEquals("s3:/lake/p/", LockdownCheck.comparable("s3://lake/p"));
        assertEquals("s3:/lake/p/", LockdownCheck.comparable("s3:/lake/p/"));
        assertEquals("hdfs:/ns/d/", LockdownCheck.comparable("hdfs://ns/d"));
        assertEquals("/tmp/a/b/", LockdownCheck.comparable("/tmp/a//b/"));
        assertEquals("/tmp/lake/", LockdownCheck.comparable("file:///tmp/lake"));
    }

    /**
     * Verifies a local root given as a file URI (Iceberg and Hive warehouses) passes the check
     * whether DuckDB shows it as set or as a plain path (1.5.6), and that the lockdown still
     * reads below it and refuses next to it.
     */
    @Test
    void acceptsFileUriRoots(@TempDir Path dir) throws Exception {
        Path temp = Files.createDirectories(dir.resolve("tmp"));
        Path lake = Files.createDirectories(dir.resolve("lake"));
        Path other = Files.createDirectories(dir.resolve("other"));
        for (Path p : List.of(lake, other)) {
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                    Statement s = conn.createStatement()) {
                s.execute("COPY (SELECT 1 AS x) TO '" + p.resolve("x.parquet") + "' (FORMAT parquet)");
            }
        }
        try (Connection conn = lockedDown("file://" + lake + "/", temp + "/")) {
            LockdownCheck.verify(conn, List.of(lake.toString(), "file://" + lake), temp, List.of());
            try (Statement s = conn.createStatement();
                    ResultSet rs = s.executeQuery("SELECT x FROM read_parquet('file://" + lake
                            + "/x.parquet')")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
            try (Statement s = conn.createStatement()) {
                assertThrows(SQLException.class, () -> s.executeQuery(
                        "SELECT x FROM read_parquet('" + other.resolve("x.parquet") + "')"));
            }
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

    /**
     * Item 30, {@code hdfsLocalCopyDir}: a copy directory outside the spill directory is accepted
     * only when the check is told about it, and a directory it was told about must be allowed.
     */
    @Test
    void acceptsALocalCopyDirectoryOnlyWhenDeclared(@TempDir Path dir) throws Exception {
        Path data = Files.createDirectories(dir.resolve("data"));
        Path temp = Files.createDirectories(dir.resolve("tmp"));
        Path copies = Files.createDirectories(dir.resolve("copies"));
        try (Connection conn = lockedDown(data + "/", copies + "/", temp + "/")) {
            LockdownCheck.verify(conn, List.of(data.toString()), temp, copies, List.of());
            IllegalStateException undeclared = assertThrows(IllegalStateException.class,
                    () -> LockdownCheck.verify(conn, List.of(data.toString()), temp, List.of()));
            assertTrue(undeclared.getMessage().contains(copies.toString()), undeclared.getMessage());
        }
        try (Connection conn = lockedDown(data + "/", temp + "/")) {
            IllegalStateException missing = assertThrows(IllegalStateException.class,
                    () -> LockdownCheck.verify(conn, List.of(data.toString()), temp, copies,
                            List.of()));
            assertTrue(missing.getMessage().contains(copies.toString()), missing.getMessage());
        }
    }

    /**
     * Item 30, {@code hdfsLocalCopyDir}: the adapter copies below its own directory there, a
     * locked-down connection reads a copy in it (and nothing next to it), the start removes the
     * directory of a dead process, and closing the adapter removes its own.
     */
    @Test
    void adapterReadsLocalCopiesFromACustomDirectory(@TempDir Path dir) throws Exception {
        Path data = Files.createDirectories(dir.resolve("data"));
        Path copies = Files.createDirectories(dir.resolve("copies"));
        Path stale = Files.createDirectories(copies.resolve("duckdb-999999999-1"));
        Path outside = Files.createDirectories(dir.resolve("outside"));
        try (Connection plain = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = plain.createStatement()) {
            s.execute("COPY (SELECT 7 AS x) TO '" + outside.resolve("x.parquet") + "' (FORMAT parquet)");
        }
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
        System.setProperty("hdfsLocalCopyDir", copies.toString());
        Path root;
        try (DuckDbAdapter adapter = new DuckDbAdapter(config, ioPool, plan)) {
            root = adapter.localCopyRoot();
            assertEquals(copies, root.getParent());
            assertTrue(Files.isDirectory(root));
            Files.copy(outside.resolve("x.parquet"), root.resolve("x.parquet"));
            try (Statement s = adapter.connection().createStatement()) {
                try (ResultSet rs = s.executeQuery("SELECT x FROM read_parquet('"
                        + root.resolve("x.parquet") + "')")) {
                    assertTrue(rs.next());
                    assertEquals(7, rs.getInt(1));
                }
                assertThrows(SQLException.class, () -> s.executeQuery(
                        "SELECT x FROM read_parquet('" + outside.resolve("x.parquet") + "')"));
            }
        } finally {
            System.clearProperty("hdfsLocalCopyDir");
            ioPool.shutdownNow();
        }
        assertTrue(Files.notExists(root), "own copy directory removed on close");
        assertTrue(Files.notExists(stale), "a dead process's copy directory removed at start");
    }

    /**
     * Regression (HDFS on a real cluster, 2026-11-02): without the hadoopfs extension DuckDB 1.5.6
     * stores an hdfs:// root as {@code <cwd>/hdfs:/ns/...}; the check names the missing extension
     * and accepts the forms DuckDB keeps when a file system claims the scheme.
     */
    @Test
    void namesTheExtensionAUriRootNeeds() {
        String cwd = Path.of("").toAbsolutePath() + "/";
        String hdfs = LockdownCheck.unhandledScheme(List.of("hdfs://ns/data/parquet"),
                List.of(cwd + "hdfs:/ns/data/parquet/", "/tmp/spill/"));
        assertTrue(hdfs != null && hdfs.contains("hadoopfs")
                && hdfs.contains("hdfs://ns/data/parquet"), hdfs);
        String s3 = LockdownCheck.unhandledScheme(List.of("s3://lake/p"),
                List.of(cwd + "s3:/lake/p/"));
        assertTrue(s3 != null && s3.contains("httpfs"), s3);
        assertEquals(null, LockdownCheck.unhandledScheme(List.of("hdfs://ns/data/parquet"),
                List.of("hdfs:/ns/data/parquet/", "/tmp/spill/")));
        assertEquals(null, LockdownCheck.unhandledScheme(List.of("s3://lake/p"),
                List.of(LockdownCheck.comparable("s3://lake/p/"))));
        assertEquals(null, LockdownCheck.unhandledScheme(List.of("/data", "file:///lake"),
                List.of("/data/", "/lake/")));
    }

    /**
     * Regression (HDFS on a real cluster, 2026-11-02): a per-connection (balanced) server with an
     * hdfs:// root and no hadoopfs extension started and refused every query with DuckDB's
     * pending-result error; it now stops at startup and names the setting.
     */
    @Test
    void balancedServerWithoutHadoopfsStopsAtStartup(@TempDir Path dir) {
        AppConfig config = new AppConfig(3, 100, 1, 131072, 0, 1, 1, null, false, null, null,
                null, null, null, false, 1048576, 67108864, 60000L, "hdfs://ns/data/parquet", null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 64, dir.resolve("spill").toString(),
                null, "complete", true, true);
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try {
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> new DuckDbAdapter(config, ioPool, List.of("hdfs://ns/data/parquet")).close());
            assertTrue(e.getMessage().contains("duckDbHdfsExtension"), e.getMessage());
        } finally {
            ioPool.shutdownNow();
        }
    }

    private static Connection lockedDown(String... allowed) throws Exception {
        // As the server opens its connections: with persistent secrets on, DuckDB (1.4.5+) reads
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
