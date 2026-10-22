package pro.surpin.data.arrowflight.server;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import pro.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.storage.DataStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the pre-plugin {@code ServerModule.dataStore(AppConfig)} and
 * {@code duckDb(AppConfig, ExecutorService, DataStore)} signatures kept for tests that build the
 * store and the adapter one by one: local directories through the Hadoop store, S3 through the
 * s3 plugin named by {@code arrowflight.plugins.dir} (set for all tests in the POM).
 */
@Tag("unit")
class ServerModuleStoreTest {

    @TempDir
    Path dir;

    /** A local data directory gives the Hadoop store and an adapter whose DuckDB reads only it. */
    @Test
    void localDirectoryThroughThePreRegistrySignatures() throws Exception {
        Path data = Files.createDirectories(dir.resolve("data"));
        Map<String, String> flags = new LinkedHashMap<>();
        flags.put("dataDir", data.toString());
        flags.put("duckDbWarmConnections", "0");
        flags.put("duckDbTempDirectory", Files.createDirectories(dir.resolve("tmp")).toString());
        withProperties(flags, () -> {
            AppConfig config = ConfigAdapter.getConfig();
            ServerModule module = module();
            DataStore store = module.dataStore(config);
            assertEquals("file", store.kind());
            ExecutorService pool = Executors.newSingleThreadExecutor();
            try (DuckDbAdapter adapter = module.duckDb(config, pool, store)) {
                Connection conn = adapter.connection();
                try (Statement st = conn.createStatement();
                        ResultSet rs = st.executeQuery("SELECT value FROM duckdb_settings()"
                                + " WHERE name = 'allowed_directories'")) {
                    assertTrue(rs.next());
                    assertTrue(rs.getString(1).contains(data.toString()), rs.getString(1));
                }
            } finally {
                pool.shutdownNow();
            }
        });
    }

    /** An s3:// data directory is opened by the real s3 plugin, without contacting the endpoint. */
    @Test
    void s3DirectoryLoadsThePlugin() throws Exception {
        Path credentials = dir.resolve("s3.credentials");
        Files.writeString(credentials, "accessKey=AKIATEST\nsecretKey=test-secret\n");
        Files.setPosixFilePermissions(credentials, PosixFilePermissions.fromString("rw-------"));
        Map<String, String> flags = new LinkedHashMap<>();
        flags.put("dataDir", "s3://lake/parquet");
        flags.put("s3Endpoint", "http://127.0.0.1:9");
        flags.put("s3CredentialsFile", credentials.toString());
        flags.put("duckDbHttpfsExtension",
                Files.writeString(dir.resolve("httpfs.duckdb_extension"), "not loaded").toString());
        withProperties(flags, () -> {
            DataStore store = module().dataStore(ConfigAdapter.getConfig());
            try {
                assertEquals("s3", store.kind());
                assertEquals(List.of("s3://lake/parquet/"), store.duckDbRoots());
            } finally {
                store.close();
            }
        });
    }

    private static ServerModule module() {
        return new ServerModule(new String[] {"localhost"}, "grpc://localhost:32010", "localhost",
                new Configuration());
    }

    /** Body that may throw. */
    private interface Body {
        void run() throws Exception;
    }

    /**
     * Runs a body with system properties set, then restores them.
     *
     * @param flags properties
     * @param body body
     * @throws Exception from the body
     */
    private static void withProperties(Map<String, String> flags, Body body) throws Exception {
        Map<String, String> saved = new LinkedHashMap<>();
        for (Map.Entry<String, String> flag : flags.entrySet()) {
            saved.put(flag.getKey(), System.getProperty(flag.getKey()));
            System.setProperty(flag.getKey(), flag.getValue());
        }
        try {
            body.run();
        } finally {
            for (Map.Entry<String, String> flag : saved.entrySet()) {
                if (flag.getValue() == null) {
                    System.clearProperty(flag.getKey());
                } else {
                    System.setProperty(flag.getKey(), flag.getValue());
                }
            }
        }
    }
}
