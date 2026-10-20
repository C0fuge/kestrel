package pro.surpin.data.arrowflight.server.storage;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
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
import pro.surpin.data.arrowflight.server.duckdb.DuckDbSetupPlan;
import pro.surpin.data.arrowflight.plugin.s3.FakeS3;
import pro.surpin.data.arrowflight.plugin.s3.TestTls;
import pro.surpin.data.arrowflight.server.model.AppConfig;

import pro.surpin.data.arrowflight.spi.storage.DataFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DuckDB reading S3 through the httpfs extension, locked down the way the server locks it down.
 * Needs the extension file of the bundled DuckDB: {@code -Ds3.it.httpfs=<path>} or
 * {@code DUCKDB_HTTPFS_EXTENSION}; skipped without it. The S3 server is the fake one, or a real
 * one when {@code FLIGHT_S3_IT_ENDPOINT} (with {@code FLIGHT_S3_ACCESS_KEY} and
 * {@code FLIGHT_S3_SECRET_KEY}) is set: the test then reads
 * {@code s3://<FLIGHT_S3_IT_BUCKET, default lake>/flight-it/}, where {@code t/x.parquet} (5000
 * rows: id, name) must have been uploaded beforehand. The store comes from the real s3 plugin
 * (flight-plugin-s3/target/plugin/s3) loaded the way the server loads it.
 */
@Tag("s3")
class S3IntegrationTest {

    private static final String SECRET = "it-secret-Zq9wX";

    @TempDir
    Path dir;

    private String httpfs;
    private FakeS3 s3;
    private FakeS3 evil;
    private ExecutorService ioPool;
    private S3PluginSupport plugin;
    private StoreRegistry registry;
    private DataStore store;
    private DuckDbAdapter duckDb;
    private String endpoint;
    private String accessKey;
    private String secretKey;
    private String bucket;

    @BeforeEach
    void setUp() throws Exception {
        httpfs = System.getProperty("s3.it.httpfs", System.getenv("DUCKDB_HTTPFS_EXTENSION"));
        Assumptions.assumeTrue(httpfs != null && Files.isReadable(Path.of(httpfs)),
                "httpfs extension not configured (-Ds3.it.httpfs or DUCKDB_HTTPFS_EXTENSION)");
        Path data = dir.resolve("data.parquet");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT range AS id, 'n' || range AS name FROM range(5000)) TO '"
                    + data + "' (FORMAT parquet)");
        }
        byte[] bytes = Files.readAllBytes(data);
        evil = FakeS3.start();
        evil.put("lake", "flight-it/t/x.parquet", bytes);
        endpoint = System.getenv("FLIGHT_S3_IT_ENDPOINT");
        bucket = System.getenv().getOrDefault("FLIGHT_S3_IT_BUCKET", "lake");
        if (endpoint == null) {
            s3 = FakeS3.start();
            s3.put(bucket, "flight-it/t/x.parquet", bytes);
            s3.put(bucket, "elsewhere/y.parquet", bytes);
            endpoint = s3.endpoint();
            accessKey = "AKIAIT";
            secretKey = SECRET;
        } else {
            accessKey = System.getenv("FLIGHT_S3_ACCESS_KEY");
            secretKey = System.getenv("FLIGHT_S3_SECRET_KEY");
        }
        plugin = new S3PluginSupport(dir);
        ioPool = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        if (duckDb != null) {
            duckDb.close();
        }
        if (registry != null) {
            registry.close();
        }
        if (ioPool != null) {
            ioPool.shutdownNow();
        }
        if (s3 != null) {
            s3.close();
        }
        if (evil != null) {
            evil.close();
        }
    }

    /** Verifies DuckDB reads the data directory and the Java client reads the same footer. */
    @Test
    void duckDbReadsTheDataDirectory() throws Exception {
        open(null);
        DataFile file = store.parquetFiles("t").get(0);

        try (Statement s = duckDb.connection().createStatement();
                ResultSet rs = s.executeQuery("SELECT count(*), sum(id) FROM read_parquet("
                        + DuckDbAdapter.sqlStringLiteral(store.duckDbUri(file)) + ")")) {
            assertTrue(rs.next());
            assertEquals(5000, rs.getLong(1));
            assertEquals(5000L * 4999 / 2, rs.getLong(2));
        }
        try (ParquetFileReader reader = ParquetFileReader.open(
                ParquetInputFiles.of(store.open(file), file.location()))) {
            assertEquals(5000, reader.getRecordCount());
        }
    }

    /** Verifies settings show no credentials and cannot be changed, and the secret is redacted. */
    @Test
    void credentialsDoNotShow() throws Exception {
        open(null);
        Statement s = duckDb.connection().createStatement();

        for (String setting : List.of("s3_secret_access_key", "s3_access_key_id", "s3_session_token",
                "s3_endpoint")) {
            try (ResultSet rs = s.executeQuery("SELECT current_setting('" + setting + "')")) {
                assertTrue(rs.next());
                assertEquals("", rs.getString(1), setting);
            }
        }
        try (ResultSet rs = s.executeQuery("SELECT string_agg(name || '=' || coalesce(value, ''), ';')"
                + " FROM duckdb_settings()")) {
            assertTrue(rs.next());
            assertFalse(rs.getString(1).contains(secretKey));
            assertTrue(rs.getString(1).contains("s3_url_compatibility_mode=true"), rs.getString(1));
            assertTrue(rs.getString(1).contains("allow_persistent_secrets=false"), rs.getString(1));
        }
        try (ResultSet rs = s.executeQuery("SELECT name, persistent, scope::VARCHAR, secret_string"
                + " FROM duckdb_secrets()")) {
            assertTrue(rs.next());
            assertEquals("flight_s3", rs.getString(1));
            assertFalse(rs.getBoolean(2));
            assertTrue(rs.getString(3).contains("s3://" + bucket + "/flight-it/"), rs.getString(3));
            assertFalse(rs.getString(4).contains(secretKey), "redacted");
            assertFalse(rs.next());
        }
        for (String sql : List.of("SET s3_endpoint = 'evil:1'", "SET s3_url_compatibility_mode = false",
                "SET enable_server_cert_verification = false", "RESET s3_region",
                "CREATE SECRET other (TYPE s3, KEY_ID 'a', SECRET 'b')", "DROP SECRET flight_s3",
                "SET allow_unredacted_secrets = true")) {
            assertThrows(SQLException.class, () -> s.execute(sql), sql);
        }
        try (ResultSet rs = s.executeQuery("SELECT secret_string FROM duckdb_secrets(redact := false)")) {
            assertTrue(rs.next());
            assertFalse(rs.getString(1).contains(secretKey), "still redacted");
        } catch (SQLException e) {
            assertFalse(e.getMessage().contains(secretKey));
        }
        s.close();
    }

    /** Verifies reads outside the data directory and query parameters on S3 URLs are refused. */
    @Test
    void otherPathsAndEndpointsAreRefused() throws Exception {
        open(null);
        int evilPort = Integer.parseInt(evil.endpoint().substring(evil.endpoint().lastIndexOf(':') + 1));
        String root = "s3://" + bucket + "/flight-it/";
        Statement s = duckDb.connection().createStatement();

        for (String path : List.of("s3://" + bucket + "/elsewhere/y.parquet",
                "s3://" + bucket + "/flight-it/../elsewhere/y.parquet",
                "s3a://" + bucket + "/flight-it/t/x.parquet", "S3://" + bucket + "/flight-it/t/x.parquet",
                "s3://other-bucket/flight-it/t/x.parquet",
                evil.endpoint() + "/lake/flight-it/t/x.parquet", "http://169.254.169.254/latest/meta-data/")) {
            SQLException e = assertThrows(SQLException.class, () -> s.executeQuery(
                    "SELECT count(*) FROM read_parquet('" + path + "')"), path);
            assertFalse(e.getMessage().contains(secretKey));
        }
        for (String query : List.of("?s3_endpoint=127.0.0.1:" + evilPort + "&s3_use_ssl=false",
                "?s3_endpoint=127.0.0.1:" + evilPort + "&s3_url_style=path&s3_access_key_id=x")) {
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM read_parquet('" + root
                    + "t/x.parquet" + query + "')")) {
                rs.next();
            } catch (SQLException e) {
                assertFalse(e.getMessage().contains(secretKey));
            }
        }
        s.close();
        assertEquals(List.of(), evil.requests(), "nothing may reach another endpoint");
        if (s3 != null) {
            assertTrue(s3.requests().stream().noneMatch(r -> r.path().contains("elsewhere")),
                    "no request outside the data directory");
        }
    }

    /**
     * Control for {@link #otherPathsAndEndpointsAreRefused}: with the URL compatibility mode
     * switched off (here only, in the statements this test passes), a query parameter does point
     * DuckDB at another endpoint, so the check above would notice if the mode stopped working.
     */
    @Test
    void endpointParameterReachesAnotherHostWithoutCompatibilityMode() throws Exception {
        Assumptions.assumeTrue(s3 != null, "fake S3 only");
        openRegistry(null);
        DuckDbSetupPlan plan = registry.duckDbSetup();
        List<String> unsafe = plan.statements().stream()
                .map(st -> st.replace("s3_url_compatibility_mode = true", "s3_url_compatibility_mode = false")
                        .replace("URL_COMPATIBILITY_MODE true", "URL_COMPATIBILITY_MODE false"))
                .toList();
        duckDb = new DuckDbAdapter(config(), ioPool, new DuckDbSetupPlan(unsafe, plan.roots(),
                plan.extraRefusedFunctions(), plan.redactor(), plan.extensions(), plan.secrets()));
        int evilPort = Integer.parseInt(evil.endpoint().substring(evil.endpoint().lastIndexOf(':') + 1));

        try (Statement s = duckDb.connection().createStatement();
                ResultSet rs = s.executeQuery("SELECT count(*) FROM read_parquet('s3://" + bucket
                        + "/flight-it/t/x.parquet?s3_endpoint=127.0.0.1:" + evilPort
                        + "&s3_use_ssl=false&s3_url_style=path')")) {
            rs.next();
        } catch (SQLException e) {
            // Only the request matters.
            assertFalse(e.getMessage().contains(secretKey));
        }

        assertFalse(evil.requests().isEmpty(), "the parameter redirected the read");
        assertTrue(evil.requests().get(0).headers().getOrDefault("authorization", "")
                .contains("Credential=" + accessKey + "/"), "and signed it with the data key");
    }

    /** Verifies DuckDB trusts the CA file over TLS and refuses another certificate. */
    @Test
    void tlsWithCaFile() throws Exception {
        Assumptions.assumeTrue(s3 != null, "fake S3 only");
        TestTls server = TestTls.create("fake-s3");
        TestTls other = TestTls.create("other-ca");
        try (FakeS3 tls = FakeS3.startTls(server.serverContext())) {
            tls.put(bucket, "flight-it/t/x.parquet", Files.readAllBytes(dir.resolve("data.parquet")));
            endpoint = tls.endpoint();
            open((server.writePem(dir.resolve("ca.pem")).toString()));
            try (Statement s = duckDb.connection().createStatement();
                    ResultSet rs = s.executeQuery("SELECT count(*) FROM read_parquet('s3://" + bucket
                            + "/flight-it/t/x.parquet')")) {
                assertTrue(rs.next());
                assertEquals(5000, rs.getLong(1));
            }
            duckDb.close();
            registry.close();
            open((other.writePem(dir.resolve("other.pem")).toString()));
            try (Statement s = duckDb.connection().createStatement()) {
                assertThrows(SQLException.class, () -> s.executeQuery("SELECT count(*) FROM read_parquet('s3://"
                        + bucket + "/flight-it/t/x.parquet')"));
            }
        }
    }

    private void openRegistry(String caFile) {
        plugin.properties.put("s3Endpoint", endpoint);
        plugin.properties.put("s3UrlStyle", "path");
        plugin.properties.put("s3MaxConnections", "4");
        plugin.properties.put("duckDbHttpfsExtension", httpfs);
        if (caFile != null) {
            plugin.properties.put("s3CaFile", caFile);
        } else {
            plugin.properties.remove("s3CaFile");
        }
        plugin.environment.put("FLIGHT_S3_ACCESS_KEY", accessKey);
        plugin.environment.put("FLIGHT_S3_SECRET_KEY", secretKey);
        registry = plugin.open(StoreRegistryTest.config("s3://" + bucket + "/flight-it"));
        store = registry.dataDirectory();
    }

    private void open(String caFile) {
        openRegistry(caFile);
        duckDb = new DuckDbAdapter(config(), ioPool, registry.duckDbSetup());
    }

    private AppConfig config() {
        return new AppConfig(3, 1024, 2, 131072, 0, 1, 1, null, false, null, null, null,
                null, "512MB", false, Integer.MAX_VALUE, 67108864, 60000L, store.root(), null, 32010,
                5701, 60, 3, 1000, 0, true, false, 16, dir.resolve("spill").toString(), null,
                "complete", true, true);
    }
}
