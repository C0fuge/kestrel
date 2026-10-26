package pro.surpin.data.arrowflight.server.storage;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import pro.surpin.data.arrowflight.plugin.s3.FakeS3;
import pro.surpin.data.arrowflight.plugin.s3.TestTls;
import pro.surpin.data.arrowflight.server.duckdb.DuckDbSetupPlan;
import pro.surpin.data.arrowflight.spi.storage.DataFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The s3 plugin as the server loads it from plugins/s3: the AWS SDK lives only in the plugin's
 * class loader, the old flat configuration names still work, and the DuckDB setup the server
 * writes from the plugin's typed contribution is byte for byte the pre-plugin one (goldens).
 */
@Tag("unit")
class S3PluginTest {

    @TempDir
    Path dir;
    private S3PluginSupport support;
    private FakeS3 s3;
    private Path httpfs;

    @BeforeEach
    void setUp() throws Exception {
        support = new S3PluginSupport(dir);
        s3 = FakeS3.start();
        httpfs = Files.writeString(dir.resolve("httpfs.duckdb_extension"), "x");
        support.properties.put("duckDbHttpfsExtension", httpfs.toString());
        support.environment.put("FLIGHT_S3_ACCESS_KEY", "AKIATEST");
        support.environment.put("FLIGHT_S3_SECRET_KEY", "SECRETTEST");
    }

    @AfterEach
    void tearDown() {
        s3.close();
    }

    @Test
    void awsSdkIsOnlyInThePlugin() {
        assertThrows(ClassNotFoundException.class,
                () -> Class.forName("software.amazon.awssdk.services.s3.S3Client"));
        assertThrows(ClassNotFoundException.class,
                () -> Class.forName("pro.surpin.data.arrowflight.plugin.s3.S3DataStore"));
    }

    /** Verifies listing and a footer through the plugin: one ranged GET, as before plugins. */
    @Test
    void listsAndReadsFootersThroughThePlugin() throws Exception {
        Path file = dir.resolve("data.parquet");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT range AS id, 'x' || range AS name FROM range(5000)) TO '"
                    + file + "' (FORMAT parquet, ROW_GROUP_SIZE 2000)");
        }
        byte[] data = Files.readAllBytes(file);
        s3.put("lake", "parquet/tpch/lineitem/data.parquet", data);
        s3.put("lake", "parquet/tpch/nation/n.parquet", new byte[10]);
        support.properties.put("s3Endpoint", s3.endpoint());

        try (StoreRegistry registry = support.open(StoreRegistryTest.config("s3://lake/parquet"))) {
            DataStore store = registry.dataDirectory();
            assertInstanceOf(SpiDataStore.class, store);
            assertEquals("s3", registry.provider());
            assertEquals("s3", store.kind());
            assertEquals(List.of("lineitem", "nation"),
                    store.directories("tpch").keySet().stream().sorted().toList());
            DataFile listed = store.file("tpch/lineitem/data.parquet");
            s3.clearRequests();
            long rows;
            try (ParquetFileReader reader = ParquetFileReader.open(store.inputFile(listed))) {
                rows = reader.getRecordCount();
            }
            assertEquals(5000, rows);
            assertEquals(1, s3.count("GET"), "requests: " + s3.requests());
            assertTrue(s3.requests().get(0).headers().get("authorization")
                    .startsWith("AWS4-HMAC-SHA256 Credential=AKIATEST/"));
            assertEquals("x *** y", registry.redactor().redact("x SECRETTEST y"));
        }
    }

    /** Golden: TLS with a CA file, vhost style, metadata cache; old flat names. */
    @Test
    void goldenSetupTlsVhost() throws Exception {
        Path ca = TestTls.create("s3.local").writePem(dir.resolve("ca.pem"));
        support.properties.putAll(Map.of("s3Endpoint", "https://s3.local:9443",
                "s3Region", "eu-west-3", "s3UrlStyle", "vhost", "s3CaFile", ca.toString()));
        support.environment.put("FLIGHT_S3_SECRET_KEY", "SEC'RET");

        DuckDbSetupPlan plan = plan("s3a://lake/parquet/");

        assertEquals(List.of(
                "LOAD '" + httpfs + "'",
                "SET s3_access_key_id = ''",
                "SET s3_secret_access_key = ''",
                "SET s3_session_token = ''",
                "SET s3_endpoint = ''",
                "SET s3_region = ''",
                "SET s3_url_compatibility_mode = true",
                "SET enable_http_logging = false",
                "SET http_retries = 6",
                "SET http_retry_wait_ms = 200",
                "SET http_retry_backoff = 2",
                "SET enable_server_cert_verification = true",
                "SET ca_cert_file = '" + ca + "'",
                "SET parquet_metadata_cache = true",
                "SET enable_http_metadata_cache = true",
                "CREATE TEMPORARY SECRET flight_s3 (TYPE s3, KEY_ID 'AKIATEST', SECRET 'SEC''RET',"
                        + " REGION 'eu-west-3', ENDPOINT 's3.local:9443', URL_STYLE 'vhost',"
                        + " USE_SSL true, URL_COMPATIBILITY_MODE true, SCOPE 's3://lake/parquet/')"),
                plan.statements());
        assertEquals(List.of("s3://lake/parquet/"), plan.roots());
        assertEquals(List.of(new DuckDbSetupPlan.DeclaredSecret("flight_s3", "s3://lake/parquet/")),
                plan.secrets());
        assertEquals("x *** y", plan.redactor().redact("x SEC'RET y"));
    }

    /** Golden: plain HTTP with a session token, no metadata cache; canonical names. */
    @Test
    void goldenSetupPlainHttp() {
        support.properties.putAll(Map.of("arrowflight.s3.endpoint", "http://minio:9000",
                "arrowflight.duckdb.s3MetadataCache", "false"));
        support.environment.put("FLIGHT_S3_SESSION_TOKEN", "TOKEN");

        DuckDbSetupPlan plan = plan("s3://lake");

        assertEquals(List.of(
                "LOAD '" + httpfs + "'",
                "SET s3_access_key_id = ''",
                "SET s3_secret_access_key = ''",
                "SET s3_session_token = ''",
                "SET s3_endpoint = ''",
                "SET s3_region = ''",
                "SET s3_url_compatibility_mode = true",
                "SET enable_http_logging = false",
                "SET http_retries = 6",
                "SET http_retry_wait_ms = 200",
                "SET http_retry_backoff = 2",
                "CREATE TEMPORARY SECRET flight_s3 (TYPE s3, KEY_ID 'AKIATEST',"
                + " SECRET 'SECRETTEST', SESSION_TOKEN 'TOKEN', REGION 'us-east-1',"
                + " ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false,"
                + " URL_COMPATIBILITY_MODE true, SCOPE 's3://lake/')"), plan.statements());
        assertEquals("*** ***", plan.redactor().redact("SECRETTEST TOKEN"));
    }

    /** Golden: TLS not verified (s3VerifyTls=false), AWS endpoint from the region. */
    @Test
    void goldenSetupUnverifiedTls() {
        support.properties.putAll(Map.of("s3VerifyTls", "false", "s3Region", "eu-north-1"));

        assertEquals(List.of(
                "LOAD '" + httpfs + "'",
                "SET s3_access_key_id = ''",
                "SET s3_secret_access_key = ''",
                "SET s3_session_token = ''",
                "SET s3_endpoint = ''",
                "SET s3_region = ''",
                "SET s3_url_compatibility_mode = true",
                "SET enable_http_logging = false",
                "SET http_retries = 6",
                "SET http_retry_wait_ms = 200",
                "SET http_retry_backoff = 2",
                "SET enable_server_cert_verification = false",
                "SET parquet_metadata_cache = true",
                "SET enable_http_metadata_cache = true",
                "CREATE TEMPORARY SECRET flight_s3 (TYPE s3, KEY_ID 'AKIATEST', SECRET 'SECRETTEST',"
                        + " REGION 'eu-north-1', ENDPOINT 's3.eu-north-1.amazonaws.com',"
                        + " URL_STYLE 'path', USE_SSL true, URL_COMPATIBILITY_MODE true,"
                        + " SCOPE 's3://lake/a/b/')"), plan("s3://lake/a/b").statements());
    }

    @Test
    void credentialsAreNeededAndNeverReadFromProperties() {
        support.environment.clear();
        IllegalArgumentException none = assertThrows(IllegalArgumentException.class,
                () -> plan("s3://lake/parquet"));
        assertTrue(none.getMessage().contains("without credentials"), none.getMessage());

        support.properties.put("s3SecretKey", "do-not-pass-me-here");
        IllegalArgumentException property = assertThrows(IllegalArgumentException.class,
                () -> plan("s3://lake/parquet"));
        assertTrue(property.getMessage().contains("s3SecretKey"), property.getMessage());
        assertFalse(property.getMessage().contains("do-not-pass-me-here"), property.getMessage());
    }

    @Test
    void missingHttpfsIsAStartupError() {
        support.properties.remove("duckDbHttpfsExtension");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> plan("s3://lake/parquet"));
        assertTrue(e.getMessage().contains("duckDbHttpfsExtension"), e.getMessage());
    }

    private DuckDbSetupPlan plan(String dataDir) {
        try (StoreRegistry registry = support.open(StoreRegistryTest.config(dataDir))) {
            return registry.duckDbSetup();
        }
    }
}
