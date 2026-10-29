package pro.surpin.data.arrowflight.plugin.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.StorageCredential;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;
import pro.surpin.data.arrowflight.spi.duckdb.SecretValue;

class VendedCredentialsTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TableName TYPES = new TableName("ice", "types");

    @TempDir
    static Path dir;
    static String warehouse;

    @BeforeAll
    static void writeTables() throws IOException {
        warehouse = dir.resolve("wh").toString();
        IcebergFixtures.write(warehouse);
    }

    @Test
    void storageCredentialsBecomeS3SecretOptions() throws IOException {
        JsonNode body = JSON.readTree("{\"config\":{\"s3.access-key-id\":\"ignored\","
                + "\"s3.secret-access-key\":\"ignored\"},\"storage-credentials\":["
                + "{\"prefix\":\"s3://lake/wh/ns/t\",\"config\":{\"s3.access-key-id\":\"AKIA1\","
                + "\"s3.secret-access-key\":\"sec\",\"s3.session-token\":\"tok\","
                + "\"s3.session-token-expires-at-ms\":\"1700000000000\",\"client.region\":\"eu-1\","
                + "\"s3.endpoint\":\"http://minio:9000/\",\"s3.path-style-access\":\"true\"}},"
                + "{\"prefix\":\"gs://bucket/t\",\"config\":{\"gcs.oauth2.token\":\"x\"}},"
                + "{\"prefix\":\"s3://lake/other\",\"config\":{\"s3.access-key-id\":\"AKIA2\"}}]}");
        List<StorageCredential> credentials = VendedCredentials.of(body, "s3://lake/wh/ns/t");
        assertEquals(1, credentials.size(), "gs and incomplete entries are skipped");
        StorageCredential credential = credentials.get(0);
        assertEquals("s3://lake/wh/ns/t", credential.prefix());
        assertEquals("s3", credential.type());
        assertEquals(Instant.ofEpochMilli(1_700_000_000_000L), credential.expiresAt());
        Map<String, SecretValue> options = credential.options();
        assertEquals(List.of("KEY_ID", "SECRET", "SESSION_TOKEN", "REGION", "ENDPOINT", "USE_SSL",
                "URL_STYLE"), List.copyOf(options.keySet()));
        assertEquals(SecretValue.plain("AKIA1"), options.get("KEY_ID"));
        assertTrue(options.get("SECRET").sensitive());
        assertTrue(options.get("SESSION_TOKEN").sensitive());
        assertEquals("minio:9000", options.get("ENDPOINT").value());
        assertEquals("false", options.get("USE_SSL").value());
        assertEquals("path", options.get("URL_STYLE").value());
        assertFalse(credential.toString().contains("sec\""));
    }

    @Test
    void configCredentialsApplyToTheTableLocation() throws IOException {
        JsonNode body = JSON.readTree("{\"config\":{\"s3.access-key-id\":\"AKIA3\","
                + "\"s3.secret-access-key\":\"sec\",\"s3.endpoint\":\"https://s3.example\"}}");
        List<StorageCredential> credentials = VendedCredentials.of(body, "s3a://lake/wh/ns/t");
        assertEquals(1, credentials.size());
        assertEquals("s3a://lake/wh/ns/t", credentials.get(0).prefix());
        assertNull(credentials.get(0).expiresAt());
        assertEquals("true", credentials.get(0).options().get("USE_SSL").value());
        assertEquals(List.of(), VendedCredentials.of(JSON.readTree("{\"config\":{}}"), "s3://x/t"));
        assertEquals(List.of(), VendedCredentials.of(body, "/local/wh/ns/t"));
        assertEquals(Long.MAX_VALUE, VendedCredentials.earliestExpiryMillis(credentials));
    }

    @Test
    void offByDefaultTheCatalogIsNotAskedForCredentials() throws IOException {
        try (FakeRestCatalog rest = new FakeRestCatalog(warehouse)) {
            rest.vendCredentials(3_600_000, "s3://lake/wh");
            try (Catalog catalog = rest(rest, Map.of())) {
                TableSnapshot snapshot = catalog.current(TYPES).orElseThrow();
                assertEquals(List.of(), snapshot.credentials());
                assertEquals(0, rest.vended());
            }
        }
    }

    @Test
    void vendedCredentialsTravelWithTheSnapshotAndAreRefreshedBeforeTheyExpire()
            throws IOException {
        try (FakeRestCatalog rest = new FakeRestCatalog(warehouse)) {
            rest.vendCredentials(3_600_000, "s3://lake/wh");
            try (Catalog catalog = rest(rest, Map.of("vendedCredentials", "true"))) {
                TableSnapshot first = catalog.current(TYPES).orElseThrow();
                assertEquals(1, first.credentials().size());
                assertEquals("AKIA1", first.credentials().get(0).options().get("KEY_ID").value());
                assertEquals("s3://lake/wh", first.credentials().get(0).prefix());
                // Within the metadata TTL and far from expiry: the same load serves.
                TableSnapshot again = catalog.current(TYPES).orElseThrow();
                assertEquals(first.credentials(), again.credentials());
                assertEquals(1, rest.vended());
                // pinned (executing nodes) hands the credentials too.
                Optional<TableSnapshot> pinned = catalog.pinned(TYPES, first.snapshotId());
                assertEquals(first.credentials(), pinned.orElseThrow().credentials());
                assertEquals(first.files(), again.files());
            }
            // Credentials that expire within the margin are replaced at the next call.
            rest.vendCredentials(IcebergCatalog.CREDENTIALS_MARGIN_MILLIS - 1000, "s3://lake/wh");
            try (Catalog catalog = rest(rest, Map.of("vendedCredentials", "true"))) {
                TableSnapshot first = catalog.current(TYPES).orElseThrow();
                TableSnapshot second = catalog.current(TYPES).orElseThrow();
                assertEquals("AKIA2", first.credentials().get(0).options().get("KEY_ID").value());
                assertEquals("AKIA3", second.credentials().get(0).options().get("KEY_ID").value());
                assertEquals(first.fingerprint(), second.fingerprint(),
                        "new credentials do not change the content fingerprint");
            }
        }
    }

    private static Catalog rest(FakeRestCatalog rest, Map<String, String> extra) throws IOException {
        Map<String, String> values = new HashMap<>(Map.of("uri", rest.uri(), "roots", warehouse,
                "credential", FakeRestCatalog.CREDENTIAL, "metadataTtlMillis", "600000"));
        values.putAll(extra);
        TestSupport.LocalStores stores = new TestSupport.LocalStores();
        Catalog catalog = new IcebergCatalogProvider(IcebergSettings.Kind.REST).open(
                new TestSupport.Context(new TestSupport.MapConfig("iceberg-rest", values), stores,
                        new SecretRedactor()));
        stores.freeze();
        return catalog;
    }
}
