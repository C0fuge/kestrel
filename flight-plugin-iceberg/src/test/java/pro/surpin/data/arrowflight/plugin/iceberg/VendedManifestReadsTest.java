package pro.surpin.data.arrowflight.plugin.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.plugin.s3.FakeS3;
import pro.surpin.data.arrowflight.plugin.s3.SigV4Check;
import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.StorageCredential;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;
import pro.surpin.data.arrowflight.spi.duckdb.SecretValue;

/**
 * C1 step 2b (ii): a REST table whose catalog vends S3 credentials has its manifest lists and
 * manifests read with those credentials (SigV4-signed GETs of the plugin's own), never through
 * the server's stores, whose keys here cannot read the warehouse.
 */
class VendedManifestReadsTest {

    private static final String WAREHOUSE = "s3://lake/wh";
    private static FakeS3 s3;
    private static InMemoryCatalog tables;

    @BeforeAll
    static void writeTables() throws IOException {
        s3 = FakeS3.start();
        tables = new InMemoryCatalog();
        tables.initialize("mem", Map.of("warehouse", WAREHOUSE));
        tables.createNamespace(Namespace.of("ice"));
        IcebergFixtures.write(tables, Set.of());
        upload(s3);
    }

    /**
     * Copies the files the in-memory catalog wrote below the warehouse into the fake S3 bucket.
     * Iceberg's InMemoryFileIO keeps them in one static map, which it does not list.
     *
     * @param target fake S3
     */
    @SuppressWarnings("unchecked")
    private static void upload(FakeS3 target) {
        Map<String, byte[]> files;
        try {
            java.lang.reflect.Field field = InMemoryFileIO.class.getDeclaredField("IN_MEMORY_FILES");
            field.setAccessible(true);
            files = (Map<String, byte[]>) field.get(null);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("InMemoryFileIO changed", e);
        }
        files.forEach((location, data) -> {
            if (location.startsWith(WAREHOUSE + "/")) {
                target.put("lake", location.substring("s3://lake/".length()), data);
            }
        });
    }

    @AfterAll
    static void stop() throws IOException {
        s3.close();
    }

    @Test
    void manifestsAreReadWithTheVendedCredentials() throws IOException {
        TestSupport.RefusingStore store = new TestSupport.RefusingStore(WAREHOUSE);
        TestSupport.LocalStores stores = new TestSupport.LocalStores();
        stores.opened.add(store);
        stores.freeze();
        try (FakeRestCatalog rest = new FakeRestCatalog(tables)) {
            rest.vendCredentials(3_600_000L, null);
            rest.vendS3Endpoint(s3.endpoint(), true);
            Map<String, String> values = Map.of("uri", rest.uri(), "roots", WAREHOUSE,
                    "credential", FakeRestCatalog.CREDENTIAL, "vendedCredentials", "true");
            try (Catalog catalog = new IcebergCatalogProvider(IcebergSettings.Kind.REST).open(
                    new TestSupport.Context(new TestSupport.MapConfig("iceberg-rest", values), stores,
                            new SecretRedactor()))) {
                s3.clearRequests();
                for (String table : List.of("types", "evolved", "parted", "deletes", "nested", "empty")) {
                    TableSnapshot snapshot = catalog.current(new TableName("ice", table)).orElseThrow();
                    assertEquals(expectedFiles(table), locations(snapshot), table);
                    assertFalse(snapshot.credentials().isEmpty(), table);
                }
                assertEquals(List.of(), store.opened, "the stores read nothing");
            }
        }
        List<FakeS3.Request> requests = s3.requests();
        assertFalse(requests.isEmpty());
        Map<String, String> secrets = new HashMap<>();
        for (int n = 1; n <= 20; n++) {
            secrets.put("AKIA" + n, "secret-" + n);
        }
        for (FakeS3.Request request : requests) {
            Optional<String> key = SigV4Check.verify(request, secrets);
            assertTrue(key.isPresent(), "valid signature: " + request);
            assertEquals("token-" + key.get().substring(4), request.headers().get("x-amz-security-token"),
                    "the session token of the same credential");
            assertTrue(request.headers().get("authorization").contains("x-amz-security-token"),
                    "the token is signed");
            assertTrue(request.path().startsWith("/lake/wh/ice/"), request.path());
        }
        assertTrue(requests.stream().anyMatch(r -> r.path().contains("/metadata/snap-")),
                "manifest lists read with the vended credentials");
    }

    @Test
    void readsOutsideTheRootsOrTheCredentialStayWithTheStores() throws IOException {
        TestSupport.RefusingStore store = new TestSupport.RefusingStore(WAREHOUSE);
        TestSupport.LocalStores stores = new TestSupport.LocalStores();
        stores.opened.add(store);
        stores.freeze();
        StoresFileIO base = new StoresFileIO(stores);
        StorageCredential credential = credential("s3://lake/wh/ice/types");
        StoresFileIO io = base.withCredentials(List.of(credential), List.of("s3://lake/wh/ice"),
                HttpClient.newHttpClient(), Duration.ofSeconds(10), Clock.systemUTC());
        assertTrue(io.vendedFor("s3://lake/wh/ice/types/metadata/x.avro") != null);
        assertEquals(null, io.vendedFor("s3://lake/wh/ice/evolved/metadata/x.avro"),
                "not below the credential's prefix");
        StoresFileIO narrow = base.withCredentials(List.of(credential("s3://lake")),
                List.of("s3://lake/wh/ice/types"), HttpClient.newHttpClient(), Duration.ofSeconds(10),
                Clock.systemUTC());
        assertEquals(null, narrow.vendedFor("s3://lake/other/x.avro"), "outside the catalog's roots");
        assertTrue(base == base.withCredentials(List.of(new StorageCredential("gs://b/t", "gcs",
                Map.of("TOKEN", SecretValue.sensitive("x")), null)), List.of("gs://b"),
                HttpClient.newHttpClient(), Duration.ofSeconds(10), Clock.systemUTC()),
                "no S3 credential: the stores only");
        InputFile outside = io.newInputFile("s3a://lake/wh/ice/evolved/metadata/missing.avro");
        assertThrows(RuntimeException.class, outside::getLength);
        assertEquals(List.of("ice/evolved/metadata/missing.avro"), store.opened);
    }

    @Test
    void missingObjectsAndRefusalsAreClear() throws IOException {
        StorageCredential credential = credential("s3://lake/wh");
        VendedS3 reader = VendedS3.of(credential, HttpClient.newHttpClient(), Duration.ofSeconds(10),
                Clock.systemUTC()).orElseThrow();
        assertThrows(FileNotFoundException.class, () -> reader.length("s3://lake/wh/nope"));
        assertFalse(reader.exists("s3://lake/wh/nope"));
        s3.put("lake", "wh/plain.bin", "0123456789".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertEquals(10, reader.length("s3://lake/wh/plain.bin"));
        byte[] buffer = new byte[4];
        reader.readFully("s3://lake/wh/plain.bin", 3, buffer, 0, 4);
        assertEquals("3456", new String(buffer, java.nio.charset.StandardCharsets.UTF_8));
        s3.ignoreRange(true);
        try {
            reader.readFully("s3://lake/wh/plain.bin", 6, buffer, 0, 4);
            assertEquals("6789", new String(buffer, java.nio.charset.StandardCharsets.UTF_8),
                    "a whole-object answer is cut to the range");
        } finally {
            s3.ignoreRange(false);
        }
    }

    @Test
    void urlsFollowTheEndpointAndStyle() throws IOException {
        VendedS3 aws = VendedS3.of(new StorageCredential("s3://bucket/t", "s3", Map.of(
                "KEY_ID", SecretValue.plain("AKIA"), "SECRET", SecretValue.sensitive("s"),
                "REGION", SecretValue.plain("eu-west-1")), null), HttpClient.newHttpClient(),
                Duration.ofSeconds(1), Clock.systemUTC()).orElseThrow();
        assertEquals("https://bucket.s3.eu-west-1.amazonaws.com/t/a%20b%2Bc.avro",
                aws.target("s3://bucket/t/a b+c.avro").url());
        VendedS3 path = VendedS3.of(credential("s3://bucket/t"), HttpClient.newHttpClient(),
                Duration.ofSeconds(1), Clock.systemUTC()).orElseThrow();
        VendedS3.Target target = path.target("s3://bucket/t/x.avro");
        assertEquals(s3.endpoint() + "/bucket/t/x.avro", target.url());
        assertEquals(target.url().replaceFirst("^http://", "").replace("/bucket/t/x.avro", ""),
                target.host());
        assertTrue(VendedS3.of(new StorageCredential("s3://b/t", "s3",
                Map.of("KEY_ID", SecretValue.plain("AKIA")), null), HttpClient.newHttpClient(),
                Duration.ofSeconds(1), Clock.systemUTC()).isEmpty(), "no secret key");
        assertFalse(path.toString().contains("secret"));
    }

    private static StorageCredential credential(String prefix) {
        Map<String, SecretValue> options = new LinkedHashMap<>();
        options.put("KEY_ID", SecretValue.plain("AKIA1"));
        options.put("SECRET", SecretValue.sensitive("secret-1"));
        options.put("SESSION_TOKEN", SecretValue.sensitive("token-1"));
        options.put("REGION", SecretValue.plain("us-east-1"));
        options.put("ENDPOINT", SecretValue.plain(s3.endpoint().replaceFirst("^http://", "")));
        options.put("USE_SSL", SecretValue.of(false));
        options.put("URL_STYLE", SecretValue.plain("path"));
        return new StorageCredential(prefix, "s3", options, null);
    }

    private static Set<String> expectedFiles(String table) throws IOException {
        Set<String> result = new TreeSet<>();
        try (CloseableIterable<FileScanTask> tasks = tables.loadTable(TableIdentifier.of("ice", table))
                .newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                result.add(task.file().location());
            }
        }
        return result;
    }

    private static Set<String> locations(TableSnapshot snapshot) {
        Set<String> result = new TreeSet<>();
        for (TableFile file : snapshot.files()) {
            result.add(file.location());
        }
        return result;
    }
}
