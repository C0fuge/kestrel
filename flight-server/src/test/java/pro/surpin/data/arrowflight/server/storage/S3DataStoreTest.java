package pro.surpin.data.arrowflight.server.storage;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import pro.surpin.data.arrowflight.server.duckdb.DuckDbSetupPlan;
import pro.surpin.data.arrowflight.server.duckdb.DuckDbSetupValidator;

import pro.surpin.data.arrowflight.spi.storage.DataFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the S3 store against a fake S3 server on the JDK's HTTP server. */
@Tag("unit")
class S3DataStoreTest {

    private static final String BUCKET = "lake";
    private static final byte[] BYTES = "not parquet".getBytes(StandardCharsets.UTF_8);

    private FakeS3 s3;
    private S3DataStore store;
    private final AtomicLong clock = new AtomicLong(1_000_000_000L);

    @BeforeEach
    void startServer() throws IOException {
        s3 = FakeS3.start();
        s3.put(BUCKET, "parquet/tpch/", new byte[0]);
        s3.put(BUCKET, "parquet/tpch/lineitem/", new byte[0]);
        s3.put(BUCKET, "parquet/tpch/lineitem/b.parquet", new byte[200],
                Instant.parse("2026-10-01T10:00:00Z"));
        s3.put(BUCKET, "parquet/tpch/lineitem/a.PARQUET", new byte[100]);
        s3.put(BUCKET, "parquet/tpch/lineitem/_SUCCESS", BYTES);
        s3.put(BUCKET, "parquet/tpch/nation/part=1/n.parquet", new byte[50]);
        s3.put(BUCKET, "parquet/other/t/x.parquet", new byte[10]);
        s3.put(BUCKET, "parquet/bad/t?x/y.parquet", new byte[10]);
        s3.put(BUCKET, "parquetx/tpch/z/z.parquet", new byte[10]);
        s3.put(BUCKET, "elsewhere/q.parquet", new byte[10]);
    }

    @AfterEach
    void stopServer() {
        if (store != null) {
            store.close();
        }
        s3.close();
    }

    /** Verifies schemas and tables come from key prefixes, without directory markers or HEADs. */
    @Test
    void schemasAndTablesComeFromTheListing() throws IOException {
        store = store(30);

        assertTrue(store.exists(""));
        // "bad" holds only a key with a "?", which is left out with its directories.
        assertEquals(Map.of("tpch", "s3://lake/parquet/tpch", "other", "s3://lake/parquet/other"),
                store.directories(""));
        assertEquals(List.of("lineitem", "nation"),
                List.copyOf(new java.util.TreeSet<>(store.directories("tpch").keySet())));
        assertTrue(store.exists("tpch/lineitem"));
        assertTrue(store.exists("tpch/lineitem/b.parquet"));
        assertFalse(store.exists("tpch/orders"));
        assertThrows(FileNotFoundException.class, () -> store.directories("tpch/orders"));
        assertEquals(0, s3.count("HEAD"));
        assertEquals(1, s3.count("GET"), "one listing request");
    }

    /** Verifies a table's Parquet files, in path order, in any extension case, with listed sizes. */
    @Test
    void parquetFilesOfATable() throws IOException {
        store = store(30);

        List<DataFile> files = store.parquetFiles("tpch/lineitem");

        assertEquals(List.of("tpch/lineitem/a.PARQUET", "tpch/lineitem/b.parquet"),
                files.stream().map(DataFile::relativePath).toList());
        assertEquals(100, files.get(0).size());
        assertEquals(200, files.get(1).size());
        assertEquals(Instant.parse("2026-10-01T10:00:00Z").toEpochMilli(),
                files.get(1).modificationTime());
        assertTrue(files.get(0).hosts().isEmpty());
        assertEquals("s3://lake/parquet/tpch/lineitem/b.parquet", files.get(1).location());
        assertEquals("s3://lake/parquet/tpch/lineitem/b.parquet", store.duckDbUri(files.get(1)));
        assertEquals(List.of("tpch/nation/part=1/n.parquet"),
                store.parquetFiles("tpch/nation").stream().map(DataFile::relativePath).toList());
        assertEquals(List.of(), store.tableFiles("tpch", "orders"));
        assertThrows(FileNotFoundException.class, () -> store.parquetFiles("tpch/orders"));
        assertEquals(0, s3.count("HEAD"));
    }

    /** Verifies listing pages are followed and keys outside the prefix or unsafe are left out. */
    @Test
    void listingFollowsPagesAndSkipsUnsafeKeys() throws IOException {
        s3.pageSize(2);
        store = store(30);

        List<String> all = store.parquetFiles("").stream().map(DataFile::relativePath).toList();

        assertEquals(List.of("other/t/x.parquet", "tpch/lineitem/a.PARQUET",
                "tpch/lineitem/b.parquet", "tpch/nation/part=1/n.parquet"), all);
        assertTrue(s3.count("GET") > 1, "paged listing: " + s3.requests());
        assertTrue(s3.requests().stream().allMatch(r -> "parquet/".equals(r.query().get("prefix"))));
    }

    /** Verifies a file missing from the listing is looked up, and a missing one is reported. */
    @Test
    void fileNewerThanTheListingIsLookedUp() throws IOException {
        store = store(30);
        store.parquetFiles("tpch/lineitem");
        s3.put(BUCKET, "parquet/tpch/lineitem/c.parquet", new byte[300]);

        DataFile file = store.file("tpch/lineitem/c.parquet");

        assertEquals(300, file.size());
        assertEquals(1, s3.count("HEAD"));
        assertEquals(100, store.file("tpch/lineitem/a.PARQUET").size());
        assertEquals(1, s3.count("HEAD"), "listed files need no lookup");
        assertThrows(FileNotFoundException.class, () -> store.file("tpch/lineitem/none.parquet"));
    }

    /** Verifies an expired listing is refreshed in the background while the old one is used. */
    @Test
    void expiredListingIsRefreshedInTheBackground() throws Exception {
        store = store(30);
        assertEquals(2, store.parquetFiles("tpch/lineitem").size());
        s3.put(BUCKET, "parquet/tpch/lineitem/c.parquet", new byte[300]);

        assertEquals(2, store.parquetFiles("tpch/lineitem").size(), "still fresh");
        clock.addAndGet(TimeUnit.SECONDS.toNanos(31));
        int seen = store.parquetFiles("tpch/lineitem").size();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (seen == 2 && System.nanoTime() < deadline) {
            Thread.sleep(20);
            seen = store.parquetFiles("tpch/lineitem").size();
        }

        assertEquals(3, seen);
    }

    /** Verifies a zero lifetime lists on every use. */
    @Test
    void zeroTtlListsEveryTime() throws IOException {
        store = store(0);
        store.parquetFiles("tpch/lineitem");
        s3.put(BUCKET, "parquet/tpch/lineitem/c.parquet", new byte[300]);

        assertEquals(3, store.parquetFiles("tpch/lineitem").size());
    }

    /** Verifies paths that climb out of the data directory are refused. */
    @Test
    void unsafePathsAreRefused() {
        store = store(30);

        assertThrows(IllegalArgumentException.class, () -> store.file("../elsewhere/q.parquet"));
        assertThrows(IllegalArgumentException.class, () -> store.duckDbUri("tpch/../../x.parquet"));
        assertThrows(IllegalArgumentException.class, () -> store.duckDbUri("/abs.parquet"));
        assertThrows(IllegalArgumentException.class,
                () -> store.duckDbUri("tpch/x.parquet?s3_endpoint=evil:1"));
    }

    /** Verifies the store describes itself as shared, without locality or a local inventory. */
    @Test
    void storeKind() {
        store = store(30);

        assertEquals("s3", store.kind());
        assertTrue(store.shared());
        assertFalse(store.hasLocality());
        assertEquals("s3://lake/parquet", store.root());
        assertEquals(List.of("s3://lake/parquet/"), store.duckDbRoots());
        assertEquals(Map.of(), store.localInventory("anyhost"));
    }

    /** Verifies a footer below 64 KB is read with one ranged GET. */
    @Test
    void footerIsReadWithOneRangedGet(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("data.parquet");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT range AS id, 'x' || range AS name FROM range(5000)) TO '"
                    + file + "' (FORMAT parquet, ROW_GROUP_SIZE 2000)");
        }
        byte[] data = Files.readAllBytes(file);
        s3.put(BUCKET, "parquet/tpch/lineitem/data.parquet", data);
        store = store(30);
        DataFile listed = store.file("tpch/lineitem/data.parquet");
        s3.clearRequests();

        long rows;
        try (ParquetFileReader reader = ParquetFileReader.open(
                ParquetInputFiles.of(store.open(listed), listed.location()))) {
            rows = reader.getFooter().getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();
        }

        assertEquals(5000, rows);
        assertEquals(1, s3.count("GET"), "requests: " + s3.requests());
        String range = s3.requests().get(0).headers().get("range");
        long start = Math.max(0, data.length - S3ReadableFile.BLOCK_SIZE);
        assertEquals("bytes=" + start + "-" + (data.length - 1), range);
    }

    /** Verifies requests carry SigV4 for the configured key and no unrequested checksums. */
    @Test
    void requestsAreSignedWithTheStaticKey() throws IOException {
        store = store(30);
        store.exists("");

        FakeS3.Request list = s3.requests().get(0);
        String authorization = list.headers().get("authorization");
        assertTrue(authorization.startsWith("AWS4-HMAC-SHA256 Credential=AKIATEST/"), authorization);
        assertTrue(authorization.contains("/eu-west-3/s3/aws4_request"), authorization);
        assertFalse(authorization.contains("SECRETTEST"));
        assertFalse(list.headers().containsKey("x-amz-checksum-mode"), list.headers().toString());
    }

    /** Verifies TLS trusts exactly the certificates of the CA file. */
    @Test
    void tlsTrustsOnlyTheCaFile(@TempDir Path dir) throws Exception {
        TestTls server = TestTls.create("fake-s3");
        TestTls other = TestTls.create("other-ca");
        try (FakeS3 tls = FakeS3.startTls(server.serverContext())) {
            tls.put(BUCKET, "parquet/tpch/lineitem/a.parquet", new byte[10]);
            S3Settings trusted = settings(tls.endpoint(), server.writePem(dir.resolve("ca.pem")).toString(), 30);
            try (S3DataStore s = new S3DataStore("s3://lake/parquet", trusted,
                    S3DataStore.createClient(trusted))) {
                assertEquals(1, s.parquetFiles("tpch/lineitem").size());
            }
            S3Settings wrongCa = settings(tls.endpoint(), other.writePem(dir.resolve("other.pem")).toString(), 30);
            try (S3DataStore s = new S3DataStore("s3://lake/parquet", wrongCa,
                    S3DataStore.createClient(wrongCa))) {
                IOException e = assertThrows(IOException.class, () -> s.parquetFiles("tpch/lineitem"));
                assertFalse(e.getMessage().contains("SECRETTEST"));
            }
        }
    }

    /**
     * Golden: the DuckDB setup written by the validator from the store's typed contribution is
     * exactly the statement list S3DataStore wrote itself before the SPI: settings blanked,
     * credentials only in a scoped secret.
     */
    @Test
    void duckDbSetupStatements(@TempDir Path dir) throws IOException {
        Path ca = Files.writeString(dir.resolve("ca.pem"), "pem");
        S3Settings settings = new S3Settings("https://s3.local:9443", "eu-west-3", "vhost",
                ca.toString(), true, "AKIATEST", "SEC'RET", null, 30, 4,
                "/ext/httpfs.duckdb_extension", true);

        DuckDbSetupPlan plan = plan(settings, "s3a://lake/parquet/");

        assertEquals(List.of(
                "LOAD '/ext/httpfs.duckdb_extension'",
                "SET s3_access_key_id = ''",
                "SET s3_secret_access_key = ''",
                "SET s3_session_token = ''",
                "SET s3_endpoint = ''",
                "SET s3_region = ''",
                "SET s3_url_compatibility_mode = true",
                "SET enable_http_logging = false",
                "SET enable_server_cert_verification = true",
                "SET ca_cert_file = '" + ca + "'",
                "SET parquet_metadata_cache = true",
                "SET enable_http_metadata_cache = true",
                "CREATE TEMPORARY SECRET flight_s3 (TYPE s3, KEY_ID 'AKIATEST', SECRET 'SEC''RET',"
                        + " REGION 'eu-west-3', ENDPOINT 's3.local:9443', URL_STYLE 'vhost',"
                        + " USE_SSL true, URL_COMPATIBILITY_MODE true, SCOPE 's3://lake/parquet/')"),
                plan.statements());
        assertEquals(1, plan.statements().stream().filter(st -> st.contains("SEC''RET")).count());
        assertEquals(List.of("s3://lake/parquet/"), plan.roots());
        assertEquals(List.of(new DuckDbSetupPlan.DeclaredSecret("flight_s3", "s3://lake/parquet/")),
                plan.secrets());
        assertEquals("x *** y", plan.redactor().redact("x SEC'RET y"));
    }

    /** Golden: plain HTTP leaves the certificate settings alone and the cache can be off. */
    @Test
    void duckDbSetupOverPlainHttp() {
        S3Settings settings = new S3Settings("http://minio:9000", null, null, null, true,
                "AKIATEST", "SECRETTEST", "TOKEN", 30, 4, "/ext/httpfs.duckdb_extension", false);

        DuckDbSetupPlan plan = plan(settings, "s3://lake");
        List<String> statements = plan.statements();

        assertEquals(List.of(
                "LOAD '/ext/httpfs.duckdb_extension'",
                "SET s3_access_key_id = ''",
                "SET s3_secret_access_key = ''",
                "SET s3_session_token = ''",
                "SET s3_endpoint = ''",
                "SET s3_region = ''",
                "SET s3_url_compatibility_mode = true",
                "SET enable_http_logging = false",
                "CREATE TEMPORARY SECRET flight_s3 (TYPE s3, KEY_ID 'AKIATEST',"
                + " SECRET 'SECRETTEST', SESSION_TOKEN 'TOKEN', REGION 'us-east-1',"
                + " ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false,"
                + " URL_COMPATIBILITY_MODE true, SCOPE 's3://lake/')"), statements);
        assertEquals("*** ***", plan.redactor().redact("SECRETTEST TOKEN"));
    }

    /** Golden: without credentials or an extension the secret has no keys and nothing is loaded. */
    @Test
    void duckDbSetupWithoutCredentials() {
        S3Settings settings = new S3Settings("https://s3.local", null, null, null, false,
                null, null, null, 30, 4, null, true);

        List<String> statements = plan(settings, "s3://lake/a/b").statements();

        assertEquals(List.of(
                "SET s3_access_key_id = ''",
                "SET s3_secret_access_key = ''",
                "SET s3_session_token = ''",
                "SET s3_endpoint = ''",
                "SET s3_region = ''",
                "SET s3_url_compatibility_mode = true",
                "SET enable_http_logging = false",
                "SET enable_server_cert_verification = false",
                "SET parquet_metadata_cache = true",
                "SET enable_http_metadata_cache = true",
                "CREATE TEMPORARY SECRET flight_s3 (TYPE s3, REGION 'us-east-1',"
                + " ENDPOINT 's3.local', URL_STYLE 'path', USE_SSL true,"
                + " URL_COMPATIBILITY_MODE true, SCOPE 's3://lake/a/b/')"), statements);
    }

    private static DuckDbSetupPlan plan(S3Settings settings, String dataDirectory) {
        S3DataStore s3Store = new S3DataStore(dataDirectory, settings,
                S3DataStore.createClient(new S3Settings("http://127.0.0.1:1", null, null, null, true,
                        "A", "B", null, 30, 1, null, true)));
        try {
            return new DuckDbSetupValidator(name -> "/ext/" + name + ".duckdb_extension").plan(
                    DuckDbSetupValidator.Contribution.ofStore("s3", s3Store), List.of(), List.of());
        } finally {
            s3Store.close();
        }
    }

    private S3DataStore store(int ttlSeconds) {
        S3Settings settings = settings(s3.endpoint(), null, ttlSeconds);
        return new S3DataStore("s3://lake/parquet", settings, S3DataStore.createClient(settings),
                clock::get);
    }

    private static S3Settings settings(String endpoint, String caFile, int ttlSeconds) {
        return new S3Settings(endpoint, "eu-west-3", "path", caFile, true, "AKIATEST",
                "SECRETTEST", null, ttlSeconds, 4, null, true);
    }
}
