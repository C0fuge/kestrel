package pro.surpin.data.arrowflight.server.storage;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.duckdb.DuckDbSetupPlan;
import pro.surpin.data.arrowflight.plugin.s3.FakeS3;
import pro.surpin.data.arrowflight.plugin.s3.TestTls;
import pro.surpin.data.arrowflight.server.model.AppConfig;

import pro.surpin.data.arrowflight.spi.storage.DataFile;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    private int threads = 1;
    private boolean externalFileCache;

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

    /**
     * Verifies a read rides out an endpoint that refuses connections for a while, at 8 DuckDB
     * threads (item 0e: on the stand MinIO refused connections at 8 threads and DuckDB's own
     * half-second of retries gave up), and that without retries the same outage fails the query.
     */
    @Test
    void connectRefusalsAreRetried() throws Exception {
        Assumptions.assumeTrue(s3 != null, "fake S3 only");
        byte[] bytes = Files.readAllBytes(dir.resolve("data.parquet"));
        StringBuilder files = new StringBuilder();
        for (int i = 0; i < 8; i++) {
            s3.put(bucket, "flight-it/m/x" + i + ".parquet", bytes);
            files.append(i == 0 ? "" : ", ").append("'s3://").append(bucket)
                    .append("/flight-it/m/x").append(i).append(".parquet'");
        }
        String sql = "SELECT count(*), sum(id) FROM read_parquet([" + files + "])";
        try (Forwarder forwarder = new Forwarder(s3.endpoint())) {
            endpoint = forwarder.endpoint();
            threads = 8;
            open(null);
            forwarder.refuseFor(1500);
            try (Statement s = duckDb.connection().createStatement();
                    ResultSet rs = s.executeQuery(sql)) {
                assertTrue(rs.next());
                assertEquals(8 * 5000, rs.getLong(1));
                assertEquals(8 * (5000L * 4999 / 2), rs.getLong(2));
            }

            duckDb.close();
            registry.close();
            plugin.properties.put("arrowflight.s3.httpRetries", "0");
            open(null);
            forwarder.refuseFor(1500);
            try (Statement s = duckDb.connection().createStatement()) {
                SQLException e = assertThrows(SQLException.class, () -> s.executeQuery(sql));
                // "Could not establish connection" (1.4.5), "Could not connect to server" (1.5.6)
                assertTrue(e.getMessage().contains("connect"), e.getMessage());
                assertFalse(e.getMessage().contains(secretKey));
            }
        }
    }

    /**
     * Counts the TCP connections a query opens at 8 DuckDB threads (item 0g: on the stand 32
     * clients ran the host out of local ports, every closed connection holding its port 60 s in
     * TIME_WAIT). Without connection caching httpfs keeps connections per open file only, and a
     * thread opens a file anew for each row group it takes, so a scan opens about one connection
     * per range read: bounded by the files and row groups, never more than the requests; the
     * external file cache reads the same files again without a connection. With connection
     * caching (the default) a session's DuckDB opens at most a few connections per thread once
     * and reads every later query over them.
     */
    @Test
    void connectionsPerQueryAreBoundedAndCachedReadsOpenNone() throws Exception {
        Assumptions.assumeTrue(s3 != null, "fake S3 only");
        int files = 8;
        int rowGroups = 4;
        Path data = dir.resolve("groups.parquet");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT range AS id, md5(range::VARCHAR) AS pad FROM range(" + rowGroups * 20_000
                    + ")) TO '" + data + "' (FORMAT parquet, ROW_GROUP_SIZE 20000)");
        }
        byte[] bytes = Files.readAllBytes(data);
        StringBuilder list = new StringBuilder();
        for (int i = 0; i < files; i++) {
            s3.put(bucket, "flight-it/g/x" + i + ".parquet", bytes);
            list.append(i == 0 ? "" : ", ").append("'s3://").append(bucket)
                    .append("/flight-it/g/x").append(i).append(".parquet'");
        }
        String sql = "SELECT count(*), sum(id) FROM read_parquet([" + list + "]) WHERE id % 7 = 1";
        long rows = 0;
        long sum = 0;
        for (long id = 1; id < rowGroups * 20_000L; id += 7) {
            rows++;
            sum += id;
        }
        try (Forwarder forwarder = new Forwarder(s3.endpoint())) {
            endpoint = forwarder.endpoint();
            threads = 8;
            plugin.properties.put("arrowflight.s3.connectionCaching", "false");
            int[] uncached = connectionsOfQueries(forwarder, sql, files * rows, files * sum, 2);
            long requests = s3.requests().stream().filter(r -> r.path().contains("/g/")).count();

            assertTrue(uncached[0] + uncached[1] <= requests,
                    uncached[0] + "+" + uncached[1] + " connections for " + requests + " requests");
            assertTrue(uncached[0] <= files * (rowGroups + 2), "first query: " + uncached[0]);
            assertTrue(uncached[1] >= files, "without any cache each file is read again: " + uncached[1]);
            assertTrue(uncached[1] <= files * (rowGroups + 2), "second query: " + uncached[1]);

            reopen();
            externalFileCache = true;
            int[] cached = connectionsOfQueries(forwarder, sql, files * rows, files * sum, 2);
            assertTrue(cached[0] <= files * (rowGroups + 2), "first query: " + cached[0]);
            assertEquals(0, cached[1], "the cached files are read without a connection");

            reopen();
            externalFileCache = false;
            plugin.properties.remove("arrowflight.s3.connectionCaching");
            int[] kept = connectionsOfQueries(forwarder, sql, files * rows, files * sum, 4);
            assertTrue(kept[0] <= 2 * threads, "first query, connections kept: " + kept[0]);
            assertTrue(kept[0] < uncached[0], kept[0] + " vs " + uncached[0] + " without caching");
            for (int q = 1; q < kept.length; q++) {
                assertEquals(0, kept[q], "query " + q + " reads over the kept connections");
            }

            // The endpoint closes the idle kept connections (idle timeout, restart): the next
            // query opens new ones and still reads everything.
            forwarder.cutConnections();
            int before = forwarder.connections();
            try (Statement s = duckDb.connection().createStatement();
                    ResultSet rs = s.executeQuery(sql)) {
                assertTrue(rs.next());
                assertEquals(files * rows, rs.getLong(1));
                assertEquals(files * sum, rs.getLong(2));
            }
            assertTrue(forwarder.connections() > before, "new connections after the cut");
        }
    }

    @Test
    void aQueryAfterAFileWasRewrittenReadsTheNewFile() throws Exception {
        Assumptions.assumeTrue(s3 != null, "fake S3 only");
        int files = 4;
        long rows = 30_000L;
        byte[] first = parquet(dir.resolve("first.parquet"), 0);
        StringBuilder list = new StringBuilder();
        for (int i = 0; i < files; i++) {
            s3.put(bucket, "flight-it/m/x" + i + ".parquet", first);
            list.append(i == 0 ? "" : ", ").append("'s3://").append(bucket)
                    .append("/flight-it/m/x").append(i).append(".parquet'");
        }
        String sql = "SELECT count(*) AS n, sum(id) AS s FROM read_parquet([" + list + "])";
        long sum = rows * (rows - 1) / 2;
        open(null);
        try (BufferAllocator allocator = new RootAllocator()) {
            assertArrayEquals(new long[] {files * rows, files * sum}, streamed(allocator, sql));
            assertArrayEquals(new long[] {files * rows, files * sum}, streamed(allocator, sql));

            // DuckDB keeps the file's metadata per connection and refuses the rewritten object
            // once ("ETag ... was initially ..."); the server runs the query again.
            s3.put(bucket, "flight-it/m/x0.parquet", parquet(dir.resolve("second.parquet"), 1_000_000),
                    java.time.Instant.parse("2026-10-27T09:00:00Z"));
            assertArrayEquals(new long[] {files * rows, files * sum + rows * 1_000_000L},
                    streamed(allocator, sql));

            // The refusal the retry covers is still DuckDB's behaviour.
            s3.put(bucket, "flight-it/m/x0.parquet", first, java.time.Instant.parse("2026-10-27T10:00:00Z"));
            SQLException refused = assertThrows(SQLException.class, () -> {
                try (Statement s = duckDb.connection().createStatement()) {
                    s.executeQuery(sql).close();
                }
            });
            assertTrue(DuckDbAdapter.remoteFileChanged(refused), refused.getMessage());
            assertArrayEquals(new long[] {files * rows, files * sum}, streamed(allocator, sql));
        }
    }

    private static byte[] parquet(Path file, long offset) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT range + " + offset + " AS id, md5(range::VARCHAR) AS pad FROM range(30000))"
                    + " TO '" + file + "' (FORMAT parquet, ROW_GROUP_SIZE 10000)");
        }
        return Files.readAllBytes(file);
    }

    private long[] streamed(BufferAllocator allocator, String sql) throws Exception {
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);
        AtomicReference<VectorSchemaRoot> root = new AtomicReference<>();
        long[] answer = new long[2];
        doAnswer(invocation -> {
            root.set(invocation.getArgument(0));
            return null;
        }).when(listener).start(any(VectorSchemaRoot.class));
        when(listener.isReady()).thenReturn(true);
        doAnswer(invocation -> {
            answer[0] = ((BigIntVector) root.get().getVector("n")).get(0);
            answer[1] = ((Number) root.get().getVector("s").getObject(0)).longValue();
            return null;
        }).when(listener).putNext();
        duckDb.streamSql(allocator, sql, listener, true);
        verify(listener, times(1)).start(any(VectorSchemaRoot.class));
        return answer;
    }

    private void reopen() {
        duckDb.close();
        duckDb = null;
        registry.close();
        registry = null;
    }

    private int[] connectionsOfQueries(Forwarder forwarder, String sql, long rows, long sum, int queries)
            throws Exception {
        open(null);
        int[] opened = new int[queries];
        for (int q = 0; q < queries; q++) {
            int before = forwarder.connections();
            try (Statement s = duckDb.connection().createStatement();
                    ResultSet rs = s.executeQuery(sql)) {
                assertTrue(rs.next());
                assertEquals(rows, rs.getLong(1));
                assertEquals(sum, rs.getLong(2));
            }
            opened[q] = forwarder.connections() - before;
        }
        return opened;
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
        return new AppConfig(3, 1024, 2, 131072, 0, 1, threads, null, false, null, null, null,
                null, "512MB", false, Integer.MAX_VALUE, 67108864, 60000L, store.root(), null, 32010,
                5701, 60, 3, 1000, 0, true, externalFileCache, 16, dir.resolve("spill").toString(), null,
                "complete", true, true);
    }

    /**
     * Forwards TCP connections to the fake S3 and can refuse new ones for a while, the way an
     * endpoint does that has no more connections to give: the port stops listening and the open
     * connections are cut.
     */
    private static final class Forwarder implements AutoCloseable {
        private final InetSocketAddress target;
        private final List<Socket> open = new CopyOnWriteArrayList<>();
        private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
        private final ExecutorService pipes = Executors.newCachedThreadPool();
        private final AtomicInteger accepted = new AtomicInteger();
        private final int port;
        private volatile ServerSocket listener;
        private volatile boolean closed;

        /**
         * Starts forwarding.
         *
         * @param target endpoint URL of the fake S3
         * @throws IOException when no port is free
         */
        Forwarder(String target) throws IOException {
            URI uri = URI.create(target);
            this.target = new InetSocketAddress(uri.getHost(), uri.getPort());
            listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            port = listener.getLocalPort();
            accept(listener);
        }

        /**
         * Names the forwarding endpoint.
         *
         * @return {@code http://127.0.0.1:<port>}
         */
        String endpoint() {
            return "http://127.0.0.1:" + port;
        }

        /**
         * Counts the connections accepted so far.
         *
         * @return connections
         */
        int connections() {
            return accepted.get();
        }

        /**
         * Cuts every open connection and keeps listening, as an endpoint closing idle
         * connections does.
         *
         * @throws IOException when a socket cannot be closed
         */
        void cutConnections() throws IOException {
            for (Socket socket : open) {
                socket.close();
            }
            open.clear();
        }

        /**
         * Stops listening and cuts every connection, then listens again after a while.
         *
         * @param millis how long connections are refused
         * @throws IOException when the port cannot be closed
         */
        void refuseFor(long millis) throws IOException {
            listener.close();
            for (Socket socket : open) {
                socket.close();
            }
            open.clear();
            timer.schedule(this::listenAgain, millis, TimeUnit.MILLISECONDS);
        }

        private void listenAgain() {
            try {
                ServerSocket socket = new ServerSocket();
                socket.setReuseAddress(true);
                socket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 50);
                listener = socket;
                accept(socket);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private void accept(ServerSocket socket) {
            pipes.execute(() -> {
                while (!closed && !socket.isClosed()) {
                    try {
                        Socket client = socket.accept();
                        accepted.incrementAndGet();
                        Socket server = new Socket(target.getAddress(), target.getPort());
                        open.add(client);
                        open.add(server);
                        pipes.execute(() -> pipe(client, server));
                        pipes.execute(() -> pipe(server, client));
                    } catch (IOException e) {
                        // Closed for an outage or at the end.
                    }
                }
            });
        }

        private static void pipe(Socket from, Socket to) {
            try (from; to) {
                from.getInputStream().transferTo(to.getOutputStream());
            } catch (IOException e) {
                // The other side or an outage closed it.
            }
        }

        @Override
        public void close() throws IOException {
            closed = true;
            listener.close();
            for (Socket socket : open) {
                socket.close();
            }
            timer.shutdownNow();
            pipes.shutdownNow();
        }
    }
}
