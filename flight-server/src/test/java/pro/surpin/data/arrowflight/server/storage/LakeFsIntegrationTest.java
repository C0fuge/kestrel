package pro.surpin.data.arrowflight.server.storage;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import pro.surpin.data.arrowflight.plugin.s3.FakeS3;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.metrics.MetricsService;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.spi.storage.DataFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DuckDB reading a lakeFS repository through the httpfs extension, locked down the way the
 * server locks it down, with the lakefs plugin loaded from plugins/lakefs: a fake S3 gateway
 * (bucket = repository) and a fake lakeFS REST API for the branch head. Needs the extension file
 * of the bundled DuckDB: {@code -Ds3.it.httpfs=<path>} or {@code DUCKDB_HTTPFS_EXTENSION};
 * skipped without it.
 */
@Tag("s3")
class LakeFsIntegrationTest {

    private static final String SECRET = "LAKEFS-it-secret-Qw7";

    @TempDir
    Path dir;

    private String httpfs;
    private S3PluginSupport support;
    private FakeS3 gateway;
    private HttpServer api;
    private ExecutorService ioPool;
    private final AtomicReference<String> head = new AtomicReference<>("c1");

    @BeforeEach
    void setUp() throws Exception {
        httpfs = System.getProperty("s3.it.httpfs", System.getenv("DUCKDB_HTTPFS_EXTENSION"));
        Assumptions.assumeTrue(httpfs != null && Files.isReadable(Path.of(httpfs)),
                "httpfs extension not configured (-Ds3.it.httpfs or DUCKDB_HTTPFS_EXTENSION)");
        gateway = FakeS3.start();
        gateway.put("lake", "c1/wh/tpch/nation/n.parquet", parquet(dir.resolve("c1.parquet"), 25));
        gateway.put("lake", "c2/wh/tpch/nation/n.parquet", parquet(dir.resolve("c2.parquet"), 30));
        api = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        api.createContext("/api/v1/repositories/lake/branches/main", exchange -> {
            byte[] body = ("{\"id\":\"main\",\"commit_id\":\"" + head.get() + "\"}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        api.start();
        support = new S3PluginSupport(dir, "lakefs",
                Path.of("../flight-plugin-lakefs/target/plugin/lakefs"));
        support.properties.put("duckDbHttpfsExtension", httpfs);
        support.properties.put("arrowflight.lakefs.endpoint", gateway.endpoint());
        support.properties.put("arrowflight.lakefs.apiEndpoint",
                "http://127.0.0.1:" + api.getAddress().getPort() + "/api/v1");
        support.properties.put("arrowflight.lakefs.pinTtlMillis", "0");
        support.environment.put("FLIGHT_LAKEFS_ACCESS_KEY", "AKIALAKEFSIT");
        support.environment.put("FLIGHT_LAKEFS_SECRET_KEY", SECRET);
        ioPool = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        if (ioPool != null) {
            ioPool.shutdownNow();
        }
        if (gateway != null) {
            gateway.close();
        }
        if (api != null) {
            api.stop(0);
        }
    }

    /**
     * Verifies the startup check passes with the lakefs secret scope, DuckDB reads the pinned
     * commit even after the branch moved, a new listing reads the new commit, and nothing outside
     * the repository or another setting is allowed.
     */
    @Test
    void duckDbReadsThePinnedCommitThroughTheGateway() throws Exception {
        try (StoreRegistry registry = support.open(StoreRegistryTest.config("lakefs://lake/main/wh"))) {
            DataStore store = registry.dataDirectory();
            DataFile pinned = store.tableFiles("tpch", "nation").get(0);
            assertEquals("@c1/tpch/nation/n.parquet", pinned.relativePath());
            try (DuckDbAdapter duckDb = new DuckDbAdapter(config(store.root()), ioPool,
                    registry.duckDbSetup());
                    Statement s = duckDb.connection().createStatement()) {
                head.set("c2");
                assertEquals(25, count(s, store.duckDbUri(pinned)));
                DataFile moved = store.tableFiles("tpch", "nation").get(0);
                assertEquals("@c2/tpch/nation/n.parquet", moved.relativePath());
                assertEquals(30, count(s, store.duckDbUri(moved)));

                try (ResultSet rs = s.executeQuery("SELECT name, scope::VARCHAR, secret_string"
                        + " FROM duckdb_secrets()")) {
                    assertTrue(rs.next());
                    assertTrue(rs.getString(2).contains("s3://lake/"), rs.getString(2));
                    assertFalse(rs.getString(3).contains(SECRET), "redacted");
                    assertFalse(rs.next());
                }
                for (String sql : List.of("SELECT count(*) FROM read_parquet('s3://other/x.parquet')",
                        "SELECT count(*) FROM read_text('/etc/hostname')",
                        "SET s3_endpoint = 'evil:1'", "DROP SECRET flight_s3")) {
                    SQLException e = assertThrows(SQLException.class, () -> s.execute(sql), sql);
                    assertFalse(String.valueOf(e.getMessage()).contains(SECRET), sql);
                }
            }
        }
    }

    /**
     * Verifies that with {@code arrowflight.lakefs.cacheMegabytes} DuckDB reads the gateway
     * through the plugin's read cache: the secret names the loopback endpoint, a second
     * connection reads the pinned commit without a GET to the gateway, a moved branch is read at
     * its new commit, and the hits show up as {@code arrowflight_lakefs_cache_*} metrics.
     */
    @Test
    void duckDbReadsThePinnedCommitThroughTheReadCache() throws Exception {
        support.properties.put("arrowflight.lakefs.cacheMegabytes", "16");
        try (StoreRegistry registry = support.open(StoreRegistryTest.config("lakefs://lake/main/wh"))) {
            DataStore store = registry.dataDirectory();
            DataFile pinned = store.tableFiles("tpch", "nation").get(0);
            String uri = store.duckDbUri(pinned);
            gateway.clearRequests();
            try (DuckDbAdapter duckDb = new DuckDbAdapter(config(store.root()), ioPool,
                    registry.duckDbSetup());
                    Statement s = duckDb.connection().createStatement()) {
                assertEquals(25, count(s, uri));
                assertTrue(gateway.count("GET") > 0, "the first read reaches the gateway");
                try (ResultSet rs = s.executeQuery("SELECT scope::VARCHAR, secret_string FROM duckdb_secrets()")) {
                    assertTrue(rs.next());
                    assertTrue(rs.getString(1).contains("s3://lake/"), rs.getString(1));
                    String gatewayHost = gateway.endpoint().substring("http://".length());
                    assertFalse(rs.getString(2).contains(gatewayHost), rs.getString(2));
                    assertFalse(rs.getString(2).contains(SECRET), "redacted");
                }
            }
            gateway.clearRequests();
            try (DuckDbAdapter other = new DuckDbAdapter(config(store.root()), ioPool,
                    registry.duckDbSetup());
                    Statement s = other.connection().createStatement()) {
                assertEquals(25, count(s, uri));
                assertEquals(0, gateway.count("GET"), "another connection reads the commit from the cache");
                head.set("c2");
                DataFile moved = store.tableFiles("tpch", "nation").get(0);
                assertEquals("@c2/tpch/nation/n.parquet", moved.relativePath());
                assertEquals(30, count(s, store.duckDbUri(moved)));
            }
            Matcher hits = Pattern.compile("\narrowflight_lakefs_cache_hits_total (\\S+)\n")
                    .matcher(MetricsService.render());
            assertTrue(hits.find(), "lakefs cache metrics on the Prometheus endpoint");
            assertTrue(Double.parseDouble(hits.group(1)) > 0, hits.group());
        }
    }

    private static long count(Statement s, String uri) throws SQLException {
        try (ResultSet rs = s.executeQuery("SELECT count(*) FROM read_parquet("
                + DuckDbAdapter.sqlStringLiteral(uri) + ")")) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    private static byte[] parquet(Path file, int rows) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT range AS n_nationkey FROM range(" + rows + ")) TO '" + file
                    + "' (FORMAT parquet)");
        }
        return Files.readAllBytes(file);
    }

    private AppConfig config(String root) {
        return new AppConfig(3, 1024, 2, 131072, 0, 1, 1, null, false, null, null, null,
                null, "512MB", false, Integer.MAX_VALUE, 67108864, 60000L, root, null, 32010,
                5701, 60, 3, 1000, 0, true, false, 16, dir.resolve("spill").toString(), null,
                "complete", true, true);
    }
}
