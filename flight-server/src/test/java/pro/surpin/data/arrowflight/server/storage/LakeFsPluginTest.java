package pro.surpin.data.arrowflight.server.storage;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import pro.surpin.data.arrowflight.plugin.s3.FakeS3;
import pro.surpin.data.arrowflight.server.duckdb.DuckDbSetupPlan;
import pro.surpin.data.arrowflight.spi.storage.DataFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The lakefs plugin as the server loads it from plugins/lakefs: a fake S3 gateway (bucket =
 * repository) and a fake lakeFS REST API for the branch head.
 */
@Tag("unit")
class LakeFsPluginTest {

    @TempDir
    Path dir;
    private S3PluginSupport support;
    private FakeS3 gateway;
    private HttpServer api;
    private final AtomicReference<String> head = new AtomicReference<>("c1");

    @BeforeEach
    void setUp() throws Exception {
        support = new S3PluginSupport(dir, "lakefs",
                Path.of("../flight-plugin-lakefs/target/plugin/lakefs"));
        gateway = FakeS3.start();
        gateway.put("lake", "c1/wh/tpch/nation/n.parquet", new byte[10]);
        gateway.put("lake", "c2/wh/tpch/nation/n.parquet", new byte[20]);
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
        Path httpfs = Files.writeString(dir.resolve("httpfs.duckdb_extension"), "x");
        support.properties.put("duckDbHttpfsExtension", httpfs.toString());
        support.properties.put("arrowflight.lakefs.endpoint", gateway.endpoint());
        support.properties.put("arrowflight.lakefs.apiEndpoint",
                "http://127.0.0.1:" + api.getAddress().getPort() + "/api/v1");
        support.properties.put("arrowflight.lakefs.pinTtlMillis", "0");
        support.environment.put("FLIGHT_LAKEFS_ACCESS_KEY", "AKIALAKEFS");
        support.environment.put("FLIGHT_LAKEFS_SECRET_KEY", "LAKEFSSECRET");
    }

    @AfterEach
    void tearDown() {
        gateway.close();
        api.stop(0);
    }

    @Test
    void branchIsPinnedAndDuckDbReadsTheRepositoryOverS3() throws Exception {
        try (StoreRegistry registry = support.open(StoreRegistryTest.config("lakefs://lake/main/wh"))) {
            assertEquals("lakefs", registry.provider());
            DataStore store = registry.dataDirectory();
            assertEquals("lakefs", store.kind());
            List<DataFile> files = store.tableFiles("tpch", "nation");
            assertEquals(List.of("@c1/tpch/nation/n.parquet"),
                    files.stream().map(DataFile::relativePath).toList());
            head.set("c2");
            assertEquals(20, store.tableFiles("tpch", "nation").get(0).size());
            assertEquals("s3://lake/c1/wh/tpch/nation/n.parquet",
                    store.duckDbUri("@c1/tpch/nation/n.parquet"));
            DuckDbSetupPlan plan = registry.duckDbSetup();
            assertEquals(List.of("s3://lake/"), plan.roots());
            assertTrue(plan.toString().contains("statements="), plan.toString());
            assertEquals("x *** y", registry.redactor().redact("x LAKEFSSECRET y"));
        }
    }

    @Test
    void secretAsPropertyIsAStartupError() {
        support.properties.put("arrowflight.lakefs.secretKey", "LAKEFSSECRET");
        RuntimeException e = assertThrows(RuntimeException.class,
                () -> support.open(StoreRegistryTest.config("lakefs://lake/main/wh")).close());
        String message = String.valueOf(e.getMessage());
        assertTrue(message.contains("secret secretKey is not read from properties"), message);
        assertFalse(message.contains("LAKEFSSECRET"), message);
    }
}
