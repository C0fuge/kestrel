package pro.surpin.data.arrowflight.server.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;
import pro.surpin.data.arrowflight.server.plugin.PluginSettings;
import pro.surpin.data.arrowflight.server.storage.StoreRegistry;

/**
 * Tests how the store registry opens catalogs from plugins: the DuckDB setup they contribute
 * (roots, avro + iceberg), the stores opened for their roots, and the startup errors for a
 * missing plugin, a missing setting, a missing extension and a secret given as a property.
 */
class CatalogRegistryTest {

    @TempDir
    Path dir;
    Path plugins;
    Path data;
    Path warehouse;
    Map<String, String> sys;

    @BeforeEach
    void setUp() throws Exception {
        Assumptions.assumeTrue(IcebergTestTables.available(), "flight-plugin-iceberg is not built");
        plugins = dir.resolve("plugins");
        IcebergTestTables.installPlugin(plugins);
        data = Files.createDirectories(dir.resolve("data"));
        warehouse = Files.createDirectories(dir.resolve("wh"));
        sys = new HashMap<>();
        sys.put("arrowflight.iceberg-hadoop.warehouse", warehouse.toString());
        // Any file will do: the extensions are only named here, never loaded.
        Path ext = Files.createDirectories(dir.resolve("ext/v1.4.5/linux_amd64"));
        Files.writeString(ext.resolve("iceberg.duckdb_extension"), "x");
        Files.writeString(ext.resolve("avro.duckdb_extension"), "x");
        sys.put("duckDbIcebergExtension", ext.resolve("iceberg.duckdb_extension").toString());
        sys.put("duckDbAvroExtension", ext.resolve("avro.duckdb_extension").toString());
    }

    private StoreRegistry open(Path pluginsDir, String catalogs, Map<String, String> env) {
        System.setProperty("dataDir", data.toString());
        AppConfig config;
        try {
            config = ConfigAdapter.getConfig();
        } finally {
            System.clearProperty("dataDir");
        }
        ConfigSource source = new ConfigSource(sys::get, new Properties(), env, null);
        return StoreRegistry.open(config, new Configuration(), "localhost",
                new PluginSettings(pluginsDir, null, catalogs, false), source, List.of());
    }

    @Test
    void catalogRootsAndExtensionsGoIntoTheDuckDbSetup() throws Exception {
        try (StoreRegistry registry = open(plugins, "iceberg-hadoop", Map.of())) {
            assertEquals(List.of(data.toString(), warehouse.toString()), registry.readableRoots());
            List<String> loads = registry.duckDbSetup().statements().stream()
                    .filter(s -> s.startsWith("LOAD")).toList();
            assertEquals(2, loads.size(), loads.toString());
            assertTrue(loads.get(0).contains("avro"), loads.toString());
            assertTrue(loads.get(1).contains("iceberg"), loads.toString());
            assertEquals(1, registry.catalogTables().catalogs().size());
            assertEquals("iceberg-hadoop", registry.catalogTables().catalogs().get(0).provider());
            assertFalse(registry.catalogTables().catalogs().get(0).shared());
            assertTrue(registry.locate(warehouse + "/ns/t").relativePath().equals("ns/t"));
        }
        try (StoreRegistry registry = open(plugins, "directory", Map.of())) {
            assertTrue(registry.catalogTables().isEmpty());
            assertEquals(List.of(data.toString()), registry.readableRoots());
            assertTrue(registry.duckDbSetup().statements().isEmpty());
        }
    }

    @Test
    void warehouseBelowTheDataDirectoryUsesItsStore() throws Exception {
        Path inside = Files.createDirectories(data.resolve("iceberg-wh"));
        sys.put("arrowflight.iceberg-hadoop.warehouse", "file:" + inside);
        try (StoreRegistry registry = open(plugins, "iceberg-hadoop", Map.of())) {
            assertEquals(List.of(data.toString(), inside.toString()), registry.readableRoots());
        }
    }

    @Test
    void startupErrorsNameTheProblem() {
        IllegalArgumentException noPlugin = assertThrows(IllegalArgumentException.class,
                () -> open(dir.resolve("none"), "iceberg-hadoop", Map.of()));
        assertTrue(noPlugin.getMessage().contains("catalog=iceberg-hadoop needs a plugin"),
                noPlugin.getMessage());

        sys.remove("arrowflight.iceberg-hadoop.warehouse");
        IllegalArgumentException noWarehouse = assertThrows(IllegalArgumentException.class,
                () -> open(plugins, "iceberg-hadoop", Map.of()));
        assertTrue(noWarehouse.getMessage().contains("iceberg-hadoop.warehouse"), noWarehouse.getMessage());
        sys.put("arrowflight.iceberg-hadoop.warehouse", warehouse.toString());

        sys.remove("duckDbIcebergExtension");
        IllegalArgumentException noExtension = assertThrows(IllegalArgumentException.class,
                () -> open(plugins, "iceberg-hadoop", Map.of()));
        assertTrue(noExtension.getMessage().contains("iceberg"), noExtension.getMessage());

        sys.put("arrowflight.iceberg-rest.uri", "http://127.0.0.1:9");
        sys.put("arrowflight.iceberg-rest.roots", warehouse.toString());
        sys.put("arrowflight.iceberg-rest.credential", "id:topsecret");
        IllegalArgumentException secret = assertThrows(IllegalArgumentException.class,
                () -> open(plugins, "iceberg-rest", Map.of()));
        assertTrue(secret.getMessage().contains("credential"), secret.getMessage());
        assertFalse(secret.getMessage().contains("topsecret"), secret.getMessage());

        sys.remove("arrowflight.iceberg-rest.credential");
        RuntimeException unreachable = assertThrows(RuntimeException.class,
                () -> open(plugins, "iceberg-rest", Map.of("FLIGHT_ICEBERG_REST_CREDENTIAL", "id:topsecret")));
        assertTrue(unreachable.getMessage().contains("iceberg-rest cannot be opened"), unreachable.getMessage());
        assertFalse(unreachable.getMessage().contains("topsecret"), unreachable.getMessage());
    }
}
