package pro.surpin.data.arrowflight.server.storage;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;
import pro.surpin.data.arrowflight.server.plugin.PluginSettings;
import pro.surpin.data.arrowflight.spi.storage.DataStoreProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the choice of store by the data directory's scheme. */
@Tag("unit")
class StoreRegistryTest {

    @Test
    void schemes() {
        assertEquals("s3", StoreRegistry.schemeOf("s3://lake/parquet"));
        assertEquals("s3a", StoreRegistry.schemeOf("S3A://lake/parquet"));
        assertEquals("s3", StoreRegistry.schemeOf("  s3://lake/parquet"));
        assertEquals("hdfs", StoreRegistry.schemeOf("hdfs://ns/data/parquet"));
        assertEquals("file", StoreRegistry.schemeOf("file:///data/parquet"));
        assertNull(StoreRegistry.schemeOf("file:/data/parquet"));
        assertNull(StoreRegistry.schemeOf("/data/parquet"));
        assertNull(StoreRegistry.schemeOf("s3data/parquet"));
        assertNull(StoreRegistry.schemeOf("C:\\data\\parquet"));
        assertNull(StoreRegistry.schemeOf(null));
    }

    /** Verifies s3:// without a provider names the plugin it needs. */
    @Test
    void s3WithoutThePluginIsAClearStartupError() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> open("s3a://lake/parquet", List.of()));

        assertTrue(e.getMessage().contains("needs the s3 plugin"), e.getMessage());
        assertTrue(e.getMessage().contains("arrowflight.plugins.dir"), e.getMessage());
    }

    /** Verifies only the directory catalog is accepted for now. */
    @Test
    void otherCatalogsAreRefused(@TempDir Path dir) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> StoreRegistry.open(config(dir.toString()), new Configuration(), "localhost",
                        new PluginSettings(null, null, "hms", false), emptySource(), List.of()));
        assertTrue(e.getMessage().contains("catalog=hms"), e.getMessage());
    }

    /** Verifies a local directory opens a Hadoop store with the client read options set. */
    @Test
    void localDirectoryOpensHadoopStore(@TempDir Path dir) {
        Configuration hadoopConfig = new Configuration();

        try (StoreRegistry registry = StoreRegistry.open(config(dir.toString()), hadoopConfig,
                "localhost", new PluginSettings(null, null, "directory", false), emptySource(),
                List.of())) {
            DataStore store = registry.dataDirectory();
            assertInstanceOf(HadoopDataStore.class, store);
            assertEquals("hadoop", registry.provider());
            assertEquals("file", store.kind());
            assertFalse(store.shared());
            assertEquals(dir.toString(), store.root());
            assertEquals(List.of(dir.toString()), registry.readableRoots());
            assertEquals(List.of(), registry.duckDbSetup().statements());
            assertEquals(131072, hadoopConfig.getInt("io.file.buffer.size", 0));
            assertTrue(hadoopConfig.getBoolean("dfs.client.read.shortcircuit", false));
            assertFalse(hadoopConfig.getBoolean("dfs.client.read.shortcircuit.skip.checksum", true));
            assertTrue(hadoopConfig.getBoolean("fs.file.impl.disable.cache", false));
        }
    }

    private static StoreRegistry open(String dataDir, List<DataStoreProvider> builtIns) {
        return StoreRegistry.open(config(dataDir), new Configuration(), "localhost",
                new PluginSettings(null, null, "directory", false), emptySource(), builtIns);
    }

    static ConfigSource emptySource() {
        return new ConfigSource(Map.<String, String>of()::get, new Properties(), Map.of(), null);
    }

    /**
     * Builds a server configuration with a data directory.
     *
     * @param dataDir data directory
     * @return configuration
     */
    static AppConfig config(String dataDir) {
        return new AppConfig(
                3, 100, 1, 131072, 0, 1, 1,
                null, false, null, null,
                null, null, null, false, 1048576, 67108864, 60000L, dataDir, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 64, null, null, "complete", true, true);
    }
}
