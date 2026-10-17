package pro.surpin.data.arrowflight.server.storage;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.model.AppConfig;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the choice of store by the data directory's scheme. */
@Tag("unit")
class DataStoresTest {

    /** Verifies the S3 schemes, in any case, select the object store. */
    @Test
    void s3SchemesSelectObjectStore() {
        assertEquals("s3", DataStores.kindOf("s3://lake/parquet"));
        assertEquals("s3", DataStores.kindOf("S3A://lake/parquet"));
        assertEquals("s3", DataStores.kindOf("s3n://lake"));
        assertEquals("s3", DataStores.kindOf("  s3://lake/parquet"));
    }

    /** Verifies paths and other schemes stay on a Hadoop file system. */
    @Test
    void otherLocationsSelectHadoop() {
        assertEquals("hadoop", DataStores.kindOf("hdfs://ns/data/parquet"));
        assertEquals("hadoop", DataStores.kindOf("/data/parquet"));
        assertEquals("hadoop", DataStores.kindOf("file:///data/parquet"));
        assertEquals("hadoop", DataStores.kindOf("file:/data/parquet"));
        assertEquals("hadoop", DataStores.kindOf("viewfs://cluster/data"));
        assertEquals("hadoop", DataStores.kindOf("s3data/parquet"));
        assertEquals("hadoop", DataStores.kindOf("C:\\data\\parquet"));
        assertEquals("hadoop", DataStores.kindOf(null));
    }

    /** Verifies an S3 data directory without credentials fails before anything is listed. */
    @Test
    void s3WithoutCredentialsIsRefused() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> DataStores.open(config("s3://lake/parquet"), new Configuration(), "localhost"));

        assertTrue(e.getMessage().contains("without credentials"), e.getMessage());
    }

    /** Verifies a local directory opens a Hadoop store with the client read options set. */
    @Test
    void localDirectoryOpensHadoopStore(@TempDir Path dir) {
        Configuration hadoopConfig = new Configuration();

        DataStore store = DataStores.open(config(dir.toString()), hadoopConfig, "localhost");

        assertInstanceOf(HadoopDataStore.class, store);
        assertEquals("file", store.kind());
        assertFalse(store.shared());
        assertEquals(dir.toString(), store.root());
        assertEquals(131072, hadoopConfig.getInt("io.file.buffer.size", 0));
        assertTrue(hadoopConfig.getBoolean("dfs.client.read.shortcircuit", false));
        assertFalse(hadoopConfig.getBoolean("dfs.client.read.shortcircuit.skip.checksum", true));
        assertTrue(hadoopConfig.getBoolean("fs.file.impl.disable.cache", false));
    }

    /**
     * Builds a server configuration with a data directory.
     *
     * @param dataDir data directory
     * @return configuration
     */
    private static AppConfig config(String dataDir) {
        return new AppConfig(
                3, 100, 1, 131072, 0, 1, 1,
                null, false, null, null,
                null, null, null, false, 1048576, 67108864, 60000L, dataDir, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 64, null, null, "complete", true, true);
    }
}
