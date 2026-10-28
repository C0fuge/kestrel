package pro.surpin.data.arrowflight.plugin.iceberg;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.ServiceLoader;

import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.spi.Plugin;
import pro.surpin.data.arrowflight.spi.catalog.CatalogProvider;

/**
 * Tests the plugin's parts: location rules, DuckDB type names, the read-only FileIO over the
 * stores, and the plugin entry point.
 */
class PluginPartsTest {

    @TempDir
    Path dir;

    @Test
    void locationsAreNormalised() {
        assertEquals("/tmp/a/b", Locations.normalize("file:/tmp/a/b"));
        assertEquals("/tmp/a/b", Locations.normalize("file:///tmp/a/b/"));
        assertEquals("/tmp/a", Locations.normalize("/tmp/a//"));
        assertEquals("s3://lake/wh", Locations.normalize("s3://lake/wh/"));
        assertEquals("hdfs://nn:8020/wh", Locations.normalize("hdfs://nn:8020/wh"));
        // Next steps 12: S3A names the same objects as s3://.
        assertEquals("s3://lake/wh/t", Locations.normalize("s3a://lake/wh/t/"));
        assertEquals("s3://lake/wh", Locations.normalize("S3N://lake/wh"));
        assertTrue(Locations.underAny(Locations.normalize("s3a://lake/wh/t/v1.metadata.json"),
                Locations.roots("s3://lake/wh")));
        assertTrue(Locations.under("/tmp/a/b", "/tmp/a"));
        assertTrue(Locations.under("/tmp/a", "/tmp/a"));
        assertFalse(Locations.under("/tmp/ab", "/tmp/a"));
        assertEquals(List.of("/x", "s3://b/y"), Locations.roots(" file:/x/ , s3://b/y ,"));
    }

    @Test
    void typesWithoutADuckDbParquetTypeAreNull() {
        assertEquals("DECIMAL(9,3)", DuckDbTypes.of(Types.DecimalType.of(9, 3)));
        assertEquals("MAP(VARCHAR, BIGINT[])", DuckDbTypes.of(Types.MapType.ofOptional(1, 2,
                Types.StringType.get(), Types.ListType.ofOptional(3, Types.LongType.get()))));
        assertEquals("STRUCT(\"we\"\"ird\" INTEGER)", DuckDbTypes.of(Types.StructType.of(
                Types.NestedField.optional(4, "we\"ird", Types.IntegerType.get()))));
        assertNull(DuckDbTypes.of(Types.VariantType.get()));
        assertNull(DuckDbTypes.of(Types.ListType.ofOptional(5, Types.VariantType.get())));
    }

    @Test
    void fileIoReadsThroughTheStoresOnly() throws IOException {
        Path file = dir.resolve("f.bin");
        byte[] content = new byte[600_000];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) (i * 31);
        }
        Files.write(file, content);
        TestSupport.LocalStores stores = new TestSupport.LocalStores();
        stores.locate(dir.toString());
        stores.freeze();
        StoresFileIO io = new StoresFileIO(stores);
        InputFile input = io.newInputFile("file:" + file);
        assertTrue(input.exists());
        assertEquals(content.length, input.getLength());
        try (SeekableInputStream in = input.newStream()) {
            byte[] read = in.readAllBytes();
            assertArrayEquals(content, read);
            in.seek(300_000);
            assertEquals(content[300_000] & 0xff, in.read());
            assertEquals(300_001, in.getPos());
            in.seek(content.length);
            assertEquals(-1, in.read());
        }
        assertFalse(io.newInputFile(dir.resolve("missing").toString()).exists());
        assertThrows(NotFoundException.class,
                () -> io.newInputFile(dir.resolve("missing").toString()).newStream());
        assertFalse(io.newInputFile("/elsewhere/x").exists());
        assertThrows(UnsupportedOperationException.class, () -> io.newOutputFile(file.toString()));
        assertThrows(UnsupportedOperationException.class, () -> io.deleteFile(file.toString()));
    }

    @Test
    void pluginNamesBothCatalogKinds() {
        // The s3 plugin is on the test class path too (RestCatalogConformanceTest).
        List<Plugin> plugins = ServiceLoader.load(Plugin.class).stream()
                .map(ServiceLoader.Provider::get).filter(IcebergPlugin.class::isInstance).toList();
        assertEquals(1, plugins.size());
        List<String> names = plugins.get(0).catalogProviders().stream()
                .map(CatalogProvider::name).toList();
        assertEquals(List.of("iceberg-hadoop", "iceberg-rest"), names);
        assertTrue(plugins.get(0).dataStoreProviders().isEmpty());
        assertTrue(new IcebergCatalogProvider(IcebergSettings.Kind.REST).configKeys().stream()
                .anyMatch(k -> k.name().equals("credential") && k.secret()));
    }
}
