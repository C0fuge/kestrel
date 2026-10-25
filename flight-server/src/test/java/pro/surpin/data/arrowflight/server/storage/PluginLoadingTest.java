package pro.surpin.data.arrowflight.server.storage;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import pro.surpin.data.arrowflight.server.plugin.ConfigSource;
import pro.surpin.data.arrowflight.server.plugin.PluginDescriptor;
import pro.surpin.data.arrowflight.server.plugin.PluginSettings;
import pro.surpin.data.arrowflight.spi.storage.Located;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Loads the fixture plugin (flight-plugin-fixture: Jackson 2.22.2 and Guava 19.0 bundled) from a
 * plugins directory the way the server does, and checks isolation, the context class loader,
 * the version check, refused jar contents, checksums and that unneeded plugins stay unloaded.
 */
@Tag("unit")
class PluginLoadingTest {

    /** Where the fixture module leaves its jar and libraries. */
    private static final Path FIXTURE = Path.of("../flight-plugin-fixture/target");

    @TempDir
    Path dir;
    private Path plugins;
    private Path fixture;

    @BeforeEach
    void layOutPlugins() throws IOException {
        plugins = Files.createDirectories(dir.resolve("plugins"));
        fixture = Files.createDirectories(plugins.resolve("fixture"));
        Files.copy(FIXTURE.resolve("arrow-flight-plugin-fixture-1.0.0.jar"),
                fixture.resolve("arrow-flight-plugin-fixture-1.0.0.jar"));
        try (Stream<Path> libs = Files.list(FIXTURE.resolve("plugin-libs"))) {
            for (Path lib : libs.toList()) {
                Files.copy(lib, fixture.resolve(lib.getFileName().toString()));
            }
        }
    }

    @Test
    void pluginGetsItsOwnLibrariesAndItsLoaderAsContext() throws Exception {
        Map<String, String> system = Map.of("arrowflight.fixture.greeting", "hi");
        try (StoreRegistry registry = open("fixture://bucket/data/", false,
                new ConfigSource(system::get, new Properties(), Map.of(), null))) {
            DataStore store = registry.dataDirectory();
            assertInstanceOf(SpiDataStore.class, store);
            assertEquals("fixture", registry.provider());
            assertEquals(List.of("fixture"), List.copyOf(registry.plugins().loaded().keySet()));

            Map<String, String> report = store.directories("");

            assertEquals("2.22.2", report.get("jackson"));
            assertEquals("2.21.8",
                    com.fasterxml.jackson.databind.cfg.PackageVersion.VERSION.toString());
            assertEquals("{\"a\":1}", report.get("jacksonWorks"));
            assertTrue(report.get("guava").endsWith("guava-19.0.jar"), report.get("guava"));
            assertNotEquals(String.valueOf(com.google.common.base.Preconditions.class
                    .getProtectionDomain().getCodeSource().getLocation()), report.get("guava"));
            assertEquals("true", report.get("tcclIsPlugin"));
            assertEquals("false", report.get("seesServer"));
            assertEquals("false", report.get("seesHadoop"));
            assertEquals("false", report.get("seesDuckDb"));
            assertEquals("true", report.get("spiFromServer"));
            assertEquals("true", report.get("slf4jFromServer"));
            assertEquals("true", report.get("threadTcclIsPlugin"));
            assertEquals("true", report.get("threadDaemon"));
            assertEquals("hi", report.get("greeting"));
            assertTrue(Thread.currentThread().getContextClassLoader()
                    != registry.plugins().loaded().get("fixture").loader(), "context restored");

            assertEquals(List.of("fixture://bucket/data/"), registry.readableRoots());
            assertTrue(registry.duckDbSetup().extraRefusedFunctions().contains("fixture_refused"));
            Located located = registry.locate("fixture://bucket/data/s/t/f.parquet");
            assertEquals("s/t/f.parquet", located.relativePath());
            assertThrows(IOException.class, () -> registry.locate("fixture://other/x"));
        }
    }

    @Test
    void unneededPluginsAreNeverLoaded() throws Exception {
        // A directory that would be refused if it were loaded: it is not even checked.
        Path broken = Files.createDirectories(plugins.resolve("broken"));
        jar(broken.resolve("evil.jar"), Map.of("org/duckdb/DuckDBDriver.class", "x",
                PluginDescriptor.DESCRIPTOR, "name=broken\nspi=9.0\nprovides.storage=evil\n"));
        Files.createDirectories(plugins.resolve("empty"));
        Path data = Files.createDirectories(dir.resolve("data"));

        try (StoreRegistry registry = open(data.toString(), false, StoreRegistryTest.emptySource())) {
            assertEquals("hadoop", registry.provider());
            assertEquals(Map.of(), registry.plugins().loaded());
            assertEquals(List.of("broken", "fixture"),
                    registry.plugins().present().keySet().stream().sorted().toList());
            assertTrue(registry.plugins().problem("empty").isPresent());
        }
    }

    @Test
    void newerSpiVersionsAreRefused() throws Exception {
        for (String spi : List.of("2.0", "1.3", "x")) {
            rewriteDescriptor("name=fixture\nspi=" + spi + "\nprovides.storage=fixture\n");
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> open("fixture://bucket/data/", false, StoreRegistryTest.emptySource()));
            assertTrue(e.getMessage().contains("fixture"), e.getMessage());
            if (!"x".equals(spi)) {
                assertTrue(e.getMessage().contains("needs SPI " + spi + ", server has 1.2"),
                        e.getMessage());
            }
        }
    }

    @Test
    void jarsWithDuckDbOrTheSpiAreRefused() throws Exception {
        for (String entry : List.of("org/duckdb/DuckDBDriver.class",
                "org/apache/arrow/memory/RootAllocator.class", "org/slf4j/simple/SimpleLogger.class",
                "pro/surpin/data/arrowflight/spi/Plugin.class",
                "META-INF/services/org.apache.hadoop.fs.FileSystem")) {
            Path extra = fixture.resolve("extra.jar");
            jar(extra, Map.of(entry, "x"));
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> open("fixture://bucket/data/", false, StoreRegistryTest.emptySource()));
            assertTrue(e.getMessage().contains("refused"), e.getMessage());
            Files.delete(extra);
        }
    }

    @Test
    void checksumsAreCheckedWhenPresentAndCanBeRequired() throws Exception {
        IllegalStateException missing = assertThrows(IllegalStateException.class,
                () -> open("fixture://bucket/data/", true, StoreRegistryTest.emptySource()));
        assertTrue(missing.getMessage().contains("SHA256SUMS"), missing.getMessage());

        StringBuilder sums = new StringBuilder();
        try (Stream<Path> jars = Files.list(fixture)) {
            for (Path jar : jars.sorted().toList()) {
                sums.append(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                        .digest(Files.readAllBytes(jar)))).append("  ").append(jar.getFileName())
                        .append('\n');
            }
        }
        Files.writeString(fixture.resolve("SHA256SUMS"), sums);
        try (StoreRegistry registry = open("fixture://bucket/data/", true,
                StoreRegistryTest.emptySource())) {
            assertEquals("fixture", registry.provider());
        }

        String tampered = (sums.charAt(0) == '0' ? "1" : "0") + sums.substring(1);
        Files.writeString(fixture.resolve("SHA256SUMS"), tampered);
        assertThrows(IllegalStateException.class,
                () -> open("fixture://bucket/data/", false, StoreRegistryTest.emptySource()));
        Files.writeString(fixture.resolve("SHA256SUMS"), sums);
        jar(fixture.resolve("unlisted.jar"), Map.of("a.txt", "x"));
        IllegalStateException unlisted = assertThrows(IllegalStateException.class,
                () -> open("fixture://bucket/data/", false, StoreRegistryTest.emptySource()));
        assertTrue(unlisted.getMessage().contains("unlisted.jar"), unlisted.getMessage());
    }

    @Test
    void twoPluginsMayNotClaimOneScheme() throws Exception {
        Path twin = Files.createDirectories(plugins.resolve("twin"));
        jar(twin.resolve("twin.jar"), Map.of(PluginDescriptor.DESCRIPTOR,
                "name=twin\nspi=1.0\nprovides.storage=fixture\n"));
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> open("fixture://bucket/data/", false, StoreRegistryTest.emptySource()));
        assertTrue(e.getMessage().contains("both claim"), e.getMessage());
    }

    private StoreRegistry open(String dataDir, boolean requireChecksums, ConfigSource source) {
        return StoreRegistry.open(StoreRegistryTest.config(dataDir), new Configuration(),
                "localhost", new PluginSettings(plugins, null, "directory", requireChecksums),
                source, List.of());
    }

    /** Replaces the descriptor inside the fixture jar. */
    private void rewriteDescriptor(String descriptor) throws IOException {
        Path jar = fixture.resolve("arrow-flight-plugin-fixture-1.0.0.jar");
        Path copy = dir.resolve("rewritten.jar");
        try (ZipInputStream in = new ZipInputStream(Files.newInputStream(jar));
                ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(copy))) {
            for (ZipEntry entry = in.getNextEntry(); entry != null; entry = in.getNextEntry()) {
                out.putNextEntry(new ZipEntry(entry.getName()));
                if (PluginDescriptor.DESCRIPTOR.equals(entry.getName())) {
                    out.write(descriptor.getBytes(StandardCharsets.UTF_8));
                } else {
                    in.transferTo(out);
                }
                out.closeEntry();
            }
        }
        Files.move(copy, jar, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    /** Writes a jar with text entries. */
    private static void jar(Path file, Map<String, String> entries) throws IOException {
        try (OutputStream raw = Files.newOutputStream(file);
                ZipOutputStream out = new ZipOutputStream(raw)) {
            for (Map.Entry<String, String> entry : entries.entrySet()) {
                out.putNextEntry(new ZipEntry(entry.getKey()));
                out.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
                out.closeEntry();
            }
        }
    }
}
