package pro.surpin.data.arrowflight.server.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.parquet.io.InputFile;

import pro.surpin.data.arrowflight.server.duckdb.DuckDbSetupValidator;

import pro.surpin.data.arrowflight.spi.storage.DataFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the Hadoop store over a local directory. */
@Tag("unit")
class HadoopDataStoreTest {

    @TempDir
    Path root;

    private LocalFileSystem fileSystem;

    /**
     * Creates two schemas: s1 with table t1 (two Parquet files, one in a subdirectory and with
     * an upper-case extension, plus a text file) and the empty table t2; s2 without tables.
     *
     * @throws Exception when the files cannot be written
     */
    @BeforeEach
    void createTree() throws Exception {
        Files.createDirectories(root.resolve("s1/t1/sub"));
        Files.createDirectories(root.resolve("s1/t2"));
        Files.createDirectories(root.resolve("s2"));
        Files.writeString(root.resolve("s1/t1/notes.txt"), "not parquet");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT range AS x FROM range(3)) TO '"
                    + root.resolve("s1/t1/a.parquet") + "' (FORMAT parquet)");
            s.execute("COPY (SELECT range AS x FROM range(5)) TO '"
                    + root.resolve("s1/t1/sub/b.PARQUET") + "' (FORMAT parquet)");
        }
        fileSystem = new LocalFileSystem();
        fileSystem.initialize(URI.create("file:///"), new Configuration());
    }

    /**
     * Creates a store over the temporary directory.
     *
     * @param storageHost storage host loopback block hosts stand for
     * @return store
     */
    private HadoopDataStore store(String storageHost) {
        return new HadoopDataStore(fileSystem, root.toString(), storageHost);
    }

    /**
     * Opens a local data directory although Hadoop's service registrations of its file systems
     * are lost (merged Hadoop class paths: "No FileSystem for scheme "file"" at startup, Trino
     * CI on int6); an explicit fs.file.impl is kept.
     *
     * @throws Exception when Hadoop's registry cannot be reached
     */
    @Test
    @SuppressWarnings("unchecked")
    void opensLocalDirectoryWithoutHadoopServiceRegistrations() throws Exception {
        java.lang.reflect.Field loaded = org.apache.hadoop.fs.FileSystem.class
                .getDeclaredField("FILE_SYSTEMS_LOADED");
        java.lang.reflect.Field services = org.apache.hadoop.fs.FileSystem.class
                .getDeclaredField("SERVICE_FILE_SYSTEMS");
        loaded.setAccessible(true);
        services.setAccessible(true);
        org.apache.hadoop.fs.FileSystem.getFileSystemClass("file", new Configuration());
        Map<String, Class<?>> registry = (Map<String, Class<?>>) services.get(null);
        Class<?> local = registry.remove("file");
        try {
            Configuration bare = new Configuration();
            // A file system cached by an earlier test would hide the lost registration.
            bare.setBoolean("fs.file.impl.disable.cache", true);
            assertThrows(IOException.class, () -> new org.apache.hadoop.fs.Path(root.toString())
                    .getFileSystem(bare), "reproduces the lost registration");

            HadoopDataStore store = HadoopDataStoreProvider.open(root.toString(),
                    new Configuration(), 65536, "localhost");
            assertEquals("file", store.kind());

            Configuration own = new Configuration();
            own.set("fs.file.impl", org.apache.hadoop.fs.RawLocalFileSystem.class.getName());
            HadoopDataStoreProvider.registerFileSystems(own);
            assertEquals(org.apache.hadoop.fs.RawLocalFileSystem.class.getName(),
                    own.get("fs.file.impl"));
        } finally {
            registry.put("file", local);
        }
    }

    /** Verifies a local directory is neither shared nor without locality. */
    @Test
    void describesLocalStorage() {
        HadoopDataStore store = store("localhost");
        assertEquals("file", store.kind());
        assertFalse(store.shared());
        assertTrue(store.hasLocality());
        assertEquals(root.toString(), store.root());
        assertEquals(List.of(), new DuckDbSetupValidator(name -> {
            throw new AssertionError("no extension expected: " + name);
        }).plan(DuckDbSetupValidator.Contribution.ofStore("hadoop", store), List.of(), List.of())
                .statements());
    }

    /** Verifies schemas and tables are the directories of the first two levels. */
    @Test
    void listsDirectories() throws IOException {
        HadoopDataStore store = store("localhost");

        Map<String, String> schemas = store.directories("");
        Map<String, String> tables = store.directories("s1");

        assertEquals(Set.of("s1", "s2"), schemas.keySet());
        assertEquals(Set.of("t1", "t2"), tables.keySet());
        assertEquals("file:" + root.resolve("s1"), schemas.get("s1"));
        assertTrue(store.directories("s2").isEmpty());
        assertTrue(store.exists(""));
        assertTrue(store.exists("s1/t1"));
        assertFalse(store.exists("s1/missing"));
    }

    /** Verifies a table lists its Parquet files at any depth, in path order, with sizes. */
    @Test
    void listsTableFilesWithRelativePathsAndSizes() throws IOException {
        HadoopDataStore store = store("dn1");

        List<DataFile> files = store.tableFiles("s1", "t1");

        assertEquals(List.of("s1/t1/a.parquet", "s1/t1/sub/b.PARQUET"),
                files.stream().map(DataFile::relativePath).toList());
        assertEquals(Files.size(root.resolve("s1/t1/a.parquet")), files.get(0).size());
        assertEquals(Files.size(root.resolve("s1/t1/sub/b.PARQUET")), files.get(1).size());
        assertEquals(Files.getLastModifiedTime(root.resolve("s1/t1/a.parquet")).toMillis(),
                files.get(0).modificationTime());
        assertEquals(Set.of("dn1"), files.get(0).hosts());
        assertTrue(store.tableFiles("s1", "t2").isEmpty());
        assertTrue(store.tableFiles("s1", "missing").isEmpty());
        assertThrows(FileNotFoundException.class, () -> store.parquetFiles("s1/missing"));
    }

    /** Verifies one file is looked up by its relative path. */
    @Test
    void looksUpOneFile() throws IOException {
        DataFile file = store("localhost").file("s1/t1/a.parquet");

        assertEquals("s1/t1/a.parquet", file.relativePath());
        assertEquals(Files.size(root.resolve("s1/t1/a.parquet")), file.size());
        assertTrue(file.hosts().isEmpty());
        assertThrows(FileNotFoundException.class, () -> store("localhost").file("s1/t1/none.parquet"));
    }

    /** Verifies DuckDB gets local paths without the file scheme, for listed and named files. */
    @Test
    void namesFilesAndRootsForDuckDb() throws IOException {
        HadoopDataStore store = store("localhost");
        String expected = root.resolve("s1/t1/a.parquet").toString();

        assertEquals(expected, store.duckDbUri("s1/t1/a.parquet"));
        assertEquals(expected, store.duckDbUri(store.tableFiles("s1", "t1").get(0)));
        assertEquals(List.of(root.toString()), store.duckDbRoots());
    }

    /** Verifies footers are read through the store, by relative path and from a listing. */
    @Test
    void readsParquetFooters() throws IOException {
        HadoopDataStore store = store("localhost");
        List<DataFile> files = store.tableFiles("s1", "t1");

        assertEquals(3L, rowCount(store.inputFile("s1/t1/a.parquet")));
        assertEquals(5L, rowCount(store.inputFile(files.get(1))));
        assertEquals(files.get(0).size(), store.inputFile("s1/t1/a.parquet").getLength());
    }

    /** Verifies the inventory holds the files whose blocks are on this server's storage host. */
    @Test
    void publishesLocalInventoryForStorageHost() throws IOException {
        HadoopDataStore store = store("dn1");

        Map<String, Long> local = store.localInventory("dn1");

        assertEquals(Set.of("s1/t1/a.parquet", "s1/t1/sub/b.PARQUET"), local.keySet());
        assertEquals(Files.size(root.resolve("s1/t1/a.parquet")), local.get("s1/t1/a.parquet"));
        assertTrue(store.localInventory("dn2").isEmpty());
    }

    /** Verifies a missing data directory gives an empty inventory instead of an error. */
    @Test
    void missingDataDirectoryHasEmptyInventory() throws IOException {
        HadoopDataStore store = new HadoopDataStore(fileSystem,
                root.resolve("missing").toString(), "localhost");

        assertTrue(store.localInventory("localhost").isEmpty());
        assertFalse(store.exists(""));
    }

    /** Verifies block hosts are compared after resolving loopback and DataNode aliases. */
    @Test
    void hasLocalBlockMatchesResolvedFlightAndDataNodeAliases() {
        assertTrue(HadoopDataStore.hasLocalBlock(Set.of("127.0.0.1"), "localhost"));
        assertFalse(HadoopDataStore.hasLocalBlock(Set.of("127.0.0.2"), "localhost"));
    }

    /** Verifies the file scheme is stripped in both of its spellings. */
    @Test
    void duckDbPathStripsFileScheme() {
        assertEquals("/data/file.parquet", HadoopDataStore.duckDbPath("file:/data/file.parquet"));
        assertEquals("/data/file.parquet", HadoopDataStore.duckDbPath("file:///data/file.parquet"));
    }

    /** Verifies a local file whose name needs escaping reaches DuckDB as its real path. */
    @Test
    void duckDbUriOfEscapedLocalNameIsTheFilePath() throws Exception {
        Files.createDirectories(root.resolve("s3/t 1"));
        Files.writeString(root.resolve("s3/t 1/part 1%.parquet"), "x");
        String path = store("localhost").duckDbUri("s3/t 1/part 1%.parquet");
        assertTrue(Files.exists(java.nio.file.Path.of(path)), path);
        assertEquals("/data/a b/c%d.parquet", HadoopDataStore.duckDbPath("file:/data/a%20b/c%25d.parquet"));
        HadoopDataStore store = store("localhost");
        assertTrue(store.parquetFiles("s3").stream()
                .anyMatch(f -> f.relativePath().equals("s3/t 1/part 1%.parquet")), "listing");
        assertTrue(store.localInventory("localhost").containsKey("s3/t 1/part 1%.parquet"), "inventory");
    }

    /** Verifies a data directory whose name needs escaping is the root DuckDB may read. */
    @Test
    void duckDbRootOfEscapedLocalDirectoryIsItsPath() throws Exception {
        java.nio.file.Path spaced = Files.createDirectories(root.resolve("my lake"));
        HadoopDataStore store = new HadoopDataStore(fileSystem, spaced.toString(), "localhost");
        assertEquals(List.of(spaced.toString()), store.duckDbRoots());
    }

    /** Verifies URIs of shared file systems reach DuckDB unchanged. */
    @Test
    void duckDbPathPreservesNonFileScheme() {
        assertEquals("hdfs://namenode/data/file.parquet",
                HadoopDataStore.duckDbPath("hdfs://namenode/data/file.parquet"));
    }

    /**
     * Sums the row counts in a Parquet footer.
     *
     * @param file Parquet input file
     * @return row count
     * @throws IOException when the footer cannot be read
     */
    private static long rowCount(InputFile file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            return reader.getFooter().getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();
        }
    }
}
