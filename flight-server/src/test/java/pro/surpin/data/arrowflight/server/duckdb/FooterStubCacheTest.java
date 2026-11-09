package pro.surpin.data.arrowflight.server.duckdb;

import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests footer stubs kept across describes (item 39 (c)). */
@Tag("unit")
class FooterStubCacheTest {

    /**
     * Verifies one stub per file version is read from storage, reused, and removed only once
     * every describe using a replaced version is done.
     *
     * @param dir temporary directory
     * @throws Exception when the test file cannot be written
     */
    @Test
    void reusesAStubUntilTheFileChanges(@TempDir Path dir) throws Exception {
        Path file = parquet(dir);
        AtomicInteger reads = new AtomicInteger();
        FooterStubCache.Source source = () -> {
            reads.incrementAndGet();
            return new LocalInputFile(file);
        };
        Path root = dir.resolve("stubs");
        String location = "hdfs://ns/data/s/t/f.parquet";
        try (FooterStubCache cache = new FooterStubCache(root, 8)) {
            FooterStubCache.Lease first = cache.lease(location, 10, 1, source);
            Path stub = first.file();
            try (FooterStubCache.Lease again = cache.lease(location, 10, 1, source)) {
                assertEquals(stub, again.file());
            }
            assertEquals(1, reads.get());
            try (FooterStubCache.Lease changed = cache.lease(location, 10, 2, source)) {
                assertNotEquals(stub, changed.file());
                assertTrue(Files.exists(stub), "a replaced stub stays while a describe uses it");
                assertTrue(Files.exists(changed.file()));
            }
            first.close();
            first.close();
            assertFalse(Files.exists(stub), "removed once its last lease closes");
            assertEquals(2, reads.get());
            assertEquals(1, cache.size());
        }
        assertEquals(0, files(root), "close removes unused stubs");
    }

    /**
     * Verifies the least recently used stub goes when the cache is full.
     *
     * @param dir temporary directory
     * @throws Exception when the test file cannot be written
     */
    @Test
    void evictsTheLeastRecentlyUsedStub(@TempDir Path dir) throws Exception {
        Path file = parquet(dir);
        Path root = dir.resolve("stubs");
        try (FooterStubCache cache = new FooterStubCache(root, 2)) {
            for (String name : new String[] {"a", "b", "a", "c"}) {
                cache.lease("s3://b/t/" + name + ".parquet", 10, 1, () -> new LocalInputFile(file)).close();
            }
            assertEquals(2, cache.size());
            assertEquals(2, files(root));
        }
    }

    /**
     * Verifies a file that is not Parquet fails and leaves no stub behind.
     *
     * @param dir temporary directory
     * @throws Exception when the test file cannot be written
     */
    @Test
    void failsForFilesThatAreNotParquet(@TempDir Path dir) throws Exception {
        Path text = Files.writeString(dir.resolve("t.parquet"), "not a parquet file, just text");
        Path root = dir.resolve("stubs");
        try (FooterStubCache cache = new FooterStubCache(root, 2)) {
            assertThrows(IOException.class,
                    () -> cache.lease("hdfs://ns/t.parquet", 1, 1, () -> new LocalInputFile(text)));
            assertEquals(0, cache.size());
        }
        assertThrows(IllegalArgumentException.class, () -> new FooterStubCache(root, 0));
    }

    /** Verifies which locations are remote. */
    @Test
    void remoteLocations() {
        assertTrue(FooterStubCache.remote("hdfs://ns/data/f.parquet"));
        assertTrue(FooterStubCache.remote("s3://bucket/f.parquet"));
        assertFalse(FooterStubCache.remote("file:///data/f.parquet"));
        assertFalse(FooterStubCache.remote("/data/f.parquet"));
        assertFalse(FooterStubCache.remote("C:/data/f.parquet"));
    }

    private static Path parquet(Path dir) throws Exception {
        Path file = dir.resolve("f.parquet");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT range AS k FROM range(10)) TO '" + file + "' (FORMAT parquet)");
        }
        return file;
    }

    private static long files(Path root) throws IOException {
        if (!Files.exists(root)) {
            return 0;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            return paths.filter(Files::isRegularFile).count();
        }
    }
}
