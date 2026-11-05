package pro.surpin.data.arrowflight.server.storage;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import pro.surpin.data.arrowflight.spi.storage.DataFile;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the node-local copy cache of HDFS files (item 30).
 */
@Tag("unit")
class LocalCopyCacheTest {

    @TempDir
    Path dir;

    /** Opens of each file location, to count copies. */
    private final Map<String, AtomicInteger> opens = new ConcurrentHashMap<>();

    /**
     * Describes a file of a given length whose bytes are its length repeated.
     *
     * @param path relative path
     * @param size length
     * @param modified modification time
     * @return data file
     */
    private static DataFile file(String path, int size, long modified) {
        return new DataFile(path, "hdfs://ns/data/" + path, size, modified, Set.of());
    }

    /**
     * Returns the bytes the test copier serves for a file.
     *
     * @param file data file
     * @return content
     */
    private static byte[] content(DataFile file) {
        byte[] bytes = new byte[(int) file.size()];
        java.util.Arrays.fill(bytes, (byte) (file.size() + file.modificationTime()));
        return bytes;
    }

    /**
     * Returns a copier that serves {@link #content(DataFile)} and counts opens.
     *
     * @return copier
     */
    private LocalCopyCache.Copier copier() {
        return f -> {
            opens.computeIfAbsent(f.location(), k -> new AtomicInteger()).incrementAndGet();
            return new ByteArrayInputStream(content(f));
        };
    }

    @Test
    void keyChangesWithLengthAndModificationTime() {
        String key = LocalCopyCache.key(file("t/a.parquet", 10, 1));
        assertEquals(key, LocalCopyCache.key(file("t/a.parquet", 10, 1)));
        assertNotEquals(key, LocalCopyCache.key(file("t/a.parquet", 11, 1)));
        assertNotEquals(key, LocalCopyCache.key(file("t/a.parquet", 10, 2)));
        assertNotEquals(key, LocalCopyCache.key(file("t/b.parquet", 10, 1)));
    }

    @Test
    void copyKeepsTheRelativePathForHivePartitions() {
        String path = LocalCopyCache.copyPath("k", "s/t/k=REG AIR/part-0.parquet");
        assertTrue(path.endsWith("/s/t/k=REG AIR/part-0.parquet"), path);
        assertFalse(LocalCopyCache.copyPath("k", "../../etc/passwd").contains(".."));
    }

    @Test
    void copiesOnceAndReusesAcrossQueries() throws IOException {
        DataFile a = file("s/t/a.parquet", 100, 1);
        try (LocalCopyCache cache = new LocalCopyCache(dir.resolve("c"), 1000, 2, copier())) {
            Path first;
            try (LocalCopyCache.Lease lease = cache.acquire(List.of(a))) {
                first = lease.localPath(a.relativePath());
                assertArrayEquals(content(a), Files.readAllBytes(first));
                assertEquals(1, lease.copied());
                assertEquals(100, lease.copiedBytes());
                // Measured from the call, also without DEBUG timing (CI logged the uptime).
                assertTrue(lease.millis() >= 0 && lease.millis() < 60_000, "millis " + lease.millis());
            }
            try (LocalCopyCache.Lease lease = cache.acquire(List.of(a))) {
                assertEquals(first, lease.localPath(a.relativePath()));
                assertEquals(0, lease.copied());
                assertEquals(1, lease.reused());
                assertNull(lease.localPath("s/t/other.parquet"));
            }
            assertEquals(1, opens.get(a.location()).get());
        }
    }

    @Test
    void rewrittenFileIsCopiedAgain() throws IOException {
        DataFile old = file("s/t/a.parquet", 100, 1);
        DataFile rewritten = file("s/t/a.parquet", 100, 2);
        try (LocalCopyCache cache = new LocalCopyCache(dir.resolve("c"), 1000, 2, copier())) {
            cache.acquire(List.of(old)).close();
            try (LocalCopyCache.Lease lease = cache.acquire(List.of(rewritten))) {
                assertEquals(1, lease.copied());
                assertArrayEquals(content(rewritten),
                        Files.readAllBytes(lease.localPath(rewritten.relativePath())));
            }
            assertEquals(2, opens.get(old.location()).get());
        }
    }

    @Test
    void evictsLeastRecentlyUsedCopiesNotInUse() throws IOException {
        DataFile a = file("s/t/a.parquet", 400, 1);
        DataFile b = file("s/t/b.parquet", 400, 1);
        DataFile c = file("s/t/c.parquet", 400, 1);
        try (LocalCopyCache cache = new LocalCopyCache(dir.resolve("c"), 1000, 2, copier())) {
            Path pathA;
            try (LocalCopyCache.Lease lease = cache.acquire(List.of(a))) {
                pathA = lease.localPath(a.relativePath());
            }
            cache.acquire(List.of(b)).close();
            // a is the least recently used: c takes its place.
            cache.acquire(List.of(c)).close();
            assertFalse(Files.exists(pathA));
            assertEquals(2, cache.size());
            assertEquals(800, cache.cachedBytes());
            cache.acquire(List.of(b)).close();
            assertEquals(1, opens.get(b.location()).get());
        }
    }

    @Test
    void copiesInUseAreKeptOverTheLimitAndDeletedAfterUse() throws IOException {
        DataFile a = file("s/t/a.parquet", 600, 1);
        DataFile b = file("s/t/b.parquet", 600, 1);
        try (LocalCopyCache cache = new LocalCopyCache(dir.resolve("c"), 1000, 2, copier())) {
            Path pathA;
            Path pathB;
            try (LocalCopyCache.Lease lease = cache.acquire(List.of(a, b))) {
                pathA = lease.localPath(a.relativePath());
                pathB = lease.localPath(b.relativePath());
                // A query needs both: both stay while it runs, over the limit.
                assertArrayEquals(content(a), Files.readAllBytes(pathA));
                assertArrayEquals(content(b), Files.readAllBytes(pathB));
                assertEquals(1200, cache.cachedBytes());
            }
            assertTrue(cache.cachedBytes() <= 1000, "after use the cache is back under its limit");
            assertFalse(Files.exists(pathA) && Files.exists(pathB));
        }
    }

    @Test
    void fileLargerThanTheLimitIsCopiedAndDeletedAfterUse() throws IOException {
        DataFile big = file("s/t/big.parquet", 2000, 1);
        try (LocalCopyCache cache = new LocalCopyCache(dir.resolve("c"), 1000, 2, copier())) {
            Path path;
            try (LocalCopyCache.Lease lease = cache.acquire(List.of(big))) {
                path = lease.localPath(big.relativePath());
                assertEquals(2000, Files.size(path));
            }
            assertFalse(Files.exists(path));
            assertEquals(0, cache.cachedBytes());
            assertEquals(0, cache.size());
        }
    }

    @Test
    void concurrentQueriesCopyOneFileOnce() throws Exception {
        DataFile a = file("s/t/a.parquet", 5000, 1);
        CountDownLatch release = new CountDownLatch(1);
        LocalCopyCache.Copier slow = f -> {
            opens.computeIfAbsent(f.location(), k -> new AtomicInteger()).incrementAndGet();
            try {
                assertTrue(release.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new ByteArrayInputStream(content(f));
        };
        ExecutorService queries = Executors.newFixedThreadPool(4);
        try (LocalCopyCache cache = new LocalCopyCache(dir.resolve("c"), 100_000, 4, slow)) {
            List<Future<Path>> results = new java.util.ArrayList<>();
            for (int i = 0; i < 4; i++) {
                results.add(queries.submit(() -> {
                    try (LocalCopyCache.Lease lease = cache.acquire(List.of(a))) {
                        return lease.localPath(a.relativePath());
                    }
                }));
            }
            Thread.sleep(200);
            release.countDown();
            Path first = results.get(0).get(10, TimeUnit.SECONDS);
            for (Future<Path> result : results) {
                assertEquals(first, result.get(10, TimeUnit.SECONDS));
            }
            assertEquals(1, opens.get(a.location()).get());
            assertArrayEquals(content(a), Files.readAllBytes(first));
        } finally {
            queries.shutdownNow();
        }
    }

    @Test
    void failedCopyLeavesNothingAndIsRetried() throws IOException {
        DataFile a = file("s/t/a.parquet", 100, 1);
        AtomicInteger calls = new AtomicInteger();
        LocalCopyCache.Copier flaky = f -> {
            if (calls.incrementAndGet() == 1) {
                throw new IOException("Connection reset by peer");
            }
            return new ByteArrayInputStream(content(f));
        };
        try (LocalCopyCache cache = new LocalCopyCache(dir.resolve("c"), 1000, 2, flaky)) {
            IOException e = assertThrows(IOException.class, () -> cache.acquire(List.of(a)));
            assertTrue(e.getMessage().contains("s/t/a.parquet"), e.getMessage());
            assertEquals(0, cache.cachedBytes());
            try (LocalCopyCache.Lease lease = cache.acquire(List.of(a))) {
                assertArrayEquals(content(a), Files.readAllBytes(lease.localPath(a.relativePath())));
            }
        }
    }

    @Test
    void truncatedCopyIsRefused() throws IOException {
        DataFile a = file("s/t/a.parquet", 100, 1);
        LocalCopyCache.Copier shortRead = f -> new ByteArrayInputStream(new byte[10]);
        try (LocalCopyCache cache = new LocalCopyCache(dir.resolve("c"), 1000, 2, shortRead)) {
            IOException e = assertThrows(IOException.class, () -> cache.acquire(List.of(a)));
            assertTrue(e.getMessage().contains("changed while it was copied"), e.getMessage());
            try (java.util.stream.Stream<Path> left = Files.walk(dir.resolve("c"))) {
                assertEquals(1, left.count(), "only the cache directory is left");
            }
        }
    }

    @Test
    void closeDeletesEveryCopy() throws IOException {
        DataFile a = file("s/t/a.parquet", 100, 1);
        LocalCopyCache cache = new LocalCopyCache(dir.resolve("c"), 1000, 2, copier());
        Path path;
        try (LocalCopyCache.Lease lease = cache.acquire(List.of(a))) {
            path = lease.localPath(a.relativePath());
        }
        cache.close();
        assertFalse(Files.exists(path));
    }

    /**
     * Returns a disk of a given capacity that holds only the files below the test directory.
     *
     * @param capacity bytes of the disk
     * @return free space of that disk
     */
    private LocalCopyCache.FreeSpace disk(long capacity) {
        return d -> {
            try (java.util.stream.Stream<Path> files = Files.walk(dir)) {
                long used = files.filter(Files::isRegularFile).mapToLong(f -> f.toFile().length()).sum();
                return capacity - used;
            }
        };
    }

    @Test
    void copiesTheDiskHasNoRoomForAreRefusedBeforeAnythingIsCopied() throws IOException {
        DataFile a = file("s/t/a.parquet", 500, 1);
        DataFile b = file("s/t/b.parquet", 400, 1);
        try (LocalCopyCache cache = new LocalCopyCache(dir.resolve("c"), 10_000, 2, copier(),
                200, disk(1000))) {
            // 900 bytes would leave 100 free, 200 must stay free: neither copy is started.
            IOException e = assertThrows(IOException.class, () -> cache.acquire(List.of(a, b)));
            // A resource limit the server answers with RESOURCE_EXHAUSTED, not INTERNAL (item 31).
            assertTrue(e instanceof LocalCopyCache.NoRoomException, e.toString());
            assertSame(e, LocalCopyCache.NoRoomException.in(new RuntimeException(new IOException(e))));
            assertNull(LocalCopyCache.NoRoomException.in(new IOException("other")));
            assertTrue(e.getMessage().contains("s/t/a.parquet"), e.getMessage());
            assertTrue(e.getMessage().contains("900 bytes"), e.getMessage());
            assertTrue(e.getMessage().contains("hdfsLocalCopyMinFreeBytes"), e.getMessage());
            assertTrue(opens.isEmpty(), "nothing was copied");
            assertEquals(0, cache.size());
            assertEquals(0, cache.cachedBytes());
            try (LocalCopyCache.Lease lease = cache.acquire(List.of(a))) {
                assertEquals(500, Files.size(lease.localPath(a.relativePath())));
            }
        }
    }

    @Test
    void unusedCopiesBelowTheLimitAreEvictedForDiskRoom() throws IOException {
        DataFile a = file("s/t/a.parquet", 500, 1);
        DataFile b = file("s/t/b.parquet", 500, 1);
        try (LocalCopyCache cache = new LocalCopyCache(dir.resolve("c"), 10_000, 2, copier(),
                100, disk(1000))) {
            Path pathA;
            try (LocalCopyCache.Lease lease = cache.acquire(List.of(a))) {
                pathA = lease.localPath(a.relativePath());
            }
            // The cache limit has room for both, the disk only for one.
            try (LocalCopyCache.Lease lease = cache.acquire(List.of(b))) {
                assertEquals(500, Files.size(lease.localPath(b.relativePath())));
            }
            assertFalse(Files.exists(pathA));
            assertEquals(1, cache.size());
        }
    }

    @Test
    void copiesInUseAreNotEvictedForDiskRoom() throws IOException {
        DataFile a = file("s/t/a.parquet", 500, 1);
        DataFile b = file("s/t/b.parquet", 500, 1);
        try (LocalCopyCache cache = new LocalCopyCache(dir.resolve("c"), 10_000, 2, copier(),
                100, disk(1000))) {
            try (LocalCopyCache.Lease held = cache.acquire(List.of(a))) {
                assertThrows(IOException.class, () -> cache.acquire(List.of(b)));
                assertArrayEquals(content(a), Files.readAllBytes(held.localPath(a.relativePath())));
                // A file the running query already has costs no room.
                try (LocalCopyCache.Lease again = cache.acquire(List.of(a))) {
                    assertEquals(1, again.reused());
                }
            }
            cache.acquire(List.of(b)).close();
            assertEquals(1, opens.get(b.location()).get());
        }
    }

    @Test
    void roomForNewCopiesNeverEvictsACopyTheSameQueryReuses() throws IOException {
        // Item 33 (C): the reused copy was evicted for the new one's room and then copied again
        // without being counted, so the disk filled up (ENOSPC) instead of a clean refusal.
        DataFile a = file("s/t/a.parquet", 400, 1);
        DataFile b = file("s/t/b.parquet", 500, 1);
        try (LocalCopyCache cache = new LocalCopyCache(dir.resolve("c"), 10_000, 2, copier(),
                150, disk(1000))) {
            Path pathA;
            try (LocalCopyCache.Lease lease = cache.acquire(List.of(a))) {
                pathA = lease.localPath(a.relativePath());
            }
            assertEquals(java.util.Set.of(a.relativePath()), cache.present(List.of(a, b)));
            // a + b = 900 bytes leave 100 free, 150 must stay free: refused, a kept and unpinned.
            LocalCopyCache.NoRoomException refused = assertThrows(LocalCopyCache.NoRoomException.class,
                    () -> cache.acquire(List.of(a, b)));
            // Item 33: the copy the refused query reuses is its own, not one of another query.
            assertTrue(refused.getMessage().contains("the 1 copies this query reuses take 400 bytes,"
                    + " copies in use by other running queries take 0 bytes"), refused.getMessage());
            assertEquals(0, cache.inUseBytes());
            assertEquals(1, opens.get(a.location()).get());
            assertNull(opens.get(b.location()));
            assertTrue(Files.exists(pathA));
            // Unpinned after the refusal: b alone evicts it.
            try (LocalCopyCache.Lease lease = cache.acquire(List.of(b))) {
                assertEquals(500, Files.size(lease.localPath(b.relativePath())));
            }
            assertFalse(Files.exists(pathA));
            assertEquals(java.util.Set.of(), cache.present(List.of(a)));
        }
    }

    @Test
    void refusalCountsCopiesPinnedByOtherQueriesApartFromItsOwn() throws IOException {
        DataFile a = file("s/t/a.parquet", 300, 1);
        DataFile b = file("s/t/b.parquet", 200, 1);
        DataFile c = file("s/t/c.parquet", 400, 1);
        try (LocalCopyCache cache = new LocalCopyCache(dir.resolve("c"), 10_000, 2, copier(),
                150, disk(1000))) {
            cache.acquire(List.of(b)).close();
            assertEquals(0, cache.inUseBytes());
            try (LocalCopyCache.Lease running = cache.acquire(List.of(a))) {
                assertEquals(300, cache.inUseBytes());
                // b (200, reused) + c (400) next to a (300, another query's) leave 100 of 150 free.
                LocalCopyCache.NoRoomException refused = assertThrows(
                        LocalCopyCache.NoRoomException.class, () -> cache.acquire(List.of(b, c)));
                assertTrue(refused.getMessage().contains("this query reuses take 200 bytes, copies in"
                        + " use by other running queries take 300 bytes"), refused.getMessage());
                assertEquals(300, cache.inUseBytes());
            }
            assertEquals(0, cache.inUseBytes());
            // With a released, b and c fit.
            try (LocalCopyCache.Lease lease = cache.acquire(List.of(b, c))) {
                assertEquals(600, cache.inUseBytes());
            }
            assertEquals(0, cache.inUseBytes());
        }
    }

    @Test
    void limitEvictionNeverEvictsACopyTheSameQueryReuses() throws IOException {
        DataFile a = file("s/t/a.parquet", 400, 1);
        DataFile b = file("s/t/b.parquet", 300, 1);
        try (LocalCopyCache cache = new LocalCopyCache(dir.resolve("c"), 500, 2, copier(),
                0, disk(1_000_000))) {
            cache.acquire(List.of(a)).close();
            try (LocalCopyCache.Lease lease = cache.acquire(List.of(a, b))) {
                assertEquals(1, lease.reused());
                assertEquals(1, lease.copied());
                assertArrayEquals(content(a), Files.readAllBytes(lease.localPath(a.relativePath())));
            }
            assertEquals(1, opens.get(a.location()).get());
            // Over the limit once released: back within it.
            assertTrue(cache.cachedBytes() <= 500, "cached " + cache.cachedBytes());
        }
    }

    @Test
    void roomIsTheLimitOrWhatTheDiskLeaves() throws IOException {
        DataFile a = file("s/t/a.parquet", 300, 1);
        try (LocalCopyCache big = new LocalCopyCache(dir.resolve("big"), 800, 1, copier(),
                100, disk(1_000_000))) {
            assertEquals(800, big.room());
            assertEquals(100, big.minFreeBytes());
        }
        try (LocalCopyCache cache = new LocalCopyCache(dir.resolve("c"), 800, 1, copier(),
                100, disk(600))) {
            assertEquals(500, cache.room());
            try (LocalCopyCache.Lease lease = cache.acquire(List.of(a))) {
                // In use: its bytes are not room.
                assertEquals(200, cache.room());
            }
            // Unused: it can be evicted, so it still counts as room.
            assertEquals(500, cache.room());
        }
        try (LocalCopyCache full = new LocalCopyCache(dir.resolve("full"), 800, 1, copier(),
                100, d -> 0)) {
            assertEquals(1, full.room());
        }
    }

    @Test
    void refusesANegativeFreeSpaceReserve() {
        InputStream unused = InputStream.nullInputStream();
        assertThrows(IllegalArgumentException.class,
                () -> new LocalCopyCache(dir, 10, 1, f -> unused, -1, LocalCopyCache.FILE_STORE));
    }

    @Test
    void refusesANonPositiveLimit() {
        InputStream unused = InputStream.nullInputStream();
        assertThrows(IllegalArgumentException.class,
                () -> new LocalCopyCache(dir, 0, 1, f -> unused));
    }
}
