package pro.surpin.data.arrowflight.plugin.s3;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.spi.metrics.MetricSample;

import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the S3 read cache's disk tier, alone and behind the loopback endpoint with fake S3. */
@Tag("unit")
class S3DiskCacheTest {

    private static final String BUCKET = "lake";
    private static final String KEY = "data/t/x.parquet";
    private static final int BLOCK = S3CacheEndpoint.BLOCK_BYTES;
    private static final int SIZE = 4 * BLOCK + 777;

    @TempDir
    Path dir;

    private final AtomicLong clock = new AtomicLong(1_000_000_000L);
    private FakeS3 s3;
    private S3Client backend;
    private S3CacheEndpoint endpoint;
    private S3Client duckDb;
    private byte[] data;

    @BeforeEach
    void start() throws IOException {
        s3 = FakeS3.start();
        data = bytes(SIZE, 1);
        s3.put(BUCKET, KEY, data);
        S3Settings settings = settings(s3.endpoint(), "AKIABACKEND", "backend-secret");
        backend = S3DataStore.createClient(settings);
        // One block in memory, eight on disk.
        endpoint = new S3CacheEndpoint(BLOCK + 100, 10, "n1", clock::get, "s3", false,
                new S3DiskCache.Settings(dir, 8L * BLOCK));
        S3CacheEndpoint.Route route = endpoint.route(S3Uri.parse("s3://lake/data"), backend, settings);
        duckDb = S3DataStore.createClient(settings("http://" + route.duckDbEndpoint(), route.keyId(),
                route.secret()));
    }

    @AfterEach
    void stop() {
        duckDb.close();
        endpoint.close();
        backend.close();
        s3.close();
    }

    /**
     * Verifies blocks the memory tier dropped are answered from their files, not S3, and are
     * kept in memory again.
     *
     * @throws Exception when the writes do not finish
     */
    @Test
    void blocksTheMemoryDroppedComeFromDisk() throws Exception {
        readEveryBlock();
        assertTrue(endpoint.disk().awaitWrites(10_000));
        s3.clearRequests();
        for (int b = 0; b < 5; b++) {
            long from = (long) b * BLOCK;
            assertArrayEquals(Arrays.copyOfRange(data, (int) from, (int) Math.min(SIZE, from + 300)),
                    range(from, from + 299), "block " + b);
        }
        assertEquals(0, s3.count("GET"), endpoint.stats());
        Map<String, Double> values = values(endpoint.samples());
        assertEquals(5.0, values.get("arrowflight_s3_cache_disk_hits_total"));
        assertEquals(5.0, values.get("arrowflight_s3_cache_disk_writes_total"));
        assertEquals(5.0, values.get("arrowflight_s3_cache_disk_blocks"));
        assertEquals(4.0 * BLOCK + 777, values.get("arrowflight_s3_cache_disk_used_bytes"));
        assertEquals(8.0 * BLOCK, values.get("arrowflight_s3_cache_disk_capacity_bytes"));
        assertEquals(0.0, values.get("arrowflight_s3_cache_disk_errors_total"));
        assertEquals(21, values.size(), "11 memory + 10 disk metrics");
        assertTrue(endpoint.stats().contains("diskHits=5"), endpoint.stats());
        assertEquals(5, blockFiles().size());

        s3.clearRequests();
        assertArrayEquals(Arrays.copyOfRange(data, 4 * BLOCK, 4 * BLOCK + 100), range(4L * BLOCK, 4L * BLOCK + 99));
        assertEquals(5.0, values(endpoint.samples()).get("arrowflight_s3_cache_disk_hits_total"),
                "the last block read from disk is in memory again");
    }

    /**
     * Verifies a damaged or deleted block file is a miss read from S3, never an answer.
     *
     * @throws Exception when the writes do not finish
     */
    @Test
    void aDamagedOrDeletedFileIsReadFromS3Again() throws Exception {
        readEveryBlock();
        assertTrue(endpoint.disk().awaitWrites(10_000));
        List<Path> files = blockFiles();
        assertEquals(5, files.size());
        for (Path file : files) {
            byte[] bytes = Files.readAllBytes(file);
            if (bytes.length == BLOCK) {
                bytes[123] ^= 1;
                Files.write(file, bytes);
            } else {
                Files.delete(file);
            }
        }
        s3.clearRequests();
        for (int b = 0; b < 4; b++) {
            long from = (long) b * BLOCK;
            assertArrayEquals(Arrays.copyOfRange(data, (int) from, (int) from + 200), range(from, from + 199),
                    "block " + b);
        }
        assertArrayEquals(Arrays.copyOfRange(data, 4 * BLOCK, SIZE), range(4L * BLOCK, SIZE - 1));
        assertEquals(5, s3.count("GET"), endpoint.stats());
        Map<String, Double> values = values(endpoint.samples());
        assertEquals(0.0, values.get("arrowflight_s3_cache_disk_hits_total"));
        assertEquals(4.0, values.get("arrowflight_s3_cache_disk_errors_total"), "the damaged ones");
    }

    /**
     * Verifies the disk tier holds at most its capacity, least recently used file deleted first.
     *
     * @throws Exception when the tier cannot be used
     */
    @Test
    void theCapacityBoundsTheFilesKept() throws Exception {
        Path own = dir.resolve("own");
        try (S3DiskCache disk = new S3DiskCache(new S3DiskCache.Settings(own, 2L * 1000), "s3", false)) {
            S3BlockCache.BlockKey a = key(0);
            S3BlockCache.BlockKey b = key(1);
            disk.offer(a, block(1000, 1));
            disk.offer(b, block(1000, 2));
            assertTrue(disk.awaitWrites(10_000));
            assertNotNull(disk.get(a));
            disk.offer(key(2), block(1000, 3));
            disk.offer(key(3), block(2001, 4));
            assertTrue(disk.awaitWrites(10_000));
            assertNull(disk.get(b), "least recently used dropped");
            assertArrayEquals(bytes(1000, 1), bytesOf(disk.get(a)));
            assertArrayEquals(bytes(1000, 3), bytesOf(disk.get(key(2))));
            S3DiskCache.Use use = disk.use();
            assertEquals(2000, use.usedBytes());
            assertEquals(1, use.evictions());
            assertEquals(2, files(own).size(), "evicted files are deleted, the oversized one never written");
        }
        assertEquals(0, files(own).size(), "close deletes every block file");
    }

    /**
     * Verifies a second server cannot use a directory that is in use, and that files left by an
     * earlier run are deleted at the start while foreign files stay.
     *
     * @throws Exception when the tier cannot be used
     */
    @Test
    void aDirectoryIsUsedByOneServerAndStartsEmpty() throws Exception {
        Path own = dir.resolve("own");
        S3DiskCache.Settings settings = new S3DiskCache.Settings(own, 1L << 20);
        Files.createDirectories(own.resolve("s3-cache/07"));
        Files.write(own.resolve("s3-cache/07/7.blk"), new byte[10]);
        Files.write(own.resolve("s3-cache/07/8.tmp"), new byte[10]);
        Files.write(own.resolve("s3-cache/notes.txt"), new byte[1]);
        try (S3DiskCache first = new S3DiskCache(settings, "s3", false)) {
            assertEquals(0, files(own).size(), "an earlier run's blocks are gone");
            assertTrue(Files.exists(own.resolve("s3-cache/notes.txt")), "foreign files stay");
            IOException e = assertThrows(IOException.class, () -> new S3DiskCache(settings, "s3", false));
            assertTrue(e.getMessage().contains("in use"), e.getMessage());
            // Another plugin has its own lock and directory under the same path.
            new S3DiskCache(settings, "lakefs", false).close();
        }
        new S3DiskCache(settings, "s3", false).close();
    }

    /**
     * Verifies admission on reuse writes a block only on its second offer.
     *
     * @throws Exception when the tier cannot be used
     */
    @Test
    void admissionOnReuseWritesABlockOfferedAgain() throws Exception {
        try (S3DiskCache disk = new S3DiskCache(new S3DiskCache.Settings(dir.resolve("r"), 4L * BLOCK), "s3",
                true)) {
            disk.offer(key(0), block(100, 1));
            assertTrue(disk.awaitWrites(10_000));
            assertNull(disk.get(key(0)));
            disk.offer(key(0), block(100, 1));
            assertTrue(disk.awaitWrites(10_000));
            assertArrayEquals(bytes(100, 1), bytesOf(disk.get(key(0))));
            assertEquals(1, disk.use().notAdmitted());
        }
    }

    /** Verifies the disk tier's settings are checked. */
    @Test
    void settingsAreChecked() {
        assertNull(S3ReadCache.diskSettings("s3", "", 0, 1 << 20));
        assertNull(S3ReadCache.diskSettings("s3", null, 0, 0));
        S3DiskCache.Settings ok = S3ReadCache.diskSettings("s3", " /var/cache/flight/../flight ", 2048, 1 << 20);
        assertEquals(Path.of("/var/cache/flight"), ok.path());
        assertEquals(2048L << 20, ok.capacityBytes());
        assertMessage("arrowflight.s3.cacheDiskPath", () -> S3ReadCache.diskSettings("s3", "", 10, 1 << 20));
        assertMessage("cacheDiskMegabytes > 0", () -> S3ReadCache.diskSettings("s3", "/c", 0, 1 << 20));
        assertMessage("cacheDiskMegabytes > 0", () -> S3ReadCache.diskSettings("s3", "/c", -1, 1 << 20));
        assertMessage("arrowflight.lakefs.cacheMegabytes > 0",
                () -> S3ReadCache.diskSettings("lakefs", "/c", 10, 0));
        assertMessage("absolute", () -> S3ReadCache.diskSettings("s3", "cache", 10, 1 << 20));
    }

    private static void assertMessage(String part, org.junit.jupiter.api.function.Executable call) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, call);
        assertTrue(e.getMessage().contains(part), e.getMessage());
    }

    private void readEveryBlock() {
        for (int b = 0; b < 5; b++) {
            long from = (long) b * BLOCK;
            assertArrayEquals(Arrays.copyOfRange(data, (int) from, (int) Math.min(SIZE, from + 10)),
                    range(from, from + 9));
        }
        assertEquals(5, s3.count("GET"));
    }

    private List<Path> blockFiles() throws IOException {
        return files(dir);
    }

    private static List<Path> files(Path root) throws IOException {
        if (!Files.exists(root)) {
            return List.of();
        }
        try (Stream<Path> walk = Files.walk(root)) {
            return walk.filter(p -> p.getFileName().toString().endsWith(".blk")).toList();
        }
    }

    private byte[] range(long from, long to) {
        return duckDb.getObjectAsBytes(r -> r.bucket(BUCKET).key(KEY).range("bytes=" + from + "-" + to))
                .asByteArray();
    }

    private static S3BlockCache.BlockKey key(long index) {
        return new S3BlockCache.BlockKey("b", "k", "\"e1\"", index);
    }

    private static S3BlockCache.Block block(int size, long seed) {
        return S3BlockCache.Block.of(bytes(size, seed));
    }

    private static byte[] bytesOf(S3BlockCache.Block block) throws IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        block.write(out, 0, block.length());
        return out.toByteArray();
    }

    private static Map<String, Double> values(List<MetricSample> samples) {
        Map<String, Double> values = new HashMap<>();
        samples.forEach(sample -> assertNull(values.put(sample.name(), sample.value()), sample.name()));
        return values;
    }

    private static S3Settings settings(String endpoint, String keyId, String secret) {
        return new S3Settings(endpoint, "eu-west-3", "path", null, true, keyId, secret, null, 30, 4, true);
    }

    private static byte[] bytes(int size, long seed) {
        byte[] bytes = new byte[size];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }
}
