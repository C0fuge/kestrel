package pro.surpin.data.arrowflight.plugin.s3;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.spi.metrics.MetricSample;
import pro.surpin.data.arrowflight.spi.metrics.MetricSource;
import pro.surpin.data.arrowflight.spi.metrics.PluginMetrics;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the S3 read cache's loopback endpoint with a signing S3 client in front, fake S3 behind. */
@Tag("unit")
class S3CacheEndpointTest {

    private static final String BUCKET = "lake";
    private static final String KEY = "data/t/x.parquet";
    private static final int SIZE = 3 * S3CacheEndpoint.BLOCK_BYTES + 12_345;
    /** Concurrent readers; the backend client has 4 connections. */
    private static final int READERS = 4;

    private final AtomicLong clock = new AtomicLong(1_000_000_000L);
    private FakeS3 s3;
    private S3Client backend;
    private S3CacheEndpoint endpoint;
    private S3CacheEndpoint.Route route;
    private S3Client duckDb;
    private byte[] data;

    @BeforeEach
    void start() throws IOException {
        s3 = FakeS3.start();
        data = bytes(SIZE, 1);
        s3.put(BUCKET, KEY, data);
        s3.put(BUCKET, "other/y.parquet", data);
        S3Settings settings = settings(s3.endpoint(), "AKIABACKEND", "backend-secret");
        backend = S3DataStore.createClient(settings);
        endpoint = new S3CacheEndpoint(2L * S3CacheEndpoint.BLOCK_BYTES + 100, 10, "n1", clock::get);
        route = endpoint.route(S3Uri.parse("s3://lake/data"), backend, settings);
        duckDb = client(route.keyId(), route.secret());
    }

    @AfterEach
    void stop() {
        duckDb.close();
        endpoint.close();
        backend.close();
        s3.close();
    }

    /** Verifies ranges are answered right and a repeated range is not read from S3 again. */
    @Test
    void repeatedRangesComeFromTheCache() {
        int from = S3CacheEndpoint.BLOCK_BYTES - 10;
        int to = S3CacheEndpoint.BLOCK_BYTES + 99;
        assertArrayEquals(Arrays.copyOfRange(data, from, to + 1), range(from, to));
        assertEquals(1, gets(), "two missing blocks are read in one GET");
        assertEquals(1, s3.count("HEAD"), "the version is looked up once");
        s3.clearRequests();

        assertArrayEquals(Arrays.copyOfRange(data, from + 5, to), range(from + 5, to - 1));
        assertEquals(0, gets());
        assertEquals(0, s3.count("HEAD"));

        // The last, short block.
        assertArrayEquals(Arrays.copyOfRange(data, SIZE - 100, SIZE), range(SIZE - 100, SIZE + 1000));
        ResponseBytes<GetObjectResponse> whole = duckDb.getObjectAsBytes(r -> r.bucket(BUCKET).key(KEY));
        assertArrayEquals(data, whole.asByteArray());
        assertEquals("\"e" + SIZE + "-" + Integer.toHexString(Arrays.hashCode(data)) + "\"", whole.response().eTag());
        assertTrue(endpoint.stats().contains("evictions="), endpoint.stats());
    }

    /** Verifies the metrics count blocks, hits and misses and the requests sent to S3. */
    @Test
    void metricsCountBlocksAndRequestsToS3() {
        int from = S3CacheEndpoint.BLOCK_BYTES - 10;
        int to = S3CacheEndpoint.BLOCK_BYTES + 99;
        range(from, to);
        range(from, to);
        Map<String, Double> values = values(endpoint.samples());
        assertEquals(2L * S3CacheEndpoint.BLOCK_BYTES + 100, values.get("arrowflight_s3_cache_capacity_bytes"));
        assertEquals(2.0 * S3CacheEndpoint.BLOCK_BYTES, values.get("arrowflight_s3_cache_used_bytes"));
        assertEquals(2.0, values.get("arrowflight_s3_cache_blocks"));
        assertEquals(2.0, values.get("arrowflight_s3_cache_hits_total"));
        assertEquals(2.0, values.get("arrowflight_s3_cache_misses_total"));
        assertEquals(0.0, values.get("arrowflight_s3_cache_evictions_total"));
        assertEquals(0.0, values.get("arrowflight_s3_cache_joined_total"));
        assertEquals((double) gets(), values.get("arrowflight_s3_cache_s3_reads_total"));
        assertEquals(2.0 * S3CacheEndpoint.BLOCK_BYTES, values.get("arrowflight_s3_cache_s3_read_bytes_total"));
        assertEquals((double) s3.count("HEAD"), values.get("arrowflight_s3_cache_s3_lookups_total"));
        assertEquals(0.0, values.get("arrowflight_s3_cache_not_admitted_total"));
        assertEquals(11, values.size());
    }

    /** Verifies the shared endpoint publishes its metrics from its start until its last route closes. */
    @Test
    void theSharedEndpointPublishesItsMetricsWhileItRuns() throws IOException {
        List<MetricSource> sources = new CopyOnWriteArrayList<>();
        PluginMetrics metrics = source -> {
            sources.add(source);
            return () -> sources.remove(source);
        };
        S3Settings settings = settings(s3.endpoint(), "AKIABACKEND", "backend-secret");
        S3CacheEndpoint.Route first = S3CacheEndpoint.register(S3CacheEndpoint.BLOCK_BYTES, 10, "n1",
                S3Uri.parse("s3://lake/data"), backend, settings, metrics);
        S3CacheEndpoint.Route second = S3CacheEndpoint.register(S3CacheEndpoint.BLOCK_BYTES, 10, "n1",
                S3Uri.parse("s3://lake/other"), backend, settings, metrics);
        try {
            assertEquals(1, sources.size(), "one source for the node-wide cache");
            assertEquals(S3CacheEndpoint.BLOCK_BYTES,
                    values(sources.get(0).samples()).get("arrowflight_s3_cache_capacity_bytes"));
            first.close();
            assertEquals(1, sources.size(), "a route is still open");
        } finally {
            first.close();
            second.close();
        }
        assertEquals(List.of(), sources);
    }

    /** Verifies the cache keeps at most its capacity: the oldest blocks are read again. */
    @Test
    void theCapacityBoundsTheBlocksKept() {
        range(0, 10);
        range(S3CacheEndpoint.BLOCK_BYTES, S3CacheEndpoint.BLOCK_BYTES + 10);
        range(2L * S3CacheEndpoint.BLOCK_BYTES, 2L * S3CacheEndpoint.BLOCK_BYTES + 10);
        s3.clearRequests();
        range(2L * S3CacheEndpoint.BLOCK_BYTES, 2L * S3CacheEndpoint.BLOCK_BYTES + 10);
        assertEquals(0, gets(), "recent block kept");
        range(0, 10);
        assertEquals(1, gets(), "oldest block dropped");
    }

    /**
     * Verifies a rewritten object is seen at the next version check, and that a block read after
     * a rewrite inside the check interval never mixes versions: If-Match fails and the answer is
     * the new version under its new ETag (DuckDB then refuses the changed file and the server
     * runs the query again).
     */
    @Test
    void aRewrittenObjectIsNeverMixedWithItsOldBlocks() {
        assertArrayEquals(Arrays.copyOfRange(data, 0, 100), range(0, 99));
        byte[] second = bytes(SIZE, 2);
        s3.put(BUCKET, KEY, second, Instant.parse("2026-10-27T09:00:00Z"));

        // Within the interval a cached block is the old version, consistently.
        assertArrayEquals(Arrays.copyOfRange(data, 0, 100), range(0, 99));
        // A missing block of the old version cannot be read any more: the new version is sent.
        ResponseBytes<GetObjectResponse> mixed = duckDb.getObjectAsBytes(r -> r.bucket(BUCKET).key(KEY)
                .range("bytes=0-" + (S3CacheEndpoint.BLOCK_BYTES + 9)));
        assertArrayEquals(Arrays.copyOfRange(second, 0, S3CacheEndpoint.BLOCK_BYTES + 10), mixed.asByteArray());
        assertNotEquals(mixed.response().eTag(), etagOf(data));

        s3.put(BUCKET, KEY, data, Instant.parse("2026-10-27T10:00:00Z"));
        clock.addAndGet(TimeUnit.SECONDS.toNanos(10));
        assertArrayEquals(Arrays.copyOfRange(data, 0, 100), range(0, 99), "checked again after the interval");
    }

    /**
     * Verifies a store that ignores If-Match (a gateway) cannot mix versions either: a miss read
     * that comes back with another ETag is refused and the new version is sent under its own ETag.
     */
    @Test
    void aStoreThatIgnoresIfMatchIsNeverMixedEither() {
        s3.ignoreIfMatch(true);
        assertArrayEquals(Arrays.copyOfRange(data, 0, 100), range(0, 99));
        byte[] second = bytes(SIZE, 4);
        s3.put(BUCKET, KEY, second, Instant.parse("2026-10-27T09:00:00Z"));

        ResponseBytes<GetObjectResponse> answer = duckDb.getObjectAsBytes(r -> r.bucket(BUCKET).key(KEY)
                .range("bytes=0-" + (S3CacheEndpoint.BLOCK_BYTES + 9)));
        assertArrayEquals(Arrays.copyOfRange(second, 0, S3CacheEndpoint.BLOCK_BYTES + 10), answer.asByteArray());
        assertEquals(etagOf(second), answer.response().eTag());
        s3.clearRequests();
        assertArrayEquals(Arrays.copyOfRange(second, 0, 100), range(0, 99), "the old block is not served again");
        assertEquals(0, gets());
    }

    /** Verifies the endpoint of another plugin names its settings and metrics after it. */
    @Test
    void anotherPluginsEndpointHasItsOwnMetricNames() throws IOException {
        S3CacheEndpoint lakefs = new S3CacheEndpoint(S3CacheEndpoint.BLOCK_BYTES, 10, "n1", clock::get, "lakefs");
        try {
            Map<String, Double> values = values(lakefs.samples());
            assertEquals(11, values.size());
            assertTrue(values.keySet().stream().allMatch(name -> name.startsWith("arrowflight_lakefs_cache_")),
                    values.toString());
        } finally {
            lakefs.close();
        }
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new S3CacheEndpoint(1, -1, "n1", clock::get, "lakefs"));
        assertTrue(e.getMessage().contains("arrowflight.lakefs.cacheCheckSeconds"), e.getMessage());
        e = assertThrows(IllegalArgumentException.class, () -> S3ReadCache.cacheBytes("lakefs", 513, 1L << 30));
        assertTrue(e.getMessage().contains("arrowflight.lakefs.cacheMegabytes"), e.getMessage());
    }

    /**
     * Verifies concurrent requests for the same missing blocks read them from S3 once: the later
     * requests take the first one's read (single flight).
     *
     * @throws Exception when a reader fails
     */
    @Test
    void concurrentMissesOfOneBlockReadItOnce() throws Exception {
        assertEquals(SIZE, duckDb.headObject(r -> r.bucket(BUCKET).key(KEY)).contentLength());
        s3.clearRequests();
        s3.latency(300);
        long from = S3CacheEndpoint.BLOCK_BYTES - 10;
        long to = S3CacheEndpoint.BLOCK_BYTES + 99;
        for (byte[] answer : concurrently(READERS, from, to)) {
            assertArrayEquals(Arrays.copyOfRange(data, (int) from, (int) to + 1), answer);
        }
        assertEquals(1, gets(), "two blocks, " + READERS + " readers, one GET: " + endpoint.stats());
        assertTrue(endpoint.stats().contains("joined=" + 2 * (READERS - 1)), endpoint.stats());
    }

    /**
     * Verifies concurrent requests for the same missing span claim it whole: one request reads all
     * its blocks with one GET, never one block each (regression: the blocks were claimed one by
     * one, so a second request could claim the next block before the first got to it).
     *
     * @throws Exception when a reader fails
     */
    @Test
    void concurrentMissesOfOneSpanAreReadWithOneGet() throws Exception {
        int readers = 16;
        long from = 0;
        long to = 2L * S3CacheEndpoint.BLOCK_BYTES - 1;
        ExecutorService pool = Executors.newFixedThreadPool(readers);
        List<S3Client> clients = new ArrayList<>();
        try {
            for (int i = 0; i < readers; i++) {
                clients.add(client(route.keyId(), route.secret()));
            }
            s3.latency(20);
            for (int round = 0; round < 25; round++) {
                byte[] content = bytes(SIZE, 10 + round);
                s3.put(BUCKET, KEY, content);
                clock.addAndGet(TimeUnit.SECONDS.toNanos(11));
                assertEquals(SIZE, duckDb.headObject(r -> r.bucket(BUCKET).key(KEY)).contentLength());
                s3.clearRequests();
                CountDownLatch start = new CountDownLatch(1);
                List<Future<byte[]>> answers = new ArrayList<>();
                for (S3Client reader : clients) {
                    answers.add(pool.submit(() -> {
                        start.await();
                        return reader.getObjectAsBytes(r -> r.bucket(BUCKET).key(KEY)
                                .range("bytes=" + from + "-" + to)).asByteArray();
                    }));
                }
                start.countDown();
                for (Future<byte[]> answer : answers) {
                    assertArrayEquals(Arrays.copyOfRange(content, (int) from, (int) to + 1),
                            answer.get(30, TimeUnit.SECONDS));
                }
                assertEquals(1, gets(), "round " + round + ": " + endpoint.stats());
            }
        } finally {
            pool.shutdownNow();
            clients.forEach(S3Client::close);
        }
    }

    /**
     * Verifies a read that finds the object changed fails every request waiting for it, and each
     * then reads the new version (again once for all), never the old one.
     *
     * @throws Exception when a reader fails
     */
    @Test
    void aChangedObjectFailsEveryWaiterAndIsReadAgainOnce() throws Exception {
        range(0, 99);
        byte[] second = bytes(SIZE, 3);
        s3.put(BUCKET, KEY, second, Instant.parse("2026-10-27T09:00:00Z"));
        s3.clearRequests();
        s3.latency(300);
        long from = S3CacheEndpoint.BLOCK_BYTES;
        long to = 2L * S3CacheEndpoint.BLOCK_BYTES + 9;
        for (byte[] answer : concurrently(READERS, from, to)) {
            assertArrayEquals(Arrays.copyOfRange(second, (int) from, (int) to + 1), answer);
        }
        assertEquals(2, gets(), "one GET refused (If-Match), one GET of the new version: " + endpoint.stats());
        s3.latency(0);
        s3.clearRequests();
        assertArrayEquals(Arrays.copyOfRange(second, (int) from, (int) to + 1), range(from, to));
        assertEquals(0, gets(), "nothing left waiting or claimed");
    }

    /** Verifies HEADs always reach S3 and answer the current version. */
    @Test
    void headsAreAlwaysLookedUp() {
        assertEquals(SIZE, duckDb.headObject(r -> r.bucket(BUCKET).key(KEY)).contentLength());
        assertEquals(SIZE, duckDb.headObject(r -> r.bucket(BUCKET).key(KEY)).contentLength());
        assertEquals(2, s3.count("HEAD"));
        S3Exception missing = assertThrows(S3Exception.class,
                () -> duckDb.headObject(r -> r.bucket(BUCKET).key("data/t/missing.parquet")));
        assertEquals(404, missing.statusCode());
    }

    /** Verifies only a correctly signed request for the route's own data directory is served. */
    @Test
    void otherKeysAndPathsAreRefused() {
        try (S3Client wrong = client(route.keyId(), "not-the-secret");
                S3Client unknown = client("AKIAUNKNOWN", route.secret())) {
            assertEquals(403, assertThrows(S3Exception.class,
                    () -> wrong.getObjectAsBytes(r -> r.bucket(BUCKET).key(KEY))).statusCode());
            assertEquals(403, assertThrows(S3Exception.class,
                    () -> unknown.getObjectAsBytes(r -> r.bucket(BUCKET).key(KEY))).statusCode());
        }
        assertEquals(403, assertThrows(S3Exception.class,
                () -> duckDb.getObjectAsBytes(r -> r.bucket(BUCKET).key("other/y.parquet"))).statusCode());
        assertEquals(403, assertThrows(S3Exception.class,
                () -> duckDb.getObjectAsBytes(r -> r.bucket("lake2").key(KEY))).statusCode());
        assertEquals(403, assertThrows(S3Exception.class,
                () -> duckDb.getObjectAsBytes(r -> r.bucket(BUCKET).key("data/../other/y.parquet"))).statusCode());
        assertTrue(S3CacheEndpoint.plainKey("data/t/x.parquet"));
        assertTrue(!S3CacheEndpoint.plainKey("data//x") && !S3CacheEndpoint.plainKey("data/./x")
                && !S3CacheEndpoint.plainKey("data/t/..") && !S3CacheEndpoint.plainKey(""));
        assertEquals(501, assertThrows(S3Exception.class,
                () -> duckDb.listObjectsV2(r -> r.bucket(BUCKET).prefix("data/"))).statusCode());
        assertEquals(0, gets(), "nothing refused reached S3");
        route.close();
        assertEquals(403, assertThrows(S3Exception.class,
                () -> duckDb.getObjectAsBytes(r -> r.bucket(BUCKET).key(KEY))).statusCode(), "closed route");
    }

    /** Verifies the Range forms httpfs and others send. */
    @Test
    void rangesAreParsed() {
        assertArrayEquals(new long[] {0, 99}, S3CacheEndpoint.span(null, 100));
        assertArrayEquals(new long[] {10, 19}, S3CacheEndpoint.span("bytes=10-19", 100));
        assertArrayEquals(new long[] {10, 99}, S3CacheEndpoint.span("bytes=10-", 100));
        assertArrayEquals(new long[] {90, 99}, S3CacheEndpoint.span("bytes=-10", 100));
        assertArrayEquals(new long[] {90, 99}, S3CacheEndpoint.span("bytes=90-500", 100));
        assertArrayEquals(new long[] {0, -1}, S3CacheEndpoint.span(null, 0));
        assertNull(S3CacheEndpoint.span("bytes=100-", 100));
        assertNull(S3CacheEndpoint.span("bytes=5-1", 100));
        assertNull(S3CacheEndpoint.span("bytes=0-1,5-6", 100));
        assertNull(S3CacheEndpoint.span("items=0-1", 100));
        assertNull(S3CacheEndpoint.span("bytes=x-1", 100));
    }

    /**
     * Verifies admission on reuse: a block is kept from its second read on, and a scan of more
     * blocks than the cache holds passes through without pushing out the blocks read again.
     *
     * @throws IOException when the endpoint cannot start
     */
    @Test
    void admissionOnReuseKeepsBlocksReadAgainAndLetsLargeScansPass() throws IOException {
        int blocks = 12;
        byte[] big = bytes(blocks * S3CacheEndpoint.BLOCK_BYTES, 6);
        s3.put(BUCKET, "data/t/big.parquet", big);
        S3CacheEndpoint reuse = new S3CacheEndpoint(4L * S3CacheEndpoint.BLOCK_BYTES, 10, "n1", clock::get,
                "s3", true);
        S3Settings settings = settings(s3.endpoint(), "AKIABACKEND", "backend-secret");
        S3CacheEndpoint.Route own = reuse.route(S3Uri.parse("s3://lake/data"), backend, settings);
        try (S3Client reader = S3DataStore.createClient(settings("http://" + own.duckDbEndpoint(), own.keyId(),
                own.secret()))) {
            for (int i = 0; i < 3; i++) {
                s3.clearRequests();
                assertArrayEquals(Arrays.copyOfRange(data, 0, 100), range(reader, KEY, 0, 99));
                assertEquals(i < 2 ? 1 : 0, gets(), "read " + i + ": " + reuse.stats());
            }
            // Two full scans of 12 blocks through a 4-block cache: never kept (each block's
            // second read is 12 names later, past the 4 remembered), the hot block stays.
            for (int scan = 0; scan < 2; scan++) {
                for (int b = 0; b < blocks; b++) {
                    long from = (long) b * S3CacheEndpoint.BLOCK_BYTES;
                    assertArrayEquals(Arrays.copyOfRange(big, (int) from, (int) from + 100),
                            range(reader, "data/t/big.parquet", from, from + 99));
                }
            }
            s3.clearRequests();
            assertArrayEquals(Arrays.copyOfRange(data, 0, 100), range(reader, KEY, 0, 99));
            assertEquals(0, gets(), "the block read again survives the scans: " + reuse.stats());
            Map<String, Double> values = values(reuse.samples());
            assertEquals(1.0, values.get("arrowflight_s3_cache_blocks"));
            assertEquals(1.0 + 2 * blocks, values.get("arrowflight_s3_cache_not_admitted_total"));
            assertEquals(0.0, values.get("arrowflight_s3_cache_evictions_total"));
            assertTrue(reuse.stats().contains("notAdmitted=" + (1 + 2 * blocks)), reuse.stats());
        } finally {
            own.close();
            reuse.close();
        }
        assertTrue(S3ReadCache.admitOnReuse("s3", "reuse"));
        assertTrue(S3ReadCache.admitOnReuse("s3", " Reuse "));
        assertEquals(false, S3ReadCache.admitOnReuse("s3", "always"));
        assertEquals(false, S3ReadCache.admitOnReuse("s3", null));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> S3ReadCache.admitOnReuse("lakefs", "lru"));
        assertTrue(e.getMessage().contains("arrowflight.lakefs.cacheAdmission"), e.getMessage());
    }

    /**
     * Verifies a store that ignores Range (sends the whole object) still gives the right bytes of
     * each block, at the start, the middle and the end of the object.
     */
    @Test
    void aStoreThatIgnoresRangeIsAnsweredRight() {
        s3.ignoreRange(true);
        long[][] spans = {{0, 99}, {S3CacheEndpoint.BLOCK_BYTES - 10, S3CacheEndpoint.BLOCK_BYTES + 99},
            {SIZE - 50, SIZE - 1}};
        for (long[] span : spans) {
            assertArrayEquals(Arrays.copyOfRange(data, (int) span[0], (int) span[1] + 1), range(span[0], span[1]),
                    span[0] + ".." + span[1]);
        }
        s3.ignoreRange(false);
        s3.clearRequests();
        assertArrayEquals(Arrays.copyOfRange(data, 10, 2 * S3CacheEndpoint.BLOCK_BYTES),
                range(10, 2L * S3CacheEndpoint.BLOCK_BYTES - 1));
    }

    /**
     * Verifies a block is held in pieces small enough never to be a humongous G1 object, and that
     * any part of it is written back exactly (across piece borders too).
     *
     * @throws IOException never, the stream is in memory
     */
    @Test
    void blocksAreHeldInPiecesBelowHalfAHeapRegion() throws IOException {
        assertTrue(S3BlockCache.Block.PIECE_BYTES <= (1 << 20) / 2 - 16, "below half of G1's smallest region");
        byte[] bytes = bytes(S3CacheEndpoint.BLOCK_BYTES, 5);
        S3BlockCache.Block block = S3BlockCache.Block.of(bytes);
        assertEquals(bytes.length, block.length());
        int piece = S3BlockCache.Block.PIECE_BYTES;
        int[][] parts = {{0, bytes.length}, {piece - 3, piece + 5}, {piece, 2 * piece}, {10, 11}, {7, 7},
            {bytes.length - 1, bytes.length}};
        for (int[] part : parts) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            block.write(out, part[0], part[1]);
            assertArrayEquals(Arrays.copyOfRange(bytes, part[0], part[1]), out.toByteArray(),
                    part[0] + ".." + part[1]);
        }
        assertThrows(EOFException.class,
                () -> S3BlockCache.Block.read(new ByteArrayInputStream(new byte[10]), 11));
    }

    /** Verifies the block cache drops the least recently used block first. */
    @Test
    void blockCacheDropsTheLeastRecentlyUsed() {
        S3BlockCache cache = new S3BlockCache(10);
        S3BlockCache.BlockKey a = new S3BlockCache.BlockKey("b", "k", "e", 0);
        S3BlockCache.BlockKey b = new S3BlockCache.BlockKey("b", "k", "e", 1);
        S3BlockCache.BlockKey c = new S3BlockCache.BlockKey("b", "k", "e", 2);
        cache.put(a, S3BlockCache.Block.of(new byte[4]));
        cache.put(b, S3BlockCache.Block.of(new byte[4]));
        cache.get(a);
        cache.put(c, S3BlockCache.Block.of(new byte[4]));
        assertNull(cache.get(b));
        assertEquals(8, cache.usedBytes());
        cache.put(new S3BlockCache.BlockKey("b", "k", "e", 3), S3BlockCache.Block.of(new byte[11]));
        assertEquals(8, cache.usedBytes(), "a block larger than the cache is not kept");
        assertThrows(IllegalArgumentException.class, () -> new S3BlockCache(0));
    }

    /** Verifies the cache size is refused above half the heap. */
    @Test
    void cacheSizeIsBoundedByTheHeap() {
        assertEquals(0, S3DataStoreProvider.cacheBytes(0, 1L << 30));
        assertEquals(512L << 20, S3DataStoreProvider.cacheBytes(512, 1L << 30));
        assertThrows(IllegalArgumentException.class, () -> S3DataStoreProvider.cacheBytes(513, 1L << 30));
        assertThrows(IllegalArgumentException.class, () -> S3DataStoreProvider.cacheBytes(-1, 1L << 30));
    }

    private List<byte[]> concurrently(int readers, long from, long to) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(readers);
        List<S3Client> clients = new ArrayList<>();
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<byte[]>> answers = new ArrayList<>();
            for (int i = 0; i < readers; i++) {
                S3Client reader = client(route.keyId(), route.secret());
                clients.add(reader);
                answers.add(pool.submit(() -> {
                    start.await();
                    return reader.getObjectAsBytes(r -> r.bucket(BUCKET).key(KEY).range("bytes=" + from + "-" + to))
                            .asByteArray();
                }));
            }
            start.countDown();
            List<byte[]> result = new ArrayList<>();
            for (Future<byte[]> answer : answers) {
                result.add(answer.get(30, TimeUnit.SECONDS));
            }
            return result;
        } finally {
            pool.shutdownNow();
            clients.forEach(S3Client::close);
        }
    }

    private byte[] range(long from, long to) {
        return range(duckDb, from, to);
    }

    private static byte[] range(S3Client reader, long from, long to) {
        return range(reader, KEY, from, to);
    }

    private static byte[] range(S3Client reader, String key, long from, long to) {
        return reader.getObjectAsBytes(r -> r.bucket(BUCKET).key(key).range("bytes=" + from + "-" + to))
                .asByteArray();
    }

    private static Map<String, Double> values(List<MetricSample> samples) {
        Map<String, Double> values = new HashMap<>();
        samples.forEach(sample -> assertNull(values.put(sample.name(), sample.value()), sample.name()));
        return values;
    }

    private long gets() {
        return s3.count("GET");
    }

    private S3Client client(String keyId, String secret) {
        return S3DataStore.createClient(settings("http://" + route.duckDbEndpoint(), keyId, secret));
    }

    private static S3Settings settings(String endpoint, String keyId, String secret) {
        return new S3Settings(endpoint, "eu-west-3", "path", null, true, keyId, secret, null, 30, 4, true);
    }

    private static String etagOf(byte[] bytes) {
        return "\"e" + bytes.length + "-" + Integer.toHexString(Arrays.hashCode(bytes)) + "\"";
    }

    private static byte[] bytes(int size, long seed) {
        byte[] bytes = new byte[size];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }
}
