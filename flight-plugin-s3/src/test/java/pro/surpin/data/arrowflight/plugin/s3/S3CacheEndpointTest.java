package pro.surpin.data.arrowflight.plugin.s3;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Random;
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

    /** Verifies the block cache drops the least recently used block first. */
    @Test
    void blockCacheDropsTheLeastRecentlyUsed() {
        S3BlockCache cache = new S3BlockCache(10);
        S3BlockCache.BlockKey a = new S3BlockCache.BlockKey("b", "k", "e", 0);
        S3BlockCache.BlockKey b = new S3BlockCache.BlockKey("b", "k", "e", 1);
        S3BlockCache.BlockKey c = new S3BlockCache.BlockKey("b", "k", "e", 2);
        cache.put(a, new byte[4]);
        cache.put(b, new byte[4]);
        cache.get(a);
        cache.put(c, new byte[4]);
        assertNull(cache.get(b));
        assertEquals(8, cache.usedBytes());
        cache.put(new S3BlockCache.BlockKey("b", "k", "e", 3), new byte[11]);
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

    private byte[] range(long from, long to) {
        return duckDb.getObjectAsBytes(r -> r.bucket(BUCKET).key(KEY).range("bytes=" + from + "-" + to))
                .asByteArray();
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
