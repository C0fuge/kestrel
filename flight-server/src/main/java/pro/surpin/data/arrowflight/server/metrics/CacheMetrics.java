package pro.surpin.data.arrowflight.server.metrics;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Counts what the server's query-text caches skip and evict, for the Prometheus endpoint.
 */
public final class CacheMetrics {

    /**
     * A cache keyed by query text.
     */
    public enum Cache {
        /** Parsed client queries ({@code ParquetQueryParser}). */
        PARSE,
        /** Result schemas of client queries ({@code ExecutionService}). */
        SCHEMA,
        /** Prepared DuckDB statements, per worker thread ({@code DuckDbAdapter}). */
        STATEMENT,
        /** Complete-mode result and parameter schemas of client queries ({@code DuckDbAdapter}). */
        DESCRIBE,
        /** Query texts that passed the single-query parse check ({@code DuckDbAdapter}). */
        PARSE_CHECK
    }

    private static final AtomicLongArray OVERSIZE = new AtomicLongArray(Cache.values().length);
    private static final AtomicLongArray EVICTIONS = new AtomicLongArray(Cache.values().length);

    /**
     * Prevents instantiation.
     */
    private CacheMetrics() {
    }

    /**
     * Records a query text too long to be cached.
     *
     * @param cache cache that skipped it
     */
    public static void oversize(Cache cache) {
        OVERSIZE.incrementAndGet(cache.ordinal());
    }

    /**
     * Records an entry dropped because the cache was full.
     *
     * @param cache cache that evicted it
     */
    public static void eviction(Cache cache) {
        EVICTIONS.incrementAndGet(cache.ordinal());
    }

    /**
     * Returns how many query texts a cache skipped for their length.
     *
     * @param cache cache
     * @return count since start
     */
    public static long oversizeCount(Cache cache) {
        return OVERSIZE.get(cache.ordinal());
    }

    /**
     * Returns how many entries a cache evicted.
     *
     * @param cache cache
     * @return count since start
     */
    public static long evictionCount(Cache cache) {
        return EVICTIONS.get(cache.ordinal());
    }

    /**
     * Returns the Prometheus label value of a cache.
     *
     * @param cache cache
     * @return lower-case name
     */
    static String label(Cache cache) {
        return cache.name().toLowerCase(Locale.ROOT);
    }
}
