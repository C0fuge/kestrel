package pro.surpin.data.arrowflight.server.services;

/**
 * Length limit for caches keyed by query text.
 */
public final class QueryCacheLimits {

    /**
     * Longest query text a cache keeps (256 Ki characters). Longer texts, such as a join key
     * list a connector sends as an IN list, are megabytes each and rarely repeat; they are
     * parsed and prepared every time instead of being pinned in memory.
     */
    public static final int MAX_CACHED_QUERY_CHARS = 262_144;

    /**
     * Prevents instantiation.
     */
    private QueryCacheLimits() {
    }

    /**
     * Tells whether a query text is short enough to be cached.
     *
     * @param text query text
     * @return true when it may be cached
     */
    public static boolean cacheable(String text) {
        return text.length() <= MAX_CACHED_QUERY_CHARS;
    }
}
