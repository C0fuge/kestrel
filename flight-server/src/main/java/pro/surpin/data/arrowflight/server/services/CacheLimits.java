package pro.surpin.data.arrowflight.server.services;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Entry caps for the metadata caches of a node (footers, schemas, file plans, statistics).
 * Each cache already drops an entry when it is read after its TTL; without a cap, entries that
 * are never read again (dropped tables, rewritten files, one-off table sets) stay for good.
 * A cache over its cap first loses its expired entries, then arbitrary live ones down to 90 %
 * of the cap, so a full cache is scanned about once per tenth of its cap instead of on every
 * insert. An evicted entry is only read again from storage: answers never change.
 */
public final class CacheLimits {

    /** Parquet footers (schema, row count, length) by file. */
    public static final int MAX_CACHED_FOOTERS = 50_000;

    /** Arrow schemas and sample files by table. */
    public static final int MAX_CACHED_TABLE_SCHEMAS = 4_096;

    /** File plans by set of source tables. */
    public static final int MAX_CACHED_FILE_PLANS = 1_024;

    /** Aggregated footer statistics by table. */
    public static final int MAX_CACHED_TABLE_STATISTICS = 4_096;

    /**
     * Prevents instantiation.
     */
    private CacheLimits() {
    }

    /**
     * Keeps a cache at or under its cap; does nothing while it is under the cap.
     *
     * @param entries cache to trim (a concurrent map: other threads may use it meanwhile)
     * @param expired tells whether an entry's TTL has passed
     * @param maximum largest number of entries kept; at least 1
     * @param <K> key type
     * @param <V> value type
     * @return number of entries removed
     */
    public static <K, V> int prune(Map<K, V> entries, Predicate<V> expired, int maximum) {
        if (maximum < 1) {
            throw new IllegalArgumentException("Cache cap must be positive: " + maximum);
        }
        int before = entries.size();
        if (before <= maximum) {
            return 0;
        }
        entries.values().removeIf(expired);
        int target = Math.max(1, maximum - maximum / 10);
        int excess = entries.size() - target;
        Iterator<V> it = entries.values().iterator();
        while (excess > 0 && it.hasNext()) {
            it.next();
            it.remove();
            excess--;
        }
        return Math.max(0, before - entries.size());
    }
}
