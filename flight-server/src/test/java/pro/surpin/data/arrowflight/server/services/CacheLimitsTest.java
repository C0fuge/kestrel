package pro.surpin.data.arrowflight.server.services;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks the entry caps of the node's metadata caches (item 49(b), upstream fd3c76a1).
 */
@Tag("unit")
class CacheLimitsTest {

    /**
     * Fills a map with keys 0..count-1 whose value is the key.
     *
     * @param count number of entries
     * @return concurrent map
     */
    private static Map<Integer, Integer> filled(int count) {
        Map<Integer, Integer> entries = new ConcurrentHashMap<>();
        IntStream.range(0, count).forEach(i -> entries.put(i, i));
        return entries;
    }

    /** A cache at or under its cap is left alone, expired entries included. */
    @Test
    void leavesCacheUnderCapAlone() {
        Map<Integer, Integer> entries = filled(100);
        assertEquals(0, CacheLimits.prune(entries, value -> true, 100));
        assertEquals(100, entries.size());
    }

    /** Over the cap, expired entries go first and live ones stay when that is enough. */
    @Test
    void dropsExpiredEntriesFirst() {
        Map<Integer, Integer> entries = filled(101);
        int removed = CacheLimits.prune(entries, value -> value % 2 == 0, 100);
        assertEquals(51, removed);
        assertEquals(50, entries.size());
        assertTrue(entries.keySet().stream().allMatch(key -> key % 2 == 1));
    }

    /** With only live entries, the cache is cut to 90 % of the cap, so the next inserts scan nothing. */
    @Test
    void cutsLiveEntriesToNinetyPercent() {
        Map<Integer, Integer> entries = filled(1001);
        assertEquals(101, CacheLimits.prune(entries, value -> false, 1000));
        assertEquals(900, entries.size());
        for (int i = 0; i < 100; i++) {
            entries.put(10_000 + i, i);
            assertEquals(0, CacheLimits.prune(entries, value -> false, 1000));
        }
        assertEquals(1000, entries.size());
    }

    /** A cap of one keeps one entry. */
    @Test
    void capOfOneKeepsOneEntry() {
        Map<Integer, Integer> entries = filled(5);
        CacheLimits.prune(entries, value -> false, 1);
        assertEquals(1, entries.size());
    }

    /** A cap below one is a programming error. */
    @Test
    void refusesCapBelowOne() {
        assertThrows(IllegalArgumentException.class,
                () -> CacheLimits.prune(filled(1), value -> false, 0));
    }

    /**
     * Threads that insert and prune at once never leave the cache above its cap once they are
     * done, and nothing fails.
     *
     * @throws Exception if a worker fails
     */
    @Test
    void staysBoundedUnderConcurrentInserts() throws Exception {
        Map<Integer, Integer> entries = new ConcurrentHashMap<>();
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            Future<?>[] workers = new Future<?>[4];
            for (int w = 0; w < workers.length; w++) {
                int base = w * 100_000;
                workers[w] = pool.submit(() -> {
                    for (int i = 0; i < 20_000; i++) {
                        entries.put(base + i, i);
                        CacheLimits.prune(entries, value -> value % 7 == 0, 500);
                    }
                });
            }
            for (Future<?> worker : workers) {
                worker.get(60, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        // A prune can miss the puts of other threads that run meanwhile, at most one each.
        assertTrue(entries.size() <= 500 + 4, "size " + entries.size());
        assertFalse(entries.isEmpty());
    }
}
