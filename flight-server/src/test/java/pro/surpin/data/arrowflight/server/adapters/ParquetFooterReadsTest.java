package pro.surpin.data.arrowflight.server.adapters;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the parallel Parquet footer reads of item 49 (c). */
@Tag("unit")
class ParquetFooterReadsTest {

    /** Keeps the order of the items whatever order the reads finish in. */
    @Test
    void keepsItemOrder() throws IOException {
        List<Integer> items = IntStream.range(0, 40).boxed().toList();

        List<Integer> results = ParquetFooterReads.readAll(items, i -> {
            sleep((40 - i) % 7);
            return i * 10;
        });

        assertEquals(items.stream().map(i -> i * 10).toList(), results);
    }

    /**
     * Reads with a simulated store round trip of 50 ms each in parallel: 16 reads take about one
     * round trip instead of sixteen (the upstream measurement: 17 ms per cold footer, 2.5 s for
     * 144 files read one after another).
     */
    @Test
    void readsInParallel() throws IOException {
        List<Integer> items = IntStream.range(0, ParquetFooterReads.DEFAULT_THREADS).boxed().toList();
        Set<String> threads = ConcurrentHashMap.newKeySet();

        long start = System.nanoTime();
        ParquetFooterReads.readAll(items, i -> {
            threads.add(Thread.currentThread().getName());
            sleep(50);
            return i;
        });
        long millis = (System.nanoTime() - start) / 1_000_000;

        assertTrue(millis < 16 * 50 / 2, "16 reads of 50 ms took " + millis + " ms");
        assertTrue(threads.size() > 1, "reads ran on " + threads);
        assertTrue(threads.contains(Thread.currentThread().getName()), "the caller reads too");
    }

    /** Reads a single item in the calling thread, and a nested call on a footer thread inline. */
    @Test
    void singleAndNestedReadsStayInline() throws IOException {
        Thread caller = Thread.currentThread();
        assertSame(caller, ParquetFooterReads.readAll(List.of(1), i -> Thread.currentThread()).get(0));
        assertTrue(ParquetFooterReads.readAll(List.of(), i -> i).isEmpty());

        List<List<Thread>> nested = ParquetFooterReads.readAll(List.of(1, 2, 3), i -> {
            Thread outer = Thread.currentThread();
            List<Thread> inner = ParquetFooterReads.readAll(List.of(1, 2, 3), j -> Thread.currentThread());
            if (outer.getName().startsWith("parquet-footer-")) {
                inner.forEach(t -> assertSame(outer, t));
            }
            return inner;
        });
        assertEquals(3, nested.size());
    }

    /** Rethrows the first failure and stops waiting for the other reads. */
    @Test
    void rethrowsFailures() {
        AtomicInteger started = new AtomicInteger();
        IOException io = assertThrows(IOException.class, () -> ParquetFooterReads.readAll(
                List.of(0, 1, 2, 3), i -> {
                    started.incrementAndGet();
                    if (i == 2) {
                        throw new IOException("bad footer " + i);
                    }
                    return i;
                }));
        assertEquals("bad footer 2", io.getMessage());

        IllegalStateException runtime = assertThrows(IllegalStateException.class,
                () -> ParquetFooterReads.readAll(List.of(0, 1), i -> {
                    if (i == 1) {
                        throw new IllegalStateException("broken");
                    }
                    return i;
                }));
        assertEquals("broken", runtime.getMessage());
        assertThrows(IOException.class, () -> ParquetFooterReads.readAll(List.of(0, 1), i -> {
            throw new IOException("first item, read by the caller");
        }));
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
