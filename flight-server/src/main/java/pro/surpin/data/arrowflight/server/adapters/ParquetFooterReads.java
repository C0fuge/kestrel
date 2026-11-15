package pro.surpin.data.arrowflight.server.adapters;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reads several Parquet footers at once on a small shared pool (item 49 (c), after upstream
 * {@code b85c78487f4f} / {@code 9e8bcf5cd51d}): a cold footer costs one or two store round trips
 * (about 17 ms each on HDFS), so reading the footers of a table one after another took seconds
 * for a table of a hundred files.
 *
 * <p>The pool has {@code -DparquetFooterThreads} threads (default 16), separate from the
 * streaming I/O pool, so planning and statistics never wait behind result streams. A single item
 * is read in the calling thread (no hand-off cost for the common point-query case), and with
 * several items the caller reads the first one itself while the pool reads the rest. A call made
 * from a footer thread reads inline, so nested use cannot deadlock the pool. The first failure
 * cancels the outstanding reads and is rethrown; results keep the order of the items.
 */
public final class ParquetFooterReads {

    /** Default number of footer threads. */
    static final int DEFAULT_THREADS = 16;

    private static final String THREAD_PREFIX = "parquet-footer-";
    private static final AtomicInteger THREAD_IDS = new AtomicInteger();
    private static final ExecutorService POOL = Executors.newFixedThreadPool(
            Math.max(1, Integer.getInteger("parquetFooterThreads", DEFAULT_THREADS)), runnable -> {
                Thread thread = new Thread(runnable, THREAD_PREFIX + THREAD_IDS.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            });

    private ParquetFooterReads() {
    }

    /**
     * One footer read.
     *
     * @param <T> item type
     * @param <R> result type
     */
    @FunctionalInterface
    public interface Read<T, R> {
        /**
         * Reads the footer of one item.
         *
         * @param item item to read
         * @return read result
         * @throws IOException on read failure
         */
        R read(T item) throws IOException;
    }

    /**
     * Reads every item, in parallel when there are several.
     *
     * @param items items to read
     * @param read footer read of one item
     * @param <T> item type
     * @param <R> result type
     * @return results in the order of {@code items}
     * @throws IOException the first read failure, or {@link InterruptedIOException} when the
     *         caller is interrupted while waiting
     */
    public static <T, R> List<R> readAll(List<T> items, Read<T, R> read) throws IOException {
        int size = items.size();
        if (size <= 1 || Thread.currentThread().getName().startsWith(THREAD_PREFIX)) {
            List<R> results = new ArrayList<>(size);
            for (T item : items) {
                results.add(read.read(item));
            }
            return results;
        }
        List<Future<R>> pending = new ArrayList<>(size - 1);
        try {
            for (int i = 1; i < size; i++) {
                T item = items.get(i);
                pending.add(POOL.submit(() -> read.read(item)));
            }
            List<R> results = new ArrayList<>(size);
            results.add(read.read(items.get(0)));
            for (Future<R> future : pending) {
                results.add(future.get());
            }
            return results;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            InterruptedIOException interrupted =
                    new InterruptedIOException("Interrupted while reading Parquet footers");
            interrupted.initCause(e);
            throw interrupted;
        } catch (ExecutionException e) {
            throw rethrow(e.getCause());
        } catch (CancellationException e) {
            throw new IOException("Parquet footer read cancelled", e);
        } finally {
            for (Future<R> future : pending) {
                future.cancel(true);
            }
        }
    }

    private static IOException rethrow(Throwable cause) {
        if (cause instanceof IOException io) {
            return io;
        }
        if (cause instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return new IOException("Unable to read Parquet footer", cause);
    }
}
