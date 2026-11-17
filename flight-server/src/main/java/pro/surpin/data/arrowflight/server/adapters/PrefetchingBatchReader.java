package pro.surpin.data.arrowflight.server.adapters;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

/**
 * Reads the batches of one DuckDB result stream on a thread of its own, up to a fixed number of
 * batches ahead of the Flight writer (item 54, {@code flightStreamPrefetchBatches}).
 *
 * <p>Without it one thread converts DuckDB's chunks to Arrow ({@code loadNextBatch}) and then
 * writes the batch ({@code putNext}), one after the other, so a single stream gets at most the
 * speed of the sum of both. Here the reader thread loads batch n+1 while the caller sends batch n.
 * Each loaded batch's buffers are moved (no copy) into a root of its own, handed over through a
 * bounded queue and moved again into the caller's target root; the rows and their order are the
 * reader's.</p>
 *
 * <p>Not thread-safe: one caller calls {@link #next(VectorSchemaRoot)} and {@link #close()}.</p>
 */
final class PrefetchingBatchReader implements AutoCloseable {

    /** Loads one batch into the reader's root; false at the end of the stream. */
    @FunctionalInterface
    interface Loader {
        /**
         * Loads the next batch.
         *
         * @return false at the end of the stream
         * @throws Exception when the stream fails
         */
        boolean load() throws Exception;
    }

    private static final AtomicInteger THREADS = new AtomicInteger();
    private static final ExecutorService POOL = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "stream-prefetch-" + THREADS.incrementAndGet());
        t.setDaemon(true);
        return t;
    });
    /** How often a full queue rechecks whether the caller went away. */
    private static final long OFFER_POLL_MILLIS = 50;

    /** Queue element: a batch, the end of the stream, or the reader's failure. */
    private record Item(VectorSchemaRoot root, Throwable error) {
    }

    private static final Item END = new Item(null, null);

    private final ArrowReader reader;
    private final Loader loader;
    private final BufferAllocator allocator;
    private final BlockingQueue<Item> queue;
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile boolean closed;
    private boolean finished;

    /**
     * Starts reading ahead.
     *
     * @param reader    DuckDB's Arrow reader (its root is reused by every load)
     * @param loader    loads the next batch into the reader's root
     * @param allocator allocator of the handed-over roots
     * @param depth     batches read ahead, at least 1
     * @throws java.io.IOException when the reader's root cannot be read
     */
    PrefetchingBatchReader(ArrowReader reader, Loader loader, BufferAllocator allocator, int depth)
            throws java.io.IOException {
        if (depth < 1) {
            throw new IllegalArgumentException("depth must be at least 1: " + depth);
        }
        this.reader = reader;
        this.loader = loader;
        this.allocator = allocator;
        this.queue = new ArrayBlockingQueue<>(depth);
        reader.getVectorSchemaRoot();
        POOL.execute(this::run);
    }

    /** Reader thread: loads, moves each batch into its own root and queues it. */
    private void run() {
        try {
            VectorSchemaRoot source = reader.getVectorSchemaRoot();
            while (!closed) {
                if (!loader.load()) {
                    put(END);
                    return;
                }
                VectorSchemaRoot copy = VectorSchemaRoot.create(source.getSchema(), allocator);
                move(source, copy);
                if (!put(new Item(copy, null))) {
                    copy.close();
                    return;
                }
            }
        } catch (Throwable e) {
            put(new Item(null, e));
        } finally {
            done.countDown();
        }
    }

    /**
     * Queues an item, waiting while the queue is full, unless the caller closed this reader.
     *
     * @param item batch, end or failure
     * @return false when closed before the item was queued
     */
    private boolean put(Item item) {
        try {
            while (!closed) {
                if (queue.offer(item, OFFER_POLL_MILLIS, TimeUnit.MILLISECONDS)) {
                    return true;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    /**
     * Moves the vectors' buffers of one root into another of the same schema (no copy).
     *
     * @param from source root, left empty
     * @param to   target root
     */
    static void move(VectorSchemaRoot from, VectorSchemaRoot to) {
        List<FieldVector> source = from.getFieldVectors();
        List<FieldVector> target = to.getFieldVectors();
        for (int i = 0; i < source.size(); i++) {
            source.get(i).makeTransferPair(target.get(i)).transfer();
        }
        to.setRowCount(from.getRowCount());
        from.setRowCount(0);
    }

    /**
     * Moves the next batch into the target root, waiting for the reader thread.
     *
     * @param target root with the reader's schema (the one the Flight listener sends)
     * @return false at the end of the stream
     * @throws Exception the reader's failure, as thrown by the load
     */
    boolean next(VectorSchemaRoot target) throws Exception {
        if (finished) {
            return false;
        }
        Item item = queue.take();
        if (item == END) {
            finished = true;
            return false;
        }
        if (item.error() != null) {
            finished = true;
            Throwable e = item.error();
            if (e instanceof Exception ex) {
                throw ex;
            }
            throw (Error) e;
        }
        try (VectorSchemaRoot root = item.root()) {
            move(root, target);
        }
        return true;
    }

    /**
     * Stops reading ahead, releases queued batches and waits until the reader thread has left the
     * reader (it may finish the batch it is loading), so that the reader can be closed after this.
     *
     * @throws InterruptedException when interrupted while waiting
     */
    @Override
    public void close() throws InterruptedException {
        closed = true;
        List<Item> left = new ArrayList<>();
        do {
            queue.drainTo(left);
        } while (!done.await(OFFER_POLL_MILLIS, TimeUnit.MILLISECONDS));
        queue.drainTo(left);
        for (Item item : left) {
            if (item.root() != null) {
                item.root().close();
            }
        }
    }
}
