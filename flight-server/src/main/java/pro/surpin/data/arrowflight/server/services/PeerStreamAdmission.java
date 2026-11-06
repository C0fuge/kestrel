package pro.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.CallStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pro.surpin.data.arrowflight.server.LogUtil;

import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Bounds the endpoint answer streams the distributed complete merges of one node hold open at
 * once (item 35): every merge opens all its endpoint streams together, and each open stream holds
 * batches in direct memory (the endpoint's producer, the read-ahead of the stream being read, the
 * merge's own output). Eight concurrent full scans of four endpoints each exhausted 2 GiB of direct
 * memory and failed with Arrow's memory limit; four passed. A merge therefore takes one permit per
 * endpoint stream (at most all permits, so a merge of more endpoints than permits runs alone)
 * before it opens any stream, and returns them when it ends; a merge that finds too few permits
 * waits for running merges to end, in arrival order, instead of failing. A merge that waits longer
 * than the configured time is refused with {@code RESOURCE_EXHAUSTED}, which a client may retry.
 */
public final class PeerStreamAdmission {

    /** Direct memory one open endpoint stream is charged with when the permits are derived: 128 MiB. */
    public static final long STREAM_BYTES = 128L << 20;
    /** Default longest wait of a merge for permits: five minutes. */
    public static final long DEFAULT_WAIT_MILLIS = 300_000;
    /** Fewest permits a derived budget gives: one merge of two endpoints. */
    static final int MIN_PERMITS = 2;
    /** How often a waiting merge checks whether its client went away. */
    private static final long CHECK_MILLIS = 1000;
    private static final Logger LOGGER = LoggerFactory.getLogger(PeerStreamAdmission.class);

    private final int permits;
    private final long waitMillis;
    private final Object lock = new Object();
    /** Waiting merges in arrival order; guarded by {@link #lock}. */
    private final ArrayDeque<Object> queue = new ArrayDeque<>();
    /** Permits no merge holds; guarded by {@link #lock}. */
    private int free;
    private final AtomicLong waited = new AtomicLong();
    private final AtomicLong waitNanos = new AtomicLong();
    private final AtomicLong refused = new AtomicLong();

    /**
     * Creates the admission of a node.
     *
     * @param permits endpoint streams all merges of the node may hold open together
     * @param waitMillis longest wait of a merge for permits
     * @throws IllegalArgumentException when permits is not positive or waitMillis is negative
     */
    public PeerStreamAdmission(int permits, long waitMillis) {
        if (permits <= 0) {
            throw new IllegalArgumentException("Peer stream permits must be positive: " + permits);
        }
        if (waitMillis < 0) {
            throw new IllegalArgumentException("Admission wait must not be negative: " + waitMillis);
        }
        this.permits = permits;
        this.waitMillis = waitMillis;
        this.free = permits;
    }

    /**
     * Derives the permits from a direct memory limit: one per {@link #STREAM_BYTES}, at least
     * {@link #MIN_PERMITS}.
     *
     * @param directMemoryBytes the JVM's direct memory limit
     * @return permits
     */
    public static int defaultPermits(long directMemoryBytes) {
        return (int) Math.max(MIN_PERMITS, Math.min(Integer.MAX_VALUE, directMemoryBytes / STREAM_BYTES));
    }

    /**
     * Returns the endpoint streams all merges may hold open together.
     *
     * @return permits
     */
    public int permits() {
        return permits;
    }

    /**
     * Returns the permits not held by a merge now.
     *
     * @return free permits
     */
    public int available() {
        synchronized (lock) {
            return free;
        }
    }

    /**
     * Counts the merges that waited for permits since start.
     *
     * @return merges that waited
     */
    public long waited() {
        return waited.get();
    }

    /**
     * Sums the time merges waited for permits since start.
     *
     * @return nanoseconds
     */
    public long waitNanos() {
        return waitNanos.get();
    }

    /**
     * Counts the merges refused after waiting the longest time since start.
     *
     * @return refused merges
     */
    public long refused() {
        return refused.get();
    }

    /**
     * Takes the permits of a merge's endpoint streams, waiting for running merges to end if too
     * few are free.
     *
     * @param streams endpoint streams the merge opens
     * @param cancelled tells whether the merge's client went away
     * @param query client SQL, for the log
     * @return the merge's permits; close it when the merge ends
     * @throws InterruptedException when interrupted while waiting
     * @throws org.apache.arrow.flight.FlightRuntimeException {@code RESOURCE_EXHAUSTED} when no
     *         permits came free in time, {@code CANCELLED} when the client went away while waiting
     */
    public Admitted admit(int streams, BooleanSupplier cancelled, String query) throws InterruptedException {
        int wanted = Math.max(1, Math.min(streams, permits));
        Object me = new Object();
        long start = System.nanoTime();
        boolean logged = false;
        synchronized (lock) {
            queue.addLast(me);
            try {
                long deadline = start + TimeUnit.MILLISECONDS.toNanos(waitMillis);
                while (queue.peekFirst() != me || free < wanted) {
                    if (!logged) {
                        logged = true;
                        waited.incrementAndGet();
                        LOGGER.info("node={} distributedComplete=admissionWait streams={} free={} permits={}"
                                + " waiting={} query='{}'", LogUtil.node(), wanted, free, permits, queue.size(),
                                query);
                    }
                    long left = deadline - System.nanoTime();
                    if (left <= 0) {
                        refused.incrementAndGet();
                        LOGGER.warn("node={} distributedComplete=admissionRefused streams={} permits={} waitedMs={}"
                                + " query='{}'", LogUtil.node(), wanted, permits, waitMillis, query);
                        throw CallStatus.RESOURCE_EXHAUSTED.withDescription("The node merges other complete"
                                + " answers from endpoint answers: no room for " + wanted + " more endpoint streams"
                                + " (of " + permits + ") after waiting " + waitMillis + " ms; retry later")
                                .toRuntimeException();
                    }
                    if (cancelled.getAsBoolean()) {
                        throw CallStatus.CANCELLED.withDescription("The client cancelled while the merge waited"
                                + " for room").toRuntimeException();
                    }
                    long waitFor = Math.min(left, TimeUnit.MILLISECONDS.toNanos(CHECK_MILLIS));
                    TimeUnit.NANOSECONDS.timedWait(lock, Math.max(1, waitFor));
                }
                free -= wanted;
            } finally {
                queue.remove(me);
                // The next in line may fit now, or may be first now that this one left.
                lock.notifyAll();
            }
        }
        if (logged) {
            long waitedNanos = System.nanoTime() - start;
            waitNanos.addAndGet(waitedNanos);
            LOGGER.info("node={} distributedComplete=admitted streams={} waitedMs={}", LogUtil.node(), wanted,
                    waitedNanos / 1_000_000);
        }
        return new Admitted(wanted);
    }

    /**
     * Returns permits a merge held and wakes the waiting merges.
     *
     * @param held permits
     */
    private void release(int held) {
        synchronized (lock) {
            free += held;
            lock.notifyAll();
        }
    }

    /** The permits one merge holds; closing returns them once. */
    public final class Admitted implements AutoCloseable {

        private final int held;
        private final AtomicBoolean released = new AtomicBoolean();

        /**
         * Holds permits.
         *
         * @param held permits taken
         */
        private Admitted(int held) {
            this.held = held;
        }

        /**
         * Returns the permits this merge holds.
         *
         * @return permits
         */
        public int held() {
            return held;
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                release(held);
            }
        }
    }
}
