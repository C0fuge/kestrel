package pro.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pro.surpin.data.arrowflight.server.LogUtil;

import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
    /**
     * Bytes a merge admitted on probation reads from its endpoint answers before it takes its
     * permits: 16 MiB (item 43).
     */
    public static final long PROBATION_BYTES = 16L << 20;
    /** Fewest merges a node keeps on probation at once (item 43). */
    static final int MIN_PROBATION = 32;
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
    private final AtomicLong unmetered = new AtomicLong();
    /** Merges on probation now: admitted without permits, not charged yet (item 43). */
    private final AtomicInteger onProbation = new AtomicInteger();
    private final int probationLimit;
    private final long probationBytes;
    private final AtomicLong probation = new AtomicLong();
    private final AtomicLong charged = new AtomicLong();

    /**
     * Creates the admission of a node.
     *
     * @param permits endpoint streams all merges of the node may hold open together
     * @param waitMillis longest wait of a merge for permits
     * @throws IllegalArgumentException when permits is not positive or waitMillis is negative
     */
    public PeerStreamAdmission(int permits, long waitMillis) {
        this(permits, waitMillis, PROBATION_BYTES);
    }

    /**
     * Creates the admission of a node with the bytes a merge on probation may read uncharged.
     *
     * @param permits endpoint streams all merges of the node may hold open together
     * @param waitMillis longest wait of a merge for permits
     * @param probationBytes bytes a merge on probation reads before it takes its permits
     * @throws IllegalArgumentException when permits is not positive, or waitMillis or
     *         probationBytes is negative
     */
    public PeerStreamAdmission(int permits, long waitMillis, long probationBytes) {
        if (probationBytes < 0) {
            throw new IllegalArgumentException("Probation bytes must not be negative: " + probationBytes);
        }
        if (permits <= 0) {
            throw new IllegalArgumentException("Peer stream permits must be positive: " + permits);
        }
        if (waitMillis < 0) {
            throw new IllegalArgumentException("Admission wait must not be negative: " + waitMillis);
        }
        this.permits = permits;
        this.waitMillis = waitMillis;
        this.free = permits;
        this.probationBytes = probationBytes;
        this.probationLimit = (int) Math.max(MIN_PROBATION, Math.min(Integer.MAX_VALUE, 8L * permits));
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
     * Derives the permits from the memory the endpoint streams can really use: the smaller of the
     * JVM's direct memory limit and the limit of the Arrow allocator they are read into (item 39
     * (d): with 8 GiB of direct memory and the default 2 GiB allocator limit, 64 permits let eight
     * concurrent full scans fill the allocator and fail at 2 GiB).
     *
     * @param directMemoryBytes the JVM's direct memory limit
     * @param allocatorLimitBytes effective limit of the allocator of the peer streams
     * @return permits
     */
    public static int defaultPermits(long directMemoryBytes, long allocatorLimitBytes) {
        return defaultPermits(Math.min(directMemoryBytes, allocatorLimitBytes));
    }

    /**
     * Returns the effective limit of an allocator: the smallest limit of it and its parents.
     *
     * @param allocator an allocator
     * @return bytes it can hand out at most
     */
    public static long effectiveLimit(BufferAllocator allocator) {
        long limit = Long.MAX_VALUE;
        for (BufferAllocator a = allocator; a != null; a = a.getParentAllocator()) {
            limit = Math.min(limit, a.getLimit());
        }
        return limit;
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
     * Counts the merges admitted without permits since start (one-row answers, item 39 (d)).
     *
     * @return merges admitted without permits
     */
    public long unmetered() {
        return unmetered.get();
    }

    /**
     * Counts the merges admitted on probation since start (item 43).
     *
     * @return merges admitted on probation
     */
    public long probation() {
        return probation.get();
    }

    /**
     * Counts the merges on probation that read more than the probation bytes and took their
     * permits since start (item 43).
     *
     * @return merges charged after probation
     */
    public long charged() {
        return charged.get();
    }

    /**
     * Returns the merges that may be on probation at once: eight per permit, at least
     * {@link #MIN_PROBATION}.
     *
     * @return merges
     */
    public int probationLimit() {
        return probationLimit;
    }

    /**
     * Admits a merge whose answer is expected to be small (a point lookup: an equality filter, or
     * a small LIMIT) on probation (item 43): it opens its endpoint streams at once, without permits,
     * and takes them through {@link Admitted#charge(long)} only once it has read more than the
     * probation bytes ({@link #PROBATION_BYTES} by default) of endpoint answers. Charging every
     * point lookup like a full scan let one merge of four endpoints hold all four permits of a node, so 61-63 % of the point
     * lookups of 32 clients queued (217 q/s against 1348 without distributed complete). When
     * {@link #probationLimit()} merges are on probation already, this one is admitted as
     * {@link #admit(int, BooleanSupplier, String)} admits it.
     *
     * @param streams endpoint streams the merge opens
     * @param cancelled tells whether the merge's client went away
     * @param query client SQL, for the log
     * @return the merge's admission; close it when the merge ends
     * @throws InterruptedException when interrupted while waiting (only when admitted at once)
     */
    public Admitted admitOnProbation(int streams, BooleanSupplier cancelled, String query)
            throws InterruptedException {
        if (onProbation.incrementAndGet() > probationLimit) {
            onProbation.decrementAndGet();
            return admit(streams, cancelled, query);
        }
        probation.incrementAndGet();
        return new Admitted(streams, cancelled, query);
    }

    /**
     * Admits a merge whose endpoint answers are one row each (an aggregate without GROUP BY and
     * DISTINCT) without permits and without waiting (item 39 (d)): their streams hold one small
     * batch, so charging them like full scans only queued small queries behind the scans (dc
     * small-query p99 8.2 s at 32 clients against 1.0 s for speed alone).
     *
     * @return an admission holding no permits
     */
    public Admitted admitUnmetered() {
        unmetered.incrementAndGet();
        return new Admitted(0);
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
        return new Admitted(take(streams, cancelled, query));
    }

    /**
     * Takes the permits of a merge's endpoint streams, waiting as {@link #admit} describes.
     *
     * @param streams endpoint streams the merge opens
     * @param cancelled tells whether the merge's client went away
     * @param query client SQL, for the log
     * @return permits taken
     * @throws InterruptedException when interrupted while waiting
     */
    private int take(int streams, BooleanSupplier cancelled, String query) throws InterruptedException {
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
        return wanted;
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

    /**
     * The permits one merge holds; closing returns them once. A merge admitted on probation holds
     * none until {@link #charge(long)} takes them.
     */
    public final class Admitted implements AutoCloseable {

        /** Guarded by this. */
        private int held;
        /** Guarded by this. */
        private boolean released;
        /** Endpoint streams to charge once probation ends; 0 when not on probation. */
        private final int streams;
        private final BooleanSupplier cancelled;
        private final String query;
        /** Set once the merge left probation: charged, or closed. */
        private final AtomicBoolean probationOver;

        /**
         * Holds permits.
         *
         * @param held permits taken
         */
        private Admitted(int held) {
            this.held = held;
            this.streams = 0;
            this.cancelled = null;
            this.query = null;
            this.probationOver = new AtomicBoolean(true);
        }

        /**
         * Holds no permits yet: on probation.
         *
         * @param streams endpoint streams to charge once probation ends
         * @param cancelled tells whether the merge's client went away
         * @param query client SQL, for the log
         */
        private Admitted(int streams, BooleanSupplier cancelled, String query) {
            this.held = 0;
            this.streams = streams;
            this.cancelled = cancelled;
            this.query = query;
            this.probationOver = new AtomicBoolean(false);
        }

        /**
         * Returns the permits this merge holds.
         *
         * @return permits
         */
        public synchronized int held() {
            return held;
        }

        /**
         * Tells whether this merge is on probation: admitted without permits and not charged.
         *
         * @return true while on probation
         */
        public boolean onProbation() {
            return !probationOver.get();
        }

        /**
         * Ends the probation of a merge once it has read more than the probation bytes: takes
         * its permits, waiting as {@link PeerStreamAdmission#admit} waits. Does nothing for a merge
         * not on probation or while it has read less.
         *
         * @param bytesRead bytes of endpoint answers the merge has read so far
         * @throws InterruptedException when interrupted while waiting
         * @throws org.apache.arrow.flight.FlightRuntimeException {@code RESOURCE_EXHAUSTED} or
         *         {@code CANCELLED} as {@link PeerStreamAdmission#admit} throws them
         */
        public void charge(long bytesRead) throws InterruptedException {
            if (bytesRead <= probationBytes || probationOver.get() || !probationOver.compareAndSet(false, true)) {
                return;
            }
            onProbation.decrementAndGet();
            charged.incrementAndGet();
            LOGGER.info("node={} distributedComplete=probationOver bytesRead={} streams={} query='{}'",
                    LogUtil.node(), bytesRead, streams, query);
            int taken = take(streams, cancelled, query);
            synchronized (this) {
                if (released) {
                    release(taken);
                } else {
                    held = taken;
                }
            }
        }

        @Override
        public void close() {
            if (probationOver.compareAndSet(false, true)) {
                onProbation.decrementAndGet();
            }
            synchronized (this) {
                if (!released) {
                    released = true;
                    if (held > 0) {
                        release(held);
                    }
                }
            }
        }
    }
}
