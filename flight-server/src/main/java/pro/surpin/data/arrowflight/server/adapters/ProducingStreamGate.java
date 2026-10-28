package pro.surpin.data.arrowflight.server.adapters;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import org.apache.arrow.flight.CallStatus;

/**
 * Caps the streams of a node that are inside DuckDB producing a batch at the same time
 * ({@code duckDbProducingStreams}). A stream holds its permit only while DuckDB runs the query or
 * computes its next batch, never while it waits for the client or the outbound budget, so streams
 * a client reads in any order cannot block each other for good. Permits are handed out in arrival
 * order.
 */
public final class ProducingStreamGate {

    /** How often a stream waiting for a permit checks whether its client went away. */
    private static final long CANCEL_POLL_MILLIS = 50;

    /** Gate of a node without the setting: every stream produces at once. */
    public static final ProducingStreamGate OFF = new ProducingStreamGate(0);

    private final int permits;
    private final Semaphore semaphore;
    private final AtomicLong waits = new AtomicLong();
    private final AtomicLong waitNanos = new AtomicLong();

    /**
     * Creates a gate.
     *
     * @param permits streams that may produce at once; 0 or less = no cap
     */
    public ProducingStreamGate(int permits) {
        this.permits = Math.max(0, permits);
        this.semaphore = this.permits > 0 ? new Semaphore(this.permits, true) : null;
    }

    /**
     * Returns the cap.
     *
     * @return streams that may produce at once, 0 = no cap
     */
    public int permits() {
        return permits;
    }

    /**
     * Returns how often a stream had to wait for a permit since start.
     *
     * @return wait count
     */
    public long waits() {
        return waits.get();
    }

    /**
     * Returns the time streams spent waiting for a permit since start.
     *
     * @return nanoseconds
     */
    public long waitNanos() {
        return waitNanos.get();
    }

    /**
     * Opens a pass for one stream; use it on the stream's thread only.
     *
     * @param cancelled whether the stream's client went away
     * @return pass, holding no permit yet
     */
    public Pass pass(BooleanSupplier cancelled) {
        return new Pass(cancelled);
    }

    /** One stream's use of the gate: {@link #enter()} before DuckDB works, {@link #leave()} after. */
    public final class Pass implements AutoCloseable {
        private final BooleanSupplier cancelled;
        private boolean held;

        /**
         * Creates a pass.
         *
         * @param cancelled whether the stream's client went away
         */
        private Pass(BooleanSupplier cancelled) {
            this.cancelled = cancelled;
        }

        /**
         * Takes a permit unless this pass holds one already.
         *
         * @throws InterruptedException if the thread is interrupted while waiting
         * @throws RuntimeException CANCELLED when the client goes away while waiting
         */
        public void enter() throws InterruptedException {
            if (semaphore == null || held) {
                return;
            }
            // A timed try honours the arrival order; the untimed one would barge.
            if (!semaphore.tryAcquire(0, TimeUnit.NANOSECONDS)) {
                long start = System.nanoTime();
                waits.incrementAndGet();
                try {
                    while (!semaphore.tryAcquire(CANCEL_POLL_MILLIS, TimeUnit.MILLISECONDS)) {
                        if (cancelled.getAsBoolean()) {
                            throw CallStatus.CANCELLED
                                    .withDescription("Flight client cancelled while the query waited to run")
                                    .toRuntimeException();
                        }
                    }
                } finally {
                    waitNanos.addAndGet(System.nanoTime() - start);
                }
            }
            held = true;
        }

        /** Gives the permit back, if this pass holds one. */
        public void leave() {
            if (held) {
                held = false;
                semaphore.release();
            }
        }

        /** Gives the permit back, if this pass still holds one. */
        @Override
        public void close() {
            leave();
        }
    }
}
