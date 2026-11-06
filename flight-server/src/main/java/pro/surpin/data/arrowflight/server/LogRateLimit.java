package pro.surpin.data.arrowflight.server;

import java.util.function.LongSupplier;

/**
 * Lets a frequent log line through at most once per interval and counts the ones held back,
 * so the next line can report them (item 36 (4)).
 */
public final class LogRateLimit {

    private final long intervalNanos;
    private final LongSupplier clock;
    private long lastNanos;
    private boolean logged;
    private long suppressed;

    /**
     * Creates a limit on the system clock.
     *
     * @param intervalNanos shortest time between two lines
     */
    public LogRateLimit(long intervalNanos) {
        this(intervalNanos, System::nanoTime);
    }

    /**
     * Creates a limit on the given clock.
     *
     * @param intervalNanos shortest time between two lines
     * @param clock nanosecond clock
     */
    LogRateLimit(long intervalNanos, LongSupplier clock) {
        this.intervalNanos = intervalNanos;
        this.clock = clock;
    }

    /**
     * Tells whether a line may be logged now.
     *
     * @return the number of lines held back since the last one let through, or -1 when this
     *         line is held back too
     */
    public synchronized long tryLog() {
        long now = clock.getAsLong();
        if (logged && now - lastNanos < intervalNanos) {
            suppressed++;
            return -1;
        }
        logged = true;
        lastNanos = now;
        long held = suppressed;
        suppressed = 0;
        return held;
    }
}
