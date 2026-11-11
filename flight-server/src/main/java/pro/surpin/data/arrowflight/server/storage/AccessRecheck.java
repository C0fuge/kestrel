package pro.surpin.data.arrowflight.server.storage;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Bounds how long a revoked read permission stays unseen (item 44 (2)): the server's listing,
 * plan and hadoopfs metadata caches let a repeat query run without any storage call, so the
 * storage's authorization (Ranger on the NameNode) is not asked again until they expire. This
 * asks the storage once per table directory at most every interval; a directory the storage
 * answered for within the interval is not asked again, and one it denied is asked by every
 * query until it answers again.
 */
public final class AccessRecheck {

    /** Upper bound of remembered directories; the map is cleared when it grows past it. */
    private static final int MAX_DIRECTORIES = 100_000;

    /** Asks the storage about one directory. */
    @FunctionalInterface
    public interface Check {

        /**
         * Asks the storage whether a directory is readable.
         *
         * @param relativeDirectory directory below the data directory
         * @return true when the storage answered that it is readable, false when it could not
         *         be asked
         * @throws IOException when the storage denies access
         */
        boolean readable(String relativeDirectory) throws IOException;
    }

    private final long intervalNanos;
    private final LongSupplier nanoTime;
    private final Check check;
    /** Directory to the time of the storage's last positive answer. */
    private final Map<String, Long> lastReadable = new ConcurrentHashMap<>();

    /**
     * Creates a recheck.
     *
     * @param intervalMs how long a positive answer is trusted, in ms (more than 0)
     * @param check asks the storage
     * @param nanoTime monotonic clock
     * @throws IllegalArgumentException when the interval is not positive
     */
    public AccessRecheck(long intervalMs, Check check, LongSupplier nanoTime) {
        if (intervalMs <= 0) {
            throw new IllegalArgumentException("access recheck interval must be positive: " + intervalMs);
        }
        this.intervalNanos = TimeUnit.MILLISECONDS.toNanos(intervalMs);
        this.check = check;
        this.nanoTime = nanoTime;
    }

    /**
     * Creates a recheck with the system's monotonic clock.
     *
     * @param intervalMs how long a positive answer is trusted, in ms (more than 0)
     * @param check asks the storage
     */
    public AccessRecheck(long intervalMs, Check check) {
        this(intervalMs, check, System::nanoTime);
    }

    /**
     * Asks the storage about a directory unless it answered positively within the interval.
     * Concurrent queries of one table may each ask once when the answer is due.
     *
     * @param relativeDirectory table directory below the data directory
     * @return true when the storage was asked
     * @throws IOException when the storage denies access
     */
    public boolean require(String relativeDirectory) throws IOException {
        long now = nanoTime.getAsLong();
        Long last = lastReadable.get(relativeDirectory);
        if (last != null && now - last < intervalNanos) {
            return false;
        }
        boolean readable;
        try {
            readable = check.readable(relativeDirectory);
        } catch (IOException e) {
            lastReadable.remove(relativeDirectory);
            throw e;
        }
        if (readable) {
            if (lastReadable.size() >= MAX_DIRECTORIES) {
                lastReadable.clear();
            }
            lastReadable.put(relativeDirectory, now);
        }
        return true;
    }
}
