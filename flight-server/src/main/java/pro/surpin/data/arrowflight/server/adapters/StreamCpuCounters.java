package pro.surpin.data.arrowflight.server.adapters;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;

/** Samples best-effort whole-process and current caller-thread CPU counters without enabling them. */
final class StreamCpuCounters {
    private final OperatingSystemMXBean process;
    private final ThreadMXBean caller;

    /** Uses the JVM's existing process and thread management beans. */
    StreamCpuCounters() {
        this(ManagementFactory.getOperatingSystemMXBean(), ManagementFactory.getThreadMXBean());
    }

    /**
     * Uses explicitly supplied CPU sources.
     *
     * @param process process management bean
     * @param caller caller-thread management bean
     */
    StreamCpuCounters(OperatingSystemMXBean process, ThreadMXBean caller) {
        this.process = process;
        this.caller = caller;
    }

    /**
     * Reads available counters without changing JVM CPU-monitoring settings.
     *
     * @return CPU nanoseconds, or -1 for an unavailable counter
     */
    Snapshot sample() {
        long processNanos = -1;
        long callerNanos = -1;
        try {
            if (process instanceof com.sun.management.OperatingSystemMXBean supported) {
                processNanos = supported.getProcessCpuTime();
            }
        } catch (RuntimeException unavailable) {
            processNanos = -1;
        }
        try {
            if (caller.isCurrentThreadCpuTimeSupported() && caller.isThreadCpuTimeEnabled()) {
                callerNanos = caller.getCurrentThreadCpuTime();
            }
        } catch (RuntimeException unavailable) {
            callerNanos = -1;
        }
        return new Snapshot(Math.max(-1, processNanos), Math.max(-1, callerNanos));
    }

    /**
     * One sample or interval of overlapping process and caller-thread CPU counters.
     *
     * @param processNanos whole-process CPU nanoseconds, or -1
     * @param callerThreadNanos caller-thread CPU nanoseconds, or -1
     */
    record Snapshot(long processNanos, long callerThreadNanos) {
        /**
         * Subtracts a start sample, preserving unavailable or regressing counters as unknown.
         *
         * @param start start sample
         * @return CPU deltas in nanoseconds, or -1 per unavailable counter
         */
        Snapshot since(Snapshot start) {
            return new Snapshot(delta(processNanos, start.processNanos()),
                    delta(callerThreadNanos, start.callerThreadNanos()));
        }

        /**
         * Subtracts comparable nonnegative counters.
         *
         * @param end end counter
         * @param start start counter
         * @return nonnegative delta, or -1 when unavailable
         */
        private static long delta(long end, long start) {
            return start >= 0 && end >= start ? end - start : -1;
        }
    }
}
