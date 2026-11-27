import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;

/** Checks CPU sampling against busy Java and unattached native threads. */
public final class CpuProfileProbe {
    private static volatile long sink;

    /** Prevents construction of the command-line probe. */
    private CpuProfileProbe() {
    }

    /**
     * Runs named busy and waiting threads without attaching native workers to Java.
     *
     * @param args native library and fresh report directory
     * @throws Exception when a worker or output fails
     */
    public static void main(String[] args) throws Exception {
        Path output = Path.of(args[1]);
        System.load(args[0]);
        Thread.currentThread().setName("ap-java-busy");
        long mono = System.nanoTime();
        long nativeMono = monotonicClock();
        long after = System.nanoTime();
        if (nativeMono < mono || nativeMono > after) {
            throw new IllegalStateException("Java nanoTime differs from Linux CLOCK_MONOTONIC");
        }
        long epoch = System.currentTimeMillis();
        startNative();
        var mx = ManagementFactory.getThreadMXBean();
        long start = mx.getCurrentThreadCpuTime();
        long until = start + 800_000_000L;
        long value = 1;
        while (mx.getCurrentThreadCpuTime() < until) {
            for (int i = 0; i < 8192; i++) {
                value = value * 6364136223846793005L + 1;
            }
            sink = value;
        }
        long cpu = mx.getCurrentThreadCpuTime() - start;
        finishNative();
        Files.writeString(output.resolve("java-clock.txt"), "monotonic_ns=" + mono
                + "\nnative_monotonic_ns=" + nativeMono + "\nmonotonic_after_ns=" + after
                + "\nepoch_ms=" + epoch + "\njava_busy_cpu_ns=" + cpu + "\n");
    }

    /** Starts native workers that never call AttachCurrentThread. */
    private static native void startNative();

    /** Joins native workers and writes their CPU counters. */
    private static native void finishNative();

    /**
     * Reads the Linux clock used to align raw query phases with host timestamps.
     *
     * @return Linux monotonic nanoseconds
     */
    private static native long monotonicClock();
}
