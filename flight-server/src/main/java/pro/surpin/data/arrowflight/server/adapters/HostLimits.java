package pro.surpin.data.arrowflight.server.adapters;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cores and memory the server process really has: the container's (cgroup) limits where set,
 * else the machine's, plus the JVM's own heap and direct memory limits.
 *
 * @param cores            processors available to the JVM
 * @param memoryBytes      memory of the container or machine, 0 when unknown
 * @param maxHeapBytes     JVM heap limit ({@code -Xmx})
 * @param maxDirectBytes   JVM direct memory limit
 */
public record HostLimits(int cores, long memoryBytes, long maxHeapBytes, long maxDirectBytes) {

    private static final Logger LOGGER = LoggerFactory.getLogger(HostLimits.class);

    /** Values at or above this in a cgroup limit file mean "no limit" (cgroup v1 writes ~2^63). */
    private static final long NO_CGROUP_LIMIT = 1L << 60;

    /**
     * Reads the limits of the running process.
     *
     * @return host limits
     */
    public static HostLimits detect() {
        long memory = physicalMemory();
        long cgroup = cgroupMemoryLimit(Path.of("/sys/fs/cgroup"));
        if (cgroup > 0 && (memory <= 0 || cgroup < memory)) {
            memory = cgroup;
        }
        return new HostLimits(Runtime.getRuntime().availableProcessors(), memory,
                Runtime.getRuntime().maxMemory(), OutboundBudget.maxDirectMemory());
    }

    /**
     * Returns the memory the JVM reports for the machine or container.
     *
     * @return bytes, 0 when not readable
     */
    static long physicalMemory() {
        try {
            java.lang.management.OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
            if (os instanceof com.sun.management.OperatingSystemMXBean sun) {
                return Math.max(0, sun.getTotalMemorySize());
            }
        } catch (RuntimeException e) {
            LOGGER.debug("Total memory size not readable", e);
        }
        return 0;
    }

    /**
     * Reads the memory limit of the process's cgroup (v2 {@code memory.max}, else v1
     * {@code memory/memory.limit_in_bytes}).
     *
     * @param cgroupRoot mount point of the cgroup file system
     * @return limit in bytes, 0 when there is none or it is not readable
     */
    static long cgroupMemoryLimit(Path cgroupRoot) {
        long v2 = readLimit(cgroupRoot.resolve("memory.max"));
        return v2 > 0 ? v2 : readLimit(cgroupRoot.resolve("memory").resolve("memory.limit_in_bytes"));
    }

    /**
     * Reads one cgroup limit file.
     *
     * @param file limit file
     * @return limit in bytes, 0 for "max", a missing file or a value meaning no limit
     */
    private static long readLimit(Path file) {
        try {
            if (!Files.isReadable(file)) {
                return 0;
            }
            String text = Files.readString(file, StandardCharsets.US_ASCII).trim();
            if (text.isEmpty() || "max".equals(text)) {
                return 0;
            }
            long value = Long.parseLong(text);
            return value <= 0 || value >= NO_CGROUP_LIMIT ? 0 : value;
        } catch (IOException | NumberFormatException e) {
            LOGGER.debug("cgroup limit {} not readable", file, e);
            return 0;
        }
    }
}
