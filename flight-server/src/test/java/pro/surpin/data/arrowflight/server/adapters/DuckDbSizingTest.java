package pro.surpin.data.arrowflight.server.adapters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests the DuckDB threads and memory defaults across host profiles (item 19b). */
@Tag("unit")
class DuckDbSizingTest {

    private static final long GIB = 1L << 30;

    /**
     * Returns the memory limit of a sizing in bytes.
     *
     * @param sizing sizing with a MiB limit
     * @return bytes
     */
    private static long limitBytes(DuckDbSizing sizing) {
        return DuckDbSizing.parseBytes(sizing.memoryLimit());
    }

    /** The screening container: 2 CPUs, 6 GB, -Xmx3g, direct 2g - 4 threads as before 19, 1 GiB floor. */
    @Test
    void smallContainerKeepsFourThreadsAndTheOneGibFloor() {
        DuckDbSizing s = DuckDbSizing.compute(new HostLimits(2, 6 * GIB, 3 * GIB, 2 * GIB),
                false, 0, null, 0);
        assertEquals(4, s.threads());
        assertEquals(2, s.connections());
        assertEquals("1024MiB", s.memoryLimit());
        DuckDbSizing s3 = DuckDbSizing.compute(new HostLimits(2, 6 * GIB, 3 * GIB, 2 * GIB),
                true, 0, null, 0);
        assertEquals(4, s3.threads());
    }

    /**
     * Regression for the laptop stand p19b (2026-10-28): under container limits 4c/8g, 8c/16g and
     * 16c/24g (-Xmx3g, direct 2g) item 19b chose 2/2/3 threads and TPC-H SF10 ran 1.35-1.7x slower
     * than with 4; the thread floor is 4 and the memory limits stay the measured 1024/2201/3942 MiB.
     */
    @Test
    void standContainersGetFourThreadsAndKeepTheirMemoryLimits() {
        DuckDbSizing c4 = DuckDbSizing.compute(new HostLimits(4, 8 * GIB, 3 * GIB, 2 * GIB),
                false, 0, null, 0);
        assertEquals(4, c4.threads(), c4.describe());
        assertEquals("1024MiB", c4.memoryLimit());
        assertEquals(2, c4.connections());
        DuckDbSizing c8 = DuckDbSizing.compute(new HostLimits(8, 16 * GIB, 3 * GIB, 2 * GIB),
                false, 0, null, 0);
        assertEquals(4, c8.threads(), c8.describe());
        assertEquals("2201MiB", c8.memoryLimit());
        assertEquals(4, c8.connections());
        DuckDbSizing c16 = DuckDbSizing.compute(new HostLimits(16, 24 * GIB, 3 * GIB, 2 * GIB),
                false, 0, null, 0);
        assertEquals(4, c16.threads(), c16.describe());
        assertEquals("3942MiB", c16.memoryLimit());
        assertEquals(4, c16.connections());
    }

    @Test
    void externalFileCacheLineNamesTheBoundOrWarnsWithoutOne() {
        DuckDbSizing sized = DuckDbSizing.compute(new HostLimits(2, 6 * GIB, 3 * GIB, 2 * GIB),
                false, 0, null, 0);
        assertFalse(sized.externalFileCacheUnbounded());
        assertEquals("perConnection=1024MiB expectedConnections=2 expectedMiB=2048 memoryMiB=6144"
                + " bound=memoryLimitTimesLiveConnections", sized.describeExternalFileCache());
        DuckDbSizing none = DuckDbSizing.compute(new HostLimits(2, 6 * GIB, 3 * GIB, 2 * GIB),
                false, 0, DuckDbSizing.MEMORY_LIMIT_NONE, 0);
        assertTrue(none.externalFileCacheUnbounded());
        assertTrue(none.describeExternalFileCache().startsWith("perConnection=duckdbDefault"),
                none.describeExternalFileCache());
        DuckDbSizing percent = DuckDbSizing.compute(new HostLimits(2, 6 * GIB, 3 * GIB, 2 * GIB),
                false, 0, "50%", 3);
        assertFalse(percent.externalFileCacheUnbounded());
        assertTrue(percent.describeExternalFileCache().contains(" expectedMiB=unknown "),
                percent.describeExternalFileCache());
    }

    @Test
    void mediumHostSplitsTheBudget() {
        DuckDbSizing s = DuckDbSizing.compute(new HostLimits(4, 16 * GIB, 4 * GIB, 4 * GIB),
                false, 0, null, 0);
        assertEquals(4, s.threads());
        assertEquals(2, s.connections());
        // 16 - 4 - 4 - 2.4 = 5.6 GiB over 2
        long limit = limitBytes(s);
        assertTrue(limit > 2 * GIB && limit < 3 * GIB, s.describe());
    }

    @Test
    void largeHostsGetMoreThreadsThanTheOldCapOfEight() {
        DuckDbSizing s16 = DuckDbSizing.compute(new HostLimits(16, 64 * GIB, 16 * GIB, 4 * GIB),
                false, 0, null, 0);
        assertEquals(4, s16.threads());
        assertEquals(4, s16.connections());
        DuckDbSizing s64 = DuckDbSizing.compute(new HostLimits(64, 512 * GIB, 32 * GIB, 8 * GIB),
                false, 0, null, 0);
        assertEquals(16, s64.threads());
        assertEquals(4, s64.connections());
        assertTrue(limitBytes(s64) > 90 * GIB, s64.describe());
    }

    @Test
    void manyCoresWithLittleMemoryAreCappedByMemory() {
        DuckDbSizing s = DuckDbSizing.compute(new HostLimits(64, 24 * GIB, 4 * GIB, 2 * GIB),
                false, 0, null, 0);
        // 24 - 4 - 2 - 3.6 = 14.4 GiB over 4 connections = 3.6 GiB -> 3 threads, floor 4
        assertEquals(16, s.coreThreads());
        assertEquals(4, s.threads());
        DuckDbSizing roomy = DuckDbSizing.compute(new HostLimits(64, 96 * GIB, 8 * GIB, 4 * GIB),
                false, 0, null, 0);
        // 96 - 8 - 4 - 14.4 = 69.6 GiB over 4 = 17.4 GiB -> 16 threads
        assertEquals(16, roomy.threads());
        DuckDbSizing mid = DuckDbSizing.compute(new HostLimits(64, 48 * GIB, 8 * GIB, 4 * GIB),
                false, 0, null, 0);
        // 48 - 8 - 4 - 7.2 = 28.8 GiB over 4 = 7.2 GiB -> 7 threads
        assertEquals(7, mid.threads());
        DuckDbSizing tiny = DuckDbSizing.compute(new HostLimits(64, 8 * GIB, 4 * GIB, 2 * GIB),
                false, 0, null, 0);
        assertEquals(4, tiny.threads());
        assertEquals("1024MiB", tiny.memoryLimit());
    }

    @Test
    void theDefaultLimitNeverExceedsDuckDbsOwnShare() {
        DuckDbSizing s = DuckDbSizing.compute(new HostLimits(1, GIB, GIB / 4, GIB / 4), false, 0, null, 0);
        assertTrue(limitBytes(s) <= GIB * 8 / 10, s.describe());
    }

    @Test
    void explicitValuesWin() {
        HostLimits host = new HostLimits(64, 512 * GIB, 32 * GIB, 8 * GIB);
        assertEquals(3, DuckDbSizing.compute(host, false, 3, null, 0).threads());
        DuckDbSizing limited = DuckDbSizing.compute(host, false, 0, "4GB", 0);
        assertEquals("4GB", limited.memoryLimit());
        assertEquals(4, limited.threads());
        assertEquals(4, DuckDbSizing.compute(host, false, 0, "2GB", 0).threads());
        assertEquals(10, DuckDbSizing.compute(host, false, 0, "10GiB", 0).threads());
        DuckDbSizing none = DuckDbSizing.compute(host, false, 0, "none", 0);
        assertNull(none.memoryLimit());
        assertEquals(16, none.threads());
        DuckDbSizing percent = DuckDbSizing.compute(host, false, 0, "50%", 0);
        assertEquals("50%", percent.memoryLimit());
        assertEquals(16, percent.threads());
        assertEquals(64, DuckDbSizing.compute(host, false, 0, null, 64).connections());
    }

    @Test
    void unknownMemoryLeavesDuckDbsDefault() {
        DuckDbSizing s = DuckDbSizing.compute(new HostLimits(8, 0, GIB, GIB), false, 0, null, 0);
        assertNull(s.memoryLimit());
        assertEquals(4, s.threads());
    }

    @Test
    void parsesDuckDbSizes() {
        assertEquals(4_000_000_000L, DuckDbSizing.parseBytes("4GB"));
        assertEquals(512L << 20, DuckDbSizing.parseBytes("512 MiB"));
        assertEquals(1536L << 20, DuckDbSizing.parseBytes("1.5gib"));
        assertEquals(1024L, DuckDbSizing.parseBytes("1024"));
        assertNull(DuckDbSizing.parseBytes("80%"));
    }

    @Test
    void readsCgroupLimits(@TempDir Path dir) throws IOException {
        assertEquals(0, HostLimits.cgroupMemoryLimit(dir));
        Files.writeString(dir.resolve("memory.max"), "max\n");
        assertEquals(0, HostLimits.cgroupMemoryLimit(dir));
        Files.createDirectories(dir.resolve("memory"));
        Files.writeString(dir.resolve("memory").resolve("memory.limit_in_bytes"), "9223372036854771712\n");
        assertEquals(0, HostLimits.cgroupMemoryLimit(dir));
        Files.writeString(dir.resolve("memory").resolve("memory.limit_in_bytes"), "4294967296\n");
        assertEquals(4L << 30, HostLimits.cgroupMemoryLimit(dir));
        Files.writeString(dir.resolve("memory.max"), "6442450944\n");
        assertEquals(6L << 30, HostLimits.cgroupMemoryLimit(dir));
    }

    @Test
    void detectsThisHost() {
        HostLimits host = HostLimits.detect();
        assertTrue(host.cores() >= 1);
        assertTrue(host.memoryBytes() > 0);
        assertTrue(host.maxHeapBytes() > 0);
    }
}
