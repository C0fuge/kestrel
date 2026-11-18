package pro.surpin.data.arrowflight.server.adapters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.server.plugin.ConfigSource;

/**
 * Tests the explicit memory budget {@code memoryBudgetBytes} (item 52).
 */
@Tag("unit")
class HostLimitsBudgetTest {

    /**
     * Creates a source from system properties and environment variables.
     *
     * @param sys system properties
     * @param env environment variables
     * @return source
     */
    private static ConfigSource source(Map<String, String> sys, Map<String, String> env) {
        return new ConfigSource(sys::get, new Properties(), env, null);
    }

    /** Verifies the accepted spellings: JVM-style, DuckDB-style and plain bytes. */
    @Test
    void parsesBudgetSpellings() {
        assertEquals(48L << 30, HostLimits.parseBudget("48g"));
        assertEquals(48L << 30, HostLimits.parseBudget("48G"));
        assertEquals(512L << 20, HostLimits.parseBudget("512m"));
        assertEquals(1L << 40, HostLimits.parseBudget("1t"));
        assertEquals(48L << 30, HostLimits.parseBudget("48GiB"));
        assertEquals(50_000_000_000L, HostLimits.parseBudget("50GB"));
        assertEquals(51_539_607_552L, HostLimits.parseBudget("51539607552"));
        assertNull(HostLimits.parseBudget("lots"));
        assertNull(HostLimits.parseBudget("80%"));
        assertNull(HostLimits.parseBudget("99999999999999999999g"));
    }

    /** Verifies the setting, its alias and the environment variable, and that unset means 0. */
    @Test
    void resolvesSettingAndEnvironment() {
        assertEquals(0, HostLimits.memoryBudgetBytes(source(Map.of(), Map.of())));
        assertEquals(48L << 30, HostLimits.memoryBudgetBytes(
                source(Map.of("memoryBudgetBytes", "48g"), Map.of())));
        assertEquals(16L << 30, HostLimits.memoryBudgetBytes(
                source(Map.of("arrowflight.memoryBudgetBytes", "16GiB"), Map.of())));
        assertEquals(96L << 30, HostLimits.memoryBudgetBytes(
                source(Map.of(), Map.of("FLIGHT_MEMORY_BUDGET", "96g"))));
        assertEquals(8L << 30, HostLimits.memoryBudgetBytes(
                source(Map.of("memoryBudgetBytes", "8g"), Map.of("FLIGHT_MEMORY_BUDGET", "96g"))),
                "the system property wins over the environment");
    }

    /** Verifies a malformed or non-positive budget fails the start instead of being ignored. */
    @Test
    void rejectsMalformedBudget() {
        assertThrows(IllegalArgumentException.class, () -> HostLimits.memoryBudgetBytes(
                source(Map.of("memoryBudgetBytes", "lots"), Map.of())));
        assertThrows(IllegalArgumentException.class, () -> HostLimits.memoryBudgetBytes(
                source(Map.of("memoryBudgetBytes", "0"), Map.of())));
        assertThrows(IllegalArgumentException.class, () -> HostLimits.memoryBudgetBytes(
                source(Map.of(), Map.of("FLIGHT_MEMORY_BUDGET", "50%"))));
    }

    /** Verifies the setting overrides both the cgroup limit and the host memory. */
    @Test
    void settingOverridesCgroupAndHost() {
        long host = 754L << 30;
        assertEquals(HostLimits.SOURCE_SETTING, HostLimits.memorySource(48L << 30, host, 0));
        assertEquals(HostLimits.SOURCE_SETTING, HostLimits.memorySource(96L << 30, host, 32L << 30),
                "an explicit budget wins even above a cgroup limit: the operator's choice");
        assertEquals(HostLimits.SOURCE_CGROUP, HostLimits.memorySource(0, host, 32L << 30));
        assertEquals(HostLimits.SOURCE_HOST, HostLimits.memorySource(0, host, 0));
        assertEquals(HostLimits.SOURCE_HOST, HostLimits.memorySource(0, 8L << 30, 16L << 30));
    }

    /** Verifies the warning appears only for a host source above 64 GiB. */
    @Test
    void warnsOnlyForBigHostWithoutBudget() {
        String warning = HostLimits.noBudgetWarning(HostLimits.SOURCE_HOST, 754L << 30);
        assertNotNull(warning);
        assertTrue(warning.contains("memoryBudgetBytes"), warning);
        assertNull(HostLimits.noBudgetWarning(HostLimits.SOURCE_HOST, 64L << 30));
        assertNull(HostLimits.noBudgetWarning(HostLimits.SOURCE_SETTING, 754L << 30));
        assertNull(HostLimits.noBudgetWarning(HostLimits.SOURCE_CGROUP, 754L << 30));
    }

    /**
     * Regression for Q-52a (real cluster 2026-11-16: a server pinned to 16 cores of a 754 GB
     * host without a cgroup memory limit sized DuckDB from the whole machine and was killed at
     * 48 GB): the detected host memory is capped at 4 GiB per available core; a setting or a
     * cgroup limit still wins, and a host below the cap keeps {@code source=host}.
     */
    @Test
    void capsTheHostMemoryPerAvailableCore() {
        long host = 754L << 30;
        assertEquals(64L << 30, HostLimits.hostCap(16));
        assertEquals(4L << 30, HostLimits.hostCap(0));
        assertEquals(HostLimits.SOURCE_HOST_CAPPED, HostLimits.memorySource(0, host, 0, 16));
        assertEquals(HostLimits.SOURCE_HOST, HostLimits.memorySource(0, 64L << 30, 0, 16),
                "exactly at the cap stays host");
        assertEquals(HostLimits.SOURCE_HOST, HostLimits.memorySource(0, 32L << 30, 0, 16));
        assertEquals(HostLimits.SOURCE_SETTING, HostLimits.memorySource(96L << 30, host, 0, 16),
                "an explicit budget wins over the cap");
        assertEquals(HostLimits.SOURCE_CGROUP, HostLimits.memorySource(0, host, 128L << 30, 16),
                "a cgroup limit wins over the cap");
        assertEquals(HostLimits.SOURCE_HOST_CAPPED, HostLimits.memorySource(0, host, 1L << 62, 16),
                "a cgroup limit above the host is none");
        assertNull(HostLimits.noBudgetWarning(HostLimits.SOURCE_HOST_CAPPED, 64L << 30),
                "16 pinned cores: 64 GiB is not warned about");
        String warning = HostLimits.noBudgetWarning(HostLimits.SOURCE_HOST_CAPPED, 320L << 30);
        assertNotNull(warning, "80 unpinned cores still size from 320 GiB: warn");
        assertTrue(warning.startsWith("source=host-capped memoryMiB=327680 "), warning);
    }

    /** Verifies detect() takes the budget as the memory, so the DuckDB sizing follows it. */
    @Test
    void detectUsesBudgetForSizing() {
        HostLimits limits = HostLimits.detect(source(Map.of("memoryBudgetBytes", "48g"), Map.of()));
        assertEquals(48L << 30, limits.memoryBytes());
        HostLimits big = new HostLimits(80, 754L << 30, 8L << 30, 8L << 30);
        HostLimits budgeted = new HostLimits(80, 48L << 30, 8L << 30, 8L << 30);
        DuckDbSizing fromHost = DuckDbSizing.compute(big, false, 4, null, 4);
        DuckDbSizing fromBudget = DuckDbSizing.compute(budgeted, false, 4, null, 4);
        long hostLimit = DuckDbSizing.parseBytes(fromHost.memoryLimit());
        long budgetLimit = DuckDbSizing.parseBytes(fromBudget.memoryLimit());
        assertTrue(budgetLimit * 4 <= (48L << 30), "4 connections stay within the budget: " + budgetLimit);
        assertTrue(budgetLimit < hostLimit / 10, fromHost.memoryLimit() + " vs " + fromBudget.memoryLimit());
    }
}
