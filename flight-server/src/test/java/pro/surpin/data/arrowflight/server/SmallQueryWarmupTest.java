package pro.surpin.data.arrowflight.server;

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.adapters.ProducingStreamGate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/** Checks opt-in warmup placement, configuration and raw failure evidence without benchmarking. */
@Tag("unit")
class SmallQueryWarmupTest {
    /** Verifies the historical eight-client warmup, automatic duration and measured ordering. */
    @Test
    void defaultsKeepTheHistoricalPlan() {
        var plan = SmallQueryThroughputPerfTest.runPlan(new Properties(), 15);
        assertEquals("legacy", plan.warmupMode());
        assertEquals(0, plan.configuredWarmupSeconds());
        assertEquals(List.of(
                new SmallQueryThroughputPerfTest.RunStep(8, 5, false, "warmup-c8"),
                new SmallQueryThroughputPerfTest.RunStep(1, 15, true, "c1"),
                new SmallQueryThroughputPerfTest.RunStep(8, 15, true, "c8"),
                new SmallQueryThroughputPerfTest.RunStep(32, 15, true, "c32")), plan.steps());
        assertEquals(3, SmallQueryThroughputPerfTest.runPlan(new Properties(), 3).steps().getFirst().seconds());
        assertEquals(20, SmallQueryThroughputPerfTest.runPlan(new Properties(), 60).steps().getFirst().seconds());
        assertThrows(UnsupportedOperationException.class, () -> plan.steps().clear());
    }

    /** Verifies one same-concurrency warmup before each measured step with distinct raw labels. */
    @Test
    void perStepPlanWarmsEachMeasuredConcurrency() {
        Properties settings = settings("per-step", "12");
        settings.setProperty("perf.clients", "8,32");
        assertEquals(List.of(
                new SmallQueryThroughputPerfTest.RunStep(8, 12, false, "warmup-step-1-c8"),
                new SmallQueryThroughputPerfTest.RunStep(8, 30, true, "c8"),
                new SmallQueryThroughputPerfTest.RunStep(32, 12, false, "warmup-step-2-c32"),
                new SmallQueryThroughputPerfTest.RunStep(32, 30, true, "c32")),
                SmallQueryThroughputPerfTest.runPlan(settings, 30).steps());
        settings.setProperty("perf.warmupSeconds", "0");
        assertEquals(10, SmallQueryThroughputPerfTest.runPlan(settings, 30).steps().getFirst().seconds());
    }

    /** Verifies bounded explicit duration overrides in both placement modes. */
    @Test
    void durationBoundariesAreAccepted() {
        for (String mode : List.of("legacy", "per-step")) {
            for (int seconds : List.of(1, 120)) {
                var plan = SmallQueryThroughputPerfTest.runPlan(settings(mode, Integer.toString(seconds)), 15);
                assertEquals(seconds, plan.configuredWarmupSeconds());
                for (var step : plan.steps()) {
                    assertEquals(step.measured() ? 15 : seconds, step.seconds());
                }
            }
        }
    }

    /** Verifies malformed and out-of-range warmup properties fail clearly. */
    @Test
    void invalidConfigurationIsRejected() {
        for (String value : List.of("-1", "121", "", "1.5", "later", "8 -Dperf.seconds=60")) {
            var error = assertThrows(IllegalArgumentException.class,
                    () -> SmallQueryThroughputPerfTest.runPlan(settings("legacy", value), 15));
            assertTrue(error.getMessage().contains("perf.warmupSeconds"));
        }
        for (String value : List.of("", "each", "PER_STEP")) {
            var error = assertThrows(IllegalArgumentException.class,
                    () -> SmallQueryThroughputPerfTest.runPlan(settings(value, "0"), 15));
            assertTrue(error.getMessage().contains("perf.warmupMode"));
        }
    }

    /**
     * Records the resolved sequence once and refuses to overwrite it.
     *
     * @param output raw output directory
     * @throws Exception when artifact reading fails
     */
    @Test
    void chosenPlanIsExplicitAndImmutable(@TempDir Path output) throws Exception {
        Properties settings = settings("per-step", "7");
        settings.setProperty("perf.clients", "8,32");
        var plan = SmallQueryThroughputPerfTest.runPlan(settings, 15);
        SmallQueryThroughputPerfTest.writeRunPlan(plan, output);
        String expected = "warmup_mode,configured_warmup_seconds,step,label,clients,seconds,measured\n"
                + "per-step,7,1,warmup-step-1-c8,8,7,false\n"
                + "per-step,7,2,c8,8,15,true\n"
                + "per-step,7,3,warmup-step-2-c32,32,7,false\n"
                + "per-step,7,4,c32,32,15,true\n";
        assertEquals(expected, Files.readString(output.resolve("warmup-plan.csv")));
        assertThrows(FileAlreadyExistsException.class, () -> SmallQueryThroughputPerfTest.writeRunPlan(plan, output));
        assertEquals(expected, Files.readString(output.resolve("warmup-plan.csv")));
    }

    /**
     * Retains existing timing artifacts while preserving cleanup's partial CSV.
     *
     * @param output raw output directory
     * @throws Exception when artifact setup or reading fails
     */
    @Test
    void warmupTimingArtifactsCannotBeOverwritten(@TempDir Path output) throws Exception {
        for (String suffix : List.of("wall", "gate")) {
            Path directory = Files.createDirectory(output.resolve(suffix));
            Path retained = directory.resolve("warmup-c1-" + suffix + ".txt");
            Files.writeString(retained, "retained timing evidence");
            String previous = System.setProperty("perf.output", directory.toString());
            try {
                assertThrows(FileAlreadyExistsException.class, () -> SmallQueryThroughputPerfTest.run(
                        mock(FlightSqlClient.class), new HeaderCallOption(new FlightCallHeaders()),
                        true, 1, 0, false, new ProducingStreamGate(0)));
            } finally {
                if (previous == null) {
                    System.clearProperty("perf.output");
                } else {
                    System.setProperty("perf.output", previous);
                }
            }
            assertEquals("retained timing evidence", Files.readString(retained));
            assertTrue(Files.readString(directory.resolve("warmup-c1.csv")).startsWith("thread,sequence,"));
        }
    }

    /**
     * Preserves a failing per-step warmup and aborts before any measured or later step.
     *
     * @param output raw output directory
     * @throws Exception when fixture setup or artifact reading fails
     */
    @Test
    void warmupFailureRetainsItsOwnLabelAndStopsThePlan(@TempDir Path output) throws Exception {
        FlightSqlClient client = mock(FlightSqlClient.class);
        when(client.execute(anyString(), any(CallOption.class)))
                .thenThrow(new IllegalStateException("test-only warmup failure"));
        Map<String, String> changes = Map.of("perf.output", output.toString(), "perf.client", "statement",
                "perf.seconds", "3", "perf.clients", "1,2", "perf.warmupMode", "per-step", "perf.warmupSeconds", "1");
        Properties previous = new Properties();
        for (var entry : changes.entrySet()) {
            String value = System.setProperty(entry.getKey(), entry.getValue());
            if (value != null) {
                previous.setProperty(entry.getKey(), value);
            }
        }
        try {
            var failure = assertThrows(ExecutionException.class,
                    () -> SmallQueryThroughputPerfTest.measureAll(client, "complete", "test-only", null));
            assertEquals("test-only warmup failure", failure.getCause().getMessage());
        } finally {
            for (String key : changes.keySet()) {
                if (previous.containsKey(key)) {
                    System.setProperty(key, previous.getProperty(key));
                } else {
                    System.clearProperty(key);
                }
            }
        }
        verify(client, times(1)).execute(anyString(), any(CallOption.class));
        List<String> rows = Files.readAllLines(output.resolve("warmup-step-1-c1.csv"));
        assertEquals(2, rows.size());
        assertTrue(rows.getLast().endsWith(",0,IllegalStateException"));
        assertTrue(Files.exists(output.resolve("warmup-plan.csv")));
        assertFalse(Files.exists(output.resolve("warmup-c8.csv")));
        assertFalse(Files.exists(output.resolve("c1.csv")));
        assertFalse(Files.exists(output.resolve("warmup-step-2-c2.csv")));
    }

    /**
     * Creates an isolated set of warmup properties.
     *
     * @param mode warmup placement
     * @param seconds configured duration
     * @return test-only harness properties
     */
    private static Properties settings(String mode, String seconds) {
        Properties settings = new Properties();
        settings.setProperty("perf.warmupMode", mode);
        settings.setProperty("perf.warmupSeconds", seconds);
        return settings;
    }
}
