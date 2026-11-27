package pro.surpin.data.arrowflight.server.adapters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.Executors;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.server.model.AppConfig;

/** Checks optional CPU counters without changing the JVM's thread-CPU monitoring settings. */
@Tag("unit")
class StreamCpuCountersTest {
    /** Reads supported monotonic counters and reports execute/total deltas in nanoseconds. */
    @Test
    void samplesSupportedCountersWithoutEnablingMonitoring() {
        var process = mock(com.sun.management.OperatingSystemMXBean.class);
        ThreadMXBean caller = mock(ThreadMXBean.class);
        when(process.getProcessCpuTime()).thenReturn(100L, 160L, 230L);
        when(caller.isCurrentThreadCpuTimeSupported()).thenReturn(true);
        when(caller.isThreadCpuTimeEnabled()).thenReturn(true);
        when(caller.getCurrentThreadCpuTime()).thenReturn(20L, 30L, 50L);
        StreamCpuCounters counters = new StreamCpuCounters(process, caller);
        var start = counters.sample();
        assertEquals(new StreamCpuCounters.Snapshot(60, 10), counters.sample().since(start));
        assertEquals(new StreamCpuCounters.Snapshot(130, 30), counters.sample().since(start));
        verify(caller, never()).setThreadCpuTimeEnabled(anyBoolean());
    }

    /** Unsupported or disabled counters remain unknown without reading or enabling thread CPU. */
    @Test
    void unsupportedAndDisabledCountersRemainUnknown() {
        ThreadMXBean caller = mock(ThreadMXBean.class);
        StreamCpuCounters counters = new StreamCpuCounters(mock(OperatingSystemMXBean.class), caller);
        assertEquals(new StreamCpuCounters.Snapshot(-1, -1), counters.sample());
        when(caller.isCurrentThreadCpuTimeSupported()).thenReturn(true);
        when(caller.isThreadCpuTimeEnabled()).thenReturn(false);
        assertEquals(new StreamCpuCounters.Snapshot(-1, -1), counters.sample());
        verify(caller, never()).getCurrentThreadCpuTime();
        verify(caller, never()).setThreadCpuTimeEnabled(anyBoolean());
    }

    /** Counter failures and resets cannot turn optional telemetry into a query failure. */
    @Test
    void failedOrRegressingCountersRemainUnknown() {
        var process = mock(com.sun.management.OperatingSystemMXBean.class);
        ThreadMXBean caller = mock(ThreadMXBean.class);
        when(process.getProcessCpuTime()).thenThrow(new UnsupportedOperationException("unavailable"));
        when(caller.isCurrentThreadCpuTimeSupported()).thenThrow(new SecurityException("unavailable"));
        assertEquals(new StreamCpuCounters.Snapshot(-1, -1), new StreamCpuCounters(process, caller).sample());
        assertEquals(new StreamCpuCounters.Snapshot(-1, -1),
                new StreamCpuCounters.Snapshot(1, -1).since(new StreamCpuCounters.Snapshot(2, 1)));
        assertEquals(new StreamCpuCounters.Snapshot(-1, -1),
                new StreamCpuCounters.Snapshot(1, 1).since(new StreamCpuCounters.Snapshot(-1, -1)));
    }

    /** The disabled native streaming path constructs no CPU sampler and still returns its rows. */
    @Test
    void disabledStreamTimingCreatesNoCpuSampler() throws Exception {
        String previous = System.getProperty("streamTimingLog");
        var io = Executors.newSingleThreadExecutor();
        try {
            System.setProperty("streamTimingLog", "false");
            AppConfig config = new AppConfig(3, 100, 1, 131072, 0, 1, 1,
                    null, false, null, null, null, null, null, false,
                    1048576, 67108864, 60000L, null, null,
                    32010, 5701, 60, 3, 1000, 30000, true, false, 2, null, null,
                    "complete", true, true);
            try (RootAllocator allocator = new RootAllocator(); DuckDbAdapter adapter = new DuckDbAdapter(config, io)) {
                var source = DuckDbAdapter.class.getDeclaredField("streamCpuCounters");
                source.setAccessible(true);
                assertNull(source.get(adapter));
                FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);
                when(listener.isReady()).thenReturn(true);
                adapter.streamSql(allocator, "SELECT range AS id FROM range(3)", listener, true);
                verify(listener).putNext();
            }
        } finally {
            io.shutdownNow();
            if (previous == null) {
                System.clearProperty("streamTimingLog");
            } else {
                System.setProperty("streamTimingLog", previous);
            }
        }
    }
}
