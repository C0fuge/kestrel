package pro.surpin.data.arrowflight.server.adapters;

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests the outbound byte budget shared by all Flight streams. */
class OutboundBudgetTest {

    private static final int MIB = 1 << 20;
    private static final int REQUESTED = 256 * MIB;
    /** Direct memory limit of the probe server JVM. */
    private static final String PROBE_DIRECT_MEMORY = "256m";

    /** Verifies each stream gets budget / open streams, capped by the requested threshold. */
    @Tag("unit")
    @Test
    void streamsShareTheBudget() {
        OutboundBudget budget = new OutboundBudget(64L * MIB);
        ServerCall<Object, Object> first = streamingCall();
        ServerCall<Object, Object> second = streamingCall();

        ServerCall.Listener<Object> firstListener = start(budget, first, REQUESTED);
        verify(first).setOnReadyThreshold(64 * MIB);
        start(budget, second, REQUESTED);
        verify(first).setOnReadyThreshold(32 * MIB);
        verify(second).setOnReadyThreshold(32 * MIB);
        assertEquals(2, budget.openStreams());

        firstListener.onComplete();
        assertEquals(1, budget.openStreams());
        verify(second).setOnReadyThreshold(64 * MIB);
    }

    /** Verifies a stream never queues more than Flight asked for, nor less than the minimum. */
    @Tag("unit")
    @Test
    void shareIsBoundedByRequestAndMinimum() {
        OutboundBudget budget = new OutboundBudget(2L * MIB);
        ServerCall<Object, Object> small = streamingCall();
        start(budget, small, MIB / 2);
        verify(small).setOnReadyThreshold(MIB / 2);

        List<ServerCall.Listener<Object>> listeners = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            listeners.add(start(budget, streamingCall(), REQUESTED));
        }
        assertEquals(OutboundBudget.MIN_STREAM_BYTES, budget.streamShare());
        listeners.forEach(ServerCall.Listener::onCancel);
        assertEquals(1, budget.openStreams());
    }

    /** Verifies calls with one response pass through without a share. */
    @Tag("unit")
    @Test
    @SuppressWarnings("unchecked")
    void unaryCallsAreNotBudgeted() {
        OutboundBudget budget = new OutboundBudget(64L * MIB);
        ServerCall<Object, Object> call = call(MethodDescriptor.MethodType.UNARY);
        ServerCallHandler<Object, Object> next = mock(ServerCallHandler.class);
        ServerCall.Listener<Object> listener = mock(ServerCall.Listener.class);
        when(next.startCall(any(), any())).thenReturn(listener);

        assertSame(listener, budget.interceptCall(call, new Metadata(), next));
        verify(next).startCall(org.mockito.ArgumentMatchers.same(call), any(Metadata.class));
        assertEquals(0, budget.openStreams());
    }

    /** Verifies a stream whose start fails gives its share back. */
    @Tag("unit")
    @Test
    @SuppressWarnings("unchecked")
    void failedStartLeavesTheBudget() {
        OutboundBudget budget = new OutboundBudget(64L * MIB);
        ServerCallHandler<Object, Object> next = mock(ServerCallHandler.class);
        when(next.startCall(any(), any())).thenAnswer(invocation -> {
            ServerCall<Object, Object> wrapped = invocation.getArgument(0);
            wrapped.setOnReadyThreshold(REQUESTED);
            throw new IllegalStateException("boom");
        });

        assertThrows(IllegalStateException.class,
                () -> budget.interceptCall(streamingCall(), new Metadata(), next));
        assertEquals(0, budget.openStreams());
    }

    /** Verifies a stream that never asks for a threshold keeps gRPC's default. */
    @Tag("unit")
    @Test
    void streamWithoutThresholdIsLeftAlone() {
        OutboundBudget budget = new OutboundBudget(64L * MIB);
        ServerCall<Object, Object> call = streamingCall();
        start(budget, call, 0);
        verify(call, never()).setOnReadyThreshold(anyInt());
        assertEquals(0, budget.openStreams());
    }

    /** Verifies a non-positive budget is refused and the default is at least one share. */
    @Tag("unit")
    @Test
    void budgetIsPositive() {
        assertThrows(IllegalArgumentException.class, () -> new OutboundBudget(0));
        assertTrue(OutboundBudget.defaultBudgetBytes() >= OutboundBudget.MIN_STREAM_BYTES);
        assertTrue(OutboundBudget.maxDirectMemory() > 0);
    }

    /**
     * Reproduces the stand failure (item 0f): many concurrent streams to slow clients with the
     * 256 MiB per-stream threshold exhaust a small direct memory ("CANCELLED: Server
     * sendMessage() failed with Error"); with the budget every stream delivers all rows.
     *
     * @throws Exception on process failure
     */
    @Tag("integration")
    @Test
    void manyStreamsStayWithinSmallDirectMemory() throws Exception {
        int streams = 16;
        int batches = 48;
        long expectedRows = (long) batches * OutboundBudgetProbe.ROWS_PER_BATCH;

        ProbeRun unbounded = runProbe(streams, batches, 0);
        assertTrue(unbounded.results.stream().anyMatch(r -> !r.equals(String.valueOf(expectedRows))),
                "without a budget the probe should exhaust direct memory: " + unbounded);

        ProbeRun bounded = runProbe(streams, batches, 16L * MIB);
        for (String result : bounded.results) {
            assertEquals(String.valueOf(expectedRows), result, bounded.toString());
        }
    }

    /**
     * Starts a probe server JVM with a small direct memory limit and reads from it.
     *
     * @param streams concurrent streams
     * @param batches batches per stream
     * @param budget outbound budget, 0 for none
     * @return per-stream results and the server's output
     * @throws Exception on process failure
     */
    private static ProbeRun runProbe(int streams, int batches, long budget) throws Exception {
        String java = System.getProperty("java.home") + File.separator + "bin" + File.separator
                + "java";
        Process server = new ProcessBuilder(java, "-XX:MaxDirectMemorySize=" + PROBE_DIRECT_MEMORY,
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "-cp", System.getProperty("java.class.path"),
                OutboundBudgetProbe.class.getName(), String.valueOf(batches),
                String.valueOf(budget), String.valueOf(REQUESTED))
                .redirectErrorStream(true).start();
        try {
            BufferedReader out = new BufferedReader(
                    new InputStreamReader(server.getInputStream(), StandardCharsets.UTF_8));
            StringBuilder log = new StringBuilder();
            int port = -1;
            String line;
            while (port < 0 && (line = out.readLine()) != null) {
                if (line.startsWith("PORT ")) {
                    port = Integer.parseInt(line.substring(5).trim());
                } else {
                    log.append(line).append('\n');
                }
            }
            assertTrue(port > 0, "probe server did not start: " + log);
            List<String> results = OutboundBudgetProbe.readAll(port, streams);
            server.getOutputStream().close();
            String peak = "";
            while ((line = out.readLine()) != null) {
                if (line.startsWith("PEAK ")) {
                    peak = line.substring(5).trim();
                }
            }
            assertTrue(server.waitFor(1, TimeUnit.MINUTES), "probe server did not stop");
            return new ProbeRun(results, peak);
        } finally {
            server.destroyForcibly();
        }
    }

    /**
     * Outcome of one probe run.
     *
     * @param results rows per stream, or the stream's failure
     * @param peak peak direct memory of the server in bytes
     */
    private record ProbeRun(List<String> results, String peak) {
    }

    /**
     * Starts a streaming call through the budget; the next handler asks for a threshold first,
     * as Flight's own interceptor does.
     *
     * @param budget budget under test
     * @param call gRPC call
     * @param requested threshold to ask for, 0 for none
     * @return the listener the budget returns
     */
    @SuppressWarnings("unchecked")
    private static ServerCall.Listener<Object> start(OutboundBudget budget,
            ServerCall<Object, Object> call, int requested) {
        ServerCallHandler<Object, Object> next = mock(ServerCallHandler.class);
        when(next.startCall(any(), any())).thenAnswer(invocation -> {
            ServerCall<Object, Object> wrapped = invocation.getArgument(0);
            if (requested > 0) {
                wrapped.setOnReadyThreshold(requested);
            }
            return mock(ServerCall.Listener.class);
        });
        return budget.interceptCall(call, new Metadata(), next);
    }

    /**
     * Mocks a server-streaming call.
     *
     * @return call
     */
    private static ServerCall<Object, Object> streamingCall() {
        return call(MethodDescriptor.MethodType.SERVER_STREAMING);
    }

    /**
     * Mocks a call of a method type.
     *
     * @param type method type
     * @return call
     */
    @SuppressWarnings("unchecked")
    private static ServerCall<Object, Object> call(MethodDescriptor.MethodType type) {
        ServerCall<Object, Object> call = mock(ServerCall.class);
        MethodDescriptor<Object, Object> method = MethodDescriptor.newBuilder()
                .setType(type)
                .setFullMethodName("test/method")
                .setRequestMarshaller(mock(MethodDescriptor.Marshaller.class))
                .setResponseMarshaller(mock(MethodDescriptor.Marshaller.class))
                .build();
        when(call.getMethodDescriptor()).thenReturn(method);
        return call;
    }
}
