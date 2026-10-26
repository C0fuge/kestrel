package pro.surpin.data.arrowflight.server;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

class HadoopArrowFlightServerUnitTest {

    /** Verifies the gRPC builder gets the keepalive permit and the outbound budget. */
    @Test
    void grpcOptionsInstallTheOutboundBudget() {
        pro.surpin.data.arrowflight.server.adapters.OutboundBudget budget =
                new pro.surpin.data.arrowflight.server.adapters.OutboundBudget(64L << 20);
        io.grpc.netty.NettyServerBuilder builder = io.grpc.netty.NettyServerBuilder.forPort(0);
        io.grpc.netty.NettyServerBuilder spy = org.mockito.Mockito.spy(builder);

        HadoopArrowFlightServer.grpcOptions(budget).accept(spy);

        org.mockito.Mockito.verify(spy).intercept(budget);
        org.mockito.Mockito.verify(spy).permitKeepAliveWithoutCalls(true);
    }

    /** Verifies the outbound budget is the configured one, else derived from direct memory. */
    @Test
    void outboundBudgetIsConfiguredOrDerived() {
        pro.surpin.data.arrowflight.server.model.AppConfig config =
                pro.surpin.data.arrowflight.server.adapters.ConfigAdapter.getConfig();
        assertEquals(pro.surpin.data.arrowflight.server.adapters.OutboundBudget.defaultBudgetBytes(),
                HadoopArrowFlightServer.outboundBudget(config).budgetBytes());
        assertEquals(64L << 20, HadoopArrowFlightServer.outboundBudget(
                config.withFlightOutboundBudget(64L << 20)).budgetBytes());
    }

    // ── getArgValue (private, non-static — tested via instance) ───────────

    @Test
    void getArgValueReturnsValueWhenKeyPresent() throws Exception {
        String[] args = new String[]{"--port", "8080", "--hosts", "127.0.0.1"};
        String result = invokeGetArgValue(args, "--port", "32010");
        assertEquals("8080", result);
    }

    @Test
    void getArgValueReturnsDefaultWhenKeyNotPresent() throws Exception {
        String[] args = new String[]{"--hosts", "127.0.0.1"};
        String result = invokeGetArgValue(args, "--port", "32010");
        assertEquals("32010", result);
    }

    @Test
    void getArgValueReturnsDefaultForEmptyArgs() throws Exception {
        String[] args = new String[0];
        String result = invokeGetArgValue(args, "--port", "32010");
        assertEquals("32010", result);
    }

    @Test
    void getArgValueReturnsDefaultWhenKeyLast() throws Exception {
        String[] args = new String[]{"--port"};
        String result = invokeGetArgValue(args, "--port", "32010");
        assertEquals("32010", result);
    }

    @Test
    void getArgValueReturnsDefaultForUnknownKey() throws Exception {
        String[] args = new String[]{"--not", "mykey", "--port", "32010"};
        String result = invokeGetArgValue(args, "--mykey", "default");
        assertEquals("default", result);
    }

    @Test
    void getArgValueHandlesMultipleKeys() throws Exception {
        String[] args = new String[]{"--data-dir", "/tmp/data", "--port", "8080"};
        assertEquals("/tmp/data", invokeGetArgValue(args, "--data-dir", "/default"));
        assertEquals("8080", invokeGetArgValue(args, "--port", "32010"));
    }

    // ── startOrReport: a failed start must end the process ───────────────

    /** Regression: a failed start before serving (cluster join timeout) must not leave a live process. */
    @Test
    void startOrReportReportsAFailedStart() {
        assertFalse(HadoopArrowFlightServer.startOrReport(() -> {
            throw new IllegalStateException("Cluster join timeout after 60s: only 1 of 3 nodes connected.");
        }));
    }

    /** An Error during start (a missing class, a failed static initializer) also ends the process. */
    @Test
    void startOrReportReportsAnErrorDuringStart() {
        assertFalse(HadoopArrowFlightServer.startOrReport(() -> {
            throw new NoClassDefFoundError("org/example/Missing");
        }));
    }

    /** A start that returns normally (the server stopped serving) is not a failure. */
    @Test
    void startOrReportAcceptsANormalReturn() {
        assertTrue(HadoopArrowFlightServer.startOrReport(() -> { }));
    }

    private static String invokeGetArgValue(String[] args, String key, String defaultValue)
            throws Exception {
        HadoopArrowFlightServer instance = new HadoopArrowFlightServer();
        Method m = HadoopArrowFlightServer.class.getDeclaredMethod(
                "getArgValue", String[].class, String.class, String.class);
        m.setAccessible(true);
        return (String) m.invoke(instance, args, key, defaultValue);
    }
}
