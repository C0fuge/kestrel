package pro.surpin.data.arrowflight.server;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

class HadoopArrowFlightServerUnitTest {

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
