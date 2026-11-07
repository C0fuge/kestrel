package pro.surpin.data.arrowflight.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/**
 * Item 38 (3): an error message carrying a remote stack trace is logged as its first line.
 */
class LogUtilFirstLineTest {

    /**
     * Verifies the first non-blank line is kept, with the number of lines left out.
     */
    @Test
    void keepsTheFirstLineAndCountsTheRest() {
        assertNull(LogUtil.firstLine(null));
        assertEquals("", LogUtil.firstLine(""));
        assertEquals("one line", LogUtil.firstLine("one line"));
        assertEquals("UNAUTHENTICATED: Internal calls are accepted from live cluster members only (+2 lines)",
                LogUtil.firstLine("UNAUTHENTICATED: Internal calls are accepted from live cluster members only\n"
                        + "\tat a.b.C.d(C.java:1)\r\n\tat e.f.G.h(G.java:2)\n\n"));
        assertEquals("cause (+1 lines)", LogUtil.firstLine("\n  \ncause\r  next"));
    }
}
