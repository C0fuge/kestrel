package pro.surpin.data.arrowflight.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests that a rate-limited log line goes through once per interval and reports the lines held
 * back (item 36 (4): ~300 pool=queueGrowth WARN lines per node under 32 clients).
 */
@Tag("unit")
class LogRateLimitTest {

    /** Verifies the first line passes, lines within the interval are held and counted. */
    @Test
    void passesOncePerIntervalAndCountsTheRest() {
        AtomicLong now = new AtomicLong(1_000);
        LogRateLimit limit = new LogRateLimit(100, now::get);
        assertEquals(0, limit.tryLog());
        assertEquals(-1, limit.tryLog());
        now.addAndGet(99);
        assertEquals(-1, limit.tryLog());
        now.addAndGet(1);
        assertEquals(2, limit.tryLog());
        now.addAndGet(500);
        assertEquals(0, limit.tryLog());
    }
}
