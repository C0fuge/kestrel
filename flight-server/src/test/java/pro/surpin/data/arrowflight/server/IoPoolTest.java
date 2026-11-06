package pro.surpin.data.arrowflight.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests the Parquet I/O pool's task count and its stop behaviour.
 */
@Tag("unit")
class IoPoolTest {

    /**
     * Regression (item 36 (4)): a task refused by the stopped pool was counted as active for
     * ever, so every later submit logged pool=queueGrowth.
     */
    @Test
    void refusedTaskIsNotCountedAsActive() throws Exception {
        IoPool pool = new IoPool(1);
        CountDownLatch release = new CountDownLatch(1);
        Future<?> busy = pool.submit(() -> {
            release.await();
            return null;
        });
        Future<?> queued = pool.submit(() -> { });
        assertEquals(2, pool.activeTasks());
        pool.shutdown();
        assertThrows(RejectedExecutionException.class, () -> pool.execute(() -> { }));
        assertEquals(2, pool.activeTasks());
        release.countDown();
        busy.get(10, TimeUnit.SECONDS);
        queued.get(10, TimeUnit.SECONDS);
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(0, pool.activeTasks());
    }
}
