package pro.surpin.data.arrowflight.server.adapters;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ProducingStreamGate}: the cap, re-entry, release on close and cancellation of
 * a waiting stream.
 */
@Tag("unit")
class ProducingStreamGateTest {

    /**
     * Verifies that a third stream waits while two hold the permits and goes on once one leaves.
     *
     * @throws Exception on failure
     */
    @Test
    void aStreamWaitsWhileTheCapIsReached() throws Exception {
        ProducingStreamGate gate = new ProducingStreamGate(2);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try (ProducingStreamGate.Pass first = gate.pass(() -> false);
                ProducingStreamGate.Pass second = gate.pass(() -> false)) {
            first.enter();
            // Entering again holds the same permit, it does not take a second one.
            first.enter();
            second.enter();
            CountDownLatch entered = new CountDownLatch(1);
            Future<?> third = pool.submit(() -> {
                try (ProducingStreamGate.Pass pass = gate.pass(() -> false)) {
                    pass.enter();
                    entered.countDown();
                }
                return null;
            });
            assertThrows(TimeoutException.class, () -> third.get(200, TimeUnit.MILLISECONDS));
            first.leave();
            third.get(5, TimeUnit.SECONDS);
            assertEquals(0, entered.getCount());
            assertEquals(1, gate.waits());
            assertTrue(gate.waitNanos() > 0);
        } finally {
            pool.shutdownNow();
        }
        // Closing the passes gave every permit back.
        try (ProducingStreamGate.Pass a = gate.pass(() -> false);
                ProducingStreamGate.Pass b = gate.pass(() -> false)) {
            a.enter();
            b.enter();
        }
        assertEquals(1, gate.waits());
    }

    /**
     * Verifies that a stream whose client goes away while it waits fails with CANCELLED and holds
     * no permit afterwards.
     *
     * @throws Exception on failure
     */
    @Test
    void aWaitingStreamStopsWhenItsClientGoesAway() throws Exception {
        ProducingStreamGate gate = new ProducingStreamGate(1);
        AtomicBoolean cancelled = new AtomicBoolean();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try (ProducingStreamGate.Pass holder = gate.pass(() -> false)) {
            holder.enter();
            Future<?> waiting = pool.submit(() -> {
                try (ProducingStreamGate.Pass pass = gate.pass(cancelled::get)) {
                    pass.enter();
                }
                return null;
            });
            assertThrows(TimeoutException.class, () -> waiting.get(100, TimeUnit.MILLISECONDS));
            cancelled.set(true);
            java.util.concurrent.ExecutionException e = assertThrows(
                    java.util.concurrent.ExecutionException.class, () -> waiting.get(5, TimeUnit.SECONDS));
            FlightRuntimeException flight = (FlightRuntimeException) e.getCause();
            assertEquals(FlightStatusCode.CANCELLED, flight.status().code());
        } finally {
            pool.shutdownNow();
        }
        try (ProducingStreamGate.Pass pass = gate.pass(() -> false)) {
            pass.enter();
        }
        assertEquals(1, gate.waits());
    }

    /**
     * Verifies that the gate without a cap never makes a stream wait.
     *
     * @throws Exception on failure
     */
    @Test
    void withoutACapNoStreamWaits() throws Exception {
        for (ProducingStreamGate gate : new ProducingStreamGate[] {
                ProducingStreamGate.OFF, new ProducingStreamGate(-3)}) {
            assertEquals(0, gate.permits());
            for (int i = 0; i < 100; i++) {
                gate.pass(() -> false).enter();
            }
            assertEquals(0, gate.waits());
        }
    }
}
