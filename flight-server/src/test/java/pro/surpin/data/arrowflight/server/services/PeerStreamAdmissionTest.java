package pro.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks the admission of distributed complete merges (item 35): merges take room for their
 * endpoint streams, wait while other merges hold it, in arrival order, and are refused with
 * RESOURCE_EXHAUSTED only after the longest wait.
 */
class PeerStreamAdmissionTest {

    @Test
    void permitsFollowTheDirectMemoryLimit() {
        assertEquals(16, PeerStreamAdmission.defaultPermits(2L << 30));
        assertEquals(8, PeerStreamAdmission.defaultPermits(1L << 30));
        assertEquals(PeerStreamAdmission.MIN_PERMITS, PeerStreamAdmission.defaultPermits(64L << 20));
        assertThrows(IllegalArgumentException.class, () -> new PeerStreamAdmission(0, 1));
        assertThrows(IllegalArgumentException.class, () -> new PeerStreamAdmission(1, -1));
    }

    /**
     * Item 39 (d): on the cluster 8 GiB of direct memory gave 64 permits while the peer streams
     * are read into the default 2 GiB Arrow allocator, so eight concurrent full scans failed at
     * 2 GiB; the permits follow the smaller limit, including a parent allocator's.
     */
    @Test
    void permitsFollowTheArrowAllocatorLimitWhenItIsSmaller() {
        assertEquals(16, PeerStreamAdmission.defaultPermits(8L << 30, 2L << 30));
        assertEquals(16, PeerStreamAdmission.defaultPermits(2L << 30, 8L << 30));
        assertEquals(64, PeerStreamAdmission.defaultPermits(8L << 30, Long.MAX_VALUE));
        try (BufferAllocator root = new RootAllocator(2L << 30);
                BufferAllocator child = root.newChildAllocator("peers", 0, Long.MAX_VALUE)) {
            assertEquals(2L << 30, PeerStreamAdmission.effectiveLimit(child));
            assertEquals(16, PeerStreamAdmission.defaultPermits(8L << 30,
                    PeerStreamAdmission.effectiveLimit(child)));
        }
    }

    /** Item 39 (d): one-row merges neither wait nor take room, even when none is free. */
    @Test
    void unmeteredMergesNeitherWaitNorTakeRoom() throws Exception {
        PeerStreamAdmission admission = new PeerStreamAdmission(4, 100);
        try (PeerStreamAdmission.Admitted running = admission.admit(4, () -> false, "scan")) {
            PeerStreamAdmission.Admitted small = admission.admitUnmetered();
            assertEquals(0, small.held());
            small.close();
            small.close();
            assertEquals(0, admission.available());
        }
        assertEquals(4, admission.available());
        assertEquals(1, admission.unmetered());
        assertEquals(0, admission.waited());
    }

    @Test
    void mergesTakeRoomAndReturnItOnce() throws Exception {
        PeerStreamAdmission admission = new PeerStreamAdmission(16, 1000);
        PeerStreamAdmission.Admitted first = admission.admit(4, () -> false, "q1");
        PeerStreamAdmission.Admitted second = admission.admit(4, () -> false, "q2");
        assertEquals(8, admission.available());
        first.close();
        first.close();
        assertEquals(12, admission.available());
        second.close();
        assertEquals(16, admission.available());
        assertEquals(0, admission.waited());
    }

    @Test
    void aMergeOfMoreStreamsThanPermitsRunsAlone() throws Exception {
        PeerStreamAdmission admission = new PeerStreamAdmission(4, 1000);
        try (PeerStreamAdmission.Admitted big = admission.admit(12, () -> false, "q")) {
            assertEquals(4, big.held());
            assertEquals(0, admission.available());
        }
        try (PeerStreamAdmission.Admitted none = admission.admit(0, () -> false, "q")) {
            assertEquals(1, none.held());
        }
    }

    @Test
    void aMergeWaitsUntilRoomComesBack() throws Exception {
        PeerStreamAdmission admission = new PeerStreamAdmission(8, 30_000);
        PeerStreamAdmission.Admitted running = admission.admit(8, () -> false, "running");
        CompletableFuture<PeerStreamAdmission.Admitted> waiting = CompletableFuture.supplyAsync(() -> {
            try {
                return admission.admit(4, () -> false, "waiting");
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        });
        waitFor(() -> admission.waited() == 1);
        assertFalse(waiting.isDone());
        running.close();
        try (PeerStreamAdmission.Admitted admitted = waiting.get(10, TimeUnit.SECONDS)) {
            assertEquals(4, admitted.held());
            assertEquals(4, admission.available());
        }
        assertEquals(0, admission.refused());
        assertTrue(admission.waitNanos() > 0);
    }

    @Test
    void waitingMergesAreAdmittedInArrivalOrder() throws Exception {
        PeerStreamAdmission admission = new PeerStreamAdmission(8, 30_000);
        PeerStreamAdmission.Admitted running = admission.admit(6, () -> false, "running");
        // A large merge arrives first; a small one that would fit the two free permits must not pass it.
        CompletableFuture<PeerStreamAdmission.Admitted> large = admit(admission, 8);
        waitFor(() -> admission.waited() == 1);
        CompletableFuture<PeerStreamAdmission.Admitted> small = admit(admission, 2);
        waitFor(() -> admission.waited() == 2);
        Thread.sleep(200);
        assertFalse(small.isDone(), "the small merge waits behind the large one");
        running.close();
        large.get(10, TimeUnit.SECONDS).close();
        small.get(10, TimeUnit.SECONDS).close();
        assertEquals(8, admission.available());
    }

    @Test
    void aMergeWithoutRoomInTimeIsRefusedAsResourceExhausted() throws Exception {
        PeerStreamAdmission admission = new PeerStreamAdmission(4, 100);
        try (PeerStreamAdmission.Admitted running = admission.admit(4, () -> false, "running")) {
            FlightRuntimeException refused = assertThrows(FlightRuntimeException.class,
                    () -> admission.admit(2, () -> false, "refused"));
            assertEquals(FlightStatusCode.RESOURCE_EXHAUSTED, refused.status().code());
            assertTrue(refused.getMessage().contains("retry later"), refused.getMessage());
        }
        assertEquals(1, admission.refused());
        assertEquals(4, admission.available(), "a refused merge holds no room");
    }

    @Test
    void aMergeWhoseClientWentAwayStopsWaiting() throws Exception {
        PeerStreamAdmission admission = new PeerStreamAdmission(2, 60_000);
        AtomicBoolean cancelled = new AtomicBoolean(true);
        try (PeerStreamAdmission.Admitted running = admission.admit(2, () -> false, "running")) {
            FlightRuntimeException stopped = assertThrows(FlightRuntimeException.class,
                    () -> admission.admit(2, cancelled::get, "cancelled"));
            assertEquals(FlightStatusCode.CANCELLED, stopped.status().code());
        }
        assertEquals(2, admission.available());
        assertEquals(0, admission.refused());
    }

    private static CompletableFuture<PeerStreamAdmission.Admitted> admit(PeerStreamAdmission admission,
            int streams) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return admission.admit(streams, () -> false, "q" + streams);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private static void waitFor(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not reached in 10 s");
            }
            Thread.sleep(10);
        }
    }
}
