package pro.surpin.data.arrowflight.server;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.flight.sql.FlightSqlClient;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/** Verifies a plain-local helper closes the exact background resources it creates. */
@Tag("integration")
class TestFlightServerHelperOwnershipTest {

    @TempDir
    Path directory;

    /** Verifies local helpers stop their adapter reaper and both helper-owned I/O executors. */
    @Test
    void plainLocalCloseStopsOwnedThreads() throws Exception {
        TestFlightServerHelper helper = TestFlightServerHelper.builder()
                .dataDir(directory.toString()).start();
        ExecutorService reaper = null;
        ExecutorService adapterIo = helper.duckDbAdapter.ioPool();
        ExecutorService executionIo = null;
        boolean closed = false;
        Throwable failure = null;
        try {
            reaper = executor(helper.duckDbAdapter, "sessionReaper");
            executionIo = executor(helper.executionService, "ioPool");
            Thread reaperThread = reaper.submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
            Thread adapterThread = adapterIo.submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
            Thread executionThread = executionIo.submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
            assertNull(helper.registry, "plain-local mode owns its adapter too");
            helper.close();
            closed = true;
            assertTrue(reaper.awaitTermination(2, TimeUnit.SECONDS), "owned reaper stopped after helper close");
            assertTrue(adapterIo.isShutdown(), "helper stops the I/O executor it passed to its adapter");
            assertTrue(executionIo.isShutdown(), "helper stops the separate execution I/O executor");
            assertTrue(adapterIo.awaitTermination(5, TimeUnit.SECONDS));
            assertTrue(executionIo.awaitTermination(5, TimeUnit.SECONDS));
            for (Thread thread : new Thread[] {reaperThread, adapterThread, executionThread}) {
                thread.join(5000);
                assertFalse(thread.isAlive(), "task-owned thread ended: " + thread.getName());
            }
        } catch (Exception | Error error) {
            failure = error;
            throw error;
        } finally {
            try {
                cleanup(helper, closed, reaper, adapterIo, executionIo);
            } catch (Exception | Error error) {
                if (failure == null) {
                    throw error;
                }
                failure.addSuppressed(error);
            }
        }
    }

    /** Verifies a later interrupted owned-worker wait preserves the primary close failure and interrupt. */
    @Test
    void closePreservesPrimaryFailureAndLaterInterruption() throws Exception {
        TestFlightServerHelper helper = TestFlightServerHelper.builder()
                .dataDir(directory.toString()).start();
        ExecutorService adapterIo = helper.duckDbAdapter.ioPool();
        ExecutorService executionIo = null;
        ExecutorService reaper = null;
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        FlightSqlClient original = helper.sqlClient;
        Field clientField = TestFlightServerHelper.class.getDeclaredField("sqlClient");
        clientField.setAccessible(true);
        Future<?> worker = null;
        boolean closeCalled = false;
        Throwable failure = null;
        try {
            executionIo = executor(helper.executionService, "ioPool");
            reaper = executor(helper.duckDbAdapter, "sessionReaper");
            Thread cleanupThread = Thread.currentThread();
            worker = adapterIo.submit(() -> {
                started.countDown();
                boolean signalled = false;
                while (release.getCount() != 0) {
                    try {
                        release.await();
                    } catch (InterruptedException ignored) {
                        // shutdownNow reaches this exact worker only after the earlier close failure.
                        if (!signalled) {
                            cleanupThread.interrupt();
                            signalled = true;
                        }
                    }
                }
            });
            assertTrue(started.await(5, TimeUnit.SECONDS), "owned worker entered its bounded fixture");
            IllegalStateException primary = new IllegalStateException("synthetic client close failure");
            FlightSqlClient failing = mock(FlightSqlClient.class);
            doThrow(primary).when(failing).close();
            clientField.set(helper, failing);
            closeCalled = true;
            Exception actual = assertThrows(Exception.class, helper::close);
            assertSame(primary, actual, "first close failure remains primary");
            assertTrue(Thread.currentThread().isInterrupted(), "later cleanup interruption is restored");
            assertTrue(java.util.Arrays.stream(actual.getSuppressed())
                    .anyMatch(InterruptedException.class::isInstance), "later interruption remains diagnostic evidence");
            assertTrue(reaper.isShutdown(), "cleanup still reaches adapter close");
            assertTrue(executionIo.isShutdown(), "cleanup still stops the other owned pool");
        } catch (Exception | Error error) {
            failure = error;
            throw error;
        } finally {
            // Clear only this fixture's deliberate interrupt so its own cleanup can wait.
            Thread.interrupted();
            release.countDown();
            try {
                try {
                    clientField.set(helper, original);
                    if (worker != null) {
                        worker.get(5, TimeUnit.SECONDS);
                    }
                } finally {
                    cleanup(helper, closeCalled, reaper, adapterIo, executionIo);
                }
            } catch (Exception | Error error) {
                if (failure == null) {
                    throw error;
                }
                failure.addSuppressed(error);
            }
        }
    }

    /**
     * Finds one background executor belonging to the supplied fixture object.
     *
     * @param owner exact task-owned adapter or execution service
     * @param name executor field
     * @return owned executor
     * @throws Exception when the fixture field cannot be read
     */
    private static ExecutorService executor(Object owner, String name) throws Exception {
        Field field = owner.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return (ExecutorService) field.get(owner);
    }

    /**
     * Stops only the resources created for this helper, including on the failing baseline.
     *
     * @param helper task helper
     * @param closed whether its close already returned
     * @param reaper its adapter's reaper, or null before capture
     * @param adapterIo its adapter I/O executor
     * @param executionIo its execution I/O executor, or null before capture
     * @throws Exception when a fixture resource cannot stop
     */
    private static void cleanup(TestFlightServerHelper helper, boolean closed, ExecutorService reaper,
            ExecutorService adapterIo, ExecutorService executionIo) throws Exception {
        try {
            if (!closed) {
                helper.close();
            }
        } finally {
            try {
                adapterIo.shutdownNow();
                if (executionIo != null) {
                    executionIo.shutdownNow();
                }
                assertTrue(adapterIo.awaitTermination(5, TimeUnit.SECONDS));
                if (executionIo != null) {
                    assertTrue(executionIo.awaitTermination(5, TimeUnit.SECONDS));
                }
            } finally {
                try {
                    if (reaper == null || !reaper.isShutdown()) {
                        helper.duckDbAdapter.close();
                    }
                } finally {
                    try {
                        if (reaper != null) {
                            reaper.shutdownNow();
                            assertTrue(reaper.awaitTermination(5, TimeUnit.SECONDS));
                        }
                    } finally {
                        helper.server.close();
                    }
                }
            }
        }
    }
}
