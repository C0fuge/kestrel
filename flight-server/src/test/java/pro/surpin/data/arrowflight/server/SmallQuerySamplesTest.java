package pro.surpin.data.arrowflight.server;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Verifies status samples and bounded waiting without executing a performance workload. */
@Tag("unit")
class SmallQuerySamplesTest {
    /**
     * Preserves a successful sample and a following execution failure with unknown answer statistics.
     *
     * @param directory raw output directory
     * @throws Exception when fixture setup or artifact reading fails
     */
    @Test
    void executionFailureKeepsTheEarlierSuccessfulSample(@TempDir Path directory) throws Exception {
        FlightSqlClient client = mock(FlightSqlClient.class);
        FlightInfo info = mock(FlightInfo.class);
        FlightEndpoint endpoint = mock(FlightEndpoint.class);
        Ticket ticket = new Ticket(new byte[] {1});
        FlightStream stream = mock(FlightStream.class);
        VectorSchemaRoot root = mock(VectorSchemaRoot.class);
        FieldVector vector = mock(FieldVector.class);
        when(client.execute(anyString(), any(CallOption.class))).thenReturn(info)
                .thenThrow(new IllegalStateException("test-only failure"));
        when(info.getEndpoints()).thenReturn(List.of(endpoint));
        when(endpoint.getTicket()).thenReturn(ticket);
        when(client.getStream(any(Ticket.class), any(CallOption.class))).thenReturn(stream);
        when(stream.next()).thenReturn(true, false);
        when(stream.getRoot()).thenReturn(root);
        when(root.getRowCount()).thenReturn(1);
        when(root.getVector(0)).thenReturn(vector);
        when(root.getFieldVectors()).thenReturn(List.of(vector));
        when(vector.getObject(0)).thenReturn(0L);
        when(vector.getBufferSize()).thenReturn(8);

        List<String[]> samples = failedSamples(client, directory);
        assertEquals(2, samples.size());
        assertEquals("1", samples.get(0)[10]);
        assertEquals("", samples.get(0)[11]);
        assertFailedSample(samples.get(1), "IllegalStateException");
    }

    /**
     * Records a stream failure rather than silently dropping the attempted query.
     *
     * @param directory raw output directory
     * @throws Exception when fixture setup or artifact reading fails
     */
    @Test
    void readFailureHasUnknownAnswerStatistics(@TempDir Path directory) throws Exception {
        FlightSqlClient client = mock(FlightSqlClient.class);
        FlightInfo info = mock(FlightInfo.class);
        when(client.execute(anyString(), any(CallOption.class))).thenReturn(info);
        when(info.getEndpoints()).thenThrow(new IllegalArgumentException("test-only failure"));
        assertFailedSample(failedSamples(client, directory).getFirst(), "IllegalArgumentException");
    }

    /**
     * Marks an incorrect answer before the assertion aborts the client task.
     *
     * @param directory raw output directory
     * @throws Exception when fixture setup or artifact reading fails
     */
    @Test
    void answerMismatchIsExplicit(@TempDir Path directory) throws Exception {
        FlightSqlClient client = mock(FlightSqlClient.class);
        FlightInfo info = mock(FlightInfo.class);
        when(client.execute(anyString(), any(CallOption.class))).thenReturn(info);
        when(info.getEndpoints()).thenReturn(List.of());
        String[] sample = failedSamples(client, directory).getFirst();
        assertEquals("0", sample[6]);
        assertEquals("0", sample[10]);
        assertEquals("answer_mismatch", sample[11]);
    }

    /** Verifies that an unfinished client cannot exceed the global collection deadline. */
    @Test
    void unfinishedClientTimesOutAtTheGlobalDeadline() {
        assertThrows(TimeoutException.class, () -> SmallQueryThroughputPerfTest.awaitClients(
                List.of(new CompletableFuture<>()), System.nanoTime() - 1));
    }

    /** Verifies that completed futures cannot hide a caller's pending interruption. */
    @Test
    void pendingInterruptionWinsOverCompletedAndFailedClients() {
        for (var future : List.of(CompletableFuture.completedFuture(new long[] {1}),
                CompletableFuture.<long[]>failedFuture(new IllegalStateException("test-only failure")))) {
            try {
                Thread.currentThread().interrupt();
                assertThrows(InterruptedException.class, () -> SmallQueryThroughputPerfTest.awaitClients(
                        List.of(future), System.nanoTime() + TimeUnit.SECONDS.toNanos(1)));
            } finally {
                Thread.interrupted();
            }
        }
    }

    /**
     * Retains partial samples and the original timeout when a client does not terminate.
     *
     * @param directory raw output directory
     * @throws Exception when fixture setup or artifact reading fails
     */
    @Test
    void nonterminatingClientKeepsPartialSamplesAndOriginalFailure(@TempDir Path directory) throws Exception {
        ExecutorService pool = mock(ExecutorService.class);
        when(pool.awaitTermination(10, TimeUnit.SECONDS)).thenReturn(false);
        TimeoutException timeout = new TimeoutException("test-only timeout");
        String row = "0,0,0,0,0,1,1,31,8,1,1,\n";
        String previous = System.setProperty("perf.output", directory.toString());
        try {
            SmallQueryThroughputPerfTest.finishClients(pool, List.of(new StringBuffer(row)), "c1", timeout);
        } finally {
            restoreOutput(previous);
        }
        assertTrue(Files.readString(directory.resolve("c1.csv")).endsWith(row));
        assertEquals(1, timeout.getSuppressed().length);
        assertEquals("Performance clients did not stop", timeout.getSuppressed()[0].getMessage());
    }

    /**
     * Keeps the caller's interruption while still creating the partial sample artifact.
     *
     * @param directory raw output directory
     * @throws Exception when artifact reading fails
     */
    @Test
    void interruptedRunKeepsItsPartialArtifact(@TempDir Path directory) throws Exception {
        FlightSqlClient client = mock(FlightSqlClient.class);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch stopped = new CountDownLatch(1);
        when(client.execute(anyString(), any(CallOption.class))).thenAnswer(invocation -> {
            started.countDown();
            try {
                new CountDownLatch(1).await();
                throw new AssertionError("The test-only worker must stop through interruption");
            } finally {
                stopped.countDown();
            }
        });
        String previous = System.setProperty("perf.output", directory.toString());
        try {
            Thread.currentThread().interrupt();
            assertThrows(InterruptedException.class, () -> SmallQueryThroughputPerfTest.run(
                    client, header(), true, 1, 1, true, null));
            assertTrue(Thread.currentThread().isInterrupted());
            if (started.getCount() == 0) {
                assertEquals(0, stopped.getCount(), "The started worker must stop before raw output is closed");
            }
        } finally {
            Thread.interrupted();
            restoreOutput(previous);
        }
        assertTrue(Files.readString(directory.resolve("c1.csv")).startsWith("thread,sequence,"));
    }

    /**
     * Stops an observed blocked worker and records its interrupted attempt before returning.
     *
     * @param directory raw output directory
     * @throws Exception when fixture setup or artifact reading fails
     */
    @Test
    void inFlightInterruptionWaitsForWorkerAndKeepsItsFailure(@TempDir Path directory) throws Exception {
        FlightSqlClient client = mock(FlightSqlClient.class);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch stopped = new CountDownLatch(1);
        when(client.execute(anyString(), any(CallOption.class))).thenAnswer(invocation -> {
            started.countDown();
            try {
                new CountDownLatch(1).await();
                throw new AssertionError("The test-only worker must stop through interruption");
            } finally {
                stopped.countDown();
            }
        });
        Thread caller = Thread.currentThread();
        Thread interrupter = new Thread(() -> {
            try {
                started.await(5, TimeUnit.SECONDS);
                caller.interrupt();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "test-only-caller-interrupter");
        String previous = System.setProperty("perf.output", directory.toString());
        try {
            interrupter.start();
            assertThrows(InterruptedException.class, () -> SmallQueryThroughputPerfTest.run(
                    client, header(), true, 1, 1, true, null));
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(0, started.getCount());
            assertEquals(0, stopped.getCount());
        } finally {
            Thread.interrupted();
            interrupter.interrupt();
            interrupter.join(1000);
            restoreOutput(previous);
        }
        List<String> rows = Files.readAllLines(directory.resolve("c1.csv"));
        assertEquals(2, rows.size());
        assertFailedSample(rows.get(1).split(",", -1), "InterruptedException");
    }

    /**
     * Retains raw rows when shutdown waiting itself is interrupted and restores the caller flag.
     *
     * @param directory raw output directory
     * @throws Exception when fixture setup or artifact reading fails
     */
    @Test
    void interruptedCleanupKeepsPartialRowsAndOriginalFailure(@TempDir Path directory) throws Exception {
        ExecutorService pool = mock(ExecutorService.class);
        when(pool.awaitTermination(10, TimeUnit.SECONDS)).thenThrow(new InterruptedException("test-only cleanup"));
        TimeoutException original = new TimeoutException("test-only timeout");
        String row = "0,0,0,0,0,1,1,31,8,1,1,\n";
        String previous = System.setProperty("perf.output", directory.toString());
        try {
            SmallQueryThroughputPerfTest.finishClients(pool, List.of(new StringBuffer(row)), "c1", original);
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            restoreOutput(previous);
        }
        assertTrue(Files.readString(directory.resolve("c1.csv")).endsWith(row));
        assertEquals(1, original.getSuppressed().length);
        assertTrue(original.getSuppressed()[0] instanceof InterruptedException);
    }

    /**
     * Preserves the primary failure and both cleanup failures when raw output cannot be created.
     *
     * @param directory raw output directory
     * @throws Exception when fixture setup fails
     */
    @Test
    void rawWriteFailureDoesNotReplaceTheOriginalFailure(@TempDir Path directory) throws Exception {
        ExecutorService pool = mock(ExecutorService.class);
        when(pool.awaitTermination(10, TimeUnit.SECONDS)).thenReturn(false);
        TimeoutException original = new TimeoutException("test-only timeout");
        Files.writeString(directory.resolve("c1.csv"), "retained");
        String previous = System.setProperty("perf.output", directory.toString());
        try {
            SmallQueryThroughputPerfTest.finishClients(pool, List.of(), "c1", original);
        } finally {
            restoreOutput(previous);
        }
        assertEquals("retained", Files.readString(directory.resolve("c1.csv")));
        assertEquals(1, original.getSuppressed().length);
        assertEquals(1, original.getSuppressed()[0].getSuppressed().length);
    }

    /**
     * Executes a mocked failing client and reads its retained raw samples.
     *
     * @param client mocked Flight client
     * @param directory raw output directory
     * @return sample columns after the header
     * @throws Exception when artifact reading fails
     */
    private static List<String[]> failedSamples(FlightSqlClient client, Path directory) throws Exception {
        String previous = System.setProperty("perf.output", directory.toString());
        try {
            assertThrows(ExecutionException.class, () -> SmallQueryThroughputPerfTest.run(
                    client, header(), true, 1, 1, true, null));
        } finally {
            restoreOutput(previous);
        }
        List<String> lines = Files.readAllLines(directory.resolve("c1.csv"));
        assertTrue(lines.getFirst().endsWith(",ok,error_type"));
        return lines.stream().skip(1).map(line -> line.split(",", -1)).toList();
    }

    /**
     * Checks failed sample status and unknown statistics.
     *
     * @param sample sample columns
     * @param error expected safe exception type
     */
    private static void assertFailedSample(String[] sample, String error) {
        assertTrue(Long.parseLong(sample[5]) >= 0);
        assertEquals("-1", sample[6]);
        assertEquals("0", sample[7]);
        assertEquals("-1", sample[8]);
        assertEquals("-1", sample[9]);
        assertEquals("0", sample[10]);
        assertEquals(error, sample[11]);
    }

    /**
     * Returns the header used by the mocked client calls.
     *
     * @return empty call header
     */
    private static CallOption header() {
        return new HeaderCallOption(new FlightCallHeaders());
    }

    /**
     * Restores the output property after a focused test.
     *
     * @param previous previous value, or null
     */
    private static void restoreOutput(String previous) {
        if (previous == null) {
            System.clearProperty("perf.output");
        } else {
            System.setProperty("perf.output", previous);
        }
    }
}
