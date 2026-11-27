package pro.surpin.data.arrowflight.server;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Checks durable TPC-H raw measurements with mocked Flight answers and concurrent clients. */
@Tag("unit")
class TpchQuerySamplesTest {
    /**
     * Retains warm-up and measured attempts with real per-answer buffer and batch accounting.
     *
     * @param directory temporary parent directory
     * @throws Exception when fixture setup or sample reading fails
     */
    @Test
    void recordsWarmupAndMeasuredAnswers(@TempDir Path directory) throws Exception {
        FlightSqlClient client = mock(FlightSqlClient.class);
        FlightStream stream = answerStream(client);
        when(stream.next()).thenReturn(true, true, false, true, true, false);
        AtomicInteger failures = new AtomicInteger();
        try (TpchQuerySamples raw = samples(directory, List.of("q01"), List.of(2L))) {
            for (int round = 0; round <= 1; round++) {
                TpchPerfTest.runPass(client, header(), List.of("SELECT 1"), List.of("q01"), List.of(2L),
                        round, 0, 1, failures, raw);
            }
        }
        List<String[]> rows = rows(directory);
        assertEquals(0, failures.get());
        assertEquals(2, rows.size());
        assertEquals("1", rows.get(0)[3]);
        assertEquals("1", rows.get(0)[5]);
        assertEquals("0", rows.get(1)[3]);
        assertEquals("0", rows.get(1)[5]);
        for (String[] row : rows) {
            assertTrue(Long.parseLong(row[6]) > 0);
            assertTrue(Long.parseLong(row[8]) >= 0);
            assertEquals("2", row[9]);
            assertFalse(row[10].equals("0000000000000000"));
            assertEquals("16", row[11]);
            assertEquals("2", row[12]);
            assertEquals("1", row[13]);
            assertEquals("", row[14]);
            assertEquals(TpchChecksum.VERSION, row[15]);
        }
        String metadata = Files.readString(directory.resolve("raw/metadata.txt"));
        assertTrue(metadata.contains("perf.rounds=1\nperf.clients=2\nperf.mode=complete\n"));
        assertTrue(metadata.contains("query.0.sha256="));
        assertTrue(metadata.contains("checksum_version=" + TpchChecksum.VERSION + "\n"));
        assertFalse(metadata.contains(directory.toString()));
    }

    /**
     * Converts Arrow text and exact decimal counts to the cross-language q12 checksum.
     *
     * @param directory temporary parent directory
     * @throws Exception when fixture setup or sample reading fails
     */
    @Test
    void arrowTextAndHugeintDecimalsUseTheCanonicalChecksum(@TempDir Path directory) throws Exception {
        FlightSqlClient client = mock(FlightSqlClient.class);
        FlightStream stream = answerStream(client);
        VectorSchemaRoot root = stream.getRoot();
        FieldVector text = mock(FieldVector.class);
        FieldVector high = mock(FieldVector.class);
        FieldVector low = mock(FieldVector.class);
        when(root.getFieldVectors()).thenReturn(List.of(text, high, low));
        when(root.getRowCount()).thenReturn(2);
        when(text.getObject(0)).thenReturn(new Text("MAIL"));
        when(text.getObject(1)).thenReturn(new Text("SHIP"));
        when(high.getObject(0)).thenReturn(new BigDecimal("6202"));
        when(high.getObject(1)).thenReturn(new BigDecimal("6200"));
        when(low.getObject(0)).thenReturn(new BigDecimal("9324"));
        when(low.getObject(1)).thenReturn(new BigDecimal("9262"));
        when(stream.next()).thenReturn(true, false);
        AtomicInteger failures = new AtomicInteger();
        try (TpchQuerySamples raw = samples(directory, List.of("q12"), List.of(2L))) {
            TpchPerfTest.runPass(client, header(), List.of("SELECT 1"), List.of("q12"), List.of(2L),
                    0, 0, 1, failures, raw);
        }
        assertEquals(0, failures.get());
        assertEquals("66191bf0fe853f3d", rows(directory).getFirst()[10]);
        assertEquals(TpchChecksum.VERSION, rows(directory).getFirst()[15]);
    }

    /**
     * Keeps both an execution error and the following wrong answer with explicit failure status.
     *
     * @param directory temporary parent directory
     * @throws Exception when fixture setup or sample reading fails
     */
    @Test
    void failedQueriesDoNotLoseFollowingMismatches(@TempDir Path directory) throws Exception {
        FlightSqlClient client = mock(FlightSqlClient.class);
        FlightInfo info = mock(FlightInfo.class);
        when(client.execute(anyString(), any(CallOption.class))).thenThrow(new IllegalStateException("fixture"))
                .thenReturn(info);
        when(info.getEndpoints()).thenReturn(List.of());
        AtomicInteger failures = new AtomicInteger();
        try (TpchQuerySamples raw = samples(directory, List.of("q01", "q02"), List.of(1L, 1L))) {
            TpchPerfTest.runPass(client, header(), List.of("SELECT 1", "SELECT 2"), List.of("q01", "q02"),
                    List.of(1L, 1L), 0, 0, 1, failures, raw);
        }
        List<String[]> rows = rows(directory);
        assertEquals(2, failures.get());
        assertEquals(2, rows.size());
        assertUnknownFailure(rows.get(0), "IllegalStateException");
        assertEquals("0", rows.get(1)[9]);
        assertEquals("0", rows.get(1)[11]);
        assertEquals("0", rows.get(1)[12]);
        assertEquals("0", rows.get(1)[13]);
        assertEquals("answer_mismatch", rows.get(1)[14]);
    }

    /**
     * Marks partial reads as unknown rather than reporting an incomplete successful answer.
     *
     * @param directory temporary parent directory
     * @throws Exception when fixture setup or sample reading fails
     */
    @Test
    void partialStreamFailureUsesUnknownAnswerMetrics(@TempDir Path directory) throws Exception {
        FlightSqlClient client = mock(FlightSqlClient.class);
        FlightStream stream = answerStream(client);
        when(stream.next()).thenReturn(true).thenThrow(new IllegalArgumentException("fixture"));
        AtomicInteger failures = new AtomicInteger();
        try (TpchQuerySamples raw = samples(directory, List.of("q01"), List.of(2L))) {
            TpchPerfTest.runPass(client, header(), List.of("SELECT 1"), List.of("q01"), List.of(2L),
                    0, 0, 1, failures, raw);
        }
        assertEquals(1, failures.get());
        assertUnknownFailure(rows(directory).getFirst(), "IllegalArgumentException");
    }

    /**
     * Flushes concurrent client samples before close and preserves every complete row.
     *
     * @param directory temporary parent directory
     * @throws Exception when concurrent recording or sample reading fails
     */
    @Test
    void concurrentClientsFlushEveryAttempt(@TempDir Path directory) throws Exception {
        try (TpchQuerySamples raw = samples(directory, List.of("q01"), List.of(1L));
                var pool = Executors.newFixedThreadPool(2)) {
            List<Future<?>> tasks = new java.util.ArrayList<>();
            for (int client = 0; client < 2; client++) {
                int index = client;
                tasks.add(pool.submit(() -> {
                    for (int sequence = 0; sequence < 20; sequence++) {
                        raw.write("q01", index, 0, sequence, System.nanoTime(), 1,
                                new TpchQuerySamples.Answer(1, 42, 8, 1), "");
                    }
                    return null;
                }));
            }
            for (Future<?> task : tasks) {
                task.get();
            }
            assertEquals(40, rows(directory).size());
        }
        assertEquals(40, rows(directory).size());
        assertTrue(rows(directory).stream().allMatch(row -> row.length == 16));
        assertEquals(20, rows(directory).stream().filter(row -> row[1].equals("0")).count());
        assertEquals(20, rows(directory).stream().filter(row -> row[1].equals("1")).count());
    }

    /**
     * Rejects reuse of a previous run directory without changing its samples.
     *
     * @param directory temporary parent directory
     * @throws Exception when creating or reading the existing run fails
     */
    @Test
    void existingRunIsNeverOverwritten(@TempDir Path directory) throws Exception {
        try (TpchQuerySamples raw = samples(directory, List.of("q01"), List.of(1L))) {
            raw.write("q01", 0, 0, 0, System.nanoTime(), 1, new TpchQuerySamples.Answer(1, 42, 8, 1), "");
        }
        String original = Files.readString(directory.resolve("raw/queries.csv"));
        assertThrows(FileAlreadyExistsException.class, () -> samples(directory, List.of("q01"), List.of(1L)));
        assertEquals(original, Files.readString(directory.resolve("raw/queries.csv")));
    }

    /**
     * Verifies that optional collection adds no buffer accounting to the default read path.
     *
     * @throws Exception when fixture setup fails
     */
    @Test
    void disabledCollectionLeavesBufferAccountingDisabled() throws Exception {
        FlightSqlClient client = mock(FlightSqlClient.class);
        FlightStream stream = answerStream(client);
        when(stream.next()).thenReturn(true, false);
        when(stream.getRoot().getFieldVectors().getFirst().getBufferSize()).thenThrow(new AssertionError());
        AtomicInteger failures = new AtomicInteger();
        TpchPerfTest.runPass(client, header(), List.of("SELECT 1"), List.of("q01"), List.of(1L),
                0, 0, 1, failures, null);
        assertEquals(0, failures.get());
        String previous = System.clearProperty("perf.rawDir");
        try {
            assertNull(TpchQuerySamples.open(List.of(), List.of(), List.of(), 1, 1, "complete"));
        } finally {
            restoreProperty(previous);
        }
    }

    /**
     * Fails the pass when raw output cannot be retained rather than treating it as a query error.
     *
     * @param directory temporary parent directory
     * @throws Exception when fixture setup fails
     */
    @Test
    void recordingFailureAbortsThePass(@TempDir Path directory) throws Exception {
        FlightSqlClient client = mock(FlightSqlClient.class);
        FlightStream stream = answerStream(client);
        when(stream.next()).thenReturn(true, false);
        TpchQuerySamples raw = samples(directory, List.of("q01"), List.of(1L));
        raw.close();
        assertThrows(IOException.class, () -> TpchPerfTest.runPass(client, header(), List.of("SELECT 1"),
                List.of("q01"), List.of(1L), 0, 0, 1, new AtomicInteger(), raw));
    }

    /**
     * Builds a mocked one-row Arrow batch with an eight-byte vector.
     *
     * @param client client receiving the answer
     * @return stream with configurable batch progression
     */
    private static FlightStream answerStream(FlightSqlClient client) {
        FlightInfo info = mock(FlightInfo.class);
        FlightEndpoint endpoint = mock(FlightEndpoint.class);
        FlightStream stream = mock(FlightStream.class);
        VectorSchemaRoot root = mock(VectorSchemaRoot.class);
        FieldVector vector = mock(FieldVector.class);
        when(client.execute(anyString(), any(CallOption.class))).thenReturn(info);
        when(info.getEndpoints()).thenReturn(List.of(endpoint));
        when(endpoint.getTicket()).thenReturn(new Ticket(new byte[] {1}));
        when(client.getStream(any(Ticket.class), any(CallOption.class))).thenReturn(stream);
        when(stream.getRoot()).thenReturn(root);
        when(root.getRowCount()).thenReturn(1);
        when(root.getFieldVectors()).thenReturn(List.of(vector));
        when(vector.getObject(0)).thenReturn(42L);
        when(vector.getBufferSize()).thenReturn(8);
        return stream;
    }

    /**
     * Opens a collector in a fresh subdirectory while restoring the caller's property.
     *
     * @param directory temporary parent directory
     * @param names query names
     * @param expected expected row counts
     * @return opened collector
     * @throws Exception when collector creation fails
     */
    private static TpchQuerySamples samples(Path directory, List<String> names, List<Long> expected)
            throws Exception {
        String previous = System.setProperty("perf.rawDir", directory.resolve("raw").toString());
        try {
            return TpchQuerySamples.open(names, names.stream().map(name -> "SELECT 1").toList(),
                    expected, 1, 2, "complete");
        } finally {
            restoreProperty(previous);
        }
    }

    /**
     * Reads the raw data rows, preserving empty trailing status fields.
     *
     * @param directory temporary parent directory
     * @return raw sample fields
     * @throws IOException when samples cannot be read
     */
    private static List<String[]> rows(Path directory) throws IOException {
        return Files.readAllLines(directory.resolve("raw/queries.csv")).stream().skip(1)
                .map(line -> line.split(",", -1)).toList();
    }

    /**
     * Checks the documented unknown metrics of a failed query.
     *
     * @param row raw sample fields
     * @param error expected safe error type
     */
    private static void assertUnknownFailure(String[] row, String error) {
        assertEquals("-1", row[9]);
        assertEquals("0000000000000000", row[10]);
        assertEquals("-1", row[11]);
        assertEquals("-1", row[12]);
        assertEquals("0", row[13]);
        assertEquals(error, row[14]);
    }

    /**
     * Returns the call header accepted by the mocked clients.
     *
     * @return empty header
     */
    private static CallOption header() {
        return new HeaderCallOption(new FlightCallHeaders());
    }

    /**
     * Restores the raw-directory property after fixture construction.
     *
     * @param previous original property, or null
     */
    private static void restoreProperty(String previous) {
        if (previous == null) {
            System.clearProperty("perf.rawDir");
        } else {
            System.setProperty("perf.rawDir", previous);
        }
    }
}
