package pro.surpin.data.arrowflight.server.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import pro.surpin.data.arrowflight.server.model.ExecutionPath;
import pro.surpin.data.arrowflight.server.model.ExecutionPathTracker;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests machine-readable runtime execution-path evidence.
 */
@Tag("unit")
class ExecutionPathRecorderTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    /**
     * Verifies every concrete server path is serialized with a stable label.
     *
     * @throws Exception if JSON parsing fails
     */
    @Test
    void serializesFooterScanAggregationAndJoinPaths() throws Exception {
        List<ExecutionPath> paths = List.of(
                ExecutionPath.FOOTER_COUNT,
                ExecutionPath.FOOTER_STATS,
                ExecutionPath.DUCKDB_SCAN,
                ExecutionPath.DUCKDB_AGGREGATION,
                ExecutionPath.DUCKDB_JOIN);

        for (ExecutionPath path : paths) {
            ExecutionPathTracker tracker = new ExecutionPathTracker();
            tracker.select(path);

            JsonNode event = JSON.readTree(ExecutionPathRecorder.toJson(
                    "qid", "SELECT  *\nFROM tpch.lineitem", tracker, true, null));

            assertEquals(path.label(), event.path("execution_path").asText());
            assertTrue(event.path("pushdown_evidence").asBoolean());
            assertTrue(event.path("success").asBoolean());
        }
    }

    /**
     * Verifies fallback evidence names its target without claiming pushdown.
     *
     * @throws Exception if JSON parsing fails
     */
    @Test
    void serializesFallbackAsNonPublishableEvidence() throws Exception {
        ExecutionPathTracker tracker = new ExecutionPathTracker();
        tracker.fallbackTo(
                ExecutionPath.DUCKDB_AGGREGATION, "missing-parquet-footer-statistics");

        JsonNode event = JSON.readTree(ExecutionPathRecorder.toJson(
                "qid", "select min(value) from sample", tracker, true, null));

        assertEquals("fallback", event.path("execution_path").asText());
        assertEquals("duckdb-aggregation", event.path("fallback_target").asText());
        assertEquals("missing-parquet-footer-statistics", event.path("reason").asText());
        assertFalse(event.path("pushdown_evidence").asBoolean());
    }

    /**
     * Verifies failed unknown paths retain a bounded failure reason.
     *
     * @throws Exception if JSON parsing fails
     */
    @Test
    void serializesUnknownFailureExplicitly() throws Exception {
        ExecutionPathTracker tracker = new ExecutionPathTracker();
        tracker.unknown("no-parquet-files");

        JsonNode event = JSON.readTree(ExecutionPathRecorder.toJson(
                "qid", "select * from missing", tracker, false, "failure\nmessage"));

        assertEquals("unknown", event.path("execution_path").asText());
        assertFalse(event.path("pushdown_evidence").asBoolean());
        assertFalse(event.path("success").asBoolean());
        assertEquals("failure message", event.path("failure_reason").asText());
    }

    /**
     * Verifies logically equivalent whitespace produces the same query digest.
     */
    @Test
    void normalizesWhitespaceBeforeDigestingQueries() {
        String first = ExecutionPathRecorder.queryDigest("SELECT *\nFROM T");
        String second = ExecutionPathRecorder.queryDigest(" select   * from t ");

        assertEquals(first, second);
        assertNotEquals(first, ExecutionPathRecorder.queryDigest("select id from t"));
    }

    /**
     * Verifies the digest is the SHA-256 of the normalized text, also when computed repeatedly on
     * one thread (the digest object is reused).
     */
    @Test
    void digestIsStableAcrossRepeatedCalls() {
        for (int i = 0; i < 3; i++) {
            assertEquals("42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772",
                    ExecutionPathRecorder.queryDigest("SELECT *\n\tFROM T"));
            assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", ExecutionPathRecorder.queryDigest(null));
        }
    }

    /**
     * Verifies events recorded concurrently are all written, in order per thread, by the
     * background writer.
     *
     * @param dir temporary directory
     * @throws Exception if writing or parsing fails
     */
    @Test
    void eventsRecordedConcurrentlyAreAllWritten(@TempDir Path dir) throws Exception {
        Path output = dir.resolve("events.jsonl");
        int threads = 8;
        int perThread = 200;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> done = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int thread = t;
                done.add(pool.submit(() -> {
                    for (int i = 0; i < perThread; i++) {
                        ExecutionPathTracker tracker = new ExecutionPathTracker();
                        tracker.select(ExecutionPath.DUCKDB_SCAN);
                        ExecutionPathRecorder.recordEvent(thread + "-" + i, "SELECT 1",
                                tracker, true, null, output);
                    }
                }));
            }
            for (Future<?> f : done) {
                f.get();
            }
        } finally {
            pool.shutdown();
        }
        assertTrue(ExecutionPathRecorder.flush(10_000));

        List<String> lines = Files.readAllLines(output);
        assertEquals(threads * perThread, lines.size());
        int[] next = new int[threads];
        for (String line : lines) {
            String[] qid = JSON.readTree(line).path("qid").asText().split("-");
            int thread = Integer.parseInt(qid[0]);
            assertEquals(next[thread]++, Integer.parseInt(qid[1]));
        }
    }
}

