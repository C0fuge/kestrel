package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.server.services.DistributedComplete;
import pro.surpin.data.arrowflight.server.services.PeerStreamAdmission;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests distributed complete mode (item 18 step 2) end to end on one node whose plans have four
 * endpoints: the node reads its own endpoints over Flight, merges them, and answers exactly as it
 * does alone; a peer that cannot be reached before the first batch falls back to that answer.
 */
@Tag("integration")
class DistributedCompleteIntegrationTest {

    private static final int FILES = 4;
    private static Path dataDir;
    private static TestFlightServerHelper helper;
    private static BufferAllocator peerAllocator;
    private static DistributedComplete distributed;

    /**
     * Writes a table of four files and starts a complete-mode server with four endpoints per node.
     *
     * @throws Exception on failure
     */
    @BeforeAll
    static void startServer() throws Exception {
        dataDir = Files.createTempDirectory("distributed-complete");
        Path table = Files.createDirectories(dataDir.resolve("s").resolve("t"));
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = duck.createStatement()) {
            for (int f = 0; f < FILES; f++) {
                // File 3 has no row with k = 9; v is unique over the table.
                statement.execute("COPY (SELECT (i % " + (f == 3 ? 9 : 10) + ")::INTEGER AS k,"
                        + " (i * " + FILES + " + " + f + ")::BIGINT AS v, (i / 4.0)::DOUBLE AS d,"
                        + " 'n' || (i % 13) AS name,"
                        + " ((i * 7919 + " + f + ") % 100003 / 100.0)::DECIMAL(15,2) AS amount"
                        + " FROM range(1000) r(i))"
                        + " TO '" + table.resolve("part-" + f + ".parquet") + "' (FORMAT PARQUET)");
            }
        }
        System.setProperty("endpointsPerServer", String.valueOf(FILES));
        // A local, unshared dataDir is pruned only when the setting is given (Q-40b-1); item 48 needs it.
        System.setProperty("fileStatsPruningMaxFiles", "1024");
        helper = TestFlightServerHelper.builder().dataDir(dataDir.toString()).resultMode("complete")
                .start();
        peerAllocator = helper.allocator.newChildAllocator("peers", 0, Long.MAX_VALUE);
    }

    /**
     * Stops the server and removes the table.
     *
     * @throws Exception on failure
     */
    @AfterAll
    static void stopServer() throws Exception {
        try {
            useDistributed(null);
            peerAllocator.close();
            helper.close();
        } finally {
            System.clearProperty("endpointsPerServer");
            System.clearProperty("fileStatsPruningMaxFiles");
            try (var files = Files.walk(dataDir)) {
                files.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    /**
     * Turns the distributed path off after each test.
     *
     * @throws Exception on failure
     */
    @AfterEach
    void off() throws Exception {
        useDistributed(null);
    }

    /** Queries whose shape merges. */
    private static final List<String> MERGED = List.of(
            "SELECT count(*) FROM s.t",
            "SELECT count(*) AS n, sum(v) AS total, min(d) AS lo, max(name) AS hi FROM s.t",
            "SELECT k, count(*) AS n, sum(v) AS total, min(v) AS lo, max(d) AS hi FROM s.t"
                    + " GROUP BY k ORDER BY k",
            "SELECT DISTINCT name FROM s.t ORDER BY name",
            "SELECT v, name FROM s.t WHERE v % 7 = 0 ORDER BY v DESC LIMIT 5",
            "SELECT v, k FROM s.t WHERE v < 40 ORDER BY 1",
            "SELECT k, count(*) AS n FROM s.t WHERE k = 9 GROUP BY k ORDER BY k",
            "SELECT avg(v) AS a FROM s.t",
            "SELECT k, avg(amount) AS aa, count(*) AS n, avg(v), avg(d) AS ad FROM s.t"
                    + " GROUP BY k ORDER BY aa DESC",
            "SELECT k, avg(CASE WHEN k < 5 THEN amount END) AS half FROM s.t GROUP BY k ORDER BY 1");

    /**
     * Verifies merged answers equal the answers the node computes alone.
     *
     * @throws Exception on failure
     */
    @Test
    void mergedAnswerEqualsSingleNodeAnswer() throws Exception {
        for (String query : MERGED) {
            String alone = answer(query);
            useDistributed(DistributedComplete.clientFactory(peerAllocator, null));
            String merged = answer(query);
            assertEquals(alone, merged, query);
            assertEquals(1, distributed.mergedAnswers(), "merged from endpoint answers: " + query);
            assertEquals(0, distributed.fallbacks(), query);
            useDistributed(null);
        }
    }

    /** Plain scans: their merge only concatenates the endpoint answers (item 55 (a)). */
    private static final List<String> SCANS = List.of(
            "SELECT * FROM s.t",
            "SELECT v, name, amount FROM s.t WHERE v % 3 = 0");

    /**
     * Verifies a merged plain scan sends the endpoint batches on without DuckDB and answers the
     * rows the node computes alone, as the DuckDB merge does when forwarding is off (item 55 (a)).
     *
     * @throws Exception on failure
     */
    @Test
    void plainScansForwardEndpointBatches() throws Exception {
        for (String query : SCANS) {
            String alone = sortedRows(answer(query));
            useDistributed(DistributedComplete.clientFactory(peerAllocator, null));
            assertEquals(alone, sortedRows(answer(query)), query);
            assertEquals(1, distributed.mergedAnswers(), query);
            assertEquals(1, distributed.forwardedAnswers(), query);
            distributed.forwardScans(false);
            assertEquals(alone, sortedRows(answer(query)), query);
            assertEquals(2, distributed.mergedAnswers(), query);
            assertEquals(1, distributed.forwardedAnswers(), query);
            assertEquals(0, distributed.fallbacks(), query);
            useDistributed(null);
        }
        // Merges that compute something are never forwarded.
        useDistributed(DistributedComplete.clientFactory(peerAllocator, null));
        for (String query : MERGED) {
            answer(query);
        }
        assertEquals(MERGED.size(), distributed.mergedAnswers());
        assertEquals(0, distributed.forwardedAnswers());
    }

    /**
     * Verifies the size rule's file list comes from the catalog views, as the storage lists the
     * table, so that a local decision lists nothing per query (item 55 (a)).
     *
     * @throws Exception on failure
     */
    @Test
    void localSizeRuleUsesTheViewsFileList() throws Exception {
        answer("SELECT count(*) AS n FROM s.t");
        List<pro.surpin.data.arrowflight.spi.storage.DataFile> files = helper.executionService.viewFiles("s", "t");
        org.junit.jupiter.api.Assertions.assertNotNull(files);
        assertEquals(FILES, files.size());
        long bytes = files.stream().mapToLong(pro.surpin.data.arrowflight.spi.storage.DataFile::size).sum();
        assertEquals(helper.queryPlanner.localScanBytes("SELECT sum(v) AS total FROM s.t"), bytes);
        org.junit.jupiter.api.Assertions.assertNull(helper.executionService.viewFiles("s", "missing"));
    }

    /**
     * Sorts the row lines of an answer (a plain scan has no order), keeping its schema line first.
     *
     * @param answer schema line and rows as {@link #answer} gives them
     * @return schema line and sorted rows
     */
    private static String sortedRows(String answer) {
        List<String> lines = new java.util.ArrayList<>(List.of(answer.split("\n")));
        String schema = lines.remove(0);
        // Each batch repeats the TSV header line; keep the rows only.
        lines.removeIf(line -> line.startsWith("v\t") || line.startsWith("k\t"));
        java.util.Collections.sort(lines);
        return schema + "\n" + String.join("\n", lines) + "\nrows=" + lines.size();
    }

    /**
     * Verifies a query whose shape does not merge keeps the single-node path untouched.
     *
     * @throws Exception on failure
     */
    @Test
    void refusedShapeKeepsSingleNodePath() throws Exception {
        for (String query : List.of("SELECT k, count(*) AS n FROM s.t GROUP BY k ORDER BY k LIMIT 3",
                "SELECT avg(DISTINCT v) AS a FROM s.t",
                "SELECT k, avg(v) FROM s.t GROUP BY k ORDER BY avg(v)")) {
            String alone = answer(query);
            useDistributed(DistributedComplete.clientFactory(peerAllocator, null));
            assertEquals(alone, answer(query), query);
            assertEquals(0, distributed.mergedAnswers(), query);
            assertEquals(0, distributed.fallbacks(), query);
            useDistributed(null);
        }
    }

    /**
     * Verifies an unreachable peer before the first batch falls back to the single-node answer.
     *
     * @throws Exception on failure
     */
    @Test
    void unreachablePeerFallsBackToSingleNodeAnswer() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        Location dead = Location.forGrpcInsecure("localhost", closedPort);
        Function<Location, FlightClient> factory = DistributedComplete.clientFactory(peerAllocator, null);
        for (String query : List.of(
                "SELECT k, count(*) AS n, sum(v) AS total FROM s.t GROUP BY k ORDER BY k",
                "SELECT v FROM s.t WHERE v < 40 ORDER BY v")) {
            String alone = answer(query);
            useDistributed(location -> factory.apply(dead));
            assertEquals(alone, answer(query), query);
            assertEquals(0, distributed.mergedAnswers(), query);
            assertEquals(1, distributed.fallbacks(), query);
            useDistributed(null);
        }
    }

    /**
     * Verifies concurrent merges wait for room among the node's endpoint streams instead of
     * opening them all at once (item 35: eight concurrent full scans exhausted direct memory),
     * and then answer exactly: with room for one merge's four streams held by the test, six
     * concurrent full scans all wait, and once it is returned they run one after another.
     *
     * @throws Exception on failure
     */
    @Test
    void concurrentMergesWaitForRoomAndStayExact() throws Exception {
        String query = "SELECT v, k, d, name, amount FROM s.t ORDER BY v";
        String alone = answer(query);
        PeerStreamAdmission admission = new PeerStreamAdmission(FILES, 60_000);
        useDistributed(DistributedComplete.clientFactory(peerAllocator, null), admission);
        int clients = 6;
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        try {
            List<Future<String>> answers = new ArrayList<>();
            try (PeerStreamAdmission.Admitted held = admission.admit(FILES, () -> false, "test")) {
                assertEquals(0, admission.available());
                for (int i = 0; i < clients; i++) {
                    answers.add(pool.submit(() -> answer(query)));
                }
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (admission.waited() < clients && System.nanoTime() < deadline) {
                    Thread.sleep(20);
                }
                assertEquals(clients, admission.waited(), "every merge waits while the room is held");
                assertEquals(0, distributed.mergedAnswers());
            }
            for (Future<String> answer : answers) {
                assertEquals(alone, answer.get(60, TimeUnit.SECONDS));
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals(clients, distributed.mergedAnswers());
        assertEquals(0, distributed.fallbacks());
        assertEquals(FILES, admission.available(), "every merge returned its room");
        assertEquals(0, admission.refused());
    }

    /**
     * Verifies one-row merges (aggregates without GROUP BY, AVG included) do not queue behind
     * merges that hold all the room (item 39 (d): dc small-query tails behind full scans), and
     * answer exactly while the room stays held.
     *
     * @throws Exception on failure
     */
    @Test
    void oneRowMergesDoNotWaitForRoom() throws Exception {
        List<String> queries = List.of("SELECT count(*) FROM s.t",
                "SELECT count(*) AS n, sum(v) AS total, min(d) AS lo FROM s.t WHERE k = 9",
                "SELECT avg(v) AS a FROM s.t");
        List<String> alone = new ArrayList<>();
        for (String query : queries) {
            alone.add(answer(query));
        }
        PeerStreamAdmission admission = new PeerStreamAdmission(FILES, 200);
        useDistributed(DistributedComplete.clientFactory(peerAllocator, null), admission);
        try (PeerStreamAdmission.Admitted held = admission.admit(FILES, () -> false, "scan")) {
            for (int i = 0; i < queries.size(); i++) {
                assertEquals(alone.get(i), answer(queries.get(i)), queries.get(i));
            }
            assertEquals(0, admission.available());
        }
        assertEquals(queries.size(), distributed.mergedAnswers());
        assertEquals(0, distributed.fallbacks());
        assertEquals(0, admission.waited());
        assertEquals(0, admission.refused());
        assertEquals(queries.size(), admission.unmetered());
        assertEquals(FILES, admission.available());
    }

    /**
     * Verifies point lookups (an equality filter) do not queue behind merges that hold all the
     * room (item 43: 61-63 % of dc point lookups waited for admission, 217 q/s against 1348
     * without distributed complete), and answer exactly while the room stays held.
     *
     * @throws Exception on failure
     */
    @Test
    void pointLookupsDoNotWaitForRoom() throws Exception {
        List<String> queries = List.of("SELECT v, k, d, name, amount FROM s.t WHERE v = 41",
                "SELECT v, name FROM s.t WHERE k = 9 AND v < 400 ORDER BY v",
                "SELECT v FROM s.t ORDER BY v DESC LIMIT 3");
        List<String> alone = new ArrayList<>();
        for (String query : queries) {
            alone.add(answer(query));
        }
        PeerStreamAdmission admission = new PeerStreamAdmission(FILES, 200);
        useDistributed(DistributedComplete.clientFactory(peerAllocator, null), admission);
        try (PeerStreamAdmission.Admitted held = admission.admit(FILES, () -> false, "scan")) {
            for (int i = 0; i < queries.size(); i++) {
                assertEquals(alone.get(i), answer(queries.get(i)), queries.get(i));
            }
            assertEquals(0, admission.available());
        }
        assertEquals(queries.size(), distributed.mergedAnswers());
        assertEquals(0, distributed.fallbacks());
        assertEquals(0, admission.waited());
        assertEquals(0, admission.refused());
        assertEquals(queries.size(), admission.probation());
        assertEquals(0, admission.charged());
        assertEquals(FILES, admission.available());
    }

    /**
     * Verifies {@code distributedCompletePointLookups=local}: a point lookup is answered exactly by
     * this node alone (it reads the files), without merging and without admission (item 43).
     *
     * @throws Exception on failure
     */
    @Test
    void localPointLookupsAreComputedHere() throws Exception {
        String query = "SELECT v, k, d, name, amount FROM s.t WHERE v = 41";
        String alone = answer(query);
        String count = "SELECT k, count(*) AS n FROM s.t WHERE k = 9 GROUP BY k";
        String countAlone = answer(count);
        String filteredCount = "SELECT count(*) AS n FROM s.t WHERE k = 9";
        String filteredCountAlone = answer(filteredCount);
        PeerStreamAdmission admission = new PeerStreamAdmission(FILES, 200);
        useDistributed(DistributedComplete.clientFactory(peerAllocator, null), admission);
        distributed.pointLookupsLocal(true);
        assertEquals(alone, answer(query));
        assertEquals(alone, answer(query));
        assertEquals(filteredCountAlone, answer(filteredCount));
        assertEquals(3, distributed.localPointLookups());
        assertEquals(0, distributed.mergedAnswers());
        assertEquals(0, distributed.fallbacks());
        assertEquals(0, admission.probation());
        // Grouped aggregates are still merged.
        assertEquals(countAlone, answer(count));
        assertEquals(1, distributed.mergedAnswers());
    }

    /**
     * Verifies item 48: with point lookups local, a filtered one-row aggregate whose footer
     * statistics keep at most {@code distributedCompleteLocalAggregateMaxFiles} files is answered
     * exactly by this node alone; more kept files, an unpruned filter, a bound of 0 and grouped
     * aggregates are still merged, and every answer equals the node's own.
     *
     * @throws Exception on failure
     */
    @Test
    void filteredOneRowAggregatesOverFewPrunedFilesAreComputedHere() throws Exception {
        // File 3 has no k = 9 (its k max is 8): the filter keeps 3 of the 4 files.
        String pruned = "SELECT count(*) AS n, sum(v) AS total, min(d) AS lo FROM s.t WHERE k > 8";
        String averaged = "SELECT avg(v) AS a, count(*) AS n FROM s.t WHERE k BETWEEN 9 AND 20";
        String unpruned = "SELECT count(*) AS n, sum(v) AS total FROM s.t WHERE k > 2";
        String grouped = "SELECT k, sum(v) AS total FROM s.t WHERE k > 8 GROUP BY k";
        String prunedAlone = answer(pruned);
        String averagedAlone = answer(averaged);
        String unprunedAlone = answer(unpruned);
        String groupedAlone = answer(grouped);
        PeerStreamAdmission admission = new PeerStreamAdmission(FILES, 200);
        useDistributed(DistributedComplete.clientFactory(peerAllocator, null), admission);
        distributed.pointLookupsLocal(true);
        distributed.localScanMaxBytes(0);   // the file bound alone (item 50's size rule off)
        try {
            distributed.localAggregateMaxFiles(3);
            assertEquals(prunedAlone, answer(pruned));
            assertEquals(averagedAlone, answer(averaged));
            assertEquals(2, distributed.localAggregates());
            assertEquals(0, distributed.mergedAnswers());
            assertEquals(unprunedAlone, answer(unpruned));
            assertEquals(groupedAlone, answer(grouped));
            assertEquals(2, distributed.mergedAnswers());
            distributed.localAggregateMaxFiles(2);
            assertEquals(prunedAlone, answer(pruned));
            distributed.localAggregateMaxFiles(0);
            assertEquals(prunedAlone, answer(pruned));
            assertEquals(2, distributed.localAggregates());
            assertEquals(4, distributed.mergedAnswers());
            assertEquals(0, distributed.fallbacks());
            assertThrows(IllegalArgumentException.class, () -> distributed.localAggregateMaxFiles(-1));
        } finally {
            distributed.localAggregateMaxFiles(DistributedComplete.DEFAULT_LOCAL_AGGREGATE_MAX_FILES);
        }
    }

    /**
     * Verifies item 50: with point lookups local, one-row aggregates whose filter prunes nothing
     * and small LIMIT queries over files of at most {@code distributedCompleteLocalScanMaxBytes}
     * are answered exactly by this node alone, an unfiltered {@code count(*)} at any size; larger
     * files, a bound of 0 and grouped aggregates are merged, and every answer equals the node's own.
     *
     * @throws Exception on failure
     */
    @Test
    void smallScansAndUnfilteredCountsAreComputedHere() throws Exception {
        String unpruned = "SELECT count(*) AS n, sum(v) AS total, min(d) AS lo FROM s.t WHERE k > 2";
        String averaged = "SELECT avg(v) AS a, count(*) AS n FROM s.t WHERE d > 10";
        String unfiltered = "SELECT sum(v) AS total FROM s.t";
        String topN = "SELECT v, name, amount FROM s.t WHERE k < 5 ORDER BY v DESC LIMIT 7";
        String count = "SELECT count(*) AS n FROM s.t";
        String grouped = "SELECT k, sum(v) AS total FROM s.t GROUP BY k ORDER BY k";
        String bigLimit = "SELECT v FROM s.t ORDER BY v LIMIT 20000";
        List<String> local = List.of(unpruned, averaged, unfiltered, topN);
        List<String> alone = new ArrayList<>();
        for (String query : local) {
            alone.add(answer(query));
        }
        String countAlone = answer(count);
        String groupedAlone = answer(grouped);
        String bigLimitAlone = answer(bigLimit);
        PeerStreamAdmission admission = new PeerStreamAdmission(FILES, 200);
        useDistributed(DistributedComplete.clientFactory(peerAllocator, null), admission);
        distributed.pointLookupsLocal(true);
        distributed.localAggregateMaxFiles(0);   // the size rule alone (item 48's file bound off)
        for (int i = 0; i < local.size(); i++) {
            assertEquals(alone.get(i), answer(local.get(i)), local.get(i));
        }
        assertEquals(countAlone, answer(count));
        assertEquals(4, distributed.localAggregates());
        assertEquals(1, distributed.localScans());
        assertEquals(0, distributed.mergedAnswers());
        assertEquals(groupedAlone, answer(grouped));
        assertEquals(bigLimitAlone, answer(bigLimit));
        assertEquals(2, distributed.mergedAnswers());
        // The table's files are larger than 1 KiB: merged, except the row count.
        distributed.localScanMaxBytes(1024);
        for (int i = 0; i < local.size(); i++) {
            assertEquals(alone.get(i), answer(local.get(i)), local.get(i));
        }
        assertEquals(countAlone, answer(count));
        assertEquals(2 + local.size(), distributed.mergedAnswers());
        distributed.localScanMaxBytes(0);
        assertEquals(alone.get(0), answer(unpruned));
        assertEquals(countAlone, answer(count));
        assertEquals(3 + local.size(), distributed.mergedAnswers());
        assertEquals(6, distributed.localAggregates());
        assertEquals(1, distributed.localScans());
        assertEquals(0, distributed.fallbacks());
        assertEquals(0, admission.waited());
        assertThrows(IllegalArgumentException.class, () -> distributed.localScanMaxBytes(-1));
    }

    /**
     * Verifies item 55: when only big aggregates are merged ({@code distributedAggregateMinBytes}
     * without distributedComplete), aggregates over at least that many bytes after footer pruning
     * are merged, smaller ones (also by pruning) and queries without aggregate are computed here,
     * and every answer equals the node's own.
     *
     * @throws Exception on failure
     */
    @Test
    void onlyBigAggregatesAreMergedWhenAsked() throws Exception {
        List<String> aggregates = List.of(
                "SELECT count(*) AS n, sum(v) AS total, min(d) AS lo, max(name) AS hi FROM s.t",
                "SELECT k, count(*) AS n, sum(v) AS total FROM s.t GROUP BY k ORDER BY k",
                "SELECT k, avg(amount) AS aa, count(*) AS n FROM s.t GROUP BY k ORDER BY aa DESC",
                "SELECT sum(v) AS total FROM s.t WHERE k > 2");
        String scan = "SELECT v, k FROM s.t WHERE v < 40 ORDER BY 1";
        String distinct = "SELECT DISTINCT name FROM s.t ORDER BY name";
        // No file holds v >= 1 000 000: footer pruning keeps none, so the aggregate reads 0 bytes.
        String prunedAway = "SELECT count(*) AS n, sum(v) AS total FROM s.t WHERE v >= 1000000";
        List<String> others = List.of(scan, distinct, prunedAway);
        List<String> aloneAggregates = new ArrayList<>();
        for (String query : aggregates) {
            aloneAggregates.add(answer(query));
        }
        List<String> aloneOthers = new ArrayList<>();
        for (String query : others) {
            aloneOthers.add(answer(query));
        }
        useDistributed(DistributedComplete.clientFactory(peerAllocator, null));
        distributed.aggregatesOnly(1);
        for (int i = 0; i < aggregates.size(); i++) {
            assertEquals(aloneAggregates.get(i), answer(aggregates.get(i)), aggregates.get(i));
            assertEquals(i + 1, distributed.mergedAnswers(), "merged: " + aggregates.get(i));
        }
        for (int i = 0; i < others.size(); i++) {
            assertEquals(aloneOthers.get(i), answer(others.get(i)), others.get(i));
        }
        assertEquals(aggregates.size(), distributed.mergedAnswers(), "no aggregate or 0 bytes: computed here");
        assertEquals(others.size(), distributed.notBigEnough());
        // The table is far below 1 TiB: nothing is merged.
        distributed.aggregatesOnly(1L << 40);
        for (int i = 0; i < aggregates.size(); i++) {
            assertEquals(aloneAggregates.get(i), answer(aggregates.get(i)), aggregates.get(i));
        }
        assertEquals(aggregates.size(), distributed.mergedAnswers());
        assertEquals(others.size() + aggregates.size(), distributed.notBigEnough());
        // 0: distributedComplete again, every mergeable shape merged.
        distributed.aggregatesOnly(0);
        assertEquals(aloneOthers.get(0), answer(scan));
        assertEquals(aggregates.size() + 1, distributed.mergedAnswers());
        assertEquals(0, distributed.fallbacks());
        assertThrows(IllegalArgumentException.class, () -> distributed.aggregatesOnly(-1));
    }

    /**
     * Verifies a merge started on probation whose answer turns out large takes its room once it
     * has read more than the probation bytes, waits for it while other merges hold it, and still
     * answers exactly (item 43: a wrong guess costs no correctness and keeps the item 35 bound).
     *
     * @throws Exception on failure
     */
    @Test
    void aGrownPointLookupWaitsForRoomAndStaysExact() throws Exception {
        String query = "SELECT v, k, d, name, amount FROM s.t WHERE name = 'n3' ORDER BY v";
        String alone = answer(query);
        PeerStreamAdmission admission = new PeerStreamAdmission(FILES, 60_000, 1);
        useDistributed(DistributedComplete.clientFactory(peerAllocator, null), admission);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<String> answer;
            try (PeerStreamAdmission.Admitted held = admission.admit(FILES, () -> false, "scan")) {
                answer = pool.submit(() -> answer(query));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (admission.waited() < 1 && System.nanoTime() < deadline) {
                    Thread.sleep(20);
                }
                assertEquals(1, admission.waited(), "the grown merge waits for room");
                assertEquals(1, admission.charged());
                assertFalse(answer.isDone());
            }
            assertEquals(alone, answer.get(60, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }
        assertEquals(1, distributed.mergedAnswers());
        assertEquals(0, distributed.fallbacks());
        assertEquals(FILES, admission.available(), "the grown merge returned its room");
    }

    /**
     * Verifies a merge that finds no room within the wait is refused with RESOURCE_EXHAUSTED,
     * which a client may retry, and the node then answers again once room is back.
     *
     * @throws Exception on failure
     */
    @Test
    void mergeWithoutRoomIsRefusedAsResourceExhausted() throws Exception {
        String query = "SELECT k, count(*) AS n FROM s.t GROUP BY k ORDER BY k";
        String alone = answer(query);
        PeerStreamAdmission admission = new PeerStreamAdmission(FILES, 200);
        useDistributed(DistributedComplete.clientFactory(peerAllocator, null), admission);
        try (PeerStreamAdmission.Admitted held = admission.admit(1, () -> false, "test")) {
            FlightRuntimeException refused = assertThrows(FlightRuntimeException.class, () -> answer(query));
            assertEquals(FlightStatusCode.RESOURCE_EXHAUSTED, refused.status().code(), refused.getMessage());
            assertTrue(refused.getMessage().contains("retry later"), refused.getMessage());
        }
        assertEquals(1, admission.refused());
        assertEquals(alone, answer(query));
        assertEquals(1, distributed.mergedAnswers());
        assertEquals(FILES, admission.available());
    }

    /**
     * Installs a distributed path on the producer, closing the previous one.
     *
     * @param factory peer client factory, or null to turn the path off
     * @throws Exception on failure
     */
    private static void useDistributed(Function<Location, FlightClient> factory) throws Exception {
        useDistributed(factory, new PeerStreamAdmission(64, PeerStreamAdmission.DEFAULT_WAIT_MILLIS));
    }

    /**
     * Installs a distributed path with an admission on the producer, closing the previous one.
     *
     * @param factory peer client factory, or null to turn the path off
     * @param admission bounds the endpoint streams of concurrent merges
     * @throws Exception on failure
     */
    private static void useDistributed(Function<Location, FlightClient> factory, PeerStreamAdmission admission)
            throws Exception {
        helper.flightSqlProducer.useDistributedComplete(null);
        if (distributed != null) {
            distributed.close();
            distributed = null;
        }
        if (factory != null) {
            distributed = new DistributedComplete(helper.queryPlanner, helper.clusterService,
                    helper.executionService, peerAllocator, factory, admission);
            helper.flightSqlProducer.useDistributedComplete(distributed);
        }
    }

    /**
     * Reads the complete answer of a query as text, with its schema.
     *
     * @param query client SQL
     * @return schema and rows
     * @throws Exception on failure
     */
    private static String answer(String query) throws Exception {
        FlightInfo info = helper.sqlClient().execute(query);
        assertEquals(1, info.getEndpoints().size());
        StringBuilder text = new StringBuilder();
        try (FlightStream stream = helper.sqlClient().getStream(info.getEndpoints().get(0).getTicket())) {
            text.append(stream.getSchema()).append('\n');
            while (stream.next()) {
                text.append(stream.getRoot().contentToTSVString());
            }
        }
        return text.toString();
    }
}
