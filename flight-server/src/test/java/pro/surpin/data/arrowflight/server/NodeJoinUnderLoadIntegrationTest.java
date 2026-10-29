package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.server.services.DistributedComplete;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A node joins a cluster under query load (lane 3, D9): two complete-mode nodes in one JVM on a
 * shared data directory answer a loop of queries from three clients (merged complete answers on
 * the first node, single-node complete answers on the second, and partial-mode answers read
 * endpoint by endpoint from their locations) while a third node starts and joins. No answer may
 * differ from the two-node cluster's and no call may fail; partial plans must start using the new
 * node, and the merged answers of the first node must read endpoints from it. Once the load
 * stops, every node's tracked load must be back to zero: identical plans of concurrent clients
 * share their (deterministic) endpoint tickets, and their load reservations used to overwrite
 * each other, so a node kept bytes as load for good and later plans left it out. Prints the join
 * timings (start -> member up, start -> first plan with the new node).
 */
@Tag("integration")
class NodeJoinUnderLoadIntegrationTest {

    private static final int FILES = 6;
    private static final int ROWS = 20_000;
    private static final int CLIENTS = 3;
    /** Longest the plans may take to use the joined node. */
    private static final long JOIN_PLANNED_SECONDS = 60;
    private static Path dataDir;
    private static final List<TestFlightServerHelper> NODE = new CopyOnWriteArrayList<>();
    private static String[] seeds;
    private static int[] flightPorts;
    private static BufferAllocator clientAllocator;
    private static BufferAllocator peerAllocator;
    private static DistributedComplete distributed;

    /** Complete-mode queries (merged on the first node); unordered ones compare as row sets. */
    private static final List<String> COMPLETE = List.of(
            "SELECT count(*) AS n, sum(v) AS total, min(d) AS lo, max(name) AS hi FROM s.t",
            "SELECT k, count(*) AS n, sum(v) AS total FROM s.t GROUP BY k ORDER BY k",
            "SELECT DISTINCT name FROM s.t ORDER BY name",
            "SELECT v, name FROM s.t WHERE v % 7 = 0 ORDER BY v DESC LIMIT 5",
            "SELECT v, k FROM s.t WHERE v % 50 = 1");

    /** Partial-mode query, read endpoint by endpoint. */
    private static final String PARTIAL = "SELECT v, k, name FROM s.t WHERE v % 10 = 3";

    /**
     * Writes a table of six files and starts two of three complete-mode nodes, two endpoints per
     * node and query; the seeds name all three members.
     *
     * @throws Exception on failure
     */
    @BeforeAll
    static void startCluster() throws Exception {
        dataDir = Files.createTempDirectory("node-join");
        Path table = Files.createDirectories(dataDir.resolve("s").resolve("t"));
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = duck.createStatement()) {
            for (int f = 0; f < FILES; f++) {
                statement.execute("COPY (SELECT (i % 10)::INTEGER AS k, (i * " + FILES + " + " + f
                        + ")::BIGINT AS v, (i / 4.0)::DOUBLE AS d, 'n' || (i % 13) AS name FROM range("
                        + ROWS + ") r(i)) TO '" + table.resolve("part-" + f + ".parquet") + "' (FORMAT PARQUET)");
            }
        }
        seeds = new String[3];
        flightPorts = new int[3];
        for (int i = 0; i < 3; i++) {
            seeds[i] = "127.0.0.1:" + TestPorts.free();
            flightPorts[i] = TestPorts.free();
        }
        for (int i = 0; i < 2; i++) {
            NODE.add(start(i));
        }
        clientAllocator = new RootAllocator();
        peerAllocator = NODE.get(0).allocator.newChildAllocator("peers", 0, Long.MAX_VALUE);
        distributed = new DistributedComplete(NODE.get(0).queryPlanner, NODE.get(0).clusterService,
                NODE.get(0).executionService, peerAllocator,
                DistributedComplete.clientFactory(peerAllocator, null));
        NODE.get(0).flightSqlProducer.useDistributedComplete(distributed);
    }

    /**
     * Stops the nodes and removes the table.
     *
     * @throws Exception on failure
     */
    @AfterAll
    static void stopCluster() throws Exception {
        try {
            if (!NODE.isEmpty()) {
                NODE.get(0).flightSqlProducer.useDistributedComplete(null);
            }
            if (distributed != null) {
                distributed.close();
            }
            if (peerAllocator != null) {
                peerAllocator.close();
            }
            for (int i = NODE.size() - 1; i >= 0; i--) {
                NODE.get(i).close();
            }
            if (clientAllocator != null) {
                clientAllocator.close();
            }
        } finally {
            try (var files = Files.walk(dataDir)) {
                files.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    /**
     * Runs the load, starts the third node, waits until plans use it, and checks every answer.
     *
     * @throws Exception on failure
     */
    @Test
    void nodeJoiningUnderLoadIsPlannedWithoutErrorsOrWrongAnswers() throws Exception {
        awaitPartialPlanNodes(2);
        Map<String, String> reference = new ConcurrentHashMap<>();
        for (String query : COMPLETE) {
            reference.put(query, complete(1, query));
        }
        reference.put(PARTIAL, partial(1, PARTIAL, location -> { }));
        for (String query : COMPLETE) {
            assertEquals(reference.get(query), complete(0, query), "merged vs single node: " + query);
        }
        assertTrue(distributed.mergedAnswers() >= COMPLETE.size(), "the first node merges");

        String joined = Location.forGrpcInsecure("localhost", flightPorts[2]).getUri().toString();
        Map<String, Long> firstPlanned = new ConcurrentHashMap<>();
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong answers = new AtomicLong();
        List<String> failures = new CopyOnWriteArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(CLIENTS);
        List<Future<?>> runs = new ArrayList<>();
        for (int c = 0; c < CLIENTS; c++) {
            int client = c;
            runs.add(pool.submit(() -> {
                long call = client;
                while (!stop.get()) {
                    List<String> queries = new ArrayList<>(COMPLETE);
                    queries.add(PARTIAL);
                    String query = queries.get((int) (call % queries.size()));
                    int node = (int) (call / queries.size() % 2);
                    call++;
                    try {
                        String answer = PARTIAL.equals(query)
                                ? partial(node, query, uri -> firstPlanned.putIfAbsent(uri, System.nanoTime()))
                                : complete(node, query);
                        if (!reference.get(query).equals(answer)) {
                            failures.add("wrong answer on node " + node + ": " + query);
                        }
                        answers.incrementAndGet();
                    } catch (Exception | AssertionError e) {
                        failures.add("node " + node + ": " + query + ": " + e);
                    }
                }
                return null;
            }));
        }
        try {
            Thread.sleep(1500);
            long mergedBefore = distributed.mergedAnswers();
            long start = System.nanoTime();
            NODE.add(start(2));
            long up = System.nanoTime();
            long deadline = up + TimeUnit.SECONDS.toNanos(JOIN_PLANNED_SECONDS);
            while (!firstPlanned.containsKey(joined) && System.nanoTime() < deadline && failures.isEmpty()) {
                Thread.sleep(50);
            }
            long planned = firstPlanned.getOrDefault(joined, -1L);
            // Keep the load on the three-node cluster for a while.
            Thread.sleep(1500);
            stop.set(true);
            for (Future<?> run : runs) {
                run.get(2, TimeUnit.MINUTES);
            }
            System.out.printf(java.util.Locale.ROOT,
                    "join: member up %d ms after start, first plan with it %d ms after up; %d answers,"
                            + " %d merged during the join, %d failures%n",
                    TimeUnit.NANOSECONDS.toMillis(up - start),
                    planned < 0 ? -1 : TimeUnit.NANOSECONDS.toMillis(planned - up), answers.get(),
                    distributed.mergedAnswers() - mergedBefore, failures.size());
            assertEquals(List.of(), failures.subList(0, Math.min(10, failures.size())), "failures under the join");
            assertTrue(planned > 0, "no partial plan used the joined node " + joined + " within "
                    + JOIN_PLANNED_SECONDS + " s; planned: " + firstPlanned.keySet());
            assertLoadsReturnToZero();
        } finally {
            stop.set(true);
            pool.shutdownNow();
        }
        // After the join the first node's merged answers (planned as the partial ones) read the new
        // node's endpoints too.
        awaitPartialPlanNodes(3);
        long merged = distributed.mergedAnswers();
        for (String query : COMPLETE) {
            assertEquals(reference.get(query), complete(0, query), "merged on three nodes: " + query);
        }
        assertEquals(merged + COMPLETE.size(), distributed.mergedAnswers(), "merged on three nodes");
        assertEquals(0, distributed.fallbacks(), "no merged answer fell back to one node");
    }

    /**
     * Checks every node's tracked load returns to zero once no query runs.
     *
     * @throws InterruptedException when interrupted
     */
    private static void assertLoadsReturnToZero() throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        Map<String, Long> loads = NODE.get(0).clusterService.allServerLoads();
        while (loads.values().stream().anyMatch(load -> load != 0L) && System.nanoTime() < deadline) {
            Thread.sleep(100);
            loads = NODE.get(0).clusterService.allServerLoads();
        }
        assertTrue(loads.values().stream().allMatch(load -> load == 0L), "load left after the queries: " + loads);
    }

    /**
     * Starts node {@code i} of the cluster.
     *
     * @param i node index
     * @return the node
     * @throws Exception on failure
     */
    private static TestFlightServerHelper start(int i) throws Exception {
        System.setProperty("endpointsPerServer", "2");
        try {
            return TestFlightServerHelper.builder().dataDir(dataDir.toString()).resultMode("complete")
                    .sharedStorage().hazelcastPort(Integer.parseInt(seeds[i].split(":")[1]))
                    .flightPort(flightPorts[i]).hazelcastSeeds(seeds).start();
        } finally {
            System.clearProperty("endpointsPerServer");
        }
    }

    /**
     * Waits until a partial plan on the first node has endpoints on the given number of nodes.
     *
     * @param nodes expected nodes
     * @throws Exception when it does not happen within a minute
     */
    private static void awaitPartialPlanNodes(int nodes) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        java.util.Set<String> seen = new java.util.HashSet<>();
        while (System.nanoTime() < deadline) {
            seen.clear();
            partial(0, PARTIAL, seen::add);
            if (seen.size() == nodes) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("partial plans span " + seen + ", not " + nodes + " nodes; loads "
                + NODE.get(0).clusterService.allServerLoads());
    }

    /**
     * Reads a node's complete answer of a query as text (schema, row count, rows; sorted unless
     * ordered).
     *
     * @param node  node index
     * @param query client SQL
     * @return answer text
     * @throws Exception on failure
     */
    private static String complete(int node, String query) throws Exception {
        FlightSqlClient client = NODE.get(node).sqlClient();
        CallOption mode = mode("complete");
        FlightInfo info = client.execute(query, mode);
        assertEquals(1, info.getEndpoints().size(), "one complete endpoint: " + query);
        StringBuilder rows = new StringBuilder();
        String schema;
        try (FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket(), mode)) {
            schema = stream.getSchema().toString();
            while (stream.next()) {
                rows.append(stream.getRoot().contentToTSVString().lines().skip(1)
                        .reduce(new StringBuilder(), (b, l) -> b.append(l).append('\n'), StringBuilder::append));
            }
        }
        return text(schema, rows, query.contains("ORDER BY"));
    }

    /**
     * Reads a node's partial answer of a query, every endpoint from its first location, as sorted
     * text.
     *
     * @param node    node index
     * @param query   client SQL
     * @param planned receives the URI of every endpoint's first location
     * @return answer text
     * @throws Exception on failure
     */
    private static String partial(int node, String query, java.util.function.Consumer<String> planned)
            throws Exception {
        CallOption mode = mode("partial");
        FlightInfo info = NODE.get(node).sqlClient().execute(query, mode);
        StringBuilder rows = new StringBuilder();
        String schema = info.getSchemaOptional().map(Object::toString).orElse("");
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            Location location = endpoint.getLocations().isEmpty() ? NODE.get(node).location
                    : endpoint.getLocations().get(0);
            planned.accept(location.getUri().toString());
            try (FlightClient flight = FlightClient.builder(clientAllocator, location).build();
                    FlightStream stream = new FlightSqlClient(flight).getStream(endpoint.getTicket(), mode)) {
                while (stream.next()) {
                    rows.append(stream.getRoot().contentToTSVString().lines().skip(1)
                            .reduce(new StringBuilder(), (b, l) -> b.append(l).append('\n'),
                                    StringBuilder::append));
                }
            }
        }
        return text(schema, rows, false);
    }

    /**
     * Formats an answer.
     *
     * @param schema  schema text
     * @param rows    rows, one per line
     * @param ordered whether the row order is part of the answer
     * @return answer text
     */
    private static String text(String schema, CharSequence rows, boolean ordered) {
        String[] lines = rows.toString().split("\n");
        if (!ordered) {
            Arrays.sort(lines);
        }
        return schema + "\n" + lines.length + " rows\n" + String.join("\n", lines);
    }

    /**
     * Call option setting the result mode.
     *
     * @param mode {@code complete} or {@code partial}
     * @return header option
     */
    private static CallOption mode(String mode) {
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert("x-flight-result-mode", mode);
        return new HeaderCallOption(headers);
    }
}
