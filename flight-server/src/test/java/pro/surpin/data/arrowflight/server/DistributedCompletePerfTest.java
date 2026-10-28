package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.server.services.DistributedComplete;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Measures distributed complete mode (item 18 step 3, D2 (b)) on three complete-mode nodes in one
 * JVM over one shared lineitem-like table: for each query shape that merges, the first node's
 * complete answer with {@code distributedComplete} off (the node reads all files alone) and on
 * (endpoint answers read from all nodes and merged). Prints the median wall time of both, the
 * Arrow bytes read from peers and the share of the merge spent waiting for peer batches (the
 * input of the parallel-prefetch decision, D1 follow-up (b)); asserts only that both answers are
 * equal and that the on runs merged. Amounts are DECIMAL, as in TPC-H: a DOUBLE sum depends on
 * the order of its terms, so merged and single-node DOUBLE sums may differ in the last digits. The nodes share the machine's cores, so the numbers show the
 * merge's overhead, not a cluster's speed-up. {@code -Dperf.rows}, {@code -Dperf.runs} and
 * {@code -Dperf.endpointsPerServer} pick the size.
 */
@Tag("perf")
class DistributedCompletePerfTest {

    private static final int NODES = 3;
    private static final int FILES = 12;
    private static final long ROWS = Long.getLong("perf.rows", 6_000_000L);
    private static final int RUNS = Integer.getInteger("perf.runs", 3);

    /** Query shapes that merge, by label. */
    private static final Map<String, String> QUERIES = new LinkedHashMap<>();

    static {
        QUERIES.put("Q1-like GROUP BY", "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty,"
                + " sum(l_extendedprice) AS sum_price, count(*) AS count_order FROM s.lineitem"
                + " WHERE l_shipdate <= DATE '1998-09-02' GROUP BY l_returnflag, l_linestatus"
                + " ORDER BY l_returnflag, l_linestatus");
        QUERIES.put("Q6-like global aggregate", "SELECT sum(l_extendedprice) AS revenue, count(*) AS n"
                + " FROM s.lineitem WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'"
                + " AND l_quantity < 24");
        QUERIES.put("wide DISTINCT", "SELECT DISTINCT l_partkey FROM s.lineitem ORDER BY l_partkey");
        QUERIES.put("ORDER BY LIMIT", "SELECT l_orderkey, l_extendedprice FROM s.lineitem"
                + " ORDER BY l_extendedprice DESC, l_orderkey LIMIT 100");
        QUERIES.put("wide GROUP BY", "SELECT l_partkey, count(*) AS n, max(l_extendedprice) AS hi"
                + " FROM s.lineitem GROUP BY l_partkey ORDER BY l_partkey");
        QUERIES.put("wide rows", "SELECT l_orderkey, l_partkey, l_comment FROM s.lineitem"
                + " WHERE l_linenumber = 1");
    }

    /**
     * Prints off/on wall times, peer bytes and peer wait for each query shape.
     *
     * @param dir temporary data directory
     * @throws Exception on failure
     */
    @Test
    void printDistributedCompleteOffVersusOn(@TempDir Path dir) throws Exception {
        writeTable(Files.createDirectories(dir.resolve("s").resolve("lineitem")));
        List<TestFlightServerHelper> nodes = new ArrayList<>();
        String[] seeds = new String[NODES];
        int[] hazelcastPorts = new int[NODES];
        for (int i = 0; i < NODES; i++) {
            hazelcastPorts[i] = freePort();
            seeds[i] = "127.0.0.1:" + hazelcastPorts[i];
        }
        System.setProperty("endpointsPerServer", Integer.getInteger("perf.endpointsPerServer", 2).toString());
        try {
            for (int i = 0; i < NODES; i++) {
                nodes.add(TestFlightServerHelper.builder().dataDir(dir.toString()).resultMode("complete")
                        .sharedStorage().hazelcastPort(hazelcastPorts[i]).flightPort(freePort())
                        .hazelcastSeeds(seeds).start());
            }
        } finally {
            System.clearProperty("endpointsPerServer");
        }
        TestFlightServerHelper first = nodes.get(0);
        try (BufferAllocator peers = first.allocator.newChildAllocator("peers", 0, Long.MAX_VALUE)) {
            awaitMembers(first);
            System.out.printf(Locale.ROOT, "distributed complete, %d nodes in one JVM, %d rows, %d files,"
                    + " %d cores, median of %d runs%n", NODES, ROWS, FILES,
                    Runtime.getRuntime().availableProcessors(), RUNS);
            for (Map.Entry<String, String> query : QUERIES.entrySet()) {
                measure(first, peers, query.getKey(), query.getValue());
            }
        } finally {
            first.flightSqlProducer.useDistributedComplete(null);
            for (int i = nodes.size() - 1; i >= 0; i--) {
                nodes.get(i).close();
            }
        }
    }

    /**
     * Measures one query off and on and prints one line.
     *
     * @param node planning node
     * @param peers allocator of the peer streams
     * @param label query label
     * @param query client SQL
     * @throws Exception on failure
     */
    private static void measure(TestFlightServerHelper node, BufferAllocator peers, String label,
            String query) throws Exception {
        long[] off = new long[RUNS];
        long[] on = new long[RUNS];
        String alone = null;
        try (DistributedComplete distributed = new DistributedComplete(node.queryPlanner, node.clusterService,
                node.executionService, peers, DistributedComplete.clientFactory(peers, null))) {
            for (int i = -1; i < RUNS; i++) {
                node.flightSqlProducer.useDistributedComplete(null);
                long t0 = System.nanoTime();
                String answer = answer(node, query);
                long offNanos = System.nanoTime() - t0;
                alone = answer;
                node.flightSqlProducer.useDistributedComplete(distributed);
                t0 = System.nanoTime();
                String merged = answer(node, query);
                long onNanos = System.nanoTime() - t0;
                assertEquals(alone, merged, label);
                if (i >= 0) {
                    off[i] = offNanos;
                    on[i] = onNanos;
                }
            }
            assertEquals(RUNS + 1, distributed.mergedAnswers(), "merged: " + label);
            long onTotal = Arrays.stream(on).sum();
            Arrays.sort(off);
            Arrays.sort(on);
            System.out.printf(Locale.ROOT, "  %-26s off %7.1f ms  on %7.1f ms  (x%.2f)  peer %8.2f MB/query"
                            + "  peer wait %3.0f%% of on%n", label, off[RUNS / 2] / 1e6, on[RUNS / 2] / 1e6,
                    (double) on[RUNS / 2] / off[RUNS / 2], distributed.peerBytes() / 1e6 / (RUNS + 1),
                    100.0 * distributed.peerWaitNanos() * RUNS / (RUNS + 1) / onTotal);
        } finally {
            node.flightSqlProducer.useDistributedComplete(null);
        }
    }

    /**
     * Writes the lineitem-like table.
     *
     * @param table table directory
     * @throws Exception on failure
     */
    private static void writeTable(Path table) throws Exception {
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = duck.createStatement()) {
            for (int f = 0; f < FILES; f++) {
                st.execute(String.format(Locale.ROOT,
                        "COPY (SELECT i AS l_orderkey, i %% 200000 AS l_partkey, i %% 7 + 1 AS l_linenumber,"
                                + " ((i * 13 %% 5000) / 100.0)::DECIMAL(15, 2) AS l_quantity,"
                                + " ((i * 7 %% 100000) / 10.0)::DECIMAL(15, 2) AS l_extendedprice, DATE '1992-01-01' + CAST(i %% 2500 AS INTEGER)"
                                + " AS l_shipdate, chr(65 + (i %% 3)::INTEGER) AS l_returnflag,"
                                + " chr(70 + (i %% 2)::INTEGER) AS l_linestatus, 'comment ' || (i %% 9973)"
                                + " AS l_comment FROM range(%d, %d) t(i)) TO '%s' (FORMAT parquet,"
                                + " ROW_GROUP_SIZE 122880)",
                        f * ROWS / FILES, (f + 1) * ROWS / FILES, table.resolve("part-" + f + ".parquet")));
            }
        }
    }

    /**
     * Waits until the node sees every member of the cluster.
     *
     * @param node node
     * @throws Exception when it does not happen within a minute
     */
    private static void awaitMembers(TestFlightServerHelper node) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            List<FlightEndpoint> endpoints = node.queryPlanner.plan("SELECT * FROM s.lineitem").endpoints();
            for (FlightEndpoint endpoint : endpoints) {
                // Planning reserved the endpoints' loads; nothing reads them.
                byte[] handle = com.google.protobuf.Any.parseFrom(endpoint.getTicket().getBytes())
                        .unpack(org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery.class)
                        .getStatementHandle().toByteArray();
                node.clusterService.releaseEndpointLoad(handle, node.clusterService.resolveEndpointHandle(handle));
            }
            if (endpoints.stream().map(e -> e.getLocations().get(0).getUri()).distinct().count() == NODES) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("the cluster did not form");
    }

    /**
     * Reads the node's complete answer of a query as text, sorted unless the query orders it.
     *
     * @param node node
     * @param query client SQL
     * @return schema and rows
     * @throws Exception on failure
     */
    private static String answer(TestFlightServerHelper node, String query) throws Exception {
        FlightInfo info = node.sqlClient().execute(query);
        List<String> lines = new ArrayList<>();
        String schema;
        try (FlightStream stream = node.sqlClient().getStream(info.getEndpoints().get(0).getTicket())) {
            schema = stream.getSchema().toString();
            while (stream.next()) {
                stream.getRoot().contentToTSVString().lines().skip(1).forEach(lines::add);
            }
        }
        if (!query.contains("ORDER BY")) {
            lines.sort(null);
        }
        return schema + "\n" + lines.size() + " rows\n" + String.join("\n", lines);
    }

    /**
     * Finds a free local port.
     *
     * @return port
     * @throws Exception on failure
     */
    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
