package pro.surpin.data.arrowflight.server;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Query load against a cluster started separately while nodes are restarted (lane 3, D8, run by
 * the manual workflow {@code distributed-restart.yml}). {@code -Dload.servers=host:port,...} names
 * the nodes, {@code -Dload.queries} a directory of {@code <name>.sql} files with optional
 * {@code <name>.rows} row counts; queries whose name starts with {@code partial_} run in result
 * mode {@code partial}, the others in {@code complete}. First reads a reference answer of every
 * query (rows and the order-independent checksum of {@link TpchPerfTest}) on the healthy cluster,
 * then {@code -Dload.clients} (default 3) clients run the queries in turn, each call on the next
 * node, until the file {@code -Dload.stopFile} exists (at most {@code -Dload.maxSeconds}, default
 * 1800). Endpoints are read from their locations in order (header
 * {@code x-flight-location-failover: 1}). Prints one line per query,
 * {@code event <epoch ms> c<client> n<node> <name> ok|wrong|error <code> <ms>}, and a summary.
 * Fails when an answer differs from its reference (never allowed, whatever the nodes do) or a
 * query failed with a code outside {@code -Dload.retryable} (default {@code UNAVAILABLE}).
 * For a node joining under load (D9, {@code distributed-join.yml}), {@code -Dload.joinServers}
 * names nodes the clients plan on only once the file {@code -Dload.joinFile} exists (they take
 * the indexes after the {@code load.servers} nodes); the first time any endpoint's first location
 * is a node, a line {@code location <epoch ms> <uri>} is printed.
 */
@Tag("perf")
class RollingRestartLoadTest {

    /** Longest a single query may take before it counts as failed. */
    private static final long CALL_TIMEOUT_SECONDS = 120;

    /**
     * Runs the reference pass and the load loop.
     *
     * @throws Exception on failure
     */
    @Test
    void loadWhileNodesRestart() throws Exception {
        String servers = System.getProperty("load.servers");
        String queriesDir = System.getProperty("load.queries");
        String stopFile = System.getProperty("load.stopFile");
        Assumptions.assumeTrue(servers != null && queriesDir != null && stopFile != null,
                "needs -Dload.servers=host:port,..., -Dload.queries=<dir> and -Dload.stopFile=<path>");
        List<Path> files;
        try (Stream<Path> list = Files.list(Path.of(queriesDir))) {
            files = list.filter(p -> p.getFileName().toString().endsWith(".sql")).sorted().toList();
        }
        assertTrue(!files.isEmpty(), "no .sql files in " + queriesDir);
        List<Query> queries = new ArrayList<>();
        for (Path f : files) {
            String name = f.getFileName().toString().replace(".sql", "");
            Path rows = f.resolveSibling(name + ".rows");
            queries.add(new Query(name, Files.readString(f, StandardCharsets.UTF_8).trim().replaceAll(";\\s*$", ""),
                    Files.exists(rows) ? Long.parseLong(Files.readString(rows).trim()) : -1L,
                    headers(name.startsWith("partial_") ? "partial" : "complete")));
        }
        List<Location> nodes = new ArrayList<>();
        for (String server : servers.split(",")) {
            String[] hostPort = server.trim().split(":");
            nodes.add(Location.forGrpcInsecure(hostPort[0], Integer.parseInt(hostPort[1])));
        }
        String joinServers = System.getProperty("load.joinServers", "");
        for (String server : joinServers.split(",")) {
            if (!server.isBlank()) {
                String[] hostPort = server.trim().split(":");
                nodes.add(Location.forGrpcInsecure(hostPort[0], Integer.parseInt(hostPort[1])));
            }
        }
        int initialNodes = servers.split(",").length;
        String joinFile = System.getProperty("load.joinFile");
        Join join = new Join(initialNodes, joinFile == null ? null : Path.of(joinFile), out());
        int clients = Integer.parseInt(System.getProperty("load.clients", "3"));
        long deadline = System.currentTimeMillis() + 1000L * Long.parseLong(System.getProperty("load.maxSeconds", "1800"));
        Set<String> retryable = Set.of(System.getProperty("load.retryable", "UNAVAILABLE").split(","));
        PrintStream out = out();
        try (BufferAllocator allocator = new RootAllocator()) {
            Reader reference = new Reader(allocator);
            try {
                for (Query q : queries) {
                    q.reference = reference.answer(nodes.get(0), q, join);
                    if (q.rows >= 0) {
                        assertEquals(q.rows, q.reference[0], q.name + ": reference rows differ from DuckDB's");
                    }
                    out.printf(Locale.ROOT, "reference %s rows %d sum %016x%n", q.name, q.reference[0], q.reference[1]);
                }
            } finally {
                reference.close();
            }
            Map<String, AtomicLong> outcomes = new ConcurrentHashMap<>();
            ExecutorService pool = Executors.newFixedThreadPool(clients);
            List<Future<?>> runs = new ArrayList<>();
            out.printf(Locale.ROOT, "load start %d%n", System.currentTimeMillis());
            for (int c = 0; c < clients; c++) {
                int client = c;
                runs.add(pool.submit(() -> {
                    loop(allocator, client, nodes, join, queries, Path.of(stopFile), deadline, outcomes, out);
                    return null;
                }));
            }
            try {
                for (Future<?> run : runs) {
                    run.get();
                }
            } finally {
                pool.shutdownNow();
            }
            out.printf(Locale.ROOT, "load end %d%n", System.currentTimeMillis());
            Map<String, Long> summary = new TreeMap<>();
            outcomes.forEach((k, v) -> summary.put(k, v.get()));
            summary.forEach((k, v) -> out.printf(Locale.ROOT, "summary %s %d%n", k, v));
            assertEquals(0L, summary.getOrDefault("wrong", 0L), "wrong answers under restarts");
            List<String> bad = summary.keySet().stream()
                    .filter(k -> k.startsWith("error ") && !retryable.contains(k.substring(6))).toList();
            assertTrue(bad.isEmpty(), "failures with codes that are not retryable: " + bad);
            assertTrue(summary.getOrDefault("ok", 0L) > 0, "no query answered");
        }
    }

    /**
     * Runs the queries on one client until the stop file exists or the deadline passes.
     *
     * @param allocator Arrow allocator
     * @param client    index of the client
     * @param nodes     node locations
     * @param join      nodes planned on only after the join file exists
     * @param queries   queries with their reference answers
     * @param stop      stop file
     * @param deadline  latest end, epoch ms
     * @param outcomes  outcome counters
     * @param out       event output
     */
    private static void loop(BufferAllocator allocator, int client, List<Location> nodes, Join join,
            List<Query> queries, Path stop, long deadline, Map<String, AtomicLong> outcomes, PrintStream out) {
        Reader reader = new Reader(allocator);
        try {
            long call = client;
            while (!Files.exists(stop) && System.currentTimeMillis() < deadline) {
                int node = (int) (call % join.active(nodes.size()));
                Query q = queries.get((int) ((call / nodes.size() + client) % queries.size()));
                call++;
                long t0 = System.nanoTime();
                String outcome;
                try {
                    long[] answer = reader.answer(nodes.get(node), q, join);
                    outcome = answer[0] == q.reference[0] && answer[1] == q.reference[1] ? "ok"
                            : String.format(Locale.ROOT, "wrong rows %d sum %016x", answer[0], answer[1]);
                } catch (FlightRuntimeException e) {
                    outcome = "error " + e.status().code() + " " + firstLine(e.getMessage());
                } catch (RuntimeException | IOException e) {
                    outcome = "error " + FlightStatusCode.UNKNOWN + " " + e.getClass().getSimpleName() + ": "
                            + firstLine(e.getMessage());
                }
                String kind = outcome.startsWith("error ") ? outcome.split(" ")[0] + " " + outcome.split(" ")[1]
                        : outcome.split(" ")[0];
                outcomes.computeIfAbsent(kind, k -> new AtomicLong()).incrementAndGet();
                out.printf(Locale.ROOT, "event %d c%d n%d %s %s %.1f%n", System.currentTimeMillis(), client, node,
                        q.name, outcome, (System.nanoTime() - t0) / 1e6);
            }
        } finally {
            reader.close();
        }
    }

    /**
     * Standard output, where the workflow reads the lines.
     *
     * @return {@code System.out}
     */
    private static PrintStream out() {
        return System.out;
    }

    /** Nodes that join during the run, and the first time each node is planned. */
    private static final class Join {
        /** Nodes planned on from the start. */
        private final int initial;
        /** File whose existence adds the joining nodes, or null. */
        private final Path file;
        /** Output of the {@code location} lines. */
        private final PrintStream out;
        /** Locations seen as an endpoint's first location. */
        private final Set<URI> planned = ConcurrentHashMap.newKeySet();
        /** Whether the join file was seen. */
        private volatile boolean joined;

        /**
         * Creates the join state.
         *
         * @param initial nodes planned on from the start
         * @param file    join file, or null
         * @param out     output
         */
        Join(int initial, Path file, PrintStream out) {
            this.initial = initial;
            this.file = file;
            this.out = out;
        }

        /**
         * Number of nodes the clients plan on now.
         *
         * @param all all nodes
         * @return the initial nodes, or all once the join file exists
         */
        int active(int all) {
            if (!joined && file != null && Files.exists(file)) {
                joined = true;
            }
            return joined || file == null ? all : initial;
        }

        /**
         * Records an endpoint's first location, printing it the first time.
         *
         * @param location location
         */
        void planned(Location location) {
            if (planned.add(location.getUri())) {
                out.printf(Locale.ROOT, "location %d %s%n", System.currentTimeMillis(), location.getUri());
            }
        }
    }

    /**
     * First line of a message.
     *
     * @param message message, may be null
     * @return first line, empty when none
     */
    private static String firstLine(String message) {
        return String.valueOf(message).lines().findFirst().orElse("");
    }

    /**
     * Call option carrying the result mode and the location failover header.
     *
     * @param mode result mode
     * @return header call option
     */
    private static CallOption headers(String mode) {
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert("x-flight-result-mode", mode);
        headers.insert("x-flight-location-failover", "1");
        return new HeaderCallOption(headers);
    }

    /** A query with its expected row count and reference answer. */
    private static final class Query {
        /** Query name (file name without {@code .sql}). */
        private final String name;
        /** Query text. */
        private final String sql;
        /** DuckDB's row count, -1 when unknown. */
        private final long rows;
        /** Result-mode header. */
        private final CallOption header;
        /** Rows and checksum read on the healthy cluster. */
        private long[] reference;

        /**
         * Creates a query.
         *
         * @param name   query name
         * @param sql    query text
         * @param rows   DuckDB's row count, -1 when unknown
         * @param header result-mode header
         */
        Query(String name, String sql, long rows, CallOption header) {
            this.name = name;
            this.sql = sql;
            this.rows = rows;
            this.header = header;
        }
    }

    /** A failure of an endpoint stream after rows of it were read: not retried on another location. */
    private static final class StreamCut extends RuntimeException {
        private static final long serialVersionUID = 1L;
        /** The stream's failure. */
        private final transient FlightRuntimeException failure;

        /**
         * Creates the wrapper.
         *
         * @param failure the stream's failure
         */
        StreamCut(FlightRuntimeException failure) {
            super(failure);
            this.failure = failure;
        }
    }

    /** One client's Flight connections, one per location it has read from. */
    private static final class Reader {
        /** Arrow allocator. */
        private final BufferAllocator allocator;
        /** Clients by location URI. */
        private final Map<URI, FlightClient> clients = new HashMap<>();

        /**
         * Creates a reader.
         *
         * @param allocator Arrow allocator
         */
        Reader(BufferAllocator allocator) {
            this.allocator = allocator;
        }

        /**
         * Runs a query on a node and reads every endpoint, trying an endpoint's locations in order
         * when one is unavailable.
         *
         * @param node node to plan on
         * @param q    query
         * @param join records the endpoints' first locations
         * @return rows read and the order-independent checksum
         * @throws IOException never in practice, declared by the stream close
         */
        long[] answer(Location node, Query q, Join join) throws IOException {
            CallOption timeout = CallOptions.timeout(CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            FlightInfo info = new FlightSqlClient(client(node)).execute(q.sql, q.header, timeout);
            long rows = 0;
            long sum = 0;
            for (FlightEndpoint endpoint : info.getEndpoints()) {
                List<Location> locations = new ArrayList<>();
                for (Location location : endpoint.getLocations()) {
                    String scheme = location.getUri().getScheme();
                    locations.add("grpc".equals(scheme) || "grpc+tcp".equals(scheme) ? location : node);
                }
                if (locations.isEmpty()) {
                    locations.add(node);
                }
                join.planned(locations.get(0));
                for (int i = 0;; i++) {
                    long[] part;
                    try {
                        part = read(new FlightSqlClient(client(locations.get(i))), endpoint, q.header, timeout);
                    } catch (StreamCut e) {
                        throw e.failure;
                    } catch (FlightRuntimeException e) {
                        if (e.status().code() != FlightStatusCode.UNAVAILABLE || i + 1 >= locations.size()) {
                            throw e;
                        }
                        continue;
                    }
                    rows += part[0];
                    sum += part[1];
                    break;
                }
            }
            return new long[] {rows, sum};
        }

        /**
         * Reads one endpoint from one location.
         *
         * @param client   Flight SQL client of the location
         * @param endpoint endpoint
         * @param header   result-mode header
         * @param timeout  call timeout
         * @return rows and checksum
         * @throws StreamCut when the stream failed after rows were read
         * @throws FlightRuntimeException when it failed before
         */
        private static long[] read(FlightSqlClient client, FlightEndpoint endpoint, CallOption header,
                CallOption timeout) {
            long rows = 0;
            long sum = 0;
            try (FlightStream stream = client.getStream(endpoint.getTicket(), header, timeout)) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    for (int row = 0; row < root.getRowCount(); row++) {
                        StringBuilder text = new StringBuilder();
                        for (FieldVector vector : root.getFieldVectors()) {
                            text.append(TpchPerfTest.normalized(vector.getObject(row))).append('|');
                        }
                        sum += TpchPerfTest.mix(text.toString().hashCode());
                    }
                    rows += root.getRowCount();
                }
            } catch (FlightRuntimeException e) {
                if (rows > 0) {
                    // Rows already counted: retrying on the next location would sum them twice.
                    throw new StreamCut(e);
                }
                throw e;
            } catch (Exception e) {
                throw CallStatus.UNKNOWN.withCause(e).withDescription(String.valueOf(e.getMessage()))
                        .toRuntimeException();
            }
            return new long[] {rows, sum};
        }

        /**
         * Connection to a location, created on first use.
         *
         * @param location location
         * @return Flight client
         */
        private FlightClient client(Location location) {
            return clients.computeIfAbsent(location.getUri(), u -> FlightClient.builder(allocator, location).build());
        }

        /** Closes every connection. */
        void close() {
            for (FlightClient client : clients.values()) {
                try {
                    client.close();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
