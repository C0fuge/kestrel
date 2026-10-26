package pro.surpin.data.arrowflight.server.adapters;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * A Flight server run in its own JVM (small {@code -XX:MaxDirectMemorySize}) that streams large
 * batches the way the server's DoGet streams do (readiness wait before each batch), plus the
 * clients that read many streams from it concurrently, each slowly at first.
 */
public final class OutboundBudgetProbe {

    /** Rows per batch: 1 MiB of BIGINT. */
    static final int ROWS_PER_BATCH = 1 << 17;

    /** Highest direct memory use seen. */
    private static volatile long peakDirect;

    /**
     * Records the highest direct memory use until the JVM exits.
     */
    private static void watchDirectMemory() {
        java.lang.management.BufferPoolMXBean direct = java.lang.management.ManagementFactory
                .getPlatformMXBeans(java.lang.management.BufferPoolMXBean.class).stream()
                .filter(pool -> "direct".equals(pool.getName())).findFirst().orElseThrow();
        while (true) {
            peakDirect = Math.max(peakDirect, direct.getMemoryUsed());
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    /**
     * Not instantiated.
     */
    private OutboundBudgetProbe() {
    }

    /**
     * Runs the server until standard input closes, then prints the peak direct memory use.
     *
     * @param args batches per stream, budget in bytes (0 = no budget), per-stream threshold in
     *             bytes
     * @throws Exception on setup failure
     */
    public static void main(String[] args) throws Exception {
        int batches = Integer.parseInt(args[0]);
        long budget = Long.parseLong(args[1]);
        int threshold = Integer.parseInt(args[2]);
        try (BufferAllocator allocator = new RootAllocator();
                FlightServer server = server(allocator, batches, budget, threshold)) {
            server.start();
            Thread peak = new Thread(OutboundBudgetProbe::watchDirectMemory, "peak");
            peak.setDaemon(true);
            peak.start();
            System.out.println("PORT " + server.getPort());
            System.out.flush();
            while (System.in.read() >= 0) {
                // Runs until the test closes standard input.
            }
            System.out.println("PEAK " + peakDirect);
            System.out.flush();
        }
        System.exit(0);
    }

    /**
     * Reads streams concurrently from a probe server, each slowly at first, for at most two
     * minutes.
     *
     * @param port probe server port
     * @param streams concurrent streams
     * @return rows read per stream, or the failure (or timeout) as a string
     * @throws InterruptedException if interrupted
     */
    static List<String> readAll(int port, int streams) throws InterruptedException {
        Location location = Location.forGrpcInsecure("127.0.0.1", port);
        ExecutorService clients = Executors.newFixedThreadPool(streams);
        // A server out of direct memory can leave streams open without data: give up on them.
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
        BufferAllocator allocator = new RootAllocator();
        try {
            List<Future<Long>> reads = new ArrayList<>();
            for (int i = 0; i < streams; i++) {
                reads.add(clients.submit(() -> read(allocator, location)));
            }
            List<String> results = new ArrayList<>();
            for (Future<Long> read : reads) {
                try {
                    long left = Math.max(1, deadline - System.nanoTime());
                    results.add(String.valueOf(read.get(left, TimeUnit.NANOSECONDS)));
                } catch (java.util.concurrent.ExecutionException
                        | java.util.concurrent.TimeoutException e) {
                    results.add(String.valueOf(e.getCause() == null ? e : e.getCause()));
                }
            }
            return results;
        } finally {
            clients.shutdownNow();
            clients.awaitTermination(1, TimeUnit.MINUTES);
            try {
                allocator.close();
            } catch (IllegalStateException e) {
                // Buffers of streams abandoned above; the JVM of the test reclaims them.
            }
        }
    }

    /**
     * Builds the server.
     *
     * @param allocator Arrow allocator
     * @param batches batches per stream
     * @param budget outbound budget, 0 for none
     * @param threshold per-stream backpressure threshold
     * @return server, not started
     */
    private static FlightServer server(BufferAllocator allocator, int batches, long budget,
            int threshold) {
        FlightServer.Builder builder = FlightServer.builder(allocator,
                Location.forGrpcInsecure("127.0.0.1", 0), new BatchProducer(allocator, batches))
                .backpressureThreshold(threshold);
        if (budget > 0) {
            OutboundBudget outbound = new OutboundBudget(budget);
            builder.transportHint("grpc.builderConsumer",
                    (java.util.function.Consumer<io.grpc.netty.NettyServerBuilder>)
                            b -> b.intercept(outbound));
        }
        return builder.build();
    }

    /**
     * Reads one stream, slowly at first, as a busy client does.
     *
     * @param allocator Arrow allocator
     * @param location server location
     * @return rows read
     * @throws Exception on stream failure
     */
    private static long read(BufferAllocator allocator, Location location) throws Exception {
        try (FlightClient client = FlightClient.builder(allocator, location).build();
                FlightStream stream = client.getStream(
                        new Ticket("all".getBytes(StandardCharsets.UTF_8)))) {
            long rows = 0;
            int seen = 0;
            while (stream.next()) {
                rows += stream.getRoot().getRowCount();
                if (seen++ < 2) {
                    Thread.sleep(1_000);
                }
            }
            return rows;
        }
    }

    /**
     * Streams {@code batches} batches of {@link #ROWS_PER_BATCH} rows per DoGet.
     */
    private static final class BatchProducer extends NoOpFlightProducer {
        private final BufferAllocator allocator;
        private final int batches;

        /**
         * Creates the producer.
         *
         * @param allocator Arrow allocator
         * @param batches batches per stream
         */
        private BatchProducer(BufferAllocator allocator, int batches) {
            this.allocator = allocator;
            this.batches = batches;
        }

        @Override
        public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
            try (BufferAllocator child = allocator.newChildAllocator("stream", 0, Long.MAX_VALUE);
                    BigIntVector values = new BigIntVector("v", child);
                    VectorSchemaRoot root = VectorSchemaRoot.of(values)) {
                values.allocateNew(ROWS_PER_BATCH);
                for (int i = 0; i < ROWS_PER_BATCH; i++) {
                    values.set(i, i);
                }
                root.setRowCount(ROWS_PER_BATCH);
                listener.start(root);
                for (int b = 0; b < batches; b++) {
                    if (!DuckDbAdapter.awaitListenerReady(listener, 120_000)) {
                        return;
                    }
                    listener.putNext();
                }
                listener.completed();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                listener.error(e);
            } catch (RuntimeException e) {
                listener.error(e);
            }
        }
    }
}
