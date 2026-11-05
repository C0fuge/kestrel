package pro.surpin.data.arrowflight.server.services;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks that an endpoint answer stream not yet read receives only its schema (item 31: idle
 * streams of a distributed complete answer each held five batches and exhausted direct memory),
 * and reads every batch once activated.
 */
class PeerReadGateTest {

    private static final int BATCHES = 20;
    private static final int ROWS = 1000;

    /** An idle gate passes the first request, holds the rest, and passes them on activation. */
    @Test
    @Tag("unit")
    void idleRequestsAreHeldUntilActivated() {
        PeerReadGate gate = new PeerReadGate();
        RecordingCall recorded = new RecordingCall();
        ClientCall<String, String> call = gate.interceptCall(method(), CallOptions.DEFAULT, channel(recorded));
        call.request(1);
        call.request(5);
        call.request(1);
        assertEquals(List.of(1), recorded.requests);
        assertEquals(6, gate.held());
        gate.activate();
        assertEquals(List.of(1, 6), recorded.requests);
        assertEquals(0, gate.held());
        call.request(1);
        gate.activate();
        assertEquals(List.of(1, 6, 1), recorded.requests);
    }

    /** Cancelling an idle call passes its held requests on, so the stream can be drained. */
    @Test
    @Tag("unit")
    void cancelReleasesHeldRequests() {
        PeerReadGate gate = new PeerReadGate();
        RecordingCall recorded = new RecordingCall();
        ClientCall<String, String> call = gate.interceptCall(method(), CallOptions.DEFAULT, channel(recorded));
        call.request(1);
        call.request(5);
        call.cancel("closed", null);
        assertEquals(List.of(1, 5), recorded.requests);
        assertEquals(0, gate.held());
    }

    /**
     * Closing an idle Flight stream (cancel, then drain) ends instead of waiting for messages the
     * gate holds back.
     *
     * @throws Exception on failure
     */
    @Test
    @Tag("integration")
    void closingAnIdleFlightStreamEnds() throws Exception {
        Schema schema = new Schema(List.of(Field.nullable("v", new ArrowType.Int(64, true))));
        BufferAllocator allocator = new RootAllocator();
        BatchProducer producer = new BatchProducer(allocator, schema);
        try (allocator;
                FlightServer server = FlightServer.builder(allocator, Location.forGrpcInsecure("localhost", 0),
                        producer).build().start();
                FlightClient client = FlightClient.builder(allocator,
                        Location.forGrpcInsecure("localhost", server.getPort())).build()) {
            PeerReadGate gate = new PeerReadGate();
            FlightStream stream = client.getStream(new Ticket("t".getBytes(StandardCharsets.UTF_8)), gate);
            assertEquals(schema.getFields(), stream.getSchema().getFields());
            assertTimeoutPreemptively(Duration.ofSeconds(20), stream::close);
            // The server's call thread may still hold a batch after the cancel: let it release
            // it before the shared allocator closes (else a spurious "Memory was leaked").
            assertTrue(producer.finished.await(20, java.util.concurrent.TimeUnit.SECONDS),
                    "producer did not end after the cancel");
        }
    }

    /** A Flight stream behind an idle gate receives its schema only; activated, it reads every row. */
    @Test
    @Tag("integration")
    void idleFlightStreamReceivesOnlyItsSchema() throws Exception {
        Schema schema = new Schema(List.of(Field.nullable("v", new ArrowType.Int(64, true))));
        try (BufferAllocator allocator = new RootAllocator();
                FlightServer server = FlightServer.builder(allocator, Location.forGrpcInsecure("localhost", 0),
                        new BatchProducer(allocator, schema)).build().start();
                FlightClient client = FlightClient.builder(allocator,
                        Location.forGrpcInsecure("localhost", server.getPort())).build()) {
            PeerReadGate gate = new PeerReadGate();
            try (FlightStream stream = client.getStream(new Ticket("t".getBytes(StandardCharsets.UTF_8)), gate)) {
                assertEquals(schema.getFields(), stream.getSchema().getFields());
                Thread.sleep(500);
                assertEquals(PeerReadGate.IDLE_MESSAGES, gate.delivered());
                gate.activate();
                long rows = 0;
                long sum = 0;
                while (stream.next()) {
                    BigIntVector v = (BigIntVector) stream.getRoot().getVector(0);
                    for (int i = 0; i < stream.getRoot().getRowCount(); i++) {
                        sum += v.get(i);
                    }
                    rows += stream.getRoot().getRowCount();
                }
                long n = (long) BATCHES * ROWS;
                assertEquals(n, rows);
                assertEquals(n * (n - 1) / 2, sum);
            }
        }
    }

    /**
     * Returns a server-streaming method descriptor of strings.
     *
     * @return method descriptor
     */
    private static MethodDescriptor<String, String> method() {
        MethodDescriptor.Marshaller<String> marshaller = new MethodDescriptor.Marshaller<>() {
            @Override
            public java.io.InputStream stream(String value) {
                return new java.io.ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
            }

            @Override
            public String parse(java.io.InputStream stream) {
                return "";
            }
        };
        return MethodDescriptor.<String, String>newBuilder().setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                .setFullMethodName("test/stream").setRequestMarshaller(marshaller)
                .setResponseMarshaller(marshaller).build();
    }

    /**
     * Returns a channel whose calls are the given recording call.
     *
     * @param call call to return
     * @return channel
     */
    private static Channel channel(RecordingCall call) {
        return new Channel() {
            @Override
            @SuppressWarnings("unchecked")
            public <R, S> ClientCall<R, S> newCall(MethodDescriptor<R, S> method, CallOptions options) {
                return (ClientCall<R, S>) call;
            }

            @Override
            public String authority() {
                return "test";
            }
        };
    }

    /** A call that records the message requests it gets. */
    private static final class RecordingCall extends ClientCall<String, String> {
        private final List<Integer> requests = new ArrayList<>();

        @Override
        public void start(Listener<String> listener, Metadata headers) {
            // Nothing to start.
        }

        @Override
        public void request(int numMessages) {
            requests.add(numMessages);
        }

        @Override
        public void cancel(String message, Throwable cause) {
            // Nothing to cancel.
        }

        @Override
        public void halfClose() {
            // Nothing to close.
        }

        @Override
        public void sendMessage(String message) {
            // Nothing to send.
        }
    }

    /** Streams {@link #BATCHES} batches of {@link #ROWS} consecutive longs for any ticket. */
    private static final class BatchProducer extends NoOpFlightProducer {
        private final BufferAllocator allocator;
        private final Schema schema;
        /** Counted down when a getStream call has released its batches. */
        private final java.util.concurrent.CountDownLatch finished = new java.util.concurrent.CountDownLatch(1);

        /**
         * Creates the producer.
         *
         * @param allocator allocator of the batches
         * @param schema schema of the batches
         */
        BatchProducer(BufferAllocator allocator, Schema schema) {
            this.allocator = allocator;
            this.schema = schema;
        }

        @Override
        public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                listener.start(root);
                BigIntVector v = (BigIntVector) root.getVector(0);
                long next = 0;
                for (int b = 0; b < BATCHES; b++) {
                    v.allocateNew(ROWS);
                    for (int i = 0; i < ROWS; i++) {
                        v.set(i, next++);
                    }
                    root.setRowCount(ROWS);
                    listener.putNext();
                }
                listener.completed();
            } finally {
                finished.countDown();
            }
        }
    }
}
