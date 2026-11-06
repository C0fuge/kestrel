package pro.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Item 36 (1): with local copies, a merge opens one endpoint stream per node at a time, so that
 * two endpoints of one node never wait for each other's copy room while the merge reads another
 * answer; the next endpoint of a node opens once the node's earlier answer has been read.
 */
@Tag("unit")
class DistributedCompleteOneStreamPerNodeTest {

    private static final Schema SCHEMA = new Schema(List.of(
            Field.nullable("v", new ArrowType.Int(64, true))));

    /** Endpoints are keyed by the node of their first location; the first of each node opens. */
    @Test
    void firstEndpointOfEachNodeOpensFirst() {
        List<FlightEndpoint> endpoints = List.of(endpoint("grpc://a:1"), endpoint("grpc://b:1"),
                endpoint("grpc://a:1"), endpoint("grpc://a:1"), endpoint("grpc://c:1"));
        List<String> nodes = DistributedComplete.nodeKeys(endpoints);
        assertEquals(List.of("grpc://a:1", "grpc://b:1", "grpc://a:1", "grpc://a:1", "grpc://c:1"), nodes);
        List<Boolean> first = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i++) {
            first.add(DistributedComplete.firstOnNode(nodes, i));
        }
        assertEquals(List.of(true, true, false, false, true), first);
        List<Boolean> opened = new ArrayList<>(first);
        assertEquals(2, DistributedComplete.nextOnNode(nodes, opened, 0));
        assertEquals(-1, DistributedComplete.nextOnNode(nodes, opened, 1));
        opened.set(2, true);
        assertEquals(3, DistributedComplete.nextOnNode(nodes, opened, 2));
        assertEquals(-1, DistributedComplete.nextOnNode(nodes, opened, 4));
    }

    /**
     * The reader opens a node's next endpoint when the node's answer before it has been read,
     * before it reads the answers in between, and reads every row in endpoint order.
     */
    @Test
    void nextEndpointOfANodeOpensWhenItsEarlierAnswerIsRead() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot r0 = root(allocator, 1);
                VectorSchemaRoot r1 = root(allocator, 2);
                VectorSchemaRoot r2 = root(allocator, 3)) {
            FlightStream s0 = stream(r0);
            FlightStream s1 = stream(r1);
            FlightStream s2 = stream(r2);
            // Endpoints 0 and 2 on node a, endpoint 1 on node b: endpoint 2 waits for endpoint 0.
            List<String> nodes = List.of("a", "b", "a");
            List<FlightStream> streams = Collections.synchronizedList(new ArrayList<>(List.of(s0, s1)));
            streams.add(null);
            List<PeerReadGate> gates = Collections.synchronizedList(new ArrayList<>(
                    List.of(new PeerReadGate(), new PeerReadGate())));
            gates.add(null);
            List<String> events = new ArrayList<>();
            DistributedComplete.StreamOpener opener = index -> {
                events.add("open" + index);
                gates.set(index, new PeerReadGate());
                streams.set(index, s2);
            };
            List<Long> values = new ArrayList<>();
            try (DistributedComplete.EndpointAnswers answers = new DistributedComplete.EndpointAnswers(
                    allocator, streams, gates, SCHEMA, nodes, opener)) {
                while (answers.loadNextBatch()) {
                    events.add("read");
                    values.add(((BigIntVector) answers.getVectorSchemaRoot().getVector(0)).get(0));
                }
            }
            assertEquals(List.of(1L, 2L, 3L), values);
            assertEquals(List.of("read", "open2", "read", "read"), events);
            assertSame(s2, streams.get(2));
        }
    }

    /** A node's later endpoint that fails to open fails the merge and is its endpoint failure. */
    @Test
    void failureOfALaterEndpointIsTheMergesFailure() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot r0 = root(allocator, 1)) {
            List<FlightStream> streams = Collections.synchronizedList(new ArrayList<>());
            streams.add(stream(r0));
            streams.add(null);
            List<PeerReadGate> gates = Collections.synchronizedList(new ArrayList<>());
            gates.add(new PeerReadGate());
            gates.add(null);
            IllegalStateException refused = new IllegalStateException("Endpoint answers have different columns");
            try (DistributedComplete.EndpointAnswers answers = new DistributedComplete.EndpointAnswers(
                    allocator, streams, gates, SCHEMA, List.of("a", "a"), index -> {
                        throw refused;
                    })) {
                assertTrue(answers.loadNextBatch());
                assertSame(refused, assertThrows(IllegalStateException.class, answers::loadNextBatch));
                assertSame(refused, answers.failure());
            }
        }
    }

    /** Without an opener every stream must be open; nodes and opener go together. */
    @Test
    void nodesAndOpenerGoTogether() {
        try (BufferAllocator allocator = new RootAllocator()) {
            List<FlightStream> streams = List.of(mock(FlightStream.class));
            List<PeerReadGate> gates = List.of(new PeerReadGate());
            assertThrows(IllegalArgumentException.class, () -> new DistributedComplete.EndpointAnswers(
                    allocator, streams, gates, SCHEMA, List.of("a"), null));
            assertThrows(IllegalArgumentException.class, () -> new DistributedComplete.EndpointAnswers(
                    allocator, streams, gates, SCHEMA, List.of("a", "b"), index -> { }));
            assertFalse(streams.isEmpty());
        }
    }

    /**
     * Builds an endpoint at one location.
     *
     * @param uri location
     * @return endpoint
     */
    private static FlightEndpoint endpoint(String uri) {
        return new FlightEndpoint(new Ticket(new byte[] {1}), new Location(java.net.URI.create(uri)));
    }

    /**
     * Builds a one-row batch.
     *
     * @param allocator allocator
     * @param value the row's value
     * @return batch
     */
    private static VectorSchemaRoot root(BufferAllocator allocator, long value) {
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator);
        BigIntVector vector = (BigIntVector) root.getVector(0);
        vector.allocateNew(1);
        vector.set(0, value);
        root.setRowCount(1);
        return root;
    }

    /**
     * Mocks an endpoint stream that answers one batch.
     *
     * @param root the batch
     * @return stream
     */
    private static FlightStream stream(VectorSchemaRoot root) {
        FlightStream stream = mock(FlightStream.class);
        when(stream.next()).thenReturn(true, false);
        when(stream.getRoot()).thenReturn(root);
        when(stream.getSchema()).thenReturn(SCHEMA);
        return stream;
    }
}
