package pro.surpin.data.arrowflight.server;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.hazelcast.map.IMap;
import org.apache.arrow.flight.CancelFlightInfoRequest;
import org.apache.arrow.flight.CancelFlightInfoResult;
import org.apache.arrow.flight.CancelStatus;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import pro.surpin.data.arrowflight.server.model.HandleState;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A node runs an endpoint planned for another node (a backup location the client failed over
 * to): same answer, and the load reservation moves to the node that ran it and is released there.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EndpointFailoverIntegrationTest {

    private static final String GONE = "grpc+tcp://gone.invalid:32010";
    private static final String QUERY = "SELECT * FROM test_schema.test_table";

    private TestFlightServerHelper helper;

    @BeforeAll
    void setUp() throws Exception {
        helper = TestFlightServerHelper.builder().start();
    }

    @AfterAll
    void tearDown() throws Exception {
        if (helper != null) {
            helper.close();
        }
    }

    /** Reads every row of a ticket as text, one string per row. */
    private List<String> rows(Ticket ticket) throws Exception {
        List<String> rows = new ArrayList<>();
        try (FlightStream stream = helper.flightClient().getStream(ticket)) {
            while (stream.next()) {
                for (int row = 0; row < stream.getRoot().getRowCount(); row++) {
                    StringBuilder line = new StringBuilder();
                    for (var vector : stream.getRoot().getFieldVectors()) {
                        line.append(vector.getObject(row)).append('|');
                    }
                    rows.add(line.toString());
                }
            }
        }
        rows.sort(String::compareTo);
        return rows;
    }

    private static Ticket ticket(byte[] handle) {
        return new Ticket(Any.pack(FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFrom(handle)).build()).toByteArray());
    }

    private static byte[] handle(Ticket ticket) throws Exception {
        return Any.parseFrom(ticket.getBytes()).unpack(FlightSql.TicketStatementQuery.class)
                .getStatementHandle().toByteArray();
    }

    @Test
    void ticketPlannedForAnotherNodeGivesTheSameAnswerAndMovesItsLoad() throws Exception {
        FlightInfo info = helper.sqlClient().execute(QUERY);
        List<String> expected = new ArrayList<>();
        List<String> files = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            expected.addAll(rows(endpoint.getTicket()));
            HandleState state = helper.clusterService.resolveEndpointHandle(handle(endpoint.getTicket()));
            files.addAll(List.of(state.filePaths()));
        }
        expected.sort(String::compareTo);
        assertFalse(expected.isEmpty());

        String self = helper.location.getUri().toString();
        IMap<String, Long> registry = helper.hazelcastAdapter.serverRegistry();
        long selfBefore = registry.getOrDefault(self, 0L);
        registry.put(GONE, 1_000L);
        HandleState plannedElsewhere = HandleState.forServerFiles(
                QUERY, files.toArray(new String[0]), GONE, 300L, true);
        byte[] handle = helper.clusterService.createEndpointHandle(plannedElsewhere);

        assertEquals(expected, rows(ticket(handle)));

        assertEquals(700L, registry.get(GONE), "the planned node's load is handed over");
        assertEquals(selfBefore, awaitLoad(registry, self, selfBefore),
                "the running node took the load over and released it at the end");
        registry.remove(GONE);
    }

    @Test
    void releaseDoesNotBringBackANodeThatLeftTheRegistry() throws Exception {
        FlightInfo info = helper.sqlClient().execute(QUERY);
        List<String> files = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            files.addAll(List.of(helper.clusterService
                    .resolveEndpointHandle(handle(endpoint.getTicket())).filePaths()));
        }
        String self = helper.location.getUri().toString();
        IMap<String, Long> registry = helper.hazelcastAdapter.serverRegistry();
        long selfBefore = registry.getOrDefault(self, 0L);
        registry.remove(GONE);
        byte[] handle = helper.clusterService.createEndpointHandle(HandleState.forServerFiles(
                QUERY, files.toArray(new String[0]), GONE, 300L, true));

        assertFalse(rows(ticket(handle)).isEmpty());

        assertEquals(selfBefore, awaitLoad(registry, self, selfBefore));
        assertFalse(registry.containsKey(GONE), "a stale node must stay out of the pool");
        assertTrue(helper.clusterService.filterLiveServers(registry.keySet()).contains(self));
    }

    @Test
    void cancelFlightInfoReleasesTheLoadOfAnUnreadPlanAtOnce() throws Exception {
        FlightInfo info = helper.sqlClient().execute(QUERY);
        List<String> files = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            files.addAll(List.of(helper.clusterService
                    .resolveEndpointHandle(handle(endpoint.getTicket())).filePaths()));
        }
        String self = helper.location.getUri().toString();
        IMap<String, Long> registry = helper.hazelcastAdapter.serverRegistry();
        long selfBefore = registry.getOrDefault(self, 0L);
        // What the planner does for an endpoint of a multi-node plan: reserve, then sign.
        helper.clusterService.addLoad(self, 300L);
        byte[] handle = helper.clusterService.createEndpointHandle(HandleState.forServerFiles(
                QUERY, files.toArray(new String[0]), self, 300L, true));
        FlightInfo plan = new FlightInfo(info.getSchema(), info.getDescriptor(),
                List.of(new FlightEndpoint(ticket(handle))), -1, -1);
        assertEquals(selfBefore + 300L, registry.get(self));

        CancelFlightInfoResult result = helper.sqlClient().cancelFlightInfo(
                new CancelFlightInfoRequest(plan));

        assertEquals(CancelStatus.CANCELLED, result.getStatus());
        assertEquals(selfBefore, registry.get(self), "released at once, not after 10 minutes");
        assertEquals(CancelStatus.CANCELLED, helper.sqlClient().cancelFlightInfo(
                new CancelFlightInfoRequest(plan)).getStatus());
        assertEquals(selfBefore, registry.get(self), "a second cancel releases nothing more");
        assertFalse(rows(ticket(handle)).isEmpty(), "the ticket still answers");
        Thread.sleep(200);
        assertEquals(selfBefore, registry.get(self), "the read after the cancel releases nothing twice");
    }

    /**
     * Waits up to 10 s for a node's load to reach a value: the server releases an endpoint's
     * load when its stream ends, which can be just after the client has read the last batch.
     *
     * @param registry load registry
     * @param node     node URI
     * @param expected expected load
     * @return the load at the end of the wait
     * @throws InterruptedException when interrupted
     */
    private static long awaitLoad(IMap<String, Long> registry, String node, long expected)
            throws InterruptedException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
        long load = registry.getOrDefault(node, 0L);
        while (load != expected && System.nanoTime() < deadline) {
            Thread.sleep(20);
            load = registry.getOrDefault(node, 0L);
        }
        return load;
    }
}
