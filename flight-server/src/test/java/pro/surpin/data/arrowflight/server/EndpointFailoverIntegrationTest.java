package pro.surpin.data.arrowflight.server;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.hazelcast.map.IMap;
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
        assertEquals(selfBefore, registry.getOrDefault(self, 0L),
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

        assertFalse(registry.containsKey(GONE), "a stale node must stay out of the pool");
        assertEquals(selfBefore, registry.getOrDefault(self, 0L));
        assertTrue(helper.clusterService.filterLiveServers(registry.keySet()).contains(self));
    }
}
