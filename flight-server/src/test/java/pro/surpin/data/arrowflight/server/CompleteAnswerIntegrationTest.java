package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests the complete-answer mode end to end through a Flight SQL client. */
@Tag("integration")
class CompleteAnswerIntegrationTest {

    private static TestFlightServerHelper helper;

    /**
     * Starts a server whose default is the complete answer.
     *
     * @throws Exception on startup failure
     */
    @BeforeAll
    static void startServer() throws Exception {
        helper = TestFlightServerHelper.builder().resultMode("complete").start();
    }

    /**
     * Stops the server.
     *
     * @throws Exception on shutdown failure
     */
    @AfterAll
    static void stopServer() throws Exception {
        if (helper != null) {
            helper.close();
        }
    }

    /** Verifies a query returns one endpoint with the whole answer. */
    @Test
    void queryReturnsOneEndpoint() throws Exception {
        FlightInfo info = helper.sqlClient().execute(
                "WITH c AS (SELECT count(*) AS n FROM test_schema.test_table) SELECT n FROM c");
        assertEquals(1, info.getEndpoints().size());
        assertNotEquals(0L, firstValue(info));
    }

    /** Verifies a prepared statement with a parameter answers for each value it is given. */
    @Test
    void preparedStatementBindsParameterValues() throws Exception {
        long zero = (Long) firstValue(helper.sqlClient().execute(
                "SELECT count(*) FROM test_schema.test_table WHERE tinyint_col = 0"));
        long one = (Long) firstValue(helper.sqlClient().execute(
                "SELECT count(*) FROM test_schema.test_table WHERE tinyint_col = 1"));
        try (FlightSqlClient.PreparedStatement prepared = helper.sqlClient().prepare(
                "SELECT count(*) FROM test_schema.test_table WHERE tinyint_col = ?")) {
            assertEquals(1, prepared.getParameterSchema().getFields().size());
            assertEquals(new ArrowType.Int(8, true),
                    prepared.getParameterSchema().getFields().get(0).getType());
            try (VectorSchemaRoot values = VectorSchemaRoot.create(prepared.getParameterSchema(),
                    helper.allocator)) {
                TinyIntVector value = (TinyIntVector) values.getVector(0);
                value.setSafe(0, 0);
                values.setRowCount(1);
                prepared.setParameters(values);
                assertEquals(zero, firstValue(prepared.execute()));

                value.setSafe(0, 1);
                values.setRowCount(1);
                prepared.setParameters(values);
                assertEquals(one, firstValue(prepared.execute()));
            }
        }
    }

    /** Verifies a text with two statements is refused as an invalid argument. */
    @Test
    void severalStatementsAreRefused() {
        FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                () -> helper.sqlClient().execute("SELECT 1; SELECT 2"));
        assertEquals(FlightStatusCode.INVALID_ARGUMENT, e.status().code());
    }

    /**
     * Reads the first value of a query's answer.
     *
     * @param info planned query
     * @return first value of the first column
     * @throws Exception on read failure
     */
    private static Object firstValue(FlightInfo info) throws Exception {
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    if (stream.getRoot().getRowCount() > 0) {
                        return stream.getRoot().getVector(0).getObject(0);
                    }
                }
            }
        }
        return null;
    }

    /** Verifies a client that keeps the first handle after sending values still gets its answer. */
    @Test
    void valuesSentForTheFirstHandleAreUsed() throws Exception {
        long one = (Long) firstValue(helper.sqlClient().execute(
                "SELECT count(*) FROM test_schema.test_table WHERE tinyint_col = 1"));
        org.apache.arrow.flight.FlightClient client = helper.flightClient();
        org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest request =
                org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest.newBuilder()
                        .setQuery("SELECT count(*) FROM test_schema.test_table WHERE tinyint_col = ?").build();
        java.util.Iterator<org.apache.arrow.flight.Result> results = client.doAction(new org.apache.arrow.flight.Action(
                "CreatePreparedStatement", com.google.protobuf.Any.pack(request).toByteArray()));
        com.google.protobuf.ByteString handle = com.google.protobuf.Any.parseFrom(results.next().getBody())
                .unpack(org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult.class)
                .getPreparedStatementHandle();
        while (results.hasNext()) {
            results.next();
        }
        org.apache.arrow.flight.FlightDescriptor descriptor = org.apache.arrow.flight.FlightDescriptor.command(
                com.google.protobuf.Any.pack(org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery
                        .newBuilder().setPreparedStatementHandle(handle).build()).toByteArray());
        org.apache.arrow.vector.types.pojo.Schema parameters = new org.apache.arrow.vector.types.pojo.Schema(
                java.util.List.of(org.apache.arrow.vector.types.pojo.Field.nullable("p", new ArrowType.Int(8, true))));
        try (VectorSchemaRoot values = VectorSchemaRoot.create(parameters, helper.allocator)) {
            ((TinyIntVector) values.getVector(0)).setSafe(0, 1);
            values.setRowCount(1);
            org.apache.arrow.flight.SyncPutListener ack = new org.apache.arrow.flight.SyncPutListener();
            org.apache.arrow.flight.FlightClient.ClientStreamListener put = client.startPut(descriptor, values, ack);
            put.putNext();
            put.completed();
            put.getResult();
            ack.close();
        }
        // As an old client does: plan with the handle it already had.
        assertEquals(one, firstValue(client.getInfo(descriptor)));
    }

    /** Verifies a parameter of no known type announces the columns it then streams. */
    @Test
    void untypedParameterKeepsTheAnnouncedSchema() throws Exception {
        try (FlightSqlClient.PreparedStatement prepared = helper.sqlClient().prepare("SELECT ? AS v")) {
            assertEquals(ArrowType.Utf8.INSTANCE, prepared.getParameterSchema().getFields().get(0).getType());
            assertEquals(ArrowType.Utf8.INSTANCE, prepared.getResultSetSchema().getFields().get(0).getType());
            try (VectorSchemaRoot values = VectorSchemaRoot.create(prepared.getParameterSchema(),
                    helper.allocator)) {
                ((org.apache.arrow.vector.VarCharVector) values.getVector(0))
                        .setSafe(0, "abc".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                values.setRowCount(1);
                prepared.setParameters(values);
                FlightInfo info = prepared.execute();
                assertEquals(ArrowType.Utf8.INSTANCE, info.getSchema().getFields().get(0).getType());
                assertEquals("abc", String.valueOf(firstValue(info)));
            }
        }
    }

    /** Verifies a parameter that does not fit an empty text is still prepared (? + 1, CAST). */
    @Test
    void untypedParameterInArithmeticIsPrepared() throws Exception {
        for (String sql : new String[] {"SELECT ? + 1 AS v", "SELECT CAST(?::VARCHAR AS INTEGER) + 1 AS v",
                "SELECT abs(?) AS v"}) {
            try (FlightSqlClient.PreparedStatement prepared = helper.sqlClient().prepare(sql)) {
                assertEquals(1, prepared.getParameterSchema().getFields().size(), sql);
                assertEquals(1, prepared.getResultSetSchema().getFields().size(), sql);
            }
        }
    }

    /** Verifies an error raised by DuckDB while the query runs is the client's error. */
    @Test
    void runtimeQueryErrorIsInvalidArgument() {
        FlightRuntimeException e = assertThrows(FlightRuntimeException.class, () -> {
            FlightInfo info = helper.sqlClient().execute("SELECT CAST(v AS INTEGER) FROM (VALUES ('x')) t(v)");
            firstValue(info);
        });
        assertEquals(FlightStatusCode.INVALID_ARGUMENT, e.status().code());
    }
}
