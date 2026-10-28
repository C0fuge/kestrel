package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.adapters.ResultModeMiddleware;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests plain Flight descriptors (plainFlightCommands) end to end with a plain Flight client, as
 * ClickHouse's arrowFlight reads: GetSchema, GetFlightInfo, then DoGet on every endpoint.
 */
@Tag("integration")
class PlainFlightCommandsIntegrationTest {

    private static TestFlightServerHelper helper;

    /**
     * Starts a server whose Flight SQL default is partial answers, with plain descriptors on.
     *
     * @throws Exception on startup failure
     */
    @BeforeAll
    static void startServer() throws Exception {
        helper = TestFlightServerHelper.builder().resultMode("partial").start();
        helper.flightSqlProducer.acceptPlainCommands(true);
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

    /** Verifies an aggregate sent as SQL text gets one complete answer, not one row per part. */
    @Test
    void sqlTextGetsTheCompleteAnswer() throws Exception {
        FlightInfo info = helper.flightClient().getInfo(
                command("SELECT count(*) AS n, min(tinyint_col) AS lo FROM test_schema.test_table"));
        assertEquals(1, info.getEndpoints().size());
        assertEquals(1L, rows(info));
        assertEquals(info.getSchema(), helper.flightClient().getSchema(
                command("SELECT count(*) AS n, min(tinyint_col) AS lo FROM test_schema.test_table")).getSchema());
        assertTrue(count() > 0);
    }

    /** Verifies a table path and its SQL text return every row, with the schema GetSchema gives. */
    @Test
    void tablePathReadsTheWholeTable() throws Exception {
        long count = count();
        for (FlightDescriptor descriptor : new FlightDescriptor[] {
            FlightDescriptor.path("test_schema.test_table"),
            FlightDescriptor.path("test_schema", "test_table"),
            command("SELECT * FROM test_schema.test_table")}) {
            FlightInfo info = helper.flightClient().getInfo(descriptor);
            assertEquals(count, rows(info), descriptor.toString());
            assertEquals(info.getSchema(), helper.flightClient().getSchema(descriptor).getSchema());
        }
    }

    /** Verifies a client asking for parts gets them, and the parts add up to the whole table. */
    @Test
    void partsOnRequest() throws Exception {
        CallHeaders headers = new FlightCallHeaders();
        headers.insert(ResultModeMiddleware.HEADER, "partial");
        FlightInfo info = helper.flightClient().getInfo(FlightDescriptor.path("test_schema.test_table"),
                new HeaderCallOption(headers));
        assertEquals(count(), rows(info));
    }

    /** Verifies bad descriptors and SQL are refused with a client error, and Flight SQL still works. */
    @Test
    void refusalsAndFlightSql() throws Exception {
        assertEquals(FlightStatusCode.INVALID_ARGUMENT, assertThrows(FlightRuntimeException.class,
                () -> helper.flightClient().getInfo(FlightDescriptor.path("test_schema.test_table;"))).status().code());
        assertEquals(FlightStatusCode.INVALID_ARGUMENT, assertThrows(FlightRuntimeException.class,
                () -> helper.flightClient().getInfo(command("   "))).status().code());
        FlightRuntimeException missing = assertThrows(FlightRuntimeException.class,
                () -> helper.flightClient().getInfo(FlightDescriptor.path("test_schema.no_such_table")));
        assertTrue(missing.status().code() == FlightStatusCode.NOT_FOUND
                || missing.status().code() == FlightStatusCode.INVALID_ARGUMENT, missing.toString());
        assertEquals(FlightStatusCode.INVALID_ARGUMENT, assertThrows(FlightRuntimeException.class,
                () -> helper.flightClient().getInfo(command("SELECT 1; SELECT 2"))).status().code());
        FlightSqlClient sql = helper.sqlClient();
        assertTrue(rows(sql.execute("SELECT * FROM test_schema.test_table")) > 0);
    }

    /** Verifies plain descriptors are refused when the setting is off. */
    @Test
    void refusedWhenOff() {
        helper.flightSqlProducer.acceptPlainCommands(false);
        try {
            assertEquals(FlightStatusCode.INVALID_ARGUMENT, assertThrows(FlightRuntimeException.class,
                    () -> helper.flightClient().getInfo(command("SELECT 1"))).status().code());
        } finally {
            helper.flightSqlProducer.acceptPlainCommands(true);
        }
    }

    /**
     * Counts the table's rows through Flight SQL in complete mode.
     *
     * @return row count
     * @throws Exception on read failure
     */
    private static long count() throws Exception {
        CallHeaders headers = new FlightCallHeaders();
        headers.insert(ResultModeMiddleware.HEADER, "complete");
        FlightInfo info = helper.sqlClient().execute("SELECT count(*) FROM test_schema.test_table",
                new HeaderCallOption(headers));
        try (FlightStream stream = helper.flightClient().getStream(info.getEndpoints().get(0).getTicket())) {
            stream.next();
            return (Long) stream.getRoot().getVector(0).getObject(0);
        }
    }

    /**
     * Reads every endpoint over the client's own connection, as ClickHouse does.
     *
     * @param info planned query
     * @return rows read
     * @throws Exception on read failure
     */
    private static long rows(FlightInfo info) throws Exception {
        long rows = 0;
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    rows += stream.getRoot().getRowCount();
                }
            }
        }
        return rows;
    }

    /**
     * Builds a command descriptor of SQL text.
     *
     * @param sql query text
     * @return descriptor
     */
    private static FlightDescriptor command(String sql) {
        return FlightDescriptor.command(sql.getBytes(StandardCharsets.UTF_8));
    }
}
