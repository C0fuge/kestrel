package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.SessionOptionValueFactory;
import org.apache.arrow.flight.SetSessionOptionsRequest;
import org.apache.arrow.flight.SetSessionOptionsResult;
import org.apache.arrow.flight.client.ClientCookieMiddleware;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Item 39 (a): a table name written without a schema that two schemas have (tpch.orders next to
 * orders.orders) is refused as ambiguous, never as "does not exist", and reads the default
 * schema when the setting defaultSchema names one; names only one schema has read it as before.
 * A session schema (Flight SQL session option or call header {@code schema}) goes before both.
 */
@Tag("integration")
class PlainNameSchemaIntegrationTest {

    /**
     * Verifies the ambiguity error without a default schema, on execute and on prepare.
     *
     * @param dir temporary directory
     * @throws Exception on failure
     */
    @Test
    void anAmbiguousPlainNameIsRefusedAsAmbiguous(@TempDir Path dir) throws Exception {
        String data = writeData(dir);
        try (TestFlightServerHelper helper = TestFlightServerHelper.builder().dataDir(data)
                .resultMode("complete").start()) {
            assertEquals(5L, first(helper, "SELECT count(*) FROM lineitem"));
            assertEquals(1L, first(helper, "SELECT max(o_orderkey) FROM tpch.orders"));
            assertEquals(2L, first(helper, "SELECT max(o_orderkey) FROM orders.orders"));
            FlightRuntimeException run = assertThrows(FlightRuntimeException.class,
                    () -> first(helper, "SELECT count(*) FROM orders JOIN lineitem"
                            + " ON o_orderkey = l_orderkey"));
            assertAmbiguous(run);
            FlightRuntimeException prepare = assertThrows(FlightRuntimeException.class, () -> {
                try (FlightSqlClient.PreparedStatement p = helper.sqlClient().prepare(
                        "SELECT o_orderkey FROM orders WHERE o_orderkey = 1")) {
                    p.getResultSetSchema();
                }
            });
            assertAmbiguous(prepare);
        }
    }

    /**
     * Verifies the default schema decides an ambiguous plain name and leaves the others alone.
     *
     * @param dir temporary directory
     * @throws Exception on failure
     */
    @Test
    void theDefaultSchemaDecidesAnAmbiguousPlainName(@TempDir Path dir) throws Exception {
        String data = writeData(dir);
        String saved = System.getProperty("defaultSchema");
        System.setProperty("defaultSchema", "TPCH");
        try (TestFlightServerHelper helper = TestFlightServerHelper.builder().dataDir(data)
                .resultMode("complete").start()) {
            assertEquals(1L, first(helper, "SELECT max(o_orderkey) FROM orders"));
            assertEquals(5L, first(helper, "SELECT count(*) FROM orders o JOIN lineitem l"
                    + " ON o.o_orderkey = l.l_orderkey"));
            assertEquals(2L, first(helper, "SELECT max(o_orderkey) FROM orders.orders"));
            assertEquals(5L, first(helper, "SELECT count(*) FROM lineitem"));
        } finally {
            if (saved == null) {
                System.clearProperty("defaultSchema");
            } else {
                System.setProperty("defaultSchema", saved);
            }
        }
    }

    /**
     * Verifies the call header schema decides plain names in both result modes, on execute and
     * on prepared statements, before the default schema; unknown schemas are refused.
     *
     * @param dir temporary directory
     * @throws Exception on failure
     */
    @Test
    void theSchemaHeaderDecidesPlainNames(@TempDir Path dir) throws Exception {
        String data = writeData(dir);
        String saved = System.getProperty("defaultSchema");
        System.setProperty("defaultSchema", "tpch");
        try (TestFlightServerHelper helper = TestFlightServerHelper.builder().dataDir(data).start()) {
            for (String mode : new String[] {"partial", "complete"}) {
                assertEquals(2L, first(helper, "SELECT max(o_orderkey) FROM orders",
                        headers("orders", mode)), mode);
                assertEquals(2L, first(helper, "SELECT max(o_orderkey) FROM orders",
                        headers("Orders", mode)), mode);
                assertEquals(1L, first(helper, "SELECT max(o_orderkey) FROM orders",
                        headers("tpch", mode)), mode);
                assertEquals(1L, first(helper, "SELECT max(o_orderkey) FROM tpch.orders",
                        headers("orders", mode)), mode);
            }
            // Partial answers need every name qualified; complete answers read the rest as before:
            // lineitem is only in tpch, so it reads tpch next to orders.orders.
            assertEquals(0L, first(helper, "SELECT count(*) FROM orders o JOIN lineitem l"
                    + " ON o.o_orderkey = l.l_orderkey", headers("orders", "complete")));
            assertEquals(1L, first(helper, "SELECT max(o_orderkey) FROM orders",
                    headers(null, "complete")), "the default schema without a header");
            // Partial answers find a table's files by its name as written, so mixed case is
            // checked on the complete answer only.
            assertEquals(2L, first(helper, "SELECT max(o_orderkey) FROM Orders",
                    headers("ORDERS", "complete")));
            HeaderCallOption header = headers("orders", "complete");
            try (FlightSqlClient.PreparedStatement p = helper.sqlClient().prepare(
                    "SELECT max(o_orderkey) FROM orders", header)) {
                FlightInfo info = p.execute(header);
                assertEquals(2L, firstOf(helper.flightClient(), info));
            }
            FlightRuntimeException unknown = assertThrows(FlightRuntimeException.class,
                    () -> first(helper, "SELECT 1", headers("nope", "complete")));
            assertEquals(FlightStatusCode.INVALID_ARGUMENT, unknown.status().code());
            assertTrue(unknown.getMessage().contains("Unknown schema nope"), unknown.getMessage());
        } finally {
            restore(saved);
        }
    }

    /**
     * Verifies the Flight SQL session option schema decides plain names, the header goes before
     * it, unknown schemas are refused per option and an empty value unsets it.
     *
     * @param dir temporary directory
     * @throws Exception on failure
     */
    @Test
    void theSessionOptionSchemaDecidesPlainNames(@TempDir Path dir) throws Exception {
        String data = writeData(dir);
        try (TestFlightServerHelper helper = TestFlightServerHelper.builder().dataDir(data)
                .resultMode("complete").start();
                FlightClient client = FlightClient.builder(helper.allocator, helper.location)
                        .intercept(new ClientCookieMiddleware.Factory()).build()) {
            FlightSqlClient sql = new FlightSqlClient(client);
            SetSessionOptionsResult result = sql.setSessionOptions(new SetSessionOptionsRequest(
                    Map.of("schema", SessionOptionValueFactory.makeSessionOptionValue("orders"))));
            assertEquals(Map.of(), result.getErrors());
            assertEquals(2L, firstOf(client, sql.execute("SELECT max(o_orderkey) FROM orders")));
            try (FlightSqlClient.PreparedStatement p = sql.prepare("SELECT max(o_orderkey) FROM orders")) {
                assertEquals(2L, firstOf(client, p.execute()));
            }
            assertEquals(1L, firstOf(client, sql.execute("SELECT max(o_orderkey) FROM orders",
                    headers("tpch", "complete"))), "the header goes first");

            result = sql.setSessionOptions(new SetSessionOptionsRequest(
                    Map.of("schema", SessionOptionValueFactory.makeSessionOptionValue("nope"))));
            assertEquals(Map.of("schema", new SetSessionOptionsResult.Error(
                    SetSessionOptionsResult.ErrorValue.INVALID_VALUE)), result.getErrors());
            assertEquals(2L, firstOf(client, sql.execute("SELECT max(o_orderkey) FROM orders")),
                    "a refused value keeps the old one");

            result = sql.setSessionOptions(new SetSessionOptionsRequest(
                    Map.of("schema", SessionOptionValueFactory.makeEmptySessionOptionValue())));
            assertEquals(Map.of(), result.getErrors());
            assertAmbiguous(assertThrows(FlightRuntimeException.class,
                    () -> sql.execute("SELECT max(o_orderkey) FROM orders")));
        }
    }

    /**
     * Verifies a plain name of the session schema that the text rewrite misses (here hidden by
     * a named window of the same name) is refused instead of reading the default schema's table.
     *
     * @param dir temporary directory
     * @throws Exception on failure
     */
    @Test
    void aMissedNameOfTheSessionSchemaIsRefused(@TempDir Path dir) throws Exception {
        String data = writeData(dir);
        String saved = System.getProperty("defaultSchema");
        System.setProperty("defaultSchema", "tpch");
        try (TestFlightServerHelper helper = TestFlightServerHelper.builder().dataDir(data)
                .resultMode("complete").start()) {
            String sql = "SELECT max(o_orderkey) OVER orders FROM orders"
                    + " WINDOW orders AS (ORDER BY o_orderkey)";
            assertEquals(1L, first(helper, sql, headers(null, "complete")));
            FlightRuntimeException missed = assertThrows(FlightRuntimeException.class,
                    () -> first(helper, sql, headers("orders", "complete")));
            assertEquals(FlightStatusCode.INVALID_ARGUMENT, missed.status().code());
            assertTrue(missed.getMessage().contains("qualify it (orders.orders)"),
                    missed.getMessage());
        } finally {
            restore(saved);
        }
    }

    private static HeaderCallOption headers(String schema, String mode) {
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert("x-flight-result-mode", mode);
        if (schema != null) {
            headers.insert("schema", schema);
        }
        return new HeaderCallOption(headers);
    }

    private static void restore(String saved) {
        if (saved == null) {
            System.clearProperty("defaultSchema");
        } else {
            System.setProperty("defaultSchema", saved);
        }
    }

    private static void assertAmbiguous(FlightRuntimeException e) {
        assertEquals(FlightStatusCode.INVALID_ARGUMENT, e.status().code(), e.getMessage());
        assertTrue(e.getMessage().contains("Table orders exists in schemas orders, tpch"),
                e.getMessage());
        assertTrue(!e.getMessage().contains("does not exist"), e.getMessage());
    }

    /**
     * Writes tpch.orders (key 1), tpch.lineitem (5 rows of key 1) and orders.orders (key 2).
     *
     * @param dir temporary directory
     * @return data directory
     * @throws Exception on write failure
     */
    private static String writeData(Path dir) throws Exception {
        Path data = dir.resolve("data");
        Path tpchOrders = Files.createDirectories(data.resolve("tpch/orders"));
        Path lineitem = Files.createDirectories(data.resolve("tpch/lineitem"));
        Path otherOrders = Files.createDirectories(data.resolve("orders/orders"));
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT 1::BIGINT AS o_orderkey) TO '" + tpchOrders.resolve("p.parquet")
                    + "' (FORMAT parquet)");
            s.execute("COPY (SELECT 1::BIGINT AS l_orderkey FROM range(5)) TO '"
                    + lineitem.resolve("p.parquet") + "' (FORMAT parquet)");
            s.execute("COPY (SELECT 2::BIGINT AS o_orderkey) TO '"
                    + otherOrders.resolve("p.parquet") + "' (FORMAT parquet)");
        }
        return data.toString();
    }

    private static Object first(TestFlightServerHelper helper, String sql, CallOption... options)
            throws Exception {
        return firstOf(helper.flightClient(), helper.sqlClient().execute(sql, options), options);
    }

    private static Object firstOf(FlightClient client, FlightInfo info, CallOption... options)
            throws Exception {
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(endpoint.getTicket(), options)) {
                while (stream.next()) {
                    if (stream.getRoot().getRowCount() > 0) {
                        return stream.getRoot().getVector(0).getObject(0);
                    }
                }
            }
        }
        return null;
    }
}
