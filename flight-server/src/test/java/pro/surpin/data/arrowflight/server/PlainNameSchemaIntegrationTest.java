package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Item 39 (a): a table name written without a schema that two schemas have (tpch.orders next to
 * orders.orders) is refused as ambiguous, never as "does not exist", and reads the default
 * schema when the setting defaultSchema names one; names only one schema has read it as before.
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

    private static Object first(TestFlightServerHelper helper, String sql) throws Exception {
        FlightInfo info = helper.sqlClient().execute(sql);
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
}
