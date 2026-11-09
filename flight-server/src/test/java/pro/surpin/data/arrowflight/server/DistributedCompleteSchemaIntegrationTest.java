package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.SessionOptionValueFactory;
import org.apache.arrow.flight.SetSessionOptionsRequest;
import org.apache.arrow.flight.client.ClientCookieMiddleware;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.server.services.DistributedComplete;
import pro.surpin.data.arrowflight.server.services.PeerStreamAdmission;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Tests that a distributed complete merge reads the schema a query's plain table names belong
 * to (items 18 and 39 (a)): the call header {@code schema}, the Flight SQL session option
 * {@code schema} and the setting {@code defaultSchema}, with two schemas that both have a table
 * {@code orders}. Endpoint calls carry no client headers, so a merge that planned or read the
 * name as written would read the other schema's rows without an error.
 */
@Tag("integration")
class DistributedCompleteSchemaIntegrationTest {

    private static final int FILES = 4;
    private static final String DEFAULT_SCHEMA = "tpch";
    private static final String OTHER_SCHEMA = "orders";
    private static Path dataDir;
    private static String savedDefaultSchema;
    private static TestFlightServerHelper helper;
    private static BufferAllocator peerAllocator;
    private static DistributedComplete distributed;

    /** Queries on the plain name {@code orders} whose shape merges. */
    private static final List<String> MERGED = List.of(
            "SELECT count(*) AS n, sum(o_orderkey) AS total, min(o_orderkey) AS lo FROM orders",
            "SELECT o_custkey, count(*) AS n, max(o_orderkey) AS hi FROM orders"
                    + " GROUP BY o_custkey ORDER BY o_custkey",
            "SELECT o_orderkey FROM orders WHERE o_orderkey % 9 = 0 ORDER BY o_orderkey DESC LIMIT 5",
            "SELECT avg(o_orderkey) AS a FROM orders");

    /**
     * Writes tpch.orders and orders.orders (four files each, different rows) and starts a
     * complete-mode server with four endpoints per plan and default schema tpch.
     *
     * @throws Exception on failure
     */
    @BeforeAll
    static void startServer() throws Exception {
        dataDir = Files.createTempDirectory("distributed-schema");
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = duck.createStatement()) {
            for (String schema : List.of(DEFAULT_SCHEMA, OTHER_SCHEMA)) {
                Path table = Files.createDirectories(dataDir.resolve(schema).resolve("orders"));
                boolean other = OTHER_SCHEMA.equals(schema);
                for (int f = 0; f < FILES; f++) {
                    statement.execute("COPY (SELECT (" + (other ? 1000 : 0) + " + i * " + FILES + " + " + f
                            + ")::BIGINT AS o_orderkey, (i % " + (other ? 5 : 7) + ")::INTEGER AS o_custkey"
                            + " FROM range(" + (other ? 50 : 100) + ") r(i)) TO '"
                            + table.resolve("part-" + f + ".parquet") + "' (FORMAT PARQUET)");
                }
            }
        }
        savedDefaultSchema = System.getProperty("defaultSchema");
        System.setProperty("defaultSchema", DEFAULT_SCHEMA);
        System.setProperty("endpointsPerServer", String.valueOf(FILES));
        helper = TestFlightServerHelper.builder().dataDir(dataDir.toString()).resultMode("complete")
                .start();
        peerAllocator = helper.allocator.newChildAllocator("peers", 0, Long.MAX_VALUE);
    }

    /**
     * Stops the server, restores the properties and removes the tables.
     *
     * @throws Exception on failure
     */
    @AfterAll
    static void stopServer() throws Exception {
        try {
            useDistributed(false);
            peerAllocator.close();
            helper.close();
        } finally {
            System.clearProperty("endpointsPerServer");
            if (savedDefaultSchema == null) {
                System.clearProperty("defaultSchema");
            } else {
                System.setProperty("defaultSchema", savedDefaultSchema);
            }
            try (var files = Files.walk(dataDir)) {
                files.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    /**
     * Turns the distributed path off after each test.
     *
     * @throws Exception on failure
     */
    @AfterEach
    void off() throws Exception {
        useDistributed(false);
    }

    /**
     * Verifies the header schema decides the table every endpoint of a merge reads, for both
     * schemas, and a query without the header reads the default schema (merged as well: the
     * plain name is planned as the table DuckDB resolves it to, item 39 (d) follow-up).
     *
     * @throws Exception on failure
     */
    @Test
    void mergeReadsTheHeaderSchema() throws Exception {
        for (String query : MERGED) {
            String fromDefault = alone(query, DEFAULT_SCHEMA);
            String fromOther = alone(query, OTHER_SCHEMA);
            assertNotEquals(fromDefault, fromOther, "the two tables differ: " + query);
            useDistributed(true);
            assertEquals(fromOther, answer(helper.flightClient(), query, headers(OTHER_SCHEMA)), query);
            assertEquals(fromDefault, answer(helper.flightClient(), query, headers(DEFAULT_SCHEMA)), query);
            assertEquals(fromDefault, answer(helper.flightClient(), query, headers(null)),
                    "default schema: " + query);
            assertEquals(3, distributed.mergedAnswers(), "merged from endpoint answers: " + query);
            assertEquals(0, distributed.fallbacks(), query);
            useDistributed(false);
        }
    }

    /**
     * Verifies the session option schema decides the table of a merge, on execute and on a
     * prepared statement, and the header still goes before it.
     *
     * @throws Exception on failure
     */
    @Test
    void mergeReadsTheSessionSchema() throws Exception {
        String query = MERGED.get(1);
        String fromDefault = alone(query, DEFAULT_SCHEMA);
        String fromOther = alone(query, OTHER_SCHEMA);
        useDistributed(true);
        try (FlightClient client = FlightClient.builder(helper.allocator, helper.location)
                .intercept(new ClientCookieMiddleware.Factory()).build()) {
            FlightSqlClient sql = new FlightSqlClient(client);
            assertEquals(Map.of(), sql.setSessionOptions(new SetSessionOptionsRequest(Map.of("schema",
                    SessionOptionValueFactory.makeSessionOptionValue(OTHER_SCHEMA)))).getErrors());
            assertEquals(fromOther, answer(client, query, headers(null)));
            try (FlightSqlClient.PreparedStatement p = sql.prepare(query, headers(null))) {
                assertEquals(fromOther, read(client, p.execute(headers(null))));
            }
            assertEquals(fromDefault, answer(client, query, headers(DEFAULT_SCHEMA)), "the header goes first");
        }
        assertEquals(3, distributed.mergedAnswers());
        assertEquals(0, distributed.fallbacks());
    }

    /**
     * Computes the answer of a query on one schema's table on this node alone, qualified.
     *
     * @param query query on the plain name orders
     * @param schema schema to read
     * @return schema and rows
     * @throws Exception on failure
     */
    private static String alone(String query, String schema) throws Exception {
        return answer(helper.flightClient(), query.replace("FROM orders", "FROM " + schema + ".orders"),
                headers(null));
    }

    /**
     * Turns the distributed path on or off on the producer, closing the previous one.
     *
     * @param on whether merges run
     * @throws Exception on failure
     */
    private static void useDistributed(boolean on) throws Exception {
        helper.flightSqlProducer.useDistributedComplete(null);
        if (distributed != null) {
            distributed.close();
            distributed = null;
        }
        if (on) {
            distributed = new DistributedComplete(helper.queryPlanner, helper.clusterService,
                    helper.executionService, peerAllocator,
                    DistributedComplete.clientFactory(peerAllocator, null),
                    new PeerStreamAdmission(64, PeerStreamAdmission.DEFAULT_WAIT_MILLIS));
            helper.flightSqlProducer.useDistributedComplete(distributed);
        }
    }

    /**
     * Builds the headers of a complete-answer call, with a schema header when one is given.
     *
     * @param schema schema header value, or null for none
     * @return call option
     */
    private static HeaderCallOption headers(String schema) {
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert("x-flight-result-mode", "complete");
        if (schema != null) {
            headers.insert("schema", schema);
        }
        return new HeaderCallOption(headers);
    }

    /**
     * Reads the complete answer of a query as text, with its schema.
     *
     * @param client Flight client of the call
     * @param query client SQL
     * @param options call options
     * @return schema and rows
     * @throws Exception on failure
     */
    private static String answer(FlightClient client, String query, CallOption... options) throws Exception {
        return read(client, new FlightSqlClient(client).execute(query, options));
    }

    /**
     * Reads the one endpoint of a complete answer as text, with its schema.
     *
     * @param client Flight client of the call
     * @param info planned answer
     * @return schema and rows
     * @throws Exception on failure
     */
    private static String read(FlightClient client, FlightInfo info) throws Exception {
        assertEquals(1, info.getEndpoints().size());
        StringBuilder text = new StringBuilder();
        try (FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket())) {
            text.append(stream.getSchema()).append('\n');
            while (stream.next()) {
                text.append(stream.getRoot().contentToTSVString());
            }
        }
        return text.toString();
    }
}
