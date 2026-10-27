package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.CloseSessionResult;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.GetSessionOptionsRequest;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.SessionOptionValue;
import org.apache.arrow.flight.SessionOptionValueFactory;
import org.apache.arrow.flight.SetSessionOptionsRequest;
import org.apache.arrow.flight.SetSessionOptionsResult;
import org.apache.arrow.flight.client.ClientCookieMiddleware;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.adapters.ResultModeMiddleware;
import pro.surpin.data.arrowflight.server.adapters.StreamCompression;
import pro.surpin.data.arrowflight.server.metrics.CompressionMetrics;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that a client asking for {@code arrow-compression: zstd} or {@code lz4} gets compressed
 * batches with the same answer as a plain read, in both result modes, and that clients that do
 * not ask keep getting plain batches.
 */
@Tag("integration")
class StreamCompressionIntegrationTest {

    private static final String TABLE = "comp.wide";
    private static final int ROWS = 120_000;

    @TempDir
    static Path data;

    private static TestFlightServerHelper helper;

    /**
     * Writes two files of mixed column types and starts a server that compresses only on request.
     *
     * @throws Exception on write or startup failure
     */
    @BeforeAll
    static void startServer() throws Exception {
        Path dir = Files.createDirectories(data.resolve("comp/wide"));
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            for (int part = 0; part < 2; part++) {
                s.execute("COPY (SELECT r::BIGINT AS id, (r % 97)::INTEGER AS k,"
                        + " CASE WHEN r % 11 = 0 THEN NULL ELSE 'name-' || (r % 1000) END AS name,"
                        + " (r * 1.25)::DECIMAL(18,2) AS amount, r / 7.0 AS ratio,"
                        + " DATE '2020-01-01' + (r % 3000)::INTEGER AS day,"
                        + " [r % 5, r % 7] AS pair, {'a': r % 13, 'b': 'x' || (r % 17)} AS rec"
                        + " FROM range(" + part * ROWS / 2 + ", " + (part + 1) * ROWS / 2 + ") t(r))"
                        + " TO '" + dir.resolve("part-" + part + ".parquet") + "' (FORMAT parquet)");
            }
        }
        helper = TestFlightServerHelper.builder().dataDir(data.toString()).resultMode("partial").start();
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

    /**
     * Verifies zstd and lz4 reads (lz4 decoded by commons-compress) return the plain answer in both result modes and the batches
     * really were compressed.
     *
     * @throws Exception on query failure
     */
    @Test
    void compressedReadsReturnThePlainAnswer() throws Exception {
        String sql = "SELECT * FROM " + TABLE;
        for (String mode : List.of("partial", "complete")) {
            List<String> plain = read(sql, mode, null);
            assertEquals(ROWS, plain.size(), mode);
            for (String codec : List.of("zstd", "lz4")) {
                long before = CompressionMetrics.compressedBatches();
                assertEquals(plain, read(sql, mode, codec), mode + " " + codec);
                assertTrue(CompressionMetrics.compressedBatches() > before, mode + " " + codec);
            }
        }
        List<String> filtered = read("SELECT name, sum(amount) AS s FROM " + TABLE
                + " GROUP BY name", "complete", null);
        assertEquals(filtered, read("SELECT name, sum(amount) AS s FROM " + TABLE
                + " GROUP BY name", "complete", "zstd"));
    }

    /**
     * Verifies a client that does not ask, or asks for none, gets plain batches.
     *
     * @throws Exception on query failure
     */
    @Test
    void clientsThatDoNotAskGetPlainBatches() throws Exception {
        long compressed = CompressionMetrics.compressedBatches();
        FlightInfo info = info("SELECT * FROM " + TABLE, "complete");
        List<String> rows = new ArrayList<>();
        try (FlightStream stream = helper.flightClient().getStream(info.getEndpoints().get(0).getTicket())) {
            collect(stream, rows);
        }
        try (FlightStream stream = helper.flightClient().getStream(info.getEndpoints().get(0).getTicket(),
                header("none"))) {
            collect(stream, rows);
        }
        assertEquals(2 * ROWS, rows.size());
        assertEquals(compressed, CompressionMetrics.compressedBatches());
    }

    /**
     * Verifies small answers stay plain under the size threshold even when the client asked.
     *
     * @throws Exception on query failure
     */
    @Test
    void smallBatchesArePlain() throws Exception {
        long compressed = CompressionMetrics.compressedBatches();
        List<String> rows = read("SELECT count(*) AS n FROM " + TABLE, "complete", "zstd");
        assertEquals(List.of(String.valueOf(ROWS)), rows);
        assertEquals(compressed, CompressionMetrics.compressedBatches());
    }

    /**
     * Verifies an unknown codec name is refused instead of being ignored.
     *
     * @throws Exception on planning failure
     */
    @Test
    void unknownCodecIsRefused() throws Exception {
        FlightInfo info = info("SELECT id FROM " + TABLE + " LIMIT 1", "complete");
        FlightRuntimeException e = assertThrows(FlightRuntimeException.class, () -> {
            try (FlightStream stream = helper.flightClient().getStream(
                    info.getEndpoints().get(0).getTicket(), header("gzip"))) {
                stream.next();
            }
        });
        assertEquals(CallStatus.INVALID_ARGUMENT.code(), e.status().code());
        assertTrue(e.getMessage().contains("gzip"), e.getMessage());
    }

    /**
     * Verifies the Flight SQL session option arrow-compression compresses the session's reads
     * without the header, the header still wins, bad options are reported per option, and after
     * CloseSession (or with an unknown cookie) reads are plain again.
     *
     * @throws Exception on query failure
     */
    @Test
    void sessionOptionCompressesTheSessionsReads() throws Exception {
        String sql = "SELECT * FROM " + TABLE;
        List<String> plain = read(helper.sqlClient(), helper.flightClient(), sql, "complete", null);
        try (FlightClient client = FlightClient.builder(helper.allocator, helper.location)
                .intercept(new ClientCookieMiddleware.Factory()).build()) {
            FlightSqlClient sql2 = new FlightSqlClient(client);
            SetSessionOptionsResult result = sql2.setSessionOptions(new SetSessionOptionsRequest(Map.of(
                    StreamCompression.HEADER, SessionOptionValueFactory.makeSessionOptionValue("zstd"),
                    "catalog", SessionOptionValueFactory.makeSessionOptionValue("x"))));
            assertEquals(Map.of("catalog",
                    new SetSessionOptionsResult.Error(SetSessionOptionsResult.ErrorValue.INVALID_NAME)),
                    result.getErrors());
            Map<String, SessionOptionValue> options =
                    sql2.getSessionOptions(new GetSessionOptionsRequest()).getSessionOptions();
            assertEquals(Map.of(StreamCompression.HEADER,
                    SessionOptionValueFactory.makeSessionOptionValue("zstd")), options);

            for (String mode : List.of("partial", "complete")) {
                long before = CompressionMetrics.compressedBatches();
                assertEquals(plain, read(sql2, client, sql, mode, null), mode);
                assertTrue(CompressionMetrics.compressedBatches() > before, mode);
            }
            long before = CompressionMetrics.compressedBatches();
            assertEquals(plain, read(sql2, client, sql, "complete", "none"));
            assertEquals(before, CompressionMetrics.compressedBatches(), "the header goes first");

            result = sql2.setSessionOptions(new SetSessionOptionsRequest(Map.of(StreamCompression.HEADER,
                    SessionOptionValueFactory.makeSessionOptionValue("gzip"))));
            assertEquals(Map.of(StreamCompression.HEADER,
                    new SetSessionOptionsResult.Error(SetSessionOptionsResult.ErrorValue.INVALID_VALUE)),
                    result.getErrors());
            assertEquals(options, sql2.getSessionOptions(new GetSessionOptionsRequest()).getSessionOptions(),
                    "a refused value keeps the old one");

            assertEquals(CloseSessionResult.Status.CLOSED,
                    sql2.closeSession(new CloseSessionRequest()).getStatus());
            assertEquals(Map.of(), sql2.getSessionOptions(new GetSessionOptionsRequest()).getSessionOptions());
            before = CompressionMetrics.compressedBatches();
            assertEquals(plain, read(sql2, client, sql, "complete", null));
            assertEquals(before, CompressionMetrics.compressedBatches(), "closed session reads plain");
        }

        FlightCallHeaders stale = new FlightCallHeaders();
        stale.insert("cookie", "arrow_flight_session_id=unknown-session");
        long before = CompressionMetrics.compressedBatches();
        List<String> rows = new ArrayList<>();
        HeaderCallOption staleCookie = new HeaderCallOption(stale);
        FlightInfo info = helper.sqlClient().execute(sql, staleCookie);
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket(), staleCookie)) {
                collect(stream, rows);
            }
        }
        Collections.sort(rows);
        assertEquals(plain, rows, "an unknown session cookie does not fail the call");
        assertEquals(before, CompressionMetrics.compressedBatches());
    }

    /**
     * Plans a query in a result mode.
     *
     * @param sql query
     * @param mode result mode header value
     * @return flight info
     */
    private static FlightInfo info(String sql, String mode) {
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert(ResultModeMiddleware.HEADER, mode);
        return helper.sqlClient().execute(sql, new HeaderCallOption(headers));
    }

    /**
     * Builds the compression header option.
     *
     * @param codec header value
     * @return call option
     */
    private static CallOption header(String codec) {
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert(StreamCompression.HEADER, codec);
        return new HeaderCallOption(headers);
    }

    /**
     * Reads every endpoint of a query. Java Flight decodes compressed batches when
     * arrow-compression is on the class path (its codec factory is found by ServiceLoader).
     *
     * @param sql query
     * @param mode result mode
     * @param codec compression header value, or null for none
     * @return rows as text, sorted
     * @throws Exception on query failure
     */
    private static List<String> read(String sql, String mode, String codec) throws Exception {
        return read(helper.sqlClient(), helper.flightClient(), sql, mode, codec);
    }

    /**
     * Reads every endpoint of a query through given clients.
     *
     * @param sqlClient client that plans
     * @param client client that reads
     * @param sql query
     * @param mode result mode
     * @param codec compression header value, or null for none
     * @return rows as text, sorted
     * @throws Exception on query failure
     */
    private static List<String> read(FlightSqlClient sqlClient, FlightClient client, String sql, String mode,
            String codec) throws Exception {
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert(ResultModeMiddleware.HEADER, mode);
        List<String> rows = new ArrayList<>();
        for (FlightEndpoint endpoint : sqlClient.execute(sql, new HeaderCallOption(headers)).getEndpoints()) {
            CallOption[] options = codec == null ? new CallOption[0] : new CallOption[]{header(codec)};
            try (FlightStream stream = client.getStream(endpoint.getTicket(), options)) {
                collect(stream, rows);
            }
        }
        Collections.sort(rows);
        return rows;
    }

    /**
     * Collects the rows of a stream as text.
     *
     * @param stream stream
     * @param rows destination
     */
    private static void collect(FlightStream stream, List<String> rows) {
        while (stream.next()) {
            VectorSchemaRoot root = stream.getRoot();
            for (int i = 0; i < root.getRowCount(); i++) {
                List<String> values = new ArrayList<>();
                for (FieldVector vector : root.getFieldVectors()) {
                    Object value = vector.getObject(i);
                    values.add(value instanceof byte[] bytes ? Arrays.toString(bytes)
                            : String.valueOf(value));
                }
                rows.add(String.join("|", values));
            }
        }
    }
}
