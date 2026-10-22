package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.adapters.ResultModeMiddleware;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that every result mode streams the schema it announces, for every column type of an
 * "all types" table: GetTables, the FlightInfo of a query and its DoGet streams must agree, and
 * both modes must return the same types and values (StarRocks M4: a partial-mode DoGet streamed
 * uint64 where GetTables and complete mode said decimal128(20,0), and the client wrapped values
 * of 2^63 and more).
 */
@Tag("integration")
class ResultModeSchemaIntegrationTest {

    /**
     * One column of the test table.
     *
     * @param name column name
     * @param expression DuckDB expression over the row number {@code r}
     * @param type DuckDB type
     */
    private record Column(String name, String expression, String type) {
    }

    /** The columns of the bench table types.alltypes_full, with edge values of each type. */
    private static final List<Column> COLUMNS = List.of(
            new Column("id", "r", "INTEGER"),
            new Column("c_bool", "r % 2 = 0", "BOOLEAN"),
            new Column("c_tinyint", "r % 100 - 50", "TINYINT"),
            new Column("c_smallint", "r * 300 - 15000", "SMALLINT"),
            new Column("c_int", "r * 40000000 - 2000000000", "INTEGER"),
            new Column("c_bigint", "(r - 50) * 180000000000000000", "BIGINT"),
            new Column("c_utinyint", "255 - r", "UTINYINT"),
            new Column("c_usmallint", "65535 - r", "USMALLINT"),
            new Column("c_uinteger", "4294967295 - r", "UINTEGER"),
            new Column("c_ubigint", "18446744073709551615 - r", "UBIGINT"),
            new Column("c_hugeint", "r * 100000000000000000000", "HUGEINT"),
            new Column("c_float", "r / 4", "FLOAT"),
            new Column("c_double", "r / 3", "DOUBLE"),
            new Column("c_dec9_2", "r * 1.25", "DECIMAL(9,2)"),
            new Column("c_dec18_4", "r * 12345.6789", "DECIMAL(18,4)"),
            new Column("c_dec38_10", "r * 1234567890.0123456789", "DECIMAL(38,10)"),
            new Column("c_varchar", "'v' || r || 'ä'", "VARCHAR"),
            new Column("c_blob", "('b' || r)::BLOB", "BLOB"),
            new Column("c_date", "DATE '1969-12-25' + r::INTEGER", "DATE"),
            new Column("c_time", "TIME '00:00:00.123456' + INTERVAL (r) MINUTE", "TIME"),
            new Column("c_ts", "TIMESTAMP '1969-12-31 23:59:58.5' + INTERVAL (r) SECOND", "TIMESTAMP"),
            new Column("c_ts_ms", "TIMESTAMP '1969-12-31 23:59:58.5' + INTERVAL (r) SECOND",
                    "TIMESTAMP_MS"),
            new Column("c_ts_ns", "CAST('1969-12-31 23:59:58.123456789' AS TIMESTAMP_NS) + INTERVAL (r) SECOND",
                    "TIMESTAMP_NS"),
            new Column("c_tstz", "TIMESTAMPTZ '2024-01-02 03:04:05.678+00' + INTERVAL (r) HOUR",
                    "TIMESTAMPTZ"),
            new Column("c_uuid", "('00000000-0000-4000-8000-' || lpad(r::VARCHAR, 12, '0'))", "UUID"),
            new Column("c_interval", "INTERVAL (r) DAY", "INTERVAL"),
            new Column("c_list", "[r, r + 1]", "BIGINT[]"),
            new Column("c_struct", "{'a': r, 'b': 's' || r}", "STRUCT(a INTEGER, b VARCHAR)"),
            new Column("c_map", "map(['k' || r], [r])", "MAP(VARCHAR, INTEGER)"));

    /** Column types without an order or a sum. */
    private static final Set<String> NESTED = Set.of("c_list", "c_struct", "c_map");

    /** Columns with a sum. */
    private static final Set<String> NUMERIC = Set.of("c_tinyint", "c_smallint", "c_int",
            "c_bigint", "c_utinyint", "c_usmallint", "c_uinteger", "c_ubigint", "c_hugeint",
            "c_float", "c_double", "c_dec9_2", "c_dec18_4", "c_dec38_10");

    private static final String TABLE = "types.alltypes";

    @TempDir
    static Path data;

    private static TestFlightServerHelper helper;

    /**
     * Writes the table as three files (the last one all NULLs) and starts a server whose default
     * is partial results.
     *
     * @throws Exception on write or startup failure
     */
    @BeforeAll
    static void startServer() throws Exception {
        Path dir = Files.createDirectories(data.resolve("types/alltypes"));
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (" + rows(0, 50) + ") TO '" + dir.resolve("part-0.parquet")
                    + "' (FORMAT parquet)");
            s.execute("COPY (" + rows(50, 100) + ") TO '" + dir.resolve("part-1.parquet")
                    + "' (FORMAT parquet)");
            List<String> nulls = new ArrayList<>();
            for (Column c : COLUMNS) {
                nulls.add(c.name().equals("id") ? "100::INTEGER AS id"
                        : "CAST(NULL AS " + c.type() + ") AS " + c.name());
            }
            s.execute("COPY (SELECT " + String.join(", ", nulls) + ") TO '"
                    + dir.resolve("part-2.parquet") + "' (FORMAT parquet)");
        }
        helper = TestFlightServerHelper.builder().dataDir(data.toString()).resultMode("partial").start();
    }

    /**
     * Renders the query of a range of rows.
     *
     * @param from first row number
     * @param to row number after the last
     * @return DuckDB query
     */
    private static String rows(int from, int to) {
        List<String> select = new ArrayList<>();
        for (Column c : COLUMNS) {
            select.add("CAST(" + c.expression() + " AS " + c.type() + ") AS " + c.name());
        }
        return "SELECT " + String.join(", ", select) + " FROM range(" + from + ", " + to + ") t(r)";
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
     * Verifies GetTables, FlightInfo and DoGet agree on every column in both modes, and both modes
     * stream the same rows. Unsigned integers are widened in both (uint64 as decimal128(20,0)).
     *
     * @throws Exception on query failure
     */
    @Test
    void everyColumnStreamsTheAnnouncedTypeInBothModes() throws Exception {
        Schema table = tableSchema();
        assertEquals(COLUMNS.size(), table.getFields().size(), table.toString());
        assertEquals(new ArrowType.Decimal(20, 0, 128), table.findField("c_ubigint").getType());
        assertEquals(new ArrowType.Int(16, true), table.findField("c_utinyint").getType());
        assertEquals(new ArrowType.Int(32, true), table.findField("c_usmallint").getType());
        assertEquals(new ArrowType.Int(64, true), table.findField("c_uinteger").getType());

        Answer all = sameInBothModes("SELECT * FROM " + TABLE);
        assertEquals(table.getFields(), all.schema().getFields(), "SELECT * vs GetTables");
        assertEquals(101, all.rows().size());

        for (Field column : table.getFields()) {
            String c = "\"" + column.getName() + "\"";
            Answer answer = sameInBothModes("SELECT " + c + " FROM " + TABLE);
            assertEquals(List.of(column), answer.schema().getFields(), column.getName());
            sameInBothModes("SELECT id, " + c + " FROM " + TABLE + " WHERE " + c
                    + " IS NOT NULL ORDER BY id DESC LIMIT 3");
            if (NESTED.contains(column.getName())) {
                continue;
            }
            Answer minMax = sameInBothModes("SELECT count(" + c + ") AS n, min(" + c + ") AS lo, max("
                    + c + ") AS hi FROM " + TABLE);
            assertEquals(column.getType(), minMax.schema().getFields().get(1).getType(),
                    "min(" + column.getName() + ")");
            sameInBothModes("SELECT " + c + ", count(*) AS n FROM " + TABLE + " GROUP BY " + c
                    + " ORDER BY " + c + " LIMIT 5");
            if (NUMERIC.contains(column.getName())) {
                String sum = "SELECT sum(" + c + ") AS s FROM " + TABLE + " WHERE id < 90";
                if (column.getType() instanceof ArrowType.FloatingPoint) {
                    sameFloatingSumInBothModes(sum);
                } else {
                    sameInBothModes(sum);
                }
            }
        }
    }

    /**
     * Verifies the uint64 values above the largest signed 64-bit value come out exactly.
     *
     * @throws Exception on query failure
     */
    @Test
    void largeUnsignedValuesAreExactInPartialMode() throws Exception {
        Answer answer = answer("SELECT c_ubigint FROM " + TABLE + " WHERE id = 0", "partial");
        assertEquals(List.of("18446744073709551615"), answer.rows());
    }

    /**
     * Verifies the answers that come from Parquet footers (count, integer min/max) stream the
     * announced schema in partial mode.
     *
     * @throws Exception on query failure
     */
    @Test
    void footerAnswersStreamTheAnnouncedSchema() throws Exception {
        assertEquals(List.of("101"), sameInBothModes("SELECT count(*) AS n FROM " + TABLE).rows());
        sameInBothModes("SELECT min(c_int) AS lo, max(c_int) AS hi, min(c_date) AS d, count(c_utinyint) AS u"
                + " FROM " + TABLE);
    }

    /**
     * Verifies expressions over aggregates in partial mode get the complete answer from one
     * endpoint (StarRocks M4: {@code CAST(sum(x) AS DECIMAL(38,2))} was a Binder Error) and that
     * FILTER is no longer dropped.
     *
     * @throws Exception on query failure
     */
    @Test
    void expressionsOverAggregatesGetTheCompleteAnswer() throws Exception {
        Answer cast = sameInBothModes(
                "SELECT CAST(sum(c_int) AS DECIMAL(38,2)) AS a FROM " + TABLE);
        assertEquals(new ArrowType.Decimal(38, 2, 128), cast.schema().getFields().get(0).getType());
        assertEquals(List.of("-2000000000.00"), cast.rows());
        Answer filtered = sameInBothModes("SELECT count(*) FILTER (WHERE c_bool) AS f, count(*) AS n,"
                + " avg(c_double) AS a FROM " + TABLE);
        assertEquals("50|101|16.5", filtered.rows().get(0));
        sameInBothModes("SELECT c_bool, sum(c_ubigint) / count(*) AS r FROM " + TABLE
                + " GROUP BY c_bool ORDER BY c_bool");
        assertEquals(1, info("SELECT avg(c_int) FROM " + TABLE, "partial").getEndpoints().size());
    }

    /**
     * The schema and rows of an answer.
     *
     * @param schema schema announced in the FlightInfo
     * @param rows rows as text, columns separated by {@code |}
     */
    private record Answer(Schema schema, List<String> rows) {
    }

    /**
     * Runs a query in both modes and checks each streams its announced schema, and both modes
     * announce the same schema and return the same rows (sorted unless the query orders them).
     *
     * @param sql query
     * @return the partial-mode answer
     * @throws Exception on query failure
     */
    private static Answer sameInBothModes(String sql) throws Exception {
        Answer partial = answer(sql, "partial");
        Answer complete = answer(sql, "complete");
        assertEquals(complete.schema(), partial.schema(), "schema of partial vs complete: " + sql);
        assertEquals(complete.rows(), partial.rows(), "rows of partial vs complete: " + sql);
        return partial;
    }

    /**
     * Like {@link #sameInBothModes} for a sum of a FLOAT or DOUBLE column: DuckDB adds
     * floating-point values in the order its threads finish, so the two modes (different SQL over
     * the same files) may differ in the last bits (1335.0 vs 1334.9999999999998 with more cores).
     *
     * @param sql query with one floating-point sum
     * @throws Exception on query failure
     */
    private static void sameFloatingSumInBothModes(String sql) throws Exception {
        Answer partial = answer(sql, "partial");
        Answer complete = answer(sql, "complete");
        assertEquals(complete.schema(), partial.schema(), "schema of partial vs complete: " + sql);
        assertEquals(1, complete.rows().size(), sql);
        assertEquals(1, partial.rows().size(), sql);
        double expected = Double.parseDouble(complete.rows().get(0));
        assertEquals(expected, Double.parseDouble(partial.rows().get(0)),
                Math.abs(expected) * 1e-9, "rows of partial vs complete: " + sql);
    }

    /**
     * Runs a query in a result mode and checks every stream has the announced schema.
     *
     * @param sql query
     * @param mode result mode header value
     * @return announced schema and rows
     * @throws Exception on query failure
     */
    private static Answer answer(String sql, String mode) throws Exception {
        FlightInfo info = info(sql, mode);
        Schema announced = info.getSchemaOptional().orElseThrow();
        List<String> rows = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = helper.flightClient().getStream(endpoint.getTicket())) {
                assertEquals(announced, stream.getSchema(), mode + " stream vs FlightInfo: " + sql);
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    assertEquals(announced, root.getSchema(), mode + " batch vs FlightInfo: " + sql);
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
        if (!sql.contains("ORDER BY")) {
            rows.sort(null);
        }
        return new Answer(announced, rows);
    }

    /**
     * Plans a query in a result mode.
     *
     * @param sql query
     * @param mode result mode header value
     * @return FlightInfo
     */
    private static FlightInfo info(String sql, String mode) {
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert(ResultModeMiddleware.HEADER, mode);
        return helper.sqlClient().execute(sql, new HeaderCallOption(headers));
    }

    /**
     * Reads the column schema GetTables announces for the table.
     *
     * @return table schema
     * @throws Exception on read failure
     */
    private static Schema tableSchema() throws Exception {
        FlightInfo info = helper.sqlClient().getTables(null, "types", "alltypes", null, true);
        try (FlightStream stream = helper.flightClient().getStream(
                info.getEndpoints().get(0).getTicket())) {
            assertTrue(stream.next());
            VarBinaryVector schemas = (VarBinaryVector) stream.getRoot().getVector("table_schema");
            return Schema.deserializeMessage(ByteBuffer.wrap(schemas.get(0)));
        }
    }
}
