package pro.surpin.data.arrowflight.server.services;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks that averages merged from the sums and counts each endpoint gives for its own rows are
 * DuckDB's complete answer bit for bit, and that averages without such a merge are refused.
 */
@Tag("unit")
class AverageMergeTest {

    /** Rows of one endpoint each, and one endpoint without rows. */
    private static final String[] PARTS = {"id % 3 = 0", "id % 3 = 1", "id % 3 = 2", "id < 0"};

    private Connection connection;

    /**
     * Creates the source rows: 30000 ids over seven groups, with null quantities.
     *
     * @throws SQLException when DuckDB fails
     */
    @BeforeEach
    void createRows() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = connection.createStatement()) {
            st.execute("CREATE TABLE src AS SELECT i::INTEGER AS id, 'g' || (i % 7) AS grp,"
                    + " CASE WHEN i % 7 = 0 THEN NULL ELSE (i * 7919) % 100003 END::BIGINT AS qty,"
                    + " ((i * 104729) % 9999991 / 100.0)::DECIMAL(15,2) AS price,"
                    + " ((i * 31) % 1000003 / 10000.0)::DECIMAL(18,4) AS rate,"
                    + " (i % 1000) / 8.0 AS d"
                    + " FROM range(30000) r(i)");
            st.execute("CREATE SCHEMA s");
        }
    }

    /**
     * Closes the DuckDB connection.
     *
     * @throws SQLException when DuckDB fails
     */
    @AfterEach
    void close() throws SQLException {
        connection.close();
    }

    /** Verifies merged averages equal DuckDB's averages over all rows, names, types and order. */
    @Test
    void mergedAveragesAreTheCompleteAnswer() throws SQLException {
        String[] queries = {
            "SELECT avg(qty) FROM s.t",
            "SELECT avg(price) AS ap, count(*) AS n, avg(rate), avg(d) AS ad FROM s.t WHERE id > 10",
            "SELECT grp, avg(price) AS ap, sum(qty) AS sq, avg(qty) FROM s.t GROUP BY grp ORDER BY ap DESC",
            "SELECT grp, avg(qty) AS aq FROM s.t GROUP BY grp ORDER BY 1",
            "SELECT grp, avg(CASE WHEN grp = 'g0' THEN NULL ELSE price END) AS half FROM s.t"
                    + " GROUP BY grp ORDER BY grp",
            "SELECT avg(price) AS ap FROM s.t WHERE id > 100000",
            "SELECT grp, avg(qty) AS qty FROM s.t GROUP BY grp ORDER BY qty",
        };
        for (String query : queries) {
            AverageMerge rewrite = AverageMerge.of(query);
            assertNotNull(rewrite, query);
            ParquetQueryParser part = ParquetQueryParser.parse(rewrite.partQuery());
            assertNull(CompleteAnswerMerge.refusal(part), rewrite.partQuery());
            List<String> types = new ArrayList<>();
            List<String> columns = endpointAnswers(rewrite.partQuery(), types);
            Answer expected = answer(query, "src");
            List<String> outputNames = expected.header.stream()
                    .map(h -> h.substring(0, h.lastIndexOf(':'))).toList();
            String merge = rewrite.mergeSql(part, "parts", columns, scales(types), outputNames);
            Answer merged = answer(merge, null);
            assertEquals(expected.header, merged.header, query + " -> " + merge);
            if (query.contains("ORDER BY")) {
                assertEquals(expected.rows, merged.rows, query + " -> " + merge);
            } else {
                assertEquals(sorted(expected.rows), sorted(merged.rows), query + " -> " + merge);
            }
        }
    }

    /** Verifies the part query has a sum and a count per average, no ORDER BY, same filter. */
    @Test
    void partQueryHasSumAndCountPerAverage() {
        AverageMerge rewrite = AverageMerge.of(
                "SELECT grp, avg(qty) AS aq, max(id) FROM s.t WHERE id > 3 GROUP BY grp ORDER BY aq;");
        assertNotNull(rewrite);
        ParquetQueryParser part = ParquetQueryParser.parse(rewrite.partQuery());
        assertEquals(List.of(ParquetQueryParser.SelectExpr.AggFunc.COLUMN,
                ParquetQueryParser.SelectExpr.AggFunc.SUM, ParquetQueryParser.SelectExpr.AggFunc.COUNT,
                ParquetQueryParser.SelectExpr.AggFunc.MAX),
                part.selectExprs.stream().map(e -> e.func).toList());
        assertEquals(AverageMerge.SUM_PREFIX + "1", part.selectExprs.get(1).outputName);
        assertEquals(AverageMerge.COUNT_PREFIX + "1", part.selectExprs.get(2).outputName);
        assertNull(part.orderBy);
        assertTrue(part.filter.contains("3"), part.filter);
        assertEquals(List.of("grp"), part.groupByColumnNames);
        assertEquals("t", part.table);
        assertEquals("s", part.schema);
    }

    /** Verifies queries without an average, or with one the merge cannot rebuild, are refused. */
    @Test
    void refusesWhatTheMergeCannotRebuild() {
        String[] refused = {
            "SELECT count(*) FROM s.t",
            "SELECT avg(DISTINCT qty) FROM s.t",
            "SELECT avg(qty) FILTER (WHERE id > 3) FROM s.t",
            "SELECT grp, avg(qty) FROM s.t GROUP BY grp HAVING avg(qty) > 1",
            "SELECT grp, avg(qty) FROM s.t GROUP BY grp ORDER BY avg(qty)",
            "SELECT grp, avg(qty) FROM s.t GROUP BY grp LIMIT 2",
            "SELECT grp, avg(qty) + 1 FROM s.t GROUP BY grp",
            "SELECT avg(a.qty) FROM s.t a JOIN s.u b ON a.id = b.id",
            "SELECT avg(qty) FROM s.t AT (VERSION => 3)",
            "not sql",
        };
        for (String query : refused) {
            AverageMerge rewrite = AverageMerge.of(query);
            boolean merges = rewrite != null
                    && CompleteAnswerMerge.refusal(ParquetQueryParser.parse(rewrite.partQuery())) == null;
            assertTrue(!merges, query);
        }
    }

    /** Verifies the endpoint answers must fit the rewrite. */
    @Test
    void refusesEndpointAnswersThatDoNotFit() {
        AverageMerge rewrite = AverageMerge.of("SELECT grp, avg(qty) FROM s.t GROUP BY grp");
        assertNotNull(rewrite);
        ParquetQueryParser part = ParquetQueryParser.parse(rewrite.partQuery());
        assertThrows(IllegalArgumentException.class, () -> rewrite.mergeSql(part, "parts",
                List.of("grp", "s"), java.util.Arrays.asList(null, 0), List.of("grp", "avg(qty)")));
    }

    /** Verifies the division: NULL for no values, exact unscaled sums. */
    @Test
    void averageDividesTheUnscaledSum() throws SQLException {
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery("SELECT " + AverageMerge.average("s", "n", 2) + ", "
                     + AverageMerge.average("z", "c", 2) + ", " + AverageMerge.average("i", "n", 0)
                     + ", " + AverageMerge.average("f", "n", null)
                     + " FROM (SELECT -12345678901234.57::DECIMAL(38,2) AS s, 3::BIGINT AS n,"
                     + " NULL::DECIMAL(38,2) AS z, 0::BIGINT AS c, 10::HUGEINT AS i, 1.5::DOUBLE AS f)")) {
            assertTrue(rs.next());
            assertEquals(-1234567890123457.0 / 300.0, rs.getDouble(1));
            assertNull(rs.getObject(2));
            assertEquals(10.0 / 3.0, rs.getDouble(3));
            assertEquals(0.5, rs.getDouble(4));
        }
    }

    /**
     * Reads the scale of each endpoint answer column from its DuckDB type name.
     *
     * @param types DuckDB type names
     * @return per column, decimal scale, 0 for integers, null otherwise
     */
    private static List<Integer> scales(List<String> types) {
        List<Integer> scales = new ArrayList<>();
        for (String type : types) {
            String upper = type.toUpperCase(Locale.ROOT);
            if (upper.startsWith("DECIMAL(")) {
                scales.add(Integer.parseInt(upper.substring(upper.indexOf(',') + 1, upper.indexOf(')')).trim()));
            } else if (upper.endsWith("INT")) {
                scales.add(0);
            } else {
                scales.add(null);
            }
        }
        return scales;
    }

    /**
     * Runs a query the way each endpoint does, over its own rows as {@code s.t}, and collects
     * all endpoint answers in the table {@code parts}.
     *
     * @param query part query over s.t
     * @param types receives the DuckDB type name of each column
     * @return column names of the endpoint answers
     * @throws SQLException when DuckDB fails
     */
    private List<String> endpointAnswers(String query, List<String> types) throws SQLException {
        try (Statement st = connection.createStatement()) {
            st.execute("DROP TABLE IF EXISTS parts");
            for (int i = 0; i < PARTS.length; i++) {
                st.execute("CREATE OR REPLACE TABLE s.t AS SELECT * FROM src WHERE " + PARTS[i]);
                st.execute(i == 0 ? "CREATE TABLE parts AS " + query : "INSERT INTO parts " + query);
            }
        }
        List<String> columns = new ArrayList<>();
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM parts LIMIT 0")) {
            ResultSetMetaData md = rs.getMetaData();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                columns.add(md.getColumnName(i));
                types.add(md.getColumnTypeName(i));
            }
        }
        return columns;
    }

    /**
     * Runs a query and keeps its column names, types and rows.
     *
     * @param sql query
     * @param over table to read as s.t first, or null to run the query as it is
     * @return the answer
     * @throws SQLException when DuckDB fails
     */
    private Answer answer(String sql, String over) throws SQLException {
        try (Statement st = connection.createStatement()) {
            if (over != null) {
                st.execute("CREATE OR REPLACE TABLE s.t AS SELECT * FROM " + over);
            }
            try (ResultSet rs = st.executeQuery(sql)) {
                ResultSetMetaData md = rs.getMetaData();
                List<String> header = new ArrayList<>();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    header.add(md.getColumnName(i) + ":" + md.getColumnTypeName(i));
                }
                List<String> rows = new ArrayList<>();
                while (rs.next()) {
                    StringBuilder row = new StringBuilder();
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        // Doubles as their exact bits, so that the last bit counts.
                        Object value = rs.getObject(i);
                        row.append(value instanceof Double v ? Long.toHexString(Double.doubleToLongBits(v))
                                : String.valueOf(value)).append('|');
                    }
                    rows.add(row.toString());
                }
                return new Answer(header, rows);
            }
        }
    }

    /**
     * Sorts rows for an order-free comparison.
     *
     * @param rows rows
     * @return sorted copy
     */
    private static List<String> sorted(List<String> rows) {
        List<String> copy = new ArrayList<>(rows);
        copy.sort(null);
        return copy;
    }

    /**
     * Column names with types, and rows rendered as text.
     *
     * @param header name:type per column
     * @param rows rows
     */
    private record Answer(List<String> header, List<String> rows) {
    }
}
