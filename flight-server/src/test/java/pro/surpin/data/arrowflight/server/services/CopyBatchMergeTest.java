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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks that the answers one node computes for batches of a ticket's files, merged by
 * {@link CopyBatchMerge}, equal the answer DuckDB computes over all rows at once (item 30),
 * also for HAVING and DISTINCT aggregates, and that other shapes are refused.
 */
@Tag("unit")
class CopyBatchMergeTest {

    /** Rows of one batch each, and one batch without rows. */
    private static final String[] BATCHES = {"id % 3 = 0", "id % 3 = 1", "id % 3 = 2", "id < 0"};

    private Connection connection;

    /**
     * Creates the source rows: 300 ids over five groups, with null quantities and names.
     *
     * @throws SQLException when DuckDB fails
     */
    @BeforeEach
    void createRows() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = connection.createStatement()) {
            st.execute("CREATE TABLE src AS SELECT i::INTEGER AS id, 'g' || (i % 5) AS grp,"
                    + " CASE WHEN i % 7 = 0 THEN NULL ELSE i * 3 END::BIGINT AS qty,"
                    + " (i * 1.25)::DECIMAL(12,2) AS price, (i % 13)::INTEGER AS k,"
                    + " CASE WHEN i % 11 = 0 THEN NULL ELSE 'n' || (i % 17) END AS name"
                    + " FROM range(300) r(i)");
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

    /** Verifies HAVING and DISTINCT aggregates merge into exactly the answer, names and types. */
    @Test
    void mergedBatchAnswersAreTheAnswer() throws SQLException {
        String[] queries = {
            // Group sums 8700..9450 without nulls: per batch every group would fail the bound.
            "SELECT grp, sum(qty) AS sq FROM s.t GROUP BY grp HAVING sum(qty) > 25000",
            "SELECT grp, count(*) AS c, max(name) FROM s.t GROUP BY grp HAVING count(qty) >= 52"
                    + " AND min(price) < 3 ORDER BY grp",
            "SELECT grp AS g, sum(price) FROM s.t WHERE id > 10 GROUP BY grp"
                    + " HAVING NOT (grp = 'g1' OR sum(price) * 2 <= 22000) ORDER BY g DESC LIMIT 2",
            "SELECT k, count(*) AS c FROM s.t GROUP BY k HAVING max(qty) - min(qty) > 800 ORDER BY c, k",
            "SELECT count(DISTINCT name) AS n FROM s.t",
            "SELECT count(DISTINCT k), sum(DISTINCT k) AS sk, count(*) AS c, sum(qty), min(name)"
                    + " FROM s.t WHERE id > 7",
            "SELECT grp, count(DISTINCT name) AS dn, count(*) AS c, max(qty) FROM s.t GROUP BY grp"
                    + " ORDER BY dn DESC, grp",
            "SELECT grp, sum(DISTINCT price) AS sp FROM s.t GROUP BY grp HAVING count(*) > 59"
                    + " ORDER BY sp LIMIT 3",
            // Item 33 (B): GROUP BY ordinals group like the columns they name.
            "SELECT grp AS g, k, count(*) AS c, sum(qty) FROM s.t GROUP BY 1, 2 HAVING count(*) > 0"
                    + " ORDER BY g, k",
            "SELECT k, count(DISTINCT grp) AS dg FROM s.t GROUP BY k HAVING sum(qty) IS NULL OR"
                    + " sum(qty) > 3000 ORDER BY k",
        };
        for (String query : queries) {
            ParquetQueryParser parsed = ParquetQueryParser.parse(query);
            CopyBatchMerge merge = CopyBatchMerge.of(query, parsed);
            if (query.contains("IS NULL")) {
                // IS NULL is not among the accepted condition forms: refused, today's path.
                assertNull(merge, query);
                continue;
            }
            assertNotNull(merge, query);
            assertTrue(merge.rewritten(), query);
            List<String> columns = batchAnswers(merge.partSql());
            List<String> outputNames = answer(query, "src").names();
            String sql = merge.mergeSql("parts", columns, outputNames);
            Answer expected = answer(query, "src");
            Answer merged = answer(sql, null);
            assertEquals(expected.header, merged.header, query + " -> " + sql);
            if (parsed.orderBy != null) {
                assertEquals(expected.rows, merged.rows, query + " -> " + sql);
            } else {
                assertEquals(sorted(expected.rows), sorted(merged.rows), query + " -> " + sql);
            }
        }
    }

    /**
     * Item 33 regression: the stand's unbatched shapes - averages (egret {@code sp-group}) and
     * several count(DISTINCT) columns (egret {@code sp-distinct}) - merge into exactly the answer
     * computed over all rows at once, names and types included.
     */
    @Test
    void averagesAndSeveralDistinctColumnsMergeIntoTheAnswer() throws SQLException {
        String[] queries = {
            "SELECT grp, k, count(*) AS n, sum(qty) AS q, avg(price) AS d FROM s.t GROUP BY grp, k",
            "SELECT count(DISTINCT grp) AS n, count(DISTINCT k) AS s FROM s.t",
            "SELECT avg(qty), avg(k) AS ak, avg(price) FROM s.t WHERE id > 3",
            "SELECT grp, avg(qty) AS aq, min(name) FROM s.t GROUP BY grp ORDER BY aq DESC",
            "SELECT grp, count(DISTINCT name) AS dn, count(DISTINCT k), sum(DISTINCT k) AS sk,"
                    + " count(*) AS c FROM s.t GROUP BY grp ORDER BY grp",
            "SELECT avg(qty) AS none FROM s.t WHERE id < 0",
        };
        for (String query : queries) {
            ParquetQueryParser parsed = ParquetQueryParser.parse(query);
            CopyBatchMerge merge = CopyBatchMerge.of(query, parsed);
            assertNotNull(merge, query);
            assertTrue(merge.rewritten(), query);
            assertEquals(query.contains("avg("), merge.averages(), query);
            List<String> columns = batchAnswers(merge.partSql());
            assertEquals(parsed.selectExprs.size() + merge.extraColumns(), columns.size(), query);
            Answer expected = answer(query, "src");
            String sql = merge.mergeSql("parts", columns, expected.names(), partScales());
            Answer merged = answer(sql, null);
            assertEquals(expected.header, merged.header, query + " -> " + sql);
            if (parsed.orderBy != null) {
                assertEquals(expected.rows, merged.rows, query + " -> " + sql);
            } else {
                assertEquals(sorted(expected.rows), sorted(merged.rows), query + " -> " + sql);
            }
        }
    }

    /**
     * Reads the decimal scale of each column of the table {@code parts} as
     * {@link DistributedComplete#sumScales} does from an Arrow schema.
     *
     * @return per column, the scale of a DECIMAL, 0 for an integer, null otherwise
     * @throws SQLException when DuckDB fails
     */
    private List<Integer> partScales() throws SQLException {
        List<Integer> scales = new ArrayList<>();
        try (Statement st = connection.createStatement();
                ResultSet rs = st.executeQuery("SELECT * FROM parts LIMIT 0")) {
            ResultSetMetaData md = rs.getMetaData();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                String type = md.getColumnTypeName(i);
                scales.add(type.startsWith("DECIMAL") ? Integer.valueOf(md.getScale(i))
                        : type.matches("(U?)(TINYINT|SMALLINT|INTEGER|BIGINT|HUGEINT)") ? Integer.valueOf(0) : null);
            }
        }
        return scales;
    }

    /** Item 33 (B): a GROUP BY ordinal naming a column is that column; others stay refused. */
    @Test
    void groupByOrdinalsNameTheirColumn() {
        ParquetQueryParser parsed = ParquetQueryParser.parse(
                "SELECT grp, sum(qty) FROM s.t GROUP BY 1");
        assertEquals(List.of("grp"), parsed.groupByColumnNames);
        assertNull(parsed.completeAnswerReason);
        assertNotNull(CopyBatchMerge.of("SELECT grp, sum(qty) FROM s.t GROUP BY 1", parsed));
        assertEquals(List.of("grp"), ParquetQueryParser.parse(
                "SELECT grp AS g, count(*) FROM s.t GROUP BY 1").groupByColumnNames);
        for (String query : List.of("SELECT grp, sum(qty) FROM s.t GROUP BY 2",
                "SELECT grp, sum(qty) FROM s.t GROUP BY 3")) {
            assertNotNull(ParquetQueryParser.parse(query).completeAnswerReason, query);
        }
    }

    /** Verifies the shapes {@link CompleteAnswerMerge} merges keep its part query and merge. */
    @Test
    void plainShapesKeepTheCompleteAnswerMerge() throws SQLException {
        String query = "SELECT grp, sum(qty) AS sq FROM s.t GROUP BY grp ORDER BY sq DESC LIMIT 2";
        CopyBatchMerge merge = CopyBatchMerge.of(query, ParquetQueryParser.parse(query));
        assertNotNull(merge);
        assertFalse(merge.rewritten());
        assertEquals(query, merge.partSql());
        assertNull(merge.partQuery().limit);
        assertEquals(0, merge.extraColumns());
        List<String> columns = batchAnswers("SELECT grp, sum(qty) AS sq FROM s.t GROUP BY grp");
        assertEquals(answer(query, "src"), answer(merge.mergeSql("parts", columns, columns), null));
    }

    /** Verifies shapes whose batch answers cannot be merged are refused. */
    @Test
    void unmergeableShapesAreRefused() {
        for (String query : List.of("SELECT grp FROM s.t", "SELECT grp, avg(qty) FROM s.t GROUP BY grp LIMIT 2",
                "SELECT grp, avg(DISTINCT qty) FROM s.t GROUP BY grp", "SELECT avg(qty) + 1 FROM s.t",
                "SELECT grp, avg(qty) FROM s.t GROUP BY grp HAVING avg(qty) > 1",
                "SELECT count(DISTINCT k + 1) FROM s.t",
                "SELECT grp, sum(qty) FROM s.t GROUP BY grp HAVING avg(qty) > 1",
                "SELECT grp, sum(qty) FROM s.t GROUP BY grp HAVING count(DISTINCT k) > 1",
                "SELECT grp, sum(qty) FROM s.t GROUP BY grp HAVING sum(qty) FILTER (WHERE k > 1) > 1",
                "SELECT sum(qty) FROM s.t GROUP BY grp HAVING sum(qty) > 1",
                "SELECT grp, count(*) FROM s.t GROUP BY grp HAVING count(*) > 1 ORDER BY count(*)",
                "SELECT grp, count(*) FROM s.t GROUP BY grp HAVING count(*) > 1 LIMIT 2 OFFSET 1",
                "SELECT x.grp, count(*) FROM s.t x JOIN s.u y ON x.id = y.id GROUP BY x.grp")) {
            assertNull(CopyBatchMerge.of(query, ParquetQueryParser.parse(query)), query);
        }
    }

    /** Verifies part answers with the wrong number of columns are refused by the merge. */
    @Test
    void wrongPartColumnsAreRefused() {
        String query = "SELECT grp, sum(qty) FROM s.t GROUP BY grp HAVING max(k) > 3";
        CopyBatchMerge merge = CopyBatchMerge.of(query, ParquetQueryParser.parse(query));
        assertNotNull(merge);
        assertEquals(1, merge.extraColumns());
        assertThrows(IllegalArgumentException.class,
                () -> merge.mergeSql("parts", List.of("grp", "s"), List.of("grp", "s")));
    }

    /**
     * Runs a part query over every batch's rows as {@code s.t} and collects all part answers in
     * the table {@code parts}.
     *
     * @param partSql the query every batch answers
     * @return column names of the part answers
     * @throws SQLException when DuckDB fails
     */
    private List<String> batchAnswers(String partSql) throws SQLException {
        try (Statement st = connection.createStatement()) {
            st.execute("DROP TABLE IF EXISTS parts");
            for (int i = 0; i < BATCHES.length; i++) {
                st.execute("CREATE OR REPLACE TABLE s.t AS SELECT * FROM src WHERE " + BATCHES[i]);
                st.execute(i == 0 ? "CREATE TABLE parts AS " + partSql : "INSERT INTO parts " + partSql);
            }
        }
        return answer("SELECT * FROM parts LIMIT 0", null).names();
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
                        row.append(rs.getString(i)).append('|');
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

        /**
         * Returns the column names.
         *
         * @return names in order
         */
        List<String> names() {
            return header.stream().map(h -> h.substring(0, h.lastIndexOf(':'))).toList();
        }
    }
}
