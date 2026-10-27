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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks that merging the answers each endpoint gives for its own rows reproduces the complete
 * answer DuckDB computes over all rows, and that shapes without such a merge are refused.
 */
@Tag("unit")
class CompleteAnswerMergeTest {

    /** Rows of one endpoint each: by id modulo 3, and one endpoint without rows. */
    private static final String[] PARTS = {"id % 3 = 0", "id % 3 = 1", "id % 3 = 2", "id < 0"};

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
                    + " (i * 1.25)::DECIMAL(12,2) AS price,"
                    + " CASE WHEN i % 11 = 0 THEN NULL ELSE 'n' || i END AS name"
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

    /** Verifies every accepted shape merges into exactly the complete answer, names and types. */
    @Test
    void mergedEndpointAnswersAreTheCompleteAnswer() throws SQLException {
        String[] queries = {
            "SELECT * FROM s.t WHERE qty > 100",
            "SELECT id, name FROM s.t WHERE grp = 'g1'",
            "SELECT count(*) FROM s.t",
            "SELECT count(*) AS c, count(qty) AS cq, sum(qty) AS sq, sum(price) AS sp,"
                    + " min(name) AS lo, max(price) AS hi FROM s.t WHERE id > 20",
            "SELECT count(*) AS c, sum(qty) AS sq, min(qty) AS lo FROM s.t WHERE id > 1000",
            "SELECT grp, count(*) AS c, sum(qty), max(name) FROM s.t GROUP BY grp",
            "SELECT t.grp, sum(t.price) AS sp FROM s.t AS t WHERE t.id < 150 GROUP BY t.grp",
            "SELECT grp FROM s.t GROUP BY grp",
            "SELECT DISTINCT grp FROM s.t",
            "SELECT DISTINCT grp, count(*) AS c FROM s.t GROUP BY grp",
            "SELECT id, qty FROM s.t ORDER BY id DESC LIMIT 7",
            "SELECT id AS k, qty FROM s.t ORDER BY k LIMIT 5",
            "SELECT id, price FROM s.t ORDER BY 2 DESC, 1 LIMIT 4",
            "SELECT DISTINCT grp FROM s.t ORDER BY grp DESC LIMIT 3",
            "SELECT grp, sum(qty) AS sq FROM s.t GROUP BY grp ORDER BY sq DESC",
            "SELECT id, name FROM s.t ORDER BY name NULLS FIRST, id LIMIT 9",
        };
        for (String query : queries) {
            ParquetQueryParser parsed = ParquetQueryParser.parse(query);
            assertNull(CompleteAnswerMerge.refusal(parsed), query);
            List<String> columns = endpointAnswers(query);
            String merge = CompleteAnswerMerge.mergeSql(parsed, "parts", columns);
            Answer expected = answer(query, "src");
            Answer merged = answer(merge, null);
            assertEquals(expected.header, merged.header, query + " -> " + merge);
            if (parsed.orderBy != null) {
                assertEquals(expected.rows, merged.rows, query + " -> " + merge);
            } else {
                assertEquals(sorted(expected.rows), sorted(merged.rows), query + " -> " + merge);
            }
        }
    }

    /** Verifies a LIMIT without ORDER BY keeps the row count and only rows of the answer. */
    @Test
    void limitWithoutOrderKeepsRowsOfTheAnswer() throws SQLException {
        String query = "SELECT id, grp FROM s.t WHERE qty > 30 LIMIT 6";
        List<String> columns = endpointAnswers(query);
        Answer merged = answer(CompleteAnswerMerge.mergeSql(
                ParquetQueryParser.parse(query), "parts", columns), null);
        Answer all = answer("SELECT id, grp FROM src WHERE qty > 30", null);
        assertEquals(6, merged.rows.size());
        assertTrue(all.rows.containsAll(merged.rows), merged.rows.toString());
    }

    /** Verifies shapes whose endpoint answers cannot give the complete answer are refused. */
    @Test
    void refusesShapesWithoutAMerge() {
        String[] refused = {
            "SELECT grp, count(*) FROM s.t GROUP BY grp LIMIT 2",
            "SELECT count(*) FROM s.t GROUP BY grp",
            "SELECT count(DISTINCT grp) FROM s.t",
            "SELECT grp, count(*) FROM s.t GROUP BY grp HAVING count(*) > 3",
            "SELECT id FROM s.t ORDER BY id LIMIT 3 OFFSET 2",
            "SELECT avg(qty) FROM s.t",
            "SELECT grp, name IS NULL FROM s.t GROUP BY grp, name IS NULL",
            "SELECT id FROM s.t ORDER BY id + 1",
            "SELECT a.id FROM s.t a JOIN s.u b ON a.id = b.id",
        };
        for (String query : refused) {
            ParquetQueryParser parsed = ParquetQueryParser.parse(query);
            assertNotNull(CompleteAnswerMerge.refusal(parsed), query);
            assertThrows(IllegalArgumentException.class,
                    () -> CompleteAnswerMerge.mergeSql(parsed, "parts", List.of("x")), query);
        }
        assertNull(CompleteAnswerMerge.refusal(ParquetQueryParser.parse("SELECT id FROM s.t ORDER BY id")));
    }

    /** Verifies an ORDER BY that names an input rather than an output column is refused. */
    @Test
    void refusesAnOrderByThatIsNotAnOutputColumn() {
        ParquetQueryParser notSelected = ParquetQueryParser.parse("SELECT id FROM s.t ORDER BY qty LIMIT 3");
        ParquetQueryParser swapped = ParquetQueryParser.parse(
                "SELECT id AS qty, qty AS id FROM s.t ORDER BY id LIMIT 3");
        assertThrows(IllegalArgumentException.class,
                () -> CompleteAnswerMerge.mergeSql(notSelected, "parts", List.of("id")));
        assertThrows(IllegalArgumentException.class,
                () -> CompleteAnswerMerge.mergeSql(swapped, "parts", List.of("qty", "id")));
        assertThrows(IllegalArgumentException.class, () -> CompleteAnswerMerge.mergeSql(
                ParquetQueryParser.parse("SELECT grp, count(*) FROM s.t GROUP BY grp"),
                "parts", List.of("c", "c")));
    }

    /**
     * Runs a query the way each endpoint does, over its own rows as {@code s.t}, and collects
     * all endpoint answers in the table {@code parts}.
     *
     * @param query client query over s.t
     * @return column names of the endpoint answers
     * @throws SQLException when DuckDB fails
     */
    private List<String> endpointAnswers(String query) throws SQLException {
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
    }
}
