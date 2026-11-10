package pro.surpin.data.arrowflight.server.services;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the describe cache key that point queries differing in their key share (item 40 (b)). */
@Tag("unit")
class DescribeKeyTest {

    /** Pairs of texts that share one description. */
    private static final String[][] SAME = {
        {"SELECT * FROM s.t WHERE k = 5", "SELECT * FROM s.t WHERE k = 123456789"},
        {"SELECT * FROM s.t WHERE k = -5", "SELECT * FROM s.t WHERE k = -7"},
        {"SELECT * FROM s.t WHERE k = - 5", "SELECT * FROM s.t WHERE k = - 7"},
        {"SELECT * FROM s.t WHERE k=5", "SELECT * FROM s.t WHERE k=6"},
        {"SELECT a, b FROM s.t WHERE k <= 5 AND j >= 3", "SELECT a, b FROM s.t WHERE k <= 9 AND j >= 1"},
        {"SELECT * FROM s.t WHERE k <> 5 OR k != 6", "SELECT * FROM s.t WHERE k <> 1 OR k != 2"},
        {"SELECT * FROM s.t WHERE (k = 5)", "SELECT * FROM s.t WHERE (k = 6)"},
        {"SELECT * FROM s.t WHERE k = 5 ORDER BY 1 LIMIT 3", "SELECT * FROM s.t WHERE k = 6 ORDER BY 1 LIMIT 3"},
        {"select o_orderkey from tpch.orders where o_orderkey = 42",
            "select o_orderkey from tpch.orders where o_orderkey = 4711"},
    };

    /** Pairs of texts that must keep their own descriptions. */
    private static final String[][] DIFFERENT = {
        {"SELECT * FROM s.t WHERE k = 5", "SELECT * FROM s.t WHERE k = 1234567890"},   // BIGINT literal
        {"SELECT * FROM s.t WHERE k = 5", "SELECT * FROM s.t WHERE k = 5.0"},
        {"SELECT * FROM s.t WHERE k = 5", "SELECT * FROM s.t WHERE k = '5'"},
        {"SELECT * FROM s.t WHERE k = 5", "SELECT * FROM s.t WHERE k = ?"},
        {"SELECT * FROM s.t WHERE k = 5", "SELECT * FROM s.t WHERE k = 5 + 1"},
        {"SELECT * FROM s.t WHERE k = 1 + 5", "SELECT * FROM s.t WHERE k = 1 + 6"},
        {"SELECT * FROM s.t WHERE k = 5 - 1", "SELECT * FROM s.t WHERE k = 6 - 1"},
        {"SELECT * FROM s.t WHERE k = 5::BIGINT", "SELECT * FROM s.t WHERE k = 6::BIGINT"},
        {"SELECT * FROM s.t WHERE k >> 5 = 0", "SELECT * FROM s.t WHERE k >> 6 = 0"},
        {"SELECT * FROM s.t WHERE k BETWEEN 1 AND 5", "SELECT * FROM s.t WHERE k BETWEEN 1 AND 6"},
        {"SELECT * FROM s.t WHERE k IN (5)", "SELECT * FROM s.t WHERE k IN (6)"},
        {"SELECT k = 5 FROM s.t WHERE k = 5", "SELECT k = 6 FROM s.t WHERE k = 5"},   // select list
        {"SELECT * FROM s.t WHERE k = 5 LIMIT 3", "SELECT * FROM s.t WHERE k = 5 LIMIT 4"},
        {"SELECT k, count(*) FROM s.t WHERE k = 5 GROUP BY k HAVING count(*) > 1",
            "SELECT k, count(*) FROM s.t WHERE k = 5 GROUP BY k HAVING count(*) > 2"},
        {"SELECT * FROM s.t WHERE k = (SELECT 5)", "SELECT * FROM s.t WHERE k = (SELECT 6)"},
        {"WITH w AS (SELECT * FROM s.t) SELECT * FROM w WHERE k = 5",
            "WITH w AS (SELECT * FROM s.t) SELECT * FROM w WHERE k = 6"},
        {"SELECT * FROM s.t WHERE k = 5 UNION ALL SELECT * FROM s.t",
            "SELECT * FROM s.t WHERE k = 6 UNION ALL SELECT * FROM s.t"},
        {"SELECT * FROM s.t WHERE k = 5 AND true; SELECT 1", "SELECT * FROM s.t WHERE k = 6 AND true; SELECT 1"},
        {"SELECT * FROM s.t WHERE CASE WHEN k = 5 THEN 1 END = 1",
            "SELECT * FROM s.t WHERE CASE WHEN k = 6 THEN 1 END = 1"},
        {"SELECT * FROM s.t WHERE f(x := 5)", "SELECT * FROM s.t WHERE f(x := 6)"},
        {"SELECT * FROM s.t WHERE s = 'k = 5'", "SELECT * FROM s.t WHERE s = 'k = 6'"},
        {"SELECT * FROM s.t WHERE k = 5 -- 5", "SELECT * FROM s.t WHERE k = 5 -- 6"},
    };

    /** Verifies point queries that differ only in their key share the key. */
    @Test
    void pointQueriesShareTheKey() {
        for (String[] pair : SAME) {
            assertEquals(DescribeKey.of(pair[0]), DescribeKey.of(pair[1]), pair[0]);
            assertNotEquals(pair[0], DescribeKey.of(pair[0]), pair[0]);
        }
    }

    /** Verifies texts whose literals can change the answer's columns or the bind keep their own key. */
    @Test
    void otherTextsKeepTheirOwnKey() {
        for (String[] pair : DIFFERENT) {
            assertNotEquals(DescribeKey.of(pair[0]), DescribeKey.of(pair[1]), pair[0] + " / " + pair[1]);
        }
        assertEquals("SELECT 1", DescribeKey.of("SELECT 1"));
        assertEquals("SELECT * FROM s.t WHERE k = x", DescribeKey.of("SELECT * FROM s.t WHERE k = x"));
        String nul = "SELECT * FROM s.t WHERE k = 5\u0000";
        assertNotEquals(DescribeKey.of("SELECT * FROM s.t WHERE k = 5"), DescribeKey.of(nul));
        assertTrue(DescribeKey.of(nul).endsWith(nul));
    }

    /**
     * Verifies the property the key relies on: whether DuckDB binds and runs a comparison of a
     * column with an INTEGER literal does not depend on the literal's value, for every column
     * type, and the answer's columns stay the same.
     */
    @Test
    void comparisonBindDoesNotDependOnTheLiteral() throws SQLException {
        String[] columns = {"ti TINYINT", "uti UTINYINT", "si SMALLINT", "i INTEGER", "b BIGINT",
            "hi HUGEINT", "ub UBIGINT", "de DECIMAL(4,1)", "d DATE", "ts TIMESTAMP", "v VARCHAR",
            "bo BOOLEAN", "f FLOAT", "bl BLOB", "u UUID", "iv INTERVAL", "e ENUM('a','b')",
            "l INTEGER[]", "bit BIT", "tm TIME"};
        String[] literals = {"0", "1", "127", "128", "255", "256", "32768", "65536", "999999999"};
        String[] operators = {"=", "<", ">", "<=", ">=", "<>", "!=", "= -"};
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t (" + String.join(", ", columns) + ")");
            for (String column : columns) {
                String name = column.substring(0, column.indexOf(' '));
                for (String op : operators) {
                    String first = null;
                    for (String literal : literals) {
                        String sql = "SELECT * FROM t WHERE " + name + " " + op + literal + " AND true";
                        String outcome;
                        try (ResultSet rs = s.executeQuery(sql)) {
                            outcome = "columns=" + rs.getMetaData().getColumnCount();
                        } catch (SQLException e) {
                            outcome = "refused";
                        }
                        if (first == null) {
                            first = outcome;
                        }
                        assertEquals(first, outcome, sql);
                        assertEquals(DescribeKey.of("SELECT * FROM t WHERE " + name + " " + op + "0 AND true"),
                                DescribeKey.of(sql), sql);
                    }
                }
            }
        }
    }
}
