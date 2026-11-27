package pro.surpin.data.arrowflight.server.adapters;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Checks exact typed substitutions before the experimental statement cache is enabled. */
@Tag("unit")
class IntegerLiteralParametersTest {
    /** Verifies that only the comparison values differ between cacheable query shapes. */
    @Test
    void reusesTheShapeAndKeepsIntegerTypes() {
        var first = DuckDbAdapter.literalParameters("SELECT 42 FROM s.t WHERE a = 1 AND b < 999999999");
        var next = DuckDbAdapter.literalParameters("SELECT 42 FROM s.t WHERE a = 2 AND b < 0");
        assertNotNull(first);
        assertNotNull(next);
        assertEquals("SELECT 42 FROM s.t WHERE a = CAST(? AS INTEGER) AND b < CAST(? AS INTEGER)", first.sql());
        assertEquals(first.sql(), next.sql());
        assertEquals(List.of(1, 999999999), first.values());
        assertEquals(List.of(2, 0), next.values());
    }

    /** Verifies that existing parameters, large literals and unsupported expressions stay untouched. */
    @Test
    void leavesOtherQueriesAlone() {
        for (String sql : List.of("SELECT ? FROM s.t WHERE a = 1", "SELECT $x FROM s.t WHERE a = 1",
                "SELECT 42", "SELECT * FROM s.t WHERE a = 1000000000",
                "SELECT * FROM s.t WHERE a = 1.5", "SELECT * FROM s.t WHERE a = '1'",
                "SELECT * FROM s.t WHERE a = abs(1)", "SELECT * FROM s.t WHERE a = 1 + 2",
                "SELECT * FROM s.t WHERE a = DATE '2026-01-01'")) {
            assertNull(DuckDbAdapter.literalParameters(sql), sql);
        }
    }

    /** Verifies that ambiguous comment and escaped-string contexts preserve the original SQL path. */
    @Test
    void leavesCommentsAndEscapeStringsAlone() {
        for (String sql : List.of(
                "SELECT * FROM s.t WHERE a = 1 /* outer /* inner */ AND b = 2 AND ignored */",
                "SELECT * FROM s.t WHERE a = 1 /* a comment */",
                "SELECT * FROM s.t WHERE a = 1 -- a comment\n",
                "SELECT * FROM s.t WHERE a = 1 AND E'x\\' AND b = 2 AND ignored' = 'x'")) {
            assertNull(DuckDbAdapter.literalParameters(sql), sql);
        }
    }

    /** Verifies that unary signs and quoted Unicode identifiers stay in their original positions. */
    @Test
    void preservesSignsAndQuotedIdentifiers() {
        var parameters = DuckDbAdapter.literalParameters(
                "SELECT \"число\" FROM s.t WHERE \"число\" >= - 5 AND \"число\" != -7");
        assertNotNull(parameters);
        assertEquals("SELECT \"число\" FROM s.t WHERE \"число\" >= - CAST(? AS INTEGER)"
                + " AND \"число\" != -CAST(? AS INTEGER)", parameters.sql());
        assertEquals(List.of(5, 7), parameters.values());
    }

    /**
     * Compares original literal and typed-parameter answers across numeric and coerced types.
     *
     * @throws Exception when DuckDB cannot be opened
     */
    @Test
    void substitutionsMatchDuckDbAnswersAndFailures() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement setup = connection.createStatement()) {
                setup.execute("CREATE SCHEMA s");
            }
            for (String type : List.of("TINYINT", "UTINYINT", "SMALLINT", "USMALLINT", "INTEGER", "UINTEGER",
                    "BIGINT", "UBIGINT", "HUGEINT", "DECIMAL(10,2)", "FLOAT", "DOUBLE", "VARCHAR", "BOOLEAN",
                    "DATE", "TIMESTAMP", "INTERVAL", "BLOB")) {
                try (Statement setup = connection.createStatement()) {
                    setup.execute("CREATE OR REPLACE TABLE s.t AS SELECT CAST(i AS " + type
                            + ") AS a FROM range(3) t(i)");
                } catch (SQLException e) {
                    // Exercise comparison binding even when this type cannot be cast from integers.
                    try (Statement setup = connection.createStatement()) {
                        setup.execute("CREATE OR REPLACE TABLE s.t(a " + type + ")");
                    }
                }
                for (String operator : List.of("=", "!=", "<", "<=", ">", ">=")) {
                    for (int key : List.of(0, 1, 2, 999999999)) {
                        String sql = "SELECT a, typeof(a) FROM s.t WHERE a " + operator + " " + key + " ORDER BY a";
                        var parameters = DuckDbAdapter.literalParameters(sql);
                        assertNotNull(parameters, sql);
                        assertEquals(answer(connection, sql, List.of()),
                                answer(connection, parameters.sql(), parameters.values()), sql);
                    }
                }
            }
        }
    }

    /**
     * Captures the typed answer or a query refusal, including bind-time errors.
     *
     * @param connection DuckDB connection
     * @param sql SQL to run
     * @param values parameter values
     * @return typed rows or a refusal marker
     */
    private static List<String> answer(Connection connection, String sql, List<Object> values) {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < values.size(); i++) {
                statement.setObject(i + 1, values.get(i));
            }
            List<String> out = new ArrayList<>();
            try (ResultSet result = statement.executeQuery()) {
                out.add(result.getMetaData().getColumnTypeName(1));
                while (result.next()) {
                    out.add(result.getString(1) + "|" + result.getString(2));
                }
            }
            return out;
        } catch (SQLException e) {
            return List.of("QUERY REFUSED");
        }
    }
}
