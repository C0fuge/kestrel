package pro.surpin.data.arrowflight.server;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Regression test: a table read only inside a subquery ({@code WHERE id IN (SELECT ...)}, a scalar
 * subquery in the select list) or named quoted in a join was not checked, and its rows were
 * returned to a caller without access. Every table of a nested query is now named from DuckDB's
 * parse and checked; sources that cannot be named are refused.
 */
@Tag("unit")
class FlightAuthorizationSubqueryTest {

    /** Reads only table s.a. */
    private static final FlightAuthorization ONLY_A = new FlightAuthorization() {
        @Override
        public boolean canSeeSchema(String principal, String schema) {
            return true;
        }

        @Override
        public boolean canReadTable(String principal, String schema, String table) {
            return "s".equals(schema) && "a".equals(table);
        }
    };

    @Test
    void tablesInSubqueriesAreChecked() {
        for (String sql : List.of(
                "SELECT * FROM s.a WHERE id IN (SELECT id FROM s.b)",
                "SELECT * FROM s.a WHERE EXISTS (SELECT 1 FROM s.b WHERE s.b.id = 5)",
                "SELECT (SELECT max(v) FROM s.b) AS m FROM s.a",
                "SELECT * FROM s.a WHERE id IN (FROM s.b)",
                "SELECT * FROM s.a WHERE id IN (TABLE s.b)",
                "SELECT * FROM s.a WHERE id IN ( select id FROM \"s\".\"b\")",
                "SELECT * FROM s.a WHERE id IN (/* x */ SELECT id FROM s.b)",
                "SELECT * FROM s.a WHERE id IN (SELECT id FROM s.b FOR VERSION AS OF 3)",
                "SELECT x.id FROM s.a x JOIN s.a y ON x.id = y.id WHERE x.id IN (SELECT id FROM s.b)",
                "SELECT * FROM s.a, LATERAL (SELECT * FROM s.b WHERE s.b.id = s.a.id) l",
                "SELECT * FROM s.a WHERE id IN (SELECT id FROM s.a WHERE v IN (SELECT v FROM s.b))",
                "SELECT id FROM s.a UNION ALL SELECT id FROM s.b",
                "SELECT id FROM s.a EXCEPT SELECT id FROM s.b",
                "WITH x AS (SELECT * FROM s.b) SELECT count(*) FROM x",
                "WITH x AS (SELECT * FROM s.a), y AS (SELECT * FROM x JOIN s.b USING (id)) SELECT * FROM y",
                // A CTE named like a table hides it only where the CTE is in scope.
                "WITH b AS (SELECT * FROM s.b) SELECT * FROM s.a WHERE id IN (SELECT id FROM b)",
                "SELECT * FROM s.a WHERE id IN (WITH x AS (SELECT 1 AS id) SELECT id FROM x)"
                        + " AND id IN (SELECT id FROM x)",
                // A name without a schema that is no CTE is not checked, so refused.
                "SELECT * FROM s.a WHERE id IN (SELECT id FROM b)",
                "SELECT * FROM s.a WHERE id IN (SELECT id FROM 'data/b.parquet')")) {
            FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                    () -> ONLY_A.requireQuery("u", sql), sql);
            assertEquals(FlightStatusCode.UNAUTHORIZED, e.status().code(), sql);
        }
    }

    @Test
    void sourcesThatCannotBeNamedAreRefused() {
        for (String sql : List.of(
                "SELECT * FROM s.a WHERE id IN (SELECT id FROM read_parquet('data/b.parquet'))",
                "SELECT * FROM s.a WHERE id IN (SELECT id FROM memory.s.b)",
                "SELECT * FROM s.a WHERE id IN (SELECT column_name FROM (DESCRIBE s.b))",
                "SELECT * FROM s.a WHERE id IN (SELECT * FROM range(3))",
                "SELECT * FROM s.a WHERE id IN (SELECT id FROM s.a WHERE"
                        + " v IN (SELECT v FROM read_csv('/etc/passwd')))")) {
            FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                    () -> ONLY_A.requireQuery("u", sql), sql);
            assertEquals(FlightStatusCode.INVALID_ARGUMENT, e.status().code(), sql);
        }
    }

    @Test
    void subqueriesOverReadableTablesPass() {
        for (String sql : List.of(
                "SELECT * FROM s.a WHERE id IN (SELECT id FROM s.a)",
                "SELECT * FROM s.a WHERE EXISTS (SELECT 1 FROM \"s\".\"a\" y WHERE y.id = 5)",
                "SELECT (SELECT max(v) FROM s.a) AS m FROM s.a",
                "SELECT id FROM s.a UNION ALL SELECT id FROM s.a",
                "WITH x AS (SELECT * FROM s.a) SELECT count(*) FROM x",
                "WITH X AS (SELECT * FROM s.a), y AS (SELECT * FROM x) SELECT * FROM y",
                "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3)"
                        + " SELECT * FROM s.a WHERE id IN (SELECT n FROM r)",
                "SELECT * FROM s.a WHERE id IN (SELECT id FROM s.a FOR VERSION AS OF 3)")) {
            assertDoesNotThrow(() -> ONLY_A.requireQuery("u", sql), sql);
        }
    }

    @Test
    void aNestedQueryWithoutTablesIsRefused() {
        FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                () -> ONLY_A.requireQuery("u", "SELECT (SELECT 1) AS x"));
        assertEquals(FlightStatusCode.UNAUTHORIZED, e.status().code());
    }

    @Test
    void quotedJoinTablesAreChecked() {
        for (String sql : List.of(
                "SELECT * FROM s.a x JOIN \"s\".\"b\" y ON x.id = y.id",
                "SELECT * FROM s.a x JOIN/* x */s.b y ON x.id = y.id",
                "SELECT * FROM \"s\".\"b\"")) {
            assertThrows(FlightRuntimeException.class, () -> ONLY_A.requireQuery("u", sql), sql);
        }
    }

    @Test
    void plainQueriesStillPass() {
        for (String sql : List.of(
                "SELECT * FROM s.a",
                "SELECT id, 'select (select 1)' AS t FROM s.a WHERE note = '(SELECT x FROM s.b)'",
                "SELECT \"select\" FROM s.a -- (SELECT 1 FROM s.b)",
                "SELECT extract(year FROM ts), substring(v FROM 2) FROM s.a WHERE id IN (1, 2)",
                "SELECT x.id FROM s.a x JOIN \"s\".\"a\" y ON x.id = y.id",
                "SELECT * FROM (SELECT id, v FROM s.a WHERE id > 1) q WHERE v > 2",
                "SELECT count(*) FROM s.a GROUP BY v ORDER BY 1 LIMIT 5")) {
            assertDoesNotThrow(() -> ONLY_A.requireQuery("u", sql), sql);
        }
        assertThrows(FlightRuntimeException.class,
                () -> ONLY_A.requireQuery("u", "SELECT * FROM (SELECT id FROM s.b WHERE id > 1) q"));
    }
}
