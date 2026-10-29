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
 * returned to a caller without access. Such subqueries are now refused and every named table is
 * checked.
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
    void subqueriesAreRefused() {
        for (String sql : List.of(
                "SELECT * FROM s.a WHERE id IN (SELECT id FROM s.b)",
                "SELECT * FROM s.a WHERE EXISTS (SELECT 1 FROM s.b WHERE s.b.id = 5)",
                "SELECT (SELECT max(v) FROM s.b) AS m FROM s.a",
                "SELECT * FROM s.a WHERE id IN (FROM s.b)",
                "SELECT * FROM s.a WHERE id IN (TABLE s.b)",
                "SELECT * FROM s.a WHERE id IN ( select id FROM \"s\".\"b\")",
                "SELECT * FROM s.a WHERE id IN (/* x */ SELECT id FROM s.b)",
                "SELECT x.id FROM s.a x JOIN s.a y ON x.id = y.id WHERE x.id IN (SELECT id FROM s.b)",
                // A subquery over a readable table is refused too: its tables cannot be named.
                "SELECT * FROM s.a WHERE id IN (SELECT id FROM s.a)")) {
            FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                    () -> ONLY_A.requireQuery("u", sql), sql);
            assertEquals(FlightStatusCode.INVALID_ARGUMENT, e.status().code(), sql);
        }
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
