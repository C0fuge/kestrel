package pro.surpin.data.arrowflight.server;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks the deterministic answers and schedule of the optional mixed-load workload. */
@Tag("unit")
class SmallQueryMixTest {
    /** Verifies that disabling heavy queries keeps the original point/count workload. */
    @Test
    void disabledMixKeepsTheOriginalQueries() {
        var point = SmallQueryThroughputPerfTest.queryFor(0, 7, 42, 0);
        var count = SmallQueryThroughputPerfTest.queryFor(1, 7, 42, 0);
        assertEquals(0, point.kind());
        assertEquals("SELECT * FROM s.orders WHERE o_orderkey = 42", point.sql());
        assertEquals(42L, point.expectedFirst());
        assertEquals(1, count.kind());
        assertEquals("SELECT count(*) FROM s.orders WHERE o_orderkey < 42", count.sql());
        assertEquals(42L, count.expectedFirst());
    }

    /** Verifies one heavy query per interval and different first-heavy positions across clients. */
    @Test
    void heavyQueriesArePhasedByClient() {
        for (int client = 0; client < 4; client++) {
            int heavy = 0;
            for (int sequence = 0; sequence < 12; sequence++) {
                boolean expected = (sequence + client) % 4 == 3;
                boolean actual = SmallQueryThroughputPerfTest.queryFor(sequence, client, 7, 4).kind() == 2;
                assertEquals(expected, actual);
                if (actual) {
                    heavy++;
                }
            }
            assertEquals(3, heavy);
        }
        assertEquals(2, SmallQueryThroughputPerfTest.queryFor(0, 0, 7, 1).kind());
    }

    /**
     * Verifies the join's exact integer result and its upper bound against native DuckDB.
     *
     * @throws Exception when DuckDB cannot answer the query
     */
    @Test
    void heavyJoinCountsMatchTheSyntheticDataset() throws Exception {
        try (var connection = DriverManager.getConnection("jdbc:duckdb:");
                var statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA s");
            statement.execute("CREATE TABLE s.orders AS SELECT i AS o_orderkey, i % 1000 AS o_custkey"
                    + " FROM range(400000) t(i)");
            for (long key : new long[] {0, 1, 7, 5001}) {
                var query = SmallQueryThroughputPerfTest.queryFor(3, 0, key, 4);
                assertEquals(2, query.kind());
                assertEquals(Math.min(key, 5000) * 400, query.expectedFirst());
                try (var result = statement.executeQuery(query.sql())) {
                    assertTrue(result.next());
                    assertEquals(query.expectedFirst(), result.getLong(1));
                    assertFalse(result.next());
                }
            }
        }
    }
}
