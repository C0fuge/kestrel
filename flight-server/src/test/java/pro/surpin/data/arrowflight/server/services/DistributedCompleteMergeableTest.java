package pro.surpin.data.arrowflight.server.services;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests which complete-mode queries distributed complete mode merges from endpoint answers.
 */
@Tag("unit")
class DistributedCompleteMergeableTest {

    /** Item 33 (D): a subquery of another table keeps the single-node path (TPC-H q04 shape). */
    @Test
    void subqueriesOfOtherTablesKeepTheSingleNodePath() {
        String q04 = "SELECT o_orderpriority, count(*) AS order_count FROM tpch.orders"
                + " WHERE o_orderdate >= DATE '1993-07-01' AND o_orderdate < DATE '1993-10-01'"
                + " AND EXISTS (SELECT * FROM tpch.lineitem WHERE l_orderkey = o_orderkey"
                + " AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority";
        assertNull(DistributedComplete.mergeable(q04));
        assertNotNull(DistributedComplete.otherTables(q04));
        for (String query : List.of(
                "SELECT count(*) FROM tpch.orders WHERE o_custkey IN (SELECT c_custkey FROM tpch.customer)",
                "SELECT sum(o_totalprice) FROM tpch.orders o WHERE NOT EXISTS"
                        + " (SELECT 1 FROM tpch.lineitem l WHERE l.l_orderkey = o.o_orderkey)")) {
            assertNull(DistributedComplete.mergeable(query), query);
        }
    }

    /** One-table aggregates are still merged. */
    @Test
    void oneTableAggregatesAreMerged() {
        for (String query : List.of("SELECT count(*) AS n, sum(l_quantity) AS q FROM tpch.lineitem",
                "SELECT l_returnflag, count(*) AS n FROM tpch.lineitem GROUP BY l_returnflag")) {
            assertNull(DistributedComplete.otherTables(query), query);
            assertNotNull(DistributedComplete.mergeable(query), query);
        }
    }

    /** Item 55 (c): a GROUP BY ordered by its aggregate is merged; refused aggregates say why. */
    @Test
    void groupByOrderedByItsAggregateIsMergedAndRefusalsAreNamed() {
        String ordered = "SELECT event_type, count(*) FROM ev.events GROUP BY event_type ORDER BY count(*) DESC";
        assertNotNull(DistributedComplete.mergeable(ordered));
        assertNull(DistributedComplete.unmergedAggregateReason(ordered));
        assertEquals("HAVING", DistributedComplete.unmergedAggregateReason(
                "SELECT event_type, count(*) FROM ev.events GROUP BY event_type HAVING count(*) > 1"));
        assertTrue(DistributedComplete.unmergedAggregateReason(
                "SELECT id % 6 AS g, count(*) FROM ev.events GROUP BY id % 5").contains("GROUP BY expression"));
        assertNull(DistributedComplete.unmergedAggregateReason("SELECT * FROM ev.events"));
        assertNull(DistributedComplete.unmergedAggregateReason("not sql"));
    }

    /**
     * Item 55 (c) step 2: a GROUP BY on a selected, deterministic expression is merged (every
     * endpoint computes the key, the merge groups by its output column); partial mode still
     * refuses it, and other expressions keep the single-node path with their reason.
     */
    @Test
    void groupByOnASelectedExpressionIsMerged() {
        for (String query : List.of(
                "SELECT id % 6 AS g, count(*) FROM ev.events GROUP BY g",
                "SELECT id % 6 AS g, count(*) FROM ev.events GROUP BY 1 ORDER BY 1",
                "SELECT id % 6, count(*) FROM ev.events GROUP BY id % 6",
                "SELECT event_type, floor(id / 100) AS b, sum(v) FROM ev.events GROUP BY event_type, b",
                "SELECT upper(substring(name, 1, 2)) AS p, min(v) FROM ev.events GROUP BY 1",
                "SELECT extract(year FROM ts) AS y, count(*) AS n FROM ev.events GROUP BY y ORDER BY n DESC",
                "SELECT cast(coalesce(v, -1) AS INTEGER) % 4 AS q, max(id) FROM ev.events GROUP BY q",
                // Step 4: date_trunc keys are rendered with their part (jOOQ's own rendering drops it).
                "SELECT date_trunc('day', ts) AS d, count(*) FROM ev.events GROUP BY 1",
                "SELECT date_trunc('month', ts) AS m, sum(v) FROM ev.events GROUP BY m ORDER BY m",
                "SELECT date_trunc('hour', cast(ts AS TIMESTAMP)) AS h, count(*) FROM ev.events"
                        + " GROUP BY date_trunc('hour', cast(ts AS TIMESTAMP))",
                // Step 6: strftime / time_bucket keys, rendered from the parse.
                "SELECT strftime(ts, '%Y-%m') AS ym, count(*) FROM ev.events GROUP BY ym ORDER BY ym",
                "SELECT time_bucket(INTERVAL '1 hour', ts) AS b, sum(v) FROM ev.events GROUP BY 1",
                "SELECT time_bucket('1 day', ts) AS b, count(*) FROM ev.events GROUP BY time_bucket('1 day', ts)")) {
            ParquetQueryParser parsed = DistributedComplete.mergeable(query);
            assertNotNull(parsed, query);
            assertNull(DistributedComplete.unmergedAggregateReason(query), query);
            // Partial mode is unchanged: its clients cannot merge computed keys.
            assertNotNull(parsed.completeAnswerReason, query);
            ParquetQueryParser.SelectExpr key = parsed.selectExprs.stream().filter(e -> e.computed)
                    .findFirst().orElseThrow();
            assertNull(parsed.sourceColumnOf(key.outputName), query);
        }
        assertEquals(List.of("g"), ParquetQueryParser.parse(
                "SELECT id % 6 AS g, count(*) FROM ev.events GROUP BY g").aliasGroupKeys);
        assertEquals(List.of(), ParquetQueryParser.parse(
                "SELECT id % 6 AS g, count(*) FROM ev.events GROUP BY 1").aliasGroupKeys);
        for (String query : List.of(
                // date_trunc: another part in GROUP BY, no alias, an argument that is no column,
                // a part outside the list, ORDER BY the function itself, nested in another key.
                "SELECT date_trunc('day', ts) AS d, count(*) FROM ev.events GROUP BY date_trunc('month', ts)",
                "SELECT date_trunc('day', ts), count(*) FROM ev.events GROUP BY 1",
                "SELECT date_trunc('day', ts + INTERVAL '1' HOUR) AS d, count(*) FROM ev.events GROUP BY 1",
                "SELECT date_trunc('millennium', ts) AS d, count(*) FROM ev.events GROUP BY 1",
                "SELECT date_trunc('day', ts) AS d, count(*) FROM ev.events GROUP BY 1"
                        + " ORDER BY date_trunc('day', ts)",
                "SELECT extract(year FROM date_trunc('month', ts)) AS y, count(*) FROM ev.events GROUP BY 1",
                // Step 6: strftime / time_bucket without alias, outside a key, with other arguments,
                // and other functions jOOQ does not know.
                "SELECT strftime(ts, '%Y'), count(*) FROM ev.events GROUP BY 1",
                "SELECT strftime(ts, '%Y') AS y, count(*) FROM ev.events WHERE strftime(ts, '%m') = '01' GROUP BY 1",
                "SELECT strftime(ts, '%Y') AS y, count(*) FROM ev.events GROUP BY 1 ORDER BY strftime(ts, '%Y')",
                "SELECT strftime(ts, '%Y') AS y, max(strftime(ts, '%m')) FROM ev.events GROUP BY 1",
                "SELECT strftime(ts, event_type) AS y, count(*) FROM ev.events GROUP BY 1",
                "SELECT strftime(ts + INTERVAL 1 HOUR, '%Y') AS y, count(*) FROM ev.events GROUP BY 1",
                "SELECT time_bucket(INTERVAL '1 hour', ts, TIMESTAMP '2020-01-01 00:30:00') AS b, count(*)"
                        + " FROM ev.events GROUP BY 1",
                "SELECT time_bucket(INTERVAL '1 month 2 days', ts) AS b, count(*) FROM ev.events GROUP BY 1",
                "SELECT time_bucket(INTERVAL '0.5' SECOND, ts) AS b, count(*) FROM ev.events GROUP BY 1",
                "SELECT time_bucket(INTERVAL 1 HOUR, ts) AS b, my_udf(v) AS u, count(*) FROM ev.events GROUP BY 1",
                "SELECT strftime(ts, '%Y') AS y, count(*) FROM ev.events GROUP BY 1 HAVING my_udf(1) > 0",
                "SELECT random() AS r, count(*) FROM ev.events GROUP BY 1",
                "SELECT extract(isodow FROM ts) AS w, count(*) FROM ev.events GROUP BY 1",
                "SELECT count(*) AS n, id % 6 AS g FROM ev.events GROUP BY 1",
                "SELECT id % 6 AS g, count(*) FROM ev.events GROUP BY id % 5",
                "SELECT id AS g, count(*) FROM ev.events GROUP BY g",
                "SELECT id % 6 AS g, count(*) FROM ev.events GROUP BY g HAVING count(*) > 2",
                "SELECT id % 6 AS g, v + 1 AS w, count(*) FROM ev.events GROUP BY g")) {
            assertNull(DistributedComplete.mergeable(query), query);
            assertNotNull(DistributedComplete.unmergedAggregateReason(query), query);
        }
    }

    /** Item 43: point lookups and small LIMITs start on probation; scans and ORs do not. */
    /** Item 55 (a): the merged line's phases are the gaps between the marks, in microseconds. */
    @Test
    void phasesAreTheGapsBetweenMarks() {
        assertEquals("parse:1,decide:2,plan:3,admit:0,open:10,schema:5,merge:1000",
                DistributedComplete.phases(0, 1_000, 3_000, 6_000, 6_500, 16_500, 21_500, 1_021_500));
        assertEquals("parse:7", DistributedComplete.phases(0, 7_000));
    }

    /** Item 55 (a): only plain scans are the concatenation of their endpoint answers. */
    @Test
    void onlyPlainScansAreConcatenations() {
        for (String query : List.of("SELECT * FROM s.t", "SELECT a, b FROM s.t WHERE a > 3")) {
            assertTrue(DistributedComplete.concatenation(ParquetQueryParser.parse(query)), query);
        }
        for (String query : List.of("SELECT count(*) FROM s.t", "SELECT DISTINCT a FROM s.t",
                "SELECT a FROM s.t ORDER BY a", "SELECT a FROM s.t LIMIT 5",
                "SELECT a, count(*) FROM s.t GROUP BY a")) {
            assertFalse(DistributedComplete.concatenation(ParquetQueryParser.parse(query)), query);
        }
    }

    @Test
    void pointLookupsAreSmallAnswers() {
        for (String query : List.of("SELECT * FROM tpch.orders WHERE o_orderkey = 5",
                "SELECT * FROM tpch.lineitem WHERE l_orderkey = 12 ORDER BY l_linenumber",
                "SELECT o_orderkey FROM tpch.orders WHERE o_custkey = 7 AND o_totalprice > 1000",
                "SELECT * FROM tpch.orders WHERE o_orderstatus = 'F'",
                "SELECT o_orderkey FROM tpch.orders ORDER BY o_orderkey LIMIT 10")) {
            assertTrue(DistributedComplete.smallAnswer(ParquetQueryParser.parse(query)), query);
        }
        for (String query : List.of("SELECT * FROM tpch.orders",
                "SELECT * FROM tpch.orders WHERE o_orderkey <= 5",
                "SELECT * FROM tpch.orders WHERE o_orderkey >= 5",
                "SELECT * FROM tpch.orders WHERE o_orderkey <> 5",
                "SELECT * FROM tpch.orders WHERE o_orderkey = 5 OR o_custkey > 3",
                "SELECT o_orderkey FROM tpch.orders WHERE o_orderkey % 7 = 0",
                "SELECT o_orderkey FROM tpch.orders ORDER BY o_orderkey LIMIT 100000",
                "SELECT o_custkey, count(*) AS n FROM tpch.orders GROUP BY o_custkey LIMIT 5")) {
            assertFalse(DistributedComplete.smallAnswer(ParquetQueryParser.parse(query)), query);
        }
        // Computed locally: equality filters, without aggregates or with one-row ones; not top-N or GROUP BY.
        for (String query : List.of("SELECT * FROM tpch.orders WHERE o_orderkey = 5",
                "SELECT count(*) FROM tpch.orders WHERE o_custkey = 7 AND o_totalprice > 1000")) {
            assertTrue(DistributedComplete.pointLookup(ParquetQueryParser.parse(query)), query);
        }
        for (String query : List.of("SELECT o_orderkey FROM tpch.orders ORDER BY o_orderkey LIMIT 10",
                "SELECT count(*) FROM tpch.orders WHERE o_custkey > 7",
                "SELECT o_custkey, count(*) AS n FROM tpch.orders WHERE o_orderstatus = 'F' GROUP BY o_custkey")) {
            assertFalse(DistributedComplete.pointLookup(ParquetQueryParser.parse(query)), query);
        }
    }

    /** Item 45: typed, cast, negative and decimal literals are point lookups too; columns and ranges are not. */
    @Test
    void typedLiteralsArePointLookups() {
        for (String query : List.of(
                "SELECT count(*) FROM tpch.orders WHERE o_orderdate = DATE '1992-01-01'",
                "SELECT count(*) FROM tpch.orders WHERE o_orderdate = date '1992-01-01'",
                "SELECT count(*) FROM tpch.orders WHERE o_orderdate = CAST('1992-01-01' AS DATE)",
                "SELECT * FROM tpch.orders WHERE o_orderdate = TRY_CAST('1992-01-01' AS DATE)",
                "SELECT * FROM tpch.orders WHERE o_orderdate = '1992-01-01'::DATE",
                "SELECT * FROM tpch.orders WHERE o_orderdate = TIMESTAMP '1992-01-01 00:00:00'",
                "SELECT * FROM tpch.orders WHERE o_totalprice = CAST(1234.5 AS DECIMAL(15, 2))",
                "SELECT * FROM tpch.orders WHERE o_totalprice = 1234.56",
                "SELECT * FROM tpch.orders WHERE o_totalprice = 1.5e3",
                "SELECT * FROM tpch.orders WHERE o_custkey = -7",
                "SELECT * FROM tpch.orders WHERE o_custkey = (7)",
                "SELECT * FROM tpch.orders WHERE \"o_orderdate\" = DATE '1992-01-01' AND o_custkey > 3")) {
            assertTrue(DistributedComplete.pointLookup(ParquetQueryParser.parse(query)), query);
            assertTrue(DistributedComplete.smallAnswer(ParquetQueryParser.parse(query)), query);
        }
        for (String query : List.of(
                "SELECT count(*) FROM tpch.orders WHERE o_orderdate >= DATE '1992-01-01'",
                "SELECT count(*) FROM tpch.orders WHERE o_orderdate <= CAST('1992-01-01' AS DATE)",
                "SELECT count(*) FROM tpch.orders WHERE o_orderdate <> DATE '1992-01-01'",
                "SELECT count(*) FROM tpch.orders WHERE o_orderdate = CAST(o_comment AS DATE)",
                "SELECT count(*) FROM tpch.orders WHERE o_orderdate = DATE '1992-01-01' OR o_custkey = 3",
                "SELECT o_custkey, count(*) AS n FROM tpch.orders WHERE o_orderdate = DATE '1992-01-01'"
                        + " GROUP BY o_custkey")) {
            assertFalse(DistributedComplete.pointLookup(ParquetQueryParser.parse(query)), query);
        }
    }

    /** A subquery of the same table would see only an endpoint's files: never merged. */
    @Test
    void subqueriesOfTheSameTableAreNotMerged() {
        assertNull(DistributedComplete.mergeable("SELECT count(*) FROM tpch.lineitem"
                + " WHERE l_quantity > (SELECT avg(l_quantity) FROM tpch.lineitem)"));
    }
}
