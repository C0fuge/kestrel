package pro.surpin.data.arrowflight.server.services;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

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

    /** Item 43: point lookups and small LIMITs start on probation; scans and ORs do not. */
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
