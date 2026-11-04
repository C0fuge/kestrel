package pro.surpin.data.arrowflight.server.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The finder of table references named with their schema: only names in FROM positions count,
 * aliases and whole-row uses are seen, literals, comments, column qualifiers, three-part names
 * and table functions are left alone.
 */
@Tag("unit")
class TableReferencesTest {

    private static List<TableReferences.Reference> find(String sql) {
        return TableReferences.find(sql, (schema, table) -> schema.equalsIgnoreCase("hive")
                && table.equalsIgnoreCase("orders"));
    }

    private static String rewrite(String sql) {
        List<TableReferences.Reference> references = find(sql);
        return TableReferences.rewrite(sql, references, references.stream()
                .map(r -> "SRC" + (r.aliased() ? "" : " AS " + r.table())).toList());
    }

    @Test
    void namesAfterFromJoinAndCommasAreReplaced() {
        assertEquals("SELECT * FROM SRC AS orders", rewrite("SELECT * FROM hive.orders"));
        assertEquals("SELECT o.id FROM SRC o, SRC AS Orders JOIN SRC AS p ON o.id = p.id",
                rewrite("SELECT o.id FROM hive.orders o, HIVE.Orders JOIN \"hive\".\"orders\""
                        + " AS p ON o.id = p.id"));
        assertEquals("FROM SRC AS orders SELECT 1", rewrite("FROM hive.orders SELECT 1"));
        assertEquals("SELECT 1 FROM (SELECT * FROM SRC AS orders) x, SRC AS orders",
                rewrite("SELECT 1 FROM (SELECT * FROM hive.orders) x, hive.orders"));
        List<TableReferences.Reference> references = find("SELECT * FROM hive.orders o");
        assertEquals(1, references.size());
        assertTrue(references.get(0).aliased());
        assertEquals("hive", references.get(0).schema());
    }

    @Test
    void otherUsesOfTheNameAreLeftAlone() {
        for (String sql : List.of(
                "SELECT 'FROM hive.orders' FROM hive.other",
                "SELECT 1 FROM hive.other -- FROM hive.orders",
                "SELECT 1 FROM db.hive.orders",
                "SELECT 1 FROM hive.orders(1)",
                "SELECT hive.orders FROM x",
                "SELECT a, hive.orders FROM x",
                "SELECT 1 FROM x WHERE a IN (1, hive.orders)",
                "SELECT 1 FROM x JOIN y USING (a, hive.orders)",
                "SELECT 1 FROM orders")) {
            assertEquals(List.of(), find(sql), sql);
        }
    }

    @Test
    void theWholeRowIsSeenThroughItsAliasOrName() {
        assertFalse(find("SELECT o.id, orders FROM hive.orders o").get(0).wholeRow());
        assertTrue(find("SELECT o FROM hive.orders o").get(0).wholeRow());
        assertTrue(find("SELECT orders FROM hive.orders").get(0).wholeRow());
        assertFalse(find("SELECT orders.id, x.orders FROM hive.orders").get(0).wholeRow());
        assertTrue(find("SELECT to_json(\"O\") FROM hive.orders AS o").get(0).wholeRow());
    }

    @Test
    void aSingleSelectHasOneQueryBlock() {
        assertTrue(TableReferences.singleSelect("SELECT a FROM hive.orders WHERE b = 1"));
        assertFalse(TableReferences.singleSelect("SELECT a FROM hive.orders WHERE b IN"
                + " (SELECT b FROM x)"));
        assertFalse(TableReferences.singleSelect("SELECT a FROM x UNION SELECT a FROM y"));
        assertFalse(TableReferences.singleSelect("WITH c AS (SELECT 1) SELECT a FROM c"));
        assertFalse(TableReferences.singleSelect("FROM hive.orders"));
        assertTrue(TableReferences.singleSelect("SELECT 'select from' FROM hive.orders"));
    }

    private static String qualify(String sql) {
        return TableReferences.qualifyPlainNames(sql, name -> switch (name.toLowerCase(java.util.Locale.ROOT)) {
            case "lineitem", "orders", "customer" -> "tpch";
            case "orders_sp" -> "spread";
            default -> null;
        });
    }

    /** Item 31: plain names get the one schema that has them, everywhere a table is read. */
    @Test
    void plainTableNamesGetTheirSchema() {
        assertEquals("SELECT count(*) FROM \"tpch\".lineitem", qualify("SELECT count(*) FROM lineitem"));
        assertEquals("SELECT count(*) FROM \"spread\".orders_sp o, \"tpch\".customer c"
                + " WHERE o.o_custkey = c.c_custkey",
                qualify("SELECT count(*) FROM orders_sp o, customer c WHERE o.o_custkey = c.c_custkey"));
        assertEquals("SELECT * FROM \"tpch\".\"Orders\" AS x JOIN \"tpch\".lineitem l ON x.a = l.b",
                qualify("SELECT * FROM \"Orders\" AS x JOIN lineitem l ON x.a = l.b"));
        assertEquals("SELECT lineitem.l_orderkey FROM \"tpch\".lineitem WHERE l_orderkey IN"
                + " (SELECT o_orderkey FROM \"tpch\".orders)",
                qualify("SELECT lineitem.l_orderkey FROM lineitem WHERE l_orderkey IN"
                        + " (SELECT o_orderkey FROM orders)"));
        assertEquals("FROM \"tpch\".lineitem SELECT 1", qualify("FROM lineitem SELECT 1"));
        assertEquals("SELECT 1 FROM (SELECT * FROM \"tpch\".orders) o, \"tpch\".customer",
                qualify("SELECT 1 FROM (SELECT * FROM orders) o, customer"));
    }

    @Test
    void qualifiedDefinedAndUnknownNamesStayAsWritten() {
        for (String sql : List.of(
                "SELECT * FROM tpch.lineitem",
                "SELECT * FROM other.lineitem l",
                "SELECT * FROM cat.tpch.lineitem",
                "SELECT * FROM read_parquet('lineitem')",
                "SELECT * FROM range(3) lineitem",
                "SELECT * FROM unknown_table",
                "SELECT 'FROM lineitem' AS x FROM unknown_table",
                "SELECT 1 FROM unknown_table -- FROM lineitem",
                "SELECT lineitem FROM unknown_table",
                "SELECT a, orders FROM unknown_table GROUP BY a, orders",
                "WITH lineitem AS (SELECT 1 AS a) SELECT * FROM lineitem",
                "WITH x AS (SELECT 1), orders(a) AS (SELECT 2) SELECT * FROM x, orders",
                "WITH orders AS MATERIALIZED (SELECT 2) SELECT * FROM orders")) {
            assertEquals(sql, qualify(sql), sql);
        }
        assertEquals("WITH c AS (SELECT * FROM \"tpch\".orders) SELECT * FROM c, \"tpch\".lineitem",
                qualify("WITH c AS (SELECT * FROM orders) SELECT * FROM c, lineitem"));
    }
}
