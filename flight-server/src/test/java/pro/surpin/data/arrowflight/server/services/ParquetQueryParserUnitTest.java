package pro.surpin.data.arrowflight.server.services;

import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.metrics.CacheMetrics;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ParquetQueryParserUnitTest {

    // ── flattenSubqueryWrapper (private) ──────────────────────────────────

    @Test
    void flattenSubqueryWrapperUnchangedWhenNoSubquery() throws Exception {
        String result = invokeFlatten("SELECT * FROM s.t WHERE id > 0");
        assertEquals("SELECT * FROM s.t WHERE id > 0", result.trim());
    }

    @Test
    void flattenSubqueryWrapperInlinesSimpleSubquery() throws Exception {
        String result = invokeFlatten(
                "SELECT * FROM (SELECT id FROM s.t WHERE id > 0) sub WHERE id < 100");
        assertTrue(result.contains("from s.t"),
                "Original table ref should be preserved, got: " + result);
        assertFalse(result.contains("sub"),
                "Alias should be removed, got: " + result);
        assertTrue(result.startsWith("SELECT id from s.t"),
                "The inner column list must survive, got: " + result);
    }

    /** Verifies a wrapped query whose meaning would be lost by flattening is refused. */
    @Test
    void flattenSubqueryWrapperRefusesWhatItWouldLose() {
        for (String inner : new String[] {
                "SELECT count(*) FROM s.t", "SELECT id FROM s.t ORDER BY id LIMIT 3",
                "SELECT a, sum(b) AS s FROM s.t GROUP BY a", "SELECT DISTINCT a FROM s.t",
                "SELECT a + 1 AS x FROM s.t"}) {
            Exception e = assertThrows(java.lang.reflect.InvocationTargetException.class,
                    () -> invokeFlatten("select * from (" + inner + ") t"), inner);
            assertInstanceOf(IllegalArgumentException.class, e.getCause(), inner);
        }
    }

    /** Verifies clauses the single-table path cannot run are errors, not silently dropped. */
    @Test
    void refusesClausesThatWouldBeDropped() {
        for (String sql : new String[] {
                "SELECT a FROM s.t UNION ALL SELECT a FROM s.u",
                "SELECT a FROM s.t INTERSECT SELECT a FROM s.u",
                "SELECT a FROM s.t WHERE b = ?",
                "WITH x AS (SELECT a FROM s.t) SELECT a FROM x"}) {
            assertThrows(RuntimeException.class, () -> ParquetQueryParser.parse(sql), sql);
        }
        assertDoesNotThrow(() -> ParquetQueryParser.parse(
                "SELECT a FROM s.t WHERE b = 'union? except' AND \"limit\" = 1;"));
    }

    /** Verifies DISTINCT, HAVING and DISTINCT aggregates reach DuckDB. */
    @Test
    void carriesDistinctAndHaving() {
        ParquetQueryParser distinct = ParquetQueryParser.parse("SELECT DISTINCT a FROM s.t");
        ParquetQueryParser having = ParquetQueryParser.parse(
                "SELECT a, count(*) AS c FROM s.t GROUP BY a HAVING count(*) > 1");
        ParquetQueryParser countDistinct = ParquetQueryParser.parse(
                "SELECT count(DISTINCT a) AS d FROM s.t");

        assertTrue(distinct.distinct);
        assertNull(distinct.singleNodeFeature());
        assertNotNull(having.having);
        assertEquals("HAVING", having.singleNodeFeature());
        assertTrue(countDistinct.selectExprs.get(0).distinct);
        assertNotNull(countDistinct.singleNodeFeature());
    }

    @Test
    void flattenSubqueryWrapperNoFromParenReturnsOriginal() throws Exception {
        String result = invokeFlatten("SELECT * FROM s.t");
        assertEquals("SELECT * FROM s.t", result.trim());
    }

    @Test
    void flattenSubqueryWrapperInnerNotSelectReturnsOriginal() throws Exception {
        String query = "SELECT * FROM (VALUES (1,2)) sub";
        String result = invokeFlatten(query);
        assertEquals(query, result.trim(),
                "Non-SELECT subquery should be returned unchanged");
    }

    @Test
    void flattenSubqueryWrapperUnbalancedParensReturnsOriginal() throws Exception {
        String query = "SELECT * FROM (SELECT id FROM s.t WHERE id > 0 sub";
        String result = invokeFlatten(query);
        assertEquals(query, result.trim(),
                "Unbalanced parentheses should return original unchanged");
    }

    @Test
    void flattenSubqueryWrapperCaseInsensitiveFrom() throws Exception {
        String result = invokeFlatten(
                "SELECT * from (SELECT id FROM s.t) sub WHERE id < 100");
        assertTrue(result.contains("from s.t"),
                "Case-insensitive FROM should work, got: " + result);
    }

    private static String invokeFlatten(String query) throws Exception {
        Method m = ParquetQueryParser.class.getDeclaredMethod(
                "flattenSubqueryWrapper", String.class);
        m.setAccessible(true);
        return (String) m.invoke(null, query);
    }

    // ── ORDER BY / LIMIT / OFFSET ─────────────────────────────────────────

    /** Verifies ordering and row limits are parsed instead of being dropped. */
    @Test
    void parsesOrderByLimitAndOffset() {
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT id, name FROM s.t WHERE id > 1 ORDER BY name DESC, id LIMIT 10 OFFSET 5");

        assertEquals(10L, pq.limit);
        assertEquals(5L, pq.offset);
        assertTrue(pq.hasOrderOrLimit());
        String tail = pq.orderAndLimitSql().toLowerCase();
        assertTrue(tail.startsWith(" order by \"name\" desc, \"id\""), "Got: " + tail);
        assertTrue(tail.endsWith(" limit 10 offset 5"), "Got: " + tail);
    }

    /** Verifies a query without ordering or limits adds nothing to the DuckDB SQL. */
    @Test
    void queryWithoutOrderOrLimitHasNone() {
        ParquetQueryParser pq = ParquetQueryParser.parse("SELECT id FROM s.t WHERE id > 1");

        assertNull(pq.orderBy);
        assertNull(pq.limit);
        assertNull(pq.offset);
        assertFalse(pq.hasOrderOrLimit());
        assertEquals("", pq.orderAndLimitSql());
    }

    /** Verifies the dataset wrapper BI tools such as Superset put around a query. */
    @Test
    void flattensBiToolWrapper() throws Exception {
        assertEquals("SELECT n_name, n_regionkey from tpch.nation LIMIT 5", invokeFlatten(
                "SELECT * FROM (SELECT n_name, n_regionkey FROM tpch.nation) AS virtual_table LIMIT 5"));
        assertEquals("SELECT n_regionkey, count(*) AS c from tpch.nation where (n_nationkey > 0) "
                        + "and (n_name <> 'x limit 1') GROUP BY n_regionkey ORDER BY c DESC LIMIT 10",
                invokeFlatten("SELECT virtual_table.n_regionkey, count(*) AS c FROM "
                        + "(SELECT n_name, n_regionkey, n_nationkey FROM tpch.nation WHERE n_nationkey > 0) "
                        + "AS virtual_table WHERE n_name <> 'x limit 1' "
                        + "GROUP BY virtual_table.n_regionkey ORDER BY c DESC LIMIT 10"));
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT * FROM (SELECT n_name FROM tpch.nation) t ORDER BY n_name DESC LIMIT 2");
        assertEquals(2L, pq.limit);
        assertEquals("nation", pq.table);
    }

    /** Verifies a join without table aliases keeps its JOIN and ON keywords. */
    @Test
    void joinWithoutAliases() {
        ParquetQueryParser pq = ParquetQueryParser.parse("SELECT n_name, r_name FROM tpch.nation "
                + "JOIN tpch.region ON n_regionkey = r_regionkey ORDER BY n_name LIMIT 3");

        assertTrue(pq.isJoin);
        assertEquals(List.of("nation", "region"),
                pq.joinTables.stream().map(ParquetQueryParser.JoinTable::alias).toList());
        assertTrue(pq.duckDbSql.contains("FROM nation JOIN region ON n_regionkey = r_regionkey"),
                pq.duckDbSql);
    }

    /** Verifies TIME literals keep their fraction and TIMESTAMPTZ literals are understood. */
    @Test
    void timeLiteralsKeepTheirFraction() {
        assertEquals("SELECT a FROM s.t WHERE t = CAST('12:34:56.789012' AS TIME) "
                        + "AND z = CAST('2024-06-01 09:00:00+00' AS TIMESTAMP WITH TIME ZONE) "
                        + "AND s = 'TIME ''x''' AND ts = TIMESTAMP '2024-01-01 00:00:00' AND runtime = 1",
                ParquetQueryParser.castTypedLiterals("SELECT a FROM s.t WHERE t = TIME '12:34:56.789012' "
                        + "AND z = timestamptz '2024-06-01 09:00:00+00' "
                        + "AND s = 'TIME ''x''' AND ts = TIMESTAMP '2024-01-01 00:00:00' AND runtime = 1"));
        ParquetQueryParser pq = ParquetQueryParser.parse(
                "SELECT c FROM s.t WHERE c = TIME '12:34:56.789012'");
        assertTrue(pq.filter.contains("12:34:56.789012"), pq.filter);
    }

    /** Verifies plain columns and plain aggregates are answered by every endpoint for its files. */
    @Test
    void plainColumnsAndAggregatesNeedNoCompleteAnswer() {
        for (String sql : new String[] {
                "SELECT a, \"Mixed Case\" AS m FROM s.t WHERE b > 1 ORDER BY a LIMIT 5",
                "SELECT a, count(*), count(b), sum(b * 2) AS s, min(c), max(c) FROM s.t GROUP BY a",
                "SELECT count(DISTINCT b) FROM s.t", "SELECT * FROM s.t",
                "SELECT n_name, r_name FROM s.nation JOIN s.region ON n_regionkey = r_regionkey"}) {
            assertNull(ParquetQueryParser.parse(sql).completeAnswerReason, sql);
        }
    }

    /**
     * Verifies select items the endpoints cannot compute ask for the complete answer: before,
     * expressions became quoted column names (Binder Error) and FILTER/OVER were dropped.
     */
    @Test
    void expressionsOverAggregatesNeedTheCompleteAnswer() {
        for (String sql : new String[] {
                "SELECT CAST(sum(x) AS DECIMAL(38,2)) AS a FROM s.t",
                "SELECT g, sum(x) / count(*) AS r FROM s.t GROUP BY g",
                "SELECT avg(x) FROM s.t",
                "SELECT count(*) FILTER (WHERE x > 1) FROM s.t",
                "SELECT max(x) FILTER (WHERE y > 0) AS m FROM s.t",
                "SELECT x, sum(x) OVER () FROM s.t",
                "SELECT x + 1 AS y FROM s.t",
                "SELECT g, count(*) FROM s.t GROUP BY g, x + 1"}) {
            assertNotNull(ParquetQueryParser.parse(sql).completeAnswerReason, sql);
        }
        assertEquals("The select item 'cast(sum(x) as decimal(38, 2))' is not a column or a plain"
                        + " count, sum, min or max",
                ParquetQueryParser.parse("SELECT CAST(sum(x) AS DECIMAL(38,2)) AS a FROM s.t")
                        .completeAnswerReason);
    }

    /** A join key list can be megabytes of SQL; remembering every distinct one would pin that memory. */
    @Test
    void doesNotCacheVeryLongQueries() {
        String small = "SELECT a FROM s.t WHERE a IN (1, 2, 3)";
        ParquetQueryParser.parse(small);
        assertTrue(ParquetQueryParser.isCached(small));

        String huge = longInListQuery();
        assertTrue(huge.length() > QueryCacheLimits.MAX_CACHED_QUERY_CHARS);
        long skipped = CacheMetrics.oversizeCount(CacheMetrics.Cache.PARSE);
        ParquetQueryParser parsed = ParquetQueryParser.parse(huge);
        assertNotNull(parsed.filter);
        assertFalse(ParquetQueryParser.isCached(huge));
        assertEquals(skipped + 1, CacheMetrics.oversizeCount(CacheMetrics.Cache.PARSE));
        // Parsed again, with the same answer.
        assertEquals(parsed.filter.toString(), ParquetQueryParser.parse(huge).filter.toString());
    }

    /**
     * Builds a query whose IN list makes it longer than the cache limit.
     *
     * @return SQL text
     */
    static String longInListQuery() {
        StringBuilder huge = new StringBuilder("SELECT a FROM s.t WHERE a IN (0");
        for (int i = 1; i < 30_000; i++) {
            huge.append(", ").append(1_000_003L * i);
        }
        return huge.append(')').toString();
    }
}
