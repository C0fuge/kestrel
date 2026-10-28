package pro.surpin.data.arrowflight.plugin.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Simple Hive views translated into DuckDB SQL give Hive's rows with the declared columns; every
 * construct whose result could differ, or that would let DuckDB read something other than the
 * catalog's tables, is refused.
 */
@Tag("unit")
class HiveViewSqlTest {

    private static final Function<String, String> SCHEMAS = d -> d.equals("hidden") ? null : "h_" + d;

    @Test
    void translatesProjectionsFiltersJoinsGroupingAndUnions() throws Exception {
        try (Connection duckdb = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = duckdb.createStatement()) {
            s.execute("CREATE SCHEMA h_db");
            s.execute("CREATE SCHEMA h_other");
            s.execute("CREATE TABLE h_db.t AS SELECT * FROM (VALUES (1, 'a_b', 'x', 1.25::DECIMAL(10,2)),"
                    + " (2, 'abc', 'y', 2.5), (3, 'a_z', NULL, NULL), (4, NULL, 'x', 3))"
                    + " v(id, name, region, amount)");
            s.execute("CREATE TABLE h_other.u AS SELECT 1 AS id, 'v1' AS v");

            HiveViewSql.Translated filter = HiveViewSql.translate("SELECT `t`.`id`, `t`.`name`"
                    + " FROM `db`.`t` WHERE `t`.`id` > 1 AND `t`.`name` LIKE 'a\\_%' -- comment\n",
                    columns("id", "int", "name", "string"), SCHEMAS);
            assertEquals(List.of(new HiveViewSql.Source("db", "t")), filter.sources());
            assertEquals(List.of("3|a_z"), rows(s, filter.sql()));
            assertEquals(List.of("id INTEGER", "name VARCHAR"), types(s, filter.sql()));

            HiveViewSql.Translated grouped = HiveViewSql.translate("select `t`.`region`, count(*),"
                    + " sum(`t`.`amount`) from `DB`.`T` group by `t`.`region`"
                    + " having count(*) >= 1", columns("region", "string", "n", "bigint",
                    "total", "decimal(20,2)"), SCHEMAS);
            assertEquals(List.of("null|1|null", "x|2|4.25", "y|1|2.50"),
                    rows(s, grouped.sql() + " ORDER BY 1 NULLS FIRST"));
            assertEquals(List.of("region VARCHAR", "n BIGINT", "total DECIMAL(20,2)"),
                    types(s, grouped.sql()));

            HiveViewSql.Translated joined = HiveViewSql.translate("SELECT `a`.`id`, `b`.`v`"
                    + " FROM `db`.`t` `a` LEFT OUTER JOIN `other`.`u` `b` ON `a`.`id` = `b`.`id`"
                    + " UNION ALL SELECT `x`.`id`, NULL FROM (SELECT `t`.`id` FROM `db`.`t`) `x`"
                    + " WHERE `x`.`id` IN (1, 2) AND `x`.`id` BETWEEN -5 AND 10",
                    columns("id", "int", "v", "varchar(10)"), SCHEMAS);
            assertEquals(List.of(new HiveViewSql.Source("db", "t"),
                    new HiveViewSql.Source("other", "u")), joined.sources());
            assertEquals(List.of("1|null", "1|v1", "2|null", "2|null", "3|null", "4|null"),
                    rows(s, joined.sql() + " ORDER BY 1, 2 NULLS FIRST"));

            HiveViewSql.Translated functions = HiveViewSql.translate("SELECT coalesce(`t`.`name`,"
                    + " \"no\\\"ne\") || 'x', CASE WHEN `t`.`id` != 2 THEN upper(`t`.`name`)"
                    + " ELSE nvl(NULL, 'it\\'s') END, `t`.`id` == 4 FROM `db`.`t`, `db`.`t` `t2`"
                    + " WHERE `t`.`id` = `t2`.`id`",
                    columns("a", "string", "b", "string", "c", "boolean"), SCHEMAS);
            assertEquals(List.of("a_bx|A_B|false", "a_zx|A_Z|false", "abcx|it's|false",
                    "no\"nex|null|true"), rows(s, functions.sql() + " ORDER BY 3, 1"));
        }
    }

    @Test
    void refusesWhatCouldGiveOtherRows() {
        for (String body : List.of("`t`.`a` / 2", "`t`.`a` % 2", "`t`.`a` div 2", "cast(`t`.`a` as int)",
                "1.5", "1L", "1e3", "`t`.`a`[0]", "rand()", "`rand`()", "concat(`t`.`a`, 'x')",
                "substr(`t`.`a`, 0, 1)", "'a' 'b'", "'\\u0041'", "! `t`.`a`", "`t`.`a` & 1",
                "`t`.`a` rlike 'x'", "`t`.`a` LIKE `t`.`b`", "row_number() over ()",
                "if(`t`.`a`, 1, 2)", "`t`.`a` * 2", "current_date")) {
            assertRefused("SELECT " + body + " FROM `db`.`t`");
        }
        assertRefused("SELECT `t`.`a` FROM `db`.`t` ORDER BY `t`.`a`");
        assertRefused("SELECT `t`.`a` FROM `db`.`t` LIMIT 1");
        assertRefused("WITH `c` AS (SELECT 1) SELECT `c`.`a` FROM `db`.`t`");
        assertRefused("SELECT `t`.`a` FROM `db`.`t` LATERAL VIEW explode(`t`.`l`) `e` AS `x`");
        assertRefused("SELECT (1 FROM `db`.`t`");
        assertRefused("SELECT 1) FROM `db`.`t`");
        assertRefused("SELECT 'unclosed FROM `db`.`t`");
        assertRefused("SELECT `t`.`a` FROM `db`.`t` /* unclosed");
        assertRefused("SELECT `t`.`a` FROM `db`.`t`; DROP TABLE x");
        assertRefused("SELECT 1");
        assertRefused(" ");
        assertThrows(IllegalArgumentException.class,
                () -> HiveViewSql.translate("SELECT `t`.`a` FROM `db`.`t`", List.of(), SCHEMAS));
        assertThrows(IllegalArgumentException.class, () -> HiveViewSql.translate(
                "SELECT `t`.`a` FROM `db`.`t`", columns("a", "uniontype<int,string>"), SCHEMAS));
    }

    @Test
    void readsOnlyTablesOfShownDatabases() {
        // DuckDB reads files and table functions named in FROM: only database tables may be there.
        for (String from : List.of("'x.parquet'", "read_parquet('x')", "`t`", "t", "`hidden`.`t`",
                "(VALUES (1)) `v`", "('x.csv')", "`db`.`t`, 'f.csv'", "`db`.`t` JOIN read_csv('f') ON true",
                "`db`.`t` JOIN 'f.csv' ON true", "`db`.`t` JOIN (TABLE `x`) ON true")) {
            assertRefused("SELECT `t`.`a` FROM " + from);
        }
        HiveViewSql.Translated quoted = HiveViewSql.translate("SELECT `t`.`a\"b`, `t`.`c``d`"
                + " FROM `db`.`we\"ird` `t`", columns("x", "int", "y", "int"), SCHEMAS);
        assertTrue(quoted.sql().contains("\"t\".\"a\"\"b\""), quoted.sql());
        assertTrue(quoted.sql().contains("\"t\".\"c`d\""), quoted.sql());
        assertTrue(quoted.sql().contains("\"h_db\".\"we\"\"ird\""), quoted.sql());
        assertEquals(List.of(new HiveViewSql.Source("db", "we\"ird")), quoted.sources());
    }

    private static void assertRefused(String view) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> HiveViewSql.translate(view, columns("a", "int"), SCHEMAS), view);
        assertTrue(e.getMessage() != null && !e.getMessage().isBlank(), view);
    }

    private static List<Map.Entry<String, String>> columns(String... namesAndTypes) {
        List<Map.Entry<String, String>> result = new ArrayList<>();
        for (int i = 0; i < namesAndTypes.length; i += 2) {
            result.add(Map.entry(namesAndTypes[i], namesAndTypes[i + 1]));
        }
        return result;
    }

    private static List<String> rows(Statement s, String sql) throws SQLException {
        List<String> result = new ArrayList<>();
        try (ResultSet rs = s.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    row.append(i > 1 ? "|" : "").append(rs.getString(i));
                }
                result.add(row.toString());
            }
        }
        return result;
    }

    private static List<String> types(Statement s, String sql) throws SQLException {
        List<String> result = new ArrayList<>();
        try (ResultSet rs = s.executeQuery("DESCRIBE " + sql)) {
            while (rs.next()) {
                result.add(rs.getString("column_name") + " " + rs.getString("column_type"));
            }
        }
        return result;
    }
}
