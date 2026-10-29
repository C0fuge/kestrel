package pro.surpin.data.arrowflight.auth.rangerhive;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests the translation of Ranger Hive row filters: the typed subset keeps the rows Hive keeps (checked in DuckDB
 * against the rows the filter means), and every construct whose result could differ is refused.
 */
@Tag("unit")
class HiveRowFilterTest {
    /** Columns of the test table. */
    private static final Map<String, String> COLUMNS = columns();

    /** Accepted filters keep the expected ids. */
    @Test
    void translatedFiltersKeepTheRows() throws Exception {
        Map<String, List<Integer>> cases = new LinkedHashMap<>();
        cases.put("id < 3", List.of(1, 2));
        cases.put("region = 'eu'", List.of(1, 3));
        cases.put("Region == \"eu\" and not (id = 1)", List.of(3));
        cases.put("region in ('us', 'apac') or region is null", List.of(2, 4, 5));
        cases.put("id not between 2 and 4", List.of(1, 5));
        cases.put("owner = current_user()", List.of(2, 5));
        cases.put("upper(owner) = 'ANA' OR lcase(region) = 'apac'", List.of(1, 4));
        cases.put("name like 'a\\_%'", List.of(1));
        cases.put("name not like '%b%'", List.of(1, 3, 4));
        cases.put("active", List.of(1, 2));
        cases.put("active = false and amount >= -1", List.of(3));
        cases.put("`id` <> 2 AND amount > 10", List.of(4));
        cases.put("birth is not null and id != 5", List.of(1, 2));
        cases.put("region in ('eu', null)", List.of(1, 3));
        cases.put("null", List.of());
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = duck.createStatement()) {
            st.execute("CREATE TABLE t AS SELECT * FROM (VALUES"
                    + " (1, 'a_x', 'eu', 'ana', true, 5.50::DECIMAL(10,2), DATE '2000-01-01'),"
                    + " (2, 'abc', 'us', 'bob', true, 7.00, DATE '2001-01-01'),"
                    + " (3, 'cd', 'eu', 'cy', false, -1.00, NULL),"
                    + " (4, 'dd', 'apac', 'dan', NULL, 12.25, NULL),"
                    + " (5, 'eb', NULL, 'bob', false, NULL, DATE '2003-01-01'))"
                    + " v(id, name, region, owner, active, amount, birth)");
            for (Map.Entry<String, List<Integer>> c : cases.entrySet()) {
                String sql = HiveRowFilter.sql(c.getKey(), COLUMNS, "bob");
                List<Integer> ids = new ArrayList<>();
                try (ResultSet rs = st.executeQuery("SELECT id FROM t WHERE " + sql + " ORDER BY id")) {
                    while (rs.next()) {
                        ids.add(rs.getInt(1));
                    }
                }
                assertEquals(c.getValue(), ids, c.getKey() + " -> " + sql);
            }
        }
    }

    /** Constructs whose results may differ between Hive and DuckDB are refused. */
    @Test
    void untranslatableFiltersAreRefused() {
        for (String filter : List.of(
                "id < '3'",
                "region = 1",
                "birth > '2000-01-01'",
                "amount > 1.5",
                "id + 1 < 3",
                "region || 'x' = 'eux'",
                "t.id = 1",
                "id in (select id from other)",
                "cast(id as string) = '1'",
                "length(name) > 2",
                "name rlike 'a.*'",
                "unknown = 1",
                "id",
                "region",
                "id < 3 -- comment",
                "name like owner",
                "id like '1%'",
                "id = 12345678901",
                "",
                "id <",
                "(id < 3",
                "id < 3)",
                "region = 'eu' and",
                "not region")) {
            assertThrows(IllegalArgumentException.class, () -> HiveRowFilter.sql(filter, COLUMNS, "bob"), filter);
        }
    }

    /** The caller's name is a literal, quoted. */
    @Test
    void currentUserIsQuoted() {
        assertEquals("(\"owner\" = 'o''brien')", HiveRowFilter.sql("owner = current_user()", COLUMNS, "o'brien"));
    }

    /**
     * Builds the test columns.
     *
     * @return name to DuckDB type
     */
    private static Map<String, String> columns() {
        Map<String, String> columns = new LinkedHashMap<>();
        columns.put("id", "INTEGER");
        columns.put("name", "VARCHAR");
        columns.put("Region", "VARCHAR");
        columns.put("owner", "VARCHAR");
        columns.put("active", "BOOLEAN");
        columns.put("amount", "DECIMAL(10,2)");
        columns.put("birth", "DATE");
        return columns;
    }
}
