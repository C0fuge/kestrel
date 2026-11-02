package pro.surpin.data.arrowflight.server;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Regression test (service matrix, cell ranger): under any authorization provider the table check
 * refused queries DuckDB runs - comma joins, a FROM subquery the text parse flattens, and
 * {@code ?} parameters - so half of TPC-H was refused. Their tables are now named by DuckDB's own
 * parse, and every one of them is checked.
 */
@Tag("unit")
class FlightAuthorizationTextParseTest {

    /** Lineitem, orders and customer of schema tpch only. */
    private static final Set<String> READABLE = Set.of("lineitem", "orders", "customer");

    /** Comma joins, FROM subqueries and parameters as TPC-H and prepared clients send them. */
    private static final List<String> QUERIES = List.of(
            "SELECT l_orderkey, sum(l_extendedprice) FROM tpch.customer, tpch.orders, tpch.lineitem"
                    + " WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey GROUP BY l_orderkey",
            "SELECT c_count, count(*) FROM (SELECT c_custkey, count(o_orderkey) AS c_count"
                    + " FROM tpch.customer LEFT OUTER JOIN tpch.orders ON c_custkey = o_custkey"
                    + " GROUP BY c_custkey) AS c_orders GROUP BY c_count",
            "SELECT o_year, sum(volume) FROM (SELECT extract(year FROM o_orderdate) AS o_year,"
                    + " l_extendedprice AS volume FROM tpch.lineitem, tpch.orders"
                    + " WHERE o_orderkey = l_orderkey) AS all_nations GROUP BY o_year",
            "SELECT count(*) FROM tpch.lineitem WHERE l_orderkey = ?",
            "SELECT * FROM tpch.orders WHERE o_orderkey BETWEEN ? AND ? AND o_custkey = $3");

    /** Records every table asked and allows {@link #READABLE} only. */
    private static final class Recording implements FlightAuthorization {
        /** Tables asked, as schema.table. */
        private final List<String> asked = new ArrayList<>();

        @Override
        public boolean canSeeSchema(String principal, String schema) {
            return true;
        }

        @Override
        public boolean canReadTable(String principal, String schema, String table) {
            asked.add(schema + "." + table);
            return "tpch".equals(schema) && READABLE.contains(table);
        }
    }

    @Test
    void joinsSubqueriesAndParametersAreAnsweredWithEveryTableChecked() {
        List<List<String>> expected = List.of(
                List.of("tpch.customer", "tpch.orders", "tpch.lineitem"),
                List.of("tpch.customer", "tpch.orders"),
                List.of("tpch.lineitem", "tpch.orders"),
                List.of("tpch.lineitem"),
                List.of("tpch.orders"));
        for (int i = 0; i < QUERIES.size(); i++) {
            String sql = QUERIES.get(i);
            Recording provider = new Recording();
            assertDoesNotThrow(() -> provider.requireQuery("u", sql), sql);
            assertEquals(Set.copyOf(expected.get(i)), Set.copyOf(provider.asked), sql);
        }
    }

    @Test
    void anUnreadableTableInThemIsDenied() {
        for (String sql : List.of(
                "SELECT * FROM tpch.lineitem, tpch.nation WHERE l_suppkey = n_nationkey",
                "SELECT * FROM (SELECT * FROM tpch.lineitem, tpch.supplier) AS x",
                "SELECT c_count FROM (SELECT o_custkey AS c_count FROM tpch.orders"
                        + " JOIN tpch.region ON o_custkey = r_regionkey) AS t",
                "SELECT * FROM tpch.part WHERE p_partkey = ?",
                "SELECT * FROM tpch.lineitem, nation WHERE l_suppkey = n_nationkey")) {
            FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                    () -> new Recording().requireQuery("u", sql), sql);
            assertEquals(FlightStatusCode.UNAUTHORIZED, e.status().code(), sql);
        }
    }

    @Test
    void whatDuckDbCannotNameIsStillRefused() {
        for (String sql : List.of(
                "SELECT * FROM tpch.lineitem, read_parquet('data/x.parquet')",
                "SELECT * FROM (SELECT * FROM read_csv('/etc/passwd')) AS x",
                "SELECT * FROM tpch.lineitem, memory.tpch.orders",
                "SELECT * FROM tpch.lineitem; SELECT * FROM tpch.part",
                "DELETE FROM tpch.lineitem WHERE l_orderkey = ?")) {
            FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                    () -> new Recording().requireQuery("u", sql), sql);
            assertEquals(FlightStatusCode.INVALID_ARGUMENT, e.status().code(), sql);
        }
    }
}
