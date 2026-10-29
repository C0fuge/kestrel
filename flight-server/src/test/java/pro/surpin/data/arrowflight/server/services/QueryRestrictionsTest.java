package pro.surpin.data.arrowflight.server.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.FlightAuthorization;
import pro.surpin.data.arrowflight.server.TableRestriction;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;

/**
 * Checks the query rewrite of table restrictions: every reference of a restricted table gets the
 * filter and masks, unrestricted tables and providers that restrict nothing leave the text as it
 * is, and anything the rewrite cannot reach is refused.
 */
@Tag("unit")
class QueryRestrictionsTest {

    /** Columns of every table. */
    private static final List<ColumnSpec> COLUMNS = List.of(new ColumnSpec("id", "INTEGER", true, null),
            new ColumnSpec("name", "VARCHAR", true, null));

    /** Masks s.a's name and keeps its rows with id below 10; s.b is whole; s.c is denied. */
    private static final FlightAuthorization RESTRICTING = new FlightAuthorization() {
        @Override
        public boolean canSeeSchema(String principal, String schema) {
            return true;
        }

        @Override
        public boolean canReadTable(String principal, String schema, String table) {
            return !"c".equals(table);
        }

        @Override
        public boolean restrictsTables() {
            return true;
        }

        @Override
        public TableRestriction restrictTable(String principal, String schema, String table,
                List<ColumnSpec> columns) {
            if ("a".equalsIgnoreCase(table)) {
                return new TableRestriction(true, "id < 10", Map.of("name", "'***'"));
            }
            return FlightAuthorization.super.restrictTable(principal, schema, table, columns);
        }
    };

    @Test
    void restrictedReferencesAreReplaced() {
        String source = "(SELECT * REPLACE (CAST(('***') AS VARCHAR) AS \"name\") FROM \"s\".\"a\""
                + " WHERE (id < 10))";
        assertEquals("SELECT * FROM " + source + " AS \"a\"", rewrite("SELECT * FROM s.a"));
        assertEquals("SELECT x.name FROM " + source + " x JOIN s.b y ON x.id = y.id",
                rewrite("SELECT x.name FROM s.a x JOIN s.b y ON x.id = y.id"));
        assertEquals("SELECT count(*) FROM " + source.replace("\"a\"", "\"A\"") + " AS \"A\", "
                + source + " AS t",
                rewrite("SELECT count(*) FROM \"s\".\"A\", s.a AS t"));
        assertEquals("SELECT * FROM s.b WHERE id IN (SELECT id FROM " + source + " AS \"a\")",
                rewrite("SELECT * FROM s.b WHERE id IN (SELECT id FROM s.a)"));
    }

    @Test
    void unrestrictedQueriesStayAsTheyAre() {
        String sql = "SELECT * FROM s.b";
        QueryRestrictions.Result result = QueryRestrictions.apply("u", sql, RESTRICTING, this::columnsOf);
        assertFalse(result.restricted());
        assertSame(sql, result.sql());
        List<String> asked = new ArrayList<>();
        FlightAuthorization plain = new FlightAuthorization() {
            @Override
            public boolean canSeeSchema(String principal, String schema) {
                return true;
            }

            @Override
            public boolean canReadTable(String principal, String schema, String table) {
                return true;
            }
        };
        String any = "SELECT * FROM s.a";
        assertSame(any, QueryRestrictions.apply("u", any, plain, (s, t) -> {
            asked.add(t);
            return COLUMNS;
        }).sql());
        assertTrue(asked.isEmpty(), "a provider that restricts nothing is not asked");
    }

    @Test
    void whatCannotBeRestrictedIsRefused() {
        for (String sql : List.of(
                // A reference the finder misses: after an ON condition's comma, after TABLE.
                "SELECT * FROM s.b JOIN s.b y ON coalesce(1, 2) = 1, s.a",
                "SELECT * FROM s.b WHERE id IN (TABLE s.a)",
                "SELECT * FROM s.a FOR VERSION AS OF 3",
                "SELECT * FROM s.c",
                "SELECT * FROM s.a JOIN s.c USING (id)")) {
            FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                    () -> rewrite(sql), sql);
            assertEquals(FlightStatusCode.UNAUTHORIZED, e.status().code(), sql);
        }
        FlightAuthorization unknownColumn = new FlightAuthorization() {
            @Override
            public boolean canSeeSchema(String principal, String schema) {
                return true;
            }

            @Override
            public boolean canReadTable(String principal, String schema, String table) {
                return true;
            }

            @Override
            public boolean restrictsTables() {
                return true;
            }

            @Override
            public TableRestriction restrictTable(String principal, String schema, String table,
                    List<ColumnSpec> columns) {
                return new TableRestriction(true, null, Map.of("salary", "0"));
            }
        };
        FlightAuthorization failing = new FlightAuthorization() {
            @Override
            public boolean canSeeSchema(String principal, String schema) {
                return true;
            }

            @Override
            public boolean canReadTable(String principal, String schema, String table) {
                return true;
            }

            @Override
            public boolean restrictsTables() {
                return true;
            }

            @Override
            public TableRestriction restrictTable(String principal, String schema, String table,
                    List<ColumnSpec> columns) {
                throw new IllegalStateException("policy engine down");
            }
        };
        for (FlightAuthorization provider : List.of(unknownColumn, failing)) {
            FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                    () -> QueryRestrictions.apply("u", "SELECT * FROM s.a", provider, this::columnsOf));
            assertEquals(FlightStatusCode.UNAUTHORIZED, e.status().code());
        }
    }

    @Test
    void restrictionsAreChecked() {
        assertThrows(IllegalArgumentException.class,
                () -> new TableRestriction(false, "id < 1", Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new TableRestriction(true, " ", Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new TableRestriction(true, null, Map.of("a", "")));
        assertFalse(TableRestriction.UNRESTRICTED.restricts());
        assertFalse(TableRestriction.DENIED.restricts());
        assertTrue(new TableRestriction(true, "true", null).restricts());
    }

    /**
     * Rewrites a query with {@link #RESTRICTING}.
     *
     * @param sql query
     * @return rewritten query
     */
    private String rewrite(String sql) {
        return QueryRestrictions.apply("u", sql, RESTRICTING, this::columnsOf).sql();
    }

    /**
     * Returns the columns of a table.
     *
     * @param schema schema name
     * @param table table name
     * @return {@link #COLUMNS}
     */
    private List<ColumnSpec> columnsOf(String schema, String table) {
        return COLUMNS;
    }
}
