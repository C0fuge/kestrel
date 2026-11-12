package pro.surpin.data.arrowflight.server.adapters;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.duckdb.DuckDbSetupPlan;
import pro.surpin.data.arrowflight.server.duckdb.HiddenAttachments;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Item 47 (Q8): the databases plugins attach are left out of DuckDB's metadata functions and
 * views on a connection, and qualified calls that would reach DuckDB's own functions are refused.
 */
@Tag("unit")
class HiddenAttachmentsTest {

    /** Attachment name of these tests; no other test uses it (the hiding is process-wide). */
    private static final String HIDDEN = "item47_hidden_db";

    @BeforeAll
    static void hide() {
        HiddenAttachments.addAll(List.of(HIDDEN.toUpperCase(java.util.Locale.ROOT)));
    }

    /** Metadata queries that listed the attached database's names before the fix. */
    private static final List<String> METADATA = List.of(
            "SELECT table_catalog, table_schema, table_name FROM information_schema.tables",
            "SELECT table_catalog, table_name, column_name FROM information_schema.columns",
            "SELECT catalog_name, schema_name FROM information_schema.schemata",
            "SELECT table_catalog, table_name FROM information_schema.views",
            "SELECT * FROM duckdb_databases()", "SELECT * FROM duckdb_schemas()",
            "SELECT * FROM duckdb_tables()", "SELECT * FROM duckdb_columns()",
            "SELECT * FROM duckdb_views()", "SELECT * FROM duckdb_indexes()",
            "SELECT * FROM duckdb_sequences()", "SELECT * FROM duckdb_types()",
            "SELECT * FROM duckdb_constraints()", "SELECT * FROM duckdb_tables",
            "SELECT * FROM system.main.duckdb_tables", "SELECT * FROM pg_catalog.pg_class",
            "SELECT * FROM pg_namespace", "SELECT * FROM pg_attribute", "SELECT * FROM pg_tables",
            "SELECT * FROM pg_views", "SELECT * FROM pg_database", "SELECT * FROM pg_type",
            "SELECT * FROM sqlite_master", "SELECT * FROM pragma_database_list", "SHOW ALL TABLES",
            "SHOW DATABASES", "PRAGMA show_tables_expanded");

    /**
     * Verifies that no metadata query lists the attached database or its names once the macros
     * exist, also on a session connection with a default catalog of its own, while the server's
     * own view stays listed and its data stays readable.
     */
    @Test
    void metadataListsNoNameOfAnAttachedDatabase() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("ATTACH ':memory:' AS " + HIDDEN);
            s.execute("CREATE SCHEMA " + HIDDEN + ".remote_schema");
            s.execute("CREATE TABLE " + HIDDEN + ".remote_schema.cfg (a INTEGER)");
            s.execute("INSERT INTO " + HIDDEN + ".remote_schema.cfg VALUES (7)");
            s.execute("CREATE TABLE " + HIDDEN + ".remote_schema.unconfigured_tbl (secret_col INTEGER)");
            s.execute("CREATE VIEW " + HIDDEN + ".remote_schema.unconfigured_view AS SELECT 1 AS x");
            s.execute("CREATE SEQUENCE " + HIDDEN + ".remote_schema.unconfigured_seq");
            s.execute("CREATE TYPE " + HIDDEN + ".remote_schema.unconfigured_type AS ENUM ('a')");
            s.execute("CREATE INDEX unconfigured_idx ON " + HIDDEN + ".remote_schema.unconfigured_tbl (secret_col)");
            s.execute("CREATE SCHEMA pg");
            s.execute("CREATE VIEW pg.cfg AS SELECT * FROM " + HIDDEN + ".remote_schema.cfg");
            assertTrue(leaks(s), "the attachment's names are listed without the macros");
            String sizes = "SELECT lower(database_name) FROM pragma_database_size()";
            assertTrue(rows(s, sizes).contains(HIDDEN), "pragma_database_size lists it without the macros");

            DuckDbAdapter.hideAttachments(s);
            assertFalse(leaks(s), "listed with the macros");
            assertFalse(rows(s, sizes).contains(HIDDEN), "pragma_database_size lists it with the macros");
            assertEquals(List.of("memory|pg|cfg"), rows(s, "SELECT table_catalog || '|' || table_schema || '|'"
                    + " || table_name FROM information_schema.tables"));
            assertEquals(List.of("7"), rows(s, "SELECT a FROM pg.cfg"));
            // Names only: the server view's definition still names the configured remote table.
            for (String names : List.of("SELECT database_name FROM duckdb_databases()",
                    "SELECT database_name || schema_name FROM duckdb_schemas()",
                    "SELECT catalog_name || schema_name FROM information_schema.schemata",
                    "SELECT database_name || schema_name || table_name FROM duckdb_columns()",
                    "SELECT database_name || schema_name || view_name FROM duckdb_views()")) {
                for (String row : rows(s, names)) {
                    assertFalse(row.contains(HIDDEN) || row.contains("remote_schema"), names + ": " + row);
                }
            }
            assertEquals(List.of("a"), rows(s, "SELECT column_name FROM information_schema.columns"
                    + " WHERE table_name = 'cfg'"));

            // A session connection of the shared DuckDB: another default catalog, its own macros.
            Connection session = conn.unwrap(org.duckdb.DuckDBConnection.class).duplicate();
            try (Statement ss = session.createStatement()) {
                assertTrue(leaks(ss), "macros are per connection");
                DuckDbAdapter.hideAttachments(ss);
                ss.execute("ATTACH ':memory:' AS flight_session_1");
                ss.execute("USE flight_session_1");
                assertFalse(leaks(ss), "listed on a session connection");
            } finally {
                session.close();
            }
        }
    }

    /**
     * Verifies that a qualified call of a hidden metadata function is refused by the parse check,
     * in any case and quoting, while unqualified calls and other functions pass.
     */
    @Test
    void qualifiedCallsOfHiddenFunctionsAreRefused() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            for (String sql : List.of("SELECT * FROM system.main.duckdb_tables()",
                    "SELECT * FROM system.duckdb_columns()", "SELECT * FROM main.duckdb_databases()",
                    "SELECT * FROM SYSTEM.MAIN.DUCKDB_SCHEMAS()", "SELECT * FROM \"system\".\"main\".\"duckdb_views\"()",
                    "SELECT (SELECT count(*) FROM system.main.duckdb_types())",
                    "SELECT * FROM (SELECT 1) x, LATERAL system.duckdb_indexes()")) {
                IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                        () -> DuckDbAdapter.requireAllowedFunctions(serialized(conn, sql)), sql);
                assertTrue(e.getMessage().contains("may not be qualified"), e.getMessage());
            }
            // Names as strings pass the qualifier refusal, so these functions are refused outright.
            for (String sql : List.of("SELECT * FROM pragma_table_info('" + HIDDEN + ".remote_schema.t')",
                    "SELECT * FROM PRAGMA_STORAGE_INFO('" + HIDDEN + "' || '.remote_schema.t')",
                    "SELECT * FROM pragma_show('t')", "SELECT * FROM pragma_metadata_info('" + HIDDEN + "')",
                    "SELECT * FROM system.main.pragma_database_size()")) {
                assertThrows(IllegalArgumentException.class,
                        () -> DuckDbAdapter.requireAllowedFunctions(serialized(conn, sql)), sql);
            }
            for (String sql : List.of("SELECT * FROM duckdb_tables()", "SELECT * FROM information_schema.tables",
                    "SELECT * FROM pragma_database_size()",
                    "SELECT * FROM system.main.duckdb_tables", "SELECT system.main.lower('a')",
                    "SELECT * FROM duckdb_columns() c JOIN duckdb_tables() t USING (table_oid)")) {
                assertDoesNotThrow(() -> DuckDbAdapter.requireAllowedFunctions(serialized(conn, sql)), sql);
            }
        }
    }

    /** Verifies that a plan's attachments are its declared secrets without a scope. */
    @Test
    void attachmentsAreTheUnscopedSecretsOfAPlan() {
        DuckDbSetupPlan plan = new DuckDbSetupPlan(List.of(), List.of(), Set.of(), new SecretRedactor(),
                List.of(), List.of(new DuckDbSetupPlan.DeclaredSecret("s3_secret", "s3://bucket/"),
                        new DuckDbSetupPlan.DeclaredSecret("jdbc_attached_pg", null)));
        assertEquals(List.of("jdbc_attached_pg"), HiddenAttachments.attachments(plan));
        assertEquals(List.of(), HiddenAttachments.attachments(DuckDbSetupPlan.of(List.of(), List.of())));
    }

    /**
     * Tells whether any metadata query lists a name of the attached database that no server
     * view uses.
     *
     * @param s statement of the connection
     * @return true when a name is listed
     * @throws Exception on failure
     */
    private static boolean leaks(Statement s) throws Exception {
        for (String sql : METADATA) {
            for (String row : rows(s, sql)) {
                String lower = row.toLowerCase(java.util.Locale.ROOT);
                if (lower.contains("unconfigured") || lower.contains("secret_col")) {
                    return true;
                }
            }
        }
        return false;
    }

    private static List<String> rows(Statement s, String sql) throws Exception {
        List<String> rows = new ArrayList<>();
        try (ResultSet rs = s.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    row.append(i > 1 ? "|" : "").append(rs.getString(i));
                }
                rows.add(row.toString());
            }
        }
        return rows;
    }

    private static String serialized(Connection conn, String sql) throws Exception {
        try (java.sql.PreparedStatement ps = conn.prepareStatement("SELECT json_serialize_sql(?::VARCHAR)")) {
            ps.setString(1, sql);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        }
    }
}
