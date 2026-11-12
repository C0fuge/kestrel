package pro.surpin.data.arrowflight.server.duckdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

/**
 * Databases plugins attach (item 47, Q8): client metadata queries must not list their schemas,
 * tables and columns, only the server's views of the configured tables. Every DuckDB connection
 * gets temporary table macros named like DuckDB's metadata functions, which leave out the rows of
 * these databases; DuckDB's own views ({@code information_schema}, {@code pg_catalog},
 * {@code SHOW ALL TABLES}) call the functions unqualified and so read the macros. A client query
 * may not call one of these functions qualified (for example {@code system.main.duckdb_tables()}),
 * which would reach DuckDB's function past the macro. Process-wide and add-only.
 */
public final class HiddenAttachments {

    /**
     * DuckDB's metadata table functions that list names of attached databases, each with a
     * {@code database_name} column. {@code duckdb_functions()} stays: the server's setup reads it,
     * and an attached database of a scanner extension holds no functions.
     */
    public static final List<String> FUNCTIONS = List.of("duckdb_databases", "duckdb_schemas",
            "duckdb_tables", "duckdb_columns", "duckdb_views", "duckdb_constraints",
            "duckdb_indexes", "duckdb_sequences", "duckdb_types", "pragma_database_size");

    /**
     * DuckDB's table functions that take a table or database name as a string, which the parse
     * check's qualifier refusal never sees: refused in client queries while a database is hidden
     * ({@code information_schema.columns} lists the configured tables' columns instead).
     */
    public static final List<String> NAME_ARGUMENT_FUNCTIONS = List.of("pragma_table_info",
            "pragma_storage_info", "pragma_show", "pragma_metadata_info");

    private static final Set<String> NAMES = new TreeSet<>();

    /** Not instantiable. */
    private HiddenAttachments() {
    }

    /**
     * Hides more attached databases.
     *
     * @param names attachment names, any case
     */
    public static synchronized void addAll(List<String> names) {
        for (String name : names) {
            NAMES.add(name.toLowerCase(Locale.ROOT));
        }
    }

    /**
     * Tells whether any attached database is hidden.
     *
     * @return true when a plugin attached a database
     */
    public static synchronized boolean any() {
        return !NAMES.isEmpty();
    }

    /**
     * Lists the attachment names of a setup plan: its declared secrets without a scope.
     *
     * @param plan setup plan
     * @return attachment names, empty when the plan attaches none or its secrets are unknown
     */
    public static List<String> attachments(DuckDbSetupPlan plan) {
        List<String> names = new ArrayList<>();
        if (plan.secrets() != null) {
            for (DuckDbSetupPlan.DeclaredSecret secret : plan.secrets()) {
                if (secret.scope() == null) {
                    names.add(secret.name());
                }
            }
        }
        return names;
    }

    /**
     * Writes the statements that create the hiding macros on a connection.
     *
     * @return one CREATE OR REPLACE TEMP MACRO per function, empty when nothing is hidden
     */
    public static synchronized List<String> macroStatements() {
        if (NAMES.isEmpty()) {
            return List.of();
        }
        StringBuilder list = new StringBuilder();
        for (String name : NAMES) {
            if (list.length() > 0) {
                list.append(", ");
            }
            list.append('\'').append(name.replace("'", "''")).append('\'');
        }
        List<String> statements = new ArrayList<>(FUNCTIONS.size());
        for (String function : FUNCTIONS) {
            statements.add("CREATE OR REPLACE TEMP MACRO " + function + "() AS TABLE SELECT * FROM system.main."
                    + function + "() WHERE lower(database_name) NOT IN (" + list + ")");
        }
        return statements;
    }

    /**
     * Refuses a client statement that calls a hidden metadata function with a qualifier, which
     * would bypass the connection's macro, or a function of {@link #NAME_ARGUMENT_FUNCTIONS},
     * while a database is hidden.
     *
     * @param serialized DuckDB's JSON form of the statement
     * @param names lower-case function and table names it uses
     * @throws IllegalArgumentException when such a call is found
     */
    public static void requireUnqualifiedCalls(String serialized, List<String> names) {
        if (!any()) {
            return;
        }
        for (String function : NAME_ARGUMENT_FUNCTIONS) {
            if (names.contains(function)) {
                throw new IllegalArgumentException("Function " + function
                        + " is not allowed while a database is attached: use information_schema.columns");
            }
        }
        String lower = null;
        for (String function : FUNCTIONS) {
            if (!names.contains(function)) {
                continue;
            }
            if (lower == null) {
                lower = serialized.toLowerCase(Locale.ROOT);
            }
            String call = "\"function_name\":\"" + function + "\",\"schema\":\"";
            int at = lower.indexOf(call);
            while (at >= 0) {
                int value = at + call.length();
                if (value < lower.length() && lower.charAt(value) != '"') {
                    throw new IllegalArgumentException("Function " + function
                            + " may not be qualified: call it as " + function + "()");
                }
                at = lower.indexOf(call, value);
            }
        }
    }
}
