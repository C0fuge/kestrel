package pro.surpin.data.arrowflight.server.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Names every table a query reads from DuckDB's own parse of the text ({@code json_serialize_sql}),
 * subqueries, CTEs and set operations included, for access checks of queries the text-based
 * finders cannot name completely. The parse runs in a small in-memory DuckDB of its own that reads
 * no files and runs nothing. A name that refers to a CTE in scope is no table; any other name
 * without a schema is returned without one (so a check refuses it). Table functions, catalog
 * qualified names and other table sources a check cannot name are refused.
 */
public final class QueryTables {

    /** Serializes one SQL text to DuckDB's JSON parse tree. */
    private static final String SERIALIZE_SQL = "SELECT json_serialize_sql(?::VARCHAR)";
    /** Table sources that read no table themselves (their parts are walked). */
    private static final Set<String> CONTAINERS = Set.of("SUBQUERY", "JOIN", "EXPRESSION_LIST", "EMPTY", "PIVOT");
    /** Table sources a check cannot name: they read files or describe tables. */
    private static final Set<String> REFUSED_SOURCES = Set.of("TABLE_FUNCTION", "SHOW_REF", "COLUMN_DATA",
            "DELIM_GET", "BOUND_TABLE_REF", "CTE");
    /** Most query texts whose tables are kept. */
    private static final int CACHE_CAPACITY = 256;
    /** JSON reader of the parse tree. */
    private static final ObjectMapper JSON = new ObjectMapper();
    /** Tables per query text, most recently used last; guarded by the class lock. */
    private static final Map<String, List<ParquetQueryParser.JoinTable>> CACHE =
            new LinkedHashMap<>(16, 0.75f, true);
    /** Parse connection, opened on first use; guarded by the class lock. */
    private static Connection connection;
    /** Serialize statement of {@link #connection}; guarded by the class lock. */
    private static PreparedStatement serialize;

    private QueryTables() {
    }

    /**
     * Names every table a query reads.
     *
     * @param sql SQL text without time travel clauses
     * @return the tables in parse order, each once; schema null for a name without one
     * @throws IllegalArgumentException when the text is not one parsable statement or reads a
     *         source that cannot be named (a table function, a catalog-qualified name)
     */
    public static synchronized List<ParquetQueryParser.JoinTable> of(String sql) {
        List<ParquetQueryParser.JoinTable> cached = CACHE.get(sql);
        if (cached != null) {
            return cached;
        }
        JsonNode root = parse(sql);
        if (root.path("error").asBoolean(false)) {
            throw new IllegalArgumentException(root.path("error_type").asText("parser") + " error: "
                    + root.path("error_message").asText(""));
        }
        JsonNode statements = root.path("statements");
        if (!statements.isArray() || statements.size() != 1) {
            throw new IllegalArgumentException("Only one query is allowed when table access is checked");
        }
        List<ParquetQueryParser.JoinTable> tables = new ArrayList<>();
        walk(statements.get(0), "", "", Set.of(), tables);
        List<ParquetQueryParser.JoinTable> result = List.copyOf(tables);
        if (QueryCacheLimits.cacheable(sql)) {
            CACHE.put(sql, result);
            if (CACHE.size() > CACHE_CAPACITY) {
                CACHE.remove(CACHE.keySet().iterator().next());
            }
        }
        return result;
    }

    /**
     * Parses a text with the parse connection, reopening it once when it fails.
     *
     * @param sql SQL text
     * @return DuckDB's JSON form of the text
     * @throws IllegalStateException when the parse connection cannot be used
     */
    private static synchronized JsonNode parse(String sql) {
        try {
            return JSON.readTree(serialized(sql));
        } catch (SQLException first) {
            closeQuietly();
            try {
                return JSON.readTree(serialized(sql));
            } catch (SQLException | java.io.IOException e) {
                closeQuietly();
                throw new IllegalStateException("Could not parse the query to check its tables", e);
            }
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Could not read the parse of the query", e);
        }
    }

    /**
     * Serializes a text with the parse connection, opening it when needed.
     *
     * @param sql SQL text
     * @return the JSON text
     * @throws SQLException when DuckDB fails
     */
    private static synchronized String serialized(String sql) throws SQLException {
        if (serialize == null) {
            Properties settings = new Properties();
            settings.setProperty("threads", "1");
            settings.setProperty("memory_limit", "64MB");
            settings.setProperty("autoload_known_extensions", "false");
            settings.setProperty("autoinstall_known_extensions", "false");
            settings.setProperty("enable_external_access", "false");
            settings.setProperty("lock_configuration", "true");
            connection = DriverManager.getConnection("jdbc:duckdb:", settings);
            serialize = connection.prepareStatement(SERIALIZE_SQL);
        }
        serialize.setString(1, sql);
        try (ResultSet rs = serialize.executeQuery()) {
            rs.next();
            return rs.getString(1);
        }
    }

    /** Closes the parse connection, ignoring failures; the next parse opens a new one. */
    private static synchronized void closeQuietly() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException ignored) {
            // A broken connection is replaced either way.
        }
        connection = null;
        serialize = null;
    }

    /**
     * Collects the tables of one part of a parse tree.
     *
     * @param node part of the tree
     * @param key field name the part is held under
     * @param parentType type of the object holding the part, empty when none
     * @param ctes names of the CTEs in scope, lower case
     * @param out tables found so far
     * @throws IllegalArgumentException when the part reads a source that cannot be named
     */
    private static void walk(JsonNode node, String key, String parentType, Set<String> ctes,
            List<ParquetQueryParser.JoinTable> out) {
        if (node.isArray()) {
            for (JsonNode item : node) {
                walk(item, key, parentType, ctes, out);
            }
            return;
        }
        if (!node.isObject()) {
            return;
        }
        String type = node.path("type").asText("");
        boolean source = "from_table".equals(key) || "source".equals(key) && "PIVOT".equals(parentType)
                || ("left".equals(key) || "right".equals(key)) && "JOIN".equals(parentType);
        if (REFUSED_SOURCES.contains(type) || source && !"BASE_TABLE".equals(type) && !CONTAINERS.contains(type)) {
            throw new IllegalArgumentException("Table source " + type
                    + " is not supported when table access is checked");
        }
        if ("BASE_TABLE".equals(type)) {
            addTable(node, ctes, out);
            return;
        }
        Set<String> scope = ctes;
        JsonNode cteMap = node.path("cte_map").path("map");
        if (cteMap.isArray() && !cteMap.isEmpty()) {
            // A CTE sees itself and the CTEs before it; the query body sees them all.
            Set<String> visible = new HashSet<>(ctes);
            for (JsonNode entry : cteMap) {
                visible.add(entry.path("key").asText("").toLowerCase(Locale.ROOT));
                walk(entry.path("value"), "value", type, Set.copyOf(visible), out);
            }
            scope = Set.copyOf(visible);
        }
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            if (!"cte_map".equals(field.getKey())) {
                walk(field.getValue(), field.getKey(), type, scope, out);
            }
        }
    }

    /**
     * Adds the table of a BASE_TABLE source unless it names a CTE in scope.
     *
     * @param node the source
     * @param ctes names of the CTEs in scope, lower case
     * @param out tables found so far
     * @throws IllegalArgumentException when the name has a catalog
     */
    private static void addTable(JsonNode node, Set<String> ctes, List<ParquetQueryParser.JoinTable> out) {
        String catalog = node.path("catalog_name").asText("");
        String schema = node.path("schema_name").asText("");
        String table = node.path("table_name").asText("");
        if (!catalog.isEmpty()) {
            throw new IllegalArgumentException("Table names with a catalog (" + catalog
                    + ") are not supported when table access is checked");
        }
        if (schema.isEmpty() && ctes.contains(table.toLowerCase(Locale.ROOT))) {
            return;
        }
        ParquetQueryParser.JoinTable named = new ParquetQueryParser.JoinTable(
                schema.isEmpty() ? null : schema, table, table);
        if (!out.contains(named)) {
            out.add(named);
        }
    }
}
