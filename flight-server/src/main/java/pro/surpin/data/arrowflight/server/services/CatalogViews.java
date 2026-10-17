package pro.surpin.data.arrowflight.server.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.storage.DataFile;
import pro.surpin.data.arrowflight.server.storage.DataStore;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Describes every table as a DuckDB view over all of its Parquet files, so that a client's SQL can
 * run unchanged in DuckDB and return the complete answer. Unsigned integer columns can be widened
 * to the next signed type, because JDBC and ODBC have no unsigned types. A table whose name is
 * used by one schema only can also be named without its schema ({@code FROM lineitem}), as tools
 * that know nothing of schemas write. The views are rebuilt when a table's files change; a table's
 * column types are read once per file set.
 */
public final class CatalogViews {

    private static final Logger LOGGER = LoggerFactory.getLogger(CatalogViews.class);
    private static final long REFRESH_NANOS = TimeUnit.SECONDS.toNanos(60);
    /** Schema names DuckDB uses itself; tables in such schemas are not exposed. */
    private static final Set<String> RESERVED_SCHEMAS = Set.of(
            "main", "information_schema", "pg_catalog", "temp", "system");

    /**
     * Statements that create the views, in two variants.
     *
     * @param version identifies the statements; changes when any table changes
     * @param dataStatements views over the Parquet files
     * @param emptyStatements views with the same columns and no rows, for describing queries
     * @param dropStatements statements that remove everything the other statements create
     */
    public record Snapshot(long version, List<String> dataStatements,
            List<String> emptyStatements, List<String> dropStatements) {
    }

    /**
     * View definitions of one table for one set of files.
     *
     * @param fingerprint file paths and lengths the definitions were made for
     * @param data view over the files
     * @param empty view with the same columns and no rows
     */
    private record TableViews(String fingerprint, String data, String empty) {
    }

    private final ParquetAdapter parquetAdapter;
    private final DuckDbAdapter duckDbAdapter;
    private final boolean widenUnsigned;
    private final Map<String, TableViews> tables = new HashMap<>();
    private Snapshot snapshot;
    private long refreshedAtNanos;

    /**
     * Creates the catalog of views.
     *
     * @param parquetAdapter source of schemas, tables and their files
     * @param duckDbAdapter DuckDB used to read the column types
     * @param widenUnsigned whether unsigned integer columns are widened
     */
    public CatalogViews(ParquetAdapter parquetAdapter, DuckDbAdapter duckDbAdapter,
            boolean widenUnsigned) {
        this.parquetAdapter = parquetAdapter;
        this.duckDbAdapter = duckDbAdapter;
        this.widenUnsigned = widenUnsigned;
    }

    /**
     * Returns the current view definitions, refreshing them at most once a minute.
     *
     * @return view definitions
     * @throws IOException when the table files cannot be listed
     * @throws SQLException when DuckDB cannot read a table's column types
     */
    public synchronized Snapshot current() throws IOException, SQLException {
        long now = System.nanoTime();
        if (snapshot != null && now - refreshedAtNanos < REFRESH_NANOS) {
            return snapshot;
        }
        long t = LogUtil.mark();
        List<String> data = new ArrayList<>();
        List<String> empty = new ArrayList<>();
        List<String> drops = new ArrayList<>();
        Map<String, TableViews> seen = new HashMap<>();
        // Table name in lower case (DuckDB ignores case) -> schemas that have it.
        Map<String, List<String>> owners = new TreeMap<>();
        Map<String, String> spelling = new HashMap<>();
        for (String schema : parquetAdapter.getSchemas(null).keySet().stream().sorted().toList()) {
            if (RESERVED_SCHEMAS.contains(schema.toLowerCase(Locale.ROOT))) {
                LOGGER.warn("node={} views=skipSchema schema={} reason=reservedName",
                        LogUtil.node(), schema);
                continue;
            }
            String createSchema = "CREATE SCHEMA IF NOT EXISTS " + DuckDbAdapter.quoteIdentifier(schema);
            boolean schemaAdded = false;
            for (String table : parquetAdapter.getTables(schema, null).keySet().stream().sorted().toList()) {
                List<DataFile> files = parquetAdapter.tableFiles(schema, table);
                if (files.isEmpty()) {
                    continue;
                }
                String key = schema + "\u0000" + table;
                String fingerprint = ParquetAdapter.filesFingerprint(files);
                TableViews views = tables.get(key);
                if (views == null || !views.fingerprint().equals(fingerprint)) {
                    views = describe(schema, table, files, fingerprint);
                }
                seen.put(key, views);
                if (!schemaAdded) {
                    data.add(createSchema);
                    empty.add(createSchema);
                    drops.add("DROP SCHEMA IF EXISTS " + DuckDbAdapter.quoteIdentifier(schema) + " CASCADE");
                    schemaAdded = true;
                }
                data.add(views.data());
                empty.add(views.empty());
                String lower = table.toLowerCase(Locale.ROOT);
                owners.computeIfAbsent(lower, k -> new ArrayList<>()).add(schema);
                spelling.put(lower, table);
            }
        }
        for (Map.Entry<String, List<String>> owner : owners.entrySet()) {
            if (owner.getValue().size() != 1) {
                continue;   // ambiguous: the schema has to be named
            }
            String table = DuckDbAdapter.quoteIdentifier(spelling.get(owner.getKey()));
            String alias = "CREATE OR REPLACE VIEW main." + table + " AS SELECT * FROM "
                    + DuckDbAdapter.quoteIdentifier(owner.getValue().get(0)) + "." + table;
            data.add(alias);
            empty.add(alias);
            drops.add(0, "DROP VIEW IF EXISTS main." + table);   // before the schemas they read
        }
        tables.clear();
        tables.putAll(seen);
        long version = (data.hashCode() * 31L) + empty.hashCode();
        if (snapshot == null || snapshot.version() != version) {
            snapshot = new Snapshot(version, List.copyOf(data), List.copyOf(empty),
                    List.copyOf(drops));
            LOGGER.info("node={} views=refreshed tables={} version={}",
                    LogUtil.node(), seen.size(), Long.toHexString(version));
        }
        refreshedAtNanos = now;
        LogUtil.logTiming(t, "views.refresh", "tables=" + seen.size());
        return snapshot;
    }

    /**
     * Builds the view definitions of one table from the column types of its first file.
     *
     * @param schema schema name
     * @param table table name
     * @param files the table's Parquet files
     * @param fingerprint file paths and lengths
     * @return view definitions
     * @throws SQLException when DuckDB cannot read the file
     */
    private TableViews describe(String schema, String table, List<DataFile> files,
            String fingerprint) throws SQLException {
        DataStore store = parquetAdapter.dataStore();
        List<String> paths = new ArrayList<>(files.size());
        for (DataFile file : files) {
            paths.add(store.duckDbUri(file));
        }
        String source = DuckDbAdapter.readParquetFromClause(paths);
        List<String> select = new ArrayList<>();
        List<String> typed = new ArrayList<>();
        boolean widened = false;
        Connection conn = duckDbAdapter.connection();
        try (Statement s = conn.createStatement();
                ResultSet columns = s.executeQuery("DESCRIBE SELECT * FROM "
                        + DuckDbAdapter.readParquetFromClause(paths.subList(0, 1)))) {
            while (columns.next()) {
                String name = DuckDbAdapter.quoteIdentifier(columns.getString("column_name"));
                String type = columns.getString("column_type");
                String wider = widenUnsigned ? widerSignedType(type) : null;
                if (wider != null) {
                    select.add("CAST(" + name + " AS " + wider + ") AS " + name);
                    widened = true;
                    type = wider;
                } else {
                    select.add(name);
                }
                typed.add("CAST(NULL AS " + type + ") AS " + name);
            }
        }
        String view = DuckDbAdapter.quoteIdentifier(schema) + "." + DuckDbAdapter.quoteIdentifier(table);
        String data = "CREATE OR REPLACE VIEW " + view + " AS SELECT "
                + (widened ? String.join(", ", select) : "*") + " FROM " + source;
        String empty = "CREATE OR REPLACE VIEW " + view + " AS SELECT "
                + String.join(", ", typed) + " WHERE false";
        return new TableViews(fingerprint, data, empty);
    }

    /**
     * Names the signed type that holds every value of an unsigned integer type.
     *
     * @param duckDbType DuckDB column type name
     * @return wider signed type, or null when the type is not an unsigned integer
     */
    static String widerSignedType(String duckDbType) {
        return switch (duckDbType.toUpperCase(Locale.ROOT)) {
            case "UTINYINT" -> "SMALLINT";
            case "USMALLINT" -> "INTEGER";
            case "UINTEGER" -> "BIGINT";
            case "UBIGINT" -> "DECIMAL(20,0)";
            default -> null;
        };
    }
}
