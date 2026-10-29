package pro.surpin.data.arrowflight.plugin.jdbc;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.duckdb.DuckDbSetup;
import pro.surpin.data.arrowflight.spi.duckdb.SecretValue;

/**
 * Tables of PostgreSQL and MySQL databases, read by DuckDB's postgres / mysql scanner
 * extensions through a read-only attachment per source. Every configured table is a server
 * table; DuckDB pushes projections and filters down to the remote database. The scanners'
 * functions are refused in client queries (they run any SQL on the remote database or connect
 * anywhere), by this plugin's own list and by the server's check of what the extension adds.
 * Each query of such a table is answered complete by one node.
 */
final class JdbcCatalog implements Catalog {

    /** Functions postgres_scanner v1.5.6 adds (C2 step 1 probe). */
    static final List<String> POSTGRES_FUNCTIONS = List.of("pg_clear_cache", "postgres_attach",
            "postgres_configure_pool", "postgres_execute", "postgres_hstore_get",
            "postgres_hstore_to_json", "postgres_query", "postgres_scan", "postgres_scan_pushdown",
            "read_postgres_binary");
    /** Functions mysql_scanner v1.5.6 adds (C2 step 1 probe). */
    static final List<String> MYSQL_FUNCTIONS = List.of("mysql_clear_cache",
            "mysql_debug_execution_plan", "mysql_execute", "mysql_explain_federated", "mysql_query");

    private final JdbcSettings settings;
    /** Server schema name to its source. */
    private final Map<String, JdbcSettings.Source> bySchema = new LinkedHashMap<>();

    /**
     * Creates the catalog.
     *
     * @param settings settings
     */
    JdbcCatalog(JdbcSettings settings) {
        this.settings = settings;
        for (JdbcSettings.Source source : settings.sources()) {
            bySchema.put(source.schema(), source);
        }
    }

    @Override
    public List<String> schemas() {
        return new ArrayList<>(bySchema.keySet());
    }

    @Override
    public List<String> tables(String schema) {
        JdbcSettings.Source source = bySchema.get(schema);
        return source == null ? List.of() : new ArrayList<>(source.tables().keySet());
    }

    @Override
    public Optional<TableSnapshot> current(TableName table) {
        JdbcSettings.Source source = bySchema.get(table.schema());
        if (source == null) {
            return Optional.empty();
        }
        JdbcSettings.RemoteTable remote = source.tables().get(table.table());
        if (remote == null) {
            return Optional.empty();
        }
        // The rows change remotely at any time: one snapshot ID, the answer is whatever the
        // remote database returns when the query runs.
        String fingerprint = "jdbc:" + source.name() + ":" + source.type() + ":" + remote.schema()
                + "." + remote.table();
        return Optional.of(new TableSnapshot(table, "0", fingerprint, List.of(), List.of(), List.of(),
                new ReadSpec.AttachedTable(source.attachment(), remote.schema(), remote.table())));
    }

    @Override
    public List<String> readableRoots() {
        return List.of();
    }

    @Override
    public boolean namesAreStatic() {
        return true;
    }

    @Override
    public void contributeDuckDb(DuckDbSetup setup) {
        Set<String> types = new LinkedHashSet<>();
        for (JdbcSettings.Source source : settings.sources()) {
            types.add(source.type());
        }
        for (String type : types) {
            if ("postgres".equals(type)) {
                setup.loadExtensionRefusingFunctions("postgres_scanner");
                setup.refuseFunctions(POSTGRES_FUNCTIONS.toArray(new String[0]));
            } else {
                setup.loadExtensionRefusingFunctions("mysql_scanner");
                setup.refuseFunctions(MYSQL_FUNCTIONS.toArray(new String[0]));
            }
        }
        for (JdbcSettings.Source source : settings.sources()) {
            setup.attachDatabase(source.attachment(), source.type(), options(source));
        }
    }

    /**
     * Writes the connection options of a source.
     *
     * @param source source
     * @return options in order, the password sensitive
     */
    static Map<String, SecretValue> options(JdbcSettings.Source source) {
        Map<String, SecretValue> options = new LinkedHashMap<>();
        options.put("HOST", SecretValue.plain(source.host()));
        if (source.port() != null) {
            options.put("PORT", SecretValue.plain(Integer.toString(source.port())));
        }
        options.put("DATABASE", SecretValue.plain(source.database()));
        options.put("USER", SecretValue.plain(source.user()));
        if (source.password() != null) {
            options.put("PASSWORD", SecretValue.sensitive(source.password()));
        }
        if (source.sslMode() != null) {
            options.put("SSL_MODE", SecretValue.plain(source.sslMode()));
        }
        return options;
    }

    @Override
    public String toString() {
        return "JdbcCatalog" + settings.sources();
    }
}
