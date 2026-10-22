package pro.surpin.data.arrowflight.server.catalog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.DeleteFile;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;

/**
 * The tables of the configured catalogs besides the data directory (Iceberg and others from
 * plugins), as the server shows and reads them. A catalog's schemas appear next to the data
 * directory's; a name the data directory or an earlier catalog has already is skipped. A table
 * whose metadata or files lie outside its catalog's readable roots is hidden. Without catalogs
 * ({@link #none()}) nothing here is ever called on a query path.
 */
public final class CatalogTables implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CatalogTables.class);
    /** Schema and table names the server accepts (as ParquetAdapter.validateName). */
    private static final Pattern NAME = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    /**
     * One open catalog.
     *
     * @param catalog catalog, calls wrapped in its plugin's class loader
     * @param roots readable roots, normalised
     * @param shared whether every node reads the roots (not a node's local disk)
     */
    public record Entry(SpiCatalog catalog, List<String> roots, boolean shared) {

        /**
         * Copies the roots.
         *
         * @param catalog catalog
         * @param roots roots
         * @param shared whether the roots are shared
         */
        public Entry {
            roots = List.copyOf(roots);
        }

        /**
         * Names the catalog's provider.
         *
         * @return provider name
         */
        public String provider() {
            return catalog.provider();
        }
    }

    /**
     * A table's snapshot with its catalog.
     *
     * @param entry catalog
     * @param snapshot snapshot
     */
    public record Resolved(Entry entry, TableSnapshot snapshot) {
    }

    private final List<Entry> catalogs;
    private final Map<Entry, List<String>> lastSchemas = new ConcurrentHashMap<>();
    private volatile Map<String, Entry> owners = Map.of();
    private volatile Set<String> taken = Set.of();
    private final Set<String> warned = ConcurrentHashMap.newKeySet();

    /**
     * Creates the tables of some catalogs.
     *
     * @param catalogs open catalogs, in configuration order
     */
    public CatalogTables(List<Entry> catalogs) {
        this.catalogs = List.copyOf(catalogs);
    }

    /**
     * Returns the tables of no catalog.
     *
     * @return empty catalog tables
     */
    public static CatalogTables none() {
        return new CatalogTables(List.of());
    }

    /**
     * Tells whether no catalog is configured.
     *
     * @return true without catalogs
     */
    public boolean isEmpty() {
        return catalogs.isEmpty();
    }

    /**
     * Lists the open catalogs.
     *
     * @return catalogs
     */
    public List<Entry> catalogs() {
        return catalogs;
    }

    /**
     * Lists the schemas of every catalog; a catalog that cannot be read keeps its last list.
     *
     * @param directorySchemas schema names the data directory has (they win)
     * @return schema name to a location text ({@code catalog:<provider>}), in catalog order
     */
    public Map<String, String> schemas(Set<String> directorySchemas) {
        if (catalogs.isEmpty()) {
            return Map.of();
        }
        Set<String> takenLower = new TreeSet<>();
        for (String schema : directorySchemas) {
            takenLower.add(schema.toLowerCase(Locale.ROOT));
        }
        Map<String, String> result = new LinkedHashMap<>();
        Map<String, Entry> newOwners = new LinkedHashMap<>();
        for (Entry entry : catalogs) {
            List<String> names;
            try {
                names = entry.catalog().schemas();
                lastSchemas.put(entry, names);
            } catch (IOException | RuntimeException e) {
                names = lastSchemas.getOrDefault(entry, List.of());
                LOGGER.warn("node={} catalog=schemasFailed provider={} kept={} error='{}'",
                        LogUtil.node(), entry.provider(), names.size(), e.getMessage());
            }
            for (String name : names) {
                if (!NAME.matcher(name).matches()) {
                    warnOnce("name:" + name, "node={} catalog=schemaSkipped provider={} schema='{}'"
                            + " reason=name", entry.provider(), name);
                    continue;
                }
                if (!takenLower.add(name.toLowerCase(Locale.ROOT))) {
                    warnOnce("taken:" + name, "node={} catalog=schemaSkipped provider={} schema={}"
                            + " reason=nameInUse", entry.provider(), name);
                    continue;
                }
                result.put(name, "catalog:" + entry.provider());
                newOwners.put(name, entry);
            }
        }
        owners = Map.copyOf(newOwners);
        taken = Set.copyOf(directorySchemas);
        return result;
    }

    /**
     * Finds the catalog that shows a schema, by exact name, else ignoring case when only one
     * schema matches.
     *
     * @param schema schema name, may be null
     * @return catalog, empty when a catalog does not show the schema
     */
    public Optional<Entry> owner(String schema) {
        if (schema == null || catalogs.isEmpty()) {
            return Optional.empty();
        }
        Map<String, Entry> current = owners;
        Entry exact = current.get(schema);
        if (exact != null) {
            return Optional.of(exact);
        }
        Entry match = null;
        for (Map.Entry<String, Entry> e : current.entrySet()) {
            if (e.getKey().equalsIgnoreCase(schema)) {
                if (match != null) {
                    return Optional.empty();
                }
                match = e.getValue();
            }
        }
        return Optional.ofNullable(match);
    }

    /**
     * Tells whether a catalog shows a schema.
     *
     * @param schema schema name, may be null
     * @return true for a catalog schema
     */
    public boolean owns(String schema) {
        return owner(schema).isPresent();
    }

    /**
     * Returns the schema name as the catalog spells it.
     *
     * @param schema schema name in any case
     * @return the catalog's spelling, or the name as given
     */
    public String spelling(String schema) {
        if (schema == null || owners.containsKey(schema)) {
            return schema;
        }
        for (String name : owners.keySet()) {
            if (name.equalsIgnoreCase(schema)) {
                return name;
            }
        }
        return schema;
    }

    /**
     * Lists the tables of a catalog schema.
     *
     * @param schema schema name
     * @return table names with valid names, sorted
     * @throws IOException when the catalog cannot be read
     */
    public List<String> tables(String schema) throws IOException {
        Optional<Entry> entry = owner(schema);
        if (entry.isEmpty()) {
            return List.of();
        }
        List<String> result = new ArrayList<>();
        for (String table : entry.get().catalog().tables(spelling(schema))) {
            if (NAME.matcher(table).matches()) {
                result.add(table);
            } else {
                warnOnce("table:" + schema + "." + table, "node={} catalog=tableSkipped provider={}"
                        + " table='{}' reason=name", entry.get().provider(), schema + "." + table);
            }
        }
        result.sort(String::compareTo);
        return result;
    }

    /**
     * Returns the current snapshot of a catalog table, checked against the catalog's roots.
     *
     * @param schema schema name
     * @param table table name (any case)
     * @return snapshot with its catalog, empty when the table does not exist or is hidden
     * @throws IOException when the catalog cannot be read
     */
    public Optional<Resolved> current(String schema, String table) throws IOException {
        Optional<Entry> entry = owner(schema);
        if (entry.isEmpty()) {
            return Optional.empty();
        }
        TableName name = tableName(entry.get(), schema, table);
        Optional<TableSnapshot> snapshot = entry.get().catalog().current(name);
        return snapshot.filter(s -> checked(entry.get(), s)).map(s -> new Resolved(entry.get(), s));
    }

    /**
     * Returns the snapshot a planning node used, for an executing node.
     *
     * @param schema schema name
     * @param table table name
     * @param snapshotId snapshot ID from the ticket
     * @return snapshot, empty when it is no longer available or hidden
     * @throws IOException when the catalog cannot be read
     */
    public Optional<Resolved> pinned(String schema, String table, String snapshotId)
            throws IOException {
        Optional<Entry> entry = owner(schema);
        if (entry.isEmpty()) {
            return Optional.empty();
        }
        TableName name = tableName(entry.get(), schema, table);
        Optional<TableSnapshot> snapshot = entry.get().catalog().pinned(name, snapshotId);
        return snapshot.filter(s -> checked(entry.get(), s)).map(s -> new Resolved(entry.get(), s));
    }

    /**
     * Tells why a snapshot must be answered completely instead of in parts.
     *
     * @param resolved snapshot with its catalog
     * @return reason, or null when its data files can be read one by one
     */
    public static String completeReason(Resolved resolved) {
        TableSnapshot snapshot = resolved.snapshot();
        if (snapshot.files().isEmpty()) {
            return "the snapshot of " + snapshot.name() + " has no data files";
        }
        int deletes = 0;
        for (TableFile file : snapshot.files()) {
            deletes += file.deletes().size();
        }
        if (deletes > 0) {
            return "snapshot " + snapshot.snapshotId() + " of " + snapshot.name() + " has "
                    + deletes + " delete file(s) for its data files";
        }
        if (snapshot.columns().isEmpty()) {
            return "the catalog declares no columns for " + snapshot.name();
        }
        for (ColumnSpec column : snapshot.columns()) {
            if (column.fieldId() == null) {
                return "column " + column.name() + " of " + snapshot.name() + " has no field ID";
            }
        }
        if (!(snapshot.read() instanceof ReadSpec.IcebergMetadata)) {
            return "the catalog's table " + snapshot.name() + " is read as a whole only";
        }
        Optional<String> problem = resolved.entry().catalog().partialReadProblem(snapshot);
        return problem.orElse(null);
    }

    @Override
    public void close() {
        for (Entry entry : catalogs) {
            try {
                entry.catalog().close();
            } catch (IOException | RuntimeException e) {
                LOGGER.warn("node={} catalog=closeFailed provider={} error={}", LogUtil.node(),
                        entry.provider(), e.toString());
            }
        }
    }

    /**
     * Builds a table name in the catalog's spelling of the schema.
     *
     * @param entry catalog
     * @param schema schema in any case
     * @param table table name
     * @return table name
     * @throws IOException when the table list cannot be read
     */
    private TableName tableName(Entry entry, String schema, String table) throws IOException {
        String spelled = spelling(schema);
        String tableSpelled = table;
        for (String name : entry.catalog().tables(spelled)) {
            if (name.equals(table)) {
                tableSpelled = name;
                break;
            }
            if (name.equalsIgnoreCase(table)) {
                tableSpelled = name;
            }
        }
        return new TableName(spelled, tableSpelled);
    }

    /**
     * Checks that a snapshot's metadata and files lie under the catalog's roots.
     *
     * @param entry catalog
     * @param snapshot snapshot
     * @return true when they do; otherwise the table is hidden (warned once per snapshot)
     */
    private boolean checked(Entry entry, TableSnapshot snapshot) {
        List<String> outside = new ArrayList<>();
        if (snapshot.read() instanceof ReadSpec.IcebergMetadata metadata
                && !CatalogLocations.underAny(metadata.metadataLocation(), entry.roots())) {
            outside.add(metadata.metadataLocation());
        }
        for (TableFile file : snapshot.files()) {
            if (!CatalogLocations.underAny(file.location(), entry.roots())) {
                outside.add(file.location());
            }
            for (DeleteFile delete : file.deletes()) {
                if (!CatalogLocations.underAny(delete.location(), entry.roots())) {
                    outside.add(delete.location());
                }
            }
            if (outside.size() > 3) {
                break;
            }
        }
        if (outside.isEmpty()) {
            return true;
        }
        warnOnce("outside:" + snapshot.fingerprint(), "node={} catalog=tableHidden provider={}"
                + " table={} reason=outsideRoots roots={} locations={}", entry.provider(),
                snapshot.name(), entry.roots(), outside);
        return false;
    }

    /**
     * Logs a warning once per key.
     *
     * @param key what the warning is about
     * @param format log format; its first argument is the node
     * @param args other arguments
     */
    private void warnOnce(String key, String format, Object... args) {
        if (warned.add(key)) {
            Object[] all = new Object[args.length + 1];
            all[0] = LogUtil.node();
            System.arraycopy(args, 0, all, 1, args.length);
            LOGGER.warn(format, all);
        }
    }

    /**
     * Returns the directory schema names given at the last listing, for tests.
     *
     * @return names
     */
    Set<String> taken() {
        return taken;
    }
}
