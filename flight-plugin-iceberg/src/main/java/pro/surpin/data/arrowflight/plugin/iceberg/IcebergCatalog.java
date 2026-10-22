package pro.surpin.data.arrowflight.plugin.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;

import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.duckdb.DuckDbSetup;

/**
 * An Iceberg catalog as the server sees it: namespaces are schemas (with an optional prefix),
 * tables are Iceberg tables, a table's snapshot is its current Iceberg snapshot with the data
 * files and delete files that apply. Snapshot IDs are {@code <snapshot id>.<schema id>}, so that
 * an executing node reads the same files with the same columns as the planning node. DuckDB reads
 * complete answers with {@code iceberg_scan} of the metadata file.
 */
final class IcebergCatalog implements Catalog {

    private final IcebergSettings settings;
    private final TableSource source;
    private final ExecutorService planPool;
    private volatile Names names;
    private final Map<String, Cached<List<String>>> tableLists = new ConcurrentHashMap<>();
    private final Map<TableName, Cached<Optional<TableSource.Loaded>>> loaded = new ConcurrentHashMap<>();
    private final SnapshotPlanner planner;

    /**
     * A value with its refresh deadline.
     *
     * @param value value
     * @param expiresAtMillis deadline
     * @param <T> value type
     */
    private record Cached<T>(T value, long expiresAtMillis) {
    }

    /**
     * Server schema names to namespaces.
     *
     * @param schemas schema name to namespace
     * @param expiresAtMillis refresh deadline
     */
    private record Names(Map<String, String> schemas, long expiresAtMillis) {
    }

    /**
     * Creates the catalog.
     *
     * @param name provider name, for logs
     * @param settings settings
     * @param source namespaces, tables and metadata
     * @param io file IO over the server's stores
     * @param planPool threads reading manifests
     */
    IcebergCatalog(String name, IcebergSettings settings, TableSource source, StoresFileIO io,
            ExecutorService planPool) {
        this.settings = settings;
        this.source = source;
        this.planPool = planPool;
        this.planner = new SnapshotPlanner(name, io, planPool);
    }

    @Override
    public List<String> schemas() throws IOException {
        return new ArrayList<>(names().schemas().keySet());
    }

    @Override
    public List<String> tables(String schema) throws IOException {
        String namespace = names().schemas().get(schema);
        if (namespace == null) {
            return List.of();
        }
        long now = System.currentTimeMillis();
        Cached<List<String>> cached = tableLists.get(namespace);
        if (cached == null || cached.expiresAtMillis() <= now) {
            List<String> tables = new ArrayList<>(source.tables(namespace));
            Collections.sort(tables);
            cached = new Cached<>(List.copyOf(tables), now + settings.namesTtlMillis());
            tableLists.put(namespace, cached);
        }
        return cached.value();
    }

    @Override
    public Optional<TableSnapshot> current(TableName table) throws IOException {
        Optional<TableSource.Loaded> table0 = load(table, false);
        if (table0.isEmpty()) {
            return Optional.empty();
        }
        TableMetadata metadata = table0.get().metadata();
        Snapshot snapshot = metadata.currentSnapshot();
        return Optional.of(planner.plan(table, table0.get().metadataLocation(), metadata, snapshot,
                metadata.schema()).snapshot());
    }

    @Override
    public Optional<TableSnapshot> pinned(TableName table, String snapshotId) throws IOException {
        Optional<SnapshotPlanner.PlanId> parsed = SnapshotPlanner.PlanId.parse(snapshotId);
        if (parsed.isEmpty()) {
            return Optional.empty();
        }
        int schemaId = parsed.get().schemaId();
        Long id = parsed.get().snapshotId();
        for (boolean fresh : new boolean[] {false, true}) {
            Optional<TableSource.Loaded> table0 = load(table, fresh);
            if (table0.isEmpty()) {
                return Optional.empty();
            }
            TableMetadata metadata = table0.get().metadata();
            Schema schema = metadata.schemasById().get(schemaId);
            Snapshot snapshot = id == null ? null : metadata.snapshot(id);
            if (schema != null && (id == null || snapshot != null)) {
                return Optional.of(planner.plan(table, table0.get().metadataLocation(), metadata,
                        snapshot, schema).snapshot());
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<String> partialReadProblem(TableSnapshot snapshot) {
        return planner.partialReadProblem(snapshot);
    }

    @Override
    public List<String> readableRoots() {
        return settings.roots();
    }

    @Override
    public void contributeDuckDb(DuckDbSetup setup) {
        // iceberg needs avro loaded first.
        setup.loadExtension("avro");
        setup.loadExtension("iceberg");
    }

    @Override
    public void close() throws IOException {
        planPool.shutdownNow();
        source.close();
    }

    /**
     * Returns the schema names, refreshing them after the names TTL.
     *
     * @return names
     * @throws IOException when the catalog cannot be read
     */
    private Names names() throws IOException {
        Names current = names;
        long now = System.currentTimeMillis();
        if (current != null && current.expiresAtMillis() > now) {
            return current;
        }
        synchronized (this) {
            current = names;
            if (current != null && current.expiresAtMillis() > now) {
                return current;
            }
            Map<String, String> schemas = new TreeMap<>();
            for (String namespace : source.namespaces()) {
                if (settings.namespaces().isEmpty() || settings.namespaces().contains(namespace)) {
                    schemas.put(settings.schemaPrefix() + namespace, namespace);
                }
            }
            names = new Names(Collections.unmodifiableMap(schemas), now + settings.namesTtlMillis());
            return names;
        }
    }

    /**
     * Loads a table's current metadata, reusing it within the metadata TTL.
     *
     * @param table table name
     * @param fresh whether to ignore the cached metadata
     * @return metadata, empty when the table does not exist
     * @throws IOException when the catalog cannot be read
     */
    private Optional<TableSource.Loaded> load(TableName table, boolean fresh) throws IOException {
        String namespace = names().schemas().get(table.schema());
        if (namespace == null) {
            return Optional.empty();
        }
        long now = System.currentTimeMillis();
        Cached<Optional<TableSource.Loaded>> cached = loaded.get(table);
        if (fresh || cached == null || cached.expiresAtMillis() <= now) {
            try {
                cached = new Cached<>(source.load(namespace, table.table()),
                        now + settings.metadataTtlMillis());
            } catch (UncheckedIOException e) {
                throw e.getCause();
            } catch (RuntimeException e) {
                throw new IOException("Cannot read Iceberg table " + table + ": " + e.getMessage(), e);
            }
            loaded.put(table, cached);
        }
        return cached.value();
    }
}
