package pro.surpin.data.arrowflight.plugin.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.DeleteFile;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergCatalog.class);
    /** Snapshot ID of a table without a snapshot. */
    private static final String NO_SNAPSHOT = "none";
    /** Planned snapshots kept, the most recently used. */
    private static final int PLANNED_CAPACITY = 256;

    private final IcebergSettings settings;
    private final TableSource source;
    private final StoresFileIO io;
    private final ExecutorService planPool;
    private final String name;
    private volatile Names names;
    private final Map<String, Cached<List<String>>> tableLists = new ConcurrentHashMap<>();
    private final Map<TableName, Cached<Optional<TableSource.Loaded>>> loaded = new ConcurrentHashMap<>();
    private final Map<String, Planned> planned = Collections.synchronizedMap(
            new LinkedHashMap<>(64, 0.75f, true) {
                private static final long serialVersionUID = 1L;

                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Planned> eldest) {
                    return size() > PLANNED_CAPACITY;
                }
            });

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
     * A planned snapshot with what keeps its files from being read one by one.
     *
     * @param snapshot snapshot
     * @param problem reason, or null
     */
    private record Planned(TableSnapshot snapshot, String problem) {
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
        this.name = name;
        this.settings = settings;
        this.source = source;
        this.io = io;
        this.planPool = planPool;
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
        return Optional.of(plan(table, table0.get(), snapshot, metadata.schema()).snapshot());
    }

    @Override
    public Optional<TableSnapshot> pinned(TableName table, String snapshotId) throws IOException {
        int dot = snapshotId.lastIndexOf('.');
        if (dot <= 0) {
            return Optional.empty();
        }
        String snapshotPart = snapshotId.substring(0, dot);
        int schemaId;
        Long id;
        try {
            schemaId = Integer.parseInt(snapshotId.substring(dot + 1));
            id = NO_SNAPSHOT.equals(snapshotPart) ? null : Long.valueOf(snapshotPart);
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
        for (boolean fresh : new boolean[] {false, true}) {
            Optional<TableSource.Loaded> table0 = load(table, fresh);
            if (table0.isEmpty()) {
                return Optional.empty();
            }
            TableMetadata metadata = table0.get().metadata();
            Schema schema = metadata.schemasById().get(schemaId);
            Snapshot snapshot = id == null ? null : metadata.snapshot(id);
            if (schema != null && (id == null || snapshot != null)) {
                return Optional.of(plan(table, table0.get(), snapshot, schema).snapshot());
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<String> partialReadProblem(TableSnapshot snapshot) {
        Planned plan = planned.get(snapshot.fingerprint());
        if (plan == null) {
            // Never say "readable" for a snapshot this catalog did not plan (or forgot).
            return Optional.of("snapshot " + snapshot.snapshotId() + " is not planned by this catalog");
        }
        return Optional.ofNullable(plan.problem());
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

    /**
     * Plans a snapshot: its data files with their deletes, read with a schema.
     *
     * @param table table name
     * @param table0 metadata
     * @param snapshot snapshot, null for a table without one
     * @param schema schema the columns come from
     * @return planned snapshot, cached
     * @throws IOException when a manifest cannot be read
     */
    private Planned plan(TableName table, TableSource.Loaded table0, Snapshot snapshot, Schema schema)
            throws IOException {
        String snapshotId = (snapshot == null ? NO_SNAPSHOT : Long.toString(snapshot.snapshotId()))
                + "." + schema.schemaId();
        String fingerprint = table0.metadataLocation() + "#" + snapshotId;
        Planned cached = planned.get(fingerprint);
        if (cached != null) {
            return cached;
        }
        TableMetadata metadata = table0.metadata();
        List<String> problems = new ArrayList<>();
        // Sorted by location: manifests are read in parallel, and plans must be stable.
        Map<String, TableFile> files = new TreeMap<>();
        if (snapshot != null) {
            try {
                BaseTable iceberg = new BaseTable(new StaticTableOperations(metadata, io),
                        name + "." + table);
                TableScan scan = iceberg.newScan().useSnapshot(snapshot.snapshotId()).planWith(planPool);
                try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
                    for (FileScanTask task : tasks) {
                        org.apache.iceberg.DataFile file = task.file();
                        String location = Locations.normalize(file.location());
                        if (file.format() != FileFormat.PARQUET && problems.stream()
                                .noneMatch(p -> p.startsWith("data files in"))) {
                            problems.add("data files in " + file.format() + " format");
                        }
                        List<DeleteFile> deletes = new ArrayList<>();
                        for (org.apache.iceberg.DeleteFile delete : task.deletes()) {
                            deletes.add(deleteFile(delete));
                        }
                        files.putIfAbsent(location, new TableFile(location, file.fileSizeInBytes(), 0L,
                                null, null, file.recordCount(), deletes));
                    }
                }
            } catch (UncheckedIOException e) {
                throw e.getCause();
            } catch (RuntimeException e) {
                throw new IOException("Cannot plan Iceberg table " + table + " snapshot "
                        + snapshot.snapshotId() + ": " + e.getMessage(), e);
            }
        }
        List<ColumnSpec> columns = new ArrayList<>();
        for (Types.NestedField field : schema.columns()) {
            String type = DuckDbTypes.of(field.type());
            if (type == null) {
                problems.add("column " + field.name() + " has type " + field.type()
                        + ", which DuckDB's Parquet reader does not map");
                type = field.type().toString();
            }
            columns.add(new ColumnSpec(field.name(), type, field.isOptional(), field.fieldId()));
            if (field.initialDefault() != null) {
                problems.add("column " + field.name() + " has an initial default value");
            }
            String nested = nestedChange(metadata, field);
            if (nested != null) {
                problems.add(nested);
            }
        }
        if (metadata.properties().containsKey(TableProperties.DEFAULT_NAME_MAPPING)) {
            problems.add("the table has a name mapping (files without field IDs)");
        }
        TableSnapshot result = new TableSnapshot(table, snapshotId, fingerprint, columns, List.of(),
                new ArrayList<>(files.values()), new ReadSpec.IcebergMetadata(
                        Locations.normalize(table0.metadataLocation())));
        Planned plan = new Planned(result, problems.isEmpty() ? null : String.join("; ", problems));
        planned.put(fingerprint, plan);
        LOGGER.debug("iceberg=planned table={} snapshot={} files={} problem={}", table, snapshotId,
                files.size(), plan.problem());
        return plan;
    }

    /**
     * Describes a change of a nested column's fields between the table's schemas: read_parquet
     * maps nested fields by name, so files written before such a change would read wrong.
     *
     * @param metadata table metadata with every schema
     * @param field top-level column
     * @return reason, or null when the column is primitive or its fields never changed
     */
    static String nestedChange(TableMetadata metadata, Types.NestedField field) {
        if (field.type().isPrimitiveType()) {
            return null;
        }
        String shape = shape(field.type());
        for (Schema other : metadata.schemas()) {
            Types.NestedField earlier = other.findField(field.fieldId());
            if (earlier != null && !earlier.type().isPrimitiveType()
                    && !shape.equals(shape(earlier.type()))) {
                return "nested column " + field.name() + " changed its fields (schema "
                        + other.schemaId() + ")";
            }
            if (earlier != null && earlier.type().isPrimitiveType()) {
                return "column " + field.name() + " changed from primitive to nested";
            }
        }
        return null;
    }

    /**
     * Describes the nested fields of a type: IDs, names, optionality and types.
     *
     * @param type type
     * @return description
     */
    private static String shape(Type type) {
        if (type.isPrimitiveType()) {
            return type.toString();
        }
        StringBuilder result = new StringBuilder(type.typeId().name()).append('<');
        for (Types.NestedField child : type.asNestedType().fields()) {
            result.append(child.fieldId()).append(':').append(child.name()).append(':')
                    .append(child.isOptional()).append(':').append(shape(child.type())).append(',');
        }
        return result.append('>').toString();
    }

    /**
     * Converts an Iceberg delete file.
     *
     * @param delete delete file
     * @return SPI delete file
     */
    private static DeleteFile deleteFile(org.apache.iceberg.DeleteFile delete) {
        DeleteFile.Kind kind;
        if (delete.content() == FileContent.EQUALITY_DELETES) {
            kind = DeleteFile.Kind.EQUALITY;
        } else if (delete.format() == FileFormat.PUFFIN) {
            kind = DeleteFile.Kind.DELETION_VECTOR;
        } else {
            kind = DeleteFile.Kind.POSITION;
        }
        return new DeleteFile(Locations.normalize(delete.location()), delete.fileSizeInBytes(), kind,
                delete.equalityFieldIds());
    }
}
