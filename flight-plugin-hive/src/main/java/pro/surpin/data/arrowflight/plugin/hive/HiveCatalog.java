package pro.surpin.data.arrowflight.plugin.hive;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pro.surpin.data.arrowflight.spi.catalog.ArrowScan;
import pro.surpin.data.arrowflight.spi.catalog.AsOf;
import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.CatalogView;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableFormat;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.duckdb.DuckDbSetup;
import pro.surpin.data.arrowflight.spi.storage.DataFile;
import pro.surpin.data.arrowflight.spi.storage.DataStore;
import pro.surpin.data.arrowflight.spi.storage.Located;
import pro.surpin.data.arrowflight.spi.storage.ReadableFile;
import pro.surpin.data.arrowflight.spi.storage.Stores;

/**
 * A Hive Metastore as the server sees it: databases are schemas (with an optional prefix),
 * Parquet tables are tables, and so are ORC tables with hive.orcTables, text tables with
 * hive.textTables and Avro tables with hive.avroTables (the catalog reads their files itself and
 * hands the rows over as Arrow streams). A table's snapshot is its files now: the files in the table
 * location, or for a partitioned table the files in every partition's location as the metastore
 * names them (never guessed from directories), each with its partition values from the
 * metastore. Every location must lie under the catalog's roots before anything below it is
 * listed; a table with a location elsewhere is hidden. Iceberg tables of the metastore go to the
 * iceberg table format when there is one. With hive.views, simple views over shown tables are
 * listed as views (translated to DuckDB SQL by {@link HiveViewSql}). Names, tables and snapshots
 * are cached for their TTLs;
 * failures are never cached, and a failed listing drops the table's cached metadata.
 */
final class HiveCatalog implements Catalog {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveCatalog.class);
    /** Tables read per get_table_objects_by_name call. */
    private static final int TABLE_BATCH = 100;
    /** Rows per Arrow batch of an ORC or text table's stream. */
    private static final int ORC_BATCH_ROWS = 4096;
    /** ORC files of one table scan decoded at once (their batches still arrive in file order). */
    private static final int ORC_READ_AHEAD = Math.max(1,
            Math.min(4, Runtime.getRuntime().availableProcessors()));
    /** Most bytes of an ORC file read as one read-ahead unit (a group of its stripes). */
    private static final long ORC_RANGE_BYTES = 32L << 20;
    /** Largest avro.schema.url file read. */
    private static final long MAX_AVRO_SCHEMA_BYTES = 4L << 20;
    /** Schema files kept before the cache starts again. */
    private static final int MAX_AVRO_SCHEMAS = 10_000;
    /** Deepest chain of views over views that is followed. */
    private static final int MAX_VIEW_DEPTH = 16;

    private final HiveSettings settings;
    private final Metastore metastore;
    private final Stores stores;
    private final TableFormat iceberg;
    private final ExecutorService listPool;
    /** Decodes ORC files; idle threads end after a minute. */
    private final ExecutorService orcPool;
    private final String node;
    private volatile Cached<Map<String, String>> schemas;
    private final Map<String, Cached<Map<String, HiveTable>>> tables = new ConcurrentHashMap<>();
    /** Virtual views of each database as last listed with its tables (hive.views only). */
    private final Map<String, Cached<Map<String, Table>>> viewTables = new ConcurrentHashMap<>();
    private final Map<TableName, Cached<Optional<TableSnapshot>>> snapshots = new ConcurrentHashMap<>();
    /** Partitioned tables: true when a partition lies outside the roots (table hidden). */
    private final Map<String, Cached<Boolean>> partitionsOutside = new ConcurrentHashMap<>();
    private final Map<TableName, Object> locks = new ConcurrentHashMap<>();
    private final Set<String> logged = ConcurrentHashMap.newKeySet();
    /** Schema files of avro.schema.url by location, read again when their size or time changes. */
    private final Map<String, SchemaFile> avroSchemas = new ConcurrentHashMap<>();
    /** Reads avro.schema.url locations under the roots. */
    private final AvroFormat.SchemaReader avroUrls = this::avroSchema;

    /**
     * An Avro schema file as last read.
     *
     * @param size file size
     * @param modificationTime file time
     * @param text file content
     */
    private record SchemaFile(long size, long modificationTime, String text) {
    }

    /**
     * A value with its refresh deadline.
     *
     * @param value value
     * @param expiresAtMillis deadline
     * @param <T> value type
     */
    private record Cached<T>(T value, long expiresAtMillis) {

        boolean fresh(long now) {
            return expiresAtMillis > now;
        }
    }

    /**
     * Creates the catalog.
     *
     * @param settings settings
     * @param metastore metastore client
     * @param stores the server's stores
     * @param iceberg Iceberg table format, or null
     * @param listPool threads listing partition directories
     * @param node this node, for logs
     */
    HiveCatalog(HiveSettings settings, Metastore metastore, Stores stores, TableFormat iceberg,
            ExecutorService listPool, String node) {
        this.settings = settings;
        this.metastore = metastore;
        this.stores = stores;
        this.iceberg = iceberg;
        this.listPool = listPool;
        this.node = node;
        AtomicInteger orcThreads = new AtomicInteger();
        this.orcPool = Executors.newCachedThreadPool(task -> {
            Thread thread = new Thread(task, "hive-orc-read-" + orcThreads.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public List<String> schemas() throws IOException {
        return new ArrayList<>(schemaMap().keySet());
    }

    @Override
    public List<String> tables(String schema) throws IOException {
        String database = schemaMap().get(schema);
        if (database == null) {
            return List.of();
        }
        List<String> result = new ArrayList<>();
        for (Map.Entry<String, HiveTable> entry : tablesOf(database, false).entrySet()) {
            HiveTable hive = entry.getValue();
            if (hive.kind() == HiveTable.Kind.ICEBERG || hive.partitionColumns().isEmpty()
                    || !partitionsOutsideRoots(hive)) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    @Override
    public List<CatalogView> views(String schema) throws IOException {
        if (!settings.views()) {
            return List.of();
        }
        String database = schemaMap().get(schema);
        if (database == null) {
            return List.of();
        }
        Map<String, Optional<CatalogView>> resolved = new HashMap<>();
        List<CatalogView> result = new ArrayList<>();
        for (String name : viewTablesOf(database).keySet()) {
            view(database, name, resolved, new HashSet<>()).ifPresent(result::add);
        }
        return result;
    }

    @Override
    public Optional<TableSnapshot> current(TableName table) throws IOException {
        long now = System.currentTimeMillis();
        Cached<Optional<TableSnapshot>> cached = snapshots.get(table);
        if (cached != null && cached.fresh(now)) {
            return cached.value();
        }
        synchronized (locks.computeIfAbsent(table, t -> new Object())) {
            cached = snapshots.get(table);
            if (cached != null && cached.fresh(System.currentTimeMillis())) {
                return cached.value();
            }
            Optional<TableSnapshot> snapshot;
            try {
                snapshot = plan(table);
            } catch (UnreadableTableException e) {
                // This table only, not the catalog: its siblings stay readable.
                invalidate(table);
                throw e;
            } catch (IOException | RuntimeException e) {
                // Never keep a failure; drop what may have led to it.
                invalidate(table);
                throw e instanceof IOException io ? io : new IOException("Cannot read Hive table "
                        + table + ": " + e.getMessage(), e);
            }
            snapshots.put(table, new Cached<>(snapshot,
                    System.currentTimeMillis() + settings.metadataTtlMillis()));
            return snapshot;
        }
    }

    @Override
    public Optional<TableSnapshot> pinned(TableName table, String snapshotId) throws IOException {
        Optional<HiveTable> hive = hiveTable(table, false);
        if (hive.isPresent() && hive.get().kind() == HiveTable.Kind.ICEBERG) {
            Optional<TableSnapshot> result = iceberg.snapshot(table,
                    hive.get().metadataLocation(), snapshotId);
            if (result.isEmpty()) {
                hive = hiveTable(table, true);   // the metadata file may have moved on
                if (hive.isPresent() && hive.get().kind() == HiveTable.Kind.ICEBERG) {
                    result = iceberg.snapshot(table, hive.get().metadataLocation(), snapshotId);
                }
            }
            return result;
        }
        return current(table).filter(s -> s.snapshotId().equals(snapshotId));
    }

    @Override
    public Optional<TableSnapshot> pinnedColumns(TableName table, String snapshotId)
            throws IOException {
        for (boolean fresh : new boolean[] {false, true}) {
            Optional<HiveTable> hive = hiveTable(table, fresh);
            if (hive.isEmpty()) {
                return Optional.empty();
            }
            if (hive.get().kind() == HiveTable.Kind.ICEBERG) {
                return pinned(table, snapshotId);
            }
            if (hive.get().schemaId().equals(snapshotId)) {
                return Optional.of(new TableSnapshot(table, snapshotId, snapshotId,
                        hive.get().columns(), hive.get().partitionColumns(), List.of(),
                        readSpec(hive.get())));
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<TableSnapshot> asOf(TableName table, AsOf point) throws IOException {
        // A snapshot ID names an immutable snapshot: the listed metadata file that has it is
        // enough. A reference moves and a later commit may predate an instant: read the
        // metastore's current metadata file for those.
        boolean cachedFirst = point instanceof AsOf.SnapshotId;
        for (boolean fresh : cachedFirst ? new boolean[] {false, true} : new boolean[] {true}) {
            Optional<HiveTable> hive = hiveTable(table, fresh);
            if (hive.isEmpty()) {
                return Optional.empty();
            }
            if (hive.get().kind() != HiveTable.Kind.ICEBERG) {
                throw new UnsupportedOperationException("only Iceberg tables of the metastore"
                        + " keep snapshot history");
            }
            try {
                return Optional.of(iceberg.asOf(table, hive.get().metadataLocation(), point));
            } catch (IllegalArgumentException e) {
                if (fresh) {
                    throw e;
                }
            }
        }
        throw new IllegalStateException("unreachable");
    }

    @Override
    public Optional<String> partialReadProblem(TableSnapshot snapshot) {
        if (snapshot.read() instanceof ReadSpec.IcebergMetadata) {
            return iceberg == null ? Optional.of("no iceberg table format")
                    : iceberg.partialReadProblem(snapshot);
        }
        return Optional.empty();
    }

    @Override
    public void exportArrowScan(TableSnapshot snapshot, List<TableFile> files, ArrowScan scan,
            long streamAddress, Object allocator) throws IOException {
        HiveTable hive = streamTable(snapshot);
        BufferAllocator buffers = (BufferAllocator) allocator;
        TableFilesReader reader;
        try {
            TableFilesReader.Format format;
            if (hive.kind() == HiveTable.Kind.TEXT) {
                format = TableFilesReader.text(hive.dataColumns(), hive.textFormat(),
                        ORC_BATCH_ROWS);
            } else if (hive.kind() == HiveTable.Kind.AVRO) {
                format = TableFilesReader.avro(hive.dataColumns(), hive.avroFormat(),
                        ORC_BATCH_ROWS);
            } else {
                Map<String, TypeDescription> types = new HashMap<>();
                for (OrcArrowReader.Column column : hive.dataColumns()) {
                    types.put(column.name(), TypeDescription.fromString(column.hiveType()));
                }
                format = TableFilesReader.orc(hive.dataColumns(),
                        OrcSearchArguments.of(scan.predicates(), types), ORC_BATCH_ROWS);
            }
            reader = new TableFilesReader(buffers, files, this::open, hive.dataColumns(),
                    hive.partitionColumns(), scan.columns(), format, orcPool, ORC_READ_AHEAD,
                    ORC_RANGE_BYTES);
        } catch (IllegalArgumentException e) {
            throw new IOException("Cannot read " + snapshot.name() + ": " + e.getMessage(), e);
        }
        Data.exportArrayStream(buffers, reader, ArrowArrayStream.wrap(streamAddress));
    }

    @Override
    public List<String> readableRoots() {
        return settings.roots();
    }

    @Override
    public void contributeDuckDb(DuckDbSetup setup) {
        if (iceberg != null) {
            iceberg.contributeDuckDb(setup);
        }
    }

    @Override
    public void close() throws IOException {
        listPool.shutdownNow();
        orcPool.shutdownNow();
        metastore.close();
        if (iceberg != null) {
            try {
                iceberg.close();
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException("Cannot close the iceberg table format", e);
            }
        }
    }

    /**
     * Returns the schema names, refreshing them after the names TTL.
     *
     * @return schema name to database
     * @throws IOException when the metastore cannot be read
     */
    private Map<String, String> schemaMap() throws IOException {
        Cached<Map<String, String>> current = schemas;
        long now = System.currentTimeMillis();
        if (current != null && current.fresh(now)) {
            return current.value();
        }
        synchronized (this) {
            current = schemas;
            if (current != null && current.fresh(System.currentTimeMillis())) {
                return current.value();
            }
            Map<String, String> result = new TreeMap<>();
            for (String database : metastore.databases()) {
                if (settings.databases().isEmpty() || settings.databases().contains(database)) {
                    result.put(settings.schemaPrefix() + database, database);
                }
            }
            schemas = new Cached<>(Collections.unmodifiableMap(result),
                    System.currentTimeMillis() + settings.namesTtlMillis());
            return schemas.value();
        }
    }

    /**
     * Returns the tables of a database the catalog shows, refreshing them after the names TTL.
     *
     * @param database database
     * @param fresh whether to ignore the cached list
     * @return table name to table, sorted
     * @throws IOException when the metastore cannot be read
     */
    private Map<String, HiveTable> tablesOf(String database, boolean fresh) throws IOException {
        long now = System.currentTimeMillis();
        Cached<Map<String, HiveTable>> cached = tables.get(database);
        if (!fresh && cached != null && cached.fresh(now)) {
            return cached.value();
        }
        List<String> names = new ArrayList<>(metastore.tables(database));
        Collections.sort(names);
        Map<String, HiveTable> result = new TreeMap<>();
        Map<String, Table> views = new TreeMap<>();
        for (int i = 0; i < names.size(); i += TABLE_BATCH) {
            List<String> batch = names.subList(i, Math.min(names.size(), i + TABLE_BATCH));
            for (Table table : metastore.tableObjects(database, batch)) {
                if (settings.views() && "VIRTUAL_VIEW".equalsIgnoreCase(table.getTableType())) {
                    views.put(table.getTableName().toLowerCase(Locale.ROOT), table);
                    continue;
                }
                classified(table).ifPresent(t -> result.put(t.name(), t));
            }
        }
        Map<String, HiveTable> value = Collections.unmodifiableMap(result);
        long expires = System.currentTimeMillis() + settings.namesTtlMillis();
        viewTables.put(database, new Cached<>(Collections.unmodifiableMap(views), expires));
        tables.put(database, new Cached<>(value, expires));
        return value;
    }

    /**
     * Returns the virtual views of a database, listed with its tables.
     *
     * @param database database
     * @return view name to metastore view, sorted
     * @throws IOException when the metastore cannot be read
     */
    private Map<String, Table> viewTablesOf(String database) throws IOException {
        Cached<Map<String, Table>> cached = viewTables.get(database);
        if (cached == null || !cached.fresh(System.currentTimeMillis())) {
            tablesOf(database, true);
            cached = viewTables.get(database);
        }
        return cached.value();
    }

    /**
     * Resolves a view: translated, and every table or view it reads shown by this catalog.
     *
     * @param database database of the view
     * @param name view name
     * @param resolved views resolved so far, by database and name
     * @param path views being resolved, which a view may not read again
     * @return the view, empty when it is not shown (the reason is logged once)
     * @throws IOException when the metastore cannot be read
     */
    private Optional<CatalogView> view(String database, String name,
            Map<String, Optional<CatalogView>> resolved, Set<String> path) throws IOException {
        String key = database + "." + name;
        Optional<CatalogView> done = resolved.get(key);
        if (done != null) {
            return done;
        }
        Table table = viewTablesOf(database).get(name);
        if (table == null) {
            return Optional.empty();
        }
        Optional<CatalogView> result;
        try {
            result = Optional.of(translatedView(database, table, resolved, path));
        } catch (IllegalArgumentException e) {
            if (logged.add("view:" + key + ":" + e.getMessage())) {
                LOGGER.info("node={} hive=viewSkipped view={} reason='{}'", node, key,
                        e.getMessage());
            }
            result = Optional.empty();
        }
        resolved.put(key, result);
        return result;
    }

    /**
     * Translates a view and checks what it reads.
     *
     * @param database database of the view
     * @param table the metastore view
     * @param resolved views resolved so far
     * @param path views being resolved
     * @return the view
     * @throws IllegalArgumentException with the reason when the view is not shown
     * @throws IOException when the metastore cannot be read
     */
    private CatalogView translatedView(String database, Table table,
            Map<String, Optional<CatalogView>> resolved, Set<String> path) throws IOException {
        String name = table.getTableName().toLowerCase(Locale.ROOT);
        String key = database + "." + name;
        if (path.size() >= MAX_VIEW_DEPTH) {
            throw new IllegalArgumentException("views nested deeper than " + MAX_VIEW_DEPTH);
        }
        List<Map.Entry<String, String>> declared = new ArrayList<>();
        List<ColumnSpec> columns = new ArrayList<>();
        List<FieldSchema> fields = table.getSd() == null || table.getSd().getCols() == null
                ? List.of() : table.getSd().getCols();
        for (FieldSchema field : fields) {
            declared.add(new AbstractMap.SimpleImmutableEntry<>(field.getName(), field.getType()));
            columns.add(new ColumnSpec(field.getName(), HiveTypes.duckDb(field.getType()), true,
                    null, field.getType()));
        }
        Map<String, String> shown = schemaMap();
        HiveViewSql.Translated translated = HiveViewSql.translate(table.getViewExpandedText(),
                declared, db -> {
                    String schema = settings.schemaPrefix() + db;
                    return db.equals(shown.get(schema)) ? schema : null;
                });
        List<TableName> sources = new ArrayList<>();
        path.add(key);
        try {
            for (HiveViewSql.Source source : translated.sources()) {
                String schema = settings.schemaPrefix() + source.database();
                String sourceKey = source.database() + "." + source.table();
                if (path.contains(sourceKey)) {
                    throw new IllegalArgumentException("view reads itself through " + sourceKey);
                }
                boolean isTable = tables(schema).contains(source.table());
                if (!isTable && (!viewTablesOf(source.database()).containsKey(source.table())
                        || view(source.database(), source.table(), resolved, path).isEmpty())) {
                    throw new IllegalArgumentException("reads " + sourceKey
                            + ", which is no table or view the catalog shows");
                }
                sources.add(new TableName(schema, source.table()));
            }
        } finally {
            path.remove(key);
        }
        return new CatalogView(name, translated.sql(), sources, columns,
                fingerprint(translated.sql()));
    }

    /**
     * Hashes a view's DuckDB query (which names its columns and their types).
     *
     * @param sql the query
     * @return hex digest prefix
     */
    private static String fingerprint(String sql) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(("hive-view|" + sql).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 16);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is missing", e);
        }
    }

    /**
     * Finds a shown table, looking it up in the metastore when the cached list lacks it.
     *
     * @param table table name
     * @param fresh whether to read the table from the metastore again
     * @return the table, empty when it does not exist or is not shown
     * @throws IOException when the metastore cannot be read
     */
    private Optional<HiveTable> hiveTable(TableName table, boolean fresh) throws IOException {
        String database = schemaMap().get(table.schema());
        if (database == null) {
            return Optional.empty();
        }
        if (!fresh) {
            HiveTable listed = tablesOf(database, false).get(table.table());
            if (listed != null) {
                return Optional.of(listed);
            }
        }
        Optional<Table> read = metastore.table(database, table.table());
        if (read.isEmpty()) {
            return Optional.empty();
        }
        return classified(read.get());
    }

    /**
     * Looks at a metastore table and logs (once) why it is skipped or hidden.
     *
     * @param table metastore table
     * @return the table when it is shown
     */
    private Optional<HiveTable> classified(Table table) {
        HiveTable.Classified result = HiveTable.classify(table, settings.roots(), iceberg != null,
                settings.orcTables(), settings.textTables(), settings.avroTables(), avroUrls,
                settings.insertOnlyTables());
        String name = table.getDbName() + "." + table.getTableName();
        if (result.skipped() != null && logged.add("skip:" + name + ":" + result.skipped())) {
            LOGGER.info("node={} hive=tableSkipped table={} reason='{}'", node, name, result.skipped());
        }
        if (result.hidden() != null && logged.add("hide:" + name + ":" + result.hidden())) {
            LOGGER.warn("node={} hive=tableHidden table={} reason=outsideRoots roots={} location={}",
                    node, name, settings.roots(), result.hidden());
        }
        return result.shown();
    }

    /**
     * Plans a table's snapshot now.
     *
     * @param table table name
     * @return snapshot, empty when the table does not exist, is not shown or is hidden
     * @throws IOException when the metastore or a location cannot be read
     */
    private Optional<TableSnapshot> plan(TableName table) throws IOException {
        Optional<HiveTable> found = hiveTable(table, false);
        if (found.isEmpty()) {
            return Optional.empty();
        }
        HiveTable hive = found.get();
        if (hive.kind() == HiveTable.Kind.ICEBERG) {
            // Read the table again: its metadata file changes with every commit.
            Optional<HiveTable> now = hiveTable(table, true);
            if (now.isEmpty() || now.get().kind() != HiveTable.Kind.ICEBERG) {
                return Optional.empty();
            }
            return iceberg.snapshot(table, now.get().metadataLocation(), null);
        }
        // Insert-only tables: one snapshot of committed writes for every directory of the table.
        InsertOnlyDirectories.Snapshot writes = hive.insertOnly()
                ? metastore.snapshot(hive.database(), hive.name()) : null;
        List<TableFile> files = new ArrayList<>();
        if (hive.partitionColumns().isEmpty()) {
            files.addAll(list(hive.location(), Map.of(), writes));
        } else {
            List<Partition> partitions = partitions(hive);
            Map<String, Map<String, String>> locations = new LinkedHashMap<>();
            List<String> outside = new ArrayList<>();
            for (Partition partition : partitions) {
                String location = partition.getSd() == null || partition.getSd().getLocation() == null
                        ? null : HiveLocations.normalize(partition.getSd().getLocation());
                if (location == null || !HiveLocations.underAny(location, settings.roots())) {
                    outside.add(String.valueOf(location));
                    continue;
                }
                checkTextPartition(hive, partition);
                checkAvroPartition(hive, partition);
                locations.put(location, values(hive, partition));
            }
            recordPartitionsOutside(hive, outside);
            if (!outside.isEmpty()) {
                return Optional.empty();
            }
            files.addAll(listAll(locations, writes));
        }
        StringBuilder content = new StringBuilder();
        for (TableFile file : files) {
            content.append(file.location()).append('|').append(file.size()).append('|')
                    .append(file.modificationTime()).append('|').append(file.partitionValues())
                    .append('\n');
        }
        String fingerprint = hive.schemaId() + "/" + files.size() + "/" + HiveTable.hash(content.toString());
        LOGGER.debug("node={} hive=planned table={} files={} fingerprint={}", node, hive.qualified(),
                files.size(), fingerprint);
        return Optional.of(new TableSnapshot(table, hive.schemaId(), fingerprint, hive.columns(),
                hive.partitionColumns(), files, readSpec(hive)));
    }

    /**
     * Tells how the server reads a Parquet, ORC, text or Avro table.
     *
     * @param hive table
     * @return read specification
     */
    private static ReadSpec readSpec(HiveTable hive) {
        return switch (hive.kind()) {
            case ORC -> new ReadSpec.ArrowStream("orc");
            case TEXT -> new ReadSpec.ArrowStream("text");
            case AVRO -> new ReadSpec.ArrowStream("avro");
            default -> new ReadSpec.ParquetFiles(false);
        };
    }

    /**
     * Refuses a partition of a text table whose files are laid out other than the table says:
     * Hive reads each partition with its own serde properties, the catalog only with the table's.
     *
     * @param hive table
     * @param partition one of its partitions
     * @throws IOException when the partition has its own input format, serde or text format
     */
    private static void checkTextPartition(HiveTable hive, Partition partition) throws IOException {
        if (hive.kind() != HiveTable.Kind.TEXT || partition.getSd() == null) {
            return;
        }
        String problem;
        if (!HiveTable.text(partition.getSd())
                || HiveTable.csv(partition.getSd()) != (hive.textFormat().csv() != null)
                || !Objects.equals(HiveTable.json(partition.getSd()), hive.textFormat().json())) {
            problem = "has another storage format";
        } else {
            try {
                Map<String, String> serde = partition.getSd().getSerdeInfo().getParameters();
                problem = hive.textFormat().forPartition(serde).equals(hive.textFormat())
                        ? null : "has other serde properties than the table";
            } catch (IllegalArgumentException e) {
                problem = e.getMessage();
            }
        }
        if (problem != null) {
            throw new IOException("Partition " + partition.getValues() + " of text table "
                    + hive.qualified() + " " + problem + ": not read");
        }
    }

    /**
     * Refuses a partition of an Avro table that Hive reads with another schema than the table's:
     * one with another storage format, or, unless a table property names the schema, whose
     * own schema property or columns give another reader schema.
     *
     * @param hive table
     * @param partition one of its partitions
     * @throws IOException when the partition is read otherwise than the table
     */
    private void checkAvroPartition(HiveTable hive, Partition partition) throws IOException {
        if (hive.kind() != HiveTable.Kind.AVRO || partition.getSd() == null) {
            return;
        }
        String problem;
        if (!HiveTable.avro(partition.getSd())) {
            problem = "has another storage format";
        } else {
            try {
                Map<String, String> serde = partition.getSd().getSerdeInfo() == null ? Map.of()
                        : partition.getSd().getSerdeInfo().getParameters();
                // Schema.equals ignores docs (column comments) and compares what is read.
                problem = hive.avroFormat().forPartition(serde, partition.getSd().getCols(), avroUrls)
                        .schema().equals(hive.avroFormat().schema()) ? null
                        : "has another Avro schema than the table";
            } catch (IllegalArgumentException e) {
                problem = e.getMessage();
            }
        }
        if (problem != null) {
            throw new IOException("Partition " + partition.getValues() + " of Avro table "
                    + hive.qualified() + " " + problem + ": not read");
        }
    }

    /**
     * Finds the ORC, text or Avro table of a snapshot with the snapshot's columns.
     *
     * @param snapshot snapshot this catalog returned
     * @return the table
     * @throws IOException when it is gone, read otherwise or its columns changed
     */
    private HiveTable streamTable(TableSnapshot snapshot) throws IOException {
        for (boolean fresh : new boolean[] {false, true}) {
            Optional<HiveTable> hive = hiveTable(snapshot.name(), fresh);
            if (hive.isPresent() && (hive.get().kind() == HiveTable.Kind.ORC
                    || hive.get().kind() == HiveTable.Kind.TEXT
                    || hive.get().kind() == HiveTable.Kind.AVRO)
                    && hive.get().schemaId().equals(snapshot.snapshotId())) {
                return hive.get();
            }
        }
        throw new IOException("The table " + snapshot.name() + " is gone or its columns or format"
                + " changed since the query was planned");
    }

    /**
     * Reads the schema file an {@code avro.schema.url} names, as Hive reads it through its file
     * system, but only under the catalog's roots (never an HTTP URL or another file). The text is
     * kept per location and read again when the file's size or time changes.
     *
     * @param url the property's value
     * @return the schema text
     * @throws IllegalArgumentException when it lies outside the roots, is too large or cannot be
     *     read
     */
    private String avroSchema(String url) {
        String location = HiveLocations.normalize(url);
        if (!HiveLocations.underAny(location, settings.roots())) {
            throw new IllegalArgumentException(AvroFormat.URL + " " + url + " lies outside the"
                    + " catalog's roots (only schema files under hive.roots are read)");
        }
        Located located;
        DataFile file;
        try {
            located = stores.locate(location);
            file = located.store().file(located.relativePath());
        } catch (IOException | RuntimeException e) {
            throw unreadableSchema(url, e);
        }
        SchemaFile known = avroSchemas.get(location);
        if (known != null && known.size() == file.size()
                && known.modificationTime() == file.modificationTime()) {
            return known.text();
        }
        if (file.size() > MAX_AVRO_SCHEMA_BYTES) {
            throw new IllegalArgumentException(AvroFormat.URL + " " + url + " has " + file.size()
                    + " bytes, more than " + MAX_AVRO_SCHEMA_BYTES);
        }
        byte[] bytes = new byte[(int) file.size()];
        try {
            located.store().open(file).readFully(0, bytes, 0, bytes.length);
        } catch (IOException | RuntimeException e) {
            throw unreadableSchema(url, e);
        }
        String text = new String(bytes, StandardCharsets.UTF_8);
        if (avroSchemas.size() >= MAX_AVRO_SCHEMAS) {
            avroSchemas.clear();
        }
        avroSchemas.put(location, new SchemaFile(file.size(), file.modificationTime(), text));
        return text;
    }

    private static IllegalArgumentException unreadableSchema(String url, Exception cause) {
        return new IllegalArgumentException("Unable to read schema from given path: " + url + " ("
                + cause.getMessage() + ")", cause);
    }

    /**
     * Opens a file of a table through the server's stores.
     *
     * @param file table file
     * @return readable file
     * @throws IOException when no store holds it or it cannot be opened
     */
    private ReadableFile open(TableFile file) throws IOException {
        if (!HiveLocations.underAny(HiveLocations.normalize(file.location()), settings.roots())) {
            throw new IOException("File " + file.location() + " lies outside the catalog's roots");
        }
        Located located = stores.locate(file.location());
        return located.store().open(located.relativePath());
    }

    /**
     * Tells whether a partition of a partitioned table lies outside the roots, which hides the
     * whole table, also from the table list. Planning records the answer as well; it is kept
     * for the table-list TTL.
     *
     * @param hive partitioned table
     * @return true when the table is hidden
     * @throws IOException when the metastore cannot be read
     */
    private boolean partitionsOutsideRoots(HiveTable hive) throws IOException {
        Cached<Boolean> cached = partitionsOutside.get(hive.qualified());
        if (cached != null && cached.fresh(System.currentTimeMillis())) {
            return cached.value();
        }
        List<String> outside = new ArrayList<>();
        for (Partition partition : partitions(hive)) {
            String location = partition.getSd() == null || partition.getSd().getLocation() == null
                    ? null : HiveLocations.normalize(partition.getSd().getLocation());
            if (location == null || !HiveLocations.underAny(location, settings.roots())) {
                outside.add(String.valueOf(location));
            }
        }
        return recordPartitionsOutside(hive, outside);
    }

    /**
     * Keeps whether a partitioned table has partitions outside the roots and logs (once) when it
     * has.
     *
     * @param hive partitioned table
     * @param outside partition locations outside the roots
     * @return true when the table is hidden
     */
    private boolean recordPartitionsOutside(HiveTable hive, List<String> outside) {
        partitionsOutside.put(hive.qualified(), new Cached<>(!outside.isEmpty(),
                System.currentTimeMillis() + settings.namesTtlMillis()));
        if (!outside.isEmpty() && logged.add("hidePartitions:" + hive.qualified() + ":" + outside)) {
            LOGGER.warn("node={} hive=tableHidden table={} reason=partitionOutsideRoots"
                    + " roots={} locations={}", node, hive.qualified(), settings.roots(),
                    outside.subList(0, Math.min(3, outside.size())));
        }
        return !outside.isEmpty();
    }

    /**
     * Reads every partition of a table from the metastore, in batches, sorted by name.
     *
     * @param hive table
     * @return partitions
     * @throws IOException when the metastore cannot be read
     */
    private List<Partition> partitions(HiveTable hive) throws IOException {
        List<String> names = new ArrayList<>(metastore.partitionNames(hive.database(), hive.name()));
        Collections.sort(names);
        List<Partition> result = new ArrayList<>(names.size());
        int batch = settings.partitionBatchSize();
        for (int i = 0; i < names.size(); i += batch) {
            result.addAll(metastore.partitions(hive.database(), hive.name(),
                    names.subList(i, Math.min(names.size(), i + batch))));
        }
        return result;
    }

    /**
     * Reads a partition's values, in partition column order; the default partition name is NULL.
     *
     * @param hive table
     * @param partition partition
     * @return partition column to value
     * @throws IOException when the partition has the wrong number of values
     */
    private Map<String, String> values(HiveTable hive, Partition partition) throws IOException {
        List<String> values = partition.getValues();
        if (values == null || values.size() != hive.partitionColumns().size()) {
            throw new IOException("Partition " + values + " of " + hive.qualified() + " does not have "
                    + hive.partitionColumns().size() + " values");
        }
        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < values.size(); i++) {
            String value = values.get(i);
            result.put(hive.partitionColumns().get(i),
                    value == null || value.equals(settings.defaultPartitionName()) ? null : value);
        }
        return result;
    }

    /**
     * Lists the files of many locations in parallel, keeping the locations' order.
     *
     * @param locations location to its partition values
     * @param writes snapshot of committed writes (insert-only tables), else null
     * @return files
     * @throws IOException when a location cannot be listed
     */
    private List<TableFile> listAll(Map<String, Map<String, String>> locations,
            InsertOnlyDirectories.Snapshot writes) throws IOException {
        List<Future<List<TableFile>>> futures = new ArrayList<>();
        for (Map.Entry<String, Map<String, String>> entry : locations.entrySet()) {
            futures.add(listPool.submit(() -> list(entry.getKey(), entry.getValue(), writes)));
        }
        List<TableFile> result = new ArrayList<>();
        try {
            for (Future<List<TableFile>> future : futures) {
                result.addAll(future.get());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while listing partitions", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnreadableTableException unreadable) {
                throw unreadable;
            }
            throw cause instanceof IOException io ? io
                    : new IOException("Cannot list a partition: " + cause, cause);
        } finally {
            for (Future<List<TableFile>> future : futures) {
                future.cancel(true);
            }
        }
        return result;
    }

    /**
     * Lists the data files of one location through the server's stores: visible, non-empty files
     * (subdirectories too with recursiveDirectories). A missing directory has no files.
     *
     * @param location normalised location under the roots
     * @param values partition values of its files
     * @param writes snapshot of committed writes (insert-only tables: only the base, deltas or
     *     original files it chooses), else null
     * @return files, in path order
     * @throws IOException when the location cannot be listed
     * @throws UnreadableTableException when an insert-only directory cannot be read correctly at
     *     the snapshot
     */
    private List<TableFile> list(String location, Map<String, String> values,
            InsertOnlyDirectories.Snapshot writes) throws IOException {
        Located located = stores.locate(location);
        DataStore store = located.store();
        String prefix = located.relativePath().isEmpty() ? "" : located.relativePath() + "/";
        List<DataFile> files;
        Set<String> chosen = null;
        try {
            files = store.files(located.relativePath(), writes != null || settings.recursiveDirectories());
            if (writes != null) {
                chosen = Set.copyOf(chooseInsertOnly(store, located.relativePath(), prefix, files, writes,
                        location));
            }
        } catch (FileNotFoundException e) {
            return List.of();
        }
        List<TableFile> result = new ArrayList<>(files.size());
        for (DataFile file : files) {
            if (chosen != null) {
                String below = file.relativePath().substring(prefix.length());
                int slash = below.indexOf('/');
                if (!chosen.contains(slash < 0 ? below : below.substring(0, slash))) {
                    continue;
                }
            }
            if (file.size() > 0) {
                result.add(new TableFile(located.store().duckDbUri(file), file.size(),
                        file.modificationTime(), file.hosts(), values, -1L, null));
            }
        }
        return result;
    }

    /**
     * Chooses the base, deltas or original files of one insert-only table directory.
     *
     * @param store the directory's store
     * @param directory directory below the store's root
     * @param prefix the directory with a trailing slash, empty for the root
     * @param files the directory's visible files at any depth
     * @param writes snapshot of committed writes
     * @param location the directory's location, for messages
     * @return names of the chosen entries directly inside the directory
     * @throws IOException when it cannot be listed
     * @throws UnreadableTableException when it cannot be read correctly at the snapshot
     */
    private static List<String> chooseInsertOnly(DataStore store, String directory, String prefix,
            List<DataFile> files, InsertOnlyDirectories.Snapshot writes, String location)
            throws IOException {
        List<InsertOnlyDirectories.Entry> entries = new ArrayList<>();
        for (String name : store.directories(directory).keySet()) {
            String plain = name.endsWith("/") ? name.substring(0, name.length() - 1) : name;
            entries.add(new InsertOnlyDirectories.Entry(plain, true));
        }
        for (DataFile file : files) {
            String below = file.relativePath().substring(prefix.length());
            if (below.indexOf('/') < 0) {
                entries.add(new InsertOnlyDirectories.Entry(below, false));
            }
        }
        try {
            return InsertOnlyDirectories.select(entries, writes.writeIds(), writes.txns(),
                    base -> compactedBase(store, prefix + base));
        } catch (UncheckedIOException e) {
            throw e.getCause();
        } catch (IllegalStateException e) {
            throw new UnreadableTableException("Insert-only table directory " + location
                    + " cannot be read at the metastore's snapshot: " + e.getMessage(), e);
        }
    }

    /**
     * Tells whether a base directory was written by the compactor: its {@code _metadata_acid}
     * file says {@code "dataFormat":"compacted"}.
     *
     * @param store the store
     * @param base the base directory below the store's root
     * @return true for a compacted base
     * @throws UncheckedIOException when the file cannot be read
     */
    private static boolean compactedBase(DataStore store, String base) {
        String marker = base + "/_metadata_acid";
        try {
            if (!store.exists(marker)) {
                return false;
            }
            ReadableFile file = store.open(marker);
            byte[] bytes = new byte[(int) Math.min(file.length(), 4096)];
            file.readFully(0, bytes, 0, bytes.length);
            String text = new String(bytes, StandardCharsets.UTF_8).replaceAll("\\s", "");
            return text.contains("\"dataFormat\":\"compacted\"");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Drops what is cached about a table and its database's table list.
     *
     * @param table table name
     */
    private void invalidate(TableName table) {
        snapshots.remove(table);
        Cached<Map<String, String>> names = schemas;
        String database = names == null ? null : names.value().get(table.schema());
        if (database != null) {
            tables.remove(database);
        }
    }

    /**
     * A table whose files cannot be read correctly (an insert-only layout refused at the
     * metastore's snapshot): a problem of that table, not of the catalog, so the server leaves
     * out this table only and refuses its queries with the message (INVALID_ARGUMENT), while the
     * metastore's other tables stay readable. An {@link IOException} from {@link #current} means
     * the catalog cannot be read.
     */
    static final class UnreadableTableException extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;

        /**
         * Creates the exception.
         *
         * @param message what cannot be read and why
         * @param cause the refusal
         */
        UnreadableTableException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
