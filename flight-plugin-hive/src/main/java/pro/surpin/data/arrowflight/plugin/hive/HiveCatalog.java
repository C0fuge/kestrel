package pro.surpin.data.arrowflight.plugin.hive;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableFormat;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.duckdb.DuckDbSetup;
import pro.surpin.data.arrowflight.spi.storage.DataFile;
import pro.surpin.data.arrowflight.spi.storage.Located;
import pro.surpin.data.arrowflight.spi.storage.Stores;

/**
 * A Hive Metastore as the server sees it: databases are schemas (with an optional prefix),
 * Parquet tables are tables. A table's snapshot is its files now: the files in the table
 * location, or for a partitioned table the files in every partition's location as the metastore
 * names them (never guessed from directories), each with its partition values from the
 * metastore. Every location must lie under the catalog's roots before anything below it is
 * listed; a table with a location elsewhere is hidden. Iceberg tables of the metastore go to the
 * iceberg table format when there is one. Names, tables and snapshots are cached for their TTLs;
 * failures are never cached, and a failed listing drops the table's cached metadata.
 */
final class HiveCatalog implements Catalog {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveCatalog.class);
    /** Tables read per get_table_objects_by_name call. */
    private static final int TABLE_BATCH = 100;

    private final HiveSettings settings;
    private final Metastore metastore;
    private final Stores stores;
    private final TableFormat iceberg;
    private final ExecutorService listPool;
    private final String node;
    private volatile Cached<Map<String, String>> schemas;
    private final Map<String, Cached<Map<String, HiveTable>>> tables = new ConcurrentHashMap<>();
    private final Map<TableName, Cached<Optional<TableSnapshot>>> snapshots = new ConcurrentHashMap<>();
    /** Partitioned tables: true when a partition lies outside the roots (table hidden). */
    private final Map<String, Cached<Boolean>> partitionsOutside = new ConcurrentHashMap<>();
    private final Map<TableName, Object> locks = new ConcurrentHashMap<>();
    private final Set<String> logged = ConcurrentHashMap.newKeySet();

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
                        new ReadSpec.ParquetFiles(false)));
            }
        }
        return Optional.empty();
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
        for (int i = 0; i < names.size(); i += TABLE_BATCH) {
            List<String> batch = names.subList(i, Math.min(names.size(), i + TABLE_BATCH));
            for (Table table : metastore.tableObjects(database, batch)) {
                classified(table).ifPresent(t -> result.put(t.name(), t));
            }
        }
        Map<String, HiveTable> value = Collections.unmodifiableMap(result);
        tables.put(database, new Cached<>(value, System.currentTimeMillis() + settings.namesTtlMillis()));
        return value;
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
        HiveTable.Classified result = HiveTable.classify(table, settings.roots(), iceberg != null);
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
        List<TableFile> files = new ArrayList<>();
        if (hive.partitionColumns().isEmpty()) {
            files.addAll(list(hive.location(), Map.of()));
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
                locations.put(location, values(hive, partition));
            }
            recordPartitionsOutside(hive, outside);
            if (!outside.isEmpty()) {
                return Optional.empty();
            }
            files.addAll(listAll(locations));
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
                hive.partitionColumns(), files, new ReadSpec.ParquetFiles(false)));
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
     * @return files
     * @throws IOException when a location cannot be listed
     */
    private List<TableFile> listAll(Map<String, Map<String, String>> locations) throws IOException {
        List<Future<List<TableFile>>> futures = new ArrayList<>();
        for (Map.Entry<String, Map<String, String>> entry : locations.entrySet()) {
            futures.add(listPool.submit(() -> list(entry.getKey(), entry.getValue())));
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
     * @return files, in path order
     * @throws IOException when the location cannot be listed
     */
    private List<TableFile> list(String location, Map<String, String> values) throws IOException {
        Located located = stores.locate(location);
        List<DataFile> files;
        try {
            files = located.store().files(located.relativePath(), settings.recursiveDirectories());
        } catch (FileNotFoundException e) {
            return List.of();
        }
        List<TableFile> result = new ArrayList<>(files.size());
        for (DataFile file : files) {
            if (file.size() > 0) {
                result.add(new TableFile(located.store().duckDbUri(file), file.size(),
                        file.modificationTime(), file.hosts(), values, -1L, null));
            }
        }
        return result;
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
}
