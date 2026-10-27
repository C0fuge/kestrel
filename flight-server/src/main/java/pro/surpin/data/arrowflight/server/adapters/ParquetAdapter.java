package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.server.catalog.CatalogTables;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.model.FileAssignment;
import pro.surpin.data.arrowflight.server.services.ParquetQueryParser;
import pro.surpin.data.arrowflight.spi.storage.DataFile;
import pro.surpin.data.arrowflight.server.storage.DataStore;

/**
 * Reads Parquet metadata and file listings from the data store.
 * Maintains caches for schemas, tables, and DDL strings.
 */
public class ParquetAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetAdapter.class);
    private static final String TABLE_TIMING_PREFIX = "table=";
    private static final String FIELD_COUNT_PREFIX = " fields=";
    private static final long METADATA_TTL_NANOS = TimeUnit.SECONDS.toNanos(30);

    private final DataStore store;
    private final String dataDirectory;
    private final String localhost;
    private final CatalogTables catalogTables;
    /** Whether this node is loading the catalog now (guarded by this). */
    private boolean refreshing;

    private volatile CatalogSnapshot catalog;
    private final Map<TableKey, CachedTableSchema> arrowSchemaCache =
            new ConcurrentHashMap<>();
    private final Map<String, CachedParquetMetadata> parquetMetadataCache =
            new ConcurrentHashMap<>();
    private final Map<TableKey, CachedSampleFile> sampleFileCache = new ConcurrentHashMap<>();

    /**
     * Identifies one table in the Arrow schema cache.
     *
     * @param schema schema name
     * @param table table name
     */
    private record TableKey(String schema, String table) {
    }

    /**
     * Stores the file a table's queries are described against and its refresh deadline.
     *
     * @param file first Parquet file of the table
     * @param expiresAtNanos monotonic refresh deadline
     */
    private record CachedSampleFile(DataFile file, long expiresAtNanos) {
    }

    /**
     * Stores one consistent catalog inventory and its refresh deadline.
     *
     * @param schemas discovered schema directories
     * @param tables discovered table directories
     * @param ddls DDL for discovered tables
     * @param catalogSchemas schemas of the other catalogs (Iceberg, ...), to a location text
     * @param expiresAtNanos monotonic refresh deadline
     */
    private record CatalogSnapshot(Map<String, String> schemas,
            Map<String, Map<String, String>> tables,
            Map<String, Map<String, String>> ddls, Map<String, String> catalogSchemas,
            long expiresAtNanos) {
    }

    /**
     * Stores a full Arrow schema with its file-inventory fingerprint.
     *
     * @param schema full Arrow schema
     * @param fingerprint table file-inventory fingerprint
     * @param expiresAtNanos monotonic refresh deadline
     */
    private record CachedTableSchema(
            Schema schema, String fingerprint, long expiresAtNanos) {
    }

    /**
     * Stores Parquet footer metadata with its file fingerprint.
     *
     * @param schema Parquet schema
     * @param rowCount Parquet footer row count
     * @param length file length
     * @param modificationTime file modification time
     * @param expiresAtNanos monotonic refresh deadline
     */
    private record CachedParquetMetadata(MessageType schema, long rowCount,
            long length, long modificationTime, long expiresAtNanos) {
    }

    /**
     * Creates a ParquetAdapter for the given data store.
     *
     * @param appConfig server configuration
     * @param store     store of the data directory
     */
    public ParquetAdapter(AppConfig appConfig, DataStore store) {
        this(appConfig, store, "localhost");
    }

    /**
     * Creates a ParquetAdapter with an explicit localhost override.
     *
     * @param appConfig server configuration
     * @param store     store of the data directory
     * @param localhost storage host colocated with this server, for the local file inventory
     */
    public ParquetAdapter(AppConfig appConfig, DataStore store, String localhost) {
        this(appConfig, store, localhost, CatalogTables.none());
    }

    /**
     * Creates a ParquetAdapter that also shows the tables of other catalogs.
     *
     * @param appConfig server configuration
     * @param store     store of the data directory
     * @param localhost storage host colocated with this server, for the local file inventory
     * @param catalogTables tables of the catalogs besides the data directory
     */
    public ParquetAdapter(AppConfig appConfig, DataStore store, String localhost,
            CatalogTables catalogTables) {
        this.store = Objects.requireNonNull(store, "store");
        this.dataDirectory = store.root();
        this.localhost = localhost;
        this.catalogTables = Objects.requireNonNull(catalogTables, "catalogTables");

        refreshCatalog();
    }

    /**
     * Returns the tables of the catalogs besides the data directory.
     *
     * @return catalog tables, empty without catalogs
     */
    public CatalogTables catalogTables() {
        return catalogTables;
    }

    /**
     * Tells whether a schema belongs to another catalog than the data directory.
     *
     * @param schema schema name, may be null
     * @return true for a schema of a catalog (Iceberg, ...)
     */
    public boolean isCatalogSchema(String schema) {
        if (schema == null || catalogTables.isEmpty()) {
            return false;
        }
        if (catalog != null) {
            currentCatalog();   // refreshes the catalog schemas after the TTL
        }
        return catalogTables.owns(schema);
    }

    /**
     * Lists all schemas (top-level directories) matching an optional filter pattern.
     *
     * @param filterExpression SQL LIKE pattern, null for all
     * @return map of schema name to location
     * @throws IOException on storage read failure
     */
    public Map<String, String> getSchemas(String filterExpression) throws IOException {
        java.util.function.Predicate<String> schemaPredicate = createLikePredicate(filterExpression);
        CatalogSnapshot current = currentCatalog();
        Map<String, String> result = current.schemas().entrySet().stream()
                .filter(entry -> filterExpression == null || schemaPredicate.test(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        for (Map.Entry<String, String> entry : current.catalogSchemas().entrySet()) {
            if (filterExpression == null || schemaPredicate.test(entry.getKey())) {
                result.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    /**
     * Lists all tables in a schema matching an optional filter pattern.
     *
     * @param schema           schema name
     * @param filterExpression SQL LIKE pattern, null for all
     * @return map of table name to location
     */
    public Map<String, String> getTables(String schema, String filterExpression) {
        CatalogSnapshot current = currentCatalog();
        Map<String, String> tables = current.tables().getOrDefault(schema, Collections.emptyMap());
        if (tables.isEmpty() && current.catalogSchemas().containsKey(schema)) {
            tables = catalogTableNames(schema);
        }
        java.util.function.Predicate<String> tablePredicate = createLikePredicate(filterExpression);
        return tables.entrySet().stream()
                .filter(entry -> filterExpression == null || tablePredicate.test(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Returns the Arrow schema for a table.
     *
     * @param schema schema name
     * @param table  table name
     * @return Arrow schema
     */
    public Schema getTableSchema(String schema, String table) {
        return getTableSchema(schema, table, null);
    }

    /**
     * Returns the Arrow schema for a table, optionally filtering to specific columns.
     *
     * @param schema  schema name
     * @param table   table name
     * @param columns columns to include, null or empty for all
     * @return Arrow schema
     */
    public Schema getTableSchema(String schema, String table, List<String> columns) {
        long t = LogUtil.mark();
        validateName(schema);
        validateName(table);
        if (isCatalogSchema(schema)) {
            // Catalog tables have no footer schema of their own; DuckDB describes them.
            LOGGER.debug("node={} parquet=catalogTableWithoutFooterSchema table={}.{}",
                    LogUtil.node(), schema, table);
            return new Schema(Collections.emptyList(), null);
        }
        TableKey key = new TableKey(schema, table);
        CachedTableSchema cached = arrowSchemaCache.compute(key,
                (ignored, current) -> refreshTableSchema(key, current));
        Schema fullSchema = cached.schema();
        if (columns == null || columns.isEmpty()) {
            LogUtil.logTiming(t, "schema.cache",
                    TABLE_TIMING_PREFIX + schema + "." + table
                            + FIELD_COUNT_PREFIX + fullSchema.getFields().size());
            return fullSchema;
        }
        List<Field> projected = fullSchema.getFields().stream()
                .filter(field -> columns.contains(field.getName()))
                .toList();
        LogUtil.logTiming(t, "schema.cacheProjection",
                TABLE_TIMING_PREFIX + schema + "." + table
                        + FIELD_COUNT_PREFIX + projected.size());
        return new Schema(projected, fullSchema.getCustomMetadata());
    }

    /**
     * Estimates row count from cached Parquet footers for assigned files.
     *
     * @param files assigned relative file paths and sizes
     * @return summed footer row count, or negative one when unavailable
     */
    public long estimateRowCount(Map<String, FileAssignment> files) {
        long result = 0L;
        try {
            for (Map.Entry<String, FileAssignment> file : files.entrySet()) {
                CachedParquetMetadata metadata =
                        parquetMetadata(file.getKey(), file.getValue().size());
                result = Math.addExact(result, metadata.rowCount());
            }
            return result;
        } catch (IOException | RuntimeException e) {
            LOGGER.warn("Unable to estimate Parquet row count: {}", e.getMessage());
            return -1L;
        }
    }

    /**
     * Refreshes a table schema after its short metadata TTL.
     *
     * @param key table key
     * @param current current cached value
     * @return refreshed cached schema
     */
    private CachedTableSchema refreshTableSchema(
            TableKey key, CachedTableSchema current) {
        long now = System.nanoTime();
        if (current != null && current.expiresAtNanos() > now) {
            return current;
        }
        try {
            String tablePath = tablePath(key.schema(), key.table());
            LOGGER.debug("node={} parquet=schemaReadStart table={}.{} path={}",
                    LogUtil.node(), key.schema(), key.table(), location(tablePath));
            List<DataFile> files = store.parquetFiles(tablePath);
            String fingerprint = inventoryFingerprint(files);
            if (current != null && current.fingerprint().equals(fingerprint)) {
                return new CachedTableSchema(
                        current.schema(), fingerprint, now + METADATA_TTL_NANOS);
            }
            if (files.isEmpty()) {
                LOGGER.warn("node={} parquet=schemaNotFound table={}.{} path={}",
                        LogUtil.node(), key.schema(), key.table(), location(tablePath));
                return new CachedTableSchema(
                        new Schema(Collections.emptyList(), null),
                        fingerprint, now + METADATA_TTL_NANOS);
            }
            DataFile first = files.get(0);
            long footerStart = LogUtil.mark();
            CachedParquetMetadata metadata = readParquetMetadata(first, now);
            Schema arrowSchema = SchemaConverter.convert(metadata.schema(), ignored -> true);
            LogUtil.logTiming(footerStart, "schema.readFooter",
                    TABLE_TIMING_PREFIX + key.schema() + "." + key.table()
                            + FIELD_COUNT_PREFIX + metadata.schema().getFieldCount());
            return new CachedTableSchema(
                    arrowSchema, fingerprint, now + METADATA_TTL_NANOS);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Returns one Parquet file of a table, enough to bind a query against the table's columns.
     *
     * @param schema schema name
     * @param table  table name
     * @return first Parquet file in path order (remembered for the metadata TTL), or empty when
     *         the table has none
     * @throws IOException on storage access failure
     */
    public java.util.Optional<DataFile> sampleFile(String schema, String table) throws IOException {
        TableKey key = new TableKey(schema, table);
        long now = System.nanoTime();
        CachedSampleFile cached = sampleFileCache.get(key);
        if (cached != null && cached.expiresAtNanos() > now) {
            return java.util.Optional.of(cached.file());
        }
        List<DataFile> files = tableFiles(schema, table);
        if (files.isEmpty()) {
            sampleFileCache.remove(key);
            return java.util.Optional.empty();
        }
        DataFile first = files.get(0);
        sampleFileCache.put(key, new CachedSampleFile(first, now + METADATA_TTL_NANOS));
        return java.util.Optional.of(first);
    }

    /**
     * Drops the remembered sample file of a table, so the next lookup lists the table again.
     *
     * @param schema schema name
     * @param table  table name
     */
    public void forgetSampleFile(String schema, String table) {
        sampleFileCache.remove(new TableKey(schema, table));
    }

    /**
     * Lists all Parquet files of a table.
     *
     * @param schema schema name
     * @param table table name
     * @return files in path order, empty when the table has none
     * @throws IOException on storage access failure
     */
    public List<DataFile> tableFiles(String schema, String table) throws IOException {
        validateName(schema);
        validateName(table);
        requireSchema(schema, table);
        if (isCatalogSchema(schema)) {
            return List.of();   // read through CatalogTables
        }
        return store.tableFiles(schema, table);
    }

    /**
     * Identifies a set of files by their paths, lengths and modification times.
     *
     * @param files table files
     * @return fingerprint that changes when a file is added, removed or rewritten
     */
    public static String filesFingerprint(List<DataFile> files) {
        return Integer.toHexString(inventoryFingerprint(files).hashCode()) + "/" + files.size();
    }

    /**
     * Resolves a table directory below the data directory.
     *
     * @param schema schema name
     * @param table table name
     * @return table directory relative to the data directory
     * @throws IllegalArgumentException when the table is named without its schema
     */
    private static String tablePath(String schema, String table) {
        requireSchema(schema, table);
        return schema + "/" + table;
    }

    /**
     * Refuses a table named without its schema.
     *
     * @param schema schema name
     * @param table table name
     * @throws IllegalArgumentException when the schema is missing
     */
    private static void requireSchema(String schema, String table) {
        if (schema == null || schema.isEmpty()) {
            throw new IllegalArgumentException("Name the table with its schema (schema." + table
                    + "): partial results need it; complete answers also find tables without a schema");
        }
    }

    /**
     * Names a path below the data directory for logs.
     *
     * @param relativePath path below the data directory
     * @return data directory and path
     */
    private String location(String relativePath) {
        return dataDirectory.endsWith("/")
                ? dataDirectory + relativePath : dataDirectory + "/" + relativePath;
    }

    /**
     * Builds a stable fingerprint from table file inventory.
     *
     * @param files sorted Parquet files
     * @return inventory fingerprint
     */
    private static String inventoryFingerprint(List<DataFile> files) {
        StringBuilder result = new StringBuilder();
        for (DataFile file : files) {
            result.append(file.relativePath()).append(':')
                    .append(file.size()).append(':')
                    .append(file.modificationTime()).append(';');
        }
        return result.toString();
    }

    /**
     * Resolves cached footer metadata for one relative Parquet path.
     *
     * @param relativePath relative path below the data directory
     * @param expectedLength length published in cluster inventory
     * @return cached footer metadata
     * @throws IOException on storage access failure
     */
    private CachedParquetMetadata parquetMetadata(
            String relativePath, long expectedLength) throws IOException {
        long now = System.nanoTime();
        CachedParquetMetadata current = parquetMetadataCache.get(relativePath);
        if (current != null && current.expiresAtNanos() > now
                && current.length() == expectedLength) {
            return current;
        }
        DataFile file = store.file(relativePath);
        if (file.size() != expectedLength) {
            throw new IOException("Parquet inventory size changed for " + relativePath);
        }
        return readParquetMetadata(file, now);
    }

    /**
     * Reads and caches schema and row count from one Parquet footer.
     *
     * @param file listed or looked-up file; its relative path is the cache key
     * @param now current monotonic time
     * @return cached footer metadata
     * @throws IOException on footer read failure
     */
    private CachedParquetMetadata readParquetMetadata(DataFile file, long now)
            throws IOException {
        String relativePath = file.relativePath();
        long length = file.size();
        long modificationTime = file.modificationTime();
        CachedParquetMetadata current = parquetMetadataCache.get(relativePath);
        if (current != null && current.length() == length
                && current.modificationTime() == modificationTime) {
            CachedParquetMetadata refreshed = new CachedParquetMetadata(
                    current.schema(), current.rowCount(), length,
                    modificationTime, now + METADATA_TTL_NANOS);
            parquetMetadataCache.put(relativePath, refreshed);
            return refreshed;
        }
        try (ParquetFileReader reader = ParquetFileReader.open(store.inputFile(file))) {
            MessageType parquetSchema =
                    reader.getFooter().getFileMetaData().getSchema();
            long rowCount = reader.getFooter().getBlocks().stream()
                    .mapToLong(block -> block.getRowCount()).sum();
            CachedParquetMetadata metadata = new CachedParquetMetadata(
                    parquetSchema, rowCount, length, modificationTime,
                    now + METADATA_TTL_NANOS);
            parquetMetadataCache.put(relativePath, metadata);
            return metadata;
        }
    }

    /**
     * Converts an Arrow schema to a DuckDB-compatible DDL statement.
     *
     * @param tableSchema schema name
     * @param tableName   table name
     * @param schema      Arrow schema
     * @return DDL string
     */
    public String arrowSchemaToDDL(String tableSchema, String tableName, Schema schema) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(schema);

        StringBuilder result = new StringBuilder("CREATE TABLE ");
        if (tableSchema != null) {
            result.append(tableSchema).append(".");
        }
        result.append(tableName).append("(\n");

        List<Field> fields = schema.getFields();
        IntStream.range(0, fields.size()).forEach(i -> {
            Field field = fields.get(i);
            if (i > 0) {
                result.append(",\n");
            }
            result.append("\t\"").append(field.getName()).append("\" ")
                    .append(DdlTypeNames.of(field.getType()));
        });
        result.append(")");
        return result.toString();
    }

    /**
     * Refreshes the catalog inventory and its DDL definitions. A refresh the loading itself
     * asks for (the data directory's tables ask whether their schema belongs to a catalog) is
     * skipped: it would recurse until the stack overflows.
     */
    public synchronized void refreshCatalog() {
        if (refreshing) {
            return;
        }
        refreshing = true;
        try {
            catalog = loadCatalog();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            refreshing = false;
        }
    }

    /**
     * Lets the catalog snapshot expire now, for tests of the refresh.
     */
    synchronized void expireCatalog() {
        CatalogSnapshot c = catalog;
        catalog = new CatalogSnapshot(c.schemas(), c.tables(), c.ddls(), c.catalogSchemas(),
                System.nanoTime() - 1);
    }

    /** Initializes cached DDL definitions for all discovered tables. */
    public void initCatalogReader() {
        refreshCatalog();
    }

    /**
     * Returns a catalog snapshot, refreshing it after the metadata TTL.
     *
     * @return current catalog snapshot
     */
    private CatalogSnapshot currentCatalog() {
        CatalogSnapshot current = catalog;
        if (current.expiresAtNanos() > System.nanoTime()) {
            return current;
        }
        synchronized (this) {
            current = catalog;
            if (current.expiresAtNanos() <= System.nanoTime()) {
                refreshCatalog();
            }
            return catalog;
        }
    }

    /**
     * Scans schema and table directories and builds a consistent DDL snapshot.
     *
     * @return newly loaded catalog snapshot
     * @throws IOException on filesystem read failure
     */
    private CatalogSnapshot loadCatalog() throws IOException {
        Map<String, String> schemas = Map.copyOf(scanSchemas());
        Map<String, Map<String, String>> tables = scanTables(schemas);
        Map<String, Map<String, String>> ddls = new HashMap<>();
        StringBuilder ddlBuilder = new StringBuilder();
        tables.forEach((schemaName, tablesMap) -> {
            Map<String, String> schemaDdls = new HashMap<>();
            tablesMap.forEach((tableName, path) -> {
                Schema schema = getTableSchema(schemaName, tableName);
                String ddl = arrowSchemaToDDL(schemaName, tableName, schema);
                schemaDdls.put(tableName, ddl);
                ddlBuilder.append(ddl).append(";\n");
            });
            ddls.put(schemaName, Map.copyOf(schemaDdls));
        });
        LOGGER.info("Parsed DDL: {}", ddlBuilder);
        Map<String, String> catalogSchemas = catalogTables.isEmpty() ? Map.of()
                : Map.copyOf(catalogTables.schemas(schemas.keySet()));
        if (!catalogSchemas.isEmpty()) {
            LOGGER.info("Catalog schemas: {}", catalogSchemas);
        }
        return new CatalogSnapshot(schemas, Map.copyOf(tables),
                Map.copyOf(ddls), catalogSchemas, System.nanoTime() + METADATA_TTL_NANOS);
    }

    /**
     * Lists the tables of a catalog schema; a catalog that cannot be read is an error, never an
     * empty schema.
     *
     * @param schema catalog schema
     * @return table name to a location text
     * @throws UncheckedIOException when the catalog cannot be read
     */
    private Map<String, String> catalogTableNames(String schema) {
        try {
            Map<String, String> result = new java.util.LinkedHashMap<>();
            for (String table : catalogTables.tables(schema)) {
                result.put(table, "catalog:" + schema + "." + table);
            }
            return result;
        } catch (IOException e) {
            LOGGER.warn("node={} catalog=tablesFailed schema={} error='{}'", LogUtil.node(), schema,
                    e.getMessage());
            throw new UncheckedIOException("The catalog of schema " + schema + " cannot be read: "
                    + e.getMessage(), e);
        }
    }

    /**
     * Returns the DDL cache (schema → table → DDL string).
     *
     * @return DDL map
     */
    public Map<String, Map<String, String>> tableDdlCache() {
        return currentCatalog().ddls();
    }

    /**
     * Resolves file locations for a SQL query across all relevant Parquet files.
     *
     * @param query SQL query
     * @return map of relative file path to FileAssignment with locality info
     * @throws IOException on storage read failure
     */
    public Map<String, FileAssignment> locationsForQuery(String query) throws IOException {
        long t = LogUtil.mark();
        ParquetQueryParser parsedQuery = ParquetQueryParser.parse(query);
        Map<String, FileAssignment> result = new HashMap<>();

        int fileCount = 0;
        long totalBytes = 0;

        if (parsedQuery.isJoin) {
            for (ParquetQueryParser.JoinTable jt : parsedQuery.joinTables) {
                validateName(jt.schema());
                validateName(jt.table());
                String parquetPath = tablePath(jt.schema(), jt.table());
                LOGGER.debug("node={} parquet=discover table={}.{} path={}",
                        LogUtil.node(), jt.schema(), jt.table(), location(parquetPath));
                for (DataFile file : store.parquetFiles(parquetPath)) {
                    result.putIfAbsent(file.relativePath(),
                            new FileAssignment(file.size(), file.hosts()));
                    fileCount++;
                    totalBytes += file.size();
                }
                LogUtil.logTiming(t, "schema.discoverJoinTable", TABLE_TIMING_PREFIX + jt.schema() + "." + jt.table() + " files=" + fileCount + " bytes=" + totalBytes);
            }
            return result;
        }

        validateName(parsedQuery.schema);
        validateName(parsedQuery.table);
        String parquetPath = tablePath(parsedQuery.schema, parsedQuery.table);
        LOGGER.debug("node={} parquet=discover table={}.{} path={}",
                LogUtil.node(), parsedQuery.schema, parsedQuery.table, location(parquetPath));
        for (DataFile file : store.parquetFiles(parquetPath)) {
            result.put(file.relativePath(), new FileAssignment(file.size(), file.hosts()));
            fileCount++;
            totalBytes += file.size();
        }
        LogUtil.logTiming(t, "schema.discover", TABLE_TIMING_PREFIX + (parsedQuery.schema != null ? parsedQuery.schema + "." : "") + (parsedQuery.table != null ? parsedQuery.table : "") + " files=" + fileCount + " bytes=" + totalBytes);
        return result;
    }

    /**
     * Lists Parquet files with at least one block on this server's colocated storage host.
     * Paths are relative to the shared data root. For local filesystems, loopback block
     * locations are mapped to the configured storage host.
     *
     * @return relative Parquet path to file size
     * @throws IOException on storage access failure
     */
    public Map<String, Long> localFileInventory() throws IOException {
        return store.localInventory(localhost);
    }

    /**
     * Returns the data directory path.
     *
     * @return data directory path string
     */
    public String dataDirectory() {
        return dataDirectory;
    }

    /**
     * Validates that a schema or table name contains only safe characters.
     * Prevents path traversal via names like ".." or "./etc".
     */
    public static String validateName(String name) {
        if (name == null) {
            return null;
        }
        if (!name.matches("^[a-zA-Z_][a-zA-Z0-9_]*$")) {
            throw new IllegalArgumentException("Invalid name: '" + name
                    + "'. Must match [a-zA-Z_][a-zA-Z0-9_]*");
        }
        return name;
    }

    /**
     * Returns the store of the data directory.
     *
     * @return data store
     */
    public DataStore dataStore() {
        return store;
    }

    /**
     * Tells whether every node can read every file (HDFS, object stores), as opposed to a local
     * directory per node, where a node can only read the files it holds itself.
     *
     * @return true unless the data directory is on the local file system
     */
    public boolean sharedStorage() {
        return store.shared();
    }

    /**
     * Tells whether files have no hosts (object stores): queries are then planned from the
     * store's listing, not from the nodes' local inventories.
     *
     * @return true when the store has no locality
     */
    public boolean withoutLocality() {
        return !store.hasLocality();
    }

    /**
     * Scans data directory for schema directories and caches them.
     *
     * @throws IOException on storage read failure
     */
    private Map<String, String> scanSchemas() throws IOException {
        LOGGER.info("Initializing schema cache for data directory: {}", dataDirectory);

        if (!store.exists("")) {
            LOGGER.info("Data directory does not exist: {}", dataDirectory);
            return Collections.emptyMap();
        }

        Map<String, String> result = store.directories("");

        LOGGER.info("Collected schemas: {}", result);
        return result;
    }

    /**
     * Scans each schema directory for table directories and caches them.
     *
     * @throws IOException on storage read failure
     */
    private Map<String, Map<String, String>> scanTables(Map<String, String> schemas)
            throws IOException {
        LOGGER.info("Initializing table cache for data directory: {}", dataDirectory);

        if (!store.exists("")) {
            LOGGER.info("Data directory does not exist: {}", dataDirectory);
            return Collections.emptyMap();
        }

        Map<String, Map<String, String>> result = new HashMap<>();
        for (Map.Entry<String, String> schema : schemas.entrySet()) {
            LOGGER.info("Collecting tables for schema: {} at path {}", schema.getKey(), schema.getValue());
            result.put(schema.getKey(), Map.copyOf(store.directories(schema.getKey())));
        }

        LOGGER.info("Collected tables: {}", result);
        return result;
    }

    /**
     * Creates a predicate from a SQL LIKE pattern.
     *
     * @param pattern SQL LIKE pattern
     * @return predicate
     */
    static java.util.function.Predicate<String> createLikePredicate(String pattern) {
        if (pattern == null || pattern.equals("%")) {
            return s -> true;
        }
        StringBuilder regex = new StringBuilder();
        for (int i = 0; i < pattern.length(); i++) {
            char current = pattern.charAt(i);
            if (current == '%') {
                regex.append(".*");
            } else if (current == '_') {
                regex.append('.');
            } else {
                regex.append(java.util.regex.Pattern.quote(String.valueOf(current)));
            }
        }
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(regex.toString(),
                java.util.regex.Pattern.CASE_INSENSITIVE);
        return s -> p.matcher(s).matches();
    }
}
