package pro.surpin.data.arrowflight.server.services;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.catalog.ArrowScanRequests;
import pro.surpin.data.arrowflight.server.catalog.ArrowStreamScans;
import pro.surpin.data.arrowflight.server.catalog.CatalogSources;
import pro.surpin.data.arrowflight.server.catalog.CatalogTables;
import pro.surpin.data.arrowflight.spi.catalog.ArrowScan;
import pro.surpin.data.arrowflight.spi.catalog.CatalogView;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.DeleteFile;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.storage.DataFile;
import pro.surpin.data.arrowflight.server.storage.DataStore;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

    /** Marks a catalog table read as Arrow streams: no view, each reference reads a stream. */
    private static final TableViews ARROW_STREAM = new TableViews("arrowStream", null, null);

    /**
     * SELECTs of a time-travel snapshot.
     *
     * @param data SELECT over the snapshot's files
     * @param empty SELECT with the same columns and no rows
     */
    private record TravelViews(String data, String empty) {
    }

    /**
     * FROM source of a time-travel snapshot.
     *
     * @param sql FROM source
     * @param scan whether DuckDB's iceberg_scan reads the snapshot (its columns must be checked)
     */
    private record TravelSource(String sql, boolean scan) {
    }

    /** Most time-travel snapshots kept described. */
    private static final int TRAVEL_CAPACITY = 64;

    /** Most rounds of view replacement (views over views); a catalog nests them less deep. */
    static final int MAX_VIEW_ROUNDS = 16;

    /**
     * A view of a catalog as the server shows it.
     *
     * @param schema schema name as the server spells it
     * @param view the catalog's view
     */
    private record ShownView(String schema, CatalogView view) {
    }

    /**
     * A client's SQL with the catalog views it names replaced by their queries.
     *
     * @param sql SQL that reads the views' tables instead of the views
     * @param sources every table and view the replaced views read, in order of first use; empty
     *        when the SQL names no view
     */
    public record Expanded(String sql, List<TableName> sources) {
    }

    private final ParquetAdapter parquetAdapter;
    private final Object viewLock = new Object();
    /** Catalog views: lower-case "schema\0view" to the view. */
    private Map<String, ShownView> shownViews = Map.of();
    /** When the catalog views were last listed, or null before the first listing. */
    private Long viewsListedAtNanos;
    /** Skip reasons of catalog views already logged. */
    private final Set<String> loggedViews = java.util.concurrent.ConcurrentHashMap.newKeySet();
    /** Described time-travel snapshots, least recently used first. */
    private final Map<String, TravelViews> travel = new java.util.LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, TravelViews> eldest) {
            return size() > TRAVEL_CAPACITY;
        }
    };
    private final DuckDbAdapter duckDbAdapter;
    private final boolean widenUnsigned;
    private final Map<String, TableViews> tables = new HashMap<>();
    /** Catalog schemas without views because their catalog failed: schema to error. */
    private final Map<String, String> unavailable = new java.util.concurrent.ConcurrentHashMap<>();
    /** Catalog tables read as Arrow streams: lower-case "schema\0table" to the stored names. */
    private volatile Map<String, List<String>> streamTables = Map.of();
    private Snapshot snapshot;
    private long refreshedAtNanos;
    /** Data version of the store the snapshot was built from (null when unknown). */
    private String dataVersion;

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
     * Returns the current view definitions, refreshing them at most once a minute or at once when
     * the store reports a new data version (for example a new commit on a lakeFS branch). All
     * tables of one refresh are listed in one read scope of the store, so the views never mix
     * versions.
     *
     * @return view definitions
     * @throws IOException when the table files cannot be listed
     * @throws SQLException when DuckDB cannot read a table's column types
     */
    public synchronized Snapshot current() throws IOException, SQLException {
        long now = System.nanoTime();
        DataStore store = parquetAdapter.dataStore();
        if (snapshot != null && now - refreshedAtNanos < REFRESH_NANOS
                && Objects.equals(dataVersion, store == null ? null : store.dataVersion())) {
            return snapshot;
        }
        try (DataStore.ReadScope scope = store == null ? DataStore.ReadScope.NONE
                : store.readScope()) {
            String version = store == null ? null : store.dataVersion();
            refresh(now);
            dataVersion = version;
        }
        return snapshot;
    }

    private void refresh(long now) throws IOException, SQLException {
        long t = LogUtil.mark();
        List<String> data = new ArrayList<>();
        List<String> empty = new ArrayList<>();
        List<String> drops = new ArrayList<>();
        Map<String, TableViews> seen = new HashMap<>();
        Map<String, List<String>> streams = new HashMap<>();
        // Table name in lower case (DuckDB ignores case) -> schemas that have it.
        Map<String, List<String>> owners = new TreeMap<>();
        Map<String, String> spelling = new HashMap<>();
        // Catalogs that failed during this refresh: their other schemas and tables keep the views
        // made before without asking the catalog again (a metastore that hangs would otherwise
        // cost its timeouts once per table, while every request waits for this refresh).
        Map<CatalogTables.Entry, String> failed = new HashMap<>();
        for (String schema : parquetAdapter.getSchemas(null).keySet().stream().sorted().toList()) {
            if (RESERVED_SCHEMAS.contains(schema.toLowerCase(Locale.ROOT))) {
                LOGGER.warn("node={} views=skipSchema schema={} reason=reservedName",
                        LogUtil.node(), schema);
                continue;
            }
            String createSchema = "CREATE SCHEMA IF NOT EXISTS " + DuckDbAdapter.quoteIdentifier(schema);
            boolean schemaAdded = false;
            boolean catalogSchema = parquetAdapter.isCatalogSchema(schema);
            CatalogTables catalogs = catalogSchema ? parquetAdapter.catalogTables() : null;
            CatalogTables.Entry owner = catalogs == null ? null : catalogs.owner(schema).orElse(null);
            List<String> tableNames;
            try {
                if (owner != null && failed.containsKey(owner)) {
                    throw new IllegalStateException(failed.get(owner));
                }
                tableNames = parquetAdapter.getTables(schema, null).keySet().stream().sorted().toList();
            } catch (RuntimeException e) {
                if (!catalogSchema) {
                    throw e;
                }
                // The catalog cannot be read now: keep the views made before (an outage of a
                // metastore must not make its tables vanish from complete answers).
                if (owner != null) {
                    failed.putIfAbsent(owner, String.valueOf(e.getMessage()));
                }
                tableNames = keptTables(schema);
                if (tableNames.isEmpty()) {
                    unavailable.put(schema.toLowerCase(Locale.ROOT), String.valueOf(e.getMessage()));
                }
                LOGGER.warn("node={} views=keepSchema schema={} tables={} reason=catalog error='{}'",
                        LogUtil.node(), schema, tableNames.size(), e.getMessage());
            }
            if (catalogSchema && !tableNames.isEmpty()) {
                unavailable.remove(schema.toLowerCase(Locale.ROOT));
            }
            for (String table : tableNames) {
                String key = schema + "\u0000" + table;
                TableViews views;
                if (catalogSchema) {
                    TableViews previous = tables.get(key);
                    if (owner != null && failed.containsKey(owner)) {
                        views = previous;
                    } else {
                        try {
                            views = catalogTableViews(schema, table, previous);
                        } catch (IOException e) {
                            if (owner != null) {
                                failed.putIfAbsent(owner, String.valueOf(e.getMessage()));
                            }
                            LOGGER.warn("node={} views={} table={}.{} reason=catalog error='{}'",
                                    LogUtil.node(), previous != null ? "keepTable" : "skipTable",
                                    schema, table, e.getMessage());
                            views = previous;
                        }
                    }
                    if (views == null) {
                        continue;
                    }
                    if (views == ARROW_STREAM) {
                        seen.put(key, views);
                        streams.put(key.toLowerCase(Locale.ROOT), List.of(schema, table));
                        continue;
                    }
                } else {
                    List<DataFile> files = parquetAdapter.tableFiles(schema, table);
                    if (files.isEmpty()) {
                        continue;
                    }
                    String fingerprint = ParquetAdapter.filesFingerprint(files);
                    views = tables.get(key);
                    if (views == null || !views.fingerprint().equals(fingerprint)) {
                        views = describe(schema, table, files, fingerprint);
                    }
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
        streamTables = Map.copyOf(streams);
        long version = (data.hashCode() * 31L) + empty.hashCode();
        if (snapshot == null || snapshot.version() != version) {
            snapshot = new Snapshot(version, List.copyOf(data), List.copyOf(empty),
                    List.copyOf(drops));
            LOGGER.info("node={} views=refreshed tables={} version={}",
                    LogUtil.node(), seen.size(), Long.toHexString(version));
        }
        refreshedAtNanos = now;
        LogUtil.logTiming(t, "views.refresh", "tables=" + seen.size());
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
     * Builds the views of a table of another catalog (Iceberg): DuckDB's iceberg extension reads
     * the snapshot's metadata file, which pins the files, deletes and columns. A table that
     * cannot be read is left out (logged), so that one broken catalog table does not stop the
     * complete answers of all others.
     *
     * @param schema schema name
     * @param table table name
     * @param previous views made before, or null
     * @return views, or null when the table is left out
     * @throws IOException when the catalog cannot be read
     */
    private TableViews catalogTableViews(String schema, String table, TableViews previous)
            throws IOException {
        try {
            Optional<CatalogTables.Resolved> resolved = parquetAdapter.catalogTables()
                    .current(schema, table);
            if (resolved.isEmpty()) {
                return null;
            }
            TableSnapshot snapshot = resolved.get().snapshot();
            if (snapshot.read() instanceof ReadSpec.ArrowStream) {
                if (snapshot.columns().isEmpty()) {
                    LOGGER.warn("node={} views=skipTable table={}.{} reason=noColumns",
                            LogUtil.node(), schema, table);
                    return null;
                }
                return ARROW_STREAM;
            }
            String fingerprint = "catalog:" + snapshot.fingerprint();
            if (previous != null && previous.fingerprint().equals(fingerprint)) {
                return previous;
            }
            String source = snapshotSource(schema, table, snapshot);
            if (source == null) {
                LOGGER.warn("node={} views=skipTable table={}.{} reason=noColumns", LogUtil.node(),
                        schema, table);
                return null;
            }
            return describeSource(schema, table, source, fingerprint);
        } catch (SQLException | RuntimeException e) {
            LOGGER.warn("node={} views=skipTable table={}.{} reason=catalog error='{}'",
                    LogUtil.node(), schema, table, e.getMessage());
            return null;
        }
    }

    /**
     * Names the FROM source of a catalog table's snapshot for complete answers.
     *
     * @param schema schema name, for the log
     * @param table table name, for the log
     * @param snapshot the snapshot
     * @return FROM source, or null when the snapshot declares no columns and has no metadata
     *         DuckDB could read them from
     */
    private static String snapshotSource(String schema, String table, TableSnapshot snapshot) {
        if (snapshot.read() instanceof ReadSpec.AttachedTable attached) {
            return attachedSource(attached, snapshot);
        }
        if (snapshot.read() instanceof ReadSpec.IcebergMetadata iceberg) {
            String source = icebergSource(iceberg.metadataLocation(), snapshot);
            if (source.startsWith("(SELECT * FROM iceberg_scan(")) {
                LOGGER.info("node={} views=icebergAllColumnsRead table={}.{}"
                        + " reason=equalityDeletes", LogUtil.node(), schema, table);
            } else if (!source.startsWith("iceberg_scan(")) {
                LOGGER.info("node={} views=icebergDeletesApplied table={}.{}", LogUtil.node(),
                        schema, table);
            }
            return source;
        }
        if (snapshot.columns().isEmpty()) {
            return null;
        }
        return snapshot.files().isEmpty() ? declaredColumnsSource(snapshot)
                : CatalogSources.filesSource(snapshot, snapshot.files());
    }

    /**
     * Names the FROM source of a table of an attached database: the declared columns of the
     * remote table, or all of them when the catalog declares none.
     *
     * @param attached the attached table
     * @param snapshot snapshot with its declared columns
     * @return FROM source
     */
    public static String attachedSource(ReadSpec.AttachedTable attached, TableSnapshot snapshot) {
        String remote = DuckDbAdapter.quoteIdentifier(attached.database()) + "."
                + DuckDbAdapter.quoteIdentifier(attached.schema()) + "."
                + DuckDbAdapter.quoteIdentifier(attached.table());
        if (snapshot.columns().isEmpty()) {
            return remote;
        }
        List<String> columns = new ArrayList<>();
        for (ColumnSpec column : snapshot.columns()) {
            columns.add(DuckDbAdapter.quoteIdentifier(column.name()));
        }
        return "(SELECT " + String.join(", ", columns) + " FROM " + remote + ")";
    }

    /**
     * Names the FROM source of a time-travel snapshot: its own data files, read by field ID with
     * the snapshot's columns and deletes like partial answers read them. A snapshot whose files
     * cannot be read one by one (deletion vectors, nested column renames) is read by DuckDB's
     * iceberg_scan of that snapshot ({@link #icebergSnapshotSource}); {@link #travelViews} then
     * refuses it unless DuckDB reads it with the snapshot's own columns.
     *
     * @param resolved the snapshot with its catalog
     * @return FROM source
     * @throws IllegalArgumentException when the snapshot cannot be read this way
     */
    private static TravelSource travelSource(CatalogTables.Resolved resolved) {
        TableSnapshot snapshot = resolved.snapshot();
        String which = "Snapshot " + snapshot.snapshotId() + " of " + snapshot.name();
        if (snapshot.columns().isEmpty()) {
            throw new IllegalArgumentException(which + " declares no columns");
        }
        if (snapshot.files().isEmpty()) {
            return new TravelSource(declaredColumnsSource(snapshot), false);
        }
        String reason = CatalogTables.completeReason(resolved);
        if (reason == null) {
            return new TravelSource(ExecutionService.catalogFilesSource(snapshot, snapshot.files()),
                    false);
        }
        if (snapshot.read() instanceof ReadSpec.IcebergMetadata iceberg) {
            LOGGER.info("node={} views=icebergSnapshotRead table={} snapshot={} reason={}",
                    LogUtil.node(), snapshot.name(), snapshot.snapshotId(), reason);
            return new TravelSource(icebergSnapshotSource(iceberg.metadataLocation(), snapshot),
                    true);
        }
        throw new IllegalArgumentException(which + " cannot be read by time travel: " + reason);
    }

    /**
     * Names DuckDB's scan of one snapshot of an Iceberg table, for snapshots the server cannot
     * read file by file. Equality deletes get the all-columns read of {@link #icebergSource}.
     *
     * @param metadataLocation a metadata file that lists the snapshot
     * @param snapshot the snapshot; its ID is the Iceberg snapshot ID, then a dot and the
     *        schema ID
     * @return {@code iceberg_scan(..., snapshot_from_id => ...)} or a subquery over it
     * @throws IllegalArgumentException when the snapshot ID names no Iceberg snapshot
     */
    static String icebergSnapshotSource(String metadataLocation, TableSnapshot snapshot) {
        String id = snapshot.snapshotId();
        int dot = id.indexOf('.');
        long icebergId;
        try {
            icebergId = Long.parseLong(dot < 0 ? id : id.substring(0, dot));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Snapshot " + id + " of " + snapshot.name()
                    + " cannot be read by time travel: not an Iceberg snapshot ID", e);
        }
        String scan = "iceberg_scan(" + DuckDbAdapter.sqlStringLiteral(metadataLocation)
                + ", snapshot_from_id => " + icebergId + ")";
        return equalityDeletes(snapshot) ? allColumnsRead(scan, snapshot) : scan;
    }

    /**
     * Checks that DuckDB reads a time-travel source with the snapshot's declared columns: names
     * and types in order, nested field names included. iceberg_scan may use another schema of
     * the table than the snapshot's, which would answer with the wrong columns.
     *
     * @param conn DuckDB connection
     * @param source FROM source
     * @param snapshot the snapshot
     * @throws SQLException when DuckDB cannot describe the source
     * @throws IllegalArgumentException when the columns differ
     */
    static void requireDeclaredColumns(Connection conn, String source, TableSnapshot snapshot)
            throws SQLException {
        List<String> read = new ArrayList<>();
        try (Statement s = conn.createStatement();
                ResultSet columns = s.executeQuery("DESCRIBE SELECT * FROM " + source)) {
            while (columns.next()) {
                read.add(columns.getString("column_name") + " " + columns.getString("column_type"));
            }
        }
        List<String> types = new ArrayList<>();
        for (ColumnSpec column : snapshot.columns()) {
            types.add("typeof(CAST(NULL AS " + column.duckDbType() + "))");
        }
        List<String> declared = new ArrayList<>();
        try (Statement s = conn.createStatement();
                ResultSet row = s.executeQuery("SELECT " + String.join(", ", types))) {
            row.next();
            for (int i = 0; i < snapshot.columns().size(); i++) {
                declared.add(snapshot.columns().get(i).name() + " " + row.getString(i + 1));
            }
        }
        if (!read.equals(declared)) {
            throw new IllegalArgumentException("Snapshot " + snapshot.snapshotId() + " of "
                    + snapshot.name() + " cannot be read by time travel: DuckDB reads it with"
                    + " the columns " + read + ", the snapshot declares " + declared);
        }
    }

    /**
     * Replaces the table references of a client's SQL that carry a time-travel clause
     * ({@link TimeTravelClause}) by a read of the snapshot they name, so that DuckDB runs the
     * query over that snapshot: its data files, deletes and columns (see {@link #travelSource}). A reference keeps its table name as its alias unless the
     * client gave one. SQL without a clause is returned unchanged.
     *
     * @param sql client SQL
     * @param describe whether the text is for describing (no rows, no file is read)
     * @return SQL DuckDB can run
     * @throws java.io.UncheckedIOException when a catalog cannot be read
     * @throws SQLException when DuckDB cannot read a snapshot's column types
     * @throws IllegalArgumentException when a clause is malformed, names a table outside an
     *         Iceberg catalog, or a point the table has no snapshot for
     */
    public String timeTravel(String sql, boolean describe) throws SQLException {
        List<TimeTravelClause.Reference> references = TimeTravelClause.find(sql);
        if (references.isEmpty()) {
            return sql;
        }
        Map<TimeTravelClause.Reference, String> bodies = new HashMap<>();
        for (TimeTravelClause.Reference reference : references) {
            List<String> parts = reference.nameParts();
            if (parts.size() != 2) {
                throw new IllegalArgumentException("Time travel needs the table named with its"
                        + " schema (schema.table), got " + String.join(".", parts));
            }
            String schema = parts.get(0);
            String table = parts.get(1);
            String name = schema + "." + table;
            if (!parquetAdapter.isCatalogSchema(schema)) {
                throw new IllegalArgumentException("Time travel (FOR VERSION/TIMESTAMP AS OF) is"
                        + " not supported for " + name + ": only tables of an Iceberg catalog"
                        + " have snapshots");
            }
            CatalogTables.Resolved resolved;
            try {
                resolved = parquetAdapter.catalogTables().asOf(schema, table, reference.point())
                        .orElseThrow(() -> new IllegalArgumentException("Catalog Error: Table "
                                + name + " does not exist"));
            } catch (IOException e) {
                // A catalog outage, not the client's SQL (UNAVAILABLE).
                throw new java.io.UncheckedIOException(e);
            }
            TravelViews views = travelViews(schema, table, resolved);
            LOGGER.debug("node={} views=timeTravel table={} point={} snapshot={}", LogUtil.node(),
                    name, reference.point(), resolved.snapshot().snapshotId());
            bodies.put(reference, "(" + (describe ? views.empty() : views.data()) + ")"
                    + (reference.aliased() ? "" : " AS " + DuckDbAdapter.quoteIdentifier(table)));
        }
        return TimeTravelClause.rewrite(sql, references, bodies::get);
    }

    /**
     * Replaces the references of a client's SQL to catalog tables read as Arrow streams (named
     * with their schema, see {@link TableReferences}) by a source of their own: for a run, one
     * stream per reference with the columns the query names (all of them when it may read the
     * whole row) and, when the query reads only that one table, the simple conditions of its
     * WHERE clause as hints; for describing, the declared columns and no rows. Such tables have
     * no view, so a reference that is not replaced fails as a missing table. SQL without such a
     * reference is returned unchanged.
     *
     * @param sql client SQL (time-travel clauses already replaced)
     * @param scans the query's streams, or null to describe
     * @return SQL DuckDB can run; a run must prepare it once and never cache it
     * @throws java.io.UncheckedIOException when a catalog cannot be read
     * @throws IOException when a catalog cannot open a stream
     * @throws SQLException when DuckDB refuses a stream or the views cannot be refreshed
     */
    public String arrowStreams(String sql, ArrowStreamScans scans) throws IOException, SQLException {
        current();
        Map<String, List<String>> streams = streamTables;
        if (streams.isEmpty()) {
            return sql;
        }
        List<TableReferences.Reference> references = TableReferences.find(sql,
                (schema, table) -> streams.containsKey((schema + "\u0000" + table)
                        .toLowerCase(Locale.ROOT)));
        if (references.isEmpty()) {
            return sql;
        }
        Map<String, CatalogTables.Resolved> resolved = new HashMap<>();
        List<String> sources = new ArrayList<>(references.size());
        String filter = references.size() == 1 && scans != null
                ? singleTableFilter(sql, references.get(0)) : null;
        for (TableReferences.Reference reference : references) {
            String key = (reference.schema() + "\u0000" + reference.table()).toLowerCase(Locale.ROOT);
            List<String> names = streams.get(key);
            CatalogTables.Resolved table = resolved.get(key);
            if (table == null) {
                try {
                    table = parquetAdapter.catalogTables().current(names.get(0), names.get(1))
                            .orElseThrow(() -> new IllegalArgumentException("Catalog Error: Table "
                                    + names.get(0) + "." + names.get(1) + " does not exist"));
                } catch (IOException e) {
                    // A catalog outage, not the client's SQL (UNAVAILABLE).
                    throw new java.io.UncheckedIOException(e);
                }
                if (!(table.snapshot().read() instanceof ReadSpec.ArrowStream)) {
                    throw new IllegalStateException("The catalog of " + names.get(0) + "."
                            + names.get(1) + " changed how it is read; run the query again");
                }
                resolved.put(key, table);
            }
            TableSnapshot snapshot = table.snapshot();
            String source;
            if (scans == null || snapshot.files().isEmpty()) {
                source = declaredColumnsSource(snapshot);
            } else {
                ArrowScan scan = ArrowScanRequests.of(snapshot, sql, filter, reference.wholeRow());
                source = scans.source(table.entry().catalog(), snapshot, snapshot.files(), scan);
            }
            sources.add(source + (reference.aliased() ? ""
                    : " AS " + DuckDbAdapter.quoteIdentifier(reference.table())));
        }
        LOGGER.debug("node={} views=arrowStreams references={} describe={}", LogUtil.node(),
                references.size(), scans == null);
        return TableReferences.rewrite(sql, references, sources);
    }

    /**
     * Replaces the references of a client's SQL to views of a catalog (named with their schema,
     * see {@link TableReferences}) by their queries, {@code (<view query>) AS "<name>"} unless
     * the client gave an alias, again and again until no view is left (views over views), so
     * that time travel, Arrow streams and the DuckDB views see only tables. An unqualified view
     * name is not replaced and fails as a missing table. SQL that names no view of a catalog
     * schema is returned unchanged without listing views.
     *
     * @param sql client SQL
     * @return the SQL with its views replaced, and what they read
     * @throws IOException when the schemas cannot be listed
     * @throws IllegalArgumentException when a view is read at an earlier snapshot or views are
     *         nested deeper than {@value #MAX_VIEW_ROUNDS}
     */
    public Expanded catalogViews(String sql) throws IOException {
        CatalogTables catalogs = parquetAdapter.catalogTables();
        if (sql == null || catalogs == null || catalogs.isEmpty()
                || TableReferences.find(sql, (schema, table) -> parquetAdapter
                        .isCatalogSchema(schema)).isEmpty()) {
            return new Expanded(sql, List.of());
        }
        Map<String, ShownView> shown = shownViews();
        if (shown.isEmpty()) {
            return new Expanded(sql, List.of());
        }
        String text = sql;
        Set<TableName> sources = new LinkedHashSet<>();
        for (int round = 0;; round++) {
            for (TimeTravelClause.Reference travelled : TimeTravelClause.find(text)) {
                List<String> parts = travelled.nameParts();
                if (parts.size() == 2 && shown.containsKey(viewKey(parts.get(0), parts.get(1)))) {
                    throw new IllegalArgumentException("Time travel (FOR VERSION/TIMESTAMP AS OF)"
                            + " is not supported for the view " + String.join(".", parts));
                }
            }
            List<TableReferences.Reference> references = TableReferences.find(text,
                    (schema, table) -> shown.containsKey(viewKey(schema, table)));
            if (references.isEmpty()) {
                break;
            }
            if (round == MAX_VIEW_ROUNDS) {
                throw new IllegalArgumentException("Views nested deeper than " + MAX_VIEW_ROUNDS
                        + " cannot be read");
            }
            List<String> bodies = new ArrayList<>(references.size());
            for (TableReferences.Reference reference : references) {
                CatalogView view = shown.get(viewKey(reference.schema(), reference.table())).view();
                sources.addAll(view.sources());
                bodies.add("(" + view.sql() + ")" + (reference.aliased() ? ""
                        : " AS " + DuckDbAdapter.quoteIdentifier(reference.table())));
            }
            text = TableReferences.rewrite(text, references, bodies);
        }
        LOGGER.debug("node={} views=catalogViews sources={}", LogUtil.node(), sources.size());
        return new Expanded(text, List.copyOf(sources));
    }

    /**
     * Lists the views of every catalog schema, as queries may name them.
     *
     * @return schema name to its views sorted by name; empty without catalogs
     * @throws IOException when the schemas cannot be listed
     */
    public Map<String, List<CatalogView>> listedViews() throws IOException {
        CatalogTables catalogs = parquetAdapter.catalogTables();
        if (catalogs == null || catalogs.isEmpty()) {
            return Map.of();
        }
        Map<String, List<CatalogView>> result = new TreeMap<>();
        for (ShownView shown : shownViews().values()) {
            result.computeIfAbsent(shown.schema(), k -> new ArrayList<>()).add(shown.view());
        }
        result.values().forEach(views -> views.sort(java.util.Comparator.comparing(
                CatalogView::name)));
        return result;
    }

    /**
     * Returns the views of every catalog schema, listed again at most once a minute. A view whose
     * name is also a table of its schema is left out (the table wins), as is one whose query names
     * a table it does not declare as a source. A catalog that cannot be read keeps the views
     * listed before; one that fails is not asked again for its other schemas in the same listing.
     *
     * @return lower-case "schema\0view" to the view
     * @throws IOException when the schemas cannot be listed
     */
    private Map<String, ShownView> shownViews() throws IOException {
        synchronized (viewLock) {
            long now = System.nanoTime();
            if (viewsListedAtNanos != null && now - viewsListedAtNanos < REFRESH_NANOS) {
                return shownViews;
            }
            CatalogTables catalogs = parquetAdapter.catalogTables();
            Map<String, ShownView> next = new HashMap<>();
            Map<CatalogTables.Entry, String> failed = new HashMap<>();
            for (String schema : parquetAdapter.getSchemas(null).keySet()) {
                CatalogTables.Entry owner = catalogs.owner(schema).orElse(null);
                if (owner == null) {
                    continue;
                }
                List<ShownView> found = new ArrayList<>();
                try {
                    if (failed.containsKey(owner)) {
                        throw new IllegalStateException(failed.get(owner));
                    }
                    List<CatalogView> views = catalogs.views(schema);
                    Set<String> tableNames = new java.util.HashSet<>();
                    if (!views.isEmpty()) {
                        for (String table : catalogs.tables(schema)) {
                            tableNames.add(table.toLowerCase(Locale.ROOT));
                        }
                    }
                    for (CatalogView view : views) {
                        String reason = tableNames.contains(view.name().toLowerCase(Locale.ROOT))
                                ? "nameOfTable" : undeclaredSource(view);
                        if (reason == null) {
                            found.add(new ShownView(schema, view));
                        } else if (loggedViews.add(schema + "." + view.name() + ":" + reason)) {
                            LOGGER.warn("node={} views=skipView view={}.{} reason='{}'",
                                    LogUtil.node(), schema, view.name(), reason);
                        }
                    }
                } catch (IOException | RuntimeException e) {
                    failed.putIfAbsent(owner, String.valueOf(e.getMessage()));
                    found.clear();
                    for (ShownView kept : shownViews.values()) {
                        if (kept.schema().equals(schema)) {
                            found.add(kept);
                        }
                    }
                    LOGGER.warn("node={} views=keepViews schema={} views={} reason=catalog"
                            + " error='{}'", LogUtil.node(), schema, found.size(), e.getMessage());
                }
                for (ShownView view : found) {
                    next.put(viewKey(schema, view.view().name()), view);
                }
            }
            shownViews = Map.copyOf(next);
            viewsListedAtNanos = now;
            return shownViews;
        }
    }

    /**
     * Tells why a view's query cannot be shown: it names a table in a FROM position that is not
     * among its declared sources (which the server checks access to).
     *
     * @param view the view
     * @return reason, or null when every table it names is declared
     */
    static String undeclaredSource(CatalogView view) {
        Set<String> declared = new java.util.HashSet<>();
        for (TableName source : view.sources()) {
            declared.add(viewKey(source.schema(), source.table()));
        }
        for (TableReferences.Reference reference : TableReferences.find(view.sql(),
                (schema, table) -> true)) {
            if (!declared.contains(viewKey(reference.schema(), reference.table()))) {
                return "reads " + reference.schema() + "." + reference.table()
                        + ", which is not a declared source";
            }
        }
        return null;
    }

    /**
     * Builds the key of a view or table name.
     *
     * @param schema schema name in any case
     * @param name view or table name in any case
     * @return lower-case "schema\0name"
     */
    private static String viewKey(String schema, String name) {
        return (schema + "\u0000" + name).toLowerCase(Locale.ROOT);
    }

    /**
     * Returns the WHERE clause of a query that reads one table and nothing else, for the hints of
     * that table's stream: a filter of a join, a union or a subquery may not hold for its rows.
     *
     * @param sql client SQL
     * @param reference the query's only reference to a stream table
     * @return the WHERE clause as the query parser renders it, or null
     */
    static String singleTableFilter(String sql, TableReferences.Reference reference) {
        if (!TableReferences.singleSelect(sql)) {
            return null;
        }
        try {
            ParquetQueryParser pq = ParquetQueryParser.parse(sql);
            if (pq.isJoin || !reference.schema().equalsIgnoreCase(pq.schema)
                    || !reference.table().equalsIgnoreCase(pq.table)) {
                return null;
            }
            return pq.filter;
        } catch (RuntimeException e) {
            return null;   // no hints: DuckDB applies every filter anyway
        }
    }

    /**
     * Returns the SELECTs of a time-travel snapshot, described once per snapshot.
     *
     * @param schema schema name
     * @param table table name
     * @param resolved the snapshot with its catalog
     * @return the SELECTs
     * @throws SQLException when DuckDB cannot read its column types
     */
    private TravelViews travelViews(String schema, String table, CatalogTables.Resolved resolved)
            throws SQLException {
        TableSnapshot snapshot = resolved.snapshot();
        String key = schema + "\u0000" + table + "\u0000" + snapshot.fingerprint();
        synchronized (travel) {
            TravelViews cached = travel.get(key);
            if (cached != null) {
                return cached;
            }
        }
        TravelSource source = travelSource(resolved);
        TravelViews views;
        synchronized (this) {
            if (source.scan()) {
                requireDeclaredColumns(duckDbAdapter.connection(), source.sql(), snapshot);
            }
            TableViews described = describeSource(schema, table, source.sql(), key);
            String prefix = "CREATE OR REPLACE VIEW " + DuckDbAdapter.quoteIdentifier(schema) + "."
                    + DuckDbAdapter.quoteIdentifier(table) + " AS ";
            views = new TravelViews(described.data().substring(prefix.length()),
                    described.empty().substring(prefix.length()));
        }
        synchronized (travel) {
            travel.put(key, views);
        }
        return views;
    }

    /**
     * Tells why a query that names a catalog schema has no views of it: the schema's catalog
     * could not be read when the views were made, and no earlier views of it exist.
     *
     * @param query client SQL
     * @return the catalog's error, or null when every schema the query names has its views
     */
    public String unavailableCatalog(String query) {
        if (unavailable.isEmpty() || query == null) {
            return null;
        }
        String lower = query.toLowerCase(Locale.ROOT);
        for (Map.Entry<String, String> entry : unavailable.entrySet()) {
            if (java.util.regex.Pattern.compile("(?<![a-z0-9_])\"?" + java.util.regex.Pattern.quote(
                    entry.getKey()) + "\"?\\s*\\.").matcher(lower).find()) {
                return entry.getValue();
            }
        }
        return null;
    }

    /**
     * Lists the tables of a schema that have views from an earlier refresh.
     *
     * @param schema schema name
     * @return table names, sorted
     */
    private List<String> keptTables(String schema) {
        String prefix = schema + "\u0000";
        List<String> result = new ArrayList<>();
        for (String key : tables.keySet()) {
            if (key.startsWith(prefix)) {
                result.add(key.substring(prefix.length()));
            }
        }
        result.sort(String::compareTo);
        return result;
    }

    /**
     * Names a source with a snapshot's declared columns and no rows, for a table without data
     * files.
     *
     * @param snapshot snapshot with its columns
     * @return FROM source
     */
    public static String declaredColumnsSource(TableSnapshot snapshot) {
        List<String> typed = new ArrayList<>();
        for (ColumnSpec column : snapshot.columns()) {
            typed.add("CAST(NULL AS " + column.duckDbType() + ") AS "
                    + DuckDbAdapter.quoteIdentifier(column.name()));
        }
        return "(SELECT " + String.join(", ", typed) + " WHERE false)";
    }

    /**
     * Names the FROM source of a catalog table's complete-answer view. A snapshot with equality
     * deletes is read by {@link CatalogSources#deletesAppliedSource}, which applies its deletes
     * with read_parquet and anti-joins. Where that cannot be used (deletion vectors, columns
     * without field ID), DuckDB's iceberg_scan reads it. DuckDB 1.4.5's
     * iceberg_scan applies equality deletes only when it reads their columns: a query that does
     * not select them fails ("Equality deletes need the relevant columns to be selected"), and one
     * that only filters on them can fail with an INTERNAL error that invalidates the database
     * (Gravitino stand, Iceberg Java equality deletes). With position deletes and renamed columns
     * too, any projection that leaves out a column can fail the same way (an aggregate or filter
     * on one column only), so a snapshot with equality deletes is read with all its columns: a
     * filter DuckDB cannot push into the scan and that is never false references every column,
     * and a LIMIT no table can reach keeps the query's own filters above the scan (a filter pushed
     * into the scan, also through a MATERIALIZED CTE, applies the deletes wrongly). Tables
     * without equality deletes are read by the plain scan.
     *
     * @param metadataLocation metadata file of the snapshot
     * @param snapshot the snapshot (its delete files tell whether there are equality deletes)
     * @return {@code iceberg_scan(...)}, the deletes-applied source, or a subquery over
     *         iceberg_scan that reads every column
     */
    public static String icebergSource(String metadataLocation, TableSnapshot snapshot) {
        String scan = icebergScan(metadataLocation);
        if (!equalityDeletes(snapshot)) {
            return scan;
        }
        String applied = CatalogSources.deletesAppliedSource(snapshot, snapshot.files());
        if (applied != null) {
            return applied;
        }
        return allColumnsRead(scan, snapshot);
    }

    /**
     * Tells whether a snapshot declares columns and has equality deletes.
     *
     * @param snapshot the snapshot
     * @return true when iceberg_scan must read all its columns
     */
    private static boolean equalityDeletes(TableSnapshot snapshot) {
        return !snapshot.columns().isEmpty() && snapshot.files().stream()
                .flatMap(file -> file.deletes().stream())
                .anyMatch(delete -> delete.kind() == DeleteFile.Kind.EQUALITY);
    }

    /**
     * Wraps an iceberg_scan so that it always reads every column and keeps the query's filters
     * above the scan (see {@link #icebergSource}).
     *
     * @param scan iceberg_scan call
     * @param snapshot the snapshot (its declared columns)
     * @return subquery over the scan
     */
    private static String allColumnsRead(String scan, TableSnapshot snapshot) {
        List<String> keep = new ArrayList<>();
        for (ColumnSpec column : snapshot.columns()) {
            keep.add("eq_keep." + DuckDbAdapter.quoteIdentifier(column.name()));
        }
        return "(SELECT * FROM " + scan + " AS eq_keep WHERE hash(" + String.join(", ", keep)
                + ") IS NOT NULL LIMIT " + Long.MAX_VALUE + ")";
    }

    /**
     * Names DuckDB's scan of an Iceberg table snapshot.
     *
     * @param metadataLocation metadata file of the snapshot
     * @return {@code iceberg_scan('<metadata>')}
     */
    public static String icebergScan(String metadataLocation) {
        return "iceberg_scan(" + DuckDbAdapter.sqlStringLiteral(metadataLocation) + ")";
    }

    /**
     * Builds the view definitions of a table read from a FROM source.
     *
     * @param schema schema name
     * @param table table name
     * @param source FROM source
     * @param fingerprint what the definitions were made for
     * @return view definitions
     * @throws SQLException when DuckDB cannot describe the source
     */
    private TableViews describeSource(String schema, String table, String source,
            String fingerprint) throws SQLException {
        List<String> typed = new ArrayList<>();
        Connection conn = duckDbAdapter.connection();
        try (Statement s = conn.createStatement();
                ResultSet columns = s.executeQuery("DESCRIBE SELECT * FROM " + source)) {
            while (columns.next()) {
                String name = DuckDbAdapter.quoteIdentifier(columns.getString("column_name"));
                typed.add("CAST(NULL AS " + columns.getString("column_type") + ") AS " + name);
            }
        }
        String view = DuckDbAdapter.quoteIdentifier(schema) + "." + DuckDbAdapter.quoteIdentifier(table);
        String data = "CREATE OR REPLACE VIEW " + view + " AS SELECT * FROM " + source;
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

    /**
     * Names the DuckDB signed type that holds every value of an unsigned Arrow integer type, the
     * same as {@link #widerSignedType(String)} does for the DuckDB type of the column.
     *
     * @param type Arrow type of a Parquet column
     * @return wider signed DuckDB type, or null when the type is not an unsigned integer
     */
    public static String widerSignedType(ArrowType type) {
        if (!(type instanceof ArrowType.Int integer) || integer.getIsSigned()) {
            return null;
        }
        return switch (integer.getBitWidth()) {
            case 8 -> "SMALLINT";
            case 16 -> "INTEGER";
            case 32 -> "BIGINT";
            case 64 -> "DECIMAL(20,0)";
            default -> null;
        };
    }

    /**
     * Returns a schema with its unsigned integer columns as DuckDB streams them once widened.
     *
     * @param schema schema read from Parquet footers
     * @return schema with uint8/16/32 as int16/32/64 and uint64 as decimal128(20,0)
     */
    public static Schema widenUnsigned(Schema schema) {
        List<Field> fields = new ArrayList<>(schema.getFields().size());
        boolean changed = false;
        for (Field field : schema.getFields()) {
            ArrowType wider = widerArrowType(field.getType());
            if (wider == null) {
                fields.add(field);
            } else {
                fields.add(new Field(field.getName(), new FieldType(field.isNullable(), wider,
                        field.getDictionary(), field.getMetadata()), field.getChildren()));
                changed = true;
            }
        }
        return changed ? new Schema(fields, schema.getCustomMetadata()) : schema;
    }

    /**
     * Names the Arrow type DuckDB streams for a widened unsigned integer.
     *
     * @param type Arrow type
     * @return wider signed Arrow type, or null when the type is not an unsigned integer
     */
    private static ArrowType widerArrowType(ArrowType type) {
        if (!(type instanceof ArrowType.Int integer) || integer.getIsSigned()) {
            return null;
        }
        return switch (integer.getBitWidth()) {
            case 8 -> new ArrowType.Int(16, true);
            case 16 -> new ArrowType.Int(32, true);
            case 32 -> new ArrowType.Int(64, true);
            case 64 -> new ArrowType.Decimal(20, 0, 128);
            default -> null;
        };
    }
}
