package pro.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import pro.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.metrics.CacheMetrics;
import pro.surpin.data.arrowflight.server.services.ParquetQueryParser;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.model.ExecutionPath;
import pro.surpin.data.arrowflight.server.model.ExecutionPathTracker;
import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.server.catalog.ArrowScanRequests;
import pro.surpin.data.arrowflight.server.catalog.ArrowStreamScans;
import pro.surpin.data.arrowflight.server.catalog.CatalogFileKey;
import pro.surpin.data.arrowflight.server.catalog.CatalogLocations;
import pro.surpin.data.arrowflight.server.catalog.CatalogSources;
import pro.surpin.data.arrowflight.server.catalog.CatalogTables;
import pro.surpin.data.arrowflight.spi.catalog.ArrowScan;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.storage.DataFile;
import pro.surpin.data.arrowflight.server.storage.DataStore;
import pro.surpin.data.arrowflight.server.storage.LocalCopyCache;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;

import java.math.BigDecimal;

/**
 * Orchestrates query execution through DuckDB.
 */
public final class ExecutionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionService.class);
    private static final String FILES_TIMING_PREFIX = "files=";
    /** Stands for an Arrow stream's source while the columns a query names are collected. */
    private static final String ARROW_SOURCE_PLACEHOLDER = "__kestrel_arrow_source";
    private static final long SCHEMA_TTL_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final int SCHEMA_CACHE_CAPACITY = 1024;
    /** Schema name standing for a copied table in the check that every reference was replaced. */
    private static final String COPY_CHECK_SCHEMA = "__localcopy_check";
    /** Name prefix of the Arrow views holding the part answers of batches of copies. */
    private static final String PARTS_VIEW_PREFIX = "__kestrel_copy_parts_";
    private static final java.util.concurrent.atomic.AtomicLong PARTS_VIEWS =
            new java.util.concurrent.atomic.AtomicLong();

    /**
     * A query's result schema with its refresh deadline.
     *
     * @param schema result schema
     * @param expiresAtNanos monotonic refresh deadline
     */
    private record CachedSchema(Schema schema, long expiresAtNanos) {
    }

    private final Map<String, CachedSchema> schemaCache = Collections.synchronizedMap(
            new LinkedHashMap<>(64, 0.75f, true));

    private final ParquetAdapter parquetAdapter;
    private final DuckDbAdapter duckDbAdapter;
    private final MetadataService metadataService;
    private final ExecutorService ioPool;
    private final AppConfig appConfig;
    private final CatalogViews catalogViews;
    /** Whether unsigned integer columns are read as wider signed types, in both result modes. */
    private final boolean widenUnsigned;
    /** Node-local copies of files only whole-file holders read; made on first use. */
    private LocalCopyCache localCopyCache;

    /**
     * Creates ExecutionService.
     *
     * @param parquetAdapter  Parquet metadata adapter
     * @param duckDbAdapter   DuckDB adapter
     * @param metadataService schema/metadata service
     * @param appConfig       server configuration
     * @param ioPool          shared I/O thread pool
     */
    public ExecutionService(ParquetAdapter parquetAdapter, DuckDbAdapter duckDbAdapter,
            MetadataService metadataService,
            AppConfig appConfig, ExecutorService ioPool) {
        this.parquetAdapter = parquetAdapter;
        this.duckDbAdapter = duckDbAdapter;
        this.metadataService = metadataService;
        this.appConfig = appConfig;
        this.ioPool = ioPool;
        this.widenUnsigned = appConfig == null || appConfig.widenUnsignedTypes();
        this.catalogViews = new CatalogViews(parquetAdapter, duckDbAdapter, widenUnsigned);
    }

    /**
     * Names some of a table's Parquet files as a DuckDB FROM source with the column types of the
     * complete-answer views: unsigned integer columns widened as {@link CatalogViews} does, so
     * that both result modes stream the types the table metadata announces.
     *
     * @param schema schema name, or null when unknown
     * @param table table name
     * @param duckDbPaths files as DuckDB reads them
     * @return {@code read_parquet([...])}, wrapped in a widening projection when needed
     */
    String tableSource(String schema, String table, List<String> duckDbPaths) {
        String files = DuckDbAdapter.readParquetFromClause(duckDbPaths);
        if (!widenUnsigned || schema == null) {
            return files;
        }
        List<String> replaced = new ArrayList<>();
        for (Field field : parquetAdapter.getTableSchema(schema, table).getFields()) {
            String wider = CatalogViews.widerSignedType(field.getType());
            if (wider != null) {
                String name = DuckDbAdapter.quoteIdentifier(field.getName());
                replaced.add("CAST(" + name + " AS " + wider + ") AS " + name);
            }
        }
        return replaced.isEmpty() ? files
                : "(SELECT * REPLACE (" + String.join(", ", replaced) + ") FROM " + files + ")";
    }

    /**
     * Counts a query as running on this node until the slot is closed; see
     * {@link DuckDbAdapter#enterQuery()}.
     *
     * @return slot to close on the same thread when the query ends
     */
    public DuckDbAdapter.QuerySlot enterQuery() {
        return duckDbAdapter.enterQuery();
    }

    /**
     * Describes a table from its Parquet footers with the column types queries stream.
     *
     * @param schema schema name
     * @param table table name
     * @return column schema
     */
    public Schema footerTableSchema(String schema, String table) {
        Schema footer = metadataService.getTableSchema(schema, table, null);
        return widenUnsigned ? CatalogViews.widenUnsigned(footer) : footer;
    }

    /**
     * Returns the schema of the complete answer to a client's query.
     *
     * @param allocator Arrow buffer allocator
     * @param query     client SQL, run as written by DuckDB over views of all tables
     * @return result schema
     * @throws IllegalArgumentException when the query is refused or DuckDB rejects it
     */
    public Schema completeSchema(BufferAllocator allocator, String query) {
        return completeSchema(allocator, query, null);
    }

    /**
     * Returns the schema of the complete answer to a client's query with parameter values.
     *
     * @param allocator  Arrow buffer allocator
     * @param query      client SQL
     * @param parameters one-row Arrow IPC stream with the values, or null
     * @return result schema
     * @throws IllegalArgumentException when the query is refused or DuckDB rejects it
     */
    public Schema completeSchema(BufferAllocator allocator, String query, byte[] parameters) {
        try {
            List<Object> values = parameters == null ? List.of()
                    : pro.surpin.data.arrowflight.server.adapters.QueryParameters.values(allocator, parameters);
            String sql = catalogViews.arrowStreams(
                    catalogViews.timeTravel(catalogViews.catalogViews(query).sql(), true), null);
            return duckDbAdapter.describeComplete(allocator, sql, values, catalogViews.current());
        } catch (IllegalArgumentException | java.io.UncheckedIOException e) {
            throw e;
        } catch (Exception e) {
            String message = String.valueOf(e.getMessage());
            if (message.contains("Error: ")) {
                unavailableCatalog(query, message, e);
                // DuckDB's own errors (parser, binder, catalog, conversion, not implemented)
                // describe the client's SQL.
                throw new IllegalArgumentException(
                        pro.surpin.data.arrowflight.server.duckdb.DuckDbErrors.errorLine(message), e);
            }
            throw new IllegalStateException("Unable to describe query: " + message, e);
        }
    }

    /**
     * Streams the complete answer to a client's query, computed on this node over all files.
     *
     * @param allocator     Arrow buffer allocator
     * @param query         client SQL
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws Exception on refusal, query or stream failure
     */
    public void readComplete(BufferAllocator allocator, String query,
            FlightProducer.ServerStreamListener listener, boolean startListener)
            throws Exception {
        readComplete(allocator, query, null, listener, startListener);
    }

    /**
     * Streams the complete answer to a client's query with parameter values.
     *
     * @param allocator     Arrow buffer allocator
     * @param query         client SQL
     * @param parameters    one-row Arrow IPC stream with the values, or null
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws Exception on refusal, query or stream failure
     */
    public void readComplete(BufferAllocator allocator, String query, byte[] parameters,
            FlightProducer.ServerStreamListener listener, boolean startListener)
            throws Exception {
        List<Object> values = parameters == null ? List.of()
                : pro.surpin.data.arrowflight.server.adapters.QueryParameters.values(allocator, parameters);
        try {
            String sql = catalogViews.timeTravel(catalogViews.catalogViews(query).sql(), false);
            CatalogViews.Snapshot views = catalogViews.current();
            try (ArrowStreamScans scans = new ArrowStreamScans(duckDbAdapter.connection(),
                    allocator)) {
                String streamed = catalogViews.arrowStreams(sql, scans);
                if (scans.size() > 0) {
                    duckDbAdapter.streamCompleteOnce(allocator, streamed, values, views, listener,
                            startListener);
                } else {
                    duckDbAdapter.streamComplete(allocator, streamed, values, views, listener,
                            startListener);
                }
            }
        } catch (java.sql.SQLException e) {
            String message = String.valueOf(e.getMessage());
            if (isQueryError(message)) {
                unavailableCatalog(query, message, e);
                throw new IllegalArgumentException(
                        pro.surpin.data.arrowflight.server.duckdb.DuckDbErrors.errorLine(message), e);
            }
            throw e;
        }
    }

    /**
     * Replaces the catalog views a client's SQL names by their queries; the complete-answer
     * calls do the same, so a text already expanded stays as it is.
     *
     * @param query client SQL
     * @return the expanded SQL and every table and view the views read
     * @throws IllegalArgumentException when a view cannot be read as written
     * @throws java.io.UncheckedIOException when the schemas cannot be listed
     */
    public CatalogViews.Expanded catalogViews(String query) {
        try {
            return catalogViews.catalogViews(query);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /**
     * Writes the schema in front of the table names a query gives without one, on HDFS read
     * short-circuit only (SASL DataNodes, item 31): there every step that decides where a file
     * is read and what is copied (planning, the readable-here check, local copies, join views)
     * works on {@code schema.table}, and a plain name such as {@code lineitem} must reach them as
     * the table DuckDB reads for it ({@code main.lineitem} is an alias of the one schema with
     * that table). Names in several schemas, unknown names and names the query defines itself
     * stay as written. Elsewhere the query is returned unchanged.
     *
     * @param query client SQL
     * @return the query with its plain table names qualified, or the query itself
     * @throws java.io.UncheckedIOException when the schemas cannot be listed
     */
    public String qualifyPlainNames(String query) {
        if (query == null || !parquetAdapter.wholeFileReadsOnly()) {
            return query;
        }
        String qualified = TableReferences.qualifyPlainNames(query, name -> {
            try {
                return catalogViews.plainOwner(name);
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            } catch (java.sql.SQLException e) {
                throw new java.io.UncheckedIOException(new IOException(e.getMessage(), e));
            }
        });
        if (!qualified.equals(query)) {
            LOGGER.debug("node={} query=qualifiedPlainNames query='{}'", LogUtil.node(), qualified);
        }
        return qualified;
    }

    /**
     * Describes the columns of one table as the complete-answer DuckDB reads it, with the type
     * its catalog declares when it comes from one.
     *
     * @param schema schema name
     * @param table table name
     * @return columns in table order
     * @throws IllegalArgumentException when DuckDB cannot read the table
     * @throws java.io.UncheckedIOException when a catalog cannot be read
     */
    public List<pro.surpin.data.arrowflight.spi.catalog.ColumnSpec> tableColumns(String schema,
            String table) {
        String query = "SELECT * FROM " + DuckDbAdapter.quoteIdentifier(schema) + "."
                + DuckDbAdapter.quoteIdentifier(table);
        try {
            String sql = catalogViews.arrowStreams(catalogViews.timeTravel(query, true), null);
            Map<String, String> described = duckDbAdapter.describeColumns(sql, catalogViews.current());
            Map<String, String> declared = new java.util.HashMap<>();
            CatalogTables catalogs = parquetAdapter.catalogTables();
            if (catalogs != null && !catalogs.isEmpty()) {
                Optional<CatalogTables.Resolved> resolved = catalogs.current(schema, table);
                if (resolved.isPresent()) {
                    for (pro.surpin.data.arrowflight.spi.catalog.ColumnSpec column
                            : resolved.get().snapshot().columns()) {
                        if (column.sourceType() != null) {
                            declared.put(column.name().toLowerCase(java.util.Locale.ROOT),
                                    column.sourceType());
                        }
                    }
                }
            }
            List<pro.surpin.data.arrowflight.spi.catalog.ColumnSpec> columns = new ArrayList<>();
            described.forEach((name, type) -> columns.add(
                    new pro.surpin.data.arrowflight.spi.catalog.ColumnSpec(name, type, true, null,
                            declared.get(name.toLowerCase(java.util.Locale.ROOT)))));
            return columns;
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        } catch (IllegalArgumentException | java.io.UncheckedIOException e) {
            throw e;
        } catch (Exception e) {
            String message = String.valueOf(e.getMessage());
            throw new IllegalArgumentException(
                    pro.surpin.data.arrowflight.server.duckdb.DuckDbErrors.errorLine(message), e);
        }
    }

    /**
     * Lists the views of every catalog schema.
     *
     * @return schema name to its views sorted by name; empty without catalogs
     * @throws java.io.UncheckedIOException when the schemas cannot be listed
     */
    public Map<String, List<pro.surpin.data.arrowflight.spi.catalog.CatalogView>> catalogViewList() {
        try {
            return catalogViews.listedViews();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /**
     * Streams the result of one statement over Arrow batches read on this node (the endpoint
     * answers of a distributed complete answer): the batches are registered on this thread's
     * DuckDB connection under a view name, read once by the statement, and dropped afterwards.
     *
     * @param allocator     Arrow buffer allocator
     * @param parts         batches to read; DuckDB closes the reader once it has read it
     * @param name          view name the statement reads the batches from
     * @param sql           DuckDB statement over the view
     * @param listener      Flight stream listener, started by this method
     * @throws Exception on query or stream failure
     */
    public void readArrowParts(BufferAllocator allocator, org.apache.arrow.vector.ipc.ArrowReader parts,
            String name, String sql, FlightProducer.ServerStreamListener listener) throws Exception {
        Connection connection = duckDbAdapter.connection();
        try (org.apache.arrow.c.ArrowArrayStream stream =
                org.apache.arrow.c.ArrowArrayStream.allocateNew(allocator)) {
            org.apache.arrow.c.Data.exportArrayStream(allocator, parts, stream);
            try {
                connection.unwrap(org.duckdb.DuckDBConnection.class).registerArrowStream(name, stream);
                duckDbAdapter.streamSqlOnce(allocator, sql, listener, true);
            } finally {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("DROP VIEW IF EXISTS " + DuckDbAdapter.quoteIdentifier(name));
                } finally {
                    // A stream DuckDB read is released already; release() then does nothing.
                    stream.release();
                }
            }
        }
    }

    /**
     * Turns DuckDB's "table does not exist" for a schema whose catalog could not be read into
     * that catalog's error, and for a data table whose first file could not be described into
     * that error, so that neither is reported as a missing table.
     *
     * @param query client SQL
     * @param message DuckDB error message
     * @param cause DuckDB error
     * @throws UncheckedIOException when a catalog the query names is unavailable
     */
    private void unavailableCatalog(String query, String message, Exception cause) {
        if (!message.contains("Catalog Error")) {
            return;
        }
        String unavailable = catalogViews.unavailableCatalog(query);
        if (unavailable == null) {
            unavailable = catalogViews.unavailableTable(query, message);
        }
        if (unavailable != null) {
            throw new java.io.UncheckedIOException(new IOException(unavailable, cause));
        }
    }

    /** DuckDB error kinds caused by the client's SQL or values, not by the server. */
    private static final List<String> QUERY_ERROR_KINDS = List.of("Invalid Input Error",
            "Conversion Error", "Out of Range Error", "Binder Error", "Parser Error",
            "Catalog Error", "Not implemented Error", "Mismatch Type Error", "Syntax Error",
            "Constraint Error", "Invalid Error", "Permission Error", "Dependency Error");

    /**
     * Tells whether a DuckDB error message describes a fault of the query or its values.
     *
     * @param message DuckDB error message
     * @return true for a client error
     */
    static boolean isQueryError(String message) {
        String text = message == null ? "" : message.strip();
        return QUERY_ERROR_KINDS.stream().anyMatch(text::startsWith);
    }

    /**
     * Describes the parameters of a client's query.
     *
     * @param query client SQL
     * @return one field per parameter
     * @throws IllegalArgumentException when DuckDB rejects the query
     */
    public Schema completeParameterSchema(String query) {
        try {
            return duckDbAdapter.describeParameters(
                    catalogViews.arrowStreams(catalogViews.timeTravel(
                            catalogViews.catalogViews(query).sql(), true), null),
                    catalogViews.current());
        } catch (IllegalArgumentException | java.io.UncheckedIOException e) {
            throw e;
        } catch (Exception e) {
            String message = String.valueOf(e.getMessage());
            if (message.contains("Error: ")) {
                throw new IllegalArgumentException(
                        pro.surpin.data.arrowflight.server.duckdb.DuckDbErrors.errorLine(message), e);
            }
            throw new IllegalStateException("Unable to describe query parameters: " + message, e);
        }
    }

    /** The catalog of a table a query names cannot be read now (metastore down or hanging). */
    public static final class CatalogUnavailableException extends java.io.UncheckedIOException {
        private static final long serialVersionUID = 1L;

        /**
         * Wraps the catalog's error.
         *
         * @param cause catalog error
         */
        public CatalogUnavailableException(IOException cause) {
            super(cause.getMessage(), cause);
        }
    }

    /**
     * Returns the schema of the batches a query streams, as DuckDB reports it for the same SQL.
     *
     * @param allocator Arrow buffer allocator
     * @param query     SQL query
     * @return result schema
     * @throws IllegalArgumentException if DuckDB rejects the query
     * @throws CatalogUnavailableException when the catalog of the queried table cannot be read
     */
    public Schema resultSchema(BufferAllocator allocator, String query) {
        long now = System.nanoTime();
        CachedSchema cached = schemaCache.get(query);
        if (cached != null && cached.expiresAtNanos() > now) {
            return cached.schema();
        }
        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        Schema schema;
        try {
            schema = describe(allocator, pq);
        } catch (Exception e) {
            String message = String.valueOf(e.getMessage());
            if (isBindError(message)) {
                throw new IllegalArgumentException(
                        pro.surpin.data.arrowflight.server.duckdb.DuckDbErrors.errorLine(message), e);
            }
            if (e instanceof IOException io && !(io instanceof java.io.FileNotFoundException)
                    && !message.startsWith("No Parquet files for table") && pq.schema != null
                    && parquetAdapter.isCatalogSchema(pq.schema)) {
                // The table's catalog cannot be read now: say so (the data directory's
                // metadata below knows nothing of catalog tables: "schema not found").
                throw new CatalogUnavailableException(io);
            }
            // Files may be unreadable from this node; the table metadata still describes them.
            LOGGER.debug("node={} schema=describeFallback error='{}'", LogUtil.node(), message);
            schema = metadataService.getQuerySchema(query);
            if (schema != null && widenUnsigned) {
                schema = CatalogViews.widenUnsigned(schema);
            }
        }
        if (schema != null) {
            cacheSchema(query, schema, now);
        }
        return schema;
    }

    /**
     * Remembers a query's result schema unless the query text is too long to be cached.
     *
     * @param query    SQL query
     * @param schema   result schema
     * @param nowNanos monotonic time of the lookup
     */
    private void cacheSchema(String query, Schema schema, long nowNanos) {
        if (!QueryCacheLimits.cacheable(query)) {
            CacheMetrics.oversize(CacheMetrics.Cache.SCHEMA);
        } else {
            synchronized (schemaCache) {
                schemaCache.put(query, new CachedSchema(schema, nowNanos + SCHEMA_TTL_NANOS));
                if (schemaCache.size() > SCHEMA_CACHE_CAPACITY) {
                    schemaCache.remove(schemaCache.keySet().iterator().next());
                    CacheMetrics.eviction(CacheMetrics.Cache.SCHEMA);
                }
            }
        }
    }

    /**
     * Asks DuckDB for the result schema of a query, binding it against one file per table.
     *
     * @param allocator Arrow buffer allocator
     * @param pq        parsed query
     * @return result schema
     * @throws Exception if the table has no files or DuckDB cannot bind the query
     */
    private Schema describe(BufferAllocator allocator, ParquetQueryParser pq) throws Exception {
        if (pq.isJoin) {
            Connection conn = duckDbAdapter.connection();
            List<String> aliases = new ArrayList<>();
            List<pro.surpin.data.arrowflight.server.duckdb.FooterStub> stubs = new ArrayList<>();
            try {
                if (parquetAdapter.wholeFileReadsOnly()) {
                    createJoinDescribeViews(conn, pq, aliases, stubs);
                } else {
                    createJoinViews(conn, pq, aliases, true, null, null);
                }
                return duckDbAdapter.describeSql(allocator, pq.duckDbSql);
            } finally {
                dropJoinViews(conn, aliases);
                for (pro.surpin.data.arrowflight.server.duckdb.FooterStub stub : stubs) {
                    stub.close();
                }
            }
        }
        if (parquetAdapter.isCatalogSchema(pq.schema)) {
            CatalogTables.Resolved resolved = parquetAdapter.catalogTables()
                    .current(pq.schema, pq.table).orElseThrow(() -> new java.io.FileNotFoundException(
                            "No Parquet files for table " + pq.schema + "." + pq.table));
            return describeCatalog(allocator, pq, resolved);
        }
        try {
            return describeAgainstSample(allocator, pq);
        } catch (Exception e) {
            String message = String.valueOf(e.getMessage());
            if (isQueryError(message) || message.startsWith("No Parquet files for table")) {
                throw e;
            }
            // The remembered sample file may be gone (rewritten table): list the table again.
            parquetAdapter.forgetSampleFile(pq.schema, pq.table);
            return describeAgainstSample(allocator, pq);
        }
    }

    /**
     * Describes a partial query of a catalog table read at an earlier snapshot (time travel),
     * as its endpoints stream it: with that snapshot's columns.
     *
     * @param allocator Arrow buffer allocator
     * @param pq        parsed query without its time-travel clause
     * @param resolved  the earlier snapshot
     * @return result schema
     * @throws IllegalArgumentException when DuckDB rejects the query's text
     */
    public Schema travelledSchema(BufferAllocator allocator, ParquetQueryParser pq,
            CatalogTables.Resolved resolved) {
        try {
            return describeCatalog(allocator, pq, resolved);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            String message = String.valueOf(e.getMessage());
            if (isBindError(message)) {
                throw new IllegalArgumentException(
                        pro.surpin.data.arrowflight.server.duckdb.DuckDbErrors.errorLine(message), e);
            }
            throw new IllegalStateException(message, e);
        }
    }

    /**
     * Asks DuckDB for the result schema of a partial query of a catalog table snapshot.
     *
     * @param allocator Arrow buffer allocator
     * @param pq        parsed query
     * @param resolved  snapshot the query reads
     * @return result schema
     * @throws Exception if DuckDB cannot bind the query
     */
    private Schema describeCatalog(BufferAllocator allocator, ParquetQueryParser pq,
            CatalogTables.Resolved resolved) throws Exception {
        String from = "(SELECT * FROM " + catalogDescribeSource(resolved) + " LIMIT 0)";
        String duckSql = pq.hasAggregation
                ? DuckDbAdapter.buildDuckSqlWithFilter(pq, from, false)
                : DuckDbAdapter.buildSelectSql(pq, from);
        return duckDbAdapter.describeSql(allocator, duckSql);
    }

    /**
     * Asks DuckDB for the result schema of a plain table query, bound against the table's sample file.
     *
     * @param allocator Arrow buffer allocator
     * @param pq        parsed query
     * @return result schema
     * @throws Exception if the table has no files or DuckDB cannot bind the query
     */
    private Schema describeAgainstSample(BufferAllocator allocator, ParquetQueryParser pq)
            throws Exception {
        DataFile sample = parquetAdapter.sampleFile(pq.schema, pq.table)
                .orElseThrow(() -> new IOException(
                        "No Parquet files for table " + pq.schema + "." + pq.table));
        pro.surpin.data.arrowflight.server.storage.DataStore store = parquetAdapter.dataStore();
        String location = store.duckDbUri(sample);
        // HDFS under SASL: a file without every block here cannot be read by DuckDB; describe
        // its footer, read through the Java client, instead (item 29).
        try (pro.surpin.data.arrowflight.server.duckdb.FooterStub stub = store.wholeFileReadsOnly()
                && !store.readableHere(sample)
                ? pro.surpin.data.arrowflight.server.duckdb.FooterStub.write(store.inputFile(sample),
                        duckDbAdapter.footerStubRoot(), location) : null) {
            String from = "(SELECT * FROM " + tableSource(pq.schema, pq.table, List.of(
                    stub == null ? location : stub.file().toString()))
                    + " LIMIT 0)";
            String duckSql = pq.hasAggregation
                    ? DuckDbAdapter.buildDuckSqlWithFilter(pq, from, false)
                    : DuckDbAdapter.buildSelectSql(pq, from);
            return duckDbAdapter.describeSql(allocator, duckSql);
        }
    }

    /**
     * Tells whether a DuckDB error message from a describe rejects the query's text.
     *
     * @param message DuckDB error message
     * @return true for binder, parser, catalog and conversion errors
     */
    private static boolean isBindError(String message) {
        return message.startsWith("Binder Error") || message.startsWith("Parser Error")
                || message.startsWith("Catalog Error")
                || message.startsWith("Conversion Error");
    }

    /**
     * Reads specific Parquet files and sends Arrow batches through the listener.
     *
     * @param allocator      Arrow buffer allocator
     * @param query          SQL query
     * @param fileUris       relative file paths
     * @param listener       Flight stream listener
     * @param startListener  whether to call listener.start()
     * @throws Exception on execution failure
     */
    public void readParquet(BufferAllocator allocator, String query, String[] fileUris,
            FlightProducer.ServerStreamListener listener, boolean startListener) throws Exception {
        readParquet(allocator, query, fileUris, listener, startListener,
                new ExecutionPathTracker());
    }

    /**
     * Reads Parquet files while recording the actual runtime-selected execution path.
     *
     * @param allocator      Arrow buffer allocator
     * @param query          SQL query
     * @param fileUris       relative file paths
     * @param listener       Flight stream listener
     * @param startListener  whether to call listener.start()
     * @param pathTracker    runtime execution-path tracker
     * @throws Exception on execution failure
     */
    public void readParquet(BufferAllocator allocator, String query, String[] fileUris,
            FlightProducer.ServerStreamListener listener, boolean startListener,
            ExecutionPathTracker pathTracker) throws Exception {
        long tParse = LogUtil.mark();
        ParquetQueryParser parsedQuery = ParquetQueryParser.parse(query);
        LogUtil.logTiming(tParse, "parseQuery");

        if (parsedQuery.isJoin) {
            pathTracker.select(ExecutionPath.DUCKDB_JOIN);
            try (LocalCopyCache.Lease copies = localCopies(fileUris)) {
                executeJoin(allocator, parsedQuery, fileUris, copies, listener, startListener);
            }
            return;
        }

        if (parquetAdapter.isCatalogSchema(parsedQuery.schema)) {
            readCatalogFiles(allocator, parsedQuery, fileUris, listener, startListener, pathTracker);
            return;
        }

        if (fileUris == null) {
            long tDiscover = LogUtil.mark();
            fileUris = parquetAdapter.locationsForQuery(query)
                    .keySet().toArray(new String[0]);
            LogUtil.logTiming(tDiscover, "files.discover", FILES_TIMING_PREFIX + fileUris.length);
        }

        List<String> parquetFiles = resolveParquetFiles(fileUris);
        if (parquetFiles.isEmpty()) {
            pathTracker.unknown("no-parquet-files");
            LOGGER.warn("No Parquet files to read for query: {}", query);
            return;
        }
        List<DataFile> unreadable = unreadableFiles(fileUris);
        if (unreadable != null && !unreadable.isEmpty() && footerCountOnly(parsedQuery)) {
            // The row counts come from the footers, read through the Java client: no copies.
            LOGGER.info("qid={} node={} hdfs=reads files={} shortCircuit={} localCopy=0 footerOnly={}",
                    LogUtil.qid(), LogUtil.node(), fileUris.length,
                    fileUris.length - unreadable.size(), unreadable.size());
            unreadable = null;
        }
        boolean concatenatable = concatenatable(parsedQuery);
        CopyBatchMerge merge = startListener && unreadable != null && !concatenatable
                ? CopyBatchMerge.of(query, parsedQuery) : null;
        // A merged answer does not depend on the file order: copies already cached go first,
        // so they are reused before the cache makes room for the others (item 33 (C)).
        List<String[]> batches = startListener && unreadable != null
                && (concatenatable || merge != null)
                ? copyBatches(merge != null ? cachedFirst(fileUris, unreadable) : fileUris,
                        unreadable, copyLimit(unreadable)) : List.of();
        if (batches.size() > 1) {
            if (concatenatable) {
                readInCopyBatches(allocator, query, parsedQuery, batches, unreadable, listener,
                        pathTracker);
                return;
            }
            if (mergeCopyBatches(allocator, query, merge, batches, unreadable, listener,
                    pathTracker)) {
                return;
            }
        }
        try (LocalCopyCache.Lease copies = copies(unreadable, fileUris.length)) {
            readParquetFiles(allocator, query, parsedQuery, parquetFiles,
                    resolveUris(fileUris, copies), listener, startListener, pathTracker);
        }
    }

    /**
     * Reads a scan whose local copies do not fit the cache at once (item 30): the ticket's files
     * go in batches whose copies fit, each batch's copies are released (and, over the limit,
     * deleted) before the next batch is copied, and the batches' rows are sent as one stream.
     *
     * @param allocator   Arrow buffer allocator
     * @param query       SQL query
     * @param parsedQuery parsed query, a plain scan
     * @param batches     the ticket's files in batches
     * @param unreadable  files this node cannot read where they are
     * @param listener    Flight stream listener, not started
     * @param pathTracker runtime execution-path tracker
     * @throws Exception on copy or execution failure
     */
    private void readInCopyBatches(BufferAllocator allocator, String query,
            ParquetQueryParser parsedQuery, List<String[]> batches, List<DataFile> unreadable,
            FlightProducer.ServerStreamListener listener, ExecutionPathTracker pathTracker)
            throws Exception {
        Map<String, DataFile> byPath = new HashMap<>();
        for (DataFile file : unreadable) {
            byPath.put(file.relativePath(), file);
        }
        LOGGER.info("qid={} node={} hdfs=copyBatches batches={} files={} maxBytes={}",
                LogUtil.qid(), LogUtil.node(), batches.size(),
                batches.stream().mapToInt(b -> b.length).sum(), localCopyCache().maxBytes());
        try (ConcatenatingStreamListener stream =
                new ConcatenatingStreamListener(listener, allocator)) {
            for (String[] batch : batches) {
                List<DataFile> toCopy = new ArrayList<>();
                for (String rel : batch) {
                    DataFile file = byPath.get(rel);
                    if (file != null) {
                        toCopy.add(file);
                    }
                }
                List<String> parquetFiles = resolveParquetFiles(batch);
                if (parquetFiles.isEmpty()) {
                    continue;
                }
                try (LocalCopyCache.Lease copies = copies(toCopy, batch.length)) {
                    readParquetFiles(allocator, query, parsedQuery, parquetFiles,
                            resolveUris(batch, copies), stream, true, pathTracker);
                }
                if (stream.isCancelled()) {
                    return;
                }
            }
        }
    }

    /**
     * Answers an aggregate, a DISTINCT or a LIMIT query whose local copies do not fit the cache
     * at once (item 30): each batch of the ticket's files answers the query (an aggregate without
     * its ORDER BY and LIMIT) with its copies held, the copies are released before the next batch
     * is copied, the part answers are kept in memory and merged into the ticket's answer as
     * distributed complete mode merges endpoint answers (counts and sums summed, minima and
     * maxima taken again, per group; DISTINCT, ORDER BY and LIMIT applied again; HAVING applied
     * to the merged groups; DISTINCT aggregates counted or summed again over the parts' distinct
     * values). The answer equals the one computed over all files at once.
     *
     * @param allocator   Arrow buffer allocator
     * @param query       SQL query
     * @param merge       the merge of the query's batch answers ({@link CopyBatchMerge#of})
     * @param batches     the ticket's files in batches
     * @param unreadable  files this node cannot read where they are
     * @param listener    Flight stream listener, not started
     * @param pathTracker runtime execution-path tracker
     * @return true when the answer was streamed; false when nothing was sent and the caller
     *         answers with every copy held at once
     * @throws Exception on copy or execution failure, or a failure after the first batch
     */
    @SuppressWarnings({"java:S107", "checkstyle:ParameterNumber"})
    private boolean mergeCopyBatches(BufferAllocator allocator, String query,
            CopyBatchMerge merge,
            List<String[]> batches, List<DataFile> unreadable,
            FlightProducer.ServerStreamListener listener, ExecutionPathTracker pathTracker)
            throws Exception {
        Schema expected = resultSchema(allocator, query);
        List<String> outputNames = expected.getFields().stream().map(Field::getName).toList();
        try {
            // An ORDER BY the merged answer cannot apply is known before anything is copied.
            List<String> probe = new ArrayList<>(outputNames);
            for (int i = 0; i < merge.extraColumns(); i++) {
                probe.add(CopyBatchMerge.HAVING_PREFIX + i);
            }
            merge.mergeSql("parts", probe, outputNames);
        } catch (IllegalArgumentException e) {
            LOGGER.warn("qid={} node={} hdfs=copyBatches merge=fallback reason='{}'",
                    LogUtil.qid(), LogUtil.node(), e.getMessage());
            return false;
        }
        ParquetQueryParser parsedQuery = merge.partQuery();
        String partSql = merge.partSql();
        Map<String, DataFile> byPath = new HashMap<>();
        for (DataFile file : unreadable) {
            byPath.put(file.relativePath(), file);
        }
        LOGGER.info("qid={} node={} hdfs=copyBatches batches={} files={} maxBytes={} merge={}",
                LogUtil.qid(), LogUtil.node(), batches.size(),
                batches.stream().mapToInt(b -> b.length).sum(), localCopyCache().maxBytes(),
                merge.rewritten() ? "rewrite" : parsedQuery.hasAggregation ? "aggregate" : "rows");
        try (CollectedAnswers parts = new CollectedAnswers(allocator, listener)) {
            for (String[] batch : batches) {
                List<DataFile> toCopy = new ArrayList<>();
                for (String rel : batch) {
                    DataFile file = byPath.get(rel);
                    if (file != null) {
                        toCopy.add(file);
                    }
                }
                List<String> parquetFiles = resolveParquetFiles(batch);
                if (parquetFiles.isEmpty()) {
                    continue;
                }
                try (LocalCopyCache.Lease copies = copies(toCopy, batch.length)) {
                    readParquetFiles(allocator, partSql, parsedQuery, parquetFiles,
                            resolveUris(batch, copies), parts, true, pathTracker);
                }
                if (listener.isCancelled()) {
                    return true;
                }
            }
            if (parts.schema() == null) {
                return false;
            }
            List<String> columns = parts.schema().getFields().stream().map(Field::getName).toList();
            String name = PARTS_VIEW_PREFIX + PARTS_VIEWS.incrementAndGet();
            DistributedComplete.DeferredStart client = new DistributedComplete.DeferredStart(listener);
            client.expect(expected);
            try (org.apache.arrow.vector.ipc.ArrowReader reader = parts.reader()) {
                String sql = merge.mergeSql(name, columns, outputNames);
                readArrowParts(allocator, reader, name, sql, client);
            } catch (Exception e) {
                if (client.sentBatch() || listener.isCancelled()) {
                    throw e;
                }
                LOGGER.warn("qid={} node={} hdfs=copyBatches merge=fallback reason='{}'",
                        LogUtil.qid(), LogUtil.node(), e.getMessage());
                return false;
            }
            client.flushStart();
            LOGGER.info("qid={} node={} hdfs=copyBatches merged parts={} partBytes={}",
                    LogUtil.qid(), LogUtil.node(), parts.batchCount(), parts.bytes());
            return true;
        }
    }

    /**
     * Tells whether an aggregate has no GROUP BY, filter, ORDER BY, LIMIT, DISTINCT or HAVING.
     *
     * @param pq parsed query
     * @return true for such an aggregate over all rows
     */
    private static boolean noGroupByNoFilter(ParquetQueryParser pq) {
        return pq.groupByColumnNames.isEmpty()
                && (pq.filter == null || pq.filter.isBlank())
                && !pq.hasOrderOrLimit()
                && !pq.distinct && pq.having == null
                && pq.selectExprs.stream().noneMatch(e -> e.distinct)
                && !pq.selectExprs.isEmpty();
    }

    /**
     * Tells whether a query is answered from the row counts in the Parquet footers alone
     * (only COUNT(*) over all rows), so no data file needs to be read or copied.
     *
     * @param pq parsed query
     * @return true for a footer-answered COUNT(*)
     */
    static boolean footerCountOnly(ParquetQueryParser pq) {
        return pq.hasAggregation && !pq.isJoin && noGroupByNoFilter(pq)
                && pq.selectExprs.stream().allMatch(
                        e -> e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR);
    }

    /**
     * Tells whether the answer of a query for a set of files can be merged from its answers for
     * parts of that set ({@link CompleteAnswerMerge#partQuery}), as {@link CompleteAnswerMerge}
     * merges endpoint answers: an aggregate (also with ORDER BY and LIMIT, HAVING, or count or
     * sum over DISTINCT values of one column: {@link CopyBatchMerge}), or a scan with DISTINCT or
     * LIMIT. A scan with ORDER BY alone is not: its parts would keep every row.
     *
     * @param query SQL query
     * @return true for a query whose merged part answers are its answer
     */
    static boolean mergeable(String query) {
        ParquetQueryParser parsed = ParquetQueryParser.parse(query);
        return !concatenatable(parsed) && CopyBatchMerge.of(query, parsed) != null;
    }

    /**
     * Tells whether the rows a query returns for a set of files are the concatenation of its
     * rows for parts of that set: a scan without aggregate, ORDER BY, LIMIT, OFFSET, DISTINCT or
     * per-query expressions (window functions).
     *
     * @param parsed parsed query
     * @return true for such a plain scan
     */
    static boolean concatenatable(ParquetQueryParser parsed) {
        return !parsed.isJoin && !parsed.hasAggregation && !parsed.hasOrderOrLimit()
                && !parsed.distinct && parsed.having == null
                && parsed.singleNodeFeature() == null && parsed.completeAnswerReason == null;
    }

    /**
     * Orders a ticket's files so that those whose copy is in the cache come first.
     *
     * @param fileUris the ticket's relative file paths
     * @param unreadable files among them that must be copied, not empty
     * @return the paths, cached copies first, otherwise in ticket order
     * @throws IOException when the cache cannot be created
     */
    private String[] cachedFirst(String[] fileUris, List<DataFile> unreadable) throws IOException {
        return cachedFirst(fileUris, localCopyCache().present(unreadable));
    }

    /**
     * Orders file paths so that the given ones come first, keeping the order otherwise.
     *
     * @param fileUris relative file paths
     * @param first paths to put first
     * @return reordered paths (the same array when nothing moves)
     */
    static String[] cachedFirst(String[] fileUris, Set<String> first) {
        if (first.isEmpty()) {
            return fileUris;
        }
        List<String> ordered = new ArrayList<>(fileUris.length);
        List<String> rest = new ArrayList<>(fileUris.length);
        for (String rel : fileUris) {
            (first.contains(rel) ? ordered : rest).add(rel);
        }
        ordered.addAll(rest);
        return ordered.toArray(new String[0]);
    }

    /**
     * Splits a ticket's files into batches whose local copies fit the cache limit; files read
     * where they are go into the first batch, a file larger than the limit gets a batch alone.
     *
     * @param fileUris the ticket's relative file paths
     * @param unreadable files among them that must be copied, or null when nothing is copied
     * @param maxBytes the cache limit, or 0 when nothing is copied
     * @return batches in ticket order, or one batch holding every file when the copies fit
     */
    static List<String[]> copyBatches(String[] fileUris, List<DataFile> unreadable,
            long maxBytes) {
        if (unreadable == null || maxBytes <= 0) {
            return Collections.singletonList(fileUris);
        }
        Map<String, Long> sizes = new HashMap<>();
        long total = 0;
        for (DataFile file : unreadable) {
            if (sizes.putIfAbsent(file.relativePath(), file.size()) == null) {
                total += file.size();
            }
        }
        if (total <= maxBytes) {
            return Collections.singletonList(fileUris);
        }
        List<String> direct = new ArrayList<>();
        List<String[]> batches = new ArrayList<>();
        List<String> current = new ArrayList<>();
        long currentBytes = 0;
        Set<String> seen = new HashSet<>();
        for (String rel : fileUris) {
            Long size = sizes.get(rel);
            if (size == null) {
                direct.add(rel);
                continue;
            }
            if (!seen.add(rel)) {
                continue;
            }
            if (!current.isEmpty() && currentBytes + size > maxBytes) {
                batches.add(current.toArray(new String[0]));
                current = new ArrayList<>();
                currentBytes = 0;
            }
            current.add(rel);
            currentBytes += size;
        }
        if (!current.isEmpty()) {
            batches.add(current.toArray(new String[0]));
        }
        if (!direct.isEmpty()) {
            direct.addAll(List.of(batches.get(0)));
            batches.set(0, direct.toArray(new String[0]));
        }
        return batches;
    }

    /**
     * Reads a query's Parquet files of one table, by aggregation or scan.
     *
     * @param allocator     Arrow buffer allocator
     * @param query         SQL query
     * @param parsedQuery   parsed query
     * @param parquetFiles  relative file paths
     * @param resolvedUris  file paths as DuckDB reads them
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @param pathTracker   runtime execution-path tracker
     * @throws Exception on execution failure
     */
    @SuppressWarnings({"java:S107", "checkstyle:ParameterNumber"})
    private void readParquetFiles(BufferAllocator allocator, String query,
            ParquetQueryParser parsedQuery, List<String> parquetFiles, List<String> resolvedUris,
            FlightProducer.ServerStreamListener listener, boolean startListener,
            ExecutionPathTracker pathTracker) throws Exception {

        if (parsedQuery.hasAggregation) {
            LOGGER.debug("qid={} node={} execution=engine engine=DuckDB(aggregation) hasGroupBy={} files={}",
                    LogUtil.qid(), LogUtil.node(), !parsedQuery.groupByColumnNames.isEmpty(),
                    parquetFiles.size());
            executeAggregation(allocator, query, parsedQuery, parquetFiles, resolvedUris,
                    listener, startListener, pathTracker);
            return;
        }

        pathTracker.select(ExecutionPath.DUCKDB_SCAN);
        LOGGER.debug("qid={} node={} execution=engine engine=DuckDB files={}",
                LogUtil.qid(), LogUtil.node(), resolvedUris.size());
        String duckSql = DuckDbAdapter.buildSelectSql(parsedQuery,
                tableSource(parsedQuery.schema, parsedQuery.table, resolvedUris));
        duckDbAdapter.streamSql(allocator, duckSql, listener, startListener);
    }

    /**
     * Executes aggregation queries via DuckDB or footer-stats fast paths.
     *
     * @param allocator     Arrow buffer allocator
     * @param query         SQL query
     * @param pq            parsed query
     * @param parquetFiles  Parquet file paths relative to the data directory
     * @param resolvedUris  file paths as DuckDB reads them
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @param pathTracker   runtime execution-path tracker
     * @throws Exception on execution failure
     */
    @SuppressWarnings("java:S3776") // Aggregation dispatch intentionally keeps all aggregate states together.
    private void executeAggregation(BufferAllocator allocator, String query,
            ParquetQueryParser pq, List<String> parquetFiles, List<String> resolvedUris,
            FlightProducer.ServerStreamListener listener,
            boolean startListener, ExecutionPathTracker pathTracker) throws Exception {
        long t = LogUtil.mark();

        if (parquetFiles.isEmpty()) {
            pathTracker.unknown("no-parquet-files");
            emitRowsAsArrow(allocator, query, pq, Collections.emptyList(), listener, startListener);
            return;
        }

        boolean noGroupByNoFilter = noGroupByNoFilter(pq);

        // Fast path A: COUNT(*) only — read row counts from Parquet footer
        if (footerCountOnly(pq)) {
            pathTracker.select(ExecutionPath.FOOTER_COUNT);
            List<Future<Long>> futs = new ArrayList<>(parquetFiles.size());
            DataStore store = parquetAdapter.dataStore();
            for (String file : parquetFiles) {
                futs.add(ioPool.submit(() -> DuckDbAdapter.footerRowCount(
                        store.inputFile(file), fileName(file))));
            }
            long total = 0;
            for (Future<Long> f : futs) {
                total += f.get();
            }
            LogUtil.logTiming(t, "engine:agg.footerCount", FILES_TIMING_PREFIX + parquetFiles.size() + " total=" + total);
            int n = pq.selectExprs.size();
            Object[] row = new Object[n];
            Arrays.fill(row, total);
            emitRowsAsArrow(allocator, query, pq, Collections.singletonList(row),
                    listener, startListener);
            return;
        }

        // Fast path B: MIN/MAX/COUNT(col) from Parquet footer statistics
        boolean statsEligible = noGroupByNoFilter
                && footerStatsAreExact(pq)
                && pq.selectExprs.stream().allMatch(e ->
                        e.func == ParquetQueryParser.SelectExpr.AggFunc.MIN
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.MAX
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR)
                && pq.selectExprs.stream().anyMatch(e ->
                        e.func == ParquetQueryParser.SelectExpr.AggFunc.MIN
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.MAX
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT);
        if (statsEligible) {
            pathTracker.select(ExecutionPath.FOOTER_STATS);
            List<Future<Optional<Object[]>>> futs = new ArrayList<>(parquetFiles.size());
            DataStore store = parquetAdapter.dataStore();
            for (String file : parquetFiles) {
                futs.add(ioPool.submit(() -> DuckDbAdapter.footerStats(
                        store.inputFile(file), fileName(file), pq)));
            }
            Object[] merged = null;
            boolean allHaveStats = true;
            for (Future<Optional<Object[]>> f : futs) {
                Optional<Object[]> opt = f.get();
                if (opt.isEmpty()) {
                    allHaveStats = false;
                    break;
                }
                if (merged == null) {
                    merged = opt.get().clone();
                } else {
                    mergeAggCols(pq.selectExprs, merged, opt.get(), 0);
                }
            }
            if (allHaveStats) {
                LogUtil.logTiming(t, "engine:agg.footerStats", FILES_TIMING_PREFIX + parquetFiles.size());
                List<Object[]> rows = merged != null
                        ? Collections.singletonList(merged) : Collections.emptyList();
                emitRowsAsArrow(allocator, query, pq, rows, listener, startListener);
                return;
            }
            LogUtil.logTiming(t, "engine:agg.footerStatsFallback", FILES_TIMING_PREFIX + parquetFiles.size());
            pathTracker.fallbackTo(
                    ExecutionPath.DUCKDB_AGGREGATION, "missing-parquet-footer-statistics");
        } else {
            pathTracker.select(ExecutionPath.DUCKDB_AGGREGATION);
        }

        String duckSql = DuckDbAdapter.buildDuckSqlWithFilter(pq,
                tableSource(pq.schema, pq.table, resolvedUris), false);
        duckDbAdapter.streamSql(allocator, duckSql, listener, startListener);
    }

    // ── catalog tables (Iceberg, Hive) in parts ──────────────────────────

    /**
     * Reads data files of a catalog table named in a partial-result ticket, with the columns of
     * the snapshot the planning node used. Iceberg files must belong to that snapshot, and the
     * snapshot must still be readable file by file; each file is read with the delete files the
     * pinned snapshot's plan gives it (the ticket does not carry them). Parquet files of other
     * catalogs (Hive) come with their partition values in the ticket and must lie under the
     * catalog's roots. Otherwise the read is refused rather than answered wrongly.
     *
     * @param allocator     Arrow buffer allocator
     * @param pq            parsed query of one catalog table
     * @param fileUris      ticket entries ({@link CatalogFileKey}), or null for the whole
     *                      current snapshot
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @param pathTracker   runtime execution-path tracker
     * @throws Exception on refusal, catalog, query or stream failure
     */
    private void readCatalogFiles(BufferAllocator allocator, ParquetQueryParser pq,
            String[] fileUris, FlightProducer.ServerStreamListener listener,
            boolean startListener, ExecutionPathTracker pathTracker) throws Exception {
        long t = LogUtil.mark();
        CatalogTables catalogs = parquetAdapter.catalogTables();
        CatalogTables.Resolved resolved;
        List<TableFile> files = new ArrayList<>();
        if (fileUris == null) {
            resolved = catalogs.current(pq.schema, pq.table).orElseThrow(() -> new IOException(
                    "No table " + pq.schema + "." + pq.table + " in its catalog"));
            files.addAll(resolved.snapshot().files());
        } else {
            List<CatalogFileKey> keys = new ArrayList<>();
            String snapshotId = null;
            for (String entry : fileUris) {
                CatalogFileKey key = CatalogFileKey.decode(entry);
                if (snapshotId != null && !snapshotId.equals(key.snapshotId())) {
                    throw new IllegalArgumentException("A ticket names files of two snapshots of "
                            + pq.schema + "." + pq.table);
                }
                if (!keys.isEmpty() && keys.get(0).carriesPartitionValues()
                        != key.carriesPartitionValues()) {
                    throw new IllegalArgumentException("A ticket mixes file entries of two kinds for "
                            + pq.schema + "." + pq.table);
                }
                snapshotId = key.snapshotId();
                keys.add(key);
            }
            if (snapshotId == null) {
                pathTracker.unknown("no-parquet-files");
                return;
            }
            String pinnedId = snapshotId;
            if (keys.get(0).carriesPartitionValues()) {
                resolved = catalogs.pinnedColumns(pq.schema, pq.table, snapshotId).orElseThrow(
                        () -> new IOException("The columns of " + pq.schema + "." + pq.table
                                + " changed since the query was planned (snapshot " + pinnedId
                                + "); plan the query again"));
                files.addAll(ticketFiles(resolved, keys));
            } else {
                resolved = catalogs.pinned(pq.schema, pq.table, snapshotId).orElseThrow(
                        () -> new IOException("Snapshot " + pinnedId + " of " + pq.schema + "."
                                + pq.table + " is no longer available in its catalog; plan the"
                                + " query again"));
                Map<String, TableFile> snapshotFiles = new java.util.HashMap<>();
                for (TableFile file : resolved.snapshot().files()) {
                    snapshotFiles.put(file.location(), file);
                }
                for (CatalogFileKey key : keys) {
                    TableFile file = snapshotFiles.get(key.location());
                    if (file == null) {
                        throw new IllegalArgumentException("File " + key.location() + " is not a"
                                + " data file of snapshot " + resolved.snapshot().snapshotId()
                                + " of " + pq.schema + "." + pq.table);
                    }
                    files.add(file);
                }
            }
        }
        // Files from the ticket (Hive) stand for the snapshot; Iceberg is checked as a whole.
        boolean shipped = shipsPartitionValues(resolved.snapshot());
        String reason = CatalogTables.completeReason(shipped ? withFiles(resolved, files) : resolved);
        if (reason != null) {
            throw new IllegalStateException("The files of " + pq.schema + "." + pq.table
                    + " cannot be read one by one (" + reason + "); ask for the complete answer");
        }
        if (files.isEmpty()) {
            pathTracker.unknown("no-parquet-files");
            return;
        }
        pathTracker.select(pq.hasAggregation ? ExecutionPath.DUCKDB_AGGREGATION
                : ExecutionPath.DUCKDB_SCAN);
        LOGGER.debug("qid={} node={} execution=engine engine=DuckDB(catalog) table={} snapshot={}"
                + " files={}", LogUtil.qid(), LogUtil.node(), resolved.snapshot().name(),
                resolved.snapshot().snapshotId(), files.size());
        LogUtil.logTiming(t, "catalog.resolveFiles", FILES_TIMING_PREFIX + files.size());
        if (resolved.snapshot().read() instanceof ReadSpec.ArrowStream) {
            streamArrowScan(allocator, pq, resolved, files, listener, startListener);
            return;
        }
        String source = catalogFilesSource(resolved.snapshot(), files);
        duckDbAdapter.streamSql(allocator, partialSql(pq, source), listener, startListener);
    }

    /**
     * Writes the SQL one endpoint of a partial query runs over its part of a table.
     *
     * @param pq parsed query of one table
     * @param source FROM source of the part
     * @return DuckDB SQL
     */
    static String partialSql(ParquetQueryParser pq, String source) {
        return pq.hasAggregation
                ? DuckDbAdapter.buildDuckSqlWithFilter(pq, source, false)
                : DuckDbAdapter.buildSelectSql(pq, source);
    }

    /**
     * Answers a partial query of a catalog table read as Arrow streams: the catalog streams the
     * files with the columns the query names (filters as hints), DuckDB runs the query over the
     * one-shot stream, and the stream is dropped afterwards whatever happened.
     *
     * @param allocator     Arrow buffer allocator
     * @param pq            parsed query of one table
     * @param resolved      the snapshot with its catalog
     * @param files         files of this endpoint
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws Exception on catalog, query or stream failure
     */
    private void streamArrowScan(BufferAllocator allocator, ParquetQueryParser pq,
            CatalogTables.Resolved resolved, List<TableFile> files,
            FlightProducer.ServerStreamListener listener, boolean startListener) throws Exception {
        TableSnapshot snapshot = resolved.snapshot();
        ArrowScan scan = ArrowScanRequests.of(snapshot,
                partialSql(pq, ARROW_SOURCE_PLACEHOLDER), pq.filter);
        try (ArrowStreamScans scans = new ArrowStreamScans(duckDbAdapter.connection(), allocator)) {
            String source = scans.source(resolved.entry().catalog(), snapshot, files, scan);
            duckDbAdapter.streamSqlOnce(allocator, partialSql(pq, source), listener,
                    startListener);
        }
    }

    /**
     * Tells whether a snapshot's files travel in tickets with their partition values (files the
     * server reads one by one without the catalog's metadata: Parquet files, Arrow streams).
     *
     * @param snapshot snapshot
     * @return true when ticket entries carry partition values
     */
    public static boolean shipsPartitionValues(TableSnapshot snapshot) {
        return snapshot.read() instanceof ReadSpec.ParquetFiles
                || snapshot.read() instanceof ReadSpec.ArrowStream;
    }

    /**
     * Names data files of a catalog snapshot as a FROM source; files with delete files are read
     * with their deletes applied.
     *
     * @param snapshot snapshot with its columns
     * @param files files of the snapshot named in the ticket
     * @return FROM source
     * @throws IllegalStateException when the deletes of the files cannot be applied file by file
     */
    static String catalogFilesSource(TableSnapshot snapshot, List<TableFile> files) {
        boolean deletes = files.stream().anyMatch(file -> !file.deletes().isEmpty());
        if (!deletes) {
            return CatalogSources.filesSource(snapshot, files);
        }
        String applied = CatalogSources.deletesAppliedSource(snapshot, files);
        if (applied == null) {
            throw new IllegalStateException("The delete files of " + snapshot.name() + " (snapshot "
                    + snapshot.snapshotId() + ") cannot be applied file by file; ask for the"
                    + " complete answer");
        }
        return applied;
    }

    /**
     * Turns ticket entries with partition values into data files of a snapshot read as Parquet
     * files: every file must lie under the catalog's roots and name exactly the snapshot's
     * partition columns.
     *
     * @param resolved snapshot (columns) with its catalog
     * @param keys ticket entries
     * @return files with their partition values
     * @throws IllegalArgumentException when an entry does not fit the snapshot
     */
    static List<TableFile> ticketFiles(CatalogTables.Resolved resolved, List<CatalogFileKey> keys) {
        TableSnapshot snapshot = resolved.snapshot();
        if (!shipsPartitionValues(snapshot)) {
            throw new IllegalArgumentException("Snapshot " + snapshot.snapshotId() + " of "
                    + snapshot.name() + " is not read file by file");
        }
        Set<String> partitions = new java.util.LinkedHashSet<>(snapshot.partitionColumns());
        List<TableFile> result = new ArrayList<>(keys.size());
        for (CatalogFileKey key : keys) {
            if (!CatalogLocations.underAny(key.location(), resolved.entry().roots())) {
                throw new IllegalArgumentException("File " + key.location() + " of "
                        + snapshot.name() + " lies outside the roots of its catalog");
            }
            if (!partitions.equals(key.partitionValues().keySet())) {
                throw new IllegalArgumentException("File " + key.location() + " names partition"
                        + " columns " + key.partitionValues().keySet() + ", " + snapshot.name()
                        + " has " + partitions);
            }
            result.add(new TableFile(key.location(), 0L, 0L, null, key.partitionValues(), -1L,
                    null));
        }
        return result;
    }

    /**
     * Returns a resolved snapshot with other files, for the checks that look at the files.
     *
     * @param resolved snapshot with its catalog
     * @param files files to put in
     * @return snapshot with those files
     */
    private static CatalogTables.Resolved withFiles(CatalogTables.Resolved resolved,
            List<TableFile> files) {
        TableSnapshot s = resolved.snapshot();
        if (s.files().equals(files)) {
            return resolved;
        }
        return new CatalogTables.Resolved(resolved.entry(), s.withFiles(files));
    }

    /**
     * Names the FROM source a partial query of a catalog table is described with: its first
     * data file read like the executing nodes read it, or DuckDB's scan of the whole snapshot
     * when its files cannot be read one by one.
     *
     * @param resolved the snapshot the query reads
     * @return FROM source
     */
    private static String catalogDescribeSource(CatalogTables.Resolved resolved) {
        TableSnapshot snapshot = resolved.snapshot();
        if (snapshot.read() instanceof ReadSpec.ArrowStream) {
            return CatalogViews.declaredColumnsSource(snapshot);
        }
        if (snapshot.read() instanceof ReadSpec.AttachedTable attached) {
            return CatalogViews.attachedSource(attached, snapshot);
        }
        if (CatalogTables.completeReason(resolved) == null) {
            return CatalogSources.filesSource(snapshot, snapshot.files().subList(0, 1));
        }
        if (snapshot.read() instanceof ReadSpec.IcebergMetadata iceberg) {
            return CatalogViews.icebergSource(iceberg.metadataLocation(), snapshot);
        }
        return CatalogViews.declaredColumnsSource(snapshot);
    }

    // ── DuckDB join execution ────────────────────────────────────────────

    /**
     * Executes JOIN queries by registering temp views in DuckDB and streaming the result.
     *
     * @param allocator     Arrow buffer allocator
     * @param pq            parsed query with join tables
     * @param fileUris      relative file paths
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws Exception on execution failure
     */
    private void executeJoin(BufferAllocator allocator, ParquetQueryParser pq,
            String[] fileUris, LocalCopyCache.Lease copies,
            FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {
        long t = LogUtil.mark();
        Connection conn = duckDbAdapter.connection();
        List<String> registeredAliases = new ArrayList<>();
        try {
            createJoinViews(conn, pq, registeredAliases, false, fileUris, copies);
            duckDbAdapter.streamSql(allocator, pq.duckDbSql, listener, startListener);
        } finally {
            dropJoinViews(conn, registeredAliases);
        }
        LogUtil.logTiming(t, "engine:join", "tables=" + pq.joinTables.size());
    }

    /**
     * Creates one temporary DuckDB view per joined table over that table's Parquet files.
     *
     * @param conn              DuckDB connection
     * @param pq                parsed query with join tables
     * @param registeredAliases receives the alias of every view created
     * @param empty             whether the views expose the columns but no rows
     * @throws Exception on file listing or DuckDB failure
     */
    private void createJoinViews(Connection conn, ParquetQueryParser pq,
            List<String> registeredAliases, boolean empty, String[] ticketFiles,
            LocalCopyCache.Lease copies) throws Exception {
        Map<String, List<String>> tableFiles = new LinkedHashMap<>();
        DataStore store = parquetAdapter.dataStore();
        // The ticket names every file of every joined table as planned (a join has one
        // endpoint): read exactly those, so a table that changed after planning (a new lakeFS
        // commit) is not mixed in. Tables the ticket does not cover are listed, in one read
        // scope so that they come from one data version.
        try (DataStore.ReadScope scope = store == null ? DataStore.ReadScope.NONE
                : store.readScope()) {
            for (ParquetQueryParser.JoinTable jt : pq.joinTables) {
                String key = (jt.schema() != null ? jt.schema() + "." : "") + jt.table();
                tableFiles.computeIfAbsent(key, k -> {
                    try {
                        List<String> planned = plannedTableFiles(store, k, ticketFiles, copies);
                        return planned.isEmpty() ? resolveTableFiles(k) : planned;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }

        for (ParquetQueryParser.JoinTable jt : pq.joinTables) {
            String key = (jt.schema() != null ? jt.schema() + "." : "") + jt.table();
            List<String> duckDbPaths = tableFiles.get(key);
            if (duckDbPaths.isEmpty()) {
                throw new IOException("No Parquet files found for table: " + key);
            }
            long tView = LogUtil.mark();
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE OR REPLACE TEMP VIEW "
                        + DuckDbAdapter.quoteIdentifier(jt.alias())
                        + " AS SELECT * FROM "
                        + tableSource(jt.schema(), jt.table(), duckDbPaths)
                        + (empty ? " LIMIT 0" : ""));
            }
            LogUtil.logTiming(tView, "engine:join.createView", "alias=" + jt.alias() + " " + FILES_TIMING_PREFIX + duckDbPaths.size());
            registeredAliases.add(jt.alias());
        }
    }

    /**
     * Creates the empty views a join is described against on HDFS read short-circuit only (SASL
     * DataNodes): each table bound on a file this node holds whole, or on a footer stub read
     * through the Java client, never through DuckDB on a file it lacks blocks of (item 30).
     *
     * @param conn DuckDB connection
     * @param pq parsed join
     * @param registeredAliases receives the alias of every view created
     * @param stubs receives the footer stubs, to close after the describe
     * @throws Exception on file listing or DuckDB failure
     */
    private void createJoinDescribeViews(Connection conn, ParquetQueryParser pq,
            List<String> registeredAliases,
            List<pro.surpin.data.arrowflight.server.duckdb.FooterStub> stubs) throws Exception {
        DataStore store = parquetAdapter.dataStore();
        for (ParquetQueryParser.JoinTable jt : pq.joinTables) {
            String path;
            if (jt.schema() == null) {
                List<String> files = resolveTableFiles(jt.table());
                if (files.isEmpty()) {
                    throw new IOException("No Parquet files found for table: " + jt.table());
                }
                path = files.get(0);
            } else {
                DataFile sample = parquetAdapter.sampleFile(jt.schema(), jt.table())
                        .orElseThrow(() -> new IOException("No Parquet files found for table: "
                                + jt.schema() + "." + jt.table()));
                path = store.duckDbUri(sample);
                if (!store.readableHere(sample)) {
                    pro.surpin.data.arrowflight.server.duckdb.FooterStub stub =
                            pro.surpin.data.arrowflight.server.duckdb.FooterStub.write(
                                    store.inputFile(sample), duckDbAdapter.footerStubRoot(), path);
                    stubs.add(stub);
                    path = stub.file().toString();
                }
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE OR REPLACE TEMP VIEW " + DuckDbAdapter.quoteIdentifier(jt.alias())
                        + " AS SELECT * FROM " + tableSource(jt.schema(), jt.table(), List.of(path))
                        + " LIMIT 0");
            }
            registeredAliases.add(jt.alias());
        }
    }

    /**
     * Drops the temporary views of a join.
     *
     * @param conn    DuckDB connection
     * @param aliases view names
     * @throws java.sql.SQLException if a statement cannot be created
     */
    private static void dropJoinViews(Connection conn, List<String> aliases)
            throws java.sql.SQLException {
        long tDrop = LogUtil.mark();
        if (conn.isClosed()) {
            // The session was dropped after DuckDB invalidated its database; its views are gone.
            return;
        }
        try (Statement stmt = conn.createStatement()) {
            for (String alias : aliases) {
                try {
                    stmt.execute("DROP VIEW IF EXISTS "
                            + DuckDbAdapter.quoteIdentifier(alias));
                } catch (Exception ignored) {
                }
            }
        }
        LogUtil.logTiming(tDrop, "engine:join.dropViews", "aliases=" + aliases.size());
    }

    /**
     * Tells whether every MIN and MAX of a query can be answered from Parquet footer statistics:
     * the input is a plain column whose statistics compare and convert like its values.
     *
     * @param pq parsed aggregation query
     * @return false when any MIN or MAX input is an expression or a decimal, string, timestamp
     *         or other type the footer path does not convert
     */
    private boolean footerStatsAreExact(ParquetQueryParser pq) {
        Schema tableSchema = parquetAdapter.getTableSchema(pq.schema, pq.table);
        for (ParquetQueryParser.SelectExpr expr : pq.selectExprs) {
            if (expr.func != ParquetQueryParser.SelectExpr.AggFunc.MIN
                    && expr.func != ParquetQueryParser.SelectExpr.AggFunc.MAX) {
                continue;
            }
            Field column = tableSchema.findField(expr.inputColumn);
            ArrowType type = column == null ? null : column.getType();
            boolean exact = type instanceof ArrowType.Int integer && integer.getIsSigned()
                    || type instanceof ArrowType.Date date
                            && date.getUnit() == org.apache.arrow.vector.types.DateUnit.DAY;
            if (!exact) {
                return false;
            }
        }
        return true;
    }

    // ── private helpers ──────────────────────────────────────────────────

    /**
     * Collects the relative paths of the files to read.
     *
     * @param fileUris relative file paths
     * @return relative file paths
     */
    private static List<String> resolveParquetFiles(String[] fileUris) {
        long t = LogUtil.mark();
        List<String> files = List.of(fileUris);
        LogUtil.logTiming(t, "files.resolvePaths", FILES_TIMING_PREFIX + files.size());
        return files;
    }

    /**
     * Names relative files the way DuckDB reads them: a local copy where there is one.
     *
     * @param fileUris relative file paths
     * @param copies local copies of the files this node cannot read where they are, or null
     * @return DuckDB file paths
     */
    private List<String> resolveUris(String[] fileUris, LocalCopyCache.Lease copies) {
        long t = LogUtil.mark();
        DataStore store = parquetAdapter.dataStore();
        List<String> uris = new ArrayList<>(fileUris.length);
        for (String rel : fileUris) {
            uris.add(duckDbUri(store, rel, copies));
        }
        LogUtil.logTiming(t, "files.resolveUris", FILES_TIMING_PREFIX + uris.size());
        return uris;
    }

    /**
     * Names one relative file the way DuckDB reads it: its local copy where there is one.
     *
     * @param store data store
     * @param relativePath relative file path
     * @param copies local copies, or null
     * @return DuckDB file path
     */
    static String duckDbUri(DataStore store, String relativePath, LocalCopyCache.Lease copies) {
        java.nio.file.Path local = copies == null ? null : copies.localPath(relativePath);
        return local == null ? store.duckDbUri(relativePath) : local.toString();
    }

    /**
     * Copies the files of a ticket that this node cannot read where they are (HDFS read
     * short-circuit only: files without every block on this node's DataNode) to the node-local
     * cache, and logs how the query's files are read (item 30).
     *
     * @param fileUris relative paths of the ticket, or null
     * @return lease of the copies, or null when every file is read where it is
     * @throws IOException when the files cannot be listed or copied
     */
    LocalCopyCache.Lease localCopies(String[] fileUris) throws IOException {
        List<DataFile> unreadable = unreadableFiles(fileUris);
        return unreadable == null ? null : copies(unreadable, fileUris.length);
    }

    /**
     * Lists the files of a ticket that this node cannot read where they are.
     *
     * @param fileUris relative paths of the ticket, or null
     * @return files to copy, or null when this node copies nothing (every file is read where it is)
     * @throws IOException when the files cannot be listed
     */
    private List<DataFile> unreadableFiles(String[] fileUris) throws IOException {
        if (fileUris == null || fileUris.length == 0 || !parquetAdapter.localCopies()) {
            return null;
        }
        return filesToCopy(parquetAdapter.dataStore(), fileUris);
    }

    /**
     * Returns the bytes a batch of a ticket's copies may take: the cache limit, or less when the
     * disk of the copies has less room ({@link LocalCopyCache#room}).
     *
     * @param unreadable files to copy, or null
     * @return the batch limit, or 0 when there is nothing to copy
     * @throws IOException when the cache cannot be created or the disk queried
     */
    private long copyLimit(List<DataFile> unreadable) throws IOException {
        return unreadable == null || unreadable.isEmpty() ? 0 : localCopyCache().room();
    }

    /**
     * Copies files this node cannot read where they are and logs how the query's files are read.
     *
     * @param unreadable files to copy
     * @param files number of files the query reads
     * @return lease of the copies, or null when there is nothing to copy
     * @throws IOException when a file cannot be copied
     */
    private LocalCopyCache.Lease copies(List<DataFile> unreadable, int files) throws IOException {
        if (unreadable == null) {
            return null;
        }
        if (unreadable.isEmpty()) {
            LOGGER.info("qid={} node={} hdfs=reads files={} shortCircuit={} localCopy=0",
                    LogUtil.qid(), LogUtil.node(), files, files);
            return null;
        }
        LocalCopyCache.Lease lease;
        try {
            lease = localCopyCache().acquire(unreadable);
        } catch (LocalCopyCache.NoRoomException e) {
            // A refused query logs its reads too (item 33 (E)): nothing was copied.
            LOGGER.info("qid={} node={} hdfs=reads files={} shortCircuit={} localCopy={} copied=0"
                    + " refused=noRoom", LogUtil.qid(), LogUtil.node(), files,
                    files - unreadable.size(), unreadable.size());
            throw e;
        }
        long ms = Math.max(1L, lease.millis());
        LOGGER.info("qid={} node={} hdfs=reads files={} shortCircuit={} localCopy={} copied={}"
                + " reused={} copiedBytes={} copyMs={} copyMBps={}", LogUtil.qid(), LogUtil.node(),
                files, files - unreadable.size(), unreadable.size(),
                lease.copied(), lease.reused(), lease.copiedBytes(), lease.millis(),
                lease.copiedBytes() * 1000L / ms / (1024 * 1024));
        return lease;
    }

    /**
     * Logs the {@code hdfs=reads} line of a query that reads no file of the data store on this
     * node (item 32), so that every query on a short-circuit-only store has one; nothing is
     * logged when local copies are off.
     *
     * @param tables why no file is read: {@code none} (no table of the data store),
     *        {@code unknown} (tables not named), or {@code endpoints=N} (merged from peers)
     */
    public void logNoFileReads(String tables) {
        if (parquetAdapter.localCopies()) {
            LOGGER.info("qid={} node={} hdfs=reads files=0 shortCircuit=0 localCopy=0 tables={}",
                    LogUtil.qid(), LogUtil.node(), tables);
        }
    }

    /**
     * Answers a complete-mode query of one table whose local copies do not fit the copy cache
     * at once (HDFS read short-circuit only, item 31): the table's files are read as one ticket
     * of the partial path, whose copies go in batches that fit (a scan's rows concatenated, an
     * aggregate, DISTINCT or LIMIT merged from the batches' answers), so the answer equals the
     * one computed over all files at once. Nothing is sent and false is returned when local
     * copies are off, the copies fit, the query reads more than one table or a shape the batches
     * cannot answer, or the partial path would describe its answer differently; the result then
     * says why, for the log line of a refusal ({@link #completeFromCopies(String, String)}).
     *
     * @param allocator   Arrow buffer allocator
     * @param clientSql   SQL of the complete answer (restrictions already applied)
     * @param listener    Flight stream listener, not started
     * @param pathTracker runtime execution-path tracker
     * @return whether the answer was streamed, and why not
     * @throws Exception on copy or execution failure
     */
    public CopyBatches completeInCopyBatches(BufferAllocator allocator, String clientSql,
            FlightProducer.ServerStreamListener listener, ExecutionPathTracker pathTracker)
            throws Exception {
        if (!parquetAdapter.localCopies()) {
            return CopyBatches.declined(null);
        }
        if (TimeTravelClause.present(clientSql)) {
            return CopyBatches.declined("the query reads a past snapshot");
        }
        String sql;
        ParquetQueryParser parsed;
        List<ParquetQueryParser.JoinTable> tables;
        try {
            sql = qualifyPlainNames(clientSql);
            parsed = ParquetQueryParser.parse(sql);
            tables = QueryTables.of(sql);
        } catch (java.io.UncheckedIOException e) {
            throw e.getCause();
        } catch (RuntimeException e) {
            // The complete path reports what is wrong with the SQL.
            return CopyBatches.declined("the query is not one query of one table ("
                    + e.getMessage() + ")");
        }
        if (parsed.schema == null || parsed.table == null) {
            return CopyBatches.declined("the query names no table with its schema");
        }
        if (parquetAdapter.isCatalogSchema(parsed.schema)) {
            return CopyBatches.declined(parsed.schema + " is a catalog schema");
        }
        String notBatchable = notBatchableReason(sql, parsed, tables);
        List<DataFile> listed;
        try {
            listed = parquetAdapter.tableFiles(parsed.schema, parsed.table);
        } catch (IllegalArgumentException | java.io.FileNotFoundException e) {
            return CopyBatches.declined(parsed.schema + "." + parsed.table
                    + " is not a table of the data store (" + e.getMessage() + ")");
        }
        DataStore store = parquetAdapter.dataStore();
        List<DataFile> unreadable = new ArrayList<>();
        String[] fileUris = new String[listed.size()];
        for (int i = 0; i < fileUris.length; i++) {
            DataFile file = listed.get(i);
            fileUris[i] = file.relativePath();
            if (!store.readableHere(file)) {
                unreadable.add(file);
            }
        }
        long room = copyLimit(unreadable);
        if (copyBatches(fileUris, unreadable, room).size() < 2) {
            String copies = "the " + unreadable.size() + " copies of " + parsed.schema + "."
                    + parsed.table + " (" + bytes(unreadable) + " bytes)";
            return CopyBatches.declined(unreadable.isEmpty() ? "no file of " + parsed.schema + "."
                    + parsed.table + " needs a copy" : bytes(unreadable) <= room
                    ? copies + " fitted the copy room (" + room + " bytes) when the query was planned"
                    : copies + " make one batch only; the copy room is " + room + " bytes");
        }
        if (notBatchable != null) {
            return CopyBatches.declined(notBatchable);
        }
        Schema complete = completeSchema(allocator, sql);
        Schema partial = resultSchema(allocator, sql);
        boolean renamed = !sameColumns(complete, partial);
        // Column names may differ (an unaliased count(*) is count_star() in the complete answer,
        // item 33 (A)): the answer then takes the complete answer's names.
        if (renamed && !RenamingStreamListener.sameTypes(complete, partial)) {
            return CopyBatches.declined("the column types of the answer in parts "
                    + partial.getFields() + " differ from the complete answer's " + complete.getFields());
        }
        LOGGER.info("qid={} node={} hdfs=completeInCopyBatches table={}.{} files={} localCopy={}"
                + " renamed={}", LogUtil.qid(), LogUtil.node(), parsed.schema, parsed.table,
                fileUris.length, unreadable.size(), renamed);
        if (!renamed) {
            readParquet(allocator, sql, fileUris, listener, true, pathTracker);
            return CopyBatches.STREAMED;
        }
        try (RenamingStreamListener named = new RenamingStreamListener(listener, allocator, complete)) {
            readParquet(allocator, sql, fileUris, named, true, pathTracker);
        }
        return CopyBatches.STREAMED;
    }

    /**
     * Returns the bytes of files.
     *
     * @param files data files
     * @return the sum of their sizes
     */
    private static long bytes(List<DataFile> files) {
        long total = 0;
        for (DataFile file : files) {
            total += file.size();
        }
        return total;
    }

    /**
     * Whether a complete answer was streamed in batches of copies and, when not, why.
     *
     * @param streamed true when the answer was streamed
     * @param notBatched why it was not, or null when it was or the question does not arise
     *        (local copies off)
     */
    public record CopyBatches(boolean streamed, String notBatched) {
        /** The answer was streamed in batches of copies. */
        public static final CopyBatches STREAMED = new CopyBatches(true, null);

        /**
         * Returns a result for an answer not streamed in batches.
         *
         * @param reason why, or null
         * @return the result
         */
        public static CopyBatches declined(String reason) {
            return new CopyBatches(false, reason);
        }
    }

    /**
     * Tells whether a complete-mode query can be answered by the partial path over all files of
     * its one table read in batches of copies: it reads exactly that table (no join, subquery
     * of another table or CTE) and its answer is the concatenation or the merge of the batches'
     * answers.
     *
     * @param sql SQL with plain names qualified
     * @param parsed the parsed SQL
     * @param tables the tables the SQL reads ({@link QueryTables#of})
     * @return true for such a query
     */
    static boolean batchableComplete(String sql, ParquetQueryParser parsed,
            List<ParquetQueryParser.JoinTable> tables) {
        return notBatchableReason(sql, parsed, tables) == null;
    }

    /**
     * Tells why a complete-mode query cannot be answered by the partial path over all files of
     * its one table read in batches of copies ({@link #batchableComplete}).
     *
     * @param sql SQL with plain names qualified
     * @param parsed the parsed SQL
     * @param tables the tables the SQL reads ({@link QueryTables#of})
     * @return the reason, or null when the batches answer it
     */
    static String notBatchableReason(String sql, ParquetQueryParser parsed,
            List<ParquetQueryParser.JoinTable> tables) {
        if (parsed.isJoin) {
            return "the query joins tables";
        }
        if (parsed.schema == null || parsed.table == null) {
            return "the query names no table with its schema";
        }
        if (tables.size() != 1 || tables.get(0).schema() == null
                || !tables.get(0).schema().equalsIgnoreCase(parsed.schema)
                || !tables.get(0).table().equalsIgnoreCase(parsed.table)) {
            List<String> names = new ArrayList<>(tables.size());
            for (ParquetQueryParser.JoinTable table : tables) {
                names.add(table.schema() == null ? table.table() : table.schema() + "." + table.table());
            }
            return "the query reads " + names + ", not only " + parsed.schema + "." + parsed.table;
        }
        if (concatenatable(parsed) || CopyBatchMerge.of(sql, parsed) != null) {
            return null;
        }
        String why = parsed.completeAnswerReason != null ? parsed.completeAnswerReason
                : parsed.singleNodeFeature() != null ? "it uses " + parsed.singleNodeFeature()
                : CompleteAnswerMerge.partsRefusal(parsed);
        return "its answer cannot be merged from batches of copies"
                + (why == null ? (parsed.hasOrderOrLimit() ? " (ORDER BY without an aggregate, DISTINCT"
                        + " or LIMIT)" : "") : ": " + why);
    }

    /**
     * Tells whether two schemas have the same column names and types, nullability and metadata
     * aside.
     *
     * @param a first schema
     * @param b second schema
     * @return true when the columns agree
     */
    static boolean sameColumns(Schema a, Schema b) {
        return sameFields(a.getFields(), b.getFields());
    }

    /**
     * Tells whether two field lists agree in names, types and children.
     *
     * @param a first fields
     * @param b second fields
     * @return true when they agree
     */
    private static boolean sameFields(List<Field> a, List<Field> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            Field x = a.get(i);
            Field y = b.get(i);
            if (!x.getName().equals(y.getName()) || !x.getType().equals(y.getType())
                    || !sameFields(x.getChildren(), y.getChildren())) {
                return false;
            }
        }
        return true;
    }

    /**
     * A complete answer's SQL with the tables this node lacks blocks of read from local copies.
     *
     * @param sql SQL to run, unchanged when nothing is copied
     * @param lease copies the SQL reads, or null; closed when the answer has been streamed
     */
    public record CopiedQuery(String sql, LocalCopyCache.Lease lease) implements AutoCloseable {
        /**
         * Tells whether the SQL reads local copies.
         *
         * @return true when files were copied for it
         */
        public boolean copied() {
            return lease != null;
        }

        @Override
        public void close() {
            if (lease != null) {
                lease.close();
            }
        }
    }

    /**
     * Prepares a complete answer on a node lacking blocks of the tables it reads (HDFS read
     * short-circuit only, item 30): copies the files of those tables that this node cannot read
     * to the node-local cache and replaces each reference to such a table (named with its
     * schema) by {@code read_parquet} over the files as they are read here, local copies for the
     * missing ones, typed as the table's view. SQL is returned unchanged when local copies are
     * off, when every file is readable here, or when DuckDB cannot name the query's tables;
     * table names written without a schema are qualified first ({@link #qualifyPlainNames}).
     * Every query that reads a table of the data store logs one {@code hdfs=reads} line, also
     * when nothing is copied ({@code localCopy=0}).
     *
     * @param clientSql SQL of the complete answer (restrictions already applied)
     * @return SQL to run and the copies it reads; close it after streaming
     * @throws IOException when a file cannot be listed or copied
     * @throws IllegalArgumentException when a table whose files are copied is also read in a way
     *         the rewrite cannot replace (e.g. without its schema), naming its files
     */
    public CopiedQuery completeFromCopies(String clientSql) throws IOException {
        return completeFromCopies(clientSql, null);
    }

    /**
     * Prepares a complete answer on a node lacking blocks of the tables it reads, as
     * {@link #completeFromCopies(String)}; when the copies are refused for lack of room, a WARN
     * line {@code hdfs=completeInCopyBatches skipped} gives the reason the answer was not read in
     * batches of copies (item 33).
     *
     * @param clientSql SQL of the complete answer (restrictions already applied)
     * @param notBatched why the answer is not read in batches of copies, or null
     * @return SQL to run and the copies it reads; close it after streaming
     * @throws IOException when a file cannot be listed or copied
     * @throws IllegalArgumentException when a table whose files are copied is also read in a way
     *         the rewrite cannot replace (e.g. without its schema), naming its files
     */
    public CopiedQuery completeFromCopies(String clientSql, String notBatched) throws IOException {
        if (!parquetAdapter.localCopies() || TimeTravelClause.present(clientSql)) {
            return new CopiedQuery(clientSql, null);
        }
        String sql;
        try {
            sql = qualifyPlainNames(clientSql);
        } catch (java.io.UncheckedIOException e) {
            throw e.getCause();
        }
        List<ParquetQueryParser.JoinTable> tables;
        try {
            tables = QueryTables.of(sql);
        } catch (IllegalArgumentException e) {
            logNoFileReads("unknown");
            return new CopiedQuery(sql, null);
        }
        DataStore store = parquetAdapter.dataStore();
        Map<String, List<DataFile>> copied = new LinkedHashMap<>();
        List<DataFile> unreadable = new ArrayList<>();
        Set<String> listedTables = new HashSet<>();
        List<String> copiedNames = new ArrayList<>();
        int files = 0;
        for (ParquetQueryParser.JoinTable table : tables) {
            String schema = schemaRead(table);
            String key = copyKey(schema, table.table());
            if (schema == null || listedTables.contains(key)
                    || parquetAdapter.isCatalogSchema(schema)) {
                continue;
            }
            List<DataFile> listed;
            try {
                listed = parquetAdapter.tableFiles(schema, table.table());
            } catch (IllegalArgumentException | java.io.FileNotFoundException e) {
                continue;   // not a table of the data store: DuckDB reports it
            }
            listedTables.add(key);
            files += listed.size();
            List<DataFile> missing = new ArrayList<>();
            for (DataFile file : listed) {
                if (!store.readableHere(file)) {
                    missing.add(file);
                }
            }
            if (!missing.isEmpty()) {
                copied.put(key, listed);
                copiedNames.add(schema + "." + table.table());
                unreadable.addAll(missing);
            }
        }
        if (copied.isEmpty()) {
            if (listedTables.isEmpty()) {
                logNoFileReads("none");
            } else {
                // Every file is held whole here: still one line per query on how it is read.
                copies(List.of(), files);
            }
            return new CopiedQuery(sql, null);
        }
        List<TableReferences.Reference> references = TableReferences.find(sql,
                (schema, table) -> copied.containsKey(copyKey(schema, table)));
        // Every read of a copied table must be one of the replaced references.
        List<String> checks = new ArrayList<>(references.size());
        for (TableReferences.Reference reference : references) {
            checks.add(DuckDbAdapter.quoteIdentifier(COPY_CHECK_SCHEMA) + "."
                    + DuckDbAdapter.quoteIdentifier(reference.table()));
        }
        for (ParquetQueryParser.JoinTable table : QueryTables.of(
                TableReferences.rewrite(sql, references, checks))) {
            // A name written without its schema that the rewrite cannot replace reads the
            // table through DuckDB's alias view, from files this node lacks blocks of.
            if (copied.containsKey(copyKey(schemaRead(table), table.table()))) {
                List<String> names = new ArrayList<>();
                for (DataFile file : unreadable) {
                    names.add(file.relativePath());
                }
                throw new IllegalArgumentException(QueryPlanner.unreadableFilesMessage(names,
                        "a reference to " + (table.schema() == null ? "" : table.schema() + ".")
                        + table.table()
                        + " cannot be read from local copies; this node lacks blocks of"));
            }
        }
        LocalCopyCache.Lease lease;
        try {
            lease = copies(unreadable, files);
        } catch (LocalCopyCache.NoRoomException e) {
            if (notBatched != null) {
                LOGGER.warn("qid={} node={} hdfs=completeInCopyBatches skipped tables={} localCopy={}"
                        + " copyBytes={} reason='{}'", LogUtil.qid(), LogUtil.node(), copiedNames,
                        unreadable.size(), bytes(unreadable), notBatched);
            }
            throw e;
        }
        try {
            List<String> sources = new ArrayList<>(references.size());
            for (TableReferences.Reference reference : references) {
                List<String> paths = new ArrayList<>();
                for (DataFile file : copied.get(copyKey(reference.schema(), reference.table()))) {
                    paths.add(duckDbUri(store, file.relativePath(), lease));
                }
                String source = tableSource(reference.schema(), reference.table(), paths);
                if (!source.startsWith("(")) {
                    source = "(SELECT * FROM " + source + ")";
                }
                sources.add(source + (reference.aliased() ? ""
                        : " AS " + DuckDbAdapter.quoteIdentifier(reference.table())));
            }
            LOGGER.info("qid={} node={} hdfs=completeFromCopies tables={} references={}",
                    LogUtil.qid(), LogUtil.node(), copied.size(), references.size());
            return new CopiedQuery(TableReferences.rewrite(sql, references, sources), lease);
        } catch (RuntimeException e) {
            lease.close();
            throw e;
        }
    }

    /**
     * Names the schema a table of a query is read from: its own, or for a name without one the
     * one schema DuckDB resolves it to through the alias views of {@code main}.
     *
     * @param table table as the query names it
     * @return schema name, or null when the name is in no schema or in several
     * @throws IOException when the schemas cannot be listed or described
     */
    private String schemaRead(ParquetQueryParser.JoinTable table) throws IOException {
        if (table.schema() != null) {
            return table.schema();
        }
        try {
            return catalogViews.plainOwner(table.table());
        } catch (java.sql.SQLException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    /**
     * Keys a table by its lower-case schema and name.
     *
     * @param schema schema name, or null
     * @param table table name
     * @return key
     */
    private static String copyKey(String schema, String table) {
        return (schema + "\u0000" + table).toLowerCase(java.util.Locale.ROOT);
    }

    /**
     * Picks the files of a ticket this node cannot read where they are, listing each table
     * directory once for the block hosts.
     *
     * @param store data store
     * @param fileUris relative paths of the ticket
     * @return files to copy, in ticket order
     * @throws IOException when a directory or file cannot be listed
     */
    static List<DataFile> filesToCopy(DataStore store, String[] fileUris) throws IOException {
        Map<String, Map<String, DataFile>> listings = new HashMap<>();
        List<DataFile> result = new ArrayList<>();
        for (String rel : fileUris) {
            String directory = tableDirectory(rel);
            Map<String, DataFile> listed = listings.get(directory);
            if (listed == null) {
                listed = new HashMap<>();
                for (DataFile file : store.parquetFiles(directory)) {
                    listed.put(file.relativePath(), file);
                }
                listings.put(directory, listed);
            }
            DataFile file = listed.get(rel);
            if (file == null) {
                // Not a Parquet file of that directory: without hosts it counts as not readable.
                file = store.file(rel);
            }
            if (!store.readableHere(file)) {
                result.add(file);
            }
        }
        return result;
    }

    /**
     * Returns the table directory of a relative file path: its first two segments
     * ({@code schema/table}), or its parent for a shorter path.
     *
     * @param relativePath relative file path
     * @return directory relative to the data directory, empty for the data directory
     */
    static String tableDirectory(String relativePath) {
        int first = relativePath.indexOf('/');
        int second = first < 0 ? -1 : relativePath.indexOf('/', first + 1);
        if (second > 0) {
            return relativePath.substring(0, second);
        }
        return first < 0 ? "" : relativePath.substring(0, first);
    }

    /**
     * Returns the node-local copy cache, made on first use below the DuckDB temp root.
     *
     * @return cache
     * @throws IOException when its directory cannot be created
     */
    private synchronized LocalCopyCache localCopyCache() throws IOException {
        if (localCopyCache == null) {
            DataStore store = parquetAdapter.dataStore();
            long maxBytes = ConfigAdapter.hdfsLocalCopyMaxBytes();
            int threads = ConfigAdapter.hdfsLocalCopyThreads();
            Path root = Files.createDirectories(duckDbAdapter.localCopyRoot());
            long diskBytes = Files.getFileStore(root).getTotalSpace();
            ConfigSource config = ConfigSource.system();
            boolean reserveSet = ConfigAdapter.hdfsLocalCopyMinFreeBytesSet(config);
            long minFreeBytes = ConfigAdapter.hdfsLocalCopyMinFreeBytes(config, diskBytes);
            localCopyCache = new LocalCopyCache(root, maxBytes, threads,
                    file -> store.inputFile(file).newStream(), minFreeBytes, LocalCopyCache.FILE_STORE);
            String reserve = reserveSet ? "set" : "default";
            if (diskBytes > 0 && minFreeBytes >= diskBytes) {
                LOGGER.warn("node={} hdfs=localCopyCache dir={} maxBytes={} threads={} minFreeBytes={}"
                        + " reserve={} diskBytes={}: the reserve is not below the disk's size, every"
                        + " copy will be refused; lower hdfsLocalCopyMinFreeBytes or give"
                        + " hdfsLocalCopyDir a larger disk", LogUtil.node(), localCopyCache.directory(),
                        maxBytes, threads, minFreeBytes, reserve, diskBytes);
            } else {
                LOGGER.info("node={} hdfs=localCopyCache dir={} maxBytes={} threads={} minFreeBytes={}"
                        + " reserve={} diskBytes={}", LogUtil.node(), localCopyCache.directory(),
                        maxBytes, threads, minFreeBytes, reserve, diskBytes);
            }
        }
        return localCopyCache;
    }

    /**
     * Returns the last segment of a relative file path, for logs.
     *
     * @param relativePath relative file path
     * @return file name
     */
    static String fileName(String relativePath) {
        return relativePath.substring(relativePath.lastIndexOf('/') + 1);
    }

    /**
     * Recursively lists Parquet files for a schema.table key.
     *
     * @param key schema.table or table name
     * @return DuckDB-compatible file paths
     * @throws IOException on storage read failure
     */
    /**
     * Picks the files of one joined table from the files of a ticket.
     *
     * @param store data store
     * @param key {@code schema.table} or {@code table}
     * @param ticketFiles relative paths of the ticket, null when there is no ticket
     * @param copies local copies of the files this node cannot read where they are, or null
     * @return DuckDB paths of the table's files in the ticket, empty when it names none
     */
    static List<String> plannedTableFiles(DataStore store, String key, String[] ticketFiles,
            LocalCopyCache.Lease copies) {
        if (store == null || ticketFiles == null) {
            return List.of();
        }
        String directory = key.replace('.', '/') + "/";
        List<String> uris = new ArrayList<>();
        for (String file : ticketFiles) {
            String plain = store.unversionedPath(file);
            if (plain != null && plain.startsWith(directory)) {
                uris.add(duckDbUri(store, file, copies));
            }
        }
        return uris;
    }

    private List<String> resolveTableFiles(String key) throws IOException {
        long t = LogUtil.mark();
        int dot = key.indexOf('.');
        String schema = dot > 0 ? key.substring(0, dot) : null;
        String table = dot > 0 ? key.substring(dot + 1) : key;
        String dir = schema != null ? schema + "/" + table : table;
        DataStore store = parquetAdapter.dataStore();
        List<String> uris = new ArrayList<>();
        for (DataFile f : store.parquetFiles(dir)) {
            uris.add(store.duckDbUri(f));
        }
        LogUtil.logTiming(t, "engine:join.resolveTableFiles", "table=" + key + " " + FILES_TIMING_PREFIX + uris.size());
        return uris;
    }

    // ── emit ─────────────────────────────────────────────────────────────

    /**
     * Emits aggregation result rows as Arrow batches through the listener.
     *
     * @param allocator     Arrow buffer allocator
     * @param query         SQL query
     * @param pq            parsed query
     * @param rows          result rows
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws InterruptedException if interrupted during send
     */
    @SuppressWarnings("java:S3776") // Vector population must branch on each supported Arrow vector type.
    private void emitRowsAsArrow(BufferAllocator allocator, String query, ParquetQueryParser pq,
            List<Object[]> rows, FlightProducer.ServerStreamListener listener,
            boolean startListener) throws InterruptedException {
        long t = LogUtil.mark();
        // The same schema the client was given for this query, so both always agree.
        Schema aggSchema = resultSchema(allocator, query);
        int numGbCols = pq.groupByColumnNames.size();

        List<FieldVector> vectors = new ArrayList<>();
        for (Field field : aggSchema.getFields()) {
            FieldVector v = field.createVector(allocator);
            if (v instanceof FixedWidthVector fv) {
                fv.allocateNew(rows.size());
            } else if (v instanceof VariableWidthVector vv) {
                vv.allocateNew(rows.size() * 16);
            }
            vectors.add(v);
        }

        try (VectorSchemaRoot root = new VectorSchemaRoot(aggSchema.getFields(), vectors)) {
            root.setRowCount(rows.size());
            int vecIdx = 0;
            int aggIdx = 0;
            for (ParquetQueryParser.SelectExpr expr : pq.selectExprs) {
                FieldVector vec = vectors.get(vecIdx++);
                if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN) {
                    int gbPos = pq.groupByColumnNames.indexOf(expr.inputColumn);
                    for (int r = 0; r < rows.size(); r++) {
                        setVectorValue(vec, r, rows.get(r)[gbPos]);
                    }
                } else {
                    int pos = numGbCols + aggIdx++;
                    for (int r = 0; r < rows.size(); r++) {
                        setVectorValue(vec, r, rows.get(r)[pos]);
                    }
                }
            }
            if (startListener) {
                listener.start(root);
            }
            if (!rows.isEmpty()) {
                if (DuckDbAdapter.awaitListenerReady(
                        listener, appConfig.flightListenerReadyTimeoutMillis())) {
                    listener.putNext();
                }
            }
        }
        LogUtil.logTiming(t, "engine:agg.emitRows", "rows=" + rows.size());
    }



    /**
     * Merges aggregation columns from one row into another.
     *
     * @param exprs     select expressions
     * @param into      target accumulators
     * @param from      source row
     * @param numGbCols number of group-by columns
     */
    static void mergeAggCols(List<ParquetQueryParser.SelectExpr> exprs,
            Object[] into, Object[] from, int numGbCols) {
        int aggIdx = 0;
        for (ParquetQueryParser.SelectExpr expr : exprs) {
            if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN) {
                continue;
            }
            int pos = numGbCols + aggIdx++;
            switch (expr.func) {
                case COUNT_STAR, COUNT -> into[pos] = addLongs(into[pos], from[pos]);
                case SUM -> into[pos] = addNumbers(into[pos], from[pos]);
                case MIN -> into[pos] = minOf(into[pos], from[pos]);
                case MAX -> into[pos] = maxOf(into[pos], from[pos]);
                default -> { }
            }
        }
    }

    /**
     * Extracts a long value from a FieldVector at the given index.
     *
     * @param vec   field vector
     * @param index row index
     * @return long value
     */
    static long toLong(FieldVector vec, int index) {
        if (vec instanceof BigIntVector v) {
            return v.get(index);
        }
        if (vec instanceof IntVector v) {
            return v.get(index);
        }
        if (vec instanceof SmallIntVector v) {
            return v.get(index);
        }
        if (vec instanceof TinyIntVector v) {
            return v.get(index);
        }
        return ((Number) vec.getObject(index)).longValue();
    }

    /**
     * Extracts a double value from a FieldVector at the given index.
     *
     * @param vec   field vector
     * @param index row index
     * @return double value
     */
    static double toDouble(FieldVector vec, int index) {
        if (vec instanceof Float8Vector v) {
            return v.get(index);
        }
        if (vec instanceof Float4Vector v) {
            return v.get(index);
        }
        return ((Number) vec.getObject(index)).doubleValue();
    }

    /**
     * Returns the minimum of two comparable values, null-safe.
     *
     * @param a first value
     * @param b second value
     * @return minimum value
     */
    @SuppressWarnings("unchecked")
    static Object minOf(Object a, Object b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return ((Comparable<Object>) a).compareTo(b) <= 0 ? a : b;
    }

    /**
     * Returns the maximum of two comparable values, null-safe.
     *
     * @param a first value
     * @param b second value
     * @return maximum value
     */
    @SuppressWarnings("unchecked")
    static Object maxOf(Object a, Object b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return ((Comparable<Object>) a).compareTo(b) >= 0 ? a : b;
    }

    /**
     * Adds two long accumulators, null-safe.
     *
     * @param a first accumulator
     * @param b second accumulator
     * @return sum as Long
     */
    static Object addLongs(Object a, Object b) {
        if (a == null) {
            return b == null ? 0L : ((Number) b).longValue();
        }
        if (b == null) {
            return ((Number) a).longValue();
        }
        return ((Number) a).longValue() + ((Number) b).longValue();
    }

    /**
     * Adds two double accumulators, null-safe.
     *
     * @param a first accumulator
     * @param b second accumulator
     * @return sum as Double
     */
    static Object addDoubles(Object a, Object b) {
        if (a == null) {
            return b == null ? 0.0 : ((Number) b).doubleValue();
        }
        if (b == null) {
            return ((Number) a).doubleValue();
        }
        return ((Number) a).doubleValue() + ((Number) b).doubleValue();
    }

    /**
     * Adds numeric aggregate values while preserving decimal precision.
     *
     * @param a first accumulator
     * @param b second accumulator
     * @return decimal sum when either value is decimal, otherwise a double sum
     */
    static Object addNumbers(Object a, Object b) {
        if (a == null) {
            return b == null ? 0.0 : b;
        }
        if (b == null) {
            return a;
        }
        if (a instanceof BigDecimal || b instanceof BigDecimal) {
            return toBigDecimal(a).add(toBigDecimal(b));
        }
        return addDoubles(a, b);
    }

    /**
     * Converts a numeric aggregate value to BigDecimal without losing integral precision.
     *
     * @param value numeric value
     * @return decimal representation
     */
    private static BigDecimal toBigDecimal(Object value) {
        if (value instanceof BigDecimal decimal) {
            return decimal;
        }
        if (value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long) {
            return BigDecimal.valueOf(((Number) value).longValue());
        }
        return BigDecimal.valueOf(((Number) value).doubleValue());
    }

    /**
     * Sets a value in a FieldVector at the given index, handling all supported types.
     *
     * @param vec   field vector
     * @param index row index
     * @param value value to set
     */
    @SuppressWarnings("unchecked")
    static void setVectorValue(FieldVector vec, int index, Object value) {
        if (value == null) {
            vec.setNull(index);
            return;
        }
        if (vec instanceof BigIntVector) {
            ((BigIntVector) vec).setSafe(index, ((Number) value).longValue());
        } else if (vec instanceof IntVector) {
            ((IntVector) vec).setSafe(index, ((Number) value).intValue());
        } else if (vec instanceof SmallIntVector) {
            ((SmallIntVector) vec).setSafe(index, ((Number) value).shortValue());
        } else if (vec instanceof TinyIntVector) {
            ((TinyIntVector) vec).setSafe(index, ((Number) value).byteValue());
        } else if (vec instanceof DateDayVector) {
            ((DateDayVector) vec).setSafe(index, ((Number) value).intValue());
        } else if (vec instanceof DecimalVector decimal) {
            decimal.setSafe(index, toBigDecimal(value)
                    .setScale(decimal.getScale(), java.math.RoundingMode.HALF_UP));
        } else if (vec instanceof Float8Vector) {
            ((Float8Vector) vec).setSafe(index, ((Number) value).doubleValue());
        } else if (vec instanceof Float4Vector) {
            ((Float4Vector) vec).setSafe(index, ((Number) value).floatValue());
        } else if (vec instanceof BitVector) {
            ((BitVector) vec).setSafe(index, ((Boolean) value) ? 1 : 0);
        } else if (vec instanceof VarCharVector) {
            byte[] bytes = value instanceof Text
                    ? ((Text) value).getBytes()
                    : value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ((VarCharVector) vec).setSafe(index, bytes);
        }
    }

    /**
     * Partitions items into numGroups using round-robin distribution.
     *
     * @param items     items to partition
     * @param numGroups number of groups
     * @return partitioned lists
     */
    static <T> List<List<T>> partitionIntoGroups(List<T> items, int numGroups) {
        List<List<T>> groups = new ArrayList<>(numGroups);
        for (int i = 0; i < numGroups; i++) {
            groups.add(new ArrayList<>());
        }
        for (int i = 0; i < items.size(); i++) {
            groups.get(i % numGroups).add(items.get(i));
        }
        return groups;
    }
}
