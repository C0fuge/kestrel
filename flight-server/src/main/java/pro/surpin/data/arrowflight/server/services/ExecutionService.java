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
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.services.ParquetQueryParser;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.model.ExecutionPath;
import pro.surpin.data.arrowflight.server.model.ExecutionPathTracker;
import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.server.storage.DataFile;
import pro.surpin.data.arrowflight.server.storage.DataStore;

import java.math.BigDecimal;

/**
 * Orchestrates query execution through DuckDB.
 */
public final class ExecutionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionService.class);
    private static final String FILES_TIMING_PREFIX = "files=";
    private static final long SCHEMA_TTL_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final int SCHEMA_CACHE_CAPACITY = 1024;

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
            return duckDbAdapter.describeComplete(allocator, query, values, catalogViews.current());
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            String message = String.valueOf(e.getMessage());
            if (message.contains("Error: ")) {
                // DuckDB's own errors (parser, binder, catalog, conversion, not implemented)
                // describe the client's SQL.
                throw new IllegalArgumentException(message.lines().findFirst().orElse(message), e);
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
            duckDbAdapter.streamComplete(allocator, query, values, catalogViews.current(), listener,
                    startListener);
        } catch (java.sql.SQLException e) {
            String message = String.valueOf(e.getMessage());
            if (isQueryError(message)) {
                throw new IllegalArgumentException(message.lines().findFirst().orElse(message), e);
            }
            throw e;
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
            return duckDbAdapter.describeParameters(query, catalogViews.current());
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            String message = String.valueOf(e.getMessage());
            if (message.contains("Error: ")) {
                throw new IllegalArgumentException(message.lines().findFirst().orElse(message), e);
            }
            throw new IllegalStateException("Unable to describe query parameters: " + message, e);
        }
    }

    /**
     * Returns the schema of the batches a query streams, as DuckDB reports it for the same SQL.
     *
     * @param allocator Arrow buffer allocator
     * @param query     SQL query
     * @return result schema
     * @throws IllegalArgumentException if DuckDB rejects the query
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
            if (message.startsWith("Binder Error") || message.startsWith("Parser Error")
                    || message.startsWith("Catalog Error")
                    || message.startsWith("Conversion Error")) {
                throw new IllegalArgumentException(message.lines().findFirst().orElse(message), e);
            }
            // Files may be unreadable from this node; the table metadata still describes them.
            LOGGER.debug("node={} schema=describeFallback error='{}'", LogUtil.node(), message);
            schema = metadataService.getQuerySchema(query);
            if (schema != null && widenUnsigned) {
                schema = CatalogViews.widenUnsigned(schema);
            }
        }
        if (schema != null) {
            synchronized (schemaCache) {
                schemaCache.put(query, new CachedSchema(schema, now + SCHEMA_TTL_NANOS));
                if (schemaCache.size() > SCHEMA_CACHE_CAPACITY) {
                    schemaCache.remove(schemaCache.keySet().iterator().next());
                }
            }
        }
        return schema;
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
            try {
                createJoinViews(conn, pq, aliases, true);
                return duckDbAdapter.describeSql(allocator, pq.duckDbSql);
            } finally {
                dropJoinViews(conn, aliases);
            }
        }
        DataFile sample = parquetAdapter.sampleFile(pq.schema, pq.table)
                .orElseThrow(() -> new IOException(
                        "No Parquet files for table " + pq.schema + "." + pq.table));
        String from = "(SELECT * FROM " + tableSource(pq.schema, pq.table, List.of(
                parquetAdapter.dataStore().duckDbUri(sample)))
                + " LIMIT 0)";
        String duckSql = pq.hasAggregation
                ? DuckDbAdapter.buildDuckSqlWithFilter(pq, from, false)
                : DuckDbAdapter.buildSelectSql(pq, from);
        return duckDbAdapter.describeSql(allocator, duckSql);
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
            executeJoin(allocator, parsedQuery, fileUris, listener, startListener);
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

        long tResolve = LogUtil.mark();
        List<String> resolvedUris = resolveUris(fileUris);
        LogUtil.logTiming(tResolve, "files.resolveUris", FILES_TIMING_PREFIX + resolvedUris.size());

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

        boolean noGroupByNoFilter = pq.groupByColumnNames.isEmpty()
                && (pq.filter == null || pq.filter.isBlank())
                && !pq.hasOrderOrLimit()
                && !pq.distinct && pq.having == null
                && pq.selectExprs.stream().noneMatch(e -> e.distinct)
                && !pq.selectExprs.isEmpty();

        // Fast path A: COUNT(*) only — read row counts from Parquet footer
        if (noGroupByNoFilter
                && pq.selectExprs.stream().allMatch(
                        e -> e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR)) {
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
            String[] fileUris, FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {
        long t = LogUtil.mark();
        Connection conn = duckDbAdapter.connection();
        List<String> registeredAliases = new ArrayList<>();
        try {
            createJoinViews(conn, pq, registeredAliases, false);
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
            List<String> registeredAliases, boolean empty) throws Exception {
        Map<String, List<String>> tableFiles = new LinkedHashMap<>();
        for (ParquetQueryParser.JoinTable jt : pq.joinTables) {
            String key = (jt.schema() != null ? jt.schema() + "." : "") + jt.table();
            tableFiles.computeIfAbsent(key, k -> {
                try {
                    return resolveTableFiles(k);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
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
     * Names relative files the way DuckDB reads them.
     *
     * @param fileUris relative file paths
     * @return DuckDB file paths
     */
    private List<String> resolveUris(String[] fileUris) {
        long t = LogUtil.mark();
        DataStore store = parquetAdapter.dataStore();
        List<String> uris = new ArrayList<>(fileUris.length);
        for (String rel : fileUris) {
            uris.add(store.duckDbUri(rel));
        }
        LogUtil.logTiming(t, "files.resolveUris", FILES_TIMING_PREFIX + uris.size());
        return uris;
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
