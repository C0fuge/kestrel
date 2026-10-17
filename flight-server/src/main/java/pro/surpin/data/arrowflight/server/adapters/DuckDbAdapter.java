package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import pro.surpin.data.arrowflight.server.services.ParquetQueryParser;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.LogUtil;

/**
 * Manages DuckDB connection pool and SQL execution.
 * Each Java worker thread gets its own DuckDB in-memory connection, closed after the thread ends.
 */
public final class DuckDbAdapter implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DuckDbAdapter.class);
    private static final long LISTENER_STATE_POLL_NANOS =
            TimeUnit.MILLISECONDS.toNanos(5);
    private static final long LISTENER_TIMING_LOG_THRESHOLD_NANOS =
            TimeUnit.MILLISECONDS.toNanos(100);
    private static final long SESSION_REAP_PERIOD_SECONDS = 15;
    private static final String TEMP_ROOT_PREFIX = "duckdb-";
    private final Path tempRoot;
    private final AtomicInteger sessionIds = new AtomicInteger();
    /** Directories DuckDB may read when locked down (as DuckDB sees the paths). */
    private final List<String> readableRoots;
    /** First words of the statements a client may run in complete-answer mode. */
    private static final Set<String> QUERY_KEYWORDS = Set.of(
            "select", "with", "values", "from", "table", "describe", "show", "summarize");
    /**
     * Parses a statement without running it. DuckDB serializes only SELECT statements (DESCRIBE,
     * SHOW, SUMMARIZE, VALUES, FROM and TABLE are SELECTs inside), so any other statement anywhere
     * in the text makes it an error. Columns: error, error type, error message, statement count.
     */
    private static final String PARSE_CHECK_SQL = "SELECT coalesce((j->>'error')::BOOLEAN, true),"
            + " j->>'error_type', j->>'error_message', coalesce(json_array_length(j->'statements'), 0),"
            + " j::VARCHAR FROM (SELECT json_serialize_sql(?::VARCHAR) AS j)";
    /**
     * Functions and system views a client query may not use. Logging is switched on by a table
     * function that lock_configuration does not stop, and the log of a DuckDB shared by many
     * clients would show their queries; secrets describe storage credentials; query(),
     * query_table() and json_execute_serialized_sql() run text that this check never sees.
     */
    private static final Set<String> REFUSED_FUNCTIONS = Set.of(
            "enable_logging", "disable_logging", "truncate_duckdb_logs", "duckdb_logs",
            "duckdb_log_contexts", "duckdb_secrets", "which_secret", "query", "query_table",
            "json_execute_serialized_sql");
    /** A function or table name in DuckDB's serialized statement. */
    private static final java.util.regex.Pattern SERIALIZED_NAME = java.util.regex.Pattern.compile(
            "\"(?:function_name|table_name)\":\"((?:[^\"\\\\]|\\\\.)*)\"");
    /** How often a running query checks whether its client has cancelled. */
    private static final long CANCEL_POLL_MILLIS = 200;
    /** Longest time a query may take to describe; it runs over views without rows. */
    private static final long DESCRIBE_TIMEOUT_SECONDS = 30;
    private final ThreadLocal<Session> threadSession;
    private final Set<Session> allSessions = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService sessionReaper;
    private final ExecutorService ioPool;

    private final int batchSize;
    private final int duckDbGroups;
    private final AppConfig appConfig;

    /** Keeps prepared statements bounded to one worker thread and closes evicted entries. */
    static final class StatementCache extends LinkedHashMap<String, PreparedStatement> {
        private final int limit;

        /**
         * Creates a cache with a finite entry limit.
         *
         * @param limit maximum statements retained
         */
        StatementCache(int limit) {
            super(16, 0.75f, true);
            if (limit <= 0) {
                throw new IllegalArgumentException("Statement cache limit must be positive");
            }
            this.limit = limit;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, PreparedStatement> eldest) {
            if (size() <= limit) {
                return false;
            }
            try {
                eldest.getValue().close();
            } catch (SQLException e) {
                LOGGER.warn("Failed to close evicted DuckDB statement", e);
            }
            return true;
        }
    }

    /**
     * Creates DuckDB adapter with thread-local connection pool.
     *
     * @param appConfig server configuration
     * @param ioPool    shared I/O thread pool for parallel execution
     */
    public DuckDbAdapter(AppConfig appConfig, ExecutorService ioPool) {
        this(appConfig, ioPool, List.of());
    }

    /**
     * Creates DuckDB adapter whose connections may read only the given directories.
     *
     * @param appConfig     server configuration
     * @param ioPool        shared I/O thread pool for parallel execution
     * @param readableRoots data directories as DuckDB sees them (for example
     *                      {@code hdfs://cluster/data/parquet}); empty disables the lockdown
     */
    public DuckDbAdapter(AppConfig appConfig, ExecutorService ioPool, List<String> readableRoots) {
        this.readableRoots = List.copyOf(readableRoots);
        this.appConfig = appConfig;
        this.ioPool = ioPool;
        this.batchSize = appConfig.batchSize();
        this.duckDbGroups = appConfig.duckDbGroups();

        this.tempRoot = createTempRoot(appConfig.duckDbTempDirectory());
        this.threadSession = ThreadLocal.withInitial(() -> {
            try {
                Properties props = new Properties();
                if (appConfig.duckDbAllowUnsignedExtensions()) {
                    props.setProperty("allow_unsigned_extensions", "true");
                }
                if (appConfig.duckDbStreamResults()) {
                    // Without it DuckDB buffers the whole result before the first batch is sent.
                    props.setProperty("jdbc_stream_results", "true");
                }
                Connection conn = DriverManager.getConnection("jdbc:duckdb:", props);
                configureConnection(conn);
                Session session = new Session(Thread.currentThread(), conn,
                        appConfig.duckDbStatementCacheSize());
                allSessions.add(session);
                return session;
            } catch (Exception e) {
                throw new RuntimeException("Failed to create thread-local DuckDB connection", e);
            }
        });
        // Flight runs streams on a cached thread pool: its threads end after a minute of
        // idleness and new ones are created later, so sessions of ended threads must be closed.
        this.sessionReaper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "duckdb-session-reaper");
            t.setDaemon(true);
            return t;
        });
        this.sessionReaper.scheduleWithFixedDelay(this::closeSessionsOfEndedThreads,
                SESSION_REAP_PERIOD_SECONDS, SESSION_REAP_PERIOD_SECONDS, TimeUnit.SECONDS);

        int warmCount = appConfig.duckDbWarmConnections();
        if (warmCount > 0) {
            LOGGER.info("node={} duckdb=warmup connections={}", LogUtil.node(), warmCount);
            List<Future<Connection>> futs = new ArrayList<>(warmCount);
            for (int i = 0; i < warmCount; i++) {
                futs.add(ioPool.submit(() -> threadSession.get().conn));
            }
            for (Future<Connection> f : futs) {
                try {
                    f.get(30, TimeUnit.SECONDS);
                } catch (Exception e) {
                    LOGGER.warn("node={} duckdb=warmupFailed", LogUtil.node(), e);
                }
            }
            LOGGER.info("node={} duckdb=warmupDone", LogUtil.node());
        }
    }

    /**
     * Configure DuckDB connection settings
     * @param conn JDBC connection
     * @throws Exception on configuration failure
     */
    private void configureConnection(Connection conn) throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("SET threads = " + appConfig.duckDbThreads());
            String memLimit = appConfig.duckDbMemoryLimit();
            if (memLimit != null && !memLimit.isBlank()) {
                s.execute("SET memory_limit = " + sqlStringLiteral(memLimit));
                LOGGER.info("node={} duckdb=memoryLimit limit={}", LogUtil.node(), memLimit);
            }
            // Every connection is a DuckDB instance of its own. They all spill to ".tmp" by default,
            // into files with the same names, and an instance removes that directory when closed.
            s.execute("SET temp_directory = " + sqlStringLiteral(
                    tempRoot.resolve("c" + sessionIds.incrementAndGet()).toString()));
            if (!appConfig.duckDbExternalFileCache()) {
                // The cache keeps file data read through an extension until memory_limit is
                // reached, in every connection; the OS page cache already does that job.
                s.execute("SET enable_external_file_cache = false");
            }
            String dataDir = appConfig.dataDir();
            if (dataDir != null) {
                setArrayOptionIfPresent(s, dataDir);
            } else {
                s.execute("SET allowed_paths = ARRAY[]");
            }
            String hdfsExtension = appConfig.duckDbHdfsExtension();
            if (hdfsExtension != null) {
                s.execute("LOAD " + sqlStringLiteral(hdfsExtension));
                LOGGER.info("node={} duckdb=hdfsExtensionLoaded path={}",
                        LogUtil.node(), hdfsExtension);
                setOptionIfPresent(s, "hdfs_default_namenode",
                        appConfig.duckDbHdfsDefaultNamenode());
                setOptionIfPresent(s, "hdfs_ha_namenodes",
                        appConfig.duckDbHdfsHaNamenodes());
                setOptionIfPresent(s, "hdfs_shortcircuit",
                        appConfig.duckDbHdfsShortcircuit());
                setOptionIfPresent(s, "hdfs_domain_socket_path",
                        appConfig.duckDbHdfsDomainSocketPath());
            }
            if (appConfig.duckDbLockdown() && !readableRoots.isEmpty()) {
                // Clients may send any SQL in complete-answer mode, and filters reach DuckDB as
                // written in the other mode. Only the data and this connection's spill directory
                // stay readable; ATTACH, COPY, INSTALL and configuration changes are refused.
                List<String> allowed = new ArrayList<>();
                for (String root : readableRoots) {
                    allowed.add(sqlStringLiteral(root.endsWith("/") ? root : root + "/"));
                }
                allowed.add(sqlStringLiteral(tempRoot.toString() + "/"));
                s.execute("SET allowed_directories = [" + String.join(", ", allowed) + "]");
                restrictExtensions(s);
                s.execute("SET enable_external_access = false");
                s.execute("SET lock_configuration = true");
            }
        }
    }

    /**
     * Stops DuckDB from installing or loading extensions on its own: a query naming a function
     * of an extension that is not loaded fails at once instead of trying to download it.
     *
     * @param s statement of the connection to restrict
     * @throws java.sql.SQLException when a setting is refused
     */
    private static void restrictExtensions(Statement s) throws java.sql.SQLException {
        s.execute("SET autoinstall_known_extensions = false");
        s.execute("SET autoload_known_extensions = false");
        s.execute("SET allow_community_extensions = false");
    }

    /**
     * Opens a DuckDB for describing queries: it reads no files and keeps little memory.
     *
     * @return DuckDB connection
     * @throws java.sql.SQLException when DuckDB cannot be opened
     */
    private static Connection openDescribeConnection() throws java.sql.SQLException {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement s = conn.createStatement()) {
            s.execute("SET threads = 1");
            s.execute("SET memory_limit = '256MB'");
            restrictExtensions(s);
            s.execute("SET enable_external_access = false");
            s.execute("SET lock_configuration = true");
        } catch (java.sql.SQLException e) {
            Session.closeQuietly(conn);
            throw e;
        }
        return conn;
    }

    /**
     * Creates this server's directory for DuckDB spill files and removes the ones left behind
     * by server processes that no longer run.
     *
     * @param configured base directory, or blank for ".tmp" in the working directory
     * @return directory that holds one subdirectory per connection
     */
    private static Path createTempRoot(String configured) {
        Path base = Path.of(configured == null || configured.isBlank() ? ".tmp" : configured)
                .toAbsolutePath();
        try {
            Files.createDirectories(base);
            try (DirectoryStream<Path> roots = Files.newDirectoryStream(base, TEMP_ROOT_PREFIX + "*")) {
                for (Path root : roots) {
                    Path name = root.getFileName();
                    String[] parts = name == null ? new String[0] : name.toString().split("-");
                    if (parts.length == 3 && parts[1].chars().allMatch(Character::isDigit)
                            && ProcessHandle.of(Long.parseLong(parts[1])).isEmpty()) {
                        deleteRecursively(root);
                    }
                }
            }
            return Files.createTempDirectory(base,
                    TEMP_ROOT_PREFIX + ProcessHandle.current().pid() + "-");
        } catch (IOException e) {
            throw new IllegalStateException("Cannot create DuckDB temp directory in " + base, e);
        }
    }

    /**
     * Deletes a directory tree, logging a failure.
     *
     * @param root directory to delete
     */
    private static void deleteRecursively(Path root) {
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        } catch (IOException | RuntimeException e) {
            LOGGER.warn("node={} duckdb=tempDirNotDeleted path={} error={}",
                    LogUtil.node(), root, e.toString());
        }
    }

    /**
     * Closes the DuckDB connections whose owner threads have ended.
     */
    void closeSessionsOfEndedThreads() {
        for (Iterator<Session> it = allSessions.iterator(); it.hasNext();) {
            Session session = it.next();
            if (!session.owner.isAlive()) {
                it.remove();
                session.close();
                LOGGER.info("node={} duckdb=sessionClosed thread={} open={}",
                        LogUtil.node(), session.owner.getName(), allSessions.size());
            }
        }
    }

    /**
     * Returns the number of open DuckDB connections.
     *
     * @return open connection count
     */
    int openSessions() {
        return allSessions.size();
    }

    /**
     * Set DuckDB option if value is non-null
     * @param statement JDBC statement
     * @param optionName option name
     * @param value option value
     * @throws Exception on SQL failure
     */
    private static void setOptionIfPresent(Statement statement, String optionName, String value)
            throws Exception {
        if (value == null) {
            return;
        }
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            statement.execute("SET " + optionName + " = " + value.toLowerCase());
        } else {
            statement.execute("SET " + optionName + " = " + sqlStringLiteral(value));
        }
    }

    /**
     * Set DuckDB array option if value is non-null
     *
     * @param statement JDBC statement
     * @param value     option value
     * @throws Exception on SQL failure
     */
    private static void setArrayOptionIfPresent(Statement statement, String value)
            throws Exception {
        if (value == null) {
            return;
        }
        // DuckDB expects VARCHAR[] e.g. SET allowed_paths = ['/path']
        String escaped = sqlStringLiteral(value);
        statement.execute("SET " + "allowed_paths" + " = " + "ARRAY[" + escaped + "]");
    }

    /**
     * Returns this thread's DuckDB connection.
     *
     * @return JDBC connection
     */
    public Connection connection() {
        return threadSession.get().conn;
    }

    /**
     * Returns the shared I/O executor service.
     *
     * @return executor service
     */
    public ExecutorService ioPool() {
        return ioPool;
    }

    /**
     * Returns the number of DuckDB groups for parallel aggregation.
     *
     * @return group count
     */
    public int duckDbGroups() {
        return duckDbGroups;
    }

    /**
     * Returns the Arrow batch size for DuckDB exports.
     *
     * @return batch size
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * Streams DuckDB SQL query results as Arrow batches to a Flight listener.
     * Reuses DuckDB's Arrow root directly because the Flight listener provides
     * bounded backpressure before each synchronous batch handoff.
     *
     * @param allocator    Arrow buffer allocator
     * @param duckSql      DuckDB SQL query
     * @param listener     Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws Exception on query or stream failure
     */
    public void streamSql(BufferAllocator allocator, String duckSql,
            org.apache.arrow.flight.FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {
        String qid = LogUtil.qid();
        long t = LogUtil.mark();
        long streamStartNanos = System.nanoTime();
        LOGGER.debug("qid={} node={} thread={} duckdb=start batchSize={} sql='{}'",
                qid, LogUtil.node(), Thread.currentThread().getName(), batchSize, duckSql);
        long firstBatchNanos = -1;
        long backpressureNanos = 0;
        Session session = threadSession.get();
        // Filters of the other mode reach DuckDB as the client wrote them.
        requireSingleQuery(session.statement(PARSE_CHECK_SQL), duckSql);
        PreparedStatement ps = session.statement(duckSql);
        // A client that goes away interrupts DuckDB at once; otherwise the query would run to its
        // first batch (all of it, for an aggregation) before anybody noticed.
        ListenerReadiness listenerReadiness = new ListenerReadiness(
                listener, appConfig.flightListenerReadyTimeoutMillis(), ps, sessionReaper);
        try (org.duckdb.DuckDBResultSet drs = (org.duckdb.DuckDBResultSet) executeQuery(ps, listener);
                ArrowReader reader = (ArrowReader) drs.arrowExportStream(allocator, batchSize)) {
            VectorSchemaRoot duckRoot = reader.getVectorSchemaRoot();
            if (startListener) {
                listener.start(duckRoot);
            }

            int batchesSent = 0;
            long rowsSent = 0;
            while (reader.loadNextBatch()) {
                int rows = duckRoot.getRowCount();
                if (rows == 0) {
                    continue;
                }
                if (firstBatchNanos < 0) {
                    firstBatchNanos = System.nanoTime() - streamStartNanos;
                    LogUtil.logTiming(t, "duckdb.firstBatch",
                            "sql='" + duckSql.substring(0, Math.min(100, duckSql.length())) + "'");
                }

                long bpStart = System.nanoTime();
                listenerReadiness.await();
                backpressureNanos += System.nanoTime() - bpStart;
                listener.putNext();
                batchesSent++;
                rowsSent += rows;
                if (batchesSent % 10 == 0) {
                    LOGGER.debug("qid={} node={} duckdb=progress batches={} rows={} elapsed={} throughput={}rows/s",
                            qid, LogUtil.node(), batchesSent, rowsSent,
                            LogUtil.elapsedNanos(streamStartNanos),
                            rowsSent * 1_000_000_000L
                                    / Math.max(1, System.nanoTime() - streamStartNanos));
                }
            }

            LogUtil.logTiming(t, "duckdb.streamSql",
                    "batches=" + batchesSent + " rows=" + rowsSent
                    + " backpressureMs=" + backpressureNanos / 1_000_000);
            LOGGER.debug("qid={} node={} duckdb=completed batches={} rows={} ttfB={} backpressureMs={} elapsed={} cancelled={}",
                    qid, LogUtil.node(), batchesSent, rowsSent,
                    firstBatchNanos >= 0 ? formatDuration(firstBatchNanos) : "N/A",
                    backpressureNanos / 1_000_000,
                    LogUtil.elapsedNanos(streamStartNanos), listener.isCancelled());
        } catch (java.sql.SQLException e) {
            if (listener.isCancelled()) {
                throw CallStatus.CANCELLED.withDescription("Flight client cancelled the query")
                        .withCause(e).toRuntimeException();
            }
            throw e;
        } finally {
            listenerReadiness.disarm();
        }
    }

    /**
     * Runs a prepared query unless the client has already cancelled the call.
     *
     * @param ps prepared query
     * @param listener Flight stream listener
     * @return result set
     * @throws java.sql.SQLException on query failure
     */
    private static java.sql.ResultSet executeQuery(PreparedStatement ps,
            org.apache.arrow.flight.FlightProducer.ServerStreamListener listener)
            throws java.sql.SQLException {
        if (listener.isCancelled()) {
            throw CallStatus.CANCELLED.withDescription("Flight client cancelled the query")
                    .toRuntimeException();
        }
        return ps.executeQuery();
    }

    /**
     * Returns the Arrow schema DuckDB produces for a query by running it. The caller passes the
     * query over empty inputs, so this costs a bind and no data is read; the query is not
     * wrapped because a wrapper would rename duplicate column names.
     *
     * @param allocator Arrow buffer allocator
     * @param duckSql   DuckDB SQL query over empty inputs
     * @return schema of the batches {@link #streamSql} sends for the same SQL over real inputs
     * @throws Exception if DuckDB cannot bind the query
     */
    public org.apache.arrow.vector.types.pojo.Schema describeSql(BufferAllocator allocator,
            String duckSql) throws Exception {
        long t = LogUtil.mark();
        Connection conn = threadSession.get().conn;
        try (PreparedStatement ps = conn.prepareStatement(duckSql);
                org.duckdb.DuckDBResultSet drs = (org.duckdb.DuckDBResultSet) ps.executeQuery();
                ArrowReader reader = (ArrowReader) drs.arrowExportStream(allocator, batchSize)) {
            org.apache.arrow.vector.types.pojo.Schema schema =
                    reader.getVectorSchemaRoot().getSchema();
            LogUtil.logTiming(t, "duckdb.describeSql", "fields=" + schema.getFields().size());
            return schema;
        }
    }

    /**
     * Formats an elapsed nanosecond duration for structured logging.
     *
     * @param nanos elapsed nanoseconds
     * @return human-readable duration
     */
    private static String formatDuration(long nanos) {
        if (nanos < 1_000_000L) {
            return nanos / 1_000L + "us";
        }
        return nanos / 1_000_000L + "ms";
    }

    /**
     * Reads only the Parquet footer to get the total row count. Zero column I/O.
     *
     * @param fileSystem Hadoop FileSystem
     * @param full       absolute file path
     * @return total row count across all row groups
     * @throws IOException on read failure
     */
    public static long footerRowCount(org.apache.hadoop.fs.FileSystem fileSystem,
            org.apache.hadoop.fs.Path full) throws IOException {
        long t = LogUtil.mark();
        final long fileLen = fileSystem.getFileStatus(full).getLen();
        try (ParquetFileReader pfr = ParquetFileReader.open(new InputFile() {
            @Override
            public long getLength() {
                return fileLen;
            }

            @Override
            public SeekableInputStream newStream() throws IOException {
                return HadoopStreams.wrap(fileSystem.open(full));
            }
        })) {
            long count = 0;
            for (BlockMetaData b : pfr.getFooter().getBlocks()) {
                count += b.getRowCount();
            }
            LogUtil.logTiming(t, "duckdb.footerRowCount", "file=" + full.getName() + " count=" + count);
            return count;
        }
    }

    /**
     * Reads MIN/MAX/COUNT statistics from Parquet footer. Zero column I/O.
     *
     * @param fileSystem Hadoop FileSystem
     * @param full       absolute file path
     * @param pq         parsed query with select expressions
     * @return optional array of aggregated stats values, empty if any stats are missing
     * @throws IOException on read failure
     */
    @SuppressWarnings("java:S3776") // Footer aggregation handles every supported statistic in one pass.
    public static Optional<Object[]> footerStats(org.apache.hadoop.fs.FileSystem fileSystem,
            org.apache.hadoop.fs.Path full, ParquetQueryParser pq) throws IOException {
        long t = LogUtil.mark();
        final long fileLen = fileSystem.getFileStatus(full).getLen();
        try (ParquetFileReader pfr = ParquetFileReader.open(new InputFile() {
            @Override
            public long getLength() {
                return fileLen;
            }

            @Override
            public SeekableInputStream newStream() throws IOException {
                return HadoopStreams.wrap(fileSystem.open(full));
            }
        })) {
            List<ParquetQueryParser.SelectExpr> exprs = pq.selectExprs;
            int n = exprs.size();
            Object[] result = new Object[n];
            long totalRows = 0;

            for (BlockMetaData block : pfr.getFooter().getBlocks()) {
                totalRows += block.getRowCount();

                for (ColumnChunkMetaData cc : block.getColumns()) {
                    String colName = cc.getPath().toDotString();

                    for (int i = 0; i < n; i++) {
                        ParquetQueryParser.SelectExpr expr = exprs.get(i);
                        if (!colName.equals(expr.inputColumn)) {
                            continue;
                        }

                        Statistics<?> stats = cc.getStatistics();
                        if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.MIN) {
                            if (stats == null || stats.isEmpty() || !stats.hasNonNullValue()) {
                                return Optional.empty();
                            }
                            result[i] = minOf(result[i], stats.genericGetMin());
                        } else if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.MAX) {
                            if (stats == null || stats.isEmpty() || !stats.hasNonNullValue()) {
                                return Optional.empty();
                            }
                            result[i] = maxOf(result[i], stats.genericGetMax());
                        } else if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT) {
                            if (stats == null || stats.getNumNulls() < 0) {
                                return Optional.empty();
                            }
                            long nonNulls = block.getRowCount() - stats.getNumNulls();
                            result[i] = (result[i] == null ? 0L
                                    : ((Number) result[i]).longValue()) + nonNulls;
                        }
                    }
                }
            }

            for (int i = 0; i < n; i++) {
                if (exprs.get(i).func == ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR) {
                    result[i] = totalRows;
                }
            }
            LogUtil.logTiming(t, "duckdb.footerStats", "file=" + full.getName());
            return Optional.of(result);
        }
    }

    /**
     * Builds a DuckDB SQL SELECT query from parsed query.
     *
     * @param pq         parsed query
     * @param fromClause FROM clause (e.g. read_parquet([...]) or table reference)
     * @return DuckDB SQL string
     */
    public static String buildDuckSql(ParquetQueryParser pq, String fromClause) {
        return buildDuckSqlWithFilter(pq, fromClause, false);
    }

    /**
     * Builds a DuckDB SQL SELECT query after filter has been applied upstream.
     *
     * @param pq              parsed query
     * @param fromClause      FROM clause
     * @param filterApplied   whether filter was already applied upstream
     * @return DuckDB SQL string
     */
    @SuppressWarnings("java:S3776") // SQL rendering branches directly over the supported expression model.
    public static String buildDuckSqlWithFilter(ParquetQueryParser pq, String fromClause,
            boolean filterApplied) {
        StringBuilder sql = new StringBuilder(pq.distinct ? "SELECT DISTINCT " : "SELECT ");
        boolean first = true;

        Set<String> gbSet = new LinkedHashSet<>(pq.groupByColumnNames);
        // Keep the columns in the order the client selected them. Grouping columns the client
        // did not select are still returned, first, so that partial groups can be merged.
        boolean selectOrder = pq.selectExprs.stream()
                .filter(e -> e.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN)
                .map(e -> e.inputColumn).toList().containsAll(gbSet);
        if (!selectOrder) {
            for (String gbCol : pq.groupByColumnNames) {
                if (!first) {
                    sql.append(", ");
                }
                first = false;
                sql.append('"').append(gbCol).append('"');
            }
        }

        for (ParquetQueryParser.SelectExpr expr : pq.selectExprs) {
            if (!selectOrder && expr.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN
                    && gbSet.contains(expr.inputColumn)) {
                continue;
            }
            if (!first) {
                sql.append(", ");
            }
            first = false;
            switch (expr.func) {
                case COUNT_STAR -> sql.append("count(*)");
                case COUNT -> sql.append("count(").append(aggregateInput(expr)).append(")");
                case SUM -> sql.append(renderSum(expr));
                case MIN -> sql.append("min(").append(aggregateInput(expr)).append(")");
                case MAX -> sql.append("max(").append(aggregateInput(expr)).append(")");
                case COLUMN -> sql.append('"').append(expr.inputColumn).append('"');
                default -> { }
            }
            boolean renamed = expr.func != ParquetQueryParser.SelectExpr.AggFunc.COLUMN
                    || !expr.outputName.equals(expr.inputColumn);
            if (renamed && expr.outputName != null && !expr.outputName.isBlank()) {
                sql.append(" AS ").append(quoteIdentifier(expr.outputName));
            }
        }

        sql.append(" FROM ").append(fromClause);
        if (!filterApplied && pq.filter != null && !pq.filter.isBlank()) {
            sql.append(" WHERE ").append(pq.filter);
        }
        if (!pq.groupByColumnNames.isEmpty()) {
            sql.append(" GROUP BY ");
            for (int i = 0; i < pq.groupByColumnNames.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append('"').append(pq.groupByColumnNames.get(i)).append('"');
            }
        }
        if (pq.having != null) {
            sql.append(" HAVING ").append(pq.having);
        }
        sql.append(pq.orderAndLimitSql());
        return sql.toString();
    }

    /**
     * Builds a grouped DuckDB SQL for parallel aggregation with multiple file references.
     *
     * @param pq                parsed query
     * @param numFiles          number of files
     * @param filterApplied     whether filter was already applied
     * @return DuckDB SQL string
     */
    public static String buildGroupedDuckSql(ParquetQueryParser pq, int numFiles,
            boolean filterApplied) {
        if (numFiles == 1) {
            return buildDuckSqlWithFilter(pq, "\"t0\"", filterApplied);
        }
        StringBuilder from = new StringBuilder("(");
        for (int i = 0; i < numFiles; i++) {
            if (i > 0) {
                from.append(" UNION ALL ");
            }
            from.append("SELECT * FROM \"t").append(i).append('"');
        }
        from.append(')');
        return buildDuckSqlWithFilter(pq, from.toString(), filterApplied);
    }

    /**
     * Creates a DuckDB read_parquet FROM clause from file paths.
     *
     * @param duckDbPaths list of file paths
     * @return FROM clause string
     */
    public static String readParquetFromClause(List<String> duckDbPaths) {
        StringBuilder sb = new StringBuilder("read_parquet([");
        for (int i = 0; i < duckDbPaths.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(sqlStringLiteral(duckDbPaths.get(i)));
        }
        sb.append("])");
        return sb.toString();
    }

    /**
     * Builds a DuckDB select SQL with read_parquet FROM clause.
     *
     * @param pq    parsed query
     * @param from  FROM clause
     * @return DuckDB SQL
     */
    public static String buildSelectSql(ParquetQueryParser pq, String from) {
        StringBuilder sql = new StringBuilder(pq.distinct ? "SELECT DISTINCT " : "SELECT ");

        if (pq.selectExprs.isEmpty()) {
            sql.append('*');
        } else {
            boolean first = true;
            for (ParquetQueryParser.SelectExpr expr : pq.selectExprs) {
                if (!first) {
                    sql.append(", ");
                }
                first = false;
                appendSelectExpr(sql, expr);
            }
        }

        sql.append(" FROM ").append(from);

        if (pq.filter != null && !pq.filter.isBlank()) {
            sql.append(" WHERE ").append(pq.filter);
        }
        sql.append(pq.orderAndLimitSql());
        return sql.toString();
    }

    /**
     * Appends a DuckDB select expression to SQL string builder.
     *
     * @param sql  SQL builder
     * @param expr select expression
     */
    public static void appendSelectExpr(StringBuilder sql, ParquetQueryParser.SelectExpr expr) {
        switch (expr.func) {
            case COUNT_STAR -> sql.append("count(*)");
            case COUNT -> sql.append("count(").append(aggregateInput(expr)).append(")");
            case SUM -> sql.append(renderSum(expr));
            case MIN -> sql.append("min(").append(aggregateInput(expr)).append(")");
            case MAX -> sql.append("max(").append(aggregateInput(expr)).append(")");
            case COLUMN -> sql.append(quoteIdentifier(expr.inputColumn));
            default -> { }
        }
        if (expr.outputName != null && !expr.outputName.isBlank()) {
            sql.append(" AS ").append(quoteIdentifier(expr.outputName));
        }
    }

    /**
     * Returns parser-validated aggregate input SQL with a simple-column fallback.
     *
     * @param expr parsed aggregate expression
     * @return executable aggregate input SQL
     */
    private static String aggregateInput(ParquetQueryParser.SelectExpr expr) {
        String input = expr.inputExpression != null
                ? expr.inputExpression : quoteIdentifier(expr.inputColumn);
        return expr.distinct ? "DISTINCT " + input : input;
    }

    /**
     * Renders a SUM aggregate, casting decimal sums to the same result precision
     * Spark derives for {@code Sum(Decimal(p, s))}: {@code Decimal(min(38, p + 10), s)}.
     * DuckDB widens decimal sums to precision 38 on its own; casting keeps the
     * streamed batch type identical to the schema advertised in FlightInfo, which
     * the strict columnar reader requires, and avoids an ANSI-mode narrowing cast
     * in Spark that overflows on large sums.
     *
     * @param expr parsed SUM expression
     * @return DuckDB SUM SQL, decimal-cast when the input is decimal
     */
    private static String renderSum(ParquetQueryParser.SelectExpr expr) {
        String sum = "sum(" + aggregateInput(expr) + ")";
        if (expr.decimalScale == null) {
            return sum;
        }
        int inputPrecision = expr.decimalPrecision == null ? 38 : expr.decimalPrecision;
        int sumPrecision = Math.min(38, inputPrecision + 10);
        return "cast(" + sum + " as decimal(" + sumPrecision + "," + expr.decimalScale + "))";
    }

    /**
     * Quotes an identifier for DuckDB SQL.
     *
     * @param identifier identifier to quote
     * @return quoted identifier
     */
    public static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    /**
     * Converts a string value to SQL string literal.
     *
     * @param value string value
     * @return SQL string literal
     */
    public static String sqlStringLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    /**
     * Checks whether a Flight listener is ready to receive data.
     *
     * @param listener      Flight stream listener
     * @param timeoutMillis maximum wait time in milliseconds
     * @return true when the listener is ready
     * @throws org.apache.arrow.flight.FlightRuntimeException if cancelled or timed out
     * @throws InterruptedException if waiting is interrupted
     * @throws IllegalArgumentException if timeout is not positive
     */
    public static boolean awaitListenerReady(
            org.apache.arrow.flight.FlightProducer.ServerStreamListener listener,
            long timeoutMillis)
            throws InterruptedException {
        return new ListenerReadiness(listener, timeoutMillis).await();
    }

    /**
     * Runs a client's query over the catalog views and streams the complete answer.
     *
     * @param allocator     Arrow buffer allocator
     * @param sql           client SQL
     * @param views         current view definitions
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws Exception on refusal, query or stream failure
     */
    public void streamComplete(BufferAllocator allocator, String sql,
            pro.surpin.data.arrowflight.server.services.CatalogViews.Snapshot views,
            org.apache.arrow.flight.FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {
        streamComplete(allocator, sql, List.of(), views, listener, startListener);
    }

    /**
     * Runs a client's query with parameter values over the catalog views and streams the
     * complete answer.
     *
     * @param allocator     Arrow buffer allocator
     * @param sql           client SQL
     * @param parameters    values of the query's parameters, in order
     * @param views         current view definitions
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws Exception on refusal, query or stream failure
     */
    public void streamComplete(BufferAllocator allocator, String sql, List<Object> parameters,
            pro.surpin.data.arrowflight.server.services.CatalogViews.Snapshot views,
            org.apache.arrow.flight.FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {
        Session session = threadSession.get();
        if (session.viewsVersion != views.version()) {
            session.viewDrops = applyViews(session.conn, session.viewDrops,
                    views.dataStatements(), views.dropStatements());
            session.viewsVersion = views.version();
        }
        // Parse first: preparing a text with several statements runs all but the last one.
        requireSingleQuery(session.statement(PARSE_CHECK_SQL), sql);
        PreparedStatement ps = session.statement(sql);
        requireQuery(sql, ps);
        int expected = ps.getParameterMetaData().getParameterCount();
        if (expected != parameters.size()) {
            throw new IllegalArgumentException("The query has " + expected + " parameter(s), "
                    + parameters.size() + " value(s) were sent");
        }
        // The statement is cached per thread: values of an earlier execution must not remain.
        ps.clearParameters();
        for (int i = 0; i < parameters.size(); i++) {
            ps.setObject(i + 1, parameters.get(i));
        }
        streamSql(allocator, sql, listener, startListener);
    }

    /**
     * Returns the Arrow schema of a client's query by running it over views that have the
     * tables' columns and no rows, in a separate DuckDB of this thread that can read no files at
     * all. A query that still takes longer than 30 s (it reads no table) is interrupted.
     *
     * @param allocator Arrow buffer allocator
     * @param sql       client SQL
     * @param views     current view definitions
     * @return schema of the complete answer
     * @throws Exception on refusal or when DuckDB rejects the query
     */
    public org.apache.arrow.vector.types.pojo.Schema describeComplete(BufferAllocator allocator,
            String sql, pro.surpin.data.arrowflight.server.services.CatalogViews.Snapshot views)
            throws Exception {
        return describeComplete(allocator, sql, List.of(), views);
    }

    /**
     * Returns the Arrow schema of a client's query with the given parameter values; without
     * values, each parameter is bound as the client will send it (see {@link QueryParameters}).
     *
     * @param allocator  Arrow buffer allocator
     * @param sql        client SQL
     * @param parameters values of the query's parameters, or an empty list
     * @param views      current view definitions
     * @return schema of the complete answer
     * @throws Exception on refusal or when DuckDB rejects the query
     */
    public org.apache.arrow.vector.types.pojo.Schema describeComplete(BufferAllocator allocator,
            String sql, List<Object> parameters,
            pro.surpin.data.arrowflight.server.services.CatalogViews.Snapshot views)
            throws Exception {
        Connection conn = describeConnection(sql, views);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            requireQuery(sql, ps);
            java.sql.ParameterMetaData parameterMetaData = ps.getParameterMetaData();
            int parameterCount = parameterMetaData.getParameterCount();
            boolean withValues = parameters.size() == parameterCount;
            boolean textProbe = false;
            for (int i = 1; i <= parameterCount; i++) {
                if (withValues) {
                    ps.setObject(i, parameters.get(i - 1));
                } else if (QueryParameters.arrowType(parameterMetaData.getParameterType(i),
                        parameterMetaData.getParameterTypeName(i)) instanceof
                        org.apache.arrow.vector.types.pojo.ArrowType.Utf8) {
                    // A parameter of no known type is sent as text: describe it as text, or
                    // the answer's columns could differ from those announced (SELECT ?).
                    ps.setObject(i, "");
                    textProbe = true;
                } else {
                    ps.setObject(i, null);
                }
            }
            try {
                return describeRun(allocator, ps);
            } catch (java.sql.SQLException e) {
                if (!textProbe) {
                    throw e;
                }
                // The empty text did not fit the query (? + 1, CAST(? AS INTEGER)): describe with
                // NULL, which fits any type, as the values are not known yet. DuckDB closes a
                // statement that failed, so prepare it again.
                try (PreparedStatement retry = conn.prepareStatement(sql)) {
                    for (int i = 1; i <= parameterCount; i++) {
                        retry.setObject(i, null);
                    }
                    return describeRun(allocator, retry);
                }
            }
        }
    }

    /**
     * Runs a describe statement whose parameters are set and returns its Arrow schema; a run
     * longer than {@link #DESCRIBE_TIMEOUT_SECONDS} is interrupted.
     *
     * @param allocator Arrow buffer allocator
     * @param ps        statement over empty views
     * @return result schema
     * @throws IllegalArgumentException when the run times out
     * @throws java.sql.SQLException when DuckDB rejects the query
     */
    private org.apache.arrow.vector.types.pojo.Schema describeRun(BufferAllocator allocator,
            PreparedStatement ps) throws Exception {
        java.util.concurrent.atomic.AtomicBoolean timedOut = new java.util.concurrent.atomic.AtomicBoolean();
        java.util.concurrent.ScheduledFuture<?> timer = sessionReaper.schedule(() -> {
            timedOut.set(true);
            cancelQuietly(ps);
        }, DESCRIBE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        try (org.duckdb.DuckDBResultSet drs = (org.duckdb.DuckDBResultSet) ps.executeQuery();
                ArrowReader reader = (ArrowReader) drs.arrowExportStream(allocator, batchSize)) {
            return reader.getVectorSchemaRoot().getSchema();
        } catch (java.sql.SQLException e) {
            if (timedOut.get()) {
                throw new IllegalArgumentException("The query took longer than "
                        + DESCRIBE_TIMEOUT_SECONDS + " s to describe without reading any table", e);
            }
            throw e;
        } finally {
            timer.cancel(false);
        }
    }

    /**
     * Describes the {@code ?} parameters of a client's query, so that the client can send values.
     *
     * @param sql   client SQL
     * @param views current view definitions
     * @return one field per parameter, in order
     * @throws Exception on refusal or when DuckDB rejects the query
     */
    public org.apache.arrow.vector.types.pojo.Schema describeParameters(String sql,
            pro.surpin.data.arrowflight.server.services.CatalogViews.Snapshot views)
            throws Exception {
        Connection conn = describeConnection(sql, views);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            requireQuery(sql, ps);
            return QueryParameters.schema(ps.getParameterMetaData());
        }
    }

    /**
     * Returns this thread's DuckDB for describing, with current empty views, after checking that
     * the text is one query.
     *
     * @param sql   client SQL
     * @param views current view definitions
     * @return DuckDB connection
     * @throws Exception on refusal or when DuckDB cannot be opened
     */
    private Connection describeConnection(String sql,
            pro.surpin.data.arrowflight.server.services.CatalogViews.Snapshot views)
            throws Exception {
        Session session = threadSession.get();
        if (session.describeConn == null) {
            session.describeConn = openDescribeConnection();
        }
        Connection conn = session.describeConn;
        if (session.describeVersion != views.version()) {
            session.describeDrops = applyViews(conn, session.describeDrops,
                    views.emptyStatements(), views.dropStatements());
            session.describeVersion = views.version();
        }
        try (PreparedStatement check = conn.prepareStatement(PARSE_CHECK_SQL)) {
            requireSingleQuery(check, sql);
        }
        return conn;
    }

    /**
     * Refuses a text unless DuckDB parses it as exactly one query that reads. Nothing is run.
     *
     * @param check the parse check statement ({@link #PARSE_CHECK_SQL}) of the connection
     * @param sql client SQL
     * @throws IllegalArgumentException when the text is not one query
     * @throws java.sql.SQLException when the check itself fails
     */
    static void requireSingleQuery(PreparedStatement check, String sql) throws java.sql.SQLException {
        check.setString(1, sql);
        boolean error;
        String type;
        String message;
        int statements;
        String serialized;
        try (java.sql.ResultSet rs = check.executeQuery()) {
            rs.next();
            error = rs.getBoolean(1);
            type = rs.getString(2);
            message = rs.getString(3);
            statements = rs.getInt(4);
            serialized = rs.getString(5);
        }
        if (error && "parser".equals(type)) {
            throw new IllegalArgumentException("Parser Error: " + message);
        }
        String first = firstKeyword(sql);
        if (first.isEmpty() && !error && statements == 0) {
            throw new IllegalArgumentException("The request has no query");
        }
        if (!first.isEmpty() && !QUERY_KEYWORDS.contains(first)) {
            throw new IllegalArgumentException("Only queries are allowed, not " + first.toUpperCase(java.util.Locale.ROOT));
        }
        if (error || statements != 1) {
            throw new IllegalArgumentException("Only one query that reads data is allowed"
                    + " (SELECT, WITH, VALUES, FROM, TABLE, DESCRIBE, SHOW, SUMMARIZE), one statement per request");
        }
        requireAllowedFunctions(serialized);
    }

    /**
     * Refuses a parsed statement that names a function or system view of {@link #REFUSED_FUNCTIONS}.
     *
     * @param serialized DuckDB's JSON form of the statement
     * @throws IllegalArgumentException when such a name is used
     */
    static void requireAllowedFunctions(String serialized) {
        if (serialized == null) {
            return;
        }
        java.util.regex.Matcher m = SERIALIZED_NAME.matcher(serialized);
        while (m.find()) {
            String name = m.group(1).toLowerCase(java.util.Locale.ROOT);
            if (REFUSED_FUNCTIONS.contains(name)) {
                throw new IllegalArgumentException("Function or view " + name + " is not allowed");
            }
        }
    }

    /**
     * Interrupts a running DuckDB statement, ignoring failures.
     *
     * @param ps statement to interrupt
     */
    private static void cancelQuietly(PreparedStatement ps) {
        try {
            ps.cancel();
        } catch (Exception e) {
            LOGGER.debug("Failed to interrupt DuckDB statement", e);
        }
    }

    /**
     * Replaces the catalog views of a connection.
     *
     * @param conn       DuckDB connection
     * @param oldDrops   statements that remove the previous views
     * @param statements CREATE SCHEMA / CREATE VIEW statements
     * @param drops      statements that remove what the new statements create
     * @return the drop statements now in force
     * @throws java.sql.SQLException when a statement fails
     */
    private static List<String> applyViews(Connection conn, List<String> oldDrops,
            List<String> statements, List<String> drops) throws java.sql.SQLException {
        try (Statement s = conn.createStatement()) {
            for (String drop : oldDrops) {
                s.execute(drop);
            }
            for (String statement : statements) {
                s.execute(statement);
            }
        }
        return drops;
    }

    /**
     * Refuses anything but a read-only query: its first word must be a query keyword and DuckDB
     * must report that it returns rows.
     *
     * @param sql client SQL
     * @param prepared the prepared statement
     * @throws IllegalArgumentException when the statement is not a query
     * @throws java.sql.SQLException when DuckDB cannot describe the statement
     */
    static void requireQuery(String sql, PreparedStatement prepared) throws java.sql.SQLException {
        String first = firstKeyword(sql);
        if (!QUERY_KEYWORDS.contains(first)) {
            throw new IllegalArgumentException("Only queries are allowed, not "
                    + (first.isEmpty() ? "this statement" : first.toUpperCase(java.util.Locale.ROOT)));
        }
        java.sql.ResultSetMetaData metaData = prepared.getMetaData();
        if (metaData instanceof org.duckdb.DuckDBResultSetMetaData duck
                && duck.getReturnType() != org.duckdb.StatementReturnType.QUERY_RESULT) {
            throw new IllegalArgumentException("Only statements that return rows are allowed");
        }
    }

    /**
     * Returns the first word of a statement, skipping comments and opening parentheses.
     *
     * @param sql SQL text
     * @return first word in lower case, or an empty string
     */
    static String firstKeyword(String sql) {
        int i = 0;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (Character.isWhitespace(c) || Character.isSpaceChar(c) || c == '\uFEFF'
                    || c == '(' || c == ';') {
                i++;
            } else if (sql.startsWith("--", i)) {
                int end = sql.indexOf('\n', i);
                i = end < 0 ? sql.length() : end + 1;
            } else if (sql.startsWith("/*", i)) {
                int end = sql.indexOf("*/", i + 2);
                i = end < 0 ? sql.length() : end + 2;
            } else {
                break;
            }
        }
        int start = i;
        while (i < sql.length() && Character.isLetter(sql.charAt(i))) {
            i++;
        }
        return sql.substring(start, i).toLowerCase(java.util.Locale.ROOT);
    }

    /** One thread's DuckDB connection with its bounded prepared-statement cache. */
    private static final class Session {
        private final Thread owner;
        private final Connection conn;
        private final int maxStatements;
        private long viewsVersion = Long.MIN_VALUE;
        private List<String> viewDrops = List.of();
        /** Separate DuckDB with empty views for describing queries; opened on first use. */
        private Connection describeConn;
        private long describeVersion = Long.MIN_VALUE;
        private List<String> describeDrops = List.of();
        private final Map<String, PreparedStatement> statements;

        /**
         * Binds a connection to the thread that uses it.
         *
         * @param owner         thread the connection belongs to
         * @param conn          DuckDB connection
         * @param maxStatements prepared statements to keep
         */
        private Session(Thread owner, Connection conn, int maxStatements) {
            this.owner = owner;
            this.conn = conn;
            this.maxStatements = Math.max(1, maxStatements);
            // Upstream's bounded LRU cache; it closes the statements it evicts.
            this.statements = new StatementCache(this.maxStatements);
        }

        /**
         * Returns the cached prepared statement for a SQL text, preparing it on first use.
         *
         * @param sql DuckDB SQL
         * @return prepared statement
         * @throws java.sql.SQLException if the statement cannot be prepared
         */
        private PreparedStatement statement(String sql) throws java.sql.SQLException {
            PreparedStatement ps = statements.get(sql);
            if (ps != null && ps.isClosed()) {
                // DuckDB closes a prepared statement whose execution failed or was interrupted.
                statements.remove(sql);
                ps = null;
            }
            if (ps == null) {
                ps = conn.prepareStatement(sql);
                statements.put(sql, ps);
            }
            return ps;
        }

        /**
         * Closes the statements and the connection.
         */
        private void close() {
            statements.values().forEach(Session::closeQuietly);
            statements.clear();
            closeQuietly(conn);
            if (describeConn != null) {
                closeQuietly(describeConn);
            }
        }

        /**
         * Closes a JDBC resource, logging a failure.
         *
         * @param resource statement or connection
         */
        private static void closeQuietly(AutoCloseable resource) {
            try {
                resource.close();
            } catch (Exception e) {
                LOGGER.warn("Failed to close DuckDB resource", e);
            }
        }
    }

    /** Maintains one readiness signal and callback registration for a Flight stream. */
    private static final class ListenerReadiness {
        private final org.apache.arrow.flight.FlightProducer.ServerStreamListener listener;
        private final long timeoutMillis;
        private final Semaphore stateChanged = new Semaphore(0);
        /** Statement interrupted when the client cancels, until the stream ends; may be null. */
        private PreparedStatement running;
        /** Checks for a cancel while DuckDB computes; may be null. */
        private java.util.concurrent.ScheduledFuture<?> watch;
        /** Query id of the stream, for the log line written on another thread. */
        private final String qid = LogUtil.qid();

        /**
         * Registers callbacks used by every batch in one Flight stream.
         *
         * @param listener Flight stream listener
         * @param timeoutMillis maximum wait time in milliseconds
         */
        private ListenerReadiness(
                org.apache.arrow.flight.FlightProducer.ServerStreamListener listener,
                long timeoutMillis) {
            this(listener, timeoutMillis, null, null);
        }

        /**
         * Registers callbacks used by every batch in one Flight stream; a cancel by the client
         * also interrupts the statement producing the stream.
         *
         * @param listener Flight stream listener
         * @param timeoutMillis maximum wait time in milliseconds
         * @param running statement to interrupt on cancel, or null
         * @param scheduler runs the cancel check while DuckDB computes, or null
         */
        private ListenerReadiness(
                org.apache.arrow.flight.FlightProducer.ServerStreamListener listener,
                long timeoutMillis, PreparedStatement running, ScheduledExecutorService scheduler) {
            if (timeoutMillis <= 0) {
                throw new IllegalArgumentException(
                        "Flight listener readiness timeout must be positive: " + timeoutMillis);
            }
            this.listener = listener;
            this.timeoutMillis = timeoutMillis;
            this.running = running;
            listener.setOnReadyHandler(stateChanged::release);
            listener.setOnCancelHandler(this::onCancel);
            if (running != null && scheduler != null) {
                // gRPC runs the cancel callback on the thread of this call, which is busy in DuckDB
                // until the first batch; the cancelled flag itself is visible at once.
                watch = scheduler.scheduleWithFixedDelay(() -> {
                    if (listener.isCancelled()) {
                        onCancel();
                    }
                }, CANCEL_POLL_MILLIS, CANCEL_POLL_MILLIS, TimeUnit.MILLISECONDS);
            }
        }

        /**
         * Wakes a waiting stream and interrupts the statement, if the stream is still running.
         */
        private synchronized void onCancel() {
            stateChanged.release();
            if (running != null) {
                LOGGER.info("qid={} node={} duckdb=interrupt reason=clientCancelled",
                        qid, LogUtil.node());
                cancelQuietly(running);
                disarm();
            }
        }

        /**
         * Stops interrupting the statement: the stream is over and the statement may be reused.
         */
        private synchronized void disarm() {
            running = null;
            if (watch != null) {
                watch.cancel(false);
                watch = null;
            }
        }

        /**
         * Waits until the listener can accept the next Arrow batch.
         *
         * @return true when the listener is ready
         * @throws InterruptedException if waiting is interrupted
         */
        private boolean await() throws InterruptedException {
            long deadlineNanos = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            long waitStart = System.nanoTime();
            long t = LogUtil.mark();
            while (true) {
                if (listener.isCancelled()) {
                    long waitedNanos = System.nanoTime() - waitStart;
                    LOGGER.info("qid={} node={} backpressure=cancelled waited={}",
                            LogUtil.qid(), LogUtil.node(), LogUtil.elapsedNanos(waitStart));
                    LogUtil.logTiming(t, "duckdb.awaitListenerReadyCancelled",
                            "waitMs=" + waitedNanos / 1_000_000);
                    throw CallStatus.CANCELLED
                            .withDescription("Flight client cancelled while waiting for readiness")
                            .toRuntimeException();
                }
                if (listener.isReady()) {
                    break;
                }

                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    LOGGER.warn("qid={} node={} backpressure=timeout timeoutMs={} waited={}",
                            LogUtil.qid(), LogUtil.node(), timeoutMillis,
                            LogUtil.elapsedNanos(waitStart));
                    LogUtil.logTiming(t, "duckdb.awaitListenerReadyTimeout",
                            "timeoutMs=" + timeoutMillis);
                    throw CallStatus.TIMED_OUT
                            .withDescription("Flight listener was not ready within "
                                    + timeoutMillis + " ms")
                            .toRuntimeException();
                }

                long waitSliceNanos = Math.min(remainingNanos, LISTENER_STATE_POLL_NANOS);
                if (stateChanged.tryAcquire(waitSliceNanos, TimeUnit.NANOSECONDS)) {
                    continue;
                }
            }
            long totalWaitNanos = System.nanoTime() - waitStart;
            if (totalWaitNanos > LISTENER_TIMING_LOG_THRESHOLD_NANOS) {
                LogUtil.logTiming(t, "duckdb.awaitListenerReady",
                        "waitMs=" + totalWaitNanos / 1_000_000);
                LOGGER.debug("qid={} node={} backpressure=waited waitMs={} ready=true cancelled=false",
                        LogUtil.qid(), LogUtil.node(), totalWaitNanos / 1_000_000);
            }
            return true;
        }
    }

    /**
     * Return minimum of two comparable values
     * @param a first value
     * @param b second value
     * @return minimum value
     */
    @SuppressWarnings("unchecked")
    private static Object minOf(Object a, Object b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return ((Comparable<Object>) a).compareTo(b) <= 0 ? a : b;
    }

    /**
     * Return maximum of two comparable values
     * @param a first value
     * @param b second value
     * @return maximum value
     */
    @SuppressWarnings("unchecked")
    private static Object maxOf(Object a, Object b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return ((Comparable<Object>) a).compareTo(b) >= 0 ? a : b;
    }

    /**
     * Closes all DuckDB connections created by the thread-local pool.
     */
    @Override
    public void close() {
        sessionReaper.shutdownNow();
        for (Session session : allSessions) {
            session.close();
        }
        allSessions.clear();
        deleteRecursively(tempRoot);
    }
}
