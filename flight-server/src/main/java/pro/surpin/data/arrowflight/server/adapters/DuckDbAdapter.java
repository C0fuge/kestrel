package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.InputFile;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import pro.surpin.data.arrowflight.server.services.ParquetQueryParser;
import pro.surpin.data.arrowflight.server.services.QueryCacheLimits;
import pro.surpin.data.arrowflight.server.metrics.CacheMetrics;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.server.duckdb.DuckDbSetupPlan;
import pro.surpin.data.arrowflight.server.duckdb.LockdownCheck;
import pro.surpin.data.arrowflight.server.duckdb.PluginRefusedFunctions;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;

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
    /**
     * Prefix of the in-memory catalog each session of a shared DuckDB keeps its catalog views
     * in; client SQL may not name such a catalog or schema (duckDbSharedDatabase).
     */
    public static final String SESSION_CATALOG_PREFIX = "flight_session_";
    private final Path tempRoot;
    private final AtomicInteger sessionIds = new AtomicInteger();
    /** Directories DuckDB may read when locked down (as DuckDB sees the paths). */
    private final List<String> readableRoots;
    /** Statements the data store needs in every connection, run before the lockdown. */
    private final List<String> storageSetupStatements;
    /** Removes the secret values of the setup from errors. */
    private final SecretRedactor redactor;
    /** Secrets the setup declares, read back at startup. */
    private final List<DuckDbSetupPlan.DeclaredSecret> declaredSecrets;
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
    /** A catalog or schema qualifier of a table in DuckDB's serialized statement. */
    private static final java.util.regex.Pattern SERIALIZED_QUALIFIER = java.util.regex.Pattern.compile(
            "\"(?:catalog_name|schema_name)\":\"((?:[^\"\\\\]|\\\\.)*)\"");
    /** A string literal in a DuckDB error, possibly cut off before its closing quote. */
    private static final java.util.regex.Pattern SETUP_LITERAL =
            java.util.regex.Pattern.compile("'(?:[^']|'')*(?:'|$)");
    /** How often a running query checks whether its client has cancelled. */
    private static final long CANCEL_POLL_MILLIS = 200;
    /** Longest time a query may take to describe; it runs over views without rows. */
    private static final long DESCRIBE_TIMEOUT_SECONDS = 30;
    /** Most query texts whose description (result and parameter schema) is kept. */
    private static final int DESCRIBE_CACHE_CAPACITY = 1024;
    /** Most query texts whose parse check result is kept. */
    private static final int PARSE_CHECK_CACHE_CAPACITY = 1024;

    /**
     * What a query without values returns and takes, described over one views snapshot.
     *
     * @param views snapshot the query was described over, compared by identity
     * @param result schema of the complete answer
     * @param parameters one field per parameter
     */
    private record Description(pro.surpin.data.arrowflight.server.services.CatalogViews.Snapshot views,
            org.apache.arrow.vector.types.pojo.Schema result,
            org.apache.arrow.vector.types.pojo.Schema parameters) {
    }

    /**
     * Descriptions by query text. A client's prepare and its GetFlightInfo describe the same text
     * over the same views; one description serves both (and repeated texts) until the views change.
     */
    private final Map<String, Description> describeCache = java.util.Collections.synchronizedMap(
            new LinkedHashMap<>(64, 0.75f, true));
    /**
     * Function and table names of the texts that passed the single-query parse check, by text.
     * The check only parses, so its outcome depends on the text alone: a client's prepare and its
     * DoGet check the same text once per node. The names are checked against the refused ones on
     * every use, because plugins may add refused names later.
     */
    private final Map<String, List<String>> parseCheckCache = java.util.Collections.synchronizedMap(
            new LinkedHashMap<>(64, 0.75f, true));
    private final ThreadLocal<Session> threadSession;
    /**
     * Node-wide threads and memory limit of duckDbSharedDatabase; null when every session is a
     * DuckDB of its own.
     */
    private final DuckDbSizing.Shared shared;
    /**
     * The node's DuckDB with duckDbSharedDatabase, set up and locked down once; every session is
     * a connection to it. Replaced when DuckDB invalidates it; null when the setting is off.
     */
    private volatile Connection sharedRoot;
    /** Guards the replacement of sharedRoot. */
    private final Object sharedRootLock = new Object();
    /**
     * Second session per thread with duckDbThreadsUnderLoad threads, opened on first use; null
     * when the setting is off.
     */
    private final ThreadLocal<Session> loadSession;
    /**
     * Further session per thread with duckDbThreadsWhenIdle threads, opened on first use; null
     * when the setting is off.
     */
    private final ThreadLocal<Session> idleSession;
    /** Sessions a query slot chose for this thread's current query; unset = threadSession. */
    private final ThreadLocal<ThreadLocal<Session>> slotSessions = new ThreadLocal<>();
    /** Queries inside a slot on this node now. */
    private final AtomicInteger runningQueries = new AtomicInteger();
    /** Queries that ran on a low-thread session since start. */
    private final AtomicLong queriesUnderLoad = new AtomicLong();
    /** Queries that ran on a high-thread session since start. */
    private final AtomicLong queriesWhenIdle = new AtomicLong();
    private final int threadsUnderLoad;
    private final int threadsWhenIdle;
    private final int cores;
    /** Caps the streams inside DuckDB at once (duckDbProducingStreams); off by default. */
    private final ProducingStreamGate producingGate;
    /**
     * Lets gRPC send DuckDB's batch buffers without a copy (flightZeroCopyWrites); safe because
     * each batch DuckDB exports lives in new buffers that gRPC keeps until it has written them.
     */
    private final boolean zeroCopyWrites;
    private final Set<Session> allSessions = ConcurrentHashMap.newKeySet();
    /** hadoopfs settings the loaded extension does not know, warned about once. */
    private final Set<String> unknownHdfsSettings = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService sessionReaper;
    private final ExecutorService ioPool;

    private final int batchSize;
    private final int duckDbGroups;
    private final AppConfig appConfig;

    /**
     * Prepares a statement for a SQL text.
     */
    @FunctionalInterface
    interface StatementPreparer {
        /**
         * Prepares a statement.
         *
         * @param sql DuckDB SQL
         * @return prepared statement
         * @throws SQLException if DuckDB cannot prepare it
         */
        PreparedStatement prepare(String sql) throws SQLException;
    }

    /**
     * Keeps prepared statements bounded to one worker thread and closes evicted entries. A SQL
     * text longer than {@link QueryCacheLimits#MAX_CACHED_QUERY_CHARS} is not cached: it is
     * held in a single slot of its own until the next long text replaces it.
     */
    static final class StatementCache extends LinkedHashMap<String, PreparedStatement> {
        private final int limit;
        private String oversizeSql;
        private PreparedStatement oversizeStatement;

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
            CacheMetrics.eviction(CacheMetrics.Cache.STATEMENT);
            closeEvicted(eldest.getValue());
            return true;
        }

        /**
         * Returns the open prepared statement for a SQL text, preparing it when there is none.
         *
         * @param sql     DuckDB SQL
         * @param preparer prepares a statement that is not cached
         * @return prepared statement, owned by this cache
         * @throws SQLException if the statement cannot be prepared
         */
        PreparedStatement statement(String sql, StatementPreparer preparer) throws SQLException {
            if (!QueryCacheLimits.cacheable(sql)) {
                if (sql.equals(oversizeSql) && !oversizeStatement.isClosed()) {
                    return oversizeStatement;
                }
                PreparedStatement ps = preparer.prepare(sql);
                CacheMetrics.oversize(CacheMetrics.Cache.STATEMENT);
                closeOversize();
                oversizeSql = sql;
                oversizeStatement = ps;
                return ps;
            }
            PreparedStatement ps = get(sql);
            if (ps != null && ps.isClosed()) {
                // DuckDB closes a prepared statement whose execution failed or was interrupted.
                remove(sql);
                ps = null;
            }
            if (ps == null) {
                ps = preparer.prepare(sql);
                put(sql, ps);
            }
            return ps;
        }

        /**
         * Closes and forgets every statement, the long-text slot included.
         */
        void closeAll() {
            values().forEach(StatementCache::closeEvicted);
            clear();
            closeOversize();
        }

        /**
         * Closes and forgets the statement of the long-text slot.
         */
        private void closeOversize() {
            if (oversizeStatement != null) {
                closeEvicted(oversizeStatement);
            }
            oversizeSql = null;
            oversizeStatement = null;
        }

        /**
         * Closes a statement the cache no longer holds, logging a failure.
         *
         * @param ps prepared statement
         */
        private static void closeEvicted(PreparedStatement ps) {
            try {
                ps.close();
            } catch (SQLException e) {
                LOGGER.warn("Failed to close evicted DuckDB statement", e);
            }
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
        this(appConfig, ioPool, readableRoots, List.of());
    }

    /**
     * Creates DuckDB adapter whose connections first run the data store's setup statements and
     * may then read only the given directories.
     *
     * @param appConfig              server configuration
     * @param ioPool                 shared I/O thread pool for parallel execution
     * @param readableRoots          data directories as DuckDB sees them; empty disables the
     *                               lockdown
     * @param storageSetupStatements statements every connection runs so that it can read the
     *                               data store, before it is locked down
     */
    public DuckDbAdapter(AppConfig appConfig, ExecutorService ioPool, List<String> readableRoots,
            List<String> storageSetupStatements) {
        this(appConfig, ioPool, DuckDbSetupPlan.of(readableRoots, storageSetupStatements));
    }

    /**
     * Creates DuckDB adapter whose connections first run the checked setup of the stores and
     * catalogs and may then read only their roots.
     *
     * @param appConfig server configuration
     * @param ioPool    shared I/O thread pool for parallel execution
     * @param setup     setup written by the DuckDbSetupValidator; empty roots disable the lockdown
     */
    public DuckDbAdapter(AppConfig appConfig, ExecutorService ioPool, DuckDbSetupPlan setup) {
        this(appConfig, ioPool, setup, 0, Runtime.getRuntime().availableProcessors());
    }

    /**
     * Creates DuckDB adapter whose queries move to a second DuckDB connection per thread with
     * fewer threads while more queries run than the cores can give duckDbThreads each.
     *
     * @param appConfig        server configuration
     * @param ioPool           shared I/O thread pool for parallel execution
     * @param setup            setup written by the DuckDbSetupValidator; empty roots disable the
     *                         lockdown
     * @param threadsUnderLoad DuckDB threads of the second connection; 0, or not below
     *                         duckDbThreads, keeps every query on its thread's one connection
     * @param cores            processors the node's queries share
     */
    public DuckDbAdapter(AppConfig appConfig, ExecutorService ioPool, DuckDbSetupPlan setup,
            int threadsUnderLoad, int cores) {
        this(appConfig, ioPool, setup, threadsUnderLoad, cores, ProducingStreamGate.OFF);
    }

    /**
     * Creates DuckDB adapter with duckDbThreadsUnderLoad and a cap on the streams that produce
     * batches at once.
     *
     * @param appConfig        server configuration
     * @param ioPool           shared I/O thread pool for parallel execution
     * @param setup            setup written by the DuckDbSetupValidator; empty roots disable the
     *                         lockdown
     * @param threadsUnderLoad DuckDB threads of the second connection; 0, or not below
     *                         duckDbThreads, keeps every query on its thread's one connection
     * @param cores            processors the node's queries share
     * @param producingGate    gate of the streams inside DuckDB at once
     */
    public DuckDbAdapter(AppConfig appConfig, ExecutorService ioPool, DuckDbSetupPlan setup,
            int threadsUnderLoad, int cores, ProducingStreamGate producingGate) {
        this(appConfig, ioPool, setup, threadsUnderLoad, cores, producingGate, false);
    }

    /**
     * Creates DuckDB adapter with duckDbThreadsUnderLoad, a cap on the streams that produce
     * batches at once and the choice of zero-copy Flight writes.
     *
     * @param appConfig        server configuration
     * @param ioPool           shared I/O thread pool for parallel execution
     * @param setup            setup written by the DuckDbSetupValidator; empty roots disable the
     *                         lockdown
     * @param threadsUnderLoad DuckDB threads of the second connection; 0, or not below
     *                         duckDbThreads, keeps every query on its thread's one connection
     * @param cores            processors the node's queries share
     * @param producingGate    gate of the streams inside DuckDB at once
     * @param zeroCopyWrites   whether result streams hand their buffers to gRPC without a copy
     */
    public DuckDbAdapter(AppConfig appConfig, ExecutorService ioPool, DuckDbSetupPlan setup,
            int threadsUnderLoad, int cores, ProducingStreamGate producingGate,
            boolean zeroCopyWrites) {
        this(appConfig, ioPool, setup, threadsUnderLoad, 0, cores, producingGate, zeroCopyWrites);
    }

    /**
     * Creates DuckDB adapter with duckDbThreadsUnderLoad, duckDbThreadsWhenIdle, a cap on the
     * streams that produce batches at once and the choice of zero-copy Flight writes.
     *
     * @param appConfig        server configuration
     * @param ioPool           shared I/O thread pool for parallel execution
     * @param setup            setup written by the DuckDbSetupValidator; empty roots disable the
     *                         lockdown
     * @param threadsUnderLoad DuckDB threads of the second connection; 0, or not below
     *                         duckDbThreads, keeps every query on its thread's one connection
     * @param threadsWhenIdle  DuckDB threads of the connection a query uses when it starts alone
     *                         on the node, at most the cores; 0, or not above duckDbThreads, is off
     * @param cores            processors the node's queries share
     * @param producingGate    gate of the streams inside DuckDB at once
     * @param zeroCopyWrites   whether result streams hand their buffers to gRPC without a copy
     */
    public DuckDbAdapter(AppConfig appConfig, ExecutorService ioPool, DuckDbSetupPlan setup,
            int threadsUnderLoad, int threadsWhenIdle, int cores, ProducingStreamGate producingGate,
            boolean zeroCopyWrites) {
        this(appConfig, ioPool, setup, threadsUnderLoad, threadsWhenIdle, cores, producingGate,
                zeroCopyWrites, null);
    }

    /**
     * Creates DuckDB adapter that may run every session on one DuckDB of the node
     * (duckDbSharedDatabase): the storage setup and the lockdown then run once, threads and
     * memory limit apply to the node, and each session keeps its catalog views in an in-memory
     * catalog of its own. duckDbThreadsUnderLoad and duckDbThreadsWhenIdle are off then.
     *
     * @param appConfig        server configuration
     * @param ioPool           shared I/O thread pool for parallel execution
     * @param setup            setup written by the DuckDbSetupValidator; empty roots disable the
     *                         lockdown
     * @param threadsUnderLoad DuckDB threads of the second connection; 0, or not below
     *                         duckDbThreads, keeps every query on its thread's one connection
     * @param threadsWhenIdle  DuckDB threads of the connection a query uses when it starts alone
     *                         on the node, at most the cores; 0, or not above duckDbThreads, is off
     * @param cores            processors the node's queries share
     * @param producingGate    gate of the streams inside DuckDB at once
     * @param zeroCopyWrites   whether result streams hand their buffers to gRPC without a copy
     * @param shared           node-wide sizing of the shared DuckDB, or null for a DuckDB per
     *                         session
     */
    @SuppressWarnings({"java:S107", "checkstyle:ParameterNumber"})
    public DuckDbAdapter(AppConfig appConfig, ExecutorService ioPool, DuckDbSetupPlan setup,
            int threadsUnderLoad, int threadsWhenIdle, int cores, ProducingStreamGate producingGate,
            boolean zeroCopyWrites, DuckDbSizing.Shared shared) {
        this.shared = shared;
        this.producingGate = producingGate;
        this.zeroCopyWrites = zeroCopyWrites;
        this.readableRoots = setup.roots();
        this.storageSetupStatements = setup.statements();
        this.redactor = setup.redactor();
        this.declaredSecrets = setup.secrets();
        PluginRefusedFunctions.addAll(setup.extraRefusedFunctions());
        this.appConfig = appConfig;
        this.ioPool = ioPool;
        this.batchSize = appConfig.batchSize();
        this.duckDbGroups = appConfig.duckDbGroups();

        this.tempRoot = createTempRoot(appConfig.duckDbTempDirectory());
        if (shared != null) {
            // Set up, checked and locked down once; a failure stops the server here.
            this.sharedRoot = openSharedRoot(true);
            if (threadsUnderLoad > 0 || threadsWhenIdle > 0) {
                LOGGER.warn("node={} duckdb=sharedDatabase ignored=duckDbThreadsUnderLoad,"
                        + "duckDbThreadsWhenIdle reason=\"threads are node-wide\"", LogUtil.node());
            }
            if (appConfig.duckDbStreamingBufferSize() != null) {
                LOGGER.warn("node={} duckdb=sharedDatabase ignored=duckDbStreamingBufferSize"
                        + " reason=\"a per-connection setting, locked before the sessions connect\"",
                        LogUtil.node());
            }
        } else if (!this.storageSetupStatements.isEmpty() || appConfig.duckDbHdfsExtension() != null) {
            // An extension file of another DuckDB version (hadoopfs v1.4.5 next to DuckDB 1.5.6)
            // stops the server here instead of failing every query.
            verifyStorageSetup();
        }
        this.threadSession = ThreadLocal.withInitial(() -> openSession(appConfig.duckDbThreads()));
        this.threadsUnderLoad = shared == null && threadsUnderLoad > 0
                && threadsUnderLoad < appConfig.duckDbThreads() ? threadsUnderLoad : 0;
        this.cores = Math.max(1, cores);
        this.loadSession = this.threadsUnderLoad > 0
                ? ThreadLocal.withInitial(() -> openSession(this.threadsUnderLoad)) : null;
        this.threadsWhenIdle = shared == null
                ? threadsWhenIdle(threadsWhenIdle, appConfig.duckDbThreads(), this.cores) : 0;
        this.idleSession = this.threadsWhenIdle > 0
                ? ThreadLocal.withInitial(() -> openSession(this.threadsWhenIdle)) : null;
        // Flight runs streams on a cached thread pool: its threads end after a minute of
        // idleness and new ones are created later, so sessions of ended threads must be closed.
        this.sessionReaper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "duckdb-session-reaper");
            t.setDaemon(true);
            return t;
        });
        this.sessionReaper.scheduleWithFixedDelay(this::closeSessionsOfEndedThreads,
                SESSION_REAP_PERIOD_SECONDS, SESSION_REAP_PERIOD_SECONDS, TimeUnit.SECONDS);

        LOGGER.info("node={} duckdb=threads perConnection={} underLoad={} whenIdle={}"
                + " producingStreams={} zeroCopyWrites={} availableProcessors={} sharedDatabase={}",
                LogUtil.node(),
                shared == null ? appConfig.duckDbThreads() : "shared", this.threadsUnderLoad,
                this.threadsWhenIdle,
                producingGate.permits(),
                zeroCopyWrites, this.cores, shared == null ? "off" : shared.describe());
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
     * Chooses the threads of the connection for queries that start alone on the node.
     *
     * @param requested duckDbThreadsWhenIdle, 0 = off
     * @param threads   duckDbThreads of every other connection
     * @param cores     processors the node's queries share
     * @return the requested threads at most the cores, or 0 when that is not above threads
     */
    static int threadsWhenIdle(int requested, int threads, int cores) {
        int capped = Math.min(requested, Math.max(1, cores));
        return capped > threads ? capped : 0;
    }

    /**
     * Opens a session of the current thread on a new DuckDB connection.
     *
     * @param threads DuckDB threads of the connection
     * @return session, registered for closing when the thread ends
     */
    private Session openSession(int threads) {
        try {
            Connection root = sharedRoot;
            Session session;
            if (root == null) {
                session = new Session(Thread.currentThread(), openConnection(false, threads),
                        appConfig.duckDbStatementCacheSize(), null, null);
            } else {
                String catalog = SESSION_CATALOG_PREFIX + sessionIds.incrementAndGet();
                session = new Session(Thread.currentThread(), openSharedConnection(root, catalog),
                        appConfig.duckDbStatementCacheSize(), root, catalog);
            }
            allSessions.add(session);
            return session;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create thread-local DuckDB connection", e);
        }
    }

    /**
     * Connects to the node's shared DuckDB with an in-memory catalog of the session's own as the
     * default, so that its catalog views are not those of other sessions.
     *
     * @param root    connection of the shared DuckDB
     * @param catalog name of the session's catalog
     * @return connection whose default catalog is the session's
     * @throws java.sql.SQLException when DuckDB cannot connect or attach
     */
    private static Connection openSharedConnection(Connection root, String catalog)
            throws java.sql.SQLException {
        Connection conn = root.unwrap(org.duckdb.DuckDBConnection.class).duplicate();
        try (Statement s = conn.createStatement()) {
            s.execute("ATTACH ':memory:' AS " + quoteIdentifier(catalog));
            s.execute("USE " + quoteIdentifier(catalog));
        } catch (java.sql.SQLException e) {
            Session.closeQuietly(conn);
            throw e;
        }
        return conn;
    }

    /**
     * Opens, sets up and locks down the node's shared DuckDB (duckDbSharedDatabase) with the
     * node-wide threads and memory limit; spill files go to a directory of its own.
     *
     * @param logExtensions whether to log the storage extensions loaded
     * @return connection that owns the shared DuckDB
     * @throws IllegalStateException when DuckDB cannot be set up or the lockdown check fails
     */
    private Connection openSharedRoot(boolean logExtensions) {
        try {
            Connection conn = openConnection(logExtensions, shared.threads(), shared.memoryLimit(),
                    tempRoot.resolve("shared" + sessionIds.incrementAndGet()));
            try {
                if (appConfig.duckDbLockdown() && !readableRoots.isEmpty()) {
                    LockdownCheck.verify(conn, readableRoots, tempRoot, declaredSecrets);
                }
            } catch (Exception e) {
                Session.closeQuietly(conn);
                throw e;
            }
            LOGGER.info("node={} duckdb=sharedDatabaseOpened {} statements={}", LogUtil.node(),
                    shared.describe(), storageSetupStatements.size());
            return conn;
        } catch (Exception e) {
            throw new IllegalStateException("DuckDB cannot be set up for the data store: "
                    + sanitizeSetupError(e.getMessage()), e instanceof java.sql.SQLException ? null : e);
        }
    }

    /**
     * Opens a new shared DuckDB in place of one DuckDB invalidated; sessions still on the old one
     * are closed when their threads next ask for a connection. A failure to open is logged and
     * the old one kept, so later queries fail with DuckDB's error.
     *
     * @param invalidated connection of the shared DuckDB that was invalidated
     */
    void replaceSharedRoot(Connection invalidated) {
        synchronized (sharedRootLock) {
            if (sharedRoot != invalidated) {
                return;
            }
            try {
                sharedRoot = openSharedRoot(false);
            } catch (RuntimeException e) {
                LOGGER.error("node={} duckdb=sharedDatabaseReopenFailed error={}", LogUtil.node(),
                        firstLine(String.valueOf(e.getMessage())));
                return;
            }
            Session.closeQuietly(invalidated);
            LOGGER.warn("node={} duckdb=sharedDatabaseReplaced reason=invalidated", LogUtil.node());
        }
    }

    /**
     * Opens and configures a DuckDB connection for queries.
     *
     * @param logExtensions whether to log the storage extensions loaded
     * @param threads       DuckDB threads of the connection
     * @return configured connection
     * @throws Exception when DuckDB cannot be opened or configured
     */
    private Connection openConnection(boolean logExtensions, int threads) throws Exception {
        return openConnection(logExtensions, threads, appConfig.duckDbMemoryLimit(),
                tempRoot.resolve("c" + sessionIds.incrementAndGet()));
    }

    /**
     * Opens and configures a DuckDB with the given threads, memory limit and spill directory.
     *
     * @param logExtensions whether to log the storage extensions loaded
     * @param threads       DuckDB threads
     * @param memoryLimit   DuckDB memory_limit, null or blank = DuckDB's default
     * @param tempDirectory spill directory of this DuckDB
     * @return configured connection
     * @throws Exception when DuckDB cannot be opened or configured
     */
    private Connection openConnection(boolean logExtensions, int threads, String memoryLimit,
            Path tempDirectory) throws Exception {
        Properties props = new Properties();
        if (appConfig.duckDbAllowUnsignedExtensions()) {
            props.setProperty("allow_unsigned_extensions", "true");
        }
        if (appConfig.duckDbStreamResults()) {
            // Without it DuckDB buffers the whole result before the first batch is sent.
            props.setProperty("jdbc_stream_results", "true");
        }
        // Secrets live in memory only: none is read from or written to ~/.duckdb/stored_secrets.
        props.setProperty("allow_persistent_secrets", "false");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:", props);
        try {
            configureConnection(conn, logExtensions, threads, memoryLimit, tempDirectory);
        } catch (Exception e) {
            Session.closeQuietly(conn);
            throw e;
        }
        return conn;
    }

    /**
     * Sets up one connection the way every query connection is set up, so that storage DuckDB
     * cannot read (a missing or mismatched extension, a refused setting) stops the server at
     * startup instead of failing every query; logs the extensions that are loaded.
     *
     * @throws IllegalStateException when the storage setup fails
     */
    private void verifyStorageSetup() {
        try (Connection conn = openConnection(true, appConfig.duckDbThreads())) {
            if (appConfig.duckDbLockdown() && !readableRoots.isEmpty()) {
                LockdownCheck.verify(conn, readableRoots, tempRoot, declaredSecrets);
            }
            LOGGER.info("node={} duckdb=storageSetupVerified statements={} open={}", LogUtil.node(),
                    storageSetupStatements.size(), !conn.isClosed());
        } catch (Exception e) {
            throw new IllegalStateException("DuckDB cannot be set up for the data store: "
                    + sanitizeSetupError(e.getMessage()), e instanceof java.sql.SQLException ? null : e);
        }
    }

    /**
     * Configure DuckDB connection settings
     * @param conn JDBC connection
     * @param logExtensions whether to log the storage extensions loaded, read before the
     *                      lockdown because the extension directory is not readable after it
     * @param threads       DuckDB threads of the connection
     * @param memLimit      DuckDB memory_limit, null or blank = DuckDB's default
     * @param tempDirectory spill directory of this DuckDB
     * @throws Exception on configuration failure
     */
    private void configureConnection(Connection conn, boolean logExtensions, int threads,
            String memLimit, Path tempDirectory) throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("SET threads = " + threads);
            if (memLimit != null && !memLimit.isBlank()) {
                s.execute("SET memory_limit = " + sqlStringLiteral(memLimit));
                LOGGER.debug("node={} duckdb=memoryLimit limit={}", LogUtil.node(), memLimit);
            }
            // Every connection is a DuckDB instance of its own. They all spill to ".tmp" by default,
            // into files with the same names, and an instance removes that directory when closed.
            s.execute("SET temp_directory = " + sqlStringLiteral(tempDirectory.toString()));
            String maxTempSize = appConfig.duckDbMaxTempSize();
            if (maxTempSize != null) {
                // The cap holds per connection (each is an instance of its own), so the node may
                // spill up to this size times the live connections.
                s.execute("SET max_temp_directory_size = " + sqlStringLiteral(maxTempSize));
            }
            if (!appConfig.duckDbPreserveInsertionOrder()) {
                // Rows of a query without ORDER BY may come in any order: parallel scan threads
                // then do not wait for each other to keep the file order.
                s.execute("SET preserve_insertion_order = false");
            }
            String streamingBuffer = appConfig.duckDbStreamingBufferSize();
            if (streamingBuffer != null && shared == null) {
                // How far a streamed result may run ahead of the Flight consumer.
                s.execute("SET streaming_buffer_size = " + sqlStringLiteral(streamingBuffer));
            }
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
                setHdfsTimeout(s, "hdfs_connect_timeout_ms",
                        appConfig.duckDbHdfsConnectTimeoutMs(), logExtensions);
                setHdfsTimeout(s, "hdfs_read_timeout_ms",
                        appConfig.duckDbHdfsReadTimeoutMs(), logExtensions);
            }
            runStorageSetup(s);
            if (logExtensions) {
                logStorageExtensions(s);
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
     * Sets a DataNode timeout of the hadoopfs extension. The setting is read when the extension
     * first connects to a namenode, so it applies to the whole connection. An extension built
     * without these settings (upstream's) keeps its own timeouts: that is logged once, not an
     * error.
     *
     * @param s statement of the connection to set up
     * @param name setting name
     * @param valueMs timeout in ms; 0 leaves the setting to the extension (LIBHDFS3_CONF)
     * @param log whether to log the value (first connection)
     * @throws java.sql.SQLException when the setting exists and cannot be set
     */
    void setHdfsTimeout(Statement s, String name, int valueMs, boolean log)
            throws java.sql.SQLException {
        if (valueMs <= 0) {
            if (log) {
                LOGGER.info("node={} duckdb=hdfsTimeout setting={} value=extensionDefault",
                        LogUtil.node(), name);
            }
            return;
        }
        try {
            s.execute("SET " + name + " = " + valueMs);
        } catch (java.sql.SQLException e) {
            String message = e.getMessage();
            if (message == null || !message.contains("unrecognized configuration parameter")) {
                throw e;
            }
            if (unknownHdfsSettings.add(name)) {
                LOGGER.warn("node={} duckdb=hdfsTimeoutUnsupported setting={} value={} reason="
                        + "\"the hadoopfs extension has no such setting (built without the"
                        + " timeout patch); its own or LIBHDFS3_CONF timeouts apply\"",
                        LogUtil.node(), name, valueMs);
            }
            return;
        }
        if (log) {
            LOGGER.info("node={} duckdb=hdfsTimeout setting={} valueMs={}", LogUtil.node(), name,
                    valueMs);
        }
    }

    /**
     * Runs the data store's setup statements. A failure names the statement by its number only:
     * such a statement may hold storage credentials, and DuckDB quotes the statement in errors.
     *
     * @param s statement of the connection to set up
     * @throws java.sql.SQLException when a setup statement fails
     */
    private void runStorageSetup(Statement s) throws java.sql.SQLException {
        for (int i = 0; i < storageSetupStatements.size(); i++) {
            try {
                s.execute(storageSetupStatements.get(i));
            } catch (java.sql.SQLException e) {
                throw new java.sql.SQLException("Storage setup statement " + (i + 1) + " of "
                        + storageSetupStatements.size() + " failed: "
                        + sanitizeSetupError(e.getMessage()), e.getSQLState());
            }
        }
    }

    /**
     * Logs the storage extensions loaded and the DuckDB version they run in.
     *
     * @param s statement of a connection after the storage setup
     * @throws java.sql.SQLException when the extension list cannot be read
     */
    private static void logStorageExtensions(Statement s) throws java.sql.SQLException {
        try (java.sql.ResultSet rs = s.executeQuery("SELECT extension_name, extension_version,"
                + " (SELECT library_version FROM pragma_version()) FROM duckdb_extensions()"
                + " WHERE loaded AND extension_name IN ('httpfs', 'hadoopfs')")) {
            while (rs.next()) {
                LOGGER.info("node={} duckdb={}Loaded version={} duckdbVersion={}", LogUtil.node(),
                        rs.getString(1), rs.getString(2), rs.getString(3));
            }
        }
    }

    /**
     * Removes what could quote a storage credential from a DuckDB error about a setup
     * statement: every string literal (DuckDB quotes the statement near a parser error, and a
     * literal cut short by its line limit has no closing quote) and the configured S3 secrets.
     *
     * @param message DuckDB error message, or null
     * @return message without literals and secrets
     */
    String sanitizeSetupError(String message) {
        if (message == null) {
            return "";
        }
        String result = redactor.redact(message);
        result = SETUP_LITERAL.matcher(result).replaceAll("'***'");
        return result.length() > 500 ? result.substring(0, 500) + "..." : result;
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
     * Returns the connection that owns the node's shared DuckDB, for tests.
     *
     * @return connection of the shared DuckDB, or null when every session is a DuckDB of its own
     */
    Connection sharedDatabase() {
        return sharedRoot;
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
     * Closes and forgets this thread's session when DuckDB reported an error after which its
     * database is unusable: after a FATAL or INTERNAL error DuckDB invalidates the database, and
     * every later statement of the session would fail with "database has been invalidated" until
     * the server restarts. The thread's next query opens a fresh DuckDB.
     *
     * @param session this thread's session
     * @param error   error of a statement run in that session
     */
    private void dropSessionIfInvalidated(Session session, Throwable error) {
        if (!invalidatesDatabase(error)) {
            return;
        }
        if (Thread.currentThread() == session.owner) {
            ThreadLocal<Session> chosen = slotSessions.get();
            if (chosen != null && chosen.get() == session) {
                slotSessions.remove();
                chosen.remove();
            } else {
                threadSession.remove();
            }
        }
        if (allSessions.remove(session)) {
            session.close();
            LOGGER.warn("node={} duckdb=sessionDropped reason=invalidated thread={} open={} error={}",
                    LogUtil.node(), session.owner.getName(), allSessions.size(),
                    firstLine(String.valueOf(error.getMessage())));
        }
        if (session.root != null) {
            // The whole shared DuckDB is invalidated, not only this session's connection.
            replaceSharedRoot(session.root);
        }
    }

    /**
     * Tells whether an error, or one of its causes, leaves DuckDB's database invalidated.
     *
     * @param error error of a statement
     * @return true for FATAL and INTERNAL errors and for "database has been invalidated"
     */
    static boolean invalidatesDatabase(Throwable error) {
        for (Throwable e = error; e != null; e = e.getCause() == e ? null : e.getCause()) {
            String message = e.getMessage();
            if (message != null && (message.contains("FATAL Error")
                    || message.contains("INTERNAL Error")
                    || message.contains("database has been invalidated"))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the first line of a message.
     *
     * @param message message
     * @return text before the first line break
     */
    private static String firstLine(String message) {
        int end = message.indexOf('\n');
        return end < 0 ? message : message.substring(0, end);
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
        return session().conn;
    }

    /**
     * Returns the session this thread's current query runs on.
     *
     * @return the session a query slot chose, else the thread's own
     */
    private Session session() {
        ThreadLocal<Session> chosen = slotSessions.get();
        ThreadLocal<Session> local = chosen != null ? chosen : threadSession;
        Session session = local.get();
        if (session.root != sharedRoot) {
            // The shared DuckDB was replaced after it was invalidated.
            local.remove();
            if (allSessions.remove(session)) {
                session.close();
            }
            session = local.get();
        }
        return session;
    }

    /**
     * Counts a query as running on this node until the returned slot is closed. With
     * duckDbThreadsUnderLoad set, the query runs on this thread's low-thread session when other
     * queries run and the running queries times duckDbThreads exceed the cores: parallel queries
     * then share the cores instead of each starting duckDbThreads threads. With
     * duckDbThreadsWhenIdle set, a query that starts while no other query runs takes this
     * thread's high-thread session and keeps it to its end, even when other queries start
     * meanwhile. Close the slot once, on the same thread.
     *
     * @return slot to close when the query ends
     */
    public QuerySlot enterQuery() {
        if (loadSession == null && idleSession == null) {
            return NO_SLOT;
        }
        int running = runningQueries.incrementAndGet();
        try {
            // A query alone keeps its threads, also when duckDbThreads exceed the cores.
            if (loadSession != null && running > 1 && (long) running * appConfig.duckDbThreads() > cores) {
                loadSession.get();
                slotSessions.set(loadSession);
                queriesUnderLoad.incrementAndGet();
            } else if (idleSession != null && running == 1) {
                idleSession.get();
                slotSessions.set(idleSession);
                queriesWhenIdle.incrementAndGet();
            }
        } catch (RuntimeException e) {
            runningQueries.decrementAndGet();
            throw e;
        }
        return () -> {
            slotSessions.remove();
            runningQueries.decrementAndGet();
        };
    }

    /**
     * Returns the number of queries that ran on a low-thread session since start.
     *
     * @return query count
     */
    public long queriesUnderLoad() {
        return queriesUnderLoad.get();
    }

    /**
     * Returns the number of queries that ran on a high-thread session since start.
     *
     * @return query count
     */
    public long queriesWhenIdle() {
        return queriesWhenIdle.get();
    }

    /**
     * Returns the gate that caps the streams producing batches at once.
     *
     * @return gate, {@link ProducingStreamGate#OFF} without duckDbProducingStreams
     */
    public ProducingStreamGate producingGate() {
        return producingGate;
    }

    /** A query's place in the node's running-query count; see {@link #enterQuery()}. */
    @FunctionalInterface
    public interface QuerySlot extends AutoCloseable {
        /** Ends the query's slot. */
        @Override
        void close();
    }

    /** Slot of a node without duckDbThreadsUnderLoad and duckDbThreadsWhenIdle: nothing is counted. */
    private static final QuerySlot NO_SLOT = () -> { };

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
        Session session = session();
        try {
            try {
                streamSql(session, allocator, duckSql, listener, startListener);
            } catch (ChangedFileBeforeStart e) {
                logChangedFileRetry(e);
                streamSql(session, allocator, duckSql, listener, startListener);
            }
        } catch (ChangedFileBeforeStart e) {
            throw e.error;
        } catch (Exception e) {
            dropSessionIfInvalidated(session, e);
            throw e;
        }
    }

    /**
     * Streams the results of a query that can run only once, because it reads Arrow streams
     * registered on this thread's connection: the statement is prepared for this run alone
     * (never cached), its parse check is not remembered, and a failure is not retried.
     *
     * @param allocator    Arrow buffer allocator
     * @param duckSql      DuckDB SQL query
     * @param listener     Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws Exception on query or stream failure
     */
    public void streamSqlOnce(BufferAllocator allocator, String duckSql,
            org.apache.arrow.flight.FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {
        Session session = session();
        try {
            requireSingleQuery(session.statement(PARSE_CHECK_SQL), duckSql);
            try (PreparedStatement ps = session.conn.prepareStatement(duckSql)) {
                streamPrepared(allocator, duckSql, ps, listener, startListener);
            }
        } catch (ChangedFileBeforeStart e) {
            throw e.error;
        } catch (Exception e) {
            dropSessionIfInvalidated(session, e);
            throw e;
        }
    }

    /**
     * Streams DuckDB SQL query results of one thread's session to a Flight listener.
     *
     * @param session      this thread's session
     * @param allocator    Arrow buffer allocator
     * @param duckSql      DuckDB SQL query
     * @param listener     Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws Exception on query or stream failure
     */
    private void streamSql(Session session, BufferAllocator allocator, String duckSql,
            org.apache.arrow.flight.FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {
        // Filters of the other mode reach DuckDB as the client wrote them.
        checkSingleQuery(session, duckSql);
        streamPrepared(allocator, duckSql, session.statement(duckSql), listener, startListener);
    }

    /**
     * Streams the results of a statement that is already prepared (and bound) to a Flight
     * listener. The caller must not take another statement from the session's cache before this
     * returns: a small cache would close the statement.
     *
     * @param allocator    Arrow buffer allocator
     * @param duckSql      DuckDB SQL of the statement, for logs
     * @param ps           prepared statement with its parameter values set
     * @param listener     Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws Exception on query or stream failure
     */
    private void streamPrepared(BufferAllocator allocator, String duckSql, PreparedStatement ps,
            org.apache.arrow.flight.FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {
        String qid = LogUtil.qid();
        long t = LogUtil.mark();
        long streamStartNanos = System.nanoTime();
        // Arguments of the debug lines below are formatted only when they are written.
        boolean debug = LOGGER.isDebugEnabled();
        if (debug) {
            LOGGER.debug("qid={} node={} thread={} duckdb=start batchSize={} sql='{}'",
                    qid, LogUtil.node(), Thread.currentThread().getName(), batchSize, duckSql);
        }
        long firstBatchNanos = -1;
        long backpressureNanos = 0;
        // A client that goes away interrupts DuckDB at once; otherwise the query would run to its
        // first batch (all of it, for an aggregation) before anybody noticed.
        ListenerReadiness listenerReadiness = new ListenerReadiness(
                listener, appConfig.flightListenerReadyTimeoutMillis(), ps, sessionReaper);
        boolean started = false;
        try (ProducingStreamGate.Pass pass = enterGate(listener);
                org.duckdb.DuckDBResultSet drs = (org.duckdb.DuckDBResultSet) executeQuery(ps, listener);
                ArrowReader reader = (ArrowReader) drs.arrowExportStream(allocator, batchSize)) {
            VectorSchemaRoot duckRoot = reader.getVectorSchemaRoot();
            started = true;
            if (zeroCopyWrites) {
                listener.setUseZeroCopy(true);
            }
            if (startListener) {
                listener.start(duckRoot);
            }

            int batchesSent = 0;
            long rowsSent = 0;
            while (loadNextBatch(reader, pass)) {
                int rows = duckRoot.getRowCount();
                if (rows == 0) {
                    continue;
                }
                if (firstBatchNanos < 0) {
                    firstBatchNanos = System.nanoTime() - streamStartNanos;
                    if (t >= 0) {
                        LogUtil.logTiming(t, "duckdb.firstBatch",
                                "sql='" + duckSql.substring(0, Math.min(100, duckSql.length())) + "'");
                    }
                }

                long bpStart = System.nanoTime();
                listenerReadiness.await();
                backpressureNanos += System.nanoTime() - bpStart;
                listener.putNext();
                batchesSent++;
                rowsSent += rows;
                if (debug && batchesSent % 10 == 0) {
                    LOGGER.debug("qid={} node={} duckdb=progress batches={} rows={} elapsed={} throughput={}rows/s",
                            qid, LogUtil.node(), batchesSent, rowsSent,
                            LogUtil.elapsedNanos(streamStartNanos),
                            rowsSent * 1_000_000_000L
                                    / Math.max(1, System.nanoTime() - streamStartNanos));
                }
            }

            if (t >= 0) {
                LogUtil.logTiming(t, "duckdb.streamSql",
                        "batches=" + batchesSent + " rows=" + rowsSent
                        + " backpressureMs=" + backpressureNanos / 1_000_000);
            }
            if (debug) {
                LOGGER.debug("qid={} node={} duckdb=completed batches={} rows={} ttfB={} backpressureMs={} elapsed={} cancelled={}",
                        qid, LogUtil.node(), batchesSent, rowsSent,
                        firstBatchNanos >= 0 ? formatDuration(firstBatchNanos) : "N/A",
                        backpressureNanos / 1_000_000,
                        LogUtil.elapsedNanos(streamStartNanos), listener.isCancelled());
            }
        } catch (java.sql.SQLException e) {
            if (listener.isCancelled()) {
                throw CallStatus.CANCELLED.withDescription("Flight client cancelled the query")
                        .withCause(e).toRuntimeException();
            }
            if (!started && remoteFileChanged(e)) {
                throw new ChangedFileBeforeStart(e);
            }
            throw e;
        } finally {
            listenerReadiness.disarm();
        }
    }

    /**
     * Opens the stream's pass of the producing-stream gate and takes its permit: DuckDB runs the
     * query (and, for a materialised result, all of it) when it is executed.
     *
     * @param listener Flight stream listener, for cancellation while waiting
     * @return pass holding a permit (none when the gate is off)
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    private ProducingStreamGate.Pass enterGate(
            org.apache.arrow.flight.FlightProducer.ServerStreamListener listener)
            throws InterruptedException {
        ProducingStreamGate.Pass pass = producingGate.pass(listener::isCancelled);
        pass.enter();
        return pass;
    }

    /**
     * Lets DuckDB compute the next batch while holding a permit of the producing-stream gate,
     * which is given back before the batch waits for the client.
     *
     * @param reader DuckDB's Arrow reader
     * @param pass   the stream's pass
     * @return whether a batch was loaded
     * @throws Exception on query failure, interruption or cancellation while waiting
     */
    private static boolean loadNextBatch(ArrowReader reader, ProducingStreamGate.Pass pass)
            throws Exception {
        pass.enter();
        try {
            return reader.loadNextBatch();
        } finally {
            pass.leave();
        }
    }

    /**
     * A query failed before anything was sent because a remote file changed since this
     * connection last read it; running it again reads the new file.
     */
    private static final class ChangedFileBeforeStart extends RuntimeException {
        private static final long serialVersionUID = 1L;
        /** DuckDB's error, thrown when the second run fails the same way. */
        private final transient java.sql.SQLException error;

        /**
         * Wraps DuckDB's error.
         *
         * @param error DuckDB's error
         */
        ChangedFileBeforeStart(java.sql.SQLException error) {
            super(error.getMessage(), error, false, false);
            this.error = error;
        }
    }

    /**
     * Tells whether DuckDB refused a remote file because its ETag differs from the one this
     * connection saw before (the object was rewritten). DuckDB keeps a file's metadata per
     * connection, so the first query after a rewrite fails and the next one reads the new file.
     *
     * @param e DuckDB's error
     * @return true for httpfs's "ETag ... was initially ... and now it returned ..." error
     */
    public static boolean remoteFileChanged(java.sql.SQLException e) {
        String message = e.getMessage();
        return message != null && message.contains("ETag on reading file")
                && message.contains("was initially") && message.contains("remote file has changed");
    }

    /**
     * Logs that a query runs again after a changed remote file was refused.
     *
     * @param e the refusal
     */
    private static void logChangedFileRetry(ChangedFileBeforeStart e) {
        LOGGER.info("qid={} node={} duckdb=changedFileRetry error={}", LogUtil.qid(), LogUtil.node(),
                firstLine(String.valueOf(e.error.getMessage())));
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
        Session session = session();
        try (PreparedStatement ps = session.conn.prepareStatement(duckSql);
                org.duckdb.DuckDBResultSet drs = (org.duckdb.DuckDBResultSet) ps.executeQuery();
                ArrowReader reader = (ArrowReader) drs.arrowExportStream(allocator, batchSize)) {
            org.apache.arrow.vector.types.pojo.Schema schema =
                    reader.getVectorSchemaRoot().getSchema();
            LogUtil.logTiming(t, "duckdb.describeSql", "fields=" + schema.getFields().size());
            return schema;
        } catch (Exception e) {
            dropSessionIfInvalidated(session, e);
            throw e;
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
     * @param file Parquet file opened through the data store
     * @param name file name, for logs
     * @return total row count across all row groups
     * @throws IOException on read failure
     */
    public static long footerRowCount(InputFile file, String name) throws IOException {
        long t = LogUtil.mark();
        try (ParquetFileReader pfr = ParquetFileReader.open(file)) {
            long count = 0;
            for (BlockMetaData b : pfr.getFooter().getBlocks()) {
                count += b.getRowCount();
            }
            LogUtil.logTiming(t, "duckdb.footerRowCount", "file=" + name + " count=" + count);
            return count;
        }
    }

    /**
     * Reads MIN/MAX/COUNT statistics from Parquet footer. Zero column I/O.
     *
     * @param file Parquet file opened through the data store
     * @param name file name, for logs
     * @param pq   parsed query with select expressions
     * @return optional array of aggregated stats values, empty if any stats are missing
     * @throws IOException on read failure
     */
    @SuppressWarnings("java:S3776") // Footer aggregation handles every supported statistic in one pass.
    public static Optional<Object[]> footerStats(InputFile file, String name,
            ParquetQueryParser pq) throws IOException {
        long t = LogUtil.mark();
        try (ParquetFileReader pfr = ParquetFileReader.open(file)) {
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
            LogUtil.logTiming(t, "duckdb.footerStats", "file=" + name);
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
        Session session = session();
        try {
            try {
                streamComplete(session, allocator, sql, parameters, views, listener, startListener);
            } catch (ChangedFileBeforeStart e) {
                logChangedFileRetry(e);
                streamComplete(session, allocator, sql, parameters, views, listener, startListener);
            }
        } catch (ChangedFileBeforeStart e) {
            throw e.error;
        } catch (Exception e) {
            dropSessionIfInvalidated(session, e);
            throw e;
        }
    }

    /**
     * Streams the complete answer of a client's query in one thread's session.
     *
     * @param session       this thread's session
     * @param allocator     Arrow buffer allocator
     * @param sql           client SQL
     * @param parameters    values of the query's parameters, in order
     * @param views         current view definitions
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws Exception on refusal, query or stream failure
     */
    @SuppressWarnings({"java:S107", "checkstyle:ParameterNumber"})
    private void streamComplete(Session session, BufferAllocator allocator, String sql,
            List<Object> parameters,
            pro.surpin.data.arrowflight.server.services.CatalogViews.Snapshot views,
            org.apache.arrow.flight.FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {
        if (session.viewsVersion != views.version()) {
            session.viewDrops = applyViews(session.conn, session.viewDrops,
                    views.dataStatements(), views.dropStatements());
            session.viewsVersion = views.version();
        }
        // Parse first: preparing a text with several statements runs all but the last one.
        checkSingleQuery(session, sql);
        PreparedStatement ps = session.statement(sql);
        requireQuery(sql, ps);
        bindParameters(ps, parameters);
        // Not streamSql: its parse check would take a statement from the cache, and a cache of
        // one statement would close this bound one ("Parameter count mismatch").
        streamPrepared(allocator, sql, ps, listener, startListener);
    }

    /**
     * Runs a client's query with parameter values over the catalog views once and streams the
     * complete answer, for SQL that reads Arrow streams registered on this thread's connection:
     * the statement is prepared for this run alone (never cached), its parse check is not
     * remembered, and a failure is not retried.
     *
     * @param allocator     Arrow buffer allocator
     * @param sql           SQL with its stream sources
     * @param parameters    values of the query's parameters, in order
     * @param views         current view definitions
     * @param listener      Flight stream listener
     * @param startListener whether to call listener.start()
     * @throws Exception on refusal, query or stream failure
     */
    public void streamCompleteOnce(BufferAllocator allocator, String sql, List<Object> parameters,
            pro.surpin.data.arrowflight.server.services.CatalogViews.Snapshot views,
            org.apache.arrow.flight.FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {
        Session session = session();
        try {
            if (session.viewsVersion != views.version()) {
                session.viewDrops = applyViews(session.conn, session.viewDrops,
                        views.dataStatements(), views.dropStatements());
                session.viewsVersion = views.version();
            }
            requireNoSessionCatalog(requireSingleQuery(session.statement(PARSE_CHECK_SQL), sql));
            try (PreparedStatement ps = session.conn.prepareStatement(sql)) {
                requireQuery(sql, ps);
                bindParameters(ps, parameters);
                streamPrepared(allocator, sql, ps, listener, startListener);
            }
        } catch (ChangedFileBeforeStart e) {
            throw e.error;
        } catch (Exception e) {
            dropSessionIfInvalidated(session, e);
            throw e;
        }
    }

    /**
     * Sets the values of a statement's parameters, first clearing those of an earlier run.
     *
     * @param ps         prepared statement
     * @param parameters values, in order
     * @throws IllegalArgumentException when the number of values does not fit the statement
     * @throws java.sql.SQLException when a value cannot be set
     */
    private static void bindParameters(PreparedStatement ps, List<Object> parameters)
            throws java.sql.SQLException {
        int expected = ps.getParameterMetaData().getParameterCount();
        if (expected != parameters.size()) {
            throw new IllegalArgumentException("The query has " + expected + " parameter(s), "
                    + parameters.size() + " value(s) were sent");
        }
        // A cached statement keeps the values of an earlier execution otherwise.
        ps.clearParameters();
        for (int i = 0; i < parameters.size(); i++) {
            ps.setObject(i + 1, parameters.get(i));
        }
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
        if (parameters.isEmpty()) {
            Description cached = cachedDescription(sql, views);
            if (cached != null) {
                return cached.result();
            }
        }
        Session session = session();
        try {
            return describeComplete(session, allocator, sql, parameters, views);
        } catch (Exception e) {
            dropSessionIfInvalidated(session, e);
            throw e;
        }
    }

    /**
     * Returns the Arrow schema of a client's query in one thread's session.
     *
     * @param session    this thread's session
     * @param allocator  Arrow buffer allocator
     * @param sql        client SQL
     * @param parameters values of the query's parameters, or an empty list
     * @param views      current view definitions
     * @return schema of the complete answer
     * @throws Exception on refusal or when DuckDB rejects the query
     */
    private org.apache.arrow.vector.types.pojo.Schema describeComplete(Session session,
            BufferAllocator allocator, String sql, List<Object> parameters,
            pro.surpin.data.arrowflight.server.services.CatalogViews.Snapshot views)
            throws Exception {
        Connection conn = describeConnection(session, sql, views);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            requireQuery(sql, ps);
            java.sql.ParameterMetaData parameterMetaData = ps.getParameterMetaData();
            if (!parameters.isEmpty()) {
                return describeBound(allocator, conn, sql, ps, parameterMetaData, parameters);
            }
            // Read before the run: DuckDB closes a statement whose run failed.
            org.apache.arrow.vector.types.pojo.Schema parameterSchema =
                    QueryParameters.schema(parameterMetaData);
            org.apache.arrow.vector.types.pojo.Schema result =
                    describeBound(allocator, conn, sql, ps, parameterMetaData, parameters);
            cacheDescription(sql, new Description(views, result, parameterSchema));
            return result;
        }
    }

    /**
     * Binds the given values, or probes for each parameter without them, and describes the run.
     *
     * @param allocator         Arrow buffer allocator
     * @param conn              describe connection
     * @param sql               client SQL
     * @param ps                the query prepared on that connection
     * @param parameterMetaData its parameters
     * @param parameters        values of the query's parameters, or an empty list
     * @return schema of the complete answer
     * @throws Exception when DuckDB rejects the query
     */
    @SuppressWarnings({"java:S107", "checkstyle:ParameterNumber"})
    private org.apache.arrow.vector.types.pojo.Schema describeBound(BufferAllocator allocator,
            Connection conn, String sql, PreparedStatement ps,
            java.sql.ParameterMetaData parameterMetaData, List<Object> parameters) throws Exception {
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

    /**
     * Returns the cached description of a query text over the given views.
     *
     * @param sql   client SQL
     * @param views current view definitions
     * @return description, or null when the text was not described over these views
     */
    private Description cachedDescription(String sql,
            pro.surpin.data.arrowflight.server.services.CatalogViews.Snapshot views) {
        Description cached = describeCache.get(sql);
        // Identity: a new snapshot is made whenever any view changes.
        return cached != null && cached.views() == views ? cached : null;
    }

    /**
     * Remembers the description of a query text unless the text is too long to be cached.
     *
     * @param sql         client SQL
     * @param description its description
     */
    private void cacheDescription(String sql, Description description) {
        if (!QueryCacheLimits.cacheable(sql)) {
            CacheMetrics.oversize(CacheMetrics.Cache.DESCRIBE);
            return;
        }
        synchronized (describeCache) {
            describeCache.put(sql, description);
            if (describeCache.size() > DESCRIBE_CACHE_CAPACITY) {
                describeCache.remove(describeCache.keySet().iterator().next());
                CacheMetrics.eviction(CacheMetrics.Cache.DESCRIBE);
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
        Description cached = cachedDescription(sql, views);
        if (cached != null) {
            return cached.parameters();
        }
        Session session = session();
        try {
            Connection conn = describeConnection(session, sql, views);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                requireQuery(sql, ps);
                return QueryParameters.schema(ps.getParameterMetaData());
            }
        } catch (Exception e) {
            dropSessionIfInvalidated(session, e);
            throw e;
        }
    }

    /**
     * Returns this thread's DuckDB for describing, with current empty views, after checking that
     * the text is one query.
     *
     * @param session this thread's session
     * @param sql     client SQL
     * @param views   current view definitions
     * @return DuckDB connection
     * @throws Exception on refusal or when DuckDB cannot be opened
     */
    private Connection describeConnection(Session session, String sql,
            pro.surpin.data.arrowflight.server.services.CatalogViews.Snapshot views)
            throws Exception {
        if (session.describeConn == null) {
            session.describeConn = openDescribeConnection();
        }
        Connection conn = session.describeConn;
        if (session.describeVersion != views.version()) {
            session.describeDrops = applyViews(conn, session.describeDrops,
                    views.emptyStatements(), views.dropStatements());
            session.describeVersion = views.version();
        }
        if (!acceptedBefore(sql)) {
            if (session.describeCheck == null || session.describeCheck.isClosed()) {
                session.describeCheck = conn.prepareStatement(PARSE_CHECK_SQL);
            }
            rememberAccepted(sql, requireSingleQuery(session.describeCheck, sql));
        }
        return conn;
    }

    /**
     * Refuses a text unless it is one query that reads, with the parse check of this thread's
     * query connection, unless the text passed the check on this node before.
     *
     * @param session this thread's session
     * @param sql     client SQL
     * @throws IllegalArgumentException when the text is not one query
     * @throws java.sql.SQLException when the check itself fails
     */
    private void checkSingleQuery(Session session, String sql) throws java.sql.SQLException {
        if (!acceptedBefore(sql)) {
            rememberAccepted(sql, requireSingleQuery(session.statement(PARSE_CHECK_SQL), sql));
        }
    }

    /**
     * Tells whether a text passed the parse check on this node before, checking the names it uses
     * against the refused ones again.
     *
     * @param sql client SQL
     * @return true when the check need not run again
     * @throws IllegalArgumentException when the text uses a name refused since
     */
    private boolean acceptedBefore(String sql) {
        List<String> names = parseCheckCache.get(sql);
        if (names == null) {
            return false;
        }
        requireAllowedNames(names);
        requireNoSessionCatalog(names);
        return true;
    }

    /**
     * Refuses names of the sessions' own catalogs when sessions share one DuckDB: a query of one
     * session may not read the catalog views of another.
     *
     * @param names lower-case names a statement uses
     * @throws IllegalArgumentException when such a name is used
     */
    private void requireNoSessionCatalog(List<String> names) {
        if (shared == null) {
            return;
        }
        for (String name : names) {
            if (name.startsWith(SESSION_CATALOG_PREFIX)) {
                throw new IllegalArgumentException("Catalog or schema " + name + " is not allowed");
            }
        }
    }

    /**
     * Tells whether a text's parse check result is kept, for tests.
     *
     * @param sql client SQL
     * @return true when the text passed the check and is remembered
     */
    boolean parseCheckRemembered(String sql) {
        return parseCheckCache.containsKey(sql);
    }

    /**
     * Remembers a text that passed the parse check unless it is too long to be cached.
     *
     * @param sql   client SQL
     * @param names function and table names the text uses
     */
    private void rememberAccepted(String sql, List<String> names) {
        requireNoSessionCatalog(names);
        if (!QueryCacheLimits.cacheable(sql)) {
            CacheMetrics.oversize(CacheMetrics.Cache.PARSE_CHECK);
            return;
        }
        synchronized (parseCheckCache) {
            parseCheckCache.put(sql, names);
            if (parseCheckCache.size() > PARSE_CHECK_CACHE_CAPACITY) {
                parseCheckCache.remove(parseCheckCache.keySet().iterator().next());
                CacheMetrics.eviction(CacheMetrics.Cache.PARSE_CHECK);
            }
        }
    }

    /**
     * Refuses a text unless DuckDB parses it as exactly one query that reads. Nothing is run.
     *
     * @param check the parse check statement ({@link #PARSE_CHECK_SQL}) of the connection
     * @param sql client SQL
     * @return the function and table names the query uses, lower case
     * @throws IllegalArgumentException when the text is not one query
     * @throws java.sql.SQLException when the check itself fails
     */
    static List<String> requireSingleQuery(PreparedStatement check, String sql) throws java.sql.SQLException {
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
        return requireAllowedFunctions(serialized);
    }

    /**
     * Refuses a parsed statement that names a function or system view of {@link #REFUSED_FUNCTIONS}.
     *
     * @param serialized DuckDB's JSON form of the statement
     * @return the function and table names it uses, lower case
     * @throws IllegalArgumentException when such a name is used
     */
    static List<String> requireAllowedFunctions(String serialized) {
        if (serialized == null) {
            return List.of();
        }
        List<String> names = new ArrayList<>();
        java.util.regex.Matcher m = SERIALIZED_NAME.matcher(serialized);
        while (m.find()) {
            String name = m.group(1).toLowerCase(java.util.Locale.ROOT);
            if (!names.contains(name)) {
                names.add(name);
            }
        }
        java.util.regex.Matcher q = SERIALIZED_QUALIFIER.matcher(serialized);
        while (q.find()) {
            // Only the qualifiers a shared DuckDB refuses are kept; others are no names to check.
            String name = q.group(1).toLowerCase(java.util.Locale.ROOT);
            if (name.startsWith(SESSION_CATALOG_PREFIX) && !names.contains(name)) {
                names.add(name);
            }
        }
        requireAllowedNames(names);
        return List.copyOf(names);
    }

    /**
     * Refuses function or table names of {@link #REFUSED_FUNCTIONS} or refused by a plugin.
     *
     * @param names lower-case names a statement uses
     * @throws IllegalArgumentException when such a name is used
     */
    private static void requireAllowedNames(List<String> names) {
        for (String name : names) {
            if (REFUSED_FUNCTIONS.contains(name) || PluginRefusedFunctions.contains(name)) {
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
        /** Parse check ({@link #PARSE_CHECK_SQL}) prepared on the describe connection. */
        private PreparedStatement describeCheck;
        private final StatementCache statements;
        /** Connection of the shared DuckDB this session connects to; null for a DuckDB of its own. */
        private final Connection root;
        /** The session's catalog in the shared DuckDB; null for a DuckDB of its own. */
        private final String catalog;

        /**
         * Binds a connection to the thread that uses it.
         *
         * @param owner         thread the connection belongs to
         * @param conn          DuckDB connection
         * @param maxStatements prepared statements to keep; 0 or less keeps only the one in use
         * @param root          connection of the shared DuckDB, or null
         * @param catalog       the session's catalog in the shared DuckDB, or null
         */
        private Session(Thread owner, Connection conn, int maxStatements, Connection root,
                String catalog) {
            this.owner = owner;
            this.conn = conn;
            this.root = root;
            this.catalog = catalog;
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
            return statements.statement(sql, conn::prepareStatement);
        }

        /**
         * Closes the statements and the connection.
         */
        private void close() {
            statements.closeAll();
            if (catalog != null) {
                // The catalog belongs to the shared DuckDB and outlives this connection.
                try (Statement s = conn.createStatement()) {
                    s.execute("USE memory");
                    s.execute("DETACH DATABASE IF EXISTS " + quoteIdentifier(catalog));
                } catch (Exception e) {
                    LOGGER.debug("Failed to detach DuckDB session catalog {}", catalog, e);
                }
            }
            closeQuietly(conn);
            if (describeCheck != null) {
                closeQuietly(describeCheck);
            }
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
        Connection root = sharedRoot;
        if (root != null) {
            Session.closeQuietly(root);
        }
        deleteRecursively(tempRoot);
    }
}
