package pro.surpin.data.arrowflight.server.adapters;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolStringList;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.BasicFlightSqlProducer;
import org.apache.arrow.flight.sql.SqlInfoBuilder;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.server.AllowAllAuthorization;
import pro.surpin.data.arrowflight.server.FlightAuthorization;
import org.slf4j.MDC;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import pro.surpin.data.arrowflight.server.model.ExecutionPath;
import pro.surpin.data.arrowflight.server.model.HandleState;
import pro.surpin.data.arrowflight.server.model.ExecutionPathTracker;
import pro.surpin.data.arrowflight.server.model.QueryPlan;
import pro.surpin.data.arrowflight.server.metrics.ExecutionPathRecorder;
import pro.surpin.data.arrowflight.server.metrics.MetricsService;
import pro.surpin.data.arrowflight.server.services.ClusterService;
import pro.surpin.data.arrowflight.server.services.ExecutionService;
import pro.surpin.data.arrowflight.server.services.MetadataService;
import pro.surpin.data.arrowflight.server.services.QueryPlanner;
import pro.surpin.data.arrowflight.server.services.TableStatisticsService;

import static com.google.protobuf.ByteString.copyFrom;
import static java.util.UUID.randomUUID;

/**
 * Flight SQL producer that handles metadata and query execution requests.
 */
public final class FlightSqlProducer extends BasicFlightSqlProducer implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlightSqlProducer.class);
    /** Counts stream executions, for query ids in the log. */
    private static final java.util.concurrent.atomic.AtomicLong EXECUTIONS = new java.util.concurrent.atomic.AtomicLong();
    private static final String SCHEMA_NOT_FOUND_MESSAGE = "Could not find Arrow schema for query";

    /** Location of metadata endpoints; null = none, the client reads over its own connection. */
    private final Location metadataLocation;
    private final BufferAllocator allocator;
    private final MetadataService metadataService;
    private final QueryPlanner queryPlanner;
    private final ExecutionService executionService;
    private final ClusterService clusterService;
    private final SqlInfoBuilder sqlInfoBuilder;
    private final FlightAuthorization authorization;
    private final String defaultResultMode;
    private final java.util.Set<Integer> knownSqlInfo;

    /**
     * @param location server location; unused, endpoints of metadata calls carry no location
     * @param allocator Arrow buffer allocator
     * @param metadataService metadata lookup service
     * @param queryPlanner query planning and endpoint determination
     * @param executionService query execution service
     * @param clusterService cluster state management
     */
    public FlightSqlProducer(Location location, BufferAllocator allocator,
            MetadataService metadataService, QueryPlanner queryPlanner,
            ExecutionService executionService, ClusterService clusterService) {
        this(location, allocator, metadataService, queryPlanner,
                executionService, clusterService, new AllowAllAuthorization());
    }

    /**
     * Creates a producer with authorization enforcement.
     *
     * @param location server location; metadata endpoints carry no location
     * @param allocator Arrow buffer allocator
     * @param metadataService metadata lookup service
     * @param queryPlanner query planning service
     * @param executionService query execution service
     * @param clusterService cluster state management
     * @param authorization authorization evaluator
     */
    public FlightSqlProducer(Location location, BufferAllocator allocator,
            MetadataService metadataService, QueryPlanner queryPlanner,
            ExecutionService executionService, ClusterService clusterService,
            FlightAuthorization authorization) {
        this(location, allocator, metadataService, queryPlanner, executionService,
                clusterService, authorization, "partial");
    }

    /**
     * Creates the producer with the result mode clients get when they do not ask for one.
     *
     * @param location server location; metadata endpoints carry no location
     * @param allocator Arrow buffer allocator
     * @param metadataService metadata lookup service
     * @param queryPlanner query planning and endpoint determination
     * @param executionService query execution service
     * @param clusterService cluster state management
     * @param defaultResultMode {@code complete} or {@code partial}
     */
    @SuppressWarnings("java:S107") // Dependencies of the producer, wired in one place.
    public FlightSqlProducer(Location location, BufferAllocator allocator,
            MetadataService metadataService, QueryPlanner queryPlanner,
            ExecutionService executionService, ClusterService clusterService,
            String defaultResultMode) {
        this(location, allocator, metadataService, queryPlanner, executionService,
                clusterService, new AllowAllAuthorization(), defaultResultMode);
    }

    /**
     * Creates the producer with authorization and a default result mode.
     *
     * @param location server location; metadata endpoints carry no location
     * @param allocator Arrow buffer allocator
     * @param metadataService metadata lookup service
     * @param queryPlanner query planning and endpoint determination
     * @param executionService query execution service
     * @param clusterService cluster state management
     * @param authorization authorization evaluator
     * @param defaultResultMode {@code complete} or {@code partial}
     */
    @SuppressWarnings("java:S107") // Dependencies of the producer, wired in one place.
    public FlightSqlProducer(Location location, BufferAllocator allocator,
            MetadataService metadataService, QueryPlanner queryPlanner,
            ExecutionService executionService, ClusterService clusterService,
            FlightAuthorization authorization, String defaultResultMode) {
        this(location, allocator, metadataService, queryPlanner, executionService,
                clusterService, authorization, defaultResultMode, false);
    }

    /**
     * Creates the producer with authorization, a default result mode and the choice of where
     * clients read metadata from.
     *
     * @param location server location (the advertised URI)
     * @param allocator Arrow buffer allocator
     * @param metadataService metadata lookup service
     * @param queryPlanner query planning and endpoint determination
     * @param executionService query execution service
     * @param clusterService cluster state management
     * @param authorization authorization evaluator
     * @param defaultResultMode {@code complete} or {@code partial}
     * @param advertiseMetadataLocation true: metadata endpoints carry {@code location};
     *                                  false: no location, the client reads them over the
     *                                  connection it already has
     */
    @SuppressWarnings({"java:S107", "checkstyle:ParameterNumber"}) // Wired in one place.
    public FlightSqlProducer(Location location, BufferAllocator allocator,
            MetadataService metadataService, QueryPlanner queryPlanner,
            ExecutionService executionService, ClusterService clusterService,
            FlightAuthorization authorization, String defaultResultMode,
            boolean advertiseMetadataLocation) {
        this.defaultResultMode = defaultResultMode == null ? "partial" : defaultResultMode.trim();
        this.metadataLocation = advertiseMetadataLocation ? location : null;
        this.allocator = allocator;
        this.metadataService = metadataService;
        this.queryPlanner = queryPlanner;
        this.executionService = executionService;
        this.clusterService = clusterService;
        this.authorization = authorization;

        this.sqlInfoBuilder = new SqlInfoBuilder();
        sqlInfoBuilder
                .withFlightSqlServerName("Hadoop-Arrow-Parquet Source")
                .withFlightSqlServerVersion("0.0.1")
                .withFlightSqlServerArrowVersion("0.0.1")
                .withFlightSqlServerReadOnly(true)
                .withFlightSqlServerSql(true)
                .withFlightSqlServerSubstrait(false)
                .withFlightSqlServerTransaction(
                        FlightSql.SqlSupportedTransaction.SQL_SUPPORTED_TRANSACTION_NONE)
                .withSqlDdlCatalog(false)
                .withSqlDdlSchema(true)
                .withSqlDdlTable(true)
                .withSqlIdentifierCase(
                        FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_LOWERCASE)
                .withSqlQuotedIdentifierCase(
                        FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UNKNOWN)
                .withSqlAllTablesAreSelectable(true)
                .withSqlNullOrdering(FlightSql.SqlNullOrdering.SQL_NULLS_SORTED_AT_END)
                .withSqlIdentifierQuoteChar("\"")
                .withSqlMaxColumnsInTable(1000);
        this.knownSqlInfo = knownSqlInfo(sqlInfoBuilder);
    }

    /**
     * Lists the SqlInfo ids the builder has a value for, by sending them all to a recorder.
     *
     * @param builder configured SqlInfo builder
     * @return ids with a value
     */
    private static java.util.Set<Integer> knownSqlInfo(SqlInfoBuilder builder) {
        java.util.Set<Integer> ids = new java.util.HashSet<>();
        FlightProducer.ServerStreamListener recorder = (FlightProducer.ServerStreamListener)
                java.lang.reflect.Proxy.newProxyInstance(
                        FlightSqlProducer.class.getClassLoader(),
                        new Class<?>[] {FlightProducer.ServerStreamListener.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("start")
                                    && args[0] instanceof VectorSchemaRoot root) {
                                org.apache.arrow.vector.UInt4Vector names =
                                        (org.apache.arrow.vector.UInt4Vector) root.getVector(0);
                                for (int row = 0; row < root.getRowCount(); row++) {
                                    ids.add(names.get(row));
                                }
                            }
                            return method.getReturnType() == boolean.class ? Boolean.FALSE : null;
                        });
        builder.send(List.of(), recorder);
        return java.util.Set.copyOf(ids);
    }

    /**
     * Answers the table statistics action (JSON {schema, table, columns}) and passes every other
     * action to the Flight SQL handling.
     *
     * @param context authenticated call context
     * @param action requested action
     * @param listener result listener
     */
    @Override
    public void doAction(FlightProducer.CallContext context, Action action,
            FlightProducer.StreamListener<Result> listener) {
        if (!TableStatisticsService.ACTION.equals(action.getType())) {
            super.doAction(context, action, listener);
            return;
        }
        try {
            TableStatisticsService.Request request =
                    TableStatisticsService.parseRequest(action.getBody());
            if (request.schema() == null || request.schema().isEmpty()
                    || request.table() == null || request.table().isEmpty()) {
                throw new IllegalArgumentException("Name the schema and the table");
            }
            ParquetAdapter.validateName(request.schema());
            ParquetAdapter.validateName(request.table());
            authorization.requireQuery(context == null ? null : context.peerIdentity(),
                    "SELECT * FROM " + request.schema() + "." + request.table());
            TableStatisticsService.TableStatistics statistics = queryPlanner.tableStatistics(
                    request.schema(), request.table(), request.columns());
            listener.onNext(new Result(TableStatisticsService.toJson(statistics)
                    .getBytes(StandardCharsets.UTF_8)));
            listener.onCompleted();
        } catch (FlightRuntimeException e) {
            listener.onError(e);
        } catch (java.io.FileNotFoundException e) {
            listener.onError(CallStatus.NOT_FOUND.withDescription(e.getMessage()).toRuntimeException());
        } catch (IllegalArgumentException e) {
            listener.onError(CallStatus.INVALID_ARGUMENT.withDescription(e.getMessage())
                    .toRuntimeException());
        } catch (IllegalStateException e) {
            listener.onError(CallStatus.UNIMPLEMENTED.withDescription(e.getMessage())
                    .toRuntimeException());
        } catch (IOException | RuntimeException e) {
            LOGGER.warn("node={} statistics=failed error='{}'", LogUtil.node(), e.toString());
            listener.onError(CallStatus.INTERNAL.withDescription("Table statistics failed: "
                    + e.getMessage()).withCause(e).toRuntimeException());
        }
    }

    /**
     * Lists the Flight SQL actions and the table statistics action.
     *
     * @param context call context
     * @param listener action type listener
     */
    @Override
    public void listActions(FlightProducer.CallContext context,
            FlightProducer.StreamListener<ActionType> listener) {
        super.listActions(context, new FlightProducer.StreamListener<>() {
            @Override
            public void onNext(ActionType value) {
                listener.onNext(value);
            }

            @Override
            public void onError(Throwable t) {
                listener.onError(t);
            }

            @Override
            public void onCompleted() {
                listener.onNext(new ActionType(TableStatisticsService.ACTION,
                        "Table statistics for an optimizer: JSON {schema, table, columns}"));
                listener.onCompleted();
            }
        });
    }

    @Override
    public void getStreamStatement(FlightSql.TicketStatementQuery ticket,
            FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        final ByteString handle = ticket.getStatementHandle();
        // Equal queries have equal tickets; the sequence number tells their executions apart.
        String qid = qid(handle) + "-" + Long.toString(EXECUTIONS.incrementAndGet(), 36);
        long tGet = LogUtil.mark();
        HandleState state;
        try {
            state = clusterService.resolveEndpointHandle(handle.toByteArray());
        } catch (IllegalArgumentException e) {
            listener.error(CallStatus.UNAUTHENTICATED
                    .withDescription("Invalid Flight endpoint ticket")
                    .withCause(e).toRuntimeException());
            return;
        }
        LogUtil.logTiming(tGet, "execution.getHandle");
        if (state == null) {
            LOGGER.error("qid={} No HandleState found", qid);
            listener.error(new IllegalStateException("No HandleState found for qid=" + qid));
            return;
        }

        String query = state.query();
        String[] filePaths = state.complete() ? new String[0] : state.filePaths();
        if (filePaths == null) {
            LOGGER.error("qid={} No file paths in handle state", qid);
            listener.error(new IllegalStateException("No file paths for qid=" + qid));
            return;
        }

        String serverUri = state.serverUri() != null ? state.serverUri() : "local";
        java.util.concurrent.CompletableFuture<Void> loadClaim = null;
        if (clusterService.plannedElsewhere(state)) {
            // A backup location: the client could not reach the planned node.
            LOGGER.info("qid={} node={} execution=failover planned={} files={}",
                    qid, LogUtil.node(), serverUri, filePaths.length);
            loadClaim = clusterService.claimEndpointLoadLater(handle.toByteArray(), state);
        }
        long bytes = state.bytes();
        long tExec = LogUtil.mark();
        long executionStartNanos = System.nanoTime();
        LogUtil.setQid(qid);
        LOGGER.debug("qid={} node={} thread={} execution=start server={} files={} bytes={} endpoint={} query='{}'",
                qid, LogUtil.node(), Thread.currentThread().getName(),
                serverUri, filePaths.length, bytes, qid, query);

        MDC.put("qid", qid);
        MetricsService.QueryObservation observation =
                MetricsService.observeQuery(bytes);
        ExecutionPathTracker pathTracker = new ExecutionPathTracker();
        boolean success = false;
        String failureReason = null;
        try {
            authorization.requireQuery(context == null ? null : context.peerIdentity(), query);
            if (state.complete()) {
                pathTracker.select(ExecutionPath.COMPLETE_ANSWER);
                executionService.readComplete(allocator, query, state.parameters(), listener, true);
            } else {
                executionService.readParquet(
                        allocator, query, filePaths, listener, true, pathTracker);
            }
            listener.completed();
            success = true;
            LogUtil.logTiming(tExec, "execution.total", "files=" + filePaths.length);
            long elapsed = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - executionStartNanos);
            LOGGER.debug("qid={} node={} thread={} execution=completed server={} elapsedMs={} files={} result=completed query='{}'",
                    qid, LogUtil.node(), Thread.currentThread().getName(),
                    serverUri, elapsed, filePaths.length, query);
        } catch (Exception e) {
            observation.markFailed();
            LogUtil.logTiming(tExec, "execution.failed", "files=" + filePaths.length);
            long elapsed = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - executionStartNanos);
            String failure = e instanceof IllegalArgumentException ? e.getMessage() : failureDescription(e);
            failureReason = failure;
            boolean cancelled = e instanceof FlightRuntimeException fe
                    && fe.status().code() == org.apache.arrow.flight.FlightStatusCode.CANCELLED;
            if (cancelled || e instanceof IllegalArgumentException) {
                // The client went away, or its query or values are wrong: not a server fault.
                LOGGER.info("qid={} node={} thread={} execution=failed server={} elapsedMs={} files={} result={} error='{}'",
                        qid, LogUtil.node(), Thread.currentThread().getName(),
                        serverUri, elapsed, filePaths.length, cancelled ? "cancelled" : "rejected", failure);
            } else {
                LOGGER.error("qid={} node={} thread={} execution=failed server={} elapsedMs={} files={} result=failed error='{}'",
                        qid, LogUtil.node(), Thread.currentThread().getName(),
                        serverUri, elapsed, filePaths.length, failure, e);
            }
            if (e instanceof FlightRuntimeException flightException) {
                listener.error(flightException);
            } else if (e instanceof IllegalArgumentException) {
                listener.error(CallStatus.INVALID_ARGUMENT.withDescription(e.getMessage())
                        .withCause(e).toRuntimeException());
            } else {
                listener.error(CallStatus.INTERNAL
                        .withDescription(failure)
                        .withCause(e)
                        .toRuntimeException());
            }
        } finally {
            observation.executionPath(pathTracker.path());
            ExecutionPathRecorder.recordEvent(
                    qid, query, pathTracker, success, failureReason);
            observation.close();
            MDC.remove("qid");
            LogUtil.setQid(null);
            if (loadClaim != null) {
                // The client has its answer already; release only what the claim moved here.
                loadClaim.join();
            }
            clusterService.releaseEndpointLoad(handle.toByteArray(), state);
        }
    }

    private static boolean isFileNotFound(Throwable e) {
        Throwable cause = e;
        while (cause != null) {
            if (cause instanceof java.io.FileNotFoundException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    /**
     * Finds the outermost storage error with a message in a failure's causes.
     *
     * @param failure failure
     * @return the I/O exception, or null when the failure has none
     */
    private static IOException storageFailure(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof IOException io && io.getMessage() != null
                    && !io.getMessage().isBlank()) {
                return io;
            }
            if (cause.getCause() == cause) {
                break;
            }
        }
        return null;
    }

    static String failureDescription(Throwable failure) {
        List<Throwable> chain = new java.util.ArrayList<>();
        for (Throwable t = failure; t != null && chain.size() < 32;
                t = t.getCause() == t ? null : t.getCause()) {
            chain.add(t);
        }
        Throwable root = chain.get(chain.size() - 1);
        String rootText = root.getMessage();
        if (rootText == null || rootText.isBlank()) {
            // e.g. ConnectException/UnresolvedAddressException without text under the store's
            // "lakeFS API GET ... not reachable": name the deepest cause that explains it.
            for (int i = chain.size() - 2; i >= 0; i--) {
                String text = chain.get(i).getMessage();
                if (text != null && !text.isBlank()) {
                    root = chain.get(i);
                    break;
                }
            }
        } else {
            // A storage IOException that repeats the root's text adds context ("lakeFS API GET
            // <uri> failed, lakeFS not reachable: HttpConnectTimeoutException HTTP connect timed
            // out"): prefer the deepest such wrapper over the bare root. Messages that are only
            // the cause's toString() (new RuntimeException(cause)) add nothing.
            for (int i = chain.size() - 2; i >= 0; i--) {
                Throwable t = chain.get(i);
                String text = t.getMessage();
                if (t instanceof IOException && text != null
                        && text.length() > rootText.length() && text.contains(rootText)
                        && !text.equals(chain.get(i + 1).toString())) {
                    root = chain.get(i);
                    break;
                }
            }
        }
        String message = StackTraceTrimmer.trim(root.getMessage());
        String description = "Query execution failed: " + root.getClass().getSimpleName();
        if (message != null && !message.isBlank()) {
            description += ": " + message;
        }
        return description.length() <= 1024 ? description : description.substring(0, 1024);
    }

    @Override
    public FlightInfo getFlightInfoStatement(FlightSql.CommandStatementQuery command,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        long t = LogUtil.mark();
        ByteString handle = copyFrom(randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        String query = command.getQuery();
        authorization.requireQuery(context == null ? null : context.peerIdentity(), query);
        String qid = qid(handle);
        LogUtil.logTiming(t, "schema.resolveQuery", "qid=" + qid);
        return flightInfoForQuery(query, descriptor, context);
    }

    /**
     * Tells whether a call gets the complete answer from one node: the client's header decides,
     * otherwise the configured default.
     *
     * @param context call context, or null
     * @return true for complete-answer mode
     */
    private boolean completeAnswer(FlightProducer.CallContext context) {
        String requested = null;
        if (context != null) {
            ResultModeMiddleware middleware = context.getMiddleware(ResultModeMiddleware.KEY);
            requested = middleware == null ? null : middleware.requested();
        }
        String mode = requested == null || requested.isBlank() ? defaultResultMode : requested;
        return "complete".equalsIgnoreCase(mode.trim());
    }

    /**
     * Tells whether the client of a call tries an endpoint's locations in order.
     *
     * @param context call context, or null
     * @return true when the client said so in its headers
     */
    private static boolean locationFailover(FlightProducer.CallContext context) {
        if (context == null) {
            return false;
        }
        ResultModeMiddleware middleware = context.getMiddleware(ResultModeMiddleware.KEY);
        return middleware != null && middleware.locationFailover();
    }

    /**
     * Describes the complete answer to a client's query.
     *
     * @param query client SQL
     * @return result schema
     * @throws FlightRuntimeException INVALID_ARGUMENT for refused or invalid SQL, NOT_FOUND for
     *         an unknown table
     */
    private Schema completeSchemaFor(String query) {
        return completeSchemaFor(query, null);
    }

    /**
     * Describes the complete answer to a client's query with parameter values.
     *
     * @param query client SQL
     * @param parameters one-row Arrow IPC stream with the values, or null
     * @return result schema
     */
    private Schema completeSchemaFor(String query, byte[] parameters) {
        return describeComplete(query, () -> executionService.completeSchema(allocator, query, parameters));
    }

    /**
     * Describes the parameters of a client's query in complete-answer mode.
     *
     * @param query client SQL
     * @return one field per parameter
     * @throws FlightRuntimeException as {@link #completeSchemaFor(String)} does
     */
    private Schema completeParameterSchemaFor(String query) {
        return describeComplete(query, () -> executionService.completeParameterSchema(query));
    }

    /**
     * Runs a describe call and maps its failures to Flight statuses.
     *
     * @param query client SQL, for the log
     * @param describe the call
     * @return its schema
     */
    private Schema describeComplete(String query, java.util.function.Supplier<Schema> describe) {
        try {
            return describe.get();
        } catch (IllegalArgumentException e) {
            String message = String.valueOf(e.getMessage());
            LOGGER.info("node={} query=refused reason='{}' query='{}'", LogUtil.node(),
                    message.lines().findFirst().orElse(message), query);
            CallStatus status = message.startsWith("Catalog Error")
                    ? CallStatus.NOT_FOUND : CallStatus.INVALID_ARGUMENT;
            throw status.withDescription(message).withCause(e).toRuntimeException();
        } catch (java.io.UncheckedIOException e) {
            // A table catalog (Hive Metastore, Iceberg REST) cannot be read now.
            LOGGER.warn("node={} query=catalogUnavailable error='{}' query='{}'", LogUtil.node(),
                    e.getCause().getMessage(), query);
            throw CallStatus.UNAVAILABLE.withDescription(e.getCause().getMessage()).withCause(e)
                    .toRuntimeException();
        } catch (RuntimeException e) {
            LOGGER.error("Error describing query: {}", query, e);
            throw CallStatus.INTERNAL.withDescription(failureDescription(e))
                    .withCause(e).toRuntimeException();
        }
    }

    /**
     * Plans a query and describes its result.
     *
     * @param query SQL query
     * @param descriptor flight descriptor of the request
     * @param context call context, which may ask for a result mode
     * @return result schema with the endpoints to read it from
     */
    private FlightInfo flightInfoForQuery(String query, FlightDescriptor descriptor,
            FlightProducer.CallContext context) {
        return flightInfoForQuery(query, null, descriptor, context);
    }

    /**
     * Plans a query with parameter values and describes its result.
     *
     * @param query SQL query
     * @param parameters one-row Arrow IPC stream with the values, or null
     * @param descriptor flight descriptor of the request
     * @param context call context, which may ask for a result mode
     * @return result schema with the endpoints to read it from
     */
    private FlightInfo flightInfoForQuery(String query, byte[] parameters,
            FlightDescriptor descriptor, FlightProducer.CallContext context) {
        if (completeAnswer(context)) {
            Schema schema = completeSchemaFor(query, parameters);
            try {
                return new FlightInfo(schema, descriptor,
                        queryPlanner.planComplete(query, parameters).endpoints(), -1, -1);
            } catch (IllegalStateException e) {
                throw CallStatus.UNIMPLEMENTED.withDescription(e.getMessage()).withCause(e).toRuntimeException();
            }
        }
        if (parameters != null) {
            throw CallStatus.INVALID_ARGUMENT
                    .withDescription("Parameters need complete answers; do not ask for partial results")
                    .toRuntimeException();
        }
        String completeReason = completeAnswerReason(query);
        if (completeReason != null) {
            return completeFlightInfo(query, descriptor, completeReason);
        }
        Schema arrowSchema = schemaForQuery(query);
        try {
            // A client that tries locations in order may also get this node as a backup.
            QueryPlan plan = locationFailover(context)
                    ? queryPlanner.plan(query, true) : queryPlanner.plan(query);
            return new FlightInfo(arrowSchema, descriptor, plan.endpoints(),
                    plan.totalBytes(), plan.totalRecords());
        } catch (IllegalArgumentException e) {
            throw CallStatus.INVALID_ARGUMENT
                    .withDescription(e.getMessage())
                    .withCause(e).toRuntimeException();
        } catch (IOException e) {
            // Name the cause: cluster state the client may wait out (UNAVAILABLE), a table
            // without files (NOT_FOUND), anything else (INTERNAL).
            String message = String.valueOf(e.getMessage());
            LOGGER.warn("Unable to plan query '{}': {}", query, message);
            CallStatus status = message.contains("No live Flight")
                    || message.contains("Metastore unavailable")
                    || message.contains("catalog unavailable")
                    || message.contains("registry is empty")
                    || message.contains("have not published")
                    ? CallStatus.UNAVAILABLE
                    : message.startsWith("No distributed Parquet files found")
                            ? CallStatus.NOT_FOUND : CallStatus.INTERNAL;
            throw status.withDescription("Unable to plan Flight query: " + message)
                    .withCause(e).toRuntimeException();
        }
    }

    /**
     * Tells why a query asked for in parts gets its complete answer instead: each endpoint
     * answers for its own files only, and some queries cannot be computed that way (see
     * {@code ParquetQueryParser.completeAnswerReason}), and queries of catalog tables whose
     * snapshot has delete files or other files that cannot be read one by one, and queries with
     * a time-travel clause.
     *
     * @param query client SQL
     * @return reason, or null when endpoints answer for their own files (also for SQL the parser
     *         rejects: the partial path reports that error)
     */
    private String completeAnswerReason(String query) {
        if (pro.surpin.data.arrowflight.server.services.TimeTravelClause.present(query)) {
            // The endpoints of a partial plan read the current snapshot's files.
            return "the query reads an earlier snapshot (FOR VERSION/TIMESTAMP AS OF)";
        }
        String reason;
        try {
            reason = pro.surpin.data.arrowflight.server.services.ParquetQueryParser.parse(query)
                    .completeAnswerReason;
        } catch (RuntimeException e) {
            return null;
        }
        // Catalog tables (Iceberg) whose files cannot be read one by one, and their joins.
        return reason != null ? reason : queryPlanner.catalogCompleteReason(query);
    }

    /**
     * Plans the complete answer to a query asked for in parts that endpoints cannot compute.
     * One endpoint returns the whole answer, so a client that merges parts still gets the right
     * result; a cluster that keeps its files on local disks refuses the query.
     *
     * @param query client SQL
     * @param descriptor flight descriptor of the request
     * @param reason why the query cannot be answered in parts
     * @return complete-answer plan
     * @throws FlightRuntimeException INVALID_ARGUMENT when no node can read every file
     */
    private FlightInfo completeFlightInfo(String query, FlightDescriptor descriptor, String reason) {
        QueryPlan plan;
        try {
            plan = queryPlanner.planComplete(query, null);
        } catch (IllegalStateException e) {
            String why = String.valueOf(e.getMessage());
            int hint = why.indexOf(" Ask for partial results");
            throw CallStatus.INVALID_ARGUMENT
                    .withDescription(reason + ", so the query needs one node to see all rows; "
                            + (hint > 0 ? why.substring(0, hint) : why))
                    .withCause(e).toRuntimeException();
        }
        LOGGER.info("node={} query=answeredCompletely mode=partial reason='{}' query='{}'",
                LogUtil.node(), reason, query);
        return new FlightInfo(completeSchemaFor(query), descriptor, plan.endpoints(), -1, -1);
    }

    /**
     * Returns the schema of the batches a query streams.
     *
     * @param query SQL query
     * @return result schema
     * @throws FlightRuntimeException INVALID_ARGUMENT for SQL the server cannot run, NOT_FOUND
     *         for an unknown table
     */
    private Schema schemaForQuery(String query) {
        if (completeAnswerReason(query) != null) {
            // Planned as a complete answer (see completeFlightInfo): describe that answer.
            return completeSchemaFor(query);
        }
        Schema arrowSchema;
        try {
            arrowSchema = executionService.resultSchema(allocator, query);
        } catch (IllegalArgumentException e) {
            throw CallStatus.INVALID_ARGUMENT
                    .withDescription(e.getMessage())
                    .withCause(e).toRuntimeException();
        } catch (ExecutionService.CatalogUnavailableException e) {
            // A table catalog (Hive Metastore, Iceberg REST) cannot be read now.
            LOGGER.warn("node={} schema=catalogUnavailable error='{}' query='{}'", LogUtil.node(),
                    e.getCause().getMessage(), query);
            throw CallStatus.UNAVAILABLE.withDescription(e.getCause().getMessage()).withCause(e)
                    .toRuntimeException();
        } catch (Exception e) {
            LOGGER.error("Error getting Arrow schema for query: {}", query, e);
            if (isFileNotFound(e)) {
                throw CallStatus.NOT_FOUND
                        .withDescription(SCHEMA_NOT_FOUND_MESSAGE)
                        .withCause(e).toRuntimeException();
            }
            String message = String.valueOf(e.getMessage());
            if (message.startsWith("Invalid SQL query")) {
                throw CallStatus.INVALID_ARGUMENT
                        .withDescription(failureDescription(e))
                        .withCause(e).toRuntimeException();
            }
            IOException storage = storageFailure(e);
            if (storage != null) {
                // Listing or footer reads failed (store or lakeFS unreachable, refused): say so.
                String why = "Cannot read the table's files: "
                        + StackTraceTrimmer.trim(storage.getMessage());
                throw CallStatus.UNAVAILABLE
                        .withDescription(why.length() <= 1024 ? why : why.substring(0, 1024))
                        .withCause(e).toRuntimeException();
            }
            throw CallStatus.INTERNAL
                    .withDescription("Error getting Arrow schema for query")
                    .withCause(e).toRuntimeException();
        }

        if (arrowSchema == null || arrowSchema.getFields().isEmpty()) {
            LOGGER.error("Arrow schema not found for query: {}", query);
            throw CallStatus.NOT_FOUND
                    .withDescription(SCHEMA_NOT_FOUND_MESSAGE)
                    .toRuntimeException();
        }
        return arrowSchema;
    }

    // ── Prepared statements ──────────────────────────────────────────────
    // Stock Flight SQL clients (JDBC, ADBC) run every query as a prepared statement. The handle
    // is the SQL text itself: nothing is kept on the server, so any node can serve the later
    // calls and nothing leaks when a client never closes a statement.

    @Override
    public void createPreparedStatement(FlightSql.ActionCreatePreparedStatementRequest request,
            FlightProducer.CallContext context, FlightProducer.StreamListener<Result> listener) {
        try {
            String query = request.getQuery();
            authorization.requireQuery(context == null ? null : context.peerIdentity(), query);
            boolean complete = completeAnswer(context);
            Schema arrowSchema = complete ? completeSchemaFor(query) : schemaForQuery(query);
            Schema parameterSchema = complete ? completeParameterSchemaFor(query) : new Schema(List.of());
            FlightSql.ActionCreatePreparedStatementResult result =
                    FlightSql.ActionCreatePreparedStatementResult.newBuilder()
                            .setPreparedStatementHandle(complete ? QueryParameters.statementHandle(query)
                                    : ByteString.copyFromUtf8(query))
                            .setDatasetSchema(copyFrom(arrowSchema.serializeAsMessage()))
                            .setParameterSchema(copyFrom(parameterSchema.serializeAsMessage()))
                            .build();
            listener.onNext(new Result(Any.pack(result).toByteArray()));
            listener.onCompleted();
        } catch (FlightRuntimeException e) {
            listener.onError(e);
        } catch (RuntimeException e) {
            listener.onError(CallStatus.INTERNAL
                    .withDescription(failureDescription(e)).withCause(e).toRuntimeException());
        }
    }

    @Override
    public void closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest request,
            FlightProducer.CallContext context, FlightProducer.StreamListener<Result> listener) {
        try {
            String id = QueryParameters.parseHandle(request.getPreparedStatementHandle()).statementId();
            if (id != null) {
                clusterService.forgetParameters(id);
            }
        } catch (RuntimeException e) {
            LOGGER.debug("Could not drop the parameter values of a closed statement", e);
        }
        listener.onCompleted();
    }

    /**
     * Takes the parameter values of a prepared statement. Nothing is kept: the answer is a new
     * handle that carries the SQL and the values, which the client uses from then on.
     */
    @Override
    public Runnable acceptPutPreparedStatementQuery(FlightSql.CommandPreparedStatementQuery command,
            FlightProducer.CallContext context, org.apache.arrow.flight.FlightStream flightStream,
            FlightProducer.StreamListener<org.apache.arrow.flight.PutResult> ackStream) {
        return () -> {
            try {
                QueryParameters.Handle prepared = parseHandle(command.getPreparedStatementHandle());
                String sql = prepared.sql();
                byte[] values = null;
                int rows = 0;
                while (flightStream.next()) {
                    VectorSchemaRoot root = flightStream.getRoot();
                    rows += root.getRowCount();
                    if (root.getRowCount() > 0) {
                        values = QueryParameters.serialize(root);
                    }
                }
                if (rows > 1) {
                    throw CallStatus.INVALID_ARGUMENT
                            .withDescription("Exactly one set of parameter values is supported, got " + rows)
                            .toRuntimeException();
                }
                if (prepared.statementId() != null) {
                    // For clients that go on using the handle they have. An empty upload drops
                    // earlier values, so that they are not reused silently.
                    if (values != null) {
                        clusterService.rememberParameters(prepared.statementId(), values);
                    } else {
                        clusterService.forgetParameters(prepared.statementId());
                    }
                }
                ByteString handle;
                if (values != null) {
                    handle = QueryParameters.boundHandle(sql, values);
                } else if (prepared.parameters() != null) {
                    // A handle that carries values, sent with none: the statement without them.
                    handle = QueryParameters.statementHandle(sql);
                } else {
                    handle = command.getPreparedStatementHandle();
                }
                byte[] result = FlightSql.DoPutPreparedStatementResult.newBuilder()
                        .setPreparedStatementHandle(handle).build().toByteArray();
                try (org.apache.arrow.memory.ArrowBuf buffer = allocator.buffer(result.length)) {
                    buffer.writeBytes(result);
                    ackStream.onNext(org.apache.arrow.flight.PutResult.metadata(buffer));
                }
                ackStream.onCompleted();
            } catch (FlightRuntimeException e) {
                ackStream.onError(e);
            } catch (Exception e) {
                ackStream.onError(CallStatus.INVALID_ARGUMENT.withDescription(failureDescription(e))
                        .withCause(e).toRuntimeException());
            }
        };
    }

    @Override
    public FlightInfo getFlightInfoPreparedStatement(
            FlightSql.CommandPreparedStatementQuery command,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        QueryParameters.Handle handle = parseHandle(command.getPreparedStatementHandle());
        byte[] parameters = handle.parameters();
        if (parameters == null && handle.statementId() != null) {
            parameters = clusterService.recallParameters(handle.statementId());
        }
        authorization.requireQuery(context == null ? null : context.peerIdentity(), handle.sql());
        return flightInfoForQuery(handle.sql(), parameters, descriptor, context);
    }

    /**
     * Reads a prepared statement handle sent by a client.
     *
     * @param handle statement handle
     * @return SQL and values
     * @throws FlightRuntimeException INVALID_ARGUMENT for a malformed handle
     */
    private static QueryParameters.Handle parseHandle(ByteString handle) {
        try {
            return QueryParameters.parseHandle(handle);
        } catch (IllegalArgumentException e) {
            throw CallStatus.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e).toRuntimeException();
        }
    }

    @Override
    public SchemaResult getSchemaPreparedStatement(
            FlightSql.CommandPreparedStatementQuery command,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        String query = parseHandle(command.getPreparedStatementHandle()).sql();
        authorization.requireQuery(context == null ? null : context.peerIdentity(), query);
        return new SchemaResult(completeAnswer(context) ? completeSchemaFor(query) : schemaForQuery(query));
    }

    @Override
    public SchemaResult getSchemaStatement(FlightSql.CommandStatementQuery command,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        String query = command.getQuery();
        authorization.requireQuery(context == null ? null : context.peerIdentity(), query);
        LOGGER.debug("getSchemaStatement: {}", query);
        return new SchemaResult(completeAnswer(context) ? completeSchemaFor(query) : schemaForQuery(query));
    }

    @Override
    protected <T extends Message> List<FlightEndpoint> determineEndpoints(
            T request, FlightDescriptor descriptor, Schema schema) {
        long t = LogUtil.mark();
        try {
            if (request instanceof FlightSql.TicketStatementQuery ticketQuery) {
                long tGet = LogUtil.mark();
                final ByteString handle = ticketQuery.getStatementHandle();
                HandleState state = clusterService.getHandle(handle.toStringUtf8());
                LogUtil.logTiming(tGet, "planning.getHandle");
                if (state == null) {
                    throw CallStatus.NOT_FOUND
                            .withDescription("No handle state found")
                            .toRuntimeException();
                }
                String query = state.query();
                List<FlightEndpoint> endpoints = queryPlanner.determineEndpoints(query);
                LogUtil.logTiming(t, "planning.determineEndpoints", "endpoints=" + endpoints.size());
                return endpoints;
            } else {
                // Without a location the client reads metadata over the connection it already
                // has, whatever name or address it reached this server by; a stock JDBC driver
                // opens a new connection (and TLS handshake) for every advertised location.
                Ticket ticket = new Ticket(Any.pack(request).toByteArray());
                return List.of(metadataLocation == null
                        ? new FlightEndpoint(ticket)
                        : new FlightEndpoint(ticket, metadataLocation));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // ── Metadata handlers ────────────────────────────────────────────────

    @Override
    public FlightInfo getFlightInfoSqlInfo(FlightSql.CommandGetSqlInfo request,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor,
                org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA);
    }

    @Override
    public void getStreamSqlInfo(FlightSql.CommandGetSqlInfo command,
            FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        // Clients ask for ids this server has no value for (drivers probe many at connect);
        // those are left out of the answer instead of failing the whole call.
        List<Integer> requested = command.getInfoList();
        List<Integer> known = requested.stream().filter(knownSqlInfo::contains).toList();
        if (!requested.isEmpty() && known.isEmpty()) {
            sendEmpty(org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA,
                    listener);
            return;
        }
        this.sqlInfoBuilder.send(known, listener);
    }

    @Override
    public FlightInfo getFlightInfoCatalogs(FlightSql.CommandGetCatalogs request,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor,
                org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA);
    }

    @Override
    public void getStreamCatalogs(FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        try (VectorSchemaRoot root = metadataService.getCatalogsRoot(allocator)) {
            listener.start(root);
            listener.putNext();
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoSchemas(FlightSql.CommandGetDbSchemas request,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor,
                org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA);
    }

    @Override
    public void getStreamSchemas(FlightSql.CommandGetDbSchemas command,
            FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        final String catalog = command.hasCatalog() ? command.getCatalog() : null;
        final String schemaFilterPattern = command.hasDbSchemaFilterPattern()
                ? command.getDbSchemaFilterPattern() : null;
        boolean errored = false;

        try {
            Map<String, String> schemas =
                    metadataService.getSchemas(schemaFilterPattern);
            try (VectorSchemaRoot root = metadataService.getSchemasRoot(
                    schemas.keySet().stream()
                            .filter(name -> authorization.canSeeSchema(
                                    context == null ? null : context.peerIdentity(), name))
                            .toList(), allocator)) {
                listener.start(root);
                listener.putNext();
            }
        } catch (IOException e) {
            LOGGER.error("Failed to getStreamSchemas", e);
            listener.error(e);
            errored = true;
        } finally {
            if (!errored) {
                listener.completed();
            }
        }
    }

    @Override
    public FlightInfo getFlightInfoTableTypes(FlightSql.CommandGetTableTypes request,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor,
                org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_TABLE_TYPES_SCHEMA);
    }

    @Override
    public void getStreamTableTypes(FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        try (VectorSchemaRoot root = metadataService.getTableTypesRoot(allocator)) {
            listener.start(root);
            listener.putNext();
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoTables(FlightSql.CommandGetTables request,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        Schema schemaToUse = org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_TABLES_SCHEMA;
        if (!request.getIncludeSchema()) {
            schemaToUse = org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA;
        }
        return getFlightInfoForSchema(request, descriptor, schemaToUse);
    }

    @Override
    @SuppressWarnings("java:S3776")
    public void getStreamTables(FlightSql.CommandGetTables command,
            FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        final String catalog = command.hasCatalog() ? command.getCatalog() : null;
        final String schemaFilterPattern = command.hasDbSchemaFilterPattern()
                ? command.getDbSchemaFilterPattern() : null;
        final String tableFilterPattern = command.hasTableNameFilterPattern()
                ? command.getTableNameFilterPattern() : null;

        // JDBC tools ask for several table types at once ("TABLE", "VIEW", ...) and for
        // catalogs they made up; a request that cannot match anything gets an empty answer.
        final ProtocolStringList tableTypesList = command.getTableTypesList();
        boolean canMatch = (catalog == null || catalog.isEmpty()
                        || MetadataService.CATALOG_NAME.equalsIgnoreCase(catalog))
                && (tableTypesList.isEmpty() || tableTypesList.stream().anyMatch(
                        MetadataService.TABLE_TYPE::equalsIgnoreCase));

        Map<String, Map<String, Schema>> tables = new java.util.LinkedHashMap<>();
        try {
            for (Map.Entry<String, String> schemaEntry : !canMatch
                    ? java.util.Set.<Map.Entry<String, String>>of()
                    : metadataService.getSchemas(schemaFilterPattern).entrySet()) {
                if (!authorization.canSeeSchema(
                        context == null ? null : context.peerIdentity(), schemaEntry.getKey())) {
                    continue;
                }
                Map<String, Schema> tablesForSchema = new java.util.LinkedHashMap<>();
                tables.put(schemaEntry.getKey(), tablesForSchema);
                for (Map.Entry<String, String> tableEntry :
                        metadataService.getTables(schemaEntry.getKey(), tableFilterPattern)
                                .entrySet()) {
                    if (!authorization.canReadTable(
                            context == null ? null : context.peerIdentity(),
                            schemaEntry.getKey(), tableEntry.getKey())) {
                        continue;
                    }
                    Schema schema = command.getIncludeSchema()
                            ? tableSchema(schemaEntry.getKey(), tableEntry.getKey())
                            : null;
                    tablesForSchema.put(tableEntry.getKey(), schema);
                }
            }
        } catch (IOException e) {
            LOGGER.error("Failed to getStreamTables", e);
            listener.error(e);
            return;
        } catch (java.io.UncheckedIOException e) {
            // A catalog (Hive Metastore, Iceberg REST) cannot be read: say so, never list less.
            LOGGER.warn("node={} getTables=catalogUnavailable error='{}'", LogUtil.node(),
                    e.getMessage());
            listener.error(CallStatus.UNAVAILABLE.withDescription(e.getMessage()).withCause(e)
                    .toRuntimeException());
            return;
        }

        boolean errored = false;
        try (VectorSchemaRoot root = metadataService.getTablesRoot(
                tables, allocator, command.getIncludeSchema(),
                schemaFilterPattern, tableFilterPattern)) {
            listener.start(root);
            listener.putNext();
        } catch (Exception e) {
            LOGGER.error("Failed to getStreamTables", e);
            listener.error(e);
            errored = true;
        } finally {
            if (!errored) {
                listener.completed();
            }
        }
    }

    @Override
    public FlightInfo getFlightInfoTypeInfo(FlightSql.CommandGetXdbcTypeInfo request,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor,
                org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_TYPE_INFO_SCHEMA);
    }

    @Override
    public void getStreamTypeInfo(FlightSql.CommandGetXdbcTypeInfo request,
            FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        try (VectorSchemaRoot root = metadataService.getTypeInfoRoot(request, allocator)) {
            listener.start(root);
            listener.putNext();
            listener.completed();
        }
    }

    /**
     * Describes a table the way a query of it returns its columns. The Parquet footer schema lists
     * nested columns as their leaves ({@code s.a}, {@code l.list.element}), which no query can
     * select; clients that build queries from the column list would fail on them.
     *
     * @param schema schema name
     * @param table table name
     * @return column schema of {@code SELECT *} from the table
     */
    private Schema tableSchema(String schema, String table) {
        String selectAll = "SELECT * FROM " + DuckDbAdapter.quoteIdentifier(schema) + "."
                + DuckDbAdapter.quoteIdentifier(table);
        try {
            // Metadata calls carry no mode of their own: describe the columns the way the
            // default mode returns them (both modes widen unsigned integers alike).
            return "complete".equalsIgnoreCase(defaultResultMode)
                    ? executionService.completeSchema(allocator, selectAll)
                    : executionService.resultSchema(allocator, selectAll);
        } catch (RuntimeException e) {
            LOGGER.debug("Table {}.{} described from its Parquet footer: {}",
                    schema, table, e.getMessage());
            return executionService.footerTableSchema(schema, table);
        }
    }

    // The tables have no keys; clients that ask (JDBC metadata, BI tools) get empty answers.

    @Override
    public void getStreamPrimaryKeys(FlightSql.CommandGetPrimaryKeys command,
            FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        sendEmpty(org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_PRIMARY_KEYS_SCHEMA,
                listener);
    }

    @Override
    public void getStreamExportedKeys(FlightSql.CommandGetExportedKeys command,
            FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        sendEmpty(org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_EXPORTED_KEYS_SCHEMA,
                listener);
    }

    @Override
    public void getStreamImportedKeys(FlightSql.CommandGetImportedKeys command,
            FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        sendEmpty(org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_IMPORTED_KEYS_SCHEMA,
                listener);
    }

    @Override
    public void getStreamCrossReference(FlightSql.CommandGetCrossReference command,
            FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        sendEmpty(org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_CROSS_REFERENCE_SCHEMA,
                listener);
    }

    /**
     * Sends a result that has a schema and no rows.
     *
     * @param schema result schema
     * @param listener Flight stream listener
     */
    private void sendEmpty(Schema schema, FlightProducer.ServerStreamListener listener) {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            listener.start(root);
            listener.completed();
        }
    }

    @Override
    public void close() throws Exception {
        // Dependencies and allocator are owned and closed by the server bootstrap.
    }

    /**
     * Makes a short query identifier for log lines.
     *
     * @param handle ticket handle as ByteString
     * @return the first 8 characters of a text handle, or 8 hex digits of a binary one
     */
    static String qid(ByteString handle) {
        String raw = handle.toStringUtf8();
        String head = raw.length() >= 8 ? raw.substring(0, 8) : raw;
        if (head.chars().allMatch(c -> c == '-' || Character.isLetterOrDigit(c) && c < 128)) {
            return head;
        }
        return String.format("%08x", java.util.Arrays.hashCode(handle.toByteArray()));
    }

    /**
     * @param request protobuf request message
     * @param descriptor flight descriptor
     * @param schema Arrow schema for the response
     * @return FlightInfo with determined endpoints
     */
    private <T extends Message> FlightInfo getFlightInfoForSchema(
            T request, FlightDescriptor descriptor, Schema schema) {
        final List<FlightEndpoint> endpoints = determineEndpoints(request, descriptor, schema);
        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }
}
