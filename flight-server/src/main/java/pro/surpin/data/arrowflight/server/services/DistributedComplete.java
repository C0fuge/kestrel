package pro.surpin.data.arrowflight.server.services;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.server.model.HandleState;
import pro.surpin.data.arrowflight.server.model.QueryPlan;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Computes a complete answer in a cluster from the answers of its partial-mode endpoints (item
 * 18, distributed complete mode, opt-in {@code distributedComplete}): the planning node plans the
 * query in parts, reads every endpoint from its node with a Flight DoGet as the client's user
 * ({@link InternalCallIdentity}), and merges the endpoint answers in its own DuckDB
 * ({@link CompleteAnswerMerge}). An endpoint answer that fails before its schema is read from the
 * endpoint's backup locations in order. A query with plain averages is planned as its
 * {@link AverageMerge} part query (sums and counts) and the averages are divided out in the merge.
 * Queries whose shape does not merge, plans of one endpoint,
 * and failures before the first batch reaches the client keep today's path (this node computes the
 * answer alone); a failure after the first batch fails the query: with {@code UNAVAILABLE} when an
 * endpoint answer was lost (the peer's stream ended with {@code UNAVAILABLE} or a transport error),
 * so that clients retry and the retry plans without the lost member, else as the merge failed.
 */
public final class DistributedComplete implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedComplete.class);
    private static final AtomicLong NEXT = new AtomicLong();
    private static final String VIEW_PREFIX = "__kestrel_endpoint_answers_";
    /** Start of the {@code UNAVAILABLE} description when an endpoint answer was lost. */
    static final String PEER_LOST = "peer lost during distributed complete answer; retry";

    private final QueryPlanner planner;
    private final ClusterService cluster;
    private final ExecutionService execution;
    private final BufferAllocator allocator;
    private final Function<Location, FlightClient> clientFactory;
    private final Map<URI, FlightClient> clients = new ConcurrentHashMap<>();
    private final AtomicLong merged = new AtomicLong();
    private final AtomicLong fallbacks = new AtomicLong();
    private final AtomicLong peerBytes = new AtomicLong();
    private final AtomicLong peerWaitNanos = new AtomicLong();
    private final AtomicLong failovers = new AtomicLong();
    private final AtomicLong peersLost = new AtomicLong();

    /**
     * Creates the distributed path of complete answers.
     *
     * @param planner plans the endpoints of a query
     * @param cluster signs the caller identity and releases endpoint loads
     * @param execution merges the endpoint answers in DuckDB
     * @param allocator allocator of the peer streams
     * @param clientFactory opens a Flight client to a peer location
     */
    public DistributedComplete(QueryPlanner planner, ClusterService cluster, ExecutionService execution,
            BufferAllocator allocator, Function<Location, FlightClient> clientFactory) {
        this.planner = Objects.requireNonNull(planner, "planner");
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.execution = Objects.requireNonNull(execution, "execution");
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.clientFactory = Objects.requireNonNull(clientFactory, "clientFactory");
    }

    /**
     * Creates the factory of peer clients with the server's transport settings: TLS locations
     * trust the given certificates, else the JVM's trust store.
     *
     * @param allocator allocator of the clients
     * @param trustedCertificates PEM file of the certificates peers present, or null
     * @return client factory
     */
    public static Function<Location, FlightClient> clientFactory(BufferAllocator allocator,
            File trustedCertificates) {
        return location -> {
            FlightClient.Builder builder = FlightClient.builder(allocator, location);
            if (trustedCertificates != null && "grpc+tls".equals(location.getUri().getScheme())) {
                try (InputStream certificates = Files.newInputStream(trustedCertificates.toPath())) {
                    builder.trustedCertificates(certificates);
                    return builder.build();
                } catch (IOException e) {
                    throw new IllegalStateException("Cannot read the trusted certificates "
                            + trustedCertificates, e);
                }
            }
            return builder.build();
        };
    }

    /**
     * Streams the complete answer of a query merged from its endpoints' answers, if it can.
     *
     * @param query client SQL
     * @param user the client's identity on this node
     * @param listener the client's stream
     * @return true when the answer was streamed (the caller completes the listener); false when
     *         nothing was sent and the caller computes the answer alone
     * @throws Exception when the merge failed after the first batch was sent ({@code UNAVAILABLE}
     *         when an endpoint answer was lost), or the client cancelled
     */
    public boolean stream(String query, String user, FlightProducer.ServerStreamListener listener)
            throws Exception {
        ParquetQueryParser parsed = mergeable(query);
        AverageMerge averages = null;
        String partQuery = query;
        if (parsed == null) {
            averages = AverageMerge.of(query);
            if (averages == null) {
                return false;
            }
            partQuery = averages.partQuery();
            parsed = mergeable(partQuery);
            if (parsed == null) {
                return false;
            }
        }
        QueryPlan plan;
        try {
            plan = planner.plan(partQuery);
        } catch (IOException | RuntimeException e) {
            LOGGER.info("node={} distributedComplete=fallback reason='planning: {}' query='{}'",
                    LogUtil.node(), e.getMessage(), query);
            fallbacks.incrementAndGet();
            return false;
        }
        List<FlightEndpoint> endpoints = plan.endpoints();
        if (endpoints.size() < 2) {
            releaseLoads(endpoints);
            return false;
        }
        long start = System.nanoTime();
        DeferredStart client = new DeferredStart(listener);
        List<FlightStream> streams = new ArrayList<>(endpoints.size());
        EndpointAnswers answers = null;
        try {
            CallOption identity = identityOption(user);
            for (FlightEndpoint endpoint : endpoints) {
                streams.add(client(endpoint, 0).getStream(endpoint.getTicket(), identity));
            }
            failOver(endpoints, streams, identity);
            Schema schema = commonSchema(streams);
            // The answer the client was promised: names and types of the single-node path.
            Schema expected = execution.completeSchema(allocator, query);
            client.expect(expected);
            List<String> columns = schema.getFields().stream().map(Field::getName).toList();
            String name = VIEW_PREFIX + NEXT.incrementAndGet();
            List<String> outputNames = expected.getFields().stream().map(Field::getName).toList();
            String sql = averages == null
                    ? CompleteAnswerMerge.mergeSql(parsed, name, columns, outputNames)
                    : averages.mergeSql(parsed, name, columns, sumScales(schema), outputNames);
            answers = new EndpointAnswers(allocator, streams, schema);
            try {
                execution.readArrowParts(allocator, answers, name, sql, client);
            } finally {
                peerBytes.addAndGet(answers.bytesRead());
                peerWaitNanos.addAndGet(answers.waitNanos());
                answers.close();
            }
            client.flushStart();
            merged.incrementAndGet();
            LOGGER.info("node={} distributedComplete=merged endpoints={} elapsedMs={} peerBytes={}"
                    + " peerWaitMs={} query='{}'", LogUtil.node(), endpoints.size(),
                    (System.nanoTime() - start) / 1_000_000, answers.bytesRead(),
                    answers.waitNanos() / 1_000_000, query);
            return true;
        } catch (Exception e) {
            cancel(streams);
            if (listener.isCancelled()) {
                throw e;
            }
            if (client.sentBatch()) {
                Throwable peerFailure = answers == null ? null : answers.failure();
                if (peerLost(peerFailure)) {
                    peersLost.incrementAndGet();
                    LOGGER.warn("node={} distributedComplete=peerLost reason='{}' endpoints={} query='{}'",
                            LogUtil.node(), peerFailure.getMessage(), endpoints.size(), query);
                    throw CallStatus.UNAVAILABLE.withDescription(PEER_LOST + ": " + peerFailure.getMessage())
                            .withCause(e).toRuntimeException();
                }
                throw e;
            }
            LOGGER.warn("node={} distributedComplete=fallback reason='{}' endpoints={} query='{}'",
                    LogUtil.node(), e.getMessage(), endpoints.size(), query);
            releaseLoads(endpoints);
            fallbacks.incrementAndGet();
            return false;
        } finally {
            closeAll(streams);
        }
    }

    /**
     * Reads the scale of each endpoint answer column for {@link AverageMerge#average}.
     *
     * @param schema schema of the endpoint answers
     * @return per column, the decimal scale, 0 for an integer, null for any other type
     */
    static List<Integer> sumScales(Schema schema) {
        List<Integer> scales = new ArrayList<>(schema.getFields().size());
        for (Field field : schema.getFields()) {
            ArrowType type = field.getType();
            if (type instanceof ArrowType.Decimal decimal) {
                scales.add(decimal.getScale());
            } else if (type instanceof ArrowType.Int) {
                scales.add(0);
            } else {
                scales.add(null);
            }
        }
        return scales;
    }

    /**
     * Counts the complete answers merged from endpoint answers since start.
     *
     * @return merged answers
     */
    public long mergedAnswers() {
        return merged.get();
    }

    /**
     * Counts the answers that failed after their first batch because an endpoint answer was lost
     * (the client got {@code UNAVAILABLE}).
     *
     * @return answers failed as retryable
     */
    public long peersLost() {
        return peersLost.get();
    }

    /**
     * Tells whether an endpoint answer's failure means its peer was lost: the stream ended with
     * {@code UNAVAILABLE}, or a transport error ({@link IOException}) is among its causes.
     *
     * @param failure failure of an endpoint answer's stream; null when none failed
     * @return true when the query may be retried without the lost member
     */
    static boolean peerLost(Throwable failure) {
        if (failure instanceof FlightRuntimeException flight
                && flight.status().code() == FlightStatusCode.UNAVAILABLE) {
            return true;
        }
        for (Throwable t = failure; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof IOException) {
                return true;
            }
        }
        return false;
    }

    /**
     * Counts the mergeable queries that fell back to the single-node path since start.
     *
     * @return fallbacks
     */
    public long fallbacks() {
        return fallbacks.get();
    }

    /**
     * Counts the endpoint answers read from a backup location since start.
     *
     * @return failovers
     */
    public long failovers() {
        return failovers.get();
    }

    /**
     * Counts the Arrow body bytes read from endpoint answers since start.
     *
     * @return bytes
     */
    public long peerBytes() {
        return peerBytes.get();
    }

    /**
     * Sums the time the merges waited for the next batch of an endpoint answer since start.
     *
     * @return nanoseconds
     */
    public long peerWaitNanos() {
        return peerWaitNanos.get();
    }

    /**
     * Parses a query whose complete answer can be merged from its endpoint answers.
     *
     * @param query client SQL
     * @return parsed query, or null when the query keeps the single-node path
     */
    static ParquetQueryParser mergeable(String query) {
        if (TimeTravelClause.present(query)) {
            return null;
        }
        ParquetQueryParser parsed;
        try {
            parsed = ParquetQueryParser.parse(query);
        } catch (RuntimeException e) {
            return null;
        }
        String refusal = CompleteAnswerMerge.refusal(parsed);
        if (refusal != null) {
            LOGGER.debug("node={} distributedComplete=skipped reason='{}'", LogUtil.node(), refusal);
            return null;
        }
        return parsed;
    }

    /**
     * Builds the call option carrying the signed identity of the client.
     *
     * @param user the client's identity
     * @return call option
     */
    private CallOption identityOption(String user) {
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert(InternalCallIdentity.HEADER,
                cluster.internalCallIdentity().sign(user, System.currentTimeMillis()));
        return new HeaderCallOption(headers);
    }

    /**
     * Returns the client of one of an endpoint's locations (the first is the node it was planned
     * for, the others its backups).
     *
     * @param endpoint endpoint
     * @param index location index
     * @return Flight client
     * @throws IllegalStateException when the endpoint has no location
     */
    private FlightClient client(FlightEndpoint endpoint, int index) {
        if (endpoint.getLocations().isEmpty()) {
            throw new IllegalStateException("An endpoint has no location");
        }
        Location location = endpoint.getLocations().get(index);
        return clients.computeIfAbsent(location.getUri(), uri -> clientFactory.apply(location));
    }

    /**
     * Waits for the schema of every endpoint answer; an answer that fails before its schema is
     * read again from the endpoint's next location, as a client fails over to a backup location.
     *
     * @param endpoints planned endpoints
     * @param streams their streams, in the same order; failed ones are replaced
     * @param identity the client's signed identity
     * @throws FlightRuntimeException when an endpoint failed at all its locations
     */
    private void failOver(List<FlightEndpoint> endpoints, List<FlightStream> streams, CallOption identity) {
        for (int i = 0; i < endpoints.size(); i++) {
            FlightEndpoint endpoint = endpoints.get(i);
            for (int location = 0; ; location++) {
                try {
                    streams.get(i).getSchema();
                    break;
                } catch (FlightRuntimeException e) {
                    if (location + 1 >= endpoint.getLocations().size()) {
                        throw e;
                    }
                    LOGGER.info("node={} distributedComplete=failover from={} to={} reason='{}'", LogUtil.node(),
                            endpoint.getLocations().get(location).getUri(),
                            endpoint.getLocations().get(location + 1).getUri(), e.getMessage());
                    closeAll(List.of(streams.get(i)));
                    streams.set(i, client(endpoint, location + 1).getStream(endpoint.getTicket(), identity));
                    failovers.incrementAndGet();
                }
            }
        }
    }

    /**
     * Waits for the schema of every endpoint answer and checks that they agree.
     *
     * @param streams endpoint streams
     * @return their schema
     * @throws IllegalStateException when two endpoints answer with different columns, or an
     *         answer uses dictionaries
     */
    static Schema commonSchema(List<FlightStream> streams) {
        Schema first = null;
        for (FlightStream stream : streams) {
            Schema schema = stream.getSchema();
            if (schema.getFields().stream().anyMatch(f -> f.getDictionary() != null)) {
                throw new IllegalStateException("An endpoint answer uses dictionary encoding");
            }
            if (first == null) {
                first = schema;
            } else if (!first.getFields().equals(schema.getFields())) {
                throw new IllegalStateException("Endpoint answers have different columns: "
                        + first + " and " + schema);
            }
        }
        if (first == null) {
            throw new IllegalStateException("No endpoint answers");
        }
        return first;
    }

    /**
     * Releases the load reservations of planned endpoints that will not run (each is released
     * once, whoever releases it first).
     *
     * @param endpoints planned endpoints
     */
    private void releaseLoads(List<FlightEndpoint> endpoints) {
        for (FlightEndpoint endpoint : endpoints) {
            try {
                byte[] handle = Any.parseFrom(endpoint.getTicket().getBytes())
                        .unpack(FlightSql.TicketStatementQuery.class).getStatementHandle().toByteArray();
                HandleState state = cluster.resolveEndpointHandle(handle);
                if (state != null) {
                    cluster.releaseEndpointLoad(handle, state);
                }
            } catch (InvalidProtocolBufferException | RuntimeException e) {
                LOGGER.debug("node={} distributedComplete=releaseFailed error='{}'", LogUtil.node(),
                        e.getMessage());
            }
        }
    }

    /**
     * Cancels the endpoint streams that are still open.
     *
     * @param streams endpoint streams
     */
    private static void cancel(List<FlightStream> streams) {
        for (FlightStream stream : streams) {
            try {
                stream.cancel("The complete answer is not merged from endpoint answers", null);
            } catch (RuntimeException e) {
                LOGGER.debug("node={} distributedComplete=cancelFailed error='{}'", LogUtil.node(),
                        e.getMessage());
            }
        }
    }

    /**
     * Closes the endpoint streams.
     *
     * @param streams endpoint streams
     */
    private static void closeAll(List<FlightStream> streams) {
        for (FlightStream stream : streams) {
            try {
                stream.close();
            } catch (Exception e) {
                LOGGER.debug("node={} distributedComplete=closeFailed error='{}'", LogUtil.node(),
                        e.getMessage());
            }
        }
    }

    /**
     * Closes the peer clients.
     *
     * @throws InterruptedException when interrupted while a client shuts down
     */
    @Override
    public void close() throws InterruptedException {
        for (FlightClient client : clients.values()) {
            client.close();
        }
        clients.clear();
    }

    /**
     * Reads the batches of all endpoint answers one stream after the other, as one Arrow stream
     * DuckDB can scan. The streams themselves are closed by {@link DistributedComplete}.
     */
    static final class EndpointAnswers extends ArrowReader {

        private final List<FlightStream> streams;
        private final Schema schema;
        private int current;
        /** Read by the caller after DuckDB's scan thread wrote them. */
        private final AtomicLong bytes = new AtomicLong();
        private final AtomicLong waitNanos = new AtomicLong();
        /** First failure of an endpoint stream; written by DuckDB's scan thread. */
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        /**
         * Creates the reader over endpoint streams of one schema.
         *
         * @param allocator allocator of the reader's batch
         * @param streams endpoint streams
         * @param schema their common schema
         */
        EndpointAnswers(BufferAllocator allocator, List<FlightStream> streams, Schema schema) {
            super(allocator);
            this.streams = List.copyOf(streams);
            this.schema = schema;
        }

        @Override
        public boolean loadNextBatch() throws IOException {
            prepareLoadNextBatch();
            while (current < streams.size()) {
                FlightStream stream = streams.get(current);
                long waitStart = System.nanoTime();
                boolean next;
                try {
                    next = stream.next();
                } catch (RuntimeException e) {
                    failure.compareAndSet(null, e);
                    throw e;
                }
                waitNanos.addAndGet(System.nanoTime() - waitStart);
                if (next) {
                    try (ArrowRecordBatch batch = new VectorUnloader(stream.getRoot()).getRecordBatch()) {
                        bytes.addAndGet(batch.computeBodyLength());
                        loadRecordBatch(batch);
                    }
                    return true;
                }
                current++;
            }
            return false;
        }

        @Override
        public long bytesRead() {
            return bytes.get();
        }

        /**
         * Sums the time spent waiting for the next batch of an endpoint answer.
         *
         * @return nanoseconds
         */
        long waitNanos() {
            return waitNanos.get();
        }

        /**
         * Returns the first failure of an endpoint stream read through this reader.
         *
         * @return the failure, or null when none failed
         */
        Throwable failure() {
            return failure.get();
        }

        @Override
        protected void closeReadSource() {
            // The streams belong to the caller.
        }

        @Override
        protected Schema readSchema() {
            return schema;
        }
    }

    /**
     * Holds back the start of the client's stream until its first batch, so that a failure
     * before it leaves the stream untouched and the single-node path can still answer.
     */
    static final class DeferredStart implements FlightProducer.ServerStreamListener {

        private final FlightProducer.ServerStreamListener delegate;
        private Schema expected;
        private VectorSchemaRoot root;
        private DictionaryProvider dictionaries;
        private IpcOption option;
        private boolean started;
        private boolean sent;

        /**
         * Wraps the client's stream.
         *
         * @param delegate the client's stream
         */
        DeferredStart(FlightProducer.ServerStreamListener delegate) {
            this.delegate = delegate;
        }

        /**
         * Sets the schema the stream must start with.
         *
         * @param schema schema of the single-node answer
         */
        void expect(Schema schema) {
            this.expected = schema;
        }

        /**
         * Tells whether a batch, an error or the end reached the client.
         *
         * @return true once the client's stream was used
         */
        boolean sentBatch() {
            return sent;
        }

        /** Starts the client's stream if no batch started it (an empty answer). */
        void flushStart() {
            if (!started && root != null) {
                started = true;
                delegate.start(root, dictionaries, option);
            }
        }

        @Override
        public boolean isCancelled() {
            return delegate.isCancelled();
        }

        @Override
        public void setOnCancelHandler(Runnable handler) {
            delegate.setOnCancelHandler(handler);
        }

        @Override
        public boolean isReady() {
            return delegate.isReady();
        }

        @Override
        public void setOnReadyHandler(Runnable handler) {
            delegate.setOnReadyHandler(handler);
        }

        @Override
        public void setUseZeroCopy(boolean enabled) {
            delegate.setUseZeroCopy(enabled);
        }

        @Override
        public void start(VectorSchemaRoot root, DictionaryProvider dictionaries, IpcOption option) {
            if (expected != null && !sameColumns(expected.getFields(), root.getSchema().getFields())) {
                throw new IllegalStateException("The merged answer has the columns " + root.getSchema()
                        + ", the complete answer " + expected);
            }
            this.root = root;
            this.dictionaries = dictionaries;
            this.option = option;
        }

        @Override
        public void putNext() {
            beforeSend();
            delegate.putNext();
        }

        @Override
        public void putNext(ArrowBuf metadata) {
            beforeSend();
            delegate.putNext(metadata);
        }

        @Override
        public void putMetadata(ArrowBuf metadata) {
            beforeSend();
            delegate.putMetadata(metadata);
        }

        @Override
        public void error(Throwable cause) {
            sent = true;
            delegate.error(cause);
        }

        @Override
        public void completed() {
            beforeSend();
            delegate.completed();
        }

        /**
         * Compares column names and types, nested ones included, ignoring nullability and
         * metadata (DuckDB marks merged columns nullable where the describe may not).
         *
         * @param a columns
         * @param b other columns
         * @return true when both have the same names and types in the same order
         */
        static boolean sameColumns(List<Field> a, List<Field> b) {
            if (a.size() != b.size()) {
                return false;
            }
            for (int i = 0; i < a.size(); i++) {
                Field x = a.get(i);
                Field y = b.get(i);
                if (!x.getName().equals(y.getName()) || !x.getType().equals(y.getType())
                        || !sameColumns(x.getChildren(), y.getChildren())) {
                    return false;
                }
            }
            return true;
        }

        /** Marks the stream as used and starts it first if needed. */
        private void beforeSend() {
            sent = true;
            flushStart();
        }
    }
}
