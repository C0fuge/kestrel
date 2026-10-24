package pro.surpin.data.arrowflight.server.adapters;

import com.google.protobuf.ByteString;
import pro.surpin.data.arrowflight.server.model.HandleState;
import pro.surpin.data.arrowflight.server.FlightAuthorization;
import pro.surpin.data.arrowflight.server.model.ExecutionPathTracker;
import pro.surpin.data.arrowflight.server.model.QueryPlan;
import pro.surpin.data.arrowflight.server.services.ClusterService;
import pro.surpin.data.arrowflight.server.services.ExecutionService;
import pro.surpin.data.arrowflight.server.services.MetadataService;
import pro.surpin.data.arrowflight.server.services.QueryPlanner;
import pro.surpin.data.arrowflight.server.services.TableStatisticsService;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests terminal Flight stream status handling in the SQL producer. */
@Tag("unit")
class FlightSqlProducerTest {

    /** Blocks schema lookup before any table metadata is returned. */
    @Test
    void rangerDeniesStatementMetadata() throws Exception {
        MetadataService metadataService = mock(MetadataService.class);
        FlightAuthorization authorization = mock(FlightAuthorization.class);
        String query = "select * from sales.orders";
        doThrow(CallStatus.UNAUTHORIZED.toRuntimeException()).when(authorization)
                .requireQuery("alice@EXAMPLE.COM", query);
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        when(context.peerIdentity()).thenReturn("alice@EXAMPLE.COM");
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), mock(BufferAllocator.class),
                metadataService, mock(QueryPlanner.class), mock(ExecutionService.class),
                mock(ClusterService.class), authorization);
        FlightSql.CommandStatementQuery command = FlightSql.CommandStatementQuery
                .newBuilder().setQuery(query).build();
        assertEquals(FlightStatusCode.UNAUTHORIZED,
                org.junit.jupiter.api.Assertions.assertThrows(FlightRuntimeException.class,
                        () -> producer.getFlightInfoStatement(command, context,
                                FlightDescriptor.command(new byte[]{1}))).status().code());
        verify(metadataService, never()).getQuerySchema(anyString());
    }

    /** Rechecks access to a query when a saved endpoint ticket is streamed. */
    @Test
    void rangerDeniesSavedTicketStream() throws Exception {
        ByteString handle = ByteString.copyFromUtf8("saved-query");
        String query = "select * from sales.orders";
        ClusterService clusterService = mock(ClusterService.class);
        HandleState state = new HandleState(query, new String[]{"part.parquet"}, null, 0L);
        when(clusterService.resolveEndpointHandle(handle.toByteArray())).thenReturn(state);
        FlightAuthorization authorization = mock(FlightAuthorization.class);
        doThrow(CallStatus.UNAUTHORIZED.toRuntimeException()).when(authorization)
                .requireQuery("alice@EXAMPLE.COM", query);
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        when(context.peerIdentity()).thenReturn("alice@EXAMPLE.COM");
        ExecutionService executionService = mock(ExecutionService.class);
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), mock(BufferAllocator.class),
                mock(MetadataService.class), mock(QueryPlanner.class), executionService,
                clusterService, authorization);
        producer.getStreamStatement(FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(handle).build(), context, listener);
        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).error(error.capture());
        assertEquals(FlightStatusCode.UNAUTHORIZED,
                assertInstanceOf(FlightRuntimeException.class, error.getValue()).status().code());
        verify(executionService, never()).readParquet(any(), anyString(), any(), any(),
                anyBoolean(), any());
        verify(clusterService).releaseEndpointLoad(handle.toByteArray(), state);
    }

    /** Omits schemas denied by Ranger from metadata discovery. */
    @Test
    void rangerFiltersSchemaListing() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        MetadataService metadataService = mock(MetadataService.class);
        when(metadataService.getSchemas(null)).thenReturn(Map.of(
                "sales", "/sales",
                "private", "/private"));
        VectorSchemaRoot root = mock(VectorSchemaRoot.class);
        when(metadataService.getSchemasRoot(any(), eq(allocator))).thenReturn(root);
        FlightAuthorization authorization = mock(FlightAuthorization.class);
        when(authorization.canSeeSchema("alice@EXAMPLE.COM", "sales")).thenReturn(true);
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        when(context.peerIdentity()).thenReturn("alice@EXAMPLE.COM");
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                metadataService, mock(QueryPlanner.class), mock(ExecutionService.class),
                mock(ClusterService.class), authorization);
        producer.getStreamSchemas(FlightSql.CommandGetDbSchemas.getDefaultInstance(),
                context, listener);
        verify(metadataService).getSchemasRoot(eq(List.of("sales")), eq(allocator));
        verify(listener).completed();
    }

    /** Verifies a backpressure timeout is reported as an error, never as completion. */
    @Test
    void timeoutDoesNotCompleteTruncatedStream() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        MetadataService metadataService = mock(MetadataService.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        ExecutionService executionService = mock(ExecutionService.class);
        ClusterService clusterService = mock(ClusterService.class);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        ByteString handle = ByteString.copyFromUtf8("timeout-query");
        HandleState state = new HandleState(
                "select * from tpch.lineitem", new String[]{"part.parquet"}, null, 0L);
        when(clusterService.resolveEndpointHandle(handle.toByteArray())).thenReturn(state);

        FlightRuntimeException timeout = CallStatus.TIMED_OUT
                .withDescription("listener timeout")
                .toRuntimeException();
        doThrow(timeout).when(executionService).readParquet(
                eq(allocator), eq(state.query()), eq(state.filePaths()), eq(listener),
                anyBoolean(), any(ExecutionPathTracker.class));

        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                metadataService, queryPlanner, executionService, clusterService);
        FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery
                .newBuilder().setStatementHandle(handle).build();

        producer.getStreamStatement(ticket, context, listener);

        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).error(error.capture());
        FlightRuntimeException reported = assertInstanceOf(
                FlightRuntimeException.class, error.getValue());
        assertEquals(FlightStatusCode.TIMED_OUT, reported.status().code());
        verify(listener, never()).completed();
    }

    /** Verifies statement planning exports Parquet statistics through FlightInfo. */
    @Test
    void statementFlightInfoContainsQueryStatistics() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        MetadataService metadataService = mock(MetadataService.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        ExecutionService executionService = mock(ExecutionService.class);
        ClusterService clusterService = mock(ClusterService.class);
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        String query = "select * from tpch.lineitem";
        Schema schema = new Schema(List.of(Field.nullable("l_orderkey", new ArrowType.Int(64, true))));
        when(executionService.resultSchema(allocator, query)).thenReturn(schema);
        when(queryPlanner.plan(query)).thenReturn(
                new QueryPlan(List.of(), 4096L, 125L));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                metadataService, queryPlanner, executionService, clusterService);
        FlightSql.CommandStatementQuery command = FlightSql.CommandStatementQuery
                .newBuilder().setQuery(query).build();
        FlightDescriptor descriptor = FlightDescriptor.command(new byte[]{1});

        FlightInfo info = producer.getFlightInfoStatement(
                command, context, descriptor);

        assertEquals(4096L, info.getBytes());
        assertEquals(125L, info.getRecords());
        verify(clusterService, never()).storeHandle(anyString(), any());
    }

    /** Verifies a prepared statement is its SQL text, planned again on every execution. */
    @Test
    void preparedStatementHandleIsTheQuery() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        ExecutionService executionService = mock(ExecutionService.class);
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        @SuppressWarnings("unchecked")
        FlightProducer.StreamListener<org.apache.arrow.flight.Result> listener =
                mock(FlightProducer.StreamListener.class);
        String query = "select l_orderkey from tpch.lineitem limit 5";
        Schema schema = new Schema(List.of(Field.nullable("l_orderkey", new ArrowType.Int(64, true))));
        when(executionService.resultSchema(allocator, query)).thenReturn(schema);
        when(queryPlanner.plan(query)).thenReturn(new QueryPlan(List.of(), 10L, 5L));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                mock(MetadataService.class), queryPlanner, executionService,
                mock(ClusterService.class));

        producer.createPreparedStatement(
                FlightSql.ActionCreatePreparedStatementRequest.newBuilder().setQuery(query).build(),
                context, listener);

        ArgumentCaptor<org.apache.arrow.flight.Result> result =
                ArgumentCaptor.forClass(org.apache.arrow.flight.Result.class);
        verify(listener).onNext(result.capture());
        verify(listener).onCompleted();
        FlightSql.ActionCreatePreparedStatementResult prepared = com.google.protobuf.Any
                .parseFrom(result.getValue().getBody())
                .unpack(FlightSql.ActionCreatePreparedStatementResult.class);
        assertEquals(query, prepared.getPreparedStatementHandle().toStringUtf8());
        assertEquals(schema, Schema.deserializeMessage(
                prepared.getDatasetSchema().asReadOnlyByteBuffer()));

        FlightInfo info = producer.getFlightInfoPreparedStatement(
                FlightSql.CommandPreparedStatementQuery.newBuilder()
                        .setPreparedStatementHandle(prepared.getPreparedStatementHandle()).build(),
                context, FlightDescriptor.command(new byte[]{1}));

        assertEquals(5L, info.getRecords());
        verify(queryPlanner).plan(query);
    }

    /** Verifies metadata is read over the client's own connection, not a fixed address. */
    @Test
    void metadataEndpointsCarryNoLocation() {
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), mock(BufferAllocator.class),
                mock(MetadataService.class), mock(QueryPlanner.class),
                mock(ExecutionService.class), mock(ClusterService.class));

        FlightInfo info = producer.getFlightInfoCatalogs(
                FlightSql.CommandGetCatalogs.getDefaultInstance(),
                mock(FlightProducer.CallContext.class), FlightDescriptor.command(new byte[]{1}));

        assertEquals(1, info.getEndpoints().size());
        assertEquals(List.of(), info.getEndpoints().get(0).getLocations());
    }

    /** Verifies SQL the server cannot run is the client's error, not a server failure. */
    @Test
    void rejectedSqlIsInvalidArgument() {
        BufferAllocator allocator = mock(BufferAllocator.class);
        ExecutionService executionService = mock(ExecutionService.class);
        String query = "select nope from tpch.lineitem";
        when(executionService.resultSchema(allocator, query))
                .thenThrow(new IllegalArgumentException("Binder Error: column nope not found"));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                mock(MetadataService.class), mock(QueryPlanner.class),
                executionService, mock(ClusterService.class));

        FlightRuntimeException error = org.junit.jupiter.api.Assertions.assertThrows(
                FlightRuntimeException.class,
                () -> producer.getFlightInfoStatement(
                        FlightSql.CommandStatementQuery.newBuilder().setQuery(query).build(),
                        mock(FlightProducer.CallContext.class),
                        FlightDescriptor.command(new byte[]{1})));

        assertEquals(FlightStatusCode.INVALID_ARGUMENT, error.status().code());
    }

    /** Verifies key metadata requests are answered with an empty result. */
    @Test
    void keysAreEmpty() {
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        try (BufferAllocator allocator = new org.apache.arrow.memory.RootAllocator()) {
            FlightSqlProducer producer = new FlightSqlProducer(
                    Location.forGrpcInsecure("localhost", 32010), allocator,
                    mock(MetadataService.class), mock(QueryPlanner.class),
                    mock(ExecutionService.class), mock(ClusterService.class));

            producer.getStreamPrimaryKeys(FlightSql.CommandGetPrimaryKeys.getDefaultInstance(),
                    mock(FlightProducer.CallContext.class), listener);
        }

        verify(listener).start(any(org.apache.arrow.vector.VectorSchemaRoot.class));
        verify(listener).completed();
        verify(listener, never()).putNext();
    }

    /** Verifies table listings tolerate the extra table types and catalogs JDBC tools send. */
    @Test
    void tableListingToleratesForeignTypesAndCatalogs() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        MetadataService metadataService = mock(MetadataService.class);
        when(metadataService.getSchemas(any())).thenReturn(java.util.Map.of());
        when(metadataService.getTablesRoot(any(), any(), anyBoolean(), any(), any()))
                .thenReturn(mock(org.apache.arrow.vector.VectorSchemaRoot.class));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                metadataService, mock(QueryPlanner.class),
                mock(ExecutionService.class), mock(ClusterService.class));
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        FlightProducer.ServerStreamListener tablesAndViews =
                mock(FlightProducer.ServerStreamListener.class);
        FlightProducer.ServerStreamListener viewsOnly =
                mock(FlightProducer.ServerStreamListener.class);
        FlightProducer.ServerStreamListener otherCatalog =
                mock(FlightProducer.ServerStreamListener.class);

        producer.getStreamTables(FlightSql.CommandGetTables.newBuilder()
                .addTableTypes("TABLE").addTableTypes("VIEW").build(), context, tablesAndViews);
        verify(metadataService).getSchemas(any());
        producer.getStreamTables(FlightSql.CommandGetTables.newBuilder()
                .addTableTypes("VIEW").build(), context, viewsOnly);
        producer.getStreamTables(FlightSql.CommandGetTables.newBuilder()
                .setCatalog("postgres").build(), context, otherCatalog);

        verify(metadataService).getSchemas(any());
        for (FlightProducer.ServerStreamListener listener
                : List.of(tablesAndViews, viewsOnly, otherCatalog)) {
            verify(listener).completed();
            verify(listener, never()).error(any());
        }
    }

    /** Verifies SqlInfo ids without a value are left out instead of failing the call. */
    @Test
    void unknownSqlInfoIsLeftOut() {
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010),
                new org.apache.arrow.memory.RootAllocator(),
                mock(MetadataService.class), mock(QueryPlanner.class),
                mock(ExecutionService.class), mock(ClusterService.class));
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        FlightProducer.ServerStreamListener mixed = mock(FlightProducer.ServerStreamListener.class);
        FlightProducer.ServerStreamListener unknownOnly =
                mock(FlightProducer.ServerStreamListener.class);
        java.util.concurrent.atomic.AtomicInteger rows = new java.util.concurrent.atomic.AtomicInteger(-1);
        org.mockito.Mockito.doAnswer(invocation -> {
            rows.set(((org.apache.arrow.vector.VectorSchemaRoot) invocation.getArgument(0)).getRowCount());
            return null;
        }).when(mixed).start(any(org.apache.arrow.vector.VectorSchemaRoot.class));

        producer.getStreamSqlInfo(FlightSql.CommandGetSqlInfo.newBuilder()
                .addInfo(0).addInfo(504).addInfo(9999).build(), context, mixed);
        producer.getStreamSqlInfo(FlightSql.CommandGetSqlInfo.newBuilder()
                .addInfo(9999).build(), context, unknownOnly);

        assertEquals(2, rows.get());
        verify(mixed, never()).error(any());
        verify(unknownOnly).completed();
        verify(unknownOnly, never()).error(any());
    }

    /** Verifies the default complete mode and the client's request for parts. */
    @Test
    void resultModeComesFromHeaderOrDefault() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        ExecutionService executionService = mock(ExecutionService.class);
        String query = "select sum(o_totalprice) from tpch.orders";
        Schema schema = new Schema(List.of(Field.nullable("sum", new ArrowType.FloatingPoint(
                org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE))));
        when(executionService.completeSchema(allocator, query)).thenReturn(schema);
        when(executionService.resultSchema(allocator, query)).thenReturn(schema);
        when(queryPlanner.planComplete(query, null)).thenReturn(new QueryPlan(List.of(), -1L, -1L));
        when(queryPlanner.plan(query)).thenReturn(new QueryPlan(List.of(), 10L, 1L));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                mock(MetadataService.class), queryPlanner, executionService,
                mock(ClusterService.class), "complete");
        FlightSql.CommandStatementQuery command =
                FlightSql.CommandStatementQuery.newBuilder().setQuery(query).build();
        FlightProducer.CallContext plain = mock(FlightProducer.CallContext.class);
        FlightProducer.CallContext partial = mock(FlightProducer.CallContext.class);
        org.apache.arrow.flight.FlightCallHeaders headers = new org.apache.arrow.flight.FlightCallHeaders();
        headers.insert(ResultModeMiddleware.HEADER, "partial");
        when(partial.getMiddleware(ResultModeMiddleware.KEY))
                .thenReturn(new ResultModeMiddleware.Factory().onCallStarted(null, headers, null));

        producer.getFlightInfoStatement(command, plain, FlightDescriptor.command(new byte[]{1}));
        verify(queryPlanner).planComplete(query, null);
        verify(queryPlanner, never()).plan(query);

        producer.getFlightInfoStatement(command, partial, FlightDescriptor.command(new byte[]{1}));
        verify(queryPlanner).plan(query);
    }

    /**
     * Verifies a query asked for in parts whose select list endpoints cannot compute (an
     * expression over an aggregate) gets one endpoint with the complete answer and its schema.
     */
    @Test
    void partialQueryWithExpressionOverAggregateIsAnsweredCompletely() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        ExecutionService executionService = mock(ExecutionService.class);
        String query = "SELECT CAST(sum(o_totalprice) AS DECIMAL(38,2)) AS a FROM tpch.orders";
        Schema schema = new Schema(List.of(Field.nullable("a", new ArrowType.Decimal(38, 2, 128))));
        when(executionService.completeSchema(allocator, query, null)).thenReturn(schema);
        when(executionService.completeSchema(allocator, query)).thenReturn(schema);
        QueryPlan plan = new QueryPlan(List.of(new org.apache.arrow.flight.FlightEndpoint(
                new org.apache.arrow.flight.Ticket(new byte[] {1}))), -1L, -1L);
        when(queryPlanner.planComplete(query, null)).thenReturn(plan);
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                mock(MetadataService.class), queryPlanner, executionService,
                mock(ClusterService.class), "partial");
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        FlightSql.CommandStatementQuery command =
                FlightSql.CommandStatementQuery.newBuilder().setQuery(query).build();

        FlightInfo info = producer.getFlightInfoStatement(command, context,
                FlightDescriptor.command(new byte[]{1}));

        assertEquals(schema, info.getSchemaOptional().orElseThrow());
        assertEquals(1, info.getEndpoints().size());
        verify(queryPlanner, never()).plan(anyString());
        verify(queryPlanner, never()).plan(anyString(), anyBoolean());
        verify(executionService, never()).resultSchema(any(), anyString());
        assertEquals(schema, producer.getSchemaStatement(command, context,
                FlightDescriptor.command(new byte[]{1})).getSchema());
    }

    /**
     * Verifies such a query is refused with the reason on a cluster that keeps its files on
     * local disks, where no node can compute the complete answer.
     */
    @Test
    void partialQueryNeedingCompleteAnswerIsRefusedWithoutSharedStorage() {
        BufferAllocator allocator = mock(BufferAllocator.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        String query = "SELECT avg(o_totalprice) FROM tpch.orders";
        when(queryPlanner.planComplete(query, null)).thenThrow(new IllegalStateException(
                "A complete answer needs storage every node can read (HDFS, S3); this cluster of 3"
                        + " nodes keeps its files on local disks. Ask for partial results (header"
                        + " x-flight-result-mode: partial) or set resultMode=partial"));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                mock(MetadataService.class), queryPlanner, mock(ExecutionService.class),
                mock(ClusterService.class), "partial");
        FlightSql.CommandStatementQuery command =
                FlightSql.CommandStatementQuery.newBuilder().setQuery(query).build();

        FlightRuntimeException e = org.junit.jupiter.api.Assertions.assertThrows(
                FlightRuntimeException.class, () -> producer.getFlightInfoStatement(command,
                        mock(FlightProducer.CallContext.class), FlightDescriptor.command(new byte[]{1})));

        assertEquals(FlightStatusCode.INVALID_ARGUMENT, e.status().code());
        assertEquals("The select item 'avg(o_totalprice)' is not a column or a plain count, sum, min"
                + " or max, so the query needs one node to see all rows; A complete answer needs"
                + " storage every node can read (HDFS, S3); this cluster of 3 nodes keeps its files"
                + " on local disks.", e.getMessage());
    }

    /** Verifies only a client that tries locations in order may get the planning node as a backup. */
    @Test
    void locationFailoverHeaderLetsThePlannerListItself() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        ExecutionService executionService = mock(ExecutionService.class);
        String query = "select l_orderkey from tpch.lineitem";
        Schema schema = new Schema(List.of(Field.nullable("l_orderkey", new ArrowType.Int(64, true))));
        when(executionService.resultSchema(allocator, query)).thenReturn(schema);
        when(queryPlanner.plan(query)).thenReturn(new QueryPlan(List.of(), 10L, 1L));
        when(queryPlanner.plan(query, true)).thenReturn(new QueryPlan(List.of(), 10L, 1L));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                mock(MetadataService.class), queryPlanner, executionService,
                mock(ClusterService.class), "partial");
        FlightSql.CommandStatementQuery command =
                FlightSql.CommandStatementQuery.newBuilder().setQuery(query).build();
        FlightProducer.CallContext older = mock(FlightProducer.CallContext.class);
        FlightProducer.CallContext failover = mock(FlightProducer.CallContext.class);
        org.apache.arrow.flight.FlightCallHeaders headers = new org.apache.arrow.flight.FlightCallHeaders();
        headers.insert(ResultModeMiddleware.LOCATION_FAILOVER_HEADER, "1");
        when(failover.getMiddleware(ResultModeMiddleware.KEY))
                .thenReturn(new ResultModeMiddleware.Factory().onCallStarted(null, headers, null));

        producer.getFlightInfoStatement(command, older, FlightDescriptor.command(new byte[]{1}));
        verify(queryPlanner).plan(query);
        verify(queryPlanner, never()).plan(query, true);

        producer.getFlightInfoStatement(command, failover, FlightDescriptor.command(new byte[]{1}));
        verify(queryPlanner).plan(query, true);
    }

    /** Verifies the header values that turn location failover on. */
    @Test
    void locationFailoverHeaderValues() {
        org.junit.jupiter.api.Assertions.assertTrue(ResultModeMiddleware.isEnabled("1"));
        org.junit.jupiter.api.Assertions.assertTrue(ResultModeMiddleware.isEnabled(" TRUE "));
        org.junit.jupiter.api.Assertions.assertFalse(ResultModeMiddleware.isEnabled(null));
        org.junit.jupiter.api.Assertions.assertFalse(ResultModeMiddleware.isEnabled("0"));
        org.junit.jupiter.api.Assertions.assertFalse(ResultModeMiddleware.isEnabled("yes"));
    }

    /** Verifies a node running an endpoint planned elsewhere takes over its load, then releases it. */
    @Test
    void endpointPlannedForAnotherNodeClaimsThenReleasesLoad() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        ExecutionService executionService = mock(ExecutionService.class);
        ClusterService clusterService = mock(ClusterService.class);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        ByteString handle = ByteString.copyFromUtf8("planned-elsewhere");
        HandleState state = HandleState.forServerFiles("select * from tpch.lineitem",
                new String[]{"tpch/lineitem/a.parquet"}, "grpc+tcp://gone:32010", 100L, true);
        when(clusterService.resolveEndpointHandle(handle.toByteArray())).thenReturn(state);
        when(clusterService.plannedElsewhere(state)).thenReturn(true);
        when(clusterService.claimEndpointLoadLater(handle.toByteArray(), state))
                .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                mock(MetadataService.class), mock(QueryPlanner.class), executionService,
                clusterService);

        producer.getStreamStatement(FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(handle).build(), mock(FlightProducer.CallContext.class), listener);

        org.mockito.InOrder order = org.mockito.Mockito.inOrder(clusterService, executionService);
        order.verify(clusterService).claimEndpointLoadLater(handle.toByteArray(), state);
        order.verify(executionService).readParquet(eq(allocator), eq(state.query()),
                eq(state.filePaths()), eq(listener), anyBoolean(), any(ExecutionPathTracker.class));
        order.verify(clusterService).releaseEndpointLoad(handle.toByteArray(), state);
        verify(listener).completed();
    }

    /** Verifies binary ticket handles give readable query ids in the log. */
    @Test
    void queryIdOfABinaryHandleIsHex() {
        assertEquals("1f0c2a9e", FlightSqlProducer.qid(com.google.protobuf.ByteString.copyFromUtf8("1f0c2a9e-77aa-4bcd")));
        org.junit.jupiter.api.Assertions.assertTrue(FlightSqlProducer.qid(com.google.protobuf.ByteString.copyFrom(new byte[] {'A', 'F', 'T', '1', 0, 0, 1, 2, 3}))
                .matches("[0-9a-f]{8}"));
    }

    /**
     * A failure whose root cause has no text (ConnectException under a store's "lakeFS not
     * reachable") is described by the deepest cause that has one; a root with text is kept.
     */
    @Test
    void failureDescriptionNamesTheDeepestCauseWithText() {
        java.io.IOException store = new java.io.IOException(
                "lakeFS API GET http://lakefs:8000/api/v1/x failed, lakeFS not reachable",
                new java.net.ConnectException());
        assertEquals("Query execution failed: IOException: lakeFS API GET http://lakefs:8000/api/v1/x"
                + " failed, lakeFS not reachable",
                FlightSqlProducer.failureDescription(new RuntimeException(store)));
        assertEquals("Query execution failed: SQLException: IO Error: x",
                FlightSqlProducer.failureDescription(new RuntimeException("outer",
                        new java.sql.SQLException("IO Error: x"))));
        assertEquals("Query execution failed: ConnectException",
                FlightSqlProducer.failureDescription(new java.net.ConnectException()));
        // root with text under a store IOException that repeats it: the store's context wins
        java.io.IOException timedOut = new java.io.IOException(
                "lakeFS API GET http://lakefs:8000/api/v1/x failed, lakeFS not reachable:"
                        + " HttpConnectTimeoutException HTTP connect timed out",
                new java.net.http.HttpConnectTimeoutException("HTTP connect timed out"));
        timedOut.getCause().initCause(new java.net.ConnectException("HTTP connect timed out"));
        assertEquals("Query execution failed: IOException: lakeFS API GET http://lakefs:8000/api/v1/x"
                + " failed, lakeFS not reachable: HttpConnectTimeoutException HTTP connect timed out",
                FlightSqlProducer.failureDescription(new IllegalStateException(
                        "Unable to describe query: " + timedOut.getMessage(), timedOut)));
        // a wrapper that is only its cause's toString() does not replace the root
        assertEquals("Query execution failed: FileNotFoundException: x",
                FlightSqlProducer.failureDescription(new java.io.IOException(
                        new java.io.FileNotFoundException("x"))));
    }

    /** Answers the table statistics action with JSON from the planner, without planning. */
    @Test
    void tableStatisticsActionAnswersJson() throws Exception {
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        when(queryPlanner.tableStatistics("tpch", "lineitem", List.of("l_orderkey"))).thenReturn(
                new TableStatisticsService.TableStatistics(125L, true, 4096L, 2, Map.of("l_orderkey",
                        new TableStatisticsService.ColumnStatistics(1.0, 600.0, 0L, 512L, null))));
        FlightSqlProducer producer = statisticsProducer(queryPlanner, mock(FlightAuthorization.class));
        @SuppressWarnings("unchecked")
        FlightProducer.StreamListener<org.apache.arrow.flight.Result> listener =
                mock(FlightProducer.StreamListener.class);

        producer.doAction(mock(FlightProducer.CallContext.class), statisticsAction(
                "{\"schema\":\"tpch\",\"table\":\"lineitem\",\"columns\":[\"l_orderkey\"]}"),
                listener);

        ArgumentCaptor<org.apache.arrow.flight.Result> result =
                ArgumentCaptor.forClass(org.apache.arrow.flight.Result.class);
        verify(listener).onNext(result.capture());
        verify(listener).onCompleted();
        com.fasterxml.jackson.databind.JsonNode json = new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(result.getValue().getBody());
        assertEquals(125L, json.get("rows").asLong());
        assertEquals(true, json.get("rowsExact").asBoolean());
        assertEquals(600.0, json.get("columns").get("l_orderkey").get("max").asDouble());
        assertEquals(true, json.get("columns").get("l_orderkey").get("ndv").isNull());
        verify(queryPlanner, never()).plan(anyString());
    }

    /** Maps refusals of the statistics action to Flight status codes. */
    @Test
    void tableStatisticsActionErrors() throws Exception {
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        FlightAuthorization authorization = mock(FlightAuthorization.class);
        doThrow(CallStatus.UNAUTHORIZED.toRuntimeException()).when(authorization)
                .requireQuery(any(), eq("SELECT * FROM sales.secret"));
        when(queryPlanner.tableStatistics(eq("sales"), eq("gone"), any()))
                .thenThrow(new java.io.FileNotFoundException("no table"));
        when(queryPlanner.tableStatistics(eq("sales"), eq("local"), any()))
                .thenThrow(new IllegalStateException("local disks"));
        FlightSqlProducer producer = statisticsProducer(queryPlanner, authorization);

        assertEquals(FlightStatusCode.UNAUTHORIZED, statisticsError(producer,
                "{\"schema\":\"sales\",\"table\":\"secret\"}"));
        assertEquals(FlightStatusCode.NOT_FOUND, statisticsError(producer,
                "{\"schema\":\"sales\",\"table\":\"gone\"}"));
        assertEquals(FlightStatusCode.INVALID_ARGUMENT, statisticsError(producer,
                "{\"schema\":\"sales\",\"table\":\"x; DROP\"}"));
        assertEquals(FlightStatusCode.INVALID_ARGUMENT, statisticsError(producer, "{\"table\":\"t\"}"));
        assertEquals(FlightStatusCode.INVALID_ARGUMENT, statisticsError(producer, "not json"));
        assertEquals(FlightStatusCode.UNIMPLEMENTED, statisticsError(producer,
                "{\"schema\":\"sales\",\"table\":\"local\"}"));
        verify(queryPlanner, never()).tableStatistics(eq("sales"), eq("secret"), any());
    }

    /** Lists the statistics action after the Flight SQL actions. */
    @Test
    void listActionsIncludesTableStatistics() {
        FlightSqlProducer producer = statisticsProducer(mock(QueryPlanner.class),
                mock(FlightAuthorization.class));
        List<String> types = new java.util.ArrayList<>();
        boolean[] completed = {false};
        producer.listActions(mock(FlightProducer.CallContext.class),
                new FlightProducer.StreamListener<>() {
                    @Override
                    public void onNext(org.apache.arrow.flight.ActionType value) {
                        types.add(value.getType());
                    }

                    @Override
                    public void onError(Throwable t) {
                        throw new AssertionError(t);
                    }

                    @Override
                    public void onCompleted() {
                        completed[0] = true;
                    }
                });

        assertEquals(TableStatisticsService.ACTION, types.get(types.size() - 1));
        assertEquals(true, types.size() > 1);
        assertEquals(true, completed[0]);
    }

    private static FlightSqlProducer statisticsProducer(QueryPlanner queryPlanner,
            FlightAuthorization authorization) {
        return new FlightSqlProducer(Location.forGrpcInsecure("localhost", 32010),
                mock(BufferAllocator.class), mock(MetadataService.class), queryPlanner,
                mock(ExecutionService.class), mock(ClusterService.class), authorization);
    }

    private static org.apache.arrow.flight.Action statisticsAction(String json) {
        return new org.apache.arrow.flight.Action(TableStatisticsService.ACTION,
                json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static FlightStatusCode statisticsError(FlightSqlProducer producer, String json) {
        @SuppressWarnings("unchecked")
        FlightProducer.StreamListener<org.apache.arrow.flight.Result> listener =
                mock(FlightProducer.StreamListener.class);
        producer.doAction(mock(FlightProducer.CallContext.class), statisticsAction(json), listener);
        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).onError(error.capture());
        verify(listener, never()).onCompleted();
        return assertInstanceOf(FlightRuntimeException.class, error.getValue()).status().code();
    }
}
