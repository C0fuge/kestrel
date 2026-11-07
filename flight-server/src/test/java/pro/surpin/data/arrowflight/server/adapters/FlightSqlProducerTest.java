package pro.surpin.data.arrowflight.server.adapters;

import com.google.protobuf.ByteString;
import pro.surpin.data.arrowflight.server.model.HandleState;
import pro.surpin.data.arrowflight.server.FlightAuthorization;
import pro.surpin.data.arrowflight.server.model.ExecutionPathTracker;
import pro.surpin.data.arrowflight.server.model.QueryPlan;
import pro.surpin.data.arrowflight.server.services.CatalogViews;
import pro.surpin.data.arrowflight.server.services.ClusterService;
import pro.surpin.data.arrowflight.server.services.ExecutionService;
import pro.surpin.data.arrowflight.server.services.MetadataService;
import pro.surpin.data.arrowflight.server.services.QueryPlanner;
import pro.surpin.data.arrowflight.server.services.TableStatisticsService;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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

    /** Requires access to every table a catalog view reads, not only to the view. */
    @Test
    void viewSourcesAreChecked() throws Exception {
        String query = "select * from h.people_view";
        ExecutionService executionService = mock(ExecutionService.class);
        when(executionService.catalogViews(query)).thenReturn(new CatalogViews.Expanded(
                "select * from (SELECT 1 FROM \"h\".\"secret\") AS \"people_view\"",
                List.of(new TableName("h", "secret"))));
        FlightAuthorization authorization = mock(FlightAuthorization.class);
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        when(context.peerIdentity()).thenReturn("alice@EXAMPLE.COM");
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), mock(BufferAllocator.class),
                mock(MetadataService.class), mock(QueryPlanner.class), executionService,
                mock(ClusterService.class), authorization, "complete");
        FlightSql.CommandStatementQuery command = FlightSql.CommandStatementQuery
                .newBuilder().setQuery(query).build();
        assertEquals(FlightStatusCode.UNAUTHORIZED,
                org.junit.jupiter.api.Assertions.assertThrows(FlightRuntimeException.class,
                        () -> producer.getFlightInfoStatement(command, context,
                                FlightDescriptor.command(new byte[]{1}))).status().code());
        verify(authorization).requireQuery("alice@EXAMPLE.COM", query);
        verify(authorization).canReadTable("alice@EXAMPLE.COM", "h", "secret");
        verify(executionService, never()).completeSchema(any(), anyString(), any());
        // A view that cannot be read as written is the client's error.
        when(executionService.catalogViews(query)).thenThrow(
                new IllegalArgumentException("Time travel is not supported for the view"));
        assertEquals(FlightStatusCode.INVALID_ARGUMENT,
                org.junit.jupiter.api.Assertions.assertThrows(FlightRuntimeException.class,
                        () -> producer.getFlightInfoStatement(command, context,
                                FlightDescriptor.command(new byte[]{1}))).status().code());
    }

    /** Runs a saved complete ticket of a view as checked: its views replaced, on this node. */
    @Test
    void savedViewTicketRunsTheCheckedText() throws Exception {
        ByteString handle = ByteString.copyFromUtf8("saved-view");
        String query = "select * from h.people_view";
        String expanded = "select * from (SELECT 1 FROM \"h\".\"people\") AS \"people_view\"";
        ClusterService clusterService = mock(ClusterService.class);
        HandleState state = HandleState.forCompleteAnswer(query, null);
        when(clusterService.resolveEndpointHandle(handle.toByteArray())).thenReturn(state);
        ExecutionService executionService = mock(ExecutionService.class);
        when(executionService.catalogViews(query)).thenReturn(new CatalogViews.Expanded(expanded,
                List.of(new TableName("h", "people"))));
        when(executionService.completeFromCopies(eq(expanded), any()))
                .thenReturn(new ExecutionService.CopiedQuery(expanded, null));
        FlightAuthorization authorization = mock(FlightAuthorization.class);
        when(authorization.canReadTable("bob", "h", "people")).thenReturn(true);
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        when(context.peerIdentity()).thenReturn("bob");
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), mock(BufferAllocator.class),
                mock(MetadataService.class), mock(QueryPlanner.class), executionService,
                clusterService, authorization);
        producer.getStreamStatement(FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(handle).build(), context, listener);
        verify(executionService).readComplete(any(), eq(expanded), any(), any(), eq(true));
        verify(listener, never()).error(any());
    }

    /** Answers a query of a catalog view asked for in parts completely: a view has no files. */
    @Test
    void partialViewQueryIsAnsweredCompletely() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        ExecutionService executionService = mock(ExecutionService.class);
        String query = "select id from h.people_view";
        Schema schema = new Schema(List.of(Field.nullable("id", new ArrowType.Int(64, true))));
        when(executionService.catalogViews(query)).thenReturn(new CatalogViews.Expanded(
                "select id from (SELECT 1 AS id FROM \"h\".\"people\") AS \"people_view\"",
                List.of(new TableName("h", "people"))));
        when(executionService.completeSchema(allocator, query, null)).thenReturn(schema);
        when(queryPlanner.planComplete(query, null)).thenReturn(new QueryPlan(List.of(), -1L, -1L));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                mock(MetadataService.class), queryPlanner, executionService,
                mock(ClusterService.class), "partial");
        producer.getFlightInfoStatement(FlightSql.CommandStatementQuery
                .newBuilder().setQuery(query).build(), mock(FlightProducer.CallContext.class),
                FlightDescriptor.command(new byte[]{1}));
        verify(executionService).completeSchema(allocator, query, null);
        verify(executionService, never()).resultSchema(any(), anyString());
        verify(queryPlanner).planComplete(query, null);
        verify(queryPlanner, never()).plan(anyString());
    }

    /**
     * Item 37 (a): a complete-mode query of a table whose first file the storage refused (no
     * view, "cannot be described") is denied, not UNAVAILABLE as a catalog outage.
     */
    @Test
    void undescribedTableRefusedByTheStorageIsDenied() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        ExecutionService executionService = mock(ExecutionService.class);
        String query = "SELECT count(*) FROM tpch.nation";
        when(executionService.completeSchema(allocator, query, null)).thenThrow(
                new java.io.UncheckedIOException(new java.io.IOException("Table tpch.nation cannot be"
                        + " described from its first file: Permission denied: user=flight, access=READ")));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                mock(MetadataService.class), mock(QueryPlanner.class), executionService,
                mock(ClusterService.class), "complete");
        FlightRuntimeException error = org.junit.jupiter.api.Assertions.assertThrows(
                FlightRuntimeException.class, () -> producer.getFlightInfoStatement(
                        FlightSql.CommandStatementQuery.newBuilder().setQuery(query).build(),
                        mock(FlightProducer.CallContext.class), FlightDescriptor.command(new byte[]{1})));
        assertEquals(FlightStatusCode.UNAUTHORIZED, error.status().code(), error.getMessage());
        assertEquals(FlightSqlProducer.STORAGE_DENIED_MESSAGE, error.status().description());
    }

    /** Lists catalog views as VIEW when the caller may read them and every table they read. */
    @Test
    @SuppressWarnings("unchecked")
    void catalogViewsAreListedAsViews() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        MetadataService metadataService = mock(MetadataService.class);
        when(metadataService.getSchemas(null)).thenReturn(Map.of("h", "catalog:hive"));
        when(metadataService.getTables("h", null)).thenReturn(Map.of("people", ""));
        ExecutionService executionService = mock(ExecutionService.class);
        pro.surpin.data.arrowflight.spi.catalog.CatalogView open = view("open", "people");
        pro.surpin.data.arrowflight.spi.catalog.CatalogView closed = view("closed", "secret");
        when(executionService.catalogViewList()).thenReturn(Map.of("h", List.of(closed, open)));
        when(executionService.catalogViews("SELECT * FROM \"h\".\"open\"")).thenReturn(
                new CatalogViews.Expanded("x", List.of(new TableName("h", "people"))));
        when(executionService.catalogViews("SELECT * FROM \"h\".\"closed\"")).thenReturn(
                new CatalogViews.Expanded("y", List.of(new TableName("h", "secret"))));
        FlightAuthorization authorization = mock(FlightAuthorization.class);
        when(authorization.canSeeSchema("bob", "h")).thenReturn(true);
        when(authorization.canReadTable("bob", "h", "people")).thenReturn(true);
        when(authorization.canReadTable("bob", "h", "open")).thenReturn(true);
        when(authorization.canReadTable("bob", "h", "closed")).thenReturn(true);
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        when(context.peerIdentity()).thenReturn("bob");
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator, metadataService,
                mock(QueryPlanner.class), executionService, mock(ClusterService.class),
                authorization, "complete");
        ArgumentCaptor<Map<String, Map<String, Schema>>> listed = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<java.util.Set<String>> views = ArgumentCaptor.forClass(java.util.Set.class);

        producer.getStreamTables(FlightSql.CommandGetTables.getDefaultInstance(), context,
                mock(FlightProducer.ServerStreamListener.class));
        verify(metadataService).getTablesRoot(listed.capture(), views.capture(), eq(allocator),
                eq(false), any(), any());
        // The view over a table bob may not read is not listed.
        assertEquals(java.util.Set.of("people", "open"), listed.getValue().get("h").keySet());
        assertEquals(java.util.Set.of("h\u0000open"), views.getValue());

        producer.getStreamTables(FlightSql.CommandGetTables.newBuilder().addTableTypes("VIEW")
                .build(), context, mock(FlightProducer.ServerStreamListener.class));
        verify(metadataService, times(2)).getTablesRoot(listed.capture(), views.capture(),
                eq(allocator), eq(false), any(), any());
        assertEquals(java.util.Set.of("open"), listed.getValue().get("h").keySet());

        producer.getStreamTables(FlightSql.CommandGetTables.newBuilder().addTableTypes("TABLE")
                .build(), context, mock(FlightProducer.ServerStreamListener.class));
        verify(metadataService, times(3)).getTablesRoot(listed.capture(), views.capture(),
                eq(allocator), eq(false), any(), any());
        assertEquals(java.util.Set.of("people"), listed.getValue().get("h").keySet());
        assertEquals(java.util.Set.of(), views.getValue());
        verify(executionService, times(2)).catalogViewList();

        producer.getStreamTableTypes(context, mock(FlightProducer.ServerStreamListener.class));
        verify(metadataService).getTableTypesRoot(allocator, true);
    }

    private static pro.surpin.data.arrowflight.spi.catalog.CatalogView view(String name,
            String table) {
        return new pro.surpin.data.arrowflight.spi.catalog.CatalogView(name,
                "SELECT * FROM \"h\".\"" + table + "\"", List.of(new TableName("h", table)),
                List.of(new pro.surpin.data.arrowflight.spi.catalog.ColumnSpec("id", "BIGINT",
                        true, null)), name);
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

    /**
     * Verifies only a complete-mode query with parameters gets a statement id: a query without
     * any is planned and closed without looking up values in the cluster.
     *
     * @throws Exception on failure
     */
    @Test
    void onlyAQueryWithParametersHasAStatementId() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        ExecutionService executionService = mock(ExecutionService.class);
        ClusterService clusterService = mock(ClusterService.class);
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        String plain = "select o_comment from s.orders where o_orderkey = 7";
        String bound = "select o_comment from s.orders where o_orderkey = ?";
        Schema schema = new Schema(List.of(Field.nullable("o_comment", ArrowType.Utf8.INSTANCE)));
        Schema parameters = new Schema(List.of(Field.nullable("parameter_1", new ArrowType.Int(64, true))));
        when(executionService.completeSchema(allocator, plain, null)).thenReturn(schema);
        when(executionService.completeSchema(allocator, bound, null)).thenReturn(schema);
        when(executionService.completeParameterSchema(plain)).thenReturn(new Schema(List.of()));
        when(executionService.completeParameterSchema(bound)).thenReturn(parameters);
        when(queryPlanner.planComplete(anyString(), any())).thenReturn(new QueryPlan(List.of(), -1L, -1L));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                mock(MetadataService.class), queryPlanner, executionService, clusterService, "complete");

        ByteString plainHandle = prepare(producer, plain, context);
        ByteString boundHandle = prepare(producer, bound, context);

        assertEquals(plain, plainHandle.toStringUtf8());
        assertNull(QueryParameters.parseHandle(plainHandle).statementId());
        String id = QueryParameters.parseHandle(boundHandle).statementId();
        assertNotNull(id);
        assertEquals(bound, QueryParameters.parseHandle(boundHandle).sql());

        for (ByteString handle : List.of(plainHandle, boundHandle)) {
            producer.getFlightInfoPreparedStatement(FlightSql.CommandPreparedStatementQuery.newBuilder()
                    .setPreparedStatementHandle(handle).build(), context, FlightDescriptor.command(new byte[]{1}));
            @SuppressWarnings("unchecked")
            FlightProducer.StreamListener<org.apache.arrow.flight.Result> closed =
                    mock(FlightProducer.StreamListener.class);
            producer.closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest.newBuilder()
                    .setPreparedStatementHandle(handle).build(), context, closed);
            verify(closed).onCompleted();
        }

        verify(clusterService).recallParameters(id);
        verify(clusterService).forgetParameters(id);
        verify(clusterService, times(1)).recallParameters(anyString());
        verify(clusterService, times(1)).forgetParameters(anyString());
    }

    /**
     * Prepares a query and returns the handle the server answers with.
     *
     * @param producer producer under test
     * @param query client SQL
     * @param context call context
     * @return prepared statement handle
     * @throws Exception on failure
     */
    private static ByteString prepare(FlightSqlProducer producer, String query,
            FlightProducer.CallContext context) throws Exception {
        @SuppressWarnings("unchecked")
        FlightProducer.StreamListener<org.apache.arrow.flight.Result> listener =
                mock(FlightProducer.StreamListener.class);
        producer.createPreparedStatement(
                FlightSql.ActionCreatePreparedStatementRequest.newBuilder().setQuery(query).build(),
                context, listener);
        ArgumentCaptor<org.apache.arrow.flight.Result> result =
                ArgumentCaptor.forClass(org.apache.arrow.flight.Result.class);
        verify(listener).onNext(result.capture());
        return com.google.protobuf.Any.parseFrom(result.getValue().getBody())
                .unpack(FlightSql.ActionCreatePreparedStatementResult.class).getPreparedStatementHandle();
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

    /**
     * Verifies queries refused while planned in parts (several tables, a feature endpoints
     * cannot answer) log one WARN line without a stack under a query id, and their reads line
     * under the same id (item 34).
     */
    @Test
    void partialRefusalsLogOneWarnLineWithQueryIdAndReads() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        ExecutionService executionService = mock(ExecutionService.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        String join = "SELECT count(*) FROM tpch.lineitem l, tpch.orders o"
                + " WHERE l.l_orderkey = o.o_orderkey";
        when(executionService.resultSchema(allocator, join)).thenThrow(new RuntimeException(
                "Invalid SQL query: " + join, new IllegalArgumentException(
                        "Only single-table queries are supported")));
        String distinct = "SELECT l_returnflag FROM tpch.lineitem";
        when(executionService.resultSchema(allocator, distinct)).thenReturn(new Schema(List.of(
                Field.nullable("l_returnflag", ArrowType.Utf8.INSTANCE))));
        when(queryPlanner.plan(distinct)).thenThrow(new IllegalArgumentException(
                "An aggregate over DISTINCT values is not supported when the table is served by"
                        + " several Flight nodes"));
        List<String> readsQids = new java.util.ArrayList<>();
        org.mockito.Mockito.doAnswer(call -> readsQids.add(
                pro.surpin.data.arrowflight.server.LogUtil.qid())).when(executionService)
                .logRefusedReads("partialPlan");
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                mock(MetadataService.class), queryPlanner, executionService,
                mock(ClusterService.class), "partial");

        java.io.PrintStream err = System.err;
        java.io.ByteArrayOutputStream log = new java.io.ByteArrayOutputStream();
        System.setErr(new java.io.PrintStream(log, true, java.nio.charset.StandardCharsets.UTF_8));
        FlightRuntimeException joinError;
        FlightRuntimeException distinctError;
        try {
            joinError = org.junit.jupiter.api.Assertions.assertThrows(FlightRuntimeException.class,
                    () -> producer.getFlightInfoStatement(
                            FlightSql.CommandStatementQuery.newBuilder().setQuery(join).build(),
                            mock(FlightProducer.CallContext.class),
                            FlightDescriptor.command(new byte[]{1})));
            distinctError = org.junit.jupiter.api.Assertions.assertThrows(
                    FlightRuntimeException.class, () -> producer.getFlightInfoStatement(
                            FlightSql.CommandStatementQuery.newBuilder().setQuery(distinct).build(),
                            mock(FlightProducer.CallContext.class),
                            FlightDescriptor.command(new byte[]{1})));
        } finally {
            System.setErr(err);
        }

        assertEquals(FlightStatusCode.INVALID_ARGUMENT, joinError.status().code());
        assertEquals(FlightStatusCode.INVALID_ARGUMENT, distinctError.status().code());
        String text = log.toString(java.nio.charset.StandardCharsets.UTF_8);
        List<String> refusals = text.lines().filter(l -> l.contains("query=refused mode=partial"))
                .toList();
        assertEquals(2, refusals.size(), text);
        for (String line : refusals) {
            assertTrue(line.contains("WARN"), line);
        }
        assertTrue(refusals.get(0).contains("IllegalArgumentException: Only single-table queries are supported'"),
                refusals.get(0));
        assertTrue(refusals.get(1).contains("reason='An aggregate over DISTINCT values"),
                refusals.get(1));
        assertTrue(!text.contains("Error getting Arrow schema") && !text.contains("\tat ")
                && !text.contains("ERROR"), text);
        assertEquals(2, readsQids.size());
        for (int i = 0; i < 2; i++) {
            assertNotNull(readsQids.get(i));
            assertTrue(refusals.get(i).contains("qid=" + readsQids.get(i) + " "), refusals.get(i));
        }
        assertTrue(!readsQids.get(0).equals(readsQids.get(1)), readsQids.toString());
        assertNull(pro.surpin.data.arrowflight.server.LogUtil.qid());
    }

    /**
     * Verifies a stream refused for lack of copy room is RESOURCE_EXHAUSTED and logs one WARN
     * line without a stack (item 34).
     */
    @Test
    void copyDiskRefusalLogsOneWarnLine() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        ExecutionService executionService = mock(ExecutionService.class);
        ClusterService clusterService = mock(ClusterService.class);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        ByteString handle = ByteString.copyFromUtf8("noroom-query");
        HandleState state = new HandleState(
                "select * from tpch.lineitem", new String[]{"part.parquet"}, null, 0L);
        when(clusterService.resolveEndpointHandle(handle.toByteArray())).thenReturn(state);
        doThrow(new java.io.IOException("copies failed",
                new pro.surpin.data.arrowflight.server.storage.LocalCopyCache.NoRoomException(
                        "No room on the disk of the node-local copy cache /x\nfor SELECT *\nFROM t")))
                .when(executionService).readParquet(eq(allocator), eq(state.query()),
                        eq(state.filePaths()), eq(listener), anyBoolean(),
                        any(ExecutionPathTracker.class));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                mock(MetadataService.class), mock(QueryPlanner.class), executionService,
                clusterService);

        java.io.PrintStream err = System.err;
        java.io.ByteArrayOutputStream log = new java.io.ByteArrayOutputStream();
        System.setErr(new java.io.PrintStream(log, true, java.nio.charset.StandardCharsets.UTF_8));
        try {
            producer.getStreamStatement(FlightSql.TicketStatementQuery.newBuilder()
                    .setStatementHandle(handle).build(), mock(FlightProducer.CallContext.class),
                    listener);
        } finally {
            System.setErr(err);
        }

        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).error(error.capture());
        FlightRuntimeException reported = assertInstanceOf(
                FlightRuntimeException.class, error.getValue());
        assertEquals(FlightStatusCode.RESOURCE_EXHAUSTED, reported.status().code());
        String text = log.toString(java.nio.charset.StandardCharsets.UTF_8);
        List<String> failed = text.lines().filter(l -> l.contains("execution=failed")).toList();
        assertEquals(1, failed.size(), text);
        // Item 36 (4): a multi-line reason stays on the record's one line.
        assertTrue(failed.get(0).contains("WARN") && failed.get(0).contains("result=noRoom error='No"
                + " room on the disk of the node-local copy cache /x\\nfor SELECT *\\nFROM t'"), failed.get(0));
        assertTrue(text.lines().noneMatch(l -> l.startsWith("FROM t")), text);
        assertTrue(!text.contains("\tat ") && !text.contains("ERROR"), text);
    }

    /**
     * Verifies a stream that runs out of Arrow memory is RESOURCE_EXHAUSTED, never
     * INVALID_ARGUMENT, and logs one WARN line without a stack (item 35: the stand's exact error,
     * Arrow's memory limit wrapped by the stream reader in an IllegalArgumentException).
     */
    @Test
    void arrowMemoryLimitIsResourceExhausted() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        ExecutionService executionService = mock(ExecutionService.class);
        ClusterService clusterService = mock(ClusterService.class);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        ByteString handle = ByteString.copyFromUtf8("memory-query");
        HandleState state = new HandleState(
                "select * from tpch.lineitem", new String[]{"part.parquet"}, null, 0L);
        when(clusterService.resolveEndpointHandle(handle.toByteArray())).thenReturn(state);
        doThrow(new IllegalArgumentException("Could not load buffers for field l_partkey: Int(64, true)."
                + " error message: Unable to allocate buffer of size 524288 due to memory limit. Current"
                + " allocation: 2146995016",
                new org.apache.arrow.memory.OutOfMemoryException("Unable to allocate buffer of size 524288 due"
                        + " to memory limit. Current allocation: 2146995016")))
                .when(executionService).readParquet(eq(allocator), eq(state.query()),
                        eq(state.filePaths()), eq(listener), anyBoolean(),
                        any(ExecutionPathTracker.class));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                mock(MetadataService.class), mock(QueryPlanner.class), executionService,
                clusterService);

        java.io.PrintStream err = System.err;
        java.io.ByteArrayOutputStream log = new java.io.ByteArrayOutputStream();
        System.setErr(new java.io.PrintStream(log, true, java.nio.charset.StandardCharsets.UTF_8));
        try {
            producer.getStreamStatement(FlightSql.TicketStatementQuery.newBuilder()
                    .setStatementHandle(handle).build(), mock(FlightProducer.CallContext.class),
                    listener);
        } finally {
            System.setErr(err);
        }

        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).error(error.capture());
        FlightRuntimeException reported = assertInstanceOf(
                FlightRuntimeException.class, error.getValue());
        assertEquals(FlightStatusCode.RESOURCE_EXHAUSTED, reported.status().code());
        assertTrue(reported.getMessage().contains("due to memory limit"), reported.getMessage());
        String text = log.toString(java.nio.charset.StandardCharsets.UTF_8);
        List<String> failed = text.lines().filter(l -> l.contains("execution=failed")).toList();
        assertEquals(1, failed.size(), text);
        assertTrue(failed.get(0).contains("WARN") && failed.get(0).contains("result=resourceExhausted"),
                failed.get(0));
        assertTrue(!text.contains("\tat ") && !text.contains("ERROR"), text);
    }

    /**
     * Verifies an access denial raised while the schema of a query in parts is described is
     * reported as the denial (PERMISSION_DENIED on the wire), not INTERNAL, and logs one WARN
     * line with the SQL on one line and no stack (item 36 (3)).
     */
    @Test
    void deniedSchemaIsUnauthorizedWithOneWarnLine() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        ExecutionService executionService = mock(ExecutionService.class);
        String query = "SELECT *\nFROM tpch.lineitem";
        when(executionService.resultSchema(allocator, query)).thenThrow(new IllegalStateException(
                "view expansion failed", CallStatus.UNAUTHORIZED
                        .withDescription("Schema or table access denied").toRuntimeException()));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                mock(MetadataService.class), mock(QueryPlanner.class), executionService,
                mock(ClusterService.class), "partial");

        java.io.PrintStream err = System.err;
        java.io.ByteArrayOutputStream log = new java.io.ByteArrayOutputStream();
        System.setErr(new java.io.PrintStream(log, true, java.nio.charset.StandardCharsets.UTF_8));
        FlightRuntimeException error;
        try {
            error = org.junit.jupiter.api.Assertions.assertThrows(FlightRuntimeException.class,
                    () -> producer.getFlightInfoStatement(
                            FlightSql.CommandStatementQuery.newBuilder().setQuery(query).build(),
                            mock(FlightProducer.CallContext.class),
                            FlightDescriptor.command(new byte[]{1})));
        } finally {
            System.setErr(err);
        }

        assertEquals(FlightStatusCode.UNAUTHORIZED, error.status().code());
        String text = log.toString(java.nio.charset.StandardCharsets.UTF_8);
        List<String> denied = text.lines().filter(l -> l.contains("query=denied")).toList();
        assertEquals(1, denied.size(), text);
        assertTrue(denied.get(0).contains("WARN") && denied.get(0).contains(
                "reason='Schema or table access denied' query='SELECT *\\nFROM tpch.lineitem'"), denied.get(0));
        assertTrue(!text.contains("\tat ") && !text.contains("ERROR"), text);
    }

    /**
     * Verifies an access denial under another failure of an execution is sent to the client as
     * the denial, not INTERNAL, with one WARN line and no stack (item 36 (3)).
     */
    @Test
    void deniedExecutionIsUnauthorizedWithOneWarnLine() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        ExecutionService executionService = mock(ExecutionService.class);
        ClusterService clusterService = mock(ClusterService.class);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        ByteString handle = ByteString.copyFromUtf8("denied-query");
        HandleState state = new HandleState(
                "select * from tpch.lineitem", new String[]{"part.parquet"}, null, 0L);
        when(clusterService.resolveEndpointHandle(handle.toByteArray())).thenReturn(state);
        doThrow(new RuntimeException("catalog view", CallStatus.UNAUTHORIZED
                .withDescription("Schema or table access denied").toRuntimeException()))
                .when(executionService).readParquet(eq(allocator), eq(state.query()),
                        eq(state.filePaths()), eq(listener), anyBoolean(),
                        any(ExecutionPathTracker.class));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                mock(MetadataService.class), mock(QueryPlanner.class), executionService,
                clusterService);

        java.io.PrintStream err = System.err;
        java.io.ByteArrayOutputStream log = new java.io.ByteArrayOutputStream();
        System.setErr(new java.io.PrintStream(log, true, java.nio.charset.StandardCharsets.UTF_8));
        try {
            producer.getStreamStatement(FlightSql.TicketStatementQuery.newBuilder()
                    .setStatementHandle(handle).build(), mock(FlightProducer.CallContext.class),
                    listener);
        } finally {
            System.setErr(err);
        }

        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).error(error.capture());
        FlightRuntimeException reported = assertInstanceOf(
                FlightRuntimeException.class, error.getValue());
        assertEquals(FlightStatusCode.UNAUTHORIZED, reported.status().code());
        String text = log.toString(java.nio.charset.StandardCharsets.UTF_8);
        List<String> failed = text.lines().filter(l -> l.contains("execution=failed")).toList();
        assertEquals(1, failed.size(), text);
        assertTrue(failed.get(0).contains("WARN") && failed.get(0).contains("result=denied"),
                failed.get(0));
        assertTrue(!text.contains("\tat ") && !text.contains("ERROR"), text);
    }

    /** Stands in for Hadoop's RemoteException (an IOException). */
    static final class RemoteException extends java.io.IOException {
        private static final long serialVersionUID = 1L;

        RemoteException(String message) {
            super(message);
        }
    }

    /** Stands in for Hadoop's AccessControlException (an IOException). */
    static final class AccessControlException extends java.io.IOException {
        private static final long serialVersionUID = 1L;

        AccessControlException(String message) {
            super(message);
        }
    }

    /**
     * Item 37 (a): storage refusals (Hadoop's RemoteException / AccessControlException, DuckDB's
     * "Permission denied" IO error) are denials; other failures are not.
     */
    @Test
    void storageRefusalsAreDenials() {
        String hdfs = "Permission denied: user=flight, access=EXECUTE, inode=\"/data\":hdfs:hdfs:drwxr-x---";
        FlightRuntimeException remote = FlightSqlProducer.denial(new RuntimeException("wrapped",
                new RemoteException(hdfs)));
        assertNotNull(remote);
        assertEquals(FlightStatusCode.UNAUTHORIZED, remote.status().code());
        assertEquals(FlightSqlProducer.STORAGE_DENIED_MESSAGE, remote.status().description());
        assertEquals("RemoteException: " + hdfs, FlightSqlProducer.storageDenial(remote.getCause()));
        assertNotNull(FlightSqlProducer.denial(new AccessControlException(hdfs)));
        assertNull(FlightSqlProducer.denial(new AccessControlException(
                "Client cannot authenticate via:[TOKEN, KERBEROS]")), "a failed login is the server's");
        assertNotNull(FlightSqlProducer.denial(new IllegalStateException("x", new java.sql.SQLException(
                "Table s.t cannot be read now: IO Error: hdfs open failed: Permission denied"))));
        assertNotNull(FlightSqlProducer.denial(new java.nio.file.AccessDeniedException("/data/f")));
        assertNull(FlightSqlProducer.denial(new java.io.IOException("Connection refused")));
        assertNull(FlightSqlProducer.denial(new IllegalArgumentException("Permission denied")),
                "client errors are not storage refusals");
        assertNull(FlightSqlProducer.storageDenial(null));
    }

    /**
     * Item 37 (a): a query whose files the storage refuses (HDFS under Ranger) is sent to the
     * client as PERMISSION_DENIED, not INTERNAL, and logs one WARN line naming the storage's
     * reason, without a stack.
     */
    @Test
    void storageRefusalOfAnExecutionIsUnauthorizedWithOneWarnLine() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        ExecutionService executionService = mock(ExecutionService.class);
        ClusterService clusterService = mock(ClusterService.class);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        ByteString handle = ByteString.copyFromUtf8("storage-denied-query");
        HandleState state = new HandleState(
                "select * from tpch.lineitem", new String[]{"part.parquet"}, null, 0L);
        when(clusterService.resolveEndpointHandle(handle.toByteArray())).thenReturn(state);
        doThrow(new RuntimeException("open failed", new RemoteException(
                "Permission denied: user=flight, access=EXECUTE, inode=\"/data/parquet/tpch\"")))
                .when(executionService).readParquet(eq(allocator), eq(state.query()),
                        eq(state.filePaths()), eq(listener), anyBoolean(),
                        any(ExecutionPathTracker.class));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                mock(MetadataService.class), mock(QueryPlanner.class), executionService,
                clusterService);

        java.io.PrintStream err = System.err;
        java.io.ByteArrayOutputStream log = new java.io.ByteArrayOutputStream();
        System.setErr(new java.io.PrintStream(log, true, java.nio.charset.StandardCharsets.UTF_8));
        try {
            producer.getStreamStatement(FlightSql.TicketStatementQuery.newBuilder()
                    .setStatementHandle(handle).build(), mock(FlightProducer.CallContext.class),
                    listener);
        } finally {
            System.setErr(err);
        }

        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).error(error.capture());
        FlightRuntimeException reported = assertInstanceOf(
                FlightRuntimeException.class, error.getValue());
        assertEquals(FlightStatusCode.UNAUTHORIZED, reported.status().code());
        assertEquals(FlightSqlProducer.STORAGE_DENIED_MESSAGE, reported.status().description());
        String text = log.toString(java.nio.charset.StandardCharsets.UTF_8);
        List<String> failed = text.lines().filter(l -> l.contains("execution=failed")).toList();
        assertEquals(1, failed.size(), text);
        assertTrue(failed.get(0).contains("WARN") && failed.get(0).contains("result=denied")
                && failed.get(0).contains("error='storage: RemoteException: Permission denied"),
                failed.get(0));
        assertTrue(!text.contains("\tat ") && !text.contains("ERROR"), text);
    }

    /**
     * Verifies a query whose execution meets a thread pool stopped by a node stop is sent to the
     * client as UNAVAILABLE, with one WARN line and no stack (item 36 (4): ~200 lines of
     * RejectedExecutionException stacks per node stop).
     */
    @Test
    void executionUnderNodeStopIsUnavailableWithOneWarnLine() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        ExecutionService executionService = mock(ExecutionService.class);
        ClusterService clusterService = mock(ClusterService.class);
        FlightProducer.ServerStreamListener listener =
                mock(FlightProducer.ServerStreamListener.class);
        ByteString handle = ByteString.copyFromUtf8("stopping-query");
        HandleState state = new HandleState(
                "select * from tpch.lineitem", new String[]{"part.parquet"}, null, 0L);
        when(clusterService.resolveEndpointHandle(handle.toByteArray())).thenReturn(state);
        doThrow(new IllegalStateException("copy", new java.util.concurrent.RejectedExecutionException(
                "Task rejected from java.util.concurrent.ThreadPoolExecutor[Shutting down]")))
                .when(executionService).readParquet(eq(allocator), eq(state.query()),
                        eq(state.filePaths()), eq(listener), anyBoolean(),
                        any(ExecutionPathTracker.class));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                mock(MetadataService.class), mock(QueryPlanner.class), executionService,
                clusterService);

        java.io.PrintStream err = System.err;
        java.io.ByteArrayOutputStream log = new java.io.ByteArrayOutputStream();
        System.setErr(new java.io.PrintStream(log, true, java.nio.charset.StandardCharsets.UTF_8));
        try {
            producer.getStreamStatement(FlightSql.TicketStatementQuery.newBuilder()
                    .setStatementHandle(handle).build(), mock(FlightProducer.CallContext.class),
                    listener);
        } finally {
            System.setErr(err);
        }

        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).error(error.capture());
        FlightRuntimeException reported = assertInstanceOf(
                FlightRuntimeException.class, error.getValue());
        assertEquals(FlightStatusCode.UNAVAILABLE, reported.status().code());
        assertEquals(FlightSqlProducer.STOPPING_MESSAGE, reported.status().description());
        String text = log.toString(java.nio.charset.StandardCharsets.UTF_8);
        List<String> failed = text.lines().filter(l -> l.contains("execution=failed")).toList();
        assertEquals(1, failed.size(), text);
        assertTrue(failed.get(0).contains("WARN") && failed.get(0).contains("result=stopping"),
                failed.get(0));
        assertTrue(!text.contains("\tat ") && !text.contains("ERROR"), text);
    }

    /** Verifies only a refused task among the causes is taken for a node stop. */
    @Test
    void stoppingIsFoundAmongTheCauses() {
        assertEquals(FlightSqlProducer.STOPPING_MESSAGE, FlightSqlProducer.stopping(
                new java.io.IOException("x", new java.util.concurrent.RejectedExecutionException())));
        assertNull(FlightSqlProducer.stopping(new IllegalStateException("other")));
        assertNull(FlightSqlProducer.stopping(null));
    }

    /** Verifies only UNAUTHORIZED statuses among the causes are taken for a denial. */
    @Test
    void denialIsFoundAmongTheCauses() {
        assertNotNull(FlightSqlProducer.denial(new RuntimeException("wrapped", CallStatus.UNAUTHORIZED
                .withDescription("no").toRuntimeException())));
        assertNull(FlightSqlProducer.denial(CallStatus.UNAUTHENTICATED.withDescription("who")
                .toRuntimeException()));
        assertNull(FlightSqlProducer.denial(new IllegalStateException("other")));
        assertEquals("a\\nb\\nc", pro.surpin.data.arrowflight.server.LogUtil.oneLine("a\r\nb\nc"));
        assertNull(pro.surpin.data.arrowflight.server.LogUtil.oneLine(null));
    }

    /** Verifies only real memory shortages are taken for one. */
    @Test
    void memoryShortageIsFoundAmongTheCauses() {
        assertNotNull(FlightSqlProducer.memoryShortage(new java.sql.SQLException("Invalid Input Error:"
                + " arrow_scan: Unable to allocate buffer of size 65536 due to memory limit. Current allocation: 9")));
        assertNotNull(FlightSqlProducer.memoryShortage(new RuntimeException("wrapped",
                new org.apache.arrow.memory.OutOfMemoryException("no room"))));
        assertNotNull(FlightSqlProducer.memoryShortage(CallStatus.RESOURCE_EXHAUSTED.withDescription("busy")
                .toRuntimeException()));
        assertNull(FlightSqlProducer.memoryShortage(new IllegalArgumentException("Invalid SQL query")));
        assertNull(FlightSqlProducer.memoryShortage(CallStatus.INVALID_ARGUMENT.withDescription("bad")
                .toRuntimeException()));
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

    /** Verifies BeginTransaction is refused as UNIMPLEMENTED with a description. */
    @Test
    void beginTransactionIsRefusedWithADescription() {
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), mock(BufferAllocator.class),
                mock(MetadataService.class), mock(QueryPlanner.class),
                mock(ExecutionService.class), mock(ClusterService.class));
        @SuppressWarnings("unchecked")
        FlightProducer.StreamListener<FlightSql.ActionBeginTransactionResult> listener =
                mock(FlightProducer.StreamListener.class);

        producer.beginTransaction(FlightSql.ActionBeginTransactionRequest.getDefaultInstance(),
                mock(FlightProducer.CallContext.class), listener);

        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).onError(error.capture());
        FlightRuntimeException refused = assertInstanceOf(FlightRuntimeException.class, error.getValue());
        assertEquals(FlightStatusCode.UNIMPLEMENTED, refused.status().code());
        assertEquals(FlightSqlProducer.TRANSACTIONS_UNSUPPORTED, refused.status().description());
        verify(listener, never()).onCompleted();
    }

    /**
     * Regression for ADBC connects that asked for a transaction (TUNING.md): many clients asking
     * for SqlInfo FLIGHT_SQL_SERVER_TRANSACTION at once over Flight all read NONE, and a
     * BeginTransaction over the wire is UNIMPLEMENTED with the description.
     */
    @Test
    void concurrentClientsAllReadNoTransactionSupport() throws Exception {
        try (BufferAllocator allocator = new org.apache.arrow.memory.RootAllocator();
                FlightSqlProducer producer = new FlightSqlProducer(
                        null, allocator, mock(MetadataService.class), mock(QueryPlanner.class),
                        mock(ExecutionService.class), mock(ClusterService.class));
                org.apache.arrow.flight.FlightServer server = org.apache.arrow.flight.FlightServer
                        .builder(allocator, Location.forGrpcInsecure("localhost", 0), producer)
                        .build().start()) {
            Location location = Location.forGrpcInsecure("localhost", server.getPort());
            int clients = 16;
            java.util.concurrent.ExecutorService pool =
                    java.util.concurrent.Executors.newFixedThreadPool(clients);
            try {
                java.util.concurrent.CountDownLatch go = new java.util.concurrent.CountDownLatch(1);
                List<java.util.concurrent.Future<List<Object>>> answers = new java.util.ArrayList<>();
                for (int c = 0; c < clients; c++) {
                    answers.add(pool.submit(() -> {
                        go.await();
                        List<Object> values = new java.util.ArrayList<>();
                        for (int round = 0; round < 25; round++) {
                            try (org.apache.arrow.flight.FlightClient flight =
                                    org.apache.arrow.flight.FlightClient.builder(allocator, location).build();
                                    org.apache.arrow.flight.sql.FlightSqlClient client =
                                            new org.apache.arrow.flight.sql.FlightSqlClient(flight)) {
                                values.add(transactionSupport(client));
                            }
                        }
                        return values;
                    }));
                }
                go.countDown();
                for (java.util.concurrent.Future<List<Object>> answer : answers) {
                    for (Object value : answer.get(60, java.util.concurrent.TimeUnit.SECONDS)) {
                        assertEquals(FlightSql.SqlSupportedTransaction.SQL_SUPPORTED_TRANSACTION_NONE
                                .getNumber(), value);
                    }
                }
            } finally {
                pool.shutdownNow();
            }

            try (org.apache.arrow.flight.FlightClient flight =
                    org.apache.arrow.flight.FlightClient.builder(allocator, location).build();
                    org.apache.arrow.flight.sql.FlightSqlClient client =
                            new org.apache.arrow.flight.sql.FlightSqlClient(flight)) {
                FlightRuntimeException refused = org.junit.jupiter.api.Assertions.assertThrows(
                        FlightRuntimeException.class, client::beginTransaction);
                assertEquals(FlightStatusCode.UNIMPLEMENTED, refused.status().code());
                assertEquals(FlightSqlProducer.TRANSACTIONS_UNSUPPORTED, refused.status().description());
            }
        }
    }

    /**
     * Reads the FLIGHT_SQL_SERVER_TRANSACTION value the way ADBC's Flight SQL driver does at connect.
     *
     * @param client Flight SQL client
     * @return the value, or null when the answer has no such row
     * @throws Exception when the call fails
     */
    private static Object transactionSupport(org.apache.arrow.flight.sql.FlightSqlClient client)
            throws Exception {
        FlightInfo info = client.getSqlInfo(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_TRANSACTION);
        Object value = null;
        for (org.apache.arrow.flight.FlightEndpoint endpoint : info.getEndpoints()) {
            try (org.apache.arrow.flight.FlightStream stream = client.getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    org.apache.arrow.vector.UInt4Vector names =
                            (org.apache.arrow.vector.UInt4Vector) root.getVector(0);
                    for (int row = 0; row < root.getRowCount(); row++) {
                        if (names.get(row) == FlightSql.SqlInfo.FLIGHT_SQL_SERVER_TRANSACTION_VALUE) {
                            value = root.getVector(1).getObject(row);
                        }
                    }
                }
            }
        }
        return value;
    }

    /**
     * Verifies plain descriptors (SQL text, table path) are refused unless accepted, then go
     * through the same Ranger check as Flight SQL queries before anything is described.
     */
    @Test
    void plainDescriptorsAreOptInAndChecked() throws Exception {
        MetadataService metadataService = mock(MetadataService.class);
        ExecutionService executionService = mock(ExecutionService.class);
        FlightAuthorization authorization = mock(FlightAuthorization.class);
        doThrow(CallStatus.UNAUTHORIZED.toRuntimeException()).when(authorization)
                .requireQuery("alice", "select * from sales.orders");
        doThrow(CallStatus.UNAUTHORIZED.toRuntimeException()).when(authorization)
                .requireQuery("alice", "SELECT * FROM sales.orders");
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        when(context.peerIdentity()).thenReturn("alice");
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), mock(BufferAllocator.class),
                metadataService, mock(QueryPlanner.class), executionService,
                mock(ClusterService.class), authorization);
        FlightDescriptor text = FlightDescriptor.command(
                "select * from sales.orders".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        FlightDescriptor path = FlightDescriptor.path("sales.orders");

        FlightRuntimeException off = org.junit.jupiter.api.Assertions.assertThrows(
                FlightRuntimeException.class, () -> producer.getFlightInfo(context, text));
        assertEquals(FlightStatusCode.INVALID_ARGUMENT, off.status().code());
        verify(authorization, never()).requireQuery(anyString(), anyString());

        producer.acceptPlainCommands(true);
        for (FlightDescriptor descriptor : List.of(text, path)) {
            assertEquals(FlightStatusCode.UNAUTHORIZED, org.junit.jupiter.api.Assertions.assertThrows(
                    FlightRuntimeException.class, () -> producer.getFlightInfo(context, descriptor))
                    .status().code());
            assertEquals(FlightStatusCode.UNAUTHORIZED, org.junit.jupiter.api.Assertions.assertThrows(
                    FlightRuntimeException.class, () -> producer.getSchema(context, descriptor))
                    .status().code());
        }
        verify(authorization, times(2)).requireQuery("alice", "select * from sales.orders");
        verify(authorization, times(2)).requireQuery("alice", "SELECT * FROM sales.orders");
        verify(metadataService, never()).getQuerySchema(anyString());
        verify(executionService, never()).completeSchema(any(), anyString());
        assertEquals(FlightStatusCode.INVALID_ARGUMENT, org.junit.jupiter.api.Assertions.assertThrows(
                FlightRuntimeException.class, () -> producer.getFlightInfo(context,
                        FlightDescriptor.path("sales.orders;drop"))).status().code());
    }

    /**
     * Verifies a plain descriptor gets the complete answer even where Flight SQL clients get
     * parts by default, unless its client asks for parts, and Flight SQL commands keep the default.
     */
    @Test
    void plainDescriptorsGetCompleteAnswersUnlessPartsAreAsked() throws Exception {
        BufferAllocator allocator = mock(BufferAllocator.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        ExecutionService executionService = mock(ExecutionService.class);
        String query = "select sum(o_totalprice) from tpch.orders";
        Schema schema = new Schema(List.of(Field.nullable("sum", new ArrowType.FloatingPoint(
                org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE))));
        when(executionService.completeSchema(allocator, query, null)).thenReturn(schema);
        when(executionService.resultSchema(allocator, query)).thenReturn(schema);
        when(queryPlanner.planComplete(query, null)).thenReturn(new QueryPlan(List.of(), -1L, -1L));
        when(queryPlanner.plan(query)).thenReturn(new QueryPlan(List.of(), 10L, 1L));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), allocator,
                mock(MetadataService.class), queryPlanner, executionService,
                mock(ClusterService.class), "partial");
        producer.acceptPlainCommands(true);
        FlightProducer.CallContext plain = mock(FlightProducer.CallContext.class);
        FlightProducer.CallContext partial = mock(FlightProducer.CallContext.class);
        org.apache.arrow.flight.FlightCallHeaders headers = new org.apache.arrow.flight.FlightCallHeaders();
        headers.insert(ResultModeMiddleware.HEADER, "partial");
        when(partial.getMiddleware(ResultModeMiddleware.KEY))
                .thenReturn(new ResultModeMiddleware.Factory().onCallStarted(null, headers, null));
        FlightDescriptor text = FlightDescriptor.command(
                (" " + query + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8));

        FlightInfo info = producer.getFlightInfo(plain, text);
        assertEquals(schema, info.getSchema());
        assertEquals(text, info.getDescriptor());
        verify(queryPlanner).planComplete(query, null);
        verify(queryPlanner, never()).plan(query);
        assertEquals(schema, producer.getSchema(plain, text).getSchema());
        verify(executionService, times(2)).completeSchema(allocator, query, null);

        producer.getFlightInfo(partial, text);
        verify(queryPlanner).plan(query);

        // A Flight SQL command keeps its handling and the server's default (parts).
        FlightDescriptor flightSql = FlightDescriptor.command(com.google.protobuf.Any.pack(
                FlightSql.CommandStatementQuery.newBuilder().setQuery(query).build()).toByteArray());
        producer.getFlightInfo(plain, flightSql);
        verify(queryPlanner, times(2)).plan(query);
        verify(queryPlanner, times(1)).planComplete(query, null);
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

    /** Verifies CancelFlightInfo, sent as the Flight action, releases the load of each tracked endpoint. */
    @Test
    void cancelFlightInfoReleasesTrackedEndpointLoad() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);
        ByteString tracked = ByteString.copyFromUtf8("tracked");
        ByteString untracked = ByteString.copyFromUtf8("untracked");
        HandleState trackedState = HandleState.forServerFiles("select * from tpch.lineitem",
                new String[]{"tpch/lineitem/a.parquet"}, "grpc+tcp://n1:32010", 100L, true);
        HandleState untrackedState = HandleState.forServerFiles("select * from tpch.lineitem",
                new String[]{"tpch/lineitem/b.parquet"}, "grpc+tcp://n1:32010", 100L, false);
        when(clusterService.resolveEndpointHandle(tracked.toByteArray())).thenReturn(trackedState);
        when(clusterService.resolveEndpointHandle(untracked.toByteArray()))
                .thenReturn(untrackedState);
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), mock(BufferAllocator.class),
                mock(MetadataService.class), mock(QueryPlanner.class),
                mock(ExecutionService.class), clusterService);
        @SuppressWarnings("unchecked")
        FlightProducer.StreamListener<org.apache.arrow.flight.Result> listener =
                mock(FlightProducer.StreamListener.class);

        producer.doAction(mock(FlightProducer.CallContext.class), cancelAction(
                statementTicket(tracked), statementTicket(untracked)), listener);

        ArgumentCaptor<org.apache.arrow.flight.Result> result =
                ArgumentCaptor.forClass(org.apache.arrow.flight.Result.class);
        verify(listener).onNext(result.capture());
        verify(listener).onCompleted();
        assertEquals(org.apache.arrow.flight.CancelStatus.CANCELLED,
                org.apache.arrow.flight.CancelFlightInfoResult.deserialize(
                        java.nio.ByteBuffer.wrap(result.getValue().getBody())).getStatus());
        verify(clusterService).releaseEndpointLoad(tracked.toByteArray(), trackedState);
        verify(clusterService, never()).releaseEndpointLoad(eq(untracked.toByteArray()), any());
    }

    /** Verifies CancelFlightInfo refuses forged or foreign tickets and skips non-statement ones. */
    @Test
    void cancelFlightInfoRefusesInvalidTickets() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);
        ByteString forged = ByteString.copyFromUtf8("forged");
        when(clusterService.resolveEndpointHandle(forged.toByteArray()))
                .thenThrow(new IllegalArgumentException("bad signature"));
        FlightSqlProducer producer = new FlightSqlProducer(
                Location.forGrpcInsecure("localhost", 32010), mock(BufferAllocator.class),
                mock(MetadataService.class), mock(QueryPlanner.class),
                mock(ExecutionService.class), clusterService);

        assertEquals(FlightStatusCode.INVALID_ARGUMENT, cancelError(producer, statementTicket(forged)));
        assertEquals(FlightStatusCode.INVALID_ARGUMENT, cancelError(producer,
                new org.apache.arrow.flight.Ticket(new byte[]{1, 2, 3})));
        verify(clusterService, never()).releaseEndpointLoad(any(), any());

        org.apache.arrow.flight.Ticket metadata = new org.apache.arrow.flight.Ticket(
                com.google.protobuf.Any.pack(FlightSql.CommandGetCatalogs.getDefaultInstance())
                        .toByteArray());
        assertEquals(null, FlightSqlProducer.statementHandle(metadata));
    }

    private static org.apache.arrow.flight.Ticket statementTicket(ByteString handle) {
        return new org.apache.arrow.flight.Ticket(com.google.protobuf.Any.pack(
                FlightSql.TicketStatementQuery.newBuilder().setStatementHandle(handle).build())
                .toByteArray());
    }

    private static org.apache.arrow.flight.Action cancelAction(
            org.apache.arrow.flight.Ticket... tickets) {
        List<org.apache.arrow.flight.FlightEndpoint> endpoints = new java.util.ArrayList<>();
        for (org.apache.arrow.flight.Ticket ticket : tickets) {
            endpoints.add(new org.apache.arrow.flight.FlightEndpoint(ticket));
        }
        FlightInfo info = new FlightInfo(new Schema(List.of()),
                FlightDescriptor.command(new byte[0]), endpoints, -1, -1);
        java.nio.ByteBuffer body = new org.apache.arrow.flight.CancelFlightInfoRequest(info)
                .serialize();
        byte[] bytes = new byte[body.remaining()];
        body.get(bytes);
        return new org.apache.arrow.flight.Action(
                org.apache.arrow.flight.FlightConstants.CANCEL_FLIGHT_INFO.getType(), bytes);
    }

    private static FlightStatusCode cancelError(FlightSqlProducer producer,
            org.apache.arrow.flight.Ticket ticket) {
        @SuppressWarnings("unchecked")
        FlightProducer.StreamListener<org.apache.arrow.flight.Result> listener =
                mock(FlightProducer.StreamListener.class);
        producer.doAction(mock(FlightProducer.CallContext.class), cancelAction(ticket), listener);
        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).onError(error.capture());
        return assertInstanceOf(FlightRuntimeException.class, error.getValue()).status().code();
    }

    /** Verifies binary ticket handles give readable query ids in the log. */
    @Test
    void queryIdOfABinaryHandleIsHex() {
        assertEquals("1f0c2a9e", FlightSqlProducer.qid(com.google.protobuf.ByteString.copyFromUtf8("1f0c2a9e-77aa-4bcd")));
        org.junit.jupiter.api.Assertions.assertTrue(FlightSqlProducer.qid(com.google.protobuf.ByteString.copyFrom(new byte[] {'A', 'F', 'T', '1', 0, 0, 1, 2, 3}))
                .matches("[0-9a-f]{8}"));
        for (byte[] bytes : new byte[][] {{0}, {1, 2, 3}, {(byte) 0xff, 0, 0, 0, 7}, new byte[64]}) {
            com.google.protobuf.ByteString handle = com.google.protobuf.ByteString.copyFrom(bytes);
            assertEquals(String.format("%08x", java.util.Arrays.hashCode(bytes)), FlightSqlProducer.qid(handle));
        }
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

    /** Lists the statistics action after the Flight SQL and session actions. */
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
        assertEquals(true, types.containsAll(List.of(
                org.apache.arrow.flight.FlightConstants.SET_SESSION_OPTIONS.getType(),
                org.apache.arrow.flight.FlightConstants.GET_SESSION_OPTIONS.getType(),
                org.apache.arrow.flight.FlightConstants.CLOSE_SESSION.getType())));
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
