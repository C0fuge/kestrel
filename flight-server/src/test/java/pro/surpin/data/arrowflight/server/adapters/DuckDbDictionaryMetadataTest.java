package pro.surpin.data.arrowflight.server.adapters;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DictionaryUtility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.duckdb.DuckDbSetupPlan;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.model.HandleState;
import pro.surpin.data.arrowflight.server.model.QueryPlan;
import pro.surpin.data.arrowflight.server.services.CatalogViews;
import pro.surpin.data.arrowflight.server.services.ClusterService;
import pro.surpin.data.arrowflight.server.services.ExecutionService;
import pro.surpin.data.arrowflight.server.services.MetadataService;
import pro.surpin.data.arrowflight.server.services.QueryPlanner;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Checks result metadata uses dictionary value types before and after Flight transport. */
@Tag("unit")
class DuckDbDictionaryMetadataTest {
    /**
     * Checks statement and prepared metadata agree with a multi-batch enum stream.
     *
     * @throws Exception on native execution or transport failure
     */
    @Test
    void enumMetadataMatchesStream() throws Exception {
        check("SELECT CASE i WHEN 0 THEN 'a' ELSE 'b' END::ENUM('a','b') AS e FROM range(2) t(i)", 2);
    }

    /**
     * Checks an empty enum stream retains its value type without reading a data batch.
     *
     * @throws Exception on native execution or transport failure
     */
    @Test
    void emptyEnumMetadataMatchesStream() throws Exception {
        check("SELECT 'a'::ENUM('a','b') AS e WHERE false", 0);
    }

    /**
     * Checks dictionary fields nested inside lists receive message-format types.
     *
     * @throws Exception on native execution or transport failure
     */
    @Test
    void nestedEnumMetadataMatchesStream() throws Exception {
        check("SELECT ['a'::ENUM('a','b'), NULL] AS es", 1);
    }

    /**
     * Checks both result modes serialize native descriptions through every metadata endpoint.
     *
     * @param sql fixed native query
     * @param expectedRows expected rows transferred by DoGet
     * @throws Exception on setup, native execution, transport or teardown failure
     */
    private static void check(String sql, int expectedRows) throws Exception {
        assertAll("result modes",
                () -> checkMode(sql, expectedRows, "complete"),
                () -> checkMode(sql, expectedRows, "partial"));
    }

    /**
     * Runs real Flight SQL metadata and DoGet using fixed native inputs in one result mode.
     *
     * @param sql fixed native query
     * @param expectedRows expected rows transferred by DoGet
     * @param mode complete or partial result mode
     * @throws Exception on setup, execution, transport or teardown failure
     */
    private static void checkMode(String sql, int expectedRows, String mode) throws Exception {
        var pool = Executors.newSingleThreadExecutor();
        try (var allocator = new RootAllocator(64L << 20);
                var adapter = new DuckDbAdapter(config(), pool, DuckDbSetupPlan.of(List.of(), List.of()))) {
            ExecutionService execution = mock(ExecutionService.class);
            QueryPlanner planner = mock(QueryPlanner.class);
            ClusterService cluster = mock(ClusterService.class);
            var views = new CatalogViews.Snapshot(1, List.of(), List.of(), List.of());
            when(execution.completeSchema(eq(allocator), eq(sql), isNull()))
                    .thenAnswer(call -> adapter.describeComplete(allocator, sql, views));
            when(execution.completeParameterSchema(sql)).thenReturn(new Schema(List.of()));
            when(execution.resultSchema(allocator, sql)).thenAnswer(call -> adapter.describeSql(allocator, sql));
            when(execution.completeFromCopies(eq(sql), any()))
                    .thenReturn(new ExecutionService.CopiedQuery(sql, null));
            doAnswer(call -> {
                adapter.streamSql(allocator, sql, call.getArgument(3), true);
                return null;
            }).when(execution).readComplete(eq(allocator), eq(sql), isNull(), any(), eq(true), anyBoolean());
            doAnswer(call -> {
                adapter.streamSql(allocator, sql, call.getArgument(3), true);
                return null;
            }).when(execution).readParquet(eq(allocator), eq(sql), any(String[].class), any(),
                    eq(true), any());
            when(cluster.resolveEndpointHandle(any())).thenReturn(new HandleState(sql,
                    new String[0], null, 0, false, mode.equals("complete")));
            var ticket = new Ticket(Any.pack(FlightSql.TicketStatementQuery.newBuilder()
                    .setStatementHandle(ByteString.copyFromUtf8("enum-fixture")).build()).toByteArray());
            var plan = new QueryPlan(List.of(new FlightEndpoint(ticket)), -1, -1);
            when(planner.planComplete(eq(sql), isNull())).thenReturn(plan);
            when(planner.plan(sql)).thenReturn(plan);
            try (var producer = new FlightSqlProducer(null, allocator, mock(MetadataService.class),
                    planner, execution, cluster, mode);
                    var server = FlightServer.builder(allocator,
                            Location.forGrpcInsecure("localhost", 0), producer).build().start();
                    var client = FlightClient.builder(allocator,
                            Location.forGrpcInsecure("localhost", server.getPort())).build();
                    var sqlClient = new FlightSqlClient(client);
                    var prepared = sqlClient.prepare(sql)) {
                Schema statementSchema = sqlClient.getExecuteSchema(sql).getSchema();
                Schema statementInfo = sqlClient.execute(sql).getSchemaOptional().orElseThrow();
                Schema preparedSchema = prepared.getResultSetSchema();
                Schema preparedFetch = prepared.fetchSchema().getSchema();
                Schema preparedInfo = prepared.execute().getSchemaOptional().orElseThrow();
                try (var stream = client.getStream(ticket)) {
                    int rows = 0;
                    while (stream.next()) {
                        rows += stream.getRoot().getRowCount();
                    }
                    Set<Long> ids = new HashSet<>();
                    Schema wire = new Schema(stream.getRoot().getSchema().getFields().stream()
                            .map(field -> DictionaryUtility.toMessageFormat(
                                    field, stream.getDictionaryProvider(), ids)).toList(),
                            stream.getRoot().getSchema().getCustomMetadata());
                    assertValueType(wire.getFields().get(0));
                    assertAll("metadata must match DoGet's dictionary value schema",
                            () -> assertEquals(wire, statementSchema, "GetSchema statement"),
                            () -> assertEquals(wire, statementInfo, "GetFlightInfo statement"),
                            () -> assertEquals(wire, preparedSchema, "prepared dataset_schema"),
                            () -> assertEquals(wire, preparedFetch, "GetSchema prepared"),
                            () -> assertEquals(wire, preparedInfo, "GetFlightInfo prepared"));
                    assertEquals(expectedRows, rows);
                }
                if (mode.equals("partial")) {
                    verify(execution, atLeastOnce()).resultSchema(allocator, sql);
                } else {
                    verify(execution, atLeastOnce()).completeSchema(allocator, sql, null);
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Requires a value-type enum field, including the child of a nested list.
     *
     * @param field result field
     */
    private static void assertValueType(Field field) {
        if (field.getType() instanceof ArrowType.List) {
            assertValueType(field.getChildren().get(0));
        } else {
            assertNotNull(field.getDictionary());
            assertInstanceOf(ArrowType.Utf8.class, field.getType());
            assertEquals(new ArrowType.Int(8, false), field.getDictionary().getIndexType());
        }
    }

    /**
     * Checks conversion preserves metadata and dictionary encoding without reading any batch.
     *
     * @throws Exception on Arrow allocation or schema access failure
     */
    @Test
    void conversionPreservesMetadataWithoutLoadingBatches() throws Exception {
        try (var allocator = new RootAllocator(1L << 20);
                var values = new VarCharVector("values", allocator)) {
            var encoding = new DictionaryEncoding(7, true, new ArrowType.Int(8, false));
            var field = new Field("e", new FieldType(true, new ArrowType.Int(8, false), encoding,
                    Map.of("field-key", "field-value")), List.of());
            var memory = new Schema(List.of(field), Map.of("schema-key", "schema-value"));
            try (var root = VectorSchemaRoot.create(memory, allocator)) {
                ArrowReader reader = mock(ArrowReader.class);
                when(reader.getVectorSchemaRoot()).thenReturn(root);
                when(reader.getDictionaryIds()).thenReturn(Set.of(7L));
                when(reader.lookup(7)).thenReturn(new Dictionary(values, encoding));
                Schema converted = DuckDbAdapter.messageSchema(reader);
                assertEquals(memory.getCustomMetadata(), converted.getCustomMetadata());
                assertEquals(field.getMetadata(), converted.getFields().get(0).getMetadata());
                assertEquals(encoding, converted.getFields().get(0).getDictionary());
                assertInstanceOf(ArrowType.Utf8.class, converted.getFields().get(0).getType());
                verify(reader, never()).loadNextBatch();
            }
        }
    }

    /**
     * Checks plain result descriptions retain their schema instance and metadata.
     *
     * @throws Exception on Arrow allocation or schema access failure
     */
    @Test
    void plainSchemaRemainsUnchanged() throws Exception {
        try (var allocator = new RootAllocator(1L << 20);
                var root = VectorSchemaRoot.create(new Schema(List.of(Field.nullable("n",
                        new ArrowType.Int(32, true))), Map.of("schema-key", "schema-value")), allocator)) {
            ArrowReader reader = mock(ArrowReader.class);
            when(reader.getVectorSchemaRoot()).thenReturn(root);
            when(reader.getDictionaryIds()).thenReturn(Set.of());
            assertSame(root.getSchema(), DuckDbAdapter.messageSchema(reader));
            verify(reader, never()).loadNextBatch();
        }
    }

    /**
     * Checks a cached enum description is refreshed when the current view changes type.
     *
     * @throws Exception on native execution or teardown failure
     */
    @Test
    void currentViewsReplaceCachedDictionarySchema() throws Exception {
        var pool = Executors.newSingleThreadExecutor();
        try (var allocator = new RootAllocator(64L << 20);
                var adapter = new DuckDbAdapter(config(), pool, DuckDbSetupPlan.of(List.of(), List.of()))) {
            String query = "SELECT e FROM s.t";
            var enums = views(1, "'a'::ENUM('a','b')");
            var text = views(2, "'a'::VARCHAR");
            Schema first = adapter.describeComplete(allocator, query, enums);
            assertValueType(first.getFields().get(0));
            assertSame(first, adapter.describeComplete(allocator, query, enums));
            Schema changed = adapter.describeComplete(allocator, query, text);
            assertInstanceOf(ArrowType.Utf8.class, changed.getFields().get(0).getType());
            assertNull(changed.getFields().get(0).getDictionary());
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Creates isolated current views for a fixed type expression.
     *
     * @param version snapshot version
     * @param expression fixed SQL expression
     * @return matching data and empty view definitions
     */
    private static CatalogViews.Snapshot views(long version, String expression) {
        List<String> statements = List.of("CREATE SCHEMA IF NOT EXISTS s",
                "CREATE OR REPLACE VIEW s.t AS SELECT " + expression + " AS e");
        return new CatalogViews.Snapshot(version, statements, statements, List.of("DROP VIEW IF EXISTS s.t"));
    }

    /**
     * Builds a single-thread native configuration with one-row export batches.
     *
     * @return isolated adapter configuration
     */
    private static AppConfig config() {
        return new AppConfig(1, 1, 1, 131072, 0, 1, 1,
                null, false, null, null, null, null, "128MB", false,
                64L << 20, 1048576, 67108864, 5000L, null, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 4, System.getProperty("java.io.tmpdir"), null,
                "complete", true, true);
    }
}
