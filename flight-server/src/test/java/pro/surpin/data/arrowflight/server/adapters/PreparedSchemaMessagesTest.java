package pro.surpin.data.arrowflight.server.adapters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import pro.surpin.data.arrowflight.server.FlightAuthorization;
import pro.surpin.data.arrowflight.server.services.ClusterService;
import pro.surpin.data.arrowflight.server.services.ExecutionService;
import pro.surpin.data.arrowflight.server.services.MetadataService;
import pro.surpin.data.arrowflight.server.services.QueryPlanner;

/** Checks exact prepared-schema IPC reuse and its retention and authorization boundaries. */
@Tag("unit")
class PreparedSchemaMessagesTest {
    private static final String PROPERTY = "arrowflight.experimental.cachePreparedSchemaMessages";
    private static final String SQL = "SELECT id FROM s.t";

    /** Reuses equal complete schemas without changing nested types, dictionaries or metadata. */
    @Test
    void preservesWireEncodingAndCompleteSchemaEquality() {
        PreparedSchemaMessages cache = new PreparedSchemaMessages();
        Field dictionary = new Field("слово", new FieldType(true, ArrowType.Utf8.INSTANCE,
                new DictionaryEncoding(7, false, new ArrowType.Int(32, true)), Map.of("unit", "words")), null);
        Field list = new Field("nested", FieldType.nullable(ArrowType.List.INSTANCE), List.of(dictionary));
        Schema schema = new Schema(List.of(list), Map.of("source", "fixture"));
        ByteString encoded = cache.encode(schema);
        assertEquals(ByteString.copyFrom(schema.serializeAsMessage()), encoded);
        assertEquals(schema, Schema.deserializeMessage(encoded.asReadOnlyByteBuffer()));
        assertSame(encoded, cache.encode(new Schema(List.of(list), Map.of("source", "fixture"))));
        for (Schema changed : List.of(
                new Schema(List.of(list), Map.of("source", "other")),
                new Schema(List.of(Field.notNullable("nested", ArrowType.List.INSTANCE))),
                new Schema(List.of(new Field("слово", new FieldType(true, ArrowType.Utf8.INSTANCE,
                        new DictionaryEncoding(8, false, new ArrowType.Int(32, true)), Map.of("unit", "words")), null))),
                new Schema(List.of(Field.nullable("id", new ArrowType.Int(32, true)))),
                new Schema(List.of(Field.nullable("id", new ArrowType.Int(64, true)))))) {
            ByteString other = cache.encode(changed);
            assertNotEquals(encoded, other);
            assertEquals(ByteString.copyFrom(changed.serializeAsMessage()), other);
            assertEquals(changed, Schema.deserializeMessage(other.asReadOnlyByteBuffer()));
        }
        Schema empty = new Schema(List.of());
        assertEquals(ByteString.copyFrom(empty.serializeAsMessage()), cache.encode(empty));
    }

    /** Distinguishes identical field names with changed metadata or dictionary encodings. */
    @Test
    void metadataAndDictionaryDetailsNeverAlias() {
        PreparedSchemaMessages cache = new PreparedSchemaMessages();
        DictionaryEncoding dictionary = new DictionaryEncoding(7, false, new ArrowType.Int(32, true));
        Schema base = new Schema(List.of(new Field("value", new FieldType(true,
                ArrowType.Utf8.INSTANCE, dictionary, Map.of("unit", "words")), null)));
        ByteString encoded = cache.encode(base);
        for (FieldType type : List.of(
                new FieldType(false, ArrowType.Utf8.INSTANCE, dictionary, Map.of("unit", "words")),
                new FieldType(true, ArrowType.Utf8.INSTANCE, dictionary, Map.of("unit", "other")),
                new FieldType(true, ArrowType.Utf8.INSTANCE,
                        new DictionaryEncoding(8, false, new ArrowType.Int(32, true)), Map.of("unit", "words")),
                new FieldType(true, ArrowType.Utf8.INSTANCE,
                        new DictionaryEncoding(7, true, new ArrowType.Int(32, true)), Map.of("unit", "words")),
                new FieldType(true, ArrowType.Utf8.INSTANCE,
                        new DictionaryEncoding(7, false, new ArrowType.Int(64, true)), Map.of("unit", "words")))) {
            Schema changed = new Schema(List.of(new Field("value", type, null)));
            ByteString result = cache.encode(changed);
            assertNotEquals(encoded, result);
            assertEquals(changed, Schema.deserializeMessage(result.asReadOnlyByteBuffer()));
        }
    }

    /** Evicts by recency and by retained bytes, and releases entries when cleared. */
    @Test
    void boundsBothEntriesAndEncodingBytes() {
        Schema first = schema("first");
        Schema second = schema("second");
        Schema third = schema("third");
        PreparedSchemaMessages entries = new PreparedSchemaMessages(2, 4096);
        ByteString a = entries.encode(first);
        ByteString b = entries.encode(second);
        assertSame(a, entries.encode(first));
        entries.encode(third);
        assertSame(a, entries.encode(first));
        assertNotSame(b, entries.encode(second));
        int byteLimit = first.serializeAsMessage().length + second.serializeAsMessage().length - 1;
        PreparedSchemaMessages bytes = new PreparedSchemaMessages(16, byteLimit);
        ByteString prior = bytes.encode(first);
        bytes.encode(second);
        assertNotSame(prior, bytes.encode(first));
        ByteString latest = bytes.encode(first);
        bytes.clear();
        assertNotSame(latest, bytes.encode(first));
    }

    /** Does not retain encodings above either the individual or aggregate byte limit. */
    @Test
    void oversizedSchemasBypassRetention() {
        Schema large = new Schema(List.of(), Map.of("large", "x".repeat(65536)));
        PreparedSchemaMessages cache = new PreparedSchemaMessages();
        ByteString first = cache.encode(large);
        assertNotSame(first, cache.encode(large));
        assertEquals(ByteString.copyFrom(large.serializeAsMessage()), first);
        PreparedSchemaMessages tiny = new PreparedSchemaMessages(16, 1);
        Schema small = schema("small");
        assertNotSame(tiny.encode(small), tiny.encode(small));
    }

    /** Shares immutable encodings under concurrent misses, hits and bounded eviction. */
    @Test
    void concurrentEncodingsRemainExact() throws Exception {
        PreparedSchemaMessages cache = new PreparedSchemaMessages(4, 4096);
        var pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<ByteString>> results = new ArrayList<>();
            Schema repeated = schema("shared");
            for (int i = 0; i < 64; i++) {
                results.add(pool.submit(() -> cache.encode(repeated)));
            }
            ByteString shared = results.getFirst().get(5, TimeUnit.SECONDS);
            for (Future<ByteString> result : results) {
                assertSame(shared, result.get(5, TimeUnit.SECONDS));
            }
            results.clear();
            for (int i = 0; i < 64; i++) {
                Schema different = schema("field" + i);
                results.add(pool.submit(() -> {
                    ByteString encoded = cache.encode(different);
                    assertEquals(ByteString.copyFrom(different.serializeAsMessage()), encoded);
                    return encoded;
                }));
            }
            for (Future<ByteString> result : results) {
                result.get(5, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /** Leaves the default path uncached and rechecks access and current schemas when enabled. */
    @Test
    void producerResolvesAccessAndSchemasBeforeEncoding() throws Exception {
        String previous = System.getProperty(PROPERTY);
        try {
            for (boolean enabled : new boolean[] {false, true}) {
                if (enabled) {
                    System.setProperty(PROPERTY, "true");
                } else {
                    System.clearProperty(PROPERTY);
                }
                BufferAllocator allocator = mock(BufferAllocator.class);
                ExecutionService execution = mock(ExecutionService.class);
                FlightAuthorization authorization = mock(FlightAuthorization.class);
                Schema current = spy(schema("id"));
                Schema parameters = new Schema(List.of());
                when(execution.completeSchema(eq(allocator), eq(SQL), any())).thenReturn(current);
                when(execution.completeParameterSchema(SQL)).thenReturn(parameters);
                try (FlightSqlProducer producer = new FlightSqlProducer(
                        Location.forGrpcInsecure("localhost", 32010), allocator,
                        mock(MetadataService.class), mock(QueryPlanner.class), execution,
                        mock(ClusterService.class), authorization, "complete")) {
                    assertEquals(schema("id"), decode(prepare(producer, null)));
                    assertEquals(schema("id"), decode(prepare(producer, null)));
                    verify(current, times(enabled ? 1 : 2)).serializeAsMessage();
                    Schema changed = schema("changed");
                    when(execution.completeSchema(eq(allocator), eq(SQL), any())).thenReturn(changed);
                    assertEquals(changed, decode(prepare(producer, null)));
                    verify(execution, times(3)).completeSchema(allocator, SQL, null);
                    FlightProducer.CallContext denied = mock(FlightProducer.CallContext.class);
                    when(denied.peerIdentity()).thenReturn("denied-user");
                    doThrow(CallStatus.UNAUTHORIZED.toRuntimeException()).when(authorization)
                            .requireQuery("denied-user", SQL);
                    @SuppressWarnings("unchecked")
                    FlightProducer.StreamListener<Result> listener = mock(FlightProducer.StreamListener.class);
                    producer.createPreparedStatement(request(), denied, listener);
                    ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
                    verify(listener).onError(error.capture());
                    assertEquals(FlightStatusCode.UNAUTHORIZED,
                            ((FlightRuntimeException) error.getValue()).status().code());
                    verify(execution, times(3)).completeSchema(allocator, SQL, null);
                }
            }
        } finally {
            if (previous == null) {
                System.clearProperty(PROPERTY);
            } else {
                System.setProperty(PROPERTY, previous);
            }
        }
    }

    /**
     * Makes a small distinct schema.
     *
     * @param name field name
     * @return schema
     */
    private static Schema schema(String name) {
        return new Schema(List.of(Field.nullable(name, new ArrowType.Int(64, true))));
    }

    /**
     * Makes a prepare request.
     *
     * @return request
     */
    private static FlightSql.ActionCreatePreparedStatementRequest request() {
        return FlightSql.ActionCreatePreparedStatementRequest.newBuilder().setQuery(SQL).build();
    }

    /**
     * Captures one prepare response.
     *
     * @param producer producer under test
     * @param context call context
     * @return response
     * @throws Exception when the response cannot be decoded
     */
    private static FlightSql.ActionCreatePreparedStatementResult prepare(FlightSqlProducer producer,
            FlightProducer.CallContext context) throws Exception {
        @SuppressWarnings("unchecked")
        FlightProducer.StreamListener<Result> listener = mock(FlightProducer.StreamListener.class);
        producer.createPreparedStatement(request(), context, listener);
        ArgumentCaptor<Result> result = ArgumentCaptor.forClass(Result.class);
        verify(listener).onNext(result.capture());
        return Any.parseFrom(result.getValue().getBody()).unpack(FlightSql.ActionCreatePreparedStatementResult.class);
    }

    /**
     * Decodes the dataset schema of a prepare response.
     *
     * @param response prepare response
     * @return result schema
     */
    private static Schema decode(FlightSql.ActionCreatePreparedStatementResult response) {
        return Schema.deserializeMessage(response.getDatasetSchema().asReadOnlyByteBuffer());
    }
}
