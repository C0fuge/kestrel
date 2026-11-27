package pro.surpin.data.arrowflight.server.adapters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import pro.surpin.data.arrowflight.server.FlightAuthorization;
import pro.surpin.data.arrowflight.server.services.ClusterService;
import pro.surpin.data.arrowflight.server.services.ExecutionService;
import pro.surpin.data.arrowflight.server.services.MetadataService;
import pro.surpin.data.arrowflight.server.services.QueryPlanner;

/** Checks sharing only the canonical empty parameter schema without caching resolved metadata. */
@Tag("unit")
class EmptyParameterSchemaTest {
    private static final String PROPERTY = "arrowflight.experimental.shareEmptyParameterSchema";
    private static final String SQL = "SELECT id FROM s.t";

    /** Shares only empty fields and metadata, preserving fresh nonempty and metadata-only IPC. */
    @Test
    void fastPathRequiresEmptyFieldsAndMetadata() throws Exception {
        for (boolean enabled : new boolean[] {false, true}) {
            try (FlightSqlProducer producer = producer(enabled, false, mock(BufferAllocator.class),
                    mock(ExecutionService.class), mock(FlightAuthorization.class))) {
                Schema empty = spy(new Schema(List.of()));
                ByteString first = producer.parameterSchemaMessage(empty);
                ByteString second = producer.parameterSchemaMessage(empty);
                if (enabled) {
                    assertSame(first, second);
                } else {
                    assertNotSame(first, second);
                }
                verify(empty, times(enabled ? 0 : 2)).serializeAsMessage();
                assertEquals(ByteString.copyFrom(new Schema(List.of()).serializeAsMessage()), first);
                assertEquals(new Schema(List.of()), Schema.deserializeMessage(first.asReadOnlyByteBuffer()));
                for (Schema resolved : List.of(
                        new Schema(List.of(), Map.of("source", "fixture")),
                        schema("value", 32), schema("value", 64))) {
                    ByteString encoded = producer.parameterSchemaMessage(resolved);
                    assertNotSame(encoded, producer.parameterSchemaMessage(resolved));
                    assertEquals(ByteString.copyFrom(resolved.serializeAsMessage()), encoded);
                    assertEquals(resolved, Schema.deserializeMessage(encoded.asReadOnlyByteBuffer()));
                }
                assertNotEquals(producer.parameterSchemaMessage(schema("value", 32)),
                        producer.parameterSchemaMessage(schema("value", 64)));
            }
        }
    }

    /** Resolves current schemas on every prepare and refuses denied calls before metadata lookup. */
    @Test
    void currentResolutionAndAuthorizationPrecedeTheShortcut() throws Exception {
        for (boolean enabled : new boolean[] {false, true}) {
            BufferAllocator allocator = mock(BufferAllocator.class);
            ExecutionService execution = mock(ExecutionService.class);
            FlightAuthorization authorization = mock(FlightAuthorization.class);
            Schema dataset = spy(schema("id", 64));
            Schema parameters = spy(new Schema(List.of()));
            when(execution.completeSchema(eq(allocator), eq(SQL), any())).thenReturn(dataset);
            when(execution.completeParameterSchema(SQL)).thenReturn(parameters);
            try (FlightSqlProducer producer = producer(enabled, true, allocator, execution, authorization)) {
                for (int call = 0; call < 2; call++) {
                    FlightSql.ActionCreatePreparedStatementResult result = prepare(producer, null);
                    assertEquals(schema("id", 64), Schema.deserializeMessage(result.getDatasetSchema()
                            .asReadOnlyByteBuffer()));
                    assertEquals(new Schema(List.of()), Schema.deserializeMessage(result.getParameterSchema()
                            .asReadOnlyByteBuffer()));
                }
                verify(dataset, times(2)).serializeAsMessage();
                verify(parameters, times(enabled ? 0 : 2)).serializeAsMessage();
                Schema changed = schema("changed", 32);
                Schema metadata = new Schema(List.of(), Map.of("source", "new"));
                when(execution.completeSchema(eq(allocator), eq(SQL), any())).thenReturn(changed);
                when(execution.completeParameterSchema(SQL)).thenReturn(metadata);
                FlightSql.ActionCreatePreparedStatementResult result = prepare(producer, null);
                assertEquals(ByteString.copyFrom(changed.serializeAsMessage()), result.getDatasetSchema());
                assertEquals(ByteString.copyFrom(metadata.serializeAsMessage()), result.getParameterSchema());
                verify(execution, times(3)).completeSchema(allocator, SQL, null);
                verify(execution, times(3)).completeParameterSchema(SQL);
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
                verify(execution, times(3)).completeParameterSchema(SQL);
            }
        }
    }

    /** Preserves the partial-mode empty parameter message and current dataset lookup. */
    @Test
    void partialPreparesKeepTheirWireSchema() throws Exception {
        for (boolean enabled : new boolean[] {false, true}) {
            BufferAllocator allocator = mock(BufferAllocator.class);
            ExecutionService execution = mock(ExecutionService.class);
            Schema dataset = schema("id", 64);
            when(execution.resultSchema(allocator, SQL)).thenReturn(dataset);
            try (FlightSqlProducer producer = producer(enabled, false, allocator, execution,
                    mock(FlightAuthorization.class))) {
                for (int call = 0; call < 2; call++) {
                    FlightSql.ActionCreatePreparedStatementResult result = prepare(producer, null);
                    assertEquals(ByteString.copyFrom(dataset.serializeAsMessage()), result.getDatasetSchema());
                    assertEquals(ByteString.copyFrom(new Schema(List.of()).serializeAsMessage()),
                            result.getParameterSchema());
                }
                verify(execution, times(2)).resultSchema(allocator, SQL);
                verify(execution, never()).completeParameterSchema(any());
            }
        }
    }

    /**
     * Constructs a producer while restoring the process property immediately afterwards.
     *
     * @param enabled whether the shortcut is enabled
     * @param complete whether complete mode is the default
     * @param allocator Arrow allocator
     * @param execution execution service
     * @param authorization access provider
     * @return producer under test
     */
    private static FlightSqlProducer producer(boolean enabled, boolean complete, BufferAllocator allocator,
            ExecutionService execution, FlightAuthorization authorization) {
        String previous = System.getProperty(PROPERTY);
        try {
            if (enabled) {
                System.setProperty(PROPERTY, "true");
            } else {
                System.clearProperty(PROPERTY);
            }
            return new FlightSqlProducer(Location.forGrpcInsecure("localhost", 32010), allocator,
                    mock(MetadataService.class), mock(QueryPlanner.class), execution,
                    mock(ClusterService.class), authorization, complete ? "complete" : "partial");
        } finally {
            if (previous == null) {
                System.clearProperty(PROPERTY);
            } else {
                System.setProperty(PROPERTY, previous);
            }
        }
    }

    /**
     * Creates a typed one-field schema.
     *
     * @param name field name
     * @param bits integer width
     * @return schema
     */
    private static Schema schema(String name, int bits) {
        return new Schema(List.of(Field.nullable(name, new ArrowType.Int(bits, true))));
    }

    /**
     * Creates a prepare request.
     *
     * @return request
     */
    private static FlightSql.ActionCreatePreparedStatementRequest request() {
        return FlightSql.ActionCreatePreparedStatementRequest.newBuilder().setQuery(SQL).build();
    }

    /**
     * Captures a prepare response.
     *
     * @param producer producer under test
     * @param context call context
     * @return response
     * @throws Exception when decoding fails
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
}
