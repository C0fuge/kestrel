package pro.surpin.data.arrowflight.server.adapters;

import com.google.protobuf.ByteString;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import pro.surpin.data.arrowflight.server.services.ClusterService;
import pro.surpin.data.arrowflight.server.services.ExecutionService;
import pro.surpin.data.arrowflight.server.services.MetadataService;
import pro.surpin.data.arrowflight.server.services.QueryPlanner;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Checks that prepared result metadata uses the same bindings as execution. */
@Tag("unit")
class PreparedStatementSchemaTest {
    private static final String SQL = "select ? as value";
    private final BufferAllocator allocator = mock(BufferAllocator.class);
    private final ExecutionService execution = mock(ExecutionService.class);
    private final ClusterService cluster = mock(ClusterService.class);
    private final Schema schema = new Schema(List.of(Field.nullable("value", new ArrowType.Int(64, true))));
    private final FlightSqlProducer producer = new FlightSqlProducer(
            Location.forGrpcInsecure("localhost", 32110), allocator, mock(MetadataService.class),
            mock(QueryPlanner.class), execution, cluster, "complete");

    /** A signed bound handle supplies its values to schema inference without a cluster lookup. */
    @Test
    void schemaUsesEmbeddedParameters() {
        byte[] values = {1, 2, 3};
        when(execution.completeSchema(allocator, SQL, values)).thenReturn(schema);
        assertEquals(schema, producer.getSchemaPreparedStatement(command(QueryParameters.boundHandle(SQL, values)),
                null, FlightDescriptor.command(new byte[]{1})).getSchema());
        verify(execution).completeSchema(allocator, SQL, values);
        verify(cluster, never()).recallParameters(anyString());
    }

    /** Clients retaining the original statement handle get metadata for their saved values. */
    @Test
    void schemaUsesClusterBindings() {
        ByteString handle = QueryParameters.statementHandle(SQL);
        String id = QueryParameters.parseHandle(handle).statementId();
        byte[] values = {4, 5, 6};
        when(cluster.recallParameters(id)).thenReturn(values);
        when(execution.completeSchema(allocator, SQL, values)).thenReturn(schema);
        assertEquals(schema, producer.getSchemaPreparedStatement(command(handle), null,
                FlightDescriptor.command(new byte[]{1})).getSchema());
        verify(cluster).recallParameters(id);
        verify(execution).completeSchema(allocator, SQL, values);
    }

    /** An unbound statement preserves the existing null-parameter describe path. */
    @Test
    void unboundSchemaRemainsSupported() {
        when(execution.completeSchema(allocator, SQL, null)).thenReturn(schema);
        assertEquals(schema, producer.getSchemaPreparedStatement(command(ByteString.copyFromUtf8(SQL)), null,
                FlightDescriptor.command(new byte[]{1})).getSchema());
        verify(cluster, never()).recallParameters(anyString());
    }

    /** Multiple parameter rows are refused before serializing an unsupported batch. */
    @Test
    void rejectsMultipleRowsBeforeSerialization() {
        FlightStream stream = mock(FlightStream.class);
        VectorSchemaRoot root = mock(VectorSchemaRoot.class);
        when(stream.next()).thenReturn(true, false);
        when(stream.getRoot()).thenReturn(root);
        when(root.getRowCount()).thenReturn(2);
        @SuppressWarnings("unchecked")
        FlightProducer.StreamListener<PutResult> listener = mock(FlightProducer.StreamListener.class);
        producer.acceptPutPreparedStatementQuery(command(ByteString.copyFromUtf8(SQL)), null, stream, listener).run();
        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).onError(error.capture());
        FlightRuntimeException failure = assertInstanceOf(FlightRuntimeException.class, error.getValue());
        assertEquals(FlightStatusCode.INVALID_ARGUMENT, failure.status().code());
        assertTrue(failure.status().description().contains("Exactly one set of parameter values"));
        verify(root, never()).getSchema();
        verify(cluster, never()).rememberParameters(anyString(), any());
    }

    /**
     * Wraps a prepared statement handle in a protocol command.
     *
     * @param handle statement handle
     * @return prepared statement command
     */
    private static FlightSql.CommandPreparedStatementQuery command(ByteString handle) {
        return FlightSql.CommandPreparedStatementQuery.newBuilder().setPreparedStatementHandle(handle).build();
    }
}
