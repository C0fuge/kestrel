package pro.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests the listener that gives a stream the column names of the complete answer (item 33 (A)).
 */
@Tag("unit")
class RenamingStreamListenerTest {

    /**
     * Returns a schema of one BIGINT column.
     *
     * @param name column name
     * @return schema
     */
    private static Schema bigint(String name) {
        return new Schema(List.of(new Field(name, FieldType.nullable(new ArrowType.Int(64, true)), null)));
    }

    @Test
    void sendsTheBatchesUnderTheGivenNames() {
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot source = VectorSchemaRoot.create(bigint("count(*)"), allocator)) {
            FlightProducer.ServerStreamListener client = mock(FlightProducer.ServerStreamListener.class);
            AtomicReference<VectorSchemaRoot> started = new AtomicReference<>();
            List<String> sent = new ArrayList<>();
            doAnswer(i -> {
                started.set(i.getArgument(0));
                return null;
            }).when(client).start(any(), any(), any());
            doAnswer(i -> {
                VectorSchemaRoot root = started.get();
                sent.add(root.getSchema().getFields().get(0).getName() + "="
                        + ((BigIntVector) root.getVector(0)).get(0));
                return null;
            }).when(client).putNext();
            try (RenamingStreamListener named = new RenamingStreamListener(client, allocator,
                    bigint("count_star()"))) {
                named.start(source, null, IpcOption.DEFAULT);
                for (long value : new long[] {7, 9}) {
                    BigIntVector v = (BigIntVector) source.getVector(0);
                    v.allocateNew(1);
                    v.set(0, value);
                    source.setRowCount(1);
                    named.putNext();
                }
                named.completed();
            }
            assertEquals(List.of("count_star()=7", "count_star()=9"), sent);
            verify(client).completed();
        }
    }

    @Test
    void refusesOtherTypes() {
        Schema text = new Schema(List.of(Field.nullable("n", ArrowType.Utf8.INSTANCE)));
        assertTrue(RenamingStreamListener.sameTypes(bigint("a"), bigint("b")));
        assertFalse(RenamingStreamListener.sameTypes(bigint("a"), text));
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot source = VectorSchemaRoot.create(text, allocator);
                RenamingStreamListener named = new RenamingStreamListener(
                        mock(FlightProducer.ServerStreamListener.class), allocator, bigint("n"))) {
            assertThrows(IllegalStateException.class, () -> named.start(source, null, IpcOption.DEFAULT));
        }
    }
}
