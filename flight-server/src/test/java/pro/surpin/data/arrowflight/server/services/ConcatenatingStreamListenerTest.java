package pro.surpin.data.arrowflight.server.services;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests of {@link ConcatenatingStreamListener} (item 30 part 2b). */
class ConcatenatingStreamListenerTest {

    private static final Schema SCHEMA = new Schema(List.of(
            Field.nullable("v", new ArrowType.Int(64, true))));

    /** Batches of two readers reach the client as one stream started once. */
    @Test
    void batchesOfSeveralReadersGoToOneStream() {
        FlightProducer.ServerStreamListener client = mock(FlightProducer.ServerStreamListener.class);
        List<VectorSchemaRoot> started = new ArrayList<>();
        List<Long> sent = new ArrayList<>();
        doAnswer(call -> {
            started.add(call.getArgument(0));
            return null;
        }).when(client).start(any(), any(), any());
        doAnswer(call -> {
            BigIntVector v = (BigIntVector) started.get(0).getVector("v");
            for (int i = 0; i < started.get(0).getRowCount(); i++) {
                sent.add(v.get(i));
            }
            return null;
        }).when(client).putNext();
        try (BufferAllocator allocator = new RootAllocator();
                ConcatenatingStreamListener stream = new ConcatenatingStreamListener(client, allocator)) {
            for (int reader = 0; reader < 2; reader++) {
                try (VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
                    stream.start(root);
                    for (int batch = 0; batch < 2; batch++) {
                        fill(root, reader * 10 + batch * 2, 2);
                        stream.putNext();
                    }
                }
            }
            assertTrue(stream.started());
            stream.completed();
        }
        assertEquals(1, started.size());
        assertEquals(List.of(0L, 1L, 2L, 3L, 10L, 11L, 12L, 13L), sent);
        verify(client, times(4)).putNext();
        verify(client).completed();
    }

    /** A reader with other columns, or dictionary-encoded ones, is refused. */
    @Test
    void otherColumnsAndDictionariesAreRefused() {
        FlightProducer.ServerStreamListener client = mock(FlightProducer.ServerStreamListener.class);
        Schema other = new Schema(List.of(Field.nullable("w", new ArrowType.Utf8())));
        Schema dictionary = new Schema(List.of(new Field("v", new FieldType(true, new ArrowType.Utf8(),
                new DictionaryEncoding(1L, false, null)), null)));
        try (BufferAllocator allocator = new RootAllocator();
                ConcatenatingStreamListener stream = new ConcatenatingStreamListener(client, allocator);
                VectorSchemaRoot first = VectorSchemaRoot.create(SCHEMA, allocator);
                VectorSchemaRoot second = VectorSchemaRoot.create(other, allocator);
                VectorSchemaRoot encoded = new VectorSchemaRoot(List.of(
                        new VarCharVector(dictionary.getFields().get(0), allocator)))) {
            assertThrows(IllegalStateException.class, stream::putNext);
            stream.start(first);
            assertThrows(IllegalStateException.class, () -> stream.start(second));
            assertThrows(IllegalStateException.class, () -> stream.start(encoded));
        }
    }

    /**
     * Fills the column of a root with consecutive values.
     *
     * @param root root of {@link #SCHEMA}
     * @param from first value
     * @param rows number of rows
     */
    private static void fill(VectorSchemaRoot root, long from, int rows) {
        BigIntVector v = (BigIntVector) root.getVector("v");
        v.allocateNew(rows);
        for (int i = 0; i < rows; i++) {
            v.set(i, from + i);
        }
        v.setValueCount(rows);
        root.setRowCount(rows);
    }
}
